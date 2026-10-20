//! The VM inside `fleet-runner-vz`. Virtualization objects are not thread-safe: each one is
//! created on, and only touched from, the VM's serial dispatch queue.

use std::{
    io::Write,
    os::{
        fd::{FromRawFd, IntoRawFd, OwnedFd, RawFd},
        unix::net::UnixStream,
    },
    path::{Path, PathBuf},
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
use block2::{DynBlock, RcBlock};
use dispatch2::{DispatchQueue, DispatchRetained};
use objc2::{AllocAnyThread, rc::Retained};
use objc2_foundation::{NSArray, NSData, NSError, NSFileHandle, NSString, NSURL};
use objc2_virtualization::*;

use super::{DISK, MANIFEST, Manifest};
use crate::driver::{
    AGENT_PORT, Exit, InstanceSpec,
    runner::{self, Vm},
};

/// The guest's virtual hardware identity, created at first boot and kept for the instance's
/// life: macOS ties its installation to it, and a saved state only restores onto the same one.
const MACHINE_ID: &str = "machine-id";
const CONSOLE: &str = "console.log";
const CALL_TIMEOUT: Duration = Duration::from_secs(120);

/// Entry point for `fleet-runner-vz`.
pub fn run(args: runner::Args) -> Result<()> {
    let vm = VzVm {
        dir: args.dir.clone(),
        machine: OnceLock::new(),
        asking_agent: AtomicBool::new(false),
    };
    runner::run(args, Arc::new(vm))
}

/// Moves a value onto the VM's queue. Only sound when the value is used on that queue alone.
struct OnQueue<T>(T);
unsafe impl<T> Send for OnQueue<T> {}
unsafe impl<T> Sync for OnQueue<T> {}

struct Machine {
    queue: DispatchRetained<DispatchQueue>,
    vm: OnQueue<Retained<VZVirtualMachine>>,
}

impl Machine {
    fn sync<R: Send>(&self, f: impl FnOnce(&VZVirtualMachine) -> R + Send) -> R {
        let mut out = None;
        let (vm, slot) = (OnQueue(&*self.vm.0), OnQueue(&mut out));
        self.queue.exec_sync(move || {
            let (vm, slot) = (vm, slot);
            *slot.0 = Some(f(vm.0));
        });
        out.expect("queue ran the closure")
    }

    /// Runs an asynchronous call whose completion handler takes only an error, and waits.
    fn call(
        &self,
        what: &str,
        f: impl FnOnce(&VZVirtualMachine, &DynBlock<dyn Fn(*mut NSError)>) + Send,
    ) -> Result<()> {
        let (tx, rx) = mpsc::channel();
        let done = OnQueue(RcBlock::new(move |error: *mut NSError| {
            let _ = tx.send(if error.is_null() {
                Ok(())
            } else {
                Err(describe(unsafe { &*error }))
            });
        }));
        self.sync(move |vm| {
            let done = done;
            f(vm, &done.0)
        });
        match rx.recv_timeout(CALL_TIMEOUT) {
            Ok(result) => result.map_err(|e| anyhow!("{what}: {e}")),
            Err(_) => bail!("{what}: no completion within {CALL_TIMEOUT:?}"),
        }
    }

    fn state(&self) -> VZVirtualMachineState {
        self.sync(|vm| unsafe { vm.state() })
    }
}

pub struct VzVm {
    dir: PathBuf,
    machine: OnceLock<Machine>,
    /// A thread is asking the guest agent to shut down; repeated stop requests share it.
    asking_agent: AtomicBool,
}

impl VzVm {
    fn machine(&self) -> Result<&Machine> {
        self.machine.get().context("the VM has not started")
    }
}

impl Vm for Arc<VzVm> {
    fn start(&self, spec: &InstanceSpec, card: OwnedFd, restore: Option<&Path>) -> Result<()> {
        let config = configure(&self.dir, spec, card)?;
        let queue = DispatchQueue::new("io.fleet.vz", None);
        let vm = {
            let (config, own, mut out) = (OnQueue(config), OnQueue(queue.clone()), None);
            let slot = OnQueue(&mut out);
            queue.exec_sync(move || {
                let (config, own, slot) = (config, own, slot);
                *slot.0 = Some(OnQueue(unsafe {
                    VZVirtualMachine::initWithConfiguration_queue(
                        VZVirtualMachine::alloc(),
                        &config.0,
                        &own.0,
                    )
                }));
            });
            out.expect("queue ran the closure")
        };
        let machine = self.machine.get_or_init(|| Machine { queue, vm });
        match restore {
            Some(path) => {
                let url = OnQueue(file_url(path));
                machine.call("restoring the saved state", move |vm, done| {
                    let url = url;
                    unsafe { vm.restoreMachineStateFromURL_completionHandler(&url.0, done) }
                })?;
                machine.call("resuming", |vm, done| unsafe {
                    vm.resumeWithCompletionHandler(done)
                })
            }
            None => machine.call("starting", |vm, done| unsafe {
                vm.startWithCompletionHandler(done)
            }),
        }
    }

    fn request_stop(&self) -> Result<()> {
        let machine = self.machine()?;
        // Both are best effort: the power button for guests that handle it, the agent for the
        // rest. The driver forces the VM once its deadline passes.
        machine.sync(|vm| unsafe {
            if vm.canRequestStop() {
                let _ = vm.requestStopWithError();
            }
        });
        // A guest still booting has no agent yet: keep asking until one answers or the VM ends.
        if self.asking_agent.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        let vm = self.clone();
        std::thread::spawn(move || {
            let Ok(machine) = vm.machine() else { return };
            while !matches!(
                machine.state(),
                VZVirtualMachineState::Stopped | VZVirtualMachineState::Error
            ) {
                let asked = vm.connect_guest(AGENT_PORT).and_then(|fd| {
                    UnixStream::from(fd).write_all(crate::driver::SHUTDOWN_REQUEST)?;
                    Ok(())
                });
                if asked.is_ok() {
                    break;
                }
                std::thread::sleep(Duration::from_millis(100));
            }
            vm.asking_agent.store(false, Ordering::SeqCst);
        });
        Ok(())
    }

    fn connect_guest(&self, port: u32) -> Result<OwnedFd> {
        let machine = self.machine()?;
        let (tx, rx) = mpsc::channel::<Result<RawFd, String>>();
        let done = OnQueue(RcBlock::new(
            move |connection: *mut VZVirtioSocketConnection, error: *mut NSError| {
                let _ = tx.send(if connection.is_null() {
                    Err(describe(unsafe { &*error }))
                } else {
                    // The connection closes its descriptor when released; keep a duplicate.
                    Ok(unsafe { libc::dup((*connection).fileDescriptor()) })
                });
            },
        ));
        machine.sync(move |vm| {
            let done = done;
            let devices = unsafe { vm.socketDevices() };
            let device = devices
                .firstObject()
                .and_then(|d| d.downcast::<VZVirtioSocketDevice>().ok());
            match device {
                Some(device) => unsafe { device.connectToPort_completionHandler(port, &done.0) },
                None => unreachable!("every configuration has one virtio socket device"),
            }
        });
        match rx.recv_timeout(Duration::from_secs(10)) {
            Ok(Ok(fd)) if fd >= 0 => {
                let fd = unsafe { OwnedFd::from_raw_fd(fd) };
                unsafe { libc::fcntl(fd_of(&fd), libc::F_SETFD, libc::FD_CLOEXEC) };
                Ok(fd)
            }
            Ok(Ok(_)) => bail!("dup: {}", std::io::Error::last_os_error()),
            Ok(Err(e)) => bail!("connecting to guest port {port}: {e}"),
            Err(_) => bail!("connecting to guest port {port}: timed out"),
        }
    }

    fn save(&self, to: &Path) -> Result<()> {
        let machine = self.machine()?;
        if machine.state() == VZVirtualMachineState::Running {
            machine.call("pausing", |vm, done| unsafe {
                vm.pauseWithCompletionHandler(done)
            })?;
        }
        let _ = std::fs::remove_file(to);
        let url = OnQueue(file_url(to));
        let saved = machine.call("saving", move |vm, done| {
            let url = url;
            unsafe { vm.saveMachineStateToURL_completionHandler(&url.0, done) }
        });
        if saved.is_err() {
            let _ = machine.call("resuming", |vm, done| unsafe {
                vm.resumeWithCompletionHandler(done)
            });
        }
        saved
    }

    fn wait(&self) -> Exit {
        let Ok(machine) = self.machine() else {
            return Exit::Failed {
                error: "the VM never started".into(),
            };
        };
        loop {
            match machine.state() {
                VZVirtualMachineState::Stopped => return Exit::PoweredOff,
                VZVirtualMachineState::Error => {
                    return Exit::Failed {
                        error: "the VM stopped with an error".into(),
                    };
                }
                _ => std::thread::sleep(Duration::from_millis(100)),
            }
        }
    }
}

fn fd_of(fd: &OwnedFd) -> RawFd {
    std::os::fd::AsRawFd::as_raw_fd(fd)
}

fn describe(error: &NSError) -> String {
    format!(
        "{} ({} {})",
        error.localizedDescription(),
        error.domain(),
        error.code()
    )
}

fn ns_error(error: Retained<NSError>) -> anyhow::Error {
    anyhow!(describe(&error))
}

fn file_url(path: &Path) -> Retained<NSURL> {
    NSURL::fileURLWithPath(&NSString::from_str(&path.to_string_lossy()))
}

fn mac_address(mac: [u8; 6]) -> String {
    mac.map(|b| format!("{b:02x}")).join(":")
}

/// Reads the machine identifier, or creates and records a new one.
fn identity<T>(
    dir: &Path,
    decode: impl FnOnce(&NSData) -> Option<Retained<T>>,
    create: impl FnOnce() -> Retained<T>,
    encode: impl FnOnce(&T) -> Retained<NSData>,
) -> Result<Retained<T>> {
    let path = dir.join(MACHINE_ID);
    match std::fs::read(&path) {
        Ok(bytes) => decode(&NSData::with_bytes(&bytes)).context("invalid stored machine id"),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            let id = create();
            std::fs::write(&path, encode(&id).to_vec())?;
            Ok(id)
        }
        Err(e) => Err(e.into()),
    }
}

fn configure(
    dir: &Path,
    spec: &InstanceSpec,
    card: OwnedFd,
) -> Result<Retained<VZVirtualMachineConfiguration>> {
    let manifest = Manifest::read(dir).with_context(|| format!("instance {MANIFEST}"))?;
    let file = |name: &str| file_url(&dir.join(name));
    unsafe {
        let config = VZVirtualMachineConfiguration::new();
        config.setCPUCount(spec.cpus as usize);
        config.setMemorySize(u64::from(spec.memory_mb) << 20);

        match &manifest {
            Manifest::Linux {
                kernel,
                initrd,
                cmdline,
                ..
            } => {
                let boot =
                    VZLinuxBootLoader::initWithKernelURL(VZLinuxBootLoader::alloc(), &file(kernel));
                if let Some(initrd) = initrd {
                    boot.setInitialRamdiskURL(Some(&file(initrd)));
                }
                boot.setCommandLine(&NSString::from_str(cmdline));
                config.setBootLoader(Some(&boot));

                let id = identity(
                    dir,
                    |data| {
                        VZGenericMachineIdentifier::initWithDataRepresentation(
                            VZGenericMachineIdentifier::alloc(),
                            data,
                        )
                    },
                    || VZGenericMachineIdentifier::new(),
                    |id| id.dataRepresentation(),
                )?;
                let platform = VZGenericPlatformConfiguration::new();
                platform.setMachineIdentifier(&id);
                config.setPlatform(&platform);

                let console = VZFileSerialPortAttachment::initWithURL_append_error(
                    VZFileSerialPortAttachment::alloc(),
                    &file(CONSOLE),
                    true,
                )
                .map_err(ns_error)
                .context("console")?;
                let serial = VZVirtioConsoleDeviceSerialPortConfiguration::new();
                serial.setAttachment(Some(&console));
                config.setSerialPorts(&NSArray::from_retained_slice(&[Retained::into_super(
                    serial,
                )]));
                config.setEntropyDevices(&NSArray::from_retained_slice(&[Retained::into_super(
                    VZVirtioEntropyDeviceConfiguration::new(),
                )]));
            }
            Manifest::Macos {
                hardware_model,
                aux,
                ..
            } => {
                let model = VZMacHardwareModel::initWithDataRepresentation(
                    VZMacHardwareModel::alloc(),
                    &NSData::with_bytes(&std::fs::read(dir.join(hardware_model))?),
                )
                .context("invalid hardware model")?;
                if !model.isSupported() {
                    bail!("this host cannot run the image's macOS hardware model");
                }
                let id = identity(
                    dir,
                    |data| {
                        VZMacMachineIdentifier::initWithDataRepresentation(
                            VZMacMachineIdentifier::alloc(),
                            data,
                        )
                    },
                    || VZMacMachineIdentifier::new(),
                    |id| id.dataRepresentation(),
                )?;
                let platform = VZMacPlatformConfiguration::new();
                platform.setHardwareModel(&model);
                platform.setMachineIdentifier(&id);
                platform.setAuxiliaryStorage(Some(&VZMacAuxiliaryStorage::initWithURL(
                    VZMacAuxiliaryStorage::alloc(),
                    &file(aux),
                )));
                config.setPlatform(&platform);
                config.setBootLoader(Some(&VZMacOSBootLoader::new()));

                // macOS expects a display even when nobody looks at it.
                let display = VZMacGraphicsDisplayConfiguration::initWithWidthInPixels_heightInPixels_pixelsPerInch(
                    VZMacGraphicsDisplayConfiguration::alloc(),
                    1024,
                    768,
                    80,
                );
                let graphics = VZMacGraphicsDeviceConfiguration::new();
                graphics.setDisplays(&NSArray::from_retained_slice(&[display]));
                config.setGraphicsDevices(&NSArray::from_retained_slice(&[Retained::into_super(
                    graphics,
                )]));
                config.setKeyboards(&NSArray::from_retained_slice(&[Retained::into_super(
                    VZMacKeyboardConfiguration::new(),
                )]));
                config.setPointingDevices(&NSArray::from_retained_slice(&[Retained::into_super(
                    VZMacTrackpadConfiguration::new(),
                )]));
            }
        }

        let disk = VZDiskImageStorageDeviceAttachment::initWithURL_readOnly_error(
            VZDiskImageStorageDeviceAttachment::alloc(),
            &file(DISK),
            false,
        )
        .map_err(ns_error)
        .context("disk")?;
        let block = VZVirtioBlockDeviceConfiguration::initWithAttachment(
            VZVirtioBlockDeviceConfiguration::alloc(),
            &disk,
        );
        config.setStorageDevices(&NSArray::from_retained_slice(&[Retained::into_super(
            block,
        )]));

        // The guest's only network card. The framework closes the descriptor with the handle.
        let handle = NSFileHandle::initWithFileDescriptor_closeOnDealloc(
            NSFileHandle::alloc(),
            card.into_raw_fd(),
            true,
        );
        let attachment = VZFileHandleNetworkDeviceAttachment::initWithFileHandle(
            VZFileHandleNetworkDeviceAttachment::alloc(),
            &handle,
        );
        attachment.setMaximumTransmissionUnit(spec.mtu as isize);
        let network = VZVirtioNetworkDeviceConfiguration::new();
        network.setAttachment(Some(&attachment));
        let mac = VZMACAddress::initWithString(
            VZMACAddress::alloc(),
            &NSString::from_str(&mac_address(spec.mac)),
        )
        .context("invalid MAC address")?;
        network.setMACAddress(&mac);
        config.setNetworkDevices(&NSArray::from_retained_slice(&[Retained::into_super(
            network,
        )]));
        config.setSocketDevices(&NSArray::from_retained_slice(&[Retained::into_super(
            VZVirtioSocketDeviceConfiguration::new(),
        )]));

        config
            .validateWithError()
            .map_err(ns_error)
            .context("invalid VM configuration")?;
        if let Err(e) = config.validateSaveRestoreSupportWithError() {
            eprintln!("this configuration cannot be saved: {}", describe(&e));
        }
        Ok(config)
    }
}
