//! A driver with no hypervisor: the "guest" is a few threads in the runner process that meet
//! the conformance test-guest contract (see `conformance`). It runs everything except real
//! virtualization, including real runner processes, so the supervision code is what is tested.
//!
//! An image bundle for the fake is a directory containing `fake.json` ([`FakeImage`]).

use std::{
    io::{Read, Write},
    os::{
        fd::{AsRawFd, OwnedFd},
        unix::net::UnixStream,
    },
    path::{Path, PathBuf},
    process::Command,
    sync::{Arc, Condvar, Mutex},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use super::{Backend, Capabilities, Exit, GuestOs, InstanceSpec, conformance, runner::Vm};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FakeImage {
    /// The guest ignores graceful stop requests, so the driver has to force it.
    #[serde(default)]
    pub ignore_graceful_stop: bool,
    #[serde(default)]
    pub boot_ms: u64,
}

impl FakeImage {
    pub fn write(&self, bundle: &Path) -> Result<()> {
        std::fs::create_dir_all(bundle)?;
        std::fs::write(bundle.join("fake.json"), serde_json::to_vec(self)?)?;
        Ok(())
    }
    fn read(bundle: &Path) -> Result<Self> {
        let path = bundle.join("fake.json");
        serde_json::from_slice(
            &std::fs::read(&path).with_context(|| format!("reading {}", path.display()))?,
        )
        .context("invalid fake image")
    }
}

pub struct FakeBackend {
    /// The `fleet` executable that provides `driver-runner`.
    pub fleet: PathBuf,
}

impl Backend for FakeBackend {
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            driver: "fake".into(),
            guests: vec![GuestOs::Linux, GuestOs::Macos],
            max_macos_guests: 2,
            save_restore: true,
            configurable_mtu: true,
        }
    }

    fn prepare(&self, spec: &InstanceSpec, dir: &Path) -> super::Result<()> {
        FakeImage::read(&spec.image)?;
        // Stands in for a disk cloned from the image.
        std::fs::write(dir.join("disk.img"), [])?;
        Ok(())
    }

    fn runner(&self, dir: &Path) -> Command {
        let mut command = Command::new(&self.fleet);
        command.arg("driver-runner").arg("fake").arg(dir);
        command
    }
}

#[derive(Default)]
pub struct FakeVm {
    state: Mutex<Guest>,
    stopped: Condvar,
}

#[derive(Default)]
struct Guest {
    image: FakeImage,
    /// Echo replies since boot; saved and restored with the guest's state.
    served: u64,
    exit: Option<Exit>,
}

#[derive(Serialize, Deserialize)]
struct Saved {
    served: u64,
}

impl FakeVm {
    fn power_off(&self, exit: Exit) {
        let mut guest = self.state.lock().unwrap();
        if guest.exit.is_none() {
            guest.exit = Some(exit);
            self.stopped.notify_all();
        }
    }
    fn is_stopped(&self) -> bool {
        self.state.lock().unwrap().exit.is_some()
    }
}

impl Vm for Arc<FakeVm> {
    fn start(&self, spec: &InstanceSpec, card: OwnedFd, restore: Option<&Path>) -> Result<()> {
        let image = FakeImage::read(&spec.image)?;
        std::thread::sleep(Duration::from_millis(image.boot_ms));
        let served = match restore {
            Some(path) => serde_json::from_slice::<Saved>(&std::fs::read(path)?)?.served,
            None => 0,
        };
        {
            let mut guest = self.state.lock().unwrap();
            guest.image = image;
            guest.served = served;
        }
        let vm = self.clone();
        let mac = spec.mac;
        std::thread::spawn(move || nic(&vm, &card, mac));
        Ok(())
    }

    fn request_stop(&self) -> Result<()> {
        if !self.state.lock().unwrap().image.ignore_graceful_stop {
            let vm = self.clone();
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(50));
                vm.power_off(Exit::PoweredOff);
            });
        }
        Ok(())
    }

    fn connect_guest(&self, port: u32) -> Result<OwnedFd> {
        if port != conformance::ECHO_PORT || self.is_stopped() {
            bail!("connection refused: nothing listens on guest port {port}");
        }
        let (host, mut guest) = UnixStream::pair()?;
        let vm = self.clone();
        std::thread::spawn(move || {
            let mut buf = [0u8; 4096];
            let n = guest.read(&mut buf).unwrap_or(0);
            let message = String::from_utf8_lossy(&buf[..n]).trim().to_owned();
            if message == "poweroff" {
                let _ = guest.write_all(b"powering off");
                drop(guest);
                vm.power_off(Exit::PoweredOff);
                return;
            }
            let served = {
                let mut state = vm.state.lock().unwrap();
                state.served += 1;
                state.served
            };
            let _ = guest.write_all(format!("echo#{served}: {message}").as_bytes());
        });
        Ok(host.into())
    }

    fn save(&self, to: &Path) -> Result<()> {
        let served = self.state.lock().unwrap().served;
        std::fs::write(to, serde_json::to_vec(&Saved { served })?)?;
        self.power_off(Exit::Saved { path: to.into() });
        Ok(())
    }

    fn wait(&self) -> Exit {
        let mut guest = self.state.lock().unwrap();
        loop {
            if let Some(exit) = &guest.exit {
                return exit.clone();
            }
            guest = self.stopped.wait(guest).unwrap();
        }
    }
}

/// The guest side of the network card: a frame every 200 ms, and an echo of each test frame.
fn nic(vm: &FakeVm, card: &OwnedFd, mac: [u8; 6]) {
    let mut buf = vec![0u8; 65536];
    let mut sent = 0u64;
    while !vm.is_stopped() {
        let mut poll = libc::pollfd {
            fd: card.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        if unsafe { libc::poll(&mut poll, 1, 200) } > 0 {
            let n = unsafe { libc::recv(card.as_raw_fd(), buf.as_mut_ptr().cast(), buf.len(), 0) };
            if n > 14 && buf[12..14] == conformance::TEST_ETHERTYPE.to_be_bytes() {
                let mut payload = b"echo: ".to_vec();
                payload.extend_from_slice(&buf[14..n as usize]);
                send(card, &buf[6..12], mac, &payload);
            }
            continue;
        }
        sent += 1;
        send(
            card,
            &[0xff; 6],
            mac,
            format!("fake frame {sent}").as_bytes(),
        );
    }
}

fn send(card: &OwnedFd, to: &[u8], from: [u8; 6], payload: &[u8]) {
    let frame = conformance::frame(to, &from, payload);
    unsafe {
        libc::send(
            card.as_raw_fd(),
            frame.as_ptr().cast(),
            frame.len(),
            libc::MSG_DONTWAIT,
        )
    };
}

/// Entry point for `fleet driver-runner fake`.
pub fn run(args: super::runner::Args) -> Result<()> {
    super::runner::run(args, Arc::new(FakeVm::default()))
}
