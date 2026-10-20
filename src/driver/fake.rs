//! A driver with no hypervisor: the "guest" is a few threads in the runner process that meet
//! the conformance test-guest contract (see `conformance`). It runs everything except real
//! virtualization, including real runner processes, so the supervision code is what is tested.
//!
//! An image bundle for the fake is a directory containing `fake.json` ([`FakeImage`]).
//!
//! Its guest agent is Fleet's real one, running commands on the host. They see a private
//! `/tmp`: every `/tmp/` in their arguments becomes the instance's `guest/tmp/`, which a fresh
//! boot empties. A `{"kind":"prepare","platform":..}` request names the host's own platform, so
//! the guest helper accepts it on any host. `FLEET_TEST_VM_LIFETIME` names a file holding the
//! runner's pid and the boot's identity while the guest runs, so test workloads can end with
//! their VM.

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
    /// A directory of switches that tests flip while instances run, relative to the bundle
    /// if it is relative, so a bundle's digest need not depend on where it is. The fake appends what its
    /// guests do to `history` there and obeys these files:
    ///
    /// - `fail-prepare`: preparing fails after the disk is created;
    /// - `fail-start`: starting fails, as when the hypervisor is at its VM limit;
    /// - `fail-after-start`: a started guest fails 3 s later, or as soon as
    ///   `release-failure` exists when `gate-failure` does;
    /// - `refuse-stop`: guests ignore graceful stop requests;
    /// - `slow-boot`: starting takes 8 s.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub faults: Option<PathBuf>,
}

impl FakeImage {
    pub fn write(&self, bundle: &Path) -> Result<()> {
        std::fs::create_dir_all(bundle)?;
        std::fs::write(bundle.join("fake.json"), serde_json::to_vec(self)?)?;
        Ok(())
    }
    fn read(bundle: &Path) -> Result<Self> {
        let path = bundle.join("fake.json");
        let mut image: Self = serde_json::from_slice(
            &std::fs::read(&path).with_context(|| format!("reading {}", path.display()))?,
        )
        .context("invalid fake image")?;
        image.faults = image.faults.map(|dir| bundle.join(dir));
        Ok(image)
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
        let image = FakeImage::read(&spec.image)?;
        // Stands in for a disk cloned from the image.
        std::fs::write(dir.join("disk.img"), [])?;
        if let Some(faults) = image.faults {
            let mut history = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(faults.join("history"))?;
            writeln!(history, "{} prepare", dir.display())?;
            if faults.join("fail-prepare").exists() {
                return Err(super::Error::Invalid(
                    "the image cannot be configured".into(),
                ));
            }
        }
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
    /// The instance directory.
    dir: PathBuf,
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
        if self.is_stopped() {
            return;
        }
        // Before the exit is visible: the runner may end as soon as it is.
        self.log(&format!("exit {exit:?}"));
        let _ = std::fs::remove_file(self.lifetime());
        let mut guest = self.state.lock().unwrap();
        if guest.exit.is_none() {
            guest.exit = Some(exit);
            self.stopped.notify_all();
        }
    }
    fn guest_tmp(&self) -> PathBuf {
        self.dir.join("guest/tmp")
    }
    fn lifetime(&self) -> PathBuf {
        self.dir.join("guest/alive")
    }
    fn fault(&self, name: &str) -> bool {
        let faults = self.state.lock().unwrap().image.faults.clone();
        faults.is_some_and(|dir| dir.join(name).exists())
    }
    fn log(&self, event: &str) {
        let faults = self.state.lock().unwrap().image.faults.clone();
        if let Some(dir) = faults {
            let line = format!("{} {event}\n", self.dir.display());
            let _ = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(dir.join("history"))
                .and_then(|mut f| f.write_all(line.as_bytes()));
        }
    }
    fn ignores_stop(&self) -> bool {
        self.state.lock().unwrap().image.ignore_graceful_stop || self.fault("refuse-stop")
    }
    /// A guest command, confined as the module documentation describes.
    fn command(&self, argv: &[String]) -> Command {
        let tmp = format!("{}/", self.guest_tmp().display());
        let argv: Vec<String> = argv
            .iter()
            .map(|arg| match serde_json::from_str::<serde_json::Value>(arg) {
                Ok(mut value) if value.is_object() => {
                    host_platform(&mut value);
                    private_tmp(&value.to_string(), &tmp)
                }
                _ => private_tmp(arg, &tmp),
            })
            .collect();
        self.log(&format!("spawn {}", argv.join(" ")));
        let mut command = Command::new(&argv[0]);
        command
            .args(&argv[1..])
            .env("FLEET_TEST_VM_LIFETIME", self.lifetime());
        command
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
        self.log(if restore.is_some() { "restore" } else { "boot" });
        if self.fault("fail-start") {
            bail!("The maximum supported number of virtual machines has been reached.");
        }
        if self.fault("slow-boot") {
            std::thread::sleep(Duration::from_secs(8));
        }
        if restore.is_none() {
            let _ = std::fs::remove_dir_all(self.guest_tmp());
        }
        std::fs::create_dir_all(self.guest_tmp())?;
        // The runner's pid first: a forced stop kills the runner but not the guest's commands.
        let boot = format!("{} {:016x}", std::process::id(), rand::random::<u64>());
        std::fs::write(self.lifetime(), boot)?;
        if self.fault("fail-after-start") {
            let vm = self.clone();
            std::thread::spawn(move || {
                let began = std::time::Instant::now();
                while if vm.fault("gate-failure") {
                    !vm.fault("release-failure")
                } else {
                    began.elapsed() < Duration::from_secs(3)
                } {
                    std::thread::sleep(Duration::from_millis(20));
                }
                vm.power_off(Exit::Failed {
                    error: "Late virtual machine capacity error".into(),
                });
            });
        }
        let vm = self.clone();
        let mac = spec.mac;
        std::thread::spawn(move || nic(&vm, &card, mac));
        Ok(())
    }

    fn request_stop(&self) -> Result<()> {
        self.log("power-button");
        if !self.ignores_stop() {
            let vm = self.clone();
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(50));
                vm.power_off(Exit::PoweredOff);
            });
        }
        Ok(())
    }

    fn connect_guest(&self, port: u32) -> Result<OwnedFd> {
        if self.is_stopped() || ![conformance::ECHO_PORT, super::AGENT_PORT].contains(&port) {
            bail!("connection refused: nothing listens on guest port {port}");
        }
        if port == super::AGENT_PORT {
            let (host, guest) = UnixStream::pair()?;
            let vm = self.clone();
            std::thread::spawn(move || {
                let guest = std::fs::File::from(OwnedFd::from(guest));
                let shutdown = || {
                    vm.log("agent-shutdown");
                    anyhow::ensure!(!vm.ignores_stop(), "shutdown refused");
                    vm.power_off(Exit::PoweredOff);
                    Ok(())
                };
                if let Err(error) =
                    fleet_guest::agent::handle(guest, |argv| vm.command(argv), shutdown)
                {
                    eprintln!("fake guest agent: {error:#}");
                }
            });
            return Ok(host.into());
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
    let vm = FakeVm {
        dir: args.dir.clone(),
        ..FakeVm::default()
    };
    super::runner::run(args, Arc::new(vm))
}

/// Replaces each `/tmp/` that begins a path, leaving ones inside other paths such as
/// `/private/tmp/`.
fn private_tmp(arg: &str, tmp: &str) -> String {
    let mut out = String::with_capacity(arg.len());
    let mut rest = arg;
    while let Some(at) = rest.find("/tmp/") {
        let starts_path = out
            .chars()
            .chain(rest[..at].chars())
            .last()
            .is_none_or(|c| !(c.is_alphanumeric() || "._-~".contains(c)));
        out.push_str(&rest[..at]);
        out.push_str(if starts_path { tmp } else { "/tmp/" });
        rest = &rest[at + "/tmp/".len()..];
    }
    out.push_str(rest);
    out
}

/// Points every guest `prepare` request's platform at this host.
fn host_platform(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            if map.get("kind").and_then(|k| k.as_str()) == Some("prepare")
                && let Some(platform) = map.get_mut("platform").and_then(|p| p.as_object_mut())
            {
                platform.insert("os".into(), std::env::consts::OS.into());
                platform.insert("arch".into(), std::env::consts::ARCH.into());
            }
            map.values_mut().for_each(host_platform);
        }
        serde_json::Value::Array(values) => values.iter_mut().for_each(host_platform),
        // An argument can itself be a JSON request, as `fleet-guest --job <observation>` is.
        serde_json::Value::String(text) => {
            if let Ok(mut inner) = serde_json::from_str::<serde_json::Value>(text)
                && inner.is_object()
            {
                host_platform(&mut inner);
                *text = inner.to_string();
            }
        }
        _ => {}
    }
}
