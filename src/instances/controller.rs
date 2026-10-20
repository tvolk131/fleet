//! One instance's controller: reconciles its recorded desired state with a VM driver
//! (`crate::driver`). The driver keeps the VM under `instances/<id>/driver/vm/`.

use std::{
    fs,
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::Duration,
};

use super::{
    Config, Desired, DriverKind, Info, Observed, State, directory, info, storage, try_lock, write,
};
use crate::driver::{
    self, Driver, InstanceId, InstanceSpec, Phase, RunnerDriver, StopMode, fake::FakeBackend,
};
use anyhow::{Context, Result, bail};

/// The driver's name for the instance's VM; each instance has a driver of its own.
const VM: &str = "vm";
/// How long a guest gets to shut down before it is forced off.
const STOP_GRACE: Duration = Duration::from_secs(30);

fn driver_root(home: &Path, id: &str) -> Result<PathBuf> {
    Ok(directory(home, id)?.join("driver"))
}

/// `fleet guest-exec` for the instance's guest, ready for the guest argv to be appended. It
/// passes stdio through, exits with the guest command's code, and killing it ends that command.
pub fn guest_exec(home: &Path, id: &str) -> Result<Command> {
    let mut command = Command::new(super::guest::companion("fleet")?);
    command
        .arg("guest-exec")
        .arg(driver_root(home, id)?.canonicalize()?.join(VM))
        .arg("--");
    Ok(command)
}

enum Vms {
    #[cfg(target_os = "macos")]
    Vz(RunnerDriver<driver::vz::VzBackend>),
    Fake(RunnerDriver<FakeBackend>),
}

impl Vms {
    fn open(home: &Path, id: &str, kind: DriverKind) -> Result<Self> {
        let root = driver_root(home, id)?;
        Ok(match kind {
            #[cfg(target_os = "macos")]
            DriverKind::Vz => {
                let runner = super::guest::companion("fleet-runner-vz")?;
                ensure_signed(home, &runner)?;
                Self::Vz(RunnerDriver::new(root, driver::vz::VzBackend { runner })?)
            }
            #[cfg(not(target_os = "macos"))]
            DriverKind::Vz => bail!("the vz driver needs macOS"),
            DriverKind::Fake => Self::Fake(RunnerDriver::new(
                root,
                FakeBackend {
                    fleet: super::guest::companion("fleet")?,
                },
            )?),
        })
    }
    fn get(&self) -> &dyn Driver {
        match self {
            #[cfg(target_os = "macos")]
            Self::Vz(d) => d,
            Self::Fake(d) => d,
        }
    }
}

/// Virtualization.framework only runs a VM for a process signed with its entitlement.
#[cfg(target_os = "macos")]
fn ensure_signed(home: &Path, runner: &Path) -> Result<()> {
    if driver::vz::runner_signed(runner)? {
        return Ok(());
    }
    let path = home.join("runner-sign.lock");
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let _lock = loop {
        if let Some(lock) = try_lock(&path)? {
            break lock;
        }
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "signing {} is busy",
            runner.display()
        );
        std::thread::sleep(Duration::from_millis(50));
    };
    if !driver::vz::runner_signed(runner)? {
        driver::vz::sign_runner(runner)?;
    }
    Ok(())
}

/// A locally administered unicast address, stable for the instance.
fn mac(id: &str) -> [u8; 6] {
    let hash = blake3::hash(id.as_bytes());
    let b = hash.as_bytes();
    [0x02, b[0], b[1], b[2], b[3], b[4]]
}

fn spec(config: &Config, instance: &Info) -> Result<InstanceSpec> {
    let record = &instance.record;
    let image = config
        .images
        .get(&record.spec.image)
        .context("image missing from saved catalog")?;
    Ok(InstanceSpec {
        id: InstanceId::new(VM)?,
        generation: record.revision,
        guest: image.guest,
        image: image.bundle.clone(),
        cpus: record.spec.cpus,
        memory_mb: record.spec.memory_mb,
        disk_gb: record.spec.disk_gb,
        mac: mac(&record.id),
        mtu: 1500,
    })
}

fn disk_guard(home: &Path, config: &Config) -> Result<()> {
    if storage::free_bytes(home)? < u64::from(config.min_free_disk_gb) * (1 << 30) {
        bail!("worker's VM free disk guard reached");
    }
    Ok(())
}

fn tail(path: &Path) -> String {
    (|| -> Result<String> {
        let mut file = fs::File::open(path)?;
        let start = file.metadata()?.len().saturating_sub(4096);
        file.seek(SeekFrom::Start(start))?;
        let mut bytes = Vec::new();
        file.take(4096).read_to_end(&mut bytes)?;
        Ok(String::from_utf8_lossy(&bytes).trim().to_owned())
    })()
    .unwrap_or_default()
}

fn observe(home: &Path, instance: &Info, state: State, error: Option<String>) -> Result<()> {
    let old = info(home, &instance.record.id)?.observed;
    if old.state == state && old.revision == instance.record.revision && old.error == error {
        return Ok(());
    }
    write(
        &directory(home, &instance.record.id)?.join("observed.json"),
        &Observed {
            state,
            revision: instance.record.revision,
            updated_at: crate::jobs::now(),
            error,
        },
    )
}

fn provision(
    home: &Path,
    dir: &Path,
    config: &Config,
    instance: &Info,
    driver: &dyn Driver,
) -> Result<()> {
    disk_guard(home, config)?;
    observe(home, instance, State::Creating, None)?;
    let image = config
        .images
        .get(&instance.record.spec.image)
        .context("image missing from saved catalog")?;
    let cancelled = || {
        crate::jobs::read_json::<super::Record>(&dir.join("desired.json"))
            .ok()
            .flatten()
            .is_some_and(|r| r.job.is_some() && r.desired != Desired::Running)
    };
    driver::bundle::verify(
        &image.bundle,
        &image.digest,
        &home.join("image-digests.json"),
        &cancelled,
    )?;
    if cancelled() {
        bail!("job VM provisioning was cancelled");
    }
    driver.prepare(&spec(config, instance)?)?;
    disk_guard(home, config)?;
    write(&dir.join("provisioned.json"), &true)?;
    Ok(())
}

/// This process owns no coordinator credentials. It operates only its recorded VM.
pub fn run(home: &Path, id: &str) -> Result<()> {
    let dir = directory(home, id)?;
    let Some(_lock) = try_lock(&dir.join("controller.lock"))? else {
        return Ok(());
    };
    let config: Config = crate::jobs::read_json(&dir.join("runtime.json"))?
        .context("missing instance runtime configuration")?;
    let vms = Vms::open(home, id, config.driver)?;
    let driver = vms.get();
    let vm = InstanceId::new(VM)?;
    let mut awake: Option<Child> = None;
    let mut failed_revision = None;
    loop {
        let instance = info(home, id)?;
        if failed_revision == Some(instance.record.revision) {
            std::thread::sleep(Duration::from_secs(1));
            continue;
        }
        let result: Result<bool> = (|| {
            let observed = driver.observe(&vm)?;
            let revision = instance.record.revision;
            // A failure recorded at an older generation belongs to a run already accounted for.
            let ended_badly = matches!(observed.phase, Phase::Failed | Phase::Lost)
                && observed.generation == revision;
            if instance.record.desired == Desired::Deleted {
                if matches!(
                    observed.phase,
                    Phase::Running | Phase::Starting | Phase::Stopping
                ) {
                    bail!("refusing to delete a VM that is not confirmed stopped");
                }
                observe(home, &instance, State::Deleting, None)?;
                driver.delete(&vm, revision)?;
                let _ = fs::remove_dir_all(driver_root(home, id)?);
                observe(home, &instance, State::Deleted, None)?;
                return Ok(true);
            }
            match observed.phase {
                Phase::Running if instance.record.desired == Desired::Stopped => {
                    observe(home, &instance, State::Stopping, None)?;
                    driver.stop(
                        &vm,
                        revision,
                        StopMode::Graceful {
                            deadline: STOP_GRACE,
                        },
                    )?;
                    return Ok(false);
                }
                Phase::Running => {
                    if let Err(error) = disk_guard(home, &config) {
                        driver.stop(
                            &vm,
                            revision,
                            StopMode::Graceful {
                                deadline: STOP_GRACE,
                            },
                        )?;
                        return Err(error.context("VM stopped at the free disk guard"));
                    }
                    if awake.is_none() {
                        awake = Command::new("/usr/bin/caffeinate")
                            .args(["-i", "-w", &std::process::id().to_string()])
                            .stdin(Stdio::null())
                            .stdout(Stdio::null())
                            .stderr(Stdio::null())
                            .spawn()
                            .ok();
                    }
                    observe(home, &instance, State::Running, None)?;
                    return Ok(false);
                }
                Phase::Starting | Phase::Stopping => return Ok(false),
                _ => {}
            }
            if let Some(mut c) = awake.take() {
                let _ = c.kill();
                let _ = c.wait();
            }
            if ended_badly && instance.record.desired == Desired::Running {
                bail!(
                    "VM ended unexpectedly ({}); VMs outside Fleet also consume platform capacity. See runner.log: {}",
                    observed
                        .error
                        .unwrap_or_else(|| "its runner disappeared".into()),
                    tail(&driver_root(home, id)?.join(VM).join("runner.log"))
                );
            }
            if instance.record.desired == Desired::Stopped && revision > 1 {
                // An explicit stop must also succeed after provisioning failed part-way. The VM is
                // confirmed stopped (or absent) here; retrying provisioning would make
                // cancellation depend on repairing the original invalid configuration.
                observe(home, &instance, State::Stopped, None)?;
                return Ok(false);
            }
            if !dir.join("provisioned.json").exists() {
                provision(home, &dir, &config, &instance, driver)?;
            } else if observed.phase == Phase::Absent {
                bail!("VM disk disappeared; refusing to replace persistent data");
            }
            if instance.record.desired == Desired::Stopped {
                observe(home, &instance, State::Stopped, None)?;
                return Ok(false);
            }
            if info(home, id)?.record.revision != revision {
                return Ok(false);
            }
            // Native submissions see desired=running and cannot race this start.
            let capacity: crate::worker_data::Hello =
                match crate::jobs::read_json(&home.join("instance-capacity.json"))? {
                    Some(current) => current,
                    None => crate::jobs::read_json(&dir.join("capacity.json"))?
                        .context("missing capacity record")?,
                };
            super::admit_vm_excluding(home, &instance.record.spec, &capacity, Some(id))
                .context("current reservations prevent restarting the VM")?;
            disk_guard(home, &config)?;
            observe(home, &instance, State::Starting, None)?;
            driver.start(&spec(&config, &instance)?).map_err(|e| {
                anyhow::anyhow!(
                    "VM failed to start ({e}); VMs outside Fleet also consume platform capacity"
                )
            })?;
            Ok(false)
        })();
        match result {
            Ok(true) => return Ok(()),
            Ok(false) => {}
            Err(e) => {
                observe(home, &instance, State::Failed, Some(format!("{e:#}")))?;
                failed_revision = Some(instance.record.revision);
            }
        }
        std::thread::sleep(Duration::from_secs(1));
    }
}
