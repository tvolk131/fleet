use std::{
    fs,
    io::{Read, Seek, SeekFrom},
    os::unix::{fs::OpenOptionsExt, process::CommandExt},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

use super::{
    Config, Desired, Info, Network, Observed, State, directory, info, network, try_lock, write,
};
use anyhow::{Context, Result, bail};
use serde::Deserialize;

struct Tart {
    config: Config,
    home: PathBuf,
    dir: PathBuf,
    name: String,
}

#[derive(Deserialize)]
struct Vm {
    #[serde(rename = "Name")]
    name: String,
    #[serde(rename = "State")]
    state: String,
}

impl Tart {
    fn command(&self) -> Command {
        let mut command = Command::new(&self.config.tart);
        command
            .env("TART_HOME", self.home.join("instance-tart"))
            .env(
                "PATH",
                format!(
                    "{}:/usr/bin:/bin:/usr/sbin:/sbin",
                    self.config.softnet.parent().unwrap().display()
                ),
            )
            .stdin(Stdio::null())
            .process_group(0);
        command
    }

    fn call(&self, args: &[&str], seconds: u64) -> Result<String> {
        let path = self.dir.join("tart-command.stdout");
        let error_path = self.dir.join("tart-command.stderr");
        let mut child = self
            .command()
            .args(args)
            .stdout(fs::File::create(&path)?)
            .stderr(fs::File::create(&error_path)?)
            .spawn()?;
        let deadline = Instant::now() + Duration::from_secs(seconds);
        loop {
            if fs::metadata(&path)?.len() > 1 << 20 || fs::metadata(&error_path)?.len() > 1 << 20 {
                crate::supervise::signal_group(child.id(), libc::SIGKILL);
                let _ = child.wait();
                bail!("Tart command output exceeded 1 MiB");
            }
            if let Some(status) = child.try_wait()? {
                let stdout = read_small(&path)?;
                if !status.success() {
                    bail!("Tart {} failed: {}", args[0], read_small(&error_path)?);
                }
                return Ok(stdout);
            }
            if Instant::now() >= deadline {
                crate::supervise::signal_group(child.id(), libc::SIGKILL);
                let _ = child.wait();
                bail!("Tart {} timed out", args[0]);
            }
            if args.first() == Some(&"clone") {
                let record: super::Record = crate::jobs::read_json(&self.dir.join("desired.json"))?
                    .context("missing VM desired state")?;
                if record.job.is_some() && record.desired != Desired::Running {
                    crate::supervise::signal_group(child.id(), libc::SIGKILL);
                    let _ = child.wait();
                    bail!("job VM provisioning was cancelled");
                }
            }
            if args.first() == Some(&"clone")
                && let Err(error) = self.disk_guard()
            {
                crate::supervise::signal_group(child.id(), libc::SIGKILL);
                let _ = child.wait();
                return Err(error.context("stopped image download at the free disk guard"));
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    fn verify(&self) -> Result<()> {
        let version = self.call(&["--version"], 15)?;
        if version.trim() != "2.40.1" {
            bail!(
                "VM backend requires qualified Tart 2.40.1, got {}",
                version.trim()
            );
        }
        // A bounded invocation, using the same output handling as Tart.
        let softnet = Tart {
            config: Config {
                tart: self.config.softnet.clone(),
                ..self.config.clone()
            },
            home: self.home.clone(),
            dir: self.dir.clone(),
            name: self.name.clone(),
        };
        let version = softnet.call(&["--version"], 15)?;
        if !version.trim().starts_with("softnet 0.24.0-") {
            bail!(
                "VM backend requires qualified Softnet 0.24.0, got {}",
                version.trim()
            );
        }
        Ok(())
    }

    fn state(&self) -> Result<Option<String>> {
        let output = self.call(&["list", "--source", "local", "--format", "json"], 15)?;
        let vms: Vec<Vm> = serde_json::from_str(&output).context("invalid Tart list response")?;
        Ok(vms
            .into_iter()
            .find(|v| v.name == self.name)
            .map(|v| v.state.to_lowercase()))
    }

    fn disk_guard(&self) -> Result<()> {
        use std::os::unix::ffi::OsStrExt;
        let path = std::ffi::CString::new(self.home.as_os_str().as_bytes())?;
        let mut stats: libc::statvfs = unsafe { std::mem::zeroed() };
        if unsafe { libc::statvfs(path.as_ptr(), &mut stats) } != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        let free = (stats.f_bavail as u128) * (stats.f_frsize as u128);
        if free < u128::from(self.config.min_free_disk_gb) * (1 << 30) {
            bail!("worker's VM free disk guard reached");
        }
        Ok(())
    }

    fn provision(&self, instance: &Info) -> Result<()> {
        self.disk_guard()?;
        observe(&self.home, instance, State::Creating, None)?;
        if self.state()?.is_none() {
            let source = self
                .config
                .images
                .get(&instance.record.spec.image)
                .context("image missing from saved catalog")?;
            self.call(&["clone", source, &self.name], 3600)?;
        }
        self.call(
            &[
                "set",
                &self.name,
                "--cpu",
                &instance.record.spec.cpus.to_string(),
                "--memory",
                &instance.record.spec.memory_mb.to_string(),
                "--disk-size",
                &instance.record.spec.disk_gb.to_string(),
            ],
            60,
        )?;
        self.disk_guard()?;
        write(&self.dir.join("provisioned.json"), &true)?;
        Ok(())
    }

    fn start(&self, instance: &Info) -> Result<Child> {
        self.verify()?;
        self.disk_guard()?;
        let subnets = network::local_subnets()?;
        let extra_blocks = Config::load(&self.home)?.block_cidrs;
        let blocks = network::block_rules(instance.record.spec.network, &subnets, &extra_blocks)?;
        write(&self.dir.join("network-subnets.json"), &subnets)?;
        write(&self.dir.join("network-block-cidrs.json"), &extra_blocks)?;
        let log = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .mode(0o600)
            .open(self.dir.join("vm.log"))?;
        let child = self
            .command()
            .args([
                "run",
                &self.name,
                "--no-graphics",
                "--no-clipboard",
                "--no-audio",
                "--no-usb-accessories",
                "--net-softnet",
                "--net-softnet-block",
                &blocks,
            ])
            .stdout(log.try_clone()?)
            .stderr(log)
            .spawn()?;
        Ok(child)
    }

    fn stop(&self) -> Result<()> {
        self.call(&["stop", &self.name, "--timeout", "30"], 45)?;
        if self.state()?.as_deref() == Some("running") {
            bail!("Tart still reports the VM running after stop");
        }
        Ok(())
    }

    fn shutdown(&self) -> Result<()> {
        // Tart's stop terminates the virtual hardware, not the guest OS. Give
        // macOS a chance to flush its filesystem over the private guest channel.
        // A guest may refuse, hang, or disconnect while shutting down, so its
        // response is never proof that the VM stopped or that capacity is free.
        if let Err(error) = self.call(
            &[
                "exec",
                &self.name,
                "/usr/bin/sudo",
                "-n",
                "/sbin/shutdown",
                "-h",
                "now",
            ],
            10,
        ) {
            tracing::warn!(%error, "guest shutdown request failed; waiting briefly before forced stop");
        }
        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
            if self.state()?.as_deref() == Some("stopped") {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        tracing::warn!(
            "guest did not shut down within the grace period; forcing VM off, unflushed guest data may be lost"
        );
        self.stop()
    }
}

fn read_small(path: &Path) -> Result<String> {
    let mut out = String::new();
    fs::File::open(path)?
        .take(1 << 20)
        .read_to_string(&mut out)?;
    Ok(out)
}

fn launch_error(path: &Path) -> String {
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

/// This process owns no coordinator credentials. It operates only its recorded VM.
pub fn run_controller(home: &Path, id: &str) -> Result<()> {
    let dir = directory(home, id)?;
    let Some(_lock) = try_lock(&dir.join("controller.lock"))? else {
        return Ok(());
    };
    let config: Config = crate::jobs::read_json(&dir.join("runtime.json"))?
        .context("missing instance runtime configuration")?;
    let tart = Tart {
        config,
        home: home.to_owned(),
        dir: dir.clone(),
        name: format!("fleet-{id}"),
    };
    let mut child: Option<Child> = None;
    let mut awake: Option<Child> = None;
    let mut failed_revision = None;
    loop {
        let exited = match child.as_mut() {
            Some(c) => c.try_wait()?,
            None => None,
        };
        if exited.is_some() {
            child = None;
        }
        let instance = info(home, id)?;
        // Tart's inventory reports its process lock before VZ finishes starting.
        // A late launch failure must not look like a successful VM followed by
        // an endless automatic restart loop (for example, external VM capacity).
        if let Some(status) = exited
            && !status.success()
            && instance.record.desired == Desired::Running
        {
            if let Some(mut c) = awake.take() {
                let _ = c.kill();
                let _ = c.wait();
            }
            observe(
                home,
                &instance,
                State::Failed,
                Some(format!(
                    "VM launcher exited ({status}); see vm.log: {}",
                    launch_error(&dir.join("vm.log"))
                )),
            )?;
            failed_revision = Some(instance.record.revision);
        }
        if failed_revision == Some(instance.record.revision) {
            std::thread::sleep(Duration::from_secs(1));
            continue;
        }
        let result: Result<bool> = (|| {
            let state = tart.state()?;
            if instance.record.desired == Desired::Deleted {
                if state.as_deref().is_some_and(|s| s != "stopped") {
                    bail!("refusing to delete a VM that is not confirmed stopped");
                }
                observe(home, &instance, State::Deleting, None)?;
                if state.is_some() {
                    tart.call(&["delete", &tart.name], 120)?;
                }
                observe(home, &instance, State::Deleted, None)?;
                return Ok(true);
            }
            if state.as_deref() == Some("running") {
                if instance.record.desired == Desired::Stopped {
                    observe(home, &instance, State::Stopping, None)?;
                    tart.shutdown()?;
                } else {
                    if let Err(error) = tart.disk_guard() {
                        tart.stop()?;
                        return Err(error.context("VM stopped at the free disk guard"));
                    }
                    if instance.record.spec.network == Network::PublicInternet {
                        let unchanged = (|| -> Result<bool> {
                            let saved: Vec<String> =
                                crate::jobs::read_json(&dir.join("network-subnets.json"))?
                                    .context("running public VM has no recorded network policy")?;
                            let extra: Vec<String> =
                                crate::jobs::read_json(&dir.join("network-block-cidrs.json"))?
                                    .context("running public VM has no recorded extra blocks")?;
                            Ok(network::local_subnets()? == saved
                                && Config::load(home)?.block_cidrs == extra)
                        })();
                        if !matches!(unchanged, Ok(true)) {
                            tart.stop()?;
                            bail!(
                                "host network changed or could not be inspected; VM stopped, stop/start to revalidate"
                            );
                        }
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
            } else if state.as_deref().is_some_and(|s| s != "stopped") {
                bail!("unexpected Tart VM state: {state:?}; refusing automatic mutation");
            }
            if let Some(mut c) = awake.take() {
                let _ = c.kill();
                let _ = c.wait();
            }
            if instance.record.desired == Desired::Stopped
                && instance.record.revision > 1
                && state.is_none()
            {
                observe(home, &instance, State::Stopped, None)?;
                return Ok(false);
            }
            if !dir.join("provisioned.json").exists() {
                tart.verify()?;
                tart.provision(&instance)?;
            } else if state.is_none() {
                bail!("VM disk disappeared; refusing to replace persistent data");
            }
            if instance.record.desired == Desired::Stopped {
                observe(home, &instance, State::Stopped, None)?;
            } else {
                if info(home, id)?.record.revision != instance.record.revision {
                    return Ok(false);
                }
                // Native submissions see desired=running and cannot race this start.
                let capacity: crate::proto::Hello =
                    match crate::jobs::read_json(&home.join("instance-capacity.json"))? {
                        Some(current) => current,
                        None => crate::jobs::read_json(&dir.join("capacity.json"))?
                            .context("missing capacity record")?,
                    };
                super::admit_vm_excluding(home, &instance.record.spec, &capacity, Some(id))
                    .context("current reservations prevent restarting the VM")?;
                observe(home, &instance, State::Starting, None)?;
                child = Some(tart.start(&instance)?);
                // Wait for Tart's VM lock or an early exit before another reconciliation.
                let deadline = Instant::now() + Duration::from_secs(60);
                loop {
                    if let Some(status) = child.as_mut().unwrap().try_wait()? {
                        child = None;
                        bail!(
                            "VM launcher exited ({status}); VMs outside Fleet also consume platform capacity. See vm.log: {}",
                            launch_error(&dir.join("vm.log"))
                        );
                    }
                    if tart.state()?.as_deref() == Some("running") {
                        break;
                    }
                    if Instant::now() >= deadline {
                        bail!("VM did not start within 60 seconds; stop to reconcile");
                    }
                    std::thread::sleep(Duration::from_millis(200));
                }
            }
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
