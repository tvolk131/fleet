//! One disposable VM per queued job. All guest calls have durable, stable IDs;
//! recovery resumes orchestration, never an uncertain execution attempt.
use crate::{
    instances::{self, Desired, State, guest},
    jobs::{self, EndReason, Exit, Job, JobSpec},
};
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeSet,
    fs,
    io::Write,
    os::unix::{
        fs::{OpenOptionsExt, PermissionsExt},
        process::CommandExt,
    },
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::Duration,
};

mod checkpoints;
mod observe;
pub use checkpoints::{captured as captured_checkpoints, files_dir as checkpoint_files_dir};
pub use checkpoints::{reclaimed as checkpoint_reclaimed, usage as checkpoint_usage};
pub use observe::{Progress, output_revision};

pub const MAX_INPUTS: usize = 128;
pub const MAX_OUTPUTS: usize = 256;
pub const MAX_TOTAL: u64 = 4 << 30;
const HELPER: &str = include_str!("vm_job.py");

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    Preparing,
    Executing,
    Collecting,
    Stopping,
    Deleting,
    Finished,
}

impl Phase {
    pub fn label(self) -> &'static str {
        match self {
            Self::Preparing => "Preparing VM",
            Self::Executing => "Running in VM",
            Self::Collecting => "Collecting final results",
            Self::Stopping => "Stopping VM",
            Self::Deleting => "Deleting VM disk",
            Self::Finished => "VM cleanup complete",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Status {
    pub instance: String,
    pub phase: Phase,
    pub error: Option<String>,
    pub outcome: Option<Exit>,
}

pub fn instance_id(job: &str) -> String {
    format!("job-{}", &blake3::hash(job.as_bytes()).to_hex()[..32])
}

pub fn validate(spec: &JobSpec, vm: &instances::Spec) -> Result<()> {
    if vm.cpus == 0
        || vm.memory_mb < 4096
        || vm.disk_gb < 50
        || vm.image.is_empty()
        || spec.threads > vm.cpus
    {
        bail!(
            "VM job requires an image, positive vCPUs, at least 4096 MiB RAM/50 GiB disk, and threads <= vCPUs"
        );
    }
    if spec.inputs.len() > MAX_INPUTS || spec.publish.len() > MAX_OUTPUTS {
        bail!(
            "VM job supports at most {MAX_INPUTS} input files and {MAX_OUTPUTS} publish patterns"
        );
    }
    if spec.resume.is_some() || spec.preempt.is_some() || spec.pause_keeps_gpu {
        bail!("VM jobs do not support pause, checkpoint suspension or preemption");
    }
    observe::validate(&spec.probes)?;
    if spec.shutdown_grace_seconds.is_some() {
        bail!(
            "VM jobs cancel by stopping the entire VM; native shutdown_grace_seconds is not supported"
        );
    }
    let exec = guest::Spec::Exec {
        argv: spec.argv.clone(),
        cwd: None,
        env: spec.env.clone(),
        timeout_seconds: spec.max_seconds.unwrap_or(86400),
    };
    exec.validate()?;
    // Inputs are exported by the trusted host, so reject ancestor conflicts up front.
    let paths: BTreeSet<_> = spec.inputs.keys().cloned().collect();
    check_paths(&paths)?;
    Ok(())
}

fn check_paths(paths: &BTreeSet<String>) -> Result<()> {
    for path in paths {
        let rel = jobs::safe_relative(path)?;
        if path.len() > 2048
            || rel.to_string_lossy() != *path
            || path.contains('\0')
            || path
                .split('/')
                .any(|p| p.is_empty() || p == "." || p == "..")
        {
            bail!("non-canonical or oversized VM file path");
        }
        for parent in rel.ancestors().skip(1) {
            if paths.contains(parent.to_string_lossy().as_ref()) {
                bail!("file paths overlap a parent directory");
            }
        }
    }
    Ok(())
}

/// Native jobs also use this launcher; VM supervisors take an OS lock before doing work.
pub fn launch(dir: &Path, executable: &Path) -> Result<()> {
    let log = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .open(dir.join("supervisor.log"))?;
    let mut child = Command::new(executable)
        .arg("supervise")
        .arg(dir)
        .stdin(Stdio::null())
        .stdout(log.try_clone()?)
        .stderr(log)
        .process_group(0)
        .spawn()?;
    std::thread::spawn(move || {
        let _ = child.wait();
    });
    Ok(())
}

pub fn ensure(dir: &Path, executable: &Path) -> Result<()> {
    if dir.join("exit.json").exists() {
        return Ok(());
    }
    if let Some(lock) = instances::try_lock(&dir.join("vm-job.lock"))? {
        drop(lock);
        launch(dir, executable)?;
    }
    Ok(())
}

/// The agent holds submission admission here: no current input export can be
/// using these directories. Only unpublished VM-job staging is reclaimed.
pub fn clean_staging(home: &Path) -> Result<()> {
    let root = home.join("staging");
    if !root.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        if let Some(job) = jobs::read_json::<Job>(&entry.path().join("job.json"))?
            && job.spec.vm.is_some()
        {
            fs::remove_dir_all(entry.path())?;
        }
    }
    Ok(())
}

/// Serialized with submission. A cancelled pending job is published durably so
/// a late Submit cannot start it, even if Cancel arrived before its inputs.
pub fn cancel(home: &Path, id: &str, spec: JobSpec, owner: &str, executable: &Path) -> Result<()> {
    spec.validate()?;
    if spec.vm.is_none() {
        bail!("CancelVmJob requires a VM specification");
    }
    let rel = jobs::safe_relative(id)?;
    if rel.components().count() != 1 {
        bail!("invalid job ID");
    }
    let dir = jobs::jobs_dir(home).join(rel);
    if let Some(job) = jobs::read_json::<Job>(&dir.join("job.json"))? {
        if job.submitter != owner || job.spec != spec {
            bail!("VM job cancellation ownership/specification conflict");
        }
        if dir.join("exit.json").exists() {
            return Ok(());
        }
        instances::write(&dir.join("cancel"), &jobs::now())?;
        instances::release_job(home, id, Desired::Stopped)?;
        return ensure(&dir, executable);
    }
    let instance = instance_id(id);
    let has_vm = instances::directory(home, &instance)?
        .join("desired.json")
        .exists();
    if has_vm {
        let i = instances::info(home, &instance)?;
        if i.record.owner != owner
            || i.record.job.as_deref() != Some(id)
            || Some(&i.record.spec) != spec.vm.as_ref()
        {
            bail!("VM reservation cancellation conflict");
        }
        instances::release_job(home, id, Desired::Stopped)?;
    }
    let staging = home
        .join("staging")
        .join(format!("cancel-{:016x}", rand::random::<u64>()));
    let result = (|| -> Result<()> {
        fs::create_dir_all(staging.join("work"))?;
        instances::write(
            &staging.join("job.json"),
            &Job {
                id: id.into(),
                spec,
                submitter: owner.into(),
                submitted_at: jobs::now(),
            },
        )?;
        instances::write(&staging.join("cancel"), &jobs::now())?;
        if !has_vm {
            let exit = Exit {
                code: None,
                signal: None,
                reason: EndReason::Cancelled,
                finished_at: jobs::now(),
            };
            instances::write(
                &staging.join("vm-job.json"),
                &Status {
                    instance,
                    phase: Phase::Finished,
                    error: None,
                    outcome: Some(exit.clone()),
                },
            )?;
            instances::write(&staging.join("exit.json"), &exit)?;
        }
        fs::create_dir_all(jobs::jobs_dir(home))?;
        fs::rename(&staging, &dir)?;
        fs::File::open(jobs::jobs_dir(home))?.sync_all()?;
        if has_vm {
            ensure(&dir, executable)?;
        }
        Ok(())
    })();
    if staging.exists() {
        let _ = fs::remove_dir_all(staging);
    }
    result
}

struct Runner {
    home: PathBuf,
    dir: PathBuf,
    job: Job,
    status: Status,
    executable: PathBuf,
}
impl Runner {
    fn save(&self) -> Result<()> {
        instances::write(&self.dir.join("vm-job.json"), &self.status)
    }
    fn phase(&mut self, phase: Phase) -> Result<()> {
        self.status.phase = phase;
        self.save()
    }
    fn check(&self) -> Result<()> {
        if self.dir.join("suspend.json").exists() {
            bail!("VM job suspended at its verified checkpoint");
        }
        if self.dir.join("cancel").exists() {
            bail!("VM job cancelled");
        }
        if self.status.phase == Phase::Preparing
            && jobs::now().saturating_sub(self.job.submitted_at) > 3600
        {
            bail!("VM preparation exceeded one hour");
        }
        if self.status.phase == Phase::Executing
            && let Some(deadline) = jobs::read_json::<u64>(&self.dir.join("vm-deadline.json"))?
            && jobs::now() >= deadline.saturating_add(5)
            && !guest::inspect(&self.home, &self.status.instance, "execute")?
                .state
                .finished()
        {
            bail!("VM job runtime limit reached");
        }
        instances::storage::guard(&self.home)
    }
    fn guest_root(&self) -> String {
        format!("/tmp/fleet-{}/work", self.status.instance)
    }
    fn run_operation(
        &mut self,
        id: &str,
        spec: guest::Spec,
        input: Option<&Path>,
    ) -> Result<guest::Operation> {
        self.check()?;
        let vm = instances::info(&self.home, &self.status.instance)?;
        let dir = guest::operation_dir(&self.home, &self.status.instance, id)?;
        if !dir.join("operation.json").exists()
            && let Some(input) = input
        {
            fs::create_dir_all(&dir)?;
            fs::set_permissions(&dir, fs::Permissions::from_mode(0o700))?;
            if !dir.join("input").exists() {
                fs::copy(input, dir.join("input"))?;
            }
        }
        guest::start(
            &self.home,
            &self.status.instance,
            id,
            vm.record.revision,
            spec,
            &self.executable,
        )?;
        let mut last_observation = std::time::Instant::now() - Duration::from_secs(5);
        loop {
            self.check()?;
            let op = guest::inspect(&self.home, &self.status.instance, id)?;
            if id == "execute" {
                self.link_logs(&dir)?;
            }
            if op.state.finished() {
                if matches!(op.state, guest::OperationState::Interrupted) {
                    bail!(
                        "guest operation {id} interrupted; no replay: {}",
                        op.error.unwrap_or_default()
                    );
                }
                return Ok(op);
            }
            if id == "execute" && last_observation.elapsed() >= Duration::from_secs(5) {
                self.observe(false)?;
                self.capture_checkpoints()?;
                last_observation = std::time::Instant::now();
            }
            let i = instances::info(&self.home, &self.status.instance)?;
            if matches!(i.observed.state, State::Failed | State::Unknown) {
                bail!("VM failed during {id}: {:?}", i.observed.error);
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }
    fn require(&mut self, id: &str, spec: guest::Spec, input: Option<&Path>) -> Result<()> {
        let op = self.run_operation(id, spec, input)?;
        if op.state != guest::OperationState::Succeeded {
            let log = guest::logs(&self.home, &self.status.instance, id, true, 0)?;
            let detail = String::from_utf8_lossy(&log.data[..log.data.len().min(4096)]);
            bail!("guest operation {id} failed: {:?}: {detail}", op.error);
        }
        Ok(())
    }
    fn helper(&self, value: serde_json::Value) -> guest::Spec {
        guest::Spec::Exec {
            argv: vec![
                "/usr/bin/python3".into(),
                "-I".into(),
                "-c".into(),
                HELPER.into(),
                value.to_string(),
            ],
            cwd: None,
            env: Default::default(),
            timeout_seconds: 120,
        }
    }
    fn link_logs(&self, operation: &Path) -> Result<()> {
        for (from, to) in [("stdout", "stdout.log"), ("stderr", "stderr.log")] {
            if operation.join(from).exists() && !self.dir.join(to).exists() {
                fs::hard_link(operation.join(from), self.dir.join(to))?;
            }
        }
        Ok(())
    }
    fn execute(&mut self) -> Result<Exit> {
        // Completed operations are revisited under their original IDs after recovery.
        // The phase is deliberately not reset: preparation's wall-clock bound must
        // not abort recovery of a long-running, already-started command.
        let ready_deadline = std::time::Instant::now() + Duration::from_secs(3600);
        loop {
            self.check()?;
            if std::time::Instant::now() >= ready_deadline {
                bail!("VM readiness deadline exceeded");
            }
            let i = instances::info(&self.home, &self.status.instance)?;
            if i.record.desired != Desired::Running
                || matches!(i.observed.state, State::Failed | State::Unknown)
            {
                bail!("job VM unavailable: {:?}", i.observed.error);
            }
            instances::ensure_controller(&self.home, &self.status.instance, &self.executable)?;
            if i.observed.state == State::Running
                && guest::readiness(&self.home, &self.status.instance)?.ready
            {
                break;
            }
            std::thread::sleep(Duration::from_secs(1));
        }
        let root = self.guest_root();
        self.require(
            "prepare",
            self.helper(serde_json::json!({"kind":"prepare", "root":root, "platform":self.job.spec.vm.as_ref().and_then(|v| v.runtime.as_ref()).map(|r| &r.platform)})),
            None,
        )?;
        let mut total = 0u64;
        for (n, (path, input)) in self.job.spec.inputs.clone().iter().enumerate() {
            self.check()?;
            let local = self.dir.join("vm-inputs").join(jobs::safe_relative(path)?);
            let size = fs::metadata(&local)?.len();
            total = total.checked_add(size).context("input size overflow")?;
            if size > guest::MAX_FILE || total > MAX_TOTAL {
                bail!("VM input limit: 1 GiB/file, 4 GiB total");
            }
            guest::verify_input(&local, input.hash, size)?;
            self.require(
                &format!("input-{n}"),
                guest::Spec::Upload {
                    path: format!("{root}/{path}"),
                    hash: input.hash,
                    size,
                    sha256: guest::sha256(&local)?,
                    executable: input.executable,
                },
                Some(&local),
            )?;
        }
        self.phase(Phase::Executing)?;
        let deadline_path = self.dir.join("vm-deadline.json");
        if !deadline_path.exists() {
            instances::write(
                &deadline_path,
                &jobs::now().saturating_add(self.job.spec.max_seconds.unwrap_or(86400)),
            )?;
        }
        let mut argv = self.job.spec.argv.clone();
        if argv[0].contains('/') && !argv[0].starts_with('/') {
            argv[0] = format!("{root}/{}", argv[0]);
        }
        let mut env = self.job.spec.env.clone();
        for key in ["RAYON_NUM_THREADS", "OMP_NUM_THREADS", "FLEET_THREADS"] {
            env.entry(key.into())
                .or_insert_with(|| self.job.spec.threads.to_string());
        }
        let op = self.run_operation(
            "execute",
            guest::Spec::Exec {
                argv,
                cwd: Some(root.clone()),
                env,
                timeout_seconds: self.job.spec.max_seconds.unwrap_or(86400),
            },
            None,
        )?;
        let code = op.exit_code.context("guest command has no exit status")?;
        if code == 124
            && jobs::read_json::<u64>(&self.dir.join("vm-deadline.json"))?
                .is_some_and(|d| jobs::now() >= d)
        {
            return Ok(Exit {
                code: Some(code),
                signal: None,
                reason: EndReason::TimeLimit,
                finished_at: jobs::now(),
            });
        }
        self.phase(Phase::Collecting)?;
        self.observe(true)?;
        self.capture_checkpoints()?;
        Ok(Exit {
            code: Some(code),
            signal: None,
            reason: EndReason::Exited,
            finished_at: jobs::now(),
        })
    }
    fn cleanup(&mut self) -> Result<()> {
        loop {
            let i = instances::info(&self.home, &self.status.instance)?;
            if i.observed.state == State::Deleted && i.record.desired == Desired::Deleted {
                break;
            }
            if i.record.desired != Desired::Deleted {
                let desired = if i.record.desired == Desired::Stopped
                    && i.observed.state == State::Stopped
                    && i.observed.revision == i.record.revision
                {
                    Desired::Deleted
                } else {
                    Desired::Stopped
                };
                instances::release_job(&self.home, &self.job.id, desired)?;
                self.phase(if desired == Desired::Deleted {
                    Phase::Deleting
                } else {
                    Phase::Stopping
                })?;
            }
            if i.record.desired == Desired::Deleted && i.observed.state == State::Failed {
                instances::release_job(&self.home, &self.job.id, Desired::Deleted)?;
            }
            instances::ensure_controller(&self.home, &self.status.instance, &self.executable)?;
            if i.observed.state == State::Failed && i.observed.revision == i.record.revision {
                bail!("VM cleanup needs retry: {:?}", i.observed.error);
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        // Keep the operation records for audit; release redundant payloads only
        // after confirmed deletion and after their detached supervisors exit.
        for op in guest::list(&self.home, &self.status.instance)? {
            let dir = guest::operation_dir(&self.home, &self.status.instance, &op.id)?;
            let _lock = instances::try_lock(&dir.join("operation.lock"))?
                .context("guest operation still finishing")?;
            for name in ["stdout", "stderr", "input"] {
                if dir.join(name).exists() {
                    fs::remove_file(dir.join(name))?;
                }
            }
        }
        // A crash between hard-linking a capture and publishing its atomic
        // replacement may leave staging behind. Cancellation skips recovery of
        // that batch, so reclaim it only after the VM and operations stopped.
        let capture = self.dir.join("vm-capture.tmp");
        if capture.exists() {
            fs::remove_file(capture)?;
        }
        let inputs = self.dir.join("vm-inputs");
        if inputs.exists() {
            fs::remove_dir_all(inputs)?;
        }
        let checkpoints = self.dir.join("checkpoints");
        if checkpoints.exists() {
            for entry in fs::read_dir(checkpoints)? {
                let entry = entry?;
                if entry.file_type()?.is_dir()
                    && entry.file_name().to_string_lossy().starts_with("pending-")
                {
                    fs::remove_dir_all(entry.path())?;
                }
            }
        }
        if let Some(error) = &self.status.error {
            let diagnostic = format!("\nfleet: {error}\n");
            let path = self.dir.join("stderr.log");
            let previous = fs::read(&path).unwrap_or_default();
            if !previous.ends_with(diagnostic.as_bytes()) {
                let mut file = fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .mode(0o600)
                    .open(path)?;
                file.write_all(diagnostic.as_bytes())?;
                file.sync_all()?;
            }
        }
        self.phase(Phase::Finished)?;
        let mut exit = self
            .status
            .outcome
            .clone()
            .context("missing VM job outcome")?;
        exit.finished_at = jobs::now();
        instances::write(&self.dir.join("exit.json"), &exit)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Capture {
    pub path: String,
    pub size: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
}
pub fn validate_manifest(files: &[Capture], patterns: &[String]) -> Result<()> {
    if files.len() > MAX_OUTPUTS {
        bail!("guest output manifest exceeds {MAX_OUTPUTS} files");
    }
    let mut paths = BTreeSet::new();
    let mut total = 0u64;
    for file in files {
        if !paths.insert(file.path.clone())
            || !patterns.iter().any(|p| jobs::matches(p, &file.path))
        {
            bail!("duplicate or undeclared guest output path");
        }
        total = total
            .checked_add(file.size)
            .context("output size overflow")?;
        if file.size > guest::MAX_FILE || total > MAX_TOTAL {
            bail!("VM output limit: 1 GiB/file, 4 GiB total");
        }
    }
    check_paths(&paths)
}

pub fn run(dir: &Path) -> Result<()> {
    let Some(_lock) = instances::try_lock(&dir.join("vm-job.lock"))? else {
        return Ok(());
    };
    if dir.join("exit.json").exists() {
        return Ok(());
    }
    let job: Job = jobs::read_json(&dir.join("job.json"))?.context("missing job")?;
    job.spec.validate()?;
    let home = dir
        .parent()
        .and_then(Path::parent)
        .context("invalid job folder")?
        .to_owned();
    let status = jobs::read_json(&dir.join("vm-job.json"))?.unwrap_or(Status {
        instance: instance_id(&job.id),
        phase: Phase::Preparing,
        error: None,
        outcome: None,
    });
    let mut r = Runner {
        home,
        dir: dir.into(),
        job,
        status,
        executable: std::env::current_exe()?,
    };
    r.save()?;
    let started = jobs::read_json::<jobs::Started>(&dir.join("supervisor.json"))?;
    instances::write(
        &dir.join("supervisor.json"),
        &jobs::Started {
            supervisor_pid: std::process::id(),
            child_pid: std::process::id(),
            started_at: started.map_or_else(jobs::now, |s| s.started_at),
        },
    )?;
    if r.status.outcome.is_none() {
        let result = r.execute();
        let cancelled = dir.join("cancel").exists();
        let suspended = dir.join("suspend.json").exists();
        let timed_out = r.status.phase == Phase::Executing
            && jobs::read_json::<u64>(&dir.join("vm-deadline.json"))?
                .is_some_and(|d| jobs::now() >= d);
        r.status.outcome = Some(match result {
            Ok(mut exit) => {
                if cancelled {
                    exit.reason = EndReason::Cancelled;
                } else if suspended {
                    exit.reason = EndReason::Suspended;
                }
                exit
            }
            Err(error) => {
                r.status.error = Some(format!("{error:#}"));
                Exit {
                    code: None,
                    signal: None,
                    reason: if cancelled {
                        EndReason::Cancelled
                    } else if suspended {
                        EndReason::Suspended
                    } else if timed_out {
                        EndReason::TimeLimit
                    } else {
                        EndReason::StartFailed
                    },
                    finished_at: jobs::now(),
                }
            }
        });
        r.phase(Phase::Stopping)?;
        instances::write(
            &dir.join("stopping.json"),
            &jobs::Stopping {
                reason: r.status.outcome.as_ref().unwrap().reason,
                started_at: jobs::now(),
                deadline: jobs::now().saturating_add(90),
            },
        )?;
    }
    if let Err(error) = r.cleanup() {
        r.status.error = Some(format!(
            "cleanup: {error:#}; job outcome: {:?}",
            r.status.outcome
        ));
        r.save()?;
        return Err(error);
    }
    Ok(())
}
