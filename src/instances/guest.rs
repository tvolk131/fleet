//! Durable guest operations. Only `fleet guest-exec` runs on the host; guest argv, paths and
//! environment are data passed to a fixed guest helper.
use super::{Desired, State, directory, info, try_lock, valid_id, write};
use crate::{blobs, jobs};
use anyhow::{Context, Result, bail};
use iroh_blobs::Hash;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs,
    io::{Read, Seek, SeekFrom, Write},
    os::{
        fd::AsRawFd,
        unix::{fs::OpenOptionsExt, process::CommandExt},
    },
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

pub const MAX_FILE: u64 = 1 << 30;
pub const MAX_LOG: u64 = 8 << 20;
pub const LOG_CHUNK: usize = 64 << 10;
/// An executable that release packages place beside Fleet: `fleet-guest`, `fleet-runner-vz`,
/// or `fleet` itself for processes that are not Fleet (tests).
pub fn companion(name: &str) -> Result<PathBuf> {
    let executable = std::env::current_exe()?.canonicalize()?;
    let parent = executable.parent().context("executable has no parent")?;
    let parent = if parent.file_name().is_some_and(|n| n == "deps") {
        parent
            .parent()
            .context("test executable has no target directory")?
    } else {
        parent
    };
    let path = parent.join(name);
    anyhow::ensure!(
        path.is_file(),
        "missing companion executable {}; install {name} beside Fleet",
        path.display()
    );
    Ok(path)
}
/// This small native executable runs in the instance's guest. No guest interpreter is needed.
/// Fake guests run on the host and use the host's build; a vz Linux guest needs the
/// aarch64-unknown-linux-musl build, installed as `fleet-guest-linux`.
pub fn helper_binary(home: &Path, id: &str) -> Result<PathBuf> {
    let config: super::Config = jobs::read_json(&directory(home, id)?.join("runtime.json"))?
        .context("missing instance runtime configuration")?;
    let spec = info(home, id)?.record.spec;
    let linux = config.guest(&spec.image)? == crate::driver::GuestOs::Linux;
    if linux && config.driver == super::DriverKind::Vz {
        companion("fleet-guest-linux").context(
            "Linux guests need fleet-guest built for aarch64-unknown-linux-musl beside Fleet",
        )
    } else {
        companion("fleet-guest")
    }
}
pub fn helper_guest_path(home: &Path, id: &str) -> Result<String> {
    Ok(format!(
        "/tmp/fleet-guest-{}",
        sha256(&helper_binary(home, id)?)?
    ))
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Spec {
    Exec {
        argv: Vec<String>,
        cwd: Option<String>,
        env: BTreeMap<String, String>,
        timeout_seconds: u64,
    },
    Upload {
        path: String,
        hash: Hash,
        size: u64,
        sha256: String,
        executable: bool,
    },
    Download {
        path: String,
        max_bytes: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        version: Option<String>,
    },
}

impl Spec {
    pub fn validate(&self) -> Result<()> {
        let path = match self {
            Self::Exec {
                argv,
                cwd,
                env,
                timeout_seconds,
            } => {
                if argv.is_empty()
                    || argv[0].is_empty()
                    || argv.len() > 256
                    || !(1..=86400).contains(timeout_seconds)
                    || argv.iter().any(|a| a.contains('\0'))
                    || env.keys().any(|k| k.is_empty() || k.contains(['=', '\0']))
                    || env.values().any(|v| v.contains('\0'))
                {
                    bail!("invalid guest command, environment, or timeout (1–86400 seconds)");
                }
                cwd.as_deref()
            }
            Self::Upload {
                path, size, sha256, ..
            } => {
                if *size > MAX_FILE
                    || sha256.len() != 64
                    || !sha256.bytes().all(|b| b.is_ascii_hexdigit())
                {
                    bail!("invalid upload size or SHA-256 (maximum 1 GiB)");
                }
                Some(path.as_str())
            }
            Self::Download {
                path,
                max_bytes,
                version,
            } => {
                if version
                    .as_ref()
                    .is_some_and(|v| v.is_empty() || v.len() > 256)
                {
                    bail!("invalid expected file version");
                }
                if !(1..=MAX_FILE).contains(max_bytes) {
                    bail!("download limit must be 1 byte–1 GiB");
                }
                Some(path.as_str())
            }
        };
        if path.is_some_and(|p| !p.starts_with('/') || p.contains('\0') || p.len() > 4096) {
            bail!("guest paths must be absolute, at most 4096 bytes, without NUL");
        }
        if serde_json::to_vec(self)?.len() > 64 << 10 {
            bail!("guest operation specification exceeds 64 KiB");
        }
        Ok(())
    }
    fn timeout(&self) -> u64 {
        match self {
            Self::Exec {
                timeout_seconds, ..
            } => timeout_seconds + 30,
            _ => 600,
        }
    }
    fn stdout_limit(&self) -> u64 {
        match self {
            Self::Download { max_bytes, .. } => *max_bytes,
            _ => MAX_LOG,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OperationState {
    Pending,
    Running,
    Succeeded,
    Failed,
    Interrupted,
}
impl OperationState {
    pub fn finished(&self) -> bool {
        !matches!(self, Self::Pending | Self::Running)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Operation {
    pub id: String,
    pub instance: String,
    pub revision: u64,
    pub spec: Spec,
    pub state: OperationState,
    pub created_at: u64,
    pub finished_at: Option<u64>,
    pub exit_code: Option<i32>,
    pub error: Option<String>,
}

/// Bounded inventory rows; full argv/environment are retrieved individually.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Summary {
    pub id: String,
    pub revision: u64,
    pub kind: String,
    pub state: OperationState,
    pub created_at: u64,
    pub finished_at: Option<u64>,
    pub exit_code: Option<i32>,
}
impl From<Operation> for Summary {
    fn from(op: Operation) -> Self {
        let kind = match op.spec {
            Spec::Exec { .. } => "exec",
            Spec::Upload { .. } => "upload",
            Spec::Download { .. } => "download",
        };
        Self {
            id: op.id,
            revision: op.revision,
            kind: kind.into(),
            state: op.state,
            created_at: op.created_at,
            finished_at: op.finished_at,
            exit_code: op.exit_code,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Readiness {
    pub ready: bool,
    pub checked_at: u64,
    pub revision: u64,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Log {
    pub data: Vec<u8>,
    pub next: u64,
    pub done: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Request {
    Ready {
        id: String,
    },
    Start {
        id: String,
        operation: String,
        revision: u64,
        spec: Spec,
    },
    Get {
        id: String,
        operation: String,
    },
    List {
        id: String,
    },
    Logs {
        id: String,
        operation: String,
        stderr: bool,
        from: u64,
    },
    Output {
        id: String,
        operation: String,
    },
    Storage,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Response {
    Ready(Readiness),
    Operation(Operation),
    Operations(Vec<Summary>),
    Logs(Log),
    Output { hash: Hash, size: u64 },
    Storage(super::storage::Report),
}

pub fn owned(home: &Path, id: &str, owner: &str) -> Result<super::Info> {
    let instance = info(home, id)?;
    if instance.record.job.is_some() {
        bail!("this VM is managed by a job; use the job logs, outputs and cancellation APIs");
    }
    if instance.record.owner != owner {
        bail!("instance belongs to a different coordinator");
    }
    Ok(instance)
}
fn running(home: &Path, id: &str, revision: u64) -> Result<()> {
    let i = info(home, id)?;
    if i.record.desired != Desired::Running
        || i.observed.state != State::Running
        || i.record.revision != revision
        || i.observed.revision != revision
    {
        bail!("instance is not running at the requested revision; inspect it again");
    }
    Ok(())
}
pub fn operation_dir(home: &Path, id: &str, operation: &str) -> Result<PathBuf> {
    valid_id(operation)?;
    Ok(directory(home, id)?.join("operations").join(operation))
}

pub fn inspect(home: &Path, id: &str, operation: &str) -> Result<Operation> {
    let dir = operation_dir(home, id, operation)?;
    let mut op: Operation =
        jobs::read_json(&dir.join("operation.json"))?.context("unknown guest operation")?;
    if !op.state.finished()
        && dir.join("attempted").exists()
        && let Some(_lock) = try_lock(&dir.join("operation.lock"))?
    {
        // The supervisor may have finished between the first read and our lock.
        op = jobs::read_json(&dir.join("operation.json"))?.context("missing operation")?;
        if op.state.finished() {
            return Ok(op);
        }
        // Never repeat a command whose guest-side effects are uncertain.
        op.state = OperationState::Interrupted;
        op.error = Some(
            "operation supervisor disappeared; guest effects are unknown; command was not replayed"
                .into(),
        );
        op.finished_at = Some(jobs::now());
        write(&dir.join("operation.json"), &op)?;
    }
    Ok(op)
}

pub fn list(home: &Path, id: &str) -> Result<Vec<Operation>> {
    let root = directory(home, id)?.join("operations");
    if !root.exists() {
        return Ok(vec![]);
    }
    let mut operations = Vec::new();
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() && entry.path().join("operation.json").exists() {
            operations.push(inspect(home, id, &entry.file_name().to_string_lossy())?);
        }
    }
    operations.sort_by(|a, b| (a.created_at, &a.id).cmp(&(b.created_at, &b.id)));
    Ok(operations)
}

/// Safe to resume only records which have never attempted a guest command.
pub fn recover(home: &Path, executable: &Path) -> Result<()> {
    for instance in super::list(home)? {
        if instance.record.job.is_some() {
            continue;
        }
        for op in list(home, &instance.record.id)? {
            if op.state == OperationState::Pending {
                launch(home, &instance.record.id, &op.id, executable)?;
            }
        }
    }
    Ok(())
}

pub fn admit(home: &Path, id: &str, revision: u64) -> Result<()> {
    running(home, id, revision)?;
    super::storage::guard(home)?;
    let all = list(home, id)?;
    if all.len() >= 1024 {
        bail!("instance operation history limit reached (1024); create a new instance");
    }
    if all.iter().filter(|o| !o.state.finished()).count() >= 4 {
        bail!("at most four active guest operations per instance");
    }
    Ok(())
}

/// Called under the agent's admission mutex. The staging file is supplied only
/// by the agent after exporting a verified content-addressed upload blob.
pub fn start(
    home: &Path,
    id: &str,
    operation: &str,
    revision: u64,
    spec: Spec,
    executable: &Path,
) -> Result<Operation> {
    spec.validate()?;
    let dir = operation_dir(home, id, operation)?;
    if dir.join("operation.json").exists() {
        let old = inspect(home, id, operation)?;
        if old.spec != spec || old.revision != revision {
            bail!("operation ID already has a different request");
        }
        if old.state == OperationState::Pending {
            launch(home, id, operation, executable)?;
        }
        return Ok(old);
    }
    admit(home, id, revision)?;
    let op = Operation {
        id: operation.into(),
        instance: id.into(),
        revision,
        spec,
        state: OperationState::Pending,
        created_at: jobs::now(),
        finished_at: None,
        exit_code: None,
        error: None,
    };
    write(&dir.join("operation.json"), &op)?;
    launch(home, id, operation, executable)?;
    Ok(op)
}

fn launch(home: &Path, id: &str, operation: &str, executable: &Path) -> Result<()> {
    let dir = operation_dir(home, id, operation)?;
    let log = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .open(dir.join("supervisor.log"))?;
    let mut child = Command::new(executable)
        .arg("instance-operation")
        .arg(home)
        .arg(id)
        .arg(operation)
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

fn channel(home: &Path, id: &str) -> Result<Command> {
    let mut cmd = super::guest_exec(home, id)?;
    cmd.stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0);
    Ok(cmd)
}

pub(crate) fn command(home: &Path, id: &str) -> Result<Command> {
    let path = helper_guest_path(home, id)?;
    let base = directory(home, id)?;
    let revision = info(home, id)?.record.revision;
    let expected = serde_json::json!({"path":path,"revision":revision});
    let deadline = Instant::now() + Duration::from_secs(30);
    let lock = loop {
        if let Some(lock) = try_lock(&base.join("guest-helper.lock"))? {
            break lock;
        }
        anyhow::ensure!(Instant::now() < deadline, "guest helper installation busy");
        std::thread::sleep(Duration::from_millis(50));
    };
    if jobs::read_json::<serde_json::Value>(&base.join("guest-helper.json"))?.as_ref()
        != Some(&expected)
    {
        let temp = base.join(format!(".helper-{:016x}", rand::random::<u64>()));
        fs::create_dir(&temp)?;
        let installed = (|| -> Result<()> {
            let mut cmd = channel(home, id)?;
            // Only fixed shell code executes. The destination is a hash generated by Fleet;
            // binary bytes arrive on stdin, never as shell text. No host directory is shared.
            cmd.args(["/bin/sh", "-c", "set -eu; umask 077; staging=$(mktemp /tmp/fleet-guest-install.XXXXXXXX); trap 'rm -f \"$staging\"' EXIT; cat >\"$staging\"; chmod 700 \"$staging\"; mv -f \"$staging\" \"$1\"; \"$1\" '{\"kind\":\"ready\"}'", "fleet-guest-install", &path]);
            cmd.stdin(fs::File::open(helper_binary(home, id)?)?);
            let code = execute(cmd, &temp, 4096, 30, None, || running(home, id, revision))?;
            anyhow::ensure!(
                code == 0
                    && fs::read(temp.join("stdout"))?
                        == format!("{}\n", fleet_guest::READY).as_bytes(),
                "guest helper installation failed (exit {code}); use a fleet-guest binary matching the guest platform: {}",
                String::from_utf8_lossy(&fs::read(temp.join("stderr"))?)
            );
            write(&base.join("guest-helper.json"), &expected)?;
            Ok(())
        })();
        let _ = fs::remove_dir_all(temp);
        installed?;
    }
    drop(lock);
    let mut cmd = channel(home, id)?;
    cmd.arg(path);
    Ok(cmd)
}

fn collect(
    mut source: impl Read + AsRawFd,
    path: &Path,
    limit: u64,
    exceeded: Arc<AtomicBool>,
    stopped: Arc<AtomicBool>,
) -> Result<()> {
    // A descendant holding a pipe open must not hang the host supervisor after
    // the guest channel exits. Drain buffered bytes, then stop without waiting for EOF.
    let fd = source.as_raw_fd();
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags < 0 || unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    let mut target = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    let mut buffer = [0u8; 65536];
    let mut written = 0;
    loop {
        let count = match source.read(&mut buffer) {
            Ok(n) => n,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                if stopped.load(Ordering::Relaxed) {
                    break;
                }
                std::thread::sleep(Duration::from_millis(20));
                continue;
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e.into()),
        };
        if count == 0 {
            break;
        }
        let remaining = limit.saturating_sub(written);
        let keep = count.min(remaining as usize);
        target.write_all(&buffer[..keep])?;
        written += keep as u64;
        if keep != count {
            exceeded.store(true, Ordering::Relaxed);
            break;
        }
    }
    target.sync_all()?;
    Ok(())
}

fn execute(
    mut cmd: Command,
    dir: &Path,
    limit: u64,
    seconds: u64,
    job_capture: Option<PathBuf>,
    check: impl Fn() -> Result<()>,
) -> Result<i32> {
    let mut child = cmd.spawn()?;
    let exceeded = Arc::new(AtomicBool::new(false));
    let stopped = Arc::new(AtomicBool::new(false));
    let stdout = child.stdout.take().context("missing guest stdout")?;
    let stderr = child.stderr.take().context("missing guest stderr")?;
    let out_path = dir.join("stdout");
    let err_path = dir.join("stderr");
    let flag = exceeded.clone();
    let stop = stopped.clone();
    let capture = job_capture.clone();
    let out = std::thread::spawn(move || match capture {
        Some(job) => crate::history::collect(stdout, &job, crate::history::Stream::Stdout, stop),
        None => collect(stdout, &out_path, limit, flag, stop),
    });
    let flag = exceeded.clone();
    let stop = stopped.clone();
    let capture = job_capture.clone();
    let err = std::thread::spawn(move || match capture {
        Some(job) => crate::history::collect(stderr, &job, crate::history::Stream::Stderr, stop),
        None => collect(stderr, &err_path, MAX_LOG, flag, stop),
    });
    let deadline = Instant::now() + Duration::from_secs(seconds);
    let result = loop {
        match child.try_wait() {
            Ok(Some(status)) => break Ok(status.code().unwrap_or(255)),
            Err(error) => break Err(error.into()),
            _ => {}
        }
        if exceeded.load(Ordering::Relaxed) {
            break Err(anyhow::anyhow!("guest output exceeded its byte limit"));
        }
        if Instant::now() >= deadline {
            break Err(anyhow::anyhow!(
                "guest channel timed out; command effects may be incomplete; stop the VM to enforce termination"
            ));
        }
        if let Err(error) = check() {
            break Err(error);
        }
        std::thread::sleep(Duration::from_millis(100));
    };
    if result.is_err() {
        crate::supervise::signal_group(child.id(), libc::SIGKILL);
    }
    let _ = child.wait();
    stopped.store(true, Ordering::Relaxed);
    for (stream, reader) in [("stdout", out), ("stderr", err)] {
        let captured = reader
            .join()
            .map_err(|_| anyhow::anyhow!("{stream} reader panicked"))
            .and_then(|r| r);
        if job_capture.is_some() {
            if let Err(error) = captured {
                tracing::warn!(%error, stream, "job log capture incomplete");
            }
        } else {
            captured?;
        }
    }
    if exceeded.load(Ordering::Relaxed) {
        bail!("guest output exceeded its byte limit");
    }
    result
}

pub fn supervise(home: &Path, id: &str, operation: &str) -> Result<()> {
    let dir = operation_dir(home, id, operation)?;
    let Some(_lock) = try_lock(&dir.join("operation.lock"))? else {
        return Ok(());
    };
    let mut op: Operation =
        jobs::read_json(&dir.join("operation.json"))?.context("missing operation")?;
    if op.state.finished() || dir.join("attempted").exists() {
        return Ok(());
    }
    // Mark before any guest side effect; a crash is ambiguous and never replayed.
    write(&dir.join("attempted"), &true)?;
    op.state = OperationState::Running;
    write(&dir.join("operation.json"), &op)?;
    let result = (|| -> Result<i32> {
        running(home, id, op.revision)?;
        let mut cmd = command(home, id)?;
        let request: fleet_guest::Request =
            serde_json::from_value(serde_json::to_value(&op.spec)?)?;
        cmd.arg(serde_json::to_string(&request)?);
        if let Spec::Upload {
            size,
            sha256: expected,
            ..
        } = &op.spec
        {
            let path = dir.join("input");
            if fs::metadata(&path)?.len() != *size || sha256(&path)? != *expected {
                bail!("upload staging verification failed");
            }
            cmd.stdin(fs::File::open(path)?);
        }
        let capture = if operation == "execute" {
            super::info(home, id)?
                .record
                .job
                .map(|job| jobs::jobs_dir(home).join(job))
        } else {
            None
        };
        execute(
            cmd,
            &dir,
            op.spec.stdout_limit(),
            op.spec.timeout(),
            capture,
            || {
                running(home, id, op.revision)?;
                super::storage::guard(home)
            },
        )
    })();
    match result {
        Ok(code) => {
            op.exit_code = Some(code);
            op.state = if code == 0 {
                OperationState::Succeeded
            } else {
                OperationState::Failed
            };
            if code != 0 {
                op.error = Some(format!(
                    "guest exited with code {code}; inspect operation stderr"
                ));
            }
        }
        Err(error) => {
            op.state = OperationState::Interrupted;
            op.error = Some(format!("{error:#}"));
        }
    }
    op.finished_at = Some(jobs::now());
    // Publish completion after releasing the temporary upload copy, so a
    // completed operation has no remaining staging cleanup in flight.
    let _ = fs::remove_file(dir.join("input"));
    write(&dir.join("operation.json"), &op)?;
    Ok(())
}

pub fn logs(home: &Path, id: &str, operation: &str, stderr: bool, from: u64) -> Result<Log> {
    let op = inspect(home, id, operation)?;
    if matches!(op.spec, Spec::Download { .. }) && !stderr {
        bail!("use download output for binary guest files");
    }
    let path = operation_dir(home, id, operation)?.join(if stderr { "stderr" } else { "stdout" });
    let mut data = Vec::new();
    let mut more = false;
    if path.exists() {
        let mut file = fs::File::open(path)?;
        if from > file.metadata()?.len() {
            bail!("log offset is beyond available output");
        }
        file.seek(SeekFrom::Start(from))?;
        file.take(LOG_CHUNK as u64).read_to_end(&mut data)?;
        more = from + (data.len() as u64)
            < fs::metadata(operation_dir(home, id, operation)?.join(if stderr {
                "stderr"
            } else {
                "stdout"
            }))?
            .len();
    } else if from != 0 {
        bail!("log offset is beyond available output");
    }
    Ok(Log {
        next: from + data.len() as u64,
        data,
        done: op.state.finished() && !more,
    })
}

pub fn readiness(home: &Path, id: &str) -> Result<Readiness> {
    let i = info(home, id)?;
    let mut report = Readiness {
        ready: false,
        checked_at: jobs::now(),
        revision: i.record.revision,
        message: String::new(),
    };
    if let Err(e) = running(home, id, i.record.revision) {
        report.message = e.to_string();
        return Ok(report);
    }
    let base = directory(home, id)?;
    let _lock = try_lock(&base.join("readiness.lock"))?
        .context("a readiness probe is already in progress")?;
    let temp = base.join(format!(".ready-{:016x}", rand::random::<u64>()));
    fs::create_dir(&temp)?;
    let result = (|| -> Result<()> {
        let mut cmd = command(home, id)?;
        cmd.arg(r#"{"kind":"ready"}"#);
        let code = execute(cmd, &temp, 4096, 5, None, || {
            running(home, id, i.record.revision)
        })?;
        if code != 0
            || fs::read(temp.join("stdout"))? != format!("{}\n", fleet_guest::READY).as_bytes()
        {
            bail!("guest command channel or Rust helper is not ready");
        }
        running(home, id, i.record.revision)?;
        Ok(())
    })();
    let _ = fs::remove_dir_all(temp);
    report.checked_at = jobs::now();
    match result {
        Ok(()) => {
            report.ready = true;
            report.message = "guest command channel and Rust helper ready".into();
        }
        Err(e) => {
            // A guest reboot can clear /tmp without changing desired state. A subsequent
            // read-only readiness probe may reinstall the helper; workload operations never replay.
            let _ = fs::remove_file(base.join("guest-helper.json"));
            report.message = format!("{e:#}");
        }
    }
    Ok(report)
}

pub fn sha256(path: &Path) -> Result<String> {
    let mut file = fs::File::open(path)?;
    let mut hash = Sha256::new();
    let mut buf = [0u8; 65536];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hash.update(&buf[..n]);
    }
    Ok(format!("{:x}", hash.finalize()))
}

pub fn output(home: &Path, id: &str, operation: &str) -> Result<PathBuf> {
    let op = inspect(home, id, operation)?;
    if op.state != OperationState::Succeeded || !matches!(op.spec, Spec::Download { .. }) {
        bail!("download has not succeeded");
    }
    let path = operation_dir(home, id, operation)?.join("stdout");
    if fs::metadata(&path)?.len() > op.spec.stdout_limit() {
        bail!("download output exceeds its limit");
    }
    Ok(path)
}

pub fn verify_input(path: &Path, hash: Hash, size: u64) -> Result<()> {
    if size > MAX_FILE || fs::metadata(path)?.len() != size || blobs::hash_file(path)? != hash {
        bail!("upload blob does not match its declared content/size");
    }
    Ok(())
}
