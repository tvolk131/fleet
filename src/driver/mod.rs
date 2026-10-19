//! VM drivers: turn a Fleet-owned instance spec into a running guest on this host. See
//! `docs/VM_DRIVERS.md`.
//!
//! Fleet decides what should exist; a driver only carries it out and reports what it observes.
//! Every VM lives in its own runner process (`fleet driver-runner`), started detached so worker
//! restarts never touch guests. [`RunnerDriver`] supervises runners for any [`Backend`]; the
//! backend only says what it can run, how to lay out an instance and how to launch its runner.

pub mod conformance;
pub mod fake;
pub mod protocol;
pub mod runner;

use std::{
    fmt, fs,
    os::{
        fd::{AsRawFd, OwnedFd},
        unix::{
            fs::{DirBuilderExt, MetadataExt, OpenOptionsExt},
            net::UnixStream,
            process::CommandExt,
        },
    },
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};

use protocol::{Request, Response};

/// A validated instance identifier; also the instance's directory name.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct InstanceId(String);

impl InstanceId {
    pub fn new(id: impl Into<String>) -> Result<Self> {
        let id = id.into();
        let valid = !id.is_empty()
            && id.len() <= 64
            && id
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
            && !id.starts_with('-');
        if valid {
            Ok(Self(id))
        } else {
            Err(Error::Invalid(format!(
                "instance id {id:?} must be 1-64 of [a-z0-9-], not starting with '-'"
            )))
        }
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl TryFrom<String> for InstanceId {
    type Error = Error;
    fn try_from(id: String) -> Result<Self> {
        Self::new(id)
    }
}
impl From<InstanceId> for String {
    fn from(id: InstanceId) -> Self {
        id.0
    }
}
impl fmt::Display for InstanceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GuestOs {
    Linux,
    Macos,
}

/// Everything a driver needs to run one instance. Fleet bumps `generation` on every change of
/// intent; a call carrying an older generation than the driver has recorded is rejected.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstanceSpec {
    pub id: InstanceId,
    pub generation: u64,
    pub guest: GuestOs,
    /// A prepared image bundle directory, resolved by the caller from the trusted catalog.
    pub image: PathBuf,
    pub cpus: u32,
    pub memory_mb: u32,
    pub disk_gb: u32,
    pub mac: [u8; 6],
    /// The host network agent drops larger frames: hypervisors may truncate rather than drop.
    pub mtu: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    /// Nothing on this host.
    Absent,
    /// Prepared, or cleanly stopped.
    Stopped,
    /// A runner holds the instance but is not serving yet.
    Starting,
    Running,
    Stopping,
    /// A runner was started and is gone without a clean exit: the guest's memory is lost.
    Lost,
    /// The last runner reported a failure; `Observed::error` says why.
    Failed,
}

/// Why the last runner ended, written by the runner (or by the driver for a forced stop).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "reason", rename_all = "snake_case")]
pub enum Exit {
    /// The guest powered itself off, possibly after a graceful stop request.
    PoweredOff,
    /// The driver killed the runner.
    Forced,
    /// The guest's state was saved and the VM stopped.
    Saved {
        path: PathBuf,
    },
    Failed {
        error: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Observed {
    pub id: InstanceId,
    /// The newest generation this driver has recorded, 0 if none.
    pub generation: u64,
    pub phase: Phase,
    pub runner_pid: Option<u32>,
    pub last_exit: Option<Exit>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Capabilities {
    pub driver: String,
    pub guests: Vec<GuestOs>,
    /// The hypervisor's limit on concurrent macOS guests, 0 if it cannot run them.
    pub max_macos_guests: usize,
    pub save_restore: bool,
    pub configurable_mtu: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopMode {
    /// Ask the guest, then force once `deadline` passes. The driver chooses how to ask.
    Graceful {
        deadline: Duration,
    },
    Force,
}

#[derive(Debug)]
pub enum Error {
    /// The call carried an older generation than the driver has recorded.
    Stale {
        recorded: u64,
        requested: u64,
    },
    NotFound(InstanceId),
    Unsupported(&'static str),
    /// The request is malformed or conflicts with the instance's current phase.
    Invalid(String),
    Other(anyhow::Error),
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stale {
                recorded,
                requested,
            } => {
                write!(f, "stale generation {requested}; driver has {recorded}")
            }
            Self::NotFound(id) => write!(f, "instance {id} is not on this host"),
            Self::Unsupported(what) => write!(f, "{what} is not supported by this driver"),
            Self::Invalid(message) => f.write_str(message),
            Self::Other(e) => write!(f, "{e:#}"),
        }
    }
}
impl std::error::Error for Error {}
impl From<anyhow::Error> for Error {
    fn from(e: anyhow::Error) -> Self {
        Self::Other(e)
    }
}
impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::Other(e.into())
    }
}
impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Self::Other(e.into())
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// The contract every VM backend meets; `conformance` is its executable specification.
///
/// All calls are idempotent and safe to make concurrently from several threads or processes.
pub trait Driver: Send + Sync {
    fn capabilities(&self) -> Capabilities;

    /// Every instance on this host, including ones Fleet no longer knows about: the caller, not
    /// the driver, decides what to delete.
    fn inventory(&self) -> Result<Vec<Observed>>;
    fn observe(&self, id: &InstanceId) -> Result<Observed>;

    /// Records the spec and lays out the instance. Repeating a generation is a no-op.
    fn prepare(&self, spec: &InstanceSpec) -> Result<()>;
    /// Prepares if needed and starts a runner unless one is already serving this generation.
    fn start(&self, spec: &InstanceSpec) -> Result<()>;
    fn stop(&self, id: &InstanceId, generation: u64, mode: StopMode) -> Result<()>;
    /// Force-stops and removes everything, including the recorded generation.
    fn delete(&self, id: &InstanceId, generation: u64) -> Result<()>;

    /// A duplicate of the host end of the guest's only network card: one datagram per Ethernet
    /// frame. The runner keeps its own copy, so dropping this never disconnects the guest.
    fn network_card(&self, id: &InstanceId) -> Result<OwnedFd>;
    /// A byte stream to a listener inside the guest.
    fn connect_guest(&self, id: &InstanceId, port: u32) -> Result<UnixStream>;

    /// Saves the guest's state to `to` and stops it. Requires `save_restore`.
    fn save_state(&self, id: &InstanceId, generation: u64, to: &Path) -> Result<()>;
    /// Starts the instance from a state saved by `save_state`. Requires `save_restore`.
    fn restore_state(&self, spec: &InstanceSpec, from: &Path) -> Result<()>;
}

/// What differs between hypervisors on the supervising side.
pub trait Backend: Send + Sync {
    fn capabilities(&self) -> Capabilities;
    /// Lays out `dir` for `spec` (disk clones, firmware variables). Must be idempotent.
    fn prepare(&self, spec: &InstanceSpec, dir: &Path) -> Result<()>;
    /// The command that runs `fleet driver-runner` (or equivalent) for an instance directory.
    fn runner(&self, dir: &Path) -> Command;
}

const SPEC: &str = "spec.json";
const START: &str = "start.json";
const EXIT: &str = "exit.json";
const RUNNER_LOCK: &str = "runner.lock";
const OP_LOCK: &str = "op.lock";
const READY_TIMEOUT: Duration = Duration::from_secs(60);

/// Written by the driver before it launches a runner. A free runner lock with this file and no
/// exit record means the runner died without a clean exit.
#[derive(Debug, Serialize, Deserialize)]
struct Start {
    generation: u64,
    pid: u32,
}

/// Supervises one runner process per instance under `root/<id>/`.
pub struct RunnerDriver<B> {
    root: PathBuf,
    backend: B,
}

impl<B: Backend> RunnerDriver<B> {
    pub fn new(root: impl Into<PathBuf>, backend: B) -> Result<Self> {
        let root = root.into();
        fs::create_dir_all(&root)?;
        // Runner sockets are named after the absolute instance directory.
        let root = root.canonicalize()?;
        Ok(Self { root, backend })
    }

    fn dir(&self, id: &InstanceId) -> PathBuf {
        self.root.join(id.as_str())
    }

    fn recorded(&self, id: &InstanceId) -> Result<Option<InstanceSpec>> {
        match fs::read(self.dir(id).join(SPEC)) {
            Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Rejects a generation older than the recorded one.
    fn fence(&self, id: &InstanceId, generation: u64) -> Result<Option<InstanceSpec>> {
        let recorded = self.recorded(id)?;
        if let Some(spec) = &recorded
            && generation < spec.generation
        {
            return Err(Error::Stale {
                recorded: spec.generation,
                requested: generation,
            });
        }
        Ok(recorded)
    }

    fn observe_unlocked(&self, id: &InstanceId) -> Result<Observed> {
        let dir = self.dir(id);
        let Some(spec) = self.recorded(id)? else {
            return Ok(Observed {
                id: id.clone(),
                generation: 0,
                phase: Phase::Absent,
                runner_pid: None,
                last_exit: None,
                error: None,
            });
        };
        let start: Option<Start> = read_json(&dir.join(START))?;
        let last_exit: Option<Exit> = read_json(&dir.join(EXIT))?;
        let mut observed = Observed {
            id: id.clone(),
            generation: spec.generation,
            phase: Phase::Stopped,
            runner_pid: None,
            last_exit: last_exit.clone(),
            error: None,
        };
        if runner_alive(&dir)? {
            observed.runner_pid = start.as_ref().map(|s| s.pid);
            observed.phase = match protocol::call(&socket_path(&dir)?, &Request::Status) {
                Ok((Response::Status { phase, pid, .. }, _)) => {
                    observed.runner_pid = Some(pid);
                    phase
                }
                // Holding the lock but not serving yet.
                _ => Phase::Starting,
            };
            return Ok(observed);
        }
        observed.phase = match (&start, &last_exit) {
            (_, Some(Exit::Failed { error })) => {
                observed.error = Some(error.clone());
                Phase::Failed
            }
            (_, Some(_)) | (None, None) => Phase::Stopped,
            (Some(_), None) => Phase::Lost,
        };
        Ok(observed)
    }

    fn record(&self, spec: &InstanceSpec) -> Result<()> {
        let dir = self.dir(&spec.id);
        fs::create_dir_all(&dir)?;
        self.backend.prepare(spec, &dir)?;
        write_json(&dir.join(SPEC), spec)
    }

    fn prepare_locked(&self, spec: &InstanceSpec) -> Result<()> {
        match self.fence(&spec.id, spec.generation)? {
            Some(recorded) if recorded == *spec => Ok(()),
            Some(recorded) if recorded.generation == spec.generation => {
                Err(Error::Invalid(format!(
                    "generation {} was already recorded with a different spec",
                    spec.generation
                )))
            }
            _ => self.record(spec),
        }
    }

    fn launch(&self, spec: &InstanceSpec, restore: Option<&Path>) -> Result<()> {
        let dir = self.dir(&spec.id);
        // Take the runner lock here and hand the same open file to the runner, so it is held
        // continuously from before launch until the runner is gone.
        let lock = open_lock(&dir.join(RUNNER_LOCK))?;
        if !try_flock(&lock)? {
            return Err(Error::Invalid(format!(
                "a runner already holds {}",
                spec.id
            )));
        }
        let _ = fs::remove_file(dir.join(EXIT));
        let _ = fs::remove_file(socket_path(&dir)?);
        let log = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .mode(0o600)
            .open(dir.join("runner.log"))?;
        let mut command = self.backend.runner(&dir);
        command.arg("--lock-fd").arg(runner::LOCK_FD.to_string());
        if let Some(path) = restore {
            command.arg("--restore").arg(path);
        }
        let lock_fd = lock.as_raw_fd();
        unsafe {
            command.pre_exec(move || {
                // dup2 onto itself is a no-op that would leave close-on-exec set.
                let ok = if lock_fd == runner::LOCK_FD {
                    libc::fcntl(lock_fd, libc::F_SETFD, 0) == 0
                } else {
                    libc::dup2(lock_fd, runner::LOCK_FD) >= 0
                };
                if !ok {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let mut child = command
            .stdin(Stdio::null())
            .stdout(log.try_clone()?)
            .stderr(log)
            .process_group(0)
            .spawn()?;
        drop(lock);
        let pid = child.id();
        write_json(
            &dir.join(START),
            &Start {
                generation: spec.generation,
                pid,
            },
        )?;
        // Reap the runner if it exits while this process lives; otherwise launchd/init does.
        std::thread::spawn(move || {
            let _ = child.wait();
        });

        let deadline = Instant::now() + READY_TIMEOUT;
        loop {
            match self.observe_unlocked(&spec.id)?.phase {
                Phase::Running => return Ok(()),
                Phase::Starting if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(20))
                }
                Phase::Starting => {
                    kill_runner(&dir, pid)?;
                    write_json(
                        &dir.join(EXIT),
                        &Exit::Failed {
                            error: "runner did not become ready".into(),
                        },
                    )?;
                    return Err(Error::Other(anyhow::anyhow!(
                        "runner for {} did not become ready within {READY_TIMEOUT:?}",
                        spec.id
                    )));
                }
                phase => {
                    let observed = self.observe_unlocked(&spec.id)?;
                    return Err(Error::Other(anyhow::anyhow!(
                        "runner for {} ended during start ({phase:?}): {}",
                        spec.id,
                        observed.error.unwrap_or_else(|| "see runner.log".into())
                    )));
                }
            }
        }
    }

    fn force_stop(&self, id: &InstanceId) -> Result<()> {
        let dir = self.dir(id);
        if !runner_alive(&dir)? {
            return Ok(());
        }
        let start: Option<Start> = read_json(&dir.join(START))?;
        let pid = start
            .map(|s| s.pid)
            .ok_or_else(|| Error::Invalid(format!("runner for {id} has no recorded pid")))?;
        kill_runner(&dir, pid)?;
        // A killed runner cannot remove its own socket.
        let _ = fs::remove_file(socket_path(&dir)?);
        write_json(&dir.join(EXIT), &Exit::Forced)
    }

    fn running_call(
        &self,
        id: &InstanceId,
        request: &Request,
    ) -> Result<(Response, Option<OwnedFd>)> {
        let observed = self.observe(id)?;
        match observed.phase {
            Phase::Running => {}
            Phase::Absent => return Err(Error::NotFound(id.clone())),
            phase => return Err(Error::Invalid(format!("instance {id} is {phase:?}"))),
        }
        match protocol::call(&socket_path(&self.dir(id))?, request)? {
            (Response::Error { message }, _) => Err(Error::Other(anyhow::anyhow!(message))),
            reply => Ok(reply),
        }
    }

    fn with_lock<T>(&self, id: &InstanceId, f: impl FnOnce() -> Result<T>) -> Result<T> {
        let dir = self.dir(id);
        fs::create_dir_all(&dir)?;
        let lock = open_lock(&dir.join(OP_LOCK))?;
        if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX) } != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        f()
    }
}

impl<B: Backend> Driver for RunnerDriver<B> {
    fn capabilities(&self) -> Capabilities {
        self.backend.capabilities()
    }

    fn inventory(&self) -> Result<Vec<Observed>> {
        let mut all = Vec::new();
        for entry in fs::read_dir(&self.root)? {
            let name = entry?.file_name();
            let Ok(id) = InstanceId::new(name.to_string_lossy().into_owned()) else {
                continue;
            };
            let observed = self.observe(&id)?;
            if observed.phase != Phase::Absent {
                all.push(observed);
            }
        }
        all.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(all)
    }

    fn observe(&self, id: &InstanceId) -> Result<Observed> {
        if !self.dir(id).exists() {
            return self.observe_unlocked(id);
        }
        self.with_lock(id, || self.observe_unlocked(id))
    }

    fn prepare(&self, spec: &InstanceSpec) -> Result<()> {
        self.check(spec)?;
        self.with_lock(&spec.id, || self.prepare_locked(spec))
    }

    fn start(&self, spec: &InstanceSpec) -> Result<()> {
        self.check(spec)?;
        self.with_lock(&spec.id, || {
            self.prepare_locked(spec)?;
            match self.observe_unlocked(&spec.id)?.phase {
                Phase::Running | Phase::Starting => Ok(()),
                Phase::Stopping => Err(Error::Invalid(format!("instance {} is stopping", spec.id))),
                _ => self.launch(spec, None),
            }
        })
    }

    fn stop(&self, id: &InstanceId, generation: u64, mode: StopMode) -> Result<()> {
        self.with_lock(id, || {
            if self.fence(id, generation)?.is_none() {
                return Ok(());
            }
            let dir = self.dir(id);
            if let StopMode::Graceful { deadline } = mode
                && runner_alive(&dir)?
            {
                let deadline = Instant::now() + deadline;
                let _ = protocol::call(&socket_path(&dir)?, &Request::Stop);
                while Instant::now() < deadline {
                    if !runner_alive(&dir)? {
                        return Ok(());
                    }
                    std::thread::sleep(Duration::from_millis(20));
                }
            }
            self.force_stop(id)
        })
    }

    fn delete(&self, id: &InstanceId, generation: u64) -> Result<()> {
        let dir = self.dir(id);
        if !dir.exists() {
            return Ok(());
        }
        self.with_lock(id, || {
            self.fence(id, generation)?;
            self.force_stop(id)?;
            // The spec goes first: a crash part-way leaves an Absent instance, not a half one.
            let _ = fs::remove_file(dir.join(SPEC));
            let _ = fs::remove_file(socket_path(&dir)?);
            Ok(())
        })?;
        fs::remove_dir_all(&dir)?;
        Ok(())
    }

    fn network_card(&self, id: &InstanceId) -> Result<OwnedFd> {
        match self.running_call(id, &Request::NetworkCard)? {
            (Response::Ok, Some(fd)) => Ok(fd),
            reply => Err(Error::Other(anyhow::anyhow!("unexpected reply {reply:?}"))),
        }
    }

    fn connect_guest(&self, id: &InstanceId, port: u32) -> Result<UnixStream> {
        match self.running_call(id, &Request::Guest { port })? {
            (Response::Ok, Some(fd)) => Ok(UnixStream::from(fd)),
            reply => Err(Error::Other(anyhow::anyhow!("unexpected reply {reply:?}"))),
        }
    }

    fn save_state(&self, id: &InstanceId, generation: u64, to: &Path) -> Result<()> {
        if !self.capabilities().save_restore {
            return Err(Error::Unsupported("save/restore"));
        }
        let to = absolute(to)?;
        self.with_lock(id, || {
            self.fence(id, generation)?;
            let dir = self.dir(id);
            match protocol::call(&socket_path(&dir)?, &Request::Save { path: to.clone() })? {
                (Response::Ok, _) => {}
                (Response::Error { message }, _) => {
                    return Err(Error::Other(anyhow::anyhow!(message)));
                }
                reply => return Err(Error::Other(anyhow::anyhow!("unexpected reply {reply:?}"))),
            }
            // The runner exits after a save; wait for it so the caller observes Stopped.
            let deadline = Instant::now() + READY_TIMEOUT;
            while runner_alive(&dir)? {
                if Instant::now() > deadline {
                    return Err(Error::Other(anyhow::anyhow!(
                        "runner did not exit after saving"
                    )));
                }
                std::thread::sleep(Duration::from_millis(20));
            }
            Ok(())
        })
    }

    fn restore_state(&self, spec: &InstanceSpec, from: &Path) -> Result<()> {
        if !self.capabilities().save_restore {
            return Err(Error::Unsupported("save/restore"));
        }
        self.check(spec)?;
        let from = absolute(from)?;
        self.with_lock(&spec.id, || {
            self.prepare_locked(spec)?;
            match self.observe_unlocked(&spec.id)?.phase {
                Phase::Running | Phase::Starting | Phase::Stopping => Err(Error::Invalid(format!(
                    "instance {} must be stopped to restore",
                    spec.id
                ))),
                _ => self.launch(spec, Some(&from)),
            }
        })
    }
}

impl<B: Backend> RunnerDriver<B> {
    fn check(&self, spec: &InstanceSpec) -> Result<()> {
        let capabilities = self.backend.capabilities();
        if !capabilities.guests.contains(&spec.guest) {
            return Err(Error::Unsupported(match spec.guest {
                GuestOs::Linux => "Linux guests",
                GuestOs::Macos => "macOS guests",
            }));
        }
        if spec.cpus == 0 || spec.memory_mb == 0 || spec.mtu < 576 {
            return Err(Error::Invalid(
                "cpus, memory and an MTU of at least 576 are required".into(),
            ));
        }
        if spec.mtu != 1500 && !capabilities.configurable_mtu {
            return Err(Error::Unsupported("an MTU other than 1500"));
        }
        Ok(())
    }
}

/// Where an instance's runner listens. Unix socket paths are limited to 104 bytes on macOS, so
/// sockets live in a short per-user directory, named by a hash of the instance directory.
pub fn socket_path(instance_dir: &Path) -> Result<PathBuf> {
    let base = match std::env::var_os("FLEET_RUNTIME_DIR") {
        Some(dir) => PathBuf::from(dir),
        None => PathBuf::from(format!("/tmp/fleet-{}", unsafe { libc::getuid() })),
    };
    match fs::DirBuilder::new().mode(0o700).create(&base) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(e.into()),
    }
    // Someone else's directory, or a symlink, could intercept every runner's control socket.
    let meta = fs::symlink_metadata(&base)?;
    if !meta.is_dir() || meta.uid() != unsafe { libc::getuid() } || meta.mode() & 0o077 != 0 {
        return Err(Error::Invalid(format!(
            "{} must be a directory owned by this user with mode 0700",
            base.display()
        )));
    }
    let hash = blake3::hash(instance_dir.as_os_str().as_encoded_bytes()).to_hex();
    Ok(base.join(format!("{}.sock", &hash[..24])))
}

fn absolute(path: &Path) -> Result<PathBuf> {
    Ok(std::path::absolute(path)?)
}

fn open_lock(path: &Path) -> Result<fs::File> {
    Ok(fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .open(path)?)
}

fn try_flock(file: &fs::File) -> Result<bool> {
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
        return Ok(true);
    }
    let error = std::io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::EWOULDBLOCK) {
        Ok(false)
    } else {
        Err(error.into())
    }
}

/// True while some runner holds the instance's runner lock.
fn runner_alive(dir: &Path) -> Result<bool> {
    let path = dir.join(RUNNER_LOCK);
    if !path.exists() {
        return Ok(false);
    }
    let lock = open_lock(&path)?;
    let free = try_flock(&lock)?;
    // Dropping the file releases our probe lock if we took it.
    Ok(!free)
}

/// Kills the runner's process group and waits for its lock to be released.
fn kill_runner(dir: &Path, pid: u32) -> Result<()> {
    // Only signal while the lock proves a runner is alive, so a reused pid is never hit.
    if runner_alive(dir)? {
        unsafe { libc::kill(-(pid as libc::pid_t), libc::SIGKILL) };
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    while runner_alive(dir)? {
        if Instant::now() > deadline {
            return Err(Error::Other(anyhow::anyhow!(
                "runner {pid} survived SIGKILL"
            )));
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    Ok(())
}

fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> Result<Option<T>> {
    match fs::read(path) {
        Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Atomic replace, so a reader never sees a partial record.
pub(crate) fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let tmp = path.with_extension("tmp");
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(&tmp)?;
    serde_json::to_writer(&mut file, value)?;
    file.sync_all()?;
    fs::rename(&tmp, path)?;
    Ok(())
}
