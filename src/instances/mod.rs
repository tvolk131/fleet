//! Persistent VM instances, independent of job lifetimes.

pub mod guest;
pub mod network;
pub mod runtime;
pub mod storage;
mod tart;

use crate::{jobs, proto::Hello};
use anyhow::{Context, Result, bail};
pub use network::Network;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs,
    io::Write,
    os::fd::AsRawFd,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    os::unix::process::CommandExt,
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

/// Apple's supported macOS guest concurrency ceiling on Apple Silicon.
/// VMs launched outside this Fleet home also consume the platform's capacity;
/// the hypervisor remains authoritative when those launches race Fleet.
pub const MACOS_VM_LIMIT: usize = 2;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Spec {
    /// Alias from the worker's trusted image catalog.
    pub image: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime: Option<runtime::Runtime>,
    pub cpus: u32,
    pub memory_mb: u32,
    pub disk_gb: u32,
    pub network: Network,
    /// Scheduling policy, not a hardware partition. Every macOS guest can use Metal.
    #[serde(default, skip_serializing_if = "Gpu::is_exclusive")]
    pub gpu: Gpu,
}

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Gpu {
    #[default]
    Exclusive,
    Shared,
}

impl Gpu {
    fn is_exclusive(&self) -> bool {
        *self == Self::Exclusive
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Desired {
    Stopped,
    Running,
    Deleted,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum State {
    Creating,
    Stopped,
    Starting,
    Running,
    Stopping,
    Deleting,
    Deleted,
    Failed,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Record {
    /// Set only for disposable job VMs. Their lifecycle belongs to the job supervisor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job: Option<String>,
    pub id: String,
    pub spec: Spec,
    pub owner: String,
    pub created_at: u64,
    pub desired: Desired,
    pub revision: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Observed {
    pub state: State,
    pub revision: u64,
    pub updated_at: u64,
    pub error: Option<String>,
}

impl Default for Observed {
    fn default() -> Self {
        Self {
            state: State::Creating,
            revision: 0,
            updated_at: jobs::now(),
            error: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Info {
    pub record: Record,
    pub observed: Observed,
}

impl Info {
    pub fn reserves_compute(&self) -> bool {
        self.record.desired == Desired::Running
            // A stale 'stopped' observation cannot release a later start's claim.
            // Initial provisioning has never launched the VM and needs only disk.
            || (self.observed.revision != self.record.revision
                && !(self.record.revision == 1 && self.observed.state == State::Creating))
            || !matches!(
                self.observed.state,
                State::Creating | State::Stopped | State::Deleted
            )
    }
}

/// Local administrator configuration, never supplied by a coordinator or guest.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub tart: PathBuf,
    pub softnet: PathBuf,
    /// Only immutable OCI references (name@sha256:...) are accepted.
    pub images: BTreeMap<String, String>,
    /// Explicit guest platform/capabilities for each approved image. Absent entries cannot run portable jobs.
    #[serde(default)]
    pub platforms: BTreeMap<String, runtime::Platform>,
    pub memory_budget_mb: u32,
    pub disk_budget_gb: u32,
    pub min_free_disk_gb: u32,
    #[serde(default)]
    pub block_cidrs: Vec<String>,
}

impl Config {
    pub fn runtimes(&self) -> Result<BTreeMap<String, runtime::Runtime>> {
        self.platforms
            .iter()
            .map(|(alias, platform)| {
                platform.validate()?;
                let image = self
                    .images
                    .get(alias)
                    .context("platform references unknown image")?;
                let (_, digest) = image.rsplit_once('@').context("image must be pinned")?;
                let runtime = runtime::Runtime {
                    platform: platform.clone(),
                    image_digest: digest.into(),
                };
                runtime.validate()?;
                Ok((alias.clone(), runtime))
            })
            .collect()
    }
    pub fn check_runtime(&self, spec: &Spec) -> Result<()> {
        if let Some(required) = &spec.runtime {
            let runtimes = self.runtimes()?;
            required.accepts(
                runtimes
                    .get(&spec.image)
                    .context("image has no explicit guest platform")?,
            )?;
        }
        Ok(())
    }

    pub fn load(home: &Path) -> Result<Self> {
        let config: Self = jobs::read_json(&home.join("instances.json"))?
            .context("VM backend disabled: configure the worker's instances.json first")?;
        if !config.tart.is_absolute()
            || !config.softnet.is_absolute()
            || config.softnet.file_name().is_none_or(|n| n != "softnet")
        {
            bail!("Tart and Softnet paths must be absolute; Softnet's filename must be softnet");
        }
        if config.memory_budget_mb == 0 || config.disk_budget_gb == 0 || config.min_free_disk_gb < 5
        {
            bail!("VM budgets must be positive and the free disk guard at least 5 GiB");
        }
        for source in config.images.values() {
            let (name, hash) = source
                .rsplit_once("@sha256:")
                .context("images must be pinned OCI digests")?;
            if name.starts_with('-')
                || !name.contains('/')
                || name.contains(char::is_whitespace)
                || hash.len() != 64
                || !hash.bytes().all(|b| b.is_ascii_hexdigit())
            {
                bail!("invalid pinned OCI image reference");
            }
        }
        config.runtimes()?;
        for subnet in &config.block_cidrs {
            network::cidr(subnet)?;
        }
        Ok(config)
    }
}

pub fn valid_id(id: &str) -> Result<()> {
    if id.is_empty()
        || id.len() > 64
        || !id.as_bytes()[0].is_ascii_alphanumeric()
        || !id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        bail!(
            "instance ID must be 1–64 ASCII letters, digits, hyphens or underscores, starting with a letter or digit"
        );
    }
    Ok(())
}

pub fn directory(home: &Path, id: &str) -> Result<PathBuf> {
    valid_id(id)?;
    Ok(home.join("instances").join(id))
}

pub fn info(home: &Path, id: &str) -> Result<Info> {
    let dir = directory(home, id)?;
    let record = jobs::read_json(&dir.join("desired.json"))?.context("unknown instance")?;
    let observed = jobs::read_json(&dir.join("observed.json"))?.unwrap_or_default();
    Ok(Info { record, observed })
}

pub fn list(home: &Path) -> Result<Vec<Info>> {
    let mut out = Vec::new();
    let root = home.join("instances");
    if !root.exists() {
        return Ok(out);
    }
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let id = entry
            .file_name()
            .into_string()
            .map_err(|_| anyhow::anyhow!("non-UTF8 instance ID"))?;
        if entry.path().join("desired.json").exists() {
            out.push(info(home, &id)?);
        }
    }
    out.sort_by(|a, b| a.record.id.cmp(&b.record.id));
    Ok(out)
}

/// Rename plus fsync: a command is acknowledged only after its desired state is durable.
pub fn write<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let parent = path.parent().context("missing parent")?;
    fs::create_dir_all(parent)?;
    fs::set_permissions(parent, fs::Permissions::from_mode(0o700))?;
    let tmp = parent.join(format!(".write-{:016x}", rand::random::<u64>()));
    let result = (|| {
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&tmp)?;
        file.write_all(&serde_json::to_vec(value)?)?;
        file.sync_all()?;
        fs::rename(&tmp, path)?;
        fs::File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(tmp);
    }
    result
}

pub fn create(home: &Path, id: &str, spec: Spec, owner: &str, hello: &Hello) -> Result<Info> {
    create_inner(home, id, spec, owner, hello, None)
}

fn create_inner(
    home: &Path,
    id: &str,
    spec: Spec,
    owner: &str,
    hello: &Hello,
    job: Option<String>,
) -> Result<Info> {
    let dir = directory(home, id)?;
    if dir.join("desired.json").exists() {
        let existing = info(home, id)?;
        if existing.record.owner == owner && existing.record.spec == spec {
            return Ok(existing);
        }
        bail!("instance {id} already exists with a different specification or owner");
    }
    let config = Config::load(home)?;
    config.check_runtime(&spec)?;
    if !config.images.contains_key(&spec.image) {
        bail!("image is not in this worker's trusted catalog");
    }
    if spec.cpus == 0
        || spec.cpus > hello.cpu_slots
        || spec.memory_mb < 4096
        || spec.memory_mb > config.memory_budget_mb
        || spec.disk_gb < 50
    {
        bail!(
            "invalid VM resources: CPU must fit the worker, memory at least 4096 MiB within its budget, disk at least 50 GiB"
        );
    }
    let used: u64 = list(home)?
        .iter()
        .filter(|i| i.observed.state != State::Deleted)
        .map(|i| u64::from(i.record.spec.disk_gb))
        .sum();
    if used + u64::from(spec.disk_gb) > u64::from(config.disk_budget_gb) {
        bail!("VM disk budget exhausted");
    }
    write(&dir.join("runtime.json"), &config)?;
    write(&dir.join("capacity.json"), hello)?;
    write(
        &dir.join("desired.json"),
        &Record {
            desired: if job.is_some() {
                Desired::Running
            } else {
                Desired::Stopped
            },
            job,
            id: id.into(),
            spec,
            owner: owner.into(),
            created_at: jobs::now(),
            revision: 1,
        },
    )?;
    info(home, id)
}

/// Caller holds the same admission mutex as native submission and continuation.
pub fn set_desired(
    home: &Path,
    id: &str,
    desired: Desired,
    revision: u64,
    owner: &str,
    hello: &Hello,
) -> Result<Info> {
    let mut instance = info(home, id)?;
    if instance.record.job.is_some() {
        bail!("this VM is managed by a job; use fleet cancel for its lifecycle");
    }
    if instance.record.owner != owner {
        bail!("instance belongs to a different coordinator");
    }
    if instance.record.desired == Desired::Deleted {
        if desired == Desired::Deleted {
            return Ok(instance);
        }
        bail!("instance was deleted; choose a new ID");
    }
    if instance.record.revision != revision {
        if revision.checked_add(1) == Some(instance.record.revision)
            && instance.record.desired == desired
        {
            return Ok(instance);
        }
        bail!("instance changed since this request; inspect it before issuing another command");
    }
    if instance.record.desired == desired
        && !matches!(instance.observed.state, State::Failed | State::Unknown)
    {
        return Ok(instance);
    }
    if desired == Desired::Deleted
        && (instance.record.desired != Desired::Stopped
            || instance.observed.state != State::Stopped)
    {
        bail!("stop the instance and wait for stopped before deleting its disk");
    }
    if desired == Desired::Running {
        if !matches!(instance.observed.state, State::Stopped) {
            bail!("instance is not stopped");
        }
        admit_vm_excluding(home, &instance.record.spec, hello, Some(id))?;
    }
    instance.record.desired = desired;
    instance.record.revision += 1;
    write(&directory(home, id)?.join("desired.json"), &instance.record)?;
    info(home, id)
}

pub fn admit_vm(home: &Path, spec: &Spec, hello: &Hello) -> Result<()> {
    admit_vm_excluding(home, spec, hello, None)
}

/// Count durable claims, including pending starts and unconfirmed stops. The
/// controller uses the same check on restart, excluding its own existing claim.
fn admit_vm_excluding(
    home: &Path,
    spec: &Spec,
    hello: &Hello,
    exclude: Option<&str>,
) -> Result<()> {
    let config = Config::load(home)?;
    let claims = list(home)?;
    let active: Vec<_> = claims
        .iter()
        .filter(|i| exclude != Some(i.record.id.as_str()) && i.reserves_compute())
        .collect();
    if !active.is_empty()
        && (spec.gpu == Gpu::Exclusive
            || active.iter().any(|i| i.record.spec.gpu == Gpu::Exclusive))
    {
        bail!(
            "exclusive GPU use conflicts with another VM reservation; all coexisting VMs must use shared GPU mode"
        );
    }
    let (threads, gpu) = job_usage(home, None)?;
    if gpu {
        bail!("GPU is reserved by a native job");
    }
    let vm_threads = active.iter().fold(u64::from(spec.cpus), |used, i| {
        used.saturating_add(u64::from(i.record.spec.cpus))
    });
    if vm_threads.saturating_add(u64::from(threads)) > u64::from(hello.cpu_slots) {
        bail!("insufficient free CPU slots for VM");
    }
    let memory_mb = active.iter().fold(u64::from(spec.memory_mb), |used, i| {
        used.saturating_add(u64::from(i.record.spec.memory_mb))
    });
    if memory_mb > u64::from(config.memory_budget_mb) || memory_mb > hello.memory_bytes / (1 << 20)
    {
        bail!("insufficient VM memory budget");
    }
    if active.len() >= MACOS_VM_LIMIT {
        bail!(
            "macOS VM limit reached: at most {MACOS_VM_LIMIT} active reservations per worker; wait for a VM to be confirmed stopped"
        );
    }
    if let Some(process) = external_gpu(home) {
        bail!("GPU is busy outside Fleet: {process}");
    }
    Ok(())
}

/// Old job records did not carry placement. Treat those as GPU jobs conservatively.
fn job_usage(home: &Path, exclude: Option<&str>) -> Result<(u32, bool)> {
    let mut threads = 0u32;
    let mut gpu = false;
    for job in jobs::list(home)? {
        if exclude == Some(job.id.as_str()) || job.state.finished() {
            continue;
        }
        let dir = jobs::dir(home, &job.id)?;
        let record: jobs::Job =
            jobs::read_json(&dir.join("job.json"))?.context("missing job metadata")?;
        if record.spec.vm.is_some() {
            // Its disposable instance owns the reservation, including during cleanup.
            continue;
        }
        let claim = jobs::read_json::<bool>(&dir.join("gpu-allocation.json"))?.unwrap_or(true);
        if job.state != jobs::State::Paused {
            threads = threads.saturating_add(record.spec.threads);
        }
        if claim && (job.state != jobs::State::Paused || record.spec.pause_keeps_gpu) {
            gpu = true;
        }
    }
    Ok((threads, gpu))
}

pub fn admit_job(
    home: &Path,
    threads: u32,
    gpu: Option<bool>,
    exclude: Option<&str>,
    hello: &Hello,
) -> Result<()> {
    let active: Vec<_> = list(home)?
        .into_iter()
        .filter(Info::reserves_compute)
        .collect();
    if active.is_empty() {
        return Ok(());
    }
    if gpu.unwrap_or(true) {
        bail!("GPU is reserved by a VM instance");
    }
    let vm_threads = active.iter().fold(0u64, |used, i| {
        used.saturating_add(u64::from(i.record.spec.cpus))
    });
    let (used, _) = job_usage(home, exclude)?;
    if vm_threads.saturating_add(u64::from(used) + u64::from(threads)) > u64::from(hello.cpu_slots)
    {
        bail!("CPU slots are reserved by VM instances and native jobs");
    }
    Ok(())
}

fn external_gpu(home: &Path) -> Option<String> {
    let patterns: Vec<_> = fs::read_to_string(home.join("gpu-processes"))
        .unwrap_or_default()
        .lines()
        .map(|s| s.split('#').next().unwrap_or("").trim().to_owned())
        .filter(|s| !s.is_empty())
        .collect();
    let own_groups = jobs::list(home)
        .unwrap_or_default()
        .iter()
        .filter(|j| !j.state.finished())
        .filter_map(|j| {
            jobs::read_json::<jobs::Started>(
                &jobs::jobs_dir(home).join(&j.id).join("supervisor.json"),
            )
            .ok()
            .flatten()
        })
        .flat_map(|s| [s.supervisor_pid, s.child_pid])
        .collect();
    crate::system::running_process(&patterns, &own_groups, &jobs::jobs_dir(home))
}

pub struct Lock {
    _file: fs::File,
}
pub fn try_lock(path: &Path) -> Result<Option<Lock>> {
    let f = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .open(path)?;
    if unsafe { libc::flock(f.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
        return Ok(Some(Lock { _file: f }));
    }
    let error = std::io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::EWOULDBLOCK) {
        Ok(None)
    } else {
        Err(error.into())
    }
}
pub fn ensure_controller(home: &Path, id: &str, executable: &Path) -> Result<()> {
    let instance = info(home, id)?;
    if instance.observed.state == State::Deleted && instance.record.desired == Desired::Deleted {
        return Ok(());
    }
    let dir = directory(home, id)?;
    let Some(lock) = try_lock(&dir.join("controller.lock"))? else {
        return Ok(());
    };
    drop(lock);
    let log = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .open(dir.join("controller.log"))?;
    let mut child = Command::new(executable)
        .arg("instance-controller")
        .arg(home)
        .arg(id)
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

pub fn run_controller(home: &Path, id: &str) -> Result<()> {
    tart::run_controller(home, id)
}

/// Bounded worker capabilities for placement; the worker rechecks admission.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Capacity {
    pub images: Vec<String>,
    pub memory_mb: u32,
    pub disk_gb: u32,
}
impl From<Config> for Capacity {
    fn from(c: Config) -> Self {
        Self {
            images: c.images.into_keys().collect(),
            memory_mb: c.memory_budget_mb,
            disk_gb: c.disk_budget_gb,
        }
    }
}

/// Called under the agent's submission mutex, before publishing the job folder.
pub fn reserve_job(home: &Path, job: &str, spec: Spec, owner: &str, hello: &Hello) -> Result<Info> {
    let id = crate::vm_job::instance_id(job);
    let dir = directory(home, &id)?;
    if dir.join("desired.json").exists() {
        let mut i = info(home, &id)?;
        // Only an unpublished reservation can be reclaimed. Its controller was
        // stopped before provisioning, and no guest operation could have run.
        if i.record.job.as_deref() != Some(job)
            || i.record.owner != owner
            || i.record.spec != spec
            || i.record.desired != Desired::Deleted
            || i.observed.state != State::Deleted
            || dir.join("provisioned.json").exists()
            || dir.join("operations").exists()
            || jobs::jobs_dir(home).join(job).exists()
        {
            bail!(
                "job VM reservation needs orphan cleanup before retry; a previously provisioned job is never replayed"
            );
        }
        let _lock = try_lock(&dir.join("controller.lock"))?
            .context("orphan controller is still stopping; retry shortly")?;
        admit_vm(home, &spec, hello)?;
        let config = Config::load(home)?;
        let disk: u64 = list(home)?
            .iter()
            .filter(|i| i.observed.state != State::Deleted)
            .map(|i| u64::from(i.record.spec.disk_gb))
            .sum();
        config.check_runtime(&spec)?;
        if !config.images.contains_key(&spec.image)
            || disk + u64::from(spec.disk_gb) > u64::from(config.disk_budget_gb)
        {
            bail!("current image catalog or disk budget cannot admit the retried VM reservation");
        }
        write(&dir.join("runtime.json"), &config)?;
        write(&dir.join("capacity.json"), hello)?;
        i.record.desired = Desired::Running;
        i.record.revision += 1;
        write(&dir.join("desired.json"), &i.record)?;
        return info(home, &id);
    }
    admit_vm(home, &spec, hello)?;
    create_inner(home, &id, spec, owner, hello, Some(job.into()))
}

/// Only job supervisors use this path, and only to release their own VM.
pub fn release_job(home: &Path, job: &str, desired: Desired) -> Result<Info> {
    let id = crate::vm_job::instance_id(job);
    let path = directory(home, &id)?.join("job-state.lock");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let _lock = loop {
        if let Some(lock) = try_lock(&path)? {
            break lock;
        }
        if std::time::Instant::now() >= deadline {
            bail!("job VM transition is busy; retry cancellation");
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    };
    let mut i = info(home, &id)?;
    if i.record.job.as_deref() != Some(job) || desired == Desired::Running {
        bail!("not this job's disposable VM");
    }
    if (i.record.desired == desired && i.observed.state != State::Failed)
        || (i.record.desired == Desired::Deleted && desired != Desired::Deleted)
    {
        return Ok(i);
    }
    if desired == Desired::Deleted
        && i.record.desired != Desired::Deleted
        && (i.observed.state != State::Stopped || i.observed.revision != i.record.revision)
    {
        bail!("wait for confirmed stop before deleting the job VM");
    }
    i.record.desired = desired;
    i.record.revision += 1;
    write(&directory(home, &id)?.join("desired.json"), &i.record)?;
    info(home, &id)
}
