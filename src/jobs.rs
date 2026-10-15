//! Jobs: their specification, their folder on the agent, and the state read back from it.
//!
//! A job's folder (`$FLEET_HOME/jobs/<id>/`) holds `job.json` (the [`Job`]), `work/` (its working directory, where
//! its inputs are placed and its outputs appear), `stdout.log` and `stderr.log`, `supervisor.json` (the supervisor's
//! and the job's process IDs, once started), `cancel` (present once cancellation was requested) and `exit.json` (the
//! [`Exit`], once finished). The folder is the job's whole state, so an agent that restarts reads it back.

use std::{
    collections::BTreeMap,
    fs,
    path::{Component, Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use iroh_blobs::Hash;
use serde::{Deserialize, Serialize};

/// A file placed in the job's working directory before it starts.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Input {
    pub hash: Hash,
    pub executable: bool,
}

/// What to run, as the agent receives it: inputs by hash.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct JobSpec {
    /// Absent executes as a trusted native process; present provisions one disposable Tart VM.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vm: Option<crate::instances::Spec>,
    pub name: String,
    /// The command. A first element containing `/` is a path inside the working directory (for example
    /// `bin/dominion-ai-gpu`); otherwise it is looked up on the agent's `PATH`.
    pub argv: Vec<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    /// Paths relative to the working directory.
    #[serde(default)]
    pub inputs: BTreeMap<String, Input>,
    /// Patterns, relative to the working directory, of the outputs to offer: `*` matches within one path segment, and
    /// a matching folder offers every file inside it.
    #[serde(default)]
    pub publish: Vec<String>,
    /// An output is offered only once it has not changed for this long (until the job ends).
    #[serde(default = "default_settle")]
    pub settle_seconds: u64,
    #[serde(default)]
    pub max_seconds: Option<u64>,
    /// Seconds between SIGTERM and SIGKILL; absent preserves the ten-second default.
    #[serde(default)]
    pub shutdown_grace_seconds: Option<u64>,
    /// Hold a sleep-prevention assertion while the job runs.
    #[serde(default = "default_true")]
    pub keep_awake: bool,
    /// CPU threads the job uses (claimed from the machine's CPU slots; fleet sets `RAYON_NUM_THREADS` and
    /// `OMP_NUM_THREADS` to this unless the job sets them).
    #[serde(default = "one")]
    pub threads: u32,
    /// What to report as the job's progress while it runs.
    #[serde(default)]
    pub probes: Vec<crate::probes::Probe>,
    /// Keep the job's GPU slot while it is paused (by default a paused job releases it for scheduling).
    #[serde(default)]
    pub pause_keeps_gpu: bool,
    /// How the job continues from a checkpoint, if it can (`fleet suspend` needs this).
    #[serde(default)]
    pub resume: Option<crate::suspend::ResumeSpec>,
    /// Whether a higher-priority job may suspend this one.
    #[serde(default)]
    pub preempt: Option<crate::suspend::PreemptSpec>,
}

fn default_settle() -> u64 {
    30
}

fn default_true() -> bool {
    true
}

fn one() -> u32 {
    1
}

/// A job as recorded in its folder.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Job {
    pub id: String,
    pub spec: JobSpec,
    /// The coordinator that submitted it.
    pub submitter: String,
    pub submitted_at: u64,
}

/// How a job ended.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Exit {
    pub code: Option<i32>,
    pub signal: Option<i32>,
    pub reason: EndReason,
    pub finished_at: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum EndReason {
    Exited,
    Cancelled,
    TimeLimit,
    /// The command could not be started.
    StartFailed,
    /// Stopped to be continued later from a checkpoint.
    Suspended,
}

/// The supervisor's record, written once the job has started.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Started {
    pub supervisor_pid: u32,
    pub child_pid: u32,
    pub started_at: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    Starting,
    Running,
    /// Frozen (SIGSTOP) until continued.
    Paused,
    /// Stopped at a checkpoint, to be continued (by a new job, its next segment).
    Suspended,
    Succeeded,
    Failed,
    Cancelled,
    TimedOut,
    /// Neither running nor finished: its supervisor disappeared (for example the machine restarted).
    Lost,
}

impl State {
    pub fn finished(self) -> bool {
        !matches!(self, State::Starting | State::Running | State::Paused)
    }
}

/// A job's summary, as `Jobs` reports it.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Stopping {
    pub reason: EndReason,
    pub started_at: u64,
    pub deadline: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct JobInfo {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vm: Option<crate::vm_job::Status>,
    pub id: String,
    pub name: String,
    pub state: State,
    pub submitted_at: u64,
    pub started_at: Option<u64>,
    #[serde(default)]
    pub stopping: Option<Stopping>,
    pub exit: Option<Exit>,
}

/// An offered output file.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct OutputFile {
    /// Relative to the working directory.
    pub path: String,
    pub size: u64,
    pub hash: Hash,
}

pub fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// Checks that a path is relative and stays inside the folder it is joined to.
pub fn safe_relative(path: &str) -> Result<PathBuf> {
    let p = Path::new(path);
    if path.is_empty()
        || p.is_absolute()
        || p.components().any(|c| !matches!(c, Component::Normal(_)))
    {
        bail!("{path:?} must be a relative path without `..`");
    }
    Ok(p.to_path_buf())
}

impl JobSpec {
    pub fn validate(&self) -> Result<()> {
        if self.name.trim().is_empty() {
            bail!("a job needs a name");
        }
        if self.argv.is_empty() {
            bail!("a job needs a command");
        }
        if let Some(vm) = &self.vm {
            crate::vm_job::validate(self, vm)?;
        }
        for path in self.inputs.keys() {
            safe_relative(path)?;
        }
        if self.threads == 0 {
            bail!("a job needs at least one thread");
        }
        for probe in &self.probes {
            probe.validate()?;
        }
        if let Some(resume) = &self.resume {
            resume.validate()?;
        }
        for pattern in &self.publish {
            safe_relative(&pattern.replace('*', "x"))?;
        }
        Ok(())
    }
}

/// A sortable, readable job ID: the submission time, the name's first characters, and random hex.
pub fn new_id(name: &str) -> String {
    let slug: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .take(24)
        .collect();
    let stamp = now();
    format!(
        "{stamp}-{}-{:04x}",
        slug.trim_matches('-'),
        rand::random::<u16>()
    )
}

pub fn jobs_dir(home: &Path) -> PathBuf {
    home.join("jobs")
}

pub fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> Result<Option<T>> {
    match fs::read(path) {
        Ok(bytes) => Ok(Some(
            serde_json::from_slice(&bytes).with_context(|| path.display().to_string())?,
        )),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Writes JSON atomically (a temporary file, then a rename).
pub fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, serde_json::to_vec_pretty(value)?)?;
    fs::rename(tmp, path)?;
    Ok(())
}

fn alive(pid: u32) -> bool {
    // Signal 0 checks existence; EPERM means it exists under another user.
    let r = unsafe { libc::kill(pid as i32, 0) };
    r == 0 || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

/// A job's summary, read from its folder.
pub fn info(dir: &Path) -> Result<JobInfo> {
    let job: Job = read_json(&dir.join("job.json"))?.context("no job.json")?;
    let started: Option<Started> = read_json(&dir.join("supervisor.json"))?;
    let exit: Option<Exit> = read_json(&dir.join("exit.json"))?;
    let vm = if job.spec.vm.is_some() {
        read_json::<crate::vm_job::Status>(&dir.join("vm-job.json"))?
    } else {
        None
    };
    let state = match (&exit, &started) {
        (Some(e), _) => match (e.reason, e.code) {
            (EndReason::Exited, Some(0)) => State::Succeeded,
            (EndReason::Cancelled, _) => State::Cancelled,
            (EndReason::TimeLimit, _) => State::TimedOut,
            (EndReason::Suspended, _) => State::Suspended,
            _ => State::Failed,
        },
        (None, _) if job.spec.vm.is_some() => {
            if vm
                .as_ref()
                .is_some_and(|v| v.phase != crate::vm_job::Phase::Preparing)
            {
                State::Running
            } else {
                State::Starting
            }
        }
        (None, Some(s)) if alive(s.supervisor_pid) && dir.join("paused").exists() => State::Paused,
        (None, Some(s)) if alive(s.supervisor_pid) => State::Running,
        (None, Some(_)) => State::Lost,
        (None, None) if now().saturating_sub(job.submitted_at) < 30 => State::Starting,
        (None, None) => State::Lost,
    };
    Ok(JobInfo {
        vm,
        id: job.id,
        name: job.spec.name,
        state,
        submitted_at: job.submitted_at,
        started_at: started.map(|s| s.started_at),
        stopping: if exit.is_none() {
            read_json(&dir.join("stopping.json"))?
        } else {
            None
        },
        exit,
    })
}

/// Every job on this agent, oldest first.
pub fn list(home: &Path) -> Result<Vec<JobInfo>> {
    let dir = jobs_dir(home);
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path.join("job.json").exists() {
            out.push(info(&path)?);
        }
    }
    out.sort_by(|a, b| a.id.cmp(&b.id));
    Ok(out)
}

/// The job's folder, refusing IDs that are not plain names.
pub fn dir(home: &Path, id: &str) -> Result<PathBuf> {
    let rel = safe_relative(id)?;
    if rel.components().count() != 1 {
        bail!("not a job ID: {id}");
    }
    let dir = jobs_dir(home).join(rel);
    if !dir.join("job.json").exists() {
        bail!("no job {id} on this agent");
    }
    Ok(dir)
}

/// Whether a relative path matches a pattern (`*` within one segment).
pub fn matches(pattern: &str, path: &str) -> bool {
    let (p, s): (Vec<&str>, Vec<&str>) = (pattern.split('/').collect(), path.split('/').collect());
    p.len() <= s.len() && p.iter().zip(&s).all(|(p, s)| segment_matches(p, s))
}

fn segment_matches(pattern: &str, text: &str) -> bool {
    match pattern.split_once('*') {
        None => pattern == text,
        Some((head, rest)) => {
            let Some(tail) = text.strip_prefix(head) else {
                return false;
            };
            if rest.is_empty() {
                return true;
            }
            (0..=tail.len())
                .filter(|&i| tail.is_char_boundary(i))
                .any(|i| segment_matches(rest, &tail[i..]))
        }
    }
}

/// Files under `work` matching any pattern (a pattern that matches a folder offers everything inside it), with their
/// sizes and modification times, skipping files changed within `settle` unless `finished`.
pub fn published_files(
    work: &Path,
    patterns: &[String],
    settle: Duration,
    finished: bool,
) -> Result<Vec<(String, u64)>> {
    let mut out = Vec::new();
    let mut stack = vec![PathBuf::new()];
    while let Some(rel) = stack.pop() {
        let abs = work.join(&rel);
        for entry in fs::read_dir(&abs)? {
            let entry = entry?;
            let rel_child = rel.join(entry.file_name());
            let rel_str = rel_child.to_string_lossy().to_string();
            let meta = entry.metadata()?;
            if meta.is_dir() {
                stack.push(rel_child);
            } else if meta.is_file() && patterns.iter().any(|p| matches(p, &rel_str)) {
                let quiet = meta
                    .modified()
                    .ok()
                    .and_then(|m| m.elapsed().ok())
                    .is_none_or(|age| age >= settle);
                if finished || quiet {
                    out.push((rel_str, meta.len()));
                }
            }
        }
    }
    out.sort();
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn patterns_match_segments_and_folders() {
        assert!(matches("B/iter-*", "B/iter-0128/weights.mpk"));
        assert!(matches("B/metrics.jsonl", "B/metrics.jsonl"));
        assert!(!matches("B/metrics.jsonl", "B/metrics.jsonl.tmp"));
        assert!(matches("*.json", "summary.json"));
        assert!(!matches("B/iter-*", "B/resume/iter-0128/state.json"));
        assert!(matches("B/resume/iter-*", "B/resume/iter-0128/state.json"));
        assert!(matches("a*b*c", "aXXbYYc"));
        assert!(!matches("a*b*c", "aXXbYY"));
    }

    #[test]
    fn unsafe_paths_are_refused() {
        for bad in ["", "/etc/passwd", "../x", "a/../../x", "./a"] {
            assert!(safe_relative(bad).is_err(), "{bad}");
        }
        assert!(safe_relative("bin/dominion-ai-gpu").is_ok());
    }
}
