//! Prepare immutable job inputs independently of transport or control-plane clients.
use crate::jobs::{Input, JobSpec, safe_relative};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fs, os::unix::fs::PermissionsExt, path::PathBuf};
/// A job as written by hand or by a runner: inputs are local files.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct JobFile {
    #[serde(default)]
    pub metric_files: Vec<String>,
    #[serde(default)]
    pub history: crate::history::Policy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vm: Option<crate::instances::Spec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint: Option<crate::checkpoint::Policy>,
    pub name: String,
    pub argv: Vec<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    /// Path in the job's working directory -> local file. A local file's executable bit is kept.
    #[serde(default)]
    pub inputs: BTreeMap<String, PathBuf>,
    #[serde(default)]
    pub publish: Vec<String>,
    #[serde(default)]
    pub settle_seconds: Option<u64>,
    #[serde(default)]
    pub max_seconds: Option<u64>,
    /// Seconds between SIGTERM and SIGKILL; absent preserves the ten-second default.
    #[serde(default)]
    pub shutdown_grace_seconds: Option<u64>,
    #[serde(default)]
    pub keep_awake: Option<bool>,
    #[serde(default)]
    pub probes: Vec<crate::probes::Probe>,
    #[serde(default)]
    pub pause_keeps_gpu: Option<bool>,
    #[serde(default)]
    pub resume: Option<crate::suspend::ResumeSpec>,
    #[serde(default)]
    pub preempt: Option<crate::suspend::PreemptSpec>,
    #[serde(default)]
    pub threads: Option<u32>,
}

pub async fn prepare(store: &iroh_blobs::api::Store, job: &JobFile) -> Result<JobSpec> {
    let mut inputs = BTreeMap::new();
    for (path, local) in &job.inputs {
        safe_relative(path)?;
        let abs = std::path::absolute(local)?;
        let mode = fs::metadata(&abs)
            .with_context(|| format!("input {}", abs.display()))?
            .permissions()
            .mode();
        let hash = store.blobs().add_path(&abs).await?.hash;
        inputs.insert(
            path.clone(),
            Input {
                hash,
                executable: mode & 0o111 != 0,
            },
        );
    }
    let spec = JobSpec {
        metric_files: job.metric_files.clone(),
        history: job.history.clone(),
        vm: job.vm.clone(),
        checkpoint: job.checkpoint.clone(),
        name: job.name.clone(),
        argv: job.argv.clone(),
        env: job.env.clone(),
        inputs,
        publish: job.publish.clone(),
        settle_seconds: job.settle_seconds.unwrap_or(30),
        max_seconds: job.max_seconds,
        shutdown_grace_seconds: job.shutdown_grace_seconds,
        keep_awake: job.keep_awake.unwrap_or(true),
        probes: job.probes.clone(),
        pause_keeps_gpu: job.pause_keeps_gpu.unwrap_or(false),
        resume: job.resume.clone(),
        preempt: job.preempt.clone(),
        threads: job
            .threads
            .unwrap_or_else(|| job.vm.as_ref().map_or(1, |v| v.cpus)),
    };
    spec.validate()?;
    Ok(spec)
}
