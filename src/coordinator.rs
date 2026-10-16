//! The coordinator's side: submitting jobs (inputs pushed only when the agent lacks them) and fetching outputs (never
//! overwriting, skipping files already present with the same content). The CLI and the tests both use this.

use std::{
    collections::{BTreeMap, HashMap},
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    sync::Mutex,
};

use anyhow::{Context, Result, bail};
use iroh::{Endpoint, EndpointAddr, EndpointId, endpoint::Connection};
use iroh_blobs::{Hash, api::Store};
use serde::{Deserialize, Serialize};

use crate::{
    blobs,
    jobs::{Input, JobInfo, JobSpec, OutputFile, safe_relative},
    proto::{self, Hello, Protocol, Request, Response},
};

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

/// What a fetch did.
#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq)]
pub struct FetchReport {
    pub fetched: Vec<String>,
    /// Files fetched before that the job has changed since (a log that grew), replaced by their new content.
    #[serde(default)]
    pub updated: Vec<String>,
    /// Already present with the same content.
    pub skipped: Vec<String>,
    pub bytes: u64,
}

/// What the coordinator knows about a job's files when fetching its outputs.
#[derive(Debug, Default, Clone)]
pub struct Known {
    /// The job's inputs by path: an output still identical to its input is not fetched back.
    pub inputs: BTreeMap<String, Hash>,
    /// Local files this job (or an earlier segment of it) was fetched into, with the content written. One still as
    /// written may be replaced by the job's newer version of it; any other existing file is never overwritten.
    pub written: HashMap<PathBuf, Hash>,
}

pub struct Coordinator {
    pub endpoint: Endpoint,
    pub store: Store,
    /// Each agent's protocol version and hostname, from its latest `Hello` (an agent restarts, to upgrade, by
    /// dropping its connections, and a new connection's first request asks again).
    agents: Mutex<HashMap<EndpointId, (Protocol, String)>>,
}

impl Coordinator {
    pub fn new(endpoint: Endpoint, store: Store) -> Self {
        Self {
            endpoint,
            store,
            agents: Mutex::default(),
        }
    }

    fn remember(&self, connection: &Connection, hello: &Hello) {
        self.agents.lock().expect("agents").insert(
            connection.remote_id(),
            (hello.protocol, hello.hostname.clone()),
        );
    }

    /// An agent's protocol version and hostname, asking its `Hello` if this coordinator has not seen one.
    pub async fn agent_protocol(&self, connection: &Connection) -> Result<(Protocol, String)> {
        let known = self
            .agents
            .lock()
            .expect("agents")
            .get(&connection.remote_id())
            .cloned();
        if let Some(known) = known {
            return Ok(known);
        }
        match proto::request(connection, &Request::hello()).await? {
            Response::Hello(hello) => {
                self.remember(connection, &hello);
                Ok((hello.protocol, hello.hostname))
            }
            Response::Error(e) => bail!("{e}"),
            other => bail!("unexpected answer to Hello: {other:?}"),
        }
    }

    pub async fn connect(&self, agent: impl Into<EndpointAddr>) -> Result<Connection> {
        let agent = agent.into();
        let id = agent.id;
        self.endpoint
            .connect(agent, proto::ALPN)
            .await
            .map_err(proto::transport)
            .with_context(|| format!("connecting to {id}"))
    }

    /// Sends a request; an agent's `Error` answer becomes an error. A request that uses something the agent's
    /// protocol version predates is refused before it is sent.
    pub async fn ask(&self, connection: &Connection, request: &Request) -> Result<Response> {
        if !matches!(request, Request::Hello { .. }) {
            let (agent, host) = self.agent_protocol(connection).await?;
            if let Some((item, minor)) = proto::too_new(request, proto::PROTOCOL.agreed(agent)) {
                bail!(
                    "the agent on {host} speaks protocol {agent}, which predates `{item}` (protocol {}.{minor}); \
                     upgrade it",
                    agent.major
                );
            }
        }
        match proto::request(connection, request).await? {
            Response::Error(e) => bail!("{e}"),
            Response::Hello(hello) => {
                self.remember(connection, &hello);
                Ok(Response::Hello(hello))
            }
            other => Ok(other),
        }
    }

    pub async fn submit(&self, agent: impl Into<EndpointAddr>, job: &JobFile) -> Result<String> {
        self.submit_as(agent, job, None).await
    }

    /// Submits with a given job ID (a queue keeps one ID end to end).
    pub async fn submit_as(
        &self,
        agent: impl Into<EndpointAddr>,
        job: &JobFile,
        id: Option<String>,
    ) -> Result<String> {
        let spec = self.prepare(job).await?;
        self.submit_spec(agent, spec, id).await
    }

    /// Hashes a job file's inputs into the local store and returns the job as the agent will receive it. A queued
    /// job keeps these hashes, so it runs the inputs as they were when queued.
    pub async fn prepare(&self, job: &JobFile) -> Result<JobSpec> {
        let mut inputs = BTreeMap::new();
        for (path, local) in &job.inputs {
            safe_relative(path)?;
            let abs = std::path::absolute(local)?;
            let mode = fs::metadata(&abs)
                .with_context(|| format!("input {}", abs.display()))?
                .permissions()
                .mode();
            let hash = self.store.blobs().add_path(&abs).await?.hash;
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

    /// Sends a prepared job to an agent: pushes the inputs it lacks, confirms they arrived, then submits.
    pub async fn submit_spec(
        &self,
        agent: impl Into<EndpointAddr>,
        spec: JobSpec,
        id: Option<String>,
    ) -> Result<String> {
        self.submit_spec_allocated(agent, spec, id, None).await
    }

    pub async fn submit_spec_allocated(
        &self,
        agent: impl Into<EndpointAddr>,
        spec: JobSpec,
        id: Option<String>,
        gpu: Option<bool>,
    ) -> Result<String> {
        let agent = agent.into();
        let connection = self.connect(agent.clone()).await?;
        let hashes: Vec<Hash> = spec.inputs.values().map(|i| i.hash).collect();
        let Response::Missing(missing) =
            self.ask(&connection, &Request::Missing { hashes }).await?
        else {
            bail!("unexpected answer to Missing");
        };
        let pushing = blobs::push(&self.store, &self.endpoint, agent, &missing).await?;
        // The agent imports pushed blobs after the push call returns, and a refused push is not reported to the
        // pusher, so confirm that every input arrived before submitting.
        let start = std::time::Instant::now();
        let mut waiting = missing;
        while !waiting.is_empty() {
            if start.elapsed() > std::time::Duration::from_secs(120) {
                bail!(
                    "{} pushed input(s) did not arrive on the agent",
                    waiting.len()
                );
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            let Response::Missing(still) = self
                .ask(&connection, &Request::Missing { hashes: waiting })
                .await?
            else {
                bail!("unexpected answer to Missing");
            };
            waiting = still;
        }
        if let Some(pushing) = pushing {
            pushing.close(0u32.into(), b"done");
        }
        let request = if let Some(gpu) = gpu
            && self.agent_protocol(&connection).await?.0.minor >= 3
        {
            Request::SubmitAllocated {
                spec: Box::new(spec),
                id,
                gpu,
            }
        } else {
            Request::Submit {
                spec: Box::new(spec),
                id,
            }
        };
        let Response::Submitted { job } = self.ask(&connection, &request).await? else {
            bail!("unexpected answer to Submit");
        };
        connection.close(0u32.into(), b"done");
        Ok(job)
    }

    pub async fn jobs(&self, connection: &Connection) -> Result<Vec<JobInfo>> {
        match self.ask(connection, &Request::Jobs).await? {
            Response::Jobs(jobs) => Ok(jobs),
            other => bail!("unexpected answer to Jobs: {other:?}"),
        }
    }

    pub async fn instance_upload(
        &self,
        agent: EndpointAddr,
        id: String,
        operation: String,
        revision: u64,
        source: &Path,
        path: String,
    ) -> Result<crate::instances::guest::Response> {
        use crate::instances::guest::{self, Request as G, Spec};
        if !source.is_absolute() {
            bail!("upload source must be an absolute local path");
        }
        let meta = fs::symlink_metadata(source)?;
        if !meta.is_file() || meta.len() > guest::MAX_FILE {
            bail!("upload requires a regular file at most 1 GiB (no symlinks)");
        }
        let connection = self.connect(agent.clone()).await?;
        // Gate before storing or transferring any bytes.
        let (protocol, _) = self.agent_protocol(&connection).await?;
        if protocol.minor < 5 {
            bail!("guest operations require agent protocol 3.5");
        }
        let hash = self.store.blobs().add_path(source).await?.hash;
        let sha256 = guest::sha256(source)?;
        let spec = Spec::Upload {
            path,
            hash,
            size: meta.len(),
            sha256,
            executable: meta.permissions().mode() & 0o111 != 0,
        };
        spec.validate()?;
        let Response::Missing(missing) = self
            .ask(&connection, &Request::Missing { hashes: vec![hash] })
            .await?
        else {
            bail!("unexpected upload staging response");
        };
        let pushing = blobs::push(&self.store, &self.endpoint, agent, &missing).await?;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
        loop {
            let Response::Missing(waiting) = self
                .ask(&connection, &Request::Missing { hashes: vec![hash] })
                .await?
            else {
                bail!("unexpected upload confirmation");
            };
            if waiting.is_empty() {
                break;
            }
            if std::time::Instant::now() > deadline {
                bail!("upload blob has not arrived; retry the same operation ID");
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        if let Some(pushing) = pushing {
            pushing.close(0u32.into(), b"done");
        }
        let Response::InstanceGuest(response) = self
            .ask(
                &connection,
                &Request::InstanceGuest(G::Start {
                    id,
                    operation,
                    revision,
                    spec,
                }),
            )
            .await?
        else {
            bail!("unexpected guest upload response");
        };
        connection.close(0u32.into(), b"done");
        Ok(response)
    }

    pub async fn instance_download(
        &self,
        agent: EndpointAddr,
        id: String,
        operation: String,
        dest: &Path,
    ) -> Result<(u64, Hash)> {
        use crate::instances::guest::{self, Request as G, Response as R};
        if !dest.is_absolute() {
            bail!("download destination must be an absolute local path");
        }
        let connection = self.connect(agent.clone()).await?;
        let Response::InstanceGuest(R::Output { hash, size }) = self
            .ask(
                &connection,
                &Request::InstanceGuest(G::Output { id, operation }),
            )
            .await?
        else {
            bail!("unexpected guest download response");
        };
        if size > guest::MAX_FILE {
            bail!("worker returned an oversized guest download");
        }
        if let Ok(meta) = fs::symlink_metadata(dest) {
            if meta.is_file() && blobs::hash_file(dest)? == hash {
                return Ok((size, hash));
            }
            bail!(
                "refusing to overwrite existing download destination {}",
                dest.display()
            );
        }
        blobs::fetch(&self.store, &self.endpoint, agent, &[hash]).await?;
        let parent = dest
            .parent()
            .context("download destination has no parent")?;
        fs::create_dir_all(parent)?;
        let partial = parent.join(format!(".fleet-download-{:016x}", rand::random::<u64>()));
        let result = async {
            self.store.blobs().export(hash, &partial).await?;
            if fs::metadata(&partial)?.len() != size || blobs::hash_file(&partial)? != hash {
                bail!("download verification failed");
            }
            fs::set_permissions(&partial, fs::Permissions::from_mode(0o600))?;
            fs::File::open(&partial)?.sync_all()?;
            // Do not overwrite a destination that appeared during the transfer.
            fs::hard_link(&partial, dest)
                .with_context(|| format!("publishing {} without overwriting", dest.display()))?;
            fs::File::open(parent)?.sync_all()?;
            Ok((size, hash))
        }
        .await;
        let _ = fs::remove_file(partial);
        connection.close(0u32.into(), b"done");
        result
    }

    pub async fn outputs(&self, connection: &Connection, job: &str) -> Result<Vec<OutputFile>> {
        match self
            .ask(connection, &Request::Outputs { job: job.into() })
            .await?
        {
            Response::Outputs(files) => Ok(files),
            other => bail!("unexpected answer to Outputs: {other:?}"),
        }
    }

    /// Fetches a job's offered outputs into `dest`. Refuses (before writing anything) if a file there has different
    /// content; skips files already present with the same content, so it can be repeated as outputs appear.
    pub async fn fetch(
        &self,
        agent: impl Into<EndpointAddr>,
        job: &str,
        dest: &Path,
    ) -> Result<FetchReport> {
        self.fetch_known(agent, job, dest, &Known::default(), |_, _| {})
            .await
    }

    /// As [`Coordinator::fetch`], knowing the job's inputs (not fetched back while unchanged) and the files it was
    /// fetched into before (replaced when the job has changed them). `on_written` is told of each file written.
    pub async fn fetch_known(
        &self,
        agent: impl Into<EndpointAddr>,
        job: &str,
        dest: &Path,
        known: &Known,
        mut on_written: impl FnMut(&Path, Hash),
    ) -> Result<FetchReport> {
        let agent = agent.into();
        let connection = self.connect(agent.clone()).await?;
        let files: Vec<OutputFile> = self
            .outputs(&connection, job)
            .await?
            .into_iter()
            .filter(|f| known.inputs.get(&f.path) != Some(&f.hash))
            .collect();
        connection.close(0u32.into(), b"done");
        let mut report = FetchReport::default();
        let mut todo = Vec::new();
        let mut conflicts = Vec::new();
        for file in files {
            let target = std::path::absolute(dest.join(safe_relative(&file.path)?))?;
            if target.exists() {
                let local = blobs::hash_file(&target)?;
                if local == file.hash {
                    report.skipped.push(file.path);
                } else if known.written.get(&target) == Some(&local) {
                    todo.push((file, target, true));
                } else {
                    conflicts.push(target.display().to_string());
                }
            } else {
                todo.push((file, target, false));
            }
        }
        if !conflicts.is_empty() {
            bail!(
                "refusing to overwrite {} file(s) with different content: {}",
                conflicts.len(),
                conflicts.join(", ")
            );
        }
        let mut wanted = Vec::new();
        for (file, _, _) in &todo {
            if !self.store.blobs().has(file.hash).await? {
                wanted.push(file.hash);
            }
        }
        wanted.sort();
        wanted.dedup();
        blobs::fetch(&self.store, &self.endpoint, agent, &wanted).await?;
        for (file, target, replacing) in todo {
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent)?;
            }
            // Written beside the target and renamed into place, so a file never appears half written.
            let name = target
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_default();
            let partial = target.with_file_name(format!(".{name}.fleet-partial"));
            let _ = fs::remove_file(&partial);
            self.store
                .blobs()
                .export(file.hash, &partial)
                .await
                .with_context(|| format!("writing {}", partial.display()))?;
            fs::rename(&partial, &target)
                .with_context(|| format!("writing {}", target.display()))?;
            on_written(&target, file.hash);
            report.bytes += file.size;
            if replacing {
                report.updated.push(file.path);
            } else {
                report.fetched.push(file.path);
            }
        }
        Ok(report)
    }
}
