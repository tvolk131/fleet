//! Shared normal-client API. Request journals and file destinations are client-local.
use super::{
    Client, Config, Mutation, Reply, Request, State,
    discovery::incarnation_cache,
    queue::Submission,
    runtime::{self, DataRequest},
    workload::*,
};
use crate::{coordinator::JobFile, instances};
use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    time::Duration,
};

const WAIT: Duration = Duration::from_secs(15);

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum ApiRequest {
    Status,
    Queue,
    Job {
        id: String,
    },
    Workers,
    Instances,
    Operation {
        id: String,
    },
    Submit {
        request_id: String,
        id: String,
        job: Box<JobFile>,
        worker: Option<String>,
        memory_mb: u32,
        #[serde(default)]
        gpu: bool,
        #[serde(default)]
        priority: i32,
        #[serde(default)]
        after: BTreeSet<String>,
        #[serde(default)]
        requires: BTreeSet<String>,
    },
    Cancel {
        request_id: String,
        id: String,
        expected_revision: u64,
    },
    Suspend {
        request_id: String,
        id: String,
        expected_revision: u64,
        checkpoint: String,
    },
    Resume {
        request_id: String,
        id: String,
        expected_revision: u64,
    },
    Checkpoints {
        id: String,
    },
    Attempts {
        id: String,
    },
    FetchCheckpoint {
        id: String,
        destination: PathBuf,
    },
    InstanceCreate {
        request_id: String,
        id: String,
        worker: String,
        spec: instances::Spec,
    },
    InstanceSet {
        request_id: String,
        id: String,
        expected_revision: u64,
        desired: InstanceDesired,
    },
    Enroll {
        request_id: String,
        registration: Registration,
    },
    Drain {
        request_id: String,
        worker: String,
        drain: bool,
    },
    Membership {
        request_id: String,
        expected_revision: u64,
        voters: std::collections::BTreeMap<u64, super::network::Peer>,
    },
    ResumeRecovery {
        request_id: String,
        #[serde(default)]
        unavailable_workers: BTreeSet<String>,
    },
    Retry {
        request_id: String,
    },
    Requests,
    Stage {
        id: Option<String>,
    },
    Logs {
        id: String,
        #[serde(default)]
        attempt: Option<u32>,
        #[serde(default)]
        stderr: bool,
        #[serde(default)]
        from: u64,
        #[serde(default = "log_size")]
        max: u32,
    },
    Outputs {
        id: String,
        #[serde(default)]
        attempt: Option<u32>,
    },
    Fetch {
        id: String,
        #[serde(default)]
        attempt: Option<u32>,
        destination: PathBuf,
    },
    Data {
        worker: String,
        request: DataRequest,
    },
}
fn log_size() -> u32 {
    65536
}
impl ApiRequest {
    pub fn request_id(&self) -> Option<&str> {
        match self {
            Self::Submit { request_id, .. }
            | Self::Cancel { request_id, .. }
            | Self::Suspend { request_id, .. }
            | Self::Resume { request_id, .. }
            | Self::InstanceCreate { request_id, .. }
            | Self::InstanceSet { request_id, .. }
            | Self::Enroll { request_id, .. }
            | Self::Drain { request_id, .. }
            | Self::Membership { request_id, .. }
            | Self::ResumeRecovery { request_id, .. }
            | Self::Retry { request_id } => Some(request_id),
            _ => None,
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Prepared {
    intent: Value,
    request: Request,
    reply: Option<Reply>,
}

pub struct AppClient {
    pub home: PathBuf,
    pub transport: Client,
    _identity_lock: crate::keys::KeyLock,
}
impl AppClient {
    pub async fn connect(home: &Path) -> Result<Self> {
        let config_path = home.join("config.json");
        let config: Config = serde_json::from_slice(
            &std::fs::read(&config_path)
                .context("configure this client with fleet client import first")?,
        )?;
        ensure!(
            home.join("node.key").is_file(),
            "client identity is missing; do not generate a replacement automatically"
        );
        let lock_home = home.to_path_buf();
        let identity_lock = tokio::task::spawn_blocking(move || {
            crate::keys::lock_coordinator(&lock_home, Duration::from_secs(30))
        })
        .await??;
        let transport = Client::with_cache(
            crate::keys::load_or_create(&home.join("node.key"))?,
            config.cluster.clone(),
            config.peers,
            incarnation_cache(&config_path, &config.cluster),
        )
        .await?;
        Ok(Self {
            home: home.into(),
            transport,
            _identity_lock: identity_lock,
        })
    }
    fn request_path(&self, id: &str) -> Result<PathBuf> {
        ensure!(
            !id.is_empty() && id.len() <= 512,
            "request ID must contain 1 to 512 bytes"
        );
        Ok(self
            .home
            .join("requests")
            .join(format!("{}.json", blake3::hash(id.as_bytes()).to_hex())))
    }
    async fn send(&mut self, mut prepared: Prepared) -> Result<Value> {
        let path = self.request_path(&prepared.request.id)?;
        let reply = self
            .transport
            .write(prepared.request.clone(), WAIT)
            .await
            .with_context(|| {
                format!(
                    "outcome unknown; retry saved request {}",
                    prepared.request.id
                )
            })?;
        prepared.reply = Some(reply.clone());
        instances::write(&path, &prepared)
            .context("outcome unknown to the local request journal; retry the saved request")?;
        Ok(json!({"request_id":prepared.request.id,"reply":reply}))
    }
    pub async fn read(&mut self) -> Result<State> {
        self.transport.read(WAIT).await
    }
    async fn prepare_mutation(&mut self, api: &ApiRequest) -> Result<Value> {
        let id = api.request_id().context("mutation request ID required")?;
        let path = self.request_path(id)?;
        std::fs::create_dir_all(path.parent().expect("parent"))?;
        let _lock = instances::try_lock(&path.with_extension("lock"))?
            .context("request is being prepared by another client; retry the same ID")?;
        let intent = serde_json::to_value(api)?;
        if path.exists() {
            let prepared: Prepared = serde_json::from_slice(&std::fs::read(&path)?)?;
            ensure!(
                prepared.intent == intent,
                "request ID already prepared with different arguments; use retry to resend its saved payload"
            );
            return self.send(prepared).await;
        }
        let mutation = match api {
            ApiRequest::Submit {
                id,
                job,
                worker,
                memory_mb,
                gpu,
                priority,
                after,
                requires,
                ..
            } => {
                let _blob_lock = instances::try_lock(&self.home.join("blobs.lock"))?
                    .context("another client is preparing or staging inputs; retry")?;
                let store = crate::blobs::open(&self.home.join("blobs")).await?;
                let c = crate::coordinator::Coordinator::new(
                    self.transport.endpoint.clone(),
                    store.clone(),
                );
                let prepared = c.prepare(job).await;
                let synced = store.sync_db().await;
                let closed = store.shutdown().await;
                let spec = prepared?;
                synced?;
                closed?;
                Mutation::Enqueue(Submission {
                    id: id.clone(),
                    workload: Workload::Job {
                        spec: Box::new(spec),
                        gpu: *gpu,
                        memory_mb: *memory_mb,
                    },
                    worker: worker.clone(),
                    priority: *priority,
                    after: after.clone(),
                    requires: requires.clone(),
                })
            }
            ApiRequest::Cancel {
                id,
                expected_revision,
                ..
            } => Mutation::CancelQueued {
                id: id.clone(),
                expected_revision: *expected_revision,
            },
            ApiRequest::Suspend {
                id,
                expected_revision,
                checkpoint,
                ..
            } => {
                let checkpoint = if checkpoint == "latest" {
                    let state = self.read().await?;
                    latest_checkpoint(&state, id)
                        .context("no verified checkpoint for the current attempt")?
                        .id
                        .clone()
                } else {
                    checkpoint.clone()
                };
                Mutation::SuspendJob {
                    id: id.clone(),
                    expected_revision: *expected_revision,
                    checkpoint,
                }
            }
            ApiRequest::Resume {
                id,
                expected_revision,
                ..
            } => Mutation::ResumeJob {
                id: id.clone(),
                expected_revision: *expected_revision,
            },
            ApiRequest::InstanceCreate {
                id, worker, spec, ..
            } => Mutation::Assign {
                id: id.clone(),
                execution: format!("vm-{id}"),
                worker: worker.clone(),
                workload: Workload::Instance { spec: spec.clone() },
            },
            ApiRequest::InstanceSet {
                id,
                expected_revision,
                desired,
                ..
            } => Mutation::Desire {
                id: id.clone(),
                expected_revision: *expected_revision,
                desired: Desired::Instance(*desired),
            },
            ApiRequest::Enroll { registration, .. } => Mutation::Enroll {
                worker: registration.clone(),
            },
            ApiRequest::Drain { worker, drain, .. } => Mutation::Drain {
                worker: worker.clone(),
                drain: *drain,
            },
            ApiRequest::Membership {
                expected_revision,
                voters,
                ..
            } => Mutation::ChangeMembership {
                expected_revision: *expected_revision,
                voters: voters.clone(),
            },
            ApiRequest::ResumeRecovery {
                unavailable_workers,
                ..
            } => Mutation::ResumeRecovery {
                unavailable_workers: unavailable_workers.clone(),
            },
            _ => bail!("not a mutation"),
        };
        let prepared = Prepared {
            intent,
            request: Request {
                origin: self.transport.cluster.clone(),
                id: id.into(),
                mutation,
            },
            reply: None,
        };
        instances::write(&path, &prepared)?;
        self.send(prepared).await
    }
    pub async fn stage(&mut self, id: Option<&str>) -> Result<Value> {
        let state = self.read().await?;
        let _lock = instances::try_lock(&self.home.join("blobs.lock"))?
            .context("another client is using input storage; retry")?;
        let store = crate::blobs::open(&self.home.join("blobs")).await?;
        let result = async {
            let mut results = Vec::new();
            for job in state
                .scheduling_order()
                .into_iter()
                .filter(|j| id.is_none_or(|id| j.id.as_ref() == id))
            {
                let Workload::Job { spec, .. } = &job.workload else {
                    continue;
                };
                if spec.inputs.is_empty() {
                    continue;
                }
                for worker in state.workers.values().filter(|w| {
                    job.worker.as_ref().is_none_or(|id| id == &w.id)
                        && !job.staged.contains(&w.id)
                        && job.requires.is_subset(&w.tags)
                        && job.workload.validate(w).is_ok()
                }) {
                    let result = tokio::time::timeout(
                        Duration::from_secs(10),
                        runtime::push_inputs(
                            &store,
                            &self.transport.endpoint,
                            &self.transport.cluster,
                            &worker.peer,
                            spec,
                        ),
                    )
                    .await;
                    let result = match result {
                        Ok(r) => r,
                        Err(e) => Err(e.into()),
                    };
                    match result {
                        Ok(()) => {
                            let request = Request {
                                origin: state.cluster.clone(),
                                id: format!("stage-{}-{}", job.id, worker.incarnation),
                                mutation: Mutation::Stage {
                                    id: job.id.to_string(),
                                    worker: worker.id.clone(),
                                },
                            };
                            let reply = self.transport.write(request, WAIT).await?;
                            results.push(json!({"job":job.id,"worker":worker.id,"reply":reply}));
                        }
                        Err(e) => results.push(
                            json!({"job":job.id,"worker":worker.id,"waiting":format!("{e:#}")}),
                        ),
                    }
                }
            }
            Ok::<_, anyhow::Error>(json!(results))
        }
        .await;
        let synced = store.sync_db().await;
        let closed = store.shutdown().await;
        let result = result?;
        synced?;
        closed?;
        Ok(result)
    }
    pub async fn execute(&mut self, api: ApiRequest) -> Result<Value> {
        match &api {
            ApiRequest::Submit { id, .. } => {
                let mut answer = self.prepare_mutation(&api).await?;
                if answer["reply"].get("Accepted").is_some() {
                    answer["staging"] = match self.stage(Some(id)).await {
                        Ok(v) => v,
                        Err(e) => json!({"waiting":format!("{e:#}")}),
                    };
                }
                Ok(answer)
            }
            ApiRequest::Cancel { .. }
            | ApiRequest::Suspend { .. }
            | ApiRequest::Resume { .. }
            | ApiRequest::InstanceCreate { .. }
            | ApiRequest::InstanceSet { .. }
            | ApiRequest::Enroll { .. }
            | ApiRequest::Drain { .. }
            | ApiRequest::Membership { .. }
            | ApiRequest::ResumeRecovery { .. } => self.prepare_mutation(&api).await,
            ApiRequest::Retry { request_id } => {
                let path = self.request_path(request_id)?;
                let _lock = instances::try_lock(&path.with_extension("lock"))?
                    .context("request is in use")?;
                let prepared: Prepared = serde_json::from_slice(
                    &std::fs::read(path).context("no saved request with that ID")?,
                )?;
                self.send(prepared).await
            }
            ApiRequest::Requests => {
                let mut requests = Vec::new();
                if let Ok(entries) = std::fs::read_dir(self.home.join("requests")) {
                    for p in entries
                        .flatten()
                        .map(|e| e.path())
                        .filter(|p| p.extension().is_some_and(|e| e == "json"))
                    {
                        let r: Prepared = serde_json::from_slice(&std::fs::read(p)?)?;
                        requests.push(
                            json!({"id":r.request.id,"origin":r.request.origin,"reply":r.reply}),
                        );
                    }
                }
                Ok(json!(requests))
            }
            ApiRequest::Stage { id } => self.stage(id.as_deref()).await,
            ApiRequest::Status => {
                let state = self.read().await?;
                let mut replicas = Vec::new();
                for id in self.transport.peers.keys().copied().collect::<Vec<_>>() {
                    replicas.push(
                        match self.transport.inspect(id, Duration::from_secs(2)).await {
                            Ok(v) => json!(v),
                            Err(e) => json!({"id":id,"unavailable":e.to_string()}),
                        },
                    );
                }
                Ok(
                    json!({"cluster":state.cluster,"quorum":true,"membership":state.membership_view(),"replicas":replicas,"recovery":state.recovery,"pending":state.scheduling_order().len()}),
                )
            }
            ApiRequest::Queue => Ok(json!(job_views(&self.read().await?))),
            ApiRequest::Job { id } => job_views(&self.read().await?)
                .into_iter()
                .find(|j| j["id"] == *id)
                .context("unknown job"),
            ApiRequest::Workers => {
                let state = self.read().await?;
                let mut values = Vec::new();
                for w in state.workers.values() {
                    let status = tokio::time::timeout(
                        Duration::from_secs(2),
                        runtime::data_call(
                            &self.transport.endpoint,
                            &state.cluster,
                            &w.peer,
                            DataRequest::Status,
                        ),
                    )
                    .await;
                    values.push(json!({"registration":w,"draining":state.drained.contains(&w.id),"status":match status {Ok(Ok(v))=>json!(v),_=>Value::Null}}));
                }
                Ok(json!(values))
            }
            ApiRequest::Checkpoints { id } => {
                let state = self.read().await?;
                ensure!(state.jobs.contains_key(id), "unknown job");
                Ok(json!(
                    state
                        .checkpoints
                        .values()
                        .filter(|c| c.job.as_ref() == id)
                        .collect::<Vec<_>>()
                ))
            }
            ApiRequest::FetchCheckpoint { id, destination } => {
                let state = self.read().await?;
                let checkpoint = state.checkpoints.get(id).context("unknown checkpoint")?;
                let worker = state
                    .workers
                    .get(&checkpoint.worker)
                    .context("unknown checkpoint worker")?;
                std::fs::create_dir(destination)
                    .context("checkpoint destination must be a new directory")?;
                let cache = self
                    .home
                    .join("downloads")
                    .join(format!("{:032x}", rand::random::<u128>()));
                let store = crate::blobs::open(&cache).await?;
                let result = async {
                    let files = destination.join("files");
                    runtime::fetch_files(
                        &store,
                        &self.transport.endpoint,
                        &worker.peer,
                        &checkpoint.content.files,
                        &files,
                    )
                    .await?;
                    for file in &checkpoint.content.files {
                        checkpoint.content.verify_file(&files, file)?;
                    }
                    instances::write(&destination.join("checkpoint.json"), checkpoint)?;
                    Ok::<_, anyhow::Error>(json!({"checkpoint":id,"destination":destination}))
                }
                .await;
                let closed = store.shutdown().await;
                let cleaned = std::fs::remove_dir_all(&cache);
                let result = result?;
                closed?;
                cleaned?;
                Ok(result)
            }
            ApiRequest::Attempts { id } => {
                let state = self.read().await?;
                let job = state.jobs.get(id).context("unknown job")?;
                Ok(json!(
                    job.attempts
                        .iter()
                        .filter_map(|a| state.assignments.get(a.as_ref()))
                        .collect::<Vec<_>>()
                ))
            }
            ApiRequest::Operation { id } => Ok(json!(
                self.read()
                    .await?
                    .membership_operations
                    .get(id)
                    .context("unknown operation")?
            )),
            ApiRequest::Instances => Ok(json!(
                self.read()
                    .await?
                    .assignments
                    .values()
                    .filter(|a| matches!(a.workload, Workload::Instance { .. }))
                    .collect::<Vec<_>>()
            )),
            ApiRequest::Data { worker, request } => {
                let state = self.read().await?;
                let w = state.workers.get(worker).context("unknown worker")?;
                Ok(json!(
                    runtime::data_call(
                        &self.transport.endpoint,
                        &state.cluster,
                        &w.peer,
                        request.clone()
                    )
                    .await?
                ))
            }
            ApiRequest::Logs { id, attempt, .. }
            | ApiRequest::Outputs { id, attempt }
            | ApiRequest::Fetch { id, attempt, .. } => {
                let state = self.read().await?;
                let job = state.jobs.get(id).context("unknown job")?;
                let a = if let Some(number) = attempt {
                    ensure!(*number > 0, "attempt numbers begin at 1");
                    job.attempts
                        .get((*number - 1) as usize)
                        .and_then(|a| state.assignments.get(a.as_ref()))
                } else {
                    job.current(&state)
                }
                .context("job has no such attempt")?;
                let w = &state.workers[&a.worker];
                let request = match api {
                    ApiRequest::Logs {
                        stderr, from, max, ..
                    } => DataRequest::Logs {
                        execution: a.execution.clone(),
                        stderr,
                        from,
                        max,
                    },
                    ApiRequest::Outputs { .. } => DataRequest::Outputs {
                        execution: a.execution.clone(),
                    },
                    ApiRequest::Fetch { destination, .. } => {
                        // Each fetch has a separate transient client cache; parallel downloads do not lock the input spool.
                        let cache = self
                            .home
                            .join("downloads")
                            .join(format!("{:032x}", rand::random::<u128>()));
                        let store = crate::blobs::open(&cache).await?;
                        let result = runtime::fetch_outputs(
                            &store,
                            &self.transport.endpoint,
                            &state.cluster,
                            &w.peer,
                            &a.execution,
                            &destination,
                        )
                        .await;
                        let closed = store.shutdown().await;
                        // Output files are independent verified copies, so the transient cache is disposable.
                        let cleaned = std::fs::remove_dir_all(&cache);
                        let outputs = result?;
                        closed?;
                        cleaned?;
                        return Ok(json!(outputs));
                    }
                    _ => unreachable!(),
                };
                Ok(json!(
                    runtime::data_call(&self.transport.endpoint, &state.cluster, &w.peer, request)
                        .await?
                ))
            }
        }
    }
    pub async fn close(self) {
        self.transport.endpoint.close().await;
    }
}
pub fn job_views(state: &State) -> Vec<Value> {
    use super::queue::JobPhase;
    let mut jobs: Vec<_> = state.jobs.values().collect();
    jobs.sort_by_key(|j| j.submitted);
    jobs.into_iter().map(|j| {
        let a = j.current(state);
        let phase = if j.phase == JobPhase::Active { a.and_then(|a|a.observed.as_ref()).map(|(_,o)|o.phase.label()).unwrap_or_else(|| "pending".into()) } else { format!("{:?}",j.phase).to_lowercase() };
        let reasons = if j.phase == JobPhase::Queued { state.workers.keys().map(|w|(w.clone(),state.placement_candidate(&j.id,w).err().map(|e|e.to_string()).unwrap_or_else(||"waiting for worker poll and scheduler".into()))).collect::<BTreeMap<_,_>>() } else { BTreeMap::new() };
        let attempts: Vec<_> = j.attempts.iter().filter_map(|id| state.assignments.get(id.as_ref())).collect();
        json!({"id":j.id,"revision":j.revision,"phase":phase,"cancel_requested":matches!(j.phase,JobPhase::Cancelling|JobPhase::Cancelled),"priority":j.priority,"after":j.after,"workload":j.workload,"assignment":a,"attempts":attempts,"latest_checkpoint":latest_checkpoint(state,&j.id),"resume_from":j.resume_from,"waiting":reasons})
    }).collect()
}

pub async fn call(home: &Path, request: ApiRequest) -> Result<Value> {
    let mut c = AppClient::connect(home).await?;
    let result = c.execute(request).await;
    c.close().await;
    result
}

pub fn latest_checkpoint<'a>(
    state: &'a State,
    job: &str,
) -> Option<&'a super::checkpoint::Checkpoint> {
    let a = state.current_attempt(job)?;
    state
        .checkpoints
        .values()
        .filter(|c| c.attempt.as_ref() == a.id)
        .max_by_key(|c| {
            (
                c.content.captured_at,
                c.content.manifest.position,
                c.content.source.clone(),
                c.id.clone(),
            )
        })
}
