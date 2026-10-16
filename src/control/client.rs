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
    Storage,
    ReviewStorageRecovery {
        request_id: String,
        expected_catalog: String,
        #[serde(default)]
        unavailable_locations: BTreeSet<String>,
    },
    Artifacts {
        #[serde(default)]
        attempt: Option<String>,
    },
    EnrollStorage {
        request_id: String,
        registration: super::artifact::Registration,
    },
    StoragePolicy {
        request_id: String,
        expected_revision: u64,
        policy: super::artifact::Policy,
    },
    ArtifactRetention {
        request_id: String,
        id: String,
        expected_revision: u64,
        pinned: bool,
        expires_at: Option<u64>,
    },
    DeleteArtifact {
        request_id: String,
        id: String,
        expected_revision: u64,
    },
    ExpireArtifacts {
        request_id: String,
        as_of: u64,
    },
    FetchArtifact {
        id: String,
        destination: PathBuf,
    },
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
        #[serde(default, skip_serializing_if = "Option::is_none")]
        worker: Option<String>,
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
            Self::ReviewStorageRecovery { request_id, .. }
            | Self::EnrollStorage { request_id, .. }
            | Self::StoragePolicy { request_id, .. }
            | Self::ArtifactRetention { request_id, .. }
            | Self::DeleteArtifact { request_id, .. }
            | Self::ExpireArtifacts { request_id, .. }
            | Self::Submit { request_id, .. }
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
            ApiRequest::ReviewStorageRecovery {
                expected_catalog,
                unavailable_locations,
                ..
            } => Mutation::ReviewStorageRecovery {
                expected_catalog: expected_catalog.clone(),
                unavailable_locations: unavailable_locations.clone(),
            },
            ApiRequest::EnrollStorage { registration, .. } => {
                Mutation::EnrollStorage(registration.clone())
            }
            ApiRequest::StoragePolicy {
                expected_revision,
                policy,
                ..
            } => Mutation::StoragePolicy {
                expected_revision: *expected_revision,
                policy: policy.clone(),
            },
            ApiRequest::ArtifactRetention {
                id,
                expected_revision,
                pinned,
                expires_at,
                ..
            } => Mutation::ArtifactRetention {
                id: id.clone(),
                expected_revision: *expected_revision,
                pinned: *pinned,
                expires_at: *expires_at,
            },
            ApiRequest::DeleteArtifact {
                id,
                expected_revision,
                ..
            } => Mutation::DeleteArtifact {
                id: id.clone(),
                expected_revision: *expected_revision,
            },
            ApiRequest::ExpireArtifacts { as_of, .. } => {
                Mutation::ExpireArtifacts { as_of: *as_of }
            }
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
                let prepared = async {
                    let spec = c.prepare(job).await?;
                    let inputs = if spec.vm.as_ref().and_then(|v| v.runtime.as_ref()).is_some() {
                        let mut files = Vec::new();
                        for (path, input) in &spec.inputs {
                            let iroh_blobs::api::blobs::BlobStatus::Complete { size } =
                                store.blobs().status(input.hash).await?
                            else {
                                bail!("input bytes missing from client spool");
                            };
                            files.push(crate::jobs::OutputFile {
                                path: path.clone(),
                                hash: input.hash,
                                size,
                            });
                        }
                        Some(super::artifact::Manifest::inputs(id.clone(), files)?)
                    } else {
                        None
                    };
                    Ok::<_, anyhow::Error>((spec, inputs))
                }
                .await;
                let synced = store.sync_db().await;
                let closed = store.shutdown().await;
                let (spec, inputs) = prepared?;
                synced?;
                closed?;
                Mutation::Enqueue(Submission {
                    inputs,
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
                worker,
                id,
                expected_revision,
                ..
            } => Mutation::ResumeJob {
                worker: worker.clone(),
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
                if let Some(bundle) = state.input_bundle(job) {
                    let result = async {
                        if state.durable_artifact(bundle) {
                            return Ok::<_, anyhow::Error>("verified storage copies ready");
                        }
                        let root = self.home.join("input-uploads").join(&bundle.manifest.id);
                        if root.exists()
                            && super::artifact_io::verify(&root, &bundle.manifest).is_err()
                        {
                            std::fs::remove_dir_all(&root)?;
                        }
                        if !root.exists() {
                            std::fs::create_dir_all(&root)?;
                            for f in &bundle.manifest.files {
                                let path = root.join(&f.path);
                                std::fs::create_dir_all(path.parent().context("input parent")?)?;
                                store.blobs().export(f.hash, &path).await?;
                            }
                            super::artifact_io::verify(&root, &bundle.manifest)?;
                        }
                        for target in &bundle.targets {
                            if bundle
                                .copies
                                .get(&format!("storage/{target}"))
                                .is_some_and(|c| {
                                    c.authority == state.cluster
                                        && c.phase == super::artifact::CopyPhase::Ready
                                })
                            {
                                continue;
                            }
                            let peer = &state.storage.stores[target].peer;
                            super::input_upload::upload(
                                &self.transport.endpoint,
                                peer,
                                &state.cluster,
                                &bundle.manifest,
                                &root,
                            )
                            .await?;
                        }
                        Ok("uploaded; awaiting storage verification report")
                    }
                    .await;
                    results.push(match result {
                        Ok(message) => json!({"job":job.id,"inputs":message}),
                        Err(e) => json!({"job":job.id,"waiting":format!("{e:#}")}),
                    });
                    continue;
                }
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
            ApiRequest::Storage => {
                let state = self.read().await?;
                let mut workers = Vec::new();
                for w in state.workers.values() {
                    let report = tokio::time::timeout(
                        Duration::from_secs(3),
                        super::runtime::data_call(
                            &self.transport.endpoint,
                            &state.cluster,
                            &w.peer,
                            DataRequest::Storage,
                        ),
                    )
                    .await;
                    workers.push(match report {
                        Ok(Ok(crate::proto::Response::StorageReport(report))) => {
                            json!({"worker":w.id,"report":report,"error":null})
                        }
                        other => json!({"worker":w.id,"report":null,"error":format!("{other:?}")}),
                    });
                }
                let mut storage_reports = Vec::new();
                for store in state.storage.stores.values() {
                    let report = super::storage_status::read(
                        &self.transport.endpoint,
                        &store.peer,
                        &state.cluster,
                    )
                    .await;
                    storage_reports.push(match report {
                        Ok(report) => json!({"storage":store.id,"report":report,"error":null}),
                        Err(error) => {
                            json!({"storage":store.id,"report":null,"error":error.to_string()})
                        }
                    });
                }
                Ok(
                    json!({"storage_reports":storage_reports,"workers":workers,"retention":state.retention_plan(),"stores":state.storage.stores,"policy":state.storage.policy,"revision":state.storage.policy_revision,"catalog_fingerprint":state.artifact_catalog_fingerprint(),"cleanup_allowed":state.artifact_cleanup_allowed(None),"quarantined_locations":state.storage.quarantined_locations}),
                )
            }
            ApiRequest::Artifacts { attempt } => {
                let state = self.read().await?;
                Ok(json!(state.storage.artifacts.values().filter(|a| attempt.as_ref().is_none_or(|id| a.manifest.attempt() == Some(id.as_str()))).map(|a| json!({"artifact":a,"protected":state.artifact_protected(&a.manifest.id)})).collect::<Vec<_>>()))
            }
            ApiRequest::FetchArtifact { id, destination } => {
                let state = self.read().await?;
                let artifact = state
                    .storage
                    .artifacts
                    .get(id)
                    .context("unknown artifact")?;
                super::artifact_io::fetch_available(
                    &self.transport.endpoint,
                    &state,
                    artifact,
                    destination,
                )
                .await?;
                Ok(json!({"artifact":id,"destination":destination,"files":artifact.manifest.files}))
            }
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
            ApiRequest::ReviewStorageRecovery { .. }
            | ApiRequest::EnrollStorage { .. }
            | ApiRequest::StoragePolicy { .. }
            | ApiRequest::ArtifactRetention { .. }
            | ApiRequest::DeleteArtifact { .. }
            | ApiRequest::ExpireArtifacts { .. }
            | ApiRequest::Cancel { .. }
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
                    json!({"format":state.format,"cluster":state.cluster,"quorum":true,"membership":state.membership_view(),"replicas":replicas,"recovery":state.recovery,"pending":state.scheduling_order().len()}),
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
                ensure!(!state.checkpoint_deleted(id), "checkpoint was deleted");
                let artifact = state
                    .storage
                    .artifacts
                    .values()
                    .find(|a| a.manifest.kind == super::artifact::Kind::Checkpoint(id.clone()))
                    .context("checkpoint artifact unavailable")?;
                std::fs::create_dir(destination).context("checkpoint destination must be new")?;
                let result = async {
                    let files = destination.join("files");
                    super::artifact_io::fetch_available(
                        &self.transport.endpoint,
                        &state,
                        artifact,
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
                if result.is_err() {
                    let _ = std::fs::remove_dir_all(destination);
                }
                result
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
                    ApiRequest::Outputs { .. } => {
                        if let Some(artifact) = state.storage.artifacts.values().find(|m| {
                            m.manifest.attempt() == Some(a.id.as_str())
                                && m.manifest.kind == super::artifact::Kind::FinalOutputs
                        }) {
                            ensure!(!artifact.deleted, "final outputs were deleted");
                            return Ok(json!({"Outputs":artifact.manifest.files}));
                        }
                        DataRequest::Outputs {
                            execution: a.execution.clone(),
                        }
                    }
                    ApiRequest::Fetch { destination, .. } => {
                        if let Some(artifact) = state.storage.artifacts.values().find(|m| {
                            m.manifest.attempt() == Some(a.id.as_str())
                                && m.manifest.kind == super::artifact::Kind::FinalOutputs
                        }) {
                            super::artifact_io::fetch_available(
                                &self.transport.endpoint,
                                &state,
                                artifact,
                                &destination,
                            )
                            .await?;
                            return Ok(json!(artifact.manifest.files));
                        }
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
        let artifacts: Vec<_> = state.storage.artifacts.values().filter(|a| a.manifest.job() == Some(j.id.as_ref()) || j.attempts.iter().any(|id| Some(id.as_ref()) == a.manifest.attempt())).collect();
        let resume_targets: BTreeMap<_,_> = if j.phase == JobPhase::Suspended && super::portable::runtime(&j.workload).is_some() { state.workers.keys().map(|worker| {
            let mut probe=state.clone();
            let error=match probe.resume_job_on(&j.id,j.revision,Some(worker)) {
                Reply::Accepted {..} => probe.placement_candidate(&j.id,worker).err().map(|e| e.to_string()),
                Reply::Rejected {reason} => Some(reason),
                _ => Some("resume unavailable".into()),
            };
            (worker.clone(),error)
        }).collect() } else { BTreeMap::new() };
        json!({"resume_targets":resume_targets,"portable":super::portable::runtime(&j.workload),"input_bundle":j.input_bundle,"portable_ready":state.portable_ready(j).is_ok(),"portability_waiting":state.portable_ready(j).err().map(|e| e.to_string()),"resume_worker":j.resume_worker,"artifacts":artifacts,"id":j.id,"revision":j.revision,"phase":phase,"cancel_requested":matches!(j.phase,JobPhase::Cancelling|JobPhase::Cancelled),"priority":j.priority,"after":j.after,"workload":j.workload,"assignment":a,"attempts":attempts,"latest_checkpoint":latest_checkpoint(state,&j.id),"resume_from":j.resume_from,"waiting":reasons})
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
        .storage
        .artifacts
        .values()
        .filter(|artifact| !artifact.deleted && artifact.manifest.attempt() == Some(a.id.as_str()))
        .filter_map(|artifact| match &artifact.manifest.kind {
            super::artifact::Kind::Checkpoint(id) => {
                state.checkpoints.get(id).map(|c| (artifact.ordinal, c))
            }
            _ => None,
        })
        .max_by_key(|(ordinal, c)| {
            (
                *ordinal,
                c.content.captured_at,
                c.content.source.as_str(),
                c.id.as_str(),
            )
        })
        .map(|(_, c)| c)
}
