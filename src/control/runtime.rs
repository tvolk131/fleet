//! Real workers pull quorum-confirmed intent and reconcile it against durable backend records.
use super::{
    Client, Mutation, Reply, Request,
    network::{Answer, Message, Peer, connect_endpoint},
    workload::*,
};
use crate::{
    agent::{Agent, AgentConfig},
    blobs, instances, jobs, proto, vm_job,
};
use anyhow::{Context, Result, bail, ensure};
use iroh::{
    Endpoint, EndpointId,
    endpoint::Connection,
    protocol::{AcceptError, ProtocolHandler, Router},
};
use redb::{Database, Durability, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

mod artifacts;
mod history;
mod reclaim;
mod vm;

pub const DATA_ALPN: &[u8] = b"fleet/worker-data/1";
const JOURNAL: TableDefinition<&str, &[u8]> = TableDefinition::new("worker_journal");
const MAX_FRAME: usize = 8 << 20;

pub(crate) fn supported_format(format: u64) -> bool {
    u32::try_from(format).is_ok_and(vm::supported)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerConfig {
    pub cluster: String,
    pub registration: Registration,
    pub controllers: BTreeMap<u64, Peer>,
    pub administrators: BTreeSet<EndpointId>,
    pub supervisor: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct JournalState {
    #[serde(default)]
    vm: super::vm::Catalog,
    #[serde(default)]
    generation: u32,
    #[serde(default)]
    vm_attempted: BTreeSet<String>,
    #[serde(default)]
    artifacts: BTreeMap<String, super::artifact::Manifest>,
    #[serde(default)]
    deleted_artifacts: BTreeSet<String>,
    starts_allowed: bool,
    checkpoints: BTreeMap<String, super::checkpoint::Checkpoint>,
    known_checkpoints: BTreeSet<String>,
    format: u32,
    held: BTreeSet<String>,
    recovery_inventory: Option<super::recovery::Inventory>,
    recovery_binding: Option<super::recovery::Manifest>,
    cluster: String,
    registration: Registration,
    acknowledged: u64,
    sequence: u64,
    assignments: BTreeMap<String, Assignment>,
    observations: BTreeMap<String, (u64, Observation)>,
    attempted: BTreeSet<String>,
    outbox: BTreeMap<u64, Report>,
}

pub struct Journal {
    db: Database,
}
impl Journal {
    fn open(home: &Path, config: &WorkerConfig, initialize: bool) -> Result<Self> {
        std::fs::create_dir_all(home)?;
        let path = home.join("worker.redb");
        if !path.exists() {
            ensure!(
                !home.join("backend/jobs").exists() && !home.join("backend/instances").exists(),
                "backend exists without worker journal; explicit recovery required"
            );
        }
        if let Some(raw) = super::version::preflight(&path, "worker_journal")? {
            ensure!(
                raw["state"]["format"]
                    .as_u64()
                    .is_some_and(|f| vm::supported(f as u32)),
                "worker metadata format mismatch; explicit baseline migration required"
            );
        }
        let db = Database::create(path)?;
        let mut tx = db.begin_write()?;
        tx.set_durability(Durability::Immediate)?;
        {
            let mut table = tx.open_table(JOURNAL)?;
            let old = table
                .get("state")?
                .map(|v| serde_json::from_slice::<JournalState>(v.value()))
                .transpose()?;
            if let Some(old) = old {
                ensure!(
                    vm::supported(old.format)
                        && old.cluster == config.cluster
                        && old.registration == config.registration,
                    "worker journal identity/configuration mismatch"
                );
            } else {
                ensure!(
                    initialize,
                    "worker journal has no durable identity; explicit recovery required"
                );
                let state = JournalState {
                    vm: Default::default(),
                    generation: 0,
                    vm_attempted: BTreeSet::new(),
                    artifacts: BTreeMap::new(),
                    deleted_artifacts: BTreeSet::new(),
                    starts_allowed: true,
                    checkpoints: BTreeMap::new(),
                    known_checkpoints: BTreeSet::new(),
                    format: super::FORMAT,
                    held: BTreeSet::new(),
                    recovery_inventory: None,
                    recovery_binding: None,
                    cluster: config.cluster.clone(),
                    registration: config.registration.clone(),
                    acknowledged: 0,
                    sequence: 0,
                    assignments: BTreeMap::new(),
                    observations: BTreeMap::new(),
                    attempted: BTreeSet::new(),
                    outbox: BTreeMap::new(),
                };
                table.insert("state", serde_json::to_vec(&state)?.as_slice())?;
            }
        }
        tx.commit()?;
        Ok(Self { db })
    }
    fn read(&self) -> Result<JournalState> {
        let tx = self.db.begin_read()?;
        let table = tx.open_table(JOURNAL)?;
        Ok(serde_json::from_slice(
            table.get("state")?.context("missing journal")?.value(),
        )?)
    }
    fn change(&self, f: impl FnOnce(&mut JournalState) -> Result<()>) -> Result<()> {
        let mut tx = self.db.begin_write()?;
        tx.set_durability(Durability::Immediate)?;
        {
            let mut table = tx.open_table(JOURNAL)?;
            let mut state: JournalState =
                serde_json::from_slice(table.get("state")?.context("missing journal")?.value())?;
            f(&mut state)?;
            table.insert("state", serde_json::to_vec(&state)?.as_slice())?;
        }
        tx.commit()?;
        Ok(())
    }
    fn accept(&self, view: WorkerView) -> Result<()> {
        self.change(|s| {
            ensure!(
                view.registration == s.registration,
                "enrollment differs from local worker configuration"
            );
            ensure!(
                view.acknowledged >= s.acknowledged && view.acknowledged <= s.sequence,
                "controller/worker journal rollback detected"
            );
            ensure!(
                view.generation >= s.generation,
                "controller generation regressed"
            );
            ensure!(
                view.generation <= super::version::MAX,
                "worker does not support controller generation"
            );
            for (id, old) in &s.vm.operations {
                let new = view
                    .vm
                    .operations
                    .get(id)
                    .context("controller lost an accepted VM operation")?;
                ensure!(
                    new.spec == old.spec
                        && new.ordinal == old.ordinal
                        && new.instance == old.instance
                        && new.execution == old.execution
                        && new.instance_revision == old.instance_revision
                        && new.worker == old.worker
                        && new.incarnation == old.incarnation
                        && new.retention_seconds == old.retention_seconds
                        && (!old.released || new.released)
                        && (!old.reclaimed || new.reclaimed)
                        && (!old.outcome.phase.terminal() || new.outcome == old.outcome)
                        && (new.revision != old.revision
                            || (new.outcome == old.outcome && new.released == old.released))
                        && new.revision >= old.revision,
                    "VM operation changed or regressed"
                );
            }
            s.vm = view.vm;
            s.generation = view.generation;
            if s.generation >= super::vm::GENERATION {
                s.format = vm::FORMAT;
            }
            for old in s.assignments.values() {
                let new = view
                    .assignments
                    .iter()
                    .find(|a| a.id == old.id)
                    .context("controller lost an accepted assignment")?;
                ensure!(
                    new.execution == old.execution
                        && new.worker == old.worker
                        && new.workload == old.workload
                        && new.attempt == old.attempt
                        && new.revision >= old.revision,
                    "accepted assignment changed or regressed"
                );
                ensure!(
                    new.revision != old.revision
                        || (new.desired == old.desired
                            && new.suspend_checkpoint == old.suspend_checkpoint),
                    "same revision changed intent"
                );
            }
            for a in view.assignments {
                ensure!(a.worker == s.registration.id, "wrong assigned worker");
                a.validate(&s.registration)?;
                s.assignments.insert(a.id.clone(), a);
            }
            ensure!(!view.recovery_required, "worker recovery has not committed");
            for (id, cp) in view.resume_checkpoints {
                cp.content.validate()?;
                ensure!(
                    s.checkpoints.get(&id).is_none_or(|old| old == &cp),
                    "resume checkpoint changed"
                );
                s.checkpoints.insert(id, cp);
            }
            s.known_checkpoints = view.checkpoints;
            s.held = view.held;
            s.starts_allowed = view.starts_allowed;
            s.recovery_inventory = None;
            s.acknowledged = view.acknowledged;
            s.outbox.retain(|seq, _| *seq > s.acknowledged);
            Ok(())
        })
    }
    fn observe(&self, a: &Assignment, observation: Observation) -> Result<()> {
        let old = self.read()?;
        if old.observations.get(&a.id) == Some(&(a.revision, observation.clone())) {
            return Ok(());
        }
        self.change(|s| {
            s.sequence += 1;
            s.outbox.insert(
                s.sequence,
                Report {
                    worker: s.registration.id.clone(),
                    incarnation: s.registration.incarnation.clone(),
                    sequence: s.sequence,
                    assignment: a.id.clone(),
                    revision: a.revision,
                    observation: observation.clone(),
                },
            );
            s.observations
                .insert(a.id.clone(), (a.revision, observation));
            Ok(())
        })
    }
}

/// Direct data plane has no execute or lifecycle commands. Those must pass through consensus.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DataRequest {
    VmInput {
        id: String,
    },
    VmLogs {
        id: String,
        stderr: bool,
        from: u64,
    },
    History {
        execution: String,
        stream: crate::history::Stream,
        from: u64,
        max: u32,
    },
    Storage,
    ReserveInputs {
        hashes: Vec<iroh_blobs::Hash>,
    },
    Status,
    Jobs,
    Instances,
    Logs {
        execution: String,
        stderr: bool,
        from: u64,
        max: u32,
    },
    Outputs {
        execution: String,
    },
    Events {
        since: u64,
        wait_ms: u32,
    },
    Missing {
        hashes: Vec<iroh_blobs::Hash>,
    },
}
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DataFrame {
    cluster: String,
    request: DataRequest,
}

#[derive(Clone, Debug)]
struct DataHandler {
    vm_access: Arc<std::sync::RwLock<super::vm::Catalog>>,
    home: PathBuf,
    cluster: String,
    allow: Arc<HashSet<EndpointId>>,
    agent: Agent,
    store: iroh_blobs::api::Store,
}
impl ProtocolHandler for DataHandler {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let peer = connection.remote_id();
        if !self.allow.contains(&peer) {
            connection.close(403u32.into(), b"not authorized");
            return Ok(());
        }
        while let Ok((mut send, mut recv)) = connection.accept_bi().await {
            // Keep stream work scoped to the router-owned connection task.
            {
                let result = tokio::time::timeout(Duration::from_secs(30), async {
                    let frame: DataFrame =
                        serde_json::from_slice(&recv.read_to_end(MAX_FRAME).await?)?;
                    ensure!(frame.cluster == self.cluster, "cluster mismatch");
                    let special = match &frame.request {
                        DataRequest::VmInput{id}=> {
                            let _gate=self.agent.blob_guard().await;
                            let op=self.vm_access.read().expect("VM access").operations.get(id).cloned().context("operation has not reached worker")?;
                            ensure!(!op.released,"operation input was released");
                            let instances::guest::Spec::Upload{hash,size,..}=op.spec else {bail!("not an upload");};
                            self.store.tags().set(vm::input_tag(id),iroh_blobs::HashAndFormat::raw(hash)).await?;
                            let complete=matches!(self.store.blobs().status(hash).await?,iroh_blobs::api::blobs::BlobStatus::Complete{size:stored} if size==stored);
                            self.store.sync_db().await?;
                            Some(proto::Response::Missing(if complete {vec![]} else {vec![hash]}))
                        }
                        DataRequest::VmLogs{id,stderr,from}=>{
                            let op=self.vm_access.read().expect("VM access").operations.get(id).cloned().context("unknown operation")?;
                            ensure!(!op.released,"operation output was released");
                            Some(proto::Response::InstanceGuest(instances::guest::Response::Logs(instances::guest::logs(&self.home.join("backend"),&op.execution,&op.backend_id(),*stderr,*from)?)))
                        }
                        DataRequest::History {
                            execution,
                            stream,
                            from,
                            max,
                        } => {
                            let dir = jobs::dir(&self.home.join("backend"), execution)?;
                            Some(proto::Response::History(crate::history::read(
                                &dir,
                                *stream,
                                *from,
                                *max as usize,
                            )?))
                        }
                        DataRequest::Storage => Some(proto::Response::StorageReport(
                            jobs::read_json(&self.home.join("storage-report.json"))?
                                .unwrap_or(serde_json::Value::Null),
                        )),
                        DataRequest::ReserveInputs { hashes } => {
                            Some(self.agent.reserve_inputs(hashes).await?)
                        }
                        _ => None,
                    };
                    let response = if let Some(response) = special {
                        response
                    } else {
                        let request = match frame.request {
                            DataRequest::VmInput{..} | DataRequest::VmLogs{..} | DataRequest::Storage
                            | DataRequest::ReserveInputs { .. }
                            | DataRequest::History { .. } => {
                                unreachable!()
                            }
                            DataRequest::Status => proto::Request::Status,
                            DataRequest::Jobs => proto::Request::Jobs,
                            DataRequest::Instances => proto::Request::Instances,
                            DataRequest::Logs {
                                execution,
                                stderr,
                                from,
                                max,
                            } => proto::Request::Logs {
                                job: execution,
                                stderr,
                                from,
                                max,
                            },
                            DataRequest::Outputs { execution } => {
                                proto::Request::Outputs { job: execution }
                            }
                            DataRequest::Events { since, wait_ms } => {
                                proto::Request::Events { since, wait_ms }
                            }
                            DataRequest::Missing { hashes } => proto::Request::Missing { hashes },
                        };
                        self.agent
                            .handle(peer, request)
                            .await
                            .unwrap_or_else(|e| proto::Response::Error(format!("{e:#}")))
                    };
                    if matches!(&response, proto::Response::Missing(hashes) if hashes.is_empty()) {
                        // A staging acknowledgement must survive worker process restart.
                        self.store.sync_db().await?;
                    }
                    Ok::<_, anyhow::Error>(response)
                })
                .await;
                let response = match result {
                    Ok(Ok(response)) => response,
                    Ok(Err(e)) => {
                        proto::Response::Error(format!("{e:#}").chars().take(1024).collect())
                    }
                    Err(_) => proto::Response::Error("worker data request timed out".into()),
                };
                let bytes = serde_json::to_vec(&response).map_err(AcceptError::from_err)?;
                if bytes.len() > MAX_FRAME {
                    continue;
                }
                let _ = tokio::time::timeout(Duration::from_secs(5), async {
                    send.write_all(&bytes).await?;
                    send.finish()?;
                    Ok::<_, anyhow::Error>(())
                })
                .await;
            }
        }
        Ok(())
    }
}

pub async fn data_call(
    endpoint: &Endpoint,
    cluster: &str,
    worker: &Peer,
    request: DataRequest,
) -> Result<proto::Response> {
    tokio::time::timeout(Duration::from_secs(30), async {
        let connection = endpoint.connect(worker.addr(), DATA_ALPN).await?;
        let (mut send, mut recv) = connection.open_bi().await?;
        let bytes = serde_json::to_vec(&DataFrame {
            cluster: cluster.into(),
            request,
        })?;
        ensure!(bytes.len() <= MAX_FRAME, "worker data request too large");
        send.write_all(&bytes).await?;
        send.finish()?;
        let response = serde_json::from_slice(&recv.read_to_end(MAX_FRAME).await?)?;
        connection.close(0u32.into(), b"read complete");
        if let proto::Response::Error(e) = response {
            bail!("worker: {e}");
        }
        Ok(response)
    })
    .await
    .context("worker data request timed out")?
}

pub struct Worker {
    reclamation: blobs::Reclamation,
    pub config: WorkerConfig,
    pub home: PathBuf,
    pub client: Client,
    pub router: Router,
    pub store: iroh_blobs::api::Store,
    journal: Journal,
    agent: Agent,
    artifact_access: Arc<std::sync::RwLock<super::artifact_io::Access>>,
    storage_view: Option<WorkerView>,
    vm_adapters: BTreeMap<String, super::tunnels::agent::Agent>,
    vm_access: Arc<std::sync::RwLock<super::vm::Catalog>>,
}

impl Worker {
    /// Offline half of an additive catalog update. A crash between the journal and config
    /// writes fails startup closed; rerunning repairs the config without replacing history.
    pub fn prepare_runtimes(home: &Path) -> Result<Request> {
        let path = home.join("worker.json");
        let mut config: WorkerConfig = serde_json::from_slice(&std::fs::read(&path)?)?;
        let db = Database::open(home.join("worker.redb"))
            .context("stop the worker before updating runtimes")?;
        let journal = Journal { db };
        let state = journal.read()?;
        ensure!(
            vm::supported(state.format) && state.cluster == config.cluster,
            "upgrade worker metadata first"
        );
        let mut identity = state.registration.clone();
        identity.runtimes = config.registration.runtimes.clone();
        ensure!(
            identity == config.registration,
            "worker identity/configuration mismatch"
        );
        ensure!(
            state.starts_allowed && state.held.is_empty() && state.recovery_inventory.is_none(),
            "worker requires recovery reconciliation"
        );
        ensure!(
            state.outbox.is_empty(),
            "let the controller acknowledge all worker reports before stopping it"
        );
        ensure!(
            state.assignments.values().all(|a| state
                .observations
                .get(&a.id)
                .is_some_and(|(r, o)| *r == a.revision && o.phase.terminal())),
            "worker has unfinished or unconfirmed work"
        );
        let vm = instances::Config::load(&home.join("backend"))?;
        ensure!(
            vm.memory_budget_mb == config.registration.budget.memory_mb
                && vm.disk_budget_gb == config.registration.budget.disk_gb
                && vm.images.keys().cloned().collect::<BTreeSet<_>>() == config.registration.images,
            "runtime update cannot change budgets or image aliases"
        );
        let runtimes = vm.runtimes()?;
        let next = state.registration.with_added_runtimes(&runtimes)?;
        config.registration.with_added_runtimes(&runtimes)?;
        ensure!(
            home.join("node.key").is_file()
                && crate::keys::load_or_create(&home.join("node.key"))?.public()
                    == next.peer.identity,
            "worker key mismatch"
        );
        let mutation = Mutation::AddWorkerRuntimes {
            worker: next.id.clone(),
            incarnation: next.incarnation.clone(),
            runtimes,
        };
        let request = Request {
            origin: config.cluster.clone(),
            id: format!(
                "worker-runtimes-{}",
                blake3::hash(&serde_json::to_vec(&mutation)?).to_hex()
            ),
            mutation,
        };
        if state.registration != next {
            super::upgrade::backup_version(&home.join("worker.redb"), super::FORMAT)?;
            journal.change(|s| {
                s.registration = next.clone();
                Ok(())
            })?;
        }
        if config.registration != next {
            super::upgrade::backup_version(&path, super::FORMAT)?;
            config.registration = next;
            instances::write(&path, &config)?;
        }
        instances::write(&home.join("runtimes-request.json"), &request)?;
        Ok(request)
    }

    pub(crate) fn upgrade_v5(
        db: &Database,
        home: &Path,
        expected: &super::State,
        preview: bool,
    ) -> Result<()> {
        let tx = db.begin_read()?;
        let table = tx.open_table(JOURNAL)?;
        let raw: serde_json::Value =
            serde_json::from_slice(table.get("state")?.context("missing worker state")?.value())?;
        let source_format = raw["format"].as_u64().context("worker format missing")? as u32;
        let already = vm::supported(source_format);
        let converted = if already {
            raw
        } else {
            super::upgrade::journal(raw)?
        };
        let state: JournalState = serde_json::from_value(converted)?;
        ensure!(
            state.cluster == expected.cluster
                && expected.workers.get(&state.registration.id) == Some(&state.registration),
            "worker does not belong to this controller state"
        );
        if already {
            return Ok(());
        }
        if preview {
            return Ok(());
        }
        drop(table);
        drop(tx);
        super::upgrade::backup_version(&home.join("worker.redb"), source_format)?;
        let mut tx = db.begin_write()?;
        tx.set_durability(Durability::Immediate)?;
        tx.open_table(JOURNAL)?
            .insert("state", serde_json::to_vec(&state)?.as_slice())?;
        tx.commit()?;
        Ok(())
    }
    /// Preparation creates the journal before enrollment/start. A missing journal on a later
    /// startup is never treated as a fresh worker with the old incarnation.
    pub fn initialize(home: &Path, config: &WorkerConfig) -> Result<()> {
        ensure!(
            !home.join("worker.redb").exists(),
            "worker journal already initialized"
        );
        let _journal = Journal::open(home, config, true)?;
        Ok(())
    }

    pub fn accept_recovery(home: &Path, manifest: &super::recovery::Manifest) -> Result<()> {
        let path = home.join("worker.json");
        let mut config: WorkerConfig = serde_json::from_slice(&std::fs::read(&path)?)?;
        // Reopen after a crash between the journal commit and config publication.
        let db = Database::open(home.join("worker.redb"))?;
        let journal = Journal { db };
        let old = journal.read()?;
        if old.cluster == manifest.plan.config.cluster {
            ensure!(
                old.recovery_binding.as_ref() == Some(manifest),
                "different recovery already selected"
            );
            manifest.verify(&manifest.plan.previous, &config.administrators)?;
        } else {
            manifest.verify(&config.cluster, &config.administrators)?;
            ensure!(
                old.cluster == config.cluster
                    && old.registration == config.registration
                    && vm::supported(old.format),
                "worker journal/config identity mismatch"
            );
            journal.change(|s| {
                s.cluster = manifest.plan.config.cluster.clone();
                s.starts_allowed = false;
                s.vm = Default::default();
                s.generation = 0;
                s.recovery_binding = Some(manifest.clone());
                let mut assignments = s.assignments.clone();
                for (id, a) in &mut assignments {
                    if let Some(observation) = s.observations.get(id) {
                        a.observed = Some(observation.clone());
                    }
                }
                s.recovery_inventory = Some(super::recovery::Inventory {
                    checkpoints: s
                        .checkpoints
                        .iter()
                        .filter(|(_, c)| c.worker == s.registration.id)
                        .map(|(id, c)| (id.clone(), c.clone()))
                        .collect(),
                    held: s.held.clone(),
                    registration: s.registration.clone(),
                    sequence: s.sequence,
                    assignments,
                });
                Ok(())
            })?;
        }
        // Preserve original identities and backend ownership; only controller authority changes.
        config.cluster = manifest.plan.config.cluster.clone();
        config.controllers = manifest.plan.config.peers.clone();
        crate::instances::write(&path, &config)?;
        Ok(())
    }

    pub async fn start(home: &Path, config: WorkerConfig, mut hello: proto::Hello) -> Result<Self> {
        config.registration.validate()?;
        ensure!(
            home.join("node.key").is_file(),
            "worker key is missing; recover it instead of creating a new identity"
        );
        let key = crate::keys::load_or_create(&home.join("node.key"))?;
        ensure!(
            key.public() == config.registration.peer.identity,
            "worker key mismatch"
        );
        ensure!(
            config.registration.budget.cpu_slots <= hello.cpu_slots
                && u64::from(config.registration.budget.memory_mb)
                    <= hello.memory_bytes / (1 << 20),
            "worker budget exceeds host capacity"
        );
        ensure!(
            config.supervisor.is_absolute() && config.supervisor.is_file(),
            "explicit absolute supervisor executable required"
        );
        ensure!(
            home.join("worker.redb").is_file(),
            "missing worker journal; prepare a new worker identity or recover its durable state"
        );
        let journal = Journal::open(home, &config, false)?;
        let inventory = journal.read()?;
        // Never adopt backend records not owned by this journal, including completed records.
        for job in jobs::list(&home.join("backend"))? {
            ensure!(
                inventory
                    .assignments
                    .values()
                    .any(|a| a.execution == job.id && matches!(a.workload, Workload::Job { .. })),
                "unrecognized backend job {}; explicit recovery required",
                job.id
            );
        }
        for instance in instances::list(&home.join("backend"))? {
            ensure!(
                inventory.assignments.values().any(|a| match &a.workload {
                    Workload::Job { spec, .. } =>
                        spec.vm.is_some()
                            && instance.record.job.as_deref() == Some(a.execution.as_str())
                            && instance.record.id == vm_job::instance_id(&a.execution),
                    Workload::Instance { .. } =>
                        instance.record.job.is_none() && instance.record.id == a.execution,
                }),
                "unrecognized backend VM {}; explicit recovery required",
                instance.record.id
            );
        }
        let backend = home.join("backend");
        if config.registration.budget.vm_slots > 0 {
            let vm = instances::Config::load(&backend)?;
            ensure!(
                vm.memory_budget_mb == config.registration.budget.memory_mb
                    && vm.disk_budget_gb == config.registration.budget.disk_gb
                    && vm.images.keys().cloned().collect::<BTreeSet<_>>()
                        == config.registration.images,
                "Tart configuration and enrollment budgets/catalog differ"
            );
        }
        hello.cpu_slots = config.registration.budget.cpu_slots;
        let endpoint = connect_endpoint(key, config.registration.peer.address).await?;
        let allow = Arc::new(
            config
                .administrators
                .iter()
                .copied()
                .collect::<HashSet<_>>(),
        );
        let (store, reclamation) = blobs::open_managed(&home.join("blobs")).await?;
        let agent = Agent::start_managed(
            allow.clone(),
            hello,
            AgentConfig {
                home: backend,
                supervisor: config.supervisor.clone(),
                store: store.clone(),
            },
        )?;
        let artifact_access =
            Arc::new(std::sync::RwLock::new(super::artifact_io::Access::default()));
        let vm_access = Arc::new(std::sync::RwLock::new(inventory.vm.clone()));
        let router = Router::builder(endpoint.clone())
            .accept(
                super::artifact_io::ALPN,
                super::artifact_io::Handler {
                    cluster: config.cluster.clone(),
                    access: artifact_access.clone(),
                    slots: Arc::new(tokio::sync::Semaphore::new(4)),
                    authorization: super::artifact_io::Authorization::new(
                        endpoint.clone(),
                        config.cluster.clone(),
                        config.controllers.clone(),
                        &home.join("worker.json"),
                    )?,
                },
            )
            .accept(
                DATA_ALPN,
                DataHandler {
                    vm_access: vm_access.clone(),
                    home: home.into(),
                    cluster: config.cluster.clone(),
                    allow: allow.clone(),
                    agent: agent.clone(),
                    store: store.clone(),
                },
            )
            .accept(iroh_blobs::ALPN, blobs::protocol(&store, allow))
            .spawn();
        let mut client =
            Client::from_endpoint(endpoint, config.cluster.clone(), config.controllers.clone())?;
        client.use_cache(super::discovery::incarnation_cache(
            &home.join("worker.json"),
            &config.cluster,
        ))?;
        Ok(Self {
            config,
            reclamation,
            home: home.into(),
            client,
            router,
            store,
            journal,
            agent,
            artifact_access,
            storage_view: None,
            vm_adapters: BTreeMap::new(),
            vm_access,
        })
    }

    /// Network failure does not prevent local supervision. Accepted intents remain authoritative.
    pub async fn tick(&mut self) -> Result<()> {
        let r = self.config.registration.clone();
        let poll = self
            .client
            .call(
                Message::WorkerRead {
                    worker: r.id.clone(),
                    incarnation: r.incarnation.clone(),
                },
                Duration::from_secs(2),
            )
            .await;
        let online = match poll {
            Ok(Answer::WorkerRead(Ok(view))) => {
                if view.recovery_required || self.journal.read()?.recovery_inventory.is_some() {
                    self.journal.change(|s| {
                        if s.recovery_inventory.is_none() {
                            let mut assignments = s.assignments.clone();
                            for (id, a) in &mut assignments {
                                if let Some(observation) = s.observations.get(id) {
                                    a.observed = Some(observation.clone());
                                }
                            }
                            s.recovery_inventory = Some(super::recovery::Inventory {
                                checkpoints: s
                                    .checkpoints
                                    .iter()
                                    .filter(|(_, c)| c.worker == s.registration.id)
                                    .map(|(id, c)| (id.clone(), c.clone()))
                                    .collect(),
                                held: s.held.clone(),
                                registration: s.registration.clone(),
                                sequence: s.sequence,
                                assignments,
                            });
                        }
                        Ok(())
                    })?;
                    let inventory = self
                        .journal
                        .read()?
                        .recovery_inventory
                        .expect("prepared inventory");
                    let reply = self
                        .client
                        .write(
                            Request {
                                origin: self.config.cluster.clone(),
                                id: format!(
                                    "recovery-inventory-{}-{}",
                                    self.config.cluster, r.incarnation
                                ),
                                mutation: Mutation::RecoverWorker(inventory),
                            },
                            Duration::from_secs(2),
                        )
                        .await?;
                    ensure!(
                        reply == Reply::Recorded,
                        "worker recovery rejected: {reply:?}"
                    );
                    if view.recovery_required {
                        return Ok(());
                    }
                }
                self.journal.accept(view.clone())?;
                self.storage_view = Some(view);
                true
            }
            Ok(other) => bail!("unexpected worker poll: {other:?}"),
            Err(e) => {
                tracing::debug!(error=%e, "worker could not refresh committed intent");
                false
            }
        };
        let journal = self.journal.read()?;
        *self.vm_access.write().expect("VM access") = journal.vm.clone();
        // A rebound worker cannot execute stale intent while its recovery inventory is pending.
        if journal.recovery_inventory.is_some() {
            return Ok(());
        }
        for a in journal.assignments.values() {
            if journal.held.contains(&a.id) || (!journal.starts_allowed && a.desired.running()) {
                // Detached supervisors continue; this agent cannot initiate new Running effects
                // until it has durably observed the administrator's recovery resume.
                continue;
            }
            let observation = match self.reconcile(a).await {
                Ok(o) => o,
                Err(e) => Observation {
                    phase: a.workload.pending(),
                    detail: Some(format!("{e:#}").chars().take(1000).collect()),
                },
            };
            if let Err(e) = self.persist_checkpoints(a).await {
                tracing::warn!(attempt=%a.id,error=%e,"checkpoint remains unavailable");
            }
            self.journal.observe(a, observation)?;
        }
        if let Err(e) = self.reconcile_vm(online).await {
            tracing::warn!(error=%e,"VM management will retry");
            instances::write(&self.home.join("managed-vm/error.json"), &format!("{e:#}"))?;
        } else {
            let _ = std::fs::remove_file(self.home.join("managed-vm/error.json"));
        }
        if online {
            if let Err(e) = self.reconcile_history().await {
                tracing::warn!(error=%e, "history archival will retry");
            }
            if let Err(e) = self.reconcile_artifacts().await {
                tracing::warn!(error=%e, "artifact lifecycle will retry");
            }
            let saved = self.journal.read()?;
            let mut pending: Vec<_> = saved
                .checkpoints
                .values()
                .filter(|c| !saved.known_checkpoints.contains(&c.id))
                .collect();
            pending.sort_by_key(|c| {
                (
                    c.content.captured_at,
                    c.content.source.clone(),
                    c.id.clone(),
                )
            });
            for checkpoint in pending {
                let reply = self
                    .client
                    .write(
                        Request {
                            origin: self.config.cluster.clone(),
                            id: format!("checkpoint-{}", checkpoint.id),
                            mutation: Mutation::CheckpointReady(super::checkpoint::Ready {
                                incarnation: r.incarnation.clone(),
                                checkpoint: checkpoint.clone(),
                            }),
                        },
                        Duration::from_secs(2),
                    )
                    .await?;
                ensure!(reply == Reply::Recorded, "checkpoint rejected: {reply:?}");
                self.journal.change(|s| {
                    s.known_checkpoints.insert(checkpoint.id.clone());
                    Ok(())
                })?;
            }
            for report in self.journal.read()?.outbox.values().take(64) {
                let reply = self
                    .client
                    .write(
                        Request {
                            origin: self.config.cluster.clone(),
                            id: format!("observation-{}-{}", report.incarnation, report.sequence),
                            mutation: Mutation::WorkerReport(report.clone()),
                        },
                        Duration::from_secs(2),
                    )
                    .await?;
                ensure!(
                    reply == Reply::Recorded,
                    "worker observation rejected: {reply:?}"
                );
            }
        }
        Ok(())
    }

    async fn persist_checkpoints(&self, a: &Assignment) -> Result<()> {
        let Workload::Job { spec, .. } = &a.workload else {
            return Ok(());
        };
        if spec.checkpoint.is_none() {
            return Ok(());
        }
        let dir = jobs::jobs_dir(&self.home.join("backend")).join(&a.execution);
        for content in vm_job::captured_checkpoints(&dir)? {
            let checkpoint = super::checkpoint::Checkpoint::new(a, content)?;
            checkpoint.validate(a)?;
            if self
                .journal
                .read()?
                .checkpoints
                .contains_key(&checkpoint.id)
            {
                continue;
            }
            let root = vm_job::checkpoint_files_dir(&dir, &checkpoint.content.id)?;
            for file in &checkpoint.content.files {
                checkpoint.content.verify_file(&root, file)?;
                blobs::import_named(
                    &self.store,
                    &root.join(&file.path),
                    file.hash,
                    format!("checkpoint/{}/{}", checkpoint.id, file.path),
                )
                .await?;
            }
            self.store.sync_db().await?;
            self.journal.change(|s| {
                s.checkpoints
                    .insert(checkpoint.id.clone(), checkpoint.clone());
                Ok(())
            })?;
        }
        Ok(())
    }

    async fn reconcile(&self, a: &Assignment) -> Result<Observation> {
        let home = self.home.join("backend");
        let owner = self.config.registration.peer.identity;
        let id = &a.execution;
        let journal = self.journal.read()?;
        if let Some((_, old)) = journal.observations.get(&a.id) {
            let finished = old.phase.terminal();
            if finished {
                if old.phase == WorkPhase::Job(AttemptPhase::Suspended)
                    && a.desired == Desired::Job(JobDesired::Cancelled)
                {
                    return Ok(Observation {
                        phase: WorkPhase::Job(AttemptPhase::Cancelled),
                        detail: None,
                    });
                }
                return Ok(old.clone());
            }
        }
        let attempted = journal.attempted.contains(&a.id);
        let phase = match &a.workload {
            Workload::Job { spec, gpu, .. } => {
                let dir = jobs::jobs_dir(&home).join(id);
                if attempted
                    && !dir.join("job.json").exists()
                    && !(a.desired == Desired::Job(JobDesired::Cancelled)
                        && spec.vm.is_some()
                        && instances::directory(&home, &vm_job::instance_id(id))?
                            .join("desired.json")
                            .exists())
                {
                    return Ok(Observation { phase: WorkPhase::Job(AttemptPhase::Uncertain), detail: Some("backend records missing after an execution attempt; refusing to recreate work".into()) });
                }
                if a.desired == Desired::Job(JobDesired::Suspended) {
                    let checkpoint = a
                        .suspend_checkpoint
                        .as_ref()
                        .context("suspension has no checkpoint")?;
                    let c = journal
                        .checkpoints
                        .get(checkpoint)
                        .context("verified checkpoint missing from worker journal")?;
                    ensure!(
                        c.attempt.as_ref() == a.id,
                        "suspension checkpoint belongs to another attempt"
                    );
                    if !dir.join("exit.json").exists() && !dir.join("suspend.json").exists() {
                        let root = vm_job::checkpoint_files_dir(&dir, &c.content.id)?;
                        for file in &c.content.files {
                            c.content.verify_file(&root, file)?;
                        }
                        // Cooperative suspension may drain declared metrics before stopping. Keep
                        // the reservation and a durable hard deadline; cancellation still stops now.
                        if !spec.metric_files.is_empty()
                            && !dir.join("history-stop-deadline.json").exists()
                        {
                            instances::write(
                                &dir.join("history-stop-deadline.json"),
                                &jobs::now().saturating_add(35),
                            )?;
                        }
                        instances::write(&dir.join("suspend.json"), checkpoint)?;
                    }
                    if !dir.join("exit.json").exists()
                        && (spec.metric_files.is_empty()
                            || jobs::read_json::<u64>(&dir.join("history-stop-deadline.json"))?
                                .is_none_or(|d| jobs::now() >= d))
                    {
                        instances::release_job(&home, id, instances::Desired::Stopped)?;
                    }
                } else if a.desired == Desired::Job(JobDesired::Cancelled) {
                    if spec.vm.is_some() {
                        self.agent
                            .handle(
                                owner,
                                proto::Request::CancelVmJob {
                                    job: id.clone(),
                                    spec: spec.clone(),
                                },
                            )
                            .await?;
                    } else if !dir.join("job.json").exists() {
                        return Ok(Observation {
                            phase: WorkPhase::Job(AttemptPhase::Cancelled),
                            detail: None,
                        });
                    } else {
                        instances::write(&dir.join("cancel"), &jobs::now())?;
                    }
                } else {
                    if !attempted {
                        self.restore_inputs(a).await?;
                        self.restore_checkpoint(a).await?;
                        let hashes = spec.inputs.values().map(|i| i.hash).collect();
                        if let proto::Response::Missing(missing) = self
                            .agent
                            .handle(owner, proto::Request::Missing { hashes })
                            .await?
                        {
                            ensure!(missing.is_empty(), "waiting for verified input blobs");
                        }
                        self.journal.change(|s| {
                            s.attempted.insert(a.id.clone());
                            Ok(())
                        })?;
                    }
                    let submitted = self
                        .agent
                        .handle(
                            owner,
                            proto::Request::SubmitAllocated {
                                spec: spec.clone(),
                                id: Some(id.clone()),
                                gpu: *gpu,
                            },
                        )
                        .await;
                    if submitted.is_err()
                        && !dir.join("job.json").exists()
                        && !instances::directory(&home, &vm_job::instance_id(id))?
                            .join("desired.json")
                            .exists()
                    {
                        // A returned pre-publication error proves no supervisor was launched.
                        // A process crash before this point leaves the attempt uncertain instead.
                        self.journal.change(|s| {
                            s.attempted.remove(&a.id);
                            Ok(())
                        })?;
                    }
                    submitted?;
                }
                if spec.vm.is_some() {
                    vm_job::ensure(&dir, &self.config.supervisor)?;
                } else if !dir.join("exit.json").exists()
                    && let Some(lock) = instances::try_lock(&dir.join("native-supervisor.lock"))?
                {
                    if dir.join("launch-attempt.json").exists() {
                        return Ok(Observation { phase: WorkPhase::Job(AttemptPhase::Uncertain), detail: Some("supervisor ended without a durable exit; launch is not retried and reservation is retained".into()) });
                    }
                    drop(lock);
                    vm_job::launch(&dir, &self.config.supervisor)?;
                }
                match jobs::info(&dir)?.state {
                    jobs::State::Starting => WorkPhase::Job(AttemptPhase::Pending),
                    jobs::State::Running | jobs::State::Paused => {
                        WorkPhase::Job(AttemptPhase::Running)
                    }
                    jobs::State::Succeeded => WorkPhase::Job(AttemptPhase::Succeeded),
                    jobs::State::Cancelled => WorkPhase::Job(AttemptPhase::Cancelled),
                    jobs::State::Failed | jobs::State::TimedOut => {
                        WorkPhase::Job(AttemptPhase::Failed)
                    }
                    jobs::State::Suspended => {
                        WorkPhase::Job(if a.desired == Desired::Job(JobDesired::Cancelled) {
                            AttemptPhase::Cancelled
                        } else {
                            AttemptPhase::Suspended
                        })
                    }
                    jobs::State::Lost => WorkPhase::Job(AttemptPhase::Uncertain),
                }
            }
            Workload::Instance { spec } => {
                let dir = instances::directory(&home, id)?;
                if attempted && !dir.join("desired.json").exists() {
                    return Ok(Observation {
                        phase: WorkPhase::Instance(InstancePhase::Uncertain),
                        detail: Some(
                            "VM records missing after a creation attempt; refusing to recreate it"
                                .into(),
                        ),
                    });
                }
                if a.desired == Desired::Instance(InstanceDesired::Deleted)
                    && !dir.join("desired.json").exists()
                {
                    return Ok(Observation {
                        phase: WorkPhase::Instance(InstancePhase::Deleted),
                        detail: None,
                    });
                }
                if !attempted {
                    self.journal.change(|s| {
                        s.attempted.insert(a.id.clone());
                        Ok(())
                    })?;
                }
                let created = self.agent.reserve_instance(owner, id, spec.clone()).await;
                if created.is_err() && !dir.join("desired.json").exists() {
                    self.journal.change(|s| {
                        s.attempted.remove(&a.id);
                        Ok(())
                    })?;
                }
                created?;
                let info = instances::info(&home, id)?;
                if a.desired == Desired::Instance(InstanceDesired::Running)
                    && info.record.desired == instances::Desired::Stopped
                    && info.observed.state == instances::State::Creating
                {
                    // Persistent VMs provision in Stopped before a start can pass admission.
                    // Only the authorized reconciliation loop may launch that provisioning controller.
                    instances::ensure_controller(&home, id, &self.config.supervisor)?;
                    return Ok(Observation {
                        phase: WorkPhase::Instance(InstancePhase::Pending),
                        detail: None,
                    });
                }
                let desired = match a.desired {
                    Desired::Instance(InstanceDesired::Running) => instances::Desired::Running,
                    Desired::Instance(InstanceDesired::Stopped) => instances::Desired::Stopped,
                    Desired::Instance(InstanceDesired::Deleted)
                        if info.record.desired == instances::Desired::Deleted
                            || info.observed.state == instances::State::Stopped =>
                    {
                        instances::Desired::Deleted
                    }
                    Desired::Instance(InstanceDesired::Deleted) => instances::Desired::Stopped,
                    Desired::Job(_) => bail!("invalid instance intent"),
                };
                if info.record.desired != desired {
                    self.agent
                        .handle(
                            owner,
                            proto::Request::InstanceSet {
                                id: id.clone(),
                                desired,
                                revision: info.record.revision,
                            },
                        )
                        .await?;
                } else {
                    instances::ensure_controller(&home, id, &self.config.supervisor)?;
                }
                let info = instances::info(&home, id)?;
                if info.record.revision != info.observed.revision {
                    WorkPhase::Instance(InstancePhase::Pending)
                } else {
                    match info.observed.state {
                        instances::State::Running => WorkPhase::Instance(InstancePhase::Running),
                        instances::State::Stopped => WorkPhase::Instance(InstancePhase::Stopped),
                        instances::State::Deleted => WorkPhase::Instance(InstancePhase::Deleted),
                        instances::State::Unknown | instances::State::Failed => {
                            return Ok(Observation {
                                phase: WorkPhase::Instance(InstancePhase::Uncertain),
                                detail: info.observed.error,
                            });
                        }
                        _ => WorkPhase::Instance(InstancePhase::Pending),
                    }
                }
            }
        };
        Ok(Observation {
            phase,
            detail: if matches!(a.workload, Workload::Job { .. }) {
                jobs::read_json::<String>(
                    &home.join("jobs").join(id).join("checkpoint-error.json"),
                )?
                .map(|error| format!("checkpoint capture: {error}"))
            } else {
                None
            },
        })
    }

    pub async fn shutdown(mut self) -> Result<()> {
        for (_, agent) in std::mem::take(&mut self.vm_adapters) {
            agent.shutdown().await;
        }
        // Supervisors are deliberately detached; stopping a worker never cancels its assignments.
        self.router.shutdown().await?;
        Ok(())
    }
}

pub async fn push_inputs(
    store: &iroh_blobs::api::Store,
    endpoint: &Endpoint,
    cluster: &str,
    worker: &Peer,
    spec: &jobs::JobSpec,
) -> Result<()> {
    let hashes: Vec<_> = spec.inputs.values().map(|i| i.hash).collect();
    let reserved = data_call(
        endpoint,
        cluster,
        worker,
        DataRequest::ReserveInputs {
            hashes: hashes.clone(),
        },
    )
    .await?;
    ensure!(
        matches!(reserved, proto::Response::Missing(ref missing) if missing.is_empty()),
        "input reservation failed: {reserved:?}"
    );
    let connection = blobs::push(store, endpoint, worker.addr(), &hashes).await?;
    let end = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let reply = data_call(
            endpoint,
            cluster,
            worker,
            DataRequest::Missing {
                hashes: hashes.clone(),
            },
        )
        .await?;
        if matches!(reply, proto::Response::Missing(ref missing) if missing.is_empty()) {
            break;
        }
        ensure!(tokio::time::Instant::now() < end, "input import timed out");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    if let Some(connection) = connection {
        connection.close(0u32.into(), b"inputs verified");
    }
    Ok(())
}

pub async fn fetch_outputs(
    store: &iroh_blobs::api::Store,
    endpoint: &Endpoint,
    cluster: &str,
    worker: &Peer,
    execution: &str,
    destination: &Path,
) -> Result<Vec<jobs::OutputFile>> {
    let proto::Response::Outputs(files) = data_call(
        endpoint,
        cluster,
        worker,
        DataRequest::Outputs {
            execution: execution.into(),
        },
    )
    .await?
    else {
        bail!("unexpected outputs response");
    };
    fetch_files(store, endpoint, worker, &files, destination).await?;
    Ok(files)
}

pub async fn fetch_files(
    store: &iroh_blobs::api::Store,
    endpoint: &Endpoint,
    worker: &Peer,
    files: &[jobs::OutputFile],
    destination: &Path,
) -> Result<()> {
    for file in files {
        jobs::safe_relative(&file.path)?;
    }
    // A fresh directory prevents overwriting prior captures or following pre-existing symlinks.
    std::fs::create_dir(destination).context("output destination must be a new directory")?;
    blobs::fetch(
        store,
        endpoint,
        worker.addr(),
        &files.iter().map(|f| f.hash).collect::<Vec<_>>(),
    )
    .await?;
    for file in files {
        let target = destination.join(&file.path);
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent)?;
        }
        store.blobs().export(file.hash, &target).await?;
        ensure!(
            blobs::hash_file(&target)? == file.hash
                && std::fs::metadata(&target)?.len() == file.size,
            "captured file hash mismatch"
        );
    }
    Ok(())
}
