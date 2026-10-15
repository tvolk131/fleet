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

pub const DATA_ALPN: &[u8] = b"fleet/worker-data/1";
const JOURNAL: TableDefinition<&str, &[u8]> = TableDefinition::new("worker_journal");
const MAX_FRAME: usize = 8 << 20;

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
    starts_allowed: bool,
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
                    old.format == super::FORMAT
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
                    starts_allowed: true,
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
                        && new.revision >= old.revision,
                    "accepted assignment changed or regressed"
                );
                ensure!(
                    new.revision != old.revision || new.desired == old.desired,
                    "same revision changed intent"
                );
            }
            for a in view.assignments {
                ensure!(a.worker == s.registration.id, "wrong assigned worker");
                instances::valid_id(&a.execution)?;
                a.workload.validate(&s.registration)?;
                s.assignments.insert(a.id.clone(), a);
            }
            ensure!(!view.recovery_required, "worker recovery has not committed");
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
                    let request = match frame.request {
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
                    let response = self
                        .agent
                        .handle(peer, request)
                        .await
                        .unwrap_or_else(|e| proto::Response::Error(format!("{e:#}")));
                    if matches!(&response, proto::Response::Missing(hashes) if hashes.is_empty()) {
                        // A staging acknowledgement must survive worker process restart.
                        self.store.sync_db().await?;
                    }
                    let bytes = serde_json::to_vec(&response)?;
                    ensure!(bytes.len() <= MAX_FRAME, "worker data response too large");
                    send.write_all(&bytes).await?;
                    send.finish()?;
                    Ok::<_, anyhow::Error>(())
                })
                .await;
                if let Ok(Err(e)) = result {
                    tracing::debug!(error=%e, "worker data stream ended");
                }
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
    pub config: WorkerConfig,
    pub home: PathBuf,
    pub client: Client,
    pub router: Router,
    pub store: iroh_blobs::api::Store,
    journal: Journal,
    agent: Agent,
}

impl Worker {
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
                    && old.format == super::FORMAT,
                "worker journal/config identity mismatch"
            );
            journal.change(|s| {
                s.cluster = manifest.plan.config.cluster.clone();
                s.starts_allowed = false;
                s.recovery_binding = Some(manifest.clone());
                let mut assignments = s.assignments.clone();
                for (id, a) in &mut assignments {
                    if let Some(observation) = s.observations.get(id) {
                        a.observed = Some(observation.clone());
                    }
                }
                s.recovery_inventory = Some(super::recovery::Inventory {
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
        let store = blobs::open(&home.join("blobs")).await?;
        let agent = Agent::start_managed(
            allow.clone(),
            hello,
            AgentConfig {
                home: backend,
                supervisor: config.supervisor.clone(),
                store: store.clone(),
            },
        )?;
        let router = Router::builder(endpoint.clone())
            .accept(
                DATA_ALPN,
                DataHandler {
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
            home: home.into(),
            client,
            router,
            store,
            journal,
            agent,
        })
    }

    /// Network failure does not prevent local supervision. Accepted intents remain authoritative.
    pub async fn tick(&mut self) -> Result<()> {
        let r = &self.config.registration;
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
                self.journal.accept(view)?;
                true
            }
            Ok(other) => bail!("unexpected worker poll: {other:?}"),
            Err(e) => {
                tracing::debug!(error=%e, "worker could not refresh committed intent");
                false
            }
        };
        let journal = self.journal.read()?;
        // A rebound worker cannot execute stale intent while its recovery inventory is pending.
        if journal.recovery_inventory.is_some() {
            return Ok(());
        }
        for a in journal.assignments.values() {
            if journal.held.contains(&a.id)
                || (!journal.starts_allowed && a.desired == Desired::Running)
            {
                // Detached supervisors continue; this agent cannot initiate new Running effects
                // until it has durably observed the administrator's recovery resume.
                continue;
            }
            let observation = match self.reconcile(a).await {
                Ok(o) => o,
                Err(e) => Observation {
                    phase: WorkPhase::Pending,
                    detail: Some(format!("{e:#}").chars().take(1000).collect()),
                },
            };
            self.journal.observe(a, observation)?;
        }
        if online {
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

    async fn reconcile(&self, a: &Assignment) -> Result<Observation> {
        let home = self.home.join("backend");
        let owner = self.config.registration.peer.identity;
        let id = &a.execution;
        let journal = self.journal.read()?;
        if let Some((_, old)) = journal.observations.get(&a.id) {
            let finished = match a.workload {
                Workload::Job { .. } => matches!(
                    old.phase,
                    WorkPhase::Succeeded | WorkPhase::Failed | WorkPhase::Cancelled
                ),
                Workload::Instance { .. } => old.phase == WorkPhase::Deleted,
            };
            if finished {
                return Ok(old.clone());
            }
        }
        let attempted = journal.attempted.contains(&a.id);
        let phase = match &a.workload {
            Workload::Job { spec, gpu, .. } => {
                let dir = jobs::jobs_dir(&home).join(id);
                if attempted
                    && !dir.join("job.json").exists()
                    && !(a.desired == Desired::Cancelled
                        && spec.vm.is_some()
                        && instances::directory(&home, &vm_job::instance_id(id))?
                            .join("desired.json")
                            .exists())
                {
                    return Ok(Observation { phase: WorkPhase::Uncertain, detail: Some("backend records missing after an execution attempt; refusing to recreate work".into()) });
                }
                if a.desired == Desired::Cancelled {
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
                            phase: WorkPhase::Cancelled,
                            detail: None,
                        });
                    } else {
                        instances::write(&dir.join("cancel"), &jobs::now())?;
                    }
                } else {
                    if !attempted {
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
                        return Ok(Observation { phase: WorkPhase::Uncertain, detail: Some("supervisor ended without a durable exit; launch is not retried and reservation is retained".into()) });
                    }
                    drop(lock);
                    vm_job::launch(&dir, &self.config.supervisor)?;
                }
                match jobs::info(&dir)?.state {
                    jobs::State::Starting => WorkPhase::Pending,
                    jobs::State::Running | jobs::State::Paused => WorkPhase::Running,
                    jobs::State::Succeeded => WorkPhase::Succeeded,
                    jobs::State::Cancelled => WorkPhase::Cancelled,
                    jobs::State::Failed | jobs::State::TimedOut => WorkPhase::Failed,
                    jobs::State::Lost | jobs::State::Suspended => WorkPhase::Uncertain,
                }
            }
            Workload::Instance { spec } => {
                let dir = instances::directory(&home, id)?;
                if attempted && !dir.join("desired.json").exists() {
                    return Ok(Observation {
                        phase: WorkPhase::Uncertain,
                        detail: Some(
                            "VM records missing after a creation attempt; refusing to recreate it"
                                .into(),
                        ),
                    });
                }
                if a.desired == Desired::Deleted && !dir.join("desired.json").exists() {
                    return Ok(Observation {
                        phase: WorkPhase::Deleted,
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
                if a.desired == Desired::Running
                    && info.record.desired == instances::Desired::Stopped
                    && info.observed.state == instances::State::Creating
                {
                    // Persistent VMs provision in Stopped before a start can pass admission.
                    // Only the authorized reconciliation loop may launch that provisioning controller.
                    instances::ensure_controller(&home, id, &self.config.supervisor)?;
                    return Ok(Observation {
                        phase: WorkPhase::Pending,
                        detail: None,
                    });
                }
                let desired = match a.desired {
                    Desired::Running => instances::Desired::Running,
                    Desired::Stopped => instances::Desired::Stopped,
                    Desired::Deleted
                        if info.record.desired == instances::Desired::Deleted
                            || info.observed.state == instances::State::Stopped =>
                    {
                        instances::Desired::Deleted
                    }
                    Desired::Deleted => instances::Desired::Stopped,
                    Desired::Cancelled => bail!("invalid instance intent"),
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
                    WorkPhase::Pending
                } else {
                    match info.observed.state {
                        instances::State::Running => WorkPhase::Running,
                        instances::State::Stopped => WorkPhase::Stopped,
                        instances::State::Deleted => WorkPhase::Deleted,
                        instances::State::Unknown | instances::State::Failed => {
                            return Ok(Observation {
                                phase: WorkPhase::Uncertain,
                                detail: info.observed.error,
                            });
                        }
                        _ => WorkPhase::Pending,
                    }
                }
            }
        };
        Ok(Observation {
            phase,
            detail: None,
        })
    }

    pub async fn shutdown(self) -> Result<()> {
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
    for file in &files {
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
    for file in &files {
        let target = destination.join(&file.path);
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent)?;
        }
        store.blobs().export(file.hash, &target).await?;
        ensure!(
            blobs::hash_file(&target)? == file.hash,
            "captured file hash mismatch"
        );
    }
    Ok(files)
}
