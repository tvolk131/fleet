//! The coordinator's core: the queue (SQLite, `$FLEET_HOME/fleet.db`), placement, a connection to each named machine
//! (its events replayed exactly from the last one seen, its status, keep-awake while work waits), and automatic
//! fetching into each job's destination.
//!
//! `fleet daemon` runs the core and serves it on a Unix socket (`$FLEET_HOME/daemon.sock`), one JSON request and one
//! JSON answer per connection. With no daemon running, the CLI uses the same core in-process ([`Core::direct`]): then
//! a submission must find a machine at once, since nothing waits for one. Only one process uses the coordinator's key
//! at a time either way.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
use iroh::{EndpointAddr, endpoint::Connection};
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{UnixListener, UnixStream},
    sync::Notify,
};

use crate::{
    PathReport,
    coordinator::{Coordinator, FetchReport, JobFile, machines},
    events::EventKind,
    jobs::{Exit, JobInfo, JobSpec, OutputFile, State, now},
    paths,
    proto::{Hello, MAX_LOG_CHUNK, Request, Response, Status},
};

/// Where a job may run.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
pub struct Placement {
    /// A named machine; otherwise any machine that fits.
    #[serde(default)]
    pub machine: Option<String>,
    /// Tags the machine must report, for example `gpu:metal`.
    #[serde(default)]
    pub tags: Vec<String>,
    /// The job needs the machine's GPU to itself.
    #[serde(default)]
    pub gpu: bool,
    /// Only on a machine running on AC power.
    #[serde(default)]
    pub ac_power: bool,
}

/// A queued job's state, as the coordinator tracks it.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum QState {
    Queued,
    /// Placed on a machine; inputs being sent, or the job starting.
    Starting,
    Running,
    /// Frozen on its machine until continued.
    Paused,
    /// Asked to stop at a checkpoint (at once, or at the next one).
    Suspending,
    /// Stopped at a checkpoint; `fleet resume` queues its next segment.
    Suspended,
    Succeeded,
    Failed,
    Cancelled,
    TimedOut,
    Lost,
}

impl QState {
    pub fn finished(self) -> bool {
        matches!(
            self,
            QState::Succeeded
                | QState::Failed
                | QState::Cancelled
                | QState::TimedOut
                | QState::Lost
                | QState::Suspended
        )
    }

    fn from_agent(state: State) -> Self {
        match state {
            State::Starting => QState::Starting,
            State::Running => QState::Running,
            State::Paused => QState::Paused,
            State::Suspended => QState::Suspended,
            State::Succeeded => QState::Succeeded,
            State::Failed => QState::Failed,
            State::Cancelled => QState::Cancelled,
            State::TimedOut => QState::TimedOut,
            State::Lost => QState::Lost,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            QState::Queued => "queued",
            QState::Starting => "starting",
            QState::Running => "running",
            QState::Paused => "paused",
            QState::Succeeded => "succeeded",
            QState::Failed => "failed",
            QState::Cancelled => "cancelled",
            QState::TimedOut => "timed_out",
            QState::Lost => "lost",
            QState::Suspending => "suspending",
            QState::Suspended => "suspended",
        }
    }

    fn parse(s: &str) -> Self {
        match s {
            "starting" => QState::Starting,
            "running" => QState::Running,
            "paused" => QState::Paused,
            "succeeded" => QState::Succeeded,
            "failed" => QState::Failed,
            "cancelled" => QState::Cancelled,
            "timed_out" => QState::TimedOut,
            "lost" => QState::Lost,
            "suspending" => QState::Suspending,
            "suspended" => QState::Suspended,
            _ => QState::Queued,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct QueuedJob {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vm_status: Option<crate::vm_job::Status>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vm: Option<crate::instances::Spec>,
    pub id: String,
    pub name: String,
    pub state: QState,
    pub machine: Option<String>,
    pub placement: Placement,
    pub dest: Option<PathBuf>,
    pub after: Vec<String>,
    pub priority: i64,
    pub submitted_at: u64,
    pub assigned_at: Option<u64>,
    pub finished_at: Option<u64>,
    pub exit: Option<Exit>,
    /// Why it is still queued, or why it failed to start.
    pub error: Option<String>,
    pub fetched_files: u64,
    pub fetched_bytes: u64,
    /// The job's latest progress probe values, and when they arrived.
    pub progress: Option<Vec<crate::probes::ProbeValue>>,
    pub progress_at: Option<u64>,
    /// Which segment of its logical job this is (1 for the original), and the segment it continues.
    pub segment: u32,
    pub parent: Option<String>,
    /// Queue the next segment as soon as this one is suspended: on this machine, or anywhere (`*`).
    pub resume_on: Option<String>,
    /// The job this one is being suspended to make room for.
    pub preempted_for: Option<String>,
    /// The segment that continues this one, once queued.
    pub continued_by: Option<String>,
    /// Whether the job declares how to resume, so it can be suspended and moved.
    pub suspendable: bool,
    /// Where a suspended job stopped: its recorded checkpoint (none if it had not reached one).
    pub checkpoint: Option<crate::suspend::Checkpoint>,
    #[serde(default)]
    pub stopping: Option<crate::jobs::Stopping>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct MachineView {
    pub name: String,
    pub id: String,
    pub online: bool,
    pub paths: Vec<PathReport>,
    pub hello: Option<Hello>,
    pub status: Option<Status>,
    pub last_seen: Option<u64>,
    pub error: Option<String>,
    /// CPU threads fleet's placed jobs claim on this machine (of `hello.cpu_slots`).
    #[serde(default)]
    pub threads_used: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum DaemonRequest {
    InstanceGuest {
        machine: String,
        request: crate::instances::guest::Request,
    },
    InstanceUpload {
        machine: String,
        id: String,
        operation: String,
        revision: u64,
        source: PathBuf,
        path: String,
    },
    InstanceDownload {
        machine: String,
        id: String,
        operation: String,
        dest: PathBuf,
    },
    Instances {
        machine: String,
    },
    InstanceCreate {
        machine: String,
        id: String,
        spec: crate::instances::Spec,
    },
    InstanceSet {
        machine: String,
        id: String,
        desired: crate::instances::Desired,
        revision: u64,
    },
    Ping,
    Machines,
    Submit {
        job: Box<JobFile>,
        #[serde(default)]
        placement: Placement,
        #[serde(default)]
        dest: Option<PathBuf>,
        #[serde(default)]
        after: Vec<String>,
        #[serde(default)]
        priority: i64,
    },
    #[serde(rename = "submit_vm_observed", alias = "submit_vm")]
    SubmitVm {
        key: String,
        job: Box<JobFile>,
        #[serde(default)]
        placement: Placement,
        #[serde(default)]
        dest: Option<PathBuf>,
        #[serde(default)]
        after: Vec<String>,
        #[serde(default)]
        priority: i64,
    },
    SubmitOnce {
        key: String,
        job: Box<JobFile>,
        #[serde(default)]
        placement: Placement,
        #[serde(default)]
        dest: Option<PathBuf>,
        #[serde(default)]
        after: Vec<String>,
        #[serde(default)]
        priority: i64,
    },
    FindSubmission {
        key: String,
    },
    Queue,
    Cancel {
        job: String,
    },
    Pause {
        job: String,
    },
    Continue {
        job: String,
    },
    SuspendPlan {
        job: String,
    },
    Suspend {
        job: String,
        #[serde(default)]
        now: bool,
        #[serde(default)]
        within_seconds: Option<u64>,
    },
    Resume {
        job: String,
        #[serde(default)]
        on: Option<String>,
    },
    Move {
        job: String,
        to: String,
        #[serde(default)]
        now: bool,
    },
    Logs {
        job: String,
        stderr: bool,
        from: u64,
    },
    Outputs {
        job: String,
    },
    Fetch {
        job: String,
        dest: PathBuf,
    },
    MachineJobs {
        machine: String,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DaemonResponse {
    InstanceGuest {
        response: crate::instances::guest::Response,
    },
    InstanceDownloaded {
        path: PathBuf,
        bytes: u64,
        hash: iroh_blobs::Hash,
    },
    Instances {
        instances: Vec<crate::instances::Info>,
        online: bool,
    },
    Instance {
        instance: crate::instances::Info,
    },
    Pong {
        version: String,
        daemon: bool,
    },
    Machines {
        machines: Vec<MachineView>,
    },
    Submitted {
        job: String,
        state: QState,
        machine: Option<String>,
    },
    Submission {
        job: Option<String>,
    },
    Queue {
        jobs: Vec<QueuedJob>,
    },
    Cancelled {
        job: String,
    },
    Done,
    Plan {
        plan: crate::suspend::Plan,
    },
    Resumed {
        job: String,
    },
    Logs {
        data: String,
        next: u64,
        done: bool,
    },
    Outputs {
        files: Vec<OutputFile>,
    },
    Fetched {
        report: FetchReport,
    },
    MachineJobs {
        jobs: Vec<JobInfo>,
    },
    Error {
        message: String,
        #[serde(default)]
        retryable: bool,
    },
}

/// What fleet's own placed jobs use on each machine.
#[derive(Default)]
struct Usage {
    vms: HashMap<String, HashMap<String, crate::instances::Spec>>,
    /// Machines whose GPU is held.
    gpu: HashSet<String>,
    /// CPU threads claimed, by machine.
    threads: HashMap<String, u32>,
}

#[derive(Clone)]
struct Snap {
    online: bool,
    hello: Option<Hello>,
    status: Option<Status>,
}

#[derive(Default)]
struct Live {
    connection: Option<Connection>,
    hello: Option<Hello>,
    status: Option<Status>,
    last_seen: Option<u64>,
    error: Option<String>,
}

pub struct Core {
    pub home: PathBuf,
    pub coordinator: Arc<Coordinator>,
    db: Mutex<rusqlite::Connection>,
    live: Mutex<HashMap<String, Live>>,
    /// True when running as the daemon (machine tasks keep connections; the queue waits for machines).
    daemon: bool,
    wake: Notify,
    fetching: Mutex<HashMap<String, bool>>,
}

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY, name TEXT NOT NULL, spec TEXT NOT NULL, placement TEXT NOT NULL, dest TEXT,
    after TEXT NOT NULL, priority INTEGER NOT NULL, state TEXT NOT NULL, machine TEXT, error TEXT,
    submitted_at INTEGER NOT NULL, assigned_at INTEGER, finished_at INTEGER, exit TEXT,
    fetched_files INTEGER NOT NULL DEFAULT 0, fetched_bytes INTEGER NOT NULL DEFAULT 0,
    next_try_at INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS submissions (key TEXT PRIMARY KEY, fingerprint TEXT NOT NULL, job TEXT NOT NULL UNIQUE REFERENCES jobs(id));
CREATE TABLE IF NOT EXISTS cursors (machine TEXT PRIMARY KEY, seq INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS instances (machine TEXT NOT NULL, id TEXT NOT NULL, info TEXT NOT NULL,
    PRIMARY KEY(machine, id));
-- Local files fetched outputs were written into, with the content written and the job lineage (first segment) that
-- wrote them: a fetch may replace only a file still as its own lineage wrote it.
CREATE TABLE IF NOT EXISTS written (path TEXT PRIMARY KEY, hash TEXT NOT NULL, root TEXT NOT NULL);
";

/// How long to wait for a machine when connecting or asking.
const PATIENCE: Duration = Duration::from_secs(20);

impl Core {
    fn open(home: &Path, coordinator: Coordinator, daemon: bool) -> Result<Arc<Self>> {
        std::fs::create_dir_all(home)?;
        let db = rusqlite::Connection::open(home.join("fleet.db"))?;
        db.execute_batch(SCHEMA)?;
        // Columns added after the first release of the schema (adding one that exists fails harmlessly).
        for column in [
            "vm_status TEXT",
            "stopping TEXT",
            "progress TEXT",
            "progress_at INTEGER",
            "segment INTEGER NOT NULL DEFAULT 1",
            "parent TEXT",
            "checkpoint TEXT",
            "source TEXT",
            "resume_on TEXT",
            "preempted_for TEXT",
            "continued_by TEXT",
        ] {
            let _ = db.execute(&format!("ALTER TABLE jobs ADD COLUMN {column}"), []);
        }
        db.busy_timeout(Duration::from_secs(5))?;
        Ok(Arc::new(Self {
            home: home.to_path_buf(),
            coordinator: Arc::new(coordinator),
            db: Mutex::new(db),
            live: Mutex::default(),
            daemon,
            wake: Notify::new(),
            fetching: Mutex::default(),
        }))
    }

    /// The core for a CLI invocation with no daemon running.
    pub fn direct(home: &Path, coordinator: Coordinator) -> Result<Arc<Self>> {
        Self::open(home, coordinator, false)
    }

    /// Starts the daemon's tasks (one per named machine, and the scheduler) and serves the socket until it fails.
    pub async fn run_daemon(home: &Path, coordinator: Coordinator) -> Result<()> {
        let core = Self::open(home, coordinator, true)?;
        core.clone().start_tasks();
        let socket = socket_path(home);
        let result = tokio::select! {
            r = serve(core.clone(), &socket) => r,
            _ = crate::shutdown_signal() => {
                tracing::info!("fleet daemon stopping");
                Ok(())
            }
        };
        let _ = std::fs::remove_file(&socket);
        core.coordinator.endpoint.close().await;
        core.coordinator.store.shutdown().await.ok();
        result
    }

    /// The machine and scheduler tasks, for a daemon (also used by tests).
    pub fn start_tasks(self: Arc<Self>) {
        let mut started = HashSet::new();
        let core = self.clone();
        tokio::spawn(async move {
            loop {
                // Machines added while the daemon runs are picked up within a few seconds.
                if let Ok(list) = core.machines() {
                    for (name, id) in list {
                        if started.insert(name.clone()) {
                            tokio::spawn(core.clone().run_machine(name, id));
                        }
                    }
                }
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        });
        tokio::spawn(self.schedule());
    }

    /// Creates a daemon core for tests, without the socket.
    pub fn for_tests(home: &Path, coordinator: Coordinator) -> Result<Arc<Self>> {
        Self::open(home, coordinator, true)
    }

    fn machines(&self) -> Result<Vec<(String, EndpointAddr)>> {
        machines::read_addrs(&self.home.join("machines"))
    }

    fn machine_id(&self, name: &str) -> Result<EndpointAddr> {
        self.machines()?
            .into_iter()
            .find(|(n, _)| n == name)
            .map(|(_, id)| id)
            .ok_or_else(|| anyhow!("no machine called {name} (add it with `fleet machine add`)"))
    }

    pub async fn handle(self: &Arc<Self>, request: DaemonRequest) -> DaemonResponse {
        match self.handle_inner(request).await {
            Ok(response) => response,
            Err(e) => DaemonResponse::Error {
                message: format!("{e:#}"),
                retryable: e.is::<crate::proto::TransportError>(),
            },
        }
    }

    async fn handle_inner(self: &Arc<Self>, request: DaemonRequest) -> Result<DaemonResponse> {
        Ok(match request {
            DaemonRequest::InstanceGuest { machine, request } => {
                let connection = self.connection(&machine).await?;
                let Response::InstanceGuest(response) = self
                    .coordinator
                    .ask(&connection, &Request::InstanceGuest(request))
                    .await?
                else {
                    bail!("unexpected guest operation response");
                };
                DaemonResponse::InstanceGuest { response }
            }
            DaemonRequest::InstanceUpload {
                machine,
                id,
                operation,
                revision,
                source,
                path,
            } => {
                let agent = self.machine_id(&machine)?;
                let response = self
                    .coordinator
                    .instance_upload(agent, id, operation, revision, &source, path)
                    .await?;
                DaemonResponse::InstanceGuest { response }
            }
            DaemonRequest::InstanceDownload {
                machine,
                id,
                operation,
                dest,
            } => {
                let agent = self.machine_id(&machine)?;
                let (bytes, hash) = self
                    .coordinator
                    .instance_download(agent, id, operation, &dest)
                    .await?;
                DaemonResponse::InstanceDownloaded {
                    path: dest,
                    bytes,
                    hash,
                }
            }
            DaemonRequest::Instances { machine } => {
                let result = async {
                    let connection = self.connection(&machine).await?;
                    match self
                        .coordinator
                        .ask(&connection, &Request::Instances)
                        .await?
                    {
                        Response::Instances(instances) => Ok::<_, anyhow::Error>(instances),
                        _ => bail!("unexpected response to Instances"),
                    }
                }
                .await;
                match result {
                    Ok(instances) => {
                        for instance in &instances {
                            self.remember_instance(&machine, instance)?;
                        }
                        DaemonResponse::Instances {
                            instances,
                            online: true,
                        }
                    }
                    Err(error) => {
                        let instances = self.cached_instances(&machine)?;
                        if instances.is_empty() {
                            return Err(error);
                        }
                        DaemonResponse::Instances {
                            instances,
                            online: false,
                        }
                    }
                }
            }
            DaemonRequest::InstanceCreate { machine, id, spec } => {
                let connection = self.connection(&machine).await?;
                let Response::Instance(instance) = self
                    .coordinator
                    .ask(&connection, &Request::InstanceCreate { id, spec })
                    .await?
                else {
                    bail!("unexpected response to InstanceCreate");
                };
                self.remember_instance(&machine, &instance)?;
                DaemonResponse::Instance { instance }
            }
            DaemonRequest::InstanceSet {
                machine,
                id,
                desired,
                revision,
            } => {
                if desired == crate::instances::Desired::Running {
                    let usage = self.usage()?;
                    if usage.gpu.contains(&machine) {
                        bail!("GPU is reserved by a native job on {machine}");
                    }
                }
                let connection = self.connection(&machine).await?;
                let Response::Instance(instance) = self
                    .coordinator
                    .ask(
                        &connection,
                        &Request::InstanceSet {
                            id,
                            desired,
                            revision,
                        },
                    )
                    .await?
                else {
                    bail!("unexpected response to InstanceSet");
                };
                self.remember_instance(&machine, &instance)?;
                self.probe(&machine).await;
                self.wake.notify_one();
                DaemonResponse::Instance { instance }
            }
            DaemonRequest::Ping => DaemonResponse::Pong {
                version: env!("CARGO_PKG_VERSION").into(),
                daemon: self.daemon,
            },
            DaemonRequest::Machines => DaemonResponse::Machines {
                machines: self.machine_views().await?,
            },
            DaemonRequest::Submit {
                job,
                placement,
                dest,
                after,
                priority,
            } => {
                let id = self
                    .enqueue(&job, placement, dest, after, priority, None)
                    .await?;
                if !self.daemon {
                    // Nothing waits for a machine without a daemon: place it now or not at all.
                    self.place_one(&id, None).await?;
                } else {
                    self.wake.notify_one();
                }
                let job = self.job(&id)?.context("the job vanished")?;
                DaemonResponse::Submitted {
                    job: id,
                    state: job.state,
                    machine: job.machine,
                }
            }
            DaemonRequest::FindSubmission { key } => {
                let job = self
                    .db
                    .lock()
                    .expect("db")
                    .query_row("SELECT job FROM submissions WHERE key = ?1", [&key], |r| {
                        r.get(0)
                    })
                    .optional()?;
                DaemonResponse::Submission { job }
            }
            DaemonRequest::SubmitVm {
                key,
                job,
                placement,
                dest,
                after,
                priority,
            }
            | DaemonRequest::SubmitOnce {
                key,
                job,
                placement,
                dest,
                after,
                priority,
            } => {
                if key.trim().is_empty() || key.len() > 256 {
                    bail!("submission key must be 1–256 bytes");
                }
                let id = self
                    .enqueue(&job, placement, dest, after, priority, Some(&key))
                    .await?;
                if !self.daemon {
                    self.place_one(&id, None).await?;
                } else {
                    self.wake.notify_one();
                }
                let record = self.job(&id)?.context("the job vanished")?;
                DaemonResponse::Submitted {
                    job: id,
                    state: record.state,
                    machine: record.machine,
                }
            }
            DaemonRequest::Queue => {
                if !self.daemon {
                    // Nothing tracks the machines without a daemon: ask them about the unfinished jobs now.
                    self.refresh_direct().await;
                }
                DaemonResponse::Queue {
                    jobs: self.queue()?,
                }
            }
            DaemonRequest::Cancel { job } => {
                let record = self
                    .job(&job)?
                    .ok_or_else(|| anyhow!("no job {job} in the queue"))?;
                match (record.state, &record.machine) {
                    (QState::Queued, _) => self.set_state(&job, QState::Cancelled, None)?,
                    (s, Some(machine)) if !s.finished() => {
                        let connection = self.connection(machine).await?;
                        let request = if record.vm.is_some() {
                            let spec: String = self.db.lock().expect("db").query_row(
                                "SELECT spec FROM jobs WHERE id=?1",
                                [&job],
                                |r| r.get(0),
                            )?;
                            Request::CancelVmJob {
                                job: job.clone(),
                                spec: Box::new(serde_json::from_str(&spec)?),
                            }
                        } else {
                            Request::Cancel { job: job.clone() }
                        };
                        self.coordinator.ask(&connection, &request).await?;
                    }
                    (s, _) => bail!("job {job} is already {}", s.as_str()),
                }
                DaemonResponse::Cancelled { job }
            }
            DaemonRequest::Pause { job } => self.pause(&job, true).await?,
            DaemonRequest::Continue { job } => self.pause(&job, false).await?,
            DaemonRequest::SuspendPlan { job } => DaemonResponse::Plan {
                plan: self.plan(&job).await?,
            },
            DaemonRequest::Suspend {
                job,
                now,
                within_seconds,
            } => {
                self.suspend(&job, now, within_seconds, None).await?;
                DaemonResponse::Done
            }
            DaemonRequest::Move { job, to, now } => {
                self.machine_id(&to)?;
                self.suspend(&job, now, None, Some(&to)).await?;
                DaemonResponse::Done
            }
            DaemonRequest::Resume { job, on } => {
                if let Some(m) = &on {
                    self.machine_id(m)?;
                }
                let next = self.continuation(&job, on.as_deref()).await?;
                self.wake.notify_one();
                DaemonResponse::Resumed { job: next }
            }
            DaemonRequest::Logs { job, stderr, from } => {
                let machine = self.job_machine(&job)?;
                let connection = self.connection(&machine).await?;
                let request = Request::Logs {
                    job,
                    stderr,
                    from,
                    max: MAX_LOG_CHUNK,
                };
                let Response::Logs { data, next, done } =
                    self.coordinator.ask(&connection, &request).await?
                else {
                    bail!("unexpected answer to Logs");
                };
                DaemonResponse::Logs { data, next, done }
            }
            DaemonRequest::Outputs { job } => {
                let machine = self.job_machine(&job)?;
                let connection = self.connection(&machine).await?;
                DaemonResponse::Outputs {
                    files: self.coordinator.outputs(&connection, &job).await?,
                }
            }
            DaemonRequest::Fetch { job, dest } => {
                let machine = self.job_machine(&job)?;
                let report = self
                    .fetch_outputs(&job, &machine, &std::path::absolute(&dest)?)
                    .await?;
                DaemonResponse::Fetched { report }
            }
            DaemonRequest::MachineJobs { machine } => {
                let connection = self.connection(&machine).await?;
                DaemonResponse::MachineJobs {
                    jobs: self.coordinator.jobs(&connection).await?,
                }
            }
        })
    }

    fn remember_instance(&self, machine: &str, instance: &crate::instances::Info) -> Result<()> {
        self.db.lock().expect("db").execute(
            "INSERT INTO instances(machine,id,info) VALUES(?1,?2,?3) ON CONFLICT(machine,id) DO UPDATE SET info=?3",
            params![machine, instance.record.id, serde_json::to_string(instance)?])?;
        Ok(())
    }

    fn cached_instances(&self, machine: &str) -> Result<Vec<crate::instances::Info>> {
        let db = self.db.lock().expect("db");
        let mut statement =
            db.prepare("SELECT info FROM instances WHERE machine=?1 ORDER BY id")?;
        let rows = statement.query_map([machine], |r| r.get::<_, String>(0))?;
        rows.map(|r| Ok(serde_json::from_str(&r?)?)).collect()
    }

    /// Pauses (freezes) or continues a job on its machine.
    async fn pause(&self, job: &str, pause: bool) -> Result<DaemonResponse> {
        let machine = self.job_machine(job)?;
        let connection = self.connection(&machine).await?;
        let job = job.to_string();
        let request = if pause {
            Request::Pause { job: job.clone() }
        } else {
            Request::Continue { job: job.clone() }
        };
        self.coordinator.ask(&connection, &request).await?;
        self.set_state(
            &job,
            if pause {
                QState::Paused
            } else {
                QState::Running
            },
            None,
        )?;
        // A paused GPU job may have freed its GPU for something queued.
        self.wake.notify_one();
        Ok(DaemonResponse::Done)
    }

    /// What suspending a job would lose now, from its machine.
    async fn plan(&self, job: &str) -> Result<crate::suspend::Plan> {
        let machine = self.job_machine(job)?;
        let connection = self.connection(&machine).await?;
        match self
            .coordinator
            .ask(
                &connection,
                &Request::SuspendPlan {
                    job: job.to_string(),
                },
            )
            .await?
        {
            Response::Plan(plan) => Ok(plan),
            other => bail!("unexpected answer to SuspendPlan: {other:?}"),
        }
    }

    /// Asks a job's machine to suspend it, at once or at its next checkpoint. `resume_on` queues its next segment as
    /// soon as it is suspended: on that machine, or anywhere its placement allows (`*`).
    async fn suspend(
        &self,
        job: &str,
        now: bool,
        within: Option<u64>,
        resume_on: Option<&str>,
    ) -> Result<()> {
        let record = self
            .job(job)?
            .ok_or_else(|| anyhow!("no job {job} in the queue"))?;
        if !matches!(record.state, QState::Running | QState::Paused) {
            bail!("job {job} is {}, not running", record.state.as_str());
        }
        let machine = record.machine.context("the job has not been placed")?;
        let connection = self.connection(&machine).await?;
        let request = Request::Suspend {
            job: job.to_string(),
            now,
            within_seconds: within,
        };
        self.coordinator.ask(&connection, &request).await?;
        self.db.lock().expect("db").execute(
            "UPDATE jobs SET state = 'suspending', resume_on = ?2 WHERE id = ?1",
            params![job, resume_on],
        )?;
        Ok(())
    }

    /// Stores a suspended job's recorded checkpoint, and queues its next segment if one was asked for.
    async fn record_suspension(self: &Arc<Self>, machine: &str, job: &str) -> Result<()> {
        let connection = self.connection(machine).await?;
        let Response::Checkpoint(recorded) = self
            .coordinator
            .ask(
                &connection,
                &Request::Checkpoint {
                    job: job.to_string(),
                },
            )
            .await?
        else {
            bail!("unexpected answer to Checkpoint");
        };
        let resume_on: Option<String> = {
            let db = self.db.lock().expect("db");
            db.execute(
                "UPDATE jobs SET checkpoint = ?2, source = ?3 WHERE id = ?1",
                params![job, serde_json::to_string(&recorded)?, machine],
            )?;
            db.query_row("SELECT resume_on FROM jobs WHERE id = ?1", [job], |r| {
                r.get(0)
            })?
        };
        if let Some(on) = resume_on {
            self.continuation(job, Some(&on)).await?;
            self.wake.notify_one();
        }
        Ok(())
    }

    /// Queues the next segment of a suspended job: the first segment's inputs plus the recorded checkpoint's files,
    /// run with the job's `resume` command; on `on` (a machine), or anywhere its placement allows (`None` or `*`).
    async fn continuation(&self, id: &str, on: Option<&str>) -> Result<String> {
        let (job, spec_json, checkpoint_json, source) = {
            let db = self.db.lock().expect("db");
            let (job, spec) = db.query_row(
                &format!("SELECT {} FROM jobs WHERE id = ?1", Self::COLUMNS),
                [id],
                Self::row_to_job,
            )?;
            let (checkpoint, source): (Option<String>, Option<String>) = db.query_row(
                "SELECT checkpoint, source FROM jobs WHERE id = ?1",
                [id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            (job, spec, checkpoint, source)
        };
        if job.state != QState::Suspended {
            bail!("job {id} is {}, not suspended", job.state.as_str());
        }
        if let Some(next) = &job.continued_by {
            bail!("job {id} already continues as {next}");
        }
        let spec: JobSpec = serde_json::from_str(&spec_json)?;
        let resume = spec
            .resume
            .clone()
            .context("the job has no resume declaration")?;
        let recorded: crate::suspend::Recorded = serde_json::from_str(
            checkpoint_json
                .as_deref()
                .context("no checkpoint was recorded for this job")?,
        )?;
        // The first segment's inputs plus this checkpoint (earlier checkpoints are not carried along).
        let root = self.root_of(id)?;
        let root_spec: JobSpec = serde_json::from_str(&self.spec_of(&root)?)?;
        let segment = job.segment + 1;
        let mut next = spec.clone();
        next.inputs = root_spec.inputs.clone();
        for f in &recorded.files {
            next.inputs.insert(
                f.path.clone(),
                crate::jobs::Input {
                    hash: f.hash,
                    executable: false,
                },
            );
        }
        next.argv = match &recorded.checkpoint {
            Some(cp) => resume.command(&cp.path, segment),
            // Suspended before its first checkpoint: it starts over.
            None => spec.argv.clone(),
        };
        let mut placement = job.placement.clone();
        if let Some(m) = on.filter(|m| *m != "*") {
            placement.machine = Some(m.to_string());
        }
        let next_id = format!("{root}-s{segment}");
        let db = self.db.lock().expect("db");
        db.execute(
            "INSERT INTO jobs (id, name, spec, placement, dest, after, priority, state, submitted_at, segment, parent, source)
             VALUES (?1, ?2, ?3, ?4, ?5, '[]', ?6, 'queued', ?7, ?8, ?9, ?10)",
            params![
                next_id,
                job.name,
                serde_json::to_string(&next)?,
                serde_json::to_string(&placement)?,
                job.dest.map(|d| d.to_string_lossy().into_owned()),
                job.priority,
                now() as i64,
                segment as i64,
                id,
                source
            ],
        )?;
        db.execute(
            "UPDATE jobs SET continued_by = ?2, resume_on = NULL WHERE id = ?1",
            params![id, next_id],
        )?;
        Ok(next_id)
    }

    fn root_of(&self, id: &str) -> Result<String> {
        let db = self.db.lock().expect("db");
        let mut current = id.to_string();
        loop {
            let parent: Option<String> =
                db.query_row("SELECT parent FROM jobs WHERE id = ?1", [&current], |r| {
                    r.get(0)
                })?;
            match parent {
                Some(p) => current = p,
                None => return Ok(current),
            }
        }
    }

    fn spec_of(&self, id: &str) -> Result<String> {
        Ok(self.db.lock().expect("db").query_row(
            "SELECT spec FROM jobs WHERE id = ?1",
            [id],
            |r| r.get(0),
        )?)
    }

    /// Before a continuation runs on a machine other than the one holding its checkpoint, copies the checkpoint's
    /// files into the coordinator's store (from where they are pushed like any input).
    async fn relay_checkpoint(&self, id: &str, machine: &str, spec: &JobSpec) -> Result<()> {
        let source: Option<String> = self.db.lock().expect("db").query_row(
            "SELECT source FROM jobs WHERE id = ?1",
            [id],
            |r| r.get(0),
        )?;
        let Some(source) = source.filter(|s| s != machine) else {
            return Ok(());
        };
        let mut need = Vec::new();
        for input in spec.inputs.values() {
            if !self.coordinator.store.blobs().has(input.hash).await? {
                need.push(input.hash);
            }
        }
        need.sort();
        need.dedup();
        crate::blobs::fetch(
            &self.coordinator.store,
            &self.coordinator.endpoint,
            self.machine_id(&source)?,
            &need,
        )
        .await
    }

    /// Preemption: when a GPU job waits only because a lower-priority, preemptible fleet job holds a GPU it could
    /// use, suspends that job under its own policy and queues its continuation. Returns whether it acted (or is
    /// already acting) for this job.
    async fn preempt_for(
        &self,
        job: &QueuedJob,
        spec: &serde_json::Value,
        snap: &HashMap<String, Snap>,
    ) -> Result<bool> {
        let queue = self.queue()?;
        if queue
            .iter()
            .any(|j| j.preempted_for.as_deref() == Some(&job.id) && j.state == QState::Suspending)
        {
            return Ok(true);
        }
        for (name, _) in self.machines()? {
            if job.placement.machine.as_ref().is_some_and(|m| m != &name) {
                continue;
            }
            // It must fit apart from a fleet GPU job.
            if Self::fits(snap, &name, &job.placement, spec, 0, &Usage::default()).is_err() {
                continue;
            }
            for holder in queue.iter().filter(|h| {
                h.machine.as_deref() == Some(&name)
                    && h.placement.gpu
                    && h.state == QState::Running
                    && h.priority < job.priority
            }) {
                let spec: JobSpec = serde_json::from_str(&self.spec_of(&holder.id)?)?;
                let (Some(preempt), Some(_)) = (&spec.preempt, &spec.resume) else {
                    continue;
                };
                let at_once = match preempt.mode {
                    crate::suspend::PreemptMode::WhenSafe => false,
                    crate::suspend::PreemptMode::Now => {
                        let plan = self.plan(&holder.id).await.ok();
                        let loss_minutes = plan
                            .and_then(|p| {
                                p.loss_compute_seconds
                                    .or(p.loss_wall_seconds.map(|w| w as f64))
                            })
                            .map(|s| s / 60.0);
                        preempt
                            .max_loss_minutes
                            .is_none_or(|max| loss_minutes.is_some_and(|l| l <= max))
                    }
                };
                self.suspend(&holder.id, at_once, None, Some("*")).await?;
                self.db.lock().expect("db").execute(
                    "UPDATE jobs SET preempted_for = ?2 WHERE id = ?1",
                    params![holder.id, job.id],
                )?;
                self.note(
                    &job.id,
                    &format!(
                        "suspending {} (priority {}) {} to make room",
                        holder.id,
                        holder.priority,
                        if at_once {
                            "now"
                        } else {
                            "at its next checkpoint"
                        }
                    ),
                )?;
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Updates unfinished, placed jobs from their machines' own lists (for invocations without a daemon).
    async fn refresh_direct(&self) {
        let Ok(queue) = self.queue() else { return };
        let mut by_machine: HashMap<String, Vec<QueuedJob>> = HashMap::new();
        for job in queue.into_iter().filter(|j| !j.state.finished()) {
            if let Some(machine) = job.machine.clone() {
                by_machine.entry(machine).or_default().push(job);
            }
        }
        for (machine, jobs) in by_machine {
            let Ok(connection) = self.connection(&machine).await else {
                continue;
            };
            let Ok(remote) = self.coordinator.jobs(&connection).await else {
                continue;
            };
            for job in jobs {
                if let Some(info) = remote.iter().find(|r| r.id == job.id) {
                    let state = QState::from_agent(info.state);
                    if state != job.state {
                        let _ = self.set_state(&job.id, state, info.exit.as_ref());
                    }
                }
            }
        }
    }

    /// A connection to a machine: the daemon's live one, or a new one.
    async fn connection(&self, name: &str) -> Result<Connection> {
        let live = self
            .live
            .lock()
            .expect("live")
            .get(name)
            .and_then(|l| l.connection.clone());
        if let Some(c) = live
            && c.close_reason().is_none()
        {
            return Ok(c);
        }
        let id = self.machine_id(name)?;
        tokio::time::timeout(PATIENCE, self.coordinator.connect(id))
            .await
            .map_err(|_| {
                crate::proto::transport(anyhow!(
                    "{name} did not answer within {} s",
                    PATIENCE.as_secs()
                ))
            })?
    }

    fn job_machine(&self, job: &str) -> Result<String> {
        self.job(job)?
            .ok_or_else(|| anyhow!("no job {job} in the queue"))?
            .machine
            .ok_or_else(|| anyhow!("job {job} has not been placed on a machine yet"))
    }

    async fn enqueue(
        &self,
        job: &JobFile,
        mut placement: Placement,
        dest: Option<PathBuf>,
        after: Vec<String>,
        priority: i64,
        key: Option<&str>,
    ) -> Result<String> {
        if let Some(m) = &placement.machine {
            self.machine_id(m)?;
        }
        for dep in &after {
            if self.job(dep)?.is_none() {
                bail!("no job {dep} to wait for");
            }
        }
        let dest = dest.map(std::path::absolute).transpose()?;
        let mut job = job.clone();
        if dest.is_some() && job.publish.is_empty() {
            // A destination with nothing published would fetch nothing: offer everything the job writes (its
            // unchanged inputs are left out when fetching).
            job.publish = vec!["*".into()];
        }
        let spec = self.coordinator.prepare(&job).await?;
        if let Some(vm) = &spec.vm {
            if placement.gpu && vm.gpu == crate::instances::Gpu::Shared {
                bail!("--gpu requests exclusive GPU use and conflicts with vm.gpu=shared");
            }
            placement.gpu = vm.gpu == crate::instances::Gpu::Exclusive;
        }
        // Input hashes are part of the specification: changing a file in place is a conflict too.
        let fingerprint = blake3::hash(&serde_json::to_vec(&serde_json::json!({
            "spec": spec, "placement": placement, "dest": dest, "after": after, "priority": priority
        }))?)
        .to_hex()
        .to_string();
        let mut db = self.db.lock().expect("db");
        let tx = db.transaction()?;
        if let Some(key) = key {
            let existing: Option<(String, String)> = tx
                .query_row(
                    "SELECT job, fingerprint FROM submissions WHERE key = ?1",
                    [key],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()?;
            if let Some((id, previous)) = existing {
                if previous != fingerprint {
                    bail!("submission key {key:?} already names a different specification");
                }
                return Ok(id);
            }
        }
        let id = crate::jobs::new_id(&job.name);
        tx.execute(
            "INSERT INTO jobs (id, name, spec, placement, dest, after, priority, state, submitted_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'queued', ?8)",
            params![
                id,
                job.name,
                serde_json::to_string(&spec)?,
                serde_json::to_string(&placement)?,
                dest.map(|d| d.to_string_lossy().into_owned()),
                serde_json::to_string(&after)?,
                priority,
                now() as i64
            ],
        )?;
        if let Some(key) = key {
            tx.execute(
                "INSERT INTO submissions (key, fingerprint, job) VALUES (?1, ?2, ?3)",
                params![key, fingerprint, id],
            )?;
        }
        tx.commit()?;
        Ok(id)
    }

    fn row_to_job(row: &rusqlite::Row) -> rusqlite::Result<(QueuedJob, String)> {
        let json = |i: usize| -> rusqlite::Result<String> { row.get(i) };
        let placement: Placement = serde_json::from_str(&json(3)?).unwrap_or_default();
        let after: Vec<String> = serde_json::from_str(&json(5)?).unwrap_or_default();
        let exit: Option<String> = row.get(13)?;
        Ok((
            QueuedJob {
                vm_status: row
                    .get::<_, Option<String>>(25)?
                    .and_then(|s| serde_json::from_str(&s).ok()),
                vm: serde_json::from_str::<JobSpec>(&json(2)?)
                    .ok()
                    .and_then(|s| s.vm),
                stopping: row
                    .get::<_, Option<String>>(24)?
                    .and_then(|s| serde_json::from_str(&s).ok()),
                id: row.get(0)?,
                name: row.get(1)?,
                state: QState::parse(&row.get::<_, String>(7)?),
                machine: row.get(8)?,
                placement,
                dest: row.get::<_, Option<String>>(4)?.map(PathBuf::from),
                after,
                priority: row.get(6)?,
                submitted_at: row.get::<_, i64>(10)? as u64,
                assigned_at: row.get::<_, Option<i64>>(11)?.map(|v| v as u64),
                finished_at: row.get::<_, Option<i64>>(12)?.map(|v| v as u64),
                exit: exit.and_then(|e| serde_json::from_str(&e).ok()),
                error: row.get(9)?,
                fetched_files: row.get::<_, i64>(14)? as u64,
                fetched_bytes: row.get::<_, i64>(15)? as u64,
                progress: row
                    .get::<_, Option<String>>(16)?
                    .and_then(|p| serde_json::from_str(&p).ok()),
                progress_at: row.get::<_, Option<i64>>(17)?.map(|v| v as u64),
                segment: row.get::<_, i64>(18)? as u32,
                parent: row.get(19)?,
                resume_on: row.get(20)?,
                preempted_for: row.get(21)?,
                continued_by: row.get(22)?,
                suspendable: serde_json::from_str::<JobSpec>(&row.get::<_, String>(2)?)
                    .is_ok_and(|s| s.resume.is_some()),
                checkpoint: row
                    .get::<_, Option<String>>(23)?
                    .and_then(|c| serde_json::from_str::<crate::suspend::Recorded>(&c).ok())
                    .and_then(|r| r.checkpoint),
            },
            row.get(2)?,
        ))
    }

    const COLUMNS: &'static str = "id, name, spec, placement, dest, after, priority, state, machine, error, submitted_at, \
         assigned_at, finished_at, exit, fetched_files, fetched_bytes, progress, progress_at, segment, parent, \
         resume_on, preempted_for, continued_by, checkpoint, stopping, vm_status";

    /// Fetches a job's outputs into `dest` (absolute), knowing its inputs and what its lineage wrote there before,
    /// and records each file written.
    async fn fetch_outputs(&self, job: &str, machine: &str, dest: &Path) -> Result<FetchReport> {
        let root = self.root_of(job)?;
        let written = {
            let db = self.db.lock().expect("db");
            let mut statement = db.prepare("SELECT path, hash FROM written WHERE root = ?1")?;
            let rows = statement.query_map([&root], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
            })?;
            rows.filter_map(Result::ok)
                .filter_map(|(path, hash)| Some((PathBuf::from(path), hash.parse().ok()?)))
                .filter(|(path, _): &(PathBuf, iroh_blobs::Hash)| path.starts_with(dest))
                .collect()
        };
        let known = crate::coordinator::Known {
            inputs: self.input_hashes(job),
            written,
        };
        self.coordinator
            .fetch_known(
                self.machine_id(machine)?,
                job,
                dest,
                &known,
                |path, hash| {
                    let _ = self.db.lock().expect("db").execute(
                        "INSERT INTO written (path, hash, root) VALUES (?1, ?2, ?3)
                     ON CONFLICT(path) DO UPDATE SET hash = ?2, root = ?3",
                        params![path.to_string_lossy(), hash.to_hex(), root],
                    );
                },
            )
            .await
    }

    /// A queued job's inputs by path and hash (empty if it is not in the queue).
    fn input_hashes(&self, id: &str) -> BTreeMap<String, iroh_blobs::Hash> {
        self.db
            .lock()
            .expect("db")
            .query_row("SELECT spec FROM jobs WHERE id = ?1", [id], |r| {
                r.get::<_, String>(0)
            })
            .ok()
            .and_then(|spec| serde_json::from_str::<JobSpec>(&spec).ok())
            .map(|spec| {
                spec.inputs
                    .into_iter()
                    .map(|(path, input)| (path, input.hash))
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn job(&self, id: &str) -> Result<Option<QueuedJob>> {
        let db = self.db.lock().expect("db");
        Ok(db
            .query_row(
                &format!("SELECT {} FROM jobs WHERE id = ?1", Self::COLUMNS),
                [id],
                Self::row_to_job,
            )
            .optional()?
            .map(|(j, _)| j))
    }

    pub fn queue(&self) -> Result<Vec<QueuedJob>> {
        let db = self.db.lock().expect("db");
        let mut statement = db.prepare(&format!(
            "SELECT {} FROM jobs ORDER BY submitted_at, id",
            Self::COLUMNS
        ))?;
        let rows = statement.query_map([], Self::row_to_job)?;
        Ok(rows
            .map(|r| r.map(|(j, _)| j))
            .collect::<rusqlite::Result<_>>()?)
    }

    fn set_state(&self, id: &str, state: QState, exit: Option<&Exit>) -> Result<()> {
        let finished_at = state.finished().then(|| now() as i64);
        self.db.lock().expect("db").execute(
            "UPDATE jobs SET state = ?2, finished_at = COALESCE(finished_at, ?3), exit = COALESCE(?4, exit), stopping = CASE WHEN ?3 IS NULL THEN stopping ELSE NULL END WHERE id = ?1",
            params![id, state.as_str(), finished_at, exit.map(serde_json::to_string).transpose()?],
        )?;
        Ok(())
    }

    /// Whether a machine fits a placement right now (`busy_gpu`: machines with a fleet GPU job unfinished).
    /// Each machine's state as one scheduling pass sees it, so jobs are judged against the same picture in queue
    /// order (a machine coming online mid-pass cannot let a later job jump an earlier one).
    fn snapshot(&self) -> HashMap<String, Snap> {
        let live = self.live.lock().expect("live");
        live.iter()
            .map(|(name, l)| {
                let online = !self.daemon
                    || l.connection
                        .as_ref()
                        .is_some_and(|c| c.close_reason().is_none());
                (
                    name.clone(),
                    Snap {
                        online,
                        hello: l.hello.clone(),
                        status: l.status.clone(),
                    },
                )
            })
            .collect()
    }

    /// Whether a machine fits a placement and can run the job (`spec`, as JSON) at its agent's protocol version.
    fn fits(
        snap: &HashMap<String, Snap>,
        name: &str,
        placement: &Placement,
        spec: &serde_json::Value,
        threads: u32,
        usage: &Usage,
    ) -> Result<(), String> {
        let Some(s) = snap.get(name) else {
            return Err("not connected".into());
        };
        let (Some(hello), Some(status)) = (&s.hello, &s.status) else {
            return Err("not connected".into());
        };
        if !s.online {
            return Err("offline".into());
        }
        if let Some((item, minor)) =
            crate::proto::job_too_new(spec, crate::proto::PROTOCOL.agreed(hello.protocol))
        {
            return Err(format!(
                "its agent speaks protocol {}, which predates `{item}` (protocol {}.{minor}); upgrade it",
                hello.protocol, hello.protocol.major
            ));
        }
        if let Some(missing) = placement.tags.iter().find(|t| !hello.tags.contains(t)) {
            return Err(format!("lacks tag {missing}"));
        }
        if placement.ac_power && status.on_ac_power != Some(true) {
            return Err("not on AC power".into());
        }
        let vm: Option<crate::instances::Spec> = spec
            .get("vm")
            .filter(|v| !v.is_null())
            .map(|v| serde_json::from_value(v.clone()))
            .transpose()
            .map_err(|e| e.to_string())?;
        let pending = usage.vms.get(name).cloned().unwrap_or_default();
        // Merge accepted/pending job reservations with observed instances by ID.
        // This counts delivery uncertainty, without charging a job's VM twice.
        let mut active = pending.clone();
        let mut disks = pending.clone();
        for i in &status.instances {
            if i.reserves_compute() {
                active
                    .entry(i.record.id.clone())
                    .or_insert_with(|| i.record.spec.clone());
            }
            if i.observed.state != crate::instances::State::Deleted {
                disks
                    .entry(i.record.id.clone())
                    .or_insert_with(|| i.record.spec.clone());
            }
        }
        let other_threads: u32 = active
            .iter()
            .filter(|(id, _)| !pending.contains_key(*id))
            .map(|(_, v)| v.cpus)
            .sum();
        let used = usage
            .threads
            .get(name)
            .copied()
            .unwrap_or(0)
            .saturating_add(other_threads);
        let needed = vm.as_ref().map_or(threads, |v| v.cpus);
        let free = hello.cpu_slots.saturating_sub(used);
        if needed > free {
            return Err(format!(
                "needs {needed} CPU threads; {free} of its {} are free",
                hello.cpu_slots
            ));
        }
        if let Some(vm) = &vm {
            let capacity = status
                .vm_capacity
                .as_ref()
                .ok_or("Tart job backend is not enabled on this worker")?;
            if !capacity.images.contains(&vm.image) {
                return Err(format!("image {} is not in its trusted catalog", vm.image));
            }
            if active.len() >= crate::instances::MACOS_VM_LIMIT {
                return Err("macOS VM limit reached".into());
            }
            if !active.is_empty()
                && (vm.gpu == crate::instances::Gpu::Exclusive
                    || active
                        .values()
                        .any(|v| v.gpu == crate::instances::Gpu::Exclusive))
            {
                return Err("exclusive GPU VM reservation conflicts".into());
            }
            let memory = active.values().map(|v| u64::from(v.memory_mb)).sum::<u64>()
                + u64::from(vm.memory_mb);
            if memory > u64::from(capacity.memory_mb) || memory > hello.memory_bytes / (1 << 20) {
                return Err("insufficient VM memory budget".into());
            }
            let disk =
                disks.values().map(|v| u64::from(v.disk_gb)).sum::<u64>() + u64::from(vm.disk_gb);
            if disk > u64::from(capacity.disk_gb) {
                return Err("insufficient VM disk budget".into());
            }
        } else if placement.gpu && !active.is_empty() {
            return Err("its GPU is reserved by a VM instance".into());
        }
        if placement.gpu || vm.is_some() {
            if usage.gpu.contains(name) {
                return Err("its GPU is running a fleet job".into());
            }
            if let Some(p) = &status.gpu_busy_external {
                return Err(format!("its GPU is busy ({p})"));
            }
        }
        Ok(())
    }

    /// What placed, unfinished fleet jobs use on each machine: its GPU, and CPU threads. A paused job releases both
    /// (its GPU only unless its specification keeps it, `pause_keeps_gpu`).
    fn usage(&self) -> Result<Usage> {
        let db = self.db.lock().expect("db");
        let mut statement = db.prepare(
            "SELECT machine, state, spec, placement, id FROM jobs WHERE machine IS NOT NULL",
        )?;
        let rows = statement.query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, String>(4)?,
            ))
        })?;
        let mut usage = Usage::default();
        for row in rows {
            let (machine, state, spec, placement, id) = row?;
            let state = QState::parse(&state);
            if state.finished() || state == QState::Queued {
                continue;
            }
            let placement: Placement = serde_json::from_str(&placement).unwrap_or_default();
            let spec: Option<JobSpec> = serde_json::from_str(&spec).ok();
            let paused = state == QState::Paused;
            if let Some(vm) = spec.as_ref().and_then(|s| s.vm.as_ref()) {
                usage
                    .vms
                    .entry(machine.clone())
                    .or_default()
                    .insert(crate::vm_job::instance_id(&id), vm.clone());
                *usage.threads.entry(machine).or_default() += vm.cpus;
                continue;
            }
            if placement.gpu && (!paused || spec.as_ref().is_some_and(|s| s.pause_keeps_gpu)) {
                usage.gpu.insert(machine.clone());
            }
            if !paused {
                *usage.threads.entry(machine).or_default() += spec.map_or(1, |s| s.threads);
            }
        }
        Ok(usage)
    }

    /// Tries to place one queued job; returns whether it was sent to a machine.
    async fn place_one(
        self: &Arc<Self>,
        id: &str,
        snap: Option<&HashMap<String, Snap>>,
    ) -> Result<bool> {
        let (job, spec) = {
            let db = self.db.lock().expect("db");
            db.query_row(
                &format!("SELECT {} FROM jobs WHERE id = ?1", Self::COLUMNS),
                [id],
                Self::row_to_job,
            )?
        };
        if job.state == QState::Starting {
            let machine = job
                .machine
                .as_deref()
                .context("starting job has no machine")?;
            let connection = self.connection(machine).await?;
            if let Some(info) = self
                .coordinator
                .jobs(&connection)
                .await?
                .into_iter()
                .find(|j| j.id == id)
            {
                if info.state != State::Starting {
                    self.apply(
                        machine,
                        &EventKind::JobState {
                            job: id.into(),
                            state: info.state,
                        },
                    )
                    .await?;
                }
                return Ok(false);
            }
            let spec: JobSpec = serde_json::from_str(&spec)?;
            self.relay_checkpoint(id, machine, &spec).await?;
            self.coordinator
                .submit_spec_allocated(
                    self.machine_id(machine)?,
                    spec,
                    Some(id.into()),
                    Some(job.placement.gpu),
                )
                .await?;
            return Ok(true);
        }
        if job.state != QState::Queued {
            return Ok(false);
        }
        for dep in &job.after {
            match self.job(dep)?.map(|d| d.state) {
                Some(QState::Succeeded) => {}
                Some(s) if s.finished() => {
                    self.db.lock().expect("db").execute(
                        "UPDATE jobs SET state = 'cancelled', error = ?2, finished_at = ?3 WHERE id = ?1",
                        params![id, format!("job {dep} it waits for ended {}", s.as_str()), now() as i64],
                    )?;
                    return Ok(false);
                }
                _ => {
                    self.note(id, &format!("waiting for {dep}"))?;
                    return Ok(false);
                }
            }
        }
        if !self.daemon {
            // A direct invocation has no machine tasks: learn about the candidates now.
            for (name, _) in self.machines()? {
                if job.placement.machine.as_ref().is_none_or(|m| m == &name) {
                    self.probe(&name).await;
                }
            }
        }
        let probed;
        let snap = match snap {
            Some(snap) => snap,
            None => {
                probed = self.snapshot();
                &probed
            }
        };
        let usage = self.usage()?;
        let threads = serde_json::from_str::<JobSpec>(&spec).map_or(1, |s| s.threads);
        let spec_json: serde_json::Value = serde_json::from_str(&spec)?;
        let mut reasons = Vec::new();
        let mut chosen = None;
        for (name, _) in self.machines()? {
            if job.placement.machine.as_ref().is_some_and(|m| m != &name) {
                continue;
            }
            match Self::fits(snap, &name, &job.placement, &spec_json, threads, &usage) {
                Ok(()) => {
                    chosen = Some(name);
                    break;
                }
                Err(why) => reasons.push(format!("{name}: {why}")),
            }
        }
        if chosen.is_none()
            && self.daemon
            && job.placement.gpu
            && spec_json.get("vm").is_none_or(serde_json::Value::is_null)
            && self.preempt_for(&job, &spec_json, snap).await?
        {
            return Ok(false);
        }
        let Some(machine) = chosen else {
            let why = if reasons.is_empty() {
                "no machine matches".to_string()
            } else {
                reasons.join("; ")
            };
            self.note(id, &why)?;
            if !self.daemon {
                self.set_state(id, QState::Cancelled, None)?;
                bail!(
                    "no machine can take {id} now ({why}); start `fleet daemon` to queue it until one can"
                );
            }
            return Ok(false);
        };
        let assigned = self.db.lock().expect("db").execute(
            "UPDATE jobs SET state = 'starting', machine = ?2, assigned_at = ?3, error = NULL WHERE id = ?1 AND state = 'queued'",
            params![id, machine, now() as i64],
        )?;
        // A cancellation while placement was checking candidates must win.
        if assigned == 0 {
            return Ok(false);
        }
        let spec: JobSpec = serde_json::from_str(&spec)?;
        let agent = self.machine_id(&machine)?;
        if let Err(e) = self.relay_checkpoint(id, &machine, &spec).await {
            self.db.lock().expect("db").execute(
                "UPDATE jobs SET state = 'queued', machine = NULL, assigned_at = NULL, error = ?2, next_try_at = ?3
                 WHERE id = ?1",
                params![id, format!("copying its checkpoint to {machine}: {e:#}"), now() as i64 + 30],
            )?;
            return Ok(false);
        }
        match self
            .coordinator
            .submit_spec_allocated(agent, spec, Some(id.to_string()), Some(job.placement.gpu))
            .await
        {
            Ok(_) => {
                tracing::info!(job = %id, %machine, "placed");
                Ok(true)
            }
            Err(e) => {
                tracing::warn!(job = %id, %machine, "could not start: {e:#}");
                self.db.lock().expect("db").execute(
                    "UPDATE jobs SET error = ?2, next_try_at = ?3 WHERE id = ?1",
                    params![
                        id,
                        format!("delivery to {machine} uncertain; reconciling the same job: {e:#}"),
                        now() as i64 + 30
                    ],
                )?;
                if !self.daemon {
                    bail!(
                        "delivery of {id} to {machine} uncertain; inspect the job before retrying: {e:#}"
                    );
                }
                Ok(false)
            }
        }
    }

    fn note(&self, id: &str, why: &str) -> Result<()> {
        self.db
            .lock()
            .expect("db")
            .execute("UPDATE jobs SET error = ?2 WHERE id = ?1", params![id, why])?;
        Ok(())
    }

    /// The scheduler: places queued jobs in priority order, then oldest first.
    async fn schedule(self: Arc<Self>) {
        loop {
            let waiting: Vec<String> = {
                let db = self.db.lock().expect("db");
                let mut statement = db
                    .prepare(
                        "SELECT id FROM jobs WHERE state IN ('queued', 'starting') AND next_try_at <= ?1
                         ORDER BY priority DESC, submitted_at, id",
                    )
                    .expect("query");
                statement
                    .query_map([now() as i64], |r| r.get(0))
                    .map(|rows| rows.filter_map(Result::ok).collect())
                    .unwrap_or_default()
            };
            let snap = self.snapshot();
            for id in waiting {
                if let Err(e) = self.place_one(&id, Some(&snap)).await {
                    tracing::warn!(job = %id, "placement failed: {e:#}");
                    let _ = self.db.lock().expect("db").execute(
                        "UPDATE jobs SET next_try_at = ?2, error = ?3 WHERE id = ?1",
                        params![
                            id,
                            now() as i64 + 30,
                            format!("reconciling placement: {e:#}")
                        ],
                    );
                }
            }
            tokio::select! {
                _ = self.wake.notified() => {}
                _ = tokio::time::sleep(Duration::from_secs(3)) => {}
            }
        }
    }

    /// One look at a machine (for direct invocations).
    async fn probe(&self, name: &str) {
        let result: Result<(Hello, Status)> = async {
            let connection = self.connection(name).await?;
            let Response::Hello(hello) =
                self.coordinator.ask(&connection, &Request::hello()).await?
            else {
                bail!("unexpected answer to Hello");
            };
            let Response::Status(status) =
                self.coordinator.ask(&connection, &Request::Status).await?
            else {
                bail!("unexpected answer to Status");
            };
            Ok((hello, status))
        }
        .await;
        let mut live = self.live.lock().expect("live");
        let entry = live.entry(name.to_string()).or_default();
        match result {
            Ok((hello, status)) => {
                entry.hello = Some(hello);
                entry.status = Some(status);
                entry.last_seen = Some(now());
                entry.error = None;
            }
            Err(e) => entry.error = Some(format!("{e:#}")),
        }
    }

    async fn machine_views(&self) -> Result<Vec<MachineView>> {
        let list = self.machines()?;
        if !self.daemon {
            for (name, _) in &list {
                self.probe(name).await;
            }
        }
        let usage = self.usage()?;
        let live = self.live.lock().expect("live");
        Ok(list
            .into_iter()
            .map(|(name, id)| {
                let l = live.get(&name);
                let connection = l
                    .and_then(|l| l.connection.as_ref())
                    .filter(|c| c.close_reason().is_none());
                MachineView {
                    online: if self.daemon {
                        connection.is_some()
                    } else {
                        l.is_some_and(|l| l.error.is_none())
                    },
                    paths: connection.map(paths).unwrap_or_default(),
                    hello: l.and_then(|l| l.hello.clone()),
                    status: l.and_then(|l| l.status.clone()),
                    last_seen: l.and_then(|l| l.last_seen),
                    error: l.and_then(|l| l.error.clone()),
                    threads_used: usage
                        .threads
                        .get(&name)
                        .copied()
                        .unwrap_or(0)
                        .saturating_add(l.and_then(|l| l.status.as_ref()).map_or(0, |s| {
                            s.instances
                                .iter()
                                .filter(|i| {
                                    i.reserves_compute()
                                        && !usage
                                            .vms
                                            .get(&name)
                                            .is_some_and(|v| v.contains_key(&i.record.id))
                                })
                                .map(|i| i.record.spec.cpus)
                                .sum()
                        })),
                    name,
                    id: id.id.to_string(),
                }
            })
            .collect())
    }

    fn cursor(&self, machine: &str) -> u64 {
        self.db
            .lock()
            .expect("db")
            .query_row(
                "SELECT seq FROM cursors WHERE machine = ?1",
                [machine],
                |r| r.get::<_, i64>(0),
            )
            .optional()
            .ok()
            .flatten()
            .unwrap_or(0) as u64
    }

    fn set_cursor(&self, machine: &str, seq: u64) -> Result<()> {
        self.db.lock().expect("db").execute(
            "INSERT INTO cursors (machine, seq) VALUES (?1, ?2) ON CONFLICT(machine) DO UPDATE SET seq = ?2",
            params![machine, seq as i64],
        )?;
        Ok(())
    }

    /// Keeps a connection to one machine: replays its events from the last one seen, refreshes its status, keeps it
    /// awake while work waits for it, and reconnects after any failure.
    async fn run_machine(self: Arc<Self>, name: String, id: EndpointAddr) {
        let mut backoff = Duration::from_secs(1);
        loop {
            match self.machine_session(&name, id.clone()).await {
                Ok(()) => backoff = Duration::from_secs(1),
                Err(e) => {
                    let mut live = self.live.lock().expect("live");
                    let entry = live.entry(name.clone()).or_default();
                    entry.connection = None;
                    entry.error = Some(format!("{e:#}"));
                }
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_secs(60));
        }
    }

    async fn machine_session(self: &Arc<Self>, name: &str, id: EndpointAddr) -> Result<()> {
        let connection = tokio::time::timeout(PATIENCE, self.coordinator.connect(id))
            .await
            .map_err(|_| anyhow!("no answer within {} s", PATIENCE.as_secs()))??;
        let Response::Hello(hello) = self.coordinator.ask(&connection, &Request::hello()).await?
        else {
            bail!("unexpected answer to Hello");
        };
        {
            let mut live = self.live.lock().expect("live");
            let entry = live.entry(name.to_string()).or_default();
            entry.connection = Some(connection.clone());
            entry.hello = Some(hello);
            entry.error = None;
            entry.last_seen = Some(now());
        }
        tracing::info!(machine = %name, "connected");
        let mut last_status = tokio::time::Instant::now() - Duration::from_secs(60);
        let mut last_awake = last_status;
        loop {
            if last_status.elapsed() >= Duration::from_secs(15) {
                let Response::Status(status) =
                    self.coordinator.ask(&connection, &Request::Status).await?
                else {
                    bail!("unexpected answer to Status");
                };
                for instance in &status.instances {
                    self.remember_instance(name, instance)?;
                }
                let mut live = self.live.lock().expect("live");
                let entry = live.entry(name.to_string()).or_default();
                entry.status = Some(status);
                entry.last_seen = Some(now());
                drop(live);
                last_status = tokio::time::Instant::now();
                self.wake.notify_one();
            }
            if last_awake.elapsed() >= Duration::from_secs(60) && self.work_waits_for(name)? {
                // Sleep prevention is best effort: an unavailable platform helper must not
                // prevent replaying job events (which also releases queued resource claims).
                if let Err(e) = self
                    .coordinator
                    .ask(&connection, &Request::KeepAwake { seconds: 180 })
                    .await
                {
                    tracing::warn!(machine = %name, "keeping the worker awake failed: {e:#}");
                }
                last_awake = tokio::time::Instant::now();
            }
            let since = self.cursor(name);
            let Response::Events { events, last } = self
                .coordinator
                .ask(
                    &connection,
                    &Request::Events {
                        since,
                        wait_ms: 10_000,
                    },
                )
                .await?
            else {
                bail!("unexpected answer to Events");
            };
            if last < since {
                // The agent's event log was reset (for example, reinstalled): start over from its beginning.
                self.set_cursor(name, 0)?;
                continue;
            }
            for event in events {
                self.apply(name, &event.kind).await?;
                self.set_cursor(name, event.seq)?;
            }
        }
    }

    /// Whether any queued job could run on this machine (ignoring a busy GPU or battery power).
    fn work_waits_for(&self, name: &str) -> Result<bool> {
        let tags = self
            .live
            .lock()
            .expect("live")
            .get(name)
            .and_then(|l| l.hello.as_ref().map(|h| h.tags.clone()));
        let Some(tags) = tags else { return Ok(false) };
        Ok(self.queue()?.iter().any(|j| {
            j.state == QState::Queued
                && j.placement.machine.as_ref().is_none_or(|m| m == name)
                && j.placement.tags.iter().all(|t| tags.contains(t))
        }))
    }

    async fn apply(self: &Arc<Self>, machine: &str, kind: &EventKind) -> Result<()> {
        match kind {
            EventKind::VmJob { job, status } => {
                self.db.lock().expect("db").execute(
                    "UPDATE jobs SET vm_status=?2 WHERE id=?1 AND machine=?3",
                    params![job, serde_json::to_string(status)?, machine],
                )?;
            }
            EventKind::InstanceState { instance } => {
                self.remember_instance(machine, instance)?;
                self.wake.notify_one();
            }
            EventKind::AgentStarted { .. } | EventKind::Unknown => {}
            EventKind::Stopping { job, stopping } => {
                self.db.lock().expect("db").execute(
                    "UPDATE jobs SET stopping = ?2 WHERE id = ?1 AND finished_at IS NULL",
                    params![job, serde_json::to_string(stopping)?],
                )?;
            }
            EventKind::Progress { job, probes } => {
                self.db.lock().expect("db").execute(
                    "UPDATE jobs SET progress = ?2, progress_at = ?3 WHERE id = ?1",
                    params![job, serde_json::to_string(probes)?, now() as i64],
                )?;
            }
            EventKind::JobState { job, state } => {
                let Some(record) = self.job(job)? else {
                    return Ok(());
                };
                let state = QState::from_agent(*state);
                if record.state == state {
                    return Ok(());
                }
                if record.state == QState::Suspending
                    && matches!(state, QState::Running | QState::Paused)
                {
                    // Still running until its checkpoint: it stays "suspending" here.
                    return Ok(());
                }
                let exit = if state.finished() {
                    let connection = self.connection(machine).await?;
                    self.coordinator
                        .jobs(&connection)
                        .await?
                        .into_iter()
                        .find(|j| &j.id == job)
                        .and_then(|j| j.exit)
                } else {
                    None
                };
                self.set_state(job, state, exit.as_ref())?;
                if state == QState::Suspended {
                    self.record_suspension(machine, job).await?;
                }
                if state.finished() {
                    self.db
                        .lock()
                        .expect("db")
                        .execute("UPDATE jobs SET stopping = NULL WHERE id = ?1", [job])?;
                    self.wake.notify_one();
                    if record.dest.is_some() {
                        self.fetch_into_dest(job.clone(), machine.to_string());
                    }
                }
            }
            EventKind::Outputs { job, .. } => {
                if self.job(job)?.is_some_and(|j| j.dest.is_some()) {
                    self.fetch_into_dest(job.clone(), machine.to_string());
                }
            }
            EventKind::SuspendGaveUp { job } => {
                if self
                    .job(job)?
                    .is_some_and(|j| j.state == QState::Suspending)
                {
                    self.db.lock().expect("db").execute(
                        "UPDATE jobs SET state = 'running', resume_on = NULL, preempted_for = NULL,
                         error = 'the suspension gave up: no checkpoint settled in time' WHERE id = ?1",
                        [job],
                    )?;
                }
            }
        }
        Ok(())
    }

    /// Fetches a job's outputs into its destination in the background (one fetch per job at a time).
    fn fetch_into_dest(self: &Arc<Self>, job: String, machine: String) {
        {
            let mut fetching = self.fetching.lock().expect("fetching");
            if let Some(again) = fetching.get_mut(&job) {
                *again = true;
                return;
            }
            fetching.insert(job.clone(), false);
        }
        let core = self.clone();
        tokio::spawn(async move {
            loop {
                let result: Result<FetchReport> = async {
                    let dest = core
                        .job(&job)?
                        .and_then(|j| j.dest)
                        .context("no destination")?;
                    core.fetch_outputs(&job, &machine, &dest).await
                }
                .await;
                match result {
                    Ok(report) => {
                        let db = core.db.lock().expect("db");
                        let _ = db.execute(
                        "UPDATE jobs SET fetched_files = fetched_files + ?2, fetched_bytes = fetched_bytes + ?3,
                         error = CASE WHEN error LIKE 'fetching outputs failed:%' THEN NULL ELSE error END
                         WHERE id = ?1",
                        params![job, report.fetched.len() as i64, report.bytes as i64],
                    );
                    }
                    Err(e) => {
                        tracing::warn!(%job, "fetching outputs failed: {e:#}");
                        let _ = core.note(&job, &format!("fetching outputs failed: {e:#}"));
                    }
                }
                let mut fetching = core.fetching.lock().expect("fetching");
                if fetching.get(&job) == Some(&true) {
                    fetching.insert(job.clone(), false);
                } else {
                    fetching.remove(&job);
                    break;
                }
            }
        });
    }
}

/// The daemon's socket: `$FLEET_HOME/daemon.sock`, or, when that path is too long for a Unix socket (macOS allows
/// 104 bytes), `/tmp/fleet-<uid>-<hash of the home>.sock`. `contrib/fleet.py` computes the same path.
pub fn socket_path(home: &Path) -> PathBuf {
    let preferred = home.join("daemon.sock");
    if preferred.as_os_str().len() < 100 {
        return preferred;
    }
    // FNV-1a (64-bit) of the home's path: simple enough to compute identically in Python.
    let hash = home
        .as_os_str()
        .as_encoded_bytes()
        .iter()
        .fold(0xcbf2_9ce4_8422_2325_u64, |h, &b| {
            (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3)
        });
    PathBuf::from(format!("/tmp/fleet-{}-{hash:016x}.sock", unsafe {
        libc::getuid()
    }))
}

/// Serves the core on a Unix socket: one JSON request line, one JSON answer line, per connection.
pub async fn serve(core: Arc<Core>, path: &Path) -> Result<()> {
    if path.exists() {
        if UnixStream::connect(path).await.is_ok() {
            bail!("a fleet daemon is already running ({})", path.display());
        }
        std::fs::remove_file(path)?;
    }
    let listener = UnixListener::bind(path)?;
    std::fs::set_permissions(path, std::os::unix::fs::PermissionsExt::from_mode(0o600))?;
    tracing::info!(socket = %path.display(), "fleet daemon listening");
    loop {
        let (stream, _) = listener.accept().await?;
        let core = core.clone();
        tokio::spawn(async move {
            let (read, mut write) = stream.into_split();
            let mut line = String::new();
            if BufReader::new(read).read_line(&mut line).await.is_err() {
                return;
            }
            let response = match serde_json::from_str::<DaemonRequest>(&line) {
                Ok(request) => core.handle(request).await,
                Err(e) => DaemonResponse::Error {
                    message: format!("unreadable request: {e}"),
                    retryable: false,
                },
            };
            if let Ok(mut json) = serde_json::to_string(&response) {
                json.push('\n');
                let _ = write.write_all(json.as_bytes()).await;
            }
        });
    }
}

/// Sends one request to a running daemon; `None` if no daemon is listening.
pub async fn ask_daemon(home: &Path, request: &DaemonRequest) -> Result<Option<DaemonResponse>> {
    let Ok(stream) = UnixStream::connect(socket_path(home)).await else {
        return Ok(None);
    };
    let (read, mut write) = stream.into_split();
    let mut json = serde_json::to_string(request)?;
    json.push('\n');
    write.write_all(json.as_bytes()).await?;
    let mut line = String::new();
    BufReader::new(read).read_line(&mut line).await?;
    Ok(Some(
        serde_json::from_str(&line).context("reading the daemon's answer")?,
    ))
}
