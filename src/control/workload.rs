//! Committed placement and worker observations. Bulk logs and files stay off the Raft log.
use super::{Reply, State, network::Peer};
use crate::{instances, jobs};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Budget {
    pub cpu_slots: u32,
    pub memory_mb: u32,
    pub disk_gb: u32,
    pub vm_slots: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Registration {
    pub id: String,
    pub peer: Peer,
    /// Changes only when the worker's durable journal is replaced, never on process restart.
    pub incarnation: String,
    pub budget: Budget,
    pub images: BTreeSet<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub runtimes: BTreeMap<String, super::portable::Runtime>,
    pub tags: BTreeSet<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Workload {
    Job {
        spec: Box<jobs::JobSpec>,
        /// Native GPU reservation; a VM always has GPU access and uses its VM GPU policy.
        gpu: bool,
        /// Native memory admission reservation. A VM uses its configured guest RAM instead.
        memory_mb: u32,
    },
    Instance {
        spec: instances::Spec,
    },
}

/// Intent and observations carry their domain, so job suspension cannot be mistaken for VM stop.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum JobDesired {
    Running,
    Suspended,
    Cancelled,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum InstanceDesired {
    Running,
    Stopped,
    Deleted,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum Desired {
    Job(JobDesired),
    Instance(InstanceDesired),
}
impl Desired {
    pub fn running(self) -> bool {
        matches!(
            self,
            Self::Job(JobDesired::Running) | Self::Instance(InstanceDesired::Running)
        )
    }
    pub fn terminating(self) -> bool {
        matches!(
            self,
            Self::Job(JobDesired::Cancelled) | Self::Instance(InstanceDesired::Deleted)
        )
    }
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum AttemptPhase {
    Pending,
    Running,
    Suspended,
    Succeeded,
    Failed,
    Cancelled,
    Uncertain,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum InstancePhase {
    Pending,
    Running,
    Stopped,
    Deleted,
    Uncertain,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum WorkPhase {
    Job(AttemptPhase),
    Instance(InstancePhase),
}
impl WorkPhase {
    pub fn terminal(self) -> bool {
        matches!(
            self,
            Self::Job(
                AttemptPhase::Suspended
                    | AttemptPhase::Succeeded
                    | AttemptPhase::Failed
                    | AttemptPhase::Cancelled
            ) | Self::Instance(InstancePhase::Deleted)
        )
    }
    pub fn label(self) -> String {
        match self {
            Self::Job(p) => format!("{p:?}"),
            Self::Instance(p) => format!("{p:?}"),
        }
        .to_lowercase()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Observation {
    pub phase: WorkPhase,
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Assignment {
    pub id: String,
    pub execution: String,
    /// Present only for a job attempt; persistent VMs have their own lifecycle.
    pub attempt: Option<super::queue::Attempt>,
    pub suspend_checkpoint: Option<String>,
    pub worker: String,
    pub workload: Workload,
    pub desired: Desired,
    pub revision: u64,
    pub observed: Option<(u64, Observation)>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Report {
    pub worker: String,
    pub incarnation: String,
    pub sequence: u64,
    pub assignment: String,
    pub revision: u64,
    pub observation: Observation,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerView {
    pub artifacts: Vec<super::artifact::Artifact>,
    pub stores: BTreeMap<String, super::artifact::Registration>,
    pub cleanup_allowed: bool,
    pub starts_allowed: bool,
    pub checkpoints: BTreeSet<String>,
    pub resume_checkpoints: BTreeMap<String, super::checkpoint::Checkpoint>,
    pub recovery_required: bool,
    pub held: BTreeSet<String>,
    pub registration: Registration,
    pub assignments: Vec<Assignment>,
    pub acknowledged: u64,
}

impl Registration {
    /// Monotonic catalog updates keep every previously accepted workload compatible.
    pub fn with_added_runtimes(
        &self,
        runtimes: &BTreeMap<String, super::portable::Runtime>,
    ) -> Result<Self> {
        for (image, old) in &self.runtimes {
            old.accepts(
                runtimes
                    .get(image)
                    .ok_or_else(|| anyhow::anyhow!("cannot remove an advertised runtime"))?,
            )?;
        }
        let mut next = self.clone();
        next.runtimes = runtimes.clone();
        next.validate()?;
        Ok(next)
    }
    pub fn validate(&self) -> Result<()> {
        self.peer.validate()?;
        for (image, runtime) in &self.runtimes {
            ensure!(self.images.contains(image), "runtime image not advertised");
            runtime.validate()?;
        }
        instances::valid_id(&self.id)?;
        ensure!(
            !self.incarnation.is_empty() && self.incarnation.len() <= 128,
            "invalid worker incarnation"
        );
        ensure!(
            self.budget.cpu_slots > 0 && self.budget.memory_mb > 0,
            "positive CPU and memory budgets required"
        );
        ensure!(
            self.budget.vm_slots <= instances::MACOS_VM_LIMIT as u32,
            "macOS permits at most two active VMs"
        );
        Ok(())
    }
    pub fn cursor(&self) -> String {
        super::model::key(&self.id, &self.incarnation)
    }
}

impl Workload {
    pub fn initial_desired(&self) -> Desired {
        match self {
            Self::Job { .. } => Desired::Job(JobDesired::Running),
            Self::Instance { .. } => Desired::Instance(InstanceDesired::Running),
        }
    }
    pub fn pending(&self) -> WorkPhase {
        match self {
            Self::Job { .. } => WorkPhase::Job(AttemptPhase::Pending),
            Self::Instance { .. } => WorkPhase::Instance(InstancePhase::Pending),
        }
    }
    pub fn uncertain(&self) -> WorkPhase {
        match self {
            Self::Job { .. } => WorkPhase::Job(AttemptPhase::Uncertain),
            Self::Instance { .. } => WorkPhase::Instance(InstancePhase::Uncertain),
        }
    }
    pub fn accepts_phase(&self, phase: WorkPhase) -> bool {
        matches!(
            (self, phase),
            (Self::Job { .. }, WorkPhase::Job(_)) | (Self::Instance { .. }, WorkPhase::Instance(_))
        )
    }

    pub fn vm(&self) -> Option<&instances::Spec> {
        match self {
            Self::Job { spec, .. } => spec.vm.as_ref(),
            Self::Instance { spec } => Some(spec),
        }
    }
    pub fn validate(&self, worker: &Registration) -> Result<()> {
        if let Self::Job {
            spec, memory_mb, ..
        } = self
        {
            spec.validate()?;
            if matches!(self, Workload::Job { gpu: true, .. }) && spec.vm.is_none() {
                ensure!(
                    worker.tags.contains("gpu:metal") || worker.tags.contains("gpu:compute"),
                    "worker does not advertise a compute GPU"
                );
            }
            ensure!(
                spec.resume.is_none() && spec.preempt.is_none(),
                "legacy native resume and preemption are not supported by the replicated worker"
            );
            ensure!(
                spec.vm.is_some() || *memory_mb > 0,
                "native jobs require a memory reservation"
            );
        }
        if let Some(vm) = self.vm() {
            if let Some(required) = &vm.runtime {
                required.accepts(
                    worker
                        .runtimes
                        .get(&vm.image)
                        .context("image platform is not advertised")?,
                )?;
            }
            ensure!(
                vm.cpus > 0 && vm.memory_mb >= 4096 && vm.disk_gb >= 50,
                "invalid VM resources"
            );
            ensure!(
                worker.images.contains(&vm.image),
                "image is not in the enrolled worker catalog"
            );
            ensure!(worker.budget.vm_slots > 0, "worker does not accept VMs");
        }
        Ok(())
    }
}

impl Assignment {
    pub fn validate(&self, worker: &Registration) -> Result<()> {
        instances::valid_id(&self.id)?;
        instances::valid_id(&self.execution)?;
        ensure!(
            self.worker == worker.id && self.revision > 0,
            "invalid assignment identity"
        );
        self.workload.validate(worker)?;
        match (&self.workload, self.desired, &self.attempt) {
            (Workload::Job { spec, .. }, Desired::Job(desired), Some(attempt)) => {
                instances::valid_id(&attempt.job)?;
                ensure!(attempt.number > 0, "invalid attempt number");
                ensure!(
                    attempt.number > 1 || attempt.checkpoint.is_none(),
                    "first attempt cannot resume a checkpoint"
                );
                ensure!(
                    desired != JobDesired::Suspended
                        || (spec.checkpoint.is_some() && self.suspend_checkpoint.is_some()),
                    "suspension requires a selected checkpoint"
                );
            }
            (Workload::Instance { .. }, Desired::Instance(_), None) => {
                ensure!(
                    self.suspend_checkpoint.is_none(),
                    "persistent VM cannot have a job checkpoint"
                );
            }
            _ => {
                anyhow::bail!("assignment lifecycle or attempt provenance does not match workload")
            }
        }
        if let Some((revision, observation)) = &self.observed {
            ensure!(
                *revision > 0
                    && *revision <= self.revision
                    && self.workload.accepts_phase(observation.phase),
                "invalid assignment observation"
            );
        }
        Ok(())
    }
    pub fn terminal(&self) -> bool {
        self.observed
            .as_ref()
            .is_some_and(|(_, o)| o.phase.terminal())
    }
    fn compute_reserved(&self) -> bool {
        if self.terminal() {
            return false;
        }
        if matches!(self.workload, Workload::Job { .. }) {
            return true;
        }
        self.desired.running()
            || !self.observed.as_ref().is_some_and(|(r, o)| {
                *r == self.revision
                    && matches!(
                        o.phase,
                        WorkPhase::Instance(InstancePhase::Stopped)
                            | WorkPhase::Instance(InstancePhase::Deleted)
                    )
            })
    }
}

impl State {
    pub fn add_worker_runtimes(
        &mut self,
        worker: &str,
        incarnation: &str,
        runtimes: &BTreeMap<String, super::portable::Runtime>,
    ) -> Reply {
        let validate = || -> Result<Registration> {
            let old = self
                .workers
                .get(worker)
                .ok_or_else(|| anyhow::anyhow!("unknown worker"))?;
            ensure!(old.incarnation == incarnation, "worker incarnation differs");
            ensure!(
                self.drained.contains(worker),
                "drain worker before updating runtimes"
            );
            ensure!(
                self.recovery_allows_work(worker),
                "worker recovery is not reconciled"
            );
            ensure!(
                self.assignments
                    .values()
                    .filter(|a| a.worker == worker)
                    .all(|a| a
                        .observed
                        .as_ref()
                        .is_some_and(|(r, o)| *r == a.revision && o.phase.terminal())),
                "worker has unfinished or unconfirmed assignments"
            );
            old.with_added_runtimes(runtimes)
        };
        match validate() {
            Ok(next) => {
                self.workers.insert(worker.into(), next);
                Reply::Recorded
            }
            Err(e) => Reply::rejected(&e.to_string()),
        }
    }

    pub fn enroll(&mut self, worker: &Registration) -> Reply {
        if let Err(e) = worker.validate() {
            return Reply::rejected(&e.to_string());
        }
        if let Some(old) = self.workers.get(&worker.id) {
            return if old == worker {
                Reply::Recorded
            } else {
                Reply::rejected(
                    "worker ID is already enrolled; journal/identity replacement requires explicit recovery",
                )
            };
        }
        if self
            .workers
            .values()
            .any(|w| w.peer.identity == worker.peer.identity)
        {
            return Reply::rejected("worker key is already enrolled");
        }
        if self
            .storage
            .stores
            .values()
            .any(|s| s.peer.identity == worker.peer.identity)
        {
            return Reply::rejected("worker needs a separate identity from storage");
        }
        self.workers.insert(worker.id.clone(), worker.clone());
        Reply::Recorded
    }

    pub fn assign(
        &mut self,
        id: &str,
        execution: &str,
        worker: &str,
        workload: &Workload,
    ) -> Reply {
        if !self.recovery_allows_work(worker) {
            return Reply::rejected("recovery is paused or worker is not reconciled");
        }
        let validate = || -> Result<()> {
            instances::valid_id(id)?;
            instances::valid_id(execution)?;
            ensure!(
                !matches!(workload, Workload::Instance { .. }) || !execution.starts_with("job-"),
                "job- is reserved for disposable VM instances"
            );
            ensure!(
                !self.assignments.contains_key(id)
                    && !self.jobs.contains_key(id)
                    && !self.assignments.values().any(|a| a.execution == execution),
                "assignment or execution ID already exists"
            );
            workload.validate(
                self.workers
                    .get(worker)
                    .ok_or_else(|| anyhow::anyhow!("worker is not enrolled"))?,
            )
        };
        if let Err(e) = validate() {
            return Reply::rejected(&e.to_string());
        }
        let candidate = Assignment {
            id: id.into(),
            execution: execution.into(),
            worker: worker.into(),
            workload: workload.clone(),
            desired: workload.initial_desired(),
            suspend_checkpoint: None,
            attempt: matches!(workload, Workload::Job { .. }).then(|| super::queue::Attempt {
                job: id.into(),
                number: 1,
                checkpoint: None,
            }),
            revision: 1,
            observed: None,
        };
        if let Err(e) = self.admit(&candidate) {
            return Reply::rejected(&e.to_string());
        }
        if matches!(workload, Workload::Job { .. }) {
            let mut job = super::queue::Submission {
                inputs: None,
                id: id.into(),
                workload: workload.clone(),
                worker: Some(worker.into()),
                priority: 0,
                requires: BTreeSet::new(),
                after: BTreeSet::new(),
            }
            .job(0);
            job.attempts.push(id.into());
            job.phase = super::queue::JobPhase::Active;
            self.jobs.insert(id.into(), job);
        }
        self.assignments.insert(id.into(), candidate);
        Reply::Accepted {
            job: id.into(),
            revision: 1,
        }
    }

    pub fn desire(&mut self, id: &str, revision: u64, desired: Desired) -> Reply {
        let Some(old) = self.assignments.get(id) else {
            return Reply::rejected("unknown assignment");
        };
        if self.worker_recovery_required(&old.worker) {
            return Reply::rejected("worker must reconcile before changing intent");
        }
        if desired.running() && !self.recovery_allows_work(&old.worker) {
            return Reply::rejected("recovery is paused");
        }
        if self.recovery.as_ref().is_some_and(|r| r.held.contains(id)) && !desired.terminating() {
            return Reply::rejected("assignment held after recovery; cancel or delete explicitly");
        }
        if old.revision != revision {
            return Reply::rejected("revision conflict");
        }
        if old.desired == desired {
            if let Some(recovery) = &mut self.recovery
                && desired.terminating()
            {
                recovery.held.remove(id);
            }
            return Reply::Accepted {
                job: id.into(),
                revision,
            };
        }
        if old.terminal() || old.desired.terminating() {
            return Reply::rejected("assignment is terminal or has a terminal intent");
        }
        let valid = match old.workload {
            Workload::Job { .. } => desired == Desired::Job(JobDesired::Cancelled),
            Workload::Instance { .. } => matches!(desired, Desired::Instance(_)),
        };
        if !valid {
            return Reply::rejected("desired state is invalid for this workload");
        }
        let mut candidate = old.clone();
        candidate.desired = desired;
        candidate.revision += 1;
        // Stopping must remain possible even while stale reservations exceed capacity.
        if desired.running()
            && let Err(e) = self.admit(&candidate)
        {
            return Reply::rejected(&e.to_string());
        }
        let revision = candidate.revision;
        self.assignments.insert(id.into(), candidate);
        if let Some(recovery) = &mut self.recovery {
            recovery.held.remove(id);
        }
        Reply::Accepted {
            job: id.into(),
            revision,
        }
    }

    /// Unknown/offline workers keep their reservations. Only confirmed cleanup releases them.
    pub(crate) fn admit(&self, candidate: &Assignment) -> Result<()> {
        let worker = &self.workers[&candidate.worker];
        let (mut cpu, mut memory, mut disk, mut vms, mut gpu_users, mut exclusive) =
            (0u64, 0u64, 0u64, 0u64, 0u64, false);
        for a in self
            .assignments
            .values()
            .filter(|a| a.worker == candidate.worker && a.id != candidate.id)
            .chain(std::iter::once(candidate))
        {
            if let Some(vm) = a.workload.vm() {
                if !a.terminal() {
                    disk += u64::from(vm.disk_gb);
                }
                if a.compute_reserved() {
                    cpu += u64::from(vm.cpus);
                    memory += u64::from(vm.memory_mb);
                    vms += 1;
                    gpu_users += 1;
                    exclusive |= vm.gpu == instances::Gpu::Exclusive;
                }
            } else if let Workload::Job {
                spec,
                gpu,
                memory_mb,
            } = &a.workload
                && a.compute_reserved()
            {
                cpu += u64::from(spec.threads);
                memory += u64::from(*memory_mb);
                if *gpu {
                    gpu_users += 1;
                    exclusive = true;
                }
            }
        }
        ensure!(
            cpu <= u64::from(worker.budget.cpu_slots),
            "worker CPU budget exhausted"
        );
        ensure!(
            memory <= u64::from(worker.budget.memory_mb),
            "worker memory budget exhausted"
        );
        ensure!(
            disk <= u64::from(worker.budget.disk_gb),
            "worker VM disk budget exhausted"
        );
        ensure!(
            vms <= u64::from(worker.budget.vm_slots),
            "worker VM slots exhausted"
        );
        ensure!(
            !exclusive || gpu_users <= 1,
            "exclusive GPU reservation conflicts"
        );
        Ok(())
    }

    pub fn report(&mut self, principal: &str, report: &Report) -> Reply {
        if self.worker_recovery_required(&report.worker) {
            return Reply::rejected("worker must reconcile before reporting");
        }
        let Some(worker) = self.workers.get(&report.worker) else {
            return Reply::rejected("worker is not enrolled");
        };
        if worker.peer.identity.to_string() != principal || worker.incarnation != report.incarnation
        {
            return Reply::rejected("worker identity or incarnation mismatch");
        }
        let cursor = worker.cursor();
        let previous = self.cursors.get(&cursor).copied().unwrap_or(0);
        if report.sequence <= previous {
            return Reply::Recorded;
        }
        if report.sequence != previous + 1 {
            return Reply::rejected("worker journal gap");
        }
        let Some(a) = self.assignments.get_mut(&report.assignment) else {
            return Reply::rejected("unknown assignment");
        };
        if a.worker != report.worker || report.revision > a.revision || report.revision == 0 {
            return Reply::rejected("observation does not match assignment");
        }
        if !a.workload.accepts_phase(report.observation.phase) {
            return Reply::rejected("observation lifecycle does not match workload");
        }
        if report
            .observation
            .detail
            .as_ref()
            .is_some_and(|d| d.len() > 4096)
        {
            return Reply::rejected("observation detail too large");
        }
        // Consume superseded reports without allowing their old intent to release a current reservation.
        if report.revision < a.revision {
            self.cursors.insert(cursor, report.sequence);
            return Reply::Recorded;
        }
        if report.observation.phase == WorkPhase::Job(AttemptPhase::Suspended)
            && (a.desired != Desired::Job(JobDesired::Suspended)
                || report.revision != a.revision
                || a.suspend_checkpoint
                    .as_ref()
                    .is_none_or(|id| !self.checkpoints.contains_key(id)))
        {
            return Reply::rejected(
                "suspension requires a verified checkpoint and matching stop revision",
            );
        }
        if !a.terminal()
            && a.observed
                .as_ref()
                .is_none_or(|(r, _)| *r <= report.revision)
        {
            a.observed = Some((report.revision, report.observation.clone()));
        }
        self.refresh_job(&report.assignment);
        self.cursors.insert(cursor, report.sequence);
        Reply::Recorded
    }
}
