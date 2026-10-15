//! Committed placement and worker observations. Bulk logs and files stay off the Raft log.
use super::{Reply, State, network::Peer};
use crate::{instances, jobs};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum Desired {
    Running,
    Stopped,
    Deleted,
    Cancelled,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum WorkPhase {
    Pending,
    Running,
    Stopped,
    Succeeded,
    Failed,
    Cancelled,
    Deleted,
    Uncertain,
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
    pub starts_allowed: bool,
    pub recovery_required: bool,
    pub held: BTreeSet<String>,
    pub registration: Registration,
    pub assignments: Vec<Assignment>,
    pub acknowledged: u64,
}

impl Registration {
    pub fn validate(&self) -> Result<()> {
        self.peer.validate()?;
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
                "checkpoint suspension is not part of the replicated worker contract yet"
            );
            ensure!(
                spec.vm.is_some() || *memory_mb > 0,
                "native jobs require a memory reservation"
            );
        }
        if let Some(vm) = self.vm() {
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
    pub fn terminal(&self) -> bool {
        self.observed
            .as_ref()
            .is_some_and(|(_, o)| match self.workload {
                Workload::Job { .. } => matches!(
                    o.phase,
                    WorkPhase::Succeeded | WorkPhase::Failed | WorkPhase::Cancelled
                ),
                Workload::Instance { .. } => o.phase == WorkPhase::Deleted,
            })
    }
    fn compute_reserved(&self) -> bool {
        if self.terminal() {
            return false;
        }
        if matches!(self.workload, Workload::Job { .. }) {
            return true;
        }
        self.desired == Desired::Running
            || !self.observed.as_ref().is_some_and(|(r, o)| {
                *r == self.revision && matches!(o.phase, WorkPhase::Stopped | WorkPhase::Deleted)
            })
    }
}

impl State {
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
                    && !self.queue.contains_key(id)
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
            desired: Desired::Running,
            revision: 1,
            observed: None,
        };
        if let Err(e) = self.admit(&candidate) {
            return Reply::rejected(&e.to_string());
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
        if desired == Desired::Running && !self.recovery_allows_work(&old.worker) {
            return Reply::rejected("recovery is paused");
        }
        if self.recovery.as_ref().is_some_and(|r| r.held.contains(id))
            && !matches!(desired, Desired::Cancelled | Desired::Deleted)
        {
            return Reply::rejected("assignment held after recovery; cancel or delete explicitly");
        }
        if old.revision != revision {
            return Reply::rejected("revision conflict");
        }
        if old.desired == desired {
            if let Some(recovery) = &mut self.recovery
                && matches!(desired, Desired::Cancelled | Desired::Deleted)
            {
                recovery.held.remove(id);
            }
            return Reply::Accepted {
                job: id.into(),
                revision,
            };
        }
        if old.terminal() || old.desired == Desired::Deleted || old.desired == Desired::Cancelled {
            return Reply::rejected("assignment is terminal or has a terminal intent");
        }
        let valid = match old.workload {
            Workload::Job { .. } => desired == Desired::Cancelled,
            Workload::Instance { .. } => desired != Desired::Cancelled,
        };
        if !valid {
            return Reply::rejected("desired state is invalid for this workload");
        }
        let mut candidate = old.clone();
        candidate.desired = desired;
        candidate.revision += 1;
        // Stopping must remain possible even while stale reservations exceed capacity.
        if desired == Desired::Running
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
        if report
            .observation
            .detail
            .as_ref()
            .is_some_and(|d| d.len() > 4096)
        {
            return Reply::rejected("observation detail too large");
        }
        if !a.terminal()
            && a.observed
                .as_ref()
                .is_none_or(|(r, _)| *r <= report.revision)
        {
            a.observed = Some((report.revision, report.observation.clone()));
        }
        self.cursors.insert(cursor, report.sequence);
        Reply::Recorded
    }
}
