//! Logical jobs, immutable execution attempts, and deterministic admission.
use super::{
    Command, Mutation, Raft, Reply, Request, State, Store,
    ids::{AttemptId, JobId},
    workload::*,
};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum JobPhase {
    Queued,
    Active,
    Suspending,
    Suspended,
    Succeeded,
    Failed,
    Cancelling,
    Cancelled,
}
impl JobPhase {
    pub fn terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }
}

/// Immutable provenance of a job's worker assignment. The assignment itself is its execution attempt.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Attempt {
    pub job: JobId,
    pub number: u32,
    pub checkpoint: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Job {
    pub id: JobId,
    /// Original submitted intent; an attempt gets its own immutable specification.
    pub workload: Workload,
    pub worker: Option<String>,
    pub priority: i32,
    pub requires: BTreeSet<String>,
    pub after: BTreeSet<String>,
    pub staged: BTreeSet<String>,
    pub submitted: u64,
    pub revision: u64,
    pub phase: JobPhase,
    pub attempts: Vec<AttemptId>,
    pub resume_from: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_bundle: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resume_worker: Option<String>,
}
impl Job {
    pub fn current<'a>(&self, state: &'a State) -> Option<&'a Assignment> {
        self.attempts
            .last()
            .and_then(|id| state.assignments.get(id.as_ref()))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Submission {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inputs: Option<super::artifact::Manifest>,
    pub id: String,
    pub workload: Workload,
    pub worker: Option<String>,
    #[serde(default)]
    pub priority: i32,
    #[serde(default)]
    pub requires: BTreeSet<String>,
    #[serde(default)]
    pub after: BTreeSet<String>,
}
impl Submission {
    pub(crate) fn job(&self, index: u64) -> Job {
        Job {
            id: self.id.clone().into(),
            workload: self.workload.clone(),
            worker: self.worker.clone(),
            priority: self.priority,
            requires: self.requires.clone(),
            after: self.after.clone(),
            staged: BTreeSet::new(),
            submitted: index,
            revision: 1,
            phase: JobPhase::Queued,
            attempts: vec![],
            resume_from: None,
            input_bundle: self.inputs.as_ref().map(|m| m.id.clone()),
            resume_worker: None,
        }
    }
}
pub type Presence = Arc<Mutex<BTreeMap<String, Instant>>>;

impl State {
    pub fn enqueue(&mut self, s: &Submission, index: u64) -> Reply {
        let job = match self.admit_submission(s, index) {
            Ok(job) => job,
            Err(error) => return Reply::rejected(&error.to_string()),
        };
        if let Some(m) = &s.inputs {
            self.register_artifact(m.clone());
        }
        self.jobs.insert(s.id.clone(), job);
        Reply::Accepted {
            job: s.id.clone(),
            revision: 1,
        }
    }

    /// Every job construction path shares admission, including targeted administrative jobs.
    pub(crate) fn admit_submission(&self, s: &Submission, index: u64) -> Result<Job> {
        crate::instances::valid_id(&s.id)?;
        ensure!(s.id.len() <= 48, "job ID must be at most 48 bytes");
        ensure!(
            !self.jobs.contains_key(&s.id) && !self.assignments.contains_key(&s.id),
            "job ID already used"
        );
        let Workload::Job {
            spec, memory_mb, ..
        } = &s.workload
        else {
            anyhow::bail!("queue accepts jobs only")
        };
        spec.validate()?;
        super::portable::validate_inputs(&s.id, &s.workload, s.inputs.as_ref())?;
        if s.inputs.is_some() {
            ensure!(
                !self.storage.policy.inputs.is_empty(),
                "configure input storage targets before submitting portable jobs"
            );
        }
        ensure!(
            !spec
                .inputs
                .keys()
                .any(|p| p == crate::checkpoint::INPUT_ROOT
                    || p.starts_with(&format!("{}/", crate::checkpoint::INPUT_ROOT))),
            "input path reserved for checkpoint staging"
        );
        ensure!(
            spec.resume.is_none() && spec.preempt.is_none(),
            "legacy checkpoint continuation and preemption are not supported"
        );
        ensure!(
            spec.vm.is_some() || *memory_mb > 0,
            "native memory reservation required"
        );
        if let Some(w) = &s.worker {
            ensure!(self.workers.contains_key(w), "unknown target worker");
        }
        ensure!(
            s.after.iter().all(|id| self.jobs.contains_key(id)),
            "dependencies must refer to existing logical jobs"
        );
        Ok(s.job(index))
    }
    pub fn current_attempt(&self, id: &str) -> Option<&Assignment> {
        self.jobs.get(id)?.current(self)
    }
    pub fn stage_job(&mut self, id: &str, worker: &str) -> Reply {
        let Some(job) = self.jobs.get_mut(id) else {
            return Reply::rejected("unknown job");
        };
        let Some(registration) = self.workers.get(worker) else {
            return Reply::rejected("unknown worker");
        };
        if job.phase != JobPhase::Queued || job.worker.as_ref().is_some_and(|w| w != worker) {
            return Reply::rejected("job is not queued or wrong worker");
        }
        if let Err(e) = job.workload.validate(registration) {
            return Reply::rejected(&e.to_string());
        }
        job.staged.insert(worker.into());
        Reply::Recorded
    }
    pub fn cancel_queued(&mut self, id: &str, revision: u64) -> Reply {
        let Some(job) = self.jobs.get(id) else {
            return Reply::rejected("unknown job");
        };
        if job.revision != revision {
            return Reply::rejected("job revision conflict");
        }
        if matches!(job.phase, JobPhase::Cancelled | JobPhase::Cancelling) {
            return Reply::Accepted {
                job: id.into(),
                revision,
            };
        }
        if job.phase.terminal() {
            return Reply::rejected("job already finished");
        }
        let active = job
            .current(self)
            .filter(|a| !a.terminal())
            .map(|a| (a.id.clone(), a.revision));
        if let Some((attempt, r)) = &active {
            let reply = self.desire(attempt, *r, Desired::Job(JobDesired::Cancelled));
            if matches!(reply, Reply::Rejected { .. }) {
                return reply;
            }
        }
        let job = self.jobs.get_mut(id).expect("job");
        job.phase = if active.is_some() {
            JobPhase::Cancelling
        } else {
            JobPhase::Cancelled
        };
        job.revision += 1;
        Reply::Accepted {
            job: id.into(),
            revision: job.revision,
        }
    }
    pub fn placement_candidate(&self, id: &str, worker: &str) -> Result<Assignment> {
        let job = self
            .jobs
            .get(id)
            .ok_or_else(|| anyhow::anyhow!("unknown job"))?;
        ensure!(job.phase == JobPhase::Queued, "job is not queued");
        for attempt in &job.attempts {
            let a = self
                .assignments
                .get(attempt.as_ref())
                .ok_or_else(|| anyhow::anyhow!("missing attempt history"))?;
            ensure!(
                a.terminal()
                    && !self
                        .recovery
                        .as_ref()
                        .is_some_and(|r| r.held.contains(&a.id)),
                "previous attempt has not confirmed cleanup"
            );
        }
        ensure!(
            self.recovery_allows_work(worker),
            "recovery reconciliation required"
        );
        ensure!(!self.drained.contains(worker), "worker is draining");
        ensure!(
            job.worker.as_ref().is_none_or(|w| w == worker),
            "different target worker"
        );
        for dependency in &job.after {
            ensure!(
                self.jobs
                    .get(dependency)
                    .is_some_and(|j| j.phase == JobPhase::Succeeded),
                "waiting for successful dependency {dependency}"
            );
        }
        let registration = self
            .workers
            .get(worker)
            .ok_or_else(|| anyhow::anyhow!("unknown worker"))?;
        ensure!(
            job.requires.is_subset(&registration.tags),
            "worker does not match required tags"
        );
        ensure!(
            job.resume_worker.as_ref().is_none_or(|w| w == worker),
            "different resume target worker"
        );
        self.portable_ready(job)?;
        let workload = self.attempt_workload(job)?;
        workload.validate(registration)?;
        if super::portable::runtime(&job.workload).is_none()
            && let Some(checkpoint) = job.resume_from.as_ref()
        {
            ensure!(
                self.checkpoints
                    .get(checkpoint)
                    .is_some_and(|c| c.worker == worker),
                "checkpoint is stored on a different worker"
            );
        }
        if let Workload::Job { spec, .. } = &job.workload {
            ensure!(
                super::portable::runtime(&job.workload).is_some()
                    || spec.inputs.is_empty()
                    || job.staged.contains(worker),
                "waiting for verified inputs on worker"
            );
        }
        let number = u32::try_from(job.attempts.len() + 1)?;
        let attempt_id = format!("{id}-a{number}");
        let a = Assignment {
            suspend_checkpoint: None,
            id: attempt_id.clone(),
            execution: format!("exec-{attempt_id}"),
            attempt: Some(Attempt {
                job: job.id.clone(),
                number,
                checkpoint: job.resume_from.clone(),
            }),
            worker: worker.into(),
            workload,
            desired: Desired::Job(JobDesired::Running),
            revision: 1,
            observed: None,
        };
        ensure!(
            !self.assignments.contains_key(&a.id)
                && !self
                    .assignments
                    .values()
                    .any(|old| old.execution == a.execution),
            "attempt or backend execution ID already used"
        );
        self.admit(&a)?;
        Ok(a)
    }
    pub fn place_queued(&mut self, id: &str, worker: &str, revision: u64) -> Reply {
        if self.jobs.get(id).is_none_or(|j| j.revision != revision) {
            return Reply::rejected("job revision conflict");
        }
        let a = match self.placement_candidate(id, worker) {
            Ok(a) => a,
            Err(e) => return Reply::rejected(&e.to_string()),
        };
        let job = &self.jobs[id];
        let bundle = job.input_bundle.clone();
        let checkpoint = job.resume_from.clone();
        let location = format!("worker/{worker}");
        for artifact in self.storage.artifacts.values_mut().filter(|a| {
            bundle.as_ref() == Some(&a.manifest.id)
                || matches!(&a.manifest.kind, super::artifact::Kind::Checkpoint(id)
                    if Some(id) == checkpoint.as_ref())
        }) {
            if let std::collections::btree_map::Entry::Vacant(entry) =
                artifact.copies.entry(location.clone())
            {
                entry.insert(Default::default());
                artifact.revision += 1;
            }
        }
        let attempt_id = a.id.clone();
        self.assignments.insert(attempt_id.clone(), a);
        let job = self.jobs.get_mut(id).expect("job");
        job.attempts.push(attempt_id.into());
        job.phase = JobPhase::Active;
        job.revision += 1;
        Reply::Accepted {
            job: id.into(),
            revision: job.revision,
        }
    }
    /// Reports from historical attempts never mutate the logical job's current lifecycle.
    pub(crate) fn refresh_job(&mut self, assignment: &str) {
        let Some(a) = self.assignments.get(assignment) else {
            return;
        };
        let Some(attempt) = &a.attempt else {
            return;
        };
        let Some(job) = self.jobs.get_mut(attempt.job.as_ref()) else {
            return;
        };
        if job
            .attempts
            .last()
            .is_none_or(|id| id.as_ref() != assignment)
            || job.phase.terminal()
            || job.phase == JobPhase::Queued
        {
            return;
        }
        let phase = match a.observed.as_ref().map(|(_, o)| o.phase) {
            Some(WorkPhase::Job(AttemptPhase::Succeeded)) => JobPhase::Succeeded,
            Some(WorkPhase::Job(AttemptPhase::Failed)) => JobPhase::Failed,
            Some(WorkPhase::Job(AttemptPhase::Cancelled)) => JobPhase::Cancelled,
            Some(WorkPhase::Job(AttemptPhase::Suspended)) => JobPhase::Suspended,
            _ => return,
        };
        if job.phase != phase {
            job.phase = phase;
            job.revision += 1;
        }
    }
    pub fn scheduling_order(&self) -> Vec<&Job> {
        let mut jobs: Vec<_> = self
            .jobs
            .values()
            .filter(|j| j.phase == JobPhase::Queued)
            .collect();
        jobs.sort_by_key(|j| (std::cmp::Reverse(j.priority), j.submitted, j.id.clone()));
        jobs
    }
}

pub fn spawn(raft: Raft, store: Store, presence: Presence) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(250)).await;
            let attempt = async {
                raft.ensure_linearizable().await?;
                let state = store.view().await?;
                let now = crate::jobs::now();
                if !state.history.attempts.is_empty()
                    && now.saturating_sub(state.history.swept_at) >= 60
                {
                    raft.client_write(Command {
                        principal: "history-maintenance".into(),
                        request: Request {
                            origin: state.cluster.clone(),
                            id: format!("history-sweep-{now}"),
                            mutation: Mutation::SweepHistory { as_of: now },
                        },
                    })
                    .await?;
                }
                if state.version.active >= super::vm::GENERATION
                    && state.vm.operations.values().any(|o| {
                        !o.released
                            && o.outcome.phase.terminal()
                            && o.outcome
                                .finished_at
                                .is_some_and(|t| t.saturating_add(o.retention_seconds) <= now)
                    })
                {
                    raft.client_write(Command {
                        principal: "vm-maintenance".into(),
                        request: Request {
                            origin: state.cluster.clone(),
                            id: format!("vm-sweep-{now}"),
                            mutation: Mutation::Vm(super::vm::Action::Sweep { as_of: now }),
                        },
                    })
                    .await?;
                }
                let live: BTreeSet<_> = presence
                    .lock()
                    .expect("presence")
                    .iter()
                    .filter(|(_, t)| t.elapsed() < Duration::from_secs(10))
                    .map(|(w, _)| w.clone())
                    .collect();
                for job in state.scheduling_order() {
                    for worker in &live {
                        if state.placement_candidate(&job.id, worker).is_ok() {
                            // The state machine rechecks reservations and job revisions atomically.
                            // A failed proposal gets a new ID next pass, while committed placement
                            // itself excludes a second concurrent attempt of this logical job.
                            raft.client_write(Command {
                                principal: format!("scheduler:{}", raft.metrics().borrow().id),
                                request: Request {
                                    origin: state.cluster.clone(),
                                    id: format!("place-{:032x}", rand::random::<u128>()),
                                    mutation: Mutation::Place {
                                        id: job.id.to_string(),
                                        worker: worker.clone(),
                                        expected_revision: job.revision,
                                    },
                                },
                            })
                            .await?;
                            return Ok::<_, anyhow::Error>(());
                        }
                    }
                }
                Ok(())
            };
            if let Ok(Err(e)) = tokio::time::timeout(Duration::from_secs(4), attempt).await {
                tracing::debug!(error=%e,"scheduler will retry");
            }
        }
    })
}
