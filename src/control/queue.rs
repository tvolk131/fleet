//! Durable jobs and deterministic admission; only the leader proposes placements.
use super::{Command, Mutation, Raft, Reply, Request, State, Store, workload::*};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct QueuedJob {
    pub id: String,
    pub workload: Workload,
    pub worker: Option<String>,
    pub priority: i32,
    pub requires: BTreeSet<String>,
    pub after: BTreeSet<String>,
    pub staged: BTreeSet<String>,
    pub submitted: u64,
    pub revision: u64,
    pub cancelled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Submission {
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

pub type Presence = Arc<Mutex<BTreeMap<String, Instant>>>;

impl State {
    pub fn enqueue(&mut self, s: &Submission, index: u64) -> Reply {
        let check = || -> Result<()> {
            crate::instances::valid_id(&s.id)?;
            ensure!(s.id.len() <= 48, "job ID must be at most 48 bytes");
            ensure!(
                !self.queue.contains_key(&s.id) && !self.assignments.contains_key(&s.id),
                "job ID already used"
            );
            let Workload::Job {
                spec, memory_mb, ..
            } = &s.workload
            else {
                anyhow::bail!("queue accepts jobs only")
            };
            spec.validate()?;
            ensure!(
                spec.resume.is_none() && spec.preempt.is_none(),
                "checkpoint continuation and preemption are not implemented"
            );
            ensure!(
                spec.vm.is_some() || *memory_mb > 0,
                "native memory reservation required"
            );
            if let Some(w) = &s.worker {
                ensure!(self.workers.contains_key(w), "unknown target worker");
            }
            ensure!(
                s.after.iter().all(|id| self.queue.contains_key(id)),
                "dependencies must refer to existing logical jobs"
            );
            Ok(())
        };
        if let Err(e) = check() {
            return Reply::rejected(&e.to_string());
        }
        self.queue.insert(
            s.id.clone(),
            QueuedJob {
                id: s.id.clone(),
                workload: s.workload.clone(),
                worker: s.worker.clone(),
                priority: s.priority,
                requires: s.requires.clone(),
                after: s.after.clone(),
                staged: BTreeSet::new(),
                submitted: index,
                revision: 1,
                cancelled: false,
            },
        );
        Reply::Accepted {
            job: s.id.clone(),
            revision: 1,
        }
    }

    pub fn stage_job(&mut self, id: &str, worker: &str) -> Reply {
        let Some(job) = self.queue.get_mut(id) else {
            return Reply::rejected("unknown job");
        };
        let Some(registration) = self.workers.get(worker) else {
            return Reply::rejected("unknown worker");
        };
        if job.cancelled || job.worker.as_ref().is_some_and(|w| w != worker) {
            return Reply::rejected("job cancelled or wrong worker");
        }
        if let Err(e) = job.workload.validate(registration) {
            return Reply::rejected(&e.to_string());
        }
        job.staged.insert(worker.into());
        Reply::Recorded
    }

    pub fn cancel_queued(&mut self, id: &str, revision: u64) -> Reply {
        let Some(job) = self.queue.get(id) else {
            return Reply::rejected("unknown job");
        };
        if job.revision != revision {
            return Reply::rejected("job revision conflict");
        }
        if job.cancelled {
            return Reply::Accepted {
                job: id.into(),
                revision,
            };
        }
        if let Some(a) = self.assignments.get(id) {
            let reply = self.desire(id, a.revision, Desired::Cancelled);
            if matches!(reply, Reply::Rejected { .. }) {
                return reply;
            }
        }
        let job = self.queue.get_mut(id).expect("job");
        job.cancelled = true;
        job.revision += 1;
        Reply::Accepted {
            job: id.into(),
            revision: job.revision,
        }
    }

    pub fn placement_candidate(&self, id: &str, worker: &str) -> Result<Assignment> {
        let job = self
            .queue
            .get(id)
            .ok_or_else(|| anyhow::anyhow!("unknown job"))?;
        ensure!(
            !job.cancelled && !self.assignments.contains_key(id),
            "job is cancelled or already assigned"
        );
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
                self.assignments.get(dependency).is_some_and(|a| a
                    .observed
                    .as_ref()
                    .is_some_and(|(_, o)| o.phase == WorkPhase::Succeeded)),
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
        job.workload.validate(registration)?;
        if let Workload::Job { spec, .. } = &job.workload {
            ensure!(
                spec.inputs.is_empty() || job.staged.contains(worker),
                "waiting for verified inputs on worker"
            );
        }
        let a = Assignment {
            id: id.into(),
            execution: format!("exec-{id}"),
            worker: worker.into(),
            workload: job.workload.clone(),
            desired: Desired::Running,
            revision: 1,
            observed: None,
        };
        ensure!(
            !self
                .assignments
                .values()
                .any(|old| old.execution == a.execution),
            "execution ID already used"
        );
        self.admit(&a)?;
        Ok(a)
    }

    pub fn place_queued(&mut self, id: &str, worker: &str, revision: u64) -> Reply {
        if self.queue.get(id).is_none_or(|j| j.revision != revision) {
            return Reply::rejected("job revision conflict");
        }
        let a = match self.placement_candidate(id, worker) {
            Ok(a) => a,
            Err(e) => return Reply::rejected(&e.to_string()),
        };
        self.assignments.insert(id.into(), a);
        let job = self.queue.get_mut(id).expect("job");
        job.revision += 1;
        Reply::Accepted {
            job: id.into(),
            revision: job.revision,
        }
    }

    pub fn scheduling_order(&self) -> Vec<&QueuedJob> {
        let mut jobs: Vec<_> = self
            .queue
            .values()
            .filter(|j| !j.cancelled && !self.assignments.contains_key(&j.id))
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
                let state = store.state().await?;
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
                            // itself permanently excludes another execution of this logical job.
                            raft.client_write(Command {
                                principal: format!("scheduler:{}", raft.metrics().borrow().id),
                                request: Request {
                                    origin: state.cluster.clone(),
                                    id: format!("place-{:032x}", rand::random::<u128>()),
                                    mutation: Mutation::Place {
                                        id: job.id.clone(),
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
