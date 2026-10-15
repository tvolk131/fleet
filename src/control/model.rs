use std::collections::BTreeMap;

use super::membership::{MembershipOperation, MembershipState};
use super::network::Peer;
use openraft::{BasicNode, LogId, StoredMembership};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Request {
    /// Immutable authority incarnation at request creation; never rewrite on retry.
    pub origin: String,
    pub id: String,
    pub mutation: Mutation,
}

/// The ingress authenticates the principal; it never trusts a client-supplied principal.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Command {
    pub principal: String,
    pub request: Request,
}

/// Lab commands intentionally use explicit placement; production scheduling is a later milestone.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Mutation {
    Enqueue(super::queue::Submission),
    Stage {
        id: String,
        worker: String,
    },
    CancelQueued {
        id: String,
        expected_revision: u64,
    },
    Place {
        id: String,
        worker: String,
        expected_revision: u64,
    },
    Drain {
        worker: String,
        drain: bool,
    },
    RecoverWorker(super::recovery::Inventory),
    ResumeRecovery {
        unavailable_workers: std::collections::BTreeSet<String>,
    },
    Enroll {
        worker: super::workload::Registration,
    },
    Assign {
        id: String,
        execution: String,
        worker: String,
        workload: super::workload::Workload,
    },
    Desire {
        id: String,
        expected_revision: u64,
        desired: super::workload::Desired,
    },
    WorkerReport(super::workload::Report),
    /// Prepare a durable, serialized administrative transition; completion is observed separately.
    ChangeMembership {
        expected_revision: u64,
        voters: BTreeMap<u64, Peer>,
    },
    Submit {
        job: String,
        execution: String,
        worker: String,
        payload: String,
    },
    Cancel {
        job: String,
        expected_revision: u64,
    },
    /// Release only this principal's full response. Its key and fingerprint remain reserved.
    Release {
        request_id: String,
    },
    /// Only lab administrators can submit simulated worker reports.
    Observe {
        worker: String,
        incarnation: String,
        sequence: u64,
        execution: String,
        revision: u64,
        phase: Phase,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum Phase {
    Running,
    Cancelled,
    Succeeded,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Reply {
    RecoveryUnknown { origin: String, current: String },
    MembershipAccepted { operation: String, revision: u64 },
    Accepted { job: String, revision: u64 },
    Rejected { reason: String },
    Recorded,
    Expired { resource: Option<String> },
}

impl Reply {
    pub(crate) fn rejected(reason: &str) -> Self {
        Self::Rejected {
            reason: reason.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Receipt {
    pub origin: String,
    pub fingerprint: String,
    pub command: Option<Mutation>,
    pub response: Option<Reply>,
    pub resource: Option<String>,
    pub index: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Job {
    pub job: String,
    pub execution: String,
    pub worker: String,
    pub payload: String,
    pub revision: u64,
    pub cancelled: bool,
    pub observed: Option<(u64, Phase)>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct State {
    pub queue: BTreeMap<String, super::queue::QueuedJob>,
    pub drained: std::collections::BTreeSet<String>,
    pub recovery: Option<Box<super::recovery::Recovery>>,
    pub format: u32,
    pub cluster: String,
    pub last_applied: Option<LogId<u64>>,
    pub membership: StoredMembership<u64, BasicNode>,
    pub configuration: MembershipState,
    pub membership_operations: BTreeMap<String, MembershipOperation>,
    pub workers: BTreeMap<String, super::workload::Registration>,
    pub assignments: BTreeMap<String, super::workload::Assignment>,
    pub jobs: BTreeMap<String, Job>,
    pub receipts: BTreeMap<String, Receipt>,
    pub cursors: BTreeMap<String, u64>,
}

pub fn key(principal: &str, id: &str) -> String {
    // Encoding a tuple avoids delimiter collisions between arbitrary identifiers.
    serde_json::to_string(&(principal, id)).expect("strings serialize")
}

impl State {
    pub fn new(cluster: String) -> Self {
        Self {
            queue: BTreeMap::new(),
            drained: Default::default(),
            recovery: None,
            format: super::FORMAT,
            cluster,
            last_applied: None,
            membership: StoredMembership::default(),
            configuration: MembershipState::default(),
            membership_operations: BTreeMap::new(),
            workers: BTreeMap::new(),
            assignments: BTreeMap::new(),
            jobs: BTreeMap::new(),
            receipts: BTreeMap::new(),
            cursors: BTreeMap::new(),
        }
    }

    pub fn apply(&mut self, command: Command, index: u64) -> Reply {
        let Command { principal, request } = command;
        let k = key(&principal, &request.id);
        let fingerprint =
            blake3::hash(&serde_json::to_vec(&request.mutation).expect("mutation serializes"))
                .to_hex()
                .to_string();
        if let Some(receipt) = self.receipts.get(&k) {
            return if receipt.fingerprint != fingerprint || receipt.origin != request.origin {
                Reply::rejected("request ID already used with different contents")
            } else {
                receipt.response.clone().unwrap_or_else(|| Reply::Expired {
                    resource: receipt.resource.clone(),
                })
            };
        }
        if request.origin != self.cluster {
            return Reply::RecoveryUnknown {
                origin: request.origin,
                current: self.cluster.clone(),
            };
        }
        let reply = self.mutate(&principal, &request.id, &request.mutation, index);
        let resource = match &reply {
            Reply::Accepted { job, .. } => Some(job.clone()),
            Reply::MembershipAccepted { operation, .. } => Some(operation.clone()),
            _ => None,
        };
        self.receipts.insert(
            k,
            Receipt {
                origin: request.origin,
                fingerprint,
                command: Some(request.mutation),
                response: Some(reply.clone()),
                resource,
                index,
            },
        );
        reply
    }

    fn mutate(
        &mut self,
        principal: &str,
        request_id: &str,
        mutation: &Mutation,
        index: u64,
    ) -> Reply {
        if self.recovery.is_some()
            && matches!(
                mutation,
                Mutation::Submit { .. } | Mutation::Cancel { .. } | Mutation::Observe { .. }
            )
        {
            return Reply::rejected(
                "simulated workloads require manual recovery; use real worker assignments",
            );
        }
        match mutation {
            Mutation::Enqueue(s) => self.enqueue(s, index),
            Mutation::Stage { id, worker } => self.stage_job(id, worker),
            Mutation::CancelQueued {
                id,
                expected_revision,
            } => self.cancel_queued(id, *expected_revision),
            Mutation::Place {
                id,
                worker,
                expected_revision,
            } => self.place_queued(id, worker, *expected_revision),
            Mutation::Drain { worker, drain } => {
                if !self.workers.contains_key(worker) {
                    return Reply::rejected("unknown worker");
                }
                if *drain {
                    self.drained.insert(worker.clone());
                } else {
                    self.drained.remove(worker);
                }
                Reply::Recorded
            }
            Mutation::RecoverWorker(inventory) => self.recover_worker(principal, inventory),
            Mutation::ResumeRecovery {
                unavailable_workers,
            } => self.resume_recovery(unavailable_workers),
            Mutation::Enroll { worker } => self.enroll(worker),
            Mutation::Assign {
                id,
                execution,
                worker,
                workload,
            } => self.assign(id, execution, worker, workload),
            Mutation::Desire {
                id,
                expected_revision,
                desired,
            } => self.desire(id, *expected_revision, *desired),
            Mutation::WorkerReport(report) => self.report(principal, report),
            Mutation::ChangeMembership {
                expected_revision,
                voters,
            } => self.prepare_membership(principal, request_id, *expected_revision, voters),
            Mutation::Submit {
                job,
                execution,
                worker,
                payload,
            } => {
                if job.is_empty()
                    || execution.is_empty()
                    || worker.is_empty()
                    || payload.len() > 4096
                {
                    return Reply::rejected(
                        "nonempty IDs and payload of at most 4096 bytes required",
                    );
                }
                if self.jobs.contains_key(job)
                    || self.jobs.values().any(|j| &j.execution == execution)
                {
                    return Reply::rejected("job or execution ID already exists");
                }
                self.jobs.insert(
                    job.clone(),
                    Job {
                        job: job.clone(),
                        execution: execution.clone(),
                        worker: worker.clone(),
                        payload: payload.clone(),
                        revision: 1,
                        cancelled: false,
                        observed: None,
                    },
                );
                Reply::Accepted {
                    job: job.clone(),
                    revision: 1,
                }
            }
            Mutation::Cancel {
                job,
                expected_revision,
            } => {
                let Some(j) = self.jobs.get_mut(job) else {
                    return Reply::rejected("unknown job");
                };
                if j.revision != *expected_revision {
                    return Reply::rejected("revision conflict");
                }
                if !j.cancelled {
                    j.revision += 1;
                    j.cancelled = true;
                }
                Reply::Accepted {
                    job: job.clone(),
                    revision: j.revision,
                }
            }
            Mutation::Release { request_id: target } => {
                if target == request_id {
                    return Reply::rejected("cannot release this request itself");
                }
                let Some(r) = self.receipts.get_mut(&key(principal, target)) else {
                    return Reply::rejected("unknown request");
                };
                r.command = None;
                r.response = None;
                Reply::Recorded
            }
            Mutation::Observe {
                worker,
                incarnation,
                sequence,
                execution,
                revision,
                phase,
            } => {
                if incarnation.is_empty() {
                    return Reply::rejected("missing journal incarnation");
                }
                let cursor_key = key(worker, incarnation);
                let old = *self.cursors.get(&cursor_key).unwrap_or(&0);
                if *sequence <= old {
                    return Reply::Recorded;
                }
                if old.checked_add(1) != Some(*sequence) {
                    return Reply::rejected("worker event gap");
                }
                let Some(j) = self
                    .jobs
                    .values_mut()
                    .find(|j| &j.execution == execution && &j.worker == worker)
                else {
                    return Reply::rejected("unknown worker execution");
                };
                if *revision > j.revision {
                    return Reply::rejected("observation ahead of desired revision");
                }
                let previous = j.observed;
                let terminal = previous.is_some_and(|(_, p)| p != Phase::Running);
                if !terminal && previous.is_none_or(|(r, _)| *revision >= r) {
                    j.observed = Some((*revision, *phase));
                }
                self.cursors.insert(cursor_key, *sequence);
                Reply::Recorded
            }
        }
    }
}
