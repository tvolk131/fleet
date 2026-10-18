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

/// Deterministic application commands. External effects are reconciled by workers after commit.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Mutation {
    ReplaceTunnels {
        expected_revision: u64,
        topology: super::tunnels::Topology,
    },
    PrepareVersion {
        expected_revision: u64,
        target: u32,
    },
    ReplicaPrepared {
        revision: u64,
        node: u64,
    },
    ActivateVersion {
        expected_revision: u64,
    },
    AbortVersion {
        expected_revision: u64,
    },
    HistoryReport(super::history::Report),
    HistoryPolicy {
        job: String,
        expected_revision: u64,
        policy: crate::history::Policy,
    },
    SweepHistory {
        as_of: u64,
    },
    Enqueue(super::queue::Submission),
    EnrollStorage(super::artifact::Registration),
    ReplaceStorage {
        storage: String,
        replacement: String,
        expected_revision: u64,
    },
    RetireStorage {
        storage: String,
        expected_revision: u64,
        expected_inventory: String,
    },
    FinishStorageRetirement {
        storage: String,
        expected_revision: u64,
    },
    DeclareStorageLost {
        storage: String,
        replacement: String,
        expected_revision: u64,
        expected_inventory: String,
        unavailable_artifacts: std::collections::BTreeSet<String>,
    },
    ReviewStorageRecovery {
        expected_catalog: String,
        unavailable_locations: std::collections::BTreeSet<String>,
    },
    StoragePolicy {
        expected_revision: u64,
        policy: super::artifact::Policy,
    },
    ArtifactReady(super::artifact::Ready),
    ArtifactReport(super::artifact::Report),
    ArtifactRetention {
        id: String,
        expected_revision: u64,
        pinned: bool,
        expires_at: Option<u64>,
    },
    DeleteArtifact {
        id: String,
        expected_revision: u64,
    },
    ExpireArtifacts {
        as_of: u64,
    },
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
    /// Add pinned runtime declarations without replacing a worker's identity or history.
    AddWorkerRuntimes {
        worker: String,
        incarnation: String,
        runtimes: BTreeMap<String, super::portable::Runtime>,
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
    CheckpointReady(super::checkpoint::Ready),
    SuspendJob {
        id: String,
        expected_revision: u64,
        checkpoint: String,
    },
    ResumeJob {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        worker: Option<String>,
        id: String,
        expected_revision: u64,
    },
    /// Prepare a durable, serialized administrative transition; completion is observed separately.
    ChangeMembership {
        expected_revision: u64,
        voters: BTreeMap<u64, Peer>,
    },
    /// Release only this principal's full response. Its key and fingerprint remain reserved.
    Release {
        request_id: String,
    },
}

impl Mutation {
    /// Explicitly classify every new operation. Ingress refuses inactive features before
    /// replication, so an older voter never needs to deserialize a newer command variant.
    pub(crate) fn minimum_generation(&self) -> u32 {
        match self {
            Self::ReplaceTunnels { .. } => super::tunnels::GENERATION,
            Self::PrepareVersion { .. }
            | Self::ReplicaPrepared { .. }
            | Self::ActivateVersion { .. }
            | Self::AbortVersion { .. }
            | Self::HistoryReport(..)
            | Self::HistoryPolicy { .. }
            | Self::SweepHistory { .. }
            | Self::Enqueue(..)
            | Self::EnrollStorage(..)
            | Self::ReplaceStorage { .. }
            | Self::RetireStorage { .. }
            | Self::FinishStorageRetirement { .. }
            | Self::DeclareStorageLost { .. }
            | Self::ReviewStorageRecovery { .. }
            | Self::StoragePolicy { .. }
            | Self::ArtifactReady(..)
            | Self::ArtifactReport(..)
            | Self::ArtifactRetention { .. }
            | Self::DeleteArtifact { .. }
            | Self::ExpireArtifacts { .. }
            | Self::Stage { .. }
            | Self::CancelQueued { .. }
            | Self::Place { .. }
            | Self::Drain { .. }
            | Self::RecoverWorker(..)
            | Self::ResumeRecovery { .. }
            | Self::Enroll { .. }
            | Self::AddWorkerRuntimes { .. }
            | Self::Assign { .. }
            | Self::Desire { .. }
            | Self::WorkerReport(..)
            | Self::CheckpointReady(..)
            | Self::SuspendJob { .. }
            | Self::ResumeJob { .. }
            | Self::ChangeMembership { .. }
            | Self::Release { .. } => 1,
        }
    }
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
    pub command: Option<serde_json::Value>,
    pub response: Option<Reply>,
    pub resource: Option<String>,
    pub index: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct State {
    #[serde(default)]
    pub tunnels: super::tunnels::Catalog,
    #[serde(default)]
    pub version: super::version::ClusterVersion,
    #[serde(default)]
    pub history: super::history::Catalog,
    #[serde(default)]
    pub storage: super::artifact::Catalog,
    pub jobs: BTreeMap<String, super::queue::Job>,
    pub checkpoints: BTreeMap<String, super::checkpoint::Checkpoint>,
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
            tunnels: Default::default(),
            // New clusters start at the binary's floor. Deserializing old metadata still
            // defaults to generation one, never silently to the running executable's version.
            version: super::version::ClusterVersion {
                active: super::version::MIN,
                ..Default::default()
            },
            history: Default::default(),
            storage: Default::default(),
            jobs: BTreeMap::new(),
            checkpoints: BTreeMap::new(),
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
        // Diagnostic producers have durable, monotonic stream/copy cursors. Their internal
        // observations converge without retaining an unbounded receipt per chunk or sample.
        // User mutations (including history policy and pins) retain normal request receipts.
        let diagnostic = match &request.mutation {
            Mutation::HistoryReport(r) => self.history_reporter(&principal, r),
            Mutation::ArtifactReady(r) => {
                matches!(r.manifest.kind, super::artifact::Kind::History { .. })
                    && r.manifest
                        .worker()
                        .and_then(|id| self.workers.get(id))
                        .is_some_and(|w| {
                            w.peer.identity.to_string() == principal
                                && w.incarnation == r.incarnation
                        })
            }
            Mutation::ArtifactReport(r) => {
                request.id.starts_with("history-copy-")
                    && self.artifact_reporter(&principal, &r.location, &r.incarnation)
            }
            Mutation::SweepHistory { .. } => principal == "history-maintenance",
            _ => false,
        };
        let reply = self.mutate(&principal, &request.id, &request.mutation, index);
        if matches!(reply, Reply::Recorded | Reply::Accepted { .. }) {
            self.apply_rolling_retention();
            self.apply_history_retention();
        }
        if diagnostic {
            return reply;
        }
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
                command: Some(serde_json::to_value(request.mutation).expect("mutation serializes")),
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
        if mutation.minimum_generation() > self.version.active {
            return Reply::rejected("operation requires an inactive cluster generation");
        }
        match mutation {
            Mutation::ReplaceTunnels {
                expected_revision,
                topology,
            } => self.replace_tunnels(*expected_revision, topology),
            Mutation::PrepareVersion {
                expected_revision,
                target,
            } => self.prepare_version(*expected_revision, *target),
            Mutation::ReplicaPrepared { revision, node } => {
                self.replica_prepared(principal, *revision, *node)
            }
            Mutation::ActivateVersion { expected_revision } => {
                self.activate_version(*expected_revision)
            }
            Mutation::AbortVersion { expected_revision } => self.abort_version(*expected_revision),
            Mutation::HistoryReport(r) => self.history_report(principal, r),
            Mutation::HistoryPolicy {
                job,
                expected_revision,
                policy,
            } => self.history_policy(job, *expected_revision, policy),
            Mutation::SweepHistory { as_of } => self.sweep_history(*as_of),
            Mutation::Enqueue(s) => self.enqueue(s, index),
            Mutation::EnrollStorage(r) => self.enroll_storage(r),
            Mutation::ReplaceStorage {
                storage,
                replacement,
                expected_revision,
            } => self.replace_storage(storage, replacement, *expected_revision),
            Mutation::RetireStorage {
                storage,
                expected_revision,
                expected_inventory,
            } => self.retire_storage(storage, *expected_revision, expected_inventory),
            Mutation::FinishStorageRetirement {
                storage,
                expected_revision,
            } => self.finish_storage_retirement(storage, *expected_revision),
            Mutation::DeclareStorageLost {
                storage,
                replacement,
                expected_revision,
                expected_inventory,
                unavailable_artifacts,
            } => self.lose_storage(
                storage,
                replacement,
                *expected_revision,
                expected_inventory,
                unavailable_artifacts,
            ),
            Mutation::ReviewStorageRecovery {
                expected_catalog,
                unavailable_locations,
            } => self.review_storage_recovery(expected_catalog, unavailable_locations),
            Mutation::StoragePolicy {
                expected_revision,
                policy,
            } => self.storage_policy(*expected_revision, policy),
            Mutation::ArtifactReady(r) => self.artifact_ready(principal, r),
            Mutation::ArtifactReport(r) => self.artifact_report(principal, r),
            Mutation::ArtifactRetention {
                id,
                expected_revision,
                pinned,
                expires_at,
            } => self.artifact_retention(id, *expected_revision, *pinned, *expires_at),
            Mutation::DeleteArtifact {
                id,
                expected_revision,
            } => self.delete_artifact(id, *expected_revision),
            Mutation::ExpireArtifacts { as_of } => self.expire_artifacts(*as_of),
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
            Mutation::AddWorkerRuntimes {
                worker,
                incarnation,
                runtimes,
            } => self.add_worker_runtimes(worker, incarnation, runtimes),
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
            Mutation::CheckpointReady(ready) => self.checkpoint_ready(principal, ready),
            Mutation::SuspendJob {
                id,
                expected_revision,
                checkpoint,
            } => self.suspend_job(id, *expected_revision, checkpoint),
            Mutation::ResumeJob {
                worker,
                id,
                expected_revision,
            } => self.resume_job_on(id, *expected_revision, worker.as_deref()),
            Mutation::ChangeMembership {
                expected_revision,
                voters,
            } => self.prepare_membership(principal, request_id, *expected_revision, voters),
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
        }
    }
}
