//! Membership intent is committed before any learner or voter change. Only a leader reconciles it.
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use anyhow::{Result, ensure};
use openraft::{BasicNode, LogId, StoredMembership};
use serde::{Deserialize, Serialize};

use super::{Raft, Reply, State, Store, model::key, network::Peer};

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct MembershipState {
    /// Stable configuration generation; learner/joint entries do not advance it.
    pub revision: u64,
    pub active: Option<String>,
    /// IDs and keys are never reused, even after retirement.
    pub catalog: BTreeMap<u64, Peer>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum MembershipStage {
    Learning,
    Joint,
    Complete,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MembershipOperation {
    pub id: String,
    pub revision: u64,
    pub voters: BTreeMap<u64, Peer>,
    pub stage: MembershipStage,
    pub completed_at: Option<LogId<u64>>,
}

/// An authenticated but possibly stale discovery hint, never proof of current leadership.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MembershipView {
    pub cluster: String,
    pub log_id: Option<LogId<u64>>,
    pub revision: u64,
    pub voter_sets: Vec<BTreeSet<u64>>,
    pub peers: BTreeMap<u64, Peer>,
    pub active: Option<String>,
}

impl State {
    pub fn membership_view(&self) -> MembershipView {
        MembershipView {
            cluster: self.cluster.clone(),
            log_id: self.last_applied,
            revision: self.configuration.revision,
            voter_sets: self.membership.membership().get_joint_config().clone(),
            peers: self
                .membership
                .nodes()
                .filter_map(|(id, n)| serde_json::from_str(&n.addr).ok().map(|peer| (*id, peer)))
                .collect(),
            active: self.configuration.active.clone(),
        }
    }

    pub fn prepare_membership(
        &mut self,
        principal: &str,
        request: &str,
        expected: u64,
        voters: &BTreeMap<u64, Peer>,
    ) -> Reply {
        let reject = |reason: &str| Reply::Rejected {
            reason: reason.into(),
        };
        if self.configuration.revision == 0 {
            return reject("cluster is not initialized");
        }
        if self.configuration.active.is_some() {
            return reject("another membership operation is active");
        }
        if expected != self.configuration.revision {
            return reject("configuration revision conflict");
        }
        if !matches!(voters.len(), 1 | 3) {
            return reject("qualified voter counts are one or three");
        }
        if voters.keys().any(|id| *id == 0) || voters.values().any(|p| p.validate().is_err()) {
            return reject("nonzero replica IDs and concrete addresses required");
        }
        if voters
            .values()
            .map(|p| p.identity)
            .collect::<BTreeSet<_>>()
            .len()
            != voters.len()
            || voters
                .values()
                .map(|p| p.address)
                .collect::<BTreeSet<_>>()
                .len()
                != voters.len()
        {
            return reject("replica keys and addresses must be distinct");
        }
        let current = self.membership_view().peers;
        for (id, peer) in voters {
            if let Some(old) = self.configuration.catalog.get(id)
                && (old != peer || !current.contains_key(id))
            {
                return reject("replica ID is immutable or retired; enroll a fresh identity");
            }
            if self
                .configuration
                .catalog
                .iter()
                .any(|(old_id, old)| old_id != id && old.identity == peer.identity)
            {
                return reject("replica identity already used by another ID");
            }
            if current
                .iter()
                .any(|(old_id, old)| old_id != id && old.address == peer.address)
            {
                return reject("address is already used by a current replica");
            }
        }
        let id = key(principal, request);
        let unchanged = self.membership.membership().get_joint_config()
            == &vec![voters.keys().copied().collect()];
        let revision = if unchanged { expected } else { expected + 1 };
        self.membership_operations.insert(
            id.clone(),
            MembershipOperation {
                id: id.clone(),
                revision,
                voters: voters.clone(),
                stage: if unchanged {
                    MembershipStage::Complete
                } else {
                    MembershipStage::Learning
                },
                completed_at: unchanged.then_some(self.last_applied).flatten(),
            },
        );
        self.configuration.catalog.extend(voters.clone());
        if !unchanged {
            self.configuration.active = Some(id.clone());
        }
        Reply::MembershipAccepted {
            operation: id,
            revision,
        }
    }

    pub fn apply_membership(&mut self, membership: StoredMembership<u64, BasicNode>) {
        self.membership = membership;
        let view = self.membership_view();
        // OpenRaft's generic storage suite also uses BasicNodes without transport metadata.
        self.configuration.catalog.extend(view.peers);
        if self.configuration.revision == 0 {
            self.configuration.revision = 1;
        }
        if let Some(id) = &self.configuration.active {
            let op = self
                .membership_operations
                .get_mut(id)
                .expect("active membership operation");
            if view.voter_sets.len() > 1 {
                op.stage = MembershipStage::Joint;
            }
            if view.voter_sets == vec![op.voters.keys().copied().collect()] {
                op.stage = MembershipStage::Complete;
                op.completed_at = *self.membership.log_id();
                self.configuration.revision = op.revision;
                self.configuration.active = None;
            }
        }
    }

    /// Current members and explicitly committed prospective members may exchange Raft traffic.
    /// Static seed trust is handled only for a replica that has never applied a configuration.
    pub fn authorized_replicas(&self) -> BTreeMap<u64, Peer> {
        let mut peers = self.membership_view().peers;
        if let Some(op) = self
            .configuration
            .active
            .as_ref()
            .and_then(|id| self.membership_operations.get(id))
        {
            peers.extend(op.voters.clone());
        }
        peers
    }

    pub fn routing_view(&self) -> MembershipView {
        let mut view = self.membership_view();
        // Acceptance teaches clients the approved destinations before the old leader can retire.
        view.peers = self.authorized_replicas();
        view
    }
}

async fn reconcile(raft: &Raft, store: &Store) -> Result<()> {
    raft.ensure_linearizable().await?;
    let leadership = raft.metrics().borrow().vote;
    let state = store.state().await?;
    let Some(id) = state.configuration.active else {
        return Ok(());
    };
    if state.recovery.is_some() && raft.metrics().borrow().purged.is_none() {
        // Recovery seeds application state outside the new Raft log. Replaying that log alone
        // would create a member with an empty historical state. Before admitting any learner,
        // compact a durable snapshot so catching up necessarily transfers the recovery image.
        if raft.metrics().borrow().snapshot.is_none() {
            raft.trigger().snapshot().await?;
        }
        let snapshot = loop {
            let metrics = raft.metrics().borrow().clone();
            ensure!(
                metrics.current_leader == Some(metrics.id) && metrics.vote == leadership,
                "leadership changed while snapshotting recovery seed"
            );
            if let Some(snapshot) = metrics.snapshot {
                break snapshot;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        raft.trigger().purge_log(snapshot.index).await?;
        loop {
            let metrics = raft.metrics().borrow().clone();
            ensure!(
                metrics.current_leader == Some(metrics.id) && metrics.vote == leadership,
                "leadership changed while compacting recovery seed"
            );
            if metrics
                .purged
                .is_some_and(|log| log.index >= snapshot.index)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    let op = &state.membership_operations[&id];
    if state.membership.membership().get_joint_config().len() == 1 {
        let voters: BTreeSet<_> = state.membership.voter_ids().collect();
        for (node, peer) in &op.voters {
            if !voters.contains(node) {
                ensure!(
                    raft.metrics().borrow().vote == leadership,
                    "leadership changed"
                );
                ensure!(
                    store.state().await?.configuration.active.as_ref() == Some(&id),
                    "operation already reconciled"
                );
                let barrier = if state.membership.membership().get_node(node).is_some() {
                    state.last_applied.expect("initialized state")
                } else {
                    raft.add_learner(*node, peer.node(), false).await?.log_id
                };
                // OpenRaft's default lag tolerance can consider even an offline new learner
                // sufficiently close on a tiny log. Require an acknowledged durable position.
                loop {
                    let metrics = raft.metrics().borrow().clone();
                    ensure!(
                        metrics.current_leader == Some(metrics.id) && metrics.vote == leadership,
                        "leadership changed during catch-up"
                    );
                    let matched = metrics
                        .replication
                        .as_ref()
                        .and_then(|r| r.get(node))
                        .copied()
                        .flatten();
                    if matched.is_some_and(|log| log.index >= barrier.index) {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
        }
    }
    raft.ensure_linearizable().await?;
    ensure!(
        raft.metrics().borrow().vote == leadership,
        "leadership changed"
    );
    ensure!(
        store.state().await?.configuration.active.as_ref() == Some(&id),
        "operation already reconciled"
    );
    // If a previous leader left joint consensus committed, this finishes that same transition.
    raft.change_membership(op.voters.keys().copied().collect::<BTreeSet<_>>(), false)
        .await?;
    Ok(())
}

pub fn spawn_reconciler(
    raft: Raft,
    store: Store,
    paused: Arc<AtomicBool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;
            if paused.load(Ordering::SeqCst) {
                continue;
            }
            // Cancellation never cancels committed intent. A later pass resumes from durable state.
            if let Ok(Err(error)) =
                tokio::time::timeout(Duration::from_secs(3), reconcile(&raft, &store)).await
            {
                tracing::debug!(%error, "membership reconciliation will retry");
            }
        }
    })
}
