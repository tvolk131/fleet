//! A rolling upgrade installs binaries first, then explicitly activates cluster behavior.
//! Command application is independent of the executing binary's supported range.
use super::{Client, Mutation, Reply, Request, State, Store};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, time::Duration};

pub const WIRE: u32 = 1;
pub const MIN: u32 = 4;
pub const MAX: u32 = 5;

/// Inspect before opening the real database for writes. An unclean redb file needs
/// repair; repair a private scratch copy under the source lock, never the source.
pub(crate) fn preflight(path: &std::path::Path, table: &str) -> Result<Option<serde_json::Value>> {
    use redb::{ReadableDatabase, TableDefinition};
    use std::{fs::OpenOptions, os::unix::fs::OpenOptionsExt};
    if !path.exists() {
        return Ok(None);
    }
    let read = |tx: redb::ReadTransaction| -> Result<serde_json::Value> {
        let table = tx.open_table(TableDefinition::<&str, &[u8]>::new(table))?;
        let mut result = serde_json::Map::new();
        for key in ["identity", "state", "version_prepared"] {
            if let Some(v) = table.get(key)? {
                result.insert(key.into(), serde_json::from_slice(v.value())?);
            }
        }
        Ok(result.into())
    };
    match redb::ReadOnlyDatabase::open(path) {
        Ok(db) => Ok(Some(read(db.begin_read()?)?)),
        Err(redb::DatabaseError::RepairAborted) => {
            let mut source = std::fs::File::open(path)?;
            source.try_lock()?;
            struct Scratch(std::path::PathBuf);
            impl Drop for Scratch {
                fn drop(&mut self) {
                    let _ = std::fs::remove_file(&self.0);
                }
            }
            let scratch = Scratch(std::env::temp_dir().join(format!(
                "fleet-version-preflight-{:032x}.redb",
                rand::random::<u128>()
            )));
            let mut copy = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&scratch.0)?;
            std::io::copy(&mut source, &mut copy)?;
            copy.sync_all()?;
            drop(copy);
            let db = redb::Database::open(&scratch.0)?;
            let value = read(db.begin_read()?)?;
            drop(db);
            Ok(Some(value))
        }
        Err(error) => Err(error.into()),
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct Software {
    pub min: u32,
    pub max: u32,
}
impl Software {
    pub const fn current() -> Self {
        Self { min: MIN, max: MAX }
    }
    pub fn overlaps(self, other: Self) -> bool {
        self.min > 0
            && self.min <= self.max
            && self.max - self.min <= 1
            && other.min > 0
            && other.min <= other.max
            && other.max - other.min <= 1
            && self.min <= other.max
            && other.min <= self.max
    }
    pub fn supports(self, generation: u32) -> bool {
        self.min > 0
            && self.min <= self.max
            && self.max - self.min <= 1
            && (self.min..=self.max).contains(&generation)
    }
    pub fn require(self, generation: u32) -> Result<()> {
        ensure!(
            self.supports(generation),
            "incompatible cluster generation {generation}; binary supports {}..={}; no automatic migration",
            self.min,
            self.max
        );
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Preparation {
    pub target: u32,
    pub members: BTreeSet<u64>,
    pub ready: BTreeSet<u64>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterVersion {
    pub active: u32,
    pub revision: u64,
    pub pending: Option<Preparation>,
}
impl Default for ClusterVersion {
    fn default() -> Self {
        Self {
            active: 1,
            revision: 0,
            pending: None,
        }
    }
}
impl ClusterVersion {
    /// A prepared member has promised not to restart with an incapable executable.
    pub fn required(&self, node: u64) -> u32 {
        self.pending
            .as_ref()
            .filter(|p| p.ready.contains(&node))
            .map_or(self.active, |p| p.target)
    }
}
impl State {
    pub(crate) fn prepare_version(&mut self, revision: u64, target: u32) -> Reply {
        if revision != self.version.revision || self.version.pending.is_some() {
            return Reply::rejected("version revision conflict or preparation already active");
        }
        if self.version.active.checked_add(1) != Some(target) {
            return Reply::rejected("only the next compatibility generation can be prepared");
        }
        if self.configuration.active.is_some()
            || self.membership.log_id().is_none()
            || self.membership.membership().get_joint_config().len() != 1
            || self.recovery.as_ref().is_some_and(|r| !r.resumed)
        {
            return Reply::rejected("settle membership and recovery before preparing an upgrade");
        }
        self.version.revision += 1;
        self.version.pending = Some(Preparation {
            target,
            members: self.membership_view().peers.keys().copied().collect(),
            ready: BTreeSet::new(),
        });
        Reply::Recorded
    }
    pub(crate) fn replica_prepared(&mut self, principal: &str, revision: u64, node: u64) -> Reply {
        let peer = self.authorized_replicas().get(&node).cloned();
        let Some(pending) = &mut self.version.pending else {
            return Reply::rejected("no version preparation");
        };
        if revision != self.version.revision
            || !pending.members.contains(&node)
            || !peer.is_some_and(|p| p.identity.to_string() == principal)
        {
            return Reply::rejected(
                "preparation requires the replica identity and current revision",
            );
        }
        pending.ready.insert(node);
        Reply::Recorded
    }
    pub(crate) fn activate_version(&mut self, revision: u64) -> Reply {
        let Some(p) = &self.version.pending else {
            return Reply::rejected("no version preparation");
        };
        if revision != self.version.revision
            || p.members.is_empty()
            || p.ready != p.members
            || self.configuration.active.is_some()
            || p.members != self.membership_view().peers.keys().copied().collect()
        {
            return Reply::rejected("all current replicas must durably prepare before activation");
        }
        self.version.active = p.target;
        self.version.pending = None;
        self.version.revision += 1;
        Reply::Recorded
    }
    pub(crate) fn abort_version(&mut self, revision: u64) -> Reply {
        if revision != self.version.revision || self.version.pending.is_none() {
            return Reply::rejected("version revision conflict or no preparation");
        }
        self.version.pending = None;
        self.version.revision += 1;
        Reply::Recorded
    }
}

/// Every replica prepares its own disk before acknowledging through the replicated journal.
/// No quorum is required to restart; a quorum is required to record readiness or activate.
pub fn spawn(
    raft: super::Raft,
    store: Store,
    mut client: Client,
    node: u64,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let tick = async {
                let state = store.view().await?;
                let Some(p) = &state.version.pending else {
                    return Ok::<_, anyhow::Error>(());
                };
                if !p.members.contains(&node) || p.ready.contains(&node) {
                    return Ok(());
                }
                Software::current().require(p.target)?;
                store
                    .prepare_version(state.version.revision, p.target)
                    .await?;
                client.learn(state.routing_view())?;
                let request = Request {
                    origin: state.cluster,
                    id: format!("replica-version-{}-{node}", state.version.revision),
                    mutation: Mutation::ReplicaPrepared {
                        revision: state.version.revision,
                        node,
                    },
                };
                if raft.metrics().borrow().current_leader == Some(node) {
                    // iroh deliberately refuses dialing one's own endpoint. Use the same Raft
                    // command path locally; followers still authenticate through the network.
                    raft.client_write(super::Command {
                        principal: client.endpoint.id().to_string(),
                        request,
                    })
                    .await?;
                } else {
                    client.write(request, Duration::from_secs(3)).await?;
                }
                Ok(())
            };
            if let Err(error) = tick.await {
                tracing::debug!(%error, "version preparation waiting");
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::network::Peer;
    use openraft::{BasicNode, CommittedLeaderId, LogId, Membership, StoredMembership};
    use std::collections::BTreeMap;

    fn state() -> State {
        let mut s = State::new("cluster".into());
        let nodes: BTreeMap<u64, BasicNode> = (1..=3)
            .map(|id| {
                (
                    id,
                    Peer {
                        identity: iroh::SecretKey::generate().public(),
                        address: format!("127.0.0.1:{}", 19000 + id).parse().unwrap(),
                    }
                    .node(),
                )
            })
            .collect();
        s.apply_membership(StoredMembership::new(
            Some(LogId::new(CommittedLeaderId::new(1, 1), 0)),
            Membership::new(vec![BTreeSet::from([1, 2, 3])], nodes),
        ));
        s
    }
    #[test]
    fn activation_requires_every_persisted_promise_and_freezes_membership() {
        let mut s = state();
        assert!(matches!(
            s.prepare_version(0, MAX + 1),
            Reply::Rejected { .. }
        ));
        assert_eq!(s.version.revision, 0);
        assert_eq!(s.prepare_version(0, MAX), Reply::Recorded);
        assert!(matches!(s.activate_version(1), Reply::Rejected { .. }));
        assert!(matches!(
            s.replica_prepared("admin", 1, 1),
            Reply::Rejected { .. }
        ));
        let members = s.membership_view().peers;
        assert!(matches!(
            s.prepare_membership("admin", "membership", s.configuration.revision, &members),
            Reply::Rejected { .. }
        ));
        for (id, p) in members {
            assert_eq!(
                s.replica_prepared(&p.identity.to_string(), 1, id),
                Reply::Recorded
            );
        }
        assert_eq!(s.activate_version(1), Reply::Recorded);
        assert_eq!(s.version.active, MAX);
        assert!(matches!(s.abort_version(1), Reply::Rejected { .. }));
    }
    #[test]
    fn abort_invalidates_delayed_acknowledgements_and_allows_next_attempt() {
        let mut s = state();
        let principal = s.membership_view().peers[&1].identity.to_string();
        assert_eq!(s.prepare_version(0, MAX), Reply::Recorded);
        assert_eq!(s.abort_version(1), Reply::Recorded);
        assert_eq!(s.prepare_version(2, MAX), Reply::Recorded);
        assert!(matches!(
            s.replica_prepared(&principal, 1, 1),
            Reply::Rejected { .. }
        ));
        assert_eq!(s.version.active, MIN);
        assert!(s.version.pending.unwrap().ready.is_empty());
    }
    #[test]
    fn bounded_ranges_do_not_conflate_release_and_cluster_generation() {
        let baseline = Software { min: 1, max: 1 };
        let next = Software { min: 1, max: 2 };
        let future = Software { min: 3, max: 4 };
        assert!(baseline.overlaps(next));
        assert!(!future.overlaps(next));
        assert!(
            !Software {
                min: 1,
                max: u32::MAX
            }
            .overlaps(next)
        );
        assert!(next.supports(1) && next.supports(2));
        assert!(!baseline.supports(2));
    }
}
