// Match OpenRaft's storage error type at its trait boundary.
#![allow(clippy::result_large_err)]

use std::{
    fmt::Debug,
    ops::RangeBounds,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use anyhow::{Context, Result, ensure};
use openraft::{
    BasicNode, EntryPayload, LogId, LogState, RaftLogReader, RaftSnapshotBuilder, Snapshot,
    SnapshotMeta, StorageError, StorageIOError, StoredMembership, Vote,
    storage::{LogFlushed, RaftLogStorage, RaftStateMachine},
};
use redb::{
    Database, Durability, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use super::{Entry, FORMAT, Reply, State, TypeConfig};

pub mod indexed;
pub mod query;
pub mod snapshot;

const META: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");
const LOG: TableDefinition<u64, &[u8]> = TableDefinition::new("raft_log");
pub const MAX_SNAPSHOT: usize = 64 << 20;

#[derive(Debug, Clone)]
pub struct Store {
    _startup_lock: Arc<std::fs::File>,
    db: Arc<Database>,
    // Held inside the blocking task, even if the awaiting future is cancelled.
    writes: Arc<tokio::sync::Mutex<()>>,
    fail_state_commit: Arc<AtomicBool>,
    hold_joint_apply: Arc<AtomicBool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SavedSnapshot {
    meta: SnapshotMeta<u64, BasicNode>,
    hash: String,
    data: Vec<u8>,
}

fn read<T: DeserializeOwned>(
    table: &impl ReadableTable<&'static str, &'static [u8]>,
    key: &str,
) -> Result<Option<T>> {
    table
        .get(key)?
        .map(|v| serde_json::from_slice(v.value()).map_err(Into::into))
        .transpose()
}

fn put<T: Serialize>(table: &mut redb::Table<&str, &[u8]>, key: &str, value: &T) -> Result<()> {
    table.insert(key, serde_json::to_vec(value)?.as_slice())?;
    Ok(())
}

impl Store {
    pub fn preflight(path: &Path, cluster: &str, node: u64) -> Result<()> {
        Self::preflight_for(path, cluster, node, super::version::Software::current())
    }
    fn preflight_for(
        path: &Path,
        cluster: &str,
        node: u64,
        software: super::version::Software,
    ) -> Result<()> {
        if let Some(raw) = super::version::preflight(path, "meta")? {
            ensure!(
                raw["identity"] == serde_json::json!([FORMAT, cluster, node])
                    || raw["identity"] == serde_json::json!([indexed::FORMAT, cluster, node]),
                "database identity or format mismatch; explicit baseline migration required"
            );
            let version: super::version::ClusterVersion =
                serde_json::from_value(raw["state"]["version"].clone())?;
            software.require(version.required(node))?;
            if let Some(p) = version.pending
                && raw["version_prepared"] == serde_json::json!([version.revision, p.target])
            {
                software.require(p.target)?;
            }
        } else {
            software.require(software.min)?;
        }
        Ok(())
    }
    /// This durable promise is written before the replica publishes its readiness acknowledgement.
    pub async fn prepare_version(
        &self,
        revision: u64,
        target: u32,
    ) -> Result<(), StorageError<u64>> {
        self.blocking(true, move |db| {
            super::version::Software::current().require(target)?;
            let mut tx = db.begin_write()?;
            tx.set_durability(Durability::Immediate)?;
            {
                let mut table = tx.open_table(META)?;
                let state: State = read(&table, "state")?.context("missing state")?;
                ensure!(
                    state.version.revision == revision
                        && state
                            .version
                            .pending
                            .as_ref()
                            .is_some_and(|p| p.target == target),
                    "version preparation changed"
                );
                put(&mut table, "version_prepared", &(revision, target))?;
            }
            tx.commit()?;
            Ok(())
        })
        .await
    }
    /// Convert an idle singleton at its applied boundary. Historical log bytes remain in the backup;
    /// the new snapshot anchors the same term/index, identity, membership, receipts, and worker IDs.
    pub(crate) fn upgrade_v5(db: &Database, path: &Path, preview: bool) -> Result<State> {
        let tx = db.begin_read()?;
        let table = tx.open_table(META)?;
        let identity: (u32, String, u64) =
            read(&table, "identity")?.context("missing controller identity")?;
        if identity.0 == FORMAT || identity.0 == indexed::FORMAT {
            let state = indexed::read_state(&tx, true)?;
            super::version::Software::current().require(state.version.required(identity.2))?;
            return Ok(state);
        }
        ensure!(matches!(identity.0, 5..=11), "unsupported source format");
        let state = super::upgrade::state(read(&table, "state")?.context("missing state")?)?;
        let applied = state
            .last_applied
            .context("controller has never applied a committed state")?;
        ensure!(
            state.cluster == identity.1
                && state.membership.voter_ids().collect::<Vec<_>>() == vec![identity.2],
            "upgrade requires the surviving sole voter"
        );
        let committed: Option<LogId<u64>> =
            read::<Option<LogId<u64>>>(&table, "committed")?.flatten();
        ensure!(
            committed.is_none_or(|c| c.index <= applied.index),
            "apply committed entries with the old binary before upgrade"
        );
        let logs = tx.open_table(LOG)?;
        ensure!(
            logs.last()?
                .is_none_or(|(index, _)| index.value() <= applied.index),
            "unapplied log suffix; let the old singleton settle before upgrading"
        );
        super::version::Software::current().require(state.version.active)?;
        let data = serde_json::to_vec(&state)?;
        ensure!(
            data.len() <= MAX_SNAPSHOT,
            "upgraded metadata exceeds snapshot limit"
        );
        if preview {
            return Ok(state);
        }
        drop(logs);
        drop(table);
        drop(tx);
        super::upgrade::backup_version(path, identity.0)?;
        let hash = snapshot_id(&data);
        let mut tx = db.begin_write()?;
        tx.set_durability(Durability::Immediate)?;
        {
            let mut meta = tx.open_table(META)?;
            put(&mut meta, "identity", &(FORMAT, identity.1, identity.2))?;
            put(&mut meta, "state", &state)?;
            put(
                &mut meta,
                "snapshot",
                &SavedSnapshot {
                    meta: SnapshotMeta {
                        last_log_id: Some(applied),
                        last_membership: state.membership.clone(),
                        snapshot_id: hash.clone(),
                    },
                    hash,
                    data,
                },
            )?;
            put(&mut meta, "purged", &applied)?;
            put(&mut meta, "committed", &Some(applied))?;
        }
        tx.open_table(LOG)?.retain(|_, _| false)?;
        tx.commit()?;
        Ok(state)
    }
    /// Acquire redb's exclusive process lock without creating or initializing a missing source.
    /// Only the applied image is exported; a locally present log suffix is not proof of commit.
    pub fn recovery_source(path: &Path, cluster: &str, node: u64, backup: &Path) -> Result<State> {
        Self::preflight(path, cluster, node)?;
        let db = Database::open(path)?;
        let tx = db.begin_read()?;
        let table = tx.open_table(META)?;
        let identity: (u32, String, u64) = read(&table, "identity")?.context("missing identity")?;
        ensure!(
            matches!(identity.0, FORMAT | indexed::FORMAT)
                && identity.1 == cluster
                && identity.2 == node,
            "source identity/format mismatch"
        );
        let state = indexed::read_state(&tx, true)?;
        ensure!(
            state.format == FORMAT
                && state.cluster == cluster
                && state.last_applied.is_some()
                && state.membership.log_id().is_some(),
            "source has no valid applied cluster state"
        );
        let mut output = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(backup)?;
        std::io::copy(&mut std::fs::File::open(path)?, &mut output)?;
        output.sync_all()?;
        Ok(state)
    }

    /// Seed a NEW Raft history from a validated application image before bootstrap.
    pub async fn seed_recovery(&self, state: State) -> Result<(), StorageError<u64>> {
        self.blocking(true, move |db| {
            super::version::Software::current().require(state.version.active)?;
            let mut tx = db.begin_write()?;
            tx.set_durability(Durability::Immediate)?;
            {
                let mut table = tx.open_table(META)?;
                let old: State = read(&table, "state")?.context("missing state")?;
                ensure!(
                    old == State::new(state.cluster.clone()) && tx.open_table(LOG)?.is_empty()?,
                    "recovery requires an empty destination store"
                );
                ensure!(
                    state.recovery.is_some()
                        && state.last_applied.is_none()
                        && state.membership.log_id().is_none(),
                    "invalid recovery seed"
                );
                if state.version.active >= indexed::GENERATION {
                    let (_, cluster, node): (u32, String, u64) =
                        read(&table, "identity")?.context("missing identity")?;
                    put(&mut table, "identity", &(indexed::FORMAT, cluster, node))?;
                    drop(table);
                    indexed::persist(&tx, &state)?;
                } else {
                    put(&mut table, "state", &state)?;
                }
            }
            tx.commit()?;
            Ok(())
        })
        .await
    }

    /// State and request receipts share one transaction. A future table/index layout can change
    /// this storage detail without changing deterministic command application.
    pub fn open(path: &Path, cluster: &str, node: u64) -> Result<Self> {
        ensure!(!cluster.is_empty(), "empty cluster ID");
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        // Serialize preflight and writable open across Fleet processes. redb retains its own lock.
        let startup_lock = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path.with_extension("startup.lock"))?;
        startup_lock
            .try_lock()
            .context("controller database is already in use")?;
        Self::preflight(path, cluster, node)?;
        let db = Database::create(path)?;
        let mut tx = db.begin_write()?;
        tx.set_durability(Durability::Immediate)?;
        {
            let mut table = tx.open_table(META)?;
            if let Some((version, old_cluster, old_node)) =
                read::<(u32, String, u64)>(&table, "identity")?
            {
                ensure!(
                    matches!(version, FORMAT | indexed::FORMAT)
                        && old_cluster == cluster
                        && old_node == node,
                    "database identity or format mismatch"
                );
            } else {
                put(&mut table, "identity", &(FORMAT, cluster, node))?;
                put(&mut table, "state", &State::new(cluster.into()))?;
            }
            tx.open_table(LOG)?;
        }
        tx.commit()?;
        Ok(Self {
            _startup_lock: Arc::new(startup_lock),
            db: Arc::new(db),
            writes: Arc::new(tokio::sync::Mutex::new(())),
            fail_state_commit: Arc::new(AtomicBool::new(false)),
            hold_joint_apply: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Lab fault injection: abort the next apply/snapshot installation before its commit.
    pub fn fail_next_state_commit(&self) {
        self.fail_state_commit.store(true, Ordering::SeqCst);
    }

    /// Lab fault injection: hold a committed joint configuration before reporting apply complete.
    pub fn hold_joint_apply(&self, hold: bool) {
        self.hold_joint_apply.store(hold, Ordering::SeqCst);
    }

    async fn blocking<T: Send + 'static>(
        &self,
        write: bool,
        f: impl FnOnce(&Database) -> Result<T> + Send + 'static,
    ) -> Result<T, StorageError<u64>> {
        let guard = if write {
            Some(self.writes.clone().lock_owned().await)
        } else {
            None
        };
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let _guard = guard;
            f(&db)
        })
        .await
        .map_err(|e| StorageIOError::read(&e))?
        .map_err(|e| StorageIOError::write(&std::io::Error::other(format!("{e:#}"))).into())
    }

    pub async fn state(&self) -> Result<State, StorageError<u64>> {
        self.blocking(false, |db| indexed::read_state(&db.begin_read()?, true))
            .await
    }

    /// Current resources without permanent receipt history. Use `state` only for explicit exports.
    pub async fn view(&self) -> Result<State, StorageError<u64>> {
        self.blocking(false, |db| indexed::read_state(&db.begin_read()?, false))
            .await
    }

    /// Version/membership/routing reads need no resource catalog or receipt history.
    pub async fn header(&self) -> Result<State, StorageError<u64>> {
        self.blocking(false, |db| {
            read(&db.begin_read()?.open_table(META)?, "state")?.context("missing state")
        })
        .await
    }

    /// Bound the legacy network export before allocating permanent receipt history. Offline
    /// recovery deliberately uses `state` and is not subject to the network frame budget.
    pub async fn export(&self, budget: usize) -> Result<State, StorageError<u64>> {
        self.blocking(false, move |db| {
            let tx = db.begin_read()?;
            let mut state = indexed::read_state(&tx, false)?;
            let mut bytes = serde_json::to_vec(&state)?.len();
            ensure!(
                bytes <= budget,
                "resource export exceeds network budget; use metadata pages"
            );
            let (format, _, _): (u32, String, u64) =
                read(&tx.open_table(META)?, "identity")?.context("missing identity")?;
            if format == indexed::FORMAT {
                for row in tx.open_table(indexed::RECEIPTS)?.iter()? {
                    let (key, value) = row?;
                    bytes += serde_json::to_vec(key.value())?.len() + value.value().len() + 2;
                    ensure!(
                        bytes <= budget,
                        "request history exceeds network budget; use metadata pages"
                    );
                    state
                        .receipts
                        .insert(key.value().into(), serde_json::from_slice(value.value())?);
                }
            } else {
                state = indexed::read_state(&tx, true)?;
                ensure!(
                    serde_json::to_vec(&state)?.len() <= budget,
                    "state export exceeds network budget; use metadata pages"
                );
            }
            Ok(state)
        })
        .await
    }

    async fn meta<T: DeserializeOwned + Send + 'static>(
        &self,
        key: &'static str,
    ) -> Result<Option<T>, StorageError<u64>> {
        self.blocking(false, move |db| {
            read(&db.begin_read()?.open_table(META)?, key)
        })
        .await
    }

    async fn save<T: Serialize + Send + 'static>(
        &self,
        key: &'static str,
        value: T,
    ) -> Result<(), StorageError<u64>> {
        self.blocking(true, move |db| {
            let mut tx = db.begin_write()?;
            tx.set_durability(Durability::Immediate)?;
            put(&mut tx.open_table(META)?, key, &value)?;
            tx.commit()?;
            Ok(())
        })
        .await
    }
}

impl RaftLogReader<TypeConfig> for Store {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry>, StorageError<u64>> {
        let bounds = (range.start_bound().cloned(), range.end_bound().cloned());
        self.blocking(false, move |db| {
            let tx = db.begin_read()?;
            let table = tx.open_table(LOG)?;
            table
                .range(bounds)?
                .map(|entry| {
                    let (_, value) = entry?;
                    Ok(serde_json::from_slice(value.value())?)
                })
                .collect()
        })
        .await
    }
}

impl RaftLogStorage<TypeConfig> for Store {
    type LogReader = Self;
    async fn get_log_reader(&mut self) -> Self {
        self.clone()
    }
    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<u64>> {
        self.blocking(false, |db| {
            let tx = db.begin_read()?;
            let purged: Option<LogId<u64>> = read(&tx.open_table(META)?, "purged")?;
            let table = tx.open_table(LOG)?;
            let last = table
                .last()?
                .map(|(_, v)| serde_json::from_slice::<Entry>(v.value()))
                .transpose()?;
            Ok(LogState {
                last_purged_log_id: purged,
                last_log_id: last.map(|e| e.log_id).or(purged),
            })
        })
        .await
    }
    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        self.save("vote", *vote).await
    }
    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        self.meta("vote").await
    }
    async fn save_committed(
        &mut self,
        committed: Option<LogId<u64>>,
    ) -> Result<(), StorageError<u64>> {
        self.save("committed", committed).await
    }
    async fn read_committed(&mut self) -> Result<Option<LogId<u64>>, StorageError<u64>> {
        Ok(self.meta("committed").await?.flatten())
    }
    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry> + Send,
        I::IntoIter: Send,
    {
        let entries: Vec<_> = entries.into_iter().collect();
        self.blocking(true, move |db| {
            let mut tx = db.begin_write()?;
            tx.set_durability(Durability::Immediate)?;
            {
                let mut table = tx.open_table(LOG)?;
                for entry in entries {
                    table.insert(entry.log_id.index, serde_json::to_vec(&entry)?.as_slice())?;
                }
            }
            tx.commit()?;
            Ok(())
        })
        .await?;
        callback.log_io_completed(Ok(()));
        Ok(())
    }
    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        self.blocking(true, move |db| {
            let mut tx = db.begin_write()?;
            tx.set_durability(Durability::Immediate)?;
            tx.open_table(LOG)?.retain(|k, _| k < log_id.index)?;
            tx.commit()?;
            Ok(())
        })
        .await
    }
    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        self.blocking(true, move |db| {
            let mut tx = db.begin_write()?;
            tx.set_durability(Durability::Immediate)?;
            put(&mut tx.open_table(META)?, "purged", &log_id)?;
            tx.open_table(LOG)?.retain(|k, _| k > log_id.index)?;
            tx.commit()?;
            Ok(())
        })
        .await
    }
}

impl RaftStateMachine<TypeConfig> for Store {
    type SnapshotBuilder = Self;
    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, BasicNode>), StorageError<u64>> {
        let state = self.view().await?;
        Ok((state.last_applied, state.membership))
    }
    async fn apply<I>(&mut self, entries: I) -> Result<Vec<Reply>, StorageError<u64>>
    where
        I: IntoIterator<Item = Entry> + Send,
        I::IntoIter: Send,
    {
        let entries: Vec<_> = entries.into_iter().collect();
        let fail = self.fail_state_commit.clone();
        let joint = entries.iter().any(|entry| matches!(&entry.payload, EntryPayload::Membership(m) if m.get_joint_config().len() > 1));
        let replies = self
            .blocking(true, move |db| {
                let mut tx = db.begin_write()?;
                tx.set_durability(Durability::Immediate)?;
                let replies = {
                    let table = tx.open_table(META)?;
                    let mut state: State = read(&table, "state")?.context("missing state")?;
                    let (format, cluster, node): (u32, String, u64) =
                        read(&table, "identity")?.context("missing identity")?;
                    drop(table);
                    let mut indexed = format == indexed::FORMAT;
                    if indexed {
                        state = indexed::hydrate(state, &tx.open_table(indexed::ROWS)?)?;
                    }
                    let mut replies = Vec::new();
                    for entry in entries {
                        state.last_applied = Some(entry.log_id);
                        replies.push(match entry.payload {
                            EntryPayload::Normal(command) => {
                                if indexed {
                                    indexed::load_receipts(
                                        &mut state,
                                        &command,
                                        &tx.open_table(indexed::RECEIPTS)?,
                                    )?;
                                }
                                let reply = state.apply(command, entry.log_id.index);
                                if indexed {
                                    let mut receipts = tx.open_table(indexed::RECEIPTS)?;
                                    for (key, receipt) in &state.receipts {
                                        put(&mut receipts, key, receipt)?;
                                    }
                                    state.receipts.clear();
                                } else if state.version.active >= indexed::GENERATION {
                                    // Activation migrates all old receipts atomically. Later entries
                                    // in this batch already use point lookups against the new table.
                                    indexed::persist(&tx, &state)?;
                                    state.receipts.clear();
                                    indexed = true;
                                }
                                reply
                            }
                            EntryPayload::Membership(m) => {
                                state
                                    .apply_membership(StoredMembership::new(Some(entry.log_id), m));
                                Reply::Recorded
                            }
                            EntryPayload::Blank => Reply::Recorded,
                        });
                    }
                    let mut table = tx.open_table(META)?;
                    super::version::Software::current().require(state.version.required(node))?;
                    // Abort/activation and the local promise change in the same transaction.
                    let prepared: Option<(u64, u32)> = read(&table, "version_prepared")?;
                    if prepared.is_some_and(|(r, t)| {
                        state.version.revision != r
                            || state.version.pending.as_ref().is_none_or(|p| p.target != t)
                    }) {
                        table.remove("version_prepared")?;
                    }
                    if indexed {
                        put(&mut table, "identity", &(indexed::FORMAT, cluster, node))?;
                        drop(table);
                        indexed::persist(&tx, &state)?;
                    } else {
                        put(&mut table, "state", &state)?;
                    }
                    replies
                };
                ensure!(
                    !fail.swap(false, Ordering::SeqCst),
                    "injected state commit failure"
                );
                tx.commit()?;
                Ok(replies)
            })
            .await?;
        while joint && self.hold_joint_apply.load(Ordering::SeqCst) {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        Ok(replies)
    }
    async fn get_snapshot_builder(&mut self) -> Self {
        self.clone()
    }
    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<tokio::fs::File>, StorageError<u64>> {
        self.blocking(false, |_| {
            Ok(Box::new(tokio::fs::File::from_std(snapshot::temporary()?)))
        })
        .await
    }
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, BasicNode>,
        mut snapshot: Box<tokio::fs::File>,
    ) -> Result<(), StorageError<u64>> {
        use tokio::io::AsyncWriteExt;
        snapshot
            .flush()
            .await
            .map_err(|e| StorageIOError::write(&e))?;
        let file = snapshot.into_std().await;
        let meta = meta.clone();
        let fail = self.fail_state_commit.clone();
        self.blocking(true, move |db| snapshot::install(db, meta, file, &fail))
            .await
    }
    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<u64>> {
        self.blocking(false, snapshot::current).await
    }
}

fn snapshot_id(data: &[u8]) -> String {
    blake3::hash(data).to_hex().to_string()
}

impl RaftSnapshotBuilder<TypeConfig> for Store {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<u64>> {
        // Capture through redb MVCC without blocking command application. Publication checks that
        // it cannot replace a newer installed/built snapshot under redb's single writer lock.
        self.blocking(false, snapshot::build).await
    }
}

#[cfg(test)]
mod version_tests {
    use super::*;
    use crate::control::{Command, Mutation, Request, network::Peer, version::Software};
    use std::collections::{BTreeMap, BTreeSet};

    #[tokio::test]
    async fn unacknowledged_preparation_blocks_old_binary_until_abort_is_applied() {
        let home = std::env::temp_dir().join(format!(
            "fleet-control-version-promise-{:032x}",
            rand::random::<u128>()
        ));
        let path = home.join("control.redb");
        let mut store = Store::open(&path, "cluster", 1).unwrap();
        let log = |index| LogId::new(openraft::CommittedLeaderId::new(1, 1), index);
        let peer = Peer {
            identity: iroh::SecretKey::generate().public(),
            address: "127.0.0.1:19001".parse().unwrap(),
        };
        store
            .apply([Entry {
                log_id: log(1),
                payload: EntryPayload::Membership(openraft::Membership::new(
                    vec![BTreeSet::from([1])],
                    BTreeMap::from([(1, peer.node())]),
                )),
            }])
            .await
            .unwrap();
        let command = |id: &str, mutation| Entry {
            log_id: log(if id == "prepare" { 2 } else { 3 }),
            payload: EntryPayload::Normal(Command {
                principal: "admin".into(),
                request: Request {
                    origin: "cluster".into(),
                    id: id.into(),
                    mutation,
                },
            }),
        };
        store
            .apply([command(
                "prepare",
                Mutation::PrepareVersion {
                    expected_revision: 0,
                    target: crate::control::version::MAX,
                },
            )])
            .await
            .unwrap();
        store
            .prepare_version(1, crate::control::version::MAX)
            .await
            .unwrap();
        assert!(
            store
                .state()
                .await
                .unwrap()
                .version
                .pending
                .unwrap()
                .ready
                .is_empty()
        );
        drop(store);
        let bytes = std::fs::read(&path).unwrap();
        let old = Software {
            min: crate::control::version::MIN - 1,
            max: crate::control::version::MIN,
        };
        assert!(Store::preflight_for(&path, "cluster", 1, old).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
        let mut store = Store::open(&path, "cluster", 1).unwrap();
        store
            .apply([command(
                "abort",
                Mutation::AbortVersion {
                    expected_revision: 1,
                },
            )])
            .await
            .unwrap();
        drop(store);
        Store::preflight_for(&path, "cluster", 1, old).unwrap();
        std::fs::remove_dir_all(home).unwrap();
    }
}
