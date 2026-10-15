// Match OpenRaft's storage error type at its trait boundary.
#![allow(clippy::result_large_err)]

use std::{
    fmt::Debug,
    io::Cursor,
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

const META: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");
const LOG: TableDefinition<u64, &[u8]> = TableDefinition::new("raft_log");
pub const MAX_SNAPSHOT: usize = 64 << 20;

#[derive(Debug, Clone)]
pub struct Store {
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
    /// Convert an idle singleton at its applied boundary. Historical log bytes remain in the backup;
    /// the new snapshot anchors the same term/index, identity, membership, receipts, and worker IDs.
    pub(crate) fn upgrade_v5(db: &Database, path: &Path, preview: bool) -> Result<State> {
        let tx = db.begin_read()?;
        let table = tx.open_table(META)?;
        let identity: (u32, String, u64) =
            read(&table, "identity")?.context("missing controller identity")?;
        if identity.0 == FORMAT {
            return read(&table, "state")?.context("missing state");
        }
        ensure!(matches!(identity.0, 5 | 6), "unsupported source format");
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
        let db = Database::open(path)?;
        let tx = db.begin_read()?;
        let table = tx.open_table(META)?;
        let identity: (u32, String, u64) = read(&table, "identity")?.context("missing identity")?;
        ensure!(
            identity == (FORMAT, cluster.into(), node),
            "source identity/format mismatch"
        );
        let state: State = read(&table, "state")?.context("missing applied state")?;
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
                put(&mut table, "state", &state)?;
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
        let db = Database::create(path)?;
        let mut tx = db.begin_write()?;
        tx.set_durability(Durability::Immediate)?;
        {
            let mut table = tx.open_table(META)?;
            if let Some((version, old_cluster, old_node)) =
                read::<(u32, String, u64)>(&table, "identity")?
            {
                ensure!(
                    (version, old_cluster.as_str(), old_node) == (FORMAT, cluster, node),
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
        self.blocking(false, |db| {
            let tx = db.begin_read()?;
            read(&tx.open_table(META)?, "state")?.context("missing state")
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
        let state = self.state().await?;
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
                    let mut table = tx.open_table(META)?;
                    let mut state: State = read(&table, "state")?.context("missing state")?;
                    let mut replies = Vec::new();
                    for entry in entries {
                        state.last_applied = Some(entry.log_id);
                        replies.push(match entry.payload {
                            EntryPayload::Normal(command) => {
                                state.apply(command, entry.log_id.index)
                            }
                            EntryPayload::Membership(m) => {
                                state
                                    .apply_membership(StoredMembership::new(Some(entry.log_id), m));
                                Reply::Recorded
                            }
                            EntryPayload::Blank => Reply::Recorded,
                        });
                    }
                    put(&mut table, "state", &state)?;
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
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        let meta = meta.clone();
        let data = snapshot.into_inner();
        let fail = self.fail_state_commit.clone();
        self.blocking(true, move |db| {
            ensure!(data.len() <= MAX_SNAPSHOT, "snapshot exceeds lab limit");
            let state: State = serde_json::from_slice(&data)?;
            ensure!(
                state.format == FORMAT
                    && state.last_applied == meta.last_log_id
                    && state.membership == meta.last_membership,
                "snapshot metadata mismatch"
            );
            ensure!(
                meta.snapshot_id == snapshot_id(&data),
                "snapshot digest mismatch"
            );
            let mut tx = db.begin_write()?;
            tx.set_durability(Durability::Immediate)?;
            {
                let mut table = tx.open_table(META)?;
                let (_, cluster, _): (u32, String, u64) =
                    read(&table, "identity")?.context("missing identity")?;
                ensure!(cluster == state.cluster, "snapshot cluster mismatch");
                put(&mut table, "state", &state)?;
                put(
                    &mut table,
                    "snapshot",
                    &SavedSnapshot {
                        meta,
                        hash: snapshot_id(&data),
                        data,
                    },
                )?;
            }
            // State and snapshot switch atomically; local votes and logs are untouched.
            ensure!(
                !fail.swap(false, Ordering::SeqCst),
                "injected snapshot commit failure"
            );
            tx.commit()?;
            Ok(())
        })
        .await
    }
    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<u64>> {
        let saved: Option<SavedSnapshot> = self.meta("snapshot").await?;
        saved
            .map(|s| {
                if s.hash != snapshot_id(&s.data) {
                    return Err(StorageIOError::read(&std::io::Error::other(
                        "snapshot digest mismatch",
                    ))
                    .into());
                }
                Ok(Snapshot {
                    meta: s.meta,
                    snapshot: Box::new(Cursor::new(s.data)),
                })
            })
            .transpose()
    }
}

fn snapshot_id(data: &[u8]) -> String {
    blake3::hash(data).to_hex().to_string()
}

impl RaftSnapshotBuilder<TypeConfig> for Store {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<u64>> {
        // Hold the storage write gate across capture and publication so older builders cannot
        // replace a newer installed snapshot. For this small lab, serialization is acceptable.
        self.blocking(true, |db| {
            let mut tx = db.begin_write()?;
            tx.set_durability(Durability::Immediate)?;
            let (meta, data) = {
                let mut table = tx.open_table(META)?;
                let state: State = read(&table, "state")?.context("missing state")?;
                let data = serde_json::to_vec(&state)?;
                ensure!(data.len() <= MAX_SNAPSHOT, "snapshot exceeds lab limit");
                let hash = snapshot_id(&data);
                let meta = SnapshotMeta {
                    last_log_id: state.last_applied,
                    last_membership: state.membership,
                    snapshot_id: hash.clone(),
                };
                put(
                    &mut table,
                    "snapshot",
                    &SavedSnapshot {
                        meta: meta.clone(),
                        hash,
                        data: data.clone(),
                    },
                )?;
                (meta, data)
            };
            tx.commit()?;
            Ok(Snapshot {
                meta,
                snapshot: Box::new(Cursor::new(data)),
            })
        })
        .await
    }
}
