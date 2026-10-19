//! One-time, offline conversion of the sole pre-baseline installation. Never modifies sources.
use anyhow::{Context, Result, ensure};
use clap::Parser;
use fleet::control::{
    FAMILY, FORMAT, State, Store,
    queue::JobPhase,
    workload::{Assignment, Desired, InstanceDesired, InstancePhase, WorkPhase},
};
use openraft::RaftSnapshotBuilder;
use redb::{Database, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};
use serde_json::{Value, json};
use std::{
    fs,
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
};
const META: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");
const JOURNAL: TableDefinition<&str, &[u8]> = TableDefinition::new("worker_journal");
const ROWS: TableDefinition<(u8, &str), &[u8]> = TableDefinition::new("state_rows");
const RECEIPTS: TableDefinition<&str, &[u8]> = TableDefinition::new("request_receipts");
const EXECUTIONS: TableDefinition<(u8, &str), bool> = TableDefinition::new("execution_markers");
const LOG: TableDefinition<u64, &[u8]> = TableDefinition::new("raft_log");
#[derive(Parser)]
#[command(
    about = "Convert stopped format-12/generation-4 controller and format-13 worker metadata into a NEW directory; source files are never changed"
)]
struct Args {
    #[arg(long)]
    controller: PathBuf,
    #[arg(long)]
    worker: PathBuf,
    #[arg(long)]
    destination: PathBuf,
}
fn get(t: &impl ReadableTable<&'static str, &'static [u8]>, key: &str) -> Result<Value> {
    Ok(serde_json::from_slice(
        t.get(key)?
            .with_context(|| format!("missing {key}"))?
            .value(),
    )?)
}
fn put(t: &mut redb::Table<&str, &[u8]>, key: &str, v: &Value) -> Result<()> {
    t.insert(key, serde_json::to_vec(v)?.as_slice())?;
    Ok(())
}
fn copy_locked(source: &Path, destination: &Path) -> Result<(fs::File, String)> {
    ensure!(
        source.is_absolute() && source.is_file(),
        "explicit source database required"
    );
    let mut source_file = fs::File::open(source)?;
    source_file
        .try_lock()
        .context("stop the source role before conversion")?;
    let mut target = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(destination)?;
    std::io::copy(&mut source_file, &mut target)?;
    target.sync_all()?;
    let hash = blake3::hash(&fs::read(destination)?).to_hex().to_string();
    Ok((source_file, hash))
}
fn idle_assignment(a: &Assignment) -> bool {
    a.terminal()
        || (a.desired == Desired::Instance(InstanceDesired::Stopped)
            && matches!(&a.observed, Some((revision, observation)) if *revision == a.revision && observation.phase == WorkPhase::Instance(InstancePhase::Stopped)))
}
async fn convert(args: Args) -> Result<Value> {
    ensure!(
        args.destination.is_absolute(),
        "absolute destination required"
    );
    // A failed destination is left for inspection and must never be mistaken for a completed import.
    fs::create_dir(&args.destination).context("destination must not exist")?;
    fs::set_permissions(
        &args.destination,
        std::os::unix::fs::PermissionsExt::from_mode(0o700),
    )?;
    let controller = args.destination.join("control.redb");
    let worker = args.destination.join("worker.redb");
    let (_controller_lock, controller_hash) = copy_locked(&args.controller, &controller)?;
    let (_worker_lock, worker_hash) = copy_locked(&args.worker, &worker)?;
    let db = Database::open(&controller)?;
    let worker_db = Database::open(&worker)?;
    let read = db.begin_read()?;
    let meta = read.open_table(META)?;
    let identity = get(&meta, "identity")?;
    let mut state = get(&meta, "state")?;
    ensure!(
        identity[0] == 12
            && state["format"] == 12
            && state["version"]["active"] == 4
            && state.get("family").is_none(),
        "unsupported source controller; expected installed format 12 / generation 4"
    );
    ensure!(
        state["version"]["pending"].is_null()
            && state["configuration"]["active"].is_null()
            && state["recovery"].is_null(),
        "settle upgrades/membership/recovery before conversion"
    );
    let applied = state["last_applied"].clone();
    let index = applied["index"].as_u64().context("missing applied index")?;
    let committed = get(&meta, "committed")?;
    ensure!(
        committed.is_null() || committed["index"].as_u64().is_some_and(|i| i <= index),
        "unapplied committed suffix"
    );
    ensure!(
        read.open_table(LOG)?
            .last()?
            .is_none_or(|(k, _)| k.value() <= index),
        "unapplied log suffix"
    );
    state["format"] = json!(FORMAT);
    state["family"] = json!(FAMILY);
    state["version"]["active"] = json!(1); // Keep the revision and all request/producer counters.
    let typed: State = serde_json::from_value(state.clone())?;
    ensure!(
        typed.cluster == identity[1].as_str().context("cluster identity")?
            && typed.membership.voter_ids().collect::<Vec<_>>()
                == vec![identity[2].as_u64().context("node identity")?],
        "conversion requires the sole installed voter"
    );
    ensure!(
        typed
            .jobs
            .values()
            .all(|j| j.phase.terminal() || j.phase == JobPhase::Suspended)
            && typed.assignments.values().all(idle_assignment)
            && typed
                .vm
                .operations
                .values()
                .all(|o| o.outcome.phase.terminal()),
        "conversion requires idle workloads and completed VM operations"
    );
    let wr = worker_db.begin_read()?;
    let wt = wr.open_table(JOURNAL)?;
    let mut journal = get(&wt, "state")?;
    ensure!(
        journal["format"] == 13 && journal["generation"] == 4 && journal.get("family").is_none(),
        "unsupported source worker; expected format 13 / generation 4"
    );
    ensure!(
        journal["cluster"] == state["cluster"]
            && journal["recovery_binding"].is_null()
            && journal["recovery_inventory"].is_null(),
        "worker authority mismatch or recovery still active"
    );
    let id = journal["registration"]["id"]
        .as_str()
        .context("worker id")?;
    ensure!(
        state["workers"][id] == journal["registration"],
        "worker registration differs from controller"
    );
    let native = journal["attempted"]
        .as_array()
        .context("native execution markers")?
        .clone();
    let guest = journal["vm_attempted"]
        .as_array()
        .context("guest execution markers")?
        .clone();
    journal.as_object_mut().unwrap().remove("attempted");
    journal.as_object_mut().unwrap().remove("vm_attempted");
    journal["format"] = json!(FORMAT);
    journal["family"] = json!(FAMILY);
    journal["generation"] = json!(1);
    drop(wt);
    drop(wr);
    drop(meta);
    drop(read);
    let mut tx = db.begin_write()?;
    tx.set_durability(redb::Durability::Immediate)?;
    {
        let mut rows = tx.open_table(ROWS)?;
        ensure!(rows.is_empty()?, "source already has resource rows");
        for (kind, path) in [
            (1, "/jobs"),
            (2, "/assignments"),
            (3, "/checkpoints"),
            (4, "/workers"),
            (5, "/membership_operations"),
            (6, "/cursors"),
            (7, "/vm/operations"),
            (8, "/storage/artifacts"),
            (9, "/history/attempts"),
            (10, "/history/jobs"),
        ] {
            let entries = state
                .pointer_mut(path)
                .context("missing resource collection")?
                .as_object_mut()
                .context("invalid resource collection")?;
            for (key, value) in entries.iter() {
                rows.insert((kind, key.as_str()), serde_json::to_vec(value)?.as_slice())?;
            }
            entries.clear();
        }
        let mut receipts = tx.open_table(RECEIPTS)?;
        ensure!(receipts.is_empty()?, "source already has indexed receipts");
        let entries = state["receipts"]
            .as_object_mut()
            .context("missing receipts")?;
        for (key, value) in entries.iter() {
            put(&mut receipts, key, value)?;
        }
        entries.clear();
        let mut meta = tx.open_table(META)?;
        put(
            &mut meta,
            "identity",
            &json!([FORMAT, identity[1], identity[2]]),
        )?;
        put(&mut meta, "state", &state)?;
        put(&mut meta, "purged", &applied)?;
        put(&mut meta, "committed", &applied)?;
        meta.remove("snapshot")?;
        meta.remove("version_prepared")?;
        tx.open_table(LOG)?.retain(|_, _| false)?;
    }
    tx.commit()?;
    let mut tx = worker_db.begin_write()?;
    tx.set_durability(redb::Durability::Immediate)?;
    {
        let mut markers = tx.open_table(EXECUTIONS)?;
        ensure!(markers.is_empty()?, "source already has execution rows");
        for (domain, ids) in [(0, native.clone()), (1, guest.clone())] {
            for id in ids {
                markers.insert((domain, id.as_str().context("invalid execution id")?), true)?;
            }
        }
        let mut table = tx.open_table(JOURNAL)?;
        put(&mut table, "state", &journal)?;
        table.insert("execution_format", b"1".as_slice())?;
    }
    tx.commit()?;
    drop(db);
    drop(worker_db);
    let mut store = Store::open(&controller, &typed.cluster, identity[2].as_u64().unwrap())?;
    ensure!(store.state().await? == typed, "converted state differs");
    store.build_snapshot().await?;
    drop(store);
    ensure!(
        blake3::hash(&fs::read(&args.controller)?).to_hex().as_str() == controller_hash
            && blake3::hash(&fs::read(&args.worker)?).to_hex().as_str() == worker_hash,
        "source changed during import"
    );
    let report = json!({"family":FAMILY,"format":FORMAT,"cluster":typed.cluster,"jobs":typed.jobs.len(),"assignments":typed.assignments.len(),"receipts":typed.receipts.len(),"native_execution_markers":native.len(),"guest_execution_markers":guest.len(),"source_blake3":{"controller":controller_hash,"worker":worker_hash},"converted_blake3":{"controller":blake3::hash(&fs::read(controller)?).to_hex().to_string(),"worker":blake3::hash(&fs::read(worker)?).to_hex().to_string()}});
    let mut complete = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(args.destination.join("complete.json"))?;
    std::io::Write::write_all(&mut complete, &serde_json::to_vec_pretty(&report)?)?;
    complete.sync_all()?;
    fs::File::open(&args.destination)?.sync_all()?;
    Ok(report)
}
#[tokio::main]
async fn main() -> Result<()> {
    println!("{}", convert(Args::parse()).await?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use fleet::control::{Command, Mutation, Reply, Request, workload::Registration};
    use openraft::{CommittedLeaderId, LogId, Membership, StoredMembership};
    use std::collections::{BTreeMap, BTreeSet};
    struct Fixture(PathBuf);
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    fn fixture() -> Result<(Fixture, State)> {
        let root = std::env::temp_dir().join(format!(
            "fleet-control-import-test-{:016x}",
            rand::random::<u64>()
        ));
        fs::create_dir(&root)?;
        let key = fleet::keys::load_or_create(&root.join("node.key"))?;
        let socket = std::net::UdpSocket::bind("127.0.0.1:0")?;
        let peer = fleet::control::network::Peer {
            identity: key.public(),
            address: socket.local_addr()?,
        };
        let registration: Registration = serde_json::from_value(
            json!({"id":"worker","incarnation":"disk-1","peer":peer,"budget":{"cpu_slots":2,"memory_mb":4096,"disk_gb":100,"vm_slots":1},"images":[],"runtimes":{},"tags":[]}),
        )?;
        let mut state = State::new("import-cluster".into());
        state
            .workers
            .insert(registration.id.clone(), registration.clone());
        state.cursors.insert(registration.cursor(), 3);
        let log = LogId::new(CommittedLeaderId::new(1, 1), 5);
        state.membership = StoredMembership::new(
            Some(log),
            Membership::new(
                vec![BTreeSet::from([1])],
                BTreeMap::from([(1, peer.node())]),
            ),
        );
        state.last_applied = Some(log);
        state.version.revision = 8;
        let r = state.apply(
            Command {
                principal: "client".into(),
                request: Request {
                    origin: state.cluster.clone(),
                    id: "never-repeat".into(),
                    mutation: Mutation::CancelQueued {
                        id: "missing".into(),
                        expected_revision: 1,
                    },
                },
            },
            5,
        );
        assert!(matches!(r, Reply::Rejected { .. }));
        let mut old = json!(state);
        old.as_object_mut().unwrap().remove("family");
        old["format"] = json!(12);
        old["version"]["active"] = json!(4);
        let db = Database::create(root.join("source-control.redb"))?;
        let tx = db.begin_write()?;
        {
            let mut meta = tx.open_table(META)?;
            put(&mut meta, "state", &old)?;
            put(&mut meta, "identity", &json!([12, state.cluster, 1]))?;
            put(&mut meta, "committed", &json!(log))?;
            tx.open_table(LOG)?;
        }
        tx.commit()?;
        drop(db);
        let db = Database::create(root.join("source-worker.redb"))?;
        let tx = db.begin_write()?;
        let journal = json!({"format":13,"generation":4,"cluster":state.cluster,"registration":registration,"vm":state.vm,"artifacts":{},"deleted_artifacts":[],"starts_allowed":true,"checkpoints":{},"known_checkpoints":[],"held":[],"recovery_inventory":null,"recovery_binding":null,"acknowledged":3,"sequence":3,"assignments":{},"observations":{},"attempted":["native-once"],"vm_attempted":["guest-once"],"outbox":{}});
        put(&mut tx.open_table(JOURNAL)?, "state", &journal)?;
        tx.commit()?;
        drop(db);
        Ok((Fixture(root), state))
    }
    fn args(f: &Fixture, name: &str) -> Args {
        Args {
            controller: f.0.join("source-control.redb"),
            worker: f.0.join("source-worker.redb"),
            destination: f.0.join(name),
        }
    }
    #[tokio::test]
    async fn conversion_preserves_receipts_markers_cursors_and_snapshot_restart() -> Result<()> {
        let (f, expected) = fixture()?;
        let before = [
            fs::read(&args(&f, "out").controller)?,
            fs::read(&args(&f, "out").worker)?,
        ];
        let report = convert(args(&f, "out")).await?;
        assert_eq!(report["receipts"], 1);
        assert_eq!(report["native_execution_markers"], 1);
        assert_eq!(report["guest_execution_markers"], 1);
        let mut store = Store::open(&f.0.join("out/control.redb"), &expected.cluster, 1)?;
        assert_eq!(store.state().await?, expected);
        let snap = store.build_snapshot().await?;
        let mut restored = Store::open(&f.0.join("restored.redb"), &expected.cluster, 2)?;
        use openraft::storage::RaftStateMachine;
        restored.install_snapshot(&snap.meta, snap.snapshot).await?;
        assert_eq!(restored.state().await?, expected);
        let db = Database::open(f.0.join("out/worker.redb"))?;
        let tx = db.begin_read()?;
        let state = get(&tx.open_table(JOURNAL)?, "state")?;
        assert_eq!(state["sequence"], 3);
        assert_eq!(state["acknowledged"], 3);
        assert_eq!(state["generation"], 1);
        assert!(
            tx.open_table(EXECUTIONS)?
                .get((0, "native-once"))?
                .is_some()
        );
        assert!(tx.open_table(EXECUTIONS)?.get((1, "guest-once"))?.is_some());
        assert!(tx.open_table(EXECUTIONS)?.get((0, "guest-once"))?.is_none());
        assert_eq!(
            before,
            [
                fs::read(args(&f, "out").controller)?,
                fs::read(args(&f, "out").worker)?
            ]
        );
        assert!(convert(args(&f, "out")).await.is_err());
        Ok(())
    }
    #[tokio::test]
    async fn converted_replica_elects_and_commits_after_compaction_and_cold_restart() -> Result<()>
    {
        use fleet::control::{
            Client, Replica,
            network::{Config, Peer},
        };
        use std::time::Duration;
        let (f, expected) = fixture()?;
        convert(args(&f, "out")).await?;
        fs::copy(f.0.join("node.key"), f.0.join("out/node.key"))?;
        let admin = iroh::SecretKey::generate();
        let peer: Peer =
            serde_json::from_str(&expected.membership.membership().get_node(&1).unwrap().addr)?;
        let config = Config {
            cluster: expected.cluster.clone(),
            id: 1,
            peers: BTreeMap::from([(1, peer)]),
            administrators: BTreeSet::from([admin.public()]),
            bootstrap_allowed: false,
        };
        let request = Request {
            origin: expected.cluster.clone(),
            id: "after-import".into(),
            mutation: Mutation::CancelQueued {
                id: "missing".into(),
                expected_revision: 1,
            },
        };
        let mut previous = None;
        for _ in 0..2 {
            let replica = Replica::start(&f.0.join("out"), config.clone()).await?;
            let mut client =
                Client::new(admin.clone(), config.cluster.clone(), config.peers.clone()).await?;
            let reply = client
                .write(request.clone(), Duration::from_secs(20))
                .await?;
            assert!(matches!(reply, Reply::Rejected { .. }));
            if let Some(expected_reply) = &previous {
                assert_eq!(&reply, expected_reply);
            }
            previous = Some(reply);
            let state = client.read(Duration::from_secs(10)).await?;
            assert_eq!(state.receipts.len(), expected.receipts.len() + 1);
            for (id, receipt) in &expected.receipts {
                assert_eq!(state.receipts.get(id), Some(receipt));
            }
            client.endpoint.close().await;
            replica.shutdown().await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn lock_mismatch_and_unapplied_suffix_refuse_without_changing_sources() -> Result<()> {
        let (f, _) = fixture()?;
        let db = Database::open(args(&f, "locked").controller)?;
        assert!(convert(args(&f, "locked")).await.is_err());
        drop(db);
        let db = Database::open(args(&f, "suffix").controller)?;
        let tx = db.begin_write()?;
        tx.open_table(LOG)?.insert(6, b"unapplied".as_slice())?;
        tx.commit()?;
        drop(db);
        let before = fs::read(args(&f, "suffix").controller)?;
        assert!(
            convert(args(&f, "suffix"))
                .await
                .unwrap_err()
                .to_string()
                .contains("suffix")
        );
        assert_eq!(fs::read(args(&f, "suffix").controller)?, before);
        assert!(!f.0.join("suffix/complete.json").exists());
        let (g, _) = fixture()?;
        let db = Database::open(args(&g, "mismatch").worker)?;
        let tx = db.begin_write()?;
        {
            let mut table = tx.open_table(JOURNAL)?;
            let mut value = get(&table, "state")?;
            value["cluster"] = json!("foreign");
            put(&mut table, "state", &value)?;
        }
        tx.commit()?;
        drop(db);
        let before = fs::read(args(&g, "mismatch").worker)?;
        assert!(convert(args(&g, "mismatch")).await.is_err());
        assert_eq!(fs::read(args(&g, "mismatch").worker)?, before);
        Ok(())
    }
}
