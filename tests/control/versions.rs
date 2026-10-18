use super::*;

fn request(id: &str, mutation: Mutation) -> Request {
    Request {
        origin: "cluster".into(),
        id: id.into(),
        mutation,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn version_preparation_survives_leader_loss_and_lost_activation_reply() {
    let mut c = Cluster::new(3).await;
    assert_eq!(
        c.client
            .write(
                request(
                    "prepare",
                    Mutation::PrepareVersion {
                        expected_revision: 0,
                        target: fleet::control::version::MAX,
                    }
                ),
                WAIT
            )
            .await
            .unwrap(),
        Reply::Recorded
    );
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        let s = c.client.read(WAIT).await.unwrap();
        if s.version.pending.as_ref().unwrap().ready == BTreeSet::from([1, 2, 3]) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "preparation did not converge"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let leader = c.leader().await;
    c.replicas
        .remove(&leader)
        .unwrap()
        .shutdown()
        .await
        .unwrap();
    let next = c.leader().await;
    c.replicas[&next]
        .drop_write_reply
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let activate = request(
        "activate",
        Mutation::ActivateVersion {
            expected_revision: 1,
        },
    );
    assert_eq!(
        c.client.write(activate.clone(), WAIT).await.unwrap(),
        Reply::Recorded
    );
    assert_eq!(
        c.client.write(activate, WAIT).await.unwrap(),
        Reply::Recorded
    );
    assert_eq!(
        c.client.read(WAIT).await.unwrap().version.active,
        fleet::control::version::MAX
    );
    c.start(leader).await;
    c.close().await;
}

#[tokio::test]
async fn unsupported_snapshot_and_startup_leave_original_bytes_unchanged() {
    use redb::{Database, ReadableDatabase, TableDefinition};
    let home = Home::new();
    let path = home.0.join("control.redb");
    let mut store = Store::open(&path, "cluster", 1).unwrap();
    store.apply([entry(1, submit("one", "job"))]).await.unwrap();
    let original = store.state().await.unwrap();
    let mut snapshot = store.build_snapshot().await.unwrap();
    let mut future = original.clone();
    future.version.active = fleet::control::version::MAX + 1;
    let bytes = serde_json::to_vec(&future).unwrap();
    snapshot.meta.snapshot_id = blake3::hash(&bytes).to_hex().to_string();
    snapshot.snapshot = Box::new(std::io::Cursor::new(bytes));
    assert!(
        store
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .is_err()
    );
    assert_eq!(store.state().await.unwrap(), original);
    let stale = store.build_snapshot().await.unwrap();
    let mut upgraded = original.clone();
    upgraded.version.active = fleet::control::version::MAX;
    upgraded.version.revision = 2;
    let bytes = serde_json::to_vec(&upgraded).unwrap();
    let mut meta = stale.meta.clone();
    meta.snapshot_id = blake3::hash(&bytes).to_hex().to_string();
    store
        .install_snapshot(&meta, Box::new(std::io::Cursor::new(bytes)))
        .await
        .unwrap();
    assert!(
        store
            .install_snapshot(&stale.meta, stale.snapshot)
            .await
            .is_err()
    );
    assert_eq!(store.state().await.unwrap(), upgraded);
    drop(store);
    // A newer on-disk application generation is rejected before opening the real file for writes.
    {
        let db = Database::open(&path).unwrap();
        let tx = db.begin_write().unwrap();
        tx.open_table(TableDefinition::<&str, &[u8]>::new("meta"))
            .unwrap()
            .insert("state", serde_json::to_vec(&future).unwrap().as_slice())
            .unwrap();
        tx.commit().unwrap();
    }
    let before = std::fs::read(&path).unwrap();
    assert!(
        Store::open(&path, "cluster", 1)
            .unwrap_err()
            .to_string()
            .contains("incompatible")
    );
    assert_eq!(std::fs::read(&path).unwrap(), before);
    let _ = Database::open(&path).unwrap().begin_read().unwrap();
}
