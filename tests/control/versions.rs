use super::*;

pub(super) fn snapshot_file(bytes: Vec<u8>) -> Box<tokio::fs::File> {
    use std::io::{Seek, Write};
    let mut file = fleet::control::store::snapshot::temporary().unwrap();
    file.write_all(&bytes).unwrap();
    file.rewind().unwrap();
    Box::new(tokio::fs::File::from_std(file))
}

fn state_snapshot(state: &State) -> Vec<u8> {
    let mut header = serde_json::to_value(state).unwrap();
    let mut rows = Vec::new();
    for (kind, path) in [
        (0, "/receipts"),
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
        let map = header.pointer_mut(path).unwrap().as_object_mut().unwrap();
        for (key, value) in map.iter() {
            rows.push(serde_json::json!([kind, key, value]));
        }
        map.clear();
    }
    let mut bytes = b"FLEET-BASELINE-1\n".to_vec();
    bytes.extend(serde_json::to_vec(&header).unwrap());
    bytes.push(b'\n');
    for row in rows {
        bytes.extend(serde_json::to_vec(&row).unwrap());
        bytes.push(b'\n');
    }
    bytes
}

// Mixed-version preparation, leader loss, rollback and incompatible writes are exercised
// by fleet-qualify rolling-upgrade using separately built 1, 1..2 and 3..4 executables.
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
    let bytes = state_snapshot(&future);
    snapshot.meta.snapshot_id = blake3::hash(&bytes).to_hex().to_string();
    snapshot.snapshot = snapshot_file(bytes);
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
    let bytes = state_snapshot(&upgraded);
    let mut meta = stale.meta.clone();
    meta.snapshot_id = blake3::hash(&bytes).to_hex().to_string();
    store
        .install_snapshot(&meta, snapshot_file(bytes))
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
