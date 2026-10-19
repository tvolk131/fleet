//! The local backend still enforces stable execution IDs after removing the old mutation RPC.
use fleet::{
    backend::{Backend, BackendConfig},
    jobs::{self, JobSpec},
    worker_data::Response,
};
use std::{path::PathBuf, time::Duration};

struct Home(PathBuf);
impl Drop for Home {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[tokio::test]
async fn concurrent_backend_reconciliation_starts_once_and_rejects_changed_intent() {
    let home = Home(std::env::temp_dir().join(format!(
        "fleet-backend-once-{:032x}",
        rand::random::<u128>()
    )));
    let peer = iroh::SecretKey::generate().public();
    let hello = serde_json::from_value(serde_json::json!({
        "agent_version":"test", "hostname":"test", "os":"test", "cpu":"test", "cores":4,
        "memory_bytes":8589934592u64, "cpu_slots":4
    }))
    .unwrap();
    let backend = Backend::start(
        hello,
        BackendConfig {
            home: home.0.clone(),
            supervisor: env!("CARGO_BIN_EXE_fleet").into(),
            store: fleet::blobs::open(&home.0.join("blobs")).await.unwrap(),
        },
    )
    .unwrap();
    let spec: JobSpec = serde_json::from_value(serde_json::json!({
        "name":"once", "argv":["/bin/sh","-c","echo started; sleep 0.1"],
        "threads":1, "keep_awake":false, "max_seconds":10, "settle_seconds":0
    }))
    .unwrap();
    let id = "stable-execution";
    let (a, b) = tokio::join!(
        backend.submit(peer, spec.clone(), Some(id.into()), Some(false)),
        backend.submit(peer, spec.clone(), Some(id.into()), Some(false)),
    );
    assert_eq!(a.unwrap(), id);
    assert_eq!(b.unwrap(), id);
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if jobs::info(&jobs::dir(&home.0, id).unwrap())
                .unwrap()
                .state
                .finished()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let Response::Logs { data, .. } = backend.logs(id, false, 0, 1024).await.unwrap() else {
        panic!("expected logs")
    };
    assert_eq!(data, "started\n");
    let mut conflict = spec.clone();
    conflict.argv = vec!["/bin/echo".into(), "different".into()];
    assert!(
        backend
            .submit(peer, conflict, Some(id.into()), Some(false))
            .await
            .is_err()
    );
    assert!(
        backend
            .submit(
                iroh::SecretKey::generate().public(),
                spec.clone(),
                Some(id.into()),
                Some(false)
            )
            .await
            .is_err()
    );
    assert!(
        backend
            .submit(peer, spec, Some(id.into()), Some(true))
            .await
            .is_err()
    );
    drop(backend);
}
