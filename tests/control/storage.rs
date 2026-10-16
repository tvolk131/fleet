use super::*;
use fleet::control::{
    artifact::{self, CopyPhase, Kind},
    artifact_io, storage,
};

pub(super) async fn backup(
    c: &mut Cluster,
    id: &str,
    capacity: u64,
) -> (PathBuf, storage::Config, storage::Agent) {
    let home = c.home.0.join(id);
    let key = fleet::keys::load_or_create(&home.join("node.key")).unwrap();
    let socket = reserve_socket();
    let registration = artifact::Registration {
        id: id.into(),
        peer: Peer {
            identity: key.public(),
            address: socket.local_addr().unwrap(),
        },
        incarnation: format!("{id}-disk"),
        failure_domain: "same-test-machine".into(),
        capacity_bytes: capacity,
    };
    let config = storage::Config {
        cluster: c.config.cluster.clone(),
        registration: registration.clone(),
        controllers: c.config.peers.clone(),
        administrators: c.config.administrators.clone(),
    };
    storage::Agent::initialize(&home, &config).unwrap();
    drop(socket);
    assert_eq!(
        c.client
            .write(
                Request {
                    origin: c.config.cluster.clone(),
                    id: format!("enroll-{id}"),
                    mutation: Mutation::EnrollStorage(registration)
                },
                WAIT
            )
            .await
            .unwrap(),
        Reply::Recorded
    );
    let agent = storage::Agent::start(&home, config.clone()).await.unwrap();
    (home, config, agent)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn storage_quota_corruption_restart_and_peer_repair_are_visible_and_verified() {
    let mut c = Cluster::new(1).await;
    let (worker_home, config) = prepare(&mut c, false).await;
    let mut w = Worker::start(&worker_home, config, hello()).await.unwrap();
    let (a_home, a_config, mut a) = backup(&mut c, "backup-a", 16 << 20).await;
    let (b_home, _, mut b) = backup(&mut c, "backup-b", 16 << 20).await;
    let (small_home, _, mut small) = backup(&mut c, "too-small", 1).await;
    c.client
        .write(
            Request {
                origin: "cluster".into(),
                id: "policy".into(),
                mutation: Mutation::StoragePolicy {
                    expected_revision: 0,
                    policy: artifact::Policy {
                        outputs: BTreeSet::from([
                            "backup-a".into(),
                            "backup-b".into(),
                            "too-small".into(),
                        ]),
                        ..Default::default()
                    },
                },
            },
            WAIT,
        )
        .await
        .unwrap();
    let mut request = super::assign("output", workload("printf verified-output > result.txt"));
    // Artifact publication belongs to a logical job, created by Enqueue in normal use.
    request.mutation = Mutation::Enqueue(fleet::control::queue::Submission {
        inputs: Default::default(),
        id: "output".into(),
        workload: workload("printf verified-output > result.txt"),
        worker: Some("worker-real".into()),
        priority: 0,
        requires: BTreeSet::new(),
        after: BTreeSet::new(),
    });
    c.client.write(request, WAIT).await.unwrap();
    wait(
        &mut c,
        &mut w,
        "output",
        WorkPhase::Job(AttemptPhase::Succeeded),
    )
    .await;
    w.tick().await.unwrap();
    a.tick().await.unwrap();
    b.tick().await.unwrap();
    small.tick().await.unwrap();
    let state = c.client.read(WAIT).await.unwrap();
    let artifact = state
        .storage
        .artifacts
        .values()
        .find(|m| m.manifest.kind == Kind::FinalOutputs)
        .unwrap()
        .clone();
    assert_eq!(artifact.copies["storage/backup-a"].phase, CopyPhase::Ready);
    assert_eq!(artifact.copies["storage/backup-b"].phase, CopyPhase::Ready);
    assert_eq!(
        artifact.copies["storage/too-small"].phase,
        CopyPhase::Failed
    );
    assert!(
        artifact.copies["storage/too-small"]
            .detail
            .as_ref()
            .unwrap()
            .contains("capacity exhausted")
    );
    assert_eq!(
        storage::usage(&small_home.join("staging")).unwrap().files,
        0
    );
    assert!(
        !small_home
            .join("objects")
            .join(&artifact.manifest.id)
            .exists()
    );
    // An enrolled storage identity has no administrator control-plane privileges.
    let mut restricted = Client::new(
        fleet::keys::load_or_create(&a_home.join("node.key")).unwrap(),
        c.config.cluster.clone(),
        c.config.peers.clone(),
    )
    .await
    .unwrap();
    assert!(restricted.read(WAIT).await.is_err());
    assert!(
        restricted
            .write(
                Request {
                    origin: c.config.cluster.clone(),
                    id: "storage-cannot-delete".into(),
                    mutation: Mutation::DeleteArtifact {
                        id: artifact.manifest.id.clone(),
                        expected_revision: artifact.revision
                    }
                },
                WAIT
            )
            .await
            .is_err()
    );
    assert!(
        restricted
            .write(
                Request {
                    origin: c.config.cluster.clone(),
                    id: "storage-cannot-spoof-peer".into(),
                    mutation: Mutation::ArtifactReport(artifact::Report {
                        location: "storage/backup-b".into(),
                        incarnation: "backup-b-disk".into(),
                        artifact: artifact.manifest.id.clone(),
                        revision: artifact.revision,
                        expected_version: artifact.copies["storage/backup-b"].version,
                        phase: CopyPhase::Missing,
                        detail: None
                    })
                },
                WAIT
            )
            .await
            .is_err()
    );
    restricted.endpoint.close().await;
    // An un-enrolled endpoint cannot retrieve files even when it knows the content/manifest IDs.
    let outsider = fleet::control::network::connect_endpoint(
        SecretKey::generate(),
        "127.0.0.1:0".parse().unwrap(),
    )
    .await
    .unwrap();
    assert!(
        artifact_io::fetch(
            &outsider,
            &a_config.registration.peer,
            "cluster",
            &artifact.manifest,
            &c.home.0.join("unauthorized")
        )
        .await
        .is_err()
    );
    assert!(!c.home.0.join("unauthorized").exists());
    outsider.close().await;
    a.shutdown().await.unwrap();
    // Interrupted staging is not a copy and is discarded at startup.
    std::fs::create_dir_all(a_home.join("staging/interrupted/files")).unwrap();
    std::fs::write(
        a_home.join("staging/interrupted/files/partial"),
        b"untrusted partial",
    )
    .unwrap();
    std::fs::write(
        a_home
            .join("objects")
            .join(&artifact.manifest.id)
            .join("files/result.txt"),
        b"corrupt",
    )
    .unwrap();
    // Remove source output and stop the worker. Repair must come from the other retained copy.
    let assignment = state.current_attempt("output").unwrap();
    std::fs::remove_file(
        worker_home
            .join("backend/jobs")
            .join(&assignment.execution)
            .join("work/result.txt"),
    )
    .unwrap();
    w.shutdown().await.unwrap();
    let mut a = storage::Agent::start(&a_home, a_config).await.unwrap();
    assert!(!a_home.join("staging/interrupted").exists());
    a.tick().await.unwrap();
    assert_eq!(
        std::fs::read(
            a_home
                .join("objects")
                .join(&artifact.manifest.id)
                .join("files/result.txt")
        )
        .unwrap(),
        b"verified-output"
    );
    assert_eq!(
        std::fs::read(
            b_home
                .join("objects")
                .join(&artifact.manifest.id)
                .join("files/result.txt")
        )
        .unwrap(),
        b"verified-output"
    );
    let exported = c.home.0.join("repaired-export");
    let state = c.client.read(WAIT).await.unwrap();
    artifact_io::fetch_available(
        &c.client.endpoint,
        &state,
        &state.storage.artifacts[&artifact.manifest.id],
        &exported,
    )
    .await
    .unwrap();
    assert_eq!(
        std::fs::read(exported.join("result.txt")).unwrap(),
        b"verified-output"
    );
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
    small.shutdown().await.unwrap();
    c.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn storage_recovery_requires_rebinding_inventory_and_explicit_cleanup_review() {
    let mut c = Cluster::new(1).await;
    let (home, config) = prepare(&mut c, false).await;
    instances::write(&home.join("worker.json"), &config).unwrap();
    let mut w = Worker::start(&home, config.clone(), hello()).await.unwrap();
    let (storage_home, original_config, mut backup) = backup(&mut c, "backup", 16 << 20).await;
    c.client
        .write(
            Request {
                origin: "cluster".into(),
                id: "policy".into(),
                mutation: Mutation::StoragePolicy {
                    expected_revision: 0,
                    policy: artifact::Policy {
                        outputs: BTreeSet::from(["backup".into()]),
                        ..Default::default()
                    },
                },
            },
            WAIT,
        )
        .await
        .unwrap();
    c.client
        .write(
            super::assign("output", workload("echo retained > result.txt")),
            WAIT,
        )
        .await
        .unwrap();
    wait(
        &mut c,
        &mut w,
        "output",
        WorkPhase::Job(AttemptPhase::Succeeded),
    )
    .await;
    w.tick().await.unwrap();
    backup.tick().await.unwrap();
    let old = c.client.read(WAIT).await.unwrap();
    let artifact = old.storage.artifacts.values().next().unwrap().clone();
    assert_eq!(artifact.copies["storage/backup"].phase, CopyPhase::Ready);
    w.shutdown().await.unwrap();
    backup.shutdown().await.unwrap();
    let (_, manifest) = super::recovery::restore(&mut c, 1, "storage-recovered").await;
    let mut tampered = manifest.clone();
    tampered.plan.previous = "not-the-old-authority".into();
    assert!(storage::accept_recovery(&storage_home, &tampered).is_err());
    storage::accept_recovery(&storage_home, &manifest).unwrap();
    storage::accept_recovery(&storage_home, &manifest).unwrap();
    assert!(
        storage::Agent::start(&storage_home, original_config.clone())
            .await
            .is_err()
    );
    let updated: storage::Config =
        serde_json::from_slice(&std::fs::read(storage_home.join("storage.json")).unwrap()).unwrap();
    let mut backup = storage::Agent::start(&storage_home, updated).await.unwrap();
    backup.tick().await.unwrap();
    assert!(
        artifact_io::fetch(
            &c.client.endpoint,
            &original_config.registration.peer,
            &old.cluster,
            &artifact.manifest,
            &c.home.0.join("old-authority-export")
        )
        .await
        .is_err()
    );
    Worker::accept_recovery(&home, &manifest).unwrap();
    let config: WorkerConfig =
        serde_json::from_slice(&std::fs::read(home.join("worker.json")).unwrap()).unwrap();
    let mut w = Worker::start(&home, config, hello()).await.unwrap();
    super::recovery::reconciled(&mut c, &mut w).await;
    super::recovery::resume(&mut c, &[]).await;
    w.tick().await.unwrap();
    backup.tick().await.unwrap();
    let state = c.client.read(WAIT).await.unwrap();
    assert!(!state.artifact_cleanup_allowed(None));
    let mut probe = state.clone();
    assert!(matches!(
        probe.delete_artifact(&artifact.manifest.id, artifact.revision),
        Reply::Rejected { .. }
    ));
    assert!(matches!(
        probe.review_storage_recovery("stale-inventory", &BTreeSet::new()),
        Reply::Rejected { .. }
    ));
    let review = Request {
        origin: c.config.cluster.clone(),
        id: "review-storage".into(),
        mutation: Mutation::ReviewStorageRecovery {
            expected_catalog: state.artifact_catalog_fingerprint(),
            unavailable_locations: BTreeSet::new(),
        },
    };
    assert_eq!(
        c.client.write(review.clone(), WAIT).await.unwrap(),
        Reply::Recorded
    );
    assert_eq!(c.client.write(review, WAIT).await.unwrap(), Reply::Recorded);
    assert_eq!(
        c.client
            .write(
                Request {
                    origin: c.config.cluster.clone(),
                    id: "delete-after-review".into(),
                    mutation: Mutation::DeleteArtifact {
                        id: artifact.manifest.id.clone(),
                        expected_revision: artifact.revision
                    }
                },
                WAIT
            )
            .await
            .unwrap(),
        Reply::Recorded
    );
    w.tick().await.unwrap();
    backup.tick().await.unwrap();
    let state = c.client.read(WAIT).await.unwrap();
    assert_eq!(
        state.storage.artifacts[&artifact.manifest.id].copies["storage/backup"].phase,
        CopyPhase::Deleted
    );
    assert!(
        !storage_home
            .join("objects")
            .join(&artifact.manifest.id)
            .exists()
    );
    backup.shutdown().await.unwrap();
    w.shutdown().await.unwrap();
    c.close().await;
}
