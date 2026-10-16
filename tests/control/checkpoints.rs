use super::*;
use fleet::control::{
    client::job_views,
    queue::{JobPhase, Submission},
};

pub(super) fn resumable() -> Workload {
    let mut spec = job("unused");
    spec.vm = Some(vm_spec());
    spec.shutdown_grace_seconds = None;
    spec.max_seconds = Some(180);
    spec.argv = vec!["/usr/bin/python3".into(), "-c".into(), r#"
import hashlib,json,pathlib,time
p=pathlib.Path('cp/one'); p.mkdir(parents=True)
data=b'7'; (p/'state').write_bytes(data)
m={'version':1,'position':7,'files':{'state':{'size':len(data),'sha256':hashlib.sha256(data).hexdigest()}}}
(p/'ready.tmp').write_text(json.dumps(m)); (p/'ready.tmp').rename(p/'fleet.json')
print('initial-once',flush=True)
time.sleep(150)
"#.into()];
    spec.checkpoint = Some(fleet::checkpoint::Policy {
        retention: None,
        manifests: "cp/*/fleet.json".into(),
        resume_argv: vec!["/usr/bin/python3".into(), "-c".into(), "import pathlib,sys; n=int(pathlib.Path(sys.argv[1]).read_text()); pathlib.Path('result.txt').write_text(str(n+1)); print('resumed-once',flush=True)".into(), "{checkpoint}/state".into()],
    });
    Workload::Job {
        spec: Box::new(spec),
        gpu: false,
        memory_mb: 0,
    }
}

async fn queued(c: &mut Cluster, workload: Workload) {
    let reply = c
        .client
        .write(
            Request {
                origin: c.config.cluster.clone(),
                id: "submit-resumable".into(),
                mutation: Mutation::Enqueue(Submission {
                    inputs: Default::default(),
                    id: "logical".into(),
                    workload,
                    worker: Some("worker-real".into()),
                    priority: 0,
                    requires: BTreeSet::new(),
                    after: BTreeSet::new(),
                }),
            },
            WAIT,
        )
        .await
        .unwrap();
    assert!(matches!(reply, Reply::Accepted { .. }), "{reply:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn verified_checkpoint_survives_vm_deletion_worker_restart_and_leader_loss() {
    let mut c = Cluster::new(3).await;
    let (home, config) = prepare(&mut c, true).await;
    let mut w = Worker::start(&home, config.clone(), hello()).await.unwrap();
    let workload = resumable();
    queued(&mut c, workload.clone()).await;
    let end = tokio::time::Instant::now() + Duration::from_secs(90);
    let checkpoint = loop {
        w.tick().await.unwrap();
        let state = c.client.read(WAIT).await.unwrap();
        if let Some(cp) = state.checkpoints.values().next() {
            break cp.clone();
        }
        assert!(
            tokio::time::Instant::now() < end,
            "no checkpoint: {state:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert_eq!(checkpoint.content.manifest.position, Some(7));
    let state = c.client.read(WAIT).await.unwrap();
    assert_eq!(state.jobs["logical"].attempts.len(), 1);
    let suspend = Request {
        origin: c.config.cluster.clone(),
        id: "suspend-logical".into(),
        mutation: Mutation::SuspendJob {
            id: "logical".into(),
            expected_revision: state.jobs["logical"].revision,
            checkpoint: checkpoint.id.clone(),
        },
    };
    let accepted = c.client.write(suspend.clone(), WAIT).await.unwrap();
    assert!(matches!(accepted, Reply::Accepted { .. }), "{accepted:?}");
    assert_eq!(
        c.client.write(suspend.clone(), WAIT).await.unwrap(),
        accepted
    );
    let state = c.client.read(WAIT).await.unwrap();
    assert_eq!(state.jobs["logical"].phase, JobPhase::Suspending);
    let mut probe = state.clone();
    assert!(
        matches!(
            probe.resume_job("logical", state.jobs["logical"].revision),
            Reply::Rejected { .. }
        ),
        "cannot resume before cleanup"
    );
    assert!(!state.current_attempt("logical").unwrap().terminal());
    // A new leader and restarted worker continue the same committed suspension.
    let leader = c.leader().await;
    c.replicas
        .remove(&leader)
        .unwrap()
        .shutdown()
        .await
        .unwrap();
    w.shutdown().await.unwrap();
    let mut w = Worker::start(&home, config.clone(), hello()).await.unwrap();
    wait(
        &mut c,
        &mut w,
        "logical",
        WorkPhase::Job(AttemptPhase::Suspended),
    )
    .await;
    let state = c.client.read(WAIT).await.unwrap();
    assert_eq!(state.jobs["logical"].phase, JobPhase::Suspended);
    assert!(
        instances::list(&home.join("backend"))
            .unwrap()
            .iter()
            .all(|vm| vm.observed.state == instances::State::Deleted)
    );
    let original = state.current_attempt("logical").unwrap().clone();
    let root = fleet::vm_job::checkpoint_files_dir(
        &home.join("backend/jobs").join(&original.execution),
        &checkpoint.content.id,
    )
    .unwrap();
    assert_eq!(std::fs::read(root.join("state")).unwrap(), b"7");
    // Lost-quorum recovery carries both checkpoint metadata and the selected suspension point.
    w.shutdown().await.unwrap();
    instances::write(&home.join("worker.json"), &config).unwrap();
    let source = c.leader().await;
    let (_, manifest) = super::recovery::restore(&mut c, source, "checkpoint-recovered").await;
    Worker::accept_recovery(&home, &manifest).unwrap();
    let config: WorkerConfig =
        serde_json::from_slice(&std::fs::read(home.join("worker.json")).unwrap()).unwrap();
    let mut w = Worker::start(&home, config.clone(), hello()).await.unwrap();
    super::recovery::reconciled(&mut c, &mut w).await;
    super::recovery::resume(&mut c, &[]).await;
    let state = c.client.read(WAIT).await.unwrap();
    assert_eq!(state.checkpoints[&checkpoint.id], checkpoint);
    assert_eq!(state.jobs["logical"].phase, JobPhase::Suspended);
    let mut probe = state.clone();
    assert!(matches!(
        probe.resume_job("logical", state.jobs["logical"].revision),
        Reply::Accepted { .. }
    ));
    assert!(matches!(
        probe.place_queued("logical", "worker-real", probe.jobs["logical"].revision),
        Reply::Accepted { .. }
    ));
    let cursor = probe.workers["worker-real"].cursor();
    let report = Report {
        worker: "worker-real".into(),
        incarnation: config.registration.incarnation.clone(),
        sequence: probe.cursors[&cursor] + 1,
        assignment: original.id.clone(),
        revision: original.revision,
        observation: Observation {
            phase: WorkPhase::Job(AttemptPhase::Succeeded),
            detail: None,
        },
    };
    assert_eq!(
        probe.report(&config.registration.peer.identity.to_string(), &report),
        Reply::Recorded
    );
    assert_eq!(
        probe.jobs["logical"].phase,
        JobPhase::Active,
        "old attempt cannot complete a resumed job"
    );
    assert!(!probe.current_attempt("logical").unwrap().terminal());
    let resume = Request {
        origin: c.config.cluster.clone(),
        id: "resume-logical".into(),
        mutation: Mutation::ResumeJob {
            worker: Default::default(),
            id: "logical".into(),
            expected_revision: state.jobs["logical"].revision,
        },
    };
    let accepted = c.client.write(resume.clone(), WAIT).await.unwrap();
    assert!(matches!(accepted, Reply::Accepted { .. }), "{accepted:?}");
    assert_eq!(
        c.client.write(resume.clone(), WAIT).await.unwrap(),
        accepted
    );
    wait(
        &mut c,
        &mut w,
        "logical",
        WorkPhase::Job(AttemptPhase::Succeeded),
    )
    .await;
    let state = c.client.read(WAIT).await.unwrap();
    assert_eq!(state.jobs["logical"].phase, JobPhase::Succeeded);
    assert_eq!(
        state.jobs["logical"].workload, workload,
        "submitted intent never changes"
    );
    assert_eq!(state.jobs["logical"].attempts.len(), 2);
    assert_eq!(
        state.assignments[&original.id], original,
        "old attempt history remains immutable"
    );
    let resumed = state.current_attempt("logical").unwrap();
    assert_eq!(
        resumed.attempt.as_ref().unwrap().checkpoint.as_deref(),
        Some(checkpoint.id.as_str())
    );
    assert_ne!(resumed.execution, original.execution);
    assert_eq!(jobs::list(&home.join("backend")).unwrap().len(), 2);
    for (a, expected) in [(&original, "initial-once\n"), (resumed, "resumed-once\n")] {
        assert_eq!(stdout(&home, &a.execution), expected);
    }
    let store = fleet::blobs::open(&c.home.0.join("resumed-outputs"))
        .await
        .unwrap();
    let dest = c.home.0.join("fetched");
    fetch_outputs(
        &store,
        &c.client.endpoint,
        &c.config.cluster,
        &config.registration.peer,
        &resumed.execution,
        &dest,
    )
    .await
    .unwrap();
    assert_eq!(
        std::fs::read_to_string(dest.join("result.txt")).unwrap(),
        "8"
    );
    store.shutdown().await.unwrap();
    assert_eq!(
        job_views(&state)[0]["attempts"].as_array().unwrap().len(),
        2
    );
    w.shutdown().await.unwrap();
    c.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn incomplete_or_corrupt_checkpoints_never_publish_or_block_a_later_valid_checkpoint() {
    let mut c = Cluster::new(1).await;
    let (home, config) = prepare(&mut c, true).await;
    let mut w = Worker::start(&home, config.clone(), hello()).await.unwrap();
    let mut workload = resumable();
    let Workload::Job { spec, .. } = &mut workload else {
        unreachable!()
    };
    spec.argv[2]=r#"
import hashlib,json,pathlib,time
for name,contents,declared in [('a-bad',b'bad',b'yes'),('b-partial',b'',b'complete'),('c-good',b'9',b'9')]:
    p=pathlib.Path('cp')/name; p.mkdir(parents=True); (p/'state').write_bytes(contents)
    m={'version':1,'position':9,'files':{'state':{'size':len(declared),'sha256':hashlib.sha256(declared).hexdigest()}}}
    (p/'fleet.json').write_text(json.dumps(m))
print('still-running',flush=True)
time.sleep(150)
"#.into();
    queued(&mut c, workload).await;
    let end = tokio::time::Instant::now() + Duration::from_secs(90);
    loop {
        w.tick().await.unwrap();
        let s = c.client.read(WAIT).await.unwrap();
        if !s.checkpoints.is_empty() {
            assert_eq!(s.checkpoints.len(), 1);
            assert_eq!(
                s.checkpoints.values().next().unwrap().content.source,
                "cp/c-good/fleet.json"
            );
            assert_eq!(s.jobs["logical"].phase, JobPhase::Active);
            let a = s.current_attempt("logical").unwrap();
            let dir = home.join("backend/jobs").join(&a.execution);
            assert!(dir.join("checkpoint-error.json").exists());
            assert!(
                std::fs::read_dir(dir.join("checkpoints"))
                    .unwrap()
                    .all(|e| !e
                        .unwrap()
                        .file_name()
                        .to_string_lossy()
                        .starts_with("pending-"))
            );
            let mut clone = s.clone();
            assert!(matches!(
                clone.suspend_job("logical", s.jobs["logical"].revision, "unknown"),
                Reply::Rejected { .. }
            ));
            let mut report = Report {
                worker: config.registration.id.clone(),
                incarnation: config.registration.incarnation.clone(),
                sequence: s.cursors[&config.registration.cursor()] + 1,
                assignment: a.id.clone(),
                revision: a.revision,
                observation: Observation {
                    phase: WorkPhase::Instance(InstancePhase::Deleted),
                    detail: None,
                },
            };
            assert!(matches!(
                clone.report(&config.registration.peer.identity.to_string(), &report),
                Reply::Rejected { .. }
            ));
            report.observation.phase = WorkPhase::Job(AttemptPhase::Suspended);
            assert!(matches!(
                clone.report(&config.registration.peer.identity.to_string(), &report),
                Reply::Rejected { .. }
            ));
            assert_eq!(
                clone, s,
                "invalid observations leave resources and history unchanged"
            );
            let reply = c
                .client
                .write(
                    Request {
                        origin: c.config.cluster.clone(),
                        id: "cancel-test".into(),
                        mutation: Mutation::CancelQueued {
                            id: "logical".into(),
                            expected_revision: s.jobs["logical"].revision,
                        },
                    },
                    WAIT,
                )
                .await
                .unwrap();
            assert!(matches!(reply, Reply::Accepted { .. }));
            break;
        }
        assert!(
            tokio::time::Instant::now() < end,
            "no valid checkpoint: {s:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    wait(
        &mut c,
        &mut w,
        "logical",
        WorkPhase::Job(AttemptPhase::Cancelled),
    )
    .await;
    w.shutdown().await.unwrap();
    c.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn checkpoint_backup_restores_without_worker_payloads_and_retention_preserves_history() {
    use fleet::control::{
        artifact::{self, CopyPhase, Kind},
        artifact_io, storage,
    };
    let mut c = Cluster::new(3).await;
    let (home, config) = prepare(&mut c, true).await;
    let storage_home = c.home.0.join("storage");
    let key = fleet::keys::load_or_create(&storage_home.join("node.key")).unwrap();
    let socket = reserve_socket();
    let registration = artifact::Registration {
        id: "backup".into(),
        peer: Peer {
            identity: key.public(),
            address: socket.local_addr().unwrap(),
        },
        incarnation: "storage-1".into(),
        failure_domain: "same-test-machine".into(),
        capacity_bytes: 16 << 20,
    };
    let storage_config = storage::Config {
        cluster: c.config.cluster.clone(),
        registration: registration.clone(),
        controllers: c.config.peers.clone(),
        administrators: c.config.administrators.clone(),
    };
    storage::Agent::initialize(&storage_home, &storage_config).unwrap();
    drop(socket);
    for (id, mutation) in [
        ("storage-enroll", Mutation::EnrollStorage(registration)),
        (
            "storage-policy",
            Mutation::StoragePolicy {
                expected_revision: 0,
                policy: artifact::Policy {
                    history: Default::default(),
                    inputs: Default::default(),
                    checkpoints: BTreeSet::from(["backup".into()]),
                    outputs: BTreeSet::from(["backup".into()]),
                },
            },
        ),
    ] {
        assert_eq!(
            c.client
                .write(
                    Request {
                        origin: c.config.cluster.clone(),
                        id: id.into(),
                        mutation
                    },
                    WAIT
                )
                .await
                .unwrap(),
            Reply::Recorded
        );
    }
    let mut storage = storage::Agent::start(&storage_home, storage_config.clone())
        .await
        .unwrap();
    let mut w = Worker::start(&home, config.clone(), hello()).await.unwrap();
    queued(&mut c, resumable()).await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    let checkpoint = loop {
        if let Err(error) = w.tick().await {
            eprintln!("worker will retry: {error:#}");
        }
        if let Err(error) = storage.tick().await {
            eprintln!("storage will retry: {error:#}");
        }
        let state = c.client.read(WAIT).await.unwrap();
        if let Some(a) = state.storage.artifacts.values().find(|a| {
            matches!(a.manifest.kind, Kind::Checkpoint(_))
                && a.copies["storage/backup"].phase == CopyPhase::Ready
        }) {
            let Kind::Checkpoint(id) = &a.manifest.kind else {
                unreachable!()
            };
            break state.checkpoints[id].clone();
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "checkpoint backup not ready: {state:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    let mut state = c.client.read(WAIT).await.unwrap();
    let artifact_id = state
        .storage
        .artifacts
        .values()
        .find(|a| a.manifest.kind == Kind::Checkpoint(checkpoint.id.clone()))
        .unwrap()
        .manifest
        .id
        .clone();
    // The latest resumable checkpoint cannot be removed, even without an explicit pin.
    let revision = state.storage.artifacts[&artifact_id].revision;
    assert!(matches!(
        state.delete_artifact(&artifact_id, revision),
        Reply::Rejected { .. }
    ));
    let reply = c
        .client
        .write(
            Request {
                origin: c.config.cluster.clone(),
                id: "backup-suspend".into(),
                mutation: Mutation::SuspendJob {
                    id: "logical".into(),
                    expected_revision: state.jobs["logical"].revision,
                    checkpoint: checkpoint.id.clone(),
                },
            },
            WAIT,
        )
        .await
        .unwrap();
    assert!(matches!(reply, Reply::Accepted { .. }));
    wait(
        &mut c,
        &mut w,
        "logical",
        WorkPhase::Job(AttemptPhase::Suspended),
    )
    .await;
    w.store.shutdown().await.unwrap();
    w.shutdown().await.unwrap();
    // Make every source payload and source blob unavailable. Keep execution identity/history intact.
    std::fs::rename(
        home.join("blobs"),
        c.home.0.join("unavailable-worker-blobs"),
    )
    .unwrap();
    let state = c.client.read(WAIT).await.unwrap();
    let original = state.current_attempt("logical").unwrap();
    let checkpoint_root = fleet::vm_job::checkpoint_files_dir(
        &home.join("backend/jobs").join(&original.execution),
        &checkpoint.content.id,
    )
    .unwrap();
    std::fs::rename(
        &checkpoint_root,
        c.home.0.join("unavailable-checkpoint-files"),
    )
    .unwrap();
    let before_history = state.assignments[&original.id].clone();
    // A fresh storage process verifies its retained copy; exporting works with the worker offline.
    storage.shutdown().await.unwrap();
    let mut storage = storage::Agent::start(&storage_home, storage_config)
        .await
        .unwrap();
    storage.tick().await.unwrap();
    let export = c.home.0.join("backup-export");
    artifact_io::fetch_available(
        &c.client.endpoint,
        &state,
        &state.storage.artifacts[&artifact_id],
        &export,
    )
    .await
    .unwrap();
    assert_eq!(std::fs::read(export.join("state")).unwrap(), b"7");
    // Retained artifacts and requests survive a change of Raft leader.
    let leader = c.leader().await;
    c.replicas
        .remove(&leader)
        .unwrap()
        .shutdown()
        .await
        .unwrap();
    let state = c.client.read(WAIT).await.unwrap();
    let reply = c
        .client
        .write(
            Request {
                origin: c.config.cluster.clone(),
                id: "backup-resume".into(),
                mutation: Mutation::ResumeJob {
                    worker: Default::default(),
                    id: "logical".into(),
                    expected_revision: state.jobs["logical"].revision,
                },
            },
            WAIT,
        )
        .await
        .unwrap();
    assert!(matches!(reply, Reply::Accepted { .. }));
    let mut w = Worker::start(&home, config, hello()).await.unwrap();
    wait(
        &mut c,
        &mut w,
        "logical",
        WorkPhase::Job(AttemptPhase::Succeeded),
    )
    .await;
    let state = c.client.read(WAIT).await.unwrap();
    assert_eq!(state.jobs["logical"].attempts.len(), 2);
    assert_eq!(state.assignments[&before_history.id], before_history);
    assert_eq!(
        std::fs::read(home.join("restored").join(&artifact_id).join("state")).unwrap(),
        b"7"
    );
    w.tick().await.unwrap();
    storage.tick().await.unwrap();
    let state = c.client.read(WAIT).await.unwrap();
    let outputs = state
        .storage
        .artifacts
        .values()
        .find(|a| a.manifest.kind == Kind::FinalOutputs)
        .unwrap();
    assert_eq!(outputs.copies["storage/backup"].phase, CopyPhase::Ready);
    let captured = c.home.0.join("final-output-export");
    artifact_io::fetch_available(&c.client.endpoint, &state, outputs, &captured)
        .await
        .unwrap();
    assert_eq!(std::fs::read(captured.join("result.txt")).unwrap(), b"8");
    // Pins and expiry are independent. Expiry is deterministic and never bypasses a pin.
    let rev = state.storage.artifacts[&artifact_id].revision;
    let request = |id: &str, mutation| Request {
        origin: "cluster".into(),
        id: id.into(),
        mutation,
    };
    assert_eq!(
        c.client
            .write(
                request(
                    "pin",
                    Mutation::ArtifactRetention {
                        id: artifact_id.clone(),
                        expected_revision: rev,
                        pinned: true,
                        expires_at: Some(1)
                    }
                ),
                WAIT
            )
            .await
            .unwrap(),
        Reply::Recorded
    );
    c.client
        .write(
            request("expire-pinned", Mutation::ExpireArtifacts { as_of: 2 }),
            WAIT,
        )
        .await
        .unwrap();
    let state = c.client.read(WAIT).await.unwrap();
    assert!(!state.storage.artifacts[&artifact_id].deleted);
    let rev = state.storage.artifacts[&artifact_id].revision;
    c.client
        .write(
            request(
                "unpin",
                Mutation::ArtifactRetention {
                    id: artifact_id.clone(),
                    expected_revision: rev,
                    pinned: false,
                    expires_at: Some(1),
                },
            ),
            WAIT,
        )
        .await
        .unwrap();
    c.client
        .write(
            request("expire", Mutation::ExpireArtifacts { as_of: 2 }),
            WAIT,
        )
        .await
        .unwrap();
    w.tick().await.unwrap();
    storage.tick().await.unwrap();
    let state = c.client.read(WAIT).await.unwrap();
    assert!(state.storage.artifacts[&artifact_id].deleted);
    assert_eq!(
        state.storage.artifacts[&artifact_id].copies["storage/backup"].phase,
        CopyPhase::Deleted
    );
    assert_eq!(
        state.storage.artifacts[&artifact_id].copies["worker/worker-real"].phase,
        CopyPhase::Deleted
    );
    assert!(!storage_home.join("objects").join(&artifact_id).exists());
    assert!(
        !home
            .join("restored")
            .join(&artifact_id)
            .join("state")
            .exists()
    );
    assert_eq!(state.jobs["logical"].attempts.len(), 2);
    assert!(
        state.checkpoints.contains_key(&checkpoint.id),
        "history must survive payload deletion"
    );
    assert!(
        artifact_io::fetch_available(
            &c.client.endpoint,
            &state,
            &state.storage.artifacts[&artifact_id],
            &c.home.0.join("deleted-export")
        )
        .await
        .is_err()
    );
    storage.shutdown().await.unwrap();
    w.shutdown().await.unwrap();
    c.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn format_six_upgrade_preserves_suspended_checkpoint_and_request_receipts() {
    upgrade_suspended(6).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn format_seven_upgrade_preserves_suspended_checkpoint_and_request_receipts() {
    upgrade_suspended(7).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn format_eight_upgrade_preserves_artifact_identity_and_suspended_checkpoint() {
    upgrade_suspended(8).await;
}
async fn upgrade_suspended(source_version: u32) {
    fn old_artifact_shape(value: &mut serde_json::Value) {
        match value {
            serde_json::Value::Object(object) => {
                if object.contains_key("files")
                    && object.contains_key("kind")
                    && let Some(origin) = object.remove("origin")
                {
                    object.insert("worker".into(), origin["Attempt"]["worker"].clone());
                    object.insert("attempt".into(), origin["Attempt"]["attempt"].clone());
                }
                for child in object.values_mut() {
                    old_artifact_shape(child);
                }
            }
            serde_json::Value::Array(array) => {
                for child in array {
                    old_artifact_shape(child);
                }
            }
            _ => {}
        }
    }
    use redb::{Database, ReadableTable, TableDefinition};
    let mut c = Cluster::new(1).await;
    let (home, config) = prepare(&mut c, true).await;
    let mut w = Worker::start(&home, config.clone(), hello()).await.unwrap();
    queued(&mut c, resumable()).await;
    let end = tokio::time::Instant::now() + Duration::from_secs(90);
    let cp = loop {
        w.tick().await.unwrap();
        let state = c.client.read(WAIT).await.unwrap();
        if let Some(cp) = state.checkpoints.values().next() {
            break cp.clone();
        }
        assert!(tokio::time::Instant::now() < end);
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    let state = c.client.read(WAIT).await.unwrap();
    let request = Request {
        origin: "cluster".into(),
        id: "upgrade-suspend".into(),
        mutation: Mutation::SuspendJob {
            id: "logical".into(),
            expected_revision: state.jobs["logical"].revision,
            checkpoint: cp.id.clone(),
        },
    };
    let reply = c.client.write(request.clone(), WAIT).await.unwrap();
    wait(
        &mut c,
        &mut w,
        "logical",
        WorkPhase::Job(AttemptPhase::Suspended),
    )
    .await;
    let before = c.client.read(WAIT).await.unwrap();
    w.shutdown().await.unwrap();
    c.replicas.remove(&1).unwrap().shutdown().await.unwrap();
    let controller = c.home.0.join("1");
    for (path, table_name) in [
        (controller.join("control.redb"), "meta"),
        (home.join("worker.redb"), "worker_journal"),
    ] {
        let db = Database::open(path).unwrap();
        let tx = db.begin_write().unwrap();
        {
            let mut table = tx
                .open_table(TableDefinition::<&str, &[u8]>::new(table_name))
                .unwrap();
            let mut value: serde_json::Value =
                serde_json::from_slice(table.get("state").unwrap().unwrap().value()).unwrap();
            value["format"] = serde_json::json!(source_version);
            if source_version == 6 {
                for field in ["storage", "artifacts", "deleted_artifacts"] {
                    value.as_object_mut().unwrap().remove(field);
                }
            } else if source_version == 7
                && let Some(catalog) = value.get_mut("storage")
            {
                catalog.as_object_mut().unwrap().remove("next_ordinal");
                for artifact in catalog["artifacts"].as_object_mut().unwrap().values_mut() {
                    artifact.as_object_mut().unwrap().remove("ordinal");
                }
            }
            old_artifact_shape(&mut value);
            table
                .insert("state", serde_json::to_vec(&value).unwrap().as_slice())
                .unwrap();
            if table_name == "meta" {
                let mut identity: serde_json::Value =
                    serde_json::from_slice(table.get("identity").unwrap().unwrap().value())
                        .unwrap();
                identity[0] = serde_json::json!(source_version);
                table
                    .insert(
                        "identity",
                        serde_json::to_vec(&identity).unwrap().as_slice(),
                    )
                    .unwrap();
            }
        }
        tx.commit().unwrap();
    }
    fleet::control::upgrade::run(&controller, std::slice::from_ref(&home), &[]).unwrap();
    assert!(
        controller
            .join(format!("control.redb.v{source_version}-backup"))
            .exists()
    );
    assert!(
        home.join(format!("worker.redb.v{source_version}-backup"))
            .exists()
    );
    let mut cfg = c.config.clone();
    cfg.id = 1;
    c.replicas
        .insert(1, Replica::start(&controller, cfg).await.unwrap());
    let after = c.client.read(WAIT).await.unwrap();
    assert_eq!(after.jobs, before.jobs);
    assert_eq!(after.checkpoints, before.checkpoints);
    assert_eq!(after.receipts, before.receipts);
    assert_eq!(after.assignments, before.assignments);
    assert_eq!(
        after
            .storage
            .artifacts
            .values()
            .filter(|a| matches!(
                a.manifest.kind,
                fleet::control::artifact::Kind::Checkpoint(_)
            ))
            .count(),
        1
    );
    assert_eq!(c.client.write(request, WAIT).await.unwrap(), reply);
    let mut w = Worker::start(&home, config.clone(), hello()).await.unwrap();
    w.tick().await.unwrap();
    let artifact = after
        .storage
        .artifacts
        .values()
        .find(|a| {
            matches!(
                a.manifest.kind,
                fleet::control::artifact::Kind::Checkpoint(_)
            )
        })
        .unwrap();
    let destination = c.home.0.join("upgraded-export");
    fleet::control::artifact_io::fetch_available(
        &c.client.endpoint,
        &after,
        artifact,
        &destination,
    )
    .await
    .unwrap();
    assert_eq!(std::fs::read(destination.join("state")).unwrap(), b"7");
    // Without a backup target, intact sealed checkpoint files can rebuild a lost blob cache.
    w.store.shutdown().await.unwrap();
    w.shutdown().await.unwrap();
    std::fs::rename(
        home.join("blobs"),
        c.home.0.join("unavailable-upgraded-cache"),
    )
    .unwrap();
    let reply = c
        .client
        .write(
            Request {
                origin: "cluster".into(),
                id: "resume-with-local-sealed-copy".into(),
                mutation: Mutation::ResumeJob {
                    worker: Default::default(),
                    id: "logical".into(),
                    expected_revision: after.jobs["logical"].revision,
                },
            },
            WAIT,
        )
        .await
        .unwrap();
    assert!(matches!(reply, Reply::Accepted { .. }));
    let mut w = Worker::start(&home, config, hello()).await.unwrap();
    wait(
        &mut c,
        &mut w,
        "logical",
        WorkPhase::Job(AttemptPhase::Succeeded),
    )
    .await;
    w.shutdown().await.unwrap();
    c.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rolling_capture_reclaims_beyond_sixteen_preserves_pin_and_resumes_after_restart() {
    let mut c = Cluster::new(1).await;
    let (home, config) = prepare(&mut c, true).await;
    let mut w = Worker::start(&home, config.clone(), hello()).await.unwrap();
    let mut work = resumable();
    let Workload::Job { spec, .. } = &mut work else {
        unreachable!()
    };
    spec.argv = vec!["/usr/bin/python3".into(), "-c".into(), r#"
import hashlib,json,pathlib,time
for n in range(1,29):
 p=pathlib.Path('cp/%03d'%n); p.mkdir(parents=True)
 data=(str(n)+'\n').encode()+b'x'*(65536-len(str(n))-1)
 (p/'state').write_bytes(data)
 m={'version':1,'position':n,'files':{'state':{'size':len(data),'sha256':hashlib.sha256(data).hexdigest()}}}
 (p/'ready.tmp').write_text(json.dumps(m)); (p/'ready.tmp').rename(p/'fleet.json')
print('initial-once',flush=True)
time.sleep(150)
"#.into()];
    let policy = spec.checkpoint.as_mut().unwrap();
    policy.retention = Some(fleet::checkpoint::Rolling {
        keep_last: 2,
        max_live_checkpoints: 6,
        max_live_bytes: 2 << 20,
        transfer_bytes_per_hour: 8 << 20,
    });
    policy.resume_argv = vec!["/usr/bin/python3".into(),"-c".into(),
        "import pathlib,sys; n=int(pathlib.Path(sys.argv[1]).read_text().splitlines()[0]); pathlib.Path('result.txt').write_text(str(n+1))".into(),"{checkpoint}/state".into()];
    queued(&mut c, work).await;
    let unrelated = w.store.blobs().add_slice(b"unrelated cache").await.unwrap();
    w.store.tags().delete(unrelated.name).await.unwrap();
    let mut pinned = None;
    let mut restarted = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(110);
    let state = loop {
        w.tick().await.unwrap();
        let state = c.client.read(WAIT).await.unwrap();
        if pinned.is_none() && state.checkpoints.len() >= 6 {
            let latest = fleet::control::client::latest_checkpoint(&state, "logical").unwrap();
            let artifact = state
                .storage
                .artifacts
                .values()
                .find(|a| {
                    a.manifest.kind == fleet::control::artifact::Kind::Checkpoint(latest.id.clone())
                })
                .unwrap();
            let reply = c
                .client
                .write(
                    Request {
                        origin: c.config.cluster.clone(),
                        id: "pin-research-point".into(),
                        mutation: Mutation::ArtifactRetention {
                            id: artifact.manifest.id.clone(),
                            expected_revision: artifact.revision,
                            pinned: true,
                            expires_at: None,
                        },
                    },
                    WAIT,
                )
                .await
                .unwrap();
            assert_eq!(reply, Reply::Recorded);
            pinned = Some(artifact.manifest.id.clone());
        }
        if !restarted && state.checkpoints.len() >= 12 {
            w.shutdown().await.unwrap();
            w = Worker::start(&home, config.clone(), hello()).await.unwrap();
            restarted = true;
        }
        if state.checkpoints.len() == 28
            && state
                .storage
                .artifacts
                .values()
                .filter(|a| {
                    !a.deleted
                        && matches!(
                            a.manifest.kind,
                            fleet::control::artifact::Kind::Checkpoint(_)
                        )
                })
                .count()
                == 3
        {
            let report: serde_json::Value = jobs::read_json(&home.join("storage-report.json"))
                .unwrap()
                .unwrap();
            let usage = report["checkpoints"]
                .as_object()
                .unwrap()
                .values()
                .next()
                .unwrap();
            if usage["live_checkpoints"] == 3
                && report["blob_payload_bytes"].as_u64().unwrap() < 4 * 65536
            {
                break state;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "rolling capture stalled: {:?}; report {:?}",
            state.checkpoints.len(),
            std::fs::read_to_string(home.join("storage-report.json"))
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert!(restarted);
    assert!(!state.storage.artifacts[&pinned.unwrap()].deleted);
    assert!(w.store.blobs().has(unrelated.hash).await.unwrap());
    let latest = fleet::control::client::latest_checkpoint(&state, "logical").unwrap();
    assert_eq!(latest.content.manifest.position, Some(28));
    let reply = c
        .client
        .write(
            Request {
                origin: c.config.cluster.clone(),
                id: "rolling-suspend".into(),
                mutation: Mutation::SuspendJob {
                    id: "logical".into(),
                    expected_revision: state.jobs["logical"].revision,
                    checkpoint: latest.id.clone(),
                },
            },
            WAIT,
        )
        .await
        .unwrap();
    assert!(matches!(reply, Reply::Accepted { .. }));
    wait(
        &mut c,
        &mut w,
        "logical",
        WorkPhase::Job(AttemptPhase::Suspended),
    )
    .await;
    let state = c.client.read(WAIT).await.unwrap();
    let reply = c
        .client
        .write(
            Request {
                origin: c.config.cluster.clone(),
                id: "rolling-resume".into(),
                mutation: Mutation::ResumeJob {
                    worker: Default::default(),
                    id: "logical".into(),
                    expected_revision: state.jobs["logical"].revision,
                },
            },
            WAIT,
        )
        .await
        .unwrap();
    assert!(matches!(reply, Reply::Accepted { .. }));
    let final_attempt = wait(
        &mut c,
        &mut w,
        "logical",
        WorkPhase::Job(AttemptPhase::Succeeded),
    )
    .await;
    assert_eq!(
        std::fs::read_to_string(
            home.join("backend/jobs")
                .join(final_attempt.execution)
                .join("work/result.txt")
        )
        .unwrap(),
        "29"
    );
    w.shutdown().await.unwrap();
    c.close().await;
}
