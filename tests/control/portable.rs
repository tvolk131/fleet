use super::*;
use fleet::control::{
    artifact::{self, CopyPhase, Kind},
    input_upload,
    portable::{Platform, Runtime},
    queue::Submission,
};

pub(super) fn runtime() -> Runtime {
    Runtime {
        platform: Platform {
            os: "macos".into(),
            arch: "aarch64".into(),
            capabilities: BTreeSet::new(),
        },
        image_digest: format!("sha256:{}", "a".repeat(64)),
    }
}

async fn write(c: &mut Cluster, id: &str, mutation: Mutation) -> Reply {
    c.client
        .write(
            Request {
                origin: c.config.cluster.clone(),
                id: id.into(),
                mutation,
            },
            WAIT,
        )
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn portable_inputs_and_checkpoint_resume_on_another_worker_without_source_or_client() {
    let mut c = Cluster::new(3).await;
    let (a_home, a_config) = prepare_named(&mut c, true, 4, 8192, "worker-real", true).await;
    let (b_home, b_config) = prepare_named(&mut c, true, 4, 8192, "destination", true).await;
    let (_storage_home, storage_config, mut storage) =
        super::storage::backup(&mut c, "portable-store", 32 << 20).await;
    assert_eq!(
        write(
            &mut c,
            "policy",
            Mutation::StoragePolicy {
                expected_revision: 0,
                policy: artifact::Policy {
                    inputs: BTreeSet::from(["portable-store".into()]),
                    checkpoints: BTreeSet::from(["portable-store".into()]),
                    outputs: BTreeSet::new()
                }
            }
        )
        .await,
        Reply::Recorded
    );
    let mut a = Worker::start(&a_home, a_config.clone(), hello())
        .await
        .unwrap();
    let mut b = Worker::start(&b_home, b_config.clone(), hello())
        .await
        .unwrap();
    write(
        &mut c,
        "drain-b",
        Mutation::Drain {
            worker: "destination".into(),
            drain: true,
        },
    )
    .await;
    let mut workload = super::checkpoints::resumable();
    let Workload::Job { spec, .. } = &mut workload else {
        unreachable!()
    };
    spec.vm.as_mut().unwrap().runtime = Some(runtime());
    // Both the executable bit and an ordinary input must survive storage and new-worker staging.
    let root = c.home.0.join("client-inputs");
    std::fs::create_dir(&root).unwrap();
    let script = format!("#!/usr/bin/python3\n{}", spec.argv[2]);
    std::fs::write(root.join("trainer"), script.as_bytes()).unwrap();
    std::fs::write(root.join("dataset"), b"immutable training data").unwrap();
    let mut files = Vec::new();
    for name in ["trainer", "dataset"] {
        let hash = fleet::blobs::hash_file(&root.join(name)).unwrap();
        spec.inputs.insert(
            name.into(),
            jobs::Input {
                hash,
                executable: name == "trainer",
            },
        );
        files.push(jobs::OutputFile {
            path: name.into(),
            hash,
            size: std::fs::metadata(root.join(name)).unwrap().len(),
        });
    }
    spec.argv = vec!["./trainer".into()];
    spec.checkpoint.as_mut().unwrap().resume_argv[2]="import pathlib,sys,os; assert os.access('trainer',os.X_OK); assert pathlib.Path('dataset').read_bytes()==b'immutable training data'; n=int(pathlib.Path(sys.argv[1]).read_text()); pathlib.Path('result.txt').write_text(str(n+1)); print('resumed-once',flush=True)".into();
    let bundle = artifact::Manifest::inputs("portable".into(), files).unwrap();
    let submission = Submission {
        id: "portable".into(),
        inputs: Some(bundle.clone()),
        workload: workload.clone(),
        worker: None,
        priority: 0,
        requires: BTreeSet::new(),
        after: BTreeSet::new(),
    };
    assert!(matches!(
        write(&mut c, "submit", Mutation::Enqueue(submission)).await,
        Reply::Accepted { .. }
    ));
    a.tick().await.unwrap();
    b.tick().await.unwrap();
    let state = c.client.read(WAIT).await.unwrap();
    assert!(state.jobs["portable"].attempts.is_empty());
    assert!(
        state
            .placement_candidate("portable", "worker-real")
            .unwrap_err()
            .to_string()
            .contains("input storage")
    );
    assert!(matches!(
        state.clone().delete_artifact(&bundle.id, 1),
        Reply::Rejected { .. }
    ));
    storage.tick().await.unwrap();
    input_upload::upload(
        &c.client.endpoint,
        &storage_config.registration.peer,
        &c.config.cluster,
        &bundle,
        &root,
    )
    .await
    .unwrap();
    // Retrying a completed upload does not rewrite or duplicate its content.
    input_upload::upload(
        &c.client.endpoint,
        &storage_config.registration.peer,
        &c.config.cluster,
        &bundle,
        &root,
    )
    .await
    .unwrap();
    std::fs::remove_dir_all(&root).unwrap();
    storage.tick().await.unwrap();
    let compatible = c.client.read(WAIT).await.unwrap();
    for field in ["image", "platform", "capability", "missing"] {
        let mut invalid = compatible.clone();
        let available = invalid.workers.get_mut("worker-real").unwrap();
        match field {
            "image" => {
                available.runtimes.get_mut("macos").unwrap().image_digest =
                    format!("sha256:{}", "b".repeat(64))
            }
            "platform" => {
                available.runtimes.get_mut("macos").unwrap().platform.arch = "x86_64".into()
            }
            "capability" => {
                let Workload::Job { spec, .. } =
                    &mut invalid.jobs.get_mut("portable").unwrap().workload
                else {
                    unreachable!()
                };
                spec.vm
                    .as_mut()
                    .unwrap()
                    .runtime
                    .as_mut()
                    .unwrap()
                    .platform
                    .capabilities
                    .insert("metal:required-feature".into());
            }
            _ => available.runtimes.clear(),
        }
        assert!(
            invalid
                .placement_candidate("portable", "worker-real")
                .is_err(),
            "accepted incompatible {field}"
        );
    }
    let end = tokio::time::Instant::now() + Duration::from_secs(90);
    let cp = loop {
        a.tick().await.unwrap();
        b.tick().await.unwrap();
        storage.tick().await.unwrap();
        let state = c.client.read(WAIT).await.unwrap();
        if let Some(cp) = state
            .checkpoints
            .values()
            .find(|cp| cp.job.as_ref() == "portable")
            && state.storage.artifacts.values().any(|art| {
                art.manifest.kind == Kind::Checkpoint(cp.id.clone())
                    && art.copies["storage/portable-store"].phase == CopyPhase::Ready
            })
        {
            break cp.clone();
        }
        assert!(
            tokio::time::Instant::now() < end,
            "checkpoint missing: {state:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    let state = c.client.read(WAIT).await.unwrap();
    assert!(matches!(
        write(
            &mut c,
            "suspend",
            Mutation::SuspendJob {
                id: "portable".into(),
                expected_revision: state.jobs["portable"].revision,
                checkpoint: cp.id.clone()
            }
        )
        .await,
        Reply::Accepted { .. }
    ));
    let state = c.client.read(WAIT).await.unwrap();
    assert!(
        matches!(
            state.clone().resume_job_on(
                "portable",
                state.jobs["portable"].revision,
                Some("destination")
            ),
            Reply::Rejected { .. }
        ),
        "must wait for VM cleanup"
    );
    wait(
        &mut c,
        &mut a,
        "portable",
        WorkPhase::Job(AttemptPhase::Suspended),
    )
    .await;
    let state = c.client.read(WAIT).await.unwrap();
    let mut pinned = state.clone();
    pinned.jobs.get_mut("portable").unwrap().worker = Some("worker-real".into());
    assert!(
        matches!(
            pinned.resume_job_on(
                "portable",
                state.jobs["portable"].revision,
                Some("destination")
            ),
            Reply::Rejected { .. }
        ),
        "resume must preserve explicit worker restriction"
    );
    let first = state.current_attempt("portable").unwrap().clone();
    assert_eq!(first.worker, "worker-real");
    a.shutdown().await.unwrap();
    // The destination must not access source files, the source journal, or the original client spool.
    std::fs::rename(&a_home, c.home.0.join("unavailable-source")).unwrap();
    let leader = c.leader().await;
    c.replicas
        .remove(&leader)
        .unwrap()
        .shutdown()
        .await
        .unwrap();
    write(
        &mut c,
        "undrain-b",
        Mutation::Drain {
            worker: "destination".into(),
            drain: false,
        },
    )
    .await;
    let request = Request {
        origin: c.config.cluster.clone(),
        id: "resume-on-b".into(),
        mutation: Mutation::ResumeJob {
            id: "portable".into(),
            expected_revision: state.jobs["portable"].revision,
            worker: Some("destination".into()),
        },
    };
    let reply = c.client.write(request.clone(), WAIT).await.unwrap();
    assert!(matches!(reply, Reply::Accepted { .. }), "{reply:?}");
    assert_eq!(c.client.write(request, WAIT).await.unwrap(), reply);
    b.shutdown().await.unwrap();
    let mut b = Worker::start(&b_home, b_config, hello()).await.unwrap();
    wait(
        &mut c,
        &mut b,
        "portable",
        WorkPhase::Job(AttemptPhase::Succeeded),
    )
    .await;
    let state = c.client.read(WAIT).await.unwrap();
    let resumed = state.current_attempt("portable").unwrap();
    assert_eq!(resumed.worker, "destination");
    assert_eq!(state.jobs["portable"].attempts.len(), 2);
    assert_eq!(state.jobs["portable"].workload, workload);
    assert_eq!(state.assignments[&first.id], first);
    assert_eq!(
        std::fs::read_to_string(
            b_home
                .join("backend/jobs")
                .join(&resumed.execution)
                .join("work/result.txt")
        )
        .unwrap(),
        "8"
    );
    assert!(
        matches!(
            state
                .clone()
                .delete_artifact(&bundle.id, state.storage.artifacts[&bundle.id].revision),
            Reply::Rejected { .. }
        ),
        "retained checkpoint protects executable/input bundle after completion"
    );
    b.shutdown().await.unwrap();
    storage.shutdown().await.unwrap();
    c.close().await;
}
