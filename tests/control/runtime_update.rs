use super::*;
use serde_json::json;

fn update(config: &WorkerConfig) -> Mutation {
    Mutation::AddWorkerRuntimes {
        worker: config.registration.id.clone(),
        incarnation: config.registration.incarnation.clone(),
        runtimes: BTreeMap::from([("macos".into(), portable::runtime())]),
    }
}
fn apply(state: &mut State, mutation: Mutation) -> Reply {
    state.apply(
        Command {
            principal: "admin".into(),
            request: Request {
                origin: state.cluster.clone(),
                id: format!("update-{}", state.receipts.len()),
                mutation,
            },
        },
        100,
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn runtime_update_requires_drain_idle_matching_identity_and_monotonic_catalog() {
    let mut c = Cluster::new(1).await;
    let (_home, config) = prepare(&mut c, true).await;
    let mut state = c.client.read(WAIT).await.unwrap();
    assert!(matches!(
        apply(&mut state, update(&config)),
        Reply::Rejected { .. }
    ));
    state.drained.insert(config.registration.id.clone());
    let mut wrong = config.clone();
    wrong.registration.incarnation = "wrong".into();
    assert!(matches!(
        apply(&mut state, update(&wrong)),
        Reply::Rejected { .. }
    ));
    assert_eq!(apply(&mut state, update(&config)), Reply::Recorded);
    let original = state.workers.clone();
    let mut changed = update(&config);
    if let Mutation::AddWorkerRuntimes { runtimes, .. } = &mut changed {
        runtimes.get_mut("macos").unwrap().image_digest = format!("blake3:{}", "b".repeat(64));
    }
    assert!(matches!(apply(&mut state, changed), Reply::Rejected { .. }));
    let mut removed = update(&config);
    if let Mutation::AddWorkerRuntimes { runtimes, .. } = &mut removed {
        runtimes.clear();
    }
    assert!(matches!(apply(&mut state, removed), Reply::Rejected { .. }));
    assert_eq!(state.workers, original);
    let mut added = update(&config);
    if let Mutation::AddWorkerRuntimes { runtimes, .. } = &mut added {
        runtimes
            .get_mut("macos")
            .unwrap()
            .platform
            .capabilities
            .insert("metal".into());
    }
    assert_eq!(apply(&mut state, added), Reply::Recorded);
    assert!(matches!(
        apply(&mut state, update(&config)),
        Reply::Rejected { .. }
    ));
    state.drained.clear();
    assert!(matches!(
        apply(
            &mut state,
            assign("unfinished", workload("sleep 30")).mutation
        ),
        Reply::Accepted { .. }
    ));
    state.drained.insert(config.registration.id.clone());
    assert!(matches!(
        apply(&mut state, update(&config)),
        Reply::Rejected { .. }
    ));
    c.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn runtime_update_preserves_history_retries_after_partial_local_write_and_replica_failover() {
    let mut c = Cluster::new(3).await;
    let (home, config) = prepare(&mut c, true).await;
    instances::write(&home.join("worker.json"), &config).unwrap();
    let mut worker = Worker::start(&home, config.clone(), hello()).await.unwrap();
    let old_request = assign("completed", workload("echo preserved > result.txt"));
    let old_reply = c.client.write(old_request.clone(), WAIT).await.unwrap();
    wait(
        &mut c,
        &mut worker,
        "completed",
        WorkPhase::Job(AttemptPhase::Succeeded),
    )
    .await;
    assert!(Worker::prepare_runtimes(&home).is_err()); // exclusive worker journal lock
    worker.tick().await.unwrap(); // acknowledge the terminal report before stopping
    worker.shutdown().await.unwrap();
    c.client
        .write(
            Request {
                origin: c.config.cluster.clone(),
                id: "drain".into(),
                mutation: Mutation::Drain {
                    worker: config.registration.id.clone(),
                    drain: true,
                },
            },
            WAIT,
        )
        .await
        .unwrap();
    let before = c.client.read(WAIT).await.unwrap();
    let path = home.join("backend/instances.json");
    let mut vm = instances::Config::load(&home.join("backend")).unwrap();
    vm.platforms
        .insert("macos".into(), portable::runtime().platform);
    instances::write(&path, &vm).unwrap();
    let request = Worker::prepare_runtimes(&home).unwrap();
    // Simulate interruption after the journal commit, before the config rename.
    instances::write(&home.join("worker.json"), &config).unwrap();
    assert!(Worker::start(&home, config.clone(), hello()).await.is_err());
    assert_eq!(Worker::prepare_runtimes(&home).unwrap(), request);
    let next: WorkerConfig =
        serde_json::from_slice(&std::fs::read(home.join("worker.json")).unwrap()).unwrap();
    assert_eq!(next.registration.peer, config.registration.peer);
    assert_eq!(
        next.registration.incarnation,
        config.registration.incarnation
    );
    assert_eq!(
        c.client.write(request.clone(), WAIT).await.unwrap(),
        Reply::Recorded
    );
    let leader = c.leader().await;
    c.replicas
        .remove(&leader)
        .unwrap()
        .shutdown()
        .await
        .unwrap();
    assert_eq!(
        c.client.write(request, WAIT).await.unwrap(),
        Reply::Recorded
    );
    let mut worker = Worker::start(&home, next, hello()).await.unwrap();
    worker.tick().await.unwrap();
    let after = c.client.read(WAIT).await.unwrap();
    assert_eq!(
        serde_json::to_value(&before.assignments).unwrap(),
        serde_json::to_value(&after.assignments).unwrap()
    );
    assert_eq!(
        serde_json::to_value(&before.jobs).unwrap(),
        serde_json::to_value(&after.jobs).unwrap()
    );
    for (id, receipt) in &before.receipts {
        assert_eq!(json!(receipt), json!(after.receipts[id]));
    }
    assert_eq!(c.client.write(old_request, WAIT).await.unwrap(), old_reply);
    assert_eq!(
        std::fs::read_to_string(home.join("backend/jobs/exec-completed/work/result.txt")).unwrap(),
        "preserved\n"
    );
    worker.shutdown().await.unwrap();
    c.close().await;
}
