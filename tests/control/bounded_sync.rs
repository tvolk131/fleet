use super::*;
use fleet::control::worker_sync::{Receiver, Reply as SyncReply, Request as SyncRequest};

async fn poll(w: &mut Worker, request: SyncRequest) -> SyncReply {
    let answer = w
        .client
        .call(
            Message::WorkerSync {
                worker: w.config.registration.id.clone(),
                incarnation: w.config.registration.incarnation.clone(),
                request,
            },
            WAIT * 3,
        )
        .await
        .unwrap();
    let Answer::WorkerSync(Ok(reply)) = answer else {
        panic!("sync refused: {answer:?}")
    };
    assert!(serde_json::to_vec(&reply).unwrap().len() < 1 << 20);
    reply
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn oversized_history_cancellation_and_replica_restart_converge_without_execution() {
    let mut c = Cluster::new(1).await;
    let (home, config) = prepare(&mut c, false).await;
    let mut w = Worker::start(&home, config.clone(), hello()).await.unwrap();
    for id in ["large-a", "large-b"] {
        let mut work = workload("echo MUST-NOT-EXECUTE > result.txt");
        let Workload::Job { spec, .. } = &mut work else {
            unreachable!()
        };
        spec.name = "x".repeat(4_300_000);
        assert!(matches!(
            c.client.write(assign(id, work), WAIT * 3).await.unwrap(),
            Reply::Accepted { .. }
        ));
    }
    // Start receiving the >8 MiB old Running image, then cancel while it is in flight.
    let mut receiver = Receiver::default();
    let first = poll(&mut w, receiver.request()).await;
    assert!(matches!(first, SyncReply::Page { total, .. } if total > 8 << 20));
    assert!(receiver.receive(first).unwrap().is_none());
    for id in ["large-a", "large-b"] {
        desire(&mut c, id, 1, Desired::Job(JobDesired::Cancelled)).await;
    }
    // A replica change loses the disposable transfer cache. The worker safely bootstraps again.
    // Find the leader without an unbounded administrative Read of these deliberately huge rows.
    let leader = c
        .replicas
        .values()
        .find_map(|r| {
            let m = r.raft.metrics().borrow().clone();
            (m.current_leader == Some(m.id)).then_some(m.id)
        })
        .unwrap();
    c.replicas
        .remove(&leader)
        .unwrap()
        .shutdown()
        .await
        .unwrap();
    c.start(leader).await;
    let reply = poll(&mut w, receiver.request()).await;
    assert!(matches!(reply, SyncReply::Reset));
    receiver.receive(reply).unwrap();
    let mut saved = None;
    for _ in 0..400 {
        let reply = poll(&mut w, receiver.request()).await;
        if let Some(view) = receiver.receive(reply).unwrap() {
            saved = Some(view);
            break;
        }
    }
    let saved = saved.expect("bounded synchronization converged");
    assert_eq!(saved.image.view().unwrap().assignments.len(), 2);
    assert!(
        saved
            .image
            .view()
            .unwrap()
            .assignments
            .iter()
            .all(|a| a.desired == Desired::Job(JobDesired::Cancelled))
    );
    w.shutdown().await.unwrap();
    // Exercise the actual worker's durable acceptance/execution path with the same large history.
    let mut w = Worker::start(&home, config.clone(), hello()).await.unwrap();
    let end = tokio::time::Instant::now() + WAIT * 12;
    loop {
        w.tick().await.unwrap();
        let local = c
            .replicas
            .values()
            .next()
            .unwrap()
            .store
            .view()
            .await
            .unwrap();
        if local.assignments.values().all(|a| a.terminal()) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < end,
            "worker cancellation did not converge"
        );
    }
    for id in ["large-a", "large-b"] {
        assert!(
            !home
                .join(format!("backend/jobs/exec-{id}/work/result.txt"))
                .exists()
        );
    }
    w.shutdown().await.unwrap();
    let mut w = Worker::start(&home, config, hello()).await.unwrap();
    w.tick().await.unwrap();
    for id in ["large-a", "large-b"] {
        assert!(
            !home
                .join(format!("backend/jobs/exec-{id}/work/result.txt"))
                .exists()
        );
    }
    w.shutdown().await.unwrap();
    c.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn worker_repairs_legacy_poisoned_outbox_without_skipping_lifecycle_observations() {
    use redb::{Database, ReadableTable, TableDefinition};
    let mut c = Cluster::new(3).await;
    let (home, config) = prepare(&mut c, false).await;
    assert!(matches!(
        c.client
            .write(
                assign("poison", workload("echo MUST-NOT-EXECUTE > result.txt")),
                WAIT
            )
            .await
            .unwrap(),
        Reply::Accepted { .. }
    ));
    desire(&mut c, "poison", 1, Desired::Job(JobDesired::Cancelled)).await;
    let assignment = c.client.read(WAIT).await.unwrap().assignments["poison"].clone();
    let report = Report {
        worker: config.registration.id.clone(),
        incarnation: config.registration.incarnation.clone(),
        sequence: 1,
        assignment: "poison".into(),
        revision: 1,
        observation: Observation {
            phase: WorkPhase::Job(AttemptPhase::Running),
            detail: Some("界".repeat(2000)),
        },
    };
    // Reproduce an already persisted, pre-fix envelope. Normal observe() now prevents this.
    let db = Database::open(home.join("worker.redb")).unwrap();
    let tx = db.begin_write().unwrap();
    {
        let mut table = tx
            .open_table(TableDefinition::<&str, &[u8]>::new("worker_journal"))
            .unwrap();
        let mut state: serde_json::Value =
            serde_json::from_slice(table.get("state").unwrap().unwrap().value()).unwrap();
        state["assignments"]["poison"] = serde_json::to_value(assignment).unwrap();
        state["sequence"] = serde_json::json!(1);
        state["outbox"]["1"] = serde_json::to_value(&report).unwrap();
        table
            .insert("state", serde_json::to_vec(&state).unwrap().as_slice())
            .unwrap();
    }
    tx.commit().unwrap();
    drop(db);
    let mut w = Worker::start(&home, config.clone(), hello()).await.unwrap();
    w.tick().await.unwrap();
    let state = c.client.read(WAIT).await.unwrap();
    assert!(state.assignments["poison"].terminal());
    assert_eq!(state.cursors[&config.registration.cursor()], 2);
    let receipt = &state.receipts[&key(
        &config.registration.peer.identity.to_string(),
        &report.request_id(),
    )];
    assert_eq!(
        receipt.response,
        Some(Reply::Rejected {
            reason: DETAIL_TOO_LARGE.into()
        })
    );
    assert!(
        !home
            .join("backend/jobs/exec-poison/work/result.txt")
            .exists()
    );
    w.shutdown().await.unwrap();
    let mut w = Worker::start(&home, config, hello()).await.unwrap();
    w.tick().await.unwrap();
    assert_eq!(
        c.client
            .read(WAIT)
            .await
            .unwrap()
            .cursors
            .values()
            .copied()
            .collect::<Vec<_>>(),
        vec![2]
    );
    w.shutdown().await.unwrap();
    c.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_during_sync_cannot_launch_old_intent_while_controllers_are_offline() {
    use redb::{Database, ReadableTable, TableDefinition};
    let mut c = Cluster::new(1).await;
    let (home, config) = prepare(&mut c, false).await;
    assert!(matches!(
        c.client
            .write(
                assign("fenced", workload("echo MUST-NOT-EXECUTE > result.txt")),
                WAIT
            )
            .await
            .unwrap(),
        Reply::Accepted { .. }
    ));
    let mut w = Worker::start(&home, config.clone(), hello()).await.unwrap();
    let mut receiver = Receiver::default();
    let saved = loop {
        let reply = poll(&mut w, receiver.request()).await;
        if let Some(saved) = receiver.receive(reply).unwrap() {
            break saved;
        }
    };
    let view = saved.image.view().unwrap();
    w.shutdown().await.unwrap();
    // Model a crash after accepting Running, then beginning a replacement transfer, but before
    // any execution. The original intent alone must not authorize a launch after this restart.
    let db = Database::open(home.join("worker.redb")).unwrap();
    let tx = db.begin_write().unwrap();
    {
        let mut table = tx
            .open_table(TableDefinition::<&str, &[u8]>::new("worker_journal"))
            .unwrap();
        let mut state: serde_json::Value =
            serde_json::from_slice(table.get("state").unwrap().unwrap().value()).unwrap();
        state["assignments"]["fenced"] = serde_json::to_value(&view.assignments[0]).unwrap();
        state["generation"] = serde_json::json!(view.generation);
        table
            .insert("state", serde_json::to_vec(&state).unwrap().as_slice())
            .unwrap();
        table
            .insert("sync", serde_json::to_vec(&saved).unwrap().as_slice())
            .unwrap();
        table.insert("sync_pending", b"true".as_slice()).unwrap();
    }
    tx.commit().unwrap();
    drop(db);
    c.replicas.remove(&1).unwrap().shutdown().await.unwrap();
    let mut w = Worker::start(&home, config, hello()).await.unwrap();
    w.tick().await.unwrap();
    assert!(!home.join("backend/jobs/exec-fenced/job.json").exists());
    c.start(1).await;
    desire(&mut c, "fenced", 1, Desired::Job(JobDesired::Cancelled)).await;
    w.tick().await.unwrap();
    assert!(c.client.read(WAIT).await.unwrap().assignments["fenced"].terminal());
    assert!(
        !home
            .join("backend/jobs/exec-fenced/work/result.txt")
            .exists()
    );
    w.shutdown().await.unwrap();
    c.close().await;
}
