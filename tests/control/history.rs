use super::*;
use fleet::{
    control::{
        artifact::{self, CopyPhase, Kind},
        artifact_io,
    },
    history::Stream,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn diagnostic_archives_survive_source_loss_and_expire_without_erasing_job_history() {
    let mut c = Cluster::new(3).await;
    let (home, config) = prepare(&mut c, false).await;
    let mut worker = Worker::start(&home, config.clone(), hello()).await.unwrap();
    let (storage_home, storage_config, mut store) =
        storage::backup(&mut c, "history-store", 32 << 20).await;
    let policy = Mutation::StoragePolicy {
        expected_revision: 0,
        policy: artifact::Policy {
            history: BTreeSet::from(["history-store".into()]),
            ..Default::default()
        },
    };
    assert_eq!(
        c.client
            .write(
                Request {
                    origin: "cluster".into(),
                    id: "history-policy".into(),
                    mutation: policy
                },
                WAIT
            )
            .await
            .unwrap(),
        Reply::Recorded
    );
    let mut spec = job(
        "printf 'training log\\n'; printf 'diagnostic\\n' >&2; printf '{\"step\":7}\\n' > metrics.jsonl",
    );
    spec.probes = serde_json::from_value(
        serde_json::json!([{"jsonl_field":{"file":"metrics.jsonl","field":"step"}}]),
    )
    .unwrap();
    let mut request = super::super::submit("history-submit", "history");
    if let Mutation::Enqueue(s) = &mut request.mutation {
        s.workload = Workload::Job {
            spec: Box::new(spec),
            gpu: false,
            memory_mb: 128,
        };
    }
    c.client.write(request, WAIT).await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        worker.tick().await.unwrap();
        let state = c.client.read(WAIT).await.unwrap();
        if state.jobs["history"].phase.terminal()
            && state.history.attempts.get("history-a1").is_some_and(|s| {
                s.get(&Stream::Metrics)
                    .is_some_and(|p| p.closed && p.observed > 0)
            })
            && state
                .storage
                .artifacts
                .values()
                .filter(|a| matches!(a.manifest.kind, Kind::History { .. }))
                .count()
                == 3
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "capture timeout: {:?}",
            state.history
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let state = c.client.read(WAIT).await.unwrap();
    assert!(
        state
            .storage
            .artifacts
            .values()
            .filter(|a| matches!(a.manifest.kind, Kind::History { .. }))
            .all(|a| !state.durable_artifact(a)),
        "storage has not reconciled yet"
    );
    store.tick().await.unwrap();
    worker.tick().await.unwrap();
    let state = c.client.read(WAIT).await.unwrap();
    let chunks: Vec<_> = state
        .storage
        .artifacts
        .values()
        .filter(|a| matches!(a.manifest.kind, Kind::History { .. }))
        .cloned()
        .collect();
    assert_eq!(chunks.len(), 3);
    assert!(chunks.iter().all(|a| state.durable_artifact(a)));
    worker.shutdown().await.unwrap();
    std::fs::rename(home.join("backend"), home.join("unavailable-backend")).unwrap();
    store.shutdown().await.unwrap();
    let mut store = super::storage::start(&storage_home, storage_config)
        .await
        .unwrap();
    store.tick().await.unwrap();
    for chunk in &chunks {
        let destination = c
            .home
            .0
            .join(format!("history-export-{}", chunk.manifest.id));
        artifact_io::fetch_available(&c.client.endpoint, &state, chunk, &destination)
            .await
            .unwrap();
        let text = std::fs::read_to_string(destination.join("data")).unwrap();
        match chunk.manifest.kind {
            Kind::History {
                stream: Stream::Stdout,
                ..
            } => assert_eq!(text, "training log\n"),
            Kind::History {
                stream: Stream::Stderr,
                ..
            } => assert_eq!(text, "diagnostic\n"),
            Kind::History {
                stream: Stream::Metrics,
                ..
            } => assert!(text.contains("7.0")),
            _ => unreachable!(),
        }
    }
    // Exercise the public client path, not just the blob transfer helper, with its worker gone.
    let mut app = fleet::control::client::AppClient::connect(&c.home.0.join("app-client"))
        .await
        .unwrap();
    use fleet::control::client::ApiRequest;
    let page = app
        .execute(ApiRequest::Logs {
            id: "history".into(),
            attempt: Some(1),
            stderr: false,
            from: 0,
            max: 4,
        })
        .await
        .unwrap();
    assert_eq!(page["Logs"]["data"], "trai");
    assert_eq!(page["Logs"]["next"], 4);
    assert_eq!(page["Logs"]["archived"], true);
    let metric = app
        .execute(ApiRequest::Metrics {
            id: "history".into(),
            attempt: Some(1),
            from: 0,
            max: 65536,
        })
        .await
        .unwrap();
    assert!(metric["data"].as_str().unwrap().contains("7.0"));
    assert_eq!(metric["done"], true);
    let summary = app
        .execute(ApiRequest::History {
            id: "history".into(),
        })
        .await
        .unwrap();
    assert_eq!(summary["attempts"][0]["streams"][0]["backed_up_bytes"], 13);
    drop(app);
    let at = jobs::now();
    for (id, as_of) in [("finish", at), ("expire", at + 91 * 86400)] {
        assert_eq!(
            c.client
                .write(
                    Request {
                        origin: "cluster".into(),
                        id: id.into(),
                        mutation: Mutation::SweepHistory { as_of }
                    },
                    WAIT
                )
                .await
                .unwrap(),
            Reply::Recorded
        );
    }
    store.tick().await.unwrap();
    let expired = c.client.read(WAIT).await.unwrap();
    assert!(expired.jobs["history"].phase.terminal());
    assert_eq!(expired.jobs["history"].attempts.len(), 1);
    for chunk in &chunks {
        let a = &expired.storage.artifacts[&chunk.manifest.id];
        assert!(a.deleted);
        assert_eq!(a.copies["storage/history-store"].phase, CopyPhase::Deleted);
    }
    // Restore the worker only to acknowledge deletion; it must not republish expired history.
    std::fs::rename(home.join("unavailable-backend"), home.join("backend")).unwrap();
    let mut worker = Worker::start(&home, config, hello()).await.unwrap();
    worker.tick().await.unwrap();
    worker.tick().await.unwrap();
    assert!(
        c.client
            .read(WAIT)
            .await
            .unwrap()
            .storage
            .artifacts
            .values()
            .all(|a| !matches!(a.manifest.kind, Kind::History { .. })),
        "fully deleted chunk metadata compacted"
    );
    worker.shutdown().await.unwrap();
    store.shutdown().await.unwrap();
    c.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn vm_metric_files_capture_every_declared_record_and_large_logs_do_not_fail_the_job() {
    let mut c = Cluster::new(1).await;
    let (home, config) = prepare(&mut c, true).await;
    let mut w = Worker::start(&home, config, hello()).await.unwrap();
    let mut spec = job("unused");
    spec.vm = Some(vm_spec());
    spec.shutdown_grace_seconds = None;
    spec.metric_files = vec!["metrics.jsonl".into()];
    spec.argv=vec!["/usr/bin/python3".into(),"-c".into(),"import json,sys; f=open('metrics.jsonl','w'); [f.write(json.dumps({'step':i,'loss':1/(i+1)})+'\\n') for i in range(2000)]; f.close(); sys.stdout.write('x'*(9<<20))".into()];
    let mut request = super::super::submit("vm-history-submit", "vm-history");
    if let Mutation::Enqueue(s) = &mut request.mutation {
        s.workload = Workload::Job {
            spec: Box::new(spec),
            gpu: false,
            memory_mb: 4096,
        };
    }
    c.client.write(request, WAIT).await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        w.tick().await.unwrap();
        let state = c.client.read(WAIT).await.unwrap();
        if state.jobs["vm-history"].phase.terminal() {
            assert_eq!(
                state.jobs["vm-history"].phase,
                fleet::control::queue::JobPhase::Succeeded,
                "{:?}",
                state.assignments
            );
            let execution = &state.current_attempt("vm-history").unwrap().execution;
            let dir = home.join("backend/jobs").join(execution);
            let metrics = fleet::history::state(&dir, Stream::Metrics).unwrap();
            if metrics.closed {
                assert_eq!(
                    fleet::history::state(&dir, Stream::Stdout)
                        .unwrap()
                        .observed,
                    9 << 20
                );
                assert_eq!(metrics.dropped, 0);
                let data = fleet::history::read(&dir, Stream::Metrics, 0, 1 << 20)
                    .unwrap()
                    .data;
                let text = String::from_utf8(data).unwrap();
                assert_eq!(
                    text.lines()
                        .filter(
                            |l| serde_json::from_str::<serde_json::Value>(l).unwrap()["kind"]
                                == "jsonl"
                        )
                        .count(),
                    2000
                );
                break;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "VM history capture timed out"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    w.shutdown().await.unwrap();
    c.close().await;
}
