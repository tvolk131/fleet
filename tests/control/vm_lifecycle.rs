//! Regression coverage moved from the retired daemon onto committed queue intent
//! and the authenticated worker data interface used by the current clients.
use super::*;
use fleet::{events::EventKind, probes::Reading};
use std::path::Path;

async fn enqueue(c: &mut Cluster, id: &str, spec: jobs::JobSpec) {
    let reply = c
        .client
        .write(
            Request {
                origin: c.config.cluster.clone(),
                id: format!("enqueue-{id}"),
                mutation: Mutation::Enqueue(fleet::control::queue::Submission {
                    inputs: Default::default(),
                    id: id.into(),
                    worker: Some("worker-real".into()),
                    priority: 0,
                    requires: BTreeSet::new(),
                    after: BTreeSet::new(),
                    workload: Workload::Job {
                        spec: Box::new(spec),
                        gpu: false,
                        memory_mb: 4096,
                    },
                }),
            },
            WAIT,
        )
        .await
        .unwrap();
    assert!(matches!(reply, Reply::Accepted { .. }), "{reply:?}");
}

async fn until(c: &mut Cluster, w: &mut Worker, what: &str, mut ready: impl FnMut(&State) -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    loop {
        w.tick().await.unwrap();
        let state = c.client.read(WAIT).await.unwrap();
        if ready(&state) {
            return;
        }
        assert!(tokio::time::Instant::now() < deadline, "{what}: {state:?}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn data(c: &Cluster, config: &WorkerConfig, request: DataRequest) -> worker_data::Response {
    data_call(
        &c.client.endpoint,
        &c.config.cluster,
        &config.registration.peer,
        request,
    )
    .await
    .unwrap()
}

async fn progress(
    c: &Cluster,
    config: &WorkerConfig,
    execution: &str,
    since: u64,
    value: f64,
) -> u64 {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(45);
    loop {
        let worker_data::Response::Events { events, last, .. } = data(
            c,
            config,
            DataRequest::Events {
                since,
                wait_ms: 100,
            },
        )
        .await
        else {
            panic!("events response")
        };
        assert!(events.windows(2).all(|w| w[0].seq < w[1].seq));
        assert!(events.iter().all(|e| e.seq > since));
        if events.iter().any(|e| matches!(&e.kind, EventKind::Progress { job, probes }
            if job == execution && probes.iter().any(|p| matches!(p.reading, Reading::Number { value: n, .. } if n == value)))) {
            return last;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no progress {value}: {events:?}"
        );
    }
}

fn downloads(home: &Path) -> usize {
    std::fs::read_to_string(home.join("backend/instance-tart/history"))
        .unwrap()
        .lines()
        .filter(|l| l.contains("\"kind\":\"download\""))
        .count()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn vm_observations_replay_and_same_size_outputs_survive_restart_and_cancel() {
    let mut c = Cluster::new(1).await;
    let (home, config) = prepare(&mut c, true).await;
    let mut w = Worker::start(&home, config.clone(), hello()).await.unwrap();
    let mut spec = job("unused");
    spec.vm = Some(vm_spec());
    spec.shutdown_grace_seconds = None;
    spec.argv = vec![
        env!("CARGO_BIN_EXE_fleet-test-workload").into(),
        "observe".into(),
    ];
    spec.max_seconds = Some(240);
    spec.publish = vec!["out/*".into()];
    spec.probes = serde_json::from_value(serde_json::json!([
        {"jsonl_field":{"name":"step","file":"metrics.jsonl","field":"step","total":10}},
        {"jsonl_series":{"name":"loss","file":"metrics.jsonl","field":"loss","points":10}}
    ]))
    .unwrap();
    enqueue(&mut c, "live", spec).await;
    let a = wait(
        &mut c,
        &mut w,
        "live",
        WorkPhase::Job(AttemptPhase::Running),
    )
    .await;
    let dir = home.join("backend/jobs").join(&a.execution);
    until(&mut c, &mut w, "first captured output", |_| {
        std::fs::read(dir.join("work/out/result")).is_ok_and(|b| b == b"v1")
    })
    .await;
    let cursor = progress(&c, &config, &a.execution, 0, 2.0).await;
    let worker_data::Response::Outputs(first) = data(
        &c,
        &config,
        DataRequest::Outputs {
            execution: a.execution.clone(),
        },
    )
    .await
    else {
        panic!()
    };
    assert_eq!(first.len(), 1);
    let store = fleet::blobs::open(&c.home.0.join("downloads"))
        .await
        .unwrap();
    let capture = c.home.0.join("capture-before");
    fetch_outputs(
        &store,
        &c.client.endpoint,
        &c.config.cluster,
        &config.registration.peer,
        &a.execution,
        &capture,
    )
    .await
    .unwrap();
    assert_eq!(std::fs::read(capture.join("out/result")).unwrap(), b"v1");
    let before = downloads(&home);
    let generation = || {
        jobs::read_json::<serde_json::Value>(&dir.join("vm-observation.json"))
            .unwrap()
            .unwrap()["generation"]
            .as_u64()
            .unwrap()
    };
    let old_generation = generation();
    let started: jobs::Started = jobs::read_json(&dir.join("supervisor.json"))
        .unwrap()
        .unwrap();
    unsafe {
        libc::kill(started.supervisor_pid as i32, libc::SIGKILL);
    }
    w.shutdown().await.unwrap();
    w = Worker::start(&home, config.clone(), hello()).await.unwrap();
    until(&mut c, &mut w, "observation after worker restart", |_| {
        generation() > old_generation
    })
    .await;
    assert_eq!(
        downloads(&home),
        before,
        "unchanged outputs are not downloaded again"
    );
    // Reconnect from the old cursor through the current worker data protocol.
    progress(&c, &config, &a.execution, 0, 2.0).await;
    let instance = fleet::vm_job::instance_id(&a.execution);
    let guest = home
        .join("backend/instance-tart/vms")
        .join(format!("fleet-{instance}/guest-work"));
    std::fs::write(guest.join("advance"), "go").unwrap();
    until(&mut c, &mut w, "same-size output replacement", |_| {
        std::fs::read(dir.join("work/out/result")).is_ok_and(|b| b == b"v2")
    })
    .await;
    progress(&c, &config, &a.execution, cursor, 3.0).await;
    let worker_data::Response::Outputs(second) = data(
        &c,
        &config,
        DataRequest::Outputs {
            execution: a.execution.clone(),
        },
    )
    .await
    else {
        panic!()
    };
    assert_eq!(first[0].size, second[0].size);
    assert_ne!(first[0].hash, second[0].hash);
    assert_eq!(downloads(&home), before + 1);
    std::fs::write(dir.join("vm-capture.tmp"), "partial").unwrap();
    let revision = c.client.read(WAIT).await.unwrap().jobs["live"].revision;
    let reply = c
        .client
        .write(
            Request {
                origin: c.config.cluster.clone(),
                id: "cancel-live".into(),
                mutation: Mutation::CancelQueued {
                    id: "live".into(),
                    expected_revision: revision,
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
        "live",
        WorkPhase::Job(AttemptPhase::Cancelled),
    )
    .await;
    assert!(!dir.join("vm-capture.tmp").exists());
    let final_capture = c.home.0.join("capture-after");
    fetch_outputs(
        &store,
        &c.client.endpoint,
        &c.config.cluster,
        &config.registration.peer,
        &a.execution,
        &final_capture,
    )
    .await
    .unwrap();
    assert_eq!(
        std::fs::read(final_capture.join("out/result")).unwrap(),
        b"v2"
    );
    let worker_data::Response::Logs { data, .. } = data(
        &c,
        &config,
        DataRequest::Logs {
            execution: a.execution.clone(),
            stderr: false,
            from: 0,
            max: 10000,
        },
    )
    .await
    else {
        panic!()
    };
    assert_eq!(data, "started-once\nadvanced-once\n");
    assert_eq!(
        instances::info(&home.join("backend"), &instance)
            .unwrap()
            .observed
            .state,
        instances::State::Deleted
    );
    assert!(
        instances::guest::list(&home.join("backend"), &instance)
            .unwrap()
            .len()
            < 10
    );
    store.shutdown().await.unwrap();
    w.shutdown().await.unwrap();
    c.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn vm_failures_release_capacity_only_after_cleanup() {
    let mut c = Cluster::new(1).await;
    let (home, config) = prepare(&mut c, true).await;
    let mut w = Worker::start(&home, config.clone(), hello()).await.unwrap();
    for (id, script, limit, backend_state) in [
        ("timeout", "echo ready; sleep 30", 1, jobs::State::TimedOut),
        ("exit-124", "exit 124", 30, jobs::State::Failed),
        (
            "unsafe-output",
            "mkdir -p out; ln -s /etc/passwd out/leak",
            30,
            jobs::State::Failed,
        ),
    ] {
        let mut spec = job(script);
        spec.vm = Some(vm_spec());
        spec.shutdown_grace_seconds = None;
        spec.publish = vec!["out/*".into()];
        spec.max_seconds = Some(limit);
        enqueue(&mut c, id, spec).await;
        let a = wait(&mut c, &mut w, id, WorkPhase::Job(AttemptPhase::Failed)).await;
        assert_eq!(
            jobs::info(&home.join("backend/jobs").join(&a.execution))
                .unwrap()
                .state,
            backend_state
        );
        assert_eq!(
            instances::info(
                &home.join("backend"),
                &fleet::vm_job::instance_id(&a.execution)
            )
            .unwrap()
            .observed
            .state,
            instances::State::Deleted
        );
        if id == "unsafe-output" {
            let worker_data::Response::Outputs(files) = data(
                &c,
                &config,
                DataRequest::Outputs {
                    execution: a.execution.clone(),
                },
            )
            .await
            else {
                panic!()
            };
            assert!(files.is_empty());
            let worker_data::Response::Logs { data, .. } = data(
                &c,
                &config,
                DataRequest::Logs {
                    execution: a.execution.clone(),
                    stderr: true,
                    from: 0,
                    max: 10000,
                },
            )
            .await
            else {
                panic!()
            };
            let diagnostic = fleet::history::state(
                &home.join("backend/jobs").join(&a.execution),
                fleet::history::Stream::Stderr,
            )
            .unwrap();
            assert!(
                diagnostic
                    .error
                    .as_deref()
                    .unwrap_or("")
                    .contains("regular file"),
                "{diagnostic:?}; captured stderr: {data}"
            );
        }
    }
    w.shutdown().await.unwrap();
    c.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancellation_during_vm_clone_never_executes_the_guest() {
    let mut c = Cluster::new(1).await;
    let (home, config) = prepare(&mut c, true).await;
    let root = home.join("backend/instance-tart");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("slow-clone"), "").unwrap();
    let mut w = Worker::start(&home, config, hello()).await.unwrap();
    let mut spec = job("echo must-not-run");
    spec.vm = Some(vm_spec());
    spec.shutdown_grace_seconds = None;
    enqueue(&mut c, "cloning", spec).await;
    until(&mut c, &mut w, "clone to begin", |_| {
        std::fs::read_to_string(root.join("history"))
            .unwrap_or_default()
            .lines()
            .any(|l| l.starts_with("clone "))
    })
    .await;
    let revision = c.client.read(WAIT).await.unwrap().jobs["cloning"].revision;
    let start = tokio::time::Instant::now();
    let reply = c
        .client
        .write(
            Request {
                origin: c.config.cluster.clone(),
                id: "cancel-clone".into(),
                mutation: Mutation::CancelQueued {
                    id: "cloning".into(),
                    expected_revision: revision,
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
        "cloning",
        WorkPhase::Job(AttemptPhase::Cancelled),
    )
    .await;
    assert!(start.elapsed() < Duration::from_secs(15));
    assert!(
        !std::fs::read_to_string(root.join("history"))
            .unwrap()
            .lines()
            .any(|l| l.starts_with("run "))
    );
    w.shutdown().await.unwrap();
    c.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uncertain_guest_execution_is_not_replayed_and_releases_its_vm() {
    let mut c = Cluster::new(1).await;
    let (home, config) = prepare(&mut c, true).await;
    let mut w = Worker::start(&home, config.clone(), hello()).await.unwrap();
    let mut spec = job("echo started-once; sleep 90");
    spec.vm = Some(vm_spec());
    spec.shutdown_grace_seconds = None;
    spec.max_seconds = Some(120);
    enqueue(&mut c, "uncertain-guest", spec).await;
    let a = wait(
        &mut c,
        &mut w,
        "uncertain-guest",
        WorkPhase::Job(AttemptPhase::Running),
    )
    .await;
    let backend = home.join("backend");
    let instance = fleet::vm_job::instance_id(&a.execution);
    let operation = instances::guest::operation_dir(&backend, &instance, "execute").unwrap();
    until(&mut c, &mut w, "guest execution started", |_| {
        operation.join("attempted").exists()
    })
    .await;
    let listing = std::process::Command::new("/bin/ps")
        .args(["-axo", "pid=,command="])
        .output()
        .unwrap();
    assert!(listing.status.success());
    let needle = format!(
        "instance-operation {} {} execute",
        backend.display(),
        instance
    );
    let listing = String::from_utf8(listing.stdout).unwrap();
    let pids: Vec<i32> = listing
        .lines()
        .filter(|l| l.contains(&needle))
        .map(|l| l.split_whitespace().next().unwrap().parse().unwrap())
        .collect();
    assert_eq!(
        pids.len(),
        1,
        "one supervisor for the exact fixture operation"
    );
    unsafe {
        libc::kill(pids[0], libc::SIGKILL);
    }
    wait(
        &mut c,
        &mut w,
        "uncertain-guest",
        WorkPhase::Job(AttemptPhase::Failed),
    )
    .await;
    let status: fleet::vm_job::Status =
        jobs::read_json(&backend.join("jobs").join(&a.execution).join("vm-job.json"))
            .unwrap()
            .unwrap();
    assert!(status.error.unwrap().contains("no replay"));
    assert_eq!(
        instances::info(&backend, &instance).unwrap().observed.state,
        instances::State::Deleted
    );
    assert_eq!(
        c.client.read(WAIT).await.unwrap().jobs["uncertain-guest"]
            .attempts
            .len(),
        1
    );
    w.shutdown().await.unwrap();
    c.close().await;
}
