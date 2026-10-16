use super::*;
use fleet::{
    control::{
        runtime::{DataRequest, Worker, WorkerConfig, data_call, fetch_outputs, push_inputs},
        workload::*,
    },
    instances, jobs, proto,
};
use std::os::unix::fs::PermissionsExt;

fn hello() -> proto::Hello {
    proto::Hello {
        agent_version: "test".into(),
        protocol: proto::PROTOCOL,
        hostname: "test".into(),
        os: "macOS".into(),
        cpu: "test".into(),
        cores: 8,
        memory_bytes: 16 << 30,
        gpu: Some("Metal".into()),
        tags: vec![],
        sleep_minutes: None,
        cpu_slots: 8,
    }
}
fn stdout(home: &std::path::Path, execution: &str) -> String {
    String::from_utf8(
        fleet::history::read(
            &home.join("backend/jobs").join(execution),
            fleet::history::Stream::Stdout,
            0,
            1 << 20,
        )
        .unwrap()
        .data,
    )
    .unwrap()
}
fn job(script: &str) -> jobs::JobSpec {
    serde_json::from_value(serde_json::json!({"name":"control canary", "argv":["/bin/sh", "-c", script], "threads":1, "keep_awake":false, "max_seconds":30, "shutdown_grace_seconds":0, "settle_seconds":0, "publish":["result.txt"]})).unwrap()
}
pub(super) fn workload(script: &str) -> Workload {
    Workload::Job {
        spec: Box::new(job(script)),
        gpu: false,
        memory_mb: 128,
    }
}
fn vm_spec() -> instances::Spec {
    instances::Spec {
        runtime: Default::default(),
        image: "macos".into(),
        cpus: 2,
        memory_mb: 4096,
        disk_gb: 50,
        network: instances::Network::Offline,
        gpu: instances::Gpu::Shared,
    }
}
async fn prepare(c: &mut Cluster, tart: bool) -> (PathBuf, WorkerConfig) {
    prepare_with_resources(c, tart, 4, 8192).await
}
async fn prepare_with_resources(
    c: &mut Cluster,
    tart: bool,
    cpu_slots: u32,
    memory_mb: u32,
) -> (PathBuf, WorkerConfig) {
    prepare_named(c, tart, cpu_slots, memory_mb, "worker-real", false).await
}
async fn prepare_named(
    c: &mut Cluster,
    tart: bool,
    cpu_slots: u32,
    memory_mb: u32,
    id: &str,
    portable: bool,
) -> (PathBuf, WorkerConfig) {
    let home = c
        .home
        .0
        .join(if id == "worker-real" { "worker" } else { id });
    let key = fleet::keys::load_or_create(&home.join("node.key")).unwrap();
    let socket = reserve_socket();
    let config = WorkerConfig {
        cluster: c.config.cluster.clone(),
        registration: Registration {
            runtimes: if portable {
                BTreeMap::from([("macos".into(), portable::runtime())])
            } else {
                BTreeMap::new()
            },
            tags: BTreeSet::from(["os:macos".into(), "gpu:metal".into()]),
            id: id.into(),
            peer: Peer {
                identity: key.public(),
                address: socket.local_addr().unwrap(),
            },
            incarnation: "journal-1".into(),
            budget: Budget {
                cpu_slots,
                memory_mb,
                disk_gb: 200,
                vm_slots: if tart { 2 } else { 0 },
            },
            images: if tart {
                BTreeSet::from(["macos".into()])
            } else {
                BTreeSet::new()
            },
        },
        controllers: c.config.peers.clone(),
        administrators: c.config.administrators.clone(),
        supervisor: env!("CARGO_BIN_EXE_fleet-control-lab").into(),
    };
    drop(socket);
    if tart {
        let backend = home.join("backend");
        std::fs::create_dir_all(&backend).unwrap();
        let fake = backend.join("tart");
        let softnet = backend.join("softnet");
        std::fs::write(&fake, include_str!("../fixtures/fake_tart.py")).unwrap();
        std::fs::write(&softnet, "#!/bin/sh\necho 'softnet 0.24.0-test'\n").unwrap();
        for p in [&fake, &softnet] {
            std::fs::set_permissions(p, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        instances::write(
            &backend.join("instances.json"),
            &instances::Config {
                platforms: if portable {
                    BTreeMap::from([("macos".into(), portable::runtime().platform)])
                } else {
                    BTreeMap::new()
                },
                tart: fake,
                softnet,
                images: BTreeMap::from([(
                    "macos".into(),
                    format!("registry.test/macos@sha256:{}", "a".repeat(64)),
                )]),
                memory_budget_mb: 8192,
                disk_budget_gb: 200,
                min_free_disk_gb: 5,
                block_cidrs: vec![],
            },
        )
        .unwrap();
    }
    Worker::initialize(&home, &config).unwrap();
    assert_eq!(
        c.client
            .write(
                Request {
                    origin: "cluster".into(),
                    id: format!("enroll-{id}"),
                    mutation: Mutation::Enroll {
                        worker: config.registration.clone()
                    }
                },
                WAIT
            )
            .await
            .unwrap(),
        Reply::Recorded
    );
    (home, config)
}
fn assign(id: &str, workload: Workload) -> Request {
    Request {
        origin: "cluster".into(),
        id: format!("submit-{id}"),
        mutation: Mutation::Assign {
            id: id.into(),
            execution: format!("exec-{id}"),
            worker: "worker-real".into(),
            workload,
        },
    }
}
async fn wait(c: &mut Cluster, w: &mut Worker, id: &str, wanted: WorkPhase) -> Assignment {
    let end = tokio::time::Instant::now() + Duration::from_secs(90);
    loop {
        w.tick().await.unwrap();
        let state = c.client.read(WAIT).await.unwrap();
        let Some(a) = state
            .current_attempt(id)
            .or_else(|| state.assignments.get(id))
            .cloned()
        else {
            assert!(
                tokio::time::Instant::now() < end,
                "waiting for placement: {state:?}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        };
        if a.observed
            .as_ref()
            .is_some_and(|(r, o)| *r == a.revision && o.phase == wanted)
        {
            return a;
        }
        assert!(
            tokio::time::Instant::now() < end,
            "waiting for {wanted:?}: {a:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
async fn desire(c: &mut Cluster, id: &str, revision: u64, desired: Desired) {
    assert!(matches!(
        c.client
            .write(
                Request {
                    origin: "cluster".into(),
                    id: format!("desire-{id}-{revision}"),
                    mutation: Mutation::Desire {
                        id: id.into(),
                        expected_revision: revision,
                        desired
                    }
                },
                WAIT
            )
            .await
            .unwrap(),
        Reply::Accepted { .. }
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_worker_survives_lost_reply_restart_and_replica_replacement() {
    let mut c = Cluster::new(3).await;
    let (home, config) = prepare(&mut c, false).await;
    let mut w = Worker::start(&home, config.clone(), hello()).await.unwrap();
    let local_store = fleet::blobs::open(&c.home.0.join("client-blobs"))
        .await
        .unwrap();
    let input = c.home.0.join("input.txt");
    std::fs::write(&input, "verified result\n").unwrap();
    let hash = local_store.blobs().add_path(&input).await.unwrap().hash;
    let mut spec = job("echo started; cat input.txt > result.txt; sleep 25");
    spec.inputs.insert(
        "input.txt".into(),
        jobs::Input {
            hash,
            executable: false,
        },
    );
    push_inputs(
        &local_store,
        &c.client.endpoint,
        &c.config.cluster,
        &config.registration.peer,
        &spec,
    )
    .await
    .unwrap();
    let request = assign(
        "native",
        Workload::Job {
            spec: Box::new(spec),
            gpu: false,
            memory_mb: 128,
        },
    );
    let leader = c.leader().await;
    c.replicas[&leader]
        .drop_write_reply
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let reply = c.client.write(request.clone(), WAIT).await.unwrap();
    assert_eq!(c.client.write(request, WAIT).await.unwrap(), reply);
    wait(
        &mut c,
        &mut w,
        "native",
        WorkPhase::Job(AttemptPhase::Running),
    )
    .await;
    let started: jobs::Started =
        jobs::read_json(&home.join("backend/jobs/exec-native/supervisor.json"))
            .unwrap()
            .unwrap();
    let logs = data_call(
        &c.client.endpoint,
        &c.config.cluster,
        &config.registration.peer,
        DataRequest::Logs {
            execution: "exec-native".into(),
            stderr: false,
            from: 0,
            max: 1000,
        },
    )
    .await
    .unwrap();
    assert!(matches!(logs, proto::Response::Logs { data, .. } if data == "started\n"));
    let files = fetch_outputs(
        &local_store,
        &c.client.endpoint,
        &c.config.cluster,
        &config.registration.peer,
        "exec-native",
        &c.home.0.join("captured"),
    )
    .await
    .unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].hash, hash);
    w.shutdown().await.unwrap();
    w = Worker::start(&home, config.clone(), hello()).await.unwrap();
    c.prepare(4, true).await;
    let voters: Vec<_> = (1..=4).filter(|id| *id != leader).collect();
    let request = c.membership_request("replace", 1, &voters);
    c.changed(request).await;
    c.replicas
        .remove(&leader)
        .unwrap()
        .shutdown()
        .await
        .unwrap();
    wait(
        &mut c,
        &mut w,
        "native",
        WorkPhase::Job(AttemptPhase::Running),
    )
    .await;
    let after: jobs::Started =
        jobs::read_json(&home.join("backend/jobs/exec-native/supervisor.json"))
            .unwrap()
            .unwrap();
    assert_eq!(started, after);
    assert!(!w.client.peers.contains_key(&leader));
    let request = c.membership_request("shrink", 2, &[4]);
    c.changed(request).await;
    desire(&mut c, "native", 1, Desired::Job(JobDesired::Cancelled)).await;
    wait(
        &mut c,
        &mut w,
        "native",
        WorkPhase::Job(AttemptPhase::Cancelled),
    )
    .await;
    assert_eq!(stdout(&home, "exec-native"), "started\n");
    w.shutdown().await.unwrap();
    c.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn worker_outage_keeps_work_and_journal_then_recovers_without_rerun() {
    let mut c = Cluster::new(1).await;
    let (home, config) = prepare(&mut c, false).await;
    let mut w = Worker::start(&home, config.clone(), hello()).await.unwrap();
    c.client
        .write(
            assign(
                "outage",
                workload("echo once; sleep 2; echo finished > result.txt"),
            ),
            WAIT,
        )
        .await
        .unwrap();
    wait(
        &mut c,
        &mut w,
        "outage",
        WorkPhase::Job(AttemptPhase::Running),
    )
    .await;
    c.replicas.remove(&1).unwrap().shutdown().await.unwrap();
    w.tick().await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    w.tick().await.unwrap();
    assert_eq!(
        jobs::info(&home.join("backend/jobs/exec-outage"))
            .unwrap()
            .state,
        jobs::State::Succeeded
    );
    w.shutdown().await.unwrap();
    c.start(1).await;
    let mut w = Worker::start(&home, config, hello()).await.unwrap();
    wait(
        &mut c,
        &mut w,
        "outage",
        WorkPhase::Job(AttemptPhase::Succeeded),
    )
    .await;
    assert_eq!(stdout(&home, "exec-outage"), "once\n");
    w.shutdown().await.unwrap();
    c.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn worker_cancellation_before_inputs_and_uncertain_launch_hold_reservations() {
    let mut c = Cluster::new(1).await;
    let (home, config) = prepare(&mut c, false).await;
    let mut w = Worker::start(&home, config.clone(), hello()).await.unwrap();
    let mut spec = job("echo must-not-run");
    spec.inputs.insert(
        "missing".into(),
        jobs::Input {
            hash: iroh_blobs::Hash::from_bytes([7; 32]),
            executable: false,
        },
    );
    c.client
        .write(
            assign(
                "pending",
                Workload::Job {
                    spec: Box::new(spec),
                    gpu: false,
                    memory_mb: 128,
                },
            ),
            WAIT,
        )
        .await
        .unwrap();
    w.tick().await.unwrap();
    desire(&mut c, "pending", 1, Desired::Job(JobDesired::Cancelled)).await;
    wait(
        &mut c,
        &mut w,
        "pending",
        WorkPhase::Job(AttemptPhase::Cancelled),
    )
    .await;
    assert!(!home.join("backend/jobs/exec-pending/job.json").exists());
    let mut spec = job("echo must-not-run > result.txt");
    spec.threads = 4;
    c.client
        .write(
            assign(
                "uncertain",
                Workload::Job {
                    spec: Box::new(spec.clone()),
                    gpu: false,
                    memory_mb: 8192,
                },
            ),
            WAIT,
        )
        .await
        .unwrap();
    // Crash boundary: launch intention persisted, but no reliable exit or supervisor remains.
    let dir = home.join("backend/jobs/exec-uncertain");
    std::fs::create_dir_all(dir.join("work")).unwrap();
    instances::write(
        &dir.join("job.json"),
        &jobs::Job {
            id: "exec-uncertain".into(),
            spec,
            submitter: config.registration.peer.identity.to_string(),
            submitted_at: jobs::now(),
        },
    )
    .unwrap();
    instances::write(&dir.join("gpu-allocation.json"), &false).unwrap();
    instances::write(&dir.join("launch-attempt.json"), &jobs::now()).unwrap();
    wait(
        &mut c,
        &mut w,
        "uncertain",
        WorkPhase::Job(AttemptPhase::Uncertain),
    )
    .await;
    assert!(matches!(
        c.client
            .write(assign("blocked", workload("true")), WAIT)
            .await
            .unwrap(),
        Reply::Rejected { .. }
    ));
    w.shutdown().await.unwrap();
    let mut w = Worker::start(&home, config, hello()).await.unwrap();
    wait(
        &mut c,
        &mut w,
        "uncertain",
        WorkPhase::Job(AttemptPhase::Uncertain),
    )
    .await;
    assert!(!dir.join("work/result.txt").exists());
    w.shutdown().await.unwrap();
    c.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn enrolled_worker_cannot_submit_jobs_or_impersonate_another_journal() {
    let mut c = Cluster::new(1).await;
    let (home, config) = prepare(&mut c, false).await;
    let mut w = Worker::start(&home, config.clone(), hello()).await.unwrap();
    assert!(
        w.client
            .write(assign("rogue", workload("true")), WAIT)
            .await
            .is_err()
    );
    assert!(w.client.read(WAIT).await.is_err());
    let response = w
        .client
        .call(
            Message::WorkerRead {
                worker: config.registration.id.clone(),
                incarnation: "wrong".into(),
            },
            WAIT,
        )
        .await;
    assert!(response.is_err());
    let mut impostor = Client::new(
        SecretKey::generate(),
        c.config.cluster.clone(),
        c.config.peers.clone(),
    )
    .await
    .unwrap();
    assert!(
        impostor
            .call(
                Message::WorkerRead {
                    worker: config.registration.id.clone(),
                    incarnation: config.registration.incarnation.clone()
                },
                Duration::from_millis(300)
            )
            .await
            .is_err()
    );
    impostor.endpoint.close().await;
    w.shutdown().await.unwrap();
    c.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tart_job_and_persistent_instances_use_replicated_intent_and_shared_budgets() {
    let mut c = Cluster::new(3).await;
    let (home, config) = prepare(&mut c, true).await;
    let mut w = Worker::start(&home, config.clone(), hello()).await.unwrap();
    // Two shared VMs fit; a native job cannot consume their reserved CPU/memory.
    for id in ["one", "two"] {
        assert!(matches!(
            c.client
                .write(assign(id, Workload::Instance { spec: vm_spec() }), WAIT)
                .await
                .unwrap(),
            Reply::Accepted { .. }
        ));
    }
    assert!(matches!(
        c.client
            .write(assign("too-many", workload("true")), WAIT)
            .await
            .unwrap(),
        Reply::Rejected { .. }
    ));
    for id in ["one", "two"] {
        wait(
            &mut c,
            &mut w,
            id,
            WorkPhase::Instance(InstancePhase::Running),
        )
        .await;
    }
    w.shutdown().await.unwrap();
    let state = c.client.read(WAIT).await.unwrap();
    for id in ["one", "two"] {
        assert_eq!(
            state.assignments[id].observed.as_ref().unwrap().1.phase,
            WorkPhase::Instance(InstancePhase::Running)
        );
    }
    let mut w = Worker::start(&home, config.clone(), hello()).await.unwrap();
    let proto::Response::Instances(inventory) = data_call(
        &c.client.endpoint,
        &c.config.cluster,
        &config.registration.peer,
        DataRequest::Instances,
    )
    .await
    .unwrap() else {
        panic!("instance inventory")
    };
    assert_eq!(inventory.len(), 2);
    assert!(
        inventory
            .iter()
            .all(|i| i.observed.state == instances::State::Running)
    );
    assert_eq!(
        std::fs::read_to_string(home.join("backend/instance-tart/history"))
            .unwrap()
            .lines()
            .filter(|l| l.starts_with("run "))
            .count(),
        2,
        "worker restart must reuse both existing VMs"
    );
    for id in ["one", "two"] {
        desire(&mut c, id, 1, Desired::Instance(InstanceDesired::Deleted)).await;
        wait(
            &mut c,
            &mut w,
            id,
            WorkPhase::Instance(InstancePhase::Deleted),
        )
        .await;
    }
    let mut spec = job("echo vm-output > result.txt; echo guest-log");
    spec.vm = Some(vm_spec());
    spec.shutdown_grace_seconds = None;
    c.client
        .write(
            assign(
                "vm-job",
                Workload::Job {
                    spec: Box::new(spec),
                    gpu: true,
                    memory_mb: 0,
                },
            ),
            WAIT,
        )
        .await
        .unwrap();
    wait(
        &mut c,
        &mut w,
        "vm-job",
        WorkPhase::Job(AttemptPhase::Succeeded),
    )
    .await;
    let store = fleet::blobs::open(&c.home.0.join("capture-store"))
        .await
        .unwrap();
    let files = fetch_outputs(
        &store,
        &c.client.endpoint,
        &c.config.cluster,
        &config.registration.peer,
        "exec-vm-job",
        &c.home.0.join("vm-capture"),
    )
    .await
    .unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(
        std::fs::read_to_string(c.home.0.join("vm-capture/result.txt")).unwrap(),
        "vm-output\n"
    );
    assert!(
        instances::list(&home.join("backend"))
            .unwrap()
            .iter()
            .all(|i| i.observed.state == instances::State::Deleted)
    );
    w.shutdown().await.unwrap();
    c.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn worker_process_sigkill_preserves_supervisor_and_durable_cancellation() {
    struct Child(std::process::Child);
    impl Drop for Child {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let mut c = Cluster::new(1).await;
    // This child probes real host capacity instead of the synthetic eight-core
    // Hello used by in-process tests. One tiny job must fit small CI runners too.
    let (home, config) = prepare_with_resources(&mut c, false, 1, 256).await;
    instances::write(&home.join("worker.json"), &config).unwrap();
    let start = || {
        Child(
            std::process::Command::new(env!("CARGO_BIN_EXE_fleet-control-lab"))
                .arg("worker")
                .arg("--home")
                .arg(&home)
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::inherit())
                .spawn()
                .unwrap(),
        )
    };
    let mut child = start();
    c.client
        .write(assign("process", workload("echo once; sleep 25")), WAIT)
        .await
        .unwrap();
    let path = home.join("backend/jobs/exec-process/supervisor.json");
    let end = tokio::time::Instant::now() + WAIT;
    while !path.exists() {
        assert!(
            child.0.try_wait().unwrap().is_none(),
            "worker process exited before launching the canary"
        );
        assert!(tokio::time::Instant::now() < end);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let before: jobs::Started = jobs::read_json(&path).unwrap().unwrap();
    child.0.kill().unwrap();
    child.0.wait().unwrap();
    desire(&mut c, "process", 1, Desired::Job(JobDesired::Cancelled)).await;
    let restarted = start();
    let end = tokio::time::Instant::now() + WAIT;
    loop {
        let state = c.client.read(WAIT).await.unwrap();
        if state.assignments["process"]
            .observed
            .as_ref()
            .is_some_and(|(_, o)| o.phase == WorkPhase::Job(AttemptPhase::Cancelled))
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < end,
            "worker did not reconcile cancellation after SIGKILL"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        jobs::read_json::<jobs::Started>(&path).unwrap().unwrap(),
        before
    );
    assert_eq!(stdout(&home, "exec-process"), "once\n");
    drop(restarted);
    c.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admission_and_observations_reject_conflicts_gaps_and_stale_release() {
    let mut c = Cluster::new(1).await;
    let (_, config) = prepare(&mut c, true).await;
    let mut state = c.client.read(WAIT).await.unwrap();
    let mut index = 100;
    let principal = config.registration.peer.identity.to_string();
    let mut apply = |state: &mut State, who: &str, mutation: Mutation| {
        index += 1;
        state.apply(
            Command {
                principal: who.into(),
                request: Request {
                    origin: "cluster".into(),
                    id: format!("r-{index}"),
                    mutation,
                },
            },
            index,
        )
    };
    assert!(matches!(
        apply(
            &mut state,
            "admin",
            assign("a", Workload::Instance { spec: vm_spec() }).mutation
        ),
        Reply::Accepted { .. }
    ));
    assert!(matches!(
        apply(
            &mut state,
            "admin",
            assign(
                "memory-overflow",
                Workload::Job {
                    spec: Box::new(job("true")),
                    gpu: false,
                    memory_mb: 8192,
                }
            )
            .mutation
        ),
        Reply::Rejected { .. }
    ));
    let mut exclusive = vm_spec();
    exclusive.gpu = instances::Gpu::Exclusive;
    assert!(matches!(
        apply(
            &mut state,
            "admin",
            assign("b", Workload::Instance { spec: exclusive }).mutation
        ),
        Reply::Rejected { .. }
    ));
    let mut report = Report {
        worker: config.registration.id.clone(),
        incarnation: config.registration.incarnation.clone(),
        sequence: 2,
        assignment: "a".into(),
        revision: 1,
        observation: Observation {
            phase: WorkPhase::Instance(InstancePhase::Stopped),
            detail: None,
        },
    };
    assert!(matches!(
        apply(
            &mut state,
            &principal,
            Mutation::WorkerReport(report.clone())
        ),
        Reply::Rejected { .. }
    ));
    report.sequence = 1;
    assert!(matches!(
        apply(
            &mut state,
            "impostor",
            Mutation::WorkerReport(report.clone())
        ),
        Reply::Rejected { .. }
    ));
    assert_eq!(
        apply(
            &mut state,
            &principal,
            Mutation::WorkerReport(report.clone())
        ),
        Reply::Recorded
    );
    apply(
        &mut state,
        "admin",
        Mutation::Desire {
            id: "a".into(),
            expected_revision: 1,
            desired: Desired::Instance(InstanceDesired::Stopped),
        },
    );
    let mut large = job("true");
    large.threads = 4;
    assert!(matches!(
        apply(
            &mut state,
            "admin",
            assign(
                "large",
                Workload::Job {
                    spec: Box::new(large.clone()),
                    gpu: false,
                    memory_mb: 8192
                }
            )
            .mutation
        ),
        Reply::Rejected { .. }
    ));
    report.sequence = 2;
    report.revision = 2;
    assert_eq!(
        apply(
            &mut state,
            &principal,
            Mutation::WorkerReport(report.clone())
        ),
        Reply::Recorded
    );
    assert!(matches!(
        apply(
            &mut state,
            "admin",
            assign(
                "fits",
                Workload::Job {
                    spec: Box::new(large),
                    gpu: false,
                    memory_mb: 8192
                }
            )
            .mutation
        ),
        Reply::Accepted { .. }
    ));
    // Deletion remains possible after the stopped VM's compute is reused.
    assert!(matches!(
        apply(
            &mut state,
            "admin",
            Mutation::Desire {
                id: "a".into(),
                expected_revision: 2,
                desired: Desired::Instance(InstanceDesired::Deleted)
            }
        ),
        Reply::Accepted { .. }
    ));
    report.sequence = 3;
    report.revision = 3;
    report.observation.phase = WorkPhase::Instance(InstancePhase::Deleted);
    assert_eq!(
        apply(
            &mut state,
            &principal,
            Mutation::WorkerReport(report.clone())
        ),
        Reply::Recorded
    );
    report.sequence = 4;
    report.revision = 1;
    report.observation.phase = WorkPhase::Instance(InstancePhase::Running);
    assert_eq!(
        apply(&mut state, &principal, Mutation::WorkerReport(report)),
        Reply::Recorded
    );
    assert!(state.assignments["a"].terminal());
    c.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn published_native_job_without_launch_is_recovered_exactly_once() {
    let mut c = Cluster::new(1).await;
    let (home, config) = prepare(&mut c, false).await;
    let mut w = Worker::start(&home, config.clone(), hello()).await.unwrap();
    let spec = job("echo one > result.txt; sleep 1");
    c.client
        .write(
            assign(
                "gap",
                Workload::Job {
                    spec: Box::new(spec.clone()),
                    gpu: false,
                    memory_mb: 128,
                },
            ),
            WAIT,
        )
        .await
        .unwrap();
    let dir = home.join("backend/jobs/exec-gap");
    std::fs::create_dir_all(dir.join("work")).unwrap();
    instances::write(
        &dir.join("job.json"),
        &jobs::Job {
            id: "exec-gap".into(),
            spec,
            submitter: config.registration.peer.identity.to_string(),
            submitted_at: jobs::now(),
        },
    )
    .unwrap();
    instances::write(&dir.join("gpu-allocation.json"), &false).unwrap();
    wait(&mut c, &mut w, "gap", WorkPhase::Job(AttemptPhase::Running)).await;
    fleet::vm_job::launch(&dir, &w.config.supervisor).unwrap();
    fleet::vm_job::launch(&dir, &w.config.supervisor).unwrap();
    wait(
        &mut c,
        &mut w,
        "gap",
        WorkPhase::Job(AttemptPhase::Succeeded),
    )
    .await;
    let started = jobs::read_json::<jobs::Started>(&dir.join("supervisor.json"))
        .unwrap()
        .unwrap();
    fleet::vm_job::launch(&dir, &w.config.supervisor).unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        jobs::read_json::<jobs::Started>(&dir.join("supervisor.json"))
            .unwrap()
            .unwrap(),
        started
    );
    w.shutdown().await.unwrap();
    c.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn missing_backend_or_worker_journal_cannot_reexecute_an_accepted_job() {
    let mut c = Cluster::new(1).await;
    let (home, config) = prepare(&mut c, false).await;
    let mut w = Worker::start(&home, config.clone(), hello()).await.unwrap();
    c.client
        .write(assign("lost-records", workload("echo once; sleep 2")), WAIT)
        .await
        .unwrap();
    wait(
        &mut c,
        &mut w,
        "lost-records",
        WorkPhase::Job(AttemptPhase::Running),
    )
    .await;
    w.shutdown().await.unwrap();
    let dir = home.join("backend/jobs/exec-lost-records");
    let end = tokio::time::Instant::now() + WAIT;
    while !dir.join("exit.json").exists() {
        assert!(tokio::time::Instant::now() < end);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // Simulate lost backend records after side effects but before a terminal journal observation.
    std::fs::rename(&dir, home.join("lost-backend-records")).unwrap();
    let mut w = Worker::start(&home, config.clone(), hello()).await.unwrap();
    wait(
        &mut c,
        &mut w,
        "lost-records",
        WorkPhase::Job(AttemptPhase::Uncertain),
    )
    .await;
    assert!(!dir.exists());
    w.shutdown().await.unwrap();
    std::fs::rename(home.join("worker.redb"), home.join("saved-worker.redb")).unwrap();
    assert!(Worker::start(&home, config.clone(), hello()).await.is_err());
    std::fs::write(home.join("worker.redb"), []).unwrap();
    assert!(Worker::start(&home, config, hello()).await.is_err());
    c.close().await;
}

#[path = "recovery.rs"]
mod recovery;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queued_native_jobs_schedule_once_and_dependencies_wait_for_success() {
    let mut c = Cluster::new(3).await;
    let (home, config) = prepare(&mut c, false).await;
    let mut w = Worker::start(&home, config, hello()).await.unwrap();
    for (id, after) in [
        ("first", BTreeSet::new()),
        ("second", BTreeSet::from(["first".into()])),
    ] {
        let s = fleet::control::queue::Submission {
            inputs: Default::default(),
            id: id.into(),
            workload: workload("echo started-once; echo result > result.txt"),
            worker: None,
            priority: 0,
            requires: BTreeSet::new(),
            after,
        };
        c.client
            .write(
                Request {
                    origin: "cluster".into(),
                    id: format!("enqueue-{id}"),
                    mutation: Mutation::Enqueue(s),
                },
                WAIT,
            )
            .await
            .unwrap();
    }
    let end = tokio::time::Instant::now() + Duration::from_secs(50);
    loop {
        w.tick().await.unwrap();
        let state = c.client.read(WAIT).await.unwrap();
        if state
            .assignments
            .get("second-a1")
            .is_some_and(|a| a.terminal())
        {
            break;
        }
        assert!(tokio::time::Instant::now() < end);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    for id in ["first", "second"] {
        assert_eq!(stdout(&home, &format!("exec-{id}-a1")), "started-once\n");
    }
    w.shutdown().await.unwrap();
    c.close().await;
}

#[path = "checkpoints.rs"]
mod checkpoints;

#[path = "upgrade.rs"]
mod upgrade;

#[path = "storage.rs"]
mod storage;

#[path = "vm_lifecycle.rs"]
mod vm_lifecycle;

#[path = "portable.rs"]
mod portable;

#[path = "runtime_update.rs"]
mod runtime_update;

#[path = "history.rs"]
mod history;
