use super::*;
use fleet::control::recovery::{Manifest, accept_client, recover};

async fn restore(c: &mut Cluster, source: u64, name: &str) -> (PathBuf, Manifest) {
    for (_, replica) in std::mem::take(&mut c.replicas) {
        replica.shutdown().await.unwrap();
    }
    let socket = reserve_socket();
    let home = c.home.0.join(name);
    let manifest = recover(
        &c.home.0.join(source.to_string()),
        &Config {
            id: source,
            ..c.config.clone()
        },
        c.client.endpoint.secret_key(),
        &home,
        socket.local_addr().unwrap(),
    )
    .await
    .unwrap();
    drop(socket);
    let replica = Replica::start(&home, manifest.plan.config.clone())
        .await
        .unwrap();
    replica
        .bootstrap(&manifest.plan.config.peers)
        .await
        .unwrap();
    c.client.endpoint.close().await;
    c.client = Client::new(
        c.client.endpoint.secret_key().clone(),
        manifest.plan.config.cluster.clone(),
        manifest.plan.config.peers.clone(),
    )
    .await
    .unwrap();
    c.config = manifest.plan.config.clone();
    c.replicas.insert(1, replica);
    c.client.read(WAIT).await.unwrap();
    (home, manifest)
}
fn scoped(c: &Cluster, mut request: Request) -> Request {
    request.origin = c.config.cluster.clone();
    request
}
async fn resume(c: &mut Cluster, unavailable: &[&str]) {
    let reply = c
        .client
        .write(
            Request {
                origin: c.config.cluster.clone(),
                id: format!("resume-{}", rand::random::<u64>()),
                mutation: Mutation::ResumeRecovery {
                    unavailable_workers: unavailable.iter().map(|s| s.to_string()).collect(),
                },
            },
            WAIT,
        )
        .await
        .unwrap();
    assert_eq!(reply, Reply::Recorded);
}
async fn reconciled(c: &mut Cluster, worker: &mut Worker) {
    for _ in 0..5 {
        worker.tick().await.unwrap();
        if !c
            .client
            .read(WAIT)
            .await
            .unwrap()
            .worker_recovery_required(&worker.config.registration.id)
        {
            worker.tick().await.unwrap();
            return;
        }
    }
    panic!("worker did not reconcile");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovery_imports_forgotten_running_native_work_and_fences_old_authority() {
    let mut c = Cluster::new(3).await;
    let (home, config) = prepare(&mut c, false).await;
    instances::write(&home.join("worker.json"), &config).unwrap();
    let mut worker = Worker::start(&home, config.clone(), hello()).await.unwrap();
    let known = assign("known", workload("echo known > result.txt"));
    let known_reply = c.client.write(known.clone(), WAIT).await.unwrap();
    wait(&mut c, &mut worker, "known", WorkPhase::Succeeded).await;
    // Ensure the selected survivor contains both the receipt and enrollment, then let it fall behind.
    let end = tokio::time::Instant::now() + WAIT;
    while !c.replicas[&1]
        .store
        .state()
        .await
        .unwrap()
        .assignments
        .get("known")
        .is_some_and(|a| a.terminal())
    {
        assert!(tokio::time::Instant::now() < end);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    c.replicas.remove(&1).unwrap().shutdown().await.unwrap();
    let missing = assign(
        "missing",
        workload("echo started-once; echo result > result.txt; sleep 25"),
    );
    c.client.write(missing.clone(), WAIT).await.unwrap();
    wait(&mut c, &mut worker, "missing", WorkPhase::Running).await;
    let supervisor: jobs::Started =
        jobs::read_json(&home.join("backend/jobs/exec-missing/supervisor.json"))
            .unwrap()
            .unwrap();
    worker.shutdown().await.unwrap();
    let original = c.config.clone();
    let (recovered_home, manifest) = restore(&mut c, 1, "recovered").await;
    assert!(recovered_home.join("source.redb").exists());
    assert_eq!(manifest.plan.logical_cluster, original.cluster);
    assert_ne!(manifest.plan.config.cluster, original.cluster);
    assert_eq!(c.client.write(known, WAIT).await.unwrap(), known_reply);
    assert!(matches!(
        c.client.write(missing.clone(), WAIT).await.unwrap(),
        Reply::RecoveryUnknown { .. }
    ));
    assert!(
        !c.client
            .read(WAIT)
            .await
            .unwrap()
            .assignments
            .contains_key("missing")
    );
    let fresh = scoped(&c, assign("new-before-resume", workload("true")));
    assert!(matches!(
        c.client.write(fresh, WAIT).await.unwrap(),
        Reply::Rejected { .. }
    ));
    Worker::accept_recovery(&home, &manifest).unwrap();
    // Rebinding is repeatable; original worker config can never be used again.
    Worker::accept_recovery(&home, &manifest).unwrap();
    assert!(Worker::start(&home, config, hello()).await.is_err());
    let next: WorkerConfig =
        serde_json::from_slice(&std::fs::read(home.join("worker.json")).unwrap()).unwrap();
    let mut worker = Worker::start(&home, next.clone(), hello()).await.unwrap();
    assert!(
        Worker::accept_recovery(&home, &manifest).is_err(),
        "running worker must hold journal lock"
    );
    c.replicas[&1]
        .drop_write_reply
        .store(true, std::sync::atomic::Ordering::SeqCst);
    reconciled(&mut c, &mut worker).await;
    let state = c.client.read(WAIT).await.unwrap();
    assert!(state.assignments.contains_key("missing"));
    assert!(!state.recovery.unwrap().resumed);
    let after: jobs::Started =
        jobs::read_json(&home.join("backend/jobs/exec-missing/supervisor.json"))
            .unwrap()
            .unwrap();
    assert_eq!(supervisor.supervisor_pid, after.supervisor_pid);
    assert_eq!(
        std::fs::read_to_string(home.join("backend/jobs/exec-missing/stdout.log")).unwrap(),
        "started-once\n"
    );
    // Rebinding the client is explicit and signed. A tampered manifest is rejected.
    let client_path = c.home.0.join("client.json");
    instances::write(&client_path, &original).unwrap();
    let mut forged = manifest.clone();
    forged.plan.config.cluster = "forged".into();
    assert!(accept_client(&client_path, &forged).is_err());
    accept_client(&client_path, &manifest).unwrap();
    accept_client(&client_path, &manifest).unwrap();
    assert!(
        worker
            .client
            .learn(State::new(original.cluster.clone()).routing_view())
            .is_err()
    );
    // Even an authenticated old replica key cannot send its old history to the new endpoint.
    let old_key = fleet::keys::load_or_create(&c.home.0.join("1/node.key")).unwrap();
    let endpoint =
        fleet::control::network::connect_endpoint(old_key, "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
    let connection = endpoint
        .connect(c.config.peers[&1].addr(), ALPN)
        .await
        .unwrap();
    let result = tokio::time::timeout(
        WAIT,
        exchange(&connection, &original.cluster, Message::Read),
    )
    .await;
    assert!(!matches!(result, Ok(Ok(Answer::Read(Ok(_))))));
    endpoint.close().await;
    resume(&mut c, &[]).await;
    let cancel = Request {
        origin: c.config.cluster.clone(),
        id: "cancel-imported".into(),
        mutation: Mutation::Desire {
            id: "missing".into(),
            expected_revision: 1,
            desired: Desired::Cancelled,
        },
    };
    assert!(matches!(
        c.client.write(cancel, WAIT).await.unwrap(),
        Reply::Accepted { .. }
    ));
    wait(&mut c, &mut worker, "missing", WorkPhase::Cancelled).await;
    // Forgetting the submission receipt remains an unknown outcome even after inventory import.
    assert!(matches!(
        c.client.write(missing, WAIT).await.unwrap(),
        Reply::RecoveryUnknown { .. }
    ));
    worker.shutdown().await.unwrap();
    let mut worker = Worker::start(&home, next, hello()).await.unwrap();
    worker.tick().await.unwrap();
    assert_eq!(
        std::fs::read_to_string(home.join("backend/jobs/exec-missing/stdout.log")).unwrap(),
        "started-once\n"
    );
    worker.shutdown().await.unwrap();
    c.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovery_holds_undelivered_vms_and_unavailable_workers() {
    let mut c = Cluster::new(1).await;
    let (home, config) = prepare(&mut c, true).await;
    instances::write(&home.join("worker.json"), &config).unwrap();
    let mut worker = Worker::start(&home, config.clone(), hello()).await.unwrap();
    c.client
        .write(
            assign("existing-vm", Workload::Instance { spec: vm_spec() }),
            WAIT,
        )
        .await
        .unwrap();
    wait(&mut c, &mut worker, "existing-vm", WorkPhase::Running).await;
    // A committed assignment never reached the worker. Recovery must not blindly start it.
    c.client
        .write(
            assign("undelivered", Workload::Instance { spec: vm_spec() }),
            WAIT,
        )
        .await
        .unwrap();
    worker.shutdown().await.unwrap();
    let (_, manifest) = restore(&mut c, 1, "recovered").await;
    resume(&mut c, &["worker-real"]).await;
    let fresh = scoped(&c, assign("blocked", workload("true")));
    assert!(matches!(
        c.client.write(fresh, WAIT).await.unwrap(),
        Reply::Rejected { .. }
    ));
    Worker::accept_recovery(&home, &manifest).unwrap();
    let config = serde_json::from_slice(&std::fs::read(home.join("worker.json")).unwrap()).unwrap();
    let mut worker = Worker::start(&home, config, hello()).await.unwrap();
    reconciled(&mut c, &mut worker).await;
    assert!(
        c.client
            .read(WAIT)
            .await
            .unwrap()
            .recovery
            .unwrap()
            .held
            .contains("undelivered")
    );
    assert!(!home.join("backend/instances/exec-undelivered").exists());
    let delete = Request {
        origin: c.config.cluster.clone(),
        id: "delete-held".into(),
        mutation: Mutation::Desire {
            id: "undelivered".into(),
            expected_revision: 1,
            desired: Desired::Deleted,
        },
    };
    assert!(matches!(
        c.client.write(delete, WAIT).await.unwrap(),
        Reply::Accepted { .. }
    ));
    wait(&mut c, &mut worker, "undelivered", WorkPhase::Deleted).await;
    let instance = scoped(
        &c,
        assign("fresh-vm", Workload::Instance { spec: vm_spec() }),
    );
    assert!(matches!(
        c.client.write(instance, WAIT).await.unwrap(),
        Reply::Accepted { .. }
    ));
    wait(&mut c, &mut worker, "fresh-vm", WorkPhase::Running).await;
    let delete = Request {
        origin: c.config.cluster.clone(),
        id: "delete-fresh".into(),
        mutation: Mutation::Desire {
            id: "fresh-vm".into(),
            expected_revision: 1,
            desired: Desired::Deleted,
        },
    };
    c.client.write(delete, WAIT).await.unwrap();
    wait(&mut c, &mut worker, "fresh-vm", WorkPhase::Deleted).await;
    let delete = Request {
        origin: c.config.cluster.clone(),
        id: "delete-existing".into(),
        mutation: Mutation::Desire {
            id: "existing-vm".into(),
            expected_revision: 1,
            desired: Desired::Deleted,
        },
    };
    c.client.write(delete, WAIT).await.unwrap();
    wait(&mut c, &mut worker, "existing-vm", WorkPhase::Deleted).await;
    worker.shutdown().await.unwrap();
    c.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovery_excludes_uncommitted_suffix_and_can_grow_normally() {
    let mut c = Cluster::new(3).await;
    let leader = c.leader().await;
    let socket = reserve_socket();
    let address = socket.local_addr().unwrap();
    // Offline source and existing administrator are mandatory; partial preparation never publishes config.
    assert!(
        recover(
            &c.home.0.join(leader.to_string()),
            &Config {
                id: leader,
                ..c.config.clone()
            },
            c.client.endpoint.secret_key(),
            &c.home.0.join("live-refused"),
            address
        )
        .await
        .is_err()
    );
    assert!(!c.home.0.join("live-refused/config.json").exists());
    for id in c.replicas.keys().copied().collect::<Vec<_>>() {
        if id != leader {
            c.replicas.remove(&id).unwrap().shutdown().await.unwrap();
        }
    }
    let request = submit("uncommitted", "uncommitted");
    assert!(
        c.client
            .write(request.clone(), Duration::from_millis(350))
            .await
            .is_err()
    );
    let applied = c.replicas[&leader].store.state().await.unwrap();
    assert!(!applied.jobs.contains_key("uncommitted"));
    use openraft::RaftLogReader;
    let logs = c
        .replicas
        .get_mut(&leader)
        .unwrap()
        .store
        .try_get_log_entries(..)
        .await
        .unwrap();
    assert!(logs.iter().any(|e| matches!(&e.payload, EntryPayload::Normal(command) if command.request.id == "uncommitted")), "test must contain an unapplied log entry");
    drop(socket);
    let (_, manifest) = restore(&mut c, leader, "recovered").await;
    assert_eq!(manifest.plan.source_applied, applied.last_applied.unwrap());
    assert!(matches!(
        c.client.write(request, WAIT).await.unwrap(),
        Reply::RecoveryUnknown { .. }
    ));
    assert!(
        !c.client
            .read(WAIT)
            .await
            .unwrap()
            .jobs
            .contains_key("uncommitted")
    );
    c.prepare(4, true).await;
    c.prepare(5, true).await;
    let request = c.membership_request("grow-recovered", 1, &[1, 4, 5]);
    let state = c.changed(request).await;
    assert_eq!(state.membership.voter_ids().count(), 3);
    assert_eq!(state.recovery.unwrap().manifest, manifest);
    c.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovered_seed_survives_growth_and_cold_restart_without_the_source_replica() {
    let mut c = Cluster::new(1).await;
    let original = submit("seed-receipt", "seed-job");
    let accepted = c.client.write(original.clone(), WAIT).await.unwrap();
    let (_, manifest) = restore(&mut c, 1, "recovered").await;
    let before = c.client.read(WAIT).await.unwrap();
    c.prepare(4, true).await;
    c.prepare(5, true).await;
    c.changed(c.membership_request("grow-seed", 1, &[1, 4, 5]))
        .await;
    // Membership alone is insufficient: these members must receive the out-of-log recovery seed.
    for (_, replica) in std::mem::take(&mut c.replicas) {
        replica.shutdown().await.unwrap();
    }
    c.start(4).await;
    c.start(5).await;
    let after = c.client.read(WAIT).await.unwrap();
    assert_eq!(after.jobs, before.jobs);
    assert_eq!(after.recovery.unwrap().manifest, manifest);
    assert_eq!(c.client.write(original, WAIT).await.unwrap(), accepted);
    assert!(matches!(
        c.client
            .write(submit("unknown-old", "unknown-old"), WAIT)
            .await
            .unwrap(),
        Reply::RecoveryUnknown { .. }
    ));
    c.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovery_inventory_conflicts_are_atomic_and_old_receipts_remain_reserved() {
    use fleet::control::recovery::Inventory;
    let mut c = Cluster::new(1).await;
    let (home, config) = prepare(&mut c, false).await;
    let original = assign("reserved", workload("true"));
    c.client.write(original.clone(), WAIT).await.unwrap();
    c.client
        .write(
            Request {
                origin: c.config.cluster.clone(),
                id: "release".into(),
                mutation: Mutation::Release {
                    request_id: original.id.clone(),
                },
            },
            WAIT,
        )
        .await
        .unwrap();
    let (_, manifest) = restore(&mut c, 1, "recovered").await;
    assert!(matches!(
        c.client.write(original.clone(), WAIT).await.unwrap(),
        Reply::Expired { .. }
    ));
    let mut changed = original.clone();
    changed.origin = c.config.cluster.clone();
    assert!(matches!(
        c.client.write(changed, WAIT).await.unwrap(),
        Reply::Rejected { .. }
    ));
    let mut state = c.client.read(WAIT).await.unwrap();
    let mut assignment = state.assignments["reserved"].clone();
    assignment.workload = workload("false");
    let inventory = Inventory {
        held: BTreeSet::new(),
        registration: config.registration.clone(),
        sequence: 0,
        assignments: BTreeMap::from([("reserved".into(), assignment)]),
    };
    let before = state.clone();
    assert!(matches!(
        state.recover_worker(&config.registration.peer.identity.to_string(), &inventory),
        Reply::Rejected { .. }
    ));
    assert_eq!(state, before);
    let mut forged = inventory.clone();
    forged.registration.incarnation = "other".into();
    assert!(matches!(
        state.recover_worker(&config.registration.peer.identity.to_string(), &forged),
        Reply::Rejected { .. }
    ));
    assert_eq!(state, before);
    // Authentication does not let a worker authorize global resume.
    let key = fleet::keys::load_or_create(&home.join("node.key")).unwrap();
    let mut worker_client = Client::new(key, c.config.cluster.clone(), c.config.peers.clone())
        .await
        .unwrap();
    assert!(
        worker_client
            .write(
                Request {
                    origin: c.config.cluster.clone(),
                    id: "unauthorized-resume".into(),
                    mutation: Mutation::ResumeRecovery {
                        unavailable_workers: BTreeSet::new()
                    }
                },
                WAIT
            )
            .await
            .is_err()
    );
    worker_client.endpoint.close().await;
    // A sibling recovery signed by the same administrator cannot silently replace an accepted choice.
    let client_path = c.home.0.join("client-choice.json");
    let old = Config {
        cluster: manifest.plan.previous.clone(),
        ..c.config.clone()
    };
    instances::write(&client_path, &old).unwrap();
    accept_client(&client_path, &manifest).unwrap();
    let socket = reserve_socket();
    let sibling = recover(
        &c.home.0.join("1"),
        &Config {
            cluster: manifest.plan.previous.clone(),
            ..old
        },
        c.client.endpoint.secret_key(),
        &c.home.0.join("sibling"),
        socket.local_addr().unwrap(),
    )
    .await
    .unwrap();
    assert!(accept_client(&client_path, &sibling).is_err());
    c.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repeated_recovery_preserves_holds_and_repairs_interrupted_worker_rebind() {
    let mut c = Cluster::new(1).await;
    let (home, original_config) = prepare(&mut c, false).await;
    instances::write(&home.join("worker.json"), &original_config).unwrap();
    c.client
        .write(assign("never-start", workload("echo must-not-run")), WAIT)
        .await
        .unwrap();
    let (first_home, first) = restore(&mut c, 1, "first-recovery").await;
    Worker::accept_recovery(&home, &first).unwrap();
    // Simulate interruption after committing the new journal but before replacing worker.json.
    instances::write(&home.join("worker.json"), &original_config).unwrap();
    assert!(
        Worker::start(&home, original_config, hello())
            .await
            .is_err()
    );
    Worker::accept_recovery(&home, &first).unwrap();
    let config = serde_json::from_slice(&std::fs::read(home.join("worker.json")).unwrap()).unwrap();
    let mut worker = Worker::start(&home, config, hello()).await.unwrap();
    reconciled(&mut c, &mut worker).await;
    assert!(
        c.client
            .read(WAIT)
            .await
            .unwrap()
            .recovery
            .unwrap()
            .held
            .contains("never-start")
    );
    worker.shutdown().await.unwrap();
    for (_, replica) in std::mem::take(&mut c.replicas) {
        replica.shutdown().await.unwrap();
    }
    let socket = reserve_socket();
    let second_home = c.home.0.join("second-recovery");
    let second = recover(
        &first_home,
        &c.config,
        c.client.endpoint.secret_key(),
        &second_home,
        socket.local_addr().unwrap(),
    )
    .await
    .unwrap();
    drop(socket);
    assert_eq!(second.plan.logical_cluster, first.plan.logical_cluster);
    assert!(second.plan.ancestors.contains(&first.plan.config.cluster));
    let replica = Replica::start(&second_home, second.plan.config.clone())
        .await
        .unwrap();
    replica.bootstrap(&second.plan.config.peers).await.unwrap();
    c.client.endpoint.close().await;
    c.config = second.plan.config.clone();
    c.client = Client::new(
        c.client.endpoint.secret_key().clone(),
        c.config.cluster.clone(),
        c.config.peers.clone(),
    )
    .await
    .unwrap();
    c.replicas.insert(1, replica);
    Worker::accept_recovery(&home, &second).unwrap();
    assert!(
        Worker::accept_recovery(&home, &first).is_err(),
        "old signed recovery cannot roll back authority"
    );
    let config = serde_json::from_slice(&std::fs::read(home.join("worker.json")).unwrap()).unwrap();
    let mut worker = Worker::start(&home, config, hello()).await.unwrap();
    reconciled(&mut c, &mut worker).await;
    resume(&mut c, &[]).await;
    worker.tick().await.unwrap();
    assert!(
        c.client
            .read(WAIT)
            .await
            .unwrap()
            .recovery
            .unwrap()
            .held
            .contains("never-start")
    );
    assert!(!home.join("backend/jobs/exec-never-start/job.json").exists());
    let cancel = Request {
        origin: c.config.cluster.clone(),
        id: "cancel-held".into(),
        mutation: Mutation::Desire {
            id: "never-start".into(),
            expected_revision: 1,
            desired: Desired::Cancelled,
        },
    };
    c.client.write(cancel, WAIT).await.unwrap();
    wait(&mut c, &mut worker, "never-start", WorkPhase::Cancelled).await;
    worker.shutdown().await.unwrap();
    c.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovered_worker_does_not_start_accepted_pending_work_before_explicit_resume() {
    let mut c = Cluster::new(1).await;
    let (home, config) = prepare(&mut c, false).await;
    instances::write(&home.join("worker.json"), &config).unwrap();
    let store = fleet::blobs::open(&c.home.0.join("input-store"))
        .await
        .unwrap();
    let input = c.home.0.join("input.txt");
    std::fs::write(&input, "verified\n").unwrap();
    let hash = store.blobs().add_path(&input).await.unwrap().hash;
    let mut spec = job("cat input.txt > result.txt; echo started-once");
    spec.inputs.insert(
        "input.txt".into(),
        jobs::Input {
            hash,
            executable: false,
        },
    );
    c.client
        .write(
            assign(
                "pending",
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
    let mut worker = Worker::start(&home, config.clone(), hello()).await.unwrap();
    worker.tick().await.unwrap(); // Durable acceptance, awaiting its input; no execution attempt.
    assert!(!home.join("backend/jobs/exec-pending/job.json").exists());
    worker.shutdown().await.unwrap();
    let (_, manifest) = restore(&mut c, 1, "recovered").await;
    Worker::accept_recovery(&home, &manifest).unwrap();
    let next: WorkerConfig =
        serde_json::from_slice(&std::fs::read(home.join("worker.json")).unwrap()).unwrap();
    let mut worker = Worker::start(&home, next.clone(), hello()).await.unwrap();
    push_inputs(
        &store,
        &c.client.endpoint,
        &c.config.cluster,
        &config.registration.peer,
        &spec,
    )
    .await
    .unwrap();
    reconciled(&mut c, &mut worker).await;
    assert!(!home.join("backend/jobs/exec-pending/job.json").exists());
    worker.shutdown().await.unwrap();
    let mut worker = Worker::start(&home, next, hello()).await.unwrap();
    worker.tick().await.unwrap();
    assert!(
        !home.join("backend/jobs/exec-pending/job.json").exists(),
        "pause must survive restart"
    );
    resume(&mut c, &[]).await;
    wait(&mut c, &mut worker, "pending", WorkPhase::Succeeded).await;
    assert_eq!(
        std::fs::read_to_string(home.join("backend/jobs/exec-pending/stdout.log")).unwrap(),
        "started-once\n"
    );
    worker.shutdown().await.unwrap();
    c.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovery_startup_does_not_run_legacy_vm_repair_before_authorization() {
    let mut c = Cluster::new(1).await;
    let (home, config) = prepare(&mut c, true).await;
    instances::write(&home.join("worker.json"), &config).unwrap();
    let mut spec = job("echo must-not-run");
    spec.vm = Some(vm_spec());
    spec.shutdown_grace_seconds = None;
    spec.inputs.insert(
        "input.txt".into(),
        jobs::Input {
            hash: iroh_blobs::Hash::from_bytes([7; 32]),
            executable: false,
        },
    );
    c.client
        .write(
            assign(
                "reserved-vm-job",
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
    let mut worker = Worker::start(&home, config.clone(), hello()).await.unwrap();
    worker.tick().await.unwrap();
    worker.shutdown().await.unwrap();
    // Model an interrupted backend publication: reservation exists, but no job/supervisor exists.
    let backend = home.join("backend");
    let record = instances::reserve_job(
        &backend,
        "exec-reserved-vm-job",
        vm_spec(),
        &config.registration.peer.identity.to_string(),
        &hello(),
    )
    .unwrap();
    let dir = instances::directory(&backend, &record.record.id).unwrap();
    let (_, manifest) = restore(&mut c, 1, "recovered").await;
    Worker::accept_recovery(&home, &manifest).unwrap();
    let next = serde_json::from_slice(&std::fs::read(home.join("worker.json")).unwrap()).unwrap();
    let mut worker = Worker::start(&home, next, hello()).await.unwrap();
    reconciled(&mut c, &mut worker).await;
    tokio::time::sleep(Duration::from_millis(2200)).await;
    assert_eq!(
        instances::info(&backend, &record.record.id).unwrap().record,
        record.record
    );
    assert!(
        !dir.join("controller.log").exists(),
        "background observer must not start VM controllers"
    );
    let cancel = Request {
        origin: c.config.cluster.clone(),
        id: "cancel-reservation".into(),
        mutation: Mutation::Desire {
            id: "reserved-vm-job".into(),
            expected_revision: 1,
            desired: Desired::Cancelled,
        },
    };
    c.client.write(cancel, WAIT).await.unwrap();
    wait(&mut c, &mut worker, "reserved-vm-job", WorkPhase::Cancelled).await;
    worker.shutdown().await.unwrap();
    c.close().await;
}
