use super::*;
use fleet::control::{queue::Submission, workload::*};
pub(super) fn state() -> State {
    let mut s = State::new("cluster".into());
    s.enroll(&Registration {
        runtimes: Default::default(),
        tags: BTreeSet::new(),
        id: "w".into(),
        peer: Peer {
            identity: SecretKey::generate().public(),
            address: "127.0.0.1:19101".parse().unwrap(),
        },
        incarnation: "one".into(),
        budget: Budget {
            cpu_slots: 1,
            memory_mb: 1024,
            disk_gb: 0,
            vm_slots: 0,
        },
        images: BTreeSet::new(),
        linux_images: BTreeSet::new(),
    });
    s
}
pub(super) fn queued(id: &str) -> Submission {
    Submission {
        inputs: Default::default(),
        id: id.into(),
        workload: Workload::Job {
            spec: Box::new(
                serde_json::from_value(serde_json::json!({"name":id,"argv":["true"],"threads":1}))
                    .unwrap(),
            ),
            gpu: false,
            memory_mb: 512,
        },
        worker: None,
        priority: 0,
        requires: BTreeSet::new(),
        after: BTreeSet::new(),
    }
}
#[test]
fn queue_reserves_atomically_and_cancellation_waits_for_worker_evidence() {
    let mut s = state();
    assert!(matches!(s.enqueue(&queued("a"), 1), Reply::Accepted { .. }));
    s.enqueue(&queued("b"), 2);
    assert!(matches!(
        s.place_queued("a", "w", 1),
        Reply::Accepted { .. }
    ));
    assert!(matches!(
        s.place_queued("b", "w", 1),
        Reply::Rejected { .. }
    ));
    assert!(matches!(s.cancel_queued("a", 2), Reply::Accepted { .. }));
    assert!(matches!(
        s.place_queued("b", "w", 1),
        Reply::Rejected { .. }
    ));
    let w = s.workers["w"].clone();
    assert_eq!(
        s.report(
            &w.peer.identity.to_string(),
            &Report {
                worker: "w".into(),
                incarnation: w.incarnation,
                sequence: 1,
                assignment: "a-a1".into(),
                revision: 2,
                observation: Observation {
                    phase: WorkPhase::Job(AttemptPhase::Cancelled),
                    detail: None
                }
            }
        ),
        Reply::Recorded
    );
    assert!(matches!(
        s.place_queued("b", "w", 1),
        Reply::Accepted { .. }
    ));
    assert!(matches!(
        s.place_queued("b", "w", 1),
        Reply::Rejected { .. }
    ));
}
#[test]
fn queue_priority_dependencies_staging_and_drain_gate_placement() {
    let mut s = state();
    s.enqueue(&queued("a"), 1);
    let mut b = queued("b");
    b.priority = 10;
    b.after.insert("a".into());
    s.enqueue(&b, 2);
    assert_eq!(s.scheduling_order()[0].id.as_ref(), "b");
    assert!(s.placement_candidate("b", "w").is_err());
    s.cancel_queued("a", 1);
    assert!(s.placement_candidate("b", "w").is_err());
    let mut c = queued("c");
    if let Workload::Job { spec, .. } = &mut c.workload {
        spec.inputs.insert(
            "input".into(),
            fleet::jobs::Input {
                hash: iroh_blobs::Hash::new(b"input"),
                executable: false,
            },
        );
    }
    s.enqueue(&c, 3);
    assert!(s.placement_candidate("c", "w").is_err());
    assert_eq!(s.stage_job("c", "w"), Reply::Recorded);
    s.drained.insert("w".into());
    assert!(s.placement_candidate("c", "w").is_err());
    s.drained.clear();
    assert!(s.placement_candidate("c", "w").is_ok());
    let round: State = serde_json::from_slice(&serde_json::to_vec(&s).unwrap()).unwrap();
    assert_eq!(round.jobs, s.jobs);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queued_intent_and_cancel_survive_leader_replacement() {
    let mut c = Cluster::new(3).await;
    let registration = state().workers["w"].clone();
    c.client
        .write(
            Request {
                origin: "cluster".into(),
                id: "enroll-w".into(),
                mutation: Mutation::Enroll {
                    worker: registration,
                },
            },
            WAIT,
        )
        .await
        .unwrap();
    let request = Request {
        origin: "cluster".into(),
        id: "enqueue".into(),
        mutation: Mutation::Enqueue(queued("job")),
    };
    let accepted = c.client.write(request.clone(), WAIT).await.unwrap();
    let leader = c.leader().await;
    c.replicas
        .remove(&leader)
        .unwrap()
        .shutdown()
        .await
        .unwrap();
    assert_eq!(c.client.write(request, WAIT * 3).await.unwrap(), accepted);
    let s = c.client.read(WAIT).await.unwrap();
    assert!(s.jobs.contains_key("job"));
    assert!(s.assignments.is_empty());
    c.client
        .write(
            Request {
                origin: "cluster".into(),
                id: "cancel".into(),
                mutation: Mutation::CancelQueued {
                    id: "job".into(),
                    expected_revision: 1,
                },
            },
            WAIT,
        )
        .await
        .unwrap();
    assert_eq!(
        c.client.read(WAIT).await.unwrap().jobs["job"].phase,
        fleet::control::queue::JobPhase::Cancelled
    );
    c.close().await;
}

/// Linux guests have no GPU and are not subject to the macOS VM slots; CPU and memory still are.
#[test]
fn linux_vms_skip_macos_slots_and_gpu_exclusivity() {
    let mut s = State::new("cluster".into());
    let worker = Registration {
        runtimes: Default::default(),
        tags: BTreeSet::new(),
        id: "w".into(),
        peer: Peer {
            identity: SecretKey::generate().public(),
            address: "127.0.0.1:19102".parse().unwrap(),
        },
        incarnation: "one".into(),
        budget: Budget {
            cpu_slots: 8,
            memory_mb: 16384,
            disk_gb: 200,
            vm_slots: 1,
        },
        images: BTreeSet::from(["macos".into(), "linux".into()]),
        linux_images: BTreeSet::from(["linux".into()]),
    };
    assert_eq!(s.enroll(&worker), Reply::Recorded);
    let instance = |spec: serde_json::Value| Workload::Instance {
        spec: serde_json::from_value(spec).unwrap(),
    };
    let linux = |cpus: u32| {
        instance(
            serde_json::json!({"image":"linux","cpus":cpus,"memory_mb":512,"disk_gb":1,"network":"offline"}),
        )
    };
    let mac = || {
        instance(
            serde_json::json!({"image":"macos","cpus":2,"memory_mb":4096,"disk_gb":50,"network":"offline","gpu":"exclusive"}),
        )
    };
    let accepted = |r: Reply| matches!(r, Reply::Accepted { .. });
    // Linux sizes are too small for a macOS image.
    assert!(!accepted(s.assign(
        "small-mac",
        "x-small-mac",
        "w",
        &instance(serde_json::json!({"image":"macos","cpus":1,"memory_mb":512,"disk_gb":1,"network":"offline"}))
    )));
    assert!(accepted(s.assign("mac", "x-mac", "w", &mac())));
    for id in ["l0", "l1", "l2"] {
        assert!(accepted(s.assign(id, &format!("x-{id}"), "w", &linux(1))));
    }
    // The one macOS slot is taken.
    assert!(!accepted(s.assign("mac2", "x-mac2", "w", &mac())));
    // 2 + 3 + 4 > 8 CPU slots.
    assert!(!accepted(s.assign("l3", "x-l3", "w", &linux(4))));
    assert!(accepted(s.assign("l3", "x-l3", "w", &linux(3))));
}
