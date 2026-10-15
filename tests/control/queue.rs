use super::*;
use fleet::control::{queue::Submission, workload::*};
fn state() -> State {
    let mut s = State::new("cluster".into());
    s.enroll(&Registration {
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
    });
    s
}
fn queued(id: &str) -> Submission {
    Submission {
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
