use super::*;
use fleet::control::{portable, workload::*};

#[test]
fn all_job_admission_paths_reject_missing_portable_bundles_and_reserved_inputs() {
    let mut state = queue_tests::state();
    let worker = state.workers.get_mut("w").unwrap();
    worker.budget = Budget {
        cpu_slots: 4,
        memory_mb: 8192,
        disk_gb: 200,
        vm_slots: 2,
    };
    worker.images.insert("macos".into());
    let runtime = portable::Runtime {
        platform: portable::Platform {
            os: "macos".into(),
            arch: "aarch64".into(),
            capabilities: BTreeSet::new(),
        },
        image_digest: format!("sha256:{}", "a".repeat(64)),
    };
    worker.runtimes.insert("macos".into(), runtime.clone());
    let mut submission = queue_tests::queued("portable");
    let Workload::Job { spec, .. } = &mut submission.workload else {
        unreachable!()
    };
    spec.vm = Some(fleet::instances::Spec {
        image: "macos".into(),
        runtime: Some(runtime),
        cpus: 1,
        memory_mb: 4096,
        disk_gb: 50,
        network: fleet::instances::Network::Offline,
        gpu: fleet::instances::Gpu::Shared,
    });
    let before = state.clone();
    let queued = state.enqueue(&submission, 1);
    let assigned = state.assign("portable", "execution", "w", &submission.workload);
    assert!(matches!(&queued, Reply::Rejected { reason } if reason.contains("input bundle")));
    assert_eq!(assigned, queued);
    assert_eq!(state, before);

    let mut submission = queue_tests::queued("reserved");
    let Workload::Job { spec, .. } = &mut submission.workload else {
        unreachable!()
    };
    spec.inputs.insert(
        format!("{}/state", fleet::checkpoint::INPUT_ROOT),
        fleet::jobs::Input {
            hash: "0".repeat(64).parse().unwrap(),
            executable: false,
        },
    );
    let queued = state.enqueue(&submission, 2);
    let assigned = state.assign("reserved", "execution", "w", &submission.workload);
    assert!(matches!(&queued, Reply::Rejected { reason } if reason.contains("reserved")));
    assert_eq!(assigned, queued);
    assert_eq!(state, before);
}

#[tokio::test]
async fn rejected_diagnostic_correction_preserves_receipts_and_sequence_across_reopen() {
    let home = Home::new();
    let path = home.0.join("report.redb");
    let mut store = Store::open(&path, "cluster", 1).unwrap();
    let state = queue_tests::state();
    let worker = state.workers["w"].clone();
    store
        .apply([entry(
            1,
            Request {
                origin: "cluster".into(),
                id: "enroll".into(),
                mutation: Mutation::Enroll {
                    worker: worker.clone(),
                },
            },
        )])
        .await
        .unwrap();
    store
        .apply([entry(
            2,
            Request {
                origin: "cluster".into(),
                id: "assign".into(),
                mutation: Mutation::Assign {
                    id: "job".into(),
                    execution: "execution".into(),
                    worker: "w".into(),
                    workload: queue_tests::queued("job").workload,
                },
            },
        )])
        .await
        .unwrap();
    let manifest = format!(r#"{{"version":1,"files":{{}},"{}":0}}"#, "界".repeat(2000));
    let error = serde_json::from_str::<fleet::checkpoint::Manifest>(&manifest).unwrap_err();
    let legacy: String = error.to_string().chars().take(2000).collect();
    let original = Report {
        worker: "w".into(),
        incarnation: worker.incarnation.clone(),
        sequence: 1,
        assignment: "job".into(),
        revision: 1,
        observation: Observation {
            phase: WorkPhase::Job(AttemptPhase::Running),
            detail: Some(format!("checkpoint capture: {legacy}")),
        },
    };
    let command = |index, report: Report, id: String| {
        let mut e = entry(
            index,
            Request {
                origin: "cluster".into(),
                id,
                mutation: Mutation::WorkerReport(report),
            },
        );
        if let EntryPayload::Normal(c) = &mut e.payload {
            c.principal = worker.peer.identity.to_string();
        }
        e
    };
    let rejected = store
        .apply([command(3, original.clone(), original.request_id())])
        .await
        .unwrap()
        .remove(0);
    assert_eq!(
        rejected,
        Reply::Rejected {
            reason: DETAIL_TOO_LARGE.into()
        }
    );
    let (corrected, id) = original.corrected_detail().unwrap();
    assert_eq!(corrected.sequence, original.sequence);
    assert_eq!(corrected.observation.phase, original.observation.phase);
    assert!(corrected.observation.detail.as_ref().unwrap().len() <= MAX_OBSERVATION_DETAIL);
    assert_ne!(id, original.request_id());
    assert_eq!(
        store
            .apply([command(4, corrected.clone(), id.clone())])
            .await
            .unwrap(),
        vec![Reply::Recorded]
    );
    // Lost correction reply, process restart, and delayed retries cannot change either receipt.
    drop(store);
    let mut store = Store::open(&path, "cluster", 1).unwrap();
    assert_eq!(
        store
            .apply([command(5, original.clone(), original.request_id())])
            .await
            .unwrap(),
        vec![rejected]
    );
    assert_eq!(
        store
            .apply([command(6, corrected.clone(), id)])
            .await
            .unwrap(),
        vec![Reply::Recorded]
    );
    assert!(matches!(
        store
            .apply([command(7, corrected.clone(), original.request_id())])
            .await
            .unwrap()[0],
        Reply::Rejected { .. }
    ));
    let mut next = corrected;
    next.sequence = 2;
    next.observation.phase = WorkPhase::Job(AttemptPhase::Succeeded);
    assert_eq!(
        store
            .apply([command(8, next.clone(), next.request_id())])
            .await
            .unwrap(),
        vec![Reply::Recorded]
    );
    let final_state = store.state().await.unwrap();
    assert_eq!(final_state.cursors[&worker.cursor()], 2);
    assert!(final_state.assignments["job"].terminal());
}
