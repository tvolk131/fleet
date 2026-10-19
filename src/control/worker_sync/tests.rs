use super::*;
use crate::control::workload::{Desired, JobDesired};

pub(crate) fn view() -> WorkerView {
    let key = iroh::SecretKey::generate().public();
    serde_json::from_value(serde_json::json!({
        "generation":1,"vm":crate::control::vm::Catalog::default(),"history":{},"artifacts":[],"stores":{},"cleanup_allowed":true,
        "starts_allowed":true,"checkpoints":[],"resume_checkpoints":{},"recovery_required":false,
        "held":[],"registration":{
            "id":"worker","incarnation":"disk-1","peer":{"identity":key,"address":"127.0.0.1:29001"},
            "budget":{"cpu_slots":2,"memory_mb":4096,"disk_gb":100,"vm_slots":1},
            "images":["macos"],"runtimes":{},"tags":[]
        },"assignments":[{
            "id":"job","execution":"exec-job","attempt":{"job":"job","number":1,"checkpoint":null},
            "suspend_checkpoint":null,"worker":"worker",
            "workload":{"Job":{"spec":{"name":"test","argv":["true"],"keep_awake":false},"gpu":false,"memory_mb":128}},
            "desired":{"Job":"Running"},"revision":1,"observed":null
        }],"acknowledged":0
    })).unwrap()
}

fn large_view() -> WorkerView {
    let mut view = view();
    let crate::control::workload::Workload::Job { spec, .. } = &mut view.assignments[0].workload
    else {
        unreachable!()
    };
    spec.name = "界".repeat(PAGE_BYTES * 2);
    view
}

pub(crate) fn collect(
    cache: &mut Cache,
    receiver: &mut Receiver,
    revision: u64,
    view: &WorkerView,
) -> Saved {
    for _ in 0..1000 {
        let reply = cache
            .answer("worker", revision, view, receiver.request())
            .unwrap();
        // Worst-case byte escaping/envelope remains comfortably below the control frame limit.
        assert!(serde_json::to_vec(&reply).unwrap().len() < 1 << 20);
        if let Some(saved) = receiver.receive(reply).unwrap() {
            return saved;
        }
    }
    panic!("sync failed to converge")
}

#[test]
fn cancellation_during_snapshot_is_folded_in_before_effects() {
    let mut cache = Cache::default();
    let mut receiver = Receiver::default();
    let mut view = large_view();
    let page = cache
        .answer("worker", 1, &view, receiver.request())
        .unwrap();
    assert!(matches!(page, Reply::Page { total, .. } if total > PAGE_BYTES as u64));
    assert!(receiver.receive(page).unwrap().is_none());
    // Frozen pages still contain Running. No intermediate image may authorize execution.
    view.assignments[0].desired = Desired::Job(JobDesired::Cancelled);
    view.assignments[0].revision = 2;
    let saved = collect(&mut cache, &mut receiver, 2, &view);
    assert_eq!(saved.cursor.revision, 2);
    assert_eq!(
        serde_json::to_value(saved.image.view().unwrap()).unwrap(),
        serde_json::to_value(&view).unwrap()
    );
}

#[test]
fn incremental_delivery_compaction_and_replica_cache_loss_are_recoverable() {
    let mut cache = Cache::default();
    let mut receiver = Receiver::default();
    let mut view = large_view();
    let saved = collect(&mut cache, &mut receiver, 1, &view);
    receiver.commit(saved.clone());
    view.history.insert("job".into(), BTreeMap::new());
    let reply = cache
        .answer("worker", 2, &view, receiver.request())
        .unwrap();
    let Reply::Page {
        ref bytes, total, ..
    } = reply
    else {
        panic!("expected delta")
    };
    assert!(total < 1024, "unchanged large spec must not be resent");
    let batch: Batch = serde_json::from_slice(bytes).unwrap();
    assert_eq!(batch.base.as_ref(), Some(&saved.cursor));
    assert_eq!(batch.changes.len(), 1);
    assert!(receiver.receive(reply).unwrap().is_none());
    let second = collect(&mut cache, &mut receiver, 2, &view);
    receiver.commit(second.clone());
    view.history.clear();
    let removed = collect(&mut cache, &mut receiver, 3, &view);
    assert!(removed.image.view().unwrap().history.is_empty());

    // A restart drops partial progress, never the last accepted image or execution fences.
    let mut restarted = Receiver::new(Some(saved.clone())).unwrap();
    let fresh_replica = &mut Cache::default();
    let page = fresh_replica
        .answer("worker", 4, &view, restarted.request())
        .unwrap();
    // Same complete image needs no transfer even when the replica has no cache.
    assert!(matches!(page, Reply::Current { .. }));
    view.starts_allowed = false;
    let crate::control::workload::Workload::Job { spec, .. } = &mut view.assignments[0].workload
    else {
        unreachable!()
    };
    spec.name.push_str("changed");
    let page = fresh_replica
        .answer("worker", 5, &view, restarted.request())
        .unwrap();
    assert!(restarted.receive(page).unwrap().is_none());
    let another_replica = &mut Cache::default();
    assert!(matches!(
        another_replica
            .answer("worker", 5, &view, restarted.request())
            .unwrap(),
        Reply::Reset
    ));
    let rebuilt = collect(another_replica, &mut restarted, 5, &view);
    assert!(!rebuilt.image.view().unwrap().starts_allowed);

    // Force old-base compaction. It changes bandwidth, never semantics.
    for revision in 6..10 {
        view.acknowledged = revision;
        collect(&mut cache, &mut Receiver::default(), revision, &view);
    }
    let reply = cache
        .answer(
            "worker",
            10,
            &view,
            Request::Poll {
                cursor: Some(second.cursor),
            },
        )
        .unwrap();
    let Reply::Page { total, .. } = reply else {
        panic!("snapshot required")
    };
    assert!(total > PAGE_BYTES as u64);
    let recovered = collect(
        &mut cache,
        &mut Receiver::new(Some(saved)).unwrap(),
        10,
        &view,
    );
    assert_eq!(recovered.image.view().unwrap().acknowledged, 9);
}

#[test]
fn page_integrity_identity_and_cursor_rollback_fail_closed() {
    let view = view();
    let mut cache = Cache::default();
    let page = cache
        .answer("worker", 9, &view, Request::Poll { cursor: None })
        .unwrap();
    let Reply::Page { token, .. } = &page else {
        unreachable!()
    };
    assert!(matches!(
        cache.continuation("other-worker", token, 0),
        Reply::Reset
    ));
    assert!(matches!(
        cache.continuation("worker", token, 1),
        Reply::Reset
    ));
    let mut bad = page.clone();
    if let Reply::Page { bytes, .. } = &mut bad {
        bytes[0] ^= 1;
    }
    assert!(Receiver::default().receive(bad).is_err());
    let mut bad = page;
    if let Reply::Page { offset, .. } = &mut bad {
        *offset = 1;
    }
    assert!(Receiver::default().receive(bad).is_err());
    let saved = collect(&mut cache, &mut Receiver::default(), 9, &view);
    let mut changed = view.clone();
    changed.starts_allowed = false;
    assert!(
        cache
            .answer(
                "worker",
                9,
                &changed,
                Request::Poll {
                    cursor: Some(saved.cursor.clone())
                }
            )
            .is_err()
    );
    assert!(
        cache
            .answer(
                "worker",
                8,
                &view,
                Request::Poll {
                    cursor: Some(saved.cursor.clone())
                }
            )
            .is_err()
    );
    let mut corrupt = saved;
    corrupt.cursor.digest = "wrong".into();
    assert!(Receiver::new(Some(corrupt)).is_err());
}
