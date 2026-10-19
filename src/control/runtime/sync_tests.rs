use super::*;
use crate::control::worker_sync::{self, Cache, Receiver};

#[test]
fn cursor_and_intent_commit_atomically_and_diagnostics_are_bounded_before_journaling() {
    let home = std::env::temp_dir().join(format!(
        "fleet-sync-journal-{:032x}",
        rand::random::<u128>()
    ));
    let view = worker_sync::tests::view();
    let config = WorkerConfig {
        cluster: "sync-test".into(),
        registration: view.registration.clone(),
        controllers: BTreeMap::new(),
        administrators: BTreeSet::new(),
        supervisor: "/fixture/fleet".into(),
    };
    let journal = Journal::open(&home, &config, true).unwrap();
    let mut cache = Cache::default();
    let saved = worker_sync::tests::collect(&mut cache, &mut Receiver::default(), 10, &view);
    journal.accept(view.clone(), Some(&saved)).unwrap();
    journal.mark_attempted(false, "exec-job", true).unwrap();
    journal.begin_sync().unwrap();
    let observation = Observation {
        phase: WorkPhase::Job(AttemptPhase::Running),
        detail: Some("界😀".repeat(2000)),
    };
    journal
        .observe(&view.assignments[0], observation.clone())
        .unwrap();
    let state = journal.read().unwrap();
    assert_eq!(state.sequence, 1);
    assert!(
        journal
            .observe(
                &view.assignments[0],
                Observation {
                    phase: WorkPhase::Instance(InstancePhase::Deleted),
                    detail: None
                }
            )
            .is_err()
    );
    assert_eq!(journal.read().unwrap().sequence, 1);
    assert!(state.outbox[&1].observation.detail.as_ref().unwrap().len() <= MAX_OBSERVATION_DETAIL);
    drop(journal);
    let journal = Journal::open(&home, &config, false).unwrap();
    assert_eq!(journal.read_sync().unwrap().unwrap().cursor, saved.cursor);
    assert!(journal.attempted(false, "exec-job").unwrap());
    assert!(journal.sync_pending().unwrap());
    let mut receiver = Receiver::new(journal.read_sync().unwrap()).unwrap();
    receiver.require_refresh();
    assert!(receiver.pending());
    assert!(matches!(
        receiver.request(),
        worker_sync::Request::Poll { cursor: None }
    ));
    journal.observe(&view.assignments[0], observation).unwrap();
    assert_eq!(
        journal.read().unwrap().sequence,
        1,
        "normalization precedes change detection"
    );

    let mut invalid = view.clone();
    invalid.generation = 0;
    let invalid_saved =
        worker_sync::tests::collect(&mut cache, &mut Receiver::default(), 11, &invalid);
    assert!(journal.accept(invalid, Some(&invalid_saved)).is_err());
    assert_eq!(journal.read_sync().unwrap().unwrap().cursor, saved.cursor);
    assert_eq!(journal.read().unwrap().generation, 1);
    assert!(
        journal.sync_pending().unwrap(),
        "failed acceptance cannot clear the pending fence"
    );
    journal.accept(view.clone(), Some(&saved)).unwrap();
    assert!(!journal.sync_pending().unwrap());
    // Emergency authority rebind discards only the delivery cursor, preserving execution fences.
    journal
        .change_with_sync(
            |state| {
                state.starts_allowed = false;
                Ok(())
            },
            Some(None),
        )
        .unwrap();
    assert!(journal.read_sync().unwrap().is_none());
    assert!(journal.attempted(false, "exec-job").unwrap());
    drop(journal);
    std::fs::remove_dir_all(home).unwrap();
}
