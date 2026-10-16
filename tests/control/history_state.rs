use super::*;
use fleet::{
    control::{
        artifact::{self, CopyPhase, Kind, Manifest},
        history::Report as HistoryReport,
        queue::JobPhase,
    },
    history::{BLOCK_BYTES, Policy, Stream},
};

fn setup() -> (State, String) {
    let mut s = queue_tests::state();
    s.enqueue(&queue_tests::queued("history"), 1);
    s.place_queued("history", "w", 1);
    let principal = s.workers["w"].peer.identity.to_string();
    (s, principal)
}
fn chunk(attempt: &str, start: u64, size: u64) -> Manifest {
    Manifest::new(
        "w".into(),
        attempt.into(),
        Kind::History {
            stream: Stream::Stdout,
            start,
            end: start + size,
        },
        vec![fleet::jobs::OutputFile {
            path: "data".into(),
            size,
            hash: iroh_blobs::Hash::new(format!("{attempt}/{start}").as_bytes()),
        }],
    )
    .unwrap()
}
fn apply(s: &mut State, principal: &str, id: &str, mutation: Mutation) -> Reply {
    s.apply(
        Command {
            principal: principal.into(),
            request: Request {
                origin: s.cluster.clone(),
                id: id.into(),
                mutation,
            },
        },
        10,
    )
}
fn publish(s: &mut State, principal: &str, m: &Manifest) -> Reply {
    apply(
        s,
        principal,
        &m.id,
        Mutation::ArtifactReady(artifact::Ready {
            incarnation: "one".into(),
            manifest: m.clone(),
        }),
    )
}
#[test]
fn retention_spans_attempts_honors_pins_and_retry_after_pressure_and_compacts_without_resurrection()
{
    let (mut s, p) = setup();
    let policy = Policy {
        log_bytes: BLOCK_BYTES,
        ..Default::default()
    };
    assert_eq!(s.history_policy("history", 0, &policy), Reply::Recorded);
    assert!(matches!(
        s.history_policy("history", 0, &policy),
        Reply::Rejected { .. }
    ));
    let first = chunk("history-a1", 0, BLOCK_BYTES);
    assert_eq!(publish(&mut s, &p, &first), Reply::Recorded);
    let revision = s.storage.artifacts[&first.id].revision;
    assert_eq!(
        s.artifact_retention(&first.id, revision, true, None),
        Reply::Recorded
    );
    let next = chunk("history-a1", BLOCK_BYTES, BLOCK_BYTES);
    assert!(matches!(publish(&mut s, &p, &next), Reply::Rejected { .. }));
    assert_eq!(
        s.receipts.len(),
        0,
        "internal chunks do not accumulate receipts"
    );
    let revision = s.storage.artifacts[&first.id].revision;
    s.artifact_retention(&first.id, revision, false, None);
    assert_eq!(
        publish(&mut s, &p, &next),
        Reply::Recorded,
        "same publication retries after unpin"
    );
    assert!(s.storage.artifacts[&first.id].deleted);
    assert!(!s.storage.artifacts[&next.id].deleted);
    // A later attempt shares the same logical job budget, while offsets restart at zero.
    let mut a2 = s.assignments["history-a1"].clone();
    a2.id = "history-a2".into();
    a2.execution = "history-execution2".into();
    a2.attempt.as_mut().unwrap().number = 2;
    s.assignments.insert(a2.id.clone(), a2);
    s.jobs
        .get_mut("history")
        .unwrap()
        .attempts
        .push("history-a2".into());
    let later = chunk("history-a2", 0, BLOCK_BYTES);
    assert_eq!(publish(&mut s, &p, &later), Reply::Recorded);
    assert!(s.storage.artifacts[&next.id].deleted);
    assert!(!s.storage.artifacts[&later.id].deleted);
    let a = s.storage.artifacts[&first.id].clone();
    let report = artifact::Report {
        location: "worker/w".into(),
        incarnation: "one".into(),
        artifact: first.id.clone(),
        revision: a.revision,
        expected_version: a.copies["worker/w"].version,
        phase: CopyPhase::Deleted,
        detail: None,
    };
    assert_eq!(
        apply(
            &mut s,
            &p,
            "history-copy-retire",
            Mutation::ArtifactReport(report)
        ),
        Reply::Recorded
    );
    assert!(!s.storage.artifacts.contains_key(&first.id));
    assert_eq!(publish(&mut s, &p, &first), Reply::Recorded);
    assert!(
        !s.storage.artifacts.contains_key(&first.id),
        "retired chunk cannot resurrect"
    );
    assert_eq!(s.receipts.len(), 0);
    // Suspended jobs don't age out; age starts at logical completion, not the attempt end.
    s.jobs.get_mut("history").unwrap().phase = JobPhase::Suspended;
    s.sweep_history(100 * 86400);
    assert_eq!(s.history_settings("history").unwrap().completed_at, None);
    assert!(!s.storage.artifacts[&later.id].deleted);
    s.jobs.get_mut("history").unwrap().phase = JobPhase::Succeeded;
    s.sweep_history(101 * 86400);
    s.sweep_history(132 * 86400);
    assert!(s.storage.artifacts[&later.id].deleted);
    assert_eq!(s.jobs["history"].attempts.len(), 2);
}
#[test]
fn capture_reports_are_authenticated_monotonic_and_bounded_without_per_sample_receipts() {
    let (mut s, p) = setup();
    let mut r = HistoryReport {
        captured_end: 6,
        revision: 1,
        error: None,
        sources: BTreeMap::new(),
        worker: "w".into(),
        incarnation: "one".into(),
        attempt: "history-a1".into(),
        stream: Stream::Stdout,
        observed: 10,
        dropped: 4,
        closed: false,
    };
    assert!(matches!(
        s.history_report("impostor", &r),
        Reply::Rejected { .. }
    ));
    for revision in 1..=100 {
        r.revision = revision;
        r.error = Some(format!("temporary error {revision}"));
        assert_eq!(
            apply(
                &mut s,
                &p,
                &format!("sample-{revision}"),
                Mutation::HistoryReport(r.clone())
            ),
            Reply::Recorded
        );
    }
    assert!(s.receipts.is_empty());
    let original = r.clone();
    r.revision = 1;
    r.observed = 1;
    r.captured_end = 1;
    r.dropped = 0;
    assert_eq!(
        s.history_report(&p, &r),
        Reply::Recorded,
        "old report is harmless"
    );
    assert_eq!(
        s.history.attempts["history-a1"][&Stream::Stdout].observed,
        10
    );
    r = original.clone();
    r.closed = true;
    assert!(
        matches!(s.history_report(&p, &r), Reply::Rejected { .. }),
        "same revision cannot change"
    );
    r.revision += 1;
    assert_eq!(s.history_report(&p, &r), Reply::Recorded);
    r.revision += 1;
    r.observed += 1;
    assert!(
        matches!(s.history_report(&p, &r), Reply::Rejected { .. }),
        "closed stream cannot grow"
    );
    let valid = Policy::default();
    assert_eq!(
        apply(
            &mut s,
            "admin",
            "user-policy",
            Mutation::HistoryPolicy {
                job: "history".into(),
                expected_revision: 0,
                policy: valid.clone()
            }
        ),
        Reply::Recorded
    );
    assert_eq!(
        s.receipts.len(),
        1,
        "user requests still have durable receipts"
    );
    assert_eq!(
        apply(
            &mut s,
            "admin",
            "user-policy",
            Mutation::HistoryPolicy {
                job: "history".into(),
                expected_revision: 0,
                policy: valid
            }
        ),
        Reply::Recorded
    );
}
