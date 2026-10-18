use super::*;
use crate::control::{Command, Mutation, Request};

fn fixture() -> (State, EndpointId, Report) {
    let mut state = State::new("test".into());
    state.version.active = crate::control::tunnels::GENERATION;
    state.tunnels.revision = 2;
    let peer = iroh::SecretKey::from_bytes(&[42; 32]).public();
    state.tunnels.known_identities.insert(peer, "a".into());
    let report = Report {
        adapter: "a".into(),
        boot: 1,
        sequence: 1,
        cached_revision: 2,
        transport: TransportStatus::Applied { revision: 2 },
    };
    (state, peer, report)
}
#[test]
fn observations_are_bounded_idempotent_authenticated_and_monotonic() {
    let (mut state, peer, report) = fixture();
    let command = Command {
        principal: peer.to_string(),
        request: Request {
            origin: "test".into(),
            id: "observe".into(),
            mutation: Mutation::TunnelReport {
                report: report.clone(),
                recorded_at_ms: 100,
            },
        },
    };
    assert_eq!(state.apply(command.clone(), 1), Reply::Recorded);
    assert_eq!(state.apply(command, 2), Reply::Recorded);
    assert!(state.receipts.is_empty());
    let saved = state.tunnels.observations[&peer].clone();
    assert_eq!(
        state.tunnel_report(&peer.to_string(), &report, 200),
        Reply::Recorded
    );
    assert_eq!(state.tunnels.observations[&peer], saved);
    let mut wrong = report.clone();
    wrong.adapter = "b".into();
    assert!(matches!(
        state.tunnel_report(&peer.to_string(), &wrong, 200),
        Reply::Rejected { .. }
    ));
    assert!(matches!(
        state.tunnel_report("admin", &report, 200),
        Reply::Rejected { .. }
    ));
    wrong = report.clone();
    wrong.transport = TransportStatus::Starting;
    assert!(matches!(
        state.tunnel_report(&peer.to_string(), &wrong, 200),
        Reply::Rejected { .. }
    ));
    wrong.sequence = 2;
    wrong.cached_revision = 1;
    assert!(matches!(
        state.tunnel_report(&peer.to_string(), &wrong, 200),
        Reply::Rejected { .. }
    ));
    wrong.cached_revision = 3;
    assert!(matches!(
        state.tunnel_report(&peer.to_string(), &wrong, 200),
        Reply::Rejected { .. }
    ));
    wrong.cached_revision = 2;
    wrong.transport = TransportStatus::Failed {
        revision: 2,
        error: "x".repeat(1025),
    };
    assert!(matches!(
        state.tunnel_report(&peer.to_string(), &wrong, 200),
        Reply::Rejected { .. }
    ));
    let mut newer = report.clone();
    newer.boot = 2;
    newer.transport = TransportStatus::Starting;
    assert_eq!(
        state.tunnel_report(&peer.to_string(), &newer, 300),
        Reply::Recorded
    );
    assert!(state.tunnels.observations[&peer].last_applied.is_none());
    assert!(matches!(
        state.tunnel_report(&peer.to_string(), &report, 400),
        Reply::Rejected { .. }
    ));
    newer.sequence = 2;
    newer.transport = TransportStatus::Applied { revision: 2 };
    assert_eq!(
        state.tunnel_report(&peer.to_string(), &newer, 500),
        Reply::Recorded
    );
    newer.sequence = 3;
    newer.transport = TransportStatus::Failed {
        revision: 2,
        error: "bind failed".into(),
    };
    assert_eq!(
        state.tunnel_report(&peer.to_string(), &newer, 600),
        Reply::Recorded
    );
    assert_eq!(
        state.tunnels.observations[&peer]
            .last_applied
            .as_ref()
            .unwrap()
            .recorded_at_ms,
        500
    );
    assert_eq!(state.tunnels.observations.len(), 1);
}

#[test]
fn history_survives_serialization_but_freshness_never_survives_term_restart_or_age() {
    let (mut state, peer, report) = fixture();
    assert_eq!(
        state.tunnel_report(&peer.to_string(), &report, 100),
        Reply::Recorded
    );
    let presence = Presence::default();
    presence.lock().unwrap().insert(
        peer,
        Contact {
            term: 7,
            boot: 1,
            sequence: 1,
            at: Instant::now(),
        },
    );
    assert!(state.tunnel_status(&presence, 7).all_confirmed);
    let mut delayed = state.tunnel_status(&presence, 7);
    delayed.elapse(FRESH_FOR + Duration::from_secs(1));
    assert!(!delayed.all_confirmed);
    assert_eq!(delayed.adapters[0].state, "unknown");
    assert!(!state.tunnel_status(&presence, 8).all_confirmed);
    let restored: State = serde_json::from_slice(&serde_json::to_vec(&state).unwrap()).unwrap();
    let snapshot = restored.tunnel_status(&Presence::default(), 7);
    assert_eq!(snapshot.adapters[0].state, "unknown");
    assert!(
        snapshot.adapters[0]
            .observation
            .as_ref()
            .unwrap()
            .last_applied
            .is_some()
    );
    // Wall-clock timestamps never determine freshness.
    state
        .tunnels
        .observations
        .get_mut(&peer)
        .unwrap()
        .recorded_at_ms = u64::MAX;
    presence.lock().unwrap().get_mut(&peer).unwrap().at =
        Instant::now() - FRESH_FOR - Duration::from_secs(1);
    assert!(!state.tunnel_status(&presence, 7).all_confirmed);
    presence.lock().unwrap().get_mut(&peer).unwrap().at = Instant::now();
    state.tunnels.revision = 3;
    assert_eq!(
        state.tunnel_status(&presence, 7).adapters[0].state,
        "pending"
    );
    let mut next = report;
    next.boot = 2;
    next.transport = TransportStatus::Starting;
    assert_eq!(
        state.tunnel_report(&peer.to_string(), &next, 200),
        Reply::Recorded
    );
    assert!(!state.tunnel_status(&presence, 7).adapters[0].fresh);
}
