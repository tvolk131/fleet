//! Durable application evidence and deliberately ephemeral, leader-local freshness.
use super::{State, Topology};
use crate::control::Reply;
use iroh::EndpointId;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

pub const FRESH_FOR: Duration = Duration::from_secs(15);

#[cfg(test)]
mod tests;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "phase", rename_all = "snake_case")]
pub enum TransportStatus {
    Starting,
    Applying { revision: u64 },
    Applied { revision: u64 },
    Failed { revision: u64, error: String },
}
impl TransportStatus {
    pub fn revision(&self) -> Option<u64> {
        match self {
            Self::Starting => None,
            Self::Applying { revision }
            | Self::Applied { revision }
            | Self::Failed { revision, .. } => Some(*revision),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Report {
    pub adapter: String,
    /// Fsynced before starting transport; a lower boot can never replace a newer one.
    pub boot: u64,
    pub sequence: u64,
    pub cached_revision: u64,
    pub transport: TransportStatus,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Applied {
    pub boot: u64,
    pub revision: u64,
    pub recorded_at_ms: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Observation {
    pub report: Report,
    /// Controller receive time is display evidence, never a freshness clock.
    pub recorded_at_ms: u64,
    pub last_applied: Option<Applied>,
}

#[derive(Clone)]
pub struct Contact {
    pub term: u64,
    pub boot: u64,
    pub sequence: u64,
    pub at: Instant,
}
pub type Presence = Arc<Mutex<BTreeMap<EndpointId, Contact>>>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Row {
    pub adapter: String,
    pub identity: EndpointId,
    pub retired: bool,
    pub desired_revision: u64,
    pub observation: Option<Observation>,
    pub last_contact_age_ms: Option<u64>,
    pub fresh: bool,
    /// Applied means a recent authenticated report, not hardware attestation.
    pub state: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    pub revision: u64,
    pub topology: Topology,
    pub known_identities: BTreeMap<EndpointId, String>,
    pub adapters: Vec<Row>,
    pub fresh_for_ms: u64,
    pub all_confirmed: bool,
}
impl Snapshot {
    /// Conservatively account for response transit time on the client. A delayed
    /// response cannot turn old contact into a fresh enforcement confirmation.
    pub(crate) fn elapse(&mut self, elapsed: Duration) {
        for row in &mut self.adapters {
            row.last_contact_age_ms = row
                .last_contact_age_ms
                .map(|age| age.saturating_add(elapsed.as_millis().min(u64::MAX as u128) as u64));
            row.fresh &= row
                .last_contact_age_ms
                .is_some_and(|age| age <= self.fresh_for_ms);
            if !row.fresh {
                row.state = "unknown".into();
            }
        }
        self.all_confirmed = self.adapters.iter().all(|r| r.state == "applied");
    }
}
impl State {
    pub fn tunnel_report_allowed(&self, peer: EndpointId, report: &Report) -> bool {
        self.tunnels.known_identities.get(&peer) == Some(&report.adapter)
    }
    pub(crate) fn tunnel_report(&mut self, principal: &str, report: &Report, at: u64) -> Reply {
        let Ok(peer) = principal.parse::<EndpointId>() else {
            return Reply::rejected("invalid adapter identity");
        };
        if !self.tunnel_report_allowed(peer, report)
            || report.boot == 0
            || report.sequence == 0
            || report.cached_revision > self.tunnels.revision
            || report
                .transport
                .revision()
                .is_some_and(|r| r > report.cached_revision)
            || matches!(&report.transport, TransportStatus::Failed {error,..} if error.len() > 1024 || serde_json::to_vec(error).map_or(true,|bytes|bytes.len()>1024))
        {
            return Reply::rejected("invalid tunnel observation");
        }
        let old = self.tunnels.observations.get(&peer);
        if let Some(old) = old {
            if (report.boot, report.sequence) < (old.report.boot, old.report.sequence) {
                return Reply::rejected("stale adapter boot or observation");
            }
            if (report.boot, report.sequence) == (old.report.boot, old.report.sequence) {
                return if report == &old.report {
                    Reply::Recorded
                } else {
                    Reply::rejected("observation sequence reused")
                };
            }
            if report.cached_revision < old.report.cached_revision {
                return Reply::rejected("cached policy rollback");
            }
        }
        let last_applied = if let TransportStatus::Applied { revision } = report.transport {
            Some(Applied {
                boot: report.boot,
                revision,
                recorded_at_ms: at,
            })
        } else {
            old.filter(|o| o.report.boot == report.boot)
                .and_then(|o| o.last_applied.clone())
        };
        self.tunnels.observations.insert(
            peer,
            Observation {
                report: report.clone(),
                recorded_at_ms: at,
                last_applied,
            },
        );
        Reply::Recorded
    }
    pub fn tunnel_status(&self, presence: &Presence, term: u64) -> Snapshot {
        let contacts = presence.lock().expect("tunnel presence");
        let adapters: Vec<_> = self
            .tunnels
            .known_identities
            .iter()
            .map(|(identity, adapter)| {
                let observation = self.tunnels.observations.get(identity).cloned();
                let age = contacts
                    .get(identity)
                    .filter(|c| {
                        c.term == term
                            && observation.as_ref().is_some_and(|o| {
                                (o.report.boot, o.report.sequence) == (c.boot, c.sequence)
                            })
                    })
                    .map(|c| c.at.elapsed());
                let fresh = age.is_some_and(|a| a <= FRESH_FOR);
                let state = match observation.as_ref().map(|o| &o.report.transport) {
                    _ if !fresh => "unknown",
                    Some(TransportStatus::Applied { revision })
                        if *revision == self.tunnels.revision =>
                    {
                        "applied"
                    }
                    Some(TransportStatus::Failed { .. }) => "failed",
                    _ => "pending",
                }
                .to_string();
                Row {
                    adapter: adapter.clone(),
                    identity: *identity,
                    retired: self
                        .tunnels
                        .topology
                        .adapters
                        .get(adapter)
                        .is_none_or(|a| a.endpoint.id != *identity),
                    desired_revision: self.tunnels.revision,
                    observation,
                    last_contact_age_ms: age.map(|a| a.as_millis().min(u64::MAX as u128) as u64),
                    fresh,
                    state,
                }
            })
            .collect();
        Snapshot {
            revision: self.tunnels.revision,
            topology: self.tunnels.topology.clone(),
            known_identities: self.tunnels.known_identities.clone(),
            all_confirmed: adapters.iter().all(|r| r.state == "applied"),
            adapters,
            fresh_for_ms: FRESH_FOR.as_millis() as u64,
        }
    }
}
