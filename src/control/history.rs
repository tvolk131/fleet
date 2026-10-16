//! Deterministic archive cursors and retention. Payloads use the artifact data plane.
use super::{
    Reply, State,
    artifact::{CopyPhase, Kind, Manifest},
    workload::Workload,
};
use crate::history::{Policy, Stream};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const MAX_CHUNKS: usize = 1024;
pub const MAX_PENDING: usize = 4096;

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Progress {
    pub captured_end: u64,
    pub revision: u64,
    pub error: Option<String>,
    pub sources: BTreeMap<String, crate::history::jsonl::Cursor>,
    /// End of the latest accepted chunk. Earlier publications cannot be resurrected after compaction.
    pub published: u64,
    pub observed: u64,
    pub dropped: u64,
    pub closed: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Settings {
    pub policy: Policy,
    pub revision: u64,
    pub completed_at: Option<u64>,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Catalog {
    pub attempts: BTreeMap<String, BTreeMap<Stream, Progress>>,
    pub jobs: BTreeMap<String, Settings>,
    pub swept_at: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Report {
    pub captured_end: u64,
    pub revision: u64,
    pub error: Option<String>,
    pub sources: BTreeMap<String, crate::history::jsonl::Cursor>,
    pub worker: String,
    pub incarnation: String,
    pub attempt: String,
    pub stream: Stream,
    pub observed: u64,
    pub dropped: u64,
    pub closed: bool,
}
impl State {
    pub fn history_job(&self, attempt: &str) -> Option<&str> {
        Some(
            self.assignments
                .get(attempt)?
                .attempt
                .as_ref()?
                .job
                .as_ref(),
        )
    }
    pub fn history_settings(&self, job: &str) -> Option<Settings> {
        if let Some(s) = self.history.jobs.get(job) {
            return Some(s.clone());
        }
        let Workload::Job { spec, .. } = &self.jobs.get(job)?.workload else {
            return None;
        };
        Some(Settings {
            policy: spec.history.clone(),
            revision: 0,
            completed_at: None,
        })
    }
    pub fn history_policy(&mut self, job: &str, revision: u64, policy: &Policy) -> Reply {
        let Some(mut settings) = self.history_settings(job) else {
            return Reply::rejected("unknown history job");
        };
        if settings.revision != revision {
            return Reply::rejected("history retention revision conflict");
        }
        if let Err(e) = policy.validate() {
            return Reply::rejected(&e.to_string());
        }
        settings.policy = policy.clone();
        settings.revision += 1;
        self.history.jobs.insert(job.into(), settings);
        Reply::Recorded
    }
    pub fn history_reporter(&self, principal: &str, r: &Report) -> bool {
        self.workers.get(&r.worker).is_some_and(|w| {
            w.peer.identity.to_string() == principal && w.incarnation == r.incarnation
        }) && self
            .assignments
            .get(&r.attempt)
            .is_some_and(|a| a.worker == r.worker && a.attempt.is_some())
            && !self.worker_recovery_required(&r.worker)
    }
    pub fn history_report(&mut self, principal: &str, r: &Report) -> Reply {
        if r.error.as_ref().is_some_and(|e| e.len() > 2048)
            || r.sources.len() > 4
            || r.sources
                .iter()
                .any(|(path, c)| path.len() > 512 || c.identity.len() > 128)
        {
            return Reply::rejected("history report exceeds metadata limits");
        }
        if !self.history_reporter(principal, r) {
            return Reply::rejected(
                "history reporter identity/attempt mismatch or recovery pending",
            );
        }
        let old = self
            .history
            .attempts
            .get(&r.attempt)
            .and_then(|s| s.get(&r.stream))
            .cloned()
            .unwrap_or_default();
        if r.captured_end > r.observed {
            return Reply::rejected("captured history exceeds observed output");
        }
        if r.revision < old.revision {
            return Reply::Recorded;
        }
        if r.revision == old.revision {
            return if r.observed == old.observed
                && r.dropped == old.dropped
                && r.closed == old.closed
                && r.error == old.error
                && r.sources == old.sources
                && r.captured_end == old.captured_end
            {
                Reply::Recorded
            } else {
                Reply::rejected("history capture revision reused with different contents")
            };
        }
        if r.observed < old.observed
            || r.captured_end < old.captured_end
            || r.dropped < old.dropped
            || r.dropped > r.observed
            || (old.closed && (!r.closed || old.observed != r.observed))
        {
            return Reply::rejected("history capture cursor rollback or closed stream changed");
        }
        self.history
            .attempts
            .entry(r.attempt.clone())
            .or_default()
            .insert(
                r.stream,
                Progress {
                    captured_end: r.captured_end,
                    revision: r.revision,
                    error: r.error.clone(),
                    sources: r.sources.clone(),
                    published: old.published,
                    observed: r.observed,
                    dropped: r.dropped,
                    closed: r.closed,
                },
            );
        Reply::Recorded
    }
    pub(crate) fn validate_history_chunk(&self, m: &Manifest) -> Result<()> {
        let Kind::History { stream, start, end } = m.kind else {
            return Ok(());
        };
        let attempt = m.attempt().context("history needs an attempt")?;
        ensure!(
            end > start && end - start <= crate::history::BLOCK_BYTES,
            "invalid history chunk range"
        );
        ensure!(
            m.files.len() == 1 && m.files[0].path == "data" && m.files[0].size == end - start,
            "history chunk must contain exact range bytes"
        );
        let published = self
            .history
            .attempts
            .get(attempt)
            .and_then(|s| s.get(&stream))
            .map_or(0, |p| p.published);
        if end <= published {
            return Ok(());
        } // retired/duplicate publications never resurrect.
        ensure!(start >= published, "overlapping history publication");
        let job = self
            .history_job(attempt)
            .context("history has no logical job")?;
        let settings = self
            .history_settings(job)
            .context("history settings missing")?;
        let metrics = stream == Stream::Metrics;
        let pinned:u64=self.storage.artifacts.values().filter(|a|a.pinned && !a.deleted && matches!(a.manifest.kind,Kind::History{stream:s,..} if (s==Stream::Metrics)==metrics) && a.manifest.attempt().and_then(|a|self.history_job(a))==Some(job)).map(|a|a.manifest.bytes()).sum();
        ensure!(
            pinned.saturating_add(m.bytes())
                <= if metrics {
                    settings.policy.metric_bytes
                } else {
                    settings.policy.log_bytes
                },
            "history budget occupied by pinned chunks"
        );
        ensure!(
            self.storage
                .artifacts
                .values()
                .filter(|a| matches!(a.manifest.kind, Kind::History { .. })
                    && a.manifest.attempt().and_then(|a| self.history_job(a)) == Some(job))
                .count()
                < MAX_PENDING,
            "history catalog full pending retention/cleanup"
        );
        Ok(())
    }
    /// Return false for a duplicate already represented by a compacted stream cursor.
    pub(crate) fn accept_history_chunk(&mut self, m: &Manifest) -> bool {
        let Kind::History {
            stream,
            start: _,
            end,
        } = m.kind
        else {
            return true;
        };
        let p = self
            .history
            .attempts
            .entry(m.attempt().unwrap().into())
            .or_default()
            .entry(stream)
            .or_default();
        if end <= p.published {
            return false;
        }
        p.published = end;
        true
    }
    pub fn sweep_history(&mut self, as_of: u64) -> Reply {
        self.history.swept_at = self.history.swept_at.max(as_of);
        for (id, job) in &self.jobs {
            if job.phase.terminal() {
                let settings = self.history_settings(id).expect("job settings");
                self.history
                    .jobs
                    .entry(id.clone())
                    .or_insert(settings)
                    .completed_at
                    .get_or_insert(self.history.swept_at);
            }
        }
        self.apply_history_retention();
        Reply::Recorded
    }
    pub(crate) fn apply_history_retention(&mut self) {
        if !self.artifact_cleanup_allowed(None) {
            return;
        }
        let mut remove = Vec::new();
        for job in self.jobs.keys() {
            let settings = self.history_settings(job).expect("job settings");
            for metrics in [false, true] {
                let mut chunks: Vec<_> = self.storage.artifacts.values().filter(|a| !a.deleted && matches!(a.manifest.kind,Kind::History{stream,..} if (stream==Stream::Metrics)==metrics) && a.manifest.attempt().and_then(|a|self.history_job(a)) == Some(job)).collect();
                chunks.sort_by_key(|a| a.ordinal);
                let budget = if metrics {
                    settings.policy.metric_bytes
                } else {
                    settings.policy.log_bytes
                };
                let days = if metrics {
                    settings.policy.metric_days
                } else {
                    settings.policy.log_days
                };
                let expired = settings.completed_at.zip(days).is_some_and(|(t, d)| {
                    self.history.swept_at >= t.saturating_add(d as u64 * 86400)
                });
                let mut bytes: u64 = chunks.iter().map(|a| a.manifest.bytes()).sum();
                let mut count = chunks.len();
                for a in chunks {
                    if a.pinned {
                        continue;
                    }
                    if expired || bytes > budget || count > MAX_CHUNKS {
                        remove.push((a.manifest.id.clone(), a.revision));
                        bytes -= a.manifest.bytes();
                        count -= 1;
                    }
                }
            }
        }
        for (id, revision) in remove {
            self.delete_artifact(&id, revision);
        }
        // Stream publication cursors reject replay after per-chunk catalog records are gone.
        // Storage/worker local tombstones remain authoritative across disaster recovery.
        let cluster = &self.cluster;
        self.storage.artifacts.retain(|_, a| {
            !(matches!(a.manifest.kind, Kind::History { .. })
                && a.deleted
                && a.copies
                    .values()
                    .all(|c| c.authority == *cluster && c.phase == CopyPhase::Deleted))
        });
    }
}
