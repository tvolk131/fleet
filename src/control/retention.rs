//! Deterministic rolling retention. A verified replacement precedes any deletion intent.
use super::{
    State,
    artifact::{Artifact, CopyPhase, Kind},
    workload::Workload,
};
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct Decision {
    pub artifact: String,
    pub job: Option<String>,
    pub bytes: u64,
    pub reasons: Vec<String>,
    pub delete: bool,
    pub deleted: bool,
    pub copy_problems: Vec<String>,
}

impl State {
    pub fn artifact_protection(&self, a: &Artifact) -> Vec<String> {
        let mut reasons = Vec::new();
        if a.pinned {
            reasons.push("pinned".into());
        }
        if let Kind::Checkpoint(id) = &a.manifest.kind {
            for j in self.jobs.values().filter(|j| !j.phase.terminal()) {
                if j.resume_from.as_ref() == Some(id) {
                    reasons.push(format!("resume dependency: {}", j.id));
                }
                if super::client::latest_checkpoint(self, &j.id).is_some_and(|c| &c.id == id) {
                    reasons.push(format!("latest resumable checkpoint: {}", j.id));
                }
            }
        }
        reasons
    }

    pub fn retention_plan(&self) -> Vec<Decision> {
        self.storage
            .artifacts
            .values()
            .map(|a| {
                let job = match &a.manifest.kind {
                    Kind::Checkpoint(id) => self.checkpoints.get(id).map(|c| c.job.to_string()),
                    Kind::FinalOutputs => None,
                };
                let mut d = Decision {
                    artifact: a.manifest.id.clone(),
                    job: job.clone(),
                    bytes: a.manifest.bytes(),
                    reasons: Vec::new(),
                    delete: false,
                    deleted: a.deleted,
                    copy_problems: a
                        .copies
                        .iter()
                        .filter(|(_, c)| {
                            if a.deleted {
                                c.phase != CopyPhase::Deleted
                            } else {
                                !matches!(c.phase, CopyPhase::Ready | CopyPhase::Deleted)
                            }
                        })
                        .map(|(location, c)| {
                            format!(
                                "{location}: {:?}{}",
                                c.phase,
                                c.detail
                                    .as_ref()
                                    .map(|d| format!(" — {d}"))
                                    .unwrap_or_default()
                            )
                        })
                        .collect(),
                };
                if a.deleted {
                    d.reasons.push(
                        if a.copies.values().all(|c| c.phase == CopyPhase::Deleted) {
                            "payload deletion acknowledged; history retained"
                        } else {
                            "deletion committed; waiting for locations to reclaim bytes"
                        }
                        .into(),
                    );
                    return d;
                }
                d.reasons = self.artifact_protection(a);
                let policy =
                    job.as_ref()
                        .and_then(|id| self.jobs.get(id))
                        .and_then(|j| match &j.workload {
                            Workload::Job { spec, .. } => {
                                spec.checkpoint.as_ref()?.retention.as_ref()
                            }
                            _ => None,
                        });
                let Some(policy) = policy else {
                    d.reasons.push("no rolling checkpoint policy".into());
                    return d;
                };
                let mut siblings: Vec<_> = self
                    .storage
                    .artifacts
                    .values()
                    .filter(|s| {
                        !s.deleted
                            && match &s.manifest.kind {
                                Kind::Checkpoint(id) => self
                                    .checkpoints
                                    .get(id)
                                    .is_some_and(|c| Some(c.job.to_string()) == job),
                                _ => false,
                            }
                    })
                    .collect();
                siblings.sort_by_key(|s| (std::cmp::Reverse(s.ordinal), &s.manifest.id));
                let newest: Vec<_> = siblings.into_iter().take(policy.keep_last).collect();
                if newest.iter().any(|s| s.manifest.id == a.manifest.id) {
                    d.reasons.push("within keep_last".into());
                }
                // A storage outage must not turn a new source-only capture into the only remaining recovery point.
                if newest.iter().any(|s| {
                    !s.copies
                        .values()
                        .any(|c| c.authority == self.cluster && c.phase == CopyPhase::Ready)
                        || s.targets.iter().any(|id| {
                            !s.copies.get(&format!("storage/{id}")).is_some_and(|c| {
                                c.authority == self.cluster && c.phase == CopyPhase::Ready
                            })
                        })
                }) {
                    d.reasons
                        .push("waiting for replacement checkpoint backups".into());
                }
                if !self.artifact_cleanup_allowed(None) {
                    d.reasons.push("cleanup held for recovery review".into());
                }
                d.delete = d.reasons.is_empty();
                if d.delete {
                    d.reasons.push("outside keep_last; safe to retire".into());
                }
                d
            })
            .collect()
    }

    pub(crate) fn apply_rolling_retention(&mut self) {
        for d in self.retention_plan().into_iter().filter(|d| d.delete) {
            let revision = self.storage.artifacts[&d.artifact].revision;
            self.delete_artifact(&d.artifact, revision);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::{
        artifact,
        checkpoint::Checkpoint,
        queue::{JobPhase, Submission},
    };
    use std::collections::{BTreeMap, BTreeSet};
    fn fixture() -> (State, Vec<String>) {
        let mut state = State::new("cluster".into());
        let spec = serde_json::from_value(serde_json::json!({"name":"retention","argv":["train"],"threads":1,
            "checkpoint":{"manifests":"cp/*/fleet.json","resume_argv":["train","{checkpoint}"],
            "retention":{"keep_last":2,"max_live_checkpoints":6,"max_live_bytes":1048576,"transfer_bytes_per_hour":1048576}}})).unwrap();
        let mut job = Submission {
            id: "job".into(),
            workload: Workload::Job {
                spec: Box::new(spec),
                gpu: false,
                memory_mb: 0,
            },
            worker: None,
            priority: 0,
            requires: BTreeSet::new(),
            after: BTreeSet::new(),
        }
        .job(1);
        job.phase = JobPhase::Active;
        state.jobs.insert("job".into(), job);
        state.storage.policy.checkpoints.insert("backup".into());
        let mut ids = Vec::new();
        for n in 1..=32 {
            let content = crate::checkpoint::Captured {
                id: format!("{n:064x}"),
                source: format!("cp/{n}/fleet.json"),
                manifest: crate::checkpoint::Manifest {
                    version: 1,
                    position: Some(1000 - n),
                    files: BTreeMap::new(),
                },
                files: vec![crate::jobs::OutputFile {
                    path: "state".into(),
                    size: 1,
                    hash: iroh_blobs::Hash::new(n.to_string()),
                }],
                captured_at: 1000 - n,
            };
            let cp = Checkpoint {
                id: format!("cp-{n}"),
                job: "job".into(),
                attempt: "attempt".into(),
                worker: "worker".into(),
                parent: None,
                content,
            };
            let m = artifact::Manifest::new(
                "worker".into(),
                "attempt".into(),
                Kind::Checkpoint(cp.id.clone()),
                cp.content.files.clone(),
            )
            .unwrap();
            ids.push(m.id.clone());
            state.checkpoints.insert(cp.id.clone(), cp);
            state.register_artifact(m);
        }
        (state, ids)
    }
    #[test]
    fn rolling_waits_for_backups_preserves_pins_resume_and_outputs_then_retires_by_commit_order() {
        let (mut state, ids) = fixture();
        state.storage.artifacts.get_mut(&ids[0]).unwrap().pinned = true;
        state.jobs.get_mut("job").unwrap().resume_from = Some("cp-2".into());
        let output = artifact::Manifest::new(
            "worker".into(),
            "attempt".into(),
            Kind::FinalOutputs,
            vec![crate::jobs::OutputFile {
                path: "result".into(),
                size: 1,
                hash: iroh_blobs::Hash::new("result"),
            }],
        )
        .unwrap();
        state.register_artifact(output.clone());
        state.apply_rolling_retention();
        assert!(state.storage.artifacts.values().all(|a| !a.deleted));
        assert!(
            state
                .retention_plan()
                .iter()
                .any(|d| d.reasons.iter().any(|r| r.contains("backups")))
        );
        for artifact in state.storage.artifacts.values_mut() {
            for copy in artifact.copies.values_mut() {
                copy.phase = CopyPhase::Ready;
                copy.authority = "cluster".into();
            }
        }
        let recovered: State =
            serde_json::from_slice(&serde_json::to_vec(&state).unwrap()).unwrap();
        assert_eq!(
            serde_json::to_value(state.retention_plan()).unwrap(),
            serde_json::to_value(recovered.retention_plan()).unwrap()
        );
        state.apply_rolling_retention();
        for (i, id) in ids.iter().enumerate() {
            assert_eq!(
                state.storage.artifacts[id].deleted,
                ![0, 1, 30, 31].contains(&i)
            );
        }
        assert!(!state.storage.artifacts[&output.id].deleted);
        state.jobs.get_mut("job").unwrap().phase = JobPhase::Succeeded;
        state.apply_rolling_retention();
        assert!(state.storage.artifacts[&ids[1]].deleted);
        assert!(!state.storage.artifacts[&ids[0]].deleted);
        assert!(!state.storage.artifacts[&output.id].deleted);
    }
    #[test]
    fn no_policy_and_missing_replacement_payloads_never_roll() {
        let (mut state, ids) = fixture();
        state.storage.policy.checkpoints.clear();
        for a in state.storage.artifacts.values_mut() {
            a.targets.clear();
            a.copies.clear();
        }
        state.apply_rolling_retention();
        assert!(state.storage.artifacts.values().all(|a| !a.deleted));
        let Workload::Job { spec, .. } = &mut state.jobs.get_mut("job").unwrap().workload else {
            unreachable!()
        };
        spec.checkpoint.as_mut().unwrap().retention = None;
        state.apply_rolling_retention();
        assert!(ids.iter().all(|id| !state.storage.artifacts[id].deleted));
    }
}
