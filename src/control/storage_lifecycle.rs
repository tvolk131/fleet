//! Storage identity lifecycle and copy replacement. All decisions here are deterministic.
use super::{
    Reply, State,
    artifact::{Artifact, Copy, CopyPhase, Registration},
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    #[default]
    Active,
    Draining,
    Cleaning,
    Retired,
    Lost,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Lifecycle {
    pub revision: u64,
    pub phase: Phase,
    pub replacement: Option<String>,
    /// Exact inventory acknowledged when the identity was declared lost, not deletion receipts.
    pub loss: Option<Loss>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Loss {
    pub inventory: String,
    pub artifacts: BTreeSet<String>,
    pub unavailable: BTreeSet<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Plan {
    pub storage: String,
    pub lifecycle: Lifecycle,
    pub inventory: String,
    pub retained_artifacts: usize,
    pub retained_bytes: u64,
    pub awaiting_replacement: BTreeSet<String>,
    pub awaiting_cleanup: BTreeSet<String>,
    /// No other current-authority Ready copy is recorded. This does not infer host liveness.
    pub unavailable_without_agent: BTreeSet<String>,
    pub protected_artifacts: BTreeSet<String>,
    pub cleanup_allowed: bool,
}

impl State {
    pub fn storage_lifecycle(&self, id: &str) -> Lifecycle {
        self.storage.lifecycle.get(id).cloned().unwrap_or_default()
    }
    pub fn storage_active(&self, id: &str) -> bool {
        self.storage.stores.contains_key(id) && self.storage_lifecycle(id).phase == Phase::Active
    }
    pub fn storage_readable(&self, id: &str) -> bool {
        self.storage.stores.contains_key(id)
            && matches!(
                self.storage_lifecycle(id).phase,
                Phase::Active | Phase::Draining
            )
    }
    pub fn storage_enrolled(&self, id: &str) -> bool {
        self.storage.stores.contains_key(id)
            && !matches!(
                self.storage_lifecycle(id).phase,
                Phase::Retired | Phase::Lost
            )
    }
    pub fn readable_stores(&self) -> BTreeMap<String, Registration> {
        self.storage
            .stores
            .iter()
            .filter(|(id, _)| self.storage_readable(id))
            .map(|(id, r)| (id.clone(), r.clone()))
            .collect()
    }
    fn storage_affected<'a>(&'a self, id: &str) -> impl Iterator<Item = &'a Artifact> {
        let location = format!("storage/{id}");
        self.storage
            .artifacts
            .values()
            .filter(move |a| a.copies.contains_key(&location))
    }
    pub fn storage_plan(&self, id: &str) -> Option<Plan> {
        let registration = self.storage.stores.get(id)?;
        let lifecycle = self.storage_lifecycle(id);
        let affected: Vec<_> = self.storage_affected(id).collect();
        let inventory = blake3::hash(
            &serde_json::to_vec(&(
                &self.cluster,
                registration,
                &lifecycle,
                &self.storage.policy,
                &affected,
                self.artifact_cleanup_allowed(Some(&format!("storage/{id}"))),
            ))
            .expect("storage inventory serializes"),
        )
        .to_hex()
        .to_string();
        let mut plan = Plan {
            storage: id.into(),
            lifecycle: lifecycle.clone(),
            inventory,
            retained_artifacts: 0,
            retained_bytes: 0,
            awaiting_replacement: BTreeSet::new(),
            awaiting_cleanup: BTreeSet::new(),
            unavailable_without_agent: BTreeSet::new(),
            protected_artifacts: BTreeSet::new(),
            cleanup_allowed: self.artifact_cleanup_allowed(Some(&format!("storage/{id}"))),
        };
        for a in affected {
            let identity = &a.manifest.id;
            let copy = &a.copies[&format!("storage/{id}")];
            if copy.phase != CopyPhase::Deleted || copy.authority != self.cluster {
                plan.awaiting_cleanup.insert(identity.clone());
            }
            if a.deleted {
                continue;
            }
            plan.retained_artifacts += 1;
            plan.retained_bytes += a.manifest.bytes();
            if self.artifact_protected(identity) {
                plan.protected_artifacts.insert(identity.clone());
            }
            if !lifecycle.replacement.as_ref().is_some_and(|target| {
                self.storage_readable(target)
                    && a.targets.contains(target)
                    && a.copies
                        .get(&format!("storage/{target}"))
                        .is_some_and(|c| c.phase == CopyPhase::Ready && c.authority == self.cluster)
            }) {
                plan.awaiting_replacement.insert(identity.clone());
            }
            let survivor = a.copies.iter().any(|(location, c)| {
                location != &format!("storage/{id}")
                    && c.phase == CopyPhase::Ready
                    && c.authority == self.cluster
                    && location
                        .strip_prefix("storage/")
                        .is_none_or(|s| self.storage_readable(s))
            });
            if !survivor {
                plan.unavailable_without_agent.insert(identity.clone());
            }
        }
        Some(plan)
    }
    fn replacement_valid(&self, id: &str, replacement: &str) -> bool {
        let policy = &self.storage.policy;
        id != replacement
            && self.storage_active(replacement)
            && ![
                &policy.inputs,
                &policy.checkpoints,
                &policy.outputs,
                &policy.history,
            ]
            .iter()
            .any(|targets| targets.contains(id) && targets.contains(replacement))
            && !self
                .storage_affected(id)
                .any(|a| !a.deleted && a.targets.contains(replacement))
    }
    /// Substitute future placement and add a separate copy for every retained old assignment.
    fn assign_replacement(&mut self, id: &str, replacement: &str, remove_old: bool) {
        let p = &mut self.storage.policy;
        let mut policy_changed = false;
        for set in [
            &mut p.inputs,
            &mut p.checkpoints,
            &mut p.outputs,
            &mut p.history,
        ] {
            if set.remove(id) {
                set.insert(replacement.into());
                policy_changed = true;
            }
        }
        if policy_changed {
            self.storage.policy_revision += 1;
        }
        let location = format!("storage/{id}");
        for a in self
            .storage
            .artifacts
            .values_mut()
            .filter(|a| a.copies.contains_key(&location))
        {
            if !a.deleted && a.targets.insert(replacement.into()) {
                a.copies
                    .insert(format!("storage/{replacement}"), Copy::default());
                a.revision += 1;
            }
            if remove_old && a.targets.remove(id) {
                a.revision += 1;
            }
        }
    }
    pub fn replace_storage(&mut self, id: &str, replacement: &str, revision: u64) -> Reply {
        let current = self.storage_lifecycle(id);
        if !self.storage_active(id) || current.revision != revision {
            return Reply::rejected("storage is not active or lifecycle revision changed");
        }
        if self
            .storage
            .lifecycle
            .values()
            .any(|s| s.phase == Phase::Draining && s.replacement.as_deref() == Some(id))
        {
            return Reply::rejected(
                "finish the incoming replacement before draining this storage agent",
            );
        }
        if !self.replacement_valid(id, replacement) {
            return Reply::rejected(
                "replacement must be active, distinct, and a new copy for every retained artifact",
            );
        }
        self.assign_replacement(id, replacement, false);
        self.storage.lifecycle.insert(
            id.into(),
            Lifecycle {
                revision: revision + 1,
                phase: Phase::Draining,
                replacement: Some(replacement.into()),
                loss: None,
            },
        );
        Reply::Recorded
    }
    pub fn retire_storage(&mut self, id: &str, revision: u64, inventory: &str) -> Reply {
        let Some(plan) = self.storage_plan(id) else {
            return Reply::rejected("unknown storage agent");
        };
        if plan.lifecycle.revision != revision || plan.inventory != inventory {
            return Reply::rejected("storage inventory changed; inspect the retirement plan again");
        }
        if !plan.cleanup_allowed {
            return Reply::rejected("storage cleanup requires recovery review");
        }
        if !matches!(plan.lifecycle.phase, Phase::Active | Phase::Draining)
            || !plan.awaiting_replacement.is_empty()
        {
            return Reply::rejected(
                "all retained artifacts need verified replacement copies before retirement",
            );
        }
        let p = &self.storage.policy;
        if [&p.inputs, &p.checkpoints, &p.outputs, &p.history]
            .iter()
            .any(|s| s.contains(id))
        {
            return Reply::rejected("remove storage from future placement before retirement");
        }
        if self
            .storage
            .lifecycle
            .values()
            .any(|s| s.phase == Phase::Draining && s.replacement.as_deref() == Some(id))
        {
            return Reply::rejected("storage is the destination of an unfinished replacement");
        }
        for a in self.storage.artifacts.values_mut() {
            if a.targets.remove(id) {
                a.revision += 1;
            }
        }
        self.storage.lifecycle.insert(
            id.into(),
            Lifecycle {
                revision: revision + 1,
                phase: Phase::Cleaning,
                ..plan.lifecycle
            },
        );
        Reply::Recorded
    }
    pub fn finish_storage_retirement(&mut self, id: &str, revision: u64) -> Reply {
        let Some(plan) = self.storage_plan(id) else {
            return Reply::rejected("unknown storage agent");
        };
        if plan.lifecycle.revision != revision
            || plan.lifecycle.phase != Phase::Cleaning
            || !plan.awaiting_cleanup.is_empty()
            || !plan.cleanup_allowed
        {
            return Reply::rejected(
                "retirement requires current-authority deletion acknowledgments for every old copy",
            );
        }
        self.storage.lifecycle.insert(
            id.into(),
            Lifecycle {
                revision: revision + 1,
                phase: Phase::Retired,
                ..plan.lifecycle
            },
        );
        Reply::Recorded
    }
    pub fn lose_storage(
        &mut self,
        id: &str,
        replacement: &str,
        revision: u64,
        inventory: &str,
        unavailable: &BTreeSet<String>,
    ) -> Reply {
        let Some(plan) = self.storage_plan(id) else {
            return Reply::rejected("unknown storage agent");
        };
        if plan.lifecycle.revision != revision
            || plan.inventory != inventory
            || unavailable != &plan.unavailable_without_agent
        {
            return Reply::rejected(
                "inspect current storage inventory and acknowledge exactly the artifacts without a known surviving copy",
            );
        }
        if !self.storage_enrolled(id) {
            return Reply::rejected("storage identity is already revoked");
        }
        // An already selected replacement remains usable for a donor lost during draining.
        let continuing = plan.lifecycle.replacement.as_deref() == Some(replacement)
            && self.storage_active(replacement);
        if !continuing && !self.replacement_valid(id, replacement) {
            return Reply::rejected(
                "lost storage needs a distinct active replacement with independent copies",
            );
        }
        let artifacts = self
            .storage_affected(id)
            .map(|a| a.manifest.id.clone())
            .collect();
        self.assign_replacement(id, replacement, true);
        for a in self.storage.artifacts.values_mut() {
            if let Some(c) = a.copies.get_mut(&format!("storage/{id}"))
                && c.phase != CopyPhase::Deleted
            {
                c.phase = CopyPhase::Lost;
                c.authority = self.cluster.clone();
                c.version += 1;
                c.detail = Some("agent declared lost; physical erasure is not confirmed".into());
            }
        }
        // A failed destination must not strand another donor's replacement operation.
        for s in self
            .storage
            .lifecycle
            .values_mut()
            .filter(|s| s.phase == Phase::Draining && s.replacement.as_deref() == Some(id))
        {
            s.replacement = Some(replacement.into());
            s.revision += 1;
        }
        self.storage.lifecycle.insert(
            id.into(),
            Lifecycle {
                revision: revision + 1,
                phase: Phase::Lost,
                replacement: Some(replacement.into()),
                loss: Some(Loss {
                    inventory: inventory.into(),
                    artifacts,
                    unavailable: unavailable.clone(),
                }),
            },
        );
        Reply::Recorded
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::{
        artifact::{Manifest, Policy, Report},
        network::Peer,
    };
    fn setup() -> (State, String) {
        let mut s = State::new("cluster".into());
        for id in ["old", "new", "other"] {
            assert_eq!(
                s.enroll_storage(&Registration {
                    id: id.into(),
                    peer: Peer {
                        identity: iroh::SecretKey::generate().public(),
                        address: "127.0.0.1:12345".parse().unwrap()
                    },
                    incarnation: id.into(),
                    failure_domain: id.into(),
                    capacity_bytes: 16 << 20,
                }),
                Reply::Recorded
            );
        }
        s.storage_policy(
            0,
            &Policy {
                inputs: BTreeSet::from(["old".into()]),
                ..Default::default()
            },
        );
        let m = Manifest::inputs(
            "job".into(),
            vec![crate::jobs::OutputFile {
                path: "executable".into(),
                size: 1,
                hash: iroh_blobs::Hash::new(b"x"),
            }],
        )
        .unwrap();
        s.register_artifact(m.clone());
        ready(&mut s, "old", &m.id);
        (s, m.id)
    }
    fn report(s: &mut State, store: &str, id: &str, phase: CopyPhase) -> Reply {
        let a = &s.storage.artifacts[id];
        let r = &s.storage.stores[store];
        let principal = r.peer.identity.to_string();
        let report = Report {
            location: format!("storage/{store}"),
            incarnation: r.incarnation.clone(),
            artifact: id.into(),
            revision: a.revision,
            expected_version: a.copies[&format!("storage/{store}")].version,
            phase,
            detail: None,
        };
        s.artifact_report(&principal, &report)
    }
    fn ready(s: &mut State, store: &str, id: &str) {
        assert_eq!(report(s, store, id, CopyPhase::Ready), Reply::Recorded);
    }
    fn rejected(r: Reply) {
        assert!(matches!(r, Reply::Rejected { .. }), "{r:?}");
    }

    #[test]
    fn replacement_preserves_pins_and_bytes_and_requires_independent_verified_copies() {
        let (mut s, id) = setup();
        s.artifact_retention(&id, 1, true, Some(100));
        let manifest = s.storage.artifacts[&id].manifest.clone();
        let early = s.storage_plan("old").unwrap();
        rejected(s.retire_storage("old", 0, &early.inventory));
        rejected(s.replace_storage("old", "old", 0));
        assert_eq!(s.replace_storage("old", "new", 0), Reply::Recorded);
        assert!(!s.storage_active("old") && s.storage_readable("old"));
        assert_eq!(s.storage.policy.inputs, BTreeSet::from(["new".into()]));
        assert_eq!(
            s.storage.artifacts[&id].targets,
            BTreeSet::from(["old".into(), "new".into()])
        );
        let plan = s.storage_plan("old").unwrap();
        rejected(s.retire_storage("old", 1, &plan.inventory));
        rejected(s.finish_storage_retirement("old", 1));
        assert_eq!(
            report(&mut s, "new", &id, CopyPhase::Failed),
            Reply::Recorded
        );
        let failed = s.storage_plan("old").unwrap();
        rejected(s.retire_storage("old", 1, &failed.inventory));
        assert_eq!(
            s.storage.artifacts[&id].copies["storage/old"].phase,
            CopyPhase::Ready
        );
        rejected(s.storage_policy(
            s.storage.policy_revision,
            &Policy {
                inputs: BTreeSet::from(["old".into()]),
                ..Default::default()
            },
        ));
        rejected(s.replace_storage("new", "other", 0));
        ready(&mut s, "new", &id);
        // A previously inspected inventory cannot authorize cleanup after any copy changed.
        rejected(s.retire_storage("old", 1, &plan.inventory));
        let plan = s.storage_plan("old").unwrap();
        assert_eq!(s.retire_storage("old", 1, &plan.inventory), Reply::Recorded);
        assert!(!s.storage_readable("old") && s.storage_enrolled("old"));
        assert_eq!(
            s.storage.artifacts[&id].targets,
            BTreeSet::from(["new".into()])
        );
        assert!(s.storage.artifacts[&id].pinned && !s.storage.artifacts[&id].deleted);
        assert_eq!(s.storage.artifacts[&id].expires_at, Some(100));
        assert_eq!(s.storage.artifacts[&id].manifest, manifest);
        rejected(report(&mut s, "old", &id, CopyPhase::Ready));
        rejected(s.finish_storage_retirement("old", 2));
        assert_eq!(
            report(&mut s, "old", &id, CopyPhase::Deleted),
            Reply::Recorded
        );
        assert_eq!(s.finish_storage_retirement("old", 2), Reply::Recorded);
        assert!(!s.storage_enrolled("old"));
        rejected(report(&mut s, "old", &id, CopyPhase::Ready));
        let old = s.storage.stores["old"].clone();
        rejected(s.enroll_storage(&old));
        let mut reused = old;
        reused.id = "different-id".into();
        rejected(s.enroll_storage(&reused));
        assert!(s.durable_artifact(&s.storage.artifacts[&id]));
    }
    #[test]
    fn loss_requires_exact_inventory_and_missing_copy_acknowledgment_without_deleting_data() {
        let (mut s, id) = setup();
        s.artifact_retention(&id, 1, true, None);
        let plan = s.storage_plan("old").unwrap();
        assert_eq!(plan.unavailable_without_agent, BTreeSet::from([id.clone()]));
        rejected(s.lose_storage("old", "new", 0, "stale", &plan.unavailable_without_agent));
        rejected(s.lose_storage("old", "new", 0, &plan.inventory, &BTreeSet::new()));
        let old = s.clone();
        assert_eq!(
            s.lose_storage(
                "old",
                "new",
                0,
                &plan.inventory,
                &plan.unavailable_without_agent
            ),
            Reply::Recorded
        );
        let a = &s.storage.artifacts[&id];
        assert_eq!(a.manifest, old.storage.artifacts[&id].manifest);
        assert!(a.pinned && !a.deleted && !s.durable_artifact(a));
        assert_eq!(a.copies["storage/old"].phase, CopyPhase::Lost);
        assert_eq!(a.copies["storage/new"].phase, CopyPhase::Pending);
        assert_eq!(
            s.storage_lifecycle("old").loss.unwrap().unavailable,
            BTreeSet::from([id.clone()])
        );
        rejected(report(&mut s, "old", &id, CopyPhase::Ready));
        rejected(report(&mut s, "old", &id, CopyPhase::Deleted));
        ready(&mut s, "new", &id);
        assert!(s.durable_artifact(&s.storage.artifacts[&id]));
        assert!(
            s.storage_plan("old")
                .unwrap()
                .awaiting_replacement
                .is_empty()
        );
        // Snapshot/restart serialization preserves the irreversible identity tombstone.
        let roundtrip: State = serde_json::from_slice(&serde_json::to_vec(&s).unwrap()).unwrap();
        assert_eq!(roundtrip, s);
        assert!(!roundtrip.storage_enrolled("old"));
    }
    #[test]
    fn failed_replacement_and_lost_donor_do_not_strand_or_collapse_copy_requirements() {
        let (mut s, id) = setup();
        s.replace_storage("old", "new", 0);
        let p = s.storage_plan("new").unwrap();
        assert_eq!(
            s.lose_storage(
                "new",
                "other",
                0,
                &p.inventory,
                &p.unavailable_without_agent
            ),
            Reply::Recorded
        );
        assert_eq!(
            s.storage_lifecycle("old").replacement.as_deref(),
            Some("other")
        );
        assert_eq!(s.storage_lifecycle("old").revision, 2);
        ready(&mut s, "other", &id);
        let p = s.storage_plan("old").unwrap();
        assert_eq!(s.retire_storage("old", 2, &p.inventory), Reply::Recorded);
        let (mut s, id) = setup();
        s.replace_storage("old", "new", 0);
        ready(&mut s, "new", &id);
        let p = s.storage_plan("old").unwrap();
        assert_eq!(
            s.lose_storage("old", "new", 1, &p.inventory, &BTreeSet::new()),
            Reply::Recorded
        );
        assert!(s.durable_artifact(&s.storage.artifacts[&id]));
        assert_eq!(
            s.storage.artifacts[&id].copies["storage/new"].phase,
            CopyPhase::Ready
        );
    }
    #[test]
    fn replacement_preserves_future_copy_count_even_without_artifacts() {
        let copies = BTreeSet::from(["old".into(), "new".into()]);
        for policy in [
            Policy {
                inputs: copies.clone(),
                ..Default::default()
            },
            Policy {
                checkpoints: copies.clone(),
                ..Default::default()
            },
            Policy {
                outputs: copies.clone(),
                ..Default::default()
            },
            Policy {
                history: copies.clone(),
                ..Default::default()
            },
        ] {
            let (mut s, _) = setup();
            s.storage.artifacts.clear();
            assert_eq!(
                s.storage_policy(s.storage.policy_revision, &policy),
                Reply::Recorded
            );
            rejected(s.replace_storage("old", "new", 0));
            let plan = s.storage_plan("old").unwrap();
            rejected(s.lose_storage("old", "new", 0, &plan.inventory, &BTreeSet::new()));
            assert_eq!(s.storage.policy, policy);
            assert_eq!(s.replace_storage("old", "other", 0), Reply::Recorded);
            let p = &s.storage.policy;
            for targets in [&p.inputs, &p.checkpoints, &p.outputs, &p.history] {
                if !targets.is_empty() {
                    assert_eq!(*targets, BTreeSet::from(["new".into(), "other".into()]));
                }
            }
        }
    }
    #[test]
    fn already_redundant_destination_cannot_silently_reduce_number_of_copies() {
        let (mut s, id) = setup();
        s.storage_policy(
            1,
            &Policy {
                inputs: BTreeSet::from(["old".into(), "new".into()]),
                ..Default::default()
            },
        );
        ready(&mut s, "new", &id);
        rejected(s.replace_storage("old", "new", 0));
        assert_eq!(s.replace_storage("old", "other", 0), Reply::Recorded);
        ready(&mut s, "other", &id);
        let p = s.storage_plan("old").unwrap();
        s.retire_storage("old", 1, &p.inventory);
        assert_eq!(
            s.storage.artifacts[&id].targets,
            BTreeSet::from(["new".into(), "other".into()])
        );
    }
}
