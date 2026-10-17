//! Immutable artifact manifests and deterministic retention/copy intent. No file IO in apply.
use super::{Reply, State, network::Peer, workload::Workload};
use crate::{checkpoint::canonical, instances, jobs::OutputFile};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

pub const MAX_FILES: usize = 256;
pub const MAX_BYTES: u64 = 4 << 30;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Registration {
    pub id: String,
    pub peer: Peer,
    pub incarnation: String,
    /// Operator-declared physical failure domain, not inferred from IP addresses.
    pub failure_domain: String,
    pub capacity_bytes: u64,
}
impl Registration {
    pub fn validate(&self) -> Result<()> {
        instances::valid_id(&self.id)?;
        self.peer.validate()?;
        ensure!(
            !self.incarnation.is_empty() && self.incarnation.len() <= 128,
            "invalid storage incarnation"
        );
        ensure!(
            !self.failure_domain.is_empty() && self.failure_domain.len() <= 128,
            "storage failure domain required"
        );
        ensure!(
            self.capacity_bytes > 0,
            "positive storage capacity required"
        );
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Kind {
    History {
        stream: crate::history::Stream,
        start: u64,
        end: u64,
    },
    Inputs,
    Checkpoint(String),
    FinalOutputs,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Origin {
    Attempt { worker: String, attempt: String },
    Job(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Manifest {
    pub id: String,
    pub origin: Origin,
    pub kind: Kind,
    pub files: Vec<OutputFile>,
}
pub(crate) fn read_manifest(path: &std::path::Path) -> Result<Manifest> {
    let mut value: serde_json::Value = serde_json::from_slice(&std::fs::read(path)?)?;
    super::upgrade::artifact_origins(&mut value);
    Ok(serde_json::from_value(value)?)
}
impl Manifest {
    pub fn new(
        worker: String,
        attempt: String,
        kind: Kind,
        mut files: Vec<OutputFile>,
    ) -> Result<Self> {
        files.sort_by(|a, b| a.path.cmp(&b.path));
        let id = blake3::hash(&serde_json::to_vec(&(&worker, &attempt, &kind, &files))?)
            .to_hex()
            .to_string();
        let value = Self {
            id,
            origin: Origin::Attempt { worker, attempt },
            kind,
            files,
        };
        value.validate()?;
        Ok(value)
    }
    pub fn inputs(job: String, mut files: Vec<OutputFile>) -> Result<Self> {
        files.sort_by(|a, b| a.path.cmp(&b.path));
        let origin = Origin::Job(job);
        let kind = Kind::Inputs;
        let id = blake3::hash(&serde_json::to_vec(&(&origin, &kind, &files))?)
            .to_hex()
            .to_string();
        let value = Self {
            id,
            origin,
            kind,
            files,
        };
        value.validate()?;
        Ok(value)
    }
    pub fn worker(&self) -> Option<&str> {
        match &self.origin {
            Origin::Attempt { worker, .. } => Some(worker),
            _ => None,
        }
    }
    pub fn attempt(&self) -> Option<&str> {
        match &self.origin {
            Origin::Attempt { attempt, .. } => Some(attempt),
            _ => None,
        }
    }
    pub fn job(&self) -> Option<&str> {
        match &self.origin {
            Origin::Job(job) => Some(job),
            _ => None,
        }
    }
    pub fn bytes(&self) -> u64 {
        self.files.iter().map(|f| f.size).sum()
    }
    pub fn validate(&self) -> Result<()> {
        if let Kind::History { start, end, .. } = self.kind {
            ensure!(
                end > start
                    && end - start <= crate::history::BLOCK_BYTES
                    && self.files.len() == 1
                    && self.files[0].path == "data"
                    && self.files[0].size == end - start,
                "invalid diagnostic history manifest"
            );
        }
        ensure!(
            (self.kind == Kind::Inputs || !self.files.is_empty()) && self.files.len() <= MAX_FILES,
            "artifact requires 1 to 256 files"
        );
        let mut paths = BTreeSet::new();
        let mut bytes = 0u64;
        for file in &self.files {
            canonical(&file.path)?;
            ensure!(
                paths.insert(&file.path) && file.size <= crate::instances::guest::MAX_FILE,
                "duplicate or oversized artifact file"
            );
            bytes = bytes
                .checked_add(file.size)
                .context("artifact size overflow")?;
            ensure!(bytes <= MAX_BYTES, "artifact exceeds 4 GiB");
        }
        for path in &paths {
            for parent in std::path::Path::new(path).ancestors().skip(1) {
                ensure!(
                    !paths.iter().any(|p| p.as_str() == parent.to_string_lossy()),
                    "artifact paths overlap"
                );
            }
        }
        ensure!(
            self.files.windows(2).all(|w| w[0].path < w[1].path),
            "artifact paths must be sorted"
        );
        let encoded = match &self.origin {
            Origin::Attempt { worker, attempt } => {
                ensure!(self.kind != Kind::Inputs, "inputs must be job-owned");
                serde_json::to_vec(&(worker, attempt, &self.kind, &self.files))?
            }
            Origin::Job(job) => {
                crate::instances::valid_id(job)?;
                ensure!(
                    self.kind == Kind::Inputs,
                    "job-owned artifact must be inputs"
                );
                serde_json::to_vec(&(&self.origin, &self.kind, &self.files))?
            }
        };
        let id = blake3::hash(&encoded).to_hex().to_string();
        ensure!(self.id == id, "artifact identity mismatch");
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum CopyPhase {
    Pending,
    Ready,
    Missing,
    Failed,
    Deleted,
    /// The administrator revoked an unavailable storage agent. This is not proof of erasure.
    Lost,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Copy {
    #[serde(default)]
    pub authority: String,
    pub version: u64,
    pub phase: CopyPhase,
    pub detail: Option<String>,
}
impl Default for Copy {
    fn default() -> Self {
        Self {
            authority: String::new(),
            version: 0,
            phase: CopyPhase::Pending,
            detail: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Artifact {
    #[serde(default)]
    pub ordinal: u64,
    pub manifest: Manifest,
    pub revision: u64,
    pub pinned: bool,
    pub expires_at: Option<u64>,
    pub deleted: bool,
    pub targets: BTreeSet<String>,
    /// Keys are worker/ID or storage/ID. Observations are CAS-versioned, not wall-clock ordered.
    pub copies: BTreeMap<String, Copy>,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Policy {
    #[serde(default)]
    pub history: BTreeSet<String>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub inputs: BTreeSet<String>,
    pub checkpoints: BTreeSet<String>,
    pub outputs: BTreeSet<String>,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct Catalog {
    #[serde(default)]
    pub lifecycle: BTreeMap<String, super::storage_lifecycle::Lifecycle>,
    #[serde(default)]
    pub next_ordinal: u64,
    #[serde(default)]
    pub cleanup_reviewed: Option<String>,
    #[serde(default)]
    pub quarantined_locations: BTreeSet<String>,
    pub stores: BTreeMap<String, Registration>,
    pub artifacts: BTreeMap<String, Artifact>,
    pub policy: Policy,
    pub policy_revision: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Ready {
    pub incarnation: String,
    pub manifest: Manifest,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Report {
    /// worker/ID or storage/ID; identity is verified against that enrolled role.
    pub location: String,
    pub incarnation: String,
    pub artifact: String,
    pub revision: u64,
    pub expected_version: u64,
    pub phase: CopyPhase,
    pub detail: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct View {
    pub registration: Registration,
    pub lifecycle: super::storage_lifecycle::Lifecycle,
    pub artifacts: Vec<Artifact>,
    pub workers: BTreeMap<String, super::workload::Registration>,
    pub stores: BTreeMap<String, Registration>,
    pub checkpoints: BTreeMap<String, super::checkpoint::Checkpoint>,
    pub cleanup_allowed: bool,
}

impl State {
    pub fn artifact_catalog_fingerprint(&self) -> String {
        blake3::hash(
            &serde_json::to_vec(&(&self.cluster, &self.storage)).expect("catalog serializes"),
        )
        .to_hex()
        .to_string()
    }
    pub fn artifact_cleanup_allowed(&self, location: Option<&str>) -> bool {
        self.recovery.is_none()
            || (self.storage.cleanup_reviewed.as_ref() == Some(&self.cluster)
                && location.is_none_or(|l| !self.storage.quarantined_locations.contains(l)))
    }
    pub fn review_storage_recovery(
        &mut self,
        fingerprint: &str,
        unavailable: &BTreeSet<String>,
    ) -> Reply {
        if !self.recovery.as_ref().is_some_and(|r| r.resumed) {
            return Reply::rejected("resume reconciled recovery before reviewing artifact cleanup");
        }
        if self.artifact_catalog_fingerprint() != fingerprint {
            return Reply::rejected("artifact catalog changed; inspect and review again");
        }
        let locations: BTreeSet<_> = self
            .storage
            .artifacts
            .values()
            .flat_map(|a| a.copies.keys().cloned())
            .collect();
        if !unavailable.is_subset(&locations) {
            return Reply::rejected("unknown unavailable artifact location");
        }
        if self.storage.artifacts.values().any(|a| {
            a.copies.iter().any(|(location, copy)| {
                copy.authority != self.cluster && !unavailable.contains(location)
            })
        }) {
            return Reply::rejected(
                "artifact locations must report under recovered authority or be explicitly quarantined",
            );
        }
        // Revoked identities cannot rebind/report after disaster recovery. An explicit review
        // may carry forward their terminal observations without claiming lost bytes were erased.
        let revoked: BTreeSet<_> = self
            .storage
            .stores
            .keys()
            .filter(|id| !self.storage_enrolled(id))
            .map(|id| format!("storage/{id}"))
            .collect();
        for a in self.storage.artifacts.values_mut() {
            for (location, copy) in &mut a.copies {
                if revoked.contains(location)
                    && unavailable.contains(location)
                    && matches!(copy.phase, CopyPhase::Deleted | CopyPhase::Lost)
                {
                    copy.authority = self.cluster.clone();
                }
            }
        }
        self.storage.cleanup_reviewed = Some(self.cluster.clone());
        self.storage.quarantined_locations = unavailable.clone();
        Reply::Recorded
    }
    pub fn enroll_storage(&mut self, registration: &Registration) -> Reply {
        let check =
            || -> Result<()> {
                registration.validate()?;
                if let Some(old) = self.storage.stores.get(&registration.id) {
                    ensure!(
                        old == registration,
                        "storage identity/configuration is immutable"
                    );
                    ensure!(
                        self.storage_enrolled(&registration.id),
                        "storage identity is permanently revoked; enroll a fresh identity and ID"
                    );
                }
                ensure!(
                    !self
                        .workers
                        .values()
                        .any(|w| w.peer.identity == registration.peer.identity),
                    "storage needs a separate identity from a worker"
                );
                ensure!(
                    !self.storage.stores.values().any(|s| s.id != registration.id
                        && s.peer.identity == registration.peer.identity),
                    "storage key already enrolled"
                );
                Ok(())
            };
        if let Err(e) = check() {
            return Reply::rejected(&e.to_string());
        }
        self.storage
            .stores
            .insert(registration.id.clone(), registration.clone());
        Reply::Recorded
    }
    pub fn artifact_ready(&mut self, principal: &str, ready: &Ready) -> Reply {
        let m = &ready.manifest;
        let check = || -> Result<()> {
            m.validate()?;
            let worker = self
                .workers
                .get(m.worker().context("artifact has no worker")?)
                .context("unknown artifact worker")?;
            ensure!(
                worker.peer.identity.to_string() == principal
                    && worker.incarnation == ready.incarnation,
                "artifact worker identity mismatch"
            );
            ensure!(
                !self.worker_recovery_required(m.worker().context("artifact has no worker")?),
                "artifact worker must reconcile first"
            );
            let a = self
                .assignments
                .get(m.attempt().context("artifact has no attempt")?)
                .context("unknown artifact attempt")?;
            ensure!(
                Some(a.worker.as_str()) == m.worker() && a.attempt.is_some(),
                "artifact must belong to a job attempt"
            );
            match &m.kind {
                Kind::History { .. } => self.validate_history_chunk(m)?,
                Kind::Inputs => anyhow::bail!("inputs are registered by job submission"),
                Kind::Checkpoint(id) => {
                    let cp = self.checkpoints.get(id).context("unknown checkpoint")?;
                    let expected = Manifest::new(
                        cp.worker.clone(),
                        cp.attempt.to_string(),
                        m.kind.clone(),
                        cp.content.files.clone(),
                    )?;
                    ensure!(m == &expected, "checkpoint artifact mismatch");
                }
                Kind::FinalOutputs => {
                    ensure!(
                        a.terminal(),
                        "final outputs require confirmed attempt cleanup"
                    );
                    let Workload::Job { spec, .. } = &a.workload else {
                        anyhow::bail!("not a job")
                    };
                    ensure!(
                        m.files.iter().all(|f| spec.publish.is_empty()
                            || spec
                                .publish
                                .iter()
                                .any(|p| crate::jobs::matches(p, &f.path))),
                        "undeclared output artifact"
                    );
                    ensure!(
                        !self
                            .storage
                            .artifacts
                            .values()
                            .any(|old| old.manifest.attempt() == m.attempt()
                                && old.manifest.kind == Kind::FinalOutputs
                                && old.manifest != *m),
                        "final output manifest is immutable"
                    );
                }
            }
            if let Some(old) = self.storage.artifacts.get(&m.id) {
                ensure!(old.manifest == *m, "artifact manifest changed");
            }
            Ok(())
        };
        if let Err(e) = check() {
            return Reply::rejected(&e.to_string());
        }
        if self.accept_history_chunk(m) {
            self.register_artifact(m.clone());
        }
        Reply::Recorded
    }
    pub(crate) fn register_artifact(&mut self, manifest: Manifest) {
        if self.storage.artifacts.contains_key(&manifest.id) {
            return;
        }
        self.storage.next_ordinal += 1;
        let targets = match manifest.kind {
            Kind::History { .. } => self.storage.policy.history.clone(),
            Kind::Inputs => self.storage.policy.inputs.clone(),
            Kind::Checkpoint(_) => self.storage.policy.checkpoints.clone(),
            Kind::FinalOutputs => self.storage.policy.outputs.clone(),
        };
        let mut copies = BTreeMap::new();
        if let Some(worker) = manifest.worker() {
            copies.insert(
                format!("worker/{worker}"),
                Copy {
                    authority: self.cluster.clone(),
                    version: 1,
                    phase: CopyPhase::Ready,
                    detail: None,
                },
            );
        }
        copies.extend(
            targets
                .iter()
                .map(|id| (format!("storage/{id}"), Copy::default())),
        );
        // In particular, never reset retention/deletion on a duplicate source report.
        self.storage
            .artifacts
            .entry(manifest.id.clone())
            .or_insert(Artifact {
                ordinal: self.storage.next_ordinal,
                manifest,
                revision: 1,
                pinned: false,
                expires_at: None,
                deleted: false,
                targets,
                copies,
            });
    }
    pub fn storage_policy(&mut self, expected_revision: u64, policy: &Policy) -> Reply {
        if expected_revision != self.storage.policy_revision {
            return Reply::rejected("storage policy revision conflict");
        }
        if policy
            .checkpoints
            .union(&policy.outputs)
            .chain(policy.inputs.iter())
            .chain(policy.history.iter())
            .any(|id| !self.storage_active(id))
        {
            return Reply::rejected("storage targets must be active");
        }
        self.storage.policy = policy.clone();
        self.storage.policy_revision += 1;
        // Removing a default does not delete existing copies. Retention is a separate explicit action.
        for artifact in self.storage.artifacts.values_mut().filter(|a| !a.deleted) {
            let targets = match artifact.manifest.kind {
                Kind::History { .. } => &policy.history,
                Kind::Inputs => &policy.inputs,
                Kind::Checkpoint(_) => &policy.checkpoints,
                Kind::FinalOutputs => &policy.outputs,
            };
            for id in targets {
                if artifact.targets.insert(id.clone()) {
                    artifact
                        .copies
                        .insert(format!("storage/{id}"), Copy::default());
                    artifact.revision += 1;
                }
            }
        }
        Reply::Recorded
    }
    pub fn artifact_reporter(&self, principal: &str, location: &str, incarnation: &str) -> bool {
        if let Some(id) = location.strip_prefix("storage/") {
            self.storage_enrolled(id)
                && self.storage.stores.get(id).is_some_and(|r| {
                    r.peer.identity.to_string() == principal && r.incarnation == incarnation
                })
        } else if let Some(id) = location.strip_prefix("worker/") {
            self.workers.get(id).is_some_and(|r| {
                r.peer.identity.to_string() == principal
                    && r.incarnation == incarnation
                    && !self.worker_recovery_required(id)
            })
        } else {
            false
        }
    }
    pub fn artifact_report(&mut self, principal: &str, report: &Report) -> Reply {
        if !self.artifact_reporter(principal, &report.location, &report.incarnation) {
            return Reply::rejected("artifact reporter identity mismatch");
        }
        let releasing = report.location.strip_prefix("storage/").is_some_and(|id| {
            self.storage_lifecycle(id).phase == super::storage_lifecycle::Phase::Cleaning
        });
        let Some(a) = self.storage.artifacts.get_mut(&report.artifact) else {
            return Reply::rejected("unknown artifact");
        };
        let Some(copy) = a.copies.get_mut(&report.location) else {
            return Reply::rejected("unassigned storage target");
        };
        if a.revision != report.revision || copy.version != report.expected_version {
            return Reply::rejected("stale artifact observation");
        }
        if report.detail.as_ref().is_some_and(|s| s.len() > 1000)
            || matches!(report.phase, CopyPhase::Pending | CopyPhase::Lost)
        {
            return Reply::rejected("invalid artifact observation");
        }
        if (a.deleted || releasing) != (report.phase == CopyPhase::Deleted)
            && report.phase != CopyPhase::Failed
        {
            return Reply::rejected("copy observation contradicts retention intent");
        }
        *copy = Copy {
            authority: self.cluster.clone(),
            version: copy.version + 1,
            phase: report.phase.clone(),
            detail: report.detail.clone(),
        };
        Reply::Recorded
    }
    pub fn artifact_protected(&self, id: &str) -> bool {
        let Some(a) = self.storage.artifacts.get(id) else {
            return false;
        };
        !self.artifact_protection(a).is_empty()
    }
    pub fn artifact_retention(
        &mut self,
        id: &str,
        revision: u64,
        pinned: bool,
        expires_at: Option<u64>,
    ) -> Reply {
        let Some(a) = self.storage.artifacts.get_mut(id) else {
            return Reply::rejected("unknown artifact");
        };
        if a.deleted || a.revision != revision {
            return Reply::rejected("artifact deleted or revision conflict");
        }
        a.pinned = pinned;
        a.expires_at = expires_at;
        a.revision += 1;
        Reply::Recorded
    }
    pub fn delete_artifact(&mut self, id: &str, revision: u64) -> Reply {
        if !self.artifact_cleanup_allowed(None) {
            return Reply::rejected("artifact cleanup requires explicit storage recovery review");
        }
        let Some(a) = self.storage.artifacts.get(id) else {
            return Reply::rejected("unknown artifact");
        };
        if a.revision != revision {
            return Reply::rejected("artifact revision conflict");
        }
        if self.artifact_protected(id) {
            return Reply::rejected("artifact is pinned or needed by a resumable job");
        }
        let a = self.storage.artifacts.get_mut(id).expect("artifact");
        if !a.deleted {
            a.deleted = true;
            a.revision += 1;
        }
        Reply::Recorded
    }
    pub fn expire_artifacts(&mut self, as_of: u64) -> Reply {
        if !self.artifact_cleanup_allowed(None) {
            return Reply::rejected("artifact cleanup requires explicit storage recovery review");
        }
        let due: Vec<_> = self
            .storage
            .artifacts
            .values()
            .filter(|a| !a.deleted && a.expires_at.is_some_and(|t| t <= as_of))
            .map(|a| (a.manifest.id.clone(), a.revision))
            .collect();
        for (id, revision) in due {
            self.delete_artifact(&id, revision);
        }
        Reply::Recorded
    }
    pub fn checkpoint_deleted(&self, id: &str) -> bool {
        self.storage
            .artifacts
            .values()
            .any(|a| a.manifest.kind == Kind::Checkpoint(id.into()) && a.deleted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn registration(id: &str) -> Registration {
        Registration {
            id: id.into(),
            peer: Peer {
                identity: iroh::SecretKey::generate().public(),
                address: "127.0.0.1:12345".parse().unwrap(),
            },
            incarnation: "disk-1".into(),
            failure_domain: "test-machine".into(),
            capacity_bytes: 16 << 20,
        }
    }
    fn manifest() -> Manifest {
        Manifest::new(
            "worker".into(),
            "job-a1".into(),
            Kind::FinalOutputs,
            vec![OutputFile {
                path: "result.txt".into(),
                size: 1,
                hash: iroh_blobs::Hash::from_bytes(*blake3::hash(b"7").as_bytes()),
            }],
        )
        .unwrap()
    }
    #[test]
    fn copy_reports_are_authenticated_versioned_and_cannot_resurrect_a_tombstone() {
        let mut state = State::new("cluster".into());
        let registration = registration("backup");
        assert_eq!(state.enroll_storage(&registration), Reply::Recorded);
        assert_eq!(
            state.storage_policy(
                0,
                &Policy {
                    outputs: BTreeSet::from(["backup".into()]),
                    ..Default::default()
                }
            ),
            Reply::Recorded
        );
        let manifest = manifest();
        state.register_artifact(manifest.clone());
        let mut report = Report {
            location: "storage/backup".into(),
            incarnation: registration.incarnation.clone(),
            artifact: manifest.id.clone(),
            revision: 1,
            expected_version: 0,
            phase: CopyPhase::Ready,
            detail: None,
        };
        let principal = registration.peer.identity.to_string();
        assert!(matches!(
            state.artifact_report("wrong-key", &report),
            Reply::Rejected { .. }
        ));
        assert_eq!(state.artifact_report(&principal, &report), Reply::Recorded);
        assert!(matches!(
            state.artifact_report(&principal, &report),
            Reply::Rejected { .. }
        ));
        assert_eq!(state.delete_artifact(&manifest.id, 1), Reply::Recorded);
        report.expected_version = 1;
        assert!(matches!(
            state.artifact_report(&principal, &report),
            Reply::Rejected { .. }
        ));
        report.revision = 2;
        assert!(matches!(
            state.artifact_report(&principal, &report),
            Reply::Rejected { .. }
        ));
        report.phase = CopyPhase::Deleted;
        assert_eq!(state.artifact_report(&principal, &report), Reply::Recorded);
        state.register_artifact(manifest.clone());
        assert!(state.storage.artifacts[&manifest.id].deleted);
        assert_eq!(
            state.storage.artifacts[&manifest.id].copies["storage/backup"].phase,
            CopyPhase::Deleted
        );
    }
    #[test]
    fn retention_is_cas_guarded_and_expiry_never_overrides_pin() {
        let mut state = State::new("cluster".into());
        let m = manifest();
        state.register_artifact(m.clone());
        assert_eq!(
            state.artifact_retention(&m.id, 1, true, Some(10)),
            Reply::Recorded
        );
        assert!(matches!(
            state.artifact_retention(&m.id, 1, false, None),
            Reply::Rejected { .. }
        ));
        assert_eq!(state.expire_artifacts(20), Reply::Recorded);
        assert!(!state.storage.artifacts[&m.id].deleted);
        assert_eq!(
            state.artifact_retention(&m.id, 2, false, Some(10)),
            Reply::Recorded
        );
        state.expire_artifacts(9);
        assert!(!state.storage.artifacts[&m.id].deleted);
        state.expire_artifacts(10);
        assert!(state.storage.artifacts[&m.id].deleted);
        assert!(matches!(
            state.artifact_retention(&m.id, 4, true, None),
            Reply::Rejected { .. }
        ));
    }
    #[test]
    fn policy_adds_targets_to_existing_artifacts_without_implicitly_deleting_copies() {
        let mut state = State::new("cluster".into());
        let m = manifest();
        state.register_artifact(m.clone());
        assert!(matches!(
            state.storage_policy(
                0,
                &Policy {
                    outputs: BTreeSet::from(["missing".into()]),
                    ..Default::default()
                }
            ),
            Reply::Rejected { .. }
        ));
        state.enroll_storage(&registration("backup"));
        let policy = Policy {
            outputs: BTreeSet::from(["backup".into()]),
            ..Default::default()
        };
        assert_eq!(state.storage_policy(0, &policy), Reply::Recorded);
        assert_eq!(
            state.storage.artifacts[&m.id].copies["storage/backup"].phase,
            CopyPhase::Pending
        );
        assert_eq!(state.storage.artifacts[&m.id].revision, 2);
        assert_eq!(state.storage_policy(1, &Policy::default()), Reply::Recorded);
        assert!(state.storage.artifacts[&m.id].targets.contains("backup"));
    }
    #[test]
    fn manifests_reject_traversal_overlapping_paths_and_altered_content_identity() {
        for paths in [vec!["../escape"], vec!["a", "a/b"], vec!["a", "a"]] {
            let files = paths
                .into_iter()
                .map(|path| OutputFile {
                    path: path.into(),
                    ..manifest().files[0].clone()
                })
                .collect();
            assert!(
                Manifest::new("worker".into(), "attempt".into(), Kind::FinalOutputs, files)
                    .is_err()
            );
        }
        let mut m = manifest();
        m.files[0].size += 1;
        assert!(m.validate().is_err());
    }
}
