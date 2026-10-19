//! Operator-authorized recovery creates a new wire identity and an explicitly reconciled history.
use super::{Config, Reply, State, Store, membership::MembershipState, network::Peer, workload::*};
use anyhow::{Context, Result, ensure};
use iroh::{EndpointId, SecretKey, Signature};
use openraft::{LogId, StoredMembership};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    path::Path,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RecoveryPlan {
    pub family: String,
    pub format: u32,
    pub logical_cluster: String,
    pub previous: String,
    pub ancestors: BTreeSet<String>,
    pub source_applied: LogId<u64>,
    pub source_hash: String,
    pub state_hash: String,
    pub config: Config,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Manifest {
    pub plan: RecoveryPlan,
    pub administrator: EndpointId,
    pub signature: Signature,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Recovery {
    pub manifest: Manifest,
    pub resumed: bool,
    /// Workers absent here remain quarantined, including workers enrolled after recovery.
    pub inventories: BTreeMap<String, String>,
    /// Restored assignments never accepted by their worker require explicit cancellation/deletion.
    pub held: BTreeSet<String>,
    pub unavailable_at_resume: BTreeSet<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Inventory {
    pub held: BTreeSet<String>,
    pub registration: Registration,
    pub sequence: u64,
    pub assignments: BTreeMap<String, Assignment>,
    pub checkpoints: BTreeMap<String, super::checkpoint::Checkpoint>,
}
fn digest(value: &impl Serialize) -> Result<String> {
    Ok(blake3::hash(&serde_json::to_vec(value)?)
        .to_hex()
        .to_string())
}
fn signing_bytes(plan: &RecoveryPlan) -> Result<Vec<u8>> {
    let mut bytes = b"fleet/baseline/disaster-recovery/1\0".to_vec();
    bytes.extend(serde_json::to_vec(plan)?);
    Ok(bytes)
}
impl Manifest {
    pub fn verify(&self, previous: &str, administrators: &BTreeSet<EndpointId>) -> Result<()> {
        ensure!(
            administrators.contains(&self.administrator),
            "recovery signer is not a trusted administrator"
        );
        self.administrator
            .verify(&signing_bytes(&self.plan)?, &self.signature)?;
        let p = &self.plan;
        ensure!(
            p.family == super::FAMILY
                && p.format == super::FORMAT
                && p.previous == previous
                && p.config.cluster != previous
                && !p.config.cluster.is_empty()
                && !p.ancestors.contains(&p.config.cluster),
            "recovery identity/format mismatch"
        );
        ensure!(
            p.config.administrators == *administrators
                && p.config.peers.len() == 1
                && p.config.peers.contains_key(&p.config.id),
            "recovery must retain administrators and start with one replica"
        );
        Ok(())
    }
}

/// Offline operation. The source's redb lock prevents recovering from a running replica.
/// The caller must first isolate old controllers; no local tool can prove a remote host is off.
pub async fn recover(
    source: &Path,
    config: &Config,
    administrator: &SecretKey,
    destination: &Path,
    address: SocketAddr,
) -> Result<Manifest> {
    ensure!(
        config.administrators.contains(&administrator.public()),
        "recovery requires an existing administrator key"
    );
    super::network::validate_address(address)?;
    // A fresh directory is intentionally never reused, even after interrupted preparation.
    std::fs::create_dir(destination).context("recovery destination must be new")?;
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(destination, std::fs::Permissions::from_mode(0o700))?;
    let backup = destination.join("source.redb");
    let source_state = Store::recovery_source(
        &source.join("control.redb"),
        &config.cluster,
        config.id,
        &backup,
    )?;
    crate::instances::write(&destination.join("source-state.json"), &source_state)?;
    let cluster = format!("recovered-{:032x}", rand::random::<u128>());
    let identity = crate::keys::load_or_create(&destination.join("node.key"))?.public();
    let next = Config {
        cluster: cluster.clone(),
        id: 1,
        peers: BTreeMap::from([(1, Peer { identity, address })]),
        administrators: config.administrators.clone(),
        bootstrap_allowed: true,
    };
    let mut ancestors = source_state
        .recovery
        .as_ref()
        .map(|r| r.manifest.plan.ancestors.clone())
        .unwrap_or_default();
    ancestors.insert(config.cluster.clone());
    let plan = RecoveryPlan {
        family: super::FAMILY.into(),
        format: super::FORMAT,
        logical_cluster: source_state
            .recovery
            .as_ref()
            .map(|r| r.manifest.plan.logical_cluster.clone())
            .unwrap_or_else(|| config.cluster.clone()),
        previous: config.cluster.clone(),
        ancestors,
        source_applied: source_state
            .last_applied
            .context("missing applied position")?,
        source_hash: crate::blobs::hash_file(&backup)?.to_string(),
        state_hash: digest(&source_state)?,
        config: next.clone(),
    };
    let manifest = Manifest {
        signature: administrator.sign(&signing_bytes(&plan)?),
        administrator: administrator.public(),
        plan,
    };
    let mut state = source_state;
    state.cluster = cluster;
    state.last_applied = None;
    state.membership = StoredMembership::default();
    state.configuration = MembershipState::default();
    state.membership_operations.clear();
    // The new authority preserves activated behavior, but old-cohort preparation is obsolete.
    state.version.pending = None;
    state.version.revision += 1;
    // A recovered log may predate a service revocation. Preserve identity
    // tombstones so adapters can read a denial, but require the operator to
    // review and reapply routes in the new authority explicitly.
    state.vm.recover(crate::jobs::now());
    state.tunnels.topology = Default::default();
    state.tunnels.observations.clear();
    state.tunnels.revision = state
        .tunnels
        .revision
        .checked_add(1)
        .context("tunnel policy revision exhausted")?;
    let held = state
        .recovery
        .as_ref()
        .map(|r| r.held.clone())
        .unwrap_or_default();
    state.recovery = Some(Box::new(Recovery {
        manifest: manifest.clone(),
        resumed: false,
        inventories: BTreeMap::new(),
        held,
        unavailable_at_resume: BTreeSet::new(),
    }));
    let store = Store::open(&destination.join("control.redb"), &next.cluster, next.id)?;
    store.seed_recovery(state).await?;
    drop(store);
    crate::instances::write(&destination.join("recovery.json"), &manifest)?;
    // Config is published last. Its presence means preparation completed.
    crate::instances::write(&destination.join("config.json"), &next)?;
    Ok(manifest)
}

/// Explicit client trust transition; a different sibling recovery cannot overwrite this choice.
pub fn accept_client(config_path: &Path, manifest: &Manifest) -> Result<()> {
    let lock_path = config_path.with_extension("recovery.lock");
    let _lock =
        crate::instances::try_lock(&lock_path)?.context("client recovery already in progress")?;
    let config: Config = serde_json::from_slice(&std::fs::read(config_path)?)?;
    if config.cluster == manifest.plan.config.cluster {
        manifest.verify(&manifest.plan.previous, &config.administrators)?;
        ensure!(
            config.peers == manifest.plan.config.peers,
            "recovery configuration differs"
        );
        return Ok(());
    }
    manifest.verify(&config.cluster, &config.administrators)?;
    // Keep the previous config as evidence. Use a new path per incarnation for discovery.
    let backup = config_path.with_extension(format!(
        "{}.before-recovery.json",
        blake3::hash(config.cluster.as_bytes()).to_hex()
    ));
    if !backup.exists() {
        crate::instances::write(&backup, &config)?;
    }
    let mut next = manifest.plan.config.clone();
    next.bootstrap_allowed = false;
    crate::instances::write(config_path, &next)
}

impl State {
    pub fn worker_recovery_required(&self, worker: &str) -> bool {
        self.recovery
            .as_ref()
            .is_some_and(|r| !r.inventories.contains_key(worker))
    }
    pub fn recovery_allows_work(&self, worker: &str) -> bool {
        self.recovery
            .as_ref()
            .is_none_or(|r| r.resumed && r.inventories.contains_key(worker))
    }
    pub fn resume_recovery(&mut self, unavailable: &BTreeSet<String>) -> Reply {
        let Some(recovery) = self.recovery.as_mut() else {
            return Reply::rejected("cluster is not recovering");
        };
        let missing: BTreeSet<_> = self
            .workers
            .keys()
            .filter(|w| !recovery.inventories.contains_key(*w))
            .cloned()
            .collect();
        if &missing != unavailable {
            return Reply::rejected(
                "explicit unavailable worker set must equal all unreconciled workers",
            );
        }
        recovery.resumed = true;
        recovery.unavailable_at_resume = unavailable.clone();
        Reply::Recorded
    }
    pub fn recover_worker(&mut self, principal: &str, inventory: &Inventory) -> Reply {
        match self.merge_inventory(principal, inventory) {
            Ok(()) => Reply::Recorded,
            Err(e) => Reply::rejected(&format!("worker recovery: {e:#}")),
        }
    }
    fn merge_inventory(&mut self, principal: &str, inventory: &Inventory) -> Result<()> {
        let recovery = self
            .recovery
            .as_ref()
            .context("cluster is not recovering")?;
        let worker = &inventory.registration;
        ensure!(
            worker.peer.identity.to_string() == principal
                && self.workers.get(&worker.id) == Some(worker),
            "worker identity/enrollment mismatch"
        );
        let hash = digest(inventory)?;
        if let Some(old) = recovery.inventories.get(&worker.id) {
            ensure!(
                old == &hash,
                "worker already reconciled with different inventory"
            );
            return Ok(());
        }
        ensure!(
            inventory.sequence >= self.cursors.get(&worker.cursor()).copied().unwrap_or(0),
            "worker journal is older than restored cursor"
        );
        // Stage every change so a conflict cannot partially import inventory.
        let mut assignments = self.assignments.clone();
        let mut held = recovery.held.clone();
        ensure!(
            inventory
                .held
                .iter()
                .all(|id| inventory.assignments.contains_key(id)),
            "held inventory refers to unknown assignments"
        );
        held.extend(inventory.held.iter().cloned());
        for (id, local) in &inventory.assignments {
            crate::instances::valid_id(id)?;
            crate::instances::valid_id(&local.execution)?;
            ensure!(
                local.id == *id && local.worker == worker.id && local.revision > 0,
                "invalid inventory identity"
            );
            local.validate(worker)?;
            if let Some(job) = local
                .attempt
                .as_ref()
                .and_then(|a| self.jobs.get(a.job.as_ref()))
            {
                ensure!(
                    (local.attempt.as_ref().is_some_and(|a| a.number > 1)
                        || job.workload == local.workload)
                        && job.worker.as_ref().is_none_or(|w| w == &local.worker),
                    "logical job conflicts with worker inventory"
                );
            }
            ensure!(
                !assignments
                    .values()
                    .any(|a| a.id != *id && a.execution == local.execution),
                "execution ID conflicts with recovered history"
            );
            if let Some(old) = assignments.get(id) {
                ensure!(
                    old.worker == local.worker
                        && old.execution == local.execution
                        && old.workload == local.workload
                        && old.attempt == local.attempt,
                    "assignment definition conflicts with recovered history"
                );
                ensure!(
                    old.revision != local.revision
                        || (old.desired == local.desired
                            && old.suspend_checkpoint == local.suspend_checkpoint),
                    "equal revisions have conflicting intentions"
                );
                let mut merged = if old.revision > local.revision {
                    old.clone()
                } else {
                    local.clone()
                };
                if old.terminal() && !local.terminal() {
                    held.insert(id.clone());
                    merged.observed = Some((
                        merged.revision,
                        Observation {
                            phase: merged.workload.uncertain(),
                            detail: Some(
                                "terminal controller record conflicts with worker history".into(),
                            ),
                        },
                    ));
                } else if local.observed.is_some() {
                    merged.observed = local.observed.clone();
                }
                assignments.insert(id.clone(), merged);
            } else {
                assignments.insert(id.clone(), local.clone());
            }
        }
        for a in assignments.values_mut().filter(|a| a.worker == worker.id) {
            if !inventory.assignments.contains_key(&a.id) {
                held.insert(a.id.clone());
                // Even a restored terminal claim is held until the missing local history is explained.
                a.observed = Some((a.revision, Observation { phase: a.workload.uncertain(), detail: Some("restored assignment absent from worker journal; explicit cancellation/deletion required".into()) }));
            }
        }
        let mut jobs = self.jobs.clone();
        let mut attempts: Vec<_> = assignments
            .values()
            .filter(|a| a.attempt.is_some())
            .collect();
        attempts.sort_by_key(|a| a.attempt.as_ref().unwrap().number);
        for a in attempts {
            let attempt = a.attempt.as_ref().expect("job attempt");
            ensure!(
                matches!(a.workload, Workload::Job { .. }) && attempt.number > 0,
                "invalid attempt provenance"
            );
            let job = jobs.entry(attempt.job.to_string()).or_insert_with(|| {
                let mut job = super::queue::Submission {
                    inputs: None,
                    id: attempt.job.to_string(),
                    workload: a.workload.clone(),
                    worker: Some(a.worker.clone()),
                    priority: 0,
                    requires: BTreeSet::new(),
                    after: BTreeSet::new(),
                }
                .job(0);
                job.phase = super::queue::JobPhase::Active;
                job
            });
            let index = (attempt.number - 1) as usize;
            if let Some(old) = job.attempts.get(index) {
                ensure!(
                    old.as_ref() == a.id,
                    "attempt ordinal conflicts with recovered history"
                );
            } else {
                ensure!(job.attempts.len() == index, "attempt history has a gap");
                job.attempts.push(a.id.clone().into());
                job.phase = super::queue::JobPhase::Active;
            }
            if job.attempts.last().is_some_and(|id| id.as_ref() == a.id)
                && a.suspend_checkpoint.is_some()
            {
                job.resume_from = a.suspend_checkpoint.clone();
            }
            if held.contains(&a.id) && job.attempts.last().is_some_and(|id| id.as_ref() == a.id) {
                job.phase = super::queue::JobPhase::Active;
            }
        }
        let mut checkpoints = self.checkpoints.clone();
        for (id, checkpoint) in &inventory.checkpoints {
            ensure!(
                id == &checkpoint.id && checkpoint.worker == worker.id,
                "checkpoint inventory identity mismatch"
            );
            checkpoint.validate(
                assignments
                    .get(checkpoint.attempt.as_ref())
                    .context("checkpoint attempt is missing")?,
            )?;
            if let Some(old) = checkpoints.get(id) {
                ensure!(
                    old == checkpoint,
                    "checkpoint inventory conflicts with history"
                );
            } else {
                checkpoints.insert(id.clone(), checkpoint.clone());
            }
        }
        let mut counts = BTreeMap::<&str, usize>::new();
        for checkpoint in checkpoints.values() {
            let count = counts.entry(checkpoint.attempt.as_ref()).or_default();
            *count += 1;
            ensure!(
                assignments.get(checkpoint.attempt.as_ref()).is_some_and(|a| matches!(&a.workload, Workload::Job {spec,..} if spec.checkpoint.as_ref().is_some_and(|p| p.retention.is_some()))) || *count <= crate::checkpoint::MAX_CHECKPOINTS,
                "checkpoint inventory exceeds per-attempt limit"
            );
        }
        let artifacts = checkpoints
            .values()
            .map(|cp| {
                super::artifact::Manifest::new(
                    cp.worker.clone(),
                    cp.attempt.to_string(),
                    super::artifact::Kind::Checkpoint(cp.id.clone()),
                    cp.content.files.clone(),
                )
            })
            .collect::<Result<Vec<_>>>()?;
        self.checkpoints = checkpoints;
        for manifest in artifacts {
            self.register_artifact(manifest);
        }
        self.jobs = jobs;
        self.assignments = assignments;
        for id in inventory.assignments.keys() {
            self.refresh_job(id);
        }
        self.cursors.insert(worker.cursor(), inventory.sequence);
        let recovery = self.recovery.as_mut().expect("checked recovery");
        recovery.held = held;
        recovery.inventories.insert(worker.id.clone(), hash);
        Ok(())
    }
}
