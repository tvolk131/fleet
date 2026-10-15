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
}
fn digest(value: &impl Serialize) -> Result<String> {
    Ok(blake3::hash(&serde_json::to_vec(value)?)
        .to_hex()
        .to_string())
}
fn signing_bytes(plan: &RecoveryPlan) -> Result<Vec<u8>> {
    let mut bytes = b"fleet/disaster-recovery/1\0".to_vec();
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
            p.format == super::FORMAT
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
            local.workload.validate(worker)?;
            if let Some(job) = self.queue.get(id) {
                ensure!(
                    job.workload == local.workload
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
                        && old.workload == local.workload,
                    "assignment definition conflicts with recovered history"
                );
                ensure!(
                    old.revision != local.revision || old.desired == local.desired,
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
                            phase: WorkPhase::Uncertain,
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
                a.observed = Some((a.revision, Observation { phase: WorkPhase::Uncertain, detail: Some("restored assignment absent from worker journal; explicit cancellation/deletion required".into()) }));
            }
        }
        for a in assignments
            .values()
            .filter(|a| matches!(a.workload, Workload::Job { .. }))
        {
            self.queue
                .entry(a.id.clone())
                .or_insert_with(|| super::queue::QueuedJob {
                    id: a.id.clone(),
                    workload: a.workload.clone(),
                    worker: Some(a.worker.clone()),
                    priority: 0,
                    requires: BTreeSet::new(),
                    after: BTreeSet::new(),
                    staged: BTreeSet::new(),
                    submitted: 0,
                    revision: 1,
                    cancelled: a.desired == Desired::Cancelled,
                });
        }
        self.assignments = assignments;
        self.cursors.insert(worker.cursor(), inventory.sequence);
        let recovery = self.recovery.as_mut().expect("checked recovery");
        recovery.held = held;
        recovery.inventories.insert(worker.id.clone(), hash);
        Ok(())
    }
}
