//! Persistent-VM intentions. Consensus holds descriptions and results, never file payloads.
use super::{
    Reply, State, tunnels,
    workload::{Assignment, Desired, InstanceDesired, Workload},
};
use crate::instances::{self, guest};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
};

pub mod client;
#[cfg(test)]
mod tests;
pub const GENERATION: u32 = 1;
pub const RETENTION: u64 = 7 * 86400;
pub const MAX_OPERATIONS: usize = 4096;
pub const MAX_RETAINED: usize = 128;
pub const MAX_RETAINED_BYTES: u64 = 4 << 30;
const MAX_OPERATION_METADATA: usize = 2 << 20;

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Connections {
    #[serde(default)]
    pub exports: BTreeMap<String, SocketAddr>,
    #[serde(default)]
    pub routes: Vec<Route>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Route {
    pub service: Target,
    pub listen: SocketAddr,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Target {
    Vm { instance: String, service: String },
    Host { service: String },
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Network {
    pub revision: u64,
    pub config: Connections,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Binding {
    pub worker: String,
    pub incarnation: String,
    pub adapter: tunnels::Adapter,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct Catalog {
    /// The operator's host topology; managed VM entries are compiled into tunnels.
    pub hosts: Option<tunnels::Topology>,
    pub networks: BTreeMap<String, Network>,
    pub bindings: BTreeMap<String, Binding>,
    pub capable: BTreeSet<String>,
    pub operations: BTreeMap<String, Operation>,
}
impl Catalog {
    pub(crate) fn recover(&mut self, as_of: u64) {
        self.networks.clear();
        self.bindings.clear();
        self.hosts = None;
        self.capable.clear();
        for op in self
            .operations
            .values_mut()
            .filter(|o| !o.outcome.phase.terminal())
        {
            op.outcome = Outcome {
                phase: Phase::Interrupted,
                error: Some("authority recovered; guest effects unknown; not replayed".into()),
                finished_at: Some(as_of),
                ..Outcome::default()
            };
            op.revision += 1;
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Action {
    Connections {
        instance: String,
        expected_revision: u64,
        config: Connections,
    },
    Start {
        id: String,
        instance: String,
        expected_revision: u64,
        spec: guest::Spec,
        #[serde(default = "retention")]
        retention_seconds: u64,
    },
    Release {
        id: String,
        expected_revision: u64,
    },
    Sweep {
        as_of: u64,
    },
}
pub fn retention() -> u64 {
    RETENTION
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Report {
    pub worker: String,
    pub incarnation: String,
    pub update: Update,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Update {
    Capable,
    Bound {
        instance: String,
        adapter: tunnels::Adapter,
    },
    Operation {
        id: String,
        expected_revision: u64,
        result: Outcome,
    },
    Reclaimed {
        id: String,
        expected_revision: u64,
    },
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    Pending,
    Running,
    Succeeded,
    Failed,
    Interrupted,
}
impl Phase {
    pub fn terminal(&self) -> bool {
        !matches!(self, Self::Pending | Self::Running)
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct File {
    pub hash: iroh_blobs::Hash,
    pub size: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Outcome {
    pub phase: Phase,
    pub exit_code: Option<i32>,
    pub error: Option<String>,
    pub finished_at: Option<u64>,
    pub files: BTreeMap<String, File>,
}
impl Default for Outcome {
    fn default() -> Self {
        Self {
            phase: Phase::Pending,
            exit_code: None,
            error: None,
            finished_at: None,
            files: BTreeMap::new(),
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Operation {
    pub id: String,
    pub ordinal: u64,
    pub instance: String,
    pub execution: String,
    pub worker: String,
    pub incarnation: String,
    pub instance_revision: u64,
    pub revision: u64,
    pub spec: guest::Spec,
    pub retention_seconds: u64,
    pub outcome: Outcome,
    pub released: bool,
    pub reclaimed: bool,
}
impl Operation {
    pub fn reservation(&self) -> u64 {
        match &self.spec {
            guest::Spec::Download { max_bytes, .. } => max_bytes + guest::MAX_LOG,
            guest::Spec::Upload { size, .. } => size + 2 * guest::MAX_LOG,
            _ => 2 * guest::MAX_LOG,
        }
    }
    pub fn backend_id(&self) -> String {
        format!(
            "managed-{}",
            &blake3::hash(self.id.as_bytes()).to_hex()[..56]
        )
    }
}
pub fn adapter_id(instance: &str) -> String {
    format!("mvm-{}", &blake3::hash(instance.as_bytes()).to_hex()[..40])
}
pub fn service_id(instance: &str, service: &str) -> String {
    format!(
        "mvs-{}",
        &blake3::hash(&serde_json::to_vec(&(instance, service)).expect("strings")).to_hex()[..40]
    )
}
fn persistent<'a>(s: &'a State, id: &str) -> Result<&'a Assignment> {
    let a = s.assignments.get(id).context("unknown instance")?;
    ensure!(
        matches!(a.workload, Workload::Instance { .. }),
        "persistent VM required"
    );
    Ok(a)
}
fn available(a: &Assignment) -> bool {
    a.desired != Desired::Instance(InstanceDesired::Deleted)
}
impl Connections {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.exports.len() <= 64 && self.routes.len() <= 64,
            "at most 64 exports and routes per VM"
        );
        let mut ports = BTreeSet::new();
        for (id, address) in &self.exports {
            instances::valid_id(id)?;
            tunnels::local_address(*address)?;
            ensure!(ports.insert(address.port()), "duplicate guest port");
        }
        for r in &self.routes {
            tunnels::local_address(r.listen)?;
            ensure!(ports.insert(r.listen.port()), "duplicate guest port");
        }
        Ok(())
    }
}
impl State {
    pub(crate) fn vm_reporter(&self, principal: &str, report: &Report) -> bool {
        self.workers.get(&report.worker).is_some_and(|w| {
            w.peer.identity.to_string() == principal && w.incarnation == report.incarnation
        })
    }
    /// Compile all connections atomically. Missing bindings remain unavailable, never use host sockets.
    fn vm_topology(&self) -> Result<tunnels::Topology> {
        let mut t = self
            .vm
            .hosts
            .clone()
            .unwrap_or_else(|| self.tunnels.topology.clone());
        ensure!(
            t.adapters.keys().all(|id| !id.starts_with("mvm-"))
                && t.services.keys().all(|id| !id.starts_with("mvs-")),
            "managed names are reserved"
        );
        for (id, b) in &self.vm.bindings {
            if persistent(self, id).is_ok_and(available) && self.vm.networks.contains_key(id) {
                t.adapters.insert(adapter_id(id), b.adapter.clone());
            }
        }
        for (id, n) in &self.vm.networks {
            let owner = adapter_id(id);
            if !t.adapters.contains_key(&owner) {
                continue;
            }
            for (name, target) in &n.config.exports {
                t.services.insert(
                    service_id(id, name),
                    tunnels::Service {
                        owner: owner.clone(),
                        target: *target,
                        allowed: BTreeSet::new(),
                    },
                );
            }
        }
        for (id, n) in &self.vm.networks {
            let owner = adapter_id(id);
            if !t.adapters.contains_key(&owner) {
                continue;
            }
            for r in &n.config.routes {
                let service = match &r.service {
                    Target::Vm { instance, service } => service_id(instance, service),
                    Target::Host { service } => service.clone(),
                };
                let Some(s) = t.services.get_mut(&service) else {
                    continue;
                };
                ensure!(s.owner != owner, "self routes are not supported");
                s.allowed.insert(owner.clone());
                t.routes.push(tunnels::Route {
                    adapter: owner.clone(),
                    service,
                    listen: r.listen,
                });
            }
        }
        t.validate()?;
        Ok(t)
    }
    pub(crate) fn refresh_vm_topology(&mut self) -> Result<()> {
        let t = self.vm_topology()?;
        if t != self.tunnels.topology {
            ensure!(
                self.replace_tunnels_raw(self.tunnels.revision, &t) == Reply::Recorded,
                "compiled topology rejected"
            );
        }
        Ok(())
    }
    pub(crate) fn vm_host_topology(&mut self, revision: u64, t: &tunnels::Topology) -> Reply {
        if revision != self.tunnels.revision {
            return Reply::rejected("tunnel revision conflict");
        }
        let old = self.vm.clone();
        self.vm.hosts = Some(t.clone());
        match self.vm_topology() {
            Ok(compiled) => {
                let reply = self.replace_tunnels_raw(revision, &compiled);
                if reply != Reply::Recorded {
                    self.vm = old;
                }
                reply
            }
            Err(e) => {
                self.vm = old;
                Reply::rejected(&e.to_string())
            }
        }
    }
    pub(crate) fn vm_action(&mut self, action: &Action) -> Reply {
        let old = self.vm.clone();
        let result = (|| -> Result<()> {
            match action {
                Action::Connections {
                    instance,
                    expected_revision,
                    config,
                } => {
                    let a = persistent(self, instance)?;
                    ensure!(available(a), "instance is deleted");
                    ensure!(
                        self.recovery_allows_work(&a.worker),
                        "reconcile recovery first"
                    );
                    ensure!(
                        self.vm.capable.contains(&self.workers[&a.worker].cursor()),
                        "worker has not advertised managed VM support"
                    );
                    ensure!(
                        self.vm.networks.get(instance).map_or(0, |n| n.revision)
                            == *expected_revision,
                        "connection revision conflict"
                    );
                    config.validate()?;
                    for r in &config.routes {
                        match &r.service {
                            Target::Vm {
                                instance: target,
                                service,
                            } => {
                                ensure!(
                                    target != instance
                                        && persistent(self, target).is_ok_and(available),
                                    "invalid target VM"
                                );
                                ensure!(
                                    self.vm
                                        .networks
                                        .get(target)
                                        .is_some_and(|n| n.config.exports.contains_key(service)),
                                    "target VM does not expose that service"
                                );
                            }
                            Target::Host { service } => ensure!(
                                self.vm
                                    .hosts
                                    .as_ref()
                                    .unwrap_or(&self.tunnels.topology)
                                    .services
                                    .contains_key(service),
                                "unknown host service"
                            ),
                        }
                    }
                    if self.vm.hosts.is_none() {
                        self.vm.hosts = Some(self.tunnels.topology.clone());
                    }
                    ensure!(
                        self.vm.networks.len() < 64 || self.vm.networks.contains_key(instance),
                        "managed VM network limit reached"
                    );
                    self.vm.networks.insert(
                        instance.clone(),
                        Network {
                            revision: expected_revision
                                .checked_add(1)
                                .context("revision exhausted")?,
                            config: config.clone(),
                        },
                    );
                    self.refresh_vm_topology()?;
                }
                Action::Start {
                    id,
                    instance,
                    expected_revision,
                    spec,
                    retention_seconds,
                } => {
                    instances::valid_id(id)?;
                    spec.validate()?;
                    ensure!(
                        !self.vm.operations.contains_key(id),
                        "operation ID already exists"
                    );
                    ensure!(
                        self.vm.operations.len() < MAX_OPERATIONS,
                        "operation history limit reached"
                    );
                    let a = persistent(self, instance)?;
                    ensure!(
                        a.revision == *expected_revision && a.desired.running(),
                        "VM revision changed or VM is not running"
                    );
                    ensure!(
                        self.recovery_allows_work(&a.worker),
                        "reconcile recovery first"
                    );
                    let w = &self.workers[&a.worker];
                    ensure!(
                        self.vm.capable.contains(&w.cursor()),
                        "worker has not advertised managed VM support"
                    );
                    ensure!(
                        (60..=30 * 86400).contains(retention_seconds),
                        "retention must be 60 seconds to 30 days"
                    );
                    let op = Operation {
                        id: id.clone(),
                        ordinal: self.vm.operations.len() as u64 + 1,
                        instance: instance.clone(),
                        execution: a.execution.clone(),
                        worker: a.worker.clone(),
                        incarnation: w.incarnation.clone(),
                        instance_revision: *expected_revision,
                        revision: 1,
                        spec: spec.clone(),
                        retention_seconds: *retention_seconds,
                        outcome: Outcome::default(),
                        released: false,
                        reclaimed: false,
                    };
                    let retained: Vec<_> = self
                        .vm
                        .operations
                        .values()
                        .filter(|o| o.instance == *instance && !o.reclaimed)
                        .collect();
                    // Reserve space for bounded result manifests/errors before accepting intent.
                    // Large argv/env histories must not grow beyond the control read frame.
                    let metadata = self
                        .vm
                        .operations
                        .values()
                        .chain(std::iter::once(&op))
                        .map(|o| serde_json::to_vec(o).map(|b| b.len() + 2048))
                        .collect::<std::result::Result<Vec<_>, _>>()?
                        .into_iter()
                        .sum::<usize>();
                    ensure!(
                        metadata <= MAX_OPERATION_METADATA,
                        "VM operation metadata budget exhausted"
                    );
                    ensure!(
                        retained.len() < MAX_RETAINED
                            && retained.iter().map(|o| o.reservation()).sum::<u64>()
                                + op.reservation()
                                <= MAX_RETAINED_BYTES,
                        "VM operation retention budget full; release completed results"
                    );
                    ensure!(
                        retained
                            .iter()
                            .filter(|o| !o.outcome.phase.terminal())
                            .count()
                            < 4,
                        "at most four unfinished operations per VM"
                    );
                    self.vm.operations.insert(id.clone(), op);
                }
                Action::Release {
                    id,
                    expected_revision,
                } => {
                    let op = self
                        .vm
                        .operations
                        .get_mut(id)
                        .context("unknown operation")?;
                    ensure!(
                        op.revision == *expected_revision && op.outcome.phase.terminal(),
                        "operation revision changed or not finished"
                    );
                    if !op.released {
                        op.released = true;
                        op.revision += 1;
                    }
                }
                Action::Sweep { as_of } => {
                    for op in self.vm.operations.values_mut() {
                        if !op.released
                            && op.outcome.phase.terminal()
                            && op
                                .outcome
                                .finished_at
                                .is_some_and(|t| t.saturating_add(op.retention_seconds) <= *as_of)
                        {
                            op.released = true;
                            op.revision += 1;
                        }
                    }
                }
            };
            Ok(())
        })();
        match result {
            Ok(()) => Reply::Recorded,
            Err(e) => {
                self.vm = old;
                Reply::rejected(&e.to_string())
            }
        }
    }
    pub(crate) fn vm_report(&mut self, principal: &str, r: &Report) -> Reply {
        if !self.vm_reporter(principal, r) {
            return Reply::rejected("wrong VM reporter");
        }
        let old = self.vm.clone();
        let result = (|| -> Result<()> {
            match &r.update {
                Update::Capable => {
                    self.vm.capable.insert(self.workers[&r.worker].cursor());
                }
                Update::Bound { instance, adapter } => {
                    let a = persistent(self, instance)?;
                    ensure!(
                        a.worker == r.worker
                            && available(a)
                            && self.vm.networks.contains_key(instance),
                        "unassigned VM attachment"
                    );
                    ensure!(
                        self.recovery_allows_work(&r.worker),
                        "reconcile recovery first"
                    );
                    ensure!(
                        !self
                            .workers
                            .values()
                            .any(|w| w.peer.identity == adapter.endpoint.id)
                            && !self
                                .storage
                                .stores
                                .values()
                                .any(|s| s.peer.identity == adapter.endpoint.id),
                        "adapter needs its own key"
                    );
                    let binding = Binding {
                        worker: r.worker.clone(),
                        incarnation: r.incarnation.clone(),
                        adapter: adapter.clone(),
                    };
                    ensure!(
                        self.vm.bindings.get(instance).is_none_or(|b| b == &binding),
                        "VM binding is immutable; identity replacement requires explicit recovery"
                    );
                    self.vm.bindings.insert(instance.clone(), binding);
                    self.refresh_vm_topology()?;
                }
                Update::Operation {
                    id,
                    expected_revision,
                    result,
                } => {
                    let op = self
                        .vm
                        .operations
                        .get_mut(id)
                        .context("unknown operation")?;
                    ensure!(
                        op.worker == r.worker && op.incarnation == r.incarnation,
                        "wrong operation owner"
                    );
                    if op.outcome == *result {
                        return Ok(());
                    }
                    ensure!(
                        op.revision == *expected_revision
                            && !op.released
                            && !op.outcome.phase.terminal(),
                        "stale operation result"
                    );
                    ensure!(
                        result.phase != Phase::Pending
                            && (result.phase.terminal() == result.finished_at.is_some()),
                        "invalid operation phase"
                    );
                    ensure!(
                        result.error.as_ref().is_none_or(|e| e.len() <= 1024),
                        "error too large"
                    );
                    for (name, file) in &result.files {
                        let limit = match (name.as_str(), &op.spec) {
                            ("stderr", _) => guest::MAX_LOG,
                            ("stdout", guest::Spec::Download { .. }) => 0,
                            ("stdout", _) => guest::MAX_LOG,
                            ("download", guest::Spec::Download { max_bytes, .. })
                                if result.phase == Phase::Succeeded =>
                            {
                                *max_bytes
                            }
                            _ => anyhow::bail!("invalid operation result file"),
                        };
                        ensure!(file.size <= limit, "operation result exceeds file budget");
                    }
                    ensure!(
                        result.phase.terminal() || result.files.is_empty(),
                        "unfinished output is not an immutable result"
                    );
                    ensure!(
                        result.phase != Phase::Succeeded
                            || !matches!(op.spec, guest::Spec::Download { .. })
                            || result.files.contains_key("download"),
                        "successful download requires a captured file"
                    );
                    op.outcome = result.clone();
                    op.revision += 1;
                }
                Update::Reclaimed {
                    id,
                    expected_revision,
                } => {
                    let op = self
                        .vm
                        .operations
                        .get_mut(id)
                        .context("unknown operation")?;
                    ensure!(
                        op.worker == r.worker
                            && op.incarnation == r.incarnation
                            && op.released
                            && op.revision == *expected_revision,
                        "stale reclamation report"
                    );
                    op.reclaimed = true;
                }
            };
            Ok(())
        })();
        match result {
            Ok(()) => Reply::Recorded,
            Err(e) => {
                self.vm = old;
                Reply::rejected(&e.to_string())
            }
        }
    }
}
