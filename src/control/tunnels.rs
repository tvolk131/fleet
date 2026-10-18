//! Replicated intent for registered TCP services. Payload bytes never enter Raft.
use super::{Reply, State};
use anyhow::{Result, ensure};
use iroh::{EndpointAddr, EndpointId};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
};

pub mod agent;
pub mod cli;
pub mod status;
pub mod transport;

/// Generation two was already advertised by the previous release. New variants
/// must wait for generation three, after every voter has installed this binary.
pub const GENERATION: u32 = 3;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Adapter {
    pub endpoint: EndpointAddr,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Service {
    pub owner: String,
    /// Fixed host-local destination; callers never supply an address.
    pub target: SocketAddr,
    pub allowed: BTreeSet<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Route {
    pub adapter: String,
    pub service: String,
    /// Loopback only: this port grants the local host access as its adapter.
    pub listen: SocketAddr,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Topology {
    pub adapters: BTreeMap<String, Adapter>,
    pub services: BTreeMap<String, Service>,
    pub routes: Vec<Route>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Catalog {
    pub revision: u64,
    pub topology: Topology,
    /// Retired adapters retain only permission to fetch an empty policy. Without
    /// these tombstones an adapter restarting after removal could keep stale access.
    pub known_identities: BTreeMap<EndpointId, String>,
    #[serde(default)]
    pub observations: BTreeMap<EndpointId, status::Observation>,
}

/// Only a quorum-confirmed view can replace an adapter's durable policy cache.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct View {
    pub cluster: String,
    pub adapter: String,
    pub identity: EndpointId,
    pub revision: u64,
    pub topology: Topology,
}

fn name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_' | b'.'))
}
pub fn local_address(address: SocketAddr) -> Result<()> {
    ensure!(
        address.ip().is_loopback() && address.port() != 0,
        "service addresses must use loopback and a nonzero port"
    );
    Ok(())
}
impl Topology {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.adapters.len() <= 64 && self.services.len() <= 256 && self.routes.len() <= 1024,
            "tunnel catalog limit exceeded"
        );
        ensure!(
            serde_json::to_vec(self)?.len() <= 256 * 1024,
            "tunnel catalog exceeds 256 KiB"
        );
        let mut identities = BTreeSet::new();
        for (id, a) in &self.adapters {
            ensure!(
                name(id) && identities.insert(a.endpoint.id),
                "invalid or duplicate adapter identity"
            );
            ensure!(
                !a.endpoint.addrs.is_empty() && a.endpoint.addrs.len() <= 8,
                "adapter needs one to eight contact addresses"
            );
            for address in &a.endpoint.addrs {
                match address {
                    iroh::TransportAddr::Ip(ip) => super::network::validate_address(*ip)?,
                    iroh::TransportAddr::Relay(url) => validate_relay(url)?,
                    _ => anyhow::bail!("unsupported tunnel transport address"),
                }
            }
        }
        let mut sockets = BTreeSet::new();
        for (id, s) in &self.services {
            ensure!(
                name(id) && self.adapters.contains_key(&s.owner),
                "invalid service or owner"
            );
            local_address(s.target)?;
            ensure!(
                s.allowed
                    .iter()
                    .all(|id| self.adapters.contains_key(id) && id != &s.owner),
                "unknown or self-authorized adapter"
            );
            // Ports have one meaning per host. This also prevents accidental proxy loops.
            ensure!(
                sockets.insert((s.owner.clone(), s.target.port())),
                "duplicate local service port"
            );
        }
        for r in &self.routes {
            local_address(r.listen)?;
            ensure!(
                self.services
                    .get(&r.service)
                    .is_some_and(|s| s.allowed.contains(&r.adapter)),
                "route is not authorized by its service"
            );
            ensure!(
                sockets.insert((r.adapter.clone(), r.listen.port())),
                "conflicting route or service port"
            );
        }
        Ok(())
    }
    pub fn filtered(&self, adapter: &str) -> Self {
        let services: BTreeMap<_, _> = self
            .services
            .iter()
            .filter(|(_, s)| s.owner == adapter || s.allowed.contains(adapter))
            .map(|(id, s)| (id.clone(), s.clone()))
            .collect();
        let mut visible = BTreeSet::from([adapter.to_owned()]);
        for s in services.values() {
            visible.insert(s.owner.clone());
            visible.extend(s.allowed.iter().cloned());
        }
        Self {
            adapters: self
                .adapters
                .iter()
                .filter(|(id, _)| visible.contains(*id))
                .map(|(id, a)| (id.clone(), a.clone()))
                .collect(),
            services,
            routes: self
                .routes
                .iter()
                .filter(|r| r.adapter == adapter)
                .cloned()
                .collect(),
        }
    }
}

pub(super) fn validate_relay(url: &iroh::RelayUrl) -> Result<()> {
    ensure!(
        matches!(url.scheme(), "http" | "https")
            && url.host_str().is_some()
            && url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none(),
        "relay URL must be HTTP(S) without credentials, query, or fragment"
    );
    Ok(())
}
impl State {
    pub(crate) fn replace_tunnels(&mut self, revision: u64, topology: &Topology) -> Reply {
        if revision != self.tunnels.revision {
            return Reply::rejected("tunnel revision conflict");
        }
        if let Err(error) = topology.validate() {
            return Reply::rejected(&error.to_string());
        }
        let Some(next) = revision.checked_add(1) else {
            return Reply::rejected("tunnel revision exhausted");
        };
        let mut known_identities = self.tunnels.known_identities.clone();
        for (name, adapter) in &topology.adapters {
            if known_identities
                .get(&adapter.endpoint.id)
                .is_some_and(|old| old != name)
            {
                return Reply::rejected("an adapter identity cannot change its registered name");
            }
            known_identities.insert(adapter.endpoint.id, name.clone());
        }
        if known_identities.len() > 4096 {
            return Reply::rejected("tunnel identity history exhausted");
        }
        self.tunnels = Catalog {
            revision: next,
            topology: topology.clone(),
            known_identities,
            observations: self.tunnels.observations.clone(),
        };
        Reply::Recorded
    }
    pub fn tunnel_view(&self, adapter: &str, identity: EndpointId) -> View {
        let authorized = self.version.active >= GENERATION
            && self
                .tunnels
                .topology
                .adapters
                .get(adapter)
                .is_some_and(|a| a.endpoint.id == identity);
        View {
            cluster: self.cluster.clone(),
            adapter: adapter.into(),
            identity,
            revision: self.tunnels.revision,
            // A removed identity receives a committed empty policy, not an error
            // which could be confused with an unavailable control plane.
            topology: if authorized {
                self.tunnels.topology.filtered(adapter)
            } else {
                Topology::default()
            },
        }
    }
}
