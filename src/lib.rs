//! fleet: a small job runner for experiment machines over iroh. See `DESIGN.md`.
//!
//! Milestone 1: keys and allowlists; the agent with `Hello`, `Status` and jobs (submit, list, logs, cancel, outputs);
//! the detached supervisor; content-addressed inputs and outputs over `iroh-blobs`; path reports.

pub mod agent;
pub mod blobs;
pub mod compose;
#[cfg(feature = "control-plane")]
pub mod control;
pub mod coordinator;
pub mod daemon;
pub mod events;
#[cfg(feature = "gui")]
pub mod gui;
#[cfg(all(feature = "gui", feature = "legacy"))]
pub mod gui_legacy;
pub mod instances;
pub mod jobs;
pub mod keys;
pub mod probes;
pub mod proto;
pub mod supervise;
pub mod suspend;
pub mod system;
pub mod vm_job;

use iroh::endpoint::Connection;
use serde::{Deserialize, Serialize};

/// An endpoint builder for fleet's processes: n0's relays and DNS address lookup by default. `FLEET_NETWORK=local`
/// turns both off (offline use and tests: machines' addresses then come from the machines file), and `FLEET_PORT` binds
/// a fixed UDP port.
pub fn endpoint_builder(key: iroh::SecretKey) -> anyhow::Result<iroh::endpoint::Builder> {
    use iroh::{Endpoint, RelayMode, endpoint::presets};
    let local = std::env::var("FLEET_NETWORK").is_ok_and(|v| v == "local");
    let mut builder = if local {
        Endpoint::builder(presets::Minimal).relay_mode(RelayMode::Disabled)
    } else {
        Endpoint::builder(presets::N0)
    };
    builder = builder.secret_key(key);
    if let Ok(port) = std::env::var("FLEET_PORT") {
        let port: u16 = port
            .parse()
            .map_err(|_| anyhow::anyhow!("FLEET_PORT must be a port number"))?;
        let ip = if local { [127, 0, 0, 1] } else { [0, 0, 0, 0] };
        builder = builder
            .clear_ip_transports()
            .bind_addr(std::net::SocketAddr::from((ip, port)))
            .map_err(|e| anyhow::anyhow!("binding port {port}: {e}"))?;
    }
    Ok(builder)
}

/// Resolves on Ctrl-C (SIGINT) or SIGTERM (what launchd sends when it stops a service).
pub async fn shutdown_signal() {
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).ok();
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = async {
            match term.as_mut() {
                Some(t) => {
                    t.recv().await;
                }
                None => std::future::pending::<()>().await,
            }
        } => {}
    }
}

/// One open network path of a connection.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct PathReport {
    /// `direct` (IP) or `relay`.
    pub kind: String,
    pub remote: String,
    pub selected: bool,
    pub rtt_ms: f64,
}

/// The connection's open paths, the selected one first.
pub fn paths(connection: &Connection) -> Vec<PathReport> {
    let list = connection.paths();
    let mut out: Vec<PathReport> = (&list)
        .into_iter()
        .map(|p| PathReport {
            kind: if p.is_relay() {
                "relay"
            } else if p.is_ip() {
                "direct"
            } else {
                "other"
            }
            .to_string(),
            remote: format!("{:?}", p.remote_addr()),
            selected: p.is_selected(),
            rtt_ms: p.rtt().as_secs_f64() * 1000.0,
        })
        .collect();
    out.sort_by_key(|p| !p.selected);
    out
}
