//! A VM's network policy. Fleet owns each VM's only network card; nothing is bridged.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, clap::ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum Network {
    /// No traffic leaves the VM's network card.
    Offline,
    /// Outbound public internet only, enforced by Fleet's host network agent. Not built yet:
    /// workers refuse these VMs.
    PublicInternet,
}
