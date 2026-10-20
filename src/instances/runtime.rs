//! Pinned guest platform and capabilities shared by worker admission and the control plane.
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Platform {
    pub os: String,
    pub arch: String,
    #[serde(default)]
    pub capabilities: BTreeSet<String>,
}
impl Platform {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            matches!(self.os.as_str(), "macos" | "linux") && self.arch == "aarch64",
            "portable jobs require macos/aarch64 or linux/aarch64 guests"
        );
        ensure!(
            self.capabilities.len() <= 64
                && self
                    .capabilities
                    .iter()
                    .all(|s| !s.is_empty() && s.len() <= 128),
            "invalid runtime capabilities"
        );
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Runtime {
    pub platform: Platform,
    /// The image bundle's content digest (`crate::driver::bundle`), independent of catalog alias.
    pub image_digest: String,
}
impl Runtime {
    pub fn validate(&self) -> Result<()> {
        self.platform.validate()?;
        crate::driver::bundle::validate(&self.image_digest)
    }
    pub fn accepts(&self, available: &Self) -> Result<()> {
        self.validate()?;
        available.validate()?;
        ensure!(
            self.image_digest == available.image_digest,
            "VM image digest differs"
        );
        ensure!(
            self.platform.os == available.platform.os
                && self.platform.arch == available.platform.arch,
            "guest OS/architecture differs"
        );
        ensure!(
            self.platform
                .capabilities
                .is_subset(&available.platform.capabilities),
            "required runtime capabilities unavailable"
        );
        Ok(())
    }
}
