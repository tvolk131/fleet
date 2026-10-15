//! Application-declared checkpoints. File contents never enter the Raft log.
use crate::{instances::guest, jobs, vm_job};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

pub const MAX_MANIFEST: u64 = 64 << 10;
pub const MAX_CHECKPOINTS: usize = 16;
pub const MAX_RETAINED_BYTES: u64 = 16 << 30;
pub const INPUT_ROOT: &str = "__fleet_checkpoint";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Policy {
    /// Matches immutable manifest files, published atomically after all named files are complete.
    pub manifests: String,
    /// No shell expansion. {checkpoint} is the staged directory; {attempt} is the new attempt number.
    pub resume_argv: Vec<String>,
}

impl Policy {
    pub fn validate(&self) -> Result<()> {
        canonical(&self.manifests.replace('*', "x"))?;
        ensure!(
            !self.manifests.starts_with(&format!("{INPUT_ROOT}/")),
            "checkpoint manifests cannot come from staged resume inputs"
        );
        ensure!(
            self.manifests.contains('/'),
            "checkpoint manifests must be inside a checkpoint directory"
        );
        ensure!(
            self.resume_argv.iter().any(|s| s.contains("{checkpoint}")),
            "resume_argv must reference {{checkpoint}}"
        );
        guest::Spec::Exec {
            argv: self.command(1),
            cwd: None,
            env: BTreeMap::new(),
            timeout_seconds: 1,
        }
        .validate()
    }
    pub fn command(&self, attempt: u32) -> Vec<String> {
        self.resume_argv
            .iter()
            .map(|s| {
                s.replace("{checkpoint}", INPUT_ROOT)
                    .replace("{attempt}", &attempt.to_string())
            })
            .collect()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct File {
    pub size: u64,
    pub sha256: String,
}

/// The application owns consistency. Hashes bind every file to one complete declared version.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    pub version: u32,
    /// Optional progress in application-defined units, for display only.
    pub position: Option<u64>,
    /// Paths relative to the directory containing this manifest.
    pub files: BTreeMap<String, File>,
}

pub fn canonical(path: &str) -> Result<()> {
    let rel = jobs::safe_relative(path)?;
    ensure!(
        path.len() <= 2048
            && !path.contains('\0')
            && rel.to_string_lossy() == path
            && path
                .split('/')
                .all(|p| !p.is_empty() && p != "." && p != ".."),
        "invalid checkpoint path"
    );
    Ok(())
}

impl Manifest {
    pub fn validate(&self) -> Result<()> {
        ensure!(self.version == 1, "unsupported checkpoint manifest version");
        ensure!(
            !self.files.is_empty() && self.files.len() <= vm_job::MAX_INPUTS,
            "checkpoint requires 1 to 128 files"
        );
        ensure!(
            serde_json::to_vec(self)?.len() as u64 <= MAX_MANIFEST,
            "checkpoint manifest exceeds 64 KiB"
        );
        let mut total = 0u64;
        for (path, file) in &self.files {
            canonical(path)?;
            ensure!(
                file.sha256.len() == 64
                    && file
                        .sha256
                        .bytes()
                        .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c)),
                "checkpoint SHA-256 must be lowercase hex"
            );
            ensure!(
                file.size <= guest::MAX_FILE,
                "checkpoint file exceeds 1 GiB"
            );
            total = total
                .checked_add(file.size)
                .ok_or_else(|| anyhow::anyhow!("checkpoint size overflow"))?;
            for parent in Path::new(path).ancestors().skip(1) {
                ensure!(
                    !self.files.contains_key(parent.to_string_lossy().as_ref()),
                    "checkpoint paths overlap"
                );
            }
        }
        ensure!(total <= vm_job::MAX_TOTAL, "checkpoint exceeds 4 GiB");
        Ok(())
    }
    pub fn id(&self, path: &str) -> Result<String> {
        Ok(blake3::hash(&serde_json::to_vec(&(path, self))?)
            .to_hex()
            .to_string())
    }
}

/// A sealed host copy, written only after every hash has been checked and every file synced.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Captured {
    pub id: String,
    pub source: String,
    pub manifest: Manifest,
    pub files: Vec<jobs::OutputFile>,
    pub captured_at: u64,
}

impl Captured {
    pub fn validate(&self) -> Result<()> {
        canonical(&self.source)?;
        self.manifest.validate()?;
        ensure!(
            self.id == self.manifest.id(&self.source)?,
            "checkpoint identity mismatch"
        );
        ensure!(
            self.files.len() == self.manifest.files.len(),
            "checkpoint inventory incomplete"
        );
        let mut seen = BTreeSet::new();
        for file in &self.files {
            ensure!(
                seen.insert(&file.path)
                    && self
                        .manifest
                        .files
                        .get(&file.path)
                        .is_some_and(|f| f.size == file.size),
                "checkpoint inventory differs from manifest"
            );
        }
        Ok(())
    }
    pub fn verify_file(&self, root: &Path, file: &jobs::OutputFile) -> Result<()> {
        let path = root.join(&file.path);
        ensure!(
            std::fs::symlink_metadata(&path)?.file_type().is_file(),
            "checkpoint must be a regular file"
        );
        ensure!(
            std::fs::metadata(&path)?.len() == file.size
                && crate::blobs::hash_file(&path)? == file.hash
                && guest::sha256(&path)? == self.manifest.files[&file.path].sha256,
            "checkpoint file failed verification"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn manifest() -> Manifest {
        Manifest {
            version: 1,
            position: Some(7),
            files: BTreeMap::from([(
                "state".into(),
                File {
                    size: 1,
                    sha256: "a".repeat(64),
                },
            )]),
        }
    }
    #[test]
    fn checkpoint_manifests_reject_traversal_overlap_invalid_hashes_and_incomplete_inventory() {
        let good = manifest();
        good.validate().unwrap();
        for path in [
            "../state",
            "/state",
            "a/../state",
            "a//state",
            "./state",
            "",
        ] {
            let mut bad = good.clone();
            let file = bad.files.remove("state").unwrap();
            bad.files.insert(path.into(), file);
            assert!(bad.validate().is_err(), "accepted {path:?}");
        }
        let mut bad = good.clone();
        bad.files
            .insert("state/child".into(), bad.files["state"].clone());
        assert!(bad.validate().is_err());
        let mut bad = good.clone();
        bad.files.get_mut("state").unwrap().sha256 = "z".repeat(64);
        assert!(bad.validate().is_err());
        let mut bad = good.clone();
        bad.files.get_mut("state").unwrap().size = guest::MAX_FILE + 1;
        assert!(bad.validate().is_err());
        let captured = Captured {
            id: good.id("cp/one/fleet.json").unwrap(),
            source: "cp/one/fleet.json".into(),
            manifest: good,
            files: vec![],
            captured_at: 0,
        };
        assert!(captured.validate().is_err());
    }
}
