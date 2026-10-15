//! Capture immutable application manifests and their verified files into a private host directory.
use super::*;
use crate::checkpoint::{Captured, MAX_CHECKPOINTS, MAX_MANIFEST, MAX_RETAINED_BYTES, Manifest};
use std::collections::BTreeMap;

#[derive(Default, Serialize, Deserialize)]
struct Ledger {
    generation: u64,
    transferred: u64,
    charged: BTreeSet<String>,
    captured: BTreeMap<String, Captured>,
}

pub fn captured(dir: &Path) -> Result<Vec<Captured>> {
    let ledger: Ledger = jobs::read_json(&dir.join("vm-checkpoints.json"))?.unwrap_or_default();
    Ok(ledger.captured.into_values().collect())
}

pub fn files_dir(dir: &Path, id: &str) -> Result<PathBuf> {
    anyhow::ensure!(
        id.len() == 64 && id.bytes().all(|c| c.is_ascii_hexdigit()),
        "invalid checkpoint capture ID"
    );
    Ok(dir.join("checkpoints").join(id).join("files"))
}

impl Runner {
    fn checkpoint_download(
        &mut self,
        id: &str,
        path: String,
        size: u64,
        version: Option<String>,
        ledger: &mut Ledger,
    ) -> Result<PathBuf> {
        if ledger.charged.insert(id.into()) {
            ledger.transferred = ledger
                .transferred
                .checked_add(size)
                .context("checkpoint transfer overflow")?;
            anyhow::ensure!(
                ledger.transferred <= MAX_RETAINED_BYTES,
                "checkpoint transfer budget exhausted (16 GiB per attempt)"
            );
            instances::write(&self.dir.join("vm-checkpoints.json"), ledger)?;
        }
        let op = self.run_operation(
            id,
            guest::Spec::Download {
                path,
                max_bytes: size.max(1),
                version,
            },
            None,
        )?;
        anyhow::ensure!(
            op.state == guest::OperationState::Succeeded,
            "checkpoint download failed: {:?}",
            op.error
        );
        let path = guest::output(&self.home, &self.status.instance, id)?;
        anyhow::ensure!(
            fs::metadata(&path)?.len() == size,
            "checkpoint size differs from manifest"
        );
        Ok(path)
    }

    fn checkpoint_batch(&mut self, ledger: &mut Ledger) -> Result<()> {
        let Some(policy) = self.job.spec.checkpoint.clone() else {
            return Ok(());
        };
        anyhow::ensure!(
            ledger.captured.len() < MAX_CHECKPOINTS,
            "checkpoint retention limit reached (16 per attempt)"
        );
        let scan = format!("cp-scan-{}", ledger.generation);
        self.require(&scan, self.helper(serde_json::json!({"kind":"manifest", "root":self.guest_root(), "patterns":[policy.manifests], "final":true})), None)?;
        let scan_path =
            guest::operation_dir(&self.home, &self.status.instance, &scan)?.join("stdout");
        anyhow::ensure!(
            fs::metadata(&scan_path)?.len() <= 1 << 20,
            "checkpoint inventory too large"
        );
        let manifests: Vec<Capture> = serde_json::from_slice(&fs::read(scan_path)?)?;
        validate_manifest(&manifests, std::slice::from_ref(&policy.manifests))?;
        let mut last_error = None;
        for (n, file) in manifests.iter().enumerate() {
            self.check()?;
            // An application must use a fresh path for each checkpoint; a sealed path is immutable.
            if ledger.captured.values().any(|c| c.source == file.path) {
                continue;
            }
            anyhow::ensure!(
                ledger.captured.len() < MAX_CHECKPOINTS,
                "checkpoint retention limit reached (16 per attempt)"
            );
            let prefix = format!("cp-{}-{n}", ledger.generation);
            if let Err(e) = self.capture_one(file, &policy, &prefix, ledger) {
                last_error = Some(e);
            } else {
                self.prune_checkpoint_operations(Some(&prefix))?;
            }
        }
        if let Some(e) = last_error {
            return Err(e);
        }
        Ok(())
    }

    fn capture_one(
        &mut self,
        file: &Capture,
        policy: &crate::checkpoint::Policy,
        prefix: &str,
        ledger: &mut Ledger,
    ) -> Result<()> {
        anyhow::ensure!(
            file.path.split('/').count() == policy.manifests.split('/').count()
                && file.size <= MAX_MANIFEST
                && file
                    .version
                    .as_ref()
                    .is_some_and(|v| !v.is_empty() && v.len() <= 256),
            "invalid checkpoint manifest inventory"
        );
        let manifest_file = self.checkpoint_download(
            &format!("{prefix}-manifest"),
            format!("{}/{}", self.guest_root(), file.path),
            file.size,
            file.version.clone(),
            ledger,
        )?;
        let manifest: Manifest = serde_json::from_slice(&fs::read(&manifest_file)?)?;
        manifest.validate()?;
        let id = manifest.id(&file.path)?;
        let root = self.dir.join("checkpoints");
        let sealed = root.join(&id);
        if let Some(c) = jobs::read_json::<Captured>(&sealed.join("captured.json"))? {
            c.validate()?;
            ledger.captured.insert(id, c);
            instances::write(&self.dir.join("vm-checkpoints.json"), ledger)?;
            return Ok(());
        }
        let staging = root.join(format!("pending-{id}"));
        fs::create_dir_all(staging.join("files"))?;
        let guest_parent = Path::new(&file.path)
            .parent()
            .context("checkpoint parent")?
            .to_string_lossy();
        let mut files = Vec::new();
        for (f, (name, expected)) in manifest.files.iter().enumerate() {
            let source = self.checkpoint_download(
                &format!("{prefix}-file-{f}"),
                format!("{}/{guest_parent}/{name}", self.guest_root()),
                expected.size,
                None,
                ledger,
            )?;
            anyhow::ensure!(
                guest::sha256(&source)? == expected.sha256,
                "checkpoint SHA-256 mismatch: {name}"
            );
            let hash = crate::blobs::hash_file(&source)?;
            let target = staging.join("files").join(name);
            fs::create_dir_all(target.parent().context("checkpoint file parent")?)?;
            fs::copy(&source, &target)?;
            fs::File::open(&target)?.sync_all()?;
            for parent in target
                .parent()
                .unwrap()
                .ancestors()
                .take_while(|p| p.starts_with(&staging))
            {
                fs::File::open(parent)?.sync_all()?;
            }
            files.push(jobs::OutputFile {
                path: name.clone(),
                size: expected.size,
                hash,
            });
        }
        let c = Captured {
            id: id.clone(),
            source: file.path.clone(),
            manifest,
            files,
            captured_at: jobs::now(),
        };
        c.validate()?;
        instances::write(&staging.join("captured.json"), &c)?;
        fs::rename(&staging, &sealed)?;
        fs::File::open(&root)?.sync_all()?;
        ledger.captured.insert(id, c);
        instances::write(&self.dir.join("vm-checkpoints.json"), ledger)?;
        Ok(())
    }

    pub(super) fn capture_checkpoints(&mut self) -> Result<()> {
        if self.job.spec.checkpoint.is_none() {
            return Ok(());
        }
        let path = self.dir.join("vm-checkpoints.json");
        let mut ledger: Ledger = jobs::read_json(&path)?.unwrap_or_default();
        let result = self.checkpoint_batch(&mut ledger);
        // Read-only captures may retry in a new batch. A sealed checkpoint never changes or replays training.
        ledger.generation = ledger
            .generation
            .checked_add(1)
            .context("checkpoint generation overflow")?;
        ledger.charged.clear();
        instances::write(&path, &ledger)?;
        self.prune_checkpoint_operations(None)?;
        let root = self.dir.join("checkpoints");
        if root.exists() {
            for entry in fs::read_dir(&root)? {
                let entry = entry?;
                if entry.file_type()?.is_dir()
                    && entry.file_name().to_string_lossy().starts_with("pending-")
                {
                    fs::remove_dir_all(entry.path())?;
                }
            }
        }
        if let Err(e) = result {
            // An invalid/incomplete checkpoint is never advertised, but need not abort useful training.
            instances::write(
                &self.dir.join("checkpoint-error.json"),
                &format!("{e:#}").chars().take(2000).collect::<String>(),
            )?;
        } else if self.dir.join("checkpoint-error.json").exists() {
            fs::remove_file(self.dir.join("checkpoint-error.json"))?;
        }
        Ok(())
    }
    fn prune_checkpoint_operations(&self, prefix: Option<&str>) -> Result<()> {
        for op in guest::list(&self.home, &self.status.instance)? {
            if op.id.starts_with("cp-")
                && prefix.is_none_or(|p| op.id.starts_with(&format!("{p}-")))
                && op.state.finished()
            {
                let dir = guest::operation_dir(&self.home, &self.status.instance, &op.id)?;
                if let Some(_lock) = instances::try_lock(&dir.join("operation.lock"))? {
                    fs::remove_dir_all(dir)?;
                }
            }
        }
        Ok(())
    }
}
