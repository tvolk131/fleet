//! Capture immutable application manifests and their verified files into a private host directory.
use super::*;
use crate::checkpoint::{Captured, MAX_CHECKPOINTS, MAX_MANIFEST, MAX_RETAINED_BYTES, Manifest};
use std::collections::BTreeMap;

#[derive(Default, Serialize, Deserialize)]
struct Ledger {
    generation: u64,
    transferred: u64,
    #[serde(default)]
    window_start: u64,
    charged: BTreeSet<String>,
    captured: BTreeMap<String, Captured>,
}

impl Ledger {
    fn renew(&mut self, now: u64) -> bool {
        if self.charged.is_empty()
            && (self.window_start == 0 || now.saturating_sub(self.window_start) >= 3600)
        {
            self.window_start = now;
            self.transferred = 0;
            true
        } else {
            false
        }
    }
    fn charge(&mut self, id: &str, size: u64, limit: u64) -> Result<()> {
        if self.charged.contains(id) {
            return Ok(());
        }
        let transferred = self
            .transferred
            .checked_add(size)
            .context("checkpoint transfer overflow")?;
        anyhow::ensure!(
            transferred <= limit,
            "checkpoint transfer budget exhausted; waiting for the next budget window (non-rolling attempts have a lifetime limit)"
        );
        self.transferred = transferred;
        self.charged.insert(id.into());
        Ok(())
    }
}

struct PendingCapture(PathBuf);
impl Drop for PendingCapture {
    fn drop(&mut self) {
        if self.0.exists() {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
}

pub fn captured(dir: &Path) -> Result<Vec<Captured>> {
    let ledger: Ledger = jobs::read_json(&dir.join("vm-checkpoints.json"))?.unwrap_or_default();
    let mut captures: Vec<_> = ledger.captured.into_values().collect();
    captures.sort_by_key(|c| (c.captured_at, c.source.clone(), c.id.clone()));
    Ok(captures)
}

/// Acknowledged only after authoritative deletion and local payload reclamation. The supervisor
/// never rewrites this marker; capture identity and path tombstones remain in its own ledger.
pub fn reclaimed(dir: &Path, id: &str) -> Result<()> {
    let root = files_dir(dir, id)?
        .parent()
        .context("capture parent")?
        .to_owned();
    instances::write(&root.join("reclaimed.json"), &true)
}

fn live<'a>(dir: &Path, ledger: &'a Ledger) -> impl Iterator<Item = &'a Captured> {
    let root = dir.join("checkpoints");
    ledger
        .captured
        .values()
        .filter(move |c| !root.join(&c.id).join("reclaimed.json").is_file())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Usage {
    pub retention: Option<crate::checkpoint::Rolling>,
    pub live_checkpoints: usize,
    pub payload_bytes: u64,
    pub transfer_bytes: u64,
    pub window_start: u64,
    pub captures_total: usize,
    pub blocked: Option<String>,
}

pub fn usage(dir: &Path) -> Result<Usage> {
    let ledger: Ledger = jobs::read_json(&dir.join("vm-checkpoints.json"))?.unwrap_or_default();
    Ok(Usage {
        retention: jobs::read_json::<jobs::Job>(&dir.join("job.json"))?
            .and_then(|j| j.spec.checkpoint)
            .and_then(|p| p.retention),
        live_checkpoints: live(dir, &ledger).count(),
        payload_bytes: live(dir, &ledger)
            .flat_map(|c| &c.files)
            .map(|f| f.size)
            .sum(),
        transfer_bytes: ledger.transferred,
        window_start: ledger.window_start,
        captures_total: ledger.captured.len(),
        blocked: jobs::read_json(&dir.join("checkpoint-error.json"))?,
    })
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
        let limit = self
            .job
            .spec
            .checkpoint
            .as_ref()
            .and_then(|p| p.retention.as_ref())
            .map_or(MAX_RETAINED_BYTES, |r| r.transfer_bytes_per_hour);
        ledger.charge(id, size, limit)?;
        instances::write(&self.dir.join("vm-checkpoints.json"), ledger)?;
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
        let limit = policy
            .retention
            .as_ref()
            .map_or(MAX_CHECKPOINTS, |r| r.max_live_checkpoints);
        anyhow::ensure!(
            (if policy.retention.is_some() {
                live(&self.dir, ledger).count()
            } else {
                ledger.captured.len()
            }) < limit,
            "checkpoint capture slots full; waiting for controller-authorized reclamation (check pins, resume dependencies and backup health)"
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
                (if policy.retention.is_some() {
                    live(&self.dir, ledger).count()
                } else {
                    ledger.captured.len()
                }) < limit,
                "checkpoint capture slots full; waiting for controller-authorized reclamation (check pins, resume dependencies and backup health)"
            );
            let prefix = format!("cp-{}-{n}", ledger.generation);
            if let Err(e) = self.capture_one(file, &policy, &prefix, ledger) {
                last_error = Some(e);
            }
            self.prune_checkpoint_operations(Some(&prefix))?;
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
        let next: u64 = manifest.files.values().map(|f| f.size).sum();
        let floor = u64::from(instances::Config::load(&self.home)?.min_free_disk_gb) * (1 << 30);
        anyhow::ensure!(
            instances::storage::free_bytes(&self.home)?
                >= floor.saturating_add(next.saturating_mul(3)),
            "checkpoint capture blocked by worker free disk guard; reclaim storage before retrying"
        );
        if let Some(rolling) = &policy.retention {
            let current: u64 = live(&self.dir, ledger)
                .flat_map(|c| &c.files)
                .map(|f| f.size)
                .sum();
            // Sealed source + content-addressed cache + transfer/staging headroom. Conservative
            // even with deduplication/APFS clones; metadata and application-owned guest files are separate.
            anyhow::ensure!(
                current
                    .saturating_add(next)
                    .saturating_mul(3)
                    .saturating_add(MAX_MANIFEST)
                    <= rolling.max_live_bytes,
                "checkpoint disk budget full; enlarge max_live_bytes or release protected checkpoints; the next checkpoint needs replacement headroom"
            );
        }
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
        let _staging = PendingCapture(staging.clone());
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
        if self
            .job
            .spec
            .checkpoint
            .as_ref()
            .is_some_and(|p| p.retention.is_some())
            && ledger.renew(jobs::now())
        {
            instances::write(&path, &ledger)?;
        }
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
        let result = self.checkpoint_batch(&mut ledger);
        // Read-only captures may retry in a new batch. A sealed checkpoint never changes or replays training.
        ledger.generation = ledger
            .generation
            .checked_add(1)
            .context("checkpoint generation overflow")?;
        ledger.charged.clear();
        instances::write(&path, &ledger)?;
        self.prune_checkpoint_operations(None)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn transfer_budget_survives_restart_failed_charges_and_clock_rollback() {
        let mut ledger = Ledger::default();
        assert!(ledger.renew(100));
        ledger.charge("download", 80, 100).unwrap();
        let mut ledger: Ledger =
            serde_json::from_slice(&serde_json::to_vec(&ledger).unwrap()).unwrap();
        assert!(
            !ledger.renew(4000),
            "an interrupted batch retains its original charges"
        );
        ledger.charge("download", 80, 100).unwrap();
        assert_eq!(ledger.transferred, 80);
        assert!(ledger.charge("too-large", 30, 100).is_err());
        assert!(!ledger.charged.contains("too-large"));
        ledger.charged.clear();
        assert!(
            !ledger.renew(1),
            "backward clock steps do not refund budget"
        );
        assert!(!ledger.renew(3699));
        assert!(ledger.renew(3700));
        ledger.charge("new-window", 100, 100).unwrap();
        assert!(ledger.charge("over-budget", 1, 100).is_err());
    }
}
