//! Independent filesystem storage role. A successful report follows verification, fsync and rename.
use super::{
    Client, Mutation, Reply, Request,
    artifact::*,
    artifact_io,
    network::{Answer, Message, Peer},
};
use anyhow::{Context, Result, ensure};
use iroh::{EndpointId, protocol::Router};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
    time::Duration,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Config {
    pub cluster: String,
    pub registration: Registration,
    pub controllers: BTreeMap<u64, Peer>,
    pub administrators: BTreeSet<EndpointId>,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Usage {
    /// File lengths include orphaned objects and partial staging, not just catalogued ready copies.
    pub file_bytes: u64,
    pub allocated_bytes: u64,
    pub files: u64,
}
pub fn usage(root: &Path) -> Result<Usage> {
    use std::os::unix::fs::MetadataExt;
    let mut result = Usage::default();
    if !root.exists() {
        return Ok(result);
    }
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        let meta = entry.path().symlink_metadata()?;
        ensure!(!meta.file_type().is_symlink(), "storage contains a symlink");
        if meta.is_dir() {
            let child = usage(&entry.path())?;
            result.file_bytes = result
                .file_bytes
                .checked_add(child.file_bytes)
                .context("storage size overflow")?;
            result.allocated_bytes = result
                .allocated_bytes
                .checked_add(child.allocated_bytes)
                .context("storage allocation overflow")?;
            result.files += child.files;
        } else if meta.is_file() {
            result.file_bytes = result
                .file_bytes
                .checked_add(meta.len())
                .context("storage size overflow")?;
            result.allocated_bytes = result
                .allocated_bytes
                .checked_add(meta.blocks().saturating_mul(512))
                .context("storage allocation overflow")?;
            result.files += 1;
        } else {
            anyhow::bail!("storage contains a non-regular object");
        }
    }
    Ok(result)
}
// statvfs field widths differ between macOS and Linux.
#[allow(clippy::unnecessary_cast)]
fn free_bytes(path: &Path) -> Result<u64> {
    use std::os::unix::ffi::OsStrExt;
    let path = std::ffi::CString::new(path.as_os_str().as_bytes())?;
    let mut stats = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // statvfs writes the initialized structure only on success.
    ensure!(
        unsafe { libc::statvfs(path.as_ptr(), stats.as_mut_ptr()) } == 0,
        "statvfs failed: {}",
        std::io::Error::last_os_error()
    );
    let stats = unsafe { stats.assume_init() };
    Ok((stats.f_bavail as u64).saturating_mul(stats.f_frsize as u64))
}

pub(super) fn reserve(home: &Path, capacity: u64, m: &Manifest) -> Result<()> {
    let used = usage(&home.join("objects"))?.file_bytes + usage(&home.join("staging"))?.file_bytes;
    // Conservative allowance for file blocks and nested directories, in addition to payload bytes.
    let entries: u64 = m
        .files
        .iter()
        .map(|f| f.path.split('/').count() as u64 + 1)
        .sum();
    let overhead = (entries * 65536).max(1 << 20);
    let reservation = m
        .bytes()
        .checked_add(serde_json::to_vec(m)?.len() as u64 + overhead)
        .context("reservation overflow")?;
    ensure!(
        used.checked_add(reservation).is_some_and(|n| n <= capacity),
        "storage capacity exhausted: used {used}, reservation {reservation}"
    );
    ensure!(
        free_bytes(home)? >= reservation,
        "insufficient filesystem space for artifact"
    );
    Ok(())
}

pub struct Agent {
    upload_allowed: Arc<RwLock<BTreeMap<String, Manifest>>>,
    gate: Arc<tokio::sync::Mutex<()>>,
    status: Arc<RwLock<serde_json::Value>>,
    pub config: Config,
    pub home: PathBuf,
    client: Client,
    router: Router,
    access: Arc<RwLock<artifact_io::Access>>,
    _lock: crate::instances::Lock,
    audited: BTreeMap<String, std::time::Instant>,
}
impl Agent {
    pub fn initialize(home: &Path, config: &Config) -> Result<()> {
        config.registration.validate()?;
        ensure!(
            !config.cluster.is_empty()
                && !config.controllers.is_empty()
                && !config.administrators.is_empty(),
            "storage authority configuration required"
        );
        ensure!(
            !home.join("identity.json").exists(),
            "storage already initialized"
        );
        crate::instances::write(&home.join("identity.json"), config)?;
        crate::instances::write(&home.join("storage.json"), config)?;
        Ok(())
    }
    pub async fn start(home: &Path, config: Config) -> Result<Self> {
        config.registration.validate()?;
        let lock = crate::instances::try_lock(&home.join("storage.lock"))?
            .context("storage role already running")?;
        let identity: Config = serde_json::from_slice(
            &std::fs::read(home.join("identity.json"))
                .context("missing storage identity; recover metadata instead of reinitializing")?,
        )?;
        ensure!(
            identity == config,
            "storage identity/config mismatch; use explicit rebind for recovery"
        );
        ensure!(home.join("node.key").is_file(), "missing storage key");
        let key = crate::keys::load_or_create(&home.join("node.key"))?;
        ensure!(
            key.public() == config.registration.peer.identity,
            "storage key mismatch"
        );
        for name in ["objects", "staging", "deleted"] {
            std::fs::create_dir_all(home.join(name))?;
        }
        // Only disposable transfer staging is discarded. Durable objects and tombstones survive restarts.
        for entry in std::fs::read_dir(home.join("staging"))? {
            let p = entry?.path();
            ensure!(p.symlink_metadata()?.is_dir(), "invalid staging entry");
            std::fs::remove_dir_all(p)?;
        }
        let endpoint =
            super::network::connect_endpoint(key, config.registration.peer.address).await?;
        let access = Arc::new(RwLock::new(artifact_io::Access::default()));
        let status = Arc::new(RwLock::new(serde_json::Value::Null));
        let upload_allowed = Arc::new(RwLock::new(BTreeMap::new()));
        let gate = Arc::new(tokio::sync::Mutex::new(()));
        let router = Router::builder(endpoint.clone())
            .accept(
                super::input_upload::ALPN,
                super::input_upload::Handler {
                    cluster: config.cluster.clone(),
                    administrators: config.administrators.clone(),
                    allowed: upload_allowed.clone(),
                    gate: gate.clone(),
                    home: home.into(),
                    capacity: config.registration.capacity_bytes,
                    slots: Arc::new(tokio::sync::Semaphore::new(4)),
                },
            )
            .accept(
                super::storage_status::ALPN,
                super::storage_status::Handler {
                    cluster: config.cluster.clone(),
                    administrators: config.administrators.clone(),
                    report: status.clone(),
                },
            )
            .accept(
                artifact_io::ALPN,
                artifact_io::Handler {
                    cluster: config.cluster.clone(),
                    access: access.clone(),
                    slots: Arc::new(tokio::sync::Semaphore::new(4)),
                },
            )
            .spawn();
        let mut client =
            Client::from_endpoint(endpoint, config.cluster.clone(), config.controllers.clone())?;
        client.use_cache(super::discovery::incarnation_cache(
            &home.join("storage.json"),
            &config.cluster,
        ))?;
        Ok(Self {
            upload_allowed,
            gate,
            status,
            config,
            home: home.into(),
            client,
            router,
            access,
            _lock: lock,
            audited: BTreeMap::new(),
        })
    }
    pub async fn shutdown(self) -> Result<()> {
        self.router.shutdown().await?;
        Ok(())
    }
    pub async fn tick(&mut self) -> Result<()> {
        let r = self.config.registration.clone();
        let Answer::StorageRead(Ok(view)) = self
            .client
            .call(
                Message::StorageRead {
                    storage: r.id.clone(),
                    incarnation: r.incarnation.clone(),
                },
                Duration::from_secs(3),
            )
            .await?
        else {
            anyhow::bail!("unexpected storage view")
        };
        ensure!(view.registration == r, "storage enrollment changed");
        let gate = self.gate.clone();
        let _gate = gate.lock().await;
        *self.upload_allowed.write().expect("input uploads") = view
            .artifacts
            .iter()
            .filter(|a| !a.deleted && a.manifest.kind == Kind::Inputs)
            .map(|a| (a.manifest.id.clone(), a.manifest.clone()))
            .collect();
        let readers = self
            .config
            .administrators
            .iter()
            .copied()
            .chain(view.workers.values().map(|w| w.peer.identity))
            .chain(view.stores.values().map(|s| s.peer.identity))
            .collect();
        self.access.write().expect("artifact access").readers = readers;
        for artifact in &view.artifacts {
            let result = self.reconcile(artifact, &view).await;
            let (phase, detail) = match result {
                Ok(phase) => (phase, None),
                Err(e) => (
                    CopyPhase::Failed,
                    Some(format!("{e:#}").chars().take(900).collect()),
                ),
            };
            let old = &artifact.copies[&format!("storage/{}", r.id)];
            if old.phase == phase && old.detail == detail && old.authority == self.config.cluster {
                continue;
            }
            let report = Report {
                location: format!("storage/{}", r.id),
                incarnation: r.incarnation.clone(),
                artifact: artifact.manifest.id.clone(),
                revision: artifact.revision,
                expected_version: old.version,
                phase,
                detail,
            };
            let id = format!(
                "{}-{}",
                if matches!(artifact.manifest.kind, Kind::History { .. }) {
                    "history-copy"
                } else {
                    "artifact"
                },
                blake3::hash(&serde_json::to_vec(&report)?).to_hex()
            );
            let reply = self
                .client
                .write(
                    Request {
                        origin: self.config.cluster.clone(),
                        id,
                        mutation: Mutation::ArtifactReport(report),
                    },
                    Duration::from_secs(3),
                )
                .await?;
            // An intervening retention change invalidates this observation. Refresh next tick.
            if !matches!(reply, Reply::Recorded | Reply::Rejected { .. }) {
                anyhow::bail!("storage report refused: {reply:?}");
            }
        }
        *self.status.write().expect("storage status") = serde_json::json!({
            "observed_at":crate::jobs::now(),"capacity_bytes":r.capacity_bytes,
            "objects":usage(&self.home.join("objects"))?,"staging":usage(&self.home.join("staging"))?,
            "free_filesystem_bytes":free_bytes(&self.home)?,"cleanup_allowed":view.cleanup_allowed,
        });
        Ok(())
    }
    async fn reconcile(&mut self, artifact: &Artifact, view: &View) -> Result<CopyPhase> {
        let m = &artifact.manifest;
        m.validate()?;
        let root = self.home.join("objects").join(&m.id);

        if artifact.deleted {
            ensure!(
                view.cleanup_allowed,
                "cleanup held after authority recovery"
            );
            // Tombstones survive a controller rollback. Never silently resurrect deleted payloads.
            tombstone(&self.home, m, true)?;
            self.audited.remove(&m.id);
            self.access
                .write()
                .expect("artifact access")
                .objects
                .remove(&m.id);
            let removed = root.exists();
            if removed {
                std::fs::remove_dir_all(&root)?;
            }
            let acknowledged = artifact
                .copies
                .get(&format!("storage/{}", self.config.registration.id))
                .is_some_and(|c| {
                    c.phase == CopyPhase::Deleted && c.authority == self.config.cluster
                });
            if removed || !acknowledged {
                std::fs::File::open(self.home.join("objects"))?.sync_all()?;
            }
            return Ok(CopyPhase::Deleted);
        }
        ensure!(
            !tombstone(&self.home, m, false)?,
            "local deletion tombstone conflicts with controller; operator reconciliation required"
        );
        if root.exists() {
            let saved: Manifest = super::artifact::read_manifest(&root.join("manifest.json"))?;
            ensure!(saved == *m, "stored manifest mismatch");
            if self
                .audited
                .get(&m.id)
                .is_some_and(|t| t.elapsed() < Duration::from_secs(300))
                || self.verify(&root.join("files"), m, view).is_ok()
            {
                self.audited
                    .entry(m.id.clone())
                    .and_modify(|t| {
                        if t.elapsed() >= Duration::from_secs(300) {
                            *t = std::time::Instant::now();
                        }
                    })
                    .or_insert_with(std::time::Instant::now);
                self.access
                    .write()
                    .expect("artifact access")
                    .objects
                    .insert(m.id.clone(), (m.clone(), root.join("files")));
                return Ok(CopyPhase::Ready);
            }
            // Corrupt bytes cannot serve as a copy; remove them before a bounded repair transfer.
            self.access
                .write()
                .expect("artifact access")
                .objects
                .remove(&m.id);
            std::fs::remove_dir_all(&root)?;
            self.audited.remove(&m.id);
        }
        reserve(&self.home, self.config.registration.capacity_bytes, m)?;
        let stage = self.home.join("staging").join(&m.id);
        std::fs::create_dir(&stage)?;
        let result = async {
            let mut sources = vec![];
            if let Some(worker) = m.worker().and_then(|id| view.workers.get(id)) {
                sources.push(worker.peer.clone());
            }
            for id in &artifact.targets {
                if id != &self.config.registration.id
                    && artifact
                        .copies
                        .get(&format!("storage/{id}"))
                        .is_some_and(|c| c.phase == CopyPhase::Ready)
                    && let Some(store) = view.stores.get(id)
                {
                    sources.push(store.peer.clone());
                }
            }
            let mut errors = Vec::new();
            for peer in sources {
                match artifact_io::fetch(
                    &self.client.endpoint,
                    &peer,
                    &self.config.cluster,
                    m,
                    &stage.join("files"),
                )
                .await
                {
                    Ok(()) => {
                        errors.clear();
                        break;
                    }
                    Err(e) => errors.push(e.to_string()),
                }
            }
            ensure!(
                stage.join("files").is_dir() && errors.is_empty(),
                "artifact sources unavailable: {}",
                errors.join("; ")
            );
            self.verify(&stage.join("files"), m, view)?;
            crate::instances::write(&stage.join("manifest.json"), m)?;
            std::fs::rename(&stage, &root)?;
            std::fs::File::open(self.home.join("objects"))?.sync_all()?;
            self.access
                .write()
                .expect("artifact access")
                .objects
                .insert(m.id.clone(), (m.clone(), root.join("files")));
            self.audited.insert(m.id.clone(), std::time::Instant::now());
            Ok::<_, anyhow::Error>(CopyPhase::Ready)
        }
        .await;
        if stage.exists() {
            let _ = std::fs::remove_dir_all(&stage);
        }
        result
    }
    fn verify(&self, root: &Path, manifest: &Manifest, view: &View) -> Result<()> {
        artifact_io::verify(root, manifest)?;
        if let Kind::Checkpoint(id) = &manifest.kind {
            let cp = view
                .checkpoints
                .get(id)
                .context("checkpoint metadata unavailable")?;
            for file in &manifest.files {
                cp.content.verify_file(root, file)?;
            }
        }
        Ok(())
    }
}

pub fn accept_recovery(home: &Path, manifest: &super::recovery::Manifest) -> Result<()> {
    let _lock = crate::instances::try_lock(&home.join("storage.lock"))?
        .context("stop storage before rebinding")?;
    let mut config: Config = serde_json::from_slice(&std::fs::read(home.join("storage.json"))?)?;
    let identity: Config = serde_json::from_slice(&std::fs::read(home.join("identity.json"))?)?;
    let prior = if config.cluster == manifest.plan.config.cluster {
        &manifest.plan.previous
    } else {
        &config.cluster
    };
    manifest.verify(prior, &config.administrators)?;
    let binding = home.join("recovery.json");
    if identity.cluster == manifest.plan.config.cluster {
        let recorded: super::recovery::Manifest =
            serde_json::from_slice(&std::fs::read(&binding)?)?;
        ensure!(
            recorded == *manifest,
            "another storage recovery already selected"
        );
    } else {
        ensure!(identity == config, "storage identity/config mismatch");
        crate::instances::write(&binding, manifest)?;
    }
    config.cluster = manifest.plan.config.cluster.clone();
    config.controllers = manifest.plan.config.peers.clone();
    crate::instances::write(&home.join("identity.json"), &config)?;
    crate::instances::write(&home.join("storage.json"), &config)?;
    Ok(())
}

/// History deletion is remembered as coalesced byte ranges, not a permanent file per chunk.
/// Pinned/live holes remain separate; a rollback cannot fetch any previously erased range.
fn tombstone(home: &Path, manifest: &Manifest, delete: bool) -> Result<bool> {
    if let Kind::History { stream, start, end } = manifest.kind {
        let identity = serde_json::to_vec(&(manifest.worker(), manifest.attempt(), stream))?;
        let path = home
            .join("deleted")
            .join(format!("history-{}.json", blake3::hash(&identity).to_hex()));
        let mut ranges: Vec<crate::history::Block> =
            crate::jobs::read_json(&path)?.unwrap_or_default();
        let overlaps = ranges.iter().any(|r| r.start < end && r.end > start);
        if !delete {
            return Ok(overlaps);
        }
        if ranges.iter().any(|r| r.start <= start && r.end >= end) {
            return Ok(true);
        }
        ranges.push(crate::history::Block { start, end });
        ranges.sort_by_key(|r| r.start);
        let mut merged: Vec<crate::history::Block> = vec![];
        for range in ranges {
            if let Some(last) = merged.last_mut().filter(|last| last.end >= range.start) {
                last.end = last.end.max(range.end);
            } else {
                merged.push(range);
            }
        }
        crate::instances::write(&path, &merged)?;
        Ok(true)
    } else {
        let path = home.join("deleted").join(format!("{}.json", manifest.id));
        if delete && !path.exists() {
            crate::instances::write(&path, manifest)?;
        }
        Ok(path.exists())
    }
}

#[cfg(test)]
mod history_tests {
    use super::*;
    #[test]
    fn compact_deletion_ranges_preserve_live_holes_and_refuse_rollback() {
        let home = std::env::temp_dir().join(format!(
            "fleet-history-delete-{:032x}",
            rand::random::<u128>()
        ));
        std::fs::create_dir_all(home.join("deleted")).unwrap();
        let manifest = |n| {
            Manifest::new(
                "w".into(),
                "a".into(),
                Kind::History {
                    stream: crate::history::Stream::Stdout,
                    start: n,
                    end: n + 1,
                },
                vec![crate::jobs::OutputFile {
                    path: "data".into(),
                    size: 1,
                    hash: iroh_blobs::Hash::new(b"x"),
                }],
            )
            .unwrap()
        };
        for n in (0..100).filter(|n| *n != 50) {
            assert!(tombstone(&home, &manifest(n), true).unwrap());
        }
        assert!(!tombstone(&home, &manifest(50), false).unwrap());
        assert!(tombstone(&home, &manifest(0), false).unwrap());
        let files: Vec<_> = std::fs::read_dir(home.join("deleted")).unwrap().collect();
        assert_eq!(files.len(), 1);
        let path = files[0].as_ref().unwrap().path();
        let ranges: Vec<crate::history::Block> = crate::jobs::read_json(&path).unwrap().unwrap();
        assert_eq!(ranges.len(), 2);
        tombstone(&home, &manifest(50), true).unwrap();
        let ranges: Vec<crate::history::Block> = crate::jobs::read_json(&path).unwrap().unwrap();
        assert_eq!(ranges, vec![crate::history::Block { start: 0, end: 100 }]);
        std::fs::remove_dir_all(home).unwrap();
    }
}
