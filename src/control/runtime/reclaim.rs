//! Reclaim only hashes belonging to committed artifact tombstones. Unknown/unpublished cache
//! objects are retained. Input reservations and publication share the agent's GC gate.
use super::*;
use futures_lite::StreamExt;
use iroh_blobs::{Hash, api::blobs::BlobStatus};
use serde_json::json;

impl Worker {
    pub(super) async fn reclaim_and_report_storage(&self) -> Result<()> {
        let view = self.storage_view.as_ref().context("missing storage view")?;
        let _gate = self.backend.blob_guard().await;
        let saved = self.journal.read()?;
        let mut candidates = HashSet::<Hash>::new();
        for a in view
            .artifacts
            .iter()
            .filter(|a| a.deleted && saved.deleted_artifacts.contains(&a.manifest.id))
        {
            candidates.extend(a.manifest.files.iter().map(|f| f.hash));
        }
        for op in saved
            .vm
            .operations
            .values()
            .filter(|o| o.released && o.reclaimed)
        {
            candidates.extend(op.outcome.files.values().map(|f| f.hash));
            if let instances::guest::Spec::Upload { hash, .. } = &op.spec {
                candidates.insert(*hash);
            }
        }
        let mut protected = HashSet::new();
        // Include uncommitted local publications, shared hashes, and in-flight resume inputs.
        for m in saved
            .artifacts
            .values()
            .filter(|m| !saved.deleted_artifacts.contains(&m.id))
        {
            protected.extend(m.files.iter().map(|f| f.hash));
        }
        for a in saved.assignments.values().filter(|a| !a.terminal()) {
            if let Workload::Job { spec, .. } = &a.workload {
                protected.extend(spec.inputs.values().map(|i| i.hash));
            }
        }
        let mut tags = self.store.tags().list().await?;
        while let Some(tag) = tags.next().await {
            protected.insert(tag?.hash);
        }
        let mut temporary = self.store.tags().list_temp_tags().await?;
        while let Some(tag) = temporary.next().await {
            protected.insert(tag.hash);
        }
        candidates.retain(|hash| !protected.contains(hash));
        let hashes = self.store.blobs().list().hashes().await?;
        candidates.retain(|hash| hashes.contains(hash));
        let mut cache_bytes = 0u64;
        let mut reclaimable_bytes = 0u64;
        for hash in &hashes {
            if let BlobStatus::Complete { size } = self.store.blobs().status(*hash).await? {
                cache_bytes += size;
                if candidates.contains(hash) {
                    reclaimable_bytes += size;
                }
            }
        }
        *self.reclamation.candidates.write().expect("GC candidates") = if view.cleanup_allowed {
            candidates.clone()
        } else {
            HashSet::new()
        };
        if view.cleanup_allowed {
            for artifact in view.artifacts.iter().filter(|a| a.deleted) {
                if !artifact
                    .copies
                    .get(&format!("worker/{}", self.config.registration.id))
                    .is_some_and(|c| {
                        c.phase == super::super::artifact::CopyPhase::Deleted
                            && c.authority == self.config.cluster
                    })
                {
                    continue;
                }
                if let super::super::artifact::Kind::Checkpoint(id) = &artifact.manifest.kind {
                    let cp = saved
                        .checkpoints
                        .get(id)
                        .context("checkpoint metadata missing")?;
                    let Some(a) = artifact
                        .manifest
                        .attempt()
                        .and_then(|id| saved.assignments.get(id))
                    else {
                        continue;
                    };
                    // Do not release capture budget until every cache copy is gone or retained
                    // for another live reference. A delayed GC safely backpressures capture.
                    let mut pending = false;
                    for f in &artifact.manifest.files {
                        if !protected.contains(&f.hash) && self.store.blobs().has(f.hash).await? {
                            pending = true;
                        }
                    }
                    if pending {
                        continue;
                    }
                    vm_job::checkpoint_reclaimed(
                        &jobs::jobs_dir(&self.home.join("backend")).join(&a.execution),
                        &cp.content.id,
                    )?;
                }
            }
        }
        let mut captures = BTreeMap::new();
        for a in saved.assignments.values() {
            let dir = jobs::jobs_dir(&self.home.join("backend")).join(&a.execution);
            if matches!(&a.workload, Workload::Job { spec, .. } if spec.checkpoint.is_some()) {
                captures.insert(a.id.clone(), vm_job::checkpoint_usage(&dir)?);
            }
        }
        let report = json!({"observed_at":jobs::now(),"cleanup_allowed":view.cleanup_allowed,
            "blob_filesystem":super::super::storage::usage(&self.home.join("blobs"))?,
            "blob_payload_bytes":cache_bytes,"reclaimable_blob_bytes":reclaimable_bytes,"checkpoints":captures,
            "scope":"Fleet input-bundle/checkpoint/output payloads and their cache entries; client input spools, logs, metadata, VM image bundles and guest-owned files are retained separately"});
        instances::write(&self.home.join("storage-report.json"), &report)?;
        Ok(())
    }
}
