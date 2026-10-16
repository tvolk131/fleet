//! Worker-owned immutable manifests, source cleanup, and same-worker checkpoint restoration.
use super::*;
use crate::control::{
    artifact::{self, CopyPhase, Kind, Manifest},
    artifact_io,
};

impl Worker {
    fn artifact_root(&self, manifest: &Manifest) -> Result<PathBuf> {
        let restored = self.home.join("restored").join(&manifest.id);
        if restored.exists() {
            return Ok(restored);
        }
        let journal = self.journal.read()?;
        let a = journal
            .assignments
            .get(&manifest.attempt)
            .context("artifact attempt missing")?;
        let dir = jobs::jobs_dir(&self.home.join("backend")).join(&a.execution);
        Ok(match &manifest.kind {
            Kind::FinalOutputs => dir.join("work"),
            Kind::Checkpoint(id) => {
                let cp = journal
                    .checkpoints
                    .get(id)
                    .context("artifact checkpoint missing")?;
                vm_job::checkpoint_files_dir(&dir, &cp.content.id)?
            }
        })
    }
    pub(super) async fn reconcile_artifacts(&mut self) -> Result<()> {
        let view = self
            .storage_view
            .clone()
            .context("no committed storage view")?;
        let saved = self.journal.read()?;
        for a in &view.assignments {
            if !a.terminal()
                || a.attempt.is_none()
                || saved
                    .artifacts
                    .values()
                    .any(|m| m.attempt == a.id && m.kind == Kind::FinalOutputs)
            {
                continue;
            }
            if !jobs::jobs_dir(&self.home.join("backend"))
                .join(&a.execution)
                .join("job.json")
                .is_file()
            {
                continue;
            }
            match self.capture_final_outputs(a).await {
                Ok(Some(manifest)) => self.journal.change(|s| {
                    s.artifacts.insert(manifest.id.clone(), manifest.clone());
                    Ok(())
                })?,
                Ok(None) => {}
                Err(error) => {
                    tracing::warn!(attempt=%a.id, %error, "final output publication unavailable; other artifacts continue")
                }
            }
        }
        for cp in saved.checkpoints.values() {
            let manifest = Manifest::new(
                cp.worker.clone(),
                cp.attempt.to_string(),
                Kind::Checkpoint(cp.id.clone()),
                cp.content.files.clone(),
            )?;
            if !saved.artifacts.contains_key(&manifest.id) {
                self.journal.change(|s| {
                    s.artifacts.insert(manifest.id.clone(), manifest.clone());
                    Ok(())
                })?;
            }
        }
        // Persist tombstones before removing local files. Replayed source reports cannot resurrect them.
        for a in view.artifacts.iter().filter(|a| a.deleted) {
            if !view.cleanup_allowed {
                self.report_artifact(
                    a,
                    CopyPhase::Failed,
                    Some("cleanup held pending storage recovery review".into()),
                )
                .await?;
                continue;
            }
            let m = &a.manifest;
            self.journal.change(|s| {
                s.deleted_artifacts.insert(m.id.clone());
                Ok(())
            })?;
            self.artifact_access
                .write()
                .expect("artifact access")
                .objects
                .remove(&m.id);
            let cleanup = async {
                let mut roots = vec![self.artifact_root(m)?];
                if let Kind::Checkpoint(id) = &m.kind {
                    let cp = saved
                        .checkpoints
                        .get(id)
                        .context("checkpoint metadata missing")?;
                    let a = &saved.assignments[&m.attempt];
                    let original = vm_job::checkpoint_files_dir(
                        &jobs::jobs_dir(&self.home.join("backend")).join(&a.execution),
                        &cp.content.id,
                    )?;
                    if !roots.contains(&original) {
                        roots.push(original);
                    }
                }
                for file in &m.files {
                    for root in &roots {
                        artifact_io::remove_owned_file(root, &file.path)?;
                    }
                    let tag = match &m.kind {
                        Kind::Checkpoint(id) => format!("checkpoint/{id}/{}", file.path),
                        Kind::FinalOutputs => format!(
                            "output/{}/{}",
                            saved.assignments[&m.attempt].execution, file.path
                        ),
                    };
                    self.store.tags().delete(tag).await?;
                }
                self.store.sync_db().await?;
                Ok::<_, anyhow::Error>(())
            }
            .await;
            match cleanup {
                Ok(()) => self.report_artifact(a, CopyPhase::Deleted, None).await?,
                Err(e) => {
                    self.report_artifact(
                        a,
                        CopyPhase::Failed,
                        Some(format!("{e:#}").chars().take(900).collect()),
                    )
                    .await?
                }
            }
        }
        let saved = self.journal.read()?;
        for artifact in view
            .artifacts
            .iter()
            .filter(|a| !a.deleted && saved.deleted_artifacts.contains(&a.manifest.id))
        {
            self.report_artifact(artifact, CopyPhase::Failed, Some("local deletion tombstone conflicts with recovered catalog; explicit reconciliation required".into())).await?;
        }
        let mut access = artifact_io::Access {
            readers: self
                .config
                .administrators
                .iter()
                .copied()
                .chain(view.stores.values().map(|s| s.peer.identity))
                .collect(),
            objects: BTreeMap::new(),
        };
        for m in saved
            .artifacts
            .values()
            .filter(|m| !saved.deleted_artifacts.contains(&m.id))
        {
            let root = self.artifact_root(m)?;
            // Contents are checked again at transfer time; a missing source is immediately visible.
            let present = m.files.iter().all(|f| root.join(&f.path).is_file());
            if present {
                access.objects.insert(m.id.clone(), (m.clone(), root));
            }
            if let Some(a) = view.artifacts.iter().find(|a| a.manifest.id == m.id) {
                if present
                    && !a.deleted
                    && a.copies
                        .get(&format!("worker/{}", m.worker))
                        .is_some_and(|c| {
                            c.phase == CopyPhase::Missing
                                || c.phase == CopyPhase::Pending
                                || c.authority != self.config.cluster
                        })
                {
                    artifact_io::verify(&self.artifact_root(m)?, m)?;
                    self.report_artifact(a, CopyPhase::Ready, None).await?;
                }
                if !present && !a.deleted {
                    self.report_artifact(
                        a,
                        CopyPhase::Missing,
                        Some("worker payload unavailable".into()),
                    )
                    .await?;
                }
            } else if present && matches!(m.kind, Kind::FinalOutputs) {
                let reply = self
                    .client
                    .write(
                        Request {
                            origin: self.config.cluster.clone(),
                            id: format!("artifact-source-{}", m.id),
                            mutation: Mutation::ArtifactReady(artifact::Ready {
                                incarnation: self.config.registration.incarnation.clone(),
                                manifest: m.clone(),
                            }),
                        },
                        Duration::from_secs(2),
                    )
                    .await?;
                ensure!(
                    reply == Reply::Recorded,
                    "artifact source rejected: {reply:?}"
                );
            }
        }
        *self.artifact_access.write().expect("artifact access") = access;
        self.reclaim_and_report_storage().await?;
        Ok(())
    }
    async fn capture_final_outputs(&self, a: &Assignment) -> Result<Option<Manifest>> {
        let proto::Response::Outputs(files) = self
            .agent
            .handle(
                self.config.registration.peer.identity,
                proto::Request::Outputs {
                    job: a.execution.clone(),
                },
            )
            .await?
        else {
            bail!("unexpected final outputs reply")
        };
        if files.is_empty() {
            return Ok(None);
        }
        let manifest = Manifest::new(a.worker.clone(), a.id.clone(), Kind::FinalOutputs, files)?;
        let root = jobs::jobs_dir(&self.home.join("backend"))
            .join(&a.execution)
            .join("work");
        let verified = manifest.clone();
        tokio::task::spawn_blocking(move || artifact_io::verify(&root, &verified)).await??;
        Ok(Some(manifest))
    }
    async fn report_artifact(
        &mut self,
        a: &artifact::Artifact,
        phase: CopyPhase,
        detail: Option<String>,
    ) -> Result<()> {
        let location = format!("worker/{}", self.config.registration.id);
        let copy = a
            .copies
            .get(&location)
            .context("missing worker copy intent")?;
        if copy.phase == phase && copy.detail == detail && copy.authority == self.config.cluster {
            return Ok(());
        }
        let report = artifact::Report {
            location,
            incarnation: self.config.registration.incarnation.clone(),
            artifact: a.manifest.id.clone(),
            revision: a.revision,
            expected_version: copy.version,
            phase,
            detail,
        };
        let id = format!(
            "artifact-{}",
            blake3::hash(&serde_json::to_vec(&report)?).to_hex()
        );
        self.client
            .write(
                Request {
                    origin: self.config.cluster.clone(),
                    id,
                    mutation: Mutation::ArtifactReport(report),
                },
                Duration::from_secs(2),
            )
            .await?;
        Ok(())
    }
    pub(super) async fn restore_checkpoint(&self, a: &Assignment) -> Result<()> {
        let Some(id) = a.attempt.as_ref().and_then(|a| a.checkpoint.as_ref()) else {
            return Ok(());
        };
        let journal = self.journal.read()?;
        let cp = journal
            .checkpoints
            .get(id)
            .context("resume checkpoint metadata missing")?;
        let manifest = Manifest::new(
            cp.worker.clone(),
            cp.attempt.to_string(),
            Kind::Checkpoint(id.clone()),
            cp.content.files.clone(),
        )?;
        ensure!(
            !journal.deleted_artifacts.contains(&manifest.id),
            "local checkpoint deletion requires reconciliation"
        );
        // Staged inputs are verified again by the backend before launch, even when already cached.
        let mut missing = false;
        for f in &cp.content.files {
            if !self.store.blobs().has(f.hash).await? {
                missing = true;
            }
        }
        if !missing {
            return Ok(());
        }
        let local = self.artifact_root(&manifest)?;
        if artifact_io::verify(&local, &manifest).is_ok() {
            for file in &cp.content.files {
                cp.content.verify_file(&local, file)?;
                blobs::import_named(
                    &self.store,
                    &local.join(&file.path),
                    file.hash,
                    format!("checkpoint/{id}/{}", file.path),
                )
                .await?;
            }
            self.store.sync_db().await?;
            return Ok(());
        }
        let view = self
            .storage_view
            .as_ref()
            .context("restore requires committed storage locations")?;
        let artifact = view
            .artifacts
            .iter()
            .find(|a| a.manifest.kind == Kind::Checkpoint(id.clone()) && !a.deleted)
            .context("checkpoint artifact is unavailable or deleted")?;
        ensure!(
            !journal.deleted_artifacts.contains(&artifact.manifest.id),
            "local checkpoint deletion requires reconciliation"
        );
        let parent = self.home.join("restored");
        std::fs::create_dir_all(&parent)?;
        let destination = parent.join(&artifact.manifest.id);
        if destination.exists() && artifact_io::verify(&destination, &artifact.manifest).is_err() {
            std::fs::remove_dir_all(&destination)?;
        }
        if !destination.exists() {
            let stage = parent.join(format!("pending-{}", artifact.manifest.id));
            if stage.exists() {
                std::fs::remove_dir_all(&stage)?;
            }
            let mut errors = Vec::new();
            for target in &artifact.targets {
                if !artifact
                    .copies
                    .get(&format!("storage/{target}"))
                    .is_some_and(|c| c.phase == CopyPhase::Ready)
                {
                    continue;
                }
                let store = view
                    .stores
                    .get(target)
                    .context("storage target disappeared")?;
                match artifact_io::fetch(
                    &self.client.endpoint,
                    &store.peer,
                    &self.config.cluster,
                    &artifact.manifest,
                    &stage,
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
                stage.is_dir() && errors.is_empty(),
                "waiting for checkpoint backup restore: {}",
                errors.join("; ")
            );
            for file in &cp.content.files {
                cp.content.verify_file(&stage, file)?;
            }
            std::fs::rename(&stage, &destination)?;
            std::fs::File::open(&parent)?.sync_all()?;
        }
        for file in &cp.content.files {
            cp.content.verify_file(&destination, file)?;
            blobs::import_named(
                &self.store,
                &destination.join(&file.path),
                file.hash,
                format!("checkpoint/{id}/{}", file.path),
            )
            .await?;
        }
        self.store.sync_db().await?;
        Ok(())
    }
}
