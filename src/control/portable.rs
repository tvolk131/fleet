//! Job-owned execution dependencies and explicit guest/runtime compatibility.
use super::{
    State,
    artifact::{self, Kind, Manifest},
    queue::Job,
    workload::Workload,
};
pub use crate::instances::runtime::{Platform, Runtime};
use anyhow::{Context, Result, ensure};
use std::collections::BTreeMap;

pub fn runtime(workload: &Workload) -> Option<&Runtime> {
    workload.vm()?.runtime.as_ref()
}

impl State {
    pub fn input_bundle(&self, job: &Job) -> Option<&artifact::Artifact> {
        job.input_bundle
            .as_ref()
            .and_then(|id| self.storage.artifacts.get(id))
    }
    pub fn durable_artifact(&self, a: &artifact::Artifact) -> bool {
        !a.deleted
            && !a.targets.is_empty()
            && a.targets.iter().all(|id| {
                self.storage_readable(id)
                    && a.copies.get(&format!("storage/{id}")).is_some_and(|c| {
                        c.authority == self.cluster && c.phase == artifact::CopyPhase::Ready
                    })
            })
    }
    pub fn portable_ready(&self, job: &Job) -> Result<()> {
        if runtime(&job.workload).is_none() {
            return Ok(());
        }
        let bundle = self.input_bundle(job).context("job input bundle missing")?;
        ensure!(
            self.durable_artifact(bundle),
            "waiting for verified input storage copies"
        );
        if let Some(id) = &job.resume_from {
            let a = self
                .storage
                .artifacts
                .values()
                .find(|a| a.manifest.kind == Kind::Checkpoint(id.clone()))
                .context("checkpoint artifact missing")?;
            ensure!(
                self.durable_artifact(a),
                "waiting for verified checkpoint storage copies"
            );
            ensure!(
                bundle.manifest.bytes() + a.manifest.bytes() <= crate::vm_job::MAX_TOTAL,
                "original inputs plus checkpoint exceed the 4 GiB VM staging limit"
            );
        }
        Ok(())
    }
}

pub fn validate_inputs(id: &str, workload: &Workload, manifest: Option<&Manifest>) -> Result<()> {
    let Some(runtime) = runtime(workload) else {
        ensure!(
            manifest.is_none(),
            "input bundles require an explicit portable runtime"
        );
        return Ok(());
    };
    runtime.validate()?;
    let Workload::Job { spec, .. } = workload else {
        anyhow::bail!("portable input bundle requires a job")
    };
    let manifest = manifest.context("portable job requires an immutable input bundle")?;
    manifest.validate()?;
    ensure!(
        manifest.origin == artifact::Origin::Job(id.into()) && manifest.kind == Kind::Inputs,
        "input bundle belongs to another job"
    );
    let files: BTreeMap<_, _> = manifest
        .files
        .iter()
        .map(|f| (f.path.as_str(), f.hash))
        .collect();
    let inputs: BTreeMap<_, _> = spec
        .inputs
        .iter()
        .map(|(p, i)| (p.as_str(), i.hash))
        .collect();
    ensure!(files == inputs, "input bundle differs from job inputs");
    Ok(())
}
