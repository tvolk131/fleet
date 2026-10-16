//! Checkpoint metadata is committed only after an authenticated worker reports durable verified files.
use super::{
    Reply, State,
    ids::{AttemptId, JobId},
    queue::JobPhase,
    workload::*,
};
use crate::checkpoint::Captured;
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Checkpoint {
    pub id: String,
    pub job: JobId,
    pub attempt: AttemptId,
    pub worker: String,
    pub parent: Option<String>,
    pub content: Captured,
}
impl Checkpoint {
    pub fn new(a: &Assignment, content: Captured) -> Result<Self> {
        let attempt = a
            .attempt
            .as_ref()
            .context("checkpoint requires a job attempt")?;
        let id = blake3::hash(&serde_json::to_vec(&(
            &attempt.job,
            &a.id,
            &a.worker,
            &content.id,
        ))?)
        .to_hex()
        .to_string();
        Ok(Self {
            id,
            job: attempt.job.clone(),
            attempt: a.id.clone().into(),
            worker: a.worker.clone(),
            parent: attempt.checkpoint.clone(),
            content,
        })
    }
    pub fn validate(&self, a: &Assignment) -> Result<()> {
        self.content.validate()?;
        ensure!(
            *self == Self::new(a, self.content.clone())?,
            "checkpoint provenance mismatch"
        );
        let Workload::Job { spec, .. } = &a.workload else {
            anyhow::bail!("checkpoint requires a job");
        };
        let policy = spec
            .checkpoint
            .as_ref()
            .context("job did not declare checkpoints")?;
        ensure!(
            spec.vm.is_some()
                && crate::jobs::matches(&policy.manifests, &self.content.source)
                && policy.manifests.split('/').count() == self.content.source.split('/').count(),
            "undeclared checkpoint manifest"
        );
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Ready {
    pub incarnation: String,
    pub checkpoint: Checkpoint,
}

impl State {
    pub(crate) fn attempt_workload(&self, job: &super::queue::Job) -> Result<Workload> {
        let mut workload = job.workload.clone();
        if let Some(id) = &job.resume_from {
            ensure!(!self.checkpoint_deleted(id), "checkpoint was deleted");
            let c = self.checkpoints.get(id).context("checkpoint is missing")?;
            ensure!(c.job == job.id, "checkpoint belongs to another job");
            let Workload::Job { spec, .. } = &mut workload else {
                anyhow::bail!("not a job");
            };
            let policy = spec.checkpoint.as_ref().context("no checkpoint policy")?;
            spec.argv = policy.command(u32::try_from(job.attempts.len() + 1)?);
            for file in &c.content.files {
                let path = format!("{}/{}", crate::checkpoint::INPUT_ROOT, file.path);
                ensure!(
                    spec.inputs
                        .insert(
                            path,
                            crate::jobs::Input {
                                hash: file.hash,
                                executable: false
                            }
                        )
                        .is_none(),
                    "checkpoint staging collides with original inputs"
                );
            }
            spec.validate()?;
        }
        Ok(workload)
    }

    pub fn checkpoint_ready(&mut self, principal: &str, ready: &Ready) -> Reply {
        let c = &ready.checkpoint;
        let check = || -> Result<()> {
            let w = self.workers.get(&c.worker).context("unknown worker")?;
            ensure!(
                w.peer.identity.to_string() == principal && w.incarnation == ready.incarnation,
                "checkpoint worker identity mismatch"
            );
            ensure!(
                !self.worker_recovery_required(&c.worker),
                "worker must reconcile before reporting checkpoints"
            );
            let a = self
                .assignments
                .get(c.attempt.as_ref())
                .context("unknown checkpoint attempt")?;
            c.validate(a)?;
            if let Some(old) = self.checkpoints.get(&c.id) {
                ensure!(old == c, "immutable checkpoint changed");
            } else {
                ensure!(
                    matches!(&a.workload, Workload::Job { spec, .. } if spec.checkpoint.as_ref().is_some_and(|p| p.retention.is_some()))
                        || self
                            .checkpoints
                            .values()
                            .filter(|old| old.attempt == c.attempt)
                            .count()
                            < crate::checkpoint::MAX_CHECKPOINTS,
                    "checkpoint retention limit reached"
                );
            }
            Ok(())
        };
        if let Err(e) = check() {
            return Reply::rejected(&e.to_string());
        }
        let manifest = match super::artifact::Manifest::new(
            c.worker.clone(),
            c.attempt.to_string(),
            super::artifact::Kind::Checkpoint(c.id.clone()),
            c.content.files.clone(),
        ) {
            Ok(m) => m,
            Err(e) => return Reply::rejected(&e.to_string()),
        };
        self.checkpoints.insert(c.id.clone(), c.clone());
        self.register_artifact(manifest);
        Reply::Recorded
    }

    pub fn suspend_job(&mut self, id: &str, revision: u64, checkpoint: &str) -> Reply {
        let check = || -> Result<(String, String)> {
            ensure!(
                !self.checkpoint_deleted(checkpoint),
                "checkpoint was deleted"
            );
            let job = self.jobs.get(id).context("unknown job")?;
            ensure!(job.revision == revision, "job revision conflict");
            ensure!(
                job.phase == JobPhase::Active,
                "only an active job can be suspended"
            );
            let a = job.current(self).context("job has no attempt")?;
            let c = self
                .checkpoints
                .get(checkpoint)
                .context("checkpoint is not durably registered")?;
            ensure!(
                c.job.as_ref() == id && c.attempt.as_ref() == a.id && c.worker == a.worker,
                "checkpoint does not belong to the current attempt"
            );
            ensure!(
                !a.terminal() && a.desired == Desired::Job(JobDesired::Running),
                "attempt is already stopping or finished"
            );
            ensure!(
                !self.worker_recovery_required(&a.worker)
                    && !self
                        .recovery
                        .as_ref()
                        .is_some_and(|r| r.held.contains(&a.id)),
                "worker recovery reconciliation required"
            );
            Ok((a.id.clone(), c.id.clone()))
        };
        let (attempt, checkpoint) = match check() {
            Ok(v) => v,
            Err(e) => return Reply::rejected(&e.to_string()),
        };
        let a = self
            .assignments
            .get_mut(&attempt)
            .expect("validated attempt");
        a.desired = Desired::Job(JobDesired::Suspended);
        a.suspend_checkpoint = Some(checkpoint.clone());
        a.revision += 1;
        let job = self.jobs.get_mut(id).expect("validated job");
        job.phase = JobPhase::Suspending;
        job.resume_from = Some(checkpoint);
        job.revision += 1;
        Reply::Accepted {
            job: id.into(),
            revision: job.revision,
        }
    }

    pub fn resume_job(&mut self, id: &str, revision: u64) -> Reply {
        let check =
            || -> Result<()> {
                let job = self.jobs.get(id).context("unknown job")?;
                ensure!(job.revision == revision, "job revision conflict");
                ensure!(job.phase == JobPhase::Suspended, "job is not suspended");
                let a = job.current(self).context("job has no attempt")?;
                ensure!(
                    a.observed.as_ref().is_some_and(|(r, o)| *r == a.revision
                        && o.phase == WorkPhase::Job(AttemptPhase::Suspended)),
                    "old attempt has not confirmed cleanup"
                );
                let c = self
                    .checkpoints
                    .get(
                        job.resume_from
                            .as_deref()
                            .context("no selected checkpoint")?,
                    )
                    .context("checkpoint is missing")?;
                ensure!(
                    a.suspend_checkpoint.as_deref() == Some(c.id.as_str())
                        && c.attempt.as_ref() == a.id,
                    "suspension checkpoint mismatch"
                );
                ensure!(
                    self.recovery_allows_work(&c.worker)
                        && !self
                            .recovery
                            .as_ref()
                            .is_some_and(|r| r.held.contains(&a.id)),
                    "recovery reconciliation required"
                );
                self.attempt_workload(job)?;
                Ok(())
            };
        if let Err(e) = check() {
            return Reply::rejected(&e.to_string());
        }
        let job = self.jobs.get_mut(id).expect("validated job");
        job.phase = JobPhase::Queued;
        job.revision += 1;
        Reply::Accepted {
            job: id.into(),
            revision: job.revision,
        }
    }
}
