//! A host-owned binding to one VM. The guest never receives the adapter key.
use super::View;
use crate::{
    control::{Config as Authority, runtime::WorkerConfig},
    instances::{self, Desired, State},
};
use anyhow::{Context, Result, ensure};
use fleet_guest::tunnel::{self, Channel, Handle, Incoming};
use iroh::EndpointId;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};
use tokio::{io::AsyncReadExt, process::Child};

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Attachment {
    #[default]
    Host,
    Tart {
        worker_home: PathBuf,
        worker_identity: EndpointId,
        worker_incarnation: String,
        instance: String,
        /// Fingerprint of immutable identity/spec, never a mutable desired revision.
        fingerprint: String,
    },
}
impl Attachment {
    pub fn lock(&self) -> Result<Option<instances::Lock>> {
        let Self::Tart {
            worker_home,
            instance,
            ..
        } = self
        else {
            return Ok(None);
        };
        let path = instances::directory(&worker_home.join("backend"), instance)?
            .join("service-attachment.lock");
        Ok(Some(
            instances::try_lock(&path)?.context("VM already has a running service adapter")?,
        ))
    }
    pub fn is_host(&self) -> bool {
        matches!(self, Self::Host)
    }
    pub fn prepare(worker_home: &Path, instance: &str, authority: &Authority) -> Result<Self> {
        let worker_home = worker_home.canonicalize()?;
        let config: WorkerConfig =
            serde_json::from_slice(&std::fs::read(worker_home.join("worker.json"))?)?;
        ensure!(
            config.cluster == authority.cluster
                && config.administrators == authority.administrators,
            "VM worker belongs to a different authority"
        );
        let record = instances::info(&worker_home.join("backend"), instance)?.record;
        ensure!(
            record.desired != Desired::Deleted,
            "cannot attach a deleted VM"
        );
        // Jobs have disposable attempt VMs. Automatic per-attempt attachments need
        // an explicit job policy; do not accidentally make them persistent identities.
        ensure!(
            record.job.is_none(),
            "attach persistent instances; disposable job attachments are not supported yet"
        );
        Ok(Self::Tart {
            worker_home,
            worker_identity: config.registration.peer.identity,
            worker_incarnation: config.registration.incarnation,
            instance: instance.into(),
            fingerprint: fingerprint(&record)?,
        })
    }
    pub fn check(&self, cluster: &str) -> Result<Option<u64>> {
        let Self::Tart {
            worker_home,
            worker_identity,
            worker_incarnation,
            instance,
            fingerprint: expected,
        } = self
        else {
            return Ok(None);
        };
        let config: WorkerConfig =
            serde_json::from_slice(&std::fs::read(worker_home.join("worker.json"))?)?;
        ensure!(
            config.cluster == cluster
                && config.registration.peer.identity == *worker_identity
                && config.registration.incarnation == *worker_incarnation,
            "VM worker authority/identity changed; rebind explicitly"
        );
        let i = instances::info(&worker_home.join("backend"), instance)?;
        ensure!(
            fingerprint(&i.record)? == *expected,
            "VM immutable identity changed"
        );
        ensure!(
            i.record.desired == Desired::Running
                && i.observed.state == State::Running
                && i.record.revision == i.observed.revision,
            "VM attachment is not running at its desired revision"
        );
        Ok(Some(i.record.revision))
    }
    pub async fn start(&self, view: &View) -> Result<Guest> {
        let revision = self
            .check(&view.cluster)?
            .context("expected VM attachment")?;
        let Self::Tart {
            worker_home,
            instance,
            ..
        } = self
        else {
            unreachable!()
        };
        let routes: Vec<_> = view
            .topology
            .routes
            .iter()
            .map(|r| (r.service.clone(), r.listen))
            .collect();
        let targets: Vec<_> = view
            .topology
            .services
            .iter()
            .filter(|(_, s)| s.owner == view.adapter)
            .map(|(id, s)| (id.clone(), s.target))
            .collect();
        let config = tunnel::Config {
            routes: routes.iter().map(|r| r.1).collect(),
            targets: targets.iter().map(|t| t.1).collect(),
        };
        config.validate()?;
        let backend = worker_home.join("backend");
        let id = instance.clone();
        // Helper installation is bounded and operates only through the fixed Tart
        // executable and generated guest path. No guest-provided host command runs.
        let mut command =
            tokio::task::spawn_blocking(move || instances::guest::command(&backend, &id)).await??;
        ensure!(
            self.check(&view.cluster)? == Some(revision),
            "VM changed during attachment setup"
        );
        command
            .args(["--services", &serde_json::to_string(&config)?])
            .stdin(Stdio::piped())
            .stderr(Stdio::piped());
        let mut command = tokio::process::Command::from(command);
        command.kill_on_drop(true);
        let mut child = command.spawn()?;
        let mut read = child.stdout.take().context("missing guest output")?;
        let write = child.stdin.take().context("missing guest input")?;
        let stderr = child.stderr.take().context("missing guest diagnostics")?;
        let mut ready = vec![0; tunnel::READY.len()];
        let readiness = tokio::time::timeout(tunnel::DEADLINE, read.read_exact(&mut ready)).await;
        if !matches!(&readiness, Ok(Ok(_))) {
            // A guest reboot can erase /tmp without changing desired state.
            // Retry installation on the next attempt; never fall back to host sockets.
            let _ = std::fs::remove_file(
                instances::directory(&worker_home.join("backend"), instance)?
                    .join("guest-helper.json"),
            );
            let _ = child.kill().await;
            let mut detail = Vec::new();
            let _ = tokio::time::timeout(
                Duration::from_secs(1),
                stderr.take(256).read_to_end(&mut detail),
            )
            .await;
            let detail: String = String::from_utf8_lossy(&detail)
                .chars()
                .filter(|c| !c.is_control())
                .take(128)
                .collect();
            anyhow::bail!("guest service setup failed: {detail}; {readiness:?}");
        }
        readiness.context("guest service attachment readiness timed out")??;
        ensure!(
            ready == tunnel::READY,
            "invalid guest service attachment greeting"
        );
        let (handle, incoming, channel) = Channel::new(true, routes.len());
        Ok(Guest {
            handle,
            incoming: Some(incoming),
            channel: Some((channel, read, write)),
            stderr: Some(stderr),
            child,
            attachment: self.clone(),
            cluster: view.cluster.clone(),
            revision,
            routes: routes.into_iter().map(|r| r.0).collect(),
            targets: targets
                .into_iter()
                .enumerate()
                .map(|(i, t)| (t.0, i as u32))
                .collect(),
        })
    }
}
fn fingerprint(record: &instances::Record) -> Result<String> {
    Ok(blake3::hash(&serde_json::to_vec(&(
        &record.id,
        &record.spec,
        &record.owner,
        record.created_at,
        &record.job,
    ))?)
    .to_hex()
    .to_string())
}
pub struct Guest {
    pub stderr: Option<tokio::process::ChildStderr>,
    pub handle: Handle,
    pub incoming: Option<Incoming>,
    pub channel: Option<(
        Channel,
        tokio::process::ChildStdout,
        tokio::process::ChildStdin,
    )>,
    child: Child,
    pub attachment: Attachment,
    pub cluster: String,
    pub revision: u64,
    pub routes: Vec<String>,
    pub targets: BTreeMap<String, u32>,
}
impl Guest {
    pub async fn shutdown(&mut self) {
        // Join the host Tart process before acknowledging enforcement. A guest
        // can retain arbitrary local listeners, but its old host channel is gone.
        let _ = self.child.kill().await;
        let _ = self.child.wait().await;
    }
    pub async fn monitor(attachment: Attachment, cluster: String, revision: u64) -> Result<()> {
        loop {
            ensure!(
                attachment.check(&cluster)? == Some(revision),
                "VM incarnation changed; reconnecting attachment"
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
}
