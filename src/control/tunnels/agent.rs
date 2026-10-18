//! Optional service role with its own identity, authority binding and durable policy.
use super::{Adapter, Topology, View, transport};
use crate::control::{
    Client, Config as Authority,
    network::{Answer, Message},
};
use anyhow::{Context, Result, ensure};
use iroh::{Endpoint, EndpointId};
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::{sync::watch, task::JoinHandle};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Config {
    pub adapter: String,
    pub identity: EndpointId,
    pub authority: Authority,
    pub network: transport::Network,
    #[serde(default)]
    pub accepted_recovery: Option<Box<crate::control::recovery::Manifest>>,
}
pub struct Agent {
    home: PathBuf,
    config: Config,
    endpoint: Endpoint,
    client: Client,
    policy: watch::Sender<View>,
    task: Option<JoinHandle<()>>,
    _lock: crate::instances::Lock,
}
impl Agent {
    pub async fn prepare(
        home: &Path,
        adapter: String,
        authority: Authority,
        network: transport::Network,
    ) -> Result<Adapter> {
        ensure!(!home.exists(), "tunnel home must be new");
        ensure!(super::name(&adapter), "invalid adapter ID");
        ensure!(
            !authority.cluster.is_empty()
                && !authority.peers.is_empty()
                && !authority.administrators.is_empty(),
            "authority configuration required"
        );
        // Creating the home exclusively prevents concurrent preparation from replacing keys.
        std::fs::create_dir(home)?;
        let key = crate::keys::load_or_create(&home.join("node.key"))?;
        let identity = key.public();
        let ep = transport::endpoint(key, &network).await?;
        if !network.relays.is_empty() {
            tokio::time::timeout(Duration::from_secs(20), ep.online()).await?;
        }
        let descriptor = Adapter {
            endpoint: ep.addr(),
        };
        ep.close().await;
        ensure!(
            !descriptor.endpoint.addrs.is_empty(),
            "no tunnel contact addresses"
        );
        crate::instances::write(
            &home.join("tunnel.json"),
            &Config {
                adapter,
                identity,
                authority,
                network,
                accepted_recovery: None,
            },
        )?;
        crate::instances::write(&home.join("adapter.json"), &descriptor)?;
        Ok(descriptor)
    }
    pub async fn start(home: &Path) -> Result<Self> {
        let lock = crate::instances::try_lock(&home.join("tunnel.lock"))?
            .context("tunnel adapter already running")?;
        let config: Config = serde_json::from_slice(&std::fs::read(home.join("tunnel.json"))?)?;
        ensure!(
            home.join("node.key").is_file(),
            "missing adapter identity; do not replace automatically"
        );
        let key = crate::keys::load_or_create(&home.join("node.key"))?;
        ensure!(
            key.public() == config.identity
                && !config.authority.administrators.contains(&config.identity),
            "adapter identity mismatch or administrator key reuse"
        );
        let cached = home.join("policy.json");
        let view: View = if cached.exists() {
            serde_json::from_slice(&std::fs::read(cached)?)?
        } else {
            Self::empty(&config)
        };
        Self::validate_view(&config, &view)?;
        // Control traffic has its own direct transport. Forcing service traffic
        // through a relay must not change the existing control-plane transport.
        let mut client = Client::new(
            key.clone(),
            config.authority.cluster.clone(),
            config.authority.peers.clone(),
        )
        .await?;
        let endpoint = transport::endpoint(key, &config.network).await?;
        client.use_cache(crate::control::discovery::incarnation_cache(
            &home.join("tunnel.json"),
            &config.authority.cluster,
        ))?;
        let (policy, receiver) = watch::channel(view);
        let task = tokio::spawn(transport::run(endpoint.clone(), receiver));
        Ok(Self {
            home: home.into(),
            config,
            endpoint,
            client,
            policy,
            task: Some(task),
            _lock: lock,
        })
    }
    fn empty(config: &Config) -> View {
        View {
            cluster: config.authority.cluster.clone(),
            adapter: config.adapter.clone(),
            identity: config.identity,
            revision: 0,
            topology: Topology::default(),
        }
    }
    fn validate_view(config: &Config, view: &View) -> Result<()> {
        ensure!(
            view.cluster == config.authority.cluster
                && view.adapter == config.adapter
                && view.identity == config.identity,
            "cached policy authority/identity mismatch"
        );
        view.topology.validate()?;
        ensure!(
            view.topology
                .adapters
                .get(&config.adapter)
                .is_none_or(|a| a.endpoint.id == config.identity),
            "policy adapter key mismatch"
        );
        Ok(())
    }
    /// Controller unavailability preserves the cached policy. A malformed or
    /// unpersistable new policy stops this role instead of silently ignoring it.
    pub async fn tick(&mut self) -> Result<()> {
        let answer = self
            .client
            .call(
                Message::TunnelRead {
                    adapter: self.config.adapter.clone(),
                },
                Duration::from_secs(5),
            )
            .await;
        let view = match answer {
            Ok(Answer::TunnelRead(Ok(view))) => view,
            other => {
                tracing::debug!(
                    ?other,
                    "tunnel controller unavailable; keeping committed policy"
                );
                return Ok(());
            }
        };
        Self::validate_view(&self.config, &view)?;
        let old = self.policy.borrow().clone();
        ensure!(
            view.revision >= old.revision,
            "tunnel policy rollback refused"
        );
        if view != old {
            ensure!(
                view.revision > old.revision || old.topology == Topology::default(),
                "different tunnel policy at the same revision"
            );
            crate::instances::write(&self.home.join("policy.json"), &view)?;
            self.policy.send(view).context("tunnel transport stopped")?;
        }
        Ok(())
    }
    pub async fn run(mut self) -> Result<()> {
        let result = loop {
            tokio::select! {
                _ = crate::shutdown_signal() => break Ok(()),
                result = self.tick() => { if let Err(e) = result { break Err(e); } }
            }
            tokio::select! { _ = crate::shutdown_signal() => break Ok(()), _ = tokio::time::sleep(Duration::from_secs(2)) => {} }
        };
        self.shutdown().await;
        result
    }
    pub async fn shutdown(mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
            let _ = task.await;
        }
        self.endpoint.close().await;
        self.client.endpoint.close().await;
    }
}
impl Drop for Agent {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

/// The operator must fence the old authority/adapters. Cache replacement is
/// explicit and offline; an interrupted trust transition fails closed at startup.
pub fn accept_recovery(home: &Path, manifest: &crate::control::recovery::Manifest) -> Result<()> {
    let _lock = crate::instances::try_lock(&home.join("tunnel.lock"))?
        .context("stop tunnel adapter before recovery")?;
    let path = home.join("tunnel.json");
    let mut config: Config = serde_json::from_slice(&std::fs::read(&path)?)?;
    if config.accepted_recovery.as_deref() == Some(manifest) {
        return Ok(());
    }
    manifest.verify(&config.authority.cluster, &config.authority.administrators)?;
    config.authority = manifest.plan.config.clone();
    config.accepted_recovery = Some(Box::new(manifest.clone()));
    crate::instances::write(&home.join("policy.json"), &Agent::empty(&config))?;
    crate::instances::write(&path, &config)?;
    Ok(())
}
