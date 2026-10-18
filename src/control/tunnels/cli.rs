use super::{agent, transport};
use crate::control::client::ApiRequest;
use anyhow::Result;
use clap::Subcommand;
use std::path::PathBuf;

#[derive(Subcommand)]
pub enum Command {
    /// Read the committed service registry and its revision.
    Status,
    /// Atomically replace services, routes and permissions using a topology JSON file.
    Apply {
        topology: PathBuf,
        #[arg(long)]
        revision: u64,
        #[arg(long)]
        request_id: Option<String>,
    },
    /// Prepare an independent adapter identity. Print its public endpoint descriptor.
    Prepare {
        #[arg(long)]
        adapter_home: PathBuf,
        #[arg(long)]
        adapter: String,
        #[arg(long)]
        authority: PathBuf,
        /// JSON with a stable UDP bind address and explicit relay URLs.
        #[arg(long)]
        network: PathBuf,
    },
    Run {
        #[arg(long)]
        adapter_home: PathBuf,
    },
    /// Offline, explicit trust transition after fencing the old authority.
    AcceptRecovery {
        #[arg(long)]
        adapter_home: PathBuf,
        manifest: PathBuf,
    },
}
pub async fn run(command: Command) -> Result<Option<ApiRequest>> {
    Ok(match command {
        Command::Status => Some(ApiRequest::Tunnels),
        Command::Apply {
            topology,
            revision,
            request_id,
        } => Some(ApiRequest::ReplaceTunnels {
            request_id: request_id
                .unwrap_or_else(|| format!("request-{:032x}", rand::random::<u128>())),
            expected_revision: revision,
            topology: serde_json::from_slice(&std::fs::read(topology)?)?,
        }),
        Command::Prepare {
            adapter_home,
            adapter,
            authority,
            network,
        } => {
            let network: transport::Network = serde_json::from_slice(&std::fs::read(network)?)?;
            anyhow::ensure!(
                network.bind.is_none_or(|b| b.port() != 0),
                "prepared adapters require stable UDP ports"
            );
            let descriptor = agent::Agent::prepare(
                &adapter_home,
                adapter,
                serde_json::from_slice(&std::fs::read(authority)?)?,
                network,
            )
            .await?;
            println!("{}", serde_json::to_string_pretty(&descriptor)?);
            None
        }
        Command::Run { adapter_home } => {
            agent::Agent::start(&adapter_home).await?.run().await?;
            None
        }
        Command::AcceptRecovery {
            adapter_home,
            manifest,
        } => {
            agent::accept_recovery(
                &adapter_home,
                &serde_json::from_slice(&std::fs::read(manifest)?)?,
            )?;
            println!("{{\"accepted\":true}}");
            None
        }
    })
}
