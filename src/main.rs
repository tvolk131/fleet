//! Normal Fleet interface: every global mutation goes through the replicated control plane.
use anyhow::{Context, Result, ensure};
use clap::{Parser, Subcommand};
use fleet::control::{
    client::{self, ApiRequest},
    service::{self, ServiceConfig},
};
use std::{collections::BTreeSet, io::Read, path::PathBuf};

#[derive(Parser)]
#[command(version, about = "Fleet: replicated job queue and VM management")]
struct Args {
    /// Client identity/configuration and request journal; separate from worker and replica homes.
    #[arg(long, global = true)]
    home: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum VersionCommand {
    Prepare {
        target: u32,
        #[arg(long)]
        revision: u64,
        #[arg(long)]
        request_id: Option<String>,
    },
    Activate {
        #[arg(long)]
        revision: u64,
        #[arg(long)]
        request_id: Option<String>,
    },
    Abort {
        #[arg(long)]
        revision: u64,
        #[arg(long)]
        request_id: Option<String>,
    },
}

#[derive(Subcommand)]
enum Command {
    /// Authenticated host-local TCP services over iroh.
    Tunnels {
        #[command(subcommand)]
        action: fleet::control::tunnels::cli::Command,
    },
    /// Cluster generation and live replica compatibility diagnostics.
    Versions,
    /// Prepare, activate, or abort a rolling cluster generation change.
    Upgrade {
        #[command(subcommand)]
        action: VersionCommand,
    },
    /// Cluster preparation, membership, recovery and low-level diagnostics.
    Cluster {
        #[command(subcommand)]
        action: fleet::control::admin::Command,
    },
    /// Configure a client from an administrator-issued identity and cluster configuration.
    Client {
        #[command(subcommand)]
        action: ClientCommand,
    },
    Service {
        #[command(subcommand)]
        action: ServiceCommand,
    },
    Storage {
        #[command(subcommand)]
        action: StorageCommand,
    },
    Artifacts {
        #[arg(long)]
        attempt: Option<String>,
    },
    FetchArtifact {
        id: String,
        destination: PathBuf,
    },
    ArtifactRetention {
        id: String,
        #[arg(long)]
        revision: u64,
        #[arg(long)]
        pinned: bool,
        #[arg(long)]
        expires_at: Option<u64>,
        #[arg(long)]
        request_id: Option<String>,
    },
    DeleteArtifact {
        id: String,
        #[arg(long)]
        revision: u64,
        #[arg(long)]
        request_id: Option<String>,
    },
    ExpireArtifacts {
        #[arg(long)]
        as_of: u64,
        #[arg(long)]
        request_id: Option<String>,
    },
    Status,
    /// Change membership using prepared public peer descriptors; requires current quorum.
    Replicas {
        #[arg(long)]
        add: Vec<PathBuf>,
        #[arg(long)]
        remove: Vec<u64>,
        #[arg(long)]
        request_id: Option<String>,
    },
    Operation {
        id: String,
    },
    AcceptRecovery {
        manifest: PathBuf,
    },
    ResumeRecovery {
        #[arg(long)]
        unavailable_worker: Vec<String>,
        #[arg(long)]
        request_id: Option<String>,
    },
    Queue,
    Job {
        id: String,
    },
    Workers,
    Instances,
    Submit {
        job: PathBuf,
        #[arg(long)]
        id: Option<String>,
        #[arg(long)]
        request_id: Option<String>,
        #[arg(long)]
        worker: Option<String>,
        #[arg(long, default_value_t = 1024)]
        memory_mb: u32,
        #[arg(long)]
        gpu: bool,
        #[arg(long, default_value_t = 0)]
        priority: i32,
        #[arg(long)]
        after: Vec<String>,
        #[arg(long)]
        requires: Vec<String>,
    },
    Cancel {
        id: String,
        #[arg(long)]
        request_id: Option<String>,
        #[arg(long)]
        revision: Option<u64>,
    },
    /// Stop at a verified checkpoint. Work performed after that checkpoint is redone on resume.
    Suspend {
        id: String,
        #[arg(long, default_value = "latest")]
        checkpoint: String,
        #[arg(long)]
        request_id: Option<String>,
        #[arg(long)]
        revision: Option<u64>,
    },
    /// Queue another attempt after confirmed VM cleanup; portable jobs may select another eligible worker.
    Resume {
        #[arg(long)]
        worker: Option<String>,
        id: String,
        #[arg(long)]
        request_id: Option<String>,
        #[arg(long)]
        revision: Option<u64>,
    },
    Checkpoints {
        id: String,
    },
    FetchCheckpoint {
        id: String,
        destination: PathBuf,
    },
    Attempts {
        id: String,
    },
    /// Diagnostic retention, archive copies, gaps, and all execution attempts.
    History {
        id: String,
    },
    /// Read a page of captured JSON-lines metric history using a stable byte offset.
    Metrics {
        id: String,
        #[arg(long)]
        attempt: Option<u32>,
        #[arg(long, default_value_t = 0)]
        from: u64,
    },
    /// Replace diagnostic retention using a JSON policy file and the current retention revision.
    HistoryRetention {
        id: String,
        #[arg(long)]
        policy: PathBuf,
        #[arg(long)]
        revision: u64,
        #[arg(long)]
        request_id: Option<String>,
    },
    Logs {
        id: String,
        #[arg(long)]
        attempt: Option<u32>,
        #[arg(long)]
        stderr: bool,
        #[arg(long, default_value_t = 0)]
        from: u64,
        #[arg(long)]
        follow: bool,
    },
    Outputs {
        id: String,
        #[arg(long)]
        attempt: Option<u32>,
    },
    Fetch {
        id: String,
        #[arg(long)]
        attempt: Option<u32>,
        destination: PathBuf,
    },
    Stage {
        id: Option<String>,
        #[arg(long)]
        watch: bool,
    },
    Requests,
    Retry {
        request_id: String,
    },
    Enroll {
        registration: PathBuf,
        #[arg(long)]
        request_id: Option<String>,
    },
    Drain {
        worker: String,
        #[arg(long)]
        resume: bool,
        #[arg(long)]
        request_id: Option<String>,
    },
    Instance {
        #[command(subcommand)]
        action: InstanceCommand,
    },
    /// A single JSON request on stdin and a JSON response envelope on stdout for automation.
    Api,
    #[cfg(feature = "gui")]
    Gui,
    #[command(hide = true)]
    Supervise {
        dir: PathBuf,
    },
    #[command(hide = true)]
    InstanceController {
        instance_home: PathBuf,
        id: String,
    },
    #[command(hide = true)]
    InstanceOperation {
        instance_home: PathBuf,
        id: String,
        operation: String,
    },
}
#[derive(Subcommand)]
enum ClientCommand {
    Import {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        key: PathBuf,
    },
}
#[derive(Subcommand)]
enum StorageCommand {
    /// Inspect lifecycle, copy progress and the inventory fingerprint before retirement or loss declaration.
    Plan {
        storage: String,
    },
    /// Drain future placement and copy retained artifacts to a separately enrolled replacement.
    Replace {
        storage: String,
        replacement: String,
        #[arg(long)]
        revision: u64,
        #[arg(long)]
        request_id: Option<String>,
    },
    /// Authorize old-copy cleanup only after every retained artifact has a verified replacement.
    Retire {
        storage: String,
        #[arg(long)]
        revision: u64,
        #[arg(long)]
        inventory: String,
        #[arg(long)]
        request_id: Option<String>,
    },
    /// Revoke the identity after all old-copy deletions are acknowledged.
    FinishRetirement {
        storage: String,
        #[arg(long)]
        revision: u64,
        #[arg(long)]
        request_id: Option<String>,
    },
    /// Permanently revoke an unavailable agent, without claiming its bytes were erased or recovered.
    DeclareLost {
        storage: String,
        replacement: String,
        #[arg(long)]
        revision: u64,
        #[arg(long)]
        inventory: String,
        #[arg(long)]
        unavailable_artifact: Vec<String>,
        #[arg(long)]
        request_id: Option<String>,
    },
    /// After inspecting recovered retention/copy inventory, authorize cleanup; named unavailable locations remain quarantined.
    ReviewRecovery {
        #[arg(long)]
        catalog_fingerprint: String,
        #[arg(long)]
        unavailable_location: Vec<String>,
        #[arg(long)]
        request_id: Option<String>,
    },
    Status,
    Enroll {
        registration: PathBuf,
        #[arg(long)]
        request_id: Option<String>,
    },
    Policy {
        #[arg(long)]
        history: Vec<String>,
        #[arg(long)]
        input: Vec<String>,
        #[arg(long)]
        revision: u64,
        #[arg(long)]
        checkpoint: Vec<String>,
        #[arg(long)]
        output: Vec<String>,
        #[arg(long)]
        request_id: Option<String>,
    },
}
#[derive(Subcommand)]
enum ServiceCommand {
    /// Validate a rendered systemd unit before the privileged installer uses it.
    ValidateUnit {
        #[arg(long)]
        file: PathBuf,
    },
    Configure {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        controller_home: Option<PathBuf>,
        #[arg(long)]
        worker_home: Option<PathBuf>,
        #[arg(long)]
        storage_home: Option<PathBuf>,
        #[arg(long)]
        client_home: Option<PathBuf>,
        #[arg(long)]
        tunnel_home: Option<PathBuf>,
        #[arg(long)]
        bootstrap: bool,
    },
    Run {
        #[arg(long)]
        config: PathBuf,
    },
    Render {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        kind: String,
        #[arg(long)]
        name: String,
        #[arg(long)]
        user: String,
        #[arg(long)]
        user_home: PathBuf,
        #[arg(long)]
        executable: Option<PathBuf>,
        #[arg(long)]
        output: PathBuf,
    },
}
#[derive(Subcommand)]
enum InstanceCommand {
    Create {
        id: String,
        #[arg(long)]
        worker: String,
        #[arg(long)]
        spec: PathBuf,
        #[arg(long)]
        request_id: Option<String>,
    },
    Start {
        id: String,
        #[arg(long)]
        request_id: Option<String>,
    },
    Stop {
        id: String,
        #[arg(long)]
        request_id: Option<String>,
    },
    Delete {
        id: String,
        #[arg(long)]
        request_id: Option<String>,
    },
}
fn request_id(id: Option<String>) -> String {
    id.unwrap_or_else(|| format!("request-{:032x}", rand::random::<u128>()))
}
fn print(value: &impl serde::Serialize) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}
fn home(path: Option<PathBuf>) -> Result<PathBuf> {
    Ok(path
        .or_else(|| std::env::var_os("FLEET_HOME").map(PathBuf::from))
        .unwrap_or(
            PathBuf::from(std::env::var_os("HOME").context("HOME missing; pass --home")?)
                .join(".fleet-client"),
        ))
}
fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();
    let args = Args::parse();
    #[cfg(feature = "gui")]
    if matches!(args.command, Command::Gui) {
        return fleet::gui::run(home(args.home)?).map_err(Into::into);
    }
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run(args))
}
async fn run(args: Args) -> Result<()> {
    let client_home = home(args.home)?;
    let api = match args.command {
        Command::Tunnels { action } => match fleet::control::tunnels::cli::run(action).await? {
            Some(api) => api,
            None => return Ok(()),
        },
        Command::Cluster { action } => return fleet::control::admin::run(action).await,
        Command::Client {
            action: ClientCommand::Import { config, key },
        } => {
            ensure!(key.is_file(), "existing client key required");
            ensure!(
                !client_home.exists() || std::fs::read_dir(&client_home)?.next().is_none(),
                "client home must be empty"
            );
            let config: fleet::control::Config = serde_json::from_slice(&std::fs::read(config)?)?;
            let secret = fleet::keys::load_or_create(&key)?;
            ensure!(
                config.administrators.contains(&secret.public()),
                "client key is not authorized by this configuration"
            );
            std::fs::create_dir_all(&client_home)?;
            std::fs::copy(key, client_home.join("node.key"))?;
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(
                client_home.join("node.key"),
                std::fs::Permissions::from_mode(0o600),
            )?;
            std::fs::File::open(client_home.join("node.key"))?.sync_all()?;
            fleet::instances::write(&client_home.join("config.json"), &config)?;
            return print(&serde_json::json!({"client_home":client_home,"cluster":config.cluster}));
        }
        Command::Service { action } => {
            return match action {
                ServiceCommand::ValidateUnit { file } => {
                    service::validate_unit(&std::fs::read_to_string(file)?)?;
                    print(&serde_json::json!({"valid":true}))
                }
                ServiceCommand::Configure {
                    config,
                    controller_home,
                    worker_home,
                    storage_home,
                    client_home,
                    tunnel_home,
                    bootstrap,
                } => {
                    ensure!(!config.exists(), "service configuration already exists");
                    let settings = ServiceConfig {
                        controller_home,
                        worker_home,
                        storage_home,
                        client_home,
                        tunnel_home,
                        bootstrap,
                    };
                    settings.validate()?;
                    fleet::instances::write(&config, &settings)?;
                    print(&settings)
                }
                ServiceCommand::Run { config } => service::run(&config).await,
                ServiceCommand::Render {
                    config,
                    kind,
                    name,
                    user,
                    user_home,
                    executable,
                    output,
                } => {
                    let text = service::render(
                        &kind,
                        &name,
                        &user,
                        &user_home,
                        &executable.unwrap_or(std::env::current_exe()?),
                        &config,
                    )?;
                    use std::io::Write;
                    let mut file = std::fs::OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .open(output)?;
                    file.write_all(text.as_bytes())?;
                    file.sync_all()?;
                    Ok(())
                }
            };
        }
        Command::Supervise { dir } => return fleet::supervise::run(&dir),
        Command::InstanceController { instance_home, id } => {
            return fleet::instances::run_controller(&instance_home, &id);
        }
        Command::InstanceOperation {
            instance_home,
            id,
            operation,
        } => return fleet::instances::guest::supervise(&instance_home, &id, &operation),
        Command::Storage { action } => match action {
            StorageCommand::Plan { storage } => ApiRequest::StoragePlan { storage },
            StorageCommand::Replace {
                storage,
                replacement,
                revision,
                request_id: rid,
            } => ApiRequest::ReplaceStorage {
                request_id: request_id(rid),
                storage,
                replacement,
                expected_revision: revision,
            },
            StorageCommand::Retire {
                storage,
                revision,
                inventory,
                request_id: rid,
            } => ApiRequest::RetireStorage {
                request_id: request_id(rid),
                storage,
                expected_revision: revision,
                expected_inventory: inventory,
            },
            StorageCommand::FinishRetirement {
                storage,
                revision,
                request_id: rid,
            } => ApiRequest::FinishStorageRetirement {
                request_id: request_id(rid),
                storage,
                expected_revision: revision,
            },
            StorageCommand::DeclareLost {
                storage,
                replacement,
                revision,
                inventory,
                unavailable_artifact,
                request_id: rid,
            } => ApiRequest::DeclareStorageLost {
                request_id: request_id(rid),
                storage,
                replacement,
                expected_revision: revision,
                expected_inventory: inventory,
                unavailable_artifacts: unavailable_artifact.into_iter().collect(),
            },
            StorageCommand::Status => ApiRequest::Storage,
            StorageCommand::ReviewRecovery {
                catalog_fingerprint,
                unavailable_location,
                request_id: rid,
            } => ApiRequest::ReviewStorageRecovery {
                request_id: request_id(rid),
                expected_catalog: catalog_fingerprint,
                unavailable_locations: unavailable_location.into_iter().collect(),
            },
            StorageCommand::Enroll {
                registration,
                request_id: rid,
            } => ApiRequest::EnrollStorage {
                request_id: request_id(rid),
                registration: serde_json::from_slice(&std::fs::read(registration)?)?,
            },
            StorageCommand::Policy {
                history,
                input,
                revision,
                checkpoint,
                output,
                request_id: rid,
            } => ApiRequest::StoragePolicy {
                request_id: request_id(rid),
                expected_revision: revision,
                policy: fleet::control::artifact::Policy {
                    history: history.into_iter().collect(),
                    inputs: input.into_iter().collect(),
                    checkpoints: checkpoint.into_iter().collect(),
                    outputs: output.into_iter().collect(),
                },
            },
        },
        Command::Artifacts { attempt } => ApiRequest::Artifacts { attempt },
        Command::FetchArtifact { id, destination } => ApiRequest::FetchArtifact { id, destination },
        Command::ArtifactRetention {
            id,
            revision,
            pinned,
            expires_at,
            request_id: rid,
        } => ApiRequest::ArtifactRetention {
            request_id: request_id(rid),
            id,
            expected_revision: revision,
            pinned,
            expires_at,
        },
        Command::DeleteArtifact {
            id,
            revision,
            request_id: rid,
        } => ApiRequest::DeleteArtifact {
            request_id: request_id(rid),
            id,
            expected_revision: revision,
        },
        Command::ExpireArtifacts {
            as_of,
            request_id: rid,
        } => ApiRequest::ExpireArtifacts {
            request_id: request_id(rid),
            as_of,
        },
        Command::Versions => ApiRequest::Versions,
        Command::Upgrade { action } => match action {
            VersionCommand::Prepare {
                target,
                revision,
                request_id,
            } => ApiRequest::PrepareVersion {
                target,
                expected_revision: revision,
                request_id: request_id
                    .unwrap_or_else(|| format!("request-{:032x}", rand::random::<u128>())),
            },
            VersionCommand::Activate {
                revision,
                request_id,
            } => ApiRequest::ActivateVersion {
                expected_revision: revision,
                request_id: request_id
                    .unwrap_or_else(|| format!("request-{:032x}", rand::random::<u128>())),
            },
            VersionCommand::Abort {
                revision,
                request_id,
            } => ApiRequest::AbortVersion {
                expected_revision: revision,
                request_id: request_id
                    .unwrap_or_else(|| format!("request-{:032x}", rand::random::<u128>())),
            },
        },
        Command::Status => ApiRequest::Status,
        Command::Replicas {
            add,
            remove,
            request_id: rid,
        } => {
            let mut c = client::AppClient::connect(&client_home).await?;
            let state = c.read().await?;
            c.close().await;
            let view = state.membership_view();
            if add.is_empty() && remove.is_empty() {
                return print(&view);
            }
            ensure!(
                view.active.is_none(),
                "membership change already in progress"
            );
            let mut voters = view.peers;
            voters.retain(|id, _| view.voter_sets.iter().any(|set| set.contains(id)));
            for id in remove {
                ensure!(
                    voters.remove(&id).is_some(),
                    "replica {id} is not a current voter"
                );
            }
            for path in add {
                let peers: std::collections::BTreeMap<u64, fleet::control::network::Peer> =
                    serde_json::from_slice(&std::fs::read(path)?)?;
                for (id, peer) in peers {
                    ensure!(
                        voters.insert(id, peer).is_none(),
                        "replica ID {id} already in desired voters"
                    );
                }
            }
            ApiRequest::Membership {
                request_id: request_id(rid),
                expected_revision: view.revision,
                voters,
            }
        }
        Command::Operation { id } => ApiRequest::Operation { id },
        Command::AcceptRecovery { manifest } => {
            let manifest = serde_json::from_slice(&std::fs::read(manifest)?)?;
            let lock_home = client_home.clone();
            let _lock = tokio::task::spawn_blocking(move || {
                fleet::keys::lock_coordinator(&lock_home, std::time::Duration::from_secs(30))
            })
            .await??;
            fleet::control::recovery::accept_client(&client_home.join("config.json"), &manifest)?;
            return Ok(());
        }
        Command::ResumeRecovery {
            unavailable_worker,
            request_id: rid,
        } => ApiRequest::ResumeRecovery {
            request_id: request_id(rid),
            unavailable_workers: unavailable_worker.into_iter().collect(),
        },
        Command::Queue => ApiRequest::Queue,
        Command::Job { id } => ApiRequest::Job { id },
        Command::Workers => ApiRequest::Workers,
        Command::Instances => ApiRequest::Instances,
        Command::Submit {
            job,
            id,
            request_id: rid,
            worker,
            memory_mb,
            gpu,
            priority,
            after,
            requires,
        } => ApiRequest::Submit {
            request_id: request_id(rid),
            id: id.unwrap_or_else(|| format!("job-{:032x}", rand::random::<u128>())),
            job: serde_json::from_slice(&std::fs::read(job)?)?,
            worker,
            memory_mb,
            gpu,
            priority,
            after: BTreeSet::from_iter(after),
            requires: BTreeSet::from_iter(requires),
        },
        Command::Cancel {
            id,
            request_id: rid,
            revision,
        } => {
            let revision=match revision {Some(v)=>v,None=>client::call(&client_home,ApiRequest::Job{id:id.clone()}).await?["revision"].as_u64().context("job revision missing")?};
            ApiRequest::Cancel {
                request_id: request_id(rid),
                id,
                expected_revision: revision,
            }
        }
        Command::Suspend {
            id,
            checkpoint,
            request_id: rid,
            revision,
        } => {
            let revision = match revision { Some(v) => v, None => client::call(&client_home, ApiRequest::Job { id: id.clone() }).await?["revision"].as_u64().context("job revision missing")? };
            ApiRequest::Suspend {
                request_id: request_id(rid),
                id,
                checkpoint,
                expected_revision: revision,
            }
        }
        Command::Resume {
            worker,
            id,
            request_id: rid,
            revision,
        } => {
            let revision = match revision { Some(v) => v, None => client::call(&client_home, ApiRequest::Job { id: id.clone() }).await?["revision"].as_u64().context("job revision missing")? };
            ApiRequest::Resume {
                worker,
                request_id: request_id(rid),
                id,
                expected_revision: revision,
            }
        }
        Command::Checkpoints { id } => ApiRequest::Checkpoints { id },
        Command::FetchCheckpoint { id, destination } => {
            ApiRequest::FetchCheckpoint { id, destination }
        }
        Command::Attempts { id } => ApiRequest::Attempts { id },
        Command::History { id } => ApiRequest::History { id },
        Command::Metrics { id, attempt, from } => ApiRequest::Metrics {
            id,
            attempt,
            from,
            max: 65536,
        },
        Command::HistoryRetention {
            id,
            policy,
            revision,
            request_id: rid,
        } => ApiRequest::HistoryRetention {
            request_id: request_id(rid),
            id,
            expected_revision: revision,
            policy: serde_json::from_slice(&std::fs::read(policy)?)?,
        },
        Command::Logs {
            id,
            attempt,
            stderr,
            mut from,
            follow,
        } => {
            let attempt = if follow && attempt.is_none() {
                let history =
                    client::call(&client_home, ApiRequest::History { id: id.clone() }).await?;
                Some(
                    history["attempts"]
                        .as_array()
                        .and_then(|a| a.last())
                        .and_then(|a| a.pointer("/attempt/number"))
                        .and_then(serde_json::Value::as_u64)
                        .context("job has no attempt to follow")? as u32,
                )
            } else {
                attempt
            };
            loop {
                let value = client::call(
                    &client_home,
                    ApiRequest::Logs {
                        attempt,
                        id: id.clone(),
                        stderr,
                        from,
                        max: 65536,
                    },
                )
                .await?;
                if !follow {
                    return print(&value);
                }
                if let Some(chunk) = value.get("Logs") {
                    if let Some(gap) = chunk.get("gap").filter(|g| !g.is_null()) {
                        eprintln!(
                            "[Fleet: history unavailable for bytes {}..{}]",
                            gap["start"], gap["end"]
                        );
                    }
                    if let Some(data) = chunk.get("data").and_then(|s| s.as_str()) {
                        print!("{data}");
                    }
                    from = chunk["next"].as_u64().unwrap_or(from);
                    if chunk["done"] == true {
                        break;
                    }
                } else {
                    print(&value)?;
                    break;
                }
                tokio::select! {_ = fleet::shutdown_signal()=>break,_=tokio::time::sleep(std::time::Duration::from_secs(1))=>{}}
            }
            return Ok(());
        }
        Command::Outputs { id, attempt } => ApiRequest::Outputs { id, attempt },
        Command::Fetch {
            id,
            attempt,
            destination,
        } => ApiRequest::Fetch {
            id,
            attempt,
            destination,
        },
        Command::Stage { id, watch } => {
            loop {
                match client::call(&client_home, ApiRequest::Stage { id: id.clone() }).await {
                    Ok(v) => print(&v)?,
                    Err(e) if watch => tracing::warn!(error=%e,"staging will retry"),
                    Err(e) => return Err(e),
                }
                if !watch {
                    break;
                }
                tokio::select! {_ = fleet::shutdown_signal()=>break,_=tokio::time::sleep(std::time::Duration::from_secs(5))=>{}}
            }
            return Ok(());
        }
        Command::Requests => ApiRequest::Requests,
        Command::Retry { request_id } => ApiRequest::Retry { request_id },
        Command::Enroll {
            registration,
            request_id: rid,
        } => {
            let value: serde_json::Value = serde_json::from_slice(&std::fs::read(registration)?)?;
            let registration = if let Some(w) = value.pointer("/mutation/Enroll/worker") {
                serde_json::from_value(w.clone())?
            } else {
                serde_json::from_value(value)?
            };
            ApiRequest::Enroll {
                request_id: request_id(rid),
                registration,
            }
        }
        Command::Drain {
            worker,
            resume,
            request_id: rid,
        } => ApiRequest::Drain {
            request_id: request_id(rid),
            worker,
            drain: !resume,
        },
        Command::Instance { action } => match action {
            InstanceCommand::Create {
                id,
                worker,
                spec,
                request_id: rid,
            } => ApiRequest::InstanceCreate {
                request_id: request_id(rid),
                id,
                worker,
                spec: serde_json::from_slice(&std::fs::read(spec)?)?,
            },
            action => {
                use fleet::control::workload::InstanceDesired;
                let (id, rid, desired) = match action {
                    InstanceCommand::Start { id, request_id } => {
                        (id, request_id, InstanceDesired::Running)
                    }
                    InstanceCommand::Stop { id, request_id } => {
                        (id, request_id, InstanceDesired::Stopped)
                    }
                    InstanceCommand::Delete { id, request_id } => {
                        (id, request_id, InstanceDesired::Deleted)
                    }
                    _ => unreachable!(),
                };
                let state = client::call(&client_home, ApiRequest::Instances).await?;
                let revision = state
                    .as_array()
                    .context("instances")?
                    .iter()
                    .find(|a| a["id"] == id)
                    .and_then(|a| a["revision"].as_u64())
                    .context("unknown instance")?;
                ApiRequest::InstanceSet {
                    request_id: request_id(rid),
                    id,
                    expected_revision: revision,
                    desired,
                }
            }
        },
        Command::Api => {
            let mut input = String::new();
            std::io::stdin().take(8 << 20).read_to_string(&mut input)?;
            let api: ApiRequest = serde_json::from_str(&input)?;
            let rid = api.request_id().map(str::to_owned);
            match client::call(&client_home, api).await {
                Ok(v) => print(&serde_json::json!({"ok":true,"result":v}))?,
                Err(e) => {
                    let message = format!("{e:#}");
                    print(
                        &serde_json::json!({"ok":false,"error":message,"outcome_unknown":message.contains("outcome unknown"),"request_id":rid}),
                    )?;
                }
            }
            return Ok(());
        }
        #[cfg(feature = "gui")]
        Command::Gui => unreachable!(),
    };
    if let Some(id) = api.request_id() {
        eprintln!("request {id}; use fleet retry {id} if its outcome is unknown");
    }
    let result = client::call(&client_home, api).await?;
    print(&result)?;
    ensure!(
        result.pointer("/reply/Rejected").is_none(),
        "request rejected"
    );
    ensure!(
        result.pointer("/reply/RecoveryUnknown").is_none(),
        "historical request outcome unknown after recovery; not reexecuted"
    );
    Ok(())
}
