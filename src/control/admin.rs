//! Replicated Fleet administration and explicit-home qualification commands.
use crate::control::{
    Client, Config, Mutation, Replica, Reply, Request,
    discovery::incarnation_cache,
    membership::{MembershipStage, MembershipView},
    network::{Peer, validate_address},
};
use anyhow::{Result, ensure};
use clap::Subcommand;
use std::{
    collections::{BTreeMap, BTreeSet},
    net::UdpSocket,
    path::PathBuf,
    time::Duration,
};

#[derive(Subcommand)]
pub enum Command {
    /// Prepare a separate storage identity/home; enroll its public registration before running it.
    PrepareStorage {
        #[arg(long)]
        home: PathBuf,
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        id: String,
        #[arg(long)]
        failure_domain: String,
        #[arg(long)]
        capacity_bytes: u64,
        #[arg(long, default_value = "127.0.0.1:0")]
        bind: std::net::SocketAddr,
    },
    /// Run the independent artifact storage role.
    Storage {
        #[arg(long)]
        home: PathBuf,
    },
    /// Read storage filesystem accounting, including staging and orphaned payloads.
    StorageUsage {
        #[arg(long)]
        home: PathBuf,
    },
    /// Read separate worker cache, restored payload, job-home and VM-image accounting; never deletes files.
    WorkerStorageUsage {
        #[arg(long)]
        home: PathBuf,
    },
    /// Explicitly accept signed recovery while retaining objects and deletion tombstones.
    RebindStorage {
        #[arg(long)]
        home: PathBuf,
        #[arg(long)]
        manifest: PathBuf,
    },
    /// Offline lost-quorum recovery. Creates a fresh single-replica home and signed manifest.
    Recover {
        #[arg(long)]
        source_home: PathBuf,
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        key: PathBuf,
        #[arg(long)]
        home: PathBuf,
        #[arg(long)]
        bind: std::net::SocketAddr,
        #[arg(long, required = true)]
        accept_data_loss: bool,
        #[arg(long, required = true)]
        old_controllers_fenced: bool,
    },
    /// Explicitly trust an administrator-signed recovery; old cached requests retain their origin.
    AcceptRecovery {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        manifest: PathBuf,
    },
    /// Rebind a stopped worker without discarding its accepted work or execution history.
    RebindWorker {
        #[arg(long)]
        home: PathBuf,
        #[arg(long)]
        manifest: PathBuf,
    },
    /// Resume new work after inventory reconciliation; unreconciled workers remain quarantined.
    ResumeRecovery {
        #[arg(long)]
        key: PathBuf,
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        origin: String,
        #[arg(long)]
        request_id: String,
        #[arg(long)]
        unavailable_worker: Vec<String>,
    },
    /// Prepare a fresh worker identity and enrollment request. Does not start workloads.
    PrepareWorker {
        #[arg(long)]
        home: PathBuf,
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        id: String,
        #[arg(long)]
        cpu_slots: u32,
        #[arg(long)]
        memory_mb: u32,
        /// Trusted VM configuration (instances.json) to copy into this worker's separate backend home.
        #[arg(long)]
        instances: Option<PathBuf>,
        #[arg(long, default_value = "127.0.0.1:0")]
        bind: std::net::SocketAddr,
    },
    /// Add runtime capabilities to a stopped idle worker from its local VM config; emit a consensus request.
    PrepareWorkerRuntimes {
        #[arg(long)]
        home: PathBuf,
    },
    /// Run a worker; its detached supervisors survive worker shutdown.
    Worker {
        #[arg(long)]
        home: PathBuf,
    },
    /// Prepare immutable inputs, push them, then submit a durable explicitly placed job.
    SubmitJob {
        #[arg(long)]
        key: PathBuf,
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        data_home: PathBuf,
        #[arg(long)]
        job: PathBuf,
        #[arg(long)]
        worker: String,
        #[arg(long)]
        id: String,
        #[arg(long)]
        request_id: String,
        #[arg(long)]
        memory_mb: u32,
        #[arg(long)]
        gpu: bool,
    },
    /// Read logs, metrics/events, outputs or status directly from an enrolled worker.
    Data {
        #[arg(long)]
        key: PathBuf,
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        worker: String,
        #[arg(long)]
        request: PathBuf,
    },
    /// Capture verified published files into a fresh directory.
    Fetch {
        #[arg(long)]
        key: PathBuf,
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        data_home: PathBuf,
        #[arg(long)]
        assignment: String,
        #[arg(long)]
        destination: PathBuf,
    },
    #[command(hide = true)]
    Supervise { dir: PathBuf },
    #[command(hide = true)]
    InstanceController { home: PathBuf, id: String },
    #[command(hide = true)]
    InstanceOperation {
        home: PathBuf,
        id: String,
        operation: String,
    },
    /// Prepare an empty home for a fresh replica; admission is a separate membership request.
    PrepareReplica {
        #[arg(long)]
        home: PathBuf,
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        id: u64,
        /// Local interface and port to advertise. Port zero chooses an available local port.
        #[arg(long, default_value = "127.0.0.1:0")]
        bind: std::net::SocketAddr,
    },
    /// Submit a desired voter map, then wait for its durable operation to complete.
    Membership {
        /// Original request incarnation; keep this value when retrying after recovery.
        #[arg(long)]
        origin: String,
        #[arg(long)]
        key: PathBuf,
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        voters: PathBuf,
        #[arg(long)]
        request_id: String,
        #[arg(long)]
        expected_revision: u64,
        #[arg(long, default_value_t = 30)]
        wait_seconds: u64,
    },
    /// Prepare an empty directory with separate identities, configs, and an example request.
    Init {
        #[arg(long)]
        home: PathBuf,
        #[arg(long, default_value_t = 3)]
        replicas: u64,
        /// Explicit advertised address for each initial replica, in replica ID order.
        /// May name remote hosts; addresses are validated but not bound during preparation.
        #[arg(long)]
        address: Vec<std::net::SocketAddr>,
    },
    Id {
        #[arg(long)]
        home: PathBuf,
    },
    Serve {
        #[arg(long)]
        home: PathBuf,
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        bootstrap: bool,
        /// Also run a worker in this process, with a separate worker home and identity.
        #[arg(long)]
        worker_home: Option<PathBuf>,
        /// Qualification only: a local JSON array of peer public keys whose traffic is blocked.
        /// Reloaded while running; never modifies host networking or installed services.
        #[cfg(feature = "qualification")]
        #[arg(long)]
        qualification_partition: Option<PathBuf>,
    },
    Request {
        #[arg(long)]
        key: PathBuf,
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        request: PathBuf,
        /// Contact only this replica, without automatic leader routing or retries.
        #[arg(long)]
        replica: Option<u64>,
    },
    Read {
        #[arg(long)]
        key: PathBuf,
        #[arg(long)]
        config: PathBuf,
        /// Require a quorum-confirmed read from this replica, without automatic leader routing.
        #[arg(long)]
        replica: Option<u64>,
    },
    /// Read one replica's local diagnostics, which may be stale and require no quorum.
    Inspect {
        #[arg(long)]
        key: PathBuf,
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        replica: u64,
    },
}

pub async fn run(command: Command) -> Result<()> {
    match command {
        Command::PrepareWorkerRuntimes { home } => {
            let request = tokio::task::spawn_blocking(move || {
                super::runtime::Worker::prepare_runtimes(&home)
            })
            .await??;
            println!("{}", serde_json::to_string_pretty(&request)?);
        }
        Command::Recover {
            source_home,
            config,
            key,
            home,
            bind,
            accept_data_loss,
            old_controllers_fenced,
        } => {
            ensure!(
                accept_data_loss && old_controllers_fenced,
                "explicit recovery acknowledgments required"
            );
            ensure!(key.is_file(), "existing administrator key required");
            let config: Config = serde_json::from_slice(&std::fs::read(config)?)?;
            let key = crate::keys::load_or_create(&key)?;
            let manifest =
                crate::control::recovery::recover(&source_home, &config, &key, &home, bind).await?;
            println!("{}", serde_json::to_string_pretty(&manifest)?);
        }
        Command::AcceptRecovery { config, manifest } => {
            let manifest = serde_json::from_slice(&std::fs::read(manifest)?)?;
            crate::control::recovery::accept_client(&config, &manifest)?;
        }
        Command::RebindWorker { home, manifest } => {
            let manifest = serde_json::from_slice(&std::fs::read(manifest)?)?;
            crate::control::runtime::Worker::accept_recovery(&home, &manifest)?;
        }
        Command::ResumeRecovery {
            key,
            config,
            origin,
            request_id,
            unavailable_worker,
        } => {
            let mut client = client(&key, &config).await?;
            let request = Request {
                origin,
                id: request_id,
                mutation: Mutation::ResumeRecovery {
                    unavailable_workers: unavailable_worker.into_iter().collect(),
                },
            };
            let reply = client.write(request, Duration::from_secs(10)).await?;
            println!("{}", serde_json::to_string_pretty(&reply)?);
            client.endpoint.close().await;
        }
        Command::PrepareStorage {
            home,
            config,
            id,
            failure_domain,
            capacity_bytes,
            bind,
        } => {
            let base: Config = serde_json::from_slice(&std::fs::read(config)?)?;
            std::fs::create_dir_all(&home)?;
            ensure!(
                std::fs::read_dir(&home)?.next().is_none(),
                "prepare requires an empty storage home"
            );
            let identity = crate::keys::load_or_create(&home.join("node.key"))?.public();
            let socket = UdpSocket::bind(bind)?;
            let registration = super::artifact::Registration {
                id,
                peer: Peer {
                    identity,
                    address: socket.local_addr()?,
                },
                incarnation: format!("{:032x}", rand::random::<u128>()),
                failure_domain,
                capacity_bytes,
            };
            registration.validate()?;
            let config = super::storage::Config {
                cluster: base.cluster,
                registration: registration.clone(),
                controllers: base.peers,
                administrators: base.administrators,
            };
            super::storage::Agent::initialize(&home, &config)?;
            crate::instances::write(&home.join("registration.json"), &registration)?;
            println!("{}", serde_json::to_string_pretty(&registration)?);
        }
        Command::Storage { home } => {
            let mut agent = super::storage::Agent::start(
                &home,
                serde_json::from_slice(&std::fs::read(home.join("storage.json"))?)?,
            )
            .await?;
            let signal = crate::shutdown_signal();
            tokio::pin!(signal);
            loop {
                if let Err(error) = agent.tick().await {
                    tracing::warn!(%error, "storage reconciliation will retry");
                }
                tokio::select! { _ = &mut signal => break, _ = tokio::time::sleep(Duration::from_secs(2)) => {} }
            }
            agent.shutdown().await?;
        }
        Command::WorkerStorageUsage { home } => {
            let _config: super::runtime::WorkerConfig =
                serde_json::from_slice(&std::fs::read(home.join("worker.json"))?)?;
            let mut categories = std::collections::BTreeMap::new();
            for (label, path) in [
                ("shared_blob_cache", "blobs"),
                ("restored_checkpoints", "restored"),
                ("job_homes_including_logs_and_captures", "backend/jobs"),
                ("vm_disks", "backend/instances"),
            ] {
                categories.insert(label, super::storage::usage(&home.join(path))?);
            }
            println!("{}", serde_json::to_string_pretty(&categories)?);
        }
        Command::StorageUsage { home } => {
            println!(
                "{}",
                serde_json::to_string_pretty(
                    &serde_json::json!({"objects":super::storage::usage(&home.join("objects"))?,"staging":super::storage::usage(&home.join("staging"))?,"metadata":super::storage::usage(&home.join("deleted"))?})
                )?
            );
        }
        Command::RebindStorage { home, manifest } => {
            super::storage::accept_recovery(
                &home,
                &serde_json::from_slice(&std::fs::read(manifest)?)?,
            )?;
        }
        Command::Supervise { dir } => crate::supervise::run(&dir)?,
        Command::InstanceController { home, id } => crate::instances::run_controller(&home, &id)?,
        Command::InstanceOperation {
            home,
            id,
            operation,
        } => crate::instances::guest::supervise(&home, &id, &operation)?,
        Command::PrepareWorker {
            home,
            config,
            id,
            cpu_slots,
            memory_mb,
            instances,
            bind,
        } => {
            use crate::control::{
                runtime::WorkerConfig,
                workload::{Budget, Registration},
            };
            let base: Config = serde_json::from_slice(&std::fs::read(config)?)?;
            std::fs::create_dir_all(&home)?;
            ensure!(
                std::fs::read_dir(&home)?.next().is_none(),
                "prepare requires an empty worker home"
            );
            let identity = crate::keys::load_or_create(&home.join("node.key"))?.public();
            let socket = UdpSocket::bind(bind)?;
            let vm = instances
                .map(|path| -> Result<crate::instances::Config> {
                    Ok(serde_json::from_slice(&std::fs::read(path)?)?)
                })
                .transpose()?;
            if let Some(vm) = &vm {
                ensure!(
                    vm.memory_budget_mb == memory_mb,
                    "VM and worker memory budgets must match"
                );
                crate::instances::write(&home.join("backend/instances.json"), vm)?;
            }
            let hello = tokio::task::spawn_blocking(crate::system::hello).await?;
            let registration = Registration {
                runtimes: vm
                    .as_ref()
                    .map(|v| v.runtimes())
                    .transpose()?
                    .unwrap_or_default(),
                tags: hello.tags.into_iter().collect(),
                id,
                peer: Peer {
                    identity,
                    address: socket.local_addr()?,
                },
                incarnation: format!("{:032x}", rand::random::<u128>()),
                budget: Budget {
                    cpu_slots,
                    memory_mb,
                    disk_gb: vm.as_ref().map_or(0, |v| v.disk_budget_gb),
                    vm_slots: if vm.is_some() { 2 } else { 0 },
                },
                images: vm
                    .as_ref()
                    .map(|v| v.images.keys().cloned().collect())
                    .unwrap_or_default(),
            };
            registration.validate()?;
            let config = WorkerConfig {
                cluster: base.cluster,
                registration: registration.clone(),
                controllers: base.peers,
                administrators: base.administrators,
                supervisor: std::env::current_exe()?,
            };
            crate::control::runtime::Worker::initialize(&home, &config)?;
            crate::instances::write(&home.join("worker.json"), &config)?;
            let request = Request {
                origin: config.cluster.clone(),
                id: format!("enroll-{}", registration.incarnation),
                mutation: Mutation::Enroll {
                    worker: registration,
                },
            };
            crate::instances::write(&home.join("enroll.json"), &request)?;
            println!("{}", serde_json::to_string_pretty(&request)?);
        }
        Command::Worker { home } => {
            let mut worker = start_worker(&home).await?;
            run_worker(&mut worker).await;
            worker.shutdown().await?;
        }
        Command::SubmitJob {
            key,
            config,
            data_home,
            job,
            worker,
            id,
            request_id,
            memory_mb,
            gpu,
        } => {
            use crate::control::{runtime::push_inputs, workload::Workload};
            crate::instances::valid_id(&request_id)?;
            let mut client = client(&key, &config).await?;
            let state = client.read(Duration::from_secs(10)).await?;
            let store = crate::blobs::open(&data_home.join("blobs")).await?;
            let path = data_home
                .join("requests")
                .join(format!("{request_id}.json"));
            let request: Request = if path.exists() {
                serde_json::from_slice(&std::fs::read(&path)?)?
            } else {
                let file: crate::inputs::JobFile = serde_json::from_slice(&std::fs::read(job)?)?;
                let spec = crate::inputs::prepare(&store, &file).await?;
                let request = Request {
                    origin: client.cluster.clone(),
                    id: request_id.clone(),
                    mutation: Mutation::Assign {
                        id: id.clone(),
                        execution: format!("exec-{id}"),
                        worker: worker.clone(),
                        workload: Workload::Job {
                            spec: Box::new(spec),
                            gpu,
                            memory_mb,
                        },
                    },
                };
                crate::instances::write(&path, &request)?;
                request
            };
            let Mutation::Assign {
                id: saved_id,
                worker: saved_worker,
                workload: Workload::Job { spec, .. },
                ..
            } = &request.mutation
            else {
                anyhow::bail!("invalid cached job request");
            };
            ensure!(
                saved_id == &id && saved_worker == &worker && request.id == request_id,
                "cached request names another assignment"
            );
            if request.origin != client.cluster {
                let reply = client.write(request, Duration::from_secs(10)).await?;
                println!("{}", serde_json::to_string_pretty(&reply)?);
                client.endpoint.close().await;
                return Ok(());
            }
            let registration = state
                .workers
                .get(&worker)
                .ok_or_else(|| anyhow::anyhow!("unknown worker"))?;
            push_inputs(
                &store,
                &client.endpoint,
                &client.cluster,
                &registration.peer,
                spec,
            )
            .await?;
            let reply = client.write(request, Duration::from_secs(10)).await?;
            println!("{}", serde_json::to_string_pretty(&reply)?);
            client.endpoint.close().await;
        }
        Command::Data {
            key,
            config,
            worker,
            request,
        } => {
            let mut client = client(&key, &config).await?;
            let state = client.read(Duration::from_secs(10)).await?;
            let worker = state
                .workers
                .get(&worker)
                .ok_or_else(|| anyhow::anyhow!("unknown worker"))?;
            let request = serde_json::from_slice(&std::fs::read(request)?)?;
            let reply = crate::control::runtime::data_call(
                &client.endpoint,
                &client.cluster,
                &worker.peer,
                request,
            )
            .await?;
            println!("{}", serde_json::to_string_pretty(&reply)?);
            client.endpoint.close().await;
        }
        Command::Fetch {
            key,
            config,
            data_home,
            assignment,
            destination,
        } => {
            let mut client = client(&key, &config).await?;
            let state = client.read(Duration::from_secs(10)).await?;
            let a = state
                .assignments
                .get(&assignment)
                .ok_or_else(|| anyhow::anyhow!("unknown assignment"))?;
            let worker = &state.workers[&a.worker];
            let store = crate::blobs::open(&data_home.join("blobs")).await?;
            let files = crate::control::runtime::fetch_outputs(
                &store,
                &client.endpoint,
                &client.cluster,
                &worker.peer,
                &a.execution,
                &destination,
            )
            .await?;
            println!("{}", serde_json::to_string_pretty(&files)?);
            client.endpoint.close().await;
        }
        Command::PrepareReplica {
            home,
            config,
            id,
            bind,
        } => {
            let mut base: Config = serde_json::from_slice(&std::fs::read(&config)?)?;
            let cache = incarnation_cache(&config, &base.cluster);
            if cache.exists() {
                let view: MembershipView = serde_json::from_slice(&std::fs::read(cache)?)?;
                ensure!(view.cluster == base.cluster, "cache cluster mismatch");
                let seeds = base.peers;
                base.peers = view.peers;
                base.peers.extend(seeds);
            }
            ensure!(
                id != 0 && !base.peers.contains_key(&id),
                "use a fresh nonzero replica ID"
            );
            std::fs::create_dir_all(&home)?;
            ensure!(
                std::fs::read_dir(&home)?.next().is_none(),
                "prepare requires an empty directory"
            );
            let identity = crate::keys::load_or_create(&home.join("node.key"))?.public();
            let socket = UdpSocket::bind(bind)?;
            let peer = Peer {
                identity,
                address: socket.local_addr()?,
            };
            peer.validate()?;
            base.id = id;
            base.bootstrap_allowed = false;
            base.peers.insert(id, peer.clone());
            std::fs::write(home.join("config.json"), serde_json::to_vec_pretty(&base)?)?;
            let descriptor = BTreeMap::from([(id, peer)]);
            std::fs::write(
                home.join("peer.json"),
                serde_json::to_vec_pretty(&descriptor)?,
            )?;
            println!("{}", serde_json::to_string_pretty(&descriptor)?);
        }
        Command::Membership {
            origin,
            key,
            config,
            voters,
            request_id,
            expected_revision,
            wait_seconds,
        } => {
            let mut client = client(&key, &config).await?;
            let request = Request {
                origin,
                id: request_id,
                mutation: Mutation::ChangeMembership {
                    expected_revision,
                    voters: serde_json::from_slice(&std::fs::read(voters)?)?,
                },
            };
            let historical = request.origin != client.cluster;
            let reply = client.write(request, Duration::from_secs(10)).await?;
            if historical {
                println!("{}", serde_json::to_string_pretty(&reply)?);
                client.endpoint.close().await;
                return Ok(());
            }
            let operation = match &reply {
                Reply::MembershipAccepted { operation, .. } => Some(operation.clone()),
                Reply::Expired { resource } => resource.clone(),
                _ => None,
            };
            if let Some(operation) = operation {
                let end = tokio::time::Instant::now() + Duration::from_secs(wait_seconds);
                loop {
                    let remaining = end.saturating_duration_since(tokio::time::Instant::now());
                    if remaining.is_zero() {
                        anyhow::bail!(
                            "membership request accepted; completion unknown; inspect or retry the same request: {operation}"
                        );
                    }
                    let state = client.read(remaining.min(Duration::from_secs(10))).await?;
                    if let Some(op) = state.membership_operations.get(&operation)
                        && op.stage == MembershipStage::Complete
                    {
                        println!("{}", serde_json::to_string_pretty(op)?);
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            } else {
                println!("{}", serde_json::to_string_pretty(&reply)?);
            }
            client.endpoint.close().await;
        }
        Command::Init {
            home,
            replicas,
            address,
        } => {
            ensure!(
                matches!(replicas, 1 | 3),
                "the lab qualifies one or three replicas"
            );
            ensure!(
                address.is_empty() || address.len() == replicas as usize,
                "supply exactly one --address per initial replica"
            );
            for address in &address {
                validate_address(*address)?;
            }
            ensure!(
                address.iter().collect::<BTreeSet<_>>().len() == address.len(),
                "replica addresses must be distinct"
            );
            std::fs::create_dir_all(&home)?;
            ensure!(
                std::fs::read_dir(&home)?.next().is_none(),
                "init requires an empty directory"
            );
            let admin = crate::keys::load_or_create(&home.join("client/node.key"))?;
            let sockets: Vec<_> = (0..if address.is_empty() { replicas } else { 0 })
                .map(|_| UdpSocket::bind("127.0.0.1:0"))
                .collect::<std::io::Result<_>>()?;
            let addresses = if address.is_empty() {
                sockets
                    .iter()
                    .map(UdpSocket::local_addr)
                    .collect::<std::io::Result<Vec<_>>>()?
            } else {
                address
            };
            let mut peers = BTreeMap::new();
            for id in 1..=replicas {
                let key = crate::keys::load_or_create(&home.join(format!("node-{id}/node.key")))?;
                peers.insert(
                    id,
                    Peer {
                        identity: key.public(),
                        address: addresses[id as usize - 1],
                    },
                );
            }
            let base = Config {
                cluster: format!("fleet-{:032x}", rand::random::<u128>()),
                id: 1,
                peers,
                administrators: BTreeSet::from([admin.public()]),
                bootstrap_allowed: true,
            };
            for id in 1..=replicas {
                std::fs::write(
                    home.join(format!("node-{id}/config.json")),
                    serde_json::to_vec_pretty(&Config { id, ..base.clone() })?,
                )?;
            }
            std::fs::write(
                home.join("client/config.json"),
                serde_json::to_vec_pretty(&base)?,
            )?;
            let request = Request {
                origin: base.cluster.clone(),
                id: "example-submit".into(),
                mutation: Mutation::Enqueue(super::queue::Submission {
                    inputs: None,
                    id: "example".into(),
                    workload: super::workload::Workload::Job {
                        spec: Box::new(serde_json::from_value(
                            serde_json::json!({"name":"example", "argv":["/bin/echo", "hello"], "keep_awake":false}),
                        )?),
                        gpu: false,
                        memory_mb: 128,
                    },
                    worker: None,
                    priority: 0,
                    requires: BTreeSet::new(),
                    after: BTreeSet::new(),
                }),
            };
            std::fs::write(
                home.join("submit.json"),
                serde_json::to_vec_pretty(&request)?,
            )?;
            println!("{}", serde_json::to_string_pretty(&base)?);
        }
        Command::Id { home } => println!(
            "{}",
            crate::keys::load_or_create(&home.join("node.key"))?.public()
        ),
        Command::Serve {
            home,
            config,
            bootstrap,
            worker_home,
            #[cfg(feature = "qualification")]
            qualification_partition,
        } => {
            let config: Config = serde_json::from_slice(&std::fs::read(config)?)?;
            let replica = Replica::start(&home, config.clone()).await?;
            #[cfg(feature = "qualification")]
            let partition_task = if let Some(path) = qualification_partition {
                let blocked = replica.blocked.clone();
                let initial = serde_json::from_slice(&std::fs::read(&path)?)?;
                *blocked.write().expect("partition lock") = initial;
                Some(tokio::spawn(async move {
                    loop {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        let update = async {
                            let bytes = tokio::fs::read(&path).await?;
                            Ok::<BTreeSet<iroh::EndpointId>, anyhow::Error>(serde_json::from_slice(
                                &bytes,
                            )?)
                        }
                        .await;
                        match update {
                            Ok(peers) => *blocked.write().expect("partition lock") = peers,
                            Err(error) => {
                                tracing::warn!(%error, "partition file unreadable; retaining last fault")
                            }
                        }
                    }
                }))
            } else {
                None
            };
            if bootstrap {
                replica.bootstrap(&config.peers).await?;
            }
            println!("ready {} {}", config.cluster, config.id);
            if let Some(home) = worker_home {
                let mut worker = start_worker(&home).await?;
                run_worker(&mut worker).await;
                worker.shutdown().await?;
            } else {
                crate::shutdown_signal().await;
            }
            #[cfg(feature = "qualification")]
            if let Some(task) = partition_task {
                task.abort();
                let _ = task.await;
            }
            replica.shutdown().await?;
        }
        Command::Request {
            key,
            config,
            request,
            replica,
        } => {
            let request: Request = serde_json::from_slice(&std::fs::read(request)?)?;
            let mut client = client(&key, &config).await?;
            let result = if let Some(id) = replica {
                match client
                    .call_replica(
                        id,
                        crate::control::network::Message::Write(Box::new(request)),
                        Duration::from_secs(10),
                    )
                    .await?
                {
                    crate::control::network::Answer::Write(Ok(reply)) => Ok(reply),
                    other => Err(anyhow::anyhow!(
                        "request not confirmed; outcome unknown: {other:?}"
                    )),
                }
            } else {
                client.write(request, Duration::from_secs(10)).await
            };
            client.endpoint.close().await;
            println!("{}", serde_json::to_string_pretty(&result?)?);
        }
        Command::Read {
            key,
            config,
            replica,
        } => {
            let mut client = client(&key, &config).await?;
            let result = if let Some(id) = replica {
                match client
                    .call_replica(
                        id,
                        crate::control::network::Message::Read,
                        Duration::from_secs(10),
                    )
                    .await?
                {
                    crate::control::network::Answer::Read(Ok(state)) => Ok(*state),
                    other => Err(anyhow::anyhow!("strong read unavailable: {other:?}")),
                }
            } else {
                client.read(Duration::from_secs(10)).await
            };
            client.endpoint.close().await;
            println!("{}", serde_json::to_string_pretty(&result?)?);
        }
        Command::Inspect {
            key,
            config,
            replica,
        } => {
            // Use the explicitly supplied contacts, not a cache which may have retired this peer.
            let config: Config = serde_json::from_slice(&std::fs::read(config)?)?;
            let client = Client::new(
                crate::keys::load_or_create(&key)?,
                config.cluster,
                config.peers,
            )
            .await?;
            let result = client.inspect(replica, Duration::from_secs(5)).await;
            client.endpoint.close().await;
            println!("{}", serde_json::to_string_pretty(&result?)?);
        }
    }
    Ok(())
}

async fn client(key: &std::path::Path, config_path: &std::path::Path) -> Result<Client> {
    let config: Config = serde_json::from_slice(&std::fs::read(config_path)?)?;
    Client::with_cache(
        crate::keys::load_or_create(key)?,
        config.cluster.clone(),
        config.peers,
        incarnation_cache(config_path, &config.cluster),
    )
    .await
}

pub async fn start_worker(home: &std::path::Path) -> Result<crate::control::runtime::Worker> {
    let config = serde_json::from_slice(&std::fs::read(home.join("worker.json"))?)?;
    let hello = tokio::task::spawn_blocking(crate::system::hello).await?;
    crate::control::runtime::Worker::start(home, config, hello).await
}

pub async fn run_worker(worker: &mut crate::control::runtime::Worker) {
    let signal = crate::shutdown_signal();
    tokio::pin!(signal);
    loop {
        // Complete a reconciliation before orderly shutdown; detached effects have their own records.
        if let Err(error) = worker.tick().await {
            tracing::warn!(%error, "worker reconciliation failed");
        }
        tokio::select! { _ = &mut signal => break, _ = tokio::time::sleep(Duration::from_millis(250)) => {} }
    }
}
