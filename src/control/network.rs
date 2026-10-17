use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, bail, ensure};
use iroh::{
    Endpoint, EndpointAddr, EndpointId, RelayMode, SecretKey,
    endpoint::{Connection, PortmapperConfig, presets},
    protocol::{AcceptError, ProtocolHandler, Router},
};
use openraft::{
    BasicNode, RaftNetwork, RaftNetworkFactory,
    error::{InstallSnapshotError, NetworkError, RPCError, RaftError, RemoteError},
    network::RPCOption,
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
};
use serde::{Deserialize, Serialize};

use super::discovery::save_cache;
use super::membership::{MembershipView, spawn_reconciler};
use super::{Command, FORMAT, Raft, Reply, Request, State, Store, TypeConfig, store::MAX_SNAPSHOT};

pub const ALPN: &[u8] = b"fleet/control/10";
const MAX_FRAME: usize = 8 << 20;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Peer {
    pub identity: EndpointId,
    pub address: SocketAddr,
}

impl Peer {
    pub fn validate(&self) -> Result<()> {
        validate_address(self.address)
    }
    pub fn addr(&self) -> EndpointAddr {
        EndpointAddr::new(self.identity).with_ip_addr(self.address)
    }
    pub fn node(&self) -> BasicNode {
        BasicNode {
            addr: serde_json::to_string(self).expect("peer serializes"),
        }
    }
}

/// An advertised contact must name a host, never a wildcard listener or discovery service.
pub fn validate_address(address: SocketAddr) -> Result<()> {
    ensure!(
        address.port() != 0
            && !address.ip().is_unspecified()
            && !address.ip().is_multicast()
            && !matches!(address.ip(), std::net::IpAddr::V4(ip) if ip.is_broadcast()),
        "a concrete unicast IP address and nonzero port are required"
    );
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Config {
    pub cluster: String,
    pub id: u64,
    pub peers: BTreeMap<u64, Peer>,
    pub administrators: BTreeSet<EndpointId>,
    /// Only initial-cluster configurations may bootstrap; prepared joiners cannot do so.
    pub bootstrap_allowed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Message {
    Append(AppendEntriesRequest<TypeConfig>),
    Vote(VoteRequest<u64>),
    Snapshot(InstallSnapshotRequest<TypeConfig>),
    Write(Box<Request>),
    Read,
    /// Local diagnostics can be stale and never authorize worker action or a membership change.
    Inspect,
    StorageRead {
        storage: String,
        incarnation: String,
    },
    AuthorizeArtifact {
        reader: EndpointId,
        artifact: String,
        upload: bool,
    },
    WorkerRead {
        worker: String,
        incarnation: String,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ReplicaStatus {
    pub id: u64,
    pub leader: Option<u64>,
    pub term: u64,
    pub last_log_index: Option<u64>,
    pub last_applied: Option<openraft::LogId<u64>>,
    pub membership: MembershipView,
    pub blocked_peers: BTreeSet<EndpointId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiError {
    pub message: String,
    pub leader: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Answer {
    Append(Result<AppendEntriesResponse<u64>, RaftError<u64>>),
    Vote(Result<VoteResponse<u64>, RaftError<u64>>),
    Snapshot(Result<InstallSnapshotResponse<u64>, RaftError<u64, InstallSnapshotError>>),
    Write(Result<Reply, ApiError>),
    Read(Result<Box<State>, ApiError>),
    Inspect(ReplicaStatus),
    WorkerRead(Result<super::workload::WorkerView, ApiError>),
    StorageRead(Result<super::artifact::View, ApiError>),
    AuthorizeArtifact(Result<bool, ApiError>),
    Refused(String),
}

#[derive(Debug, Serialize, Deserialize)]
struct Frame {
    format: u32,
    cluster: String,
    message: Message,
}

#[derive(Debug, Serialize, Deserialize)]
struct WireAnswer {
    answer: Answer,
    // A committed local view may be stale. It is a routing hint, never a strong read.
    discovery: MembershipView,
}

pub async fn connect_endpoint(key: SecretKey, address: SocketAddr) -> Result<Endpoint> {
    Ok(Endpoint::builder(presets::Minimal)
        .secret_key(key)
        .relay_mode(RelayMode::Disabled)
        .portmapper_config(PortmapperConfig::Disabled)
        .clear_ip_transports()
        .bind_addr(address)?
        .bind()
        .await?)
}

pub async fn exchange(connection: &Connection, cluster: &str, message: Message) -> Result<Answer> {
    Ok(exchange_with_view(connection, cluster, message)
        .await?
        .answer)
}

async fn exchange_with_view(
    connection: &Connection,
    cluster: &str,
    message: Message,
) -> Result<WireAnswer> {
    let bytes = serde_json::to_vec(&Frame {
        format: FORMAT,
        cluster: cluster.into(),
        message,
    })?;
    ensure!(bytes.len() <= MAX_FRAME, "control frame exceeds lab limit");
    let (mut send, mut recv) = connection.open_bi().await?;
    send.write_all(&bytes).await?;
    send.finish()?;
    Ok(serde_json::from_slice(&recv.read_to_end(MAX_FRAME).await?)?)
}

/// Fault injection affects only replica traffic in this isolated lab, never system networking.
pub type Blocked = Arc<RwLock<BTreeSet<EndpointId>>>;

#[derive(Clone)]
struct Factory {
    endpoint: Endpoint,
    cluster: String,
    blocked: Blocked,
    connections: ConnectionPool,
}

type PendingConnection = tokio::sync::watch::Receiver<Option<Result<Connection, String>>>;

/// A heartbeat deadline must not cancel the handshake it needs to make future progress.
/// Share a bounded connection attempt across RPCs and RaftNetwork instances for the same identity.
#[derive(Clone, Default)]
struct ConnectionPool(Arc<Mutex<BTreeMap<EndpointId, PendingConnection>>>);

impl ConnectionPool {
    async fn get(&self, endpoint: &Endpoint, peer: &Peer) -> Result<Connection> {
        let mut receiver = {
            let mut entries = self.0.lock().expect("connection pool");
            if let Some(old) = entries.get(&peer.identity) {
                let reusable = match old.borrow().as_ref() {
                    None => true,
                    Some(Ok(connection)) => connection.close_reason().is_none(),
                    Some(Err(_)) => false,
                };
                if !reusable {
                    entries.remove(&peer.identity);
                }
            }
            entries
                .entry(peer.identity)
                .or_insert_with(|| {
                    let (sender, receiver) = tokio::sync::watch::channel(None);
                    let endpoint = endpoint.clone();
                    let address = peer.addr();
                    tokio::spawn(async move {
                        let result = tokio::time::timeout(
                            Duration::from_secs(5),
                            endpoint.connect(address, ALPN),
                        )
                        .await
                        .map_err(|error| error.to_string())
                        .and_then(|result| result.map_err(|error| error.to_string()));
                        let _ = sender.send(Some(result));
                    });
                    receiver
                })
                .clone()
        };
        let value = receiver
            .wait_for(Option::is_some)
            .await
            .context("connection task stopped")?;
        value
            .as_ref()
            .expect("completed connection")
            .clone()
            .map_err(anyhow::Error::msg)
    }
}

pub struct Network {
    factory: Factory,
    target: u64,
    node: BasicNode,
    connection: Option<Connection>,
}

impl RaftNetworkFactory<TypeConfig> for Factory {
    type Network = Network;
    async fn new_client(&mut self, target: u64, node: &BasicNode) -> Network {
        Network {
            factory: self.clone(),
            target,
            node: node.clone(),
            connection: None,
        }
    }
}

impl Network {
    async fn send(&mut self, message: Message, option: RPCOption) -> Result<Answer> {
        let peer: Peer = serde_json::from_str(&self.node.addr)?;
        ensure!(
            !self
                .factory
                .blocked
                .read()
                .expect("blocked")
                .contains(&peer.identity),
            "injected partition"
        );
        let result = tokio::time::timeout(option.hard_ttl(), async {
            if self
                .connection
                .as_ref()
                .is_none_or(|c| c.close_reason().is_some())
            {
                self.connection = Some(
                    self.factory
                        .connections
                        .get(&self.factory.endpoint, &peer)
                        .await?,
                );
            }
            exchange(
                self.connection.as_ref().expect("connected"),
                &self.factory.cluster,
                message,
            )
            .await
        })
        .await
        .context("raft RPC timeout")?;
        if result.is_err() {
            self.connection = None;
        }
        result
    }
    fn failure<E: std::error::Error>(
        &self,
        error: impl std::fmt::Display,
    ) -> RPCError<u64, BasicNode, E> {
        RPCError::Network(NetworkError::new(&std::io::Error::other(error.to_string())))
    }
}

impl RaftNetwork<TypeConfig> for Network {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        match self
            .send(Message::Append(rpc), option)
            .await
            .map_err(|e| self.failure(e))?
        {
            Answer::Append(r) => {
                r.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
            }
            answer => Err(self.failure(format!("unexpected append reply: {answer:?}"))),
        }
    }
    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        match self
            .send(Message::Vote(rpc), option)
            .await
            .map_err(|e| self.failure(e))?
        {
            Answer::Vote(r) => {
                r.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
            }
            answer => Err(self.failure(format!("unexpected vote reply: {answer:?}"))),
        }
    }
    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>,
    > {
        match self
            .send(Message::Snapshot(rpc), option)
            .await
            .map_err(|e| self.failure(e))?
        {
            Answer::Snapshot(r) => {
                r.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
            }
            answer => Err(self.failure(format!("unexpected snapshot reply: {answer:?}"))),
        }
    }
}

#[derive(Clone)]
struct Handler {
    raft: Raft,
    store: Store,
    config: Config,
    blocked: Blocked,
    drop_write_reply: Arc<AtomicBool>,
    presence: super::queue::Presence,
}

impl std::fmt::Debug for Handler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ControlHandler")
            .field("node", &self.config.id)
            .finish_non_exhaustive()
    }
}

impl Handler {
    async fn peers(&self) -> Result<BTreeMap<u64, Peer>> {
        let state = self.store.state().await?;
        Ok(if state.membership.log_id().is_none() {
            self.config.peers.clone()
        } else {
            state.authorized_replicas()
        })
    }

    async fn answer(&self, peer: EndpointId, frame: Frame) -> Answer {
        if frame.format != FORMAT || frame.cluster != self.config.cluster {
            return Answer::Refused("cluster or protocol mismatch".into());
        }
        let peers = match self.peers().await {
            Ok(peers) => peers,
            Err(e) => return Answer::Refused(format!("membership unavailable: {e}")),
        };
        let sender = match &frame.message {
            Message::Append(rpc) => Some(rpc.vote.leader_id.node_id),
            Message::Vote(rpc) => Some(rpc.vote.leader_id.node_id),
            Message::Snapshot(rpc) => Some(rpc.vote.leader_id.node_id),
            _ => None,
        };
        let is_replica = sender
            .and_then(|id| peers.get(&id))
            .is_some_and(|p| p.identity == peer);
        let is_admin = self.config.administrators.contains(&peer);
        match frame.message {
            Message::Append(rpc) if is_replica => {
                Answer::Append(self.raft.append_entries(rpc).await)
            }
            Message::Vote(rpc) if is_replica => Answer::Vote(self.raft.vote(rpc).await),
            Message::Snapshot(rpc) if is_replica => {
                if rpc.offset.saturating_add(rpc.data.len() as u64) > MAX_SNAPSHOT as u64 {
                    return Answer::Refused("snapshot exceeds lab limit".into());
                }
                Answer::Snapshot(self.raft.install_snapshot(rpc).await)
            }
            Message::Write(request)
                if is_admin
                    || matches!(&request.mutation, super::Mutation::HistoryReport(r) if self.store.state().await.ok().is_some_and(|s| s.history_reporter(&peer.to_string(),r)))
                    || matches!(&request.mutation,
                super::Mutation::WorkerReport(report) if self.store.state().await.ok().and_then(|s| s.workers.get(&report.worker).cloned()).is_some_and(|w| w.peer.identity == peer && w.incarnation == report.incarnation))
                    || matches!(&request.mutation, super::Mutation::ArtifactReady(ready) if self.store.state().await.ok().and_then(|s| ready.manifest.worker().and_then(|id| s.workers.get(id)).cloned()).is_some_and(|w| w.peer.identity == peer && w.incarnation == ready.incarnation))
                    || matches!(&request.mutation, super::Mutation::ArtifactReport(report) if self.store.state().await.ok().is_some_and(|s| s.artifact_reporter(&peer.to_string(), &report.location, &report.incarnation)))
                    || matches!(&request.mutation, super::Mutation::CheckpointReady(ready) if self.store.state().await.ok().and_then(|s| s.workers.get(&ready.checkpoint.worker).cloned()).is_some_and(|w| w.peer.identity == peer && w.incarnation == ready.incarnation))
                    || matches!(&request.mutation, super::Mutation::RecoverWorker(inventory) if self.store.state().await.ok().and_then(|s| s.workers.get(&inventory.registration.id).cloned()).is_some_and(|w| w.peer.identity == peer && w == inventory.registration)) =>
            {
                if request.id.is_empty() || request.id.len() > 512 {
                    return Answer::Refused("request ID must contain 1 to 512 bytes".into());
                }
                Answer::Write(
                    self.raft
                        .client_write(Command {
                            principal: peer.to_string(),
                            request: *request,
                        })
                        .await
                        .map(|r| r.data)
                        .map_err(|e| ApiError {
                            message: e.to_string(),
                            leader: e.forward_to_leader().and_then(|f| f.leader_id),
                        }),
                )
            }
            Message::WorkerRead {
                worker,
                incarnation,
            } => {
                if let Err(e) = self.raft.ensure_linearizable().await {
                    return Answer::WorkerRead(Err(ApiError {
                        message: e.to_string(),
                        leader: e.forward_to_leader().and_then(|f| f.leader_id),
                    }));
                }
                let state = match self.store.state().await {
                    Ok(s) => s,
                    Err(e) => {
                        return Answer::WorkerRead(Err(ApiError {
                            message: e.to_string(),
                            leader: None,
                        }));
                    }
                };
                let Some(registration) = state
                    .workers
                    .get(&worker)
                    .filter(|w| w.peer.identity == peer && w.incarnation == incarnation)
                else {
                    return Answer::Refused(
                        "worker identity or incarnation is not enrolled".into(),
                    );
                };
                self.presence
                    .lock()
                    .expect("presence")
                    .insert(worker.clone(), std::time::Instant::now());
                Answer::WorkerRead(Ok(super::workload::WorkerView {
                    history: state
                        .history
                        .attempts
                        .iter()
                        .filter(|(id, _)| {
                            state
                                .assignments
                                .get(*id)
                                .is_some_and(|a| a.worker == worker)
                        })
                        .map(|(id, s)| (id.clone(), s.clone()))
                        .collect(),
                    artifacts: state
                        .storage
                        .artifacts
                        .values()
                        .filter(|a| a.copies.contains_key(&format!("worker/{worker}")))
                        .cloned()
                        .collect(),
                    stores: state.readable_stores(),
                    cleanup_allowed: state
                        .artifact_cleanup_allowed(Some(&format!("worker/{worker}"))),
                    starts_allowed: state.recovery.as_ref().is_none_or(|r| r.resumed),
                    resume_checkpoints: state
                        .checkpoints
                        .values()
                        .filter(|c| {
                            state.assignments.values().any(|a| {
                                a.worker == worker
                                    && a.attempt.as_ref().and_then(|p| p.checkpoint.as_ref())
                                        == Some(&c.id)
                            })
                        })
                        .map(|c| (c.id.clone(), c.clone()))
                        .collect(),
                    checkpoints: state
                        .checkpoints
                        .values()
                        .filter(|c| {
                            c.worker == worker
                                || state.assignments.values().any(|a| {
                                    a.worker == worker
                                        && a.attempt.as_ref().and_then(|p| p.checkpoint.as_ref())
                                            == Some(&c.id)
                                })
                        })
                        .map(|c| c.id.clone())
                        .collect(),
                    recovery_required: state.worker_recovery_required(&worker),
                    held: state
                        .recovery
                        .as_ref()
                        .map(|r| r.held.clone())
                        .unwrap_or_default(),
                    registration: registration.clone(),
                    acknowledged: state
                        .cursors
                        .get(&registration.cursor())
                        .copied()
                        .unwrap_or(0),
                    assignments: state
                        .assignments
                        .values()
                        .filter(|a| a.worker == worker)
                        .cloned()
                        .collect(),
                }))
            }
            Message::StorageRead {
                storage,
                incarnation,
            } => {
                if let Err(e) = self.raft.ensure_linearizable().await {
                    return Answer::StorageRead(Err(ApiError {
                        message: e.to_string(),
                        leader: e.forward_to_leader().and_then(|f| f.leader_id),
                    }));
                }
                let state = match self.store.state().await {
                    Ok(s) => s,
                    Err(e) => return Answer::Refused(e.to_string()),
                };
                let Some(registration) = state.storage.stores.get(&storage).filter(|r| {
                    r.peer.identity == peer
                        && r.incarnation == incarnation
                        && state.storage_enrolled(&storage)
                }) else {
                    return Answer::Refused(
                        "storage identity or incarnation is not enrolled".into(),
                    );
                };
                let cleanup_allowed =
                    state.artifact_cleanup_allowed(Some(&format!("storage/{storage}")));
                Answer::StorageRead(Ok(super::artifact::View {
                    registration: registration.clone(),
                    lifecycle: state.storage_lifecycle(&storage),
                    artifacts: state
                        .storage
                        .artifacts
                        .values()
                        .filter(|a| a.copies.contains_key(&format!("storage/{storage}")))
                        .cloned()
                        .collect(),
                    stores: state.readable_stores(),
                    workers: state.workers,
                    checkpoints: state.checkpoints,
                    cleanup_allowed,
                }))
            }
            Message::AuthorizeArtifact {
                reader,
                artifact,
                upload,
            } => {
                if let Err(e) = self.raft.ensure_linearizable().await {
                    return Answer::AuthorizeArtifact(Err(ApiError {
                        message: e.to_string(),
                        leader: e.forward_to_leader().and_then(|f| f.leader_id),
                    }));
                }
                let state = match self.store.state().await {
                    Ok(s) => s,
                    Err(e) => return Answer::Refused(e.to_string()),
                };
                let permitted = state.storage.artifacts.get(&artifact).is_some_and(|a| {
                    if a.deleted {
                        return false;
                    }
                    let storage_source = state.storage.stores.values().any(|r| {
                        r.peer.identity == peer
                            && state.storage_readable(&r.id)
                            && a.targets.contains(&r.id)
                    });
                    let worker_source = !upload
                        && state.workers.values().any(|w| {
                            w.peer.identity == peer
                                && !state.worker_recovery_required(&w.id)
                                && a.copies.contains_key(&format!("worker/{}", w.id))
                        });
                    let allowed_reader = self.config.administrators.contains(&reader)
                        || (!upload
                            && (state.workers.values().any(|w| {
                                w.peer.identity == reader && !state.worker_recovery_required(&w.id)
                            }) || state.storage.stores.values().any(|r| {
                                r.peer.identity == reader
                                    && state.storage_readable(&r.id)
                                    && a.targets.contains(&r.id)
                            })));
                    (storage_source || worker_source)
                        && allowed_reader
                        && (!upload || a.manifest.kind == super::artifact::Kind::Inputs)
                });
                Answer::AuthorizeArtifact(Ok(permitted))
            }
            Message::Read if is_admin => {
                if let Err(e) = self.raft.ensure_linearizable().await {
                    return Answer::Read(Err(ApiError {
                        message: e.to_string(),
                        leader: e.forward_to_leader().and_then(|f| f.leader_id),
                    }));
                }
                Answer::Read(
                    self.store
                        .state()
                        .await
                        .map(Box::new)
                        .map_err(|e| ApiError {
                            message: e.to_string(),
                            leader: None,
                        }),
                )
            }
            Message::Inspect if is_admin => {
                let metrics = self.raft.metrics().borrow().clone();
                let state = match self.store.state().await {
                    Ok(state) => state,
                    Err(error) => return Answer::Refused(error.to_string()),
                };
                Answer::Inspect(ReplicaStatus {
                    id: self.config.id,
                    leader: metrics.current_leader,
                    term: metrics.current_term,
                    last_log_index: metrics.last_log_index,
                    last_applied: state.last_applied,
                    membership: state.membership_view(),
                    blocked_peers: self.blocked.read().expect("blocked").clone(),
                })
            }
            _ => Answer::Refused("role not authorized for this operation".into()),
        }
    }
}

impl ProtocolHandler for Handler {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let peer = connection.remote_id();
        if !self
            .peers()
            .await
            .map(|p| p.values().any(|p| p.identity == peer))
            .unwrap_or(false)
            && !self.config.administrators.contains(&peer)
            && !self
                .store
                .state()
                .await
                .map(|s| {
                    s.workers.values().any(|w| w.peer.identity == peer)
                        || s.storage.stores.values().any(|w| w.peer.identity == peer)
                })
                .unwrap_or(false)
        {
            connection.close(403u32.into(), b"not authorized");
            return Ok(());
        }
        while let Ok((mut send, mut recv)) = connection.accept_bi().await {
            let handler = self.clone();
            tokio::spawn(async move {
                let operation = async {
                    if handler.blocked.read().expect("blocked").contains(&peer) {
                        bail!("injected partition");
                    }
                    let bytes = recv.read_to_end(MAX_FRAME).await?;
                    let frame: Frame = serde_json::from_slice(&bytes)?;
                    let answer = handler.answer(peer, frame).await;
                    if matches!(answer, Answer::Write(Ok(_)))
                        && handler.drop_write_reply.swap(false, Ordering::SeqCst)
                    {
                        // Drop the stream after commitment without returning the response.
                        return Ok(());
                    }
                    let bytes = serde_json::to_vec(&WireAnswer {
                        answer,
                        discovery: handler.store.state().await?.routing_view(),
                    })?;
                    ensure!(bytes.len() <= MAX_FRAME, "response exceeds lab limit");
                    send.write_all(&bytes).await?;
                    send.finish()?;
                    Ok::<_, anyhow::Error>(())
                };
                // Limits half-open streams and client writes waiting on a lost quorum.
                if let Ok(Err(error)) =
                    tokio::time::timeout(Duration::from_secs(5), operation).await
                {
                    tracing::debug!(%error, "control lab stream ended");
                }
            });
        }
        Ok(())
    }
}

pub struct Replica {
    pub raft: Raft,
    pub store: Store,
    pub blocked: Blocked,
    pub router: Router,
    pub drop_write_reply: Arc<AtomicBool>,
    pub membership_paused: Arc<AtomicBool>,
    pub membership_task: tokio::task::JoinHandle<()>,
    scheduler_task: tokio::task::JoinHandle<()>,
    bootstrap_peers: Option<BTreeMap<u64, Peer>>,
}

impl Replica {
    pub async fn start(home: &Path, config: Config) -> Result<Self> {
        for peer in config.peers.values() {
            peer.validate()?;
        }
        let key = crate::keys::load_or_create(&home.join("node.key"))?;
        let peer = config
            .peers
            .get(&config.id)
            .context("node missing from configured peers")?;
        ensure!(
            key.public() == peer.identity,
            "node key does not match configured identity"
        );
        ensure!(
            !config.administrators.is_empty(),
            "lab requires an administrator"
        );
        ensure!(
            config
                .peers
                .values()
                .map(|p| p.identity)
                .collect::<BTreeSet<_>>()
                .len()
                == config.peers.len(),
            "duplicate replica identity"
        );
        let endpoint = connect_endpoint(key, peer.address).await?;
        let store = Store::open(&home.join("control.redb"), &config.cluster, config.id)?;
        if let Some(recorded) = store.state().await?.configuration.catalog.get(&config.id) {
            ensure!(
                recorded == peer,
                "configured replica differs from its durable identity"
            );
        }
        let bootstrap_peers = config.bootstrap_allowed.then(|| config.peers.clone());
        let blocked = Blocked::default();
        let drop_write_reply = Arc::new(AtomicBool::new(false));
        let tuning = openraft::Config {
            cluster_name: config.cluster.clone(),
            heartbeat_interval: 250,
            election_timeout_min: 1500,
            election_timeout_max: 3000,
            max_payload_entries: 32,
            snapshot_max_chunk_size: 256 << 10,
            snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(100),
            max_in_snapshot_log_to_keep: 0,
            purge_batch_size: 1,
            ..Default::default()
        }
        .validate()?;
        let factory = Factory {
            endpoint: endpoint.clone(),
            cluster: config.cluster.clone(),
            blocked: blocked.clone(),
            connections: ConnectionPool::default(),
        };
        let raft = Raft::new(
            config.id,
            Arc::new(tuning),
            factory,
            store.clone(),
            store.clone(),
        )
        .await?;
        let presence = super::queue::Presence::default();
        let scheduler_task = super::queue::spawn(raft.clone(), store.clone(), presence.clone());
        let router = Router::builder(endpoint)
            .accept(
                ALPN,
                Handler {
                    raft: raft.clone(),
                    store: store.clone(),
                    config,
                    presence,
                    blocked: blocked.clone(),
                    drop_write_reply: drop_write_reply.clone(),
                },
            )
            .spawn();
        let membership_paused = Arc::new(AtomicBool::new(false));
        let membership_task =
            spawn_reconciler(raft.clone(), store.clone(), membership_paused.clone());
        Ok(Self {
            raft,
            store,
            blocked,
            router,
            drop_write_reply,
            membership_paused,
            membership_task,
            scheduler_task,
            bootstrap_peers,
        })
    }
    pub async fn bootstrap(&self, peers: &BTreeMap<u64, Peer>) -> Result<()> {
        ensure!(
            self.bootstrap_peers.as_ref() == Some(peers),
            "only the initial configured cohort may bootstrap; prepared replicas must join"
        );
        ensure!(
            matches!(peers.len(), 1 | 3),
            "qualified bootstrap sizes are one or three"
        );
        ensure!(
            !self.raft.is_initialized().await?,
            "cluster already initialized; bootstrap is not recovery"
        );
        self.raft
            .initialize(
                peers
                    .iter()
                    .map(|(id, p)| (*id, p.node()))
                    .collect::<BTreeMap<_, _>>(),
            )
            .await?;
        Ok(())
    }
    pub async fn shutdown(self) -> Result<()> {
        self.scheduler_task.abort();
        let _ = self.scheduler_task.await;
        self.membership_task.abort();
        let _ = self.membership_task.await;
        self.raft.shutdown().await?;
        self.router.shutdown().await?;
        Ok(())
    }
}

pub struct Client {
    pub endpoint: Endpoint,
    pub cluster: String,
    pub peers: BTreeMap<u64, Peer>,
    preferred: Option<u64>,
    connections: BTreeMap<u64, Connection>,
    pub discovery: Option<MembershipView>,
    cache: Option<PathBuf>,
}

impl Client {
    pub async fn new(key: SecretKey, cluster: String, peers: BTreeMap<u64, Peer>) -> Result<Self> {
        ensure!(!peers.is_empty(), "no bootstrap peers");
        for peer in peers.values() {
            peer.validate()?;
        }
        // Minimal has no public address lookup. Default IP transports allow both LAN families;
        // replicas/workers still bind only their explicitly configured address.
        let endpoint = Endpoint::builder(presets::Minimal)
            .secret_key(key)
            .relay_mode(RelayMode::Disabled)
            .portmapper_config(PortmapperConfig::Disabled)
            .bind()
            .await?;
        Self::from_endpoint(endpoint, cluster, peers)
    }

    pub fn from_endpoint(
        endpoint: Endpoint,
        cluster: String,
        peers: BTreeMap<u64, Peer>,
    ) -> Result<Self> {
        ensure!(!peers.is_empty(), "no bootstrap peers");
        Ok(Self {
            endpoint,
            cluster,
            peers,
            preferred: None,
            connections: BTreeMap::new(),
            discovery: None,
            cache: None,
        })
    }

    pub async fn with_cache(
        key: SecretKey,
        cluster: String,
        peers: BTreeMap<u64, Peer>,
        path: PathBuf,
    ) -> Result<Self> {
        let seeds = peers.clone();
        let mut client = Self::new(key, cluster, peers).await?;
        if path.exists() {
            client.learn(serde_json::from_slice(&std::fs::read(&path)?)?)?;
        }
        // Explicitly supplied bootstrap contacts remain usable when every cached peer retired.
        // A subsequent authenticated view replaces this union with the current membership.
        client.peers.extend(seeds);
        client.cache = Some(path);
        Ok(client)
    }

    pub fn use_cache(&mut self, path: PathBuf) -> Result<()> {
        let seeds = self.peers.clone();
        if path.exists() {
            self.learn(serde_json::from_slice(&std::fs::read(&path)?)?)?;
        }
        self.peers.extend(seeds);
        self.cache = Some(path);
        Ok(())
    }

    /// Import a previously authenticated local cache, or a view from a pinned peer's response.
    pub fn learn(&mut self, view: MembershipView) -> Result<()> {
        ensure!(view.cluster == self.cluster, "discovery cluster mismatch");
        if view.peers.is_empty() || view.log_id.is_none() {
            return Ok(());
        }
        if self
            .discovery
            .as_ref()
            .is_some_and(|old| old.log_id.map(|l| l.index) >= view.log_id.map(|l| l.index))
        {
            return Ok(());
        }
        self.peers = view.peers.clone();
        self.connections.retain(|id, connection| {
            self.peers
                .get(id)
                .is_some_and(|p| p.identity == connection.remote_id())
        });
        self.discovery = Some(view);
        if let Some(path) = &self.cache {
            // Cache failure must not turn a known committed response into an apparent failure.
            if let Err(error) = save_cache(path, self.discovery.as_ref().expect("learned view")) {
                tracing::warn!(%error, "could not persist membership discovery cache");
            }
        }
        Ok(())
    }

    pub async fn call(&mut self, message: Message, deadline: Duration) -> Result<Answer> {
        let start = tokio::time::Instant::now();
        let mut last_error = String::new();
        while start.elapsed() < deadline {
            let mut ids: Vec<_> = self.peers.keys().copied().collect();
            ids.sort_by_key(|id| Some(*id) != self.preferred);
            for id in ids {
                if !self.peers.contains_key(&id) {
                    continue;
                }
                let remaining = deadline.saturating_sub(start.elapsed());
                if remaining.is_zero() {
                    break;
                }
                let result = tokio::time::timeout(remaining.min(Duration::from_secs(1)), async {
                    if self
                        .connections
                        .get(&id)
                        .is_none_or(|c| c.close_reason().is_some())
                    {
                        let c = self.endpoint.connect(self.peers[&id].addr(), ALPN).await?;
                        self.connections.insert(id, c);
                    }
                    exchange_with_view(&self.connections[&id], &self.cluster, message.clone()).await
                })
                .await;
                let result = match result {
                    Ok(Ok(wire)) => {
                        self.learn(wire.discovery)?;
                        Ok(Ok(wire.answer))
                    }
                    Ok(Err(error)) => Ok(Err(error)),
                    Err(error) => Err(error),
                };
                match result {
                    Ok(Ok(
                        Answer::Write(Err(e))
                        | Answer::Read(Err(e))
                        | Answer::WorkerRead(Err(e))
                        | Answer::StorageRead(Err(e))
                        | Answer::AuthorizeArtifact(Err(e)),
                    )) => {
                        self.preferred = e.leader.filter(|id| self.peers.contains_key(id));
                        last_error = e.message;
                    }
                    Ok(Ok(Answer::Refused(reason))) => bail!("request refused: {reason}"),
                    Ok(Ok(answer)) => {
                        self.preferred = Some(id);
                        return Ok(answer);
                    }
                    other => {
                        self.connections.remove(&id);
                        last_error = format!("{other:?}");
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
        if matches!(message, Message::Write(_)) {
            bail!(
                "control-plane deadline exceeded; mutation outcome is unknown; reuse request ID: {last_error}"
            );
        }
        bail!("control-plane request deadline exceeded: {last_error}")
    }
    pub async fn write(&mut self, request: Request, deadline: Duration) -> Result<Reply> {
        match self
            .call(Message::Write(Box::new(request)), deadline)
            .await?
        {
            Answer::Write(Ok(reply)) => Ok(reply),
            other => bail!("unexpected reply: {other:?}"),
        }
    }
    pub async fn read(&mut self, deadline: Duration) -> Result<State> {
        match self.call(Message::Read, deadline).await? {
            Answer::Read(Ok(state)) => Ok(*state),
            other => bail!("unexpected reply: {other:?}"),
        }
    }

    /// Inspect exactly one pinned replica, without following leader hints or claiming a strong read.
    pub async fn inspect(&self, id: u64, deadline: Duration) -> Result<ReplicaStatus> {
        match self.call_replica(id, Message::Inspect, deadline).await? {
            Answer::Inspect(status) => Ok(status),
            other => bail!("unexpected diagnostic reply: {other:?}"),
        }
    }

    /// Address one replica only. Used for diagnostics and explicit failure qualification.
    /// An error from a write leaves its outcome unknown, just like the retrying client API.
    pub async fn call_replica(
        &self,
        id: u64,
        message: Message,
        deadline: Duration,
    ) -> Result<Answer> {
        let peer = self.peers.get(&id).context("unknown replica")?;
        let timeout_context = if matches!(message, Message::Write(_)) {
            "replica deadline exceeded; mutation outcome is unknown"
        } else {
            "replica request deadline exceeded"
        };
        tokio::time::timeout(deadline, async {
            let connection = self.endpoint.connect(peer.addr(), ALPN).await?;
            exchange(&connection, &self.cluster, message).await
        })
        .await
        .context(timeout_context)?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct HoldConnection;

    impl ProtocolHandler for HoldConnection {
        async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
            connection.closed().await;
            Ok(())
        }
    }

    #[tokio::test]
    async fn handshake_completes_after_its_first_rpc_deadline_expires() {
        let server = connect_endpoint(SecretKey::generate(), "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let peer = Peer {
            identity: server.id(),
            address: server.bound_sockets().into_iter().next().unwrap(),
        };
        let router = Router::builder(server).accept(ALPN, HoldConnection).spawn();
        let client = connect_endpoint(SecretKey::generate(), "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let pool = ConnectionPool::default();
        // Populate the shared attempt, then cancel the caller without waiting for a handshake.
        let mut first = Box::pin(pool.get(&client, &peer));
        assert!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(first.as_mut().poll(cx)))
                .await
                .is_pending()
        );
        drop(first);
        let mut attempt = pool.0.lock().unwrap()[&peer.identity].clone();
        tokio::time::timeout(Duration::from_secs(5), attempt.wait_for(Option::is_some))
            .await
            .unwrap()
            .unwrap();
        let connection = pool.get(&client, &peer).await.unwrap();
        assert_eq!(connection.remote_id(), peer.identity);
        client.close().await;
        router.shutdown().await.unwrap();
    }
}
