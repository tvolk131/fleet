//! One TCP connection per authenticated QUIC connection; no byte replay on failure.
use super::status::TransportStatus;
use super::{Topology, View};
use anyhow::{Context, Result, ensure};
use iroh::{
    Endpoint, EndpointId, RelayMode, RelayUrl, SecretKey,
    endpoint::{Connection, PortmapperConfig, QuicTransportConfig, presets},
};
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Semaphore, watch},
    task::JoinSet,
};

pub const ALPN: &[u8] = b"fleet/service-tunnel/1";
const HANDSHAKE: Duration = Duration::from_secs(10);
const MAX_HEADER: usize = 1024;
const MAX_STREAMS: usize = 64;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Network {
    /// None disables direct UDP completely, allowing relay-only qualification.
    pub bind: Option<SocketAddr>,
    /// Explicit relay servers; no public relay/discovery infrastructure by default.
    pub relays: Vec<RelayUrl>,
}
pub async fn endpoint(key: SecretKey, network: &Network) -> Result<Endpoint> {
    ensure!(
        network.bind.is_some() || !network.relays.is_empty(),
        "tunnel needs a direct listener or relay"
    );
    ensure!(network.relays.len() <= 8, "too many relays");
    for relay in &network.relays {
        super::validate_relay(relay)?;
    }
    let transport = QuicTransportConfig::builder()
        .max_concurrent_bidi_streams(1u32.into())
        .max_concurrent_uni_streams(0u32.into())
        .stream_receive_window((256u32 * 1024).into())
        .receive_window((256u32 * 1024).into())
        .send_window(256 * 1024)
        .keep_alive_interval(Duration::from_secs(5))
        .max_idle_timeout(Some(Duration::from_secs(20).try_into()?))
        .datagram_receive_buffer_size(None)
        .build();
    let mut builder = Endpoint::builder(presets::Minimal)
        .secret_key(key)
        .alpns(vec![ALPN.to_vec()])
        .transport_config(transport)
        .portmapper_config(PortmapperConfig::Disabled)
        .clear_ip_transports()
        .relay_mode(if network.relays.is_empty() {
            RelayMode::Disabled
        } else {
            RelayMode::custom(network.relays.clone())
        });
    if let Some(bind) = network.bind {
        builder = builder.bind_addr(bind)?;
    }
    Ok(builder.bind().await?)
}

#[derive(Serialize, Deserialize)]
struct Hello {
    cluster: String,
    service: String,
}

struct Close(Connection);
impl Drop for Close {
    fn drop(&mut self) {
        self.0.close(0u32.into(), b"tunnel ended");
    }
}

async fn bridge(
    tcp: TcpStream,
    mut send: iroh::endpoint::SendStream,
    mut recv: iroh::endpoint::RecvStream,
) -> Result<()> {
    tcp.set_nodelay(true)?;
    let (mut read, mut write) = tcp.into_split();
    // copy uses fixed buffers and awaits backpressure. Each direction independently
    // propagates EOF; receiving a FIN must not truncate the response direction.
    tokio::try_join!(
        async {
            tokio::io::copy(&mut read, &mut send).await?;
            send.finish()?;
            Ok::<_, anyhow::Error>(())
        },
        async {
            tokio::io::copy(&mut recv, &mut write).await?;
            write.shutdown().await?;
            Ok::<_, anyhow::Error>(())
        }
    )?;
    // Wait for peer acknowledgement before Close tears down the QUIC connection.
    send.stopped().await?;
    Ok(())
}

async fn incoming(connection: Connection, view: Arc<View>) -> Result<()> {
    let connection = Close(connection);
    let peer = connection.0.remote_id();
    let (tcp, send, recv) = tokio::time::timeout(HANDSHAKE, async {
        let (mut send, mut recv) = connection.0.accept_bi().await?;
        let length = recv.read_u32().await? as usize;
        ensure!(length <= MAX_HEADER, "oversized tunnel header");
        let mut bytes = vec![0; length];
        recv.read_exact(&mut bytes).await?;
        let hello: Hello = serde_json::from_slice(&bytes)?;
        ensure!(hello.cluster == view.cluster, "tunnel authority mismatch");
        let service = view
            .topology
            .services
            .get(&hello.service)
            .context("unknown service")?;
        ensure!(
            service.owner == view.adapter
                && service.allowed.iter().any(|id| view
                    .topology
                    .adapters
                    .get(id)
                    .is_some_and(|a| a.endpoint.id == peer)),
            "service access denied"
        );
        let tcp = TcpStream::connect(service.target).await?;
        send.write_all(&[1]).await?;
        Ok::<_, anyhow::Error>((tcp, send, recv))
    })
    .await??;
    bridge(tcp, send, recv).await
}

async fn outgoing(
    endpoint: Endpoint,
    tcp: TcpStream,
    view: Arc<View>,
    service_id: String,
) -> Result<()> {
    let service = view
        .topology
        .services
        .get(&service_id)
        .context("unknown route")?;
    ensure!(
        service.allowed.contains(&view.adapter),
        "route access denied"
    );
    let owner = view
        .topology
        .adapters
        .get(&service.owner)
        .context("unknown owner")?;
    let connection = Close(
        tokio::time::timeout(HANDSHAKE, endpoint.connect(owner.endpoint.clone(), ALPN)).await??,
    );
    let (send, recv) = tokio::time::timeout(HANDSHAKE, async {
        let (mut send, mut recv) = connection.0.open_bi().await?;
        let bytes = serde_json::to_vec(&Hello {
            cluster: view.cluster.clone(),
            service: service_id,
        })?;
        ensure!(bytes.len() <= MAX_HEADER, "oversized tunnel header");
        send.write_u32(bytes.len() as u32).await?;
        send.write_all(&bytes).await?;
        ensure!(recv.read_u8().await? == 1, "service refused");
        Ok::<_, anyhow::Error>((send, recv))
    })
    .await??;
    bridge(tcp, send, recv).await
}

fn known_inbound(topology: &Topology, owner: &str, peer: EndpointId) -> bool {
    topology.services.values().any(|s| {
        s.owner == owner
            && s.allowed.iter().any(|id| {
                topology
                    .adapters
                    .get(id)
                    .is_some_and(|a| a.endpoint.id == peer)
            })
    })
}

struct Session {
    endpoint: Endpoint,
    view: Arc<View>,
    slots: Arc<Semaphore>,
    tasks: JoinSet<Result<()>>,
    listeners: JoinSet<Result<()>>,
    rx: tokio::sync::mpsc::Receiver<(TcpStream, String)>,
    _tx: tokio::sync::mpsc::Sender<(TcpStream, String)>,
}
impl Session {
    async fn bind(endpoint: Endpoint, view: View) -> Result<Self> {
        view.topology.validate()?;
        let view = Arc::new(view);
        let slots = Arc::new(Semaphore::new(MAX_STREAMS));
        let tasks = JoinSet::new();
        let mut listeners = JoinSet::new();
        // Bind the entire policy before accepting traffic. A collision fails closed.
        let mut bound = Vec::new();
        for route in &view.topology.routes {
            ensure!(
                route.adapter == view.adapter,
                "foreign route in adapter view"
            );
            bound.push((
                TcpListener::bind(route.listen).await?,
                route.service.clone(),
            ));
        }
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        for (listener, service) in bound {
            let tx = tx.clone();
            listeners.spawn(async move {
                loop {
                    let (socket, _) = listener.accept().await?;
                    // Never accumulate an unbounded queue of accepted local sockets.
                    match tx.try_send((socket, service.clone())) {
                        Ok(()) | Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {}
                        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => break,
                    }
                }
                Ok::<_, anyhow::Error>(())
            });
        }
        // Keep a sender alive for an adapter with no outbound routes.
        Ok(Self {
            endpoint,
            view,
            slots,
            tasks,
            listeners,
            rx,
            _tx: tx,
        })
    }
    async fn serve(&mut self) -> Result<()> {
        let Self {
            endpoint,
            view,
            slots,
            tasks,
            listeners,
            rx,
            ..
        } = self;
        loop {
            // Completed tasks still occupy JoinSet entries. Drain them before
            // accepting more work so a flood of short connections cannot accumulate
            // an unbounded completion queue after their semaphore permits release.
            while let Some(result) = tasks.try_join_next() {
                if let Err(error) = result? {
                    tracing::debug!(%error, "tunnel connection closed");
                }
            }
            tokio::select! {
                Some(incoming_conn) = endpoint.accept() => {
                    let Ok(permit) = slots.clone().try_acquire_owned() else { incoming_conn.refuse(); continue; };
                    let view = view.clone();
                    tasks.spawn(async move {
                        let _permit = permit;
                        let conn = tokio::time::timeout(HANDSHAKE, incoming_conn).await??;
                        if !known_inbound(&view.topology, &view.adapter, conn.remote_id()) {
                            conn.close(1u32.into(), b"unauthorized adapter");
                            return Ok(());
                        }
                        incoming(conn, view).await
                    });
                }
                Some((tcp, service)) = rx.recv() => {
                    let Ok(permit) = slots.clone().try_acquire_owned() else { continue; };
                    let (ep, view) = (endpoint.clone(), view.clone());
                    tasks.spawn(async move { let _permit = permit; outgoing(ep, tcp, view, service).await });
                }
                Some(result) = tasks.join_next(), if !tasks.is_empty() => {
                    if let Err(error) = result? { tracing::debug!(%error, "tunnel connection closed"); }
                }
                Some(result) = listeners.join_next(), if !listeners.is_empty() => { result??; anyhow::bail!("tunnel listener stopped"); }
            }
        }
    }
    async fn shutdown(mut self) {
        self.listeners.abort_all();
        self.tasks.abort_all();
        // Abort alone schedules cancellation. Join every task before acknowledging
        // replacement, so no task holding an old permission/socket remains alive.
        while self.listeners.join_next().await.is_some() {}
        while self.tasks.join_next().await.is_some() {}
    }
}

pub async fn run(endpoint: Endpoint, policies: watch::Receiver<View>) {
    let (status, _) = watch::channel(TransportStatus::Starting);
    run_observed(endpoint, policies, status).await;
}

pub async fn run_observed(
    endpoint: Endpoint,
    mut policies: watch::Receiver<View>,
    status: watch::Sender<TransportStatus>,
) {
    loop {
        let view = policies.borrow_and_update().clone();
        let revision = view.revision;
        status.send_replace(TransportStatus::Applying { revision });
        let binding = tokio::select! {
            result = Session::bind(endpoint.clone(), view) => result,
            changed = policies.changed() => { if changed.is_err() { break; } else { continue; } }
        };
        let error = match binding {
            Ok(mut session) => {
                status.send_replace(TransportStatus::Applied { revision });
                let result = tokio::select! {
                    result = session.serve() => Some(result),
                    changed = policies.changed() => { if changed.is_err() {session.shutdown().await; break;} None }
                };
                session.shutdown().await;
                match result {
                    None => continue,
                    Some(Ok(())) => "transport stopped".into(),
                    Some(Err(e)) => e.to_string(),
                }
            }
            Err(e) => e.to_string(),
        };
        tracing::warn!(%error, "tunnel policy failed closed; retrying");
        status.send_replace(TransportStatus::Failed {
            revision,
            error: error.chars().take(128).collect(),
        });
        tokio::select! { _ = tokio::time::sleep(Duration::from_secs(2)) => {}, changed = policies.changed() => { if changed.is_err() { break; } } }
    }
}
