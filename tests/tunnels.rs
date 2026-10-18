#![cfg(feature = "control-plane")]
use fleet::control::tunnels::{
    Adapter, Route, Service, Topology, View,
    transport::{self, Network},
};
use iroh::{Endpoint, EndpointAddr, SecretKey};
use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::watch,
};

async fn ep(relay: Option<iroh::RelayUrl>) -> Endpoint {
    let network = Network {
        bind: if relay.is_none() {
            Some("127.0.0.1:0".parse().unwrap())
        } else {
            None
        },
        relays: relay.into_iter().collect(),
    };
    let ep = transport::endpoint(SecretKey::from_bytes(&rand::random()), &network)
        .await
        .unwrap();
    if !network.relays.is_empty() {
        tokio::time::timeout(Duration::from_secs(10), ep.online())
            .await
            .unwrap();
    }
    ep
}
fn topology(
    a: EndpointAddr,
    b: EndpointAddr,
    target: std::net::SocketAddr,
    listen: std::net::SocketAddr,
) -> Topology {
    Topology {
        adapters: BTreeMap::from([
            ("a".into(), Adapter { endpoint: a }),
            ("b".into(), Adapter { endpoint: b }),
        ]),
        services: BTreeMap::from([(
            "echo".into(),
            Service {
                owner: "b".into(),
                target,
                allowed: BTreeSet::from(["a".into()]),
            },
        )]),
        routes: vec![Route {
            adapter: "a".into(),
            service: "echo".into(),
            listen,
        }],
    }
}
fn view(t: &Topology, adapter: &str, revision: u64) -> View {
    View {
        cluster: "test-authority".into(),
        adapter: adapter.into(),
        identity: t.adapters[adapter].endpoint.id,
        revision,
        topology: t.filtered(adapter),
    }
}
async fn connect(addr: std::net::SocketAddr) -> TcpStream {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match TcpStream::connect(addr).await {
                Ok(t) => return t,
                Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        }
    })
    .await
    .unwrap()
}
async fn half_close_roundtrip(relay: Option<iroh::RelayUrl>) {
    let a = ep(relay.clone()).await;
    let b = ep(relay).await;
    if a.addr().addrs.iter().all(|x| x.is_relay()) {
        assert!(b.addr().addrs.iter().all(|x| x.is_relay()));
    }
    let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let reservation = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let listen = reservation.local_addr().unwrap();
    drop(reservation);
    let t = topology(a.addr(), b.addr(), target.local_addr().unwrap(), listen);
    t.validate().unwrap();
    let (pa, ra) = watch::channel(view(&t, "a", 1));
    let (pb, rb) = watch::channel(view(&t, "b", 1));
    let ta = tokio::spawn(transport::run(a.clone(), ra));
    let tb = tokio::spawn(transport::run(b.clone(), rb));
    // The service deliberately waits for TCP EOF before replying, exercising
    // both half-close directions with data much larger than the stream window.
    let service = tokio::spawn(async move {
        let (mut stream, _) = target.accept().await.unwrap();
        let mut bytes = Vec::new();
        stream.read_to_end(&mut bytes).await.unwrap();
        stream.write_all(&bytes).await.unwrap();
        stream.shutdown().await.unwrap();
    });
    let mut socket = connect(listen).await;
    let expected: Vec<u8> = (0..2 * 1024 * 1024).map(|i| (i % 251) as u8).collect();
    tokio::time::timeout(Duration::from_secs(20), async {
        socket.write_all(&expected).await.unwrap();
        socket.shutdown().await.unwrap();
        let mut actual = Vec::new();
        socket.read_to_end(&mut actual).await.unwrap();
        assert_eq!(actual, expected);
        service.await.unwrap();
    })
    .await
    .unwrap();
    drop((pa, pb));
    ta.await.unwrap();
    tb.await.unwrap();
    a.close().await;
    b.close().await;
}

#[tokio::test]
async fn direct_large_transfer_preserves_tcp_half_close() {
    half_close_roundtrip(None).await;
}

#[tokio::test]
async fn forced_relay_large_transfer_without_any_direct_transport() {
    let mut config = iroh_relay::server::ServerConfig::default();
    config.relay = Some(iroh_relay::server::RelayConfig::new(
        "127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap(),
    ));
    let server = iroh_relay::server::Server::spawn(config).await.unwrap();
    let url = format!("http://{}", server.http_addr().unwrap())
        .parse()
        .unwrap();
    half_close_roundtrip(Some(url)).await;
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn authenticated_peer_cannot_choose_an_unregistered_service_or_authority() {
    let a = ep(None).await;
    let b = ep(None).await;
    let outsider = ep(None).await;
    let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut t = topology(
        a.addr(),
        b.addr(),
        target.local_addr().unwrap(),
        "127.0.0.1:31009".parse().unwrap(),
    );
    let secret = TcpListener::bind("127.0.0.1:0").await.unwrap();
    t.services.insert(
        "secret".into(),
        Service {
            owner: "b".into(),
            target: secret.local_addr().unwrap(),
            allowed: BTreeSet::new(),
        },
    );
    let (tx, rx) = watch::channel(view(&t, "b", 1));
    let task = tokio::spawn(transport::run(b.clone(), rx));
    for (endpoint, cluster, service) in [
        (&a, "wrong-authority", "echo"),
        (&a, "test-authority", "127.0.0.1:22"),
        (&a, "test-authority", "secret"),
        (&outsider, "test-authority", "echo"),
    ] {
        let denied = tokio::time::timeout(Duration::from_secs(5), async {
            let c = endpoint.connect(b.addr(), transport::ALPN).await?;
            let (mut send, mut recv) = c.open_bi().await?;
            let bytes =
                serde_json::to_vec(&serde_json::json!({"cluster":cluster,"service":service}))
                    .unwrap();
            send.write_u32(bytes.len() as u32).await?;
            send.write_all(&bytes).await?;
            recv.read_u8().await?;
            Ok::<_, anyhow::Error>(())
        })
        .await
        .unwrap();
        assert!(denied.is_err());
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(100), target.accept())
            .await
            .is_err()
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(100), secret.accept())
            .await
            .is_err()
    );
    let c = a.connect(b.addr(), transport::ALPN).await.unwrap();
    let (mut send, mut recv) = c.open_bi().await.unwrap();
    send.write_u32(u32::MAX).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(5), recv.read_u8())
            .await
            .unwrap()
            .is_err()
    );
    drop(tx);
    task.await.unwrap();
    a.close().await;
    b.close().await;
    outsider.close().await;
}

#[tokio::test]
async fn committed_revocation_closes_an_established_tunnel() {
    let a = ep(None).await;
    let b = ep(None).await;
    let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let reserved = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let listen = reserved.local_addr().unwrap();
    drop(reserved);
    let t = topology(a.addr(), b.addr(), target.local_addr().unwrap(), listen);
    let (pa, ra) = watch::channel(view(&t, "a", 1));
    let (pb, rb) = watch::channel(view(&t, "b", 1));
    let ta = tokio::spawn(transport::run(a.clone(), ra));
    let tb = tokio::spawn(transport::run(b.clone(), rb));
    let mut socket = connect(listen).await;
    socket.write_all(b"hello").await.unwrap();
    let (mut service, _) = tokio::time::timeout(Duration::from_secs(5), target.accept())
        .await
        .unwrap()
        .unwrap();
    let mut msg = [0; 5];
    service.read_exact(&mut msg).await.unwrap();
    assert_eq!(&msg, b"hello");
    let mut revoked = view(&t, "b", 2);
    revoked.topology = Topology::default();
    pb.send(revoked).unwrap();
    let read = tokio::time::timeout(Duration::from_secs(5), socket.read_u8())
        .await
        .unwrap();
    assert!(read.is_err());
    drop((pa, pb));
    ta.await.unwrap();
    tb.await.unwrap();
    a.close().await;
    b.close().await;
}

#[tokio::test]
async fn stalled_authenticated_handshakes_cannot_exceed_the_connection_budget() {
    let a = ep(None).await;
    let b = ep(None).await;
    let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let t = topology(
        a.addr(),
        b.addr(),
        target.local_addr().unwrap(),
        "127.0.0.1:31009".parse().unwrap(),
    );
    let (tx, rx) = watch::channel(view(&t, "b", 1));
    let task = tokio::spawn(transport::run(b.clone(), rx));
    let mut held = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), async {
        for _ in 0..64 {
            held.push(a.connect(b.addr(), transport::ALPN).await.unwrap());
        }
        assert!(a.connect(b.addr(), transport::ALPN).await.is_err());
    })
    .await
    .unwrap();
    for c in held {
        c.close(0u32.into(), b"fixture complete");
    }
    drop(tx);
    task.await.unwrap();
    a.close().await;
    b.close().await;
}

/// A local diagnostic, deliberately excluded from pass/fail CI timing thresholds.
/// Run explicitly with --ignored --nocapture; report the build profile with results.
#[tokio::test]
#[ignore = "local throughput diagnostic"]
async fn tunnel_throughput_diagnostic() {
    let payload: Vec<u8> = rand::random_iter::<u8>().take(8 << 20).collect();
    async fn transfer(addr: std::net::SocketAddr, payload: &[u8]) -> f64 {
        let start = std::time::Instant::now();
        let mut socket = connect(addr).await;
        socket.write_all(payload).await.unwrap();
        socket.shutdown().await.unwrap();
        let mut bytes = Vec::new();
        socket.read_to_end(&mut bytes).await.unwrap();
        assert_eq!(bytes, payload);
        start.elapsed().as_secs_f64()
    }
    let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = target.local_addr().unwrap();
    let echo = tokio::spawn(async move {
        let mut tasks = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                result=target.accept()=>{let(mut socket,_)=result.unwrap();tasks.spawn(async move{let mut bytes=Vec::new();socket.read_to_end(&mut bytes).await.unwrap();socket.write_all(&bytes).await.unwrap();});}
                _=tasks.join_next(),if !tasks.is_empty()=>{}
            }
        }
    });
    let mut samples = std::collections::BTreeMap::new();
    let mut direct = Vec::new();
    for _ in 0..3 {
        direct.push(transfer(addr, &payload).await);
    }
    samples.insert("tcp", direct);
    let mut relay_config = iroh_relay::server::ServerConfig::default();
    relay_config.relay = Some(iroh_relay::server::RelayConfig::new(
        "127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap(),
    ));
    let relay = iroh_relay::server::Server::spawn(relay_config)
        .await
        .unwrap();
    let relay_url = format!("http://{}", relay.http_addr().unwrap())
        .parse()
        .unwrap();
    for (name, relay_url) in [("iroh_direct", None), ("iroh_relay", Some(relay_url))] {
        let a = ep(relay_url.clone()).await;
        let b = ep(relay_url).await;
        let reserve = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let listen = reserve.local_addr().unwrap();
        drop(reserve);
        let t = topology(a.addr(), b.addr(), addr, listen);
        let (pa, ra) = watch::channel(view(&t, "a", 1));
        let (pb, rb) = watch::channel(view(&t, "b", 1));
        let ta = tokio::spawn(transport::run(a.clone(), ra));
        let tb = tokio::spawn(transport::run(b.clone(), rb));
        let mut times = Vec::new();
        for _ in 0..3 {
            times.push(transfer(listen, &payload).await);
        }
        samples.insert(name, times);
        drop((pa, pb));
        ta.await.unwrap();
        tb.await.unwrap();
        a.close().await;
        b.close().await;
    }
    echo.abort();
    relay.shutdown().await.unwrap();
    println!(
        "{}",
        serde_json::json!({"profile":if cfg!(debug_assertions){"debug"}else{"release"},"payload_bytes_each_direction":payload.len(),"roundtrip_seconds":samples,"scope":"single host, new connection per sample, local relay; no WAN latency or physical host-loss claim"})
    );
}
