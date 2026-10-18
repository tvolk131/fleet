use super::*;
use fleet::control::tunnels::{
    self, Adapter, Route, Service, Topology, agent::Agent, transport::Network,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

async fn activate(c: &mut Cluster) {
    assert_eq!(
        c.client
            .write(
                Request {
                    origin: "cluster".into(),
                    id: "tunnel-prepare".into(),
                    mutation: Mutation::PrepareVersion {
                        expected_revision: 0,
                        target: tunnels::GENERATION
                    }
                },
                WAIT
            )
            .await
            .unwrap(),
        Reply::Recorded
    );
    let end = tokio::time::Instant::now() + WAIT;
    loop {
        if c.client
            .read(WAIT)
            .await
            .unwrap()
            .version
            .pending
            .as_ref()
            .unwrap()
            .ready
            .len()
            == c.replicas.len()
        {
            break;
        }
        assert!(tokio::time::Instant::now() < end);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        c.client
            .write(
                Request {
                    origin: "cluster".into(),
                    id: "tunnel-activate".into(),
                    mutation: Mutation::ActivateVersion {
                        expected_revision: 1
                    }
                },
                WAIT
            )
            .await
            .unwrap(),
        Reply::Recorded
    );
}
fn replace(topology: Topology, revision: u64) -> Request {
    Request {
        origin: "cluster".into(),
        id: format!("tunnels-{revision}"),
        mutation: Mutation::ReplaceTunnels {
            expected_revision: revision,
            topology,
        },
    }
}
async fn descriptor(c: &Cluster, id: &str) -> Adapter {
    let sock = reserve_socket();
    let addr = sock.local_addr().unwrap();
    drop(sock);
    Agent::prepare(
        &c.home.0.join(id),
        id.into(),
        c.config.clone(),
        Network {
            bind: Some(addr),
            relays: vec![],
        },
    )
    .await
    .unwrap()
}
async fn echo(addr: std::net::SocketAddr) {
    tokio::time::timeout(WAIT, async {
        let mut stream = loop {
            if let Ok(s) = TcpStream::connect(addr).await {
                break s;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        stream.write_all(b"policy survived").await.unwrap();
        stream.shutdown().await.unwrap();
        let mut bytes = Vec::new();
        stream.read_to_end(&mut bytes).await.unwrap();
        assert_eq!(bytes, b"policy survived");
    })
    .await
    .unwrap();
}

async fn status(c: &mut Cluster) -> tunnels::status::Snapshot {
    match c.client.call(Message::TunnelStatus, WAIT).await.unwrap() {
        Answer::TunnelStatus(Ok(s)) => s,
        other => panic!("status: {other:?}"),
    }
}
async fn applied(c: &mut Cluster, agent: &mut Agent) -> tunnels::status::Snapshot {
    tokio::time::timeout(WAIT, async {
        loop {
            agent.tick().await.unwrap();
            let s = status(c).await;
            if s.all_confirmed {
                return s;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn enforcement_is_durable_but_leader_changes_restarts_and_late_reports_cannot_fake_freshness()
{
    let mut c = Cluster::new(3).await;
    activate(&mut c).await;
    let adapter = descriptor(&c, "a").await;
    let identity = adapter.endpoint.id;
    assert_eq!(
        c.client
            .write(
                replace(
                    Topology {
                        adapters: BTreeMap::from([("a".into(), adapter)]),
                        ..Default::default()
                    },
                    0
                ),
                WAIT
            )
            .await
            .unwrap(),
        Reply::Recorded
    );
    let s = status(&mut c).await;
    assert!(!s.all_confirmed);
    assert_eq!(s.adapters[0].state, "unknown");
    let mut agent = Agent::start(&c.home.0.join("a")).await.unwrap();
    let s = applied(&mut c, &mut agent).await;
    let old = s.adapters[0].observation.clone().unwrap();
    let before = c.client.read(WAIT).await.unwrap().last_applied;
    // Heartbeats and lost-reply retries update leader-local contact without
    // appending a consensus record or an ever-growing request receipt.
    agent.tick().await.unwrap();
    assert_eq!(c.client.read(WAIT).await.unwrap().last_applied, before);
    let leader = c.leader().await;
    c.replicas
        .remove(&leader)
        .unwrap()
        .shutdown()
        .await
        .unwrap();
    let s = status(&mut c).await;
    assert_eq!(s.adapters[0].observation, Some(old.clone()));
    assert_eq!(s.adapters[0].state, "unknown");
    assert!(!s.all_confirmed);
    applied(&mut c, &mut agent).await;
    agent.shutdown().await;
    let mut agent = Agent::start(&c.home.0.join("a")).await.unwrap();
    let s = applied(&mut c, &mut agent).await;
    let new = s.adapters[0].observation.clone().unwrap();
    assert!(new.report.boot > old.report.boot);
    let mut peer = Client::new(
        fleet::keys::load_or_create(&c.home.0.join("a/node.key")).unwrap(),
        "cluster".into(),
        c.config.peers.clone(),
    )
    .await
    .unwrap();
    assert!(matches!(
        peer.call(Message::TunnelObserve(old.report), WAIT)
            .await
            .unwrap(),
        Answer::TunnelObserve(Ok(Reply::Rejected { .. }))
    ));
    assert_eq!(
        status(&mut c).await.adapters[0].observation,
        Some(new.clone())
    );
    let mut forged = new.report;
    forged.adapter = "other-adapter".into();
    assert!(
        peer.call(Message::TunnelObserve(forged), WAIT)
            .await
            .is_err()
    );
    assert!(peer.call(Message::TunnelStatus, WAIT).await.is_err());
    assert_eq!(
        c.client
            .write(replace(Topology::default(), 1), WAIT)
            .await
            .unwrap(),
        Reply::Recorded
    );
    let pending = status(&mut c).await;
    assert!(pending.adapters[0].retired);
    assert_eq!(pending.adapters[0].state, "pending");
    let removed = applied(&mut c, &mut agent).await;
    assert!(removed.adapters[0].retired);
    assert_eq!(removed.adapters[0].desired_revision, 2);
    assert_eq!(removed.known_identities[&identity], "a");
    peer.endpoint.close().await;
    agent.shutdown().await;
    c.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wait_reports_failure_timeout_success_scope_and_supersession_without_mutating_policy() {
    use fleet::control::client::{ApiRequest, AppClient};
    let mut c = Cluster::new(1).await;
    activate(&mut c).await;
    let a = descriptor(&c, "a").await;
    let b = descriptor(&c, "b").await;
    let identity = a.endpoint.id;
    let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let occupied = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let t = Topology {
        adapters: BTreeMap::from([("a".into(), a), ("b".into(), b)]),
        services: BTreeMap::from([(
            "echo".into(),
            Service {
                owner: "b".into(),
                target: target.local_addr().unwrap(),
                allowed: BTreeSet::from(["a".into()]),
            },
        )]),
        routes: vec![Route {
            adapter: "a".into(),
            service: "echo".into(),
            listen: occupied.local_addr().unwrap(),
        }],
    };
    assert_eq!(
        c.client.write(replace(t, 0), WAIT).await.unwrap(),
        Reply::Recorded
    );
    let mut app = AppClient::connect(&c.home.0.join("app-client"))
        .await
        .unwrap();
    let mut agent = Agent::start(&c.home.0.join("a")).await.unwrap();
    let failed = tokio::time::timeout(WAIT, async {
        loop {
            agent.tick().await.unwrap();
            let s = app.execute(ApiRequest::Tunnels).await.unwrap();
            if s["adapters"]
                .as_array()
                .unwrap()
                .iter()
                .any(|r| r["state"] == "failed")
            {
                return s;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(failed["all_confirmed"], false);
    let all = ApiRequest::WaitTunnels {
        revision: 1,
        timeout_seconds: 1,
        identities: BTreeSet::new(),
    };
    let timeout = app.execute(all.clone()).await.unwrap();
    assert_eq!(timeout["confirmed"], false);
    assert_eq!(timeout["outcome"], "unconfirmed");
    assert_eq!(timeout["last_snapshot"]["revision"], 1);
    assert_eq!(c.client.read(WAIT).await.unwrap().tunnels.revision, 1);
    drop(occupied);
    tokio::time::timeout(WAIT, async {
        loop {
            agent.tick().await.unwrap();
            let s = status(&mut c).await;
            if s.adapters
                .iter()
                .any(|r| r.identity == identity && r.state == "applied")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let selected = ApiRequest::WaitTunnels {
        revision: 1,
        timeout_seconds: 2,
        identities: BTreeSet::from([identity]),
    };
    assert_eq!(app.execute(selected).await.unwrap()["confirmed"], true);
    // The absent owner still prevents a claim that the entire topology is enforced.
    assert_eq!(app.execute(all).await.unwrap()["confirmed"], false);
    assert_eq!(
        c.client
            .write(replace(Topology::default(), 1), WAIT)
            .await
            .unwrap(),
        Reply::Recorded
    );
    let old = ApiRequest::WaitTunnels {
        revision: 1,
        timeout_seconds: 2,
        identities: BTreeSet::new(),
    };
    assert_eq!(app.execute(old).await.unwrap()["outcome"], "superseded");
    agent.shutdown().await;
    app.transport.endpoint.close().await;
    c.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tunnel_catalog_is_gated_replicated_and_not_writable_by_adapters() {
    let mut c = Cluster::new(3).await;
    let request = replace(Topology::default(), 0);
    assert!(
        c.client
            .write(request.clone(), WAIT)
            .await
            .unwrap_err()
            .to_string()
            .contains("inactive")
    );
    assert!(
        !c.client
            .read(WAIT)
            .await
            .unwrap()
            .receipts
            .values()
            .any(|r| r
                .command
                .as_ref()
                .is_some_and(|v| v.get("ReplaceTunnels").is_some()))
    );
    activate(&mut c).await;
    let adapter = descriptor(&c, "test-adapter").await;
    let t = Topology {
        adapters: BTreeMap::from([("test-adapter".into(), adapter)]),
        ..Default::default()
    };
    let request = replace(t.clone(), 0);
    assert_eq!(
        c.client.write(request.clone(), WAIT).await.unwrap(),
        Reply::Recorded
    );
    let leader = c.leader().await;
    c.replicas
        .remove(&leader)
        .unwrap()
        .shutdown()
        .await
        .unwrap();
    assert_eq!(
        c.client.write(request, WAIT).await.unwrap(),
        Reply::Recorded
    );
    assert_eq!(c.client.read(WAIT).await.unwrap().tunnels.topology, t);
    let mut adapter_client = Client::new(
        fleet::keys::load_or_create(&c.home.0.join("test-adapter/node.key")).unwrap(),
        "cluster".into(),
        c.config.peers.clone(),
    )
    .await
    .unwrap();
    assert!(
        adapter_client
            .write(replace(Topology::default(), 1), WAIT)
            .await
            .is_err()
    );
    assert!(adapter_client.read(WAIT).await.is_err());
    let answer = adapter_client
        .call(
            Message::TunnelRead {
                adapter: "test-adapter".into(),
            },
            WAIT,
        )
        .await
        .unwrap();
    assert!(matches!(answer, Answer::TunnelRead(Ok(v)) if v.revision==1));
    adapter_client.endpoint.close().await;
    c.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn adapter_restart_and_controller_outage_preserve_durable_routes_then_revocation_removes_them()
 {
    let mut c = Cluster::new(1).await;
    activate(&mut c).await;
    let a = descriptor(&c, "a").await;
    let b = descriptor(&c, "b").await;
    let server = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let target = server.local_addr().unwrap();
    let echo_server = tokio::spawn(async move {
        loop {
            let (mut stream, _) = server.accept().await.unwrap();
            tokio::spawn(async move {
                let mut bytes = Vec::new();
                stream.read_to_end(&mut bytes).await.unwrap();
                let _ = stream.write_all(&bytes).await;
            });
        }
    });
    let reserve = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let listen = reserve.local_addr().unwrap();
    drop(reserve);
    let topology = Topology {
        adapters: BTreeMap::from([("a".into(), a), ("b".into(), b)]),
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
    };
    assert_eq!(
        c.client.write(replace(topology, 0), WAIT).await.unwrap(),
        Reply::Recorded
    );
    let mut a = Agent::start(&c.home.0.join("a")).await.unwrap();
    let mut b = Agent::start(&c.home.0.join("b")).await.unwrap();
    a.tick().await.unwrap();
    b.tick().await.unwrap();
    echo(listen).await;
    c.replicas.remove(&1).unwrap().shutdown().await.unwrap();
    a.shutdown().await;
    let mut a = Agent::start(&c.home.0.join("a")).await.unwrap();
    // No controller is running. A new transport connection must use the durable cache.
    echo(listen).await;
    a.tick().await.unwrap();
    echo(listen).await;
    c.start(1).await;
    assert_eq!(
        c.client
            .write(replace(Topology::default(), 1), WAIT)
            .await
            .unwrap(),
        Reply::Recorded
    );
    a.tick().await.unwrap();
    b.tick().await.unwrap();
    let deadline = tokio::time::Instant::now() + WAIT;
    while TcpStream::connect(listen).await.is_ok() {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    a.shutdown().await;
    b.shutdown().await;
    echo_server.abort();
    let cached: tunnels::View =
        serde_json::from_slice(&std::fs::read(c.home.0.join("a/policy.json")).unwrap()).unwrap();
    assert_eq!(cached.revision, 2);
    assert_eq!(cached.topology, Topology::default());
    c.close().await;
}

#[test]
fn catalog_rejects_lan_targets_proxy_loops_and_missing_permissions() {
    let a = iroh::EndpointAddr::new(SecretKey::from_bytes(&rand::random()).public())
        .with_ip_addr("127.0.0.1:13001".parse().unwrap());
    let b = iroh::EndpointAddr::new(SecretKey::from_bytes(&rand::random()).public())
        .with_ip_addr("127.0.0.1:13002".parse().unwrap());
    let t = Topology {
        adapters: BTreeMap::from([
            ("a".into(), Adapter { endpoint: a }),
            ("b".into(), Adapter { endpoint: b }),
        ]),
        services: BTreeMap::from([(
            "s".into(),
            Service {
                owner: "a".into(),
                target: "127.0.0.1:14001".parse().unwrap(),
                allowed: BTreeSet::from(["b".into()]),
            },
        )]),
        routes: vec![Route {
            adapter: "b".into(),
            service: "s".into(),
            listen: "127.0.0.1:14001".parse().unwrap(),
        }],
    };
    t.validate().unwrap();
    let mut bad = t.clone();
    bad.services.get_mut("s").unwrap().target = "192.168.1.1:80".parse().unwrap();
    assert!(bad.validate().is_err());
    let mut bad = t.clone();
    bad.routes[0].adapter = "a".into();
    assert!(bad.validate().is_err());
    let mut bad = t.clone();
    bad.routes.push(bad.routes[0].clone());
    assert!(bad.validate().is_err());
    let mut bad = t;
    bad.services.get_mut("s").unwrap().allowed.clear();
    assert!(bad.validate().is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovered_authority_requires_explicit_adapter_rebind_and_reviewed_routes() {
    let mut c = Cluster::new(1).await;
    activate(&mut c).await;
    let descriptor = descriptor(&c, "adapter").await;
    let identity = descriptor.endpoint.id;
    let topology = Topology {
        adapters: BTreeMap::from([("adapter".into(), descriptor)]),
        services: BTreeMap::from([(
            "local-service".into(),
            Service {
                owner: "adapter".into(),
                target: "127.0.0.1:29998".parse().unwrap(),
                allowed: BTreeSet::new(),
            },
        )]),
        routes: vec![],
    };
    assert_eq!(
        c.client
            .write(replace(topology.clone(), 0), WAIT)
            .await
            .unwrap(),
        Reply::Recorded
    );
    let home = c.home.0.join("adapter");
    let mut adapter = Agent::start(&home).await.unwrap();
    adapter.tick().await.unwrap();
    adapter.shutdown().await;
    let (_, manifest) = super::real_worker::recovery::restore(&mut c, 1, "tunnel-recovery").await;
    let state = c.client.read(WAIT).await.unwrap();
    assert_eq!(state.tunnels.topology, Topology::default());
    assert!(state.tunnels.known_identities.contains_key(&identity));
    assert!(state.tunnels.observations.is_empty());
    let old: tunnels::View =
        serde_json::from_slice(&std::fs::read(home.join("policy.json")).unwrap()).unwrap();
    assert_ne!(old.cluster, state.cluster);
    // Mere recovery on the controller does not rewrite a disconnected adapter.
    let mut forged = manifest.clone();
    forged.plan.config.cluster.push_str("-forged");
    assert!(tunnels::agent::accept_recovery(&home, &forged).is_err());
    tunnels::agent::accept_recovery(&home, &manifest).unwrap();
    tunnels::agent::accept_recovery(&home, &manifest).unwrap();
    let mut adapter = Agent::start(&home).await.unwrap();
    adapter.tick().await.unwrap();
    let empty: tunnels::View =
        serde_json::from_slice(&std::fs::read(home.join("policy.json")).unwrap()).unwrap();
    assert_eq!(empty.cluster, state.cluster);
    assert_eq!(empty.topology, Topology::default());
    let mut request = replace(topology, state.tunnels.revision);
    request.origin = state.cluster;
    assert_eq!(
        c.client.write(request, WAIT).await.unwrap(),
        Reply::Recorded
    );
    adapter.tick().await.unwrap();
    adapter.shutdown().await;
    let restored: tunnels::View =
        serde_json::from_slice(&std::fs::read(home.join("policy.json")).unwrap()).unwrap();
    assert!(restored.topology.services.contains_key("local-service"));
    c.close().await;
}
