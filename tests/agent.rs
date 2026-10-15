//! An agent and coordinators as in-process endpoints over localhost, with relays and address lookup off.

use std::collections::HashSet;

use fleet::{
    agent::{Agent, AgentConfig},
    blobs,
    coordinator::Coordinator,
    paths,
    proto::{ALPN, Hello, PROTOCOL, Protocol, Request, Response, describe, request},
};
use iroh::{Endpoint, RelayMode, SecretKey, endpoint::presets, protocol::Router};

async fn endpoint(key: SecretKey) -> Endpoint {
    Endpoint::builder(presets::Minimal)
        .relay_mode(RelayMode::Disabled)
        .secret_key(key)
        .bind()
        .await
        .expect("bind")
}

fn test_hello() -> Hello {
    Hello {
        agent_version: "test".into(),
        protocol: PROTOCOL,
        hostname: "worker".into(),
        os: "macOS".into(),
        cpu: "Apple M1 Pro".into(),
        cores: 8,
        memory_bytes: 32 << 30,
        gpu: Some("Apple M1 Pro, 14 cores, Metal 4".into()),
        tags: vec!["os:macos".into(), "gpu:metal".into()],
        sleep_minutes: Some(1),
        cpu_slots: 8,
    }
}

async fn agent(coordinator: iroh::EndpointId) -> Agent {
    agent_saying(coordinator, test_hello()).await
}

async fn agent_saying(coordinator: iroh::EndpointId, hello: Hello) -> Agent {
    let home = std::env::temp_dir().join(format!(
        "fleet-agent-test-{}-{:08x}",
        std::process::id(),
        rand::random::<u32>()
    ));
    let store = blobs::open(&home.join("agent-blobs")).await.unwrap();
    let config = AgentConfig {
        home,
        supervisor: env!("CARGO_BIN_EXE_fleet").into(),
        store,
    };
    Agent::start(
        std::sync::Arc::new(HashSet::from([coordinator])),
        hello,
        config,
    )
    .unwrap()
}

#[tokio::test]
async fn an_allowed_coordinator_gets_answers_over_a_direct_path() {
    let coordinator = SecretKey::generate();
    let agent = agent(coordinator.public()).await;
    let router = Router::builder(endpoint(SecretKey::generate()).await)
        .accept(ALPN, agent)
        .spawn();
    let client = endpoint(coordinator).await;
    let connection = client
        .connect(router.endpoint().addr(), ALPN)
        .await
        .expect("connect");

    assert_eq!(
        request(&connection, &Request::hello()).await.unwrap(),
        Response::Hello(test_hello())
    );
    assert!(matches!(
        request(&connection, &Request::Status).await.unwrap(),
        Response::Status(_)
    ));
    // Several requests share one connection, each on its own stream.
    for _ in 0..3 {
        assert!(matches!(
            request(&connection, &Request::hello()).await.unwrap(),
            Response::Hello(_)
        ));
    }
    let report = paths(&connection);
    assert!(
        report.iter().any(|p| p.kind == "direct" && p.selected),
        "{report:?}"
    );

    client.close().await;
    router.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_key_not_on_the_allowlist_is_refused() {
    let coordinator = SecretKey::generate();
    let agent = agent(coordinator.public()).await;
    let router = Router::builder(endpoint(SecretKey::generate()).await)
        .accept(ALPN, agent)
        .spawn();
    let stranger = endpoint(SecretKey::generate()).await;
    let connection = stranger
        .connect(router.endpoint().addr(), ALPN)
        .await
        .expect("the handshake itself succeeds");

    let error = request(&connection, &Request::hello())
        .await
        .expect_err("the agent must refuse");
    assert!(error.to_string().contains("allowlist"), "{error:#}");

    stranger.close().await;
    router.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_request_from_a_newer_coordinator_is_refused_by_name() {
    let coordinator = SecretKey::generate();
    let agent = agent(coordinator.public()).await;
    let router = Router::builder(endpoint(SecretKey::generate()).await)
        .accept(ALPN, agent)
        .spawn();
    let client = endpoint(coordinator).await;
    let connection = client
        .connect(router.endpoint().addr(), ALPN)
        .await
        .unwrap();
    // A request this version of the agent does not know, as a later coordinator might send it.
    let (mut send, mut recv) = connection.open_bi().await.unwrap();
    send.write_all(br#"{"Teleport": {"to": "mars"}}"#)
        .await
        .unwrap();
    send.finish().unwrap();
    let answer: Response =
        serde_json::from_slice(&recv.read_to_end(1 << 20).await.unwrap()).unwrap();
    let Response::Error(message) = answer else {
        panic!("{answer:?}")
    };
    assert!(message.contains("does not support `Teleport`"), "{message}");
    // The connection still serves requests it knows.
    assert!(matches!(
        request(&connection, &Request::hello()).await.unwrap(),
        Response::Hello(_)
    ));
    client.close().await;
    router.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_coordinator_learns_an_agents_version_before_its_first_request() {
    let coordinator = SecretKey::generate();
    // An agent from a later minor version of the protocol.
    let later = Protocol {
        minor: PROTOCOL.minor + 7,
        ..PROTOCOL
    };
    let agent = agent_saying(
        coordinator.public(),
        Hello {
            protocol: later,
            ..test_hello()
        },
    )
    .await;
    let router = Router::builder(endpoint(SecretKey::generate()).await)
        .accept(ALPN, agent)
        .spawn();
    let dir = std::env::temp_dir().join(format!(
        "fleet-agent-test-versions-{}-{:08x}",
        std::process::id(),
        rand::random::<u32>()
    ));
    let client = Coordinator::new(
        endpoint(coordinator).await,
        blobs::open(&dir.join("coordinator-blobs")).await.unwrap(),
    );
    let connection = client.connect(router.endpoint().addr()).await.unwrap();
    // Its first request asks the agent's Hello first, so it can check the request against the agent's version.
    assert!(matches!(
        client.ask(&connection, &Request::Status).await.unwrap(),
        Response::Status(_)
    ));
    assert_eq!(
        client.agent_protocol(&connection).await.unwrap(),
        (later, "worker".to_string())
    );
    assert_eq!(PROTOCOL.agreed(later), PROTOCOL);
    assert!(describe(later).contains("newer than this coordinator's"));
    client.endpoint.close().await;
    router.shutdown().await.unwrap();
    std::fs::remove_dir_all(&dir).ok();
}
