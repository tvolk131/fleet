use super::*;

#[test]
fn advertised_contacts_reject_wildcards_multicast_and_zero_ports() {
    use fleet::control::network::validate_address;
    for address in [
        "0.0.0.0:1234",
        "[::]:1234",
        "127.0.0.1:0",
        "224.0.0.1:1234",
        "255.255.255.255:1234",
        "[ff02::1]:1234",
    ] {
        assert!(
            validate_address(address.parse().unwrap()).is_err(),
            "{address}"
        );
    }
    for address in ["127.0.0.1:1234", "192.168.1.10:1234", "[fd00::1]:1234"] {
        validate_address(address.parse().unwrap()).unwrap();
    }
}

#[test]
fn initial_remote_contacts_are_prepared_without_binding_remote_interfaces() {
    let home = Home::new();
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_fleet-control-lab"))
        .args(["init", "--home"])
        .arg(&home.0)
        .args([
            "--address",
            "192.0.2.1:28001",
            "--address",
            "192.0.2.2:28001",
            "--address",
            "192.0.2.3:28001",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let config: Config = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(config.peers.len(), 3);
    for id in 1..=3 {
        assert_eq!(
            config.peers[&id].address.to_string(),
            format!("192.0.2.{id}:28001")
        );
        let saved: Config = serde_json::from_slice(
            &std::fs::read(home.0.join(format!("node-{id}/config.json"))).unwrap(),
        )
        .unwrap();
        assert_eq!(saved.peers, config.peers);
    }
    let bad = Home::new();
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_fleet-control-lab"))
        .args(["init", "--home"])
        .arg(&bad.0)
        .args(["--replicas", "1", "--address", "0.0.0.0:28001"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(!bad.0.join("client/node.key").exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_diagnostics_remain_authenticated_and_distinct_from_quorum_reads() {
    let mut c = Cluster::new(3).await;
    let leader = c.leader().await;
    let initial = c.client.inspect(leader, WAIT).await.unwrap();
    assert_eq!(initial.id, leader);
    assert_eq!(initial.leader, Some(leader));
    let stranger = Client::new(
        SecretKey::generate(),
        c.config.cluster.clone(),
        c.config.peers.clone(),
    )
    .await
    .unwrap();
    assert!(
        stranger
            .inspect(leader, Duration::from_secs(2))
            .await
            .is_err()
    );
    stranger.endpoint.close().await;
    for id in c.replicas.keys().copied().collect::<Vec<_>>() {
        if id != leader {
            c.replicas.remove(&id).unwrap().shutdown().await.unwrap();
        }
    }
    assert!(c.client.read(Duration::from_secs(2)).await.is_err());
    let diagnostic = c.client.inspect(leader, WAIT).await.unwrap();
    assert_eq!(diagnostic.id, leader);
    assert_eq!(
        diagnostic.membership.voter_sets,
        vec![BTreeSet::from([1, 2, 3])]
    );
    assert_eq!(diagnostic.last_applied, initial.last_applied);
    c.close().await;
}
