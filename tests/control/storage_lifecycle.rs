use super::*;
use fleet::control::{
    artifact::{self, CopyPhase, Kind},
    artifact_io,
    storage_lifecycle::Phase,
};

async fn write(c: &mut Cluster, id: &str, mutation: Mutation) -> Reply {
    c.client
        .write(
            Request {
                origin: c.config.cluster.clone(),
                id: id.into(),
                mutation,
            },
            WAIT,
        )
        .await
        .unwrap()
}
fn rejected(reply: Reply) {
    assert!(matches!(reply, Reply::Rejected { .. }), "{reply:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn artifact_authorization_follows_role_discovery_after_every_bootstrap_replica_retires() {
    let mut c = Cluster::new(3).await;
    let (home, config) = prepare(&mut c, false).await;
    let mut w = Worker::start(&home, config.clone(), hello()).await.unwrap();
    let (backup_home, backup_config, mut backup) =
        storage::backup(&mut c, "backup", 16 << 20).await;
    write(
        &mut c,
        "policy",
        Mutation::StoragePolicy {
            expected_revision: 0,
            policy: artifact::Policy {
                outputs: BTreeSet::from(["backup".into()]),
                ..Default::default()
            },
        },
    )
    .await;
    let a = output(&mut c, &mut w).await;
    backup.tick().await.unwrap();
    for id in 4..=6 {
        c.prepare(id, true).await;
    }
    let request = c.membership_request("rotate-all", 1, &[4, 5, 6]);
    c.changed(request).await;
    // Only normal role polling learns the new membership. Neither artifact handler is called.
    w.tick().await.unwrap();
    backup.tick().await.unwrap();
    for id in 1..=3 {
        c.replicas.remove(&id).unwrap().shutdown().await.unwrap();
    }
    for (name, peer) in [
        ("worker", &config.registration.peer),
        ("storage", &backup_config.registration.peer),
    ] {
        artifact_io::fetch(
            &c.client.endpoint,
            peer,
            "cluster",
            &a.manifest,
            &c.home.0.join(format!("rotated-{name}")),
        )
        .await
        .unwrap();
    }
    w.shutdown().await.unwrap();
    backup.shutdown().await.unwrap();
    // Original on-disk configs still name 1/2/3. The authority-scoped discovery cache survives.
    let mut w = Worker::start(&home, config.clone(), hello()).await.unwrap();
    let mut backup = fleet::control::storage::Agent::start(&backup_home, backup_config.clone())
        .await
        .unwrap();
    w.tick().await.unwrap();
    backup.tick().await.unwrap();
    for (name, peer) in [
        ("worker", &config.registration.peer),
        ("storage", &backup_config.registration.peer),
    ] {
        artifact_io::fetch(
            &c.client.endpoint,
            peer,
            "cluster",
            &a.manifest,
            &c.home.0.join(format!("restarted-{name}")),
        )
        .await
        .unwrap();
    }
    w.shutdown().await.unwrap();
    backup.shutdown().await.unwrap();
    c.close().await;
}
async fn output(c: &mut Cluster, w: &mut Worker) -> artifact::Artifact {
    let mut r = super::super::submit("submit", "lifecycle-job");
    if let Mutation::Enqueue(s) = &mut r.mutation {
        s.workload = workload("printf retained-data > result.txt");
    }
    c.client.write(r, WAIT).await.unwrap();
    wait(
        c,
        w,
        "lifecycle-job-a1",
        WorkPhase::Job(AttemptPhase::Succeeded),
    )
    .await;
    w.tick().await.unwrap();
    c.client
        .read(WAIT)
        .await
        .unwrap()
        .storage
        .artifacts
        .values()
        .find(|a| a.manifest.kind == Kind::FinalOutputs)
        .unwrap()
        .clone()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replacement_survives_leader_loss_restarts_and_revokes_stale_data_access() {
    let mut c = Cluster::new(3).await;
    let (home, config) = prepare(&mut c, false).await;
    let mut w = Worker::start(&home, config, hello()).await.unwrap();
    let (old_home, old_config, mut old) = storage::backup(&mut c, "old-store", 16 << 20).await;
    let (new_home, new_config, mut new) = storage::backup(&mut c, "new-store", 16 << 20).await;
    write(
        &mut c,
        "policy",
        Mutation::StoragePolicy {
            expected_revision: 0,
            policy: artifact::Policy {
                outputs: BTreeSet::from(["old-store".into()]),
                ..Default::default()
            },
        },
    )
    .await;
    let a = output(&mut c, &mut w).await;
    old.tick().await.unwrap();
    write(
        &mut c,
        "pin",
        Mutation::ArtifactRetention {
            id: a.manifest.id.clone(),
            expected_revision: a.revision,
            pinned: true,
            expires_at: None,
        },
    )
    .await;
    // Keep the worker's cached readers unchanged after retirement to exercise the fresh gate.
    w.tick().await.unwrap();
    let replacement = Mutation::ReplaceStorage {
        storage: "old-store".into(),
        replacement: "new-store".into(),
        expected_revision: 0,
    };
    assert_eq!(
        write(&mut c, "replace", replacement.clone()).await,
        Reply::Recorded
    );
    let pending = c
        .client
        .read(WAIT)
        .await
        .unwrap()
        .storage_plan("old-store")
        .unwrap();
    rejected(
        write(
            &mut c,
            "early-retire",
            Mutation::RetireStorage {
                storage: "old-store".into(),
                expected_revision: 1,
                expected_inventory: pending.inventory,
            },
        )
        .await,
    );
    let leader = c.leader().await;
    c.replicas
        .remove(&leader)
        .unwrap()
        .shutdown()
        .await
        .unwrap();
    assert_eq!(write(&mut c, "replace", replacement).await, Reply::Recorded);
    new.tick().await.unwrap();
    // Restart the destination and verify its committed files before old-copy cleanup.
    new.shutdown().await.unwrap();
    let mut new = fleet::control::storage::Agent::start(&new_home, new_config.clone())
        .await
        .unwrap();
    new.tick().await.unwrap();
    let state = c.client.read(WAIT).await.unwrap();
    assert_eq!(
        state.storage.artifacts[&a.manifest.id].copies["storage/new-store"].phase,
        CopyPhase::Ready
    );
    let plan = state.storage_plan("old-store").unwrap();
    assert_eq!(
        write(
            &mut c,
            "retire",
            Mutation::RetireStorage {
                storage: "old-store".into(),
                expected_revision: 1,
                expected_inventory: plan.inventory
            }
        )
        .await,
        Reply::Recorded
    );
    let stale = fleet::control::network::connect_endpoint(
        fleet::keys::load_or_create(&old_home.join("node.key")).unwrap(),
        "127.0.0.1:0".parse().unwrap(),
    )
    .await
    .unwrap();
    assert!(
        artifact_io::fetch(
            &stale,
            &new_config.registration.peer,
            "cluster",
            &a.manifest,
            &c.home.0.join("denied-stale-reader")
        )
        .await
        .is_err()
    );
    assert!(
        artifact_io::fetch(
            &stale,
            &state.workers["worker-real"].peer,
            "cluster",
            &a.manifest,
            &c.home.0.join("denied-stale-worker-cache")
        )
        .await
        .is_err()
    );
    // Even the old server's own pre-retirement object cache cannot authorize a new read.
    assert!(
        artifact_io::fetch(
            &c.client.endpoint,
            &old_config.registration.peer,
            "cluster",
            &a.manifest,
            &c.home.0.join("denied-stale-source")
        )
        .await
        .is_err()
    );
    old.tick().await.unwrap();
    assert!(!old_home.join("objects").join(&a.manifest.id).exists());
    assert_eq!(
        write(
            &mut c,
            "finish",
            Mutation::FinishStorageRetirement {
                storage: "old-store".into(),
                expected_revision: 2
            }
        )
        .await,
        Reply::Recorded
    );
    assert!(old.tick().await.is_err());
    old.shutdown().await.unwrap();
    let mut old = fleet::control::storage::Agent::start(&old_home, old_config)
        .await
        .unwrap();
    assert!(old.tick().await.is_err());
    let state = c.client.read(WAIT).await.unwrap();
    assert_eq!(state.storage_lifecycle("old-store").phase, Phase::Retired);
    assert!(state.storage.artifacts[&a.manifest.id].pinned);
    w.shutdown().await.unwrap();
    std::fs::rename(home.join("backend"), home.join("unavailable-source")).unwrap();
    artifact_io::fetch_available(
        &c.client.endpoint,
        &state,
        &state.storage.artifacts[&a.manifest.id],
        &c.home.0.join("export"),
    )
    .await
    .unwrap();
    assert_eq!(
        std::fs::read(c.home.0.join("export/result.txt")).unwrap(),
        b"retained-data"
    );
    let follower = *c.replicas.keys().next().unwrap();
    c.replicas
        .remove(&follower)
        .unwrap()
        .shutdown()
        .await
        .unwrap();
    assert!(
        artifact_io::fetch(
            &c.client.endpoint,
            &new_config.registration.peer,
            "cluster",
            &a.manifest,
            &c.home.0.join("no-quorum-export")
        )
        .await
        .is_err(),
        "cached permissions cannot authorize a new transfer without quorum"
    );
    c.start(follower).await;
    c.start(leader).await;
    let restarted = c.client.read(WAIT).await.unwrap();
    assert_eq!(restarted.jobs, state.jobs);
    assert_eq!(
        restarted.storage_lifecycle("old-store").phase,
        Phase::Retired
    );
    stale.close().await;
    old.shutdown().await.unwrap();
    new.shutdown().await.unwrap();
    c.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_agent_is_fenced_without_claiming_erasure_or_recovery_of_unavailable_payloads() {
    let mut c = Cluster::new(3).await;
    let (home, config) = prepare(&mut c, false).await;
    let mut w = Worker::start(&home, config, hello()).await.unwrap();
    let (lost_home, lost_config, mut lost) = storage::backup(&mut c, "lost-store", 16 << 20).await;
    let (new_home, _, mut new) = storage::backup(&mut c, "replacement", 16 << 20).await;
    write(
        &mut c,
        "policy",
        Mutation::StoragePolicy {
            expected_revision: 0,
            policy: artifact::Policy {
                outputs: BTreeSet::from(["lost-store".into()]),
                ..Default::default()
            },
        },
    )
    .await;
    let a = output(&mut c, &mut w).await;
    lost.tick().await.unwrap();
    // A current Missing observation removes the worker from the known-survivor set.
    std::fs::remove_file(home.join("backend/jobs/exec-lifecycle-job-a1/work/result.txt")).unwrap();
    w.tick().await.unwrap();
    let before = c.client.read(WAIT).await.unwrap();
    let plan = before.storage_plan("lost-store").unwrap();
    assert_eq!(
        plan.unavailable_without_agent,
        BTreeSet::from([a.manifest.id.clone()])
    );
    let mut request = Mutation::DeclareStorageLost {
        storage: "lost-store".into(),
        replacement: "replacement".into(),
        expected_revision: 0,
        expected_inventory: plan.inventory,
        unavailable_artifacts: BTreeSet::new(),
    };
    rejected(write(&mut c, "missing-ack", request.clone()).await);
    if let Mutation::DeclareStorageLost {
        unavailable_artifacts,
        ..
    } = &mut request
    {
        *unavailable_artifacts = plan.unavailable_without_agent;
    }
    assert_eq!(
        write(&mut c, "declare-lost", request.clone()).await,
        Reply::Recorded
    );
    assert_eq!(
        write(&mut c, "declare-lost", request).await,
        Reply::Recorded
    );
    // Lost is an operator decision; even an agent still online with cached data must be fenced.
    assert!(
        artifact_io::fetch(
            &c.client.endpoint,
            &lost_config.registration.peer,
            "cluster",
            &a.manifest,
            &c.home.0.join("lost-export")
        )
        .await
        .is_err()
    );
    assert!(lost.tick().await.is_err());
    new.tick().await.unwrap();
    let state = c.client.read(WAIT).await.unwrap();
    let art = &state.storage.artifacts[&a.manifest.id];
    assert!(!art.deleted && !state.durable_artifact(art));
    assert_eq!(art.copies["storage/lost-store"].phase, CopyPhase::Lost);
    assert_eq!(art.copies["storage/replacement"].phase, CopyPhase::Failed);
    assert!(!new_home.join("objects").join(&a.manifest.id).exists());
    assert!(
        artifact_io::fetch_available(
            &c.client.endpoint,
            &state,
            art,
            &c.home.0.join("unavailable-export")
        )
        .await
        .is_err()
    );
    assert_eq!(state.jobs, before.jobs);
    assert!(
        lost_home.join("objects").join(&a.manifest.id).is_dir(),
        "loss must not pretend to erase an unreachable disk"
    );
    lost.shutdown().await.unwrap();
    let mut lost = fleet::control::storage::Agent::start(&lost_home, lost_config)
        .await
        .unwrap();
    assert!(lost.tick().await.is_err());
    lost.shutdown().await.unwrap();
    new.shutdown().await.unwrap();
    w.shutdown().await.unwrap();
    c.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn revoked_storage_observations_survive_explicit_disaster_recovery_review() {
    let mut c = Cluster::new(1).await;
    let (home, config) = prepare(&mut c, false).await;
    instances::write(&home.join("worker.json"), &config).unwrap();
    let mut w = Worker::start(&home, config, hello()).await.unwrap();
    let (old_home, old_config, mut old) = storage::backup(&mut c, "old-store", 16 << 20).await;
    let (new_home, _, mut new) = storage::backup(&mut c, "new-store", 16 << 20).await;
    write(
        &mut c,
        "policy",
        Mutation::StoragePolicy {
            expected_revision: 0,
            policy: artifact::Policy {
                outputs: BTreeSet::from(["old-store".into()]),
                ..Default::default()
            },
        },
    )
    .await;
    let a = output(&mut c, &mut w).await;
    old.tick().await.unwrap();
    write(
        &mut c,
        "replace",
        Mutation::ReplaceStorage {
            storage: "old-store".into(),
            replacement: "new-store".into(),
            expected_revision: 0,
        },
    )
    .await;
    new.tick().await.unwrap();
    let plan = c
        .client
        .read(WAIT)
        .await
        .unwrap()
        .storage_plan("old-store")
        .unwrap();
    assert_eq!(
        write(
            &mut c,
            "lose",
            Mutation::DeclareStorageLost {
                storage: "old-store".into(),
                replacement: "new-store".into(),
                expected_revision: 1,
                expected_inventory: plan.inventory,
                unavailable_artifacts: plan.unavailable_without_agent
            }
        )
        .await,
        Reply::Recorded
    );
    w.shutdown().await.unwrap();
    old.shutdown().await.unwrap();
    new.shutdown().await.unwrap();
    let (_, manifest) = super::recovery::restore(&mut c, 1, "storage-lifecycle-recovered").await;
    fleet::control::storage::accept_recovery(&new_home, &manifest).unwrap();
    let config =
        serde_json::from_slice(&std::fs::read(new_home.join("storage.json")).unwrap()).unwrap();
    let mut new = fleet::control::storage::Agent::start(&new_home, config)
        .await
        .unwrap();
    new.tick().await.unwrap();
    Worker::accept_recovery(&home, &manifest).unwrap();
    let config = serde_json::from_slice(&std::fs::read(home.join("worker.json")).unwrap()).unwrap();
    let mut w = Worker::start(&home, config, hello()).await.unwrap();
    super::recovery::reconciled(&mut c, &mut w).await;
    super::recovery::resume(&mut c, &[]).await;
    w.tick().await.unwrap();
    new.tick().await.unwrap();
    let state = c.client.read(WAIT).await.unwrap();
    let mut check = state.clone();
    rejected(check.retire_storage(
        "new-store",
        0,
        &state.storage_plan("new-store").unwrap().inventory,
    ));
    let review = Mutation::ReviewStorageRecovery {
        expected_catalog: state.artifact_catalog_fingerprint(),
        unavailable_locations: BTreeSet::from(["storage/old-store".into()]),
    };
    assert_eq!(write(&mut c, "review", review).await, Reply::Recorded);
    let state = c.client.read(WAIT).await.unwrap();
    let lost = &state.storage.artifacts[&a.manifest.id].copies["storage/old-store"];
    assert_eq!(lost.phase, CopyPhase::Lost);
    assert_eq!(lost.authority, state.cluster);
    assert!(!state.storage_enrolled("old-store"));
    assert_eq!(state.storage_lifecycle("old-store").phase, Phase::Lost);
    assert!(old_home.join("objects").join(&a.manifest.id).exists());
    // An old service config cannot cross the recovery authority, much less regain a revoked role.
    let mut old = fleet::control::storage::Agent::start(&old_home, old_config)
        .await
        .unwrap();
    assert!(old.tick().await.is_err());
    old.shutdown().await.unwrap();
    new.shutdown().await.unwrap();
    w.shutdown().await.unwrap();
    c.close().await;
}
