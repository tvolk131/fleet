use super::*;
use fleet::control::store::{
    indexed::Collection,
    query::{Cursor, Page, Query},
};
use serde_json::json;

async fn activate(store: &mut Store) {
    let peer = Peer {
        identity: SecretKey::generate().public(),
        address: "127.0.0.1:19000".parse().unwrap(),
    };
    store
        .apply([Entry {
            log_id: entry(1, submit("unused", "unused")).log_id,
            payload: EntryPayload::Membership(openraft::Membership::new(
                vec![BTreeSet::from([1])],
                BTreeMap::from([(1, peer.node())]),
            )),
        }])
        .await
        .unwrap();
    assert!(matches!(
        store
            .apply([entry(2, submit("before", "before-job"))])
            .await
            .unwrap()[0],
        Reply::Accepted { .. }
    ));
    store
        .apply([entry(
            3,
            Request {
                origin: "cluster".into(),
                id: "prepare".into(),
                mutation: Mutation::PrepareVersion {
                    expected_revision: 0,
                    target: 5,
                },
            },
        )])
        .await
        .unwrap();
    store.prepare_version(1, 5).await.unwrap();
    let mut ready = entry(
        4,
        Request {
            origin: "cluster".into(),
            id: "ready".into(),
            mutation: Mutation::ReplicaPrepared {
                revision: 1,
                node: 1,
            },
        },
    );
    if let EntryPayload::Normal(c) = &mut ready.payload {
        c.principal = peer.identity.to_string();
    }
    store.apply([ready]).await.unwrap();
    assert_eq!(
        store
            .apply([entry(
                5,
                Request {
                    origin: "cluster".into(),
                    id: "activate".into(),
                    mutation: Mutation::ActivateVersion {
                        expected_revision: 1
                    }
                }
            )])
            .await
            .unwrap(),
        vec![Reply::Recorded]
    );
    assert_eq!(
        store.query(Query::Stats).await.unwrap()["physical_format"],
        14
    );
}

fn release(index: u64, id: &str, target: &str) -> Entry {
    entry(
        index,
        Request {
            origin: "cluster".into(),
            id: id.into(),
            mutation: Mutation::Release {
                request_id: target.into(),
            },
        },
    )
}

#[tokio::test]
async fn activation_receipts_atomic_batches_point_queries_and_streaming_restart() {
    let home = Home::new();
    let path = home.0.join("control.redb");
    let mut store = Store::open(&path, "cluster", 1).unwrap();
    activate(&mut store).await;
    let before = store.state().await.unwrap();
    assert_eq!(before.receipts.len(), 4);
    assert!(store.view().await.unwrap().receipts.is_empty());
    assert_eq!(
        store
            .query(Query::Get {
                collection: Collection::Jobs,
                key: "before-job".into()
            })
            .await
            .unwrap()["id"],
        "before-job"
    );
    let original = store
        .query(Query::Receipt {
            principal: "client".into(),
            request_id: "before".into(),
        })
        .await
        .unwrap();
    assert!(original["response"].get("Accepted").is_some());
    store.fail_next_state_commit();
    assert!(
        store
            .apply([
                entry(6, submit("new", "new-job")),
                release(7, "release", "before")
            ])
            .await
            .is_err()
    );
    assert_eq!(store.state().await.unwrap(), before);
    let replies = store
        .apply([
            entry(6, submit("new", "new-job")),
            entry(7, submit("new", "new-job")),
            release(8, "release", "before"),
            entry(9, submit("before", "before-job")),
        ])
        .await
        .unwrap();
    assert_eq!(replies[0], replies[1]);
    assert_eq!(replies[2], Reply::Recorded);
    assert!(matches!(&replies[3], Reply::Expired { resource: Some(id) } if id == "before-job"));
    assert!(matches!(
        store
            .apply([entry(10, submit("before", "different-job"))])
            .await
            .unwrap()[0],
        Reply::Rejected { .. }
    ));
    let expected = store.state().await.unwrap();
    assert_eq!(store.export(4 << 20).await.unwrap(), expected);
    assert!(store.export(1).await.is_err());
    assert_eq!(
        store
            .query(Query::Job {
                id: "before-job".into()
            })
            .await
            .unwrap(),
        fleet::control::client::job_view(&expected, &expected.jobs["before-job"])
    );
    let mut cursor: Option<Cursor> = None;
    let mut keys = BTreeSet::new();
    loop {
        let page: Page = serde_json::from_value(
            store
                .query(Query::Page {
                    collection: None,
                    cursor: cursor.clone(),
                    limit: 2,
                })
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(page.applied, expected.last_applied);
        for (key, _) in page.items {
            assert!(keys.insert(key));
        }
        cursor = page.next;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(keys, expected.receipts.keys().cloned().collect());
    assert!(
        store
            .query(Query::Page {
                collection: None,
                cursor: Some(Cursor {
                    cluster: "other".into(),
                    collection: None,
                    after: String::new()
                }),
                limit: 2
            })
            .await
            .is_err()
    );
    let snapshot = store.build_snapshot().await.unwrap();
    let mut target = Store::open(&home.0.join("target.redb"), "cluster", 2).unwrap();
    let empty = target.state().await.unwrap();
    target.fail_next_state_commit();
    assert!(
        target
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .is_err()
    );
    assert_eq!(target.state().await.unwrap(), empty);
    let snapshot = store.get_current_snapshot().await.unwrap().unwrap();
    target
        .install_snapshot(&snapshot.meta, snapshot.snapshot)
        .await
        .unwrap();
    assert_eq!(target.state().await.unwrap(), expected);
    drop(store);
    let mut store = Store::open(&path, "cluster", 1).unwrap();
    assert_eq!(store.state().await.unwrap(), expected);
    assert!(matches!(
        store
            .apply([entry(11, submit("before", "before-job"))])
            .await
            .unwrap()[0],
        Reply::Expired { .. }
    ));
    let snapshot = store.get_current_snapshot().await.unwrap().unwrap();
    target
        .install_snapshot(&snapshot.meta, snapshot.snapshot)
        .await
        .unwrap();
    assert_eq!(target.state().await.unwrap(), expected);
}

#[tokio::test]
async fn indexed_recovery_copies_permanent_receipts_and_refuses_old_origin() {
    let home = Home::new();
    let path = home.0.join("control.redb");
    let mut store = Store::open(&path, "cluster", 1).unwrap();
    activate(&mut store).await;
    store
        .apply([release(6, "release", "before")])
        .await
        .unwrap();
    let expected = store.state().await.unwrap();
    drop(store);
    let recovered =
        Store::recovery_source(&path, "cluster", 1, &home.0.join("backup.redb")).unwrap();
    assert_eq!(recovered, expected);
    // The pure recovery image keeps all tombstones; a missing old-origin ID is never executed.
    let mut recovered = recovered;
    recovered.cluster = "replacement".into();
    assert!(matches!(
        recovered.apply(
            Command {
                principal: "client".into(),
                request: submit("before", "before-job")
            },
            7
        ),
        Reply::Expired { .. }
    ));
    assert!(matches!(
        recovered.apply(
            Command {
                principal: "client".into(),
                request: submit("unseen", "unseen")
            },
            8
        ),
        Reply::RecoveryUnknown { .. }
    ));
}

#[tokio::test]
async fn malformed_indexed_snapshots_abort_without_replacing_receipts_or_resources() {
    use tokio::io::AsyncReadExt;
    let home = Home::new();
    let mut source = Store::open(&home.0.join("source.redb"), "cluster", 1).unwrap();
    activate(&mut source).await;
    let mut snapshot = source.build_snapshot().await.unwrap();
    let mut bytes = Vec::new();
    snapshot.snapshot.read_to_end(&mut bytes).await.unwrap();
    let original = String::from_utf8(bytes).unwrap();
    let mut duplicate = original.clone();
    duplicate.push_str(original.lines().nth(2).unwrap());
    duplicate.push('\n');
    let unknown = format!("{original}[255,\"invalid\",{{}}]\n");
    let truncated = original.trim_end_matches('\n').to_string();
    let mut malformed: Vec<_> = original.lines().map(str::to_string).collect();
    malformed[2] = "[0,\"invalid receipt key\",{}]".into();
    let malformed = format!("{}\n", malformed.join("\n"));
    let mut target = Store::open(&home.0.join("target.redb"), "cluster", 2).unwrap();
    let before = target.state().await.unwrap();
    for bad in [duplicate, unknown, truncated, malformed] {
        let mut meta = snapshot.meta.clone();
        meta.snapshot_id = blake3::hash(bad.as_bytes()).to_hex().to_string();
        assert!(
            target
                .install_snapshot(&meta, versions::snapshot_file(bad.into_bytes()))
                .await
                .is_err()
        );
        assert_eq!(target.state().await.unwrap(), before);
    }
}

async fn scale(count: u64) {
    use std::time::Instant;
    let home = Home::new();
    let path = home.0.join("control.redb");
    let mut store = Store::open(&path, "cluster", 1).unwrap();
    activate(&mut store).await;
    let start = Instant::now();
    for base in (0..count).step_by(1000) {
        let entries = (base..(base + 1000).min(count))
            .map(|n| release(n + 6, &format!("history-{n:010}"), "never-existed"));
        let replies = store.apply(entries).await.unwrap();
        assert!(replies.iter().all(|r| matches!(r, Reply::Rejected { .. })));
    }
    let insert_ms = start.elapsed().as_millis();
    assert_eq!(
        store.query(Query::Stats).await.unwrap()["receipts"],
        count + 4
    );
    let mut timings = Vec::new();
    for n in [0, count / 2, count - 1] {
        let start = Instant::now();
        let value = store
            .query(Query::Receipt {
                principal: "client".into(),
                request_id: format!("history-{n:010}"),
            })
            .await
            .unwrap();
        assert!(value["response"].get("Rejected").is_some());
        timings.push(start.elapsed().as_micros());
    }
    let start = Instant::now();
    let snapshot = store.build_snapshot().await.unwrap();
    let snapshot_ms = start.elapsed().as_millis();
    let snapshot_bytes = snapshot.snapshot.metadata().await.unwrap().len();
    if count >= 1_000_000 {
        assert!(snapshot_bytes > 64 << 20);
    }
    let mut receiver = Store::open(&home.0.join("receiver.redb"), "cluster", 2).unwrap();
    let start = Instant::now();
    receiver
        .install_snapshot(&snapshot.meta, snapshot.snapshot)
        .await
        .unwrap();
    let install_ms = start.elapsed().as_millis();
    assert_eq!(
        store.query(Query::Stats).await.unwrap(),
        receiver.query(Query::Stats).await.unwrap()
    );
    drop(store);
    let start = Instant::now();
    let mut store = Store::open(&path, "cluster", 1).unwrap();
    let reopen_ms = start.elapsed().as_millis();
    assert!(matches!(
        store
            .apply([release(count + 6, "history-0000000000", "never-existed")])
            .await
            .unwrap()[0],
        Reply::Rejected { .. }
    ));
    let page: Page = serde_json::from_value(
        store
            .query(Query::Page {
                collection: None,
                cursor: None,
                limit: 256,
            })
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(page.items.len(), 256);
    assert!(page.next.is_some());
    eprintln!(
        "indexed-scale: {}",
        json!({"receipts":count,"database_bytes":std::fs::metadata(&path).unwrap().len(),"insert_ms":insert_ms,"lookup_us":timings,"snapshot_bytes":snapshot_bytes,"snapshot_ms":snapshot_ms,"install_ms":install_ms,"reopen_ms":reopen_ms})
    );
}

#[tokio::test]
async fn indexed_history_scale_smoke() {
    scale(10_000).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn indexed_queries_and_receipts_survive_network_snapshot_and_leader_loss() {
    let mut c = Cluster::new(3).await;
    assert!(
        c.client
            .query(Query::Stats, WAIT)
            .await
            .unwrap_err()
            .to_string()
            .contains("generation 5")
    );
    let request = submit("once", "job");
    let accepted = c.client.write(request.clone(), WAIT).await.unwrap();
    c.client
        .write(
            Request {
                origin: "cluster".into(),
                id: "prepare-indexed".into(),
                mutation: Mutation::PrepareVersion {
                    expected_revision: 0,
                    target: 5,
                },
            },
            WAIT,
        )
        .await
        .unwrap();
    tokio::time::timeout(WAIT, async {
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
                == 3
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    c.client
        .write(
            Request {
                origin: "cluster".into(),
                id: "activate-indexed".into(),
                mutation: Mutation::ActivateVersion {
                    expected_revision: 1,
                },
            },
            WAIT,
        )
        .await
        .unwrap();
    assert_eq!(
        c.client
            .query(Query::Job { id: "job".into() }, WAIT)
            .await
            .unwrap()["id"],
        "job"
    );
    let leader = c.leader().await;
    let follower = (1..=3).find(|id| *id != leader).unwrap();
    let successor = (1..=3).find(|id| *id != leader && *id != follower).unwrap();
    let oversized = Request {
        origin: "cluster".into(),
        id: "oversized-command".into(),
        mutation: Mutation::Release {
            request_id: "x".repeat(7 << 20),
        },
    };
    assert!(
        c.client
            .write(oversized, WAIT)
            .await
            .unwrap_err()
            .to_string()
            .contains("command exceeds")
    );
    assert_eq!(
        c.client
            .query(
                Query::Receipt {
                    principal: c.client.endpoint.id().to_string(),
                    request_id: "oversized-command".into(),
                },
                WAIT
            )
            .await
            .unwrap(),
        serde_json::Value::Null
    );
    c.replicas
        .remove(&follower)
        .unwrap()
        .shutdown()
        .await
        .unwrap();
    // Exceed the former 64 MiB ceiling through the actual Raft transport, not just Store::install.
    let absent = "x".repeat(650 << 10);
    for n in 0..110 {
        c.client
            .write(
                Request {
                    origin: "cluster".into(),
                    id: format!("history-{n}"),
                    mutation: Mutation::Release {
                        request_id: absent.clone(),
                    },
                },
                WAIT,
            )
            .await
            .unwrap();
    }
    let barrier = c.client.query(Query::Stats, WAIT).await.unwrap()["applied"]["index"]
        .as_u64()
        .unwrap();
    c.replicas[&leader].raft.trigger().snapshot().await.unwrap();
    tokio::time::timeout(WAIT * 12, async {
        loop {
            if c.replicas[&leader]
                .raft
                .metrics()
                .borrow()
                .purged
                .is_some_and(|l| l.index >= barrier)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    c.start(follower).await;
    tokio::time::timeout(WAIT * 12, async {
        loop {
            if c.replicas[&follower]
                .store
                .query(Query::Stats)
                .await
                .unwrap()["receipts"]
                .as_u64()
                .is_some_and(|n| n >= 116)
                && c.replicas[&follower]
                    .raft
                    .metrics()
                    .borrow()
                    .snapshot
                    .is_some_and(|log| log.index >= barrier)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert!(
        c.replicas[&follower]
            .raft
            .metrics()
            .borrow()
            .snapshot
            .is_some()
    );
    let snapshot = c
        .replicas
        .get_mut(&follower)
        .unwrap()
        .store
        .get_current_snapshot()
        .await
        .unwrap()
        .unwrap();
    assert!(snapshot.snapshot.metadata().await.unwrap().len() > 64 << 20);
    c.replicas
        .remove(&leader)
        .unwrap()
        .shutdown()
        .await
        .unwrap();
    // Prefer the survivor with the older snapshot. Its initial catch-up AppendEntries can exceed
    // the frame budget even though every individual command fits; it must split and make progress.
    c.replicas[&successor].raft.trigger().elect().await.unwrap();
    // Reconciliation may walk the 70+ MiB retained log before the new leader can commit. A short
    // client deadline is allowed to expire with an unknown outcome; it is not a failover SLO.
    match c
        .client
        .write(request.clone(), Duration::from_millis(750))
        .await
    {
        Ok(reply) => assert_eq!(reply, accepted),
        Err(error) => assert!(error.to_string().contains("outcome is unknown")),
    }
    let retried = c.client.write(request.clone(), WAIT * 3).await;
    if retried.is_err() {
        for (id, replica) in &c.replicas {
            eprintln!("replica {id}: {:?}", *replica.raft.metrics().borrow());
        }
    }
    assert_eq!(retried.unwrap(), accepted);
    let principal = c.client.endpoint.id().to_string();
    assert_eq!(
        c.client
            .query(
                Query::Receipt {
                    principal,
                    request_id: request.id
                },
                WAIT
            )
            .await
            .unwrap()["response"],
        json!(accepted)
    );
    assert!(c.client.view(WAIT).await.unwrap().receipts.is_empty());
    let page = c
        .client
        .query(
            Query::Page {
                collection: None,
                cursor: None,
                limit: 20,
            },
            WAIT,
        )
        .await
        .unwrap();
    assert!((1..20).contains(&page["items"].as_array().unwrap().len()));
    assert!(!page["next"].is_null());
    c.client.query(Query::Stats, WAIT).await.unwrap();
    let leader = c
        .replicas
        .values()
        .find_map(|r| {
            let m = r.raft.metrics().borrow().clone();
            (m.current_leader == Some(m.id)).then_some(m.id)
        })
        .unwrap();
    let other = *c.replicas.keys().find(|id| **id != leader).unwrap();
    c.replicas.remove(&other).unwrap().shutdown().await.unwrap();
    assert!(
        c.client
            .query(Query::Stats, Duration::from_millis(750))
            .await
            .is_err()
    );
    c.close().await;
}

#[tokio::test]
#[ignore = "explicit million-receipt disk/latency qualification"]
async fn indexed_million_receipts() {
    scale(1_000_000).await;
}
