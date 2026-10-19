#![cfg(feature = "control-plane")]

use fleet::control::{
    Client, Command, Config, Entry, Mutation, Replica, Reply, Request, State, Store, TypeConfig,
    model::key,
    network::{ALPN, Answer, Message, Peer, exchange},
    workload::{Assignment, AttemptPhase, Desired, JobDesired, Observation, Report, WorkPhase},
};
use iroh::SecretKey;
use openraft::{
    EntryPayload, LogId, RaftSnapshotBuilder, Vote,
    storage::{RaftLogStorage, RaftStateMachine},
    testing::{StoreBuilder, Suite},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    net::UdpSocket,
    path::PathBuf,
    time::Duration,
};

#[path = "control/simulated.rs"]
mod simulated;
use simulated::SimulatedWorker;

const WAIT: Duration = Duration::from_secs(10);

fn reserve_socket() -> UdpSocket {
    use std::sync::atomic::{AtomicU32, Ordering};
    static NEXT: AtomicU32 = AtomicU32::new(10000);
    for _ in 0..20000 {
        let port = 10000 + (NEXT.fetch_add(1, Ordering::SeqCst) % 20000);
        if let Ok(socket) = UdpSocket::bind((std::net::Ipv4Addr::LOCALHOST, port as u16)) {
            return socket;
        }
    }
    panic!("no free isolated test port");
}

struct Home(PathBuf);
impl Home {
    fn new() -> Self {
        let p = std::env::temp_dir().join(format!(
            "fleet-control-test-{}-{:032x}",
            std::process::id(),
            rand::random::<u128>()
        ));
        std::fs::create_dir_all(&p).unwrap();
        Self(p)
    }
}
impl Drop for Home {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct Builder;
impl StoreBuilder<TypeConfig, Store, Store, Home> for Builder {
    async fn build(&self) -> Result<(Home, Store, Store), openraft::StorageError<u64>> {
        let home = Home::new();
        let store = Store::open(&home.0.join("test.redb"), "storage-suite", 0).unwrap();
        Ok((home, store.clone(), store))
    }
}

#[test]
fn openraft_storage_contract() {
    Suite::<TypeConfig, Store, Store, Builder, Home>::test_all(Builder).unwrap();
}

fn submit(id: &str, job: &str) -> Request {
    Request {
        origin: "cluster".into(),
        id: id.into(),
        mutation: Mutation::Enqueue(fleet::control::queue::Submission {
            inputs: Default::default(),
            id: job.into(),
            workload: real_worker::workload("echo test"),
            worker: None,
            priority: 0,
            requires: BTreeSet::new(),
            after: BTreeSet::new(),
        }),
    }
}

fn entry(index: u64, request: Request) -> Entry {
    Entry {
        log_id: LogId::new(openraft::CommittedLeaderId::new(1, 1), index),
        payload: EntryPayload::Normal(Command {
            principal: "client".into(),
            request,
        }),
    }
}

#[tokio::test]
async fn receipts_survive_reopen_pruning_and_snapshot_installation() {
    let home = Home::new();
    let path = home.0.join("one.redb");
    let mut store = Store::open(&path, "cluster", 1).unwrap();
    let request = submit("submit-1", "job-1");
    let first = store
        .apply([entry(1, request.clone())])
        .await
        .unwrap()
        .remove(0);
    drop(store);
    let mut store = Store::open(&path, "cluster", 1).unwrap();
    assert_eq!(
        store.apply([entry(2, request.clone())]).await.unwrap(),
        vec![first]
    );
    assert_eq!(store.state().await.unwrap().jobs.len(), 1);
    let release = Request {
        origin: "cluster".into(),
        id: "release".into(),
        mutation: Mutation::Release {
            request_id: request.id.clone(),
        },
    };
    store.apply([entry(3, release)]).await.unwrap();
    let snapshot = store.build_snapshot().await.unwrap();
    let mut other = Store::open(&home.0.join("two.redb"), "cluster", 2).unwrap();
    let vote = Vote::new(9, 2);
    other.save_vote(&vote).await.unwrap();
    other
        .install_snapshot(&snapshot.meta, snapshot.snapshot)
        .await
        .unwrap();
    assert_eq!(other.read_vote().await.unwrap(), Some(vote));
    assert!(matches!(
        other.apply([entry(4, request)]).await.unwrap()[0],
        Reply::Expired { .. }
    ));
    assert_eq!(other.state().await.unwrap().jobs.len(), 1);
    let changed = submit("submit-1", "different-job");
    assert!(matches!(
        other.apply([entry(5, changed)]).await.unwrap()[0],
        Reply::Rejected { .. }
    ));
    let state = other.state().await.unwrap();
    assert!(state.receipts[&key("client", "submit-1")].command.is_none());
    assert!(
        state.receipts[&key("client", "submit-1")]
            .response
            .is_none()
    );
    assert!(other.get_current_snapshot().await.unwrap().is_some());
    drop(other);
    let mut other = Store::open(&home.0.join("two.redb"), "cluster", 2).unwrap();
    assert_eq!(other.state().await.unwrap(), state);
    assert!(other.get_current_snapshot().await.unwrap().is_some());
}

#[tokio::test]
async fn invalid_snapshots_leave_state_and_vote_unchanged() {
    let home = Home::new();
    let mut source = Store::open(&home.0.join("source.redb"), "one", 1).unwrap();
    let mut request = submit("s", "j");
    request.origin = "one".into();
    source.apply([entry(1, request)]).await.unwrap();
    let snapshot = source.build_snapshot().await.unwrap();
    let mut target = Store::open(&home.0.join("target.redb"), "two", 2).unwrap();
    let before = target.state().await.unwrap();
    assert!(
        target
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .is_err()
    );
    assert_eq!(target.state().await.unwrap(), before);
    let mut snapshot = source.build_snapshot().await.unwrap();
    {
        use tokio::io::{AsyncSeekExt, AsyncWriteExt};
        snapshot.snapshot.rewind().await.unwrap();
        snapshot.snapshot.write_all(b"!").await.unwrap();
    }
    assert!(
        source
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .is_err()
    );
    assert_eq!(source.state().await.unwrap().jobs.len(), 1);
    drop(target);
    assert!(Store::open(&home.0.join("target.redb"), "wrong-cluster", 2).is_err());
}

#[tokio::test]
async fn failed_apply_and_snapshot_installation_leave_no_partial_state() {
    let home = Home::new();
    let path = home.0.join("local.redb");
    let mut store = Store::open(&path, "cluster", 1).unwrap();
    let initial = store.state().await.unwrap();
    store.fail_next_state_commit();
    assert!(store.apply([entry(1, submit("s", "j"))]).await.is_err());
    assert_eq!(store.state().await.unwrap(), initial);
    drop(store);
    let mut store = Store::open(&path, "cluster", 1).unwrap();
    assert_eq!(store.state().await.unwrap(), initial);
    store.apply([entry(1, submit("s", "j"))]).await.unwrap();
    let snapshot = store.build_snapshot().await.unwrap();
    let mut target = Store::open(&home.0.join("target.redb"), "cluster", 2).unwrap();
    target.fail_next_state_commit();
    assert!(
        target
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .is_err()
    );
    assert_eq!(target.state().await.unwrap(), initial);
    assert!(target.get_current_snapshot().await.unwrap().is_none());
    drop(target);
    let target = Store::open(&home.0.join("target.redb"), "cluster", 2).unwrap();
    assert_eq!(target.state().await.unwrap(), initial);
}

#[test]
fn simulation_tombstones_prevent_delayed_launch_and_survive_restart() {
    let home = Home::new();
    let path = home.0.join("worker.redb");
    let worker = SimulatedWorker::open(&path, "cluster", "worker").unwrap();
    let running = Assignment {
        id: "j".into(),
        execution: "e".into(),
        attempt: Some(fleet::control::queue::Attempt {
            job: "j".into(),
            number: 1,
            checkpoint: None,
        }),
        worker: "worker".into(),
        workload: real_worker::workload("echo test"),
        revision: 1,
        desired: Desired::Job(JobDesired::Running),
        suspend_checkpoint: None,
        observed: None,
    };
    let stopped = Assignment {
        revision: 2,
        desired: Desired::Job(JobDesired::Cancelled),
        ..running.clone()
    };
    assert_eq!(worker.ensure(&stopped).unwrap().starts, 0);
    drop(worker);
    let worker = SimulatedWorker::open(&path, "cluster", "worker").unwrap();
    let late = worker.ensure(&running).unwrap();
    assert_eq!(late.starts, 0);
    assert_eq!(late.desired.desired, Desired::Job(JobDesired::Cancelled));
    let another = Assignment {
        id: "j2".into(),
        execution: "e2".into(),
        ..running
    };
    assert_eq!(worker.ensure(&another).unwrap().starts, 1);
    assert_eq!(worker.ensure(&another).unwrap().starts, 1);
    assert_eq!(worker.events().unwrap().len(), 2);
}

#[test]
fn observation_cursor_and_terminal_state_cannot_go_backwards() {
    let mut state = State::new("cluster".into());
    state.apply(
        Command {
            principal: "admin".into(),
            request: submit("s", "j"),
        },
        1,
    );
    let registration = simulation_registration(SecretKey::generate().public());
    let principal = registration.peer.identity.to_string();
    assert_eq!(state.enroll(&registration), Reply::Recorded);
    assert!(matches!(
        state.place_queued("j", "worker", 1),
        Reply::Accepted { .. }
    ));
    let event = |id: &str, sequence, phase| Command {
        principal: principal.clone(),
        request: Request {
            origin: "cluster".into(),
            id: id.into(),
            mutation: Mutation::WorkerReport(Report {
                worker: "worker".into(),
                incarnation: "simulated-journal".into(),
                sequence,
                assignment: "j-a1".into(),
                revision: 1,
                observation: Observation {
                    phase,
                    detail: None,
                },
            }),
        },
    };
    assert!(matches!(
        state.apply(event("gap", 2, WorkPhase::Job(AttemptPhase::Running)), 2),
        Reply::Rejected { .. }
    ));
    assert!(state.cursors.is_empty());
    assert_eq!(
        state.apply(event("done", 1, WorkPhase::Job(AttemptPhase::Succeeded)), 3),
        Reply::Recorded
    );
    state.apply(event("stale", 2, WorkPhase::Job(AttemptPhase::Running)), 4);
    assert_eq!(
        state.assignments["j-a1"]
            .observed
            .as_ref()
            .map(|(r, o)| (*r, o.phase)),
        Some((1, WorkPhase::Job(AttemptPhase::Succeeded)))
    );
    assert_eq!(state.cursors[&key("worker", "simulated-journal")], 2);
}

struct Cluster {
    home: Home,
    config: Config,
    replicas: BTreeMap<u64, Replica>,
    client: Client,
}

impl Cluster {
    async fn prepare(&mut self, id: u64, start: bool) -> Peer {
        let key = fleet::keys::load_or_create(&self.home.0.join(id.to_string()).join("node.key"))
            .unwrap();
        let socket = reserve_socket();
        let peer = Peer {
            identity: key.public(),
            address: socket.local_addr().unwrap(),
        };
        drop(socket);
        self.config.peers.insert(id, peer.clone());
        if start {
            self.start(id).await;
        }
        peer
    }
    async fn start(&mut self, id: u64) {
        let replica = Replica::start(
            &self.home.0.join(id.to_string()),
            Config {
                id,
                bootstrap_allowed: false,
                ..self.config.clone()
            },
        )
        .await
        .unwrap();
        self.replicas.insert(id, replica);
    }
    fn membership_request(&self, request: &str, revision: u64, ids: &[u64]) -> Request {
        Request {
            origin: self.config.cluster.clone(),
            id: request.into(),
            mutation: Mutation::ChangeMembership {
                expected_revision: revision,
                voters: ids
                    .iter()
                    .map(|id| (*id, self.config.peers[id].clone()))
                    .collect(),
            },
        }
    }
    async fn changed(&mut self, request: Request) -> State {
        // Joint-consensus recovery can require multiple elections and cold handshakes.
        // Allow a bounded convergence window with the LAN election timings.
        let Reply::MembershipAccepted {
            operation,
            revision,
        } = self.client.write(request, WAIT * 3).await.unwrap()
        else {
            panic!("not accepted");
        };
        let end = tokio::time::Instant::now() + WAIT * 3;
        loop {
            let state = self.client.read(WAIT).await.unwrap();
            if state.membership_operations[&operation].stage
                == fleet::control::membership::MembershipStage::Complete
            {
                assert_eq!(state.configuration.revision, revision);
                return state;
            }
            assert!(
                tokio::time::Instant::now() < end,
                "operation did not complete: {state:?}"
            );
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    }
    async fn new(n: u64) -> Self {
        let home = Home::new();
        let client_key = fleet::keys::load_or_create(&home.0.join("app-client/node.key")).unwrap();
        let mut peers = BTreeMap::new();
        // Keep reservations alive until all addresses have been chosen.
        let sockets: Vec<_> = (0..n).map(|_| reserve_socket()).collect();
        for id in 1..=n {
            let key =
                fleet::keys::load_or_create(&home.0.join(id.to_string()).join("node.key")).unwrap();
            peers.insert(
                id,
                Peer {
                    identity: key.public(),
                    address: sockets[id as usize - 1].local_addr().unwrap(),
                },
            );
        }
        drop(sockets);
        let config = Config {
            cluster: "cluster".into(),
            id: 1,
            peers: peers.clone(),
            administrators: BTreeSet::from([client_key.public()]),
            bootstrap_allowed: true,
        };
        fleet::instances::write(&home.0.join("app-client/config.json"), &config).unwrap();
        let mut replicas = BTreeMap::new();
        for id in 1..=n {
            replicas.insert(
                id,
                Replica::start(
                    &home.0.join(id.to_string()),
                    Config {
                        id,
                        ..config.clone()
                    },
                )
                .await
                .unwrap(),
            );
        }
        replicas[&1].bootstrap(&peers).await.unwrap();
        let client = Client::new(client_key, config.cluster.clone(), peers)
            .await
            .unwrap();
        let mut this = Self {
            home,
            config,
            replicas,
            client,
        };
        this.client.read(WAIT).await.unwrap();
        this
    }
    async fn leader(&mut self) -> u64 {
        self.client.read(WAIT).await.unwrap();
        self.replicas
            .values()
            .find_map(|r| {
                let m = r.raft.metrics().borrow().clone();
                (m.current_leader == Some(m.id)).then_some(m.id)
            })
            .expect("leader")
    }
    fn partition(&self, a: u64, b: u64, blocked: bool) {
        for (from, to) in [(a, b), (b, a)] {
            let mut set = self.replicas[&from].blocked.write().unwrap();
            if blocked {
                set.insert(self.config.peers[&to].identity);
            } else {
                set.remove(&self.config.peers[&to].identity);
            }
        }
    }
    async fn close(self) {
        self.client.endpoint.close().await;
        for r in self.replicas.into_values() {
            r.shutdown().await.unwrap();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn membership_grows_replaces_leader_and_shrinks_preserving_history() {
    let mut c = Cluster::new(1).await;
    let submission = submit("retained", "training");
    let accepted = c.client.write(submission.clone(), WAIT).await.unwrap();
    c.prepare(2, true).await;
    c.prepare(3, true).await;
    let grow = c.membership_request("grow", 1, &[1, 2, 3]);
    let state = c.changed(grow.clone()).await;
    assert_eq!(
        state.membership.voter_ids().collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    let replay = c.client.write(grow, WAIT).await.unwrap();
    assert!(matches!(
        replay,
        Reply::MembershipAccepted { revision: 2, .. }
    ));
    let old_leader = c.leader().await;
    c.prepare(4, true).await;
    let target: Vec<_> = (1..=4).filter(|id| *id != old_leader).collect();
    let replace = c.membership_request("replace-leader", 2, &target);
    c.changed(replace).await;
    // No manual discovery update; the client learned the new peer from authenticated replies.
    assert!(c.client.peers.contains_key(&4));
    c.replicas
        .remove(&old_leader)
        .unwrap()
        .shutdown()
        .await
        .unwrap();
    let retiring = *target.iter().find(|id| **id != 4).unwrap();
    let old_connection = c.replicas[&retiring]
        .router
        .endpoint()
        .connect(c.config.peers[&4].addr(), ALPN)
        .await
        .unwrap();
    let forbidden_admin = c.membership_request("replica-is-not-admin", 3, &[4]);
    assert!(matches!(
        exchange(
            &old_connection,
            &c.config.cluster,
            Message::Write(Box::new(forbidden_admin))
        )
        .await
        .unwrap(),
        Answer::Refused(_)
    ));
    let shrink = c.membership_request("shrink", 3, &[4]);
    let state = c.changed(shrink).await;
    assert_eq!(state.membership.voter_ids().collect::<Vec<_>>(), vec![4]);
    // The authorization contract changes when THIS replica applies the final configuration.
    // The operation may have been reported complete by a different replica a few milliseconds earlier.
    tokio::time::timeout(WAIT, async {
        while c.replicas[&4].store.state().await.unwrap().membership != state.membership {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    // Let the sole remaining voter establish its leadership before comparing durable votes.
    c.client.read(WAIT).await.unwrap();
    // Recheck authorization on existing connections, not only at handshake time.
    let vote_before = c
        .replicas
        .get_mut(&4)
        .unwrap()
        .store
        .read_vote()
        .await
        .unwrap();
    assert!(matches!(
        exchange(
            &old_connection,
            &c.config.cluster,
            Message::Vote(openraft::raft::VoteRequest {
                vote: Vote::new(10000, retiring),
                last_log_id: None,
            })
        )
        .await
        .unwrap(),
        Answer::Refused(_)
    ));
    assert_eq!(
        c.replicas
            .get_mut(&4)
            .unwrap()
            .store
            .read_vote()
            .await
            .unwrap(),
        vote_before
    );
    for id in target.into_iter().filter(|id| *id != 4) {
        c.replicas.remove(&id).unwrap().shutdown().await.unwrap();
    }
    assert_eq!(c.client.write(submission, WAIT).await.unwrap(), accepted);
    assert_eq!(c.client.read(WAIT).await.unwrap().jobs.len(), 1);
    // Membership changes preserve the exact submitted job intent.
    assert_eq!(
        state.jobs["training"].workload,
        real_worker::workload("echo test")
    );
    c.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn membership_rejects_concurrency_reused_identities_and_stale_revisions() {
    use std::sync::atomic::Ordering;
    let mut c = Cluster::new(1).await;
    c.prepare(2, false).await;
    c.prepare(3, false).await;
    let stale = c.membership_request("stale", 0, &[1, 2, 3]);
    assert!(matches!(
        c.client.write(stale, WAIT).await.unwrap(),
        Reply::Rejected { .. }
    ));
    let two = c.membership_request("two", 1, &[1, 2]);
    assert!(matches!(
        c.client.write(two, WAIT).await.unwrap(),
        Reply::Rejected { .. }
    ));
    let mut reused = c.membership_request("reused", 1, &[1, 2, 3]);
    if let Mutation::ChangeMembership { voters, .. } = &mut reused.mutation {
        voters.get_mut(&2).unwrap().identity = voters[&1].identity;
    }
    assert!(matches!(
        c.client.write(reused, WAIT).await.unwrap(),
        Reply::Rejected { .. }
    ));
    c.replicas[&1]
        .membership_paused
        .store(true, Ordering::SeqCst);
    let grow = c.membership_request("grow", 1, &[1, 2, 3]);
    let accepted = c.client.write(grow.clone(), WAIT).await.unwrap();
    let other = c.membership_request("concurrent", 1, &[1]);
    assert!(matches!(
        c.client.write(other, WAIT).await.unwrap(),
        Reply::Rejected { .. }
    ));
    assert_eq!(c.client.write(grow.clone(), WAIT).await.unwrap(), accepted);
    c.start(2).await;
    c.start(3).await;
    assert!(
        c.replicas[&2]
            .bootstrap(&BTreeMap::from([(2, c.config.peers[&2].clone())]))
            .await
            .is_err()
    );
    c.replicas[&1]
        .membership_paused
        .store(false, Ordering::SeqCst);
    c.changed(grow).await;
    let shrink = c.membership_request("shrink", 2, &[1]);
    c.changed(shrink).await;
    let retired = c.membership_request("retired", 3, &[1, 2, 3]);
    assert!(matches!(
        c.client.write(retired, WAIT).await.unwrap(),
        Reply::Rejected { .. }
    ));
    c.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn membership_recovers_after_leader_loss_in_committed_joint_configuration() {
    use fleet::control::membership::MembershipStage;
    for shrink in [false, true] {
        let mut c = Cluster::new(3).await;
        c.prepare(4, true).await;
        let leader = c.leader().await;
        let removed = (1..=3).find(|id| *id != leader).unwrap();
        let target: Vec<_> = if shrink {
            vec![removed]
        } else {
            (1..=4).filter(|id| *id != removed).collect()
        };
        let request = c.membership_request("joint-crash", 1, &target);
        c.replicas[&leader].store.hold_joint_apply(true);
        let Reply::MembershipAccepted { operation, .. } =
            c.client.write(request.clone(), WAIT).await.unwrap()
        else {
            panic!("acceptance");
        };
        tokio::time::timeout(WAIT, async {
            loop {
                let state = c.replicas[&leader].store.state().await.unwrap();
                if state.membership_operations[&operation].stage == MembershipStage::Joint {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        // Interrupt before change_membership can propose its second (uniform) entry.
        c.replicas[&leader].membership_task.abort();
        tokio::task::yield_now().await;
        c.replicas[&leader].store.hold_joint_apply(false);
        let old = c.replicas.remove(&leader).unwrap();
        old.shutdown().await.unwrap();
        let state = c.changed(request).await;
        assert_eq!(state.configuration.revision, 2);
        assert_eq!(state.membership_operations.len(), 1);
        assert_eq!(state.membership.voter_ids().collect::<Vec<_>>(), target);
        if !shrink {
            c.start(leader).await;
            tokio::time::timeout(WAIT, async {
                loop {
                    if c.replicas[&leader]
                        .store
                        .state()
                        .await
                        .unwrap()
                        .configuration
                        .revision
                        == 2
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .unwrap();
        }
        c.close().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn membership_lost_acceptance_and_offline_learner_resume_on_a_new_leader() {
    use std::sync::atomic::Ordering;
    let mut c = Cluster::new(3).await;
    let leader = c.leader().await;
    c.prepare(4, false).await;
    let request = c.membership_request("lost-membership-reply", 1, &[2, 3, 4]);
    c.replicas[&leader]
        .drop_write_reply
        .store(true, Ordering::SeqCst);
    let connection = c
        .client
        .endpoint
        .connect(c.config.peers[&leader].addr(), ALPN)
        .await
        .unwrap();
    assert!(
        exchange(
            &connection,
            &c.config.cluster,
            Message::Write(Box::new(request.clone()))
        )
        .await
        .is_err()
    );
    let initial = c.client.read(WAIT).await.unwrap();
    let operation = initial.configuration.active.clone().unwrap();
    assert_eq!(initial.membership_operations.len(), 1);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let state = c.client.read(WAIT).await.unwrap();
    assert_eq!(
        state.membership.voter_ids().collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    c.replicas
        .remove(&leader)
        .unwrap()
        .shutdown()
        .await
        .unwrap();
    c.start(4).await;
    let final_state = c.changed(request.clone()).await;
    assert_eq!(final_state.membership_operations.len(), 1);
    assert!(
        final_state.membership_operations[&operation]
            .completed_at
            .is_some()
    );
    let release = Request {
        origin: "cluster".into(),
        id: "release-membership".into(),
        mutation: Mutation::Release {
            request_id: request.id.clone(),
        },
    };
    c.client.write(release, WAIT).await.unwrap();
    assert!(
        matches!(c.client.write(request, WAIT).await.unwrap(), Reply::Expired { resource: Some(id) } if id == operation)
    );
    c.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn minority_cannot_reduce_membership_or_force_bootstrap() {
    let mut c = Cluster::new(3).await;
    let leader = c.leader().await;
    for id in (1..=3).filter(|id| *id != leader) {
        c.replicas.remove(&id).unwrap().shutdown().await.unwrap();
    }
    let shrink = c.membership_request("unsafe-shrink", 1, &[leader]);
    assert!(
        c.client
            .write(shrink, Duration::from_secs(2))
            .await
            .is_err()
    );
    let state = c.replicas[&leader].store.state().await.unwrap();
    assert_eq!(state.configuration.revision, 1);
    assert!(state.configuration.active.is_none());
    assert!(
        c.replicas[&leader]
            .bootstrap(&BTreeMap::from([(leader, c.config.peers[&leader].clone())]))
            .await
            .is_err()
    );
    c.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_replica_and_client_authentication() {
    let mut c = Cluster::new(1).await;
    let request = submit("s", "j");
    assert!(matches!(
        c.client.write(request.clone(), WAIT).await.unwrap(),
        Reply::Accepted { .. }
    ));
    let endpoint = fleet::control::network::connect_endpoint(
        SecretKey::generate(),
        "127.0.0.1:0".parse().unwrap(),
    )
    .await
    .unwrap();
    let conn = endpoint
        .connect(c.config.peers[&1].addr(), ALPN)
        .await
        .unwrap();
    assert!(
        exchange(&conn, &c.config.cluster, Message::Read)
            .await
            .is_err()
    );
    endpoint.close().await;
    let conn = c
        .client
        .endpoint
        .connect(c.config.peers[&1].addr(), ALPN)
        .await
        .unwrap();
    assert!(matches!(
        exchange(&conn, "different-cluster", Message::Read)
            .await
            .unwrap(),
        Answer::Refused(_)
    ));
    let wrong_role = Message::Vote(openraft::raft::VoteRequest {
        vote: Vote::new(100, 1),
        last_log_id: None,
    });
    assert!(matches!(
        exchange(&conn, &c.config.cluster, wrong_role)
            .await
            .unwrap(),
        Answer::Refused(_)
    ));
    c.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_failover_lost_reply_and_worker_reconciliation() {
    let mut c = Cluster::new(3).await;
    let leader = c.leader().await;
    let request = submit("stable-request", "j");
    // The server commits, then drops the stream before sending any response.
    c.replicas[&leader]
        .drop_write_reply
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let conn = c
        .client
        .endpoint
        .connect(c.config.peers[&leader].addr(), ALPN)
        .await
        .unwrap();
    assert!(
        exchange(
            &conn,
            &c.config.cluster,
            Message::Write(Box::new(request.clone()))
        )
        .await
        .is_err()
    );
    assert_eq!(
        c.replicas[&leader].store.state().await.unwrap().jobs.len(),
        1
    );
    c.replicas
        .remove(&leader)
        .unwrap()
        .shutdown()
        .await
        .unwrap();
    let replay = c.client.write(request.clone(), WAIT).await.unwrap();
    assert!(matches!(replay, Reply::Accepted { .. }));
    let state = c.client.read(WAIT).await.unwrap();
    assert_eq!(state.jobs.len(), 1);
    simulation_place(&mut c.client, "j").await;
    let state = c.client.read(WAIT).await.unwrap();
    let worker_path = c.home.0.join("simulated.redb");
    let worker = SimulatedWorker::open(&worker_path, &c.config.cluster, "worker").unwrap();
    assert_eq!(worker.ensure(&state.assignments["j-a1"]).unwrap().starts, 1);
    drop(worker);
    let worker = SimulatedWorker::open(&worker_path, &c.config.cluster, "worker").unwrap();
    assert_eq!(worker.ensure(&state.assignments["j-a1"]).unwrap().starts, 1);
    c.client
        .write(
            Request {
                origin: "cluster".into(),
                id: "cancel".into(),
                mutation: Mutation::CancelQueued {
                    id: "j".into(),
                    expected_revision: 2,
                },
            },
            WAIT,
        )
        .await
        .unwrap();
    let cancelled = c.client.read(WAIT).await.unwrap();
    worker.ensure(&cancelled.assignments["j-a1"]).unwrap();
    for (i, event) in worker.events().unwrap().into_iter().enumerate() {
        let r = Request {
            origin: "cluster".into(),
            id: format!("worker-event-{i}"),
            mutation: event,
        };
        assert_eq!(
            c.client.write(r.clone(), WAIT).await.unwrap(),
            Reply::Recorded
        );
        assert_eq!(c.client.write(r, WAIT).await.unwrap(), Reply::Recorded);
    }
    assert_eq!(
        c.client.read(WAIT).await.unwrap().assignments["j-a1"]
            .observed
            .as_ref()
            .map(|(r, o)| (*r, o.phase)),
        Some((2, WorkPhase::Job(AttemptPhase::Cancelled)))
    );
    assert_eq!(worker.ensure(&state.assignments["j-a1"]).unwrap().starts, 1);
    let restarted = Replica::start(
        &c.home.0.join(leader.to_string()),
        Config {
            id: leader,
            ..c.config.clone()
        },
    )
    .await
    .unwrap();
    c.replicas.insert(leader, restarted);
    c.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn minority_cannot_commit_or_serve_strong_reads_and_recovers_after_healing() {
    let mut c = Cluster::new(3).await;
    c.client
        .write(submit("initial", "initial"), WAIT)
        .await
        .unwrap();
    let leader = c.leader().await;
    let others: Vec<_> = c
        .replicas
        .keys()
        .copied()
        .filter(|id| *id != leader)
        .collect();
    for other in &others {
        c.partition(leader, *other, true);
    }
    // Probe the isolated server directly: the normal client now discovers and escapes to the majority.
    let connection = c
        .client
        .endpoint
        .connect(c.config.peers[&leader].addr(), ALPN)
        .await
        .unwrap();
    let write = tokio::time::timeout(
        Duration::from_millis(1400),
        exchange(
            &connection,
            &c.config.cluster,
            Message::Write(Box::new(submit("uncertain", "uncertain"))),
        ),
    )
    .await;
    assert!(!matches!(write, Ok(Ok(Answer::Write(Ok(_))))));
    let read = tokio::time::timeout(
        Duration::from_millis(500),
        exchange(&connection, &c.config.cluster, Message::Read),
    )
    .await;
    assert!(!matches!(read, Ok(Ok(Answer::Read(Ok(_))))));
    let state = c.replicas[&leader].store.state().await.unwrap();
    assert!(!state.jobs.contains_key("uncertain"));
    c.client
        .write(submit("majority", "majority"), WAIT)
        .await
        .unwrap();
    for other in &others {
        c.partition(leader, *other, false);
    }
    // Retrying the unknown request is safe whether the old uncommitted entry is retained or replaced.
    c.client
        .write(submit("uncertain", "uncertain"), WAIT)
        .await
        .unwrap();
    assert_eq!(c.client.read(WAIT).await.unwrap().jobs.len(), 3);
    c.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_catches_up_from_a_compacted_snapshot_with_pruned_receipts() {
    let mut c = Cluster::new(3).await;
    let leader = c.leader().await;
    let lagging = *c.replicas.keys().find(|id| **id != leader).unwrap();
    c.replicas
        .remove(&lagging)
        .unwrap()
        .shutdown()
        .await
        .unwrap();
    let original = submit("original", "original");
    c.client.write(original.clone(), WAIT).await.unwrap();
    c.client
        .write(
            Request {
                origin: "cluster".into(),
                id: "release".into(),
                mutation: Mutation::Release {
                    request_id: "original".into(),
                },
            },
            WAIT,
        )
        .await
        .unwrap();
    for i in 0..105 {
        c.client
            .write(submit(&format!("r{i}"), &format!("j{i}")), WAIT)
            .await
            .unwrap();
    }
    c.replicas[&leader].raft.trigger().snapshot().await.unwrap();
    tokio::time::timeout(WAIT, async {
        loop {
            if c.replicas[&leader]
                .raft
                .metrics()
                .borrow()
                .purged
                .is_some_and(|id| id.index >= 100)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    let restarted = Replica::start(
        &c.home.0.join(lagging.to_string()),
        Config {
            id: lagging,
            ..c.config.clone()
        },
    )
    .await
    .unwrap();
    tokio::time::timeout(WAIT, async {
        loop {
            if restarted.store.state().await.unwrap().jobs.len() == 106 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    assert!(
        restarted
            .store
            .clone()
            .get_current_snapshot()
            .await
            .unwrap()
            .is_some()
    );
    c.replicas.insert(lagging, restarted);
    assert!(matches!(
        c.client.write(original, WAIT).await.unwrap(),
        Reply::Expired { .. }
    ));
    c.close().await;
}

// Run a real process to distinguish durable database recovery from in-memory reconstruction.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn committed_request_survives_sigkill_and_process_restart() {
    struct Child(std::process::Child);
    impl Drop for Child {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let home = Home::new();
    let initialized = std::process::Command::new(env!("CARGO_BIN_EXE_fleet-control-lab"))
        .arg("init")
        .arg("--home")
        .arg(&home.0)
        .arg("--replicas")
        .arg("1")
        .output()
        .unwrap();
    assert!(
        initialized.status.success(),
        "{}",
        String::from_utf8_lossy(&initialized.stderr)
    );
    let config_path = home.0.join("node-1/config.json");
    let config: Config = serde_json::from_slice(&std::fs::read(&config_path).unwrap()).unwrap();
    let admin = fleet::keys::load_or_create(&home.0.join("client/node.key")).unwrap();
    let start = |bootstrap: bool| {
        let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_fleet-control-lab"));
        cmd.arg("serve")
            .arg("--home")
            .arg(home.0.join("node-1"))
            .arg("--config")
            .arg(&config_path)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::inherit());
        if bootstrap {
            cmd.arg("--bootstrap");
        }
        Child(cmd.spawn().unwrap())
    };
    let mut child = start(true);
    let mut client = Client::new(admin, config.cluster, config.peers)
        .await
        .unwrap();
    let mut request = submit("durable", "j");
    request.origin = client.cluster.clone();
    let result = client.write(request.clone(), WAIT).await.unwrap();
    child.0.kill().unwrap();
    child.0.wait().unwrap();
    let _restarted = start(false);
    assert_eq!(client.write(request, WAIT).await.unwrap(), result);
    assert_eq!(client.read(WAIT).await.unwrap().jobs.len(), 1);
    simulation_place(&mut client, "j").await;
    for _ in 0..2 {
        let worker =
            SimulatedWorker::open(&home.0.join("simulation.redb"), &client.cluster, "worker")
                .unwrap();
        let state = client.read(WAIT).await.unwrap();
        assert_eq!(worker.ensure(&state.assignments["j-a1"]).unwrap().starts, 1);
        for (i, mutation) in worker.events().unwrap().into_iter().enumerate() {
            let reply = client
                .write(
                    Request {
                        origin: client.cluster.clone(),
                        id: format!("sim-{i}"),
                        mutation,
                    },
                    WAIT,
                )
                .await
                .unwrap();
            assert_eq!(reply, Reply::Recorded);
        }
    }
    assert_eq!(
        client.read(WAIT).await.unwrap().assignments["j-a1"]
            .observed
            .as_ref()
            .map(|(r, o)| (*r, o.phase)),
        Some((1, WorkPhase::Job(AttemptPhase::Running)))
    );
    client.endpoint.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn membership_learner_snapshot_preserves_pending_operation_and_receipts() {
    let mut c = Cluster::new(1).await;
    c.prepare(2, false).await;
    c.prepare(3, false).await;
    let request = c.membership_request("snapshot-grow", 1, &[1, 2, 3]);
    let Reply::MembershipAccepted { operation, .. } =
        c.client.write(request.clone(), WAIT).await.unwrap()
    else {
        panic!("accepted");
    };
    c.client
        .write(
            Request {
                origin: "cluster".into(),
                id: "release-grow".into(),
                mutation: Mutation::Release {
                    request_id: request.id.clone(),
                },
            },
            WAIT,
        )
        .await
        .unwrap();
    for i in 0..105 {
        c.client
            .write(
                submit(&format!("snapshot-r{i}"), &format!("snapshot-j{i}")),
                WAIT,
            )
            .await
            .unwrap();
    }
    c.replicas[&1].raft.trigger().snapshot().await.unwrap();
    tokio::time::timeout(WAIT, async {
        loop {
            if c.replicas[&1]
                .raft
                .metrics()
                .borrow()
                .purged
                .is_some_and(|id| id.index >= 100)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    c.start(2).await;
    c.start(3).await;
    tokio::time::timeout(WAIT * 2, async {
        loop {
            let state = c.client.read(WAIT).await.unwrap();
            if state.configuration.revision == 2 {
                assert_eq!(state.jobs.len(), 105);
                assert_eq!(state.membership_operations.len(), 1);
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    for id in [2, 3] {
        let snapshot = c
            .replicas
            .get_mut(&id)
            .unwrap()
            .store
            .get_current_snapshot()
            .await
            .unwrap()
            .expect("learner received compacted snapshot");
        let mut receiver = Store::open(
            &c.home.0.join(format!("snapshot-inspect-{id}.redb")),
            "cluster",
            99,
        )
        .unwrap();
        receiver
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .unwrap();
        let state = receiver.state().await.unwrap();
        assert!(state.membership_operations.contains_key(&operation));
        assert!(
            state.receipts[&key(
                &c.client.endpoint.secret_key().public().to_string(),
                &request.id
            )]
                .response
                .is_none()
        );
    }
    assert!(matches!(
        c.client.write(request, WAIT).await.unwrap(),
        Reply::Expired { .. }
    ));
    c.close().await;
}

#[test]
fn membership_cache_is_monotonic_and_bound_to_the_cluster() {
    use fleet::control::{discovery::save_cache, membership::MembershipView};
    let home = Home::new();
    let path = home.0.join("members.json");
    let newer = MembershipView {
        cluster: "cluster".into(),
        log_id: Some(LogId::new(openraft::CommittedLeaderId::new(3, 1), 40)),
        revision: 4,
        voter_sets: vec![BTreeSet::from([4])],
        peers: BTreeMap::new(),
        active: None,
    };
    save_cache(&path, &newer).unwrap();
    let mut older = newer.clone();
    older.log_id = Some(LogId::new(openraft::CommittedLeaderId::new(1, 1), 10));
    older.revision = 1;
    save_cache(&path, &older).unwrap();
    assert_eq!(
        serde_json::from_slice::<MembershipView>(&std::fs::read(&path).unwrap()).unwrap(),
        newer
    );
    let mut wrong = newer.clone();
    wrong.cluster = "other".into();
    assert!(save_cache(&path, &wrong).is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn membership_cli_survives_retiring_every_original_bootstrap_peer() {
    use fleet::control::membership::{MembershipOperation, MembershipStage};
    struct Child(std::process::Child);
    impl Drop for Child {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let home = Home::new();
    let root = home.0.to_str().unwrap().to_string();
    let config_path = format!("{root}/client/config.json");
    let key_path = format!("{root}/client/node.key");
    let run = |args: &[&str]| -> serde_json::Value {
        let output = std::process::Command::new(env!("CARGO_BIN_EXE_fleet-control-lab"))
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "command {args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        serde_json::from_slice(&output.stdout).unwrap()
    };
    run(&["init", "--home", &root, "--replicas", "1"]);
    let start = |id: u64, bootstrap: bool| {
        let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_fleet-control-lab"));
        command
            .args([
                "serve",
                "--home",
                &format!("{root}/node-{id}"),
                "--config",
                &format!("{root}/node-{id}/config.json"),
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::inherit());
        if bootstrap {
            command.arg("--bootstrap");
        }
        Child(command.spawn().unwrap())
    };
    let mut children = BTreeMap::from([(1, start(1, true))]);
    let original: Config = serde_json::from_slice(&std::fs::read(&config_path).unwrap()).unwrap();
    let mut peers = original.peers.clone();
    let submission = run(&[
        "request",
        "--key",
        &key_path,
        "--config",
        &config_path,
        "--request",
        &format!("{root}/submit.json"),
    ]);
    let stale_cache = home.0.join("offline-cache.json");
    std::fs::copy(
        fleet::control::discovery::incarnation_cache(
            std::path::Path::new(&config_path),
            &original.cluster,
        ),
        &stale_cache,
    )
    .unwrap();
    let transition = |request: &str, revision: u64, voters: &BTreeMap<u64, Peer>| {
        let path = format!("{root}/{request}.json");
        std::fs::write(&path, serde_json::to_vec(voters).unwrap()).unwrap();
        let args = [
            "membership",
            "--origin",
            &original.cluster,
            "--key",
            &key_path,
            "--config",
            &config_path,
            "--voters",
            &path,
            "--request-id",
            request,
            "--expected-revision",
            &revision.to_string(),
        ];
        let result: MembershipOperation = serde_json::from_value(run(&args)).unwrap();
        assert_eq!(result.stage, MembershipStage::Complete);
        // A fresh client process retries the same operation after completion.
        assert_eq!(
            serde_json::from_value::<MembershipOperation>(run(&args)).unwrap(),
            result
        );
    };
    for id in [2, 3] {
        let descriptor = run(&[
            "prepare-replica",
            "--home",
            &format!("{root}/node-{id}"),
            "--config",
            &config_path,
            "--id",
            &id.to_string(),
        ]);
        peers.extend(serde_json::from_value::<BTreeMap<u64, Peer>>(descriptor).unwrap());
        children.insert(id, start(id, false));
    }
    transition("grow", 1, &peers);
    let descriptor = run(&[
        "prepare-replica",
        "--home",
        &format!("{root}/node-4"),
        "--config",
        &config_path,
        "--id",
        "4",
    ]);
    peers.extend(serde_json::from_value::<BTreeMap<u64, Peer>>(descriptor).unwrap());
    children.insert(4, start(4, false));
    peers.remove(&1);
    transition("replace", 2, &peers);
    drop(children.remove(&1));
    peers.retain(|id, _| *id == 4);
    transition("shrink", 3, &peers);
    drop(children.remove(&2));
    drop(children.remove(&3));
    assert_eq!(
        run(&[
            "request",
            "--key",
            &key_path,
            "--config",
            &config_path,
            "--request",
            &format!("{root}/submit.json")
        ]),
        submission
    );
    // An offline client whose only seed retired needs an explicitly trusted replacement contact.
    let admin = fleet::keys::load_or_create(std::path::Path::new(&key_path)).unwrap();
    let mut cold = Client::new(admin.clone(), original.cluster.clone(), original.peers)
        .await
        .unwrap();
    assert!(cold.read(Duration::from_secs(1)).await.is_err());
    cold.endpoint.close().await;
    let mut recovered = Client::with_cache(admin, original.cluster, peers, stale_cache)
        .await
        .unwrap();
    assert_eq!(
        recovered.read(WAIT).await.unwrap().configuration.revision,
        4
    );
    recovered.endpoint.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn membership_single_voter_failure_during_growth_waits_for_its_restart() {
    use fleet::control::membership::MembershipStage;
    let mut c = Cluster::new(1).await;
    c.prepare(2, true).await;
    c.prepare(3, true).await;
    c.replicas[&1].store.hold_joint_apply(true);
    let request = c.membership_request("grow-with-restart", 1, &[1, 2, 3]);
    let Reply::MembershipAccepted { operation, .. } =
        c.client.write(request.clone(), WAIT).await.unwrap()
    else {
        panic!("accepted");
    };
    tokio::time::timeout(WAIT, async {
        loop {
            if c.replicas[&1]
                .store
                .state()
                .await
                .unwrap()
                .membership_operations[&operation]
                .stage
                == MembershipStage::Joint
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    c.replicas[&1].membership_task.abort();
    tokio::task::yield_now().await;
    c.replicas[&1].store.hold_joint_apply(false);
    c.replicas.remove(&1).unwrap().shutdown().await.unwrap();
    assert!(
        c.client.read(Duration::from_secs(2)).await.is_err(),
        "the old sole voter is still required in joint consensus"
    );
    c.start(1).await;
    let state = c.changed(request).await;
    assert_eq!(state.configuration.revision, 2);
    assert_eq!(state.membership_operations.len(), 1);
    c.close().await;
}

#[path = "control/real_worker.rs"]
mod real_worker;

#[path = "control/networking.rs"]
mod networking;

#[path = "control/queue.rs"]
mod queue_tests;

fn simulation_registration(identity: iroh::EndpointId) -> fleet::control::workload::Registration {
    use fleet::control::workload::*;
    Registration {
        runtimes: Default::default(),
        id: "worker".into(),
        peer: Peer {
            identity,
            address: "127.0.0.1:1".parse().unwrap(),
        },
        incarnation: "simulated-journal".into(),
        budget: Budget {
            cpu_slots: 4,
            memory_mb: 8192,
            disk_gb: 0,
            vm_slots: 0,
        },
        images: BTreeSet::new(),
        tags: BTreeSet::new(),
    }
}
async fn simulation_place(client: &mut Client, job: &str) {
    for (id, mutation) in [
        (
            "sim-enroll",
            Mutation::Enroll {
                worker: simulation_registration(client.endpoint.id()),
            },
        ),
        (
            "sim-place",
            Mutation::Place {
                id: job.into(),
                worker: "worker".into(),
                expected_revision: 1,
            },
        ),
    ] {
        let reply = client
            .write(
                Request {
                    origin: client.cluster.clone(),
                    id: id.into(),
                    mutation,
                },
                WAIT,
            )
            .await
            .unwrap();
        assert!(!matches!(reply, Reply::Rejected { .. }), "{reply:?}");
    }
}

#[path = "control/history_state.rs"]
mod history_state;

#[path = "control/indexed.rs"]
mod indexed;
#[path = "control/tunnels.rs"]
mod tunnels;
#[path = "control/versions.rs"]
mod versions;
