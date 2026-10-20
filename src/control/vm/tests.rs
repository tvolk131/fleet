use super::*;
use crate::control::{
    Command, Mutation, Request,
    network::Peer,
    workload::{Budget, Registration},
};
fn setup() -> (State, String) {
    let mut s = State::new("vm-test".into());
    s.version.active = GENERATION;
    let key = iroh::SecretKey::generate();
    let principal = key.public().to_string();
    let worker = Registration {
        id: "worker".into(),
        incarnation: "disk-1".into(),
        peer: Peer {
            identity: key.public(),
            address: "127.0.0.1:29301".parse().unwrap(),
        },
        budget: Budget {
            cpu_slots: 4,
            memory_mb: 8192,
            disk_gb: 200,
            vm_slots: 2,
        },
        images: BTreeSet::from(["macos".into()]),
        linux_images: BTreeSet::new(),
        runtimes: Default::default(),
        tags: Default::default(),
    };
    assert_eq!(s.enroll(&worker), Reply::Recorded);
    for id in ["database", "web"] {
        let spec=serde_json::from_value(serde_json::json!({"image":"macos","cpus":1,"memory_mb":4096,"disk_gb":50,"network":"offline","gpu":"shared"})).unwrap();
        assert!(matches!(
            s.assign(
                id,
                &format!("vm-{id}"),
                "worker",
                &Workload::Instance { spec }
            ),
            Reply::Accepted { .. }
        ));
    }
    assert_eq!(report(&mut s, &principal, Update::Capable), Reply::Recorded);
    (s, principal)
}
fn report(s: &mut State, principal: &str, update: Update) -> Reply {
    s.vm_report(
        principal,
        &Report {
            worker: "worker".into(),
            incarnation: "disk-1".into(),
            update,
        },
    )
}
fn configure(s: &mut State, id: &str, revision: u64, config: Connections) -> Reply {
    s.vm_action(&Action::Connections {
        instance: id.into(),
        expected_revision: revision,
        config,
    })
}
fn adapter() -> tunnels::Adapter {
    tunnels::Adapter {
        endpoint: iroh::EndpointAddr::new(iroh::SecretKey::generate().public())
            .with_ip_addr("127.0.0.1:29302".parse::<SocketAddr>().unwrap()),
    }
}
fn expose() -> Connections {
    Connections {
        exports: BTreeMap::from([("sql".into(), "127.0.0.1:5432".parse().unwrap())]),
        routes: vec![],
    }
}
fn consume() -> Connections {
    Connections {
        exports: Default::default(),
        routes: vec![Route {
            service: Target::Vm {
                instance: "database".into(),
                service: "sql".into(),
            },
            listen: "127.0.0.1:15432".parse().unwrap(),
        }],
    }
}
fn start(id: &str) -> Action {
    Action::Start {
        id: id.into(),
        instance: "web".into(),
        expected_revision: 1,
        spec: guest::Spec::Exec {
            argv: vec!["/bin/echo".into(), "hello".into()],
            cwd: None,
            env: Default::default(),
            timeout_seconds: 30,
        },
        retention_seconds: 60,
    }
}
#[test]
fn connections_require_capabilities_valid_references_and_explicit_grants() {
    let (mut s, p) = setup();
    s.vm.capable.clear();
    assert!(matches!(
        configure(&mut s, "database", 0, expose()),
        Reply::Rejected { .. }
    ));
    assert_eq!(report(&mut s, &p, Update::Capable), Reply::Recorded);
    assert!(matches!(
        configure(&mut s, "web", 0, consume()),
        Reply::Rejected { .. }
    ));
    assert_eq!(configure(&mut s, "database", 0, expose()), Reply::Recorded);
    assert_eq!(configure(&mut s, "web", 0, consume()), Reply::Recorded);
    assert!(s.tunnels.topology.adapters.is_empty());
    assert_eq!(
        report(
            &mut s,
            &p,
            Update::Bound {
                instance: "database".into(),
                adapter: adapter()
            }
        ),
        Reply::Recorded
    );
    assert!(
        s.tunnels.topology.services[&service_id("database", "sql")]
            .allowed
            .is_empty()
    );
    assert_eq!(
        report(
            &mut s,
            &p,
            Update::Bound {
                instance: "web".into(),
                adapter: adapter()
            }
        ),
        Reply::Recorded
    );
    assert_eq!(
        s.tunnels.topology.services[&service_id("database", "sql")].allowed,
        BTreeSet::from([adapter_id("web")])
    );
    assert_eq!(s.tunnels.topology.routes.len(), 1);
    let before = s.vm.clone();
    assert!(matches!(
        configure(&mut s, "web", 0, Connections::default()),
        Reply::Rejected { .. }
    ));
    assert_eq!(s.vm, before);
}
#[test]
fn attachment_identity_is_authenticated_stable_and_deleted_identity_is_denied() {
    let (mut s, p) = setup();
    assert_eq!(configure(&mut s, "database", 0, expose()), Reply::Recorded);
    let a = adapter();
    let update = Update::Bound {
        instance: "database".into(),
        adapter: a.clone(),
    };
    assert!(matches!(
        report(&mut s, "other", update.clone()),
        Reply::Rejected { .. }
    ));
    assert_eq!(report(&mut s, &p, update.clone()), Reply::Recorded);
    assert_eq!(report(&mut s, &p, update), Reply::Recorded);
    assert!(matches!(
        report(
            &mut s,
            &p,
            Update::Bound {
                instance: "database".into(),
                adapter: adapter()
            }
        ),
        Reply::Rejected { .. }
    ));
    let spec = s.vm.networks["database"].clone();
    assert!(matches!(
        s.desire("database", 1, Desired::Instance(InstanceDesired::Stopped)),
        Reply::Accepted { .. }
    ));
    assert_eq!(s.vm.networks["database"], spec);
    assert_eq!(s.vm.bindings["database"].adapter, a);
    assert!(matches!(
        s.desire("database", 2, Desired::Instance(InstanceDesired::Deleted)),
        Reply::Accepted { .. }
    ));
    assert!(
        s.tunnel_view(&adapter_id("database"), a.endpoint.id)
            .topology
            .adapters
            .is_empty()
    );
    assert!(s.tunnels.known_identities.contains_key(&a.endpoint.id));
    assert!(matches!(
        configure(&mut s, "database", 1, expose()),
        Reply::Rejected { .. }
    ));
}
#[test]
fn operation_request_replay_is_idempotent_and_cannot_change_intent() {
    let (mut s, _) = setup();
    let request = Request {
        origin: s.cluster.clone(),
        id: "req".into(),
        mutation: Mutation::Vm(start("install")),
    };
    let command = Command {
        principal: "admin".into(),
        request,
    };
    assert_eq!(s.apply(command.clone(), 1), Reply::Recorded);
    assert_eq!(s.apply(command.clone(), 2), Reply::Recorded);
    assert_eq!(s.vm.operations.len(), 1);
    assert!(instances::valid_id(&s.vm.operations["install"].backend_id()).is_ok());
    let mut changed = command;
    changed.request.mutation = Mutation::Vm(start("different"));
    assert!(matches!(s.apply(changed, 3), Reply::Rejected { .. }));
    assert!(matches!(
        s.vm_action(&Action::Release {
            id: "install".into(),
            expected_revision: 1
        }),
        Reply::Rejected { .. }
    ));
}
#[test]
fn stale_reports_cannot_resurrect_released_results_and_cleanup_is_observed() {
    let (mut s, p) = setup();
    assert_eq!(s.vm_action(&start("install")), Reply::Recorded);
    let result = Outcome {
        phase: Phase::Succeeded,
        finished_at: Some(100),
        exit_code: Some(0),
        ..Default::default()
    };
    assert!(matches!(
        report(
            &mut s,
            "other",
            Update::Operation {
                id: "install".into(),
                expected_revision: 1,
                result: result.clone()
            }
        ),
        Reply::Rejected { .. }
    ));
    assert_eq!(
        report(
            &mut s,
            &p,
            Update::Operation {
                id: "install".into(),
                expected_revision: 1,
                result: result.clone()
            }
        ),
        Reply::Recorded
    );
    assert_eq!(s.vm_action(&Action::Sweep { as_of: 159 }), Reply::Recorded);
    assert!(!s.vm.operations["install"].released);
    assert_eq!(s.vm_action(&Action::Sweep { as_of: 160 }), Reply::Recorded);
    assert!(s.vm.operations["install"].released);
    assert!(!s.vm.operations["install"].reclaimed);
    assert_eq!(
        report(
            &mut s,
            &p,
            Update::Operation {
                id: "install".into(),
                expected_revision: 1,
                result
            }
        ),
        Reply::Recorded
    );
    assert!(s.vm.operations["install"].released);
    assert!(matches!(
        report(
            &mut s,
            &p,
            Update::Reclaimed {
                id: "install".into(),
                expected_revision: 2
            }
        ),
        Reply::Rejected { .. }
    ));
    assert_eq!(
        report(
            &mut s,
            &p,
            Update::Reclaimed {
                id: "install".into(),
                expected_revision: 3
            }
        ),
        Reply::Recorded
    );
}
#[test]
fn stopped_vm_and_inactive_generation_cannot_accept_execution() {
    let (mut s, _) = setup();
    s.version.active = GENERATION - 1;
    assert!(matches!(
        s.apply(
            Command {
                principal: "admin".into(),
                request: Request {
                    origin: s.cluster.clone(),
                    id: "too-early".into(),
                    mutation: Mutation::Vm(start("install"))
                }
            },
            1
        ),
        Reply::Rejected { .. }
    ));
    assert!(s.vm.operations.is_empty());
    s.version.active = GENERATION;
    s.desire("web", 1, Desired::Instance(InstanceDesired::Stopped));
    assert!(matches!(
        s.vm_action(&start("install")),
        Reply::Rejected { .. }
    ));
}
#[test]
fn managed_names_and_host_configuration_cannot_override_vm_identity() {
    let (mut s, p) = setup();
    configure(&mut s, "database", 0, expose());
    report(
        &mut s,
        &p,
        Update::Bound {
            instance: "database".into(),
            adapter: adapter(),
        },
    );
    let before = s.tunnels.clone();
    let mut malicious = tunnels::Topology::default();
    malicious.adapters.insert(adapter_id("database"), adapter());
    assert!(matches!(
        s.vm_host_topology(s.tunnels.revision, &malicious),
        Reply::Rejected { .. }
    ));
    assert_eq!(s.tunnels, before);
    let bad = Connections {
        exports: BTreeMap::from([("escape".into(), "192.168.1.1:80".parse().unwrap())]),
        routes: vec![],
    };
    assert!(matches!(
        configure(&mut s, "web", 0, bad),
        Reply::Rejected { .. }
    ));
}

#[test]
fn oversized_and_mislabelled_results_are_rejected_without_changing_state() {
    let (mut s, p) = setup();
    assert_eq!(s.vm_action(&start("bounded")), Reply::Recorded);
    for (name, size) in [("stdout", u64::MAX), ("download", 1), ("../escape", 0)] {
        let old = s.vm.clone();
        let result = Outcome {
            phase: Phase::Succeeded,
            finished_at: Some(10),
            files: BTreeMap::from([(
                name.into(),
                File {
                    hash: iroh_blobs::Hash::new(b"x"),
                    size,
                },
            )]),
            ..Outcome::default()
        };
        assert!(matches!(
            report(
                &mut s,
                &p,
                Update::Operation {
                    id: "bounded".into(),
                    expected_revision: 1,
                    result
                }
            ),
            Reply::Rejected { .. }
        ));
        assert_eq!(s.vm, old);
    }
}
#[test]
fn unfinished_operation_limit_and_retention_bounds_do_not_reserve_rejected_work() {
    let (mut s, _) = setup();
    let mut bad = start("bad-retention");
    if let Action::Start {
        retention_seconds, ..
    } = &mut bad
    {
        *retention_seconds = u64::MAX;
    }
    assert!(matches!(s.vm_action(&bad), Reply::Rejected { .. }));
    for i in 0..4 {
        assert_eq!(
            s.vm_action(&start(&format!("pending-{i}"))),
            Reply::Recorded
        );
    }
    assert!(matches!(
        s.vm_action(&start("overflow")),
        Reply::Rejected { .. }
    ));
    assert_eq!(s.vm.operations.len(), 4);
}

#[test]
fn authority_recovery_clears_permissions_fences_uncertainty_and_keeps_completed_results() {
    let (mut s, p) = setup();
    assert_eq!(configure(&mut s, "database", 0, expose()), Reply::Recorded);
    assert_eq!(
        report(
            &mut s,
            &p,
            Update::Bound {
                instance: "database".into(),
                adapter: adapter()
            }
        ),
        Reply::Recorded
    );
    assert_eq!(s.vm_action(&start("pending")), Reply::Recorded);
    assert_eq!(s.vm_action(&start("complete")), Reply::Recorded);
    assert_eq!(
        report(
            &mut s,
            &p,
            Update::Operation {
                id: "complete".into(),
                expected_revision: 1,
                result: Outcome {
                    phase: Phase::Succeeded,
                    finished_at: Some(10),
                    ..Outcome::default()
                }
            }
        ),
        Reply::Recorded
    );
    let complete = s.vm.operations["complete"].clone();
    s.vm.recover(20);
    assert!(
        s.vm.networks.is_empty()
            && s.vm.bindings.is_empty()
            && s.vm.capable.is_empty()
            && s.vm.hosts.is_none()
    );
    assert_eq!(s.vm.operations["complete"], complete);
    assert_eq!(s.vm.operations["pending"].outcome.phase, Phase::Interrupted);
    assert_eq!(s.vm.operations["pending"].outcome.finished_at, Some(20));
    assert_eq!(s.vm.operations["pending"].revision, 2);
}

#[test]
fn large_command_history_is_bounded_before_it_can_break_control_reads() {
    let (mut s, p) = setup();
    let mut rejected = false;
    for n in 0..40 {
        let id = format!("large-{n}");
        let mut action = start(&id);
        if let Action::Start {
            spec: guest::Spec::Exec { argv, .. },
            ..
        } = &mut action
        {
            argv.push("x".repeat(60 << 10));
        }
        let before = s.vm.clone();
        match s.vm_action(&action) {
            Reply::Recorded => assert_eq!(
                report(
                    &mut s,
                    &p,
                    Update::Operation {
                        id,
                        expected_revision: 1,
                        result: Outcome {
                            phase: Phase::Succeeded,
                            finished_at: Some(10),
                            ..Outcome::default()
                        }
                    }
                ),
                Reply::Recorded
            ),
            Reply::Rejected { reason } => {
                assert!(reason.contains("metadata budget"));
                assert_eq!(s.vm, before);
                rejected = true;
                break;
            }
            other => panic!("{other:?}"),
        }
    }
    assert!(rejected);
    assert!(serde_json::to_vec(&s.vm).unwrap().len() < MAX_OPERATION_METADATA);
}
