use fleet::{
    instances::{self, Config, Desired, Gpu, Network, Spec, State},
    proto::{Hello, PROTOCOL},
};
use std::{
    collections::BTreeMap,
    fs,
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

#[path = "instances/guest.rs"]
mod guest_tests;

fn hello() -> Hello {
    Hello {
        agent_version: "test".into(),
        protocol: PROTOCOL,
        hostname: "test".into(),
        os: "macOS".into(),
        cpu: "test".into(),
        cores: 8,
        memory_bytes: 32 << 30,
        gpu: Some("Metal".into()),
        tags: vec![],
        sleep_minutes: None,
        cpu_slots: 8,
    }
}

struct Fixture {
    home: PathBuf,
    controllers: Vec<Child>,
}
impl Fixture {
    fn new() -> Self {
        let home = std::env::temp_dir().join(format!("fleet-vm-{:016x}", rand::random::<u64>()));
        fs::create_dir_all(&home).unwrap();
        let tart = home.join("tart");
        fs::write(&tart, include_str!("fixtures/fake_tart.py")).unwrap();
        fs::set_permissions(&tart, fs::Permissions::from_mode(0o700)).unwrap();
        let softnet = home.join("softnet");
        fs::write(&softnet, "#!/bin/sh\necho 'softnet 0.24.0-test'\n").unwrap();
        fs::set_permissions(&softnet, fs::Permissions::from_mode(0o700)).unwrap();
        instances::write(
            &home.join("instances.json"),
            &Config {
                platforms: Default::default(),
                tart,
                softnet,
                images: BTreeMap::from([(
                    "macos".into(),
                    format!("registry.test/macos@sha256:{}", "a".repeat(64)),
                )]),
                memory_budget_mb: 8192,
                disk_budget_gb: 200,
                min_free_disk_gb: 5,
                block_cidrs: vec![],
            },
        )
        .unwrap();
        Self {
            home,
            controllers: vec![],
        }
    }
    fn create(&self, id: &str) -> instances::Info {
        instances::create(&self.home, id, spec(), "owner", &hello()).unwrap()
    }
    fn controller(&mut self, id: &str) {
        self.controllers.push(
            Command::new(env!("CARGO_BIN_EXE_fleet"))
                .arg("instance-controller")
                .arg(&self.home)
                .arg(id)
                .env("FLEET_HOME", &self.home)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap(),
        );
    }
    fn set(&self, id: &str, desired: Desired) -> instances::Info {
        let revision = instances::info(&self.home, id).unwrap().record.revision;
        instances::set_desired(&self.home, id, desired, revision, "owner", &hello()).unwrap()
    }
    fn wait(&self, id: &str, state: State) -> instances::Info {
        let deadline = Instant::now() + Duration::from_secs(90);
        loop {
            let i = instances::info(&self.home, id).unwrap();
            if i.observed.state == state && i.observed.revision == i.record.revision {
                return i;
            }
            assert!(
                Instant::now() < deadline,
                "waiting for {state:?}: {i:?}; VM log: {:?}",
                fs::read_to_string(self.home.join("instances").join(id).join("vm.log"))
            );
            std::thread::sleep(Duration::from_millis(100));
        }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        // Only fixture-owned processes. This also handles an assertion failure.
        for child in &mut self.controllers {
            let _ = child.kill();
            let _ = child.wait();
        }
        if let Ok(entries) = fs::read_dir(self.home.join("instance-tart/vms")) {
            for entry in entries.flatten() {
                if let Ok(pid) = fs::read_to_string(entry.path().join("pid"))
                    && let Ok(pid) = pid.parse::<i32>()
                {
                    unsafe {
                        libc::kill(pid, libc::SIGTERM);
                    }
                }
            }
        }
        let _ = fs::remove_dir_all(&self.home);
    }
}
fn spec() -> Spec {
    Spec {
        runtime: Default::default(),
        image: "macos".into(),
        cpus: 4,
        memory_mb: 4096,
        disk_gb: 50,
        network: Network::Offline,
        gpu: Gpu::Exclusive,
    }
}

fn shared_spec() -> Spec {
    Spec {
        cpus: 2,
        gpu: Gpu::Shared,
        ..spec()
    }
}

/// Create a provisioned VM without launching a controller, so admission tests
/// can inspect the durable reservation before a start or stop is acknowledged.
fn stopped(f: &Fixture, id: &str, spec: Spec) {
    let instance = instances::create(&f.home, id, spec, "owner", &hello()).unwrap();
    instances::write(
        &f.home.join("instances").join(id).join("observed.json"),
        &instances::Observed {
            state: State::Stopped,
            revision: instance.record.revision,
            ..Default::default()
        },
    )
    .unwrap();
}

#[test]
fn gpu_modes_default_to_exclusive_and_gate_shared_on_protocol_3_4() {
    use fleet::proto::{Protocol, Request, too_new};
    let legacy = serde_json::to_value(spec()).unwrap();
    assert!(
        legacy.get("gpu").is_none(),
        "old agents reject unknown spec fields"
    );
    assert_eq!(
        serde_json::from_value::<Spec>(legacy.clone()).unwrap().gpu,
        Gpu::Exclusive
    );
    let mut explicit = legacy.clone();
    explicit["gpu"] = "exclusive".into();
    assert_eq!(
        serde_json::from_value::<Spec>(explicit.clone()).unwrap(),
        spec()
    );
    for unsupported in ["none", "disabled", "dedicated", "half"] {
        explicit["gpu"] = unsupported.into();
        assert!(serde_json::from_value::<Spec>(explicit.clone()).is_err());
    }
    let mut unsupported_cpu = legacy;
    unsupported_cpu["cpu_mode"] = "dedicated".into();
    assert!(serde_json::from_value::<Spec>(unsupported_cpu).is_err());
    let exclusive = Request::InstanceCreate {
        id: "old".into(),
        spec: spec(),
    };
    let shared = Request::InstanceCreate {
        id: "new".into(),
        spec: shared_spec(),
    };
    assert_eq!(too_new(&exclusive, Protocol { major: 3, minor: 3 }), None);
    assert_eq!(
        too_new(&shared, Protocol { major: 3, minor: 3 }),
        Some(("InstanceCreate.spec.gpu", 4))
    );
    assert_eq!(too_new(&shared, PROTOCOL), None);
}

#[test]
fn exclusive_and_shared_vm_reservations_conflict_in_both_directions() {
    for first_gpu in [Gpu::Exclusive, Gpu::Shared] {
        let f = Fixture::new();
        stopped(
            &f,
            "first",
            Spec {
                gpu: first_gpu,
                ..shared_spec()
            },
        );
        f.set("first", Desired::Running);
        for next_gpu in [Gpu::Exclusive, Gpu::Shared] {
            let result = instances::admit_vm(
                &f.home,
                &Spec {
                    gpu: next_gpu,
                    ..shared_spec()
                },
                &hello(),
            );
            assert_eq!(
                result.is_ok(),
                first_gpu == Gpu::Shared && next_gpu == Gpu::Shared,
                "{first_gpu:?} + {next_gpu:?}: {result:?}"
            );
        }
        assert!(instances::admit_job(&f.home, 1, Some(true), None, &hello()).is_err());
        assert!(instances::admit_job(&f.home, 1, None, None, &hello()).is_err());
    }
}

#[test]
fn shared_vms_enforce_aggregate_cpu_and_memory_budgets() {
    let f = Fixture::new();
    stopped(&f, "one", shared_spec());
    stopped(&f, "two", shared_spec());
    f.set("one", Desired::Running);
    // Each VM fits separately; adding a second must count the first.
    let mut capacity = hello();
    capacity.cpu_slots = 3;
    assert!(
        instances::admit_vm(&f.home, &shared_spec(), &capacity)
            .unwrap_err()
            .to_string()
            .contains("CPU")
    );
    capacity = hello();
    capacity.memory_bytes = 6 << 30;
    assert!(
        instances::admit_vm(&f.home, &shared_spec(), &capacity)
            .unwrap_err()
            .to_string()
            .contains("memory")
    );
    let mut config = Config::load(&f.home).unwrap();
    config.memory_budget_mb = 6144;
    instances::write(&f.home.join("instances.json"), &config).unwrap();
    assert!(
        instances::admit_vm(&f.home, &shared_spec(), &hello())
            .unwrap_err()
            .to_string()
            .contains("memory")
    );
    config.memory_budget_mb = 8192;
    instances::write(&f.home.join("instances.json"), &config).unwrap();
    f.set("two", Desired::Running); // Exactly fills the memory budget.
    assert!(instances::admit_job(&f.home, 4, Some(false), None, &hello()).is_ok());
    assert!(instances::admit_job(&f.home, 5, Some(false), None, &hello()).is_err());
    assert!(
        instances::admit_vm(&f.home, &shared_spec(), &hello())
            .unwrap_err()
            .to_string()
            .contains("memory")
    );
    // A stop request, failed launch or unknown state must not prematurely free capacity.
    f.set("one", Desired::Stopped);
    for state in [
        State::Stopped,
        State::Stopping,
        State::Failed,
        State::Unknown,
    ] {
        let mut observed = instances::info(&f.home, "one").unwrap().observed;
        observed.state = state;
        instances::write(&f.home.join("instances/one/observed.json"), &observed).unwrap();
        assert!(
            instances::admit_vm(&f.home, &shared_spec(), &hello()).is_err(),
            "{state:?}"
        );
    }
    let mut one = instances::info(&f.home, "one").unwrap();
    one.observed.state = State::Stopped;
    one.observed.revision = one.record.revision;
    instances::write(&f.home.join("instances/one/observed.json"), &one.observed).unwrap();
    assert!(instances::admit_vm(&f.home, &shared_spec(), &hello()).is_ok());
}

#[test]
fn macos_vm_count_includes_pending_starts_and_unconfirmed_stops() {
    let f = Fixture::new();
    let mut config = Config::load(&f.home).unwrap();
    config.memory_budget_mb = 16384;
    instances::write(&f.home.join("instances.json"), &config).unwrap();
    for id in ["one", "two", "three", "four"] {
        stopped(&f, id, shared_spec());
    }
    f.set("one", Desired::Running);
    f.set("two", Desired::Running);
    let rejected = || {
        let third = instances::info(&f.home, "three").unwrap();
        assert!(
            instances::set_desired(
                &f.home,
                "three",
                Desired::Running,
                third.record.revision,
                "owner",
                &hello()
            )
            .unwrap_err()
            .to_string()
            .contains("macOS VM limit"),
        );
        assert_eq!(
            instances::info(&f.home, "three").unwrap().record,
            third.record
        );
    };
    rejected(); // CPU and RAM fit, but two pending starts already own the slots.
    f.set("one", Desired::Stopped);
    for state in [
        State::Stopped,
        State::Stopping,
        State::Failed,
        State::Unknown,
    ] {
        let mut observed = instances::info(&f.home, "one").unwrap().observed;
        observed.state = state;
        instances::write(&f.home.join("instances/one/observed.json"), &observed).unwrap();
        rejected();
    }
    let mut one = instances::info(&f.home, "one").unwrap();
    one.observed.state = State::Stopped;
    one.observed.revision = one.record.revision;
    instances::write(&f.home.join("instances/one/observed.json"), &one.observed).unwrap();
    f.set("three", Desired::Running);
}

#[test]
fn two_shared_vms_run_and_restart_without_double_counting_their_claims() {
    let mut f = Fixture::new();
    for id in ["one", "two"] {
        instances::create(&f.home, id, shared_spec(), "owner", &hello()).unwrap();
        f.controller(id);
        f.wait(id, State::Stopped);
    }
    // Both desired-state claims exist before either controller starts its VM.
    f.set("one", Desired::Running);
    f.set("two", Desired::Running);
    for id in ["one", "two"] {
        f.wait(id, State::Running);
    }
    // Simulate controller loss and a host reboot: kill only fixture-owned processes.
    for child in &mut f.controllers {
        child.kill().unwrap();
        child.wait().unwrap();
    }
    f.controllers.clear();
    for id in ["one", "two"] {
        assert!(
            Command::new(f.home.join("tart"))
                .env("TART_HOME", f.home.join("instance-tart"))
                .args(["stop", &format!("fleet-{id}")])
                .status()
                .unwrap()
                .success()
        );
        let mut observed = instances::info(&f.home, id).unwrap().observed;
        observed.state = State::Unknown;
        instances::write(
            &f.home.join("instances").join(id).join("observed.json"),
            &observed,
        )
        .unwrap();
    }
    for id in ["one", "two"] {
        f.controller(id);
    }
    for id in ["one", "two"] {
        f.wait(id, State::Running);
    }
    assert_eq!(
        fs::read_to_string(f.home.join("instance-tart/history"))
            .unwrap()
            .lines()
            .filter(|s| s.starts_with("run "))
            .count(),
        4
    );
    // Restart admission uses the current budget, including the other VM's claim.
    f.controllers[0].kill().unwrap();
    f.controllers[0].wait().unwrap();
    assert!(
        Command::new(f.home.join("tart"))
            .env("TART_HOME", f.home.join("instance-tart"))
            .args(["stop", "fleet-one"])
            .status()
            .unwrap()
            .success()
    );
    let mut config = Config::load(&f.home).unwrap();
    config.memory_budget_mb = 6144;
    instances::write(&f.home.join("instances.json"), &config).unwrap();
    f.controller("one");
    let failed = f.wait("one", State::Failed);
    assert!(failed.observed.error.unwrap().contains("memory"));
    assert!(!f.home.join("instance-tart/vms/fleet-one/pid").exists());
    for id in ["one", "two"] {
        f.set(id, Desired::Stopped);
        f.wait(id, State::Stopped);
        f.set(id, Desired::Deleted);
        f.wait(id, State::Deleted);
    }
}

#[test]
fn validation_and_replay_do_not_change_an_existing_instance() {
    let f = Fixture::new();
    for id in ["", "../escape", "-option", "a/b", "💻"] {
        assert!(instances::create(&f.home, id, spec(), "owner", &hello()).is_err());
    }
    let first = f.create("one");
    assert_eq!(f.create("one"), first);
    let mut changed = spec();
    changed.cpus = 5;
    assert!(instances::create(&f.home, "one", changed, "owner", &hello()).is_err());
    assert!(instances::create(&f.home, "one", spec(), "stranger", &hello()).is_err());
    assert!(
        instances::set_desired(&f.home, "one", Desired::Running, 1, "owner", &hello()).is_err(),
        "cannot start before provisioning finishes"
    );
    assert_eq!(
        fs::metadata(f.home.join("instances/one/desired.json"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
}

#[test]
fn stale_stopped_observations_do_not_release_compute() {
    let f = Fixture::new();
    let mut instance = f.create("one");
    assert!(!instance.reserves_compute());
    instance.observed.state = State::Stopped;
    instance.observed.revision = 1;
    instance.record.revision = 3; // start followed by stop before the controller responds
    assert!(instance.reserves_compute());
    instance.observed.revision = 3;
    assert!(!instance.reserves_compute());
}

#[test]
fn lost_network_policy_stops_a_public_vm() {
    let mut f = Fixture::new();
    let mut public = spec();
    public.network = Network::PublicInternet;
    instances::create(&f.home, "public", public, "owner", &hello()).unwrap();
    f.controller("public");
    f.wait("public", State::Stopped);
    f.set("public", Desired::Running);
    f.wait("public", State::Running);
    fs::write(f.home.join("instance-tart/refuse-shutdown"), "").unwrap();
    fs::remove_file(f.home.join("instances/public/network-subnets.json")).unwrap();
    f.wait("public", State::Failed);
    assert!(!f.home.join("instance-tart/vms/fleet-public/pid").exists());
    assert!(
        !fs::read_to_string(f.home.join("instance-tart/history"))
            .unwrap()
            .contains("exec fleet-public"),
        "network-policy failures must not wait for the guest"
    );
    f.set("public", Desired::Stopped);
    f.wait("public", State::Stopped);
    f.set("public", Desired::Deleted);
    f.wait("public", State::Deleted);
}

#[test]
fn lifecycle_survives_controller_loss_preserves_disk_and_rejects_stale_commands() {
    let mut f = Fixture::new();
    f.create("one");
    f.controller("one");
    f.wait("one", State::Stopped);
    f.set("one", Desired::Running);
    f.wait("one", State::Running);
    let disk = f.home.join("instance-tart/vms/fleet-one/disk");
    fs::write(&disk, "checkpoint").unwrap();
    f.controllers[0].kill().unwrap();
    f.controllers[0].wait().unwrap();
    f.controller("one");
    std::thread::sleep(Duration::from_secs(2));
    let history = fs::read_to_string(f.home.join("instance-tart/history")).unwrap();
    assert_eq!(
        history.lines().filter(|s| s.starts_with("run ")).count(),
        1,
        "adoption must not launch another VM"
    );
    let started = instances::info(&f.home, "one").unwrap();
    assert!(instances::admit_job(&f.home, 1, Some(true), None, &hello()).is_err());
    assert!(
        instances::admit_job(&f.home, 1, None, None, &hello()).is_err(),
        "legacy submissions are conservative"
    );
    assert!(instances::admit_job(&f.home, 4, Some(false), None, &hello()).is_ok());
    assert!(instances::admit_job(&f.home, 5, Some(false), None, &hello()).is_err());
    f.set("one", Desired::Stopped);
    f.wait("one", State::Stopped);
    assert!(
        instances::set_desired(
            &f.home,
            "one",
            Desired::Running,
            started.record.revision - 1,
            "owner",
            &hello()
        )
        .is_err(),
        "delayed start must not undo stop"
    );
    assert_eq!(fs::read_to_string(&disk).unwrap(), "checkpoint");
    f.set("one", Desired::Running);
    f.wait("one", State::Running);
    assert!(
        instances::set_desired(
            &f.home,
            "one",
            Desired::Deleted,
            instances::info(&f.home, "one").unwrap().record.revision,
            "owner",
            &hello()
        )
        .is_err()
    );
    f.set("one", Desired::Stopped);
    f.wait("one", State::Stopped);
    f.set("one", Desired::Deleted);
    f.wait("one", State::Deleted);
    assert!(!disk.exists());
    assert_eq!(
        f.create("one").record.desired,
        Desired::Deleted,
        "create retry must not resurrect a deleted disk"
    );
}

#[test]
fn a_late_launcher_failure_is_reported_without_a_restart_loop() {
    let mut f = Fixture::new();
    f.create("one");
    f.controller("one");
    f.wait("one", State::Stopped);
    fs::write(f.home.join("instance-tart/fail-after-lock"), "").unwrap();
    fs::write(f.home.join("instance-tart/gate-launcher-failure"), "").unwrap();
    f.set("one", Desired::Running);
    f.wait("one", State::Running);
    // Force the launcher to exit during inventory, between try_wait and reconciliation.
    fs::write(f.home.join("instance-tart/fail-launcher-during-list"), "").unwrap();
    let failed = f.wait("one", State::Failed);
    assert!(failed.reserves_compute());
    assert!(
        failed
            .observed
            .error
            .unwrap()
            .contains("Late virtual machine capacity error")
    );
    std::thread::sleep(Duration::from_secs(2));
    assert_eq!(
        fs::read_to_string(f.home.join("instance-tart/history"))
            .unwrap()
            .lines()
            .filter(|s| s.starts_with("run "))
            .count(),
        1
    );
    f.set("one", Desired::Stopped);
    f.wait("one", State::Stopped);
    f.set("one", Desired::Deleted);
    f.wait("one", State::Deleted);
}

#[test]
fn uncooperative_guest_cannot_prevent_stop_or_release_its_claim_early() {
    let mut f = Fixture::new();
    f.create("one");
    f.controller("one");
    f.wait("one", State::Stopped);
    f.set("one", Desired::Running);
    f.wait("one", State::Running);
    fs::write(f.home.join("instance-tart/refuse-shutdown"), "").unwrap();
    f.set("one", Desired::Stopped);
    f.wait("one", State::Stopping);
    assert!(instances::admit_job(&f.home, 1, Some(true), None, &hello()).is_err());
    f.wait("one", State::Stopped);
    let history = fs::read_to_string(f.home.join("instance-tart/history")).unwrap();
    assert!(history.contains("exec fleet-one /usr/bin/sudo -n /sbin/shutdown -h now"));
    assert!(history.contains("stop fleet-one --timeout 30"));
    assert!(instances::admit_job(&f.home, 1, Some(true), None, &hello()).is_ok());
    f.set("one", Desired::Deleted);
    f.wait("one", State::Deleted);
}

#[test]
fn failed_launcher_keeps_reservation_until_stop_confirms_termination() {
    let mut f = Fixture::new();
    f.create("one");
    f.controller("one");
    f.wait("one", State::Stopped);
    fs::write(f.home.join("instance-tart/fail-run"), "").unwrap();
    f.set("one", Desired::Running);
    let failed = f.wait("one", State::Failed);
    assert!(failed.reserves_compute());
    assert!(
        failed
            .observed
            .error
            .as_deref()
            .unwrap()
            .contains("maximum supported number")
    );
    f.set("one", Desired::Stopped);
    f.wait("one", State::Stopped);
    assert!(instances::admit_job(&f.home, 1, Some(true), None, &hello()).is_ok());
    f.set("one", Desired::Deleted);
    f.wait("one", State::Deleted);
}

#[test]
fn protocol_requires_upgraded_agent_for_instances() {
    let request = fleet::proto::Request::InstanceCreate {
        id: "one".into(),
        spec: spec(),
    };
    assert_eq!(
        fleet::proto::too_new(&request, fleet::proto::Protocol { major: 3, minor: 2 }),
        Some(("InstanceCreate", 3))
    );
}

#[test]
fn native_jobs_and_paused_gpu_reservations_are_respected() {
    let f = Fixture::new();
    let dir = f.home.join("jobs/native");
    fs::create_dir_all(&dir).unwrap();
    let job: fleet::jobs::Job = serde_json::from_value(
        serde_json::json!({"id":"native","submitter":"owner","submitted_at":fleet::jobs::now(),
        "spec":{"name":"native","argv":["/bin/sleep","60"],"threads":5}}),
    )
    .unwrap();
    fleet::jobs::write_json(&dir.join("job.json"), &job).unwrap();
    instances::write(&dir.join("gpu-allocation.json"), &false).unwrap();
    assert!(
        instances::admit_vm(&f.home, &spec(), &hello()).is_err(),
        "CPU slots conflict"
    );
    instances::write(&dir.join("gpu-allocation.json"), &true).unwrap();
    assert!(
        instances::admit_vm(&f.home, &spec(), &hello()).is_err(),
        "GPU conflict"
    );
    fleet::jobs::write_json(
        &dir.join("supervisor.json"),
        &fleet::jobs::Started {
            supervisor_pid: std::process::id(),
            child_pid: std::process::id(),
            started_at: fleet::jobs::now(),
        },
    )
    .unwrap();
    fs::write(dir.join("paused"), "").unwrap();
    assert!(
        instances::admit_vm(&f.home, &spec(), &hello()).is_ok(),
        "paused jobs release slots by default"
    );
    let mut job = job;
    job.spec.pause_keeps_gpu = true;
    fleet::jobs::write_json(&dir.join("job.json"), &job).unwrap();
    assert!(
        instances::admit_vm(&f.home, &spec(), &hello()).is_err(),
        "pause_keeps_gpu remains authoritative"
    );
}

fn wait_state(home: &std::path::Path, id: &str, state: State) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let i = instances::info(home, id).unwrap();
        if i.observed.state == state && i.observed.revision == i.record.revision {
            return;
        }
        assert!(Instant::now() < deadline, "waiting for {state:?}: {i:?}");
        std::thread::sleep(Duration::from_millis(100));
    }
}
