use fleet::{
    driver::{Driver, Exit, InstanceId, Phase, RunnerDriver, StopMode, fake::FakeBackend},
    instances::{self, Config, Desired, DriverKind, Gpu, Image, Network, Spec, State},
    worker_data::Hello,
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
        instances::write(
            &home.join("instances.json"),
            &Config {
                platforms: Default::default(),
                images: BTreeMap::from([("macos".into(), Image::fake(&home).unwrap())]),
                memory_budget_mb: 8192,
                disk_budget_gb: 200,
                min_free_disk_gb: 5,
                driver: DriverKind::Fake,
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
    /// Switches the fake guests' failure injections (see `fleet::driver::fake::FakeImage`).
    fn fault(&self, name: &str) {
        fs::write(self.home.join("vm-faults").join(name), "").unwrap();
    }
    /// What the instance's fake guest has done: `boot`, `power-button`, `spawn <argv>`, ...
    fn history(&self, id: &str) -> Vec<String> {
        let dir = self.vm_dir(id);
        let prefixes = [&dir, &dir.canonicalize().unwrap()].map(|d| format!("{} ", d.display()));
        fs::read_to_string(self.home.join("vm-faults/history"))
            .unwrap_or_default()
            .lines()
            .filter_map(|l| prefixes.iter().find_map(|p| l.strip_prefix(p.as_str())))
            .map(str::to_owned)
            .collect()
    }
    fn count(&self, id: &str, event: &str) -> usize {
        self.history(id).iter().filter(|e| *e == event).count()
    }
    fn vm_dir(&self, id: &str) -> PathBuf {
        self.home.join("instances").join(id).join("driver/vm")
    }
    /// The instance's VM as its driver sees it, without going through the controller.
    fn driver(&self, id: &str) -> RunnerDriver<FakeBackend> {
        RunnerDriver::new(
            self.home.join("instances").join(id).join("driver"),
            FakeBackend {
                fleet: env!("CARGO_BIN_EXE_fleet").into(),
            },
        )
        .unwrap()
    }
    fn vm(&self, id: &str) -> fleet::driver::Observed {
        self.driver(id).observe(&vm()).unwrap()
    }
    /// Ends the VM behind Fleet's back, as a host restart or another tool would.
    fn kill_vm(&self, id: &str) {
        self.try_kill_vm(id).unwrap();
    }
    fn try_kill_vm(&self, id: &str) -> fleet::driver::Result<()> {
        let driver = self.driver(id);
        let generation = driver.observe(&vm())?.generation;
        driver.stop(&vm(), generation, StopMode::Force)
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
                fs::read_to_string(self.vm_dir(id).join("runner.log"))
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
        if let Ok(entries) = fs::read_dir(self.home.join("instances")) {
            for entry in entries.flatten() {
                if let Some(id) = entry.file_name().to_str()
                    && self.vm_dir(id).exists()
                {
                    let _ = self.try_kill_vm(id);
                }
            }
        }
        let _ = fs::remove_dir_all(&self.home);
    }
}
fn vm() -> InstanceId {
    InstanceId::new("vm").unwrap()
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
fn failed_configuration_can_be_stopped_and_deleted_without_reprovisioning() {
    let mut f = Fixture::new();
    f.fault("fail-prepare");
    f.create("broken");
    f.controller("broken");
    f.wait("broken", State::Failed);
    assert!(f.vm_dir("broken").join("disk.img").is_file());
    assert!(!f.home.join("instances/broken/provisioned.json").exists());
    let history = f.history("broken");

    f.set("broken", Desired::Stopped);
    f.wait("broken", State::Stopped);
    f.set("broken", Desired::Deleted);
    f.wait("broken", State::Deleted);
    assert!(!f.home.join("instances/broken/driver").exists());
    assert_eq!(history, ["prepare"]);
    assert!(
        !fs::read_to_string(f.home.join("vm-faults/history"))
            .unwrap()
            .contains(" boot"),
        "a failed configuration never boots"
    );
}

#[test]
fn gpu_modes_default_to_exclusive_and_preserve_explicit_shared() {
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
        f.kill_vm(id);
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
    for id in ["one", "two"] {
        assert_eq!(f.count(id, "boot"), 2, "{id}");
    }
    // Restart admission uses the current budget, including the other VM's claim.
    f.controllers[0].kill().unwrap();
    f.controllers[0].wait().unwrap();
    f.kill_vm("one");
    let mut config = Config::load(&f.home).unwrap();
    config.memory_budget_mb = 6144;
    instances::write(&f.home.join("instances.json"), &config).unwrap();
    f.controller("one");
    let failed = f.wait("one", State::Failed);
    assert!(failed.observed.error.unwrap().contains("memory"));
    assert_eq!(f.vm("one").phase, Phase::Stopped);
    assert_eq!(f.count("one", "boot"), 2);
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
fn public_internet_vms_are_refused_until_fleet_has_a_network_agent() {
    let f = Fixture::new();
    let mut public = spec();
    public.network = Network::PublicInternet;
    let error = instances::create(&f.home, "public", public, "owner", &hello()).unwrap_err();
    assert!(
        error.to_string().contains("host network agent"),
        "{error:#}"
    );
    assert!(instances::info(&f.home, "public").is_err());
}

#[test]
fn lifecycle_survives_controller_loss_preserves_disk_and_rejects_stale_commands() {
    let mut f = Fixture::new();
    f.create("one");
    f.controller("one");
    f.wait("one", State::Stopped);
    f.set("one", Desired::Running);
    f.wait("one", State::Running);
    let disk = f.vm_dir("one").join("disk.img");
    fs::write(&disk, "checkpoint").unwrap();
    f.controllers[0].kill().unwrap();
    f.controllers[0].wait().unwrap();
    f.controller("one");
    std::thread::sleep(Duration::from_secs(2));
    assert_eq!(
        f.count("one", "boot"),
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
    f.fault("fail-after-start");
    f.fault("gate-failure");
    f.set("one", Desired::Running);
    f.wait("one", State::Running);
    f.fault("release-failure");
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
    assert_eq!(f.count("one", "boot"), 1);
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
    f.fault("refuse-stop");
    f.set("one", Desired::Stopped);
    f.wait("one", State::Stopping);
    assert!(instances::admit_job(&f.home, 1, Some(true), None, &hello()).is_err());
    f.wait("one", State::Stopped);
    assert_eq!(f.count("one", "power-button"), 1);
    assert_eq!(f.vm("one").last_exit, Some(Exit::Forced));
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
    f.fault("fail-start");
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
