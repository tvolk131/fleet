//! A persistent Linux instance on Virtualization.framework, through the worker's catalog and
//! instance controller. It boots the test guest that runs Fleet's agent, so it needs Apple
//! silicon outside a VM and the test guest bundles; it is ignored by default:
//!
//!     tests/vm-guest/build.sh
//!     cargo test --test instances_vz_linux -- --ignored
//!
//! `FLEET_VZ_TEST_IMAGES` overrides the bundle directory (default `target/vm-guest`).
#![cfg(target_os = "macos")]

use std::{
    collections::BTreeMap,
    io::Write,
    path::PathBuf,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

use fleet::{
    driver::{Driver, Exit, GuestOs, InstanceId, RunnerDriver, vz::VzBackend},
    instances::{self, Config, Desired, DriverKind, Gpu, Image, Network, Spec, State},
    worker_data::Hello,
};

fn hello() -> Hello {
    serde_json::from_value(serde_json::json!({
        "agent_version": "test", "hostname": "test", "os": "macOS", "cpu": "test", "cores": 8,
        "memory_bytes": 32u64 << 30, "gpu": null, "tags": [], "sleep_minutes": null,
        "cpu_slots": 8,
    }))
    .unwrap()
}

struct Home {
    path: PathBuf,
    controller: Option<Child>,
}

impl Drop for Home {
    fn drop(&mut self) {
        if let Some(mut c) = self.controller.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
        // A failed assertion must not leave the VM running.
        let _ = Command::new("/usr/bin/pkill")
            .arg("-f")
            .arg(format!("fleet-runner-vz {}", self.path.display()))
            .status();
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

fn wait(home: &std::path::Path, state: State) {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let i = instances::info(home, "linux").unwrap();
        if i.observed.state == state && i.observed.revision == i.record.revision {
            return;
        }
        assert!(Instant::now() < deadline, "waiting for {state:?}: {i:?}");
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn set(home: &std::path::Path, desired: Desired) {
    let revision = instances::info(home, "linux").unwrap().record.revision;
    instances::set_desired(home, "linux", desired, revision, "owner", &hello()).unwrap();
}

#[test]
#[ignore = "boots a real VM; run tests/vm-guest/build.sh, then pass --ignored"]
fn linux_instance_runs_commands_and_stops_through_its_agent() {
    let images = std::env::var_os("FLEET_VZ_TEST_IMAGES")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/vm-guest"));
    let bundle = images.join("agent").canonicalize().unwrap();
    let path = std::env::temp_dir()
        .canonicalize()
        .unwrap()
        .join(format!("fleet-vz-linux-{:016x}", rand::random::<u64>()));
    std::fs::create_dir_all(&path).unwrap();
    let mut home = Home {
        path,
        controller: None,
    };
    let entry = Image {
        digest: fleet::driver::bundle::digest(&bundle).unwrap(),
        guest: Image::declared_guest(&bundle).unwrap(),
        bundle,
    };
    assert_eq!(entry.guest, GuestOs::Linux);
    instances::write(
        &home.path.join("instances.json"),
        &Config {
            images: BTreeMap::from([("linux".into(), entry)]),
            platforms: Default::default(),
            memory_budget_mb: 4096,
            disk_budget_gb: 10,
            min_free_disk_gb: 5,
            driver: DriverKind::Vz,
        },
    )
    .unwrap();
    let spec = Spec {
        image: "linux".into(),
        runtime: None,
        cpus: 1,
        memory_mb: 512,
        disk_gb: 1,
        network: Network::Offline,
        gpu: Gpu::Exclusive,
    };
    instances::create(&home.path, "linux", spec, "owner", &hello()).unwrap();
    home.controller = Some(
        Command::new(env!("CARGO_BIN_EXE_fleet"))
            .arg("instance-controller")
            .arg(&home.path)
            .arg("linux")
            .env("FLEET_HOME", &home.path)
            .stdin(Stdio::null())
            .spawn()
            .unwrap(),
    );
    wait(&home.path, State::Stopped);
    set(&home.path, Desired::Running);
    wait(&home.path, State::Running);

    // The agent may still be starting once the VM runs.
    let deadline = Instant::now() + Duration::from_secs(20);
    let output = loop {
        let mut child = instances::guest_exec(&home.path, "linux")
            .unwrap()
            .args(["/init", "cat"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(b"from a Linux instance")
            .unwrap();
        let output = child.wait_with_output().unwrap();
        if output.status.success() || Instant::now() > deadline {
            break output;
        }
        std::thread::sleep(Duration::from_millis(200));
    };
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(output.stdout, b"from a Linux instance");

    // A graceful stop: the agent powers the guest off well before the controller's 30 s grace.
    let began = Instant::now();
    set(&home.path, Desired::Stopped);
    wait(&home.path, State::Stopped);
    assert!(
        began.elapsed() < Duration::from_secs(15),
        "stop took {:?}",
        began.elapsed()
    );
    let driver = RunnerDriver::new(
        home.path.join("instances/linux/driver"),
        VzBackend {
            runner: env!("CARGO_BIN_EXE_fleet-runner-vz").into(),
        },
    )
    .unwrap();
    let observed = driver.observe(&InstanceId::new("vm").unwrap()).unwrap();
    assert_eq!(observed.last_exit, Some(Exit::PoweredOff), "{observed:?}");

    set(&home.path, Desired::Deleted);
    wait(&home.path, State::Deleted);
}
