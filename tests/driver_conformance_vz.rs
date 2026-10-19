//! The driver conformance suite against Virtualization.framework. It boots real Linux guests, so
//! it needs Apple silicon outside a VM and the test guest bundles; it is ignored by default:
//!
//!     tests/vm-guest/build.sh
//!     cargo test --test driver_conformance_vz -- --ignored
//!
//! `FLEET_VZ_TEST_IMAGES` overrides the bundle directory (default `target/vm-guest`).
#![cfg(target_os = "macos")]

use std::{path::PathBuf, sync::Once};

use fleet::driver::{
    Driver, RunnerDriver,
    conformance::{Harness, TestImage},
    vz::VzBackend,
};

const RUNNER: &str = env!("CARGO_BIN_EXE_fleet-runner-vz");

struct Vz {
    root: PathBuf,
    images: PathBuf,
}

impl Vz {
    fn new() -> Self {
        static SIGNED: Once = Once::new();
        SIGNED.call_once(|| fleet::driver::vz::sign_runner(RUNNER.as_ref()).unwrap());
        let images = std::env::var_os("FLEET_VZ_TEST_IMAGES")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/vm-guest"));
        assert!(
            images.join("standard/image.json").is_file(),
            "no test guest in {}; run tests/vm-guest/build.sh",
            images.display()
        );
        let root = std::env::temp_dir().join(format!("fleet-vz-{:016x}", rand::random::<u64>()));
        std::fs::create_dir_all(root.join("scratch")).unwrap();
        Self { root, images }
    }
}

impl Harness for Vz {
    type Driver = RunnerDriver<VzBackend>;
    fn driver(&self) -> Self::Driver {
        let backend = VzBackend {
            runner: RUNNER.into(),
        };
        RunnerDriver::new(self.root.join("instances"), backend).unwrap()
    }
    fn image(&self, image: TestImage) -> PathBuf {
        self.images.join(match image {
            TestImage::Standard => "standard",
            TestImage::IgnoresStop => "ignores-stop",
        })
    }
    fn scratch(&self) -> PathBuf {
        self.root.join("scratch")
    }
}

/// A failed case must not leave VMs behind.
impl Drop for Vz {
    fn drop(&mut self) {
        let driver = self.driver();
        for observed in driver.inventory().unwrap_or_default() {
            let _ = driver.delete(&observed.id, observed.generation);
        }
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fleet::driver_conformance!(
    Vz::new(),
    #[ignore = "boots real VMs; run tests/vm-guest/build.sh, then pass --ignored"]
);

/// Fleet's own guest agent, the replacement for `tart exec`, running inside a real VM.
#[test]
#[ignore = "boots real VMs; run tests/vm-guest/build.sh, then pass --ignored"]
fn guest_agent_runs_commands_over_vsock() {
    use fleet::driver::{AGENT_PORT, GuestOs, InstanceId, InstanceSpec};
    use std::time::{Duration, Instant};

    let h = Vz::new();
    let driver = h.driver();
    let spec = InstanceSpec {
        id: InstanceId::new("agent").unwrap(),
        generation: 1,
        guest: GuestOs::Linux,
        image: h.images.join("agent"),
        cpus: 1,
        memory_mb: 512,
        disk_gb: 1,
        mac: [0x02, 0xf1, 0xee, 0x00, 0x00, 0x07],
        mtu: 1500,
    };
    driver.start(&spec).unwrap();
    let connect = || {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            match driver.connect_guest(&spec.id, AGENT_PORT) {
                Ok(stream) => return stream,
                Err(e) if Instant::now() > deadline => panic!("no agent: {e}"),
                Err(_) => std::thread::sleep(Duration::from_millis(50)),
            }
        }
    };
    let run = |argv: &[&str], input: &'static [u8]| {
        let argv: Vec<String> = argv.iter().map(|s| s.to_string()).collect();
        let (mut out, mut err) = (Vec::new(), Vec::new());
        let code = fleet_guest::agent::client(connect(), &argv, input, &mut out, &mut err).unwrap();
        (code, out, err)
    };
    let (code, out, _) = run(&["/init", "cat"], b"through vsock and back");
    assert_eq!((code, out.as_slice()), (0, &b"through vsock and back"[..]));
    assert_eq!(run(&["/init", "exit", "42"], b"").0, 42);
    let (code, _, err) = run(&["/missing"], b"");
    assert_eq!(code, 127, "{}", String::from_utf8_lossy(&err));

    // `fleet guest-exec`, the drop-in for `tart exec -i`, over the same agent.
    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_fleet"))
        .arg("guest-exec")
        .arg(h.root.join("instances/agent").canonicalize().unwrap())
        .args(["--", "/init", "cat"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    use std::io::Write;
    child
        .stdin
        .take()
        .unwrap()
        .write_all(b"via fleet guest-exec")
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());
    assert_eq!(output.stdout, b"via fleet guest-exec");
    let status = std::process::Command::new(env!("CARGO_BIN_EXE_fleet"))
        .arg("guest-exec")
        .arg(h.root.join("instances/agent").canonicalize().unwrap())
        .args(["--", "/init", "exit", "3"])
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(3));
    driver.delete(&spec.id, 1).unwrap();
}
