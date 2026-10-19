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
