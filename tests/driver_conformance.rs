//! The driver conformance suite against the fake backend, which needs no hypervisor.

use std::path::PathBuf;

use fleet::driver::{
    Driver, RunnerDriver,
    conformance::{Harness, TestImage},
    fake::{FakeBackend, FakeImage},
};

struct Fake {
    root: PathBuf,
}

impl Fake {
    fn new() -> Self {
        let root =
            std::env::temp_dir().join(format!("fleet-driver-{:016x}", rand::random::<u64>()));
        FakeImage::default()
            .write(&root.join("images/standard"))
            .unwrap();
        FakeImage {
            ignore_graceful_stop: true,
            ..Default::default()
        }
        .write(&root.join("images/ignores-stop"))
        .unwrap();
        std::fs::create_dir_all(root.join("scratch")).unwrap();
        Self { root }
    }
}

impl Harness for Fake {
    type Driver = RunnerDriver<FakeBackend>;
    fn driver(&self) -> Self::Driver {
        let backend = FakeBackend {
            fleet: env!("CARGO_BIN_EXE_fleet").into(),
        };
        RunnerDriver::new(self.root.join("instances"), backend).unwrap()
    }
    fn image(&self, image: TestImage) -> PathBuf {
        self.root.join(match image {
            TestImage::Standard => "images/standard",
            TestImage::IgnoresStop => "images/ignores-stop",
        })
    }
    fn scratch(&self) -> PathBuf {
        self.root.join("scratch")
    }
}

/// A failed case must not leave runner processes behind.
impl Drop for Fake {
    fn drop(&mut self) {
        let driver = self.driver();
        for observed in driver.inventory().unwrap_or_default() {
            let _ = driver.delete(&observed.id, observed.generation);
        }
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fleet::driver_conformance!(Fake::new());
