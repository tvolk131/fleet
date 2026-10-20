//! What VMs cost on this host through the `vz` driver: start times, disk and memory. Ignored by
//! default; it boots real guests and prints a report:
//!
//!     tests/vm-guest/build.sh
//!     cargo test --test driver_vz_bench -- --ignored --nocapture --test-threads=1
//!
//! The macOS case uses the bundle from `driver_vz_macos` (with Fleet's guest agent installed).
//! Disk use is measured as the change in the volume's free space, since APFS clones share
//! blocks that per-file sizes count again; other writers on the volume add noise. Memory is
//! the resident size of Virtualization.framework's per-VM service processes.
#![cfg(target_os = "macos")]

use std::{
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use fleet::driver::{
    AGENT_PORT, Driver, GuestOs, InstanceId, InstanceSpec, RunnerDriver, StopMode,
    conformance::ECHO_PORT,
    vz::{self, VzBackend},
};

const RUNNER: &str = env!("CARGO_BIN_EXE_fleet-runner-vz");

fn driver(root: &Path) -> RunnerDriver<VzBackend> {
    vz::sign_runner(RUNNER.as_ref()).unwrap();
    RunnerDriver::new(
        root,
        VzBackend {
            runner: RUNNER.into(),
        },
    )
    .unwrap()
}

fn free(path: &Path) -> i64 {
    fleet::instances::storage::free_bytes(path).unwrap() as i64
}

fn gib(bytes: i64) -> String {
    format!("{:.2} GiB", bytes as f64 / f64::from(1 << 30))
}

/// Resident memory of every Virtualization.framework VM service process.
fn vm_memory() -> (usize, u64) {
    let out = std::process::Command::new("/bin/ps")
        .args(["-axo", "rss=,comm="])
        .output()
        .unwrap();
    let rows: Vec<u64> = String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter(|l| l.contains("com.apple.Virtualization.VirtualMachine"))
        .filter_map(|l| l.split_whitespace().next()?.parse().ok())
        .collect();
    (rows.len(), rows.iter().sum::<u64>() << 10)
}

fn spec(id: &str, n: u8, guest: GuestOs, image: &Path, cpus: u32, memory_mb: u32) -> InstanceSpec {
    InstanceSpec {
        id: InstanceId::new(id).unwrap(),
        generation: 1,
        guest,
        image: image.into(),
        cpus,
        memory_mb,
        disk_gb: if guest == GuestOs::Macos { 50 } else { 1 },
        mac: [0x02, 0xbe, 0x0c, 0, 0, n],
        mtu: 1500,
    }
}

/// Retries until something listens on `port` inside the guest, then runs `ready` on it.
fn wait_guest(
    driver: &impl Driver,
    id: &InstanceId,
    port: u32,
    ready: impl Fn(std::os::unix::net::UnixStream) -> bool,
) -> Duration {
    let began = Instant::now();
    loop {
        if let Ok(stream) = driver.connect_guest(id, port)
            && ready(stream)
        {
            return began.elapsed();
        }
        assert!(
            began.elapsed() < Duration::from_secs(300),
            "{id} never answered"
        );
        std::thread::sleep(Duration::from_millis(200));
    }
}

fn cleanup(driver: &impl Driver) {
    for observed in driver.inventory().unwrap_or_default() {
        let _ = driver.delete(&observed.id, observed.generation);
    }
}

#[test]
#[ignore = "boots macOS guests; see the module docs"]
fn macos_guests() {
    let image = std::env::var_os("FLEET_VZ_MACOS_IMAGE")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/vz-macos-image"));
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/vz-bench-macos");
    let _ = std::fs::remove_dir_all(&root);
    let driver = driver(&root);
    let image_size: u64 = std::fs::read_dir(&image)
        .unwrap()
        .map(|e| e.unwrap().metadata().unwrap().len())
        .sum();
    println!("image bundle: {} logical", gib(image_size as i64));

    // Ten instances' disks.
    let specs: Vec<_> = (0..10)
        .map(|n| spec(&format!("mac-{n}"), n, GuestOs::Macos, &image, 4, 8192))
        .collect();
    let before = free(root.parent().unwrap());
    let began = Instant::now();
    for spec in &specs {
        driver.prepare(spec).unwrap();
    }
    println!(
        "prepare 10 instances: {:?} total, disk used {}",
        began.elapsed(),
        gib(before - free(&root))
    );

    // Apple allows two macOS guests at once.
    let booted = free(&root);
    let began = Instant::now();
    let starts: Vec<_> = std::thread::scope(|s| {
        let handles: Vec<_> = specs[..2]
            .iter()
            .map(|spec| {
                let driver = &driver;
                s.spawn(move || {
                    let t = Instant::now();
                    driver.start(spec).unwrap();
                    let running = t.elapsed();
                    let agent = wait_guest(driver, &spec.id, AGENT_PORT, |stream| {
                        fleet_guest::agent::client(
                            stream,
                            &["/usr/bin/true".to_string()],
                            std::io::empty(),
                            std::io::sink(),
                            std::io::sink(),
                        )
                        .is_ok_and(|code| code == 0)
                    });
                    (running, running + agent)
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    for (n, (running, agent)) in starts.iter().enumerate() {
        println!("mac-{n}: running after {running:?}, guest agent answering after {agent:?}");
    }
    println!("both ready after {:?}", began.elapsed());
    std::thread::sleep(Duration::from_secs(30));
    let (processes, memory) = vm_memory();
    println!(
        "2 running macOS guests (8 GiB each), settled 30 s: {processes} VM processes resident {}, disk written since boot {}",
        gib(memory as i64),
        gib(booted - free(&root))
    );

    let state = root.join("mac-0.vzvmsave");
    let began = Instant::now();
    driver.save_state(&specs[0].id, 1, &state).unwrap();
    println!(
        "save one: {:?}, {}",
        began.elapsed(),
        gib(std::fs::metadata(&state).unwrap().len() as i64)
    );
    let began = Instant::now();
    driver.restore_state(&specs[0], &state).unwrap();
    println!("restore one: {:?}", began.elapsed());
    let _ = std::fs::remove_file(&state);

    for spec in &specs[..2] {
        let began = Instant::now();
        driver
            .stop(
                &spec.id,
                1,
                StopMode::Graceful {
                    deadline: Duration::from_secs(90),
                },
            )
            .unwrap();
        println!(
            "{}: graceful stop {:?}, {:?}",
            spec.id,
            began.elapsed(),
            driver.observe(&spec.id).unwrap().last_exit
        );
    }
    println!(
        "10 instances, 2 booted once, all stopped: disk used {}",
        gib(before - free(&root))
    );
    cleanup(&driver);
    println!("after delete: disk used {}", gib(before - free(&root)));
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
#[ignore = "boots Linux guests; see the module docs"]
fn ten_linux_guests() {
    let image = std::env::var_os("FLEET_VZ_TEST_IMAGES")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/vm-guest"))
        .join("standard");
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/vz-bench-linux");
    let _ = std::fs::remove_dir_all(&root);
    let driver = driver(&root);
    let specs: Vec<_> = (0..10)
        .map(|n| spec(&format!("linux-{n}"), n, GuestOs::Linux, &image, 1, 512))
        .collect();
    let (_, baseline) = vm_memory();
    let before = free(root.parent().unwrap());
    let began = Instant::now();
    let starts: Vec<_> = std::thread::scope(|s| {
        let handles: Vec<_> = specs
            .iter()
            .map(|spec| {
                let driver = &driver;
                s.spawn(move || {
                    let t = Instant::now();
                    driver.start(spec).unwrap();
                    let running = t.elapsed();
                    let ready = wait_guest(driver, &spec.id, ECHO_PORT, |mut stream| {
                        use std::io::{Read, Write};
                        let mut buf = [0u8; 4];
                        stream.write_all(b"ping").is_ok() && stream.read_exact(&mut buf).is_ok()
                    });
                    (running, running + ready)
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let all = began.elapsed();
    let max = |f: fn(&(Duration, Duration)) -> Duration| starts.iter().map(f).max().unwrap();
    let min = |f: fn(&(Duration, Duration)) -> Duration| starts.iter().map(f).min().unwrap();
    println!(
        "10 Linux guests (1 vCPU, 512 MiB) started together: running {:?}..{:?}, guest answering {:?}..{:?}, all within {all:?}",
        min(|s| s.0),
        max(|s| s.0),
        min(|s| s.1),
        max(|s| s.1)
    );
    std::thread::sleep(Duration::from_secs(5));
    let (processes, memory) = vm_memory();
    println!(
        "{processes} VM processes resident {} ({} more than before), disk used {}",
        gib(memory as i64),
        gib(memory as i64 - baseline as i64),
        gib(before - free(&root))
    );
    let began = Instant::now();
    for spec in &specs {
        driver.stop(&spec.id, 1, StopMode::Force).unwrap();
    }
    println!("force-stopped all 10 in {:?}", began.elapsed());
    cleanup(&driver);
    let _ = std::fs::remove_dir_all(&root);
}
