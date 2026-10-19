//! A macOS guest through the `vz` driver. Ignored by default: it needs a macOS image bundle,
//! several GiB of memory and about a minute.
//!
//!     FLEET_VZ_MACOS_TART=<Tart VM directory> cargo test --test driver_vz_macos -- --ignored --nocapture
//!
//! The Tart VM (`config.json`, `disk.img`, `nvram.bin`) is imported once into
//! `target/vz-macos-image` as APFS clones. `FLEET_VZ_MACOS_IMAGE` names a bundle directly.
#![cfg(target_os = "macos")]

use std::{
    os::fd::{AsRawFd, OwnedFd},
    path::PathBuf,
    time::{Duration, Instant},
};

use fleet::driver::{
    Driver, GuestOs, InstanceId, InstanceSpec, Phase, RunnerDriver, StopMode,
    vz::{self, VzBackend},
};

const RUNNER: &str = env!("CARGO_BIN_EXE_fleet-runner-vz");
const MAC: [u8; 6] = [0x02, 0xf1, 0xee, 0x00, 0x00, 0x42];

fn image() -> PathBuf {
    if let Some(bundle) = std::env::var_os("FLEET_VZ_MACOS_IMAGE") {
        return bundle.into();
    }
    let bundle = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/vz-macos-image");
    if !bundle.exists() {
        let tart = std::env::var_os("FLEET_VZ_MACOS_TART")
            .expect("set FLEET_VZ_MACOS_TART to a Tart VM directory, or FLEET_VZ_MACOS_IMAGE");
        vz::import_tart(tart.as_ref(), &bundle).unwrap();
    }
    bundle
}

/// The first frame the guest itself sends, and how long it took to arrive.
fn first_guest_frame(card: &OwnedFd, within: Duration) -> Option<(Duration, u16, usize)> {
    let began = Instant::now();
    let mut buf = vec![0u8; 65536];
    while began.elapsed() < within {
        let mut poll = libc::pollfd {
            fd: card.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        if unsafe { libc::poll(&mut poll, 1, 200) } <= 0 {
            continue;
        }
        let n = unsafe { libc::recv(card.as_raw_fd(), buf.as_mut_ptr().cast(), buf.len(), 0) };
        if n >= 14 && buf[6..12] == MAC {
            return Some((
                began.elapsed(),
                u16::from_be_bytes([buf[12], buf[13]]),
                n as usize,
            ));
        }
    }
    None
}

#[test]
#[ignore = "boots macOS; set FLEET_VZ_MACOS_TART or FLEET_VZ_MACOS_IMAGE and pass --ignored"]
fn macos_guest_boots_networks_saves_and_stops() {
    vz::sign_runner(RUNNER.as_ref()).unwrap();
    let image = image();
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/vz-macos-instances");
    let driver = RunnerDriver::new(
        &root,
        VzBackend {
            runner: RUNNER.into(),
        },
    )
    .unwrap();
    let spec = InstanceSpec {
        id: InstanceId::new("macos-smoke").unwrap(),
        generation: 1,
        guest: GuestOs::Macos,
        image,
        cpus: 4,
        memory_mb: 8192,
        disk_gb: 50,
        mac: MAC,
        mtu: 1500,
    };
    let _ = driver.delete(&spec.id, 1);

    let began = Instant::now();
    driver.start(&spec).unwrap();
    println!("running after {:?}", began.elapsed());
    let card = driver.network_card(&spec.id).unwrap();
    let (after, ethertype, len) = first_guest_frame(&card, Duration::from_secs(180))
        .expect("the guest sent nothing on its network card");
    println!("first guest frame after {after:?}: ethertype {ethertype:#06x}, {len} bytes");
    // Let it settle into its login session before saving.
    std::thread::sleep(Duration::from_secs(20));

    let state = root.join("macos-smoke.vzvmsave");
    let began = Instant::now();
    driver.save_state(&spec.id, 1, &state).unwrap();
    println!(
        "saved in {:?}: {} MiB",
        began.elapsed(),
        std::fs::metadata(&state).unwrap().len() >> 20
    );
    let began = Instant::now();
    driver.restore_state(&spec, &state).unwrap();
    println!("restored in {:?}", began.elapsed());
    let card = driver.network_card(&spec.id).unwrap();
    let (after, ethertype, _) = first_guest_frame(&card, Duration::from_secs(120))
        .expect("the restored guest sent nothing on its new network card");
    println!("restored guest's first frame after {after:?}: ethertype {ethertype:#06x}");
    let _ = std::fs::remove_file(&state);

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
    let observed = driver.observe(&spec.id).unwrap();
    println!(
        "graceful stop took {:?}: {:?}",
        began.elapsed(),
        observed.last_exit
    );
    assert_eq!(observed.phase, Phase::Stopped);
    driver.delete(&spec.id, 1).unwrap();
}
