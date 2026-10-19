//! The executable specification of [`Driver`]. Every backend runs these cases through
//! [`driver_conformance!`](crate::driver_conformance) with a [`Harness`] for its host.
//!
//! Test images must behave as the **test guest**:
//! - guest port [`ECHO_PORT`] serves one request per connection: `poweroff` powers the guest
//!   off; anything else is answered `echo#N: <request>`, where N counts answers since first boot
//!   and survives save/restore;
//! - on its network card it sends a [`TEST_ETHERTYPE`] frame at least once a second, and
//!   answers each such frame with one whose payload is `echo: ` followed by the original;
//! - [`TestImage::Standard`] powers off on a graceful stop request; [`TestImage::IgnoresStop`]
//!   ignores it.

use std::{
    io::{Read, Write},
    os::fd::{AsRawFd, OwnedFd},
    path::PathBuf,
    time::{Duration, Instant},
};

use super::{Driver, Error, Exit, GuestOs, InstanceId, InstanceSpec, Observed, Phase, StopMode};

pub const ECHO_PORT: u32 = 1234;
pub const TEST_ETHERTYPE: u16 = 0x88b5;
const HOST_MAC: [u8; 6] = [0x02, 0xf1, 0xee, 0x00, 0x00, 0xfe];
const PATIENCE: Duration = Duration::from_secs(20);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TestImage {
    Standard,
    IgnoresStop,
}

/// One fresh, isolated environment per case.
pub trait Harness {
    type Driver: Driver;
    /// A new driver over the same state, standing in for a restarted worker.
    fn driver(&self) -> Self::Driver;
    fn image(&self, image: TestImage) -> PathBuf;
    fn guest(&self) -> GuestOs {
        GuestOs::Linux
    }
    /// A directory the case may write save files into.
    fn scratch(&self) -> PathBuf;
}

/// Generates one `#[test]` per conformance case. `$harness` is evaluated once per case.
#[macro_export]
macro_rules! driver_conformance {
    ($harness:expr) => {
        $crate::driver_conformance!(@cases $harness;
            capabilities_are_consistent,
            unknown_instance_is_absent,
            prepare_is_idempotent,
            same_generation_different_spec_is_rejected,
            stale_generation_is_rejected,
            start_is_idempotent,
            guest_channel_round_trip,
            guest_channel_unknown_port_fails_fast,
            network_card_carries_frames_both_ways,
            network_card_survives_borrower_closing,
            runners_survive_driver_restart,
            inventory_includes_every_instance,
            graceful_stop_lets_guest_power_off,
            graceful_stop_escalates_after_deadline,
            force_stop,
            guest_power_off_is_stopped_not_lost,
            runner_death_is_lost_and_recoverable,
            delete_is_complete_and_idempotent,
            save_and_restore_preserve_guest_state,
            unsupported_save_is_reported
        );
    };
    (@cases $harness:expr; $($case:ident),* $(,)?) => {
        $(
            #[test]
            fn $case() {
                $crate::driver::conformance::$case(&$harness);
            }
        )*
    };
}

pub fn frame(to: &[u8], from: &[u8], payload: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(14 + payload.len());
    frame.extend_from_slice(to);
    frame.extend_from_slice(from);
    frame.extend_from_slice(&TEST_ETHERTYPE.to_be_bytes());
    frame.extend_from_slice(payload);
    frame
}

fn id(name: &str) -> InstanceId {
    InstanceId::new(name).unwrap()
}

fn spec(h: &impl Harness, name: &str, generation: u64, image: TestImage) -> InstanceSpec {
    InstanceSpec {
        id: id(name),
        generation,
        guest: h.guest(),
        image: h.image(image),
        cpus: 1,
        memory_mb: 512,
        disk_gb: 1,
        mac: [0x02, 0xf1, 0xee, 0x00, 0x00, 0x01],
        mtu: 1500,
    }
}

fn wait_for(driver: &impl Driver, id: &InstanceId, phase: Phase) -> Observed {
    let deadline = Instant::now() + PATIENCE;
    loop {
        let observed = driver.observe(id).unwrap();
        if observed.phase == phase {
            return observed;
        }
        assert!(
            Instant::now() < deadline,
            "{id} stayed {:?}, expected {phase:?}: {observed:?}",
            observed.phase
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn ask(driver: &impl Driver, id: &InstanceId, request: &str) -> String {
    let mut stream = driver.connect_guest(id, ECHO_PORT).unwrap();
    stream.set_read_timeout(Some(PATIENCE)).unwrap();
    stream.write_all(request.as_bytes()).unwrap();
    let mut reply = String::new();
    let mut buf = [0u8; 4096];
    let n = stream.read(&mut buf).unwrap();
    reply.push_str(&String::from_utf8_lossy(&buf[..n]));
    reply
}

/// The payload of the next test frame from the guest that satisfies `want`.
fn next_frame(card: &OwnedFd, want: impl Fn(&str) -> bool) -> String {
    let deadline = Instant::now() + PATIENCE;
    let mut buf = vec![0u8; 65536];
    while Instant::now() < deadline {
        let mut poll = libc::pollfd {
            fd: card.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        if unsafe { libc::poll(&mut poll, 1, 100) } <= 0 {
            continue;
        }
        let n = unsafe { libc::recv(card.as_raw_fd(), buf.as_mut_ptr().cast(), buf.len(), 0) };
        if n > 14 && buf[12..14] == TEST_ETHERTYPE.to_be_bytes() {
            let payload = String::from_utf8_lossy(&buf[14..n as usize]).into_owned();
            if want(&payload) {
                return payload;
            }
        }
    }
    panic!("no matching frame from the guest within {PATIENCE:?}");
}

fn send_frame(card: &OwnedFd, payload: &str) {
    let frame = frame(&[0xff; 6], &HOST_MAC, payload.as_bytes());
    let n = unsafe { libc::send(card.as_raw_fd(), frame.as_ptr().cast(), frame.len(), 0) };
    assert_eq!(
        n,
        frame.len() as isize,
        "{}",
        std::io::Error::last_os_error()
    );
}

fn started(h: &impl Harness, name: &str, image: TestImage) -> (impl Driver, InstanceSpec) {
    let driver = h.driver();
    let spec = spec(h, name, 1, image);
    driver.start(&spec).unwrap();
    assert_eq!(driver.observe(&spec.id).unwrap().phase, Phase::Running);
    (driver, spec)
}

pub fn capabilities_are_consistent(h: &impl Harness) {
    let capabilities = h.driver().capabilities();
    assert!(!capabilities.driver.is_empty());
    assert!(capabilities.guests.contains(&h.guest()));
    assert_eq!(
        capabilities.guests.contains(&GuestOs::Macos),
        capabilities.max_macos_guests > 0
    );
}

pub fn unknown_instance_is_absent(h: &impl Harness) {
    let driver = h.driver();
    let observed = driver.observe(&id("nobody")).unwrap();
    assert_eq!((observed.phase, observed.generation), (Phase::Absent, 0));
    assert!(matches!(
        driver.connect_guest(&id("nobody"), ECHO_PORT),
        Err(Error::NotFound(_))
    ));
}

pub fn prepare_is_idempotent(h: &impl Harness) {
    let driver = h.driver();
    let spec = spec(h, "prepared", 3, TestImage::Standard);
    driver.prepare(&spec).unwrap();
    driver.prepare(&spec).unwrap();
    let observed = driver.observe(&spec.id).unwrap();
    assert_eq!((observed.phase, observed.generation), (Phase::Stopped, 3));
    assert_eq!(observed.last_exit, None);
}

pub fn same_generation_different_spec_is_rejected(h: &impl Harness) {
    let driver = h.driver();
    let spec = spec(h, "changed", 1, TestImage::Standard);
    driver.prepare(&spec).unwrap();
    let changed = InstanceSpec {
        cpus: 2,
        ..spec.clone()
    };
    assert!(matches!(driver.prepare(&changed), Err(Error::Invalid(_))));
    driver
        .prepare(&InstanceSpec {
            generation: 2,
            ..changed
        })
        .unwrap();
}

pub fn stale_generation_is_rejected(h: &impl Harness) {
    let driver = h.driver();
    let newer = spec(h, "fenced", 5, TestImage::Standard);
    driver.prepare(&newer).unwrap();
    let older = spec(h, "fenced", 4, TestImage::Standard);
    let stale = |r: super::Result<()>| {
        matches!(
            r,
            Err(Error::Stale {
                recorded: 5,
                requested: 4
            })
        )
    };
    assert!(stale(driver.prepare(&older)));
    assert!(stale(driver.start(&older)));
    assert!(stale(driver.stop(&older.id, 4, StopMode::Force)));
    assert!(stale(driver.delete(&older.id, 4)));
    assert_eq!(driver.observe(&older.id).unwrap().phase, Phase::Stopped);
}

pub fn start_is_idempotent(h: &impl Harness) {
    let (driver, spec) = started(h, "twice", TestImage::Standard);
    let first = driver.observe(&spec.id).unwrap().runner_pid;
    assert!(first.is_some());
    driver.start(&spec).unwrap();
    assert_eq!(driver.observe(&spec.id).unwrap().runner_pid, first);
    driver.delete(&spec.id, 1).unwrap();
}

pub fn guest_channel_round_trip(h: &impl Harness) {
    let (driver, spec) = started(h, "echo", TestImage::Standard);
    assert_eq!(ask(&driver, &spec.id, "hello"), "echo#1: hello");
    assert_eq!(ask(&driver, &spec.id, "again"), "echo#2: again");
    driver.delete(&spec.id, 1).unwrap();
}

pub fn guest_channel_unknown_port_fails_fast(h: &impl Harness) {
    let (driver, spec) = started(h, "closed-port", TestImage::Standard);
    let began = Instant::now();
    assert!(driver.connect_guest(&spec.id, 9999).is_err());
    assert!(
        began.elapsed() < Duration::from_secs(5),
        "took {:?}",
        began.elapsed()
    );
    driver.delete(&spec.id, 1).unwrap();
}

pub fn network_card_carries_frames_both_ways(h: &impl Harness) {
    let (driver, spec) = started(h, "frames", TestImage::Standard);
    let card = driver.network_card(&spec.id).unwrap();
    next_frame(&card, |_| true);
    send_frame(&card, "ping 1");
    next_frame(&card, |p| p == "echo: ping 1");
    driver.delete(&spec.id, 1).unwrap();
}

pub fn network_card_survives_borrower_closing(h: &impl Harness) {
    let (driver, spec) = started(h, "borrowed", TestImage::Standard);
    drop(driver.network_card(&spec.id).unwrap());
    let card = driver.network_card(&spec.id).unwrap();
    send_frame(&card, "after reborrow");
    next_frame(&card, |p| p == "echo: after reborrow");
    driver.delete(&spec.id, 1).unwrap();
}

pub fn runners_survive_driver_restart(h: &impl Harness) {
    let (first, spec) = started(h, "adopted", TestImage::Standard);
    let pid = first.observe(&spec.id).unwrap().runner_pid;
    assert_eq!(ask(&first, &spec.id, "before"), "echo#1: before");
    drop(first);
    let second = h.driver();
    let observed = second.observe(&spec.id).unwrap();
    assert_eq!((observed.phase, observed.runner_pid), (Phase::Running, pid));
    assert_eq!(ask(&second, &spec.id, "after"), "echo#2: after");
    second.start(&spec).unwrap();
    assert_eq!(second.observe(&spec.id).unwrap().runner_pid, pid);
    second.delete(&spec.id, 1).unwrap();
}

pub fn inventory_includes_every_instance(h: &impl Harness) {
    let driver = h.driver();
    driver
        .prepare(&spec(h, "inv-a", 1, TestImage::Standard))
        .unwrap();
    driver
        .start(&spec(h, "inv-b", 1, TestImage::Standard))
        .unwrap();
    // A different driver value over the same state has never been told about either.
    let all = h.driver().inventory().unwrap();
    let phases: Vec<_> = all.iter().map(|o| (o.id.as_str(), o.phase)).collect();
    assert_eq!(
        phases,
        [("inv-a", Phase::Stopped), ("inv-b", Phase::Running)]
    );
    driver.delete(&id("inv-b"), 1).unwrap();
}

pub fn graceful_stop_lets_guest_power_off(h: &impl Harness) {
    let (driver, spec) = started(h, "graceful", TestImage::Standard);
    let began = Instant::now();
    driver
        .stop(&spec.id, 1, StopMode::Graceful { deadline: PATIENCE })
        .unwrap();
    assert!(
        began.elapsed() < PATIENCE,
        "the guest was forced, not stopped"
    );
    let observed = driver.observe(&spec.id).unwrap();
    assert_eq!(observed.phase, Phase::Stopped);
    assert_eq!(observed.last_exit, Some(Exit::PoweredOff));
}

pub fn graceful_stop_escalates_after_deadline(h: &impl Harness) {
    let (driver, spec) = started(h, "stubborn", TestImage::IgnoresStop);
    let deadline = Duration::from_millis(500);
    let began = Instant::now();
    driver
        .stop(&spec.id, 1, StopMode::Graceful { deadline })
        .unwrap();
    assert!(began.elapsed() >= deadline);
    let observed = driver.observe(&spec.id).unwrap();
    assert_eq!(
        (observed.phase, observed.last_exit),
        (Phase::Stopped, Some(Exit::Forced))
    );
}

pub fn force_stop(h: &impl Harness) {
    let (driver, spec) = started(h, "forced", TestImage::Standard);
    driver.stop(&spec.id, 1, StopMode::Force).unwrap();
    let observed = driver.observe(&spec.id).unwrap();
    assert_eq!(
        (observed.phase, observed.last_exit),
        (Phase::Stopped, Some(Exit::Forced))
    );
    // Stopping a stopped instance is a no-op, and it can start again.
    driver.stop(&spec.id, 1, StopMode::Force).unwrap();
    driver.start(&spec).unwrap();
    assert_eq!(driver.observe(&spec.id).unwrap().phase, Phase::Running);
    driver.delete(&spec.id, 1).unwrap();
}

pub fn guest_power_off_is_stopped_not_lost(h: &impl Harness) {
    let (driver, spec) = started(h, "self-stop", TestImage::Standard);
    assert_eq!(ask(&driver, &spec.id, "poweroff"), "powering off");
    let observed = wait_for(&driver, &spec.id, Phase::Stopped);
    assert_eq!(observed.last_exit, Some(Exit::PoweredOff));
}

pub fn runner_death_is_lost_and_recoverable(h: &impl Harness) {
    let (driver, spec) = started(h, "crashed", TestImage::Standard);
    let pid = driver.observe(&spec.id).unwrap().runner_pid.unwrap();
    unsafe { libc::kill(pid as libc::pid_t, libc::SIGKILL) };
    let observed = wait_for(&driver, &spec.id, Phase::Lost);
    assert_eq!(observed.last_exit, None);
    driver.start(&spec).unwrap();
    let observed = driver.observe(&spec.id).unwrap();
    assert_eq!(observed.phase, Phase::Running);
    assert_ne!(observed.runner_pid, Some(pid));
    // A new boot: the guest's memory did not survive.
    assert_eq!(ask(&driver, &spec.id, "fresh"), "echo#1: fresh");
    driver.delete(&spec.id, 1).unwrap();
}

pub fn delete_is_complete_and_idempotent(h: &impl Harness) {
    let (driver, spec) = started(h, "deleted", TestImage::Standard);
    let pid = driver.observe(&spec.id).unwrap().runner_pid.unwrap();
    driver.delete(&spec.id, 1).unwrap();
    assert_eq!(driver.observe(&spec.id).unwrap().phase, Phase::Absent);
    assert!(driver.inventory().unwrap().iter().all(|o| o.id != spec.id));
    assert_ne!(
        unsafe { libc::kill(pid as libc::pid_t, 0) },
        0,
        "runner {pid} survived"
    );
    driver.delete(&spec.id, 1).unwrap();
    // Deleting forgets the generation, so the id can be reused from the start.
    driver.start(&spec).unwrap();
    driver.delete(&spec.id, 1).unwrap();
}

pub fn save_and_restore_preserve_guest_state(h: &impl Harness) {
    let driver = h.driver();
    if !driver.capabilities().save_restore {
        return;
    }
    let spec = spec(h, "saved", 1, TestImage::Standard);
    driver.start(&spec).unwrap();
    assert_eq!(ask(&driver, &spec.id, "one"), "echo#1: one");
    let path = h.scratch().join("saved.state");
    driver.save_state(&spec.id, 1, &path).unwrap();
    let observed = driver.observe(&spec.id).unwrap();
    assert_eq!(observed.phase, Phase::Stopped);
    assert!(matches!(observed.last_exit, Some(Exit::Saved { .. })));

    driver.restore_state(&spec, &path).unwrap();
    assert_eq!(driver.observe(&spec.id).unwrap().phase, Phase::Running);
    assert_eq!(ask(&driver, &spec.id, "two"), "echo#2: two");
    // The restored guest is attached to a fresh network card.
    let card = driver.network_card(&spec.id).unwrap();
    send_frame(&card, "after restore");
    next_frame(&card, |p| p == "echo: after restore");
    // A running instance cannot be restored over.
    assert!(matches!(
        driver.restore_state(&spec, &path),
        Err(Error::Invalid(_))
    ));
    driver.delete(&spec.id, 1).unwrap();
}

pub fn unsupported_save_is_reported(h: &impl Harness) {
    let driver = h.driver();
    if driver.capabilities().save_restore {
        return;
    }
    let (driver, spec) = started(h, "unsaveable", TestImage::Standard);
    let path = h.scratch().join("never.state");
    assert!(matches!(
        driver.save_state(&spec.id, 1, &path),
        Err(Error::Unsupported(_))
    ));
    assert!(matches!(
        driver.restore_state(&spec, &path),
        Err(Error::Unsupported(_))
    ));
    driver.delete(&spec.id, 1).unwrap();
}
