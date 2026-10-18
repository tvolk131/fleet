use super::*;
use fleet::instances::guest::{self, Operation, OperationState as O, Spec as S};
use std::path::Path;

fn boot() -> Fixture {
    let mut f = Fixture::new();
    f.create("guest");
    f.controller("guest");
    f.wait("guest", State::Stopped);
    f.set("guest", Desired::Running);
    f.wait("guest", State::Running);
    f
}
fn command(code: &str) -> S {
    S::Exec {
        argv: vec![
            env!("CARGO_BIN_EXE_fleet-test-workload").into(),
            code.into(),
        ],
        cwd: None,
        env: BTreeMap::new(),
        timeout_seconds: 10,
    }
}
fn command_args(args: &[&str]) -> S {
    let mut spec = command(args[0]);
    if let S::Exec { argv, .. } = &mut spec {
        argv.extend(args[1..].iter().map(|s| s.to_string()));
    }
    spec
}
fn start(f: &Fixture, id: &str, spec: S) -> Operation {
    guest::start(
        &f.home,
        "guest",
        id,
        2,
        spec,
        Path::new(env!("CARGO_BIN_EXE_fleet")),
    )
    .unwrap()
}
fn done(f: &Fixture, id: &str) -> Operation {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let op = guest::inspect(&f.home, "guest", id).unwrap();
        if op.state.finished() {
            return op;
        }
        assert!(
            Instant::now() < deadline,
            "operation never completed: {op:?}"
        );
        std::thread::sleep(Duration::from_millis(30));
    }
}
fn stop(f: &Fixture) {
    f.set("guest", Desired::Stopped);
    f.wait("guest", State::Stopped);
}

#[test]
fn validation_and_protocol_refuse_unsafe_or_unsupported_guest_requests() {
    use fleet::proto::{Protocol, Request, too_new};
    let request = Request::InstanceGuest(guest::Request::Storage);
    assert_eq!(
        too_new(&request, Protocol { major: 3, minor: 4 }),
        Some(("InstanceGuest", 5))
    );
    assert_eq!(too_new(&request, PROTOCOL), None);
    for spec in [
        S::Download {
            version: None,
            path: "relative".into(),
            max_bytes: 1,
        },
        S::Download {
            version: None,
            path: "/tmp/a".into(),
            max_bytes: guest::MAX_FILE + 1,
        },
        S::Download {
            version: None,
            path: "/tmp/a\0".into(),
            max_bytes: 1,
        },
        S::Exec {
            argv: vec![],
            cwd: None,
            env: BTreeMap::new(),
            timeout_seconds: 1,
        },
        S::Exec {
            argv: vec!["true".into()],
            cwd: None,
            env: BTreeMap::from([("A=B".into(), "x".into())]),
            timeout_seconds: 1,
        },
        S::Exec {
            argv: vec!["true".into()],
            cwd: None,
            env: BTreeMap::new(),
            timeout_seconds: 0,
        },
        command(&"x".repeat(65536)),
    ] {
        assert!(spec.validate().is_err(), "{spec:?}");
    }
}

#[test]
fn guest_readiness_exec_streams_and_idempotency_are_separate_from_vm_lifecycle() {
    let f = boot();
    let readiness = guest::readiness(&f.home, "guest").unwrap();
    assert!(readiness.ready, "{readiness:?}");
    assert!(guest::owned(&f.home, "guest", "stranger").is_err());
    let mut s = command("streams");
    if let S::Exec { cwd, env, .. } = &mut s {
        *cwd = Some(f.home.to_string_lossy().into());
        env.insert("LITERAL".into(), "$(touch bad); x".into());
    }
    start(&f, "once", s.clone());
    let op = done(&f, "once");
    assert_eq!((op.state, op.exit_code), (O::Failed, Some(23)));
    let mut bytes = vec![];
    let mut offset = 0;
    loop {
        let log = guest::logs(&f.home, "guest", "once", false, offset).unwrap();
        bytes.extend(log.data);
        offset = log.next;
        if log.done {
            break;
        }
    }
    assert_eq!(
        bytes,
        (0..=255).cycle().take(256 * 300).collect::<Vec<u8>>()
    );
    assert_eq!(
        guest::logs(&f.home, "guest", "once", true, 0).unwrap().data,
        b"error stream"
    );
    assert!(guest::logs(&f.home, "guest", "once", false, offset + 1).is_err());
    start(&f, "once", s.clone());
    assert!(
        guest::start(
            &f.home,
            "guest",
            "once",
            2,
            command("pass"),
            Path::new(env!("CARGO_BIN_EXE_fleet"))
        )
        .is_err()
    );
    assert!(
        guest::start(
            &f.home,
            "guest",
            "stale",
            1,
            s.clone(),
            Path::new(env!("CARGO_BIN_EXE_fleet"))
        )
        .is_err()
    );
    stop(&f);
    assert!(!guest::readiness(&f.home, "guest").unwrap().ready);
    start(&f, "once", s); // Reads the saved result even when the VM is stopped.
    assert_eq!(fs::read(f.home.join("count")).unwrap(), b"x");
    assert!(!f.home.join("bad").exists());
    assert!(
        guest::start(
            &f.home,
            "guest",
            "new",
            2,
            command("pass"),
            Path::new(env!("CARGO_BIN_EXE_fleet"))
        )
        .is_err()
    );
}

#[test]
fn command_timeout_output_caps_and_concurrent_admission_are_bounded() {
    let f = boot();
    let mut spec = command("sleep");
    if let S::Exec {
        timeout_seconds, ..
    } = &mut spec
    {
        *timeout_seconds = 1;
    }
    start(&f, "timeout", spec);
    assert_eq!(done(&f, "timeout").exit_code, Some(124));
    start(&f, "excess", command("excess"));
    let op = done(&f, "excess");
    assert_eq!(op.state, O::Interrupted);
    assert!(op.error.unwrap().contains("byte limit"));
    assert_eq!(
        fs::metadata(
            guest::operation_dir(&f.home, "guest", "excess")
                .unwrap()
                .join("stdout")
        )
        .unwrap()
        .len(),
        guest::MAX_LOG
    );
    for n in 0..4 {
        start(&f, &format!("parallel-{n}"), command_args(&["sleep", "1"]));
    }
    assert!(
        guest::start(
            &f.home,
            "guest",
            "fifth",
            2,
            command("pass"),
            Path::new(env!("CARGO_BIN_EXE_fleet"))
        )
        .is_err()
    );
    for n in 0..4 {
        assert_eq!(done(&f, &format!("parallel-{n}")).state, O::Succeeded);
    }
    stop(&f);
}

#[test]
fn interrupted_supervisor_is_not_replayed_but_unattempted_records_recover() {
    let f = boot();
    let marker = f.home.join("once-count");
    let spec = command_args(&["once", marker.to_str().unwrap()]);
    // A launcher that exits models an agent dying after recording but before launch.
    guest::start(
        &f.home,
        "guest",
        "crash",
        2,
        spec.clone(),
        Path::new("/usr/bin/true"),
    )
    .unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_fleet"))
        .args(["instance-operation"])
        .arg(&f.home)
        .args(["guest", "crash"])
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while !marker.exists() {
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(20));
    }
    child.kill().unwrap();
    child.wait().unwrap();
    let interrupted = done(&f, "crash");
    assert_eq!(interrupted.state, O::Interrupted);
    assert!(interrupted.error.unwrap().contains("not replayed"));
    start(&f, "crash", spec);
    std::thread::sleep(Duration::from_millis(1200));
    assert_eq!(fs::read(marker).unwrap(), b"x");
    guest::start(
        &f.home,
        "guest",
        "pending",
        2,
        command_args(&["print", "recovered"]),
        Path::new("/usr/bin/true"),
    )
    .unwrap();
    guest::recover(&f.home, Path::new(env!("CARGO_BIN_EXE_fleet"))).unwrap();
    assert_eq!(done(&f, "pending").state, O::Succeeded);
    stop(&f);
}

#[test]
fn regular_file_helpers_verify_content_and_refuse_conflicts_symlinks_and_oversize() {
    let f = boot();
    let source = f.home.join("source");
    let target = f.home.join("guest-data/sub/file");
    let data: Vec<u8> = (0..=255).cycle().take(200_000).collect();
    fs::write(&source, &data).unwrap();
    let upload = S::Upload {
        path: target.to_string_lossy().into(),
        hash: fleet::blobs::hash_file(&source).unwrap(),
        size: data.len() as u64,
        sha256: guest::sha256(&source).unwrap(),
        executable: true,
    };
    for id in ["upload", "identical", "conflict"] {
        let dir = guest::operation_dir(&f.home, "guest", id).unwrap();
        fs::create_dir_all(&dir).unwrap();
        fs::copy(&source, dir.join("input")).unwrap();
        if id == "conflict" {
            fs::write(&target, "keep me").unwrap();
        }
        start(&f, id, upload.clone());
        assert_eq!(
            done(&f, id).state,
            if id == "conflict" {
                O::Failed
            } else {
                O::Succeeded
            }
        );
        assert!(!dir.join("input").exists());
    }
    assert_eq!(fs::read(&target).unwrap(), b"keep me");
    assert_eq!(
        fs::metadata(&target).unwrap().permissions().mode() & 0o777,
        0o700
    );
    let symlink = f.home.join("symlink");
    std::os::unix::fs::symlink(&target, &symlink).unwrap();
    for (id, path, max, expected) in [
        ("download", &target, 100, O::Succeeded),
        ("too-big", &target, 1, O::Failed),
        ("link", &symlink, 100, O::Failed),
    ] {
        start(
            &f,
            id,
            S::Download {
                version: None,
                path: path.to_string_lossy().into(),
                max_bytes: max,
            },
        );
        assert_eq!(done(&f, id).state, expected);
    }
    stop(&f);
    assert_eq!(
        fs::read(guest::output(&f.home, "guest", "download").unwrap()).unwrap(),
        b"keep me"
    );
    assert!(guest::output(&f.home, "guest", "too-big").is_err());
}

#[test]
fn storage_inventory_distinguishes_sparse_capacity_and_ignores_symlinks() {
    let f = Fixture::new();
    f.create("one");
    let vm = f.home.join("instance-tart/vms/fleet-one");
    fs::create_dir_all(&vm).unwrap();
    fs::File::create(vm.join("disk.img"))
        .unwrap()
        .set_len(1 << 30)
        .unwrap();
    let cache = f.home.join("instance-tart/cache");
    fs::create_dir_all(&cache).unwrap();
    fs::write(cache.join("image"), "abc").unwrap();
    std::os::unix::fs::symlink(&vm, cache.join("do-not-follow")).unwrap();
    let report = instances::storage::report(&f.home).unwrap();
    assert_eq!(report.reserved_capacity_gb, 50);
    assert_eq!(report.instances[0].disk.logical_bytes, 1 << 30);
    assert!(report.instances[0].disk.allocated_bytes < report.instances[0].disk.logical_bytes);
    assert_eq!(report.image_cache.logical_bytes, 3);
    assert_eq!(report.image_cache.files, 1);
    assert!(
        report
            .note
            .contains("neither unique physical use nor reclaimable")
    );
    assert!(vm.join("disk.img").exists());
}

#[tokio::test(flavor = "multi_thread")]
async fn every_guest_endpoint_checks_instance_owner_over_authenticated_transport() {
    use fleet::{
        agent::{Agent, AgentConfig},
        blobs,
        proto::{ALPN, Request, Response},
    };
    use iroh::{Endpoint, RelayMode, SecretKey, endpoint::presets, protocol::Router};
    use std::{collections::HashSet, sync::Arc};
    let f = Fixture::new();
    let owner = SecretKey::generate();
    let other = SecretKey::generate();
    instances::create(
        &f.home,
        "private",
        spec(),
        &owner.public().to_string(),
        &hello(),
    )
    .unwrap();
    let store = blobs::open(&f.home.join("blobs")).await.unwrap();
    let endpoint = Endpoint::builder(presets::Minimal)
        .relay_mode(RelayMode::Disabled)
        .bind()
        .await
        .unwrap();
    let agent = Agent::start(
        Arc::new(HashSet::from([owner.public(), other.public()])),
        hello(),
        AgentConfig {
            home: f.home.clone(),
            supervisor: env!("CARGO_BIN_EXE_fleet").into(),
            store: store.clone(),
        },
    )
    .unwrap();
    let router = Router::builder(endpoint).accept(ALPN, agent).spawn();
    let stranger = Endpoint::builder(presets::Minimal)
        .secret_key(other)
        .relay_mode(RelayMode::Disabled)
        .bind()
        .await
        .unwrap();
    let connection = stranger
        .connect(router.endpoint().addr(), ALPN)
        .await
        .unwrap();
    use guest::Request as G;
    for request in [
        G::Ready {
            id: "private".into(),
        },
        G::List {
            id: "private".into(),
        },
        G::Get {
            id: "private".into(),
            operation: "x".into(),
        },
        G::Logs {
            id: "private".into(),
            operation: "x".into(),
            stderr: false,
            from: 0,
        },
        G::Output {
            id: "private".into(),
            operation: "x".into(),
        },
        G::Start {
            id: "private".into(),
            operation: "x".into(),
            revision: 1,
            spec: command("pass"),
        },
    ] {
        let Response::Error(error) =
            fleet::proto::request(&connection, &Request::InstanceGuest(request))
                .await
                .unwrap()
        else {
            panic!("non-owner was allowed");
        };
        assert!(error.contains("different coordinator"), "{error}");
    }
    assert!(!f.home.join("instances/private/operations/x").exists());
    let home = f.home.clone();
    tokio::task::spawn_blocking(move || wait_state(&home, "private", State::Stopped))
        .await
        .unwrap();
    instances::set_desired(
        &f.home,
        "private",
        Desired::Deleted,
        1,
        &owner.public().to_string(),
        &hello(),
    )
    .unwrap();
    let home = f.home.clone();
    tokio::task::spawn_blocking(move || wait_state(&home, "private", State::Deleted))
        .await
        .unwrap();
    stranger.close().await;
    router.shutdown().await.unwrap();
    store.shutdown().await.unwrap();
}

#[test]
fn a_version_checked_download_rejects_same_size_replacement() {
    use std::os::unix::fs::MetadataExt;
    let f = boot();
    let file = f.home.join("changing-output");
    fs::write(&file, "v1").unwrap();
    let m = fs::metadata(&file).unwrap();
    let version = format!(
        "{}:{}:{}:{}:{}",
        m.dev(),
        m.ino(),
        m.len(),
        m.mtime() * 1_000_000_000 + m.mtime_nsec(),
        m.ctime() * 1_000_000_000 + m.ctime_nsec()
    );
    let spec = S::Download {
        path: file.to_string_lossy().into(),
        max_bytes: 2,
        version: Some(version),
    };
    start(&f, "unchanged", spec.clone());
    assert_eq!(done(&f, "unchanged").state, O::Succeeded);
    fs::write(f.home.join("replacement"), "v2").unwrap();
    fs::rename(f.home.join("replacement"), &file).unwrap();
    start(&f, "changed", spec);
    assert_eq!(done(&f, "changed").exit_code, Some(75));
    assert!(guest::output(&f.home, "guest", "changed").is_err());
    stop(&f);
}

#[test]
fn helper_installation_is_reused_and_recovers_after_guest_temporary_files_disappear() {
    let f = boot();
    assert!(guest::readiness(&f.home, "guest").unwrap().ready);
    let history = f.home.join("instance-tart/history");
    let installs = || {
        fs::read_to_string(&history)
            .unwrap()
            .lines()
            .filter(|line| line.contains("fleet-guest-install"))
            .count()
    };
    assert_eq!(installs(), 1);
    start(
        &f,
        "once",
        command_args(&["once", f.home.join("once-count").to_str().unwrap()]),
    );
    assert_eq!(done(&f, "once").state, O::Succeeded);
    assert!(guest::readiness(&f.home, "guest").unwrap().ready);
    assert_eq!(installs(), 1);
    fs::remove_dir_all(f.home.join("instance-tart/vms/fleet-guest/guest-helpers")).unwrap();
    assert!(!guest::readiness(&f.home, "guest").unwrap().ready);
    assert!(guest::readiness(&f.home, "guest").unwrap().ready);
    assert_eq!(installs(), 2);
    assert_eq!(fs::read(f.home.join("once-count")).unwrap(), b"x");
    assert_eq!(
        guest::inspect(&f.home, "guest", "once").unwrap().state,
        O::Succeeded
    );
    stop(&f);
}
