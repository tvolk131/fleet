use super::*;
use fleet::{
    instances::{self, Config, Gpu, Network, Spec},
    jobs,
    proto::{Request, Response},
};
use std::{fs, os::unix::fs::PermissionsExt};

async fn setup(name: &str) -> Fleet {
    let mut f = Fleet::new(name, &[("vm", &["gpu:metal"])]).await;
    let w = &mut f.workers[0];
    w.stop().await;
    let tart = w.home.join("fake-tart");
    fs::write(&tart, include_str!("../fixtures/fake_tart.py")).unwrap();
    fs::set_permissions(&tart, fs::Permissions::from_mode(0o700)).unwrap();
    let softnet = w.home.join("softnet");
    fs::write(&softnet, "#!/bin/sh\necho softnet 0.24.0-test\n").unwrap();
    fs::set_permissions(&softnet, fs::Permissions::from_mode(0o700)).unwrap();
    instances::write(
        &w.home.join("instances.json"),
        &Config {
            tart,
            softnet,
            images: BTreeMap::from([(
                "macos".into(),
                format!("registry.test/macos@sha256:{}", "a".repeat(64)),
            )]),
            memory_budget_mb: 8192,
            disk_budget_gb: 128,
            min_free_disk_gb: 5,
            block_cidrs: vec![],
        },
    )
    .unwrap();
    w.start(f.coordinator_key.public(), &f.lookup).await;
    f
}
fn spec(gpu: Gpu) -> Spec {
    Spec {
        image: "macos".into(),
        cpus: 2,
        memory_mb: 4096,
        disk_gb: 50,
        network: Network::Offline,
        gpu,
    }
}
async fn phase(f: &Fleet, id: &str, want: fleet::vm_job::Phase) {
    let start = Instant::now();
    loop {
        let dir = jobs::dir(&f.workers[0].home, id);
        if let Ok(dir) = dir
            && let Some(s) =
                jobs::read_json::<fleet::vm_job::Status>(&dir.join("vm-job.json")).unwrap()
            && s.phase == want
        {
            return;
        }
        assert!(
            start.elapsed() < Duration::from_secs(60),
            "waiting for {want:?} {id}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
async fn request(core: &Arc<Core>, f: &Fleet, r: Request) -> Response {
    let c = core
        .coordinator
        .connect(f.workers[0].router.as_ref().unwrap().endpoint().addr())
        .await
        .unwrap();
    core.coordinator.ask(&c, &r).await.unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn vm_job_stages_fetches_and_recovers_without_reexecuting() {
    let mut f = setup("vm-job-recovery").await;
    let core = f.daemon().await;
    let input = f.dir.join("input");
    fs::write(&input, [0, 255, 10, 42]).unwrap();
    let mut job = f.job("isolated", "mkdir -p out; printf x >> out/attempts; echo before; sleep 8; cp data/input out/result; echo after; echo err >&2");
    job.vm = Some(spec(Gpu::Exclusive));
    job.inputs.insert("data/input".into(), input);
    let dest = f.dir.join("results");
    let submission = DaemonRequest::SubmitVm {
        key: "vm-idempotency".into(),
        job: Box::new(job),
        placement: Placement::default(),
        dest: Some(dest.clone()),
        after: vec![],
        priority: 0,
    };
    let DaemonResponse::Submitted { job: id, .. } = core.handle(submission.clone()).await else {
        panic!()
    };
    let DaemonResponse::Submitted { job: replay, .. } = core.handle(submission).await else {
        panic!()
    };
    assert_eq!(id, replay);
    phase(&f, &id, fleet::vm_job::Phase::Executing).await;
    // Wait until a guest side effect exists, then lose the orchestration process.
    let home = &f.workers[0].home;
    let instance = fleet::vm_job::instance_id(&id);
    let attempt = home
        .join("instance-tart/vms")
        .join(format!("fleet-{instance}/guest-work/out/attempts"));
    for _ in 0..200 {
        if fs::read(&attempt).is_ok_and(|bytes| bytes == b"x") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(fs::read(&attempt).unwrap(), b"x");
    let dir = jobs::dir(home, &id).unwrap();
    let started: jobs::Started = jobs::read_json(&dir.join("supervisor.json"))
        .unwrap()
        .unwrap();
    unsafe {
        libc::kill(started.supervisor_pid as i32, libc::SIGKILL);
    }
    // Worker and coordinator connections can also disappear; the guest operation survives.
    f.workers[0].stop().await;
    core.coordinator.endpoint.close().await;
    core.coordinator.store.shutdown().await.unwrap();
    f.core = None;
    drop(core);
    f.workers[0]
        .start(f.coordinator_key.public(), &f.lookup)
        .await;
    let core = f.daemon().await;
    let done = wait_for(&core, &id, "VM success", |j| j.state.finished()).await;
    assert_eq!(
        done.state,
        QState::Succeeded,
        "{done:?}; {:?}",
        fs::read_to_string(dir.join("vm-job.json"))
    );
    wait_for(&core, &id, "VM outputs fetched", |j| j.fetched_files == 2).await;
    assert_eq!(fs::read(dest.join("out/result")).unwrap(), [0, 255, 10, 42]);
    assert_eq!(fs::read(dest.join("out/attempts")).unwrap(), b"x");
    let Response::Logs { data, done, .. } = request(
        &core,
        &f,
        Request::Logs {
            job: id.clone(),
            stderr: false,
            from: 0,
            max: 1000,
        },
    )
    .await
    else {
        panic!()
    };
    assert_eq!(data, "before\nafter\n");
    assert!(done);
    let i = instances::info(&f.workers[0].home, &instance).unwrap();
    assert_eq!(i.observed.state, instances::State::Deleted);
    assert!(!i.reserves_compute());
    assert!(!dir.join("vm-inputs").exists());
    assert!(
        !guest_dir(&f, &id).join("execute/stdout").exists(),
        "redundant command payload removed"
    );
    f.finish().await;
}
fn guest_dir(f: &Fleet, id: &str) -> PathBuf {
    f.workers[0]
        .home
        .join("instances")
        .join(fleet::vm_job::instance_id(id))
        .join("operations")
}

#[tokio::test(flavor = "multi_thread")]
async fn vm_jobs_share_within_budgets_and_cancel_at_the_vm_boundary() {
    let mut f = setup("vm-budget-cancel").await;
    let core = f.daemon().await;
    let mut vm = f.job("shared", "echo running; sleep 60");
    vm.vm = Some(spec(Gpu::Shared));
    let first = submit(&core, vm.clone(), Placement::default(), None, vec![]).await;
    phase(&f, &first, fleet::vm_job::Phase::Executing).await;
    let second = submit(&core, vm.clone(), Placement::default(), None, vec![]).await;
    phase(&f, &second, fleet::vm_job::Phase::Executing).await;
    let third = submit(&core, vm, Placement::default(), None, vec![]).await;
    let native = submit(
        &core,
        f.job("native", "echo native"),
        Placement {
            gpu: true,
            ..Placement::default()
        },
        None,
        vec![],
    )
    .await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(core.job(&third).unwrap().unwrap().state, QState::Queued);
    assert_eq!(core.job(&native).unwrap().unwrap().state, QState::Queued);
    let DaemonResponse::Machines { machines } = core.handle(DaemonRequest::Machines).await else {
        panic!()
    };
    assert_eq!(machines[0].threads_used, 4, "a VM job is counted once");
    assert!(matches!(
        core.handle(DaemonRequest::Pause { job: first.clone() })
            .await,
        DaemonResponse::Error { .. }
    ));
    for id in [&third, &native, &first, &second] {
        core.handle(DaemonRequest::Cancel { job: id.clone() }).await;
    }
    for id in [&first, &second] {
        let done = wait_for(&core, id, "cancelled after VM deletion", |j| {
            j.state.finished()
        })
        .await;
        assert_eq!(done.state, QState::Cancelled);
        assert_eq!(
            instances::info(&f.workers[0].home, &fleet::vm_job::instance_id(id))
                .unwrap()
                .observed
                .state,
            instances::State::Deleted
        );
    }
    let next = submit(
        &core,
        f.job("native-after-vms", "echo free"),
        Placement {
            gpu: true,
            ..Placement::default()
        },
        None,
        vec![],
    )
    .await;
    assert_eq!(
        wait_for(&core, &next, "native follows VM cleanup", |j| j
            .state
            .finished())
        .await
        .state,
        QState::Succeeded
    );
    f.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn uncertain_guest_attempt_fails_without_replay_and_releases_vm() {
    let mut f = setup("vm-uncertain").await;
    let core = f.daemon().await;
    let mut vm = f.job("uncertain", "echo once; sleep 60");
    vm.vm = Some(spec(Gpu::Exclusive));
    let id = submit(&core, vm, Placement::default(), None, vec![]).await;
    phase(&f, &id, fleet::vm_job::Phase::Executing).await;
    let op = guest_dir(&f, &id).join("execute");
    for _ in 0..200 {
        if op.join("attempted").exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // Kill only the detached operation supervisor selected by its exact test path.
    let listing = std::process::Command::new("/bin/ps")
        .args(["-axo", "pid=,command="])
        .output()
        .unwrap();
    let needle = format!(
        "instance-operation {} {} execute",
        f.workers[0].home.display(),
        fleet::vm_job::instance_id(&id)
    );
    let text = String::from_utf8(listing.stdout).unwrap();
    let pids: Vec<i32> = text
        .lines()
        .filter(|l| l.contains(&needle))
        .map(|l| l.split_whitespace().next().unwrap().parse().unwrap())
        .collect();
    assert_eq!(pids.len(), 1, "{text}");
    unsafe {
        libc::kill(pids[0], libc::SIGKILL);
    }
    let done = wait_for(&core, &id, "failed uncertain attempt", |j| {
        j.state.finished()
    })
    .await;
    assert_eq!(done.state, QState::Failed);
    let status: fleet::vm_job::Status = jobs::read_json(
        &jobs::dir(&f.workers[0].home, &id)
            .unwrap()
            .join("vm-job.json"),
    )
    .unwrap()
    .unwrap();
    assert!(status.error.unwrap().contains("no replay"));
    assert_eq!(
        instances::info(&f.workers[0].home, &fleet::vm_job::instance_id(&id))
            .unwrap()
            .observed
            .state,
        instances::State::Deleted
    );
    f.finish().await;
}

#[test]
fn guest_manifest_and_vm_protocol_are_fail_closed() {
    use fleet::vm_job::{Capture, validate_manifest};
    for path in ["../escape", "/absolute", "a/../b", "a//b", "a/./b"] {
        assert!(
            validate_manifest(
                &[Capture {
                    version: None,
                    path: path.into(),
                    size: 1
                }],
                &["*".into()]
            )
            .is_err()
        );
    }
    assert!(
        validate_manifest(
            &[
                Capture {
                    version: None,
                    path: "a".into(),
                    size: 1
                },
                Capture {
                    version: None,
                    path: "a/b".into(),
                    size: 1
                }
            ],
            &["*".into()]
        )
        .is_err()
    );
    assert!(
        validate_manifest(
            &[Capture {
                version: None,
                path: "secret".into(),
                size: 1
            }],
            &["out/*".into()]
        )
        .is_err()
    );
    assert!(
        validate_manifest(
            &[Capture {
                version: None,
                path: "out/big".into(),
                size: (1 << 30) + 1
            }],
            &["out/*".into()]
        )
        .is_err()
    );
    let value = serde_json::json!({"vm":spec(Gpu::Exclusive)});
    assert!(
        fleet::proto::job_too_new(&value, fleet::proto::Protocol { major: 3, minor: 5 }).is_some()
    );
    assert_eq!(
        fleet::proto::job_too_new(&value, fleet::proto::Protocol { major: 3, minor: 6 }),
        Some(("VM job observations", 7))
    );
    assert!(fleet::proto::job_too_new(&value, fleet::proto::PROTOCOL).is_none());

    let home = temp_dir("staging-cleanup");
    for (name, vm) in [
        ("native", serde_json::Value::Null),
        ("vm", serde_json::to_value(spec(Gpu::Exclusive)).unwrap()),
    ] {
        let dir = home.join("staging").join(name);
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("job.json"),
            serde_json::to_vec(&serde_json::json!({
                "id":name,"submitter":"test","submitted_at":0,
                "spec":{"name":name,"argv":["true"],"vm":vm}
            }))
            .unwrap(),
        )
        .unwrap();
    }
    fleet::vm_job::clean_staging(&home).unwrap();
    assert!(!home.join("staging/vm").exists());
    assert!(home.join("staging/native/job.json").exists());
    fs::remove_dir_all(home).unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn vm_timeout_and_unsafe_output_failures_still_clean_up() {
    let mut f = setup("vm-failures").await;
    let core = f.daemon().await;
    for (name, script, limit, state) in [
        ("timeout", "echo ready; sleep 30", 1, QState::TimedOut),
        ("exit-124", "exit 124", 30, QState::Failed),
        (
            "unsafe-output",
            "mkdir -p out; ln -s /etc/passwd out/leak",
            30,
            QState::Failed,
        ),
    ] {
        let mut job = f.job(name, script);
        job.vm = Some(spec(Gpu::Exclusive));
        job.max_seconds = Some(limit);
        let id = submit(&core, job, Placement::default(), None, vec![]).await;
        let done = wait_for(&core, &id, "bounded failure", |j| j.state.finished()).await;
        assert_eq!(done.state, state, "{done:?}");
        let i = instances::info(&f.workers[0].home, &fleet::vm_job::instance_id(&id)).unwrap();
        assert_eq!(i.observed.state, instances::State::Deleted);
        if name == "unsafe-output" {
            let Response::Outputs(files) =
                request(&core, &f, Request::Outputs { job: id.clone() }).await
            else {
                panic!()
            };
            assert!(files.is_empty());
            let Response::Logs { data, .. } = request(
                &core,
                &f,
                Request::Logs {
                    job: id,
                    stderr: true,
                    from: 0,
                    max: 10000,
                },
            )
            .await
            else {
                panic!()
            };
            assert!(data.contains("regular file"), "{data}");
        }
    }
    f.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_crash_before_job_publication_reclaims_the_vm_without_running_it() {
    let f = setup("vm-orphan").await;
    let home = &f.workers[0].home;
    let orphan = instances::reserve_job(
        home,
        "orphan",
        spec(Gpu::Exclusive),
        &f.coordinator_key.public().to_string(),
        &hello(&[]),
    )
    .unwrap();
    let start = Instant::now();
    loop {
        let i = instances::info(home, &orphan.record.id).unwrap();
        if i.observed.state == instances::State::Deleted {
            break;
        }
        assert!(start.elapsed() < Duration::from_secs(30), "{i:?}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let history = fs::read_to_string(home.join("instance-tart/history")).unwrap();
    assert!(!history.lines().any(|l| l.starts_with("run ")));
    let coordinator = f.coordinator().await;
    let mut job = f.job("orphan-retry", "echo recovered");
    job.vm = Some(spec(Gpu::Exclusive));
    coordinator
        .submit_as(
            f.workers[0].router.as_ref().unwrap().endpoint().addr(),
            &job,
            Some("orphan".into()),
        )
        .await
        .unwrap();
    let start = Instant::now();
    loop {
        let info = jobs::info(&jobs::dir(home, "orphan").unwrap()).unwrap();
        if info.state.finished() {
            assert_eq!(info.state, jobs::State::Succeeded, "{info:?}");
            break;
        }
        assert!(start.elapsed() < Duration::from_secs(60));
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let mut pending = f.job("cancel-before-delivery", "echo must-not-run");
    pending.vm = Some(spec(Gpu::Exclusive));
    let prepared = coordinator.prepare(&pending).await.unwrap();
    let addr = f.workers[0].router.as_ref().unwrap().endpoint().addr();
    let c = coordinator.connect(addr.clone()).await.unwrap();
    coordinator
        .ask(
            &c,
            &Request::CancelVmJob {
                job: "cancel-before-delivery".into(),
                spec: Box::new(prepared.clone()),
            },
        )
        .await
        .unwrap();
    coordinator
        .submit_spec(addr, prepared, Some("cancel-before-delivery".into()))
        .await
        .unwrap();
    assert_eq!(
        jobs::info(&jobs::dir(home, "cancel-before-delivery").unwrap())
            .unwrap()
            .state,
        jobs::State::Cancelled
    );
    assert!(
        instances::info(home, &fleet::vm_job::instance_id("cancel-before-delivery")).is_err(),
        "cancelled delivery never allocates a VM"
    );
    coordinator.endpoint.close().await;
    f.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn cancelling_during_image_cloning_does_not_wait_for_the_download() {
    let mut f = setup("vm-cancel-clone").await;
    let root = f.workers[0].home.join("instance-tart");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("slow-clone"), "").unwrap();
    let core = f.daemon().await;
    let mut job = f.job("cancel-clone", "echo should-not-run");
    job.vm = Some(spec(Gpu::Exclusive));
    let id = submit(&core, job, Placement::default(), None, vec![]).await;
    let start = Instant::now();
    loop {
        if fs::read_to_string(root.join("history"))
            .unwrap_or_default()
            .lines()
            .any(|l| l.starts_with("clone "))
        {
            break;
        }
        assert!(start.elapsed() < Duration::from_secs(30));
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let start = Instant::now();
    core.handle(DaemonRequest::Cancel { job: id.clone() }).await;
    let done = wait_for(&core, &id, "cloning cancelled", |j| j.state.finished()).await;
    assert_eq!(done.state, QState::Cancelled);
    assert!(start.elapsed() < Duration::from_secs(15));
    assert!(
        !fs::read_to_string(root.join("history"))
            .unwrap()
            .lines()
            .any(|l| l.starts_with("run "))
    );
    f.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn vm_live_metrics_same_size_updates_restart_and_cancel_keep_results() {
    use fleet::probes::{Probe, Reading};
    let mut f = setup("vm-live-observation").await;
    let core = f.daemon().await;
    let mut job = f.job("live", "unused");
    job.vm = Some(spec(Gpu::Exclusive));
    job.argv = vec![
        "/usr/bin/python3".into(),
        "-c".into(),
        r#"
import os,time
os.makedirs('out/checkpoint')
open('out/checkpoint/model','w').write('checkpoint')
open('out/result','w').write('v1')
open('metrics.jsonl','w').write('{"step":1,"loss":0.8}\n{"step":2,"loss":0.6}\n')
print('phase: training',flush=True)
while not os.path.exists('advance'): time.sleep(.1)
open('out/result','w').write('v2')
with open('metrics.jsonl','a') as f: f.write('{"step":3,"loss":0.4}\n')
print('phase: waiting',flush=True)
time.sleep(150)
"#
        .into(),
    ];
    job.settle_seconds = Some(0);
    job.max_seconds = Some(180);
    job.probes = vec![
        Probe::JsonlField {
            name: Some("steps".into()),
            file: "metrics.jsonl".into(),
            field: "step".into(),
            total: Some(10.0),
        },
        Probe::JsonlSeries {
            name: Some("loss".into()),
            file: "metrics.jsonl".into(),
            field: "loss".into(),
            points: Some(10),
        },
        Probe::GlobCount {
            name: Some("checkpoints".into()),
            pattern: "out/checkpoint/*".into(),
        },
        Probe::LogRegex {
            name: Some("phase".into()),
            pattern: "phase: (.*)".into(),
            stderr: false,
        },
    ];
    let dest = f.dir.join("live-results");
    let id = submit(&core, job, Placement::default(), Some(&dest), vec![]).await;
    let live = wait_for(&core, &id, "live checkpoint and metrics", |j| {
        j.state == QState::Running
            && fs::read(dest.join("out/result")).is_ok_and(|b| b == b"v1")
            && j.progress.as_ref().is_some_and(|p| p.len() == 4)
    })
    .await;
    assert_eq!(
        live.vm_status.unwrap().phase,
        fleet::vm_job::Phase::Executing
    );
    let readings = live.progress.unwrap();
    assert!(
        readings
            .iter()
            .any(|v| matches!(&v.reading, Reading::Series { values } if *values == vec![0.8, 0.6]))
    );
    assert!(
        readings
            .iter()
            .any(|v| matches!(&v.reading, Reading::Count { count: 1 }))
    );
    assert_eq!(
        fs::read(dest.join("out/checkpoint/model")).unwrap(),
        b"checkpoint"
    );
    let home = f.workers[0].home.clone();
    let dir = jobs::dir(&home, &id).unwrap();
    let history = home.join("instance-tart/history");
    let downloads = || {
        fs::read_to_string(&history)
            .unwrap()
            .lines()
            .filter(|l| l.contains("\"kind\":\"download\""))
            .count()
    };
    let before = downloads();
    assert_eq!(before, 2);
    let started: jobs::Started = jobs::read_json(&dir.join("supervisor.json"))
        .unwrap()
        .unwrap();
    unsafe {
        libc::kill(started.supervisor_pid as i32, libc::SIGKILL);
    }
    f.workers[0].stop().await;
    core.coordinator.endpoint.close().await;
    core.coordinator.store.shutdown().await.unwrap();
    f.core = None;
    drop(core);
    f.workers[0]
        .start(f.coordinator_key.public(), &f.lookup)
        .await;
    let core = f.daemon().await;
    let first_generation = jobs::read_json::<serde_json::Value>(&dir.join("vm-observation.json"))
        .unwrap()
        .unwrap()["generation"]
        .as_u64()
        .unwrap();
    wait_for(&core, &id, "fresh observation after restart", |_| {
        jobs::read_json::<serde_json::Value>(&dir.join("vm-observation.json"))
            .unwrap()
            .is_some_and(|s| s["generation"].as_u64().unwrap() > first_generation)
    })
    .await;
    assert_eq!(
        downloads(),
        before,
        "unchanged artifacts must not be retransferred"
    );
    let guest_work = home.join("instance-tart/vms").join(format!(
        "fleet-{}/guest-work",
        fleet::vm_job::instance_id(&id)
    ));
    fs::write(guest_work.join("advance"), "go").unwrap();
    wait_for(
        &core,
        &id,
        "same-size replacement and updated metric",
        |j| {
            fs::read(dest.join("out/result")).is_ok_and(|b| b == b"v2")
                && j.progress.as_ref().is_some_and(|p| {
                    p.iter().any(|p| {
                        matches!(
                            &p.reading,
                            Reading::Number {
                                value: 3.0,
                                per_hour: Some(_),
                                ..
                            }
                        )
                    })
                })
        },
    )
    .await;
    assert_eq!(downloads(), 3);
    // Model a crash after staging a capture but before its atomic publication.
    // A subsequent cancellation must retain committed results, not this copy.
    fs::write(dir.join("vm-capture.tmp"), "partial capture").unwrap();

    let result = core.handle(DaemonRequest::Cancel { job: id.clone() }).await;
    assert!(
        !matches!(result, DaemonResponse::Error { .. }),
        "{result:?}"
    );
    let done = wait_for(&core, &id, "cancelled with VM cleanup", |j| {
        j.state.finished()
            && j.vm_status
                .as_ref()
                .is_some_and(|s| s.phase == fleet::vm_job::Phase::Finished)
    })
    .await;
    assert_eq!(done.state, QState::Cancelled);
    assert!(!dir.join("vm-capture.tmp").exists());
    assert_eq!(fs::read(dest.join("out/result")).unwrap(), b"v2");
    assert_eq!(
        fs::read(dest.join("out/checkpoint/model")).unwrap(),
        b"checkpoint"
    );
    assert_eq!(
        fs::read_to_string(dir.join("stdout.log"))
            .unwrap()
            .matches("phase: training")
            .count(),
        1
    );
    assert_eq!(
        instances::info(&home, &fleet::vm_job::instance_id(&id))
            .unwrap()
            .observed
            .state,
        instances::State::Deleted
    );
    assert!(
        fleet::instances::guest::list(&home, &fleet::vm_job::instance_id(&id))
            .unwrap()
            .len()
            < 10,
        "observation operations must be reclaimed"
    );
    f.finish().await;
}
