//! The coordinator's core end to end: in-process agents and a daemon core over localhost, with relays off and an
//! in-memory address lookup standing in for DNS, so the daemon dials machines by ID as it does for real.

use std::{
    collections::{BTreeMap, HashSet},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use fleet::{
    agent::{Agent, AgentConfig},
    blobs,
    coordinator::{Coordinator, JobFile, machines},
    daemon::{Core, DaemonRequest, DaemonResponse, Placement, QState, QueuedJob},
    probes::{Probe, Reading},
    proto::{ALPN, Hello},
    suspend::{PreemptMode, PreemptSpec, ResumeSpec},
};
use iroh::{
    Endpoint, EndpointId, RelayMode, SecretKey, address_lookup::MemoryLookup, endpoint::presets,
    protocol::Router,
};

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "fleet-d-{name}-{}-{:08x}",
        std::process::id(),
        rand::random::<u32>()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

async fn endpoint(key: SecretKey, lookup: &MemoryLookup) -> Endpoint {
    Endpoint::builder(presets::Minimal)
        .relay_mode(RelayMode::Disabled)
        .address_lookup(lookup.clone())
        .secret_key(key)
        .bind()
        .await
        .unwrap()
}

fn hello(tags: &[&str]) -> Hello {
    Hello {
        agent_version: "test".into(),
        protocol: fleet::proto::PROTOCOL,
        hostname: "worker".into(),
        os: "macOS".into(),
        cpu: "test".into(),
        cores: 1,
        memory_bytes: 32 << 30,
        gpu: None,
        tags: tags.iter().map(|t| t.to_string()).collect(),
        sleep_minutes: None,
        cpu_slots: 8,
    }
}

struct Worker {
    key: SecretKey,
    home: PathBuf,
    tags: Vec<String>,
    router: Option<Router>,
}

impl Worker {
    async fn start(&mut self, coordinator: EndpointId, lookup: &MemoryLookup) {
        let store = blobs::open(&self.home.join("agent-blobs")).await.unwrap();
        let allow = Arc::new(HashSet::from([coordinator]));
        let config = AgentConfig {
            home: self.home.clone(),
            supervisor: env!("CARGO_BIN_EXE_fleet").into(),
            store: store.clone(),
        };
        let tags: Vec<&str> = self.tags.iter().map(String::as_str).collect();
        let router = Router::builder(endpoint(self.key.clone(), lookup).await)
            .accept(
                ALPN,
                Agent::start(allow.clone(), hello(&tags), config).unwrap(),
            )
            .accept(iroh_blobs::ALPN, blobs::protocol(&store, allow))
            .spawn();
        lookup.add_endpoint_info(router.endpoint().addr());
        self.router = Some(router);
    }

    async fn stop(&mut self) {
        if let Some(router) = self.router.take() {
            router.shutdown().await.unwrap();
        }
    }
}

struct Fleet {
    dir: PathBuf,
    lookup: MemoryLookup,
    coordinator_key: SecretKey,
    workers: Vec<Worker>,
    core: Option<Arc<Core>>,
}

impl Fleet {
    async fn new(name: &str, workers: &[(&str, &[&str])]) -> Self {
        let dir = temp_dir(name);
        let lookup = MemoryLookup::new();
        let coordinator_key = SecretKey::generate();
        let mut fleet = Fleet {
            dir,
            lookup,
            coordinator_key,
            workers: Vec::new(),
            core: None,
        };
        for (worker, tags) in workers {
            let mut w = Worker {
                key: SecretKey::generate(),
                home: fleet.dir.join(format!("worker-{worker}")),
                tags: tags.iter().map(|t| t.to_string()).collect(),
                router: None,
            };
            w.start(fleet.coordinator_key.public(), &fleet.lookup).await;
            machines::add(&fleet.home().join("machines"), worker, w.key.public(), &[]).unwrap();
            fleet.workers.push(w);
        }
        fleet
    }

    fn home(&self) -> PathBuf {
        self.dir.join("coordinator-home")
    }

    async fn coordinator(&self) -> Coordinator {
        Coordinator::new(
            endpoint(self.coordinator_key.clone(), &self.lookup).await,
            blobs::open(&self.home().join("coordinator-blobs"))
                .await
                .unwrap(),
        )
    }

    async fn daemon(&mut self) -> Arc<Core> {
        let core = Core::for_tests(&self.home(), self.coordinator().await).unwrap();
        core.clone().start_tasks();
        self.core = Some(core.clone());
        core
    }

    fn job(&self, name: &str, script: &str) -> JobFile {
        JobFile {
            vm: None,
            name: name.into(),
            argv: vec!["/bin/sh".into(), "-c".into(), script.into()],
            env: BTreeMap::new(),
            inputs: BTreeMap::new(),
            publish: vec!["out/*".into()],
            settle_seconds: Some(0),
            max_seconds: Some(120),
            shutdown_grace_seconds: None,
            keep_awake: Some(false),
            probes: Vec::new(),
            pause_keeps_gpu: None,
            resume: None,
            preempt: None,
            threads: None,
        }
    }

    async fn finish(mut self) {
        for w in &mut self.workers {
            w.stop().await;
        }
        if let Some(core) = self.core.take() {
            core.coordinator.endpoint.close().await;
        }
        std::fs::remove_dir_all(&self.dir).ok();
    }
}

async fn submit(
    core: &Arc<Core>,
    job: JobFile,
    placement: Placement,
    dest: Option<&Path>,
    after: Vec<String>,
) -> String {
    let request = DaemonRequest::Submit {
        job: Box::new(job),
        placement,
        dest: dest.map(Path::to_path_buf),
        after,
        priority: 0,
    };
    match core.handle(request).await {
        DaemonResponse::Submitted { job, .. } => job,
        other => panic!("{other:?}"),
    }
}

async fn wait_for(
    core: &Arc<Core>,
    id: &str,
    what: &str,
    until: impl Fn(&QueuedJob) -> bool,
) -> QueuedJob {
    let start = Instant::now();
    loop {
        let job = core.job(id).unwrap().expect("queued");
        if until(&job) {
            return job;
        }
        assert!(
            start.elapsed() < Duration::from_secs(90),
            "timed out waiting for {what}: {job:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_queued_job_runs_where_it_fits_and_its_outputs_arrive() {
    let mut f = Fleet::new(
        "fit",
        &[
            ("plain", &["os:macos"]),
            ("metal", &["os:macos", "gpu:metal"]),
        ],
    )
    .await;
    let core = f.daemon().await;
    let dest = f.dir.join("results");
    let job = f.job("fits", "mkdir -p out && echo result > out/r.txt");
    let placement = Placement {
        tags: vec!["gpu:metal".into()],
        ..Placement::default()
    };
    let id = submit(&core, job, placement, Some(&dest), vec![]).await;

    let done = wait_for(&core, &id, "success", |j| j.state == QState::Succeeded).await;
    assert_eq!(
        done.machine.as_deref(),
        Some("metal"),
        "only the machine with the tag fits"
    );
    wait_for(&core, &id, "the fetch", |j| j.fetched_files == 1).await;
    assert_eq!(
        std::fs::read_to_string(dest.join("out/r.txt")).unwrap(),
        "result\n"
    );
    f.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_destination_without_publish_fetches_all_but_unchanged_inputs() {
    let mut f = Fleet::new("dest-all", &[("plain", &["os:macos"])]).await;
    let core = f.daemon().await;
    let local = f.dir.join("local");
    std::fs::create_dir_all(&local).unwrap();
    std::fs::write(local.join("in.txt"), "input\n").unwrap();
    std::fs::write(local.join("edit.txt"), "before\n").unwrap();
    let mut job = f.job(
        "dest-all",
        "mkdir -p out && cat data/in.txt > out/copy.txt && echo top > top.txt && echo after > data/edit.txt",
    );
    job.publish.clear();
    job.inputs
        .insert("data/in.txt".into(), local.join("in.txt"));
    job.inputs
        .insert("data/edit.txt".into(), local.join("edit.txt"));
    let dest = f.dir.join("results");
    let id = submit(&core, job, Placement::default(), Some(&dest), vec![]).await;
    wait_for(&core, &id, "success", |j| j.state == QState::Succeeded).await;
    wait_for(&core, &id, "the fetch", |j| j.fetched_files == 3).await;
    assert_eq!(
        std::fs::read_to_string(dest.join("out/copy.txt")).unwrap(),
        "input\n"
    );
    assert_eq!(
        std::fs::read_to_string(dest.join("top.txt")).unwrap(),
        "top\n"
    );
    assert_eq!(
        std::fs::read_to_string(dest.join("data/edit.txt")).unwrap(),
        "after\n",
        "an input the job changed is an output"
    );
    assert!(
        !dest.join("data/in.txt").exists(),
        "an unchanged input is not fetched back"
    );
    f.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_growing_output_is_fetched_again_but_a_local_file_is_never_overwritten() {
    let mut f = Fleet::new("grow", &[("plain", &["os:macos"])]).await;
    let core = f.daemon().await;
    let dest = f.dir.join("results");
    // A log fetched once settled, then appended to: the coordinator's own copy is brought up to date.
    let job = f.job(
        "grow",
        "mkdir -p out && echo one > out/log.txt && while [ ! -e append ]; do sleep 0.1; done; echo two >> out/log.txt",
    );
    let id = submit(&core, job, Placement::default(), Some(&dest), vec![]).await;
    let log = dest.join("out/log.txt");
    let start = std::time::Instant::now();
    while std::fs::read_to_string(&log).ok().as_deref() != Some("one\n") {
        assert!(
            start.elapsed() < Duration::from_secs(60),
            "the first version was never fetched"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    std::fs::write(
        f.workers[0].home.join("jobs").join(&id).join("work/append"),
        "go",
    )
    .unwrap();
    wait_for(&core, &id, "success", |j| j.state == QState::Succeeded).await;
    let start = std::time::Instant::now();
    while std::fs::read_to_string(&log).unwrap() != "one\ntwo\n" {
        assert!(
            start.elapsed() < Duration::from_secs(60),
            "the grown log was never fetched"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let done = wait_for(&core, &id, "no fetch error", |j| j.error.is_none()).await;
    assert_eq!(done.fetched_files, 1, "an update is not a new file");

    // A file at the destination that fleet did not write stays as it is.
    std::fs::write(dest.join("out/mine.txt"), "local\n").unwrap();
    let other = submit(
        &core,
        f.job("other", "mkdir -p out && echo theirs > out/mine.txt"),
        Placement::default(),
        Some(&dest),
        vec![],
    )
    .await;
    let failed = wait_for(&core, &other, "the refusal", |j| {
        j.error
            .as_deref()
            .is_some_and(|e| e.contains("refusing to overwrite"))
    })
    .await;
    assert!(failed.error.unwrap().contains("mine.txt"));
    assert_eq!(
        std::fs::read_to_string(dest.join("out/mine.txt")).unwrap(),
        "local\n"
    );
    f.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn gpu_jobs_take_turns_and_dependencies_are_honored() {
    let mut f = Fleet::new("turns", &[("metal", &["gpu:metal"])]).await;
    let core = f.daemon().await;
    let gpu = Placement {
        gpu: true,
        ..Placement::default()
    };
    let first = submit(&core, f.job("first", "sleep 2"), gpu.clone(), None, vec![]).await;
    let second = submit(&core, f.job("second", "sleep 1"), gpu, None, vec![]).await;
    let failing = submit(
        &core,
        f.job("failing", "exit 3"),
        Placement::default(),
        None,
        vec![],
    )
    .await;
    let blocked = submit(
        &core,
        f.job("blocked", "true"),
        Placement::default(),
        None,
        vec![failing.clone()],
    )
    .await;
    let follows = submit(
        &core,
        f.job("follows", "true"),
        Placement::default(),
        None,
        vec![first.clone()],
    )
    .await;

    let a = wait_for(&core, &first, "first", |j| j.state == QState::Succeeded).await;
    let b = wait_for(&core, &second, "second", |j| j.state == QState::Succeeded).await;
    assert!(
        b.assigned_at.unwrap() >= a.finished_at.unwrap(),
        "the second GPU job waited: {a:?} {b:?}"
    );
    assert_eq!(
        wait_for(&core, &failing, "failure", |j| j.state.finished())
            .await
            .state,
        QState::Failed
    );
    let blocked = wait_for(&core, &blocked, "cancellation", |j| j.state.finished()).await;
    assert_eq!(blocked.state, QState::Cancelled);
    assert!(blocked.error.unwrap().contains("ended failed"));
    assert_eq!(
        wait_for(&core, &follows, "follower", |j| j.state.finished())
            .await
            .state,
        QState::Succeeded
    );
    f.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_gpu_process_started_outside_fleet_holds_gpu_jobs_back() {
    let mut f = Fleet::new("external", &[("metal", &["gpu:metal"])]).await;
    // A process with a recognisable command line stands in for a training run started by hand.
    let marker = format!("{}", 100_000 + rand::random::<u32>() % 800_000);
    let mut external = std::process::Command::new("/bin/sleep")
        .arg(&marker)
        .spawn()
        .unwrap();
    std::fs::write(
        f.workers[0].home.join("gpu-processes"),
        format!("sleep {marker}\n"),
    )
    .unwrap();
    let core = f.daemon().await;
    let id = submit(
        &core,
        f.job("gpu", "true"),
        Placement {
            gpu: true,
            ..Placement::default()
        },
        None,
        vec![],
    )
    .await;

    let waiting = wait_for(&core, &id, "the busy note", |j| {
        j.error.as_deref().is_some_and(|e| e.contains(&marker))
    })
    .await;
    assert_eq!(waiting.state, QState::Queued);
    external.kill().unwrap();
    external.wait().unwrap();
    assert_eq!(
        wait_for(&core, &id, "the run", |j| j.state.finished())
            .await
            .state,
        QState::Succeeded
    );
    f.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn after_a_disconnect_the_missed_events_are_replayed() {
    let mut f = Fleet::new("replay", &[("worker", &[])]).await;
    let core = f.daemon().await;
    let dest = f.dir.join("results");
    let job = f.job(
        "slow",
        "sleep 4 && mkdir -p out && echo late > out/late.txt",
    );
    let id = submit(&core, job, Placement::default(), Some(&dest), vec![]).await;
    wait_for(&core, &id, "the start", |j| j.state == QState::Running).await;

    // The agent goes away while the job (in its own supervisor process) keeps running, then comes back.
    f.workers[0].stop().await;
    tokio::time::sleep(Duration::from_secs(6)).await;
    let coordinator = f.coordinator_key.public();
    f.workers[0].start(coordinator, &f.lookup).await;

    let done = wait_for(&core, &id, "the replayed success", |j| {
        j.state == QState::Succeeded
    })
    .await;
    assert!(done.exit.is_some());
    wait_for(&core, &id, "the fetch", |j| j.fetched_files == 1).await;
    assert_eq!(
        std::fs::read_to_string(dest.join("out/late.txt")).unwrap(),
        "late\n"
    );
    f.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn without_a_daemon_a_job_is_placed_at_once_or_refused() {
    let f = Fleet::new("direct", &[("worker", &["os:macos"])]).await;
    let core = Core::direct(&f.home(), f.coordinator().await).unwrap();
    let id = submit(
        &core,
        f.job("now", "echo hi"),
        Placement::default(),
        None,
        vec![],
    )
    .await;
    let placed = core.job(&id).unwrap().unwrap();
    assert_eq!(placed.machine.as_deref(), Some("worker"));

    let impossible = Placement {
        tags: vec!["gpu:cuda".into()],
        ..Placement::default()
    };
    let request = DaemonRequest::Submit {
        job: Box::new(f.job("never", "true")),
        placement: impossible,
        dest: None,
        after: vec![],
        priority: 0,
    };
    match core.handle(request).await {
        DaemonResponse::Error { message, .. } => {
            assert!(message.contains("lacks tag gpu:cuda"), "{message}")
        }
        other => panic!("{other:?}"),
    }
    core.coordinator.endpoint.close().await;
    f.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn progress_probes_reach_the_queue() {
    let mut f = Fleet::new("probes", &[("worker", &[])]).await;
    let core = f.daemon().await;
    let mut job = f.job(
        "progress",
        "mkdir -p B && for i in 1 2 3 4 5 6; do echo \"{\\\"iteration\\\": $i}\" >> B/metrics.jsonl; sleep 1; done; sleep 20",
    );
    job.probes = vec![Probe::JsonlField {
        name: None,
        file: "B/metrics.jsonl".into(),
        field: "iteration".into(),
        total: Some(12.0),
    }];
    let id = submit(&core, job, Placement::default(), None, vec![]).await;
    let seen = wait_for(&core, &id, "a progress reading", |j| {
        j.progress
            .as_ref()
            .is_some_and(|p| matches!(p[0].reading, Reading::Number { value, .. } if value >= 2.0))
    })
    .await;
    let Reading::Number { total, .. } = &seen.progress.unwrap()[0].reading else {
        unreachable!()
    };
    assert_eq!(*total, Some(12.0));
    core.handle(DaemonRequest::Cancel { job: id.clone() }).await;
    f.finish().await;
}

fn lines(path: &Path) -> usize {
    std::fs::read_to_string(path)
        .map(|s| s.lines().count())
        .unwrap_or(0)
}

#[tokio::test(flavor = "multi_thread")]
async fn pausing_freezes_a_job_without_spending_its_time_limit() {
    let mut f = Fleet::new("pause", &[("metal", &["gpu:metal"])]).await;
    let core = f.daemon().await;
    let gpu = Placement {
        gpu: true,
        ..Placement::default()
    };
    // 20 ticks of 0.2 s (4 s of work) under a 6 s limit.
    let mut job = f.job(
        "ticker",
        "mkdir -p out; for i in $(seq 1 20); do echo $i >> out/count; sleep 0.2; done",
    );
    job.max_seconds = Some(6);
    let id = submit(&core, job, gpu.clone(), None, vec![]).await;
    wait_for(&core, &id, "the start", |j| j.state == QState::Running).await;
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert!(matches!(
        core.handle(DaemonRequest::Pause { job: id.clone() }).await,
        DaemonResponse::Done
    ));
    assert_eq!(core.job(&id).unwrap().unwrap().state, QState::Paused);
    let count = f.workers[0]
        .home
        .join("jobs")
        .join(&id)
        .join("work/out/count");
    let frozen = lines(&count);
    assert!(frozen > 0 && frozen < 20, "{frozen}");

    // While it is paused, its GPU is free for another GPU job.
    let other = submit(&core, f.job("other", "true"), gpu, None, vec![]).await;
    wait_for(&core, &other, "the other GPU job", |j| {
        j.state == QState::Succeeded
    })
    .await;

    // Frozen for longer than its whole time limit.
    tokio::time::sleep(Duration::from_secs(7)).await;
    assert_eq!(lines(&count), frozen, "nothing ran while paused");
    assert!(matches!(
        core.handle(DaemonRequest::Continue { job: id.clone() })
            .await,
        DaemonResponse::Done
    ));
    let done = wait_for(&core, &id, "the finish", |j| j.state.finished()).await;
    assert_eq!(
        done.state,
        QState::Succeeded,
        "paused time did not count toward the limit: {done:?}"
    );
    assert_eq!(lines(&count), 20);

    // A paused job can still be cancelled, promptly.
    let sleeper = submit(
        &core,
        f.job("sleeper", "sleep 60"),
        Placement::default(),
        None,
        vec![],
    )
    .await;
    wait_for(&core, &sleeper, "the start", |j| j.state == QState::Running).await;
    core.handle(DaemonRequest::Pause {
        job: sleeper.clone(),
    })
    .await;
    let asked = Instant::now();
    core.handle(DaemonRequest::Cancel {
        job: sleeper.clone(),
    })
    .await;
    assert_eq!(
        wait_for(&core, &sleeper, "the cancel", |j| j.state.finished())
            .await
            .state,
        QState::Cancelled
    );
    assert!(
        asked.elapsed() < Duration::from_secs(8),
        "cancelled within the grace period"
    );
    f.finish().await;
}

/// A job that trains for 30 steps of 0.25 s, writes a checkpoint every 5 steps (atomically, by renaming a folder), and
/// can continue from one: the shape of our trainer, in a few lines of shell.
fn training_job(f: &Fleet, name: &str) -> JobFile {
    let script = f.dir.join("train.sh");
    std::fs::write(
        &script,
        r#"#!/bin/sh
out=$1
step=0
if [ -n "$2" ]; then step=$(cat "$2/state"); fi
mkdir -p "$out/ckpt"
while [ $step -lt 30 ]; do
  step=$((step + 1))
  echo "{\"step\": $step}" >> metrics.jsonl
  sleep 0.25
  if [ $((step % 5)) -eq 0 ]; then
    mkdir -p "$out/tmp"; echo $step > "$out/tmp/state"
    mv "$out/tmp" "$out/ckpt/step-$(printf %04d $step)"
  fi
done
echo "finished at $step"
"#,
    )
    .unwrap();
    std::fs::set_permissions(&script, std::os::unix::fs::PermissionsExt::from_mode(0o755)).unwrap();
    let mut job = f.job(name, "unused");
    job.argv = vec!["bin/train.sh".into(), "out1".into()];
    job.inputs.insert("bin/train.sh".into(), script);
    job.probes = vec![Probe::JsonlField {
        name: None,
        file: "metrics.jsonl".into(),
        field: "step".into(),
        total: Some(30.0),
    }];
    job.resume = Some(ResumeSpec {
        points: "out*/ckpt/step-*".into(),
        position: Some(r"step-(\d+)".into()),
        progress: Some("step".into()),
        every: None,
        argv: vec![
            "bin/train.sh".into(),
            "out{segment}".into(),
            "{latest}".into(),
        ],
        carry: vec!["metrics.jsonl".into()],
    });
    job
}

/// The steps a job's working directory has logged so far.
fn steps(f: &Fleet, worker: usize, job: &str) -> Vec<u32> {
    std::fs::read_to_string(
        f.workers[worker]
            .home
            .join("jobs")
            .join(job)
            .join("work/metrics.jsonl"),
    )
    .unwrap_or_default()
    .lines()
    .filter_map(|l| {
        serde_json::from_str::<serde_json::Value>(l).ok()?["step"]
            .as_u64()
            .map(|s| s as u32)
    })
    .collect()
}

async fn wait_steps(f: &Fleet, worker: usize, job: &str, at_least: u32) {
    let start = Instant::now();
    while steps(f, worker, job).last().copied().unwrap_or(0) < at_least {
        assert!(
            start.elapsed() < Duration::from_secs(30),
            "the job never reached step {at_least}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The step a continuation picked up from: its first logged step after the lines carried from the segment before.
fn resumed_at(log: &[u32], carried: usize) -> Option<u32> {
    log.get(carried).copied()
}

#[tokio::test(flavor = "multi_thread")]
async fn suspending_now_states_the_loss_and_the_next_segment_resumes_from_the_checkpoint() {
    let mut f = Fleet::new("suspend-now", &[("worker", &[])]).await;
    let core = f.daemon().await;
    let id = submit(
        &core,
        training_job(&f, "train"),
        Placement::default(),
        None,
        vec![],
    )
    .await;
    wait_steps(&f, 0, &id, 7).await;

    let DaemonResponse::Plan { plan } = core
        .handle(DaemonRequest::SuspendPlan { job: id.clone() })
        .await
    else {
        panic!("no plan")
    };
    let latest = plan
        .latest
        .as_ref()
        .and_then(|c| c.position)
        .expect("a checkpoint");
    let current = plan.current.expect("the current step");
    assert_eq!(latest % 5.0, 0.0);
    assert_eq!(plan.loss_units, Some(current - latest), "{plan:?}");
    assert_eq!(plan.next_position, Some(latest + 5.0));
    assert!(
        fleet::suspend::describe(&plan)
            .join(" ")
            .contains("Suspending now redoes")
    );

    core.handle(DaemonRequest::Suspend {
        job: id.clone(),
        now: true,
        within_seconds: None,
    })
    .await;
    let suspended = wait_for(&core, &id, "the suspension", |j| {
        j.state == QState::Suspended
    })
    .await;
    let checkpoint = suspended.checkpoint.expect("a recorded checkpoint");
    let DaemonResponse::Resumed { job: next } = core
        .handle(DaemonRequest::Resume {
            job: id.clone(),
            on: None,
        })
        .await
    else {
        panic!("not resumed")
    };
    let done = wait_for(&core, &next, "the continuation", |j| j.state.finished()).await;
    assert_eq!(done.state, QState::Succeeded, "{done:?}");
    assert_eq!(done.segment, 2);
    let log = steps(&f, 0, &next);
    assert_eq!(log.last(), Some(&30));
    let carried = steps(&f, 0, &id).len();
    assert_eq!(
        resumed_at(&log, carried),
        Some(checkpoint.position.unwrap() as u32 + 1),
        "continued right after the checkpoint"
    );
    assert_eq!(
        core.job(&id).unwrap().unwrap().continued_by.as_deref(),
        Some(next.as_str())
    );
    f.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn suspending_when_safe_waits_for_the_next_checkpoint() {
    let mut f = Fleet::new("suspend-safe", &[("worker", &[])]).await;
    let core = f.daemon().await;
    let id = submit(
        &core,
        training_job(&f, "train"),
        Placement::default(),
        None,
        vec![],
    )
    .await;
    wait_steps(&f, 0, &id, 6).await;
    let before = steps(&f, 0, &id).last().copied().unwrap();
    core.handle(DaemonRequest::Suspend {
        job: id.clone(),
        now: false,
        within_seconds: None,
    })
    .await;
    assert_eq!(core.job(&id).unwrap().unwrap().state, QState::Suspending);
    let suspended = wait_for(&core, &id, "the suspension", |j| {
        j.state == QState::Suspended
    })
    .await;
    let at = suspended.checkpoint.unwrap().position.unwrap() as u32;
    assert_eq!(
        at,
        before.div_ceil(5) * 5,
        "stopped at the first checkpoint after the request"
    );
    let last = steps(&f, 0, &id).last().copied().unwrap();
    assert!(
        last <= at + 4,
        "lost at most the detection delay: stopped at {last}, checkpoint {at}"
    );
    f.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_job_moves_to_another_machine_and_finishes_there() {
    let mut f = Fleet::new("move", &[("a", &[]), ("b", &[])]).await;
    let core = f.daemon().await;
    let pinned = Placement {
        machine: Some("a".into()),
        ..Placement::default()
    };
    let id = submit(&core, training_job(&f, "train"), pinned, None, vec![]).await;
    wait_steps(&f, 0, &id, 7).await;
    core.handle(DaemonRequest::Move {
        job: id.clone(),
        to: "b".into(),
        now: true,
    })
    .await;
    let moved = wait_for(&core, &id, "the move", |j| j.continued_by.is_some()).await;
    let next = moved.continued_by.unwrap();
    let done = wait_for(&core, &next, "the continuation on b", |j| {
        j.state.finished()
    })
    .await;
    assert_eq!(
        (done.state, done.machine.as_deref()),
        (QState::Succeeded, Some("b")),
        "{done:?}"
    );
    let log = steps(&f, 1, &next);
    assert_eq!(log.last(), Some(&30));
    let carried = steps(&f, 0, &id).len();
    assert_eq!(
        resumed_at(&log, carried),
        Some(moved.checkpoint.unwrap().position.unwrap() as u32 + 1)
    );
    f.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_higher_priority_gpu_job_preempts_a_preemptible_one() {
    let mut f = Fleet::new("preempt", &[("metal", &["gpu:metal"])]).await;
    let core = f.daemon().await;
    let gpu = Placement {
        gpu: true,
        ..Placement::default()
    };
    let mut low = training_job(&f, "low");
    low.preempt = Some(PreemptSpec {
        mode: PreemptMode::Now,
        max_loss_minutes: Some(60.0),
    });
    let low_id = submit(&core, low, gpu.clone(), None, vec![]).await;
    wait_steps(&f, 0, &low_id, 3).await;

    let request = DaemonRequest::Submit {
        job: Box::new(f.job("high", "sleep 1; echo high")),
        placement: gpu,
        dest: None,
        after: vec![],
        priority: 5,
    };
    let DaemonResponse::Submitted { job: high_id, .. } = core.handle(request).await else {
        panic!()
    };
    let low_suspended = wait_for(&core, &low_id, "the preemption", |j| {
        j.state == QState::Suspended
    })
    .await;
    assert_eq!(
        low_suspended.preempted_for.as_deref(),
        Some(high_id.as_str())
    );
    let high = wait_for(&core, &high_id, "the high-priority job", |j| {
        j.state.finished()
    })
    .await;
    assert_eq!(high.state, QState::Succeeded);
    let next = wait_for(&core, &low_id, "the queued continuation", |j| {
        j.continued_by.is_some()
    })
    .await
    .continued_by
    .unwrap();
    let resumed = wait_for(&core, &next, "the continuation", |j| j.state.finished()).await;
    assert_eq!(resumed.state, QState::Succeeded, "{resumed:?}");
    assert!(
        resumed.assigned_at.unwrap() >= high.finished_at.unwrap(),
        "it waited for the GPU"
    );
    assert_eq!(steps(&f, 0, &next).last(), Some(&30));
    f.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn jobs_share_a_machine_by_cpu_threads() {
    let mut f = Fleet::new("threads", &[("worker", &[])]).await;
    let core = f.daemon().await;
    let job = |name: &str, threads: u32, seconds: u32| {
        let mut j = f.job(
            name,
            &format!("echo threads=$RAYON_NUM_THREADS; sleep {seconds}"),
        );
        j.threads = Some(threads);
        j
    };
    // 8 slots: 6 + 2 fit together; 4 more must wait for the 6 to finish.
    let big = submit(&core, job("big", 6, 3), Placement::default(), None, vec![]).await;
    let small = submit(
        &core,
        job("small", 2, 1),
        Placement::default(),
        None,
        vec![],
    )
    .await;
    let waits = submit(
        &core,
        job("waits", 4, 1),
        Placement::default(),
        None,
        vec![],
    )
    .await;
    let s = wait_for(&core, &small, "the small job", |j| j.state.finished()).await;
    let b = wait_for(&core, &big, "the big job", |j| j.state.finished()).await;
    let w = wait_for(&core, &waits, "the waiting job", |j| j.state.finished()).await;
    assert!(
        s.assigned_at.unwrap() < b.finished_at.unwrap(),
        "the 2-thread job ran beside the 6-thread one"
    );
    assert!(
        w.assigned_at.unwrap() >= b.finished_at.unwrap(),
        "the 4-thread job waited: {w:?} {b:?}"
    );
    let log = |id: &str| {
        std::fs::read_to_string(f.workers[0].home.join("jobs").join(id).join("stdout.log")).unwrap()
    };
    assert_eq!(log(&big), "threads=6\n");
    assert_eq!(log(&waits), "threads=4\n");
    let request = DaemonRequest::Submit {
        job: Box::new(job("huge", 9, 0)),
        placement: Placement::default(),
        dest: None,
        after: vec![],
        priority: 0,
    };
    let DaemonResponse::Submitted { job: huge, .. } = core.handle(request).await else {
        panic!()
    };
    let stuck = wait_for(&core, &huge, "the slot note", |j| {
        j.error
            .as_deref()
            .is_some_and(|e| e.contains("needs 9 CPU threads"))
    })
    .await;
    assert_eq!(stuck.state, QState::Queued);
    f.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn submission_keys_are_atomic_persistent_and_check_content() {
    let f = Fleet::new("submit-once", &[]).await;
    let core = Core::for_tests(&f.home(), f.coordinator().await).unwrap();
    let source = f.dir.join("input");
    std::fs::write(&source, "first").unwrap();
    let mut job = f.job("once", "true");
    job.inputs.insert("input".into(), source.clone());
    let request = DaemonRequest::SubmitOnce {
        key: "run/phase-1".into(),
        job: Box::new(job),
        placement: Placement::default(),
        dest: None,
        after: vec![],
        priority: 0,
    };
    let (a, b) = tokio::join!(core.handle(request.clone()), core.handle(request.clone()));
    let id = match (a, b) {
        (DaemonResponse::Submitted { job: a, .. }, DaemonResponse::Submitted { job: b, .. }) => {
            assert_eq!(a, b);
            a
        }
        other => panic!("{other:?}"),
    };
    assert_eq!(core.queue().unwrap().len(), 1);
    core.coordinator.endpoint.close().await;
    core.coordinator.store.shutdown().await.unwrap();
    drop(core);
    let core = Core::for_tests(&f.home(), f.coordinator().await).unwrap();
    assert!(
        matches!(core.handle(request.clone()).await, DaemonResponse::Submitted {job, ..} if job == id)
    );
    assert!(
        matches!(core.handle(DaemonRequest::FindSubmission { key: "run/phase-1".into() }).await,
        DaemonResponse::Submission { job: Some(job) } if job == id)
    );
    std::fs::write(source, "changed in place").unwrap();
    assert!(
        matches!(core.handle(request).await, DaemonResponse::Error {message, ..} if message.contains("different specification"))
    );
    assert_eq!(core.queue().unwrap().len(), 1);
    core.coordinator.endpoint.close().await;
    f.finish().await;
}

#[path = "daemon/vm_jobs.rs"]
mod vm_jobs;
