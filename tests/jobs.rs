//! Jobs end to end: an in-process agent (whose supervisors are real `fleet supervise` processes) and an in-process
//! coordinator over localhost, with relays and address lookup off.

use std::{
    collections::{BTreeMap, HashSet},
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use fleet::{
    agent::{Agent, AgentConfig},
    blobs,
    coordinator::{Coordinator, JobFile},
    jobs::{EndReason, JobInfo, State},
    proto::{ALPN, Hello, Request, Response},
};
use iroh::{Endpoint, EndpointAddr, RelayMode, SecretKey, endpoint::presets, protocol::Router};

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "fleet-it-{name}-{}-{:08x}",
        std::process::id(),
        rand::random::<u32>()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

async fn endpoint(key: SecretKey) -> Endpoint {
    Endpoint::builder(presets::Minimal)
        .relay_mode(RelayMode::Disabled)
        .secret_key(key)
        .bind()
        .await
        .unwrap()
}

fn hello() -> Hello {
    Hello {
        agent_version: "test".into(),
        protocol: fleet::proto::PROTOCOL,
        hostname: "worker".into(),
        os: "macOS".into(),
        cpu: "test".into(),
        cores: 1,
        memory_bytes: 0,
        gpu: None,
        tags: vec!["os:macos".into()],
        sleep_minutes: None,
        cpu_slots: 8,
    }
}

struct Setup {
    agent: Router,
    addr: EndpointAddr,
    coordinator: Coordinator,
    dir: PathBuf,
}

async fn setup(name: &str) -> Setup {
    let dir = temp_dir(name);
    let key = SecretKey::generate();
    let allow = Arc::new(HashSet::from([key.public()]));
    let home = dir.join("agent-home");
    let store = blobs::open(&home.join("agent-blobs")).await.unwrap();
    let config = AgentConfig {
        home,
        supervisor: PathBuf::from(env!("CARGO_BIN_EXE_fleet")),
        store: store.clone(),
    };
    let agent = Router::builder(endpoint(SecretKey::generate()).await)
        .accept(ALPN, Agent::start(allow.clone(), hello(), config).unwrap())
        .accept(iroh_blobs::ALPN, blobs::protocol(&store, allow))
        .spawn();
    let addr = agent.endpoint().addr();
    let coordinator = Coordinator::new(
        endpoint(key).await,
        blobs::open(&dir.join("coordinator-blobs")).await.unwrap(),
    );
    Setup {
        agent,
        addr,
        coordinator,
        dir,
    }
}

impl Setup {
    fn job(&self, name: &str, argv: &[&str]) -> JobFile {
        JobFile {
            vm: None,
            name: name.into(),
            argv: argv.iter().map(|s| s.to_string()).collect(),
            env: BTreeMap::new(),
            inputs: BTreeMap::new(),
            publish: Vec::new(),
            settle_seconds: Some(0),
            max_seconds: None,
            shutdown_grace_seconds: None,
            keep_awake: Some(false),
            probes: Vec::new(),
            pause_keeps_gpu: None,
            resume: None,
            preempt: None,
            threads: None,
        }
    }

    fn write(&self, name: &str, contents: &str) -> PathBuf {
        let path = self.dir.join(name);
        std::fs::write(&path, contents).unwrap();
        path
    }

    async fn info(&self, job: &str) -> JobInfo {
        let connection = self.coordinator.connect(self.addr.clone()).await.unwrap();
        let jobs = self.coordinator.jobs(&connection).await.unwrap();
        jobs.into_iter()
            .find(|j| j.id == job)
            .expect("the job is listed")
    }

    async fn wait(&self, job: &str, until: impl Fn(State) -> bool) -> JobInfo {
        let start = Instant::now();
        loop {
            let info = self.info(job).await;
            if until(info.state) {
                return info;
            }
            assert!(
                start.elapsed() < Duration::from_secs(30),
                "timed out waiting: {info:?}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn logs(&self, job: &str, stderr: bool) -> String {
        let connection = self.coordinator.connect(self.addr.clone()).await.unwrap();
        let request = Request::Logs {
            job: job.into(),
            stderr,
            from: 0,
            max: 1 << 16,
        };
        match self.coordinator.ask(&connection, &request).await.unwrap() {
            Response::Logs { data, .. } => data,
            other => panic!("{other:?}"),
        }
    }

    async fn finish(self) {
        self.coordinator.endpoint.close().await;
        self.agent.shutdown().await.unwrap();
        std::fs::remove_dir_all(&self.dir).ok();
    }
}

#[tokio::test]
async fn a_job_runs_and_its_outputs_come_back_verified() {
    let s = setup("lifecycle").await;
    let input = s.write("input.txt", "hello fleet\n");
    let mut job = s.job(
        "copy",
        &[
            "/bin/sh",
            "-c",
            "mkdir -p out && cp input.txt out/copy.txt && echo ran && echo note >&2",
        ],
    );
    job.inputs.insert("input.txt".into(), input.clone());
    job.publish = vec!["out/*".into()];
    let id = s.coordinator.submit(s.addr.clone(), &job).await.unwrap();

    let info = s.wait(&id, State::finished).await;
    assert_eq!(info.state, State::Succeeded, "{info:?}");
    assert_eq!(s.logs(&id, false).await, "ran\n");
    assert_eq!(s.logs(&id, true).await, "note\n");

    let connection = s.coordinator.connect(s.addr.clone()).await.unwrap();
    let outputs = s.coordinator.outputs(&connection, &id).await.unwrap();
    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0].path, "out/copy.txt");
    assert_eq!(
        outputs[0].hash,
        blobs::hash_file(&input).unwrap(),
        "same content, same hash"
    );

    let dest = s.dir.join("fetched");
    let report = s
        .coordinator
        .fetch(s.addr.clone(), &id, &dest)
        .await
        .unwrap();
    assert_eq!(report.fetched, vec!["out/copy.txt".to_string()]);
    assert_eq!(
        std::fs::read_to_string(dest.join("out/copy.txt")).unwrap(),
        "hello fleet\n"
    );

    let again = s
        .coordinator
        .fetch(s.addr.clone(), &id, &dest)
        .await
        .unwrap();
    assert!(
        again.fetched.is_empty() && again.skipped == vec!["out/copy.txt".to_string()],
        "{again:?}"
    );

    std::fs::write(dest.join("out/copy.txt"), "changed\n").unwrap();
    let error = s
        .coordinator
        .fetch(s.addr.clone(), &id, &dest)
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("refusing to overwrite"),
        "{error:#}"
    );
    assert_eq!(
        std::fs::read_to_string(dest.join("out/copy.txt")).unwrap(),
        "changed\n",
        "untouched"
    );
    s.finish().await;
}

#[tokio::test]
async fn inputs_are_pushed_once_and_keep_their_executable_bit() {
    let s = setup("inputs").await;
    let script = s.write("run.sh", "#!/bin/sh\necho from script\n");
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
    let mut job = s.job("script", &["bin/run.sh"]);
    job.inputs.insert("bin/run.sh".into(), script.clone());
    let first = s.coordinator.submit(s.addr.clone(), &job).await.unwrap();
    assert_eq!(
        s.wait(&first, State::finished).await.state,
        State::Succeeded
    );
    assert_eq!(s.logs(&first, false).await, "from script\n");

    let connection = s.coordinator.connect(s.addr.clone()).await.unwrap();
    let hash = blobs::hash_file(&script).unwrap();
    let answer = s
        .coordinator
        .ask(&connection, &Request::Missing { hashes: vec![hash] })
        .await
        .unwrap();
    assert_eq!(
        answer,
        Response::Missing(vec![]),
        "the agent already has it, so a resubmission pushes nothing"
    );
    let second = s.coordinator.submit(s.addr.clone(), &job).await.unwrap();
    assert_ne!(first, second);
    assert_eq!(
        s.wait(&second, State::finished).await.state,
        State::Succeeded
    );
    s.finish().await;
}

#[tokio::test]
async fn cancellation_and_time_limits_stop_jobs() {
    let s = setup("stop").await;
    let cancel = s
        .coordinator
        .submit(s.addr.clone(), &s.job("sleeper", &["/bin/sleep", "60"]))
        .await
        .unwrap();
    s.wait(&cancel, |st| st == State::Running).await;
    let connection = s.coordinator.connect(s.addr.clone()).await.unwrap();
    assert_eq!(
        s.coordinator
            .ask(
                &connection,
                &Request::Cancel {
                    job: cancel.clone()
                }
            )
            .await
            .unwrap(),
        Response::Cancelled
    );
    let info = s.wait(&cancel, State::finished).await;
    assert_eq!(info.state, State::Cancelled, "{info:?}");

    let mut limited = s.job("limited", &["/bin/sleep", "60"]);
    limited.max_seconds = Some(1);
    let id = s
        .coordinator
        .submit(s.addr.clone(), &limited)
        .await
        .unwrap();
    let info = s.wait(&id, State::finished).await;
    assert_eq!(info.state, State::TimedOut, "{info:?}");
    assert_eq!(info.exit.unwrap().reason, EndReason::TimeLimit);
    s.finish().await;
}

#[tokio::test]
async fn a_command_that_cannot_start_fails_cleanly() {
    let s = setup("nostart").await;
    let id = s
        .coordinator
        .submit(s.addr.clone(), &s.job("missing", &["/nonexistent/program"]))
        .await
        .unwrap();
    let info = s.wait(&id, State::finished).await;
    assert_eq!(info.state, State::Failed);
    assert_eq!(info.exit.unwrap().reason, EndReason::StartFailed);
    assert!(s.logs(&id, true).await.contains("could not start"));
    s.finish().await;
}

#[tokio::test]
async fn unsafe_paths_and_strangers_are_refused() {
    let s = setup("refuse").await;
    let input = s.write("x.txt", "x");
    let mut job = s.job("escape", &["/bin/true"]);
    job.inputs.insert("../outside.txt".into(), input.clone());
    assert!(s.coordinator.submit(s.addr.clone(), &job).await.is_err());

    let stranger = Coordinator::new(
        endpoint(SecretKey::generate()).await,
        blobs::open(&s.dir.join("stranger-blobs")).await.unwrap(),
    );
    let hash = stranger.store.blobs().add_path(&input).await.unwrap().hash;
    let pushed = blobs::push(&stranger.store, &stranger.endpoint, s.addr.clone(), &[hash]).await;
    // A refused push is not always reported to the pusher, so check the agent's store directly.
    tokio::time::sleep(Duration::from_millis(500)).await;
    drop(pushed);
    let connection = s.coordinator.connect(s.addr.clone()).await.unwrap();
    let answer = s
        .coordinator
        .ask(&connection, &Request::Missing { hashes: vec![hash] })
        .await
        .unwrap();
    assert_eq!(
        answer,
        Response::Missing(vec![hash]),
        "the agent's store refused the stranger's push"
    );
    stranger.endpoint.close().await;
    s.finish().await;
}

#[tokio::test]
async fn repeated_worker_submission_starts_one_process_and_rejects_conflicts() {
    let s = setup("retry-worker").await;
    let job = s.job("one", &["/bin/sh", "-c", "echo started; sleep 0.2"]);
    let (a, b) = tokio::join!(
        s.coordinator
            .submit_as(s.addr.clone(), &job, Some("one-stable-id".into())),
        s.coordinator
            .submit_as(s.addr.clone(), &job, Some("one-stable-id".into()))
    );
    assert_eq!(a.unwrap(), b.unwrap());
    s.wait("one-stable-id", State::finished).await;
    assert_eq!(s.logs("one-stable-id", false).await, "started\n");
    // A coordinator upgraded to 3.3 may retry an older submission with an
    // allocation declaration. Preserve the old conservative claim and process.
    let spec = s.coordinator.prepare(&job).await.unwrap();
    assert_eq!(
        s.coordinator
            .submit_spec_allocated(
                s.addr.clone(),
                spec,
                Some("one-stable-id".into()),
                Some(false),
            )
            .await
            .unwrap(),
        "one-stable-id"
    );
    assert_eq!(s.logs("one-stable-id", false).await, "started\n");
    let other = s.job("one", &["/bin/echo", "different"]);
    assert!(
        s.coordinator
            .submit_as(s.addr.clone(), &other, Some("one-stable-id".into()))
            .await
            .is_err()
    );
    s.finish().await;
}
