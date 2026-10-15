#![cfg(feature = "legacy")]
//! fleet as real processes: a `fleet agent` and a `fleet daemon` started from the built binary, driven through the CLI,
//! fully offline (`FLEET_NETWORK=local`: no relays, no DNS; the agent's address is in the machines file).

use std::{
    net::UdpSocket,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

const BIN: &str = env!("CARGO_BIN_EXE_fleet-legacy");

struct Homes {
    root: PathBuf,
    agent: PathBuf,
    coordinator: PathBuf,
    port: u16,
}

impl Homes {
    fn new(name: &str) -> Self {
        let root = std::env::temp_dir().join(format!(
            "fleet-p-{name}-{}-{:06x}",
            std::process::id(),
            rand::random::<u32>() & 0xffffff
        ));
        std::fs::create_dir_all(&root).unwrap();
        // A free UDP port for the agent (released just before the agent binds it).
        let port = UdpSocket::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        Homes {
            agent: root.join("agent"),
            coordinator: root.join("coordinator"),
            root,
            port,
        }
    }

    fn command(&self, home: &Path, args: &[&str]) -> Command {
        let mut c = Command::new(BIN);
        c.args(args)
            .env("FLEET_HOME", home)
            .env("FLEET_NETWORK", "local")
            .env("RUST_LOG", "warn");
        c
    }

    /// Runs a CLI command as the coordinator and returns its standard output.
    fn fleet(&self, args: &[&str]) -> String {
        let out = self.command(&self.coordinator, args).output().unwrap();
        assert!(
            out.status.success(),
            "fleet {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8(out.stdout).unwrap()
    }

    fn json(&self, args: &[&str]) -> serde_json::Value {
        serde_json::from_str(&self.fleet(args)).unwrap()
    }

    fn start_agent(&self) -> Child {
        let log = std::fs::File::create(self.root.join("agent.log")).unwrap();
        self.command(&self.agent, &["agent"])
            .env("FLEET_PORT", self.port.to_string())
            .stdout(Stdio::null())
            .stderr(log)
            .spawn()
            .unwrap()
    }

    fn start_daemon(&self) -> Child {
        let log = std::fs::File::create(self.root.join("daemon.log")).unwrap();
        self.command(&self.coordinator, &["daemon"])
            .stdout(Stdio::null())
            .stderr(log)
            .spawn()
            .unwrap()
    }

    fn wait(&self, what: &str, mut until: impl FnMut() -> bool) {
        let start = Instant::now();
        while !until() {
            if start.elapsed() > Duration::from_secs(60) {
                let queue = self
                    .command(&self.coordinator, &["queue", "--json"])
                    .output()
                    .unwrap();
                let read = |f: &str| std::fs::read_to_string(self.root.join(f)).unwrap_or_default();
                panic!(
                    "timed out waiting for {what}\nqueue: {}\n--- daemon.log\n{}\n--- agent.log\n{}\n--- follower.stdout\n{}\n--- follower.stderr\n{}",
                    String::from_utf8_lossy(&queue.stdout),
                    read("daemon.log"),
                    read("agent.log"),
                    read("follower.stdout"),
                    read("follower.stderr")
                );
            }
            std::thread::sleep(Duration::from_millis(250));
        }
    }

    fn queued(&self, id: &str) -> serde_json::Value {
        let queue = self.json(&["queue", "--json"]);
        queue
            .as_array()
            .unwrap()
            .iter()
            .find(|j| j["id"] == id)
            .cloned()
            .expect("queued")
    }
}

struct Running(Vec<Child>);

impl Drop for Running {
    fn drop(&mut self) {
        for child in &mut self.0 {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// Pairs a coordinator with an agent (named `local`) and starts both, waiting until the machine is online.
fn start(h: &Homes) -> Running {
    let coordinator = h.fleet(&["id"]);
    let out = h
        .command(&h.agent, &["allow", coordinator.trim()])
        .output()
        .unwrap();
    assert!(out.status.success());
    let agent_id = String::from_utf8(
        h.command(&h.agent, &["id", "--role", "agent"])
            .output()
            .unwrap()
            .stdout,
    )
    .unwrap();
    h.fleet(&[
        "machine",
        "add",
        "local",
        agent_id.trim(),
        "--addr",
        &format!("127.0.0.1:{}", h.port),
    ]);
    let running = Running(vec![h.start_agent(), h.start_daemon()]);
    // CLI reads can fall back to an in-process coordinator before the daemon
    // binds. Confirm the daemon is serving before treating those reads as readiness.
    h.wait("the daemon socket to accept connections", || {
        std::os::unix::net::UnixStream::connect(fleet::daemon::socket_path(&h.coordinator)).is_ok()
    });
    h.wait("the machine to come online", || {
        h.command(&h.coordinator, &["machines", "--json"])
            .output()
            .ok()
            .and_then(|o| serde_json::from_slice::<serde_json::Value>(&o.stdout).ok())
            .is_some_and(|m| m[0]["online"] == true && !m[0]["status"].is_null())
    });
    running
}

#[test]
fn run_ships_named_files_and_sweeps_fan_out() {
    let h = Homes::new("run");
    let running = start(&h);
    // `fleet run --follow` from a folder with a script in it: the script is shipped, its output streamed, its exit
    // code returned.
    std::fs::write(h.root.join("hello.sh"), "#!/bin/sh\necho \"hello $1\"\n").unwrap();
    std::fs::set_permissions(
        h.root.join("hello.sh"),
        std::os::unix::fs::PermissionsExt::from_mode(0o755),
    )
    .unwrap();
    let out = h
        .command(
            &h.coordinator,
            &["run", "--follow", "--", "./hello.sh", "fleet"],
        )
        .current_dir(&h.root)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(
        String::from_utf8_lossy(&out.stdout).lines().last(),
        Some("hello fleet")
    );
    let failed = h
        .command(
            &h.coordinator,
            &["run", "--follow", "--", "/bin/sh", "-c", "exit 3"],
        )
        .output()
        .unwrap();
    assert_eq!(
        failed.status.code(),
        Some(3),
        "the job's exit code comes back"
    );

    // A sweep: one job per value, each fetching into its own folder.
    let template = serde_json::json!({
        "name": "square", "argv": ["/bin/sh", "-c", "mkdir -p out && echo $(({n} * {n})) > out/square.txt"],
        "publish": ["out/*"], "settle_seconds": 0, "keep_awake": false
    });
    std::fs::write(h.root.join("sweep.json"), template.to_string()).unwrap();
    let dest = h.root.join("squares");
    let made = h.json(&[
        "sweep",
        h.root.join("sweep.json").to_str().unwrap(),
        "--param",
        "n=3,4",
        "--dest",
        dest.to_str().unwrap(),
        "--json",
    ]);
    let ids: Vec<String> = made
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["job"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(ids.len(), 2);
    for id in &ids {
        h.wait("each square", || {
            let j = h.queued(id);
            j["state"] == "succeeded" && j["fetched_files"] == 1
        });
    }
    assert_eq!(
        std::fs::read_to_string(dest.join("n-3/out/square.txt")).unwrap(),
        "9\n"
    );
    assert_eq!(
        std::fs::read_to_string(dest.join("n-4/out/square.txt")).unwrap(),
        "16\n"
    );
    drop(running);
    std::fs::remove_dir_all(&h.root).ok();
}

#[test]
fn a_follower_waits_out_a_daemon_restart_without_taking_its_key() {
    let h = Homes::new("follow");
    let mut running = start(&h);
    let follower_log = h.root.join("follower.stderr");
    let follower_stdout = h.root.join("follower.stdout");
    let mut follower = Running(vec![
        h.command(
            &h.coordinator,
            &[
                "run",
                "--follow",
                "--",
                "/bin/sh",
                "-c",
                "echo following; while [ ! -e release ]; do sleep 0.1; done; echo finished",
            ],
        )
        .stdout(std::fs::File::create(&follower_stdout).unwrap())
        .stderr(std::fs::File::create(&follower_log).unwrap())
        .spawn()
        .unwrap(),
    ]);
    // A running queue entry alone does not prove the submitting CLI has entered
    // follow mode: stopping the daemon before its initial ping selects direct mode.
    h.wait("the follower to receive the first log line", || {
        std::fs::read_to_string(&follower_stdout)
            .unwrap()
            .lines()
            .any(|line| line == "following")
    });
    // The daemon restarts (stopped as launchd stops it) while the follower is polling.
    unsafe {
        libc::kill(running.0[1].id() as i32, libc::SIGTERM);
    }
    running.0[1].wait().unwrap();
    // Keep the job alive until the follower has actually observed the outage: timing alone flakes under load.
    h.wait("the follower to observe the outage", || {
        std::fs::read_to_string(&follower_log)
            .unwrap()
            .contains("waiting for it to come back")
    });
    running.0[1] = h.start_daemon();
    let queue = h.json(&["queue", "--json"]);
    let id = queue[0]["id"].as_str().unwrap();
    std::fs::write(h.agent.join("jobs").join(id).join("work/release"), "go").unwrap();
    let status = follower.0[0].wait().unwrap();
    let stderr = std::fs::read_to_string(&follower_log).unwrap();
    assert!(status.success(), "{stderr}");
    assert_eq!(
        std::fs::read_to_string(&follower_stdout)
            .unwrap()
            .lines()
            .last(),
        Some("finished")
    );
    assert!(
        stderr.contains("waiting for it to come back"),
        "the follower waited for the daemon rather than act in its place: {stderr}"
    );
    drop(running);
    std::fs::remove_dir_all(&h.root).ok();
}

#[test]
fn real_processes_run_a_job_and_survive_an_agent_restart() {
    let h = Homes::new("e2e");
    let coordinator = h.fleet(&["id"]);
    let coordinator = coordinator.trim();
    let out = h
        .command(&h.agent, &["allow", coordinator])
        .output()
        .unwrap();
    assert!(out.status.success());
    let agent_id = String::from_utf8(
        h.command(&h.agent, &["id", "--role", "agent"])
            .output()
            .unwrap()
            .stdout,
    )
    .unwrap();
    let addr = format!("127.0.0.1:{}", h.port);
    h.fleet(&["machine", "add", "local", agent_id.trim(), "--addr", &addr]);

    let mut running = Running(vec![h.start_agent(), h.start_daemon()]);
    h.wait("the machine to come online", || {
        h.command(&h.coordinator, &["machines", "--json"])
            .output()
            .ok()
            .and_then(|o| serde_json::from_slice::<serde_json::Value>(&o.stdout).ok())
            .is_some_and(|m| m[0]["online"] == true && !m[0]["status"].is_null())
    });

    // A job with an input script, a progress probe and an output that comes back by itself.
    let script = h.root.join("work.sh");
    std::fs::write(
        &script,
        "#!/bin/sh\nmkdir -p out B\nfor i in 1 2 3; do echo \"{\\\"step\\\": $i}\" >> B/metrics.jsonl; sleep 1; done\necho $1 > out/answer.txt\necho done\n",
    )
    .unwrap();
    std::os::unix::fs::PermissionsExt::set_mode(
        &mut std::fs::metadata(&script).unwrap().permissions(),
        0o755,
    );
    std::fs::set_permissions(&script, std::os::unix::fs::PermissionsExt::from_mode(0o755)).unwrap();
    let job = serde_json::json!({
        "name": "process test", "argv": ["bin/work.sh", "42"], "inputs": {"bin/work.sh": script},
        "publish": ["out/*"], "settle_seconds": 0, "keep_awake": false,
        "probes": [{"jsonl_field": {"file": "B/metrics.jsonl", "field": "step", "total": 3}}]
    });
    let job_file = h.root.join("job.json");
    std::fs::write(&job_file, job.to_string()).unwrap();
    let dest = h.root.join("results");
    let submitted = h.json(&[
        "submit",
        job_file.to_str().unwrap(),
        "--dest",
        dest.to_str().unwrap(),
        "--json",
    ]);
    let id = submitted["job"].as_str().unwrap().to_string();
    h.wait("success and the fetch", || {
        let j = h.queued(&id);
        j["state"] == "succeeded" && j["fetched_files"] == 1
    });
    assert_eq!(
        std::fs::read_to_string(dest.join("out/answer.txt")).unwrap(),
        "42\n"
    );
    assert_eq!(h.fleet(&["logs", &id]), "done\n");
    // Completion/fetch and the periodic progress scan are separate event streams.
    h.wait("the final progress reading", || {
        h.queued(&id)["progress"][0]["reading"]["number"]["value"] == 3.0
    });
    let progress = &h.queued(&id)["progress"][0]["reading"]["number"];
    assert_eq!(progress["value"], 3.0, "{progress}");

    // Stop the agent process mid-job; the job lives on in its supervisor, and a new agent picks it back up. First a
    // job that can never be placed (it wants 100 threads), so the daemon asks the agent to stay awake: an agent holding
    // a keep-awake assertion and running a job is the case that used to hang on SIGTERM until the job ended.
    let blocker = serde_json::json!({
        "name": "never placed", "argv": ["/bin/sh", "-c", "true"], "threads": 100, "keep_awake": false
    });
    std::fs::write(&job_file, blocker.to_string()).unwrap();
    let blocker_id = h.json(&["submit", job_file.to_str().unwrap(), "--json"])["job"]
        .as_str()
        .unwrap()
        .to_string();
    #[cfg(target_os = "macos")]
    h.wait("the agent to hold a keep-awake assertion", || {
        Command::new("pgrep")
            .args(["-P", &running.0[0].id().to_string(), "caffeinate"])
            .output()
            .is_ok_and(|o| o.status.success())
    });
    let slow = serde_json::json!({
        "name": "outlives its agent", "argv": ["/bin/sh", "-c", "sleep 9; mkdir -p out; echo survived > out/s.txt"],
        "publish": ["out/*"], "settle_seconds": 0, "keep_awake": false
    });
    std::fs::write(&job_file, slow.to_string()).unwrap();
    let slow_id = h.json(&[
        "submit",
        job_file.to_str().unwrap(),
        "--dest",
        dest.to_str().unwrap(),
        "--json",
    ])["job"]
        .as_str()
        .unwrap()
        .to_string();
    h.wait("the slow job to start", || {
        h.queued(&slow_id)["state"] == "running"
    });
    // Stopped as launchd stops it (SIGTERM), the agent exits promptly although a job it started is still running
    // (a few seconds in: signalled at once, the agent could exit before its wait on the job's supervisor began).
    std::thread::sleep(Duration::from_secs(3));
    unsafe {
        libc::kill(running.0[0].id() as i32, libc::SIGTERM);
    }
    let stopping = Instant::now();
    while running.0[0].try_wait().unwrap().is_none() {
        assert!(
            stopping.elapsed() < Duration::from_secs(5),
            "the agent did not exit on SIGTERM while a job ran"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    std::thread::sleep(Duration::from_secs(6));
    running.0[0] = h.start_agent();
    h.wait("the replayed success and its fetch", || {
        let j = h.queued(&slow_id);
        j["state"] == "succeeded" && j["fetched_files"] == 1
    });
    assert_eq!(
        std::fs::read_to_string(dest.join("out/s.txt")).unwrap(),
        "survived\n"
    );
    h.fleet(&["cancel", &blocker_id]);

    drop(running);
    std::fs::remove_dir_all(&h.root).ok();
}
