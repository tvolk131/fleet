//! Real process tests for termination; no network or live Fleet installation is used.
use fleet::jobs::{self, EndReason, Exit, Started, Stopping};
use std::{
    fs,
    path::PathBuf,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

struct Job {
    dir: PathBuf,
    child: Child,
}
impl Job {
    fn start(script: &str, grace: Option<u64>, limit: Option<u64>) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "fleet-stop-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ));
        fs::create_dir_all(dir.join("work")).unwrap();

        fs::write(dir.join("job.json"), serde_json::to_vec(&serde_json::json!({
            "id": "test", "submitter": "test", "submitted_at": jobs::now(),
            "spec": { "name": "test", "argv": [env!("CARGO_BIN_EXE_fleet-test-workload"), script], "keep_awake": false,
                      "shutdown_grace_seconds": grace, "max_seconds": limit }
        })).unwrap()).unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_fleet"))
            .args(["supervise", dir.to_str().unwrap()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let job = Self { dir, child };
        job.until(|| job.dir.join("work/ready").exists());
        job
    }
    fn until(&self, predicate: impl Fn() -> bool) {
        let start = Instant::now();
        while !predicate() {
            assert!(start.elapsed() < Duration::from_secs(15));
            std::thread::sleep(Duration::from_millis(20));
        }
    }
    fn stop(&self, marker: &str) {
        fs::write(self.dir.join(marker), "requested").unwrap();
    }
    fn finish(&mut self) -> Exit {
        self.until(|| self.dir.join("exit.json").exists());
        assert!(self.child.wait().unwrap().success());
        jobs::read_json(&self.dir.join("exit.json"))
            .unwrap()
            .unwrap()
    }
}
impl Drop for Job {
    fn drop(&mut self) {
        if let Ok(Some(s)) = jobs::read_json::<Started>(&self.dir.join("supervisor.json")) {
            fleet::supervise::signal_group(s.child_pid, libc::SIGKILL);
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = fs::remove_dir_all(&self.dir);
    }
}
const GRACEFUL: &str = "graceful";
const STUBBORN: &str = "stubborn";

#[test]
fn graceful_stop_preserves_each_reason_and_sends_one_term() {
    for (marker, limit, reason) in [
        (Some("cancel"), None, EndReason::Cancelled),
        (Some("suspend"), None, EndReason::Suspended),
        (None, Some(1), EndReason::TimeLimit),
    ] {
        let mut j = Job::start(GRACEFUL, Some(2), limit);
        if let Some(marker) = marker {
            j.stop(marker);
            j.stop(marker);
        }
        let exit = j.finish();
        assert_eq!(exit.reason, reason);
        assert_eq!(exit.code, Some(0));
        assert!(j.dir.join("work/flushed").exists());
        assert_eq!(
            fs::read_to_string(j.dir.join("work/terms")).unwrap(),
            "term\n"
        );
        let stop: Stopping = jobs::read_json(&j.dir.join("stopping.json"))
            .unwrap()
            .unwrap();
        assert_eq!(stop.deadline - stop.started_at, 2);
    }
}
#[test]
fn each_cause_escalates_and_repeated_requests_do_not_extend_the_deadline() {
    for (marker, limit, reason) in [
        (Some("cancel"), None, EndReason::Cancelled),
        (Some("suspend"), None, EndReason::Suspended),
        (None, Some(1), EndReason::TimeLimit),
    ] {
        let mut j = Job::start(STUBBORN, Some(1), limit);
        let at = Instant::now();
        if let Some(marker) = marker {
            j.stop(marker);
            std::thread::sleep(Duration::from_millis(700));
            j.stop(marker);
        }
        let exit = j.finish();
        assert_eq!(exit.reason, reason);
        assert_eq!(exit.signal, Some(libc::SIGKILL));
        assert!(at.elapsed() >= Duration::from_millis(900));
        assert!(at.elapsed() < Duration::from_secs(4));
    }
}
#[test]
fn paused_jobs_can_flush_and_legacy_default_is_ten_seconds() {
    let mut j = Job::start(GRACEFUL, None, None);
    let started: Started = jobs::read_json(&j.dir.join("supervisor.json"))
        .unwrap()
        .unwrap();
    fleet::supervise::signal_group(started.child_pid, libc::SIGSTOP);
    fs::write(j.dir.join("paused"), "1").unwrap();
    j.stop("cancel");
    assert_eq!(j.finish().code, Some(0));
    let stop: Stopping = jobs::read_json(&j.dir.join("stopping.json"))
        .unwrap()
        .unwrap();
    assert_eq!(stop.deadline - stop.started_at, 10);
    assert!(j.dir.join("work/flushed").exists());
}
#[test]
fn wrapper_exit_does_not_abandon_its_stubborn_descendant() {
    let script = "descendant";
    let mut j = Job::start(script, Some(1), None);
    j.stop("cancel");
    let at = Instant::now();
    assert_eq!(j.finish().reason, EndReason::Cancelled);
    assert!(at.elapsed() >= Duration::from_millis(900));
    let heartbeat = fs::read(j.dir.join("work/heartbeat")).unwrap();
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(fs::read(j.dir.join("work/heartbeat")).unwrap(), heartbeat);
}
#[test]
fn custom_grace_is_version_gated_including_zero() {
    let old = fleet::proto::Protocol { major: 3, minor: 1 };
    for seconds in [0, 10, 120] {
        assert!(
            fleet::proto::job_too_new(&serde_json::json!({"shutdown_grace_seconds": seconds}), old)
                .is_some()
        );
    }
    assert!(fleet::proto::job_too_new(&serde_json::json!({}), old).is_none());
}
