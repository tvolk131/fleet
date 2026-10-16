//! `fleet supervise <job folder>`: runs one job, detached from the agent.
//!
//! The agent starts this process in its own process group, so a restarting agent leaves it running. It starts the
//! job's command in another new process group (so cancellation reaches everything the job started), holds a
//! sleep-prevention assertion for the job's lifetime (`caffeinate -i -w <pid>`), enforces `max_seconds`, and writes
//! `exit.json`.

use std::{
    fs,
    os::unix::{fs::PermissionsExt, process::CommandExt},
    path::{Path, PathBuf},
    process::{Command, ExitStatus, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result};

use crate::instances::write as write_json;
use crate::jobs::{EndReason, Exit, Job, Started, now, read_json};

/// How long a job gets between SIGTERM and SIGKILL.
const DEFAULT_GRACE_SECONDS: u64 = 10;

fn group_alive(pgid: u32) -> bool {
    let result = unsafe { libc::kill(-(pgid as i32), 0) };
    result == 0 || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

pub fn signal_group(pgid: u32, signal: i32) {
    unsafe {
        libc::kill(-(pgid as i32), signal);
    }
}

pub fn run(dir: &Path) -> Result<()> {
    let job: Job = read_json(&dir.join("job.json"))?.context("no job.json")?;
    if job.spec.vm.is_some() {
        return crate::vm_job::run(dir);
    }
    // The lock prevents concurrent supervisors; the durable marker prevents re-execution after
    // a supervisor dies in the spawn/record gap. Such a launch is uncertain, never blindly retried.
    let Some(_lock) = crate::instances::try_lock(&dir.join("native-supervisor.lock"))? else {
        return Ok(());
    };
    if dir.join("exit.json").exists() {
        return Ok(());
    }
    anyhow::ensure!(
        !dir.join("launch-attempt.json").exists(),
        "native launch already attempted; reconcile uncertain execution instead of relaunching"
    );
    if dir.join("cancel").exists() {
        return write_json(
            &dir.join("exit.json"),
            &Exit {
                code: None,
                signal: None,
                reason: EndReason::Cancelled,
                finished_at: now(),
            },
        );
    }
    write_json(&dir.join("launch-attempt.json"), &now())?;
    let work = dir.join("work");
    let program = {
        let first = &job.spec.argv[0];
        if first.contains('/') && !Path::new(first).is_absolute() {
            work.join(first)
        } else {
            PathBuf::from(first)
        }
    };
    let mut command = Command::new(&program);
    command
        .args(&job.spec.argv[1..])
        .current_dir(&work)
        .envs(
            ["RAYON_NUM_THREADS", "OMP_NUM_THREADS"]
                .into_iter()
                .filter(|k| !job.spec.env.contains_key(*k))
                .map(|k| (k.to_string(), job.spec.threads.to_string())),
        )
        .env("FLEET_THREADS", job.spec.threads.to_string())
        .envs(&job.spec.env)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0);
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(e) => {
            let _ = crate::history::append(
                dir,
                crate::history::Stream::Stderr,
                format!("fleet: could not start {}: {e}\n", program.display()).as_bytes(),
            );
            let _ = crate::history::close(dir, crate::history::Stream::Stderr);
            let _ = crate::history::close(dir, crate::history::Stream::Stdout);
            return write_json(
                &dir.join("exit.json"),
                &Exit {
                    code: None,
                    signal: None,
                    reason: EndReason::StartFailed,
                    finished_at: now(),
                },
            );
        }
    };
    let capture_stopped = Arc::new(AtomicBool::new(false));
    let out = child.stdout.take().context("missing child stdout")?;
    let err = child.stderr.take().context("missing child stderr")?;
    let capture_out = {
        let dir = dir.to_owned();
        let stop = capture_stopped.clone();
        std::thread::spawn(move || {
            crate::history::collect(out, &dir, crate::history::Stream::Stdout, stop)
        })
    };
    let capture_err = {
        let dir = dir.to_owned();
        let stop = capture_stopped.clone();
        std::thread::spawn(move || {
            crate::history::collect(err, &dir, crate::history::Stream::Stderr, stop)
        })
    };
    let pid = child.id();
    let hold_awake = |pid: u32| {
        Command::new("/usr/bin/caffeinate")
            .args(["-i", "-w", &pid.to_string()])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .ok()
    };
    let mut awake = if job.spec.keep_awake {
        hold_awake(pid)
    } else {
        None
    };
    write_json(
        &dir.join("supervisor.json"),
        &Started {
            supervisor_pid: std::process::id(),
            child_pid: pid,
            started_at: now(),
        },
    )?;

    let limit = job.spec.max_seconds.map(Duration::from_secs);
    // Running time, which excludes paused time (for the time limit).
    let mut ran = Duration::ZERO;
    let mut last = Instant::now();
    let mut was_paused = false;
    let mut terminated_at: Option<Instant> = None;
    let mut reason = EndReason::Exited;
    let grace_seconds = job
        .spec
        .shutdown_grace_seconds
        .unwrap_or(DEFAULT_GRACE_SECONDS);
    let grace = Duration::from_secs(grace_seconds);
    let mut leader_status: Option<ExitStatus> = None;
    let status: ExitStatus = loop {
        let paused = dir.join("paused").exists();
        if !paused {
            ran += last.elapsed();
        }
        last = Instant::now();
        if paused != was_paused && job.spec.keep_awake {
            if paused {
                if let Some(mut c) = awake.take() {
                    let _ = c.kill();
                    let _ = c.wait();
                }
            } else {
                awake = hold_awake(pid);
            }
        }
        was_paused = paused;
        // Latch the request before reaping: a clean, immediate exit still has the requested reason.
        if terminated_at.is_none() {
            if dir.join("cancel").exists() {
                reason = EndReason::Cancelled;
            } else if dir.join("suspend").exists() {
                reason = EndReason::Suspended;
            } else if limit.is_some_and(|l| ran >= l) {
                reason = EndReason::TimeLimit;
            }
        }
        if leader_status.is_none() {
            leader_status = child.try_wait()?;
        }
        let remaining = group_alive(pid);
        if let Some(status) = leader_status
            && !remaining
        {
            break status;
        }
        // Also clean up children left behind by a wrapper which exited normally.
        if terminated_at.is_none() && (reason != EndReason::Exited || leader_status.is_some()) {
            let at = now();
            write_json(
                &dir.join("stopping.json"),
                &crate::jobs::Stopping {
                    reason,
                    started_at: at,
                    deadline: at.saturating_add(grace_seconds),
                },
            )?;
            let _ = fs::remove_file(dir.join("paused"));
            signal_group(pid, libc::SIGTERM);
            signal_group(pid, libc::SIGCONT);
            terminated_at = Some(Instant::now());
        }
        if terminated_at.is_some_and(|t| t.elapsed() >= grace) {
            signal_group(pid, libc::SIGKILL);
            // Reap the leader. Descendants can remain as zombies until their new parent reaps them;
            // after SIGKILL they cannot execute or keep writing outputs.
            break match leader_status {
                Some(status) => status,
                None => child.wait()?,
            };
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    if let Some(mut c) = awake {
        let _ = c.kill();
        let _ = c.wait();
    }
    capture_stopped.store(true, Ordering::Relaxed);
    for reader in [capture_out, capture_err] {
        match reader.join() {
            Ok(Ok(())) => {}
            other => tracing::warn!(?other, "job log capture incomplete"),
        }
    }
    let _ = fs::remove_file(dir.join("paused"));
    use std::os::unix::process::ExitStatusExt;
    write_json(
        &dir.join("exit.json"),
        &Exit {
            code: status.code(),
            signal: status.signal(),
            reason,
            finished_at: now(),
        },
    )
}

/// Marks an input executable (`chmod 755`).
pub fn make_executable(path: &Path) -> Result<()> {
    fs::set_permissions(path, fs::Permissions::from_mode(0o755))?;
    Ok(())
}
