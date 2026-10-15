//! Suspending a job at a checkpoint and continuing it later: the job's `resume` declaration, finding its latest settled
//! checkpoint, and the plan that states what suspending now would lose and when the next checkpoint is due. See
//! `docs/PAUSE_AND_SUSPEND.md`.

use std::{
    fs,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};

use crate::{
    jobs::{OutputFile, matches, safe_relative},
    probes::{ProbeValue, Reading},
};

/// How a job continues from a checkpoint.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ResumeSpec {
    /// Where checkpoints appear (files or folders), relative to the working directory; `*` within a segment.
    pub points: String,
    /// A pattern whose first capture is a checkpoint's position, read from its name (`iter-(\d+)`).
    #[serde(default)]
    pub position: Option<String>,
    /// The progress probe holding the job's current position, in the same units.
    #[serde(default)]
    pub progress: Option<String>,
    /// How far apart checkpoints are, in the same units (otherwise inferred from the checkpoints so far).
    #[serde(default)]
    pub every: Option<f64>,
    /// The continuation's command: `{latest}` is the checkpoint's path, `{segment}` the segment's number.
    pub argv: Vec<String>,
    /// Other files the continuation needs, relative to the working directory.
    #[serde(default)]
    pub carry: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PreemptMode {
    /// Suspend at the next checkpoint (no work lost).
    WhenSafe,
    /// Suspend at once if the stated loss is within `max_loss_minutes`; otherwise at the next checkpoint.
    Now,
}

/// Whether, and how, a higher-priority job may suspend this one.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct PreemptSpec {
    pub mode: PreemptMode,
    #[serde(default)]
    pub max_loss_minutes: Option<f64>,
}

impl ResumeSpec {
    pub fn validate(&self) -> anyhow::Result<()> {
        safe_relative(&self.points.replace('*', "x"))?;
        for c in &self.carry {
            safe_relative(c)?;
        }
        if let Some(p) = &self.position {
            regex::Regex::new(p)?;
        }
        if self.argv.is_empty() {
            anyhow::bail!("resume.argv is empty");
        }
        Ok(())
    }

    /// The continuation's command for a checkpoint at `latest` (relative to the working directory).
    pub fn command(&self, latest: &str, segment: u32) -> Vec<String> {
        self.argv
            .iter()
            .map(|a| {
                a.replace("{latest}", latest)
                    .replace("{segment}", &segment.to_string())
            })
            .collect()
    }
}

/// A checkpoint found in a job's working directory.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Checkpoint {
    /// Relative to the working directory.
    pub path: String,
    pub position: Option<f64>,
    /// When it last changed (seconds since the epoch).
    pub written_at: u64,
}

/// A suspended job's checkpoint, recorded by its agent: the files a continuation needs, by hash.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Recorded {
    /// None if the job was suspended before its first checkpoint: its continuation starts over.
    pub checkpoint: Option<Checkpoint>,
    /// The checkpoint's files and the carried files, relative to the working directory.
    pub files: Vec<OutputFile>,
}

fn secs(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs())
}

/// The newest modification time of a file, or of any file inside a folder.
fn newest(path: &Path) -> Option<SystemTime> {
    let meta = fs::metadata(path).ok()?;
    if !meta.is_dir() {
        return meta.modified().ok();
    }
    let mut latest = meta.modified().ok();
    for entry in fs::read_dir(path).ok()?.flatten() {
        if let Some(t) = newest(&entry.path()) {
            latest = Some(latest.map_or(t, |n| n.max(t)));
        }
    }
    latest
}

/// Paths under `work` matching `pattern` segment for segment (files or folders), relative.
pub fn matching(work: &Path, pattern: &str) -> Vec<String> {
    let depth = pattern.split('/').count();
    let mut out = Vec::new();
    let mut stack = vec![(work.to_path_buf(), String::new(), 0usize)];
    while let Some((abs, rel, level)) = stack.pop() {
        let Ok(entries) = fs::read_dir(&abs) else {
            continue;
        };
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            let child = if rel.is_empty() {
                name
            } else {
                format!("{rel}/{name}")
            };
            if level + 1 == depth {
                if matches(pattern, &child) {
                    out.push(child);
                }
            } else if entry.file_type().is_ok_and(|t| t.is_dir()) {
                stack.push((entry.path(), child, level + 1));
            }
        }
    }
    out
}

/// The job's checkpoints that have stopped changing for `settle` (all of them if `settled_anyway`), oldest first (by
/// position when known, else by time).
pub fn checkpoints(
    work: &Path,
    spec: &ResumeSpec,
    settle: Duration,
    settled_anyway: bool,
) -> Vec<Checkpoint> {
    let position = spec
        .position
        .as_deref()
        .and_then(|p| regex::Regex::new(p).ok());
    let now = SystemTime::now();
    let mut found: Vec<Checkpoint> = matching(work, &spec.points)
        .into_iter()
        .filter_map(|path| {
            let written = newest(&work.join(&path))?;
            let quiet = now.duration_since(written).unwrap_or_default() >= settle;
            if !(settled_anyway || quiet) {
                return None;
            }
            let name = path.rsplit('/').next().unwrap_or(&path);
            let position = position
                .as_ref()
                .and_then(|re| re.captures(name))
                .and_then(|c| c.get(1).or_else(|| c.get(0)))
                .and_then(|m| m.as_str().parse::<f64>().ok());
            Some(Checkpoint {
                path,
                position,
                written_at: secs(written),
            })
        })
        .collect();
    found.sort_by(|a, b| {
        a.position
            .partial_cmp(&b.position)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(a.written_at.cmp(&b.written_at))
    });
    found
}

/// Every file of a checkpoint (a file, or a folder's files) plus the carried files, relative to `work`.
pub fn files_to_carry(work: &Path, checkpoint: &str, carry: &[String]) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![PathBuf::from(checkpoint)];
    while let Some(rel) = stack.pop() {
        let abs = work.join(&rel);
        if abs.is_dir() {
            if let Ok(entries) = fs::read_dir(&abs) {
                for e in entries.flatten() {
                    stack.push(rel.join(e.file_name()));
                }
            }
        } else if abs.is_file() {
            out.push(rel);
        }
    }
    for c in carry {
        if work.join(c).is_file() {
            out.push(PathBuf::from(c));
        }
    }
    out.sort();
    out
}

/// What suspending would cost, and when it could be done without loss.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub struct Plan {
    /// The probe that measures the job's position, and its current value and rate.
    pub unit: Option<String>,
    pub current: Option<f64>,
    pub per_hour: Option<f64>,
    pub latest: Option<Checkpoint>,
    /// Suspending now: work redone, in the job's units, in wall-clock seconds since the checkpoint, and in compute
    /// seconds (units at the current rate).
    pub loss_units: Option<f64>,
    pub loss_wall_seconds: Option<u64>,
    pub loss_compute_seconds: Option<f64>,
    /// The next checkpoint, from the spacing of earlier ones, and the time to reach it at the current rate.
    pub next_position: Option<f64>,
    pub next_eta_seconds: Option<f64>,
}

pub fn plan(
    work: &Path,
    spec: &ResumeSpec,
    settle: Duration,
    probes: &[ProbeValue],
    now: u64,
) -> Plan {
    let points = checkpoints(work, spec, settle, false);
    let latest = points.last().cloned();
    let (current, per_hour) = spec
        .progress
        .as_ref()
        .and_then(|name| probes.iter().find(|p| &p.name == name))
        .and_then(|p| match &p.reading {
            Reading::Number {
                value, per_hour, ..
            } => Some((Some(*value), *per_hour)),
            _ => None,
        })
        .unwrap_or((None, None));
    let loss_units = match (current, latest.as_ref().and_then(|c| c.position)) {
        (Some(c), Some(p)) => Some((c - p).max(0.0)),
        (Some(c), None) if latest.is_none() => Some(c),
        _ => None,
    };
    let loss_wall_seconds = latest.as_ref().map(|c| now.saturating_sub(c.written_at));
    let loss_compute_seconds = match (loss_units, per_hour) {
        (Some(u), Some(r)) if r > 0.0 => Some(u / r * 3600.0),
        _ => None,
    };
    let positions: Vec<f64> = points.iter().filter_map(|c| c.position).collect();
    // The declared spacing; else the spacing of the last two; else, from a single checkpoint at p, every p from 0.
    let next_position = match (spec.every, positions.as_slice()) {
        (Some(every), [.., last]) if every > 0.0 => Some(last + every),
        (Some(every), []) if every > 0.0 => Some(every),
        (_, [.., a, b]) if b > a => Some(b + (b - a)),
        (_, [p]) if *p > 0.0 => Some(2.0 * p),
        _ => None,
    };
    let next_eta_seconds = match (next_position, current, per_hour) {
        (Some(n), Some(c), Some(r)) if r > 0.0 && n > c => Some((n - c) / r * 3600.0),
        _ => None,
    };
    Plan {
        unit: spec.progress.clone(),
        current,
        per_hour,
        latest,
        loss_units,
        loss_wall_seconds,
        loss_compute_seconds,
        next_position,
        next_eta_seconds,
    }
}

/// The plan in words: what suspending now loses, and when it can be done without loss.
pub fn describe(plan: &Plan) -> Vec<String> {
    let unit = plan.unit.as_deref().unwrap_or("position");
    let n = |v: f64| {
        if v.fract() == 0.0 {
            format!("{v:.0}")
        } else {
            format!("{v:.2}")
        }
    };
    let mut lines = Vec::new();
    match &plan.latest {
        Some(c) => lines.push(format!(
            "The latest checkpoint is {} ({}written {} ago){}.",
            c.path,
            c.position
                .map(|p| format!("{unit} {}, ", n(p)))
                .unwrap_or_default(),
            crate::probes::duration(plan.loss_wall_seconds.unwrap_or(0) as f64),
            plan.current
                .map(|v| format!("; the job is at {unit} {}", n(v)))
                .unwrap_or_default()
        )),
        None => lines.push(
            "The job has no checkpoint yet: suspending now would restart it from the beginning."
                .into(),
        ),
    }
    let mut loss = Vec::new();
    match (
        plan.loss_units,
        plan.latest.as_ref().and_then(|c| c.position),
        plan.current,
    ) {
        (Some(u), Some(from), Some(to)) if u > 0.0 => loss.push(format!(
            "{unit} {} to {} ({} in all)",
            n(from + 1.0),
            n(to),
            n(u)
        )),
        (Some(u), _, _) if u > 0.0 => loss.push(format!("{} of {unit}", n(u))),
        (Some(_), _, _) => loss.push("nothing: the job is at its latest checkpoint".into()),
        _ => {}
    }
    if let Some(s) = plan.loss_compute_seconds {
        loss.push(format!(
            "about {} of this machine's time",
            crate::probes::duration(s)
        ));
    } else if let Some(w) = plan.loss_wall_seconds {
        loss.push(format!(
            "up to {} of work",
            crate::probes::duration(w as f64)
        ));
    }
    if !loss.is_empty() {
        lines.push(format!("Suspending now redoes {}.", loss.join(", ")));
    }
    match (plan.next_position, plan.next_eta_seconds) {
        (Some(p), Some(eta)) => lines.push(format!(
            "The next checkpoint, {unit} {}, is due in about {} (suspending when safe loses nothing).",
            n(p),
            crate::probes::duration(eta)
        )),
        (Some(p), None) => lines.push(format!("The next checkpoint is expected at {unit} {}.", n(p))),
        _ => lines.push("When the next checkpoint will come is unknown.".into()),
    }
    lines
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_plan_states_the_loss_and_the_next_checkpoint() {
        let dir = std::env::temp_dir().join(format!("fleet-plan-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        for step in ["0005", "0010"] {
            fs::create_dir_all(dir.join(format!("out1/ckpt/step-{step}"))).unwrap();
            fs::write(dir.join(format!("out1/ckpt/step-{step}/state")), step).unwrap();
        }
        let spec = ResumeSpec {
            points: "out*/ckpt/step-*".into(),
            position: Some(r"step-(\d+)".into()),
            progress: Some("step".into()),
            every: None,
            argv: vec!["train.sh".into(), "out{segment}".into(), "{latest}".into()],
            carry: vec![],
        };
        let probes = vec![ProbeValue {
            name: "step".into(),
            reading: Reading::Number {
                value: 13.0,
                total: Some(30.0),
                per_hour: Some(3600.0),
                eta_seconds: None,
            },
        }];
        let p = plan(
            &dir,
            &spec,
            Duration::ZERO,
            &probes,
            secs(SystemTime::now()),
        );
        assert_eq!(p.latest.as_ref().unwrap().path, "out1/ckpt/step-0010");
        assert_eq!(p.loss_units, Some(3.0));
        assert_eq!(p.loss_compute_seconds, Some(3.0), "3 steps at one a second");
        assert_eq!(p.next_position, Some(15.0));
        assert_eq!(p.next_eta_seconds, Some(2.0));
        let words = describe(&p).join(" ");
        assert!(
            words.contains("redoes step 11 to 13 (3 in all), about 3 s"),
            "{words}"
        );
        assert_eq!(
            spec.command("out1/ckpt/step-0010", 2),
            vec!["train.sh", "out2", "out1/ckpt/step-0010"]
        );
        assert_eq!(
            files_to_carry(&dir, "out1/ckpt/step-0010", &[]),
            vec![PathBuf::from("out1/ckpt/step-0010/state")]
        );
        fs::remove_dir_all(dir).unwrap();
    }
}
