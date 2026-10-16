//! Progress probes: what a job's specification asks the agent to read while the job runs, and the values it reports.
//!
//! - `jsonl_field`: the last value of a field in a JSON-lines file, with an optional total; the agent adds the rate
//!   (per hour, from the values it has seen) and an estimated time to the total.
//!
//! A JSON-lines file may contain `*` within a path segment (`B*/metrics.jsonl`): the most recently changed match is
//! read, so a probe follows a run whose continuations write folders of their own (`B`, `B-s2`, ...).
//! - `jsonl_series`: the field's recent values, for a sparkline.
//! - `glob_count`: how many paths in the working directory match a pattern.
//! - `log_regex`: the last match of a pattern in the job's output (the first capture group if there is one).

use std::{
    fs,
    io::{Read, Seek, SeekFrom},
    path::Path,
};

use serde::{Deserialize, Serialize};

use crate::jobs::{matches, safe_relative};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Probe {
    JsonlField {
        #[serde(default)]
        name: Option<String>,
        file: String,
        field: String,
        #[serde(default)]
        total: Option<f64>,
    },
    JsonlSeries {
        #[serde(default)]
        name: Option<String>,
        file: String,
        field: String,
        #[serde(default)]
        points: Option<usize>,
    },
    GlobCount {
        #[serde(default)]
        name: Option<String>,
        pattern: String,
    },
    LogRegex {
        #[serde(default)]
        name: Option<String>,
        pattern: String,
        #[serde(default)]
        stderr: bool,
    },
}

impl Probe {
    pub fn name(&self) -> String {
        match self {
            Probe::JsonlField { name, field, .. } | Probe::JsonlSeries { name, field, .. } => {
                name.clone().unwrap_or_else(|| field.clone())
            }
            Probe::GlobCount { name, pattern } => name.clone().unwrap_or_else(|| pattern.clone()),
            Probe::LogRegex { name, pattern, .. } => {
                name.clone().unwrap_or_else(|| pattern.clone())
            }
        }
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        match self {
            Probe::JsonlField { file, .. } | Probe::JsonlSeries { file, .. } => {
                safe_relative(&file.replace('*', "x")).map(|_| ())
            }
            Probe::GlobCount { pattern, .. } => {
                safe_relative(&pattern.replace('*', "x")).map(|_| ())
            }
            Probe::LogRegex { pattern, .. } => {
                regex::Regex::new(pattern).map(|_| ()).map_err(Into::into)
            }
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Reading {
    Number {
        value: f64,
        total: Option<f64>,
        per_hour: Option<f64>,
        eta_seconds: Option<f64>,
    },
    Series {
        values: Vec<f64>,
    },
    Count {
        count: u64,
    },
    Text {
        text: String,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ProbeValue {
    pub name: String,
    pub reading: Reading,
}

/// The first value a numeric probe was seen at, for its rate.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Origin {
    pub at: f64,
    pub value: f64,
}

/// Apply the same host-calculated rate/ETA to native and VM numeric samples.
pub fn number(
    value: f64,
    total: Option<f64>,
    name: &str,
    origins: &mut std::collections::HashMap<String, Origin>,
    now: f64,
) -> Reading {
    let origin = *origins
        .entry(name.into())
        .or_insert(Origin { at: now, value });
    let hours = (now - origin.at) / 3600.0;
    let per_hour = (hours > 0.0 && value != origin.value)
        .then(|| (value - origin.value) / hours)
        .filter(|v| v.is_finite());
    let eta_seconds = match (total, per_hour) {
        (Some(t), Some(r)) if r > 0.0 && t > value => {
            Some((t - value) / r * 3600.0).filter(|v| v.is_finite())
        }
        _ => None,
    };
    Reading::Number {
        value,
        total,
        per_hour,
        eta_seconds,
    }
}

/// A short, human description of a duration in seconds.
pub fn duration(seconds: f64) -> String {
    match seconds {
        s if s < 90.0 => format!("{s:.0} s"),
        s if s < 5400.0 => format!("{:.0} min", s / 60.0),
        s => format!("{:.1} h", s / 3600.0),
    }
}

/// One line for a set of probe values, for lists (`fleet queue`, the GUI's rows).
pub fn summary(values: &[ProbeValue]) -> String {
    values
        .iter()
        .filter_map(|p| match &p.reading {
            Reading::Number {
                value,
                total,
                per_hour,
                eta_seconds,
            } => {
                let mut s = match total {
                    Some(t) if *t > 0.0 => {
                        format!("{} {value}/{t} ({:.0}%)", p.name, 100.0 * value / t)
                    }
                    _ => format!("{} {value}", p.name),
                };
                if let Some(r) = per_hour {
                    s += &format!(", {r:.0}/h");
                }
                if let Some(e) = eta_seconds {
                    s += &format!(", about {} left", duration(*e));
                }
                Some(s)
            }
            Reading::Count { count } => Some(format!("{} {count}", p.name)),
            Reading::Text { text } => Some(format!("{} {text}", p.name)),
            Reading::Series { .. } => None,
        })
        .collect::<Vec<_>>()
        .join("; ")
}

/// Reads the last `bytes` of a file, from the start of a line (decoding lossily).
fn tail(path: &Path, bytes: u64) -> Option<String> {
    let mut file = fs::File::open(path).ok()?;
    let len = file.metadata().ok()?.len();
    let start = len.saturating_sub(bytes);
    file.seek(SeekFrom::Start(start)).ok()?;
    let mut raw = Vec::new();
    file.take(bytes).read_to_end(&mut raw).ok()?;
    let text = String::from_utf8_lossy(&raw).into_owned();
    Some(if start > 0 {
        text.split_once('\n')
            .map_or(String::new(), |(_, rest)| rest.to_string())
    } else {
        text
    })
}

/// Whether two sets of values differ in what they measure (ignoring rates and estimates, which drift with time).
pub fn changed(a: &[ProbeValue], b: &[ProbeValue]) -> bool {
    let key = |v: &[ProbeValue]| -> Vec<(String, String)> {
        v.iter()
            .map(|p| {
                let r = match &p.reading {
                    Reading::Number { value, total, .. } => format!("{value} {total:?}"),
                    other => format!("{other:?}"),
                };
                (p.name.clone(), r)
            })
            .collect()
    };
    key(a) != key(b)
}

/// The field's values in the file's last complete lines, oldest first.
fn jsonl_values(path: &Path, field: &str, want: usize) -> Vec<f64> {
    let Some(text) = tail(path, 512 * 1024) else {
        return Vec::new();
    };
    let mut values: Vec<f64> = text
        .lines()
        .rev()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .filter_map(|v| {
            field
                .split('.')
                .try_fold(&v, |v, key| v.get(key))
                .and_then(serde_json::Value::as_f64)
        })
        .take(want)
        .collect();
    values.reverse();
    values
}

/// Evaluates a job's probes. `origins` holds each numeric probe's first reading (by name), so rates span the run;
/// `now` is seconds since the epoch.
pub fn evaluate(
    probes: &[Probe],
    job_dir: &Path,
    origins: &mut std::collections::HashMap<String, Origin>,
    now: f64,
) -> Vec<ProbeValue> {
    let work = job_dir.join("work");
    probes
        .iter()
        .filter_map(|probe| {
            let name = probe.name();
            let reading = match probe {
                Probe::JsonlField {
                    file, field, total, ..
                } => {
                    let value = *jsonl_values(&resolve(&work, file)?, field, 1).last()?;
                    number(value, *total, &name, origins, now)
                }
                Probe::JsonlSeries {
                    file,
                    field,
                    points,
                    ..
                } => {
                    let values =
                        jsonl_values(&resolve(&work, file)?, field, points.unwrap_or(60).min(500));
                    if values.is_empty() {
                        return None;
                    }
                    Reading::Series { values }
                }
                Probe::GlobCount { pattern, .. } => Reading::Count {
                    count: count_matches(&work, pattern),
                },
                Probe::LogRegex {
                    pattern, stderr, ..
                } => {
                    let re = regex::Regex::new(pattern).ok()?;
                    let stream = crate::history::Stream::log(*stderr);
                    let text = if crate::history::root(job_dir, stream)
                        .join("state.json")
                        .exists()
                    {
                        let state = crate::history::state(job_dir, stream).ok()?;
                        let start = state
                            .blocks
                            .last()?
                            .start
                            .max(state.observed.saturating_sub(64 * 1024));
                        String::from_utf8_lossy(
                            &crate::history::read(job_dir, stream, start, 64 * 1024)
                                .ok()?
                                .data,
                        )
                        .into_owned()
                    } else {
                        tail(
                            &job_dir.join(if *stderr { "stderr.log" } else { "stdout.log" }),
                            64 * 1024,
                        )?
                    };
                    let caps = re.captures_iter(&text).last()?;
                    let m = caps.get(1).or_else(|| caps.get(0))?;
                    Reading::Text {
                        text: m.as_str().chars().take(200).collect(),
                    }
                }
            };
            Some(ProbeValue { name, reading })
        })
        .collect()
}

/// Paths (files or folders) under `work` matching the pattern exactly, segment for segment, relative to `work`.
fn matching(work: &Path, pattern: &str) -> Vec<String> {
    let depth = pattern.split('/').count();
    let mut found = Vec::new();
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
                    found.push(child);
                }
            } else if entry.file_type().is_ok_and(|t| t.is_dir()) {
                stack.push((entry.path(), child, level + 1));
            }
        }
    }
    found
}

fn count_matches(work: &Path, pattern: &str) -> u64 {
    matching(work, pattern).len() as u64
}

/// The file a probe reads: the path itself, or with `*` in it the most recently changed match.
fn resolve(work: &Path, file: &str) -> Option<std::path::PathBuf> {
    if !file.contains('*') {
        return Some(work.join(file));
    }
    matching(work, file)
        .into_iter()
        .map(|rel| work.join(rel))
        .filter_map(|path| Some((fs::metadata(&path).ok()?.modified().ok()?, path)))
        .max()
        .map(|(_, path)| path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn probes_read_metrics_counts_and_logs() {
        let dir = std::env::temp_dir().join(format!("fleet-probes-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let work = dir.join("work");
        fs::create_dir_all(work.join("B/iter-0064")).unwrap();
        fs::create_dir_all(work.join("B/iter-0128")).unwrap();
        fs::write(work.join("B/notes.txt"), "x").unwrap();
        let lines: String = (1..=10)
            .map(|i| {
                format!(
                    "{{\"iteration\":{i},\"health\":{{\"kl\":{}}}}}\n",
                    i as f64 / 1000.0
                )
            })
            .collect();
        fs::write(
            work.join("B/metrics.jsonl"),
            lines + "{\"iteration\":11,\"heal",
        )
        .unwrap();
        fs::write(
            dir.join("stdout.log"),
            "Validation iteration 64: 61.0%\nValidation iteration 128: 64.5%\n",
        )
        .unwrap();
        let probes = vec![
            Probe::JsonlField {
                name: None,
                file: "B/metrics.jsonl".into(),
                field: "iteration".into(),
                total: Some(20.0),
            },
            Probe::JsonlSeries {
                name: Some("kl".into()),
                file: "B/metrics.jsonl".into(),
                field: "health.kl".into(),
                points: Some(3),
            },
            Probe::GlobCount {
                name: Some("checkpoints".into()),
                pattern: "B/iter-*".into(),
            },
            Probe::LogRegex {
                name: Some("validation".into()),
                pattern: r"Validation iteration \d+: ([\d.]+%)".into(),
                stderr: false,
            },
        ];
        let mut origins = std::collections::HashMap::new();
        origins.insert(
            "iteration".to_string(),
            Origin {
                at: 0.0,
                value: 0.0,
            },
        );
        let values = evaluate(&probes, &dir, &mut origins, 3600.0);
        assert_eq!(values.len(), 4);
        assert_eq!(
            values[0].reading,
            Reading::Number {
                value: 10.0,
                total: Some(20.0),
                per_hour: Some(10.0),
                eta_seconds: Some(3600.0)
            },
            "a cut-off last line is skipped; 10 per hour leaves an hour to 20"
        );
        assert_eq!(
            values[1].reading,
            Reading::Series {
                values: vec![0.008, 0.009, 0.01]
            }
        );
        assert_eq!(values[2].reading, Reading::Count { count: 2 });
        assert_eq!(
            values[3].reading,
            Reading::Text {
                text: "64.5%".into()
            }
        );
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_probe_with_a_star_follows_the_newest_segment() {
        let dir = std::env::temp_dir().join(format!("fleet-probes-star-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let work = dir.join("work");
        // A continuation's folder: the carried checkpoint from the first segment, and the new segment's output.
        fs::create_dir_all(work.join("B/resume/iter-0256")).unwrap();
        fs::write(work.join("B/metrics.jsonl"), "{\"iteration\":256}\n").unwrap();
        std::thread::sleep(std::time::Duration::from_millis(20));
        fs::create_dir_all(work.join("B-s2")).unwrap();
        fs::write(work.join("B-s2/metrics.jsonl"), "{\"iteration\":300}\n").unwrap();
        let probe = Probe::JsonlField {
            name: None,
            file: "B*/metrics.jsonl".into(),
            field: "iteration".into(),
            total: Some(512.0),
        };
        probe.validate().unwrap();
        let values = evaluate(&[probe], &dir, &mut Default::default(), 0.0);
        assert!(
            matches!(values[0].reading, Reading::Number { value, .. } if value == 300.0),
            "{values:?}"
        );
        let none = Probe::JsonlSeries {
            name: None,
            file: "C*/metrics.jsonl".into(),
            field: "iteration".into(),
            points: None,
        };
        assert!(evaluate(&[none], &dir, &mut Default::default(), 0.0).is_empty());
        fs::remove_dir_all(dir).unwrap();
    }
}
