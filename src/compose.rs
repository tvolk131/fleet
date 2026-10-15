//! Building jobs without writing JSON by hand: `fleet run -- command…` (files named in the command become inputs) and
//! `fleet sweep` (one job file, one job per combination of parameters). Plain functions, so they are tested directly.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Component, Path, PathBuf},
};

use anyhow::{Result, bail};

use crate::coordinator::JobFile;

/// Makes a job file's relative input paths absolute, against `base` (the job file's folder, or the current folder).
/// The daemon resolves paths in its own working directory, so the CLI must send absolute ones.
pub fn absolute_inputs(job: &mut JobFile, base: &Path) {
    for local in job.inputs.values_mut() {
        if local.is_relative() {
            *local = base.join(&*local);
        }
    }
}

fn is_plain_relative(p: &Path) -> bool {
    p.is_relative()
        && p.components()
            .all(|c| matches!(c, Component::Normal(_) | Component::CurDir))
}

fn without_dot(p: &Path) -> String {
    p.components()
        .filter(|c| !matches!(c, Component::CurDir))
        .map(|c| c.as_os_str().to_string_lossy().into_owned())
        .collect::<Vec<_>>()
        .join("/")
}

/// A job for a command line. Each argument that is a relative path to an existing local file becomes an input, at the
/// same path in the job's folder. Absolute paths are left alone (assumed to exist on the worker, like `/bin/sh`),
/// unless listed in `extra`: files and folders to ship (a folder ships every file in it); an absolute one goes under
/// `inputs/` and arguments naming it are rewritten to match.
pub fn job_from_command(cwd: &Path, command: &[String], extra: &[PathBuf]) -> Result<JobFile> {
    if command.is_empty() {
        bail!("no command given (put it after --)");
    }
    let mut inputs: BTreeMap<String, PathBuf> = BTreeMap::new();
    let mut taken: BTreeSet<String> = BTreeSet::new();
    let mut place = |local: &Path, inputs: &mut BTreeMap<String, PathBuf>| -> String {
        let abs = cwd.join(local);
        let rel = if is_plain_relative(local) && !local.as_os_str().is_empty() {
            without_dot(local)
        } else {
            let name = local
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_else(|| "input".into());
            let mut candidate = format!("inputs/{name}");
            let mut n = 2;
            while taken.contains(&candidate) && inputs.get(&candidate) != Some(&abs) {
                candidate = format!("inputs/{n}-{name}");
                n += 1;
            }
            candidate
        };
        taken.insert(rel.clone());
        inputs.insert(rel.clone(), abs);
        rel
    };
    let shipped_absolute: Vec<PathBuf> =
        extra.iter().filter(|p| p.is_absolute()).cloned().collect();
    let mut argv = Vec::with_capacity(command.len());
    for (i, arg) in command.iter().enumerate() {
        let path = Path::new(arg);
        let ship = if path.is_absolute() {
            shipped_absolute.iter().any(|p| p == path)
        } else {
            is_plain_relative(path)
        };
        if ship && cwd.join(path).is_file() {
            let rel = place(path, &mut inputs);
            // A program in the job's folder needs a path with a slash to be run from there.
            argv.push(if i == 0 && !rel.contains('/') {
                format!("./{rel}")
            } else {
                rel
            });
        } else {
            argv.push(arg.clone());
        }
    }
    for item in extra {
        let abs = cwd.join(item);
        if abs.is_file() {
            if !inputs.values().any(|v| v == &abs) {
                place(item, &mut inputs);
            }
        } else if abs.is_dir() {
            let mut stack = vec![PathBuf::new()];
            while let Some(sub) = stack.pop() {
                for entry in std::fs::read_dir(abs.join(&sub))?.flatten() {
                    let rel = sub.join(entry.file_name());
                    if entry.file_type()?.is_dir() {
                        stack.push(rel);
                    } else {
                        let local = item.join(&rel);
                        let name = if is_plain_relative(item) {
                            without_dot(&local)
                        } else {
                            let dir = item
                                .file_name()
                                .map(|n| n.to_string_lossy().into_owned())
                                .unwrap_or_default();
                            format!("inputs/{dir}/{}", without_dot(&rel))
                        };
                        inputs.insert(name, cwd.join(&local));
                    }
                }
            }
        } else {
            bail!("--input {}: no such file or folder", item.display());
        }
    }
    let program = Path::new(&command[0])
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    let name = match command
        .get(1)
        .filter(|a| !a.starts_with('-') && !cwd.join(a).exists())
    {
        Some(sub) => format!("{program} {sub}"),
        None => program,
    };
    Ok(JobFile {
        checkpoint: None,
        vm: None,
        name,
        argv,
        env: BTreeMap::new(),
        inputs,
        publish: Vec::new(),
        settle_seconds: None,
        max_seconds: None,
        shutdown_grace_seconds: None,
        keep_awake: None,
        probes: Vec::new(),
        pause_keeps_gpu: None,
        resume: None,
        preempt: None,
        threads: None,
    })
}

/// Parses `name=a,b,c` sweep parameters.
pub fn parse_params(raw: &[String]) -> Result<Vec<(String, Vec<String>)>> {
    raw.iter()
        .map(|p| {
            let (k, v) = p
                .split_once('=')
                .ok_or_else(|| anyhow::anyhow!("--param {p}: expected name=value,value"))?;
            let values: Vec<String> = v
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(String::from)
                .collect();
            if k.trim().is_empty() || values.is_empty() {
                bail!("--param {p}: expected name=value,value");
            }
            Ok((k.trim().to_string(), values))
        })
        .collect()
}

/// Every combination of the parameters' values, in order (the last parameter varies fastest).
pub fn combinations(params: &[(String, Vec<String>)]) -> Vec<BTreeMap<String, String>> {
    let mut out = vec![BTreeMap::new()];
    for (name, values) in params {
        out = out
            .into_iter()
            .flat_map(|base| {
                values.iter().map(move |v| {
                    let mut c = base.clone();
                    c.insert(name.clone(), v.clone());
                    c
                })
            })
            .collect();
    }
    out
}

/// Replaces `{name}` with each parameter's value everywhere in a JSON value (strings and object keys).
pub fn substitute(
    value: &serde_json::Value,
    combo: &BTreeMap<String, String>,
) -> serde_json::Value {
    let fill = |s: &str| {
        combo.iter().fold(s.to_string(), |acc, (k, v)| {
            acc.replace(&format!("{{{k}}}"), v)
        })
    };
    match value {
        serde_json::Value::String(s) => serde_json::Value::String(fill(s)),
        serde_json::Value::Array(items) => {
            serde_json::Value::Array(items.iter().map(|v| substitute(v, combo)).collect())
        }
        serde_json::Value::Object(map) => serde_json::Value::Object(
            map.iter()
                .map(|(k, v)| (fill(k), substitute(v, combo)))
                .collect(),
        ),
        other => other.clone(),
    }
}

/// One job per combination: the template with its parameters filled in, named so each is distinct.
pub fn expand(
    template: &serde_json::Value,
    params: &[(String, Vec<String>)],
) -> Result<Vec<(BTreeMap<String, String>, JobFile)>> {
    let named_by_params = params.iter().all(|(k, _)| {
        template
            .get("name")
            .and_then(|n| n.as_str())
            .is_some_and(|n| n.contains(&format!("{{{k}}}")))
    });
    combinations(params)
        .into_iter()
        .map(|combo| {
            let mut job: JobFile = serde_json::from_value(substitute(template, &combo))?;
            if !named_by_params {
                let suffix: Vec<String> = combo.iter().map(|(k, v)| format!("{k}={v}")).collect();
                job.name = format!("{} {}", job.name, suffix.join(" "));
            }
            Ok((combo, job))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn files_named_in_a_command_become_inputs() {
        let dir = std::env::temp_dir().join(format!("fleet-compose-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("bin")).unwrap();
        std::fs::create_dir_all(dir.join("data/sub")).unwrap();
        std::fs::write(dir.join("bin/tool"), "x").unwrap();
        std::fs::write(dir.join("config.json"), "{}").unwrap();
        std::fs::write(dir.join("data/a.txt"), "a").unwrap();
        std::fs::write(dir.join("data/sub/b.txt"), "b").unwrap();
        let elsewhere =
            std::env::temp_dir().join(format!("fleet-compose-other-{}.bin", std::process::id()));
        std::fs::write(&elsewhere, "z").unwrap();
        let command: Vec<String> = [
            "./bin/tool",
            "train",
            "--config",
            "config.json",
            "--out",
            "results",
            elsewhere.to_str().unwrap(),
        ]
        .map(String::from)
        .to_vec();
        // An absolute path stays as it is unless shipped with --input.
        let plain = job_from_command(&dir, &command, &[]).unwrap();
        assert_eq!(plain.argv[6], elsewhere.to_str().unwrap());
        assert!(!plain.inputs.values().any(|v| v == &elsewhere));
        let job =
            job_from_command(&dir, &command, &[PathBuf::from("data"), elsewhere.clone()]).unwrap();
        let other = format!(
            "inputs/{}",
            elsewhere.file_name().unwrap().to_string_lossy()
        );
        assert_eq!(
            job.argv,
            vec![
                "bin/tool",
                "train",
                "--config",
                "config.json",
                "--out",
                "results",
                other.as_str()
            ]
        );
        assert_eq!(job.name, "tool train");
        let keys: Vec<&str> = job.inputs.keys().map(String::as_str).collect();
        assert_eq!(
            keys,
            vec![
                "bin/tool",
                "config.json",
                "data/a.txt",
                "data/sub/b.txt",
                other.as_str()
            ]
        );
        assert_eq!(job.inputs[&other], elsewhere);
        std::fs::remove_dir_all(&dir).unwrap();
        std::fs::remove_file(&elsewhere).unwrap();
    }

    #[test]
    fn a_sweep_makes_one_job_per_combination() {
        let template = serde_json::json!({
            "name": "screen", "argv": ["bin/t", "--seed", "{seed}", "--lr", "{lr}", "--out", "run-{seed}-{lr}"],
            "env": {"TAG": "lr{lr}"}
        });
        let params = parse_params(&["seed=521,733".into(), "lr=0.0003, 0.0006".into()]).unwrap();
        let jobs = expand(&template, &params).unwrap();
        assert_eq!(jobs.len(), 4);
        assert_eq!(
            jobs[1].1.argv,
            vec![
                "bin/t",
                "--seed",
                "521",
                "--lr",
                "0.0006",
                "--out",
                "run-521-0.0006"
            ]
        );
        assert_eq!(jobs[1].1.env["TAG"], "lr0.0006");
        assert_eq!(jobs[1].1.name, "screen lr=0.0006 seed=521");
        assert!(parse_params(&["oops".into()]).is_err());
    }
}
