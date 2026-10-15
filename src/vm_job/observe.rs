//! Bounded read-only observations alongside an execution. A persisted batch
//! reconnects to in-flight transfers; committed file versions avoid re-copying.
use super::*;
use crate::probes::{self, Origin, Probe, ProbeValue, Reading};
use std::collections::{BTreeMap, HashMap};

const MAX_PROBES: usize = 32;
const MAX_SNAPSHOT: u64 = 1 << 20;
const MAX_TRANSFERRED: u64 = 16 << 30;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Sample {
    Values(Vec<f64>),
    Count(u64),
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Snapshot {
    files: Vec<Capture>,
    metrics: Vec<Option<Sample>>,
}
#[derive(Debug, Serialize, Deserialize)]
struct Saved {
    file: Capture,
    hash: iroh_blobs::Hash,
}
#[derive(Debug, Default, Serialize, Deserialize)]
struct Observation {
    generation: u64,
    revision: u64,
    pending_final: Option<bool>,
    charged: BTreeSet<usize>,
    transferred: u64,
    captured: BTreeMap<String, Saved>,
    origins: HashMap<String, Origin>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Progress {
    pub at: u64,
    pub values: Vec<ProbeValue>,
}

pub(super) fn validate(probes: &[Probe]) -> Result<()> {
    if probes.len() > MAX_PROBES || serde_json::to_vec(probes)?.len() > 16 << 10 {
        bail!("VM probes exceed 32 probes or 16 KiB of configuration");
    }
    Ok(())
}

fn readings(
    snapshot: &Snapshot,
    probes: &[Probe],
    dir: &Path,
    origins: &mut HashMap<String, Origin>,
) -> Result<Vec<ProbeValue>> {
    if snapshot.metrics.len() != probes.len() {
        bail!("guest metric count mismatch");
    }
    let mut values = Vec::new();
    for (probe, sample) in probes.iter().zip(&snapshot.metrics) {
        let name = probe.name();
        let reading = match (probe, sample) {
            (Probe::JsonlField { total, .. }, Some(Sample::Values(v))) => {
                if v.len() > 1 || v.iter().any(|v| !v.is_finite() || v.abs() > 1e100) {
                    bail!("invalid numeric sample");
                }
                let Some(value) = v.last() else {
                    continue;
                };
                probes::number(*value, *total, &name, origins, jobs::now() as f64)
            }
            (Probe::JsonlSeries { points, .. }, Some(Sample::Values(v))) => {
                if v.len() > points.unwrap_or(60).min(500)
                    || v.iter().any(|v| !v.is_finite() || v.abs() > 1e100)
                {
                    bail!("invalid metric series");
                }
                if v.is_empty() {
                    continue;
                }
                Reading::Series { values: v.clone() }
            }
            (Probe::GlobCount { .. }, Some(Sample::Count(count))) if *count <= 100000 => {
                Reading::Count { count: *count }
            }
            (Probe::LogRegex { .. }, None) => {
                values.extend(probes::evaluate(
                    std::slice::from_ref(probe),
                    dir,
                    origins,
                    jobs::now() as f64,
                ));
                continue;
            }
            _ => bail!("guest metric does not match its declared probe"),
        };
        values.push(ProbeValue { name, reading });
    }
    Ok(values)
}

impl Runner {
    /// Only completed private observation operations are reclaimed. Command/input
    /// operation identities remain for recovery and are never replayed/pruned here.
    fn prune_observations(&self, keep: Option<u64>) -> Result<()> {
        for op in guest::list(&self.home, &self.status.instance)? {
            let current = keep.is_some_and(|n| {
                op.id == format!("sample-{n}") || op.id.starts_with(&format!("capture-{n}-"))
            });
            if current
                || !(op.id.starts_with("sample-") || op.id.starts_with("capture-"))
                || !op.state.finished()
            {
                continue;
            }
            let dir = guest::operation_dir(&self.home, &self.status.instance, &op.id)?;
            if let Some(_lock) = instances::try_lock(&dir.join("operation.lock"))? {
                fs::remove_dir_all(dir)?;
            }
        }
        Ok(())
    }
    pub(super) fn observe(&mut self, final_pass: bool) -> Result<()> {
        if self.job.spec.publish.is_empty() && self.job.spec.probes.is_empty() {
            return Ok(());
        }
        let path = self.dir.join("vm-observation.json");
        let mut state: Observation = jobs::read_json(&path)?.unwrap_or_default();
        // An interrupted live batch is completed before taking the final snapshot.
        loop {
            self.prune_observations(state.pending_final.map(|_| state.generation))?;
            if state.pending_final.is_none() {
                state.pending_final = Some(final_pass);
                instances::write(&path, &state)?;
            }
            let is_final = state.pending_final.unwrap();
            let sample_id = format!("sample-{}", state.generation);
            let helper = self.helper(serde_json::json!({"kind":"observe", "root":self.guest_root(), "patterns":self.job.spec.publish,
                "probes":self.job.spec.probes, "settle_seconds":self.job.spec.settle_seconds, "final":is_final}));
            self.require(&sample_id, helper, None)?;
            let snapshot_path =
                guest::operation_dir(&self.home, &self.status.instance, &sample_id)?.join("stdout");
            if fs::metadata(&snapshot_path)?.len() > MAX_SNAPSHOT {
                bail!("guest observation exceeds 1 MiB");
            }
            let snapshot: Snapshot = serde_json::from_slice(&fs::read(snapshot_path)?)?;
            validate_manifest(&snapshot.files, &self.job.spec.publish)?;
            if snapshot.files.iter().any(|f| {
                f.version
                    .as_ref()
                    .is_none_or(|v| v.is_empty() || v.len() > 256)
            }) {
                bail!("missing or invalid output version");
            }
            // Retained paths cannot cumulatively exceed the limits or conflict
            // with an earlier capture, even if the guest deletes/renames files.
            let mut inventory: BTreeMap<_, _> = state
                .captured
                .iter()
                .map(|(p, s)| (p.clone(), s.file.clone()))
                .collect();
            for file in &snapshot.files {
                inventory.insert(file.path.clone(), file.clone());
            }
            validate_manifest(
                &inventory.into_values().collect::<Vec<_>>(),
                &self.job.spec.publish,
            )?;
            let values = readings(
                &snapshot,
                &self.job.spec.probes,
                &self.dir,
                &mut state.origins,
            )?;
            instances::write(&path, &state)?;
            instances::write(
                &self.dir.join("vm-progress.json"),
                &Progress {
                    at: jobs::now(),
                    values,
                },
            )?;
            for (n, file) in snapshot.files.iter().enumerate() {
                self.check()?;
                if state
                    .captured
                    .get(&file.path)
                    .is_some_and(|s| s.file.version == file.version && s.file.size == file.size)
                {
                    continue;
                }
                let id = format!("capture-{}-{n}", state.generation);
                if state.charged.insert(n) {
                    state.transferred = state
                        .transferred
                        .checked_add(file.size)
                        .context("transfer size overflow")?;
                    if state.transferred > MAX_TRANSFERRED {
                        bail!("VM job exceeded 16 GiB cumulative output transfer limit");
                    }
                    instances::write(&path, &state)?;
                }
                let op = self.run_operation(
                    &id,
                    guest::Spec::Download {
                        path: format!("{}/{}", self.guest_root(), file.path),
                        max_bytes: file.size.max(1),
                        version: file.version.clone(),
                    },
                    None,
                )?;
                if !is_final && op.exit_code == Some(75) {
                    continue;
                } // changed: retry a later sample
                if op.state != guest::OperationState::Succeeded {
                    bail!("output capture failed for {}: {:?}", file.path, op.error);
                }
                let source = guest::output(&self.home, &self.status.instance, &id)?;
                if fs::metadata(&source)?.len() != file.size {
                    bail!("output changed size during capture: {}", file.path);
                }
                let hash = crate::blobs::hash_file(&source)?;
                let target = self.dir.join("work").join(jobs::safe_relative(&file.path)?);
                fs::create_dir_all(target.parent().context("output parent")?)?;
                let staging = self.dir.join("vm-capture.tmp");
                if staging.exists() {
                    fs::remove_file(&staging)?;
                }
                fs::hard_link(&source, &staging)?;
                fs::rename(&staging, &target)?;
                fs::File::open(target.parent().unwrap())?.sync_all()?;
                state.revision += 1;
                state.captured.insert(
                    file.path.clone(),
                    Saved {
                        file: file.clone(),
                        hash,
                    },
                );
                instances::write(&path, &state)?;
            }
            state.generation = state
                .generation
                .checked_add(1)
                .context("observation counter overflow")?;
            state.pending_final = None;
            state.charged.clear();
            instances::write(&path, &state)?;
            self.prune_observations(None)?;
            if !final_pass || is_final {
                break;
            }
        }
        Ok(())
    }
}

/// Revision changes even for same-sized output replacements and empty files.
pub fn output_revision(dir: &Path) -> Result<u64> {
    Ok(jobs::read_json::<Observation>(&dir.join("vm-observation.json"))?.map_or(0, |s| s.revision))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn guest_samples_complete_metric_lines_and_defers_unsettled_files() {
        use std::os::unix::fs::symlink;
        let root =
            std::env::temp_dir().join(format!("fleet-observation-{:016x}", rand::random::<u64>()));
        fs::create_dir_all(root.join("out/checkpoint")).unwrap();
        fs::write(root.join("out/checkpoint/model"), "checkpoint").unwrap();
        fs::write(
            root.join("metrics.jsonl"),
            "{\"nested\":{\"loss\":0.5}}\n{\"nested\":{\"loss\":0.2}}",
        )
        .unwrap();
        symlink(root.join("out"), root.join("alias")).unwrap();
        let mut request = serde_json::json!({"kind":"observe", "root":root, "patterns":["out/*"], "settle_seconds":100, "final":false,
            "probes":[{"jsonl_series":{"file":"*.jsonl", "field":"nested.loss", "points":2}}, {"glob_count":{"pattern":"out/checkpoint/*"}}]});
        let run = |request: &serde_json::Value| {
            let output = Command::new("/usr/bin/python3")
                .args(["-I", "-c", HELPER, &request.to_string()])
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            serde_json::from_slice::<Snapshot>(&output.stdout).unwrap()
        };
        let live = run(&request);
        assert!(live.files.is_empty());
        assert!(matches!(&live.metrics[0], Some(Sample::Values(v)) if v == &[0.5]));
        assert!(matches!(live.metrics[1], Some(Sample::Count(1))));
        request["final"] = true.into();
        let final_sample = run(&request);
        assert_eq!(final_sample.files.len(), 1);
        assert_eq!(final_sample.files[0].path, "out/checkpoint/model");
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn untrusted_metric_shapes_and_limits_are_checked_on_host() {
        let series = Probe::JsonlSeries {
            name: None,
            file: "metrics".into(),
            field: "loss".into(),
            points: Some(2),
        };
        for sample in [
            Sample::Values(vec![1.0, 2.0, 3.0]),
            Sample::Values(vec![f64::NAN]),
            Sample::Count(1),
        ] {
            let snapshot = Snapshot {
                files: vec![],
                metrics: vec![Some(sample)],
            };
            assert!(
                readings(
                    &snapshot,
                    std::slice::from_ref(&series),
                    Path::new("/unused"),
                    &mut HashMap::new()
                )
                .is_err()
            );
        }
        assert!(validate(&vec![series; 33]).is_err());
        let snapshot = Snapshot {
            files: vec![],
            metrics: vec![Some(Sample::Count(100001))],
        };
        let count = Probe::GlobCount {
            name: None,
            pattern: "*".into(),
        };
        assert!(
            readings(
                &snapshot,
                &[count],
                Path::new("/unused"),
                &mut HashMap::new()
            )
            .is_err()
        );
    }
}
