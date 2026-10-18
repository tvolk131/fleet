//! Explicit offline conversion of idle format-5 through 11 metadata. No rolling mixed-version protocol.
use super::{Mutation, State};
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::path::Path;

/// Migrate only persisted artifact metadata; content identities and file bytes do not change.
pub(crate) fn artifact_origins(value: &mut Value) {
    match value {
        Value::Object(fields) => {
            if fields.contains_key("id")
                && fields.contains_key("kind")
                && fields.contains_key("files")
                && fields.contains_key("worker")
                && fields.contains_key("attempt")
                && !fields.contains_key("origin")
            {
                let worker = fields.remove("worker").unwrap();
                let attempt = fields.remove("attempt").unwrap();
                fields.insert(
                    "origin".into(),
                    json!({"Attempt":{"worker":worker,"attempt":attempt}}),
                );
            }
            for v in fields.values_mut() {
                artifact_origins(v);
            }
        }
        Value::Array(values) => {
            for v in values {
                artifact_origins(v);
            }
        }
        _ => {}
    }
}
fn rehash_receipts(state: &mut State) -> Result<()> {
    for receipt in state.receipts.values_mut() {
        if let Some(command) = &mut receipt.command {
            artifact_origins(command);
            let mutation: Mutation = serde_json::from_value(command.clone())?;
            *command = serde_json::to_value(&mutation)?;
            receipt.fingerprint = blake3::hash(&serde_json::to_vec(&mutation)?)
                .to_hex()
                .to_string();
        }
    }
    Ok(())
}

fn job_kind(a: &Value) -> bool {
    a["workload"].get("Job").is_some()
}
fn phase(value: &mut Value, job: bool) -> Result<()> {
    let old = value
        .as_str()
        .context("expected format-5 phase")?
        .to_owned();
    *value = if job {
        json!({"Job":old})
    } else {
        json!({"Instance":old})
    };
    Ok(())
}
fn observation(value: &mut Value, job: bool) -> Result<()> {
    if !value.is_null() {
        phase(&mut value[1]["phase"], job)?;
    }
    Ok(())
}
fn assignment(id: &str, a: &mut Value) -> Result<()> {
    let job = job_kind(a);
    phase(&mut a["desired"], job)?;
    observation(&mut a["observed"], job)?;
    a["attempt"] = if job {
        json!({"job":id,"number":1,"checkpoint":null})
    } else {
        Value::Null
    };
    a["suspend_checkpoint"] = Value::Null;
    Ok(())
}

pub(crate) fn mutation(value: &mut Value, assignments: &Value) -> Result<()> {
    artifact_origins(value);
    if let Some(desire) = value.get_mut("Desire") {
        let id = desire["id"].as_str().context("missing assignment ID")?;
        let job = match desire["desired"].as_str() {
            Some("Cancelled") => true,
            Some("Stopped" | "Deleted") => false,
            Some("Running") => job_kind(
                assignments
                    .get(id)
                    .context("cannot classify outstanding Running intent")?,
            ),
            None => return Ok(()), // already converted client journal
            _ => anyhow::bail!("unknown old intent"),
        };
        phase(&mut desire["desired"], job)?;
    }
    if let Some(report) = value.get_mut("WorkerReport") {
        let id = report["assignment"]
            .as_str()
            .context("missing report assignment")?;
        let job = job_kind(assignments.get(id).context("unknown reported assignment")?);
        phase(&mut report["observation"]["phase"], job)?;
    }
    Ok(())
}

pub(crate) fn state(mut value: Value) -> Result<State> {
    artifact_origins(&mut value);
    if matches!(value["format"].as_u64(), Some(6..=11)) {
        let mut state: State = serde_json::from_value(value)?;
        ensure!(
            state.recovery.is_none(),
            "format-6/7/8/9/10/11 recovery-bound state requires a separate migration"
        );
        ensure!(
            state.membership.voter_ids().count() == 1
                && state
                    .membership_operations
                    .values()
                    .all(|op| op.stage == super::membership::MembershipStage::Complete),
            "upgrade requires settled singleton membership"
        );
        ensure!(
            state.assignments.values().all(|a| a.terminal()
                || a.observed
                    .as_ref()
                    .is_some_and(|(revision, o)| *revision == a.revision
                        && a.desired
                            == super::workload::Desired::Instance(
                                super::workload::InstanceDesired::Stopped
                            )
                        && o.phase
                            == super::workload::WorkPhase::Instance(
                                super::workload::InstancePhase::Stopped
                            ))),
            "finish or suspend jobs and stop VMs before upgrade"
        );
        ensure!(
            state
                .jobs
                .values()
                .all(|j| j.phase.terminal() || j.phase == super::queue::JobPhase::Suspended),
            "finish or suspend all logical jobs before upgrade"
        );
        rehash_receipts(&mut state)?;
        state.format = super::FORMAT;
        for cp in state.checkpoints.clone().into_values() {
            state.register_artifact(super::artifact::Manifest::new(
                cp.worker.clone(),
                cp.attempt.to_string(),
                super::artifact::Kind::Checkpoint(cp.id.clone()),
                cp.content.files.clone(),
            )?);
        }
        return Ok(state);
    }
    ensure!(value["format"] == 5, "upgrade requires format 5");
    ensure!(
        value["recovery"].is_null(),
        "format-5 clusters with emergency recovery history require a separate migration; this upgrade refuses recovery-bound state"
    );
    ensure!(
        value["jobs"]
            .as_object()
            .context("missing prototype job map")?
            .is_empty(),
        "prototype jobs cannot be upgraded into executable jobs; retain this lab database as an archive"
    );
    let old_assignments = value["assignments"].clone();
    let mut jobs = value["queue"]
        .as_object()
        .context("missing job registry")?
        .clone();
    for (id, a) in old_assignments.as_object().context("missing assignments")? {
        let observed = a["observed"][1]["phase"].as_str().unwrap_or("");
        ensure!(
            if job_kind(a) {
                matches!(observed, "Succeeded" | "Failed" | "Cancelled")
            } else {
                matches!(observed, "Stopped" | "Deleted")
            },
            "finish jobs and stop VMs before upgrade: {id}"
        );
        if job_kind(a) {
            jobs.entry(id.clone()).or_insert_with(|| json!({"id":id,"workload":a["workload"],"worker":a["worker"],"priority":0,"requires":[],"after":[],"staged":[],"submitted":0,"revision":1,"cancelled":false}));
        }
    }
    for (id, j) in &mut jobs {
        let a = old_assignments.get(id).filter(|a| job_kind(a));
        ensure!(
            a.is_some() || j["cancelled"] == true,
            "cancel queued job before upgrade: {id}"
        );
        j["phase"] = a
            .map(|a| a["observed"][1]["phase"].clone())
            .unwrap_or_else(|| json!("Cancelled"));
        j["attempts"] = if a.is_some() { json!([id]) } else { json!([]) };
        j["resume_from"] = Value::Null;
        j.as_object_mut().unwrap().remove("cancelled");
    }
    for (id, a) in value["assignments"].as_object_mut().unwrap() {
        assignment(id, a)?;
    }
    for receipt in value["receipts"]
        .as_object_mut()
        .context("missing receipts")?
        .values_mut()
    {
        if !receipt["command"].is_null() {
            mutation(&mut receipt["command"], &old_assignments)?;
            // Unsupported prototype commands remain inert audit JSON, never executable variants.
            if let Ok(command) = serde_json::from_value::<Mutation>(receipt["command"].clone()) {
                receipt["fingerprint"] = json!(
                    blake3::hash(&serde_json::to_vec(&command)?)
                        .to_hex()
                        .to_string()
                );
            }
        }
    }
    value.as_object_mut().unwrap().remove("queue");
    value["jobs"] = jobs.into();
    value["checkpoints"] = json!({});
    value["format"] = json!(super::FORMAT);
    let state: State = serde_json::from_value(value)?;
    ensure!(
        state.membership.voter_ids().count() == 1,
        "cooperatively shrink to one voter before upgrading; re-add freshly prepared replicas afterward"
    );
    ensure!(
        state
            .membership_operations
            .values()
            .all(|op| op.stage == super::membership::MembershipStage::Complete),
        "finish membership changes before upgrading"
    );
    Ok(state)
}

pub(crate) fn journal(mut value: Value) -> Result<Value> {
    artifact_origins(&mut value);
    if value["format"] == super::FORMAT {
        return Ok(value);
    }
    if matches!(value["format"].as_u64(), Some(6..=11)) {
        ensure!(
            value["recovery_inventory"].is_null() && value["recovery_binding"].is_null(),
            "format-6/7/8/9/10/11 recovery-bound worker requires a separate migration"
        );
        for (_, item) in value["observations"]
            .as_object()
            .context("missing worker observations")?
        {
            let o: super::workload::Observation = serde_json::from_value(item[1].clone())?;
            ensure!(
                o.phase.terminal()
                    || o.phase
                        == super::workload::WorkPhase::Instance(
                            super::workload::InstancePhase::Stopped
                        ),
                "worker has unfinished accepted work"
            );
        }
        value["format"] = json!(super::FORMAT);
        return Ok(value);
    }
    ensure!(
        value["format"] == 5
            && value["recovery_inventory"].is_null()
            && value["recovery_binding"].is_null(),
        "worker requires an idle format-5 journal without a recovery binding"
    );
    let old = value["assignments"].clone();
    for (id, a) in old.as_object().context("missing worker assignments")? {
        let phase = value["observations"][id][1]["phase"].as_str().unwrap_or("");
        ensure!(
            if job_kind(a) {
                matches!(phase, "Succeeded" | "Failed" | "Cancelled")
            } else {
                matches!(phase, "Stopped" | "Deleted")
            },
            "worker has unfinished accepted work: {id}"
        );
    }
    for (id, a) in value["assignments"].as_object_mut().unwrap() {
        assignment(id, a)?;
    }
    for (id, o) in value["observations"]
        .as_object_mut()
        .context("missing worker observations")?
    {
        observation(o, job_kind(&old[id]))?;
    }
    for report in value["outbox"]
        .as_object_mut()
        .context("missing worker outbox")?
        .values_mut()
    {
        let id = report["assignment"]
            .as_str()
            .context("missing report assignment")?
            .to_owned();
        phase(&mut report["observation"]["phase"], job_kind(&old[&id]))?;
    }
    value["format"] = json!(super::FORMAT);
    value["checkpoints"] = json!({});
    value["known_checkpoints"] = json!([]);
    Ok(value)
}

pub(crate) fn backup(path: &Path) -> Result<()> {
    backup_version(path, 5)
}
pub(crate) fn backup_version(path: &Path, version: u32) -> Result<()> {
    let mut backup = path.with_extension(format!(
        "{}.v{version}-backup",
        path.extension().and_then(|s| s.to_str()).unwrap_or("file")
    ));
    if backup.exists() {
        backup = backup.with_extension(format!("v{version}-backup-{:016x}", rand::random::<u64>()));
    }
    let mut target = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&backup)
        .with_context(|| format!("create immutable upgrade backup {}", backup.display()))?;
    std::io::copy(&mut std::fs::File::open(path)?, &mut target)?;
    target.sync_all()?;
    std::fs::File::open(path.parent().context("backup parent")?)?.sync_all()?;
    Ok(())
}

/// Explicit metadata-only conversion. Each database commits atomically and a rerun skips converted databases.
pub fn run(
    controller: &Path,
    workers: &[std::path::PathBuf],
    clients: &[std::path::PathBuf],
) -> Result<()> {
    let controller_path = controller.join("control.redb");
    // Even an explicitly requested offline migration must reject incompatible source metadata
    // before redb can repair or mutate the real file.
    let raw = super::version::preflight(&controller_path, "meta")?
        .context("missing controller database")?;
    let source_format = raw["identity"][0]
        .as_u64()
        .context("missing source format")?;
    if source_format == super::FORMAT as u64 {
        let cluster = raw["identity"][1]
            .as_str()
            .context("missing source cluster")?;
        let node = raw["identity"][2].as_u64().context("missing source node")?;
        super::Store::preflight(&controller_path, cluster, node)?;
    } else {
        ensure!(
            (5..=11).contains(&source_format),
            "unsupported source format"
        );
        let generation = raw["state"]["version"]["active"].as_u64().unwrap_or(1);
        super::version::Software::current().require(u32::try_from(generation)?)?;
    }
    let controller_db =
        redb::Database::open(&controller_path).context("stop the controller before upgrading")?;
    let worker_dbs = workers
        .iter()
        .map(|home| redb::Database::open(home.join("worker.redb")))
        .collect::<std::result::Result<Vec<_>, _>>()
        .context("stop every selected worker before upgrading")?;
    let _client_locks = clients
        .iter()
        .map(|home| crate::keys::lock_coordinator(home, std::time::Duration::from_secs(1)))
        .collect::<Result<Vec<_>>>()?;
    let state = super::store::Store::upgrade_v5(&controller_db, &controller_path, true)?;
    let assignments = serde_json::to_value(&state.assignments)?;
    for (home, db) in workers.iter().zip(&worker_dbs) {
        super::runtime::Worker::upgrade_v5(db, home, &state, true)?;
    }
    let mut requests = Vec::new();
    for home in clients {
        let config: super::Config =
            serde_json::from_slice(&std::fs::read(home.join("config.json"))?)?;
        ensure!(
            config.cluster == state.cluster,
            "client belongs to another control plane"
        );
        let root = home.join("requests");
        if !root.exists() {
            continue;
        }
        for file in std::fs::read_dir(root)? {
            let path = file?.path();
            if path.extension().and_then(|s| s.to_str()) != Some("json") {
                continue;
            }
            let mut value: Value = serde_json::from_slice(&std::fs::read(&path)?)?;
            let before = value.clone();
            mutation(&mut value["request"]["mutation"], &assignments)?;
            if value != before {
                requests.push((path, value));
            }
        }
    }
    // All identities and source records validate, and every selected database stays exclusively locked.
    super::store::Store::upgrade_v5(&controller_db, &controller_path, false)?;
    for (home, db) in workers.iter().zip(&worker_dbs) {
        super::runtime::Worker::upgrade_v5(db, home, &state, false)?;
    }
    for (path, value) in requests {
        backup(&path)?;
        crate::instances::write(&path, &value)?;
    }
    println!(
        "{}",
        serde_json::to_string(
            &json!({"format":super::FORMAT,"cluster":state.cluster,"jobs":state.jobs.len(),"assignments":state.assignments.len(),"workers_upgraded":workers.len(),"clients_upgraded":clients.len()})
        )?
    );
    Ok(())
}
