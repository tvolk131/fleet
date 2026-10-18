use super::support::*;
use anyhow::{Context, Result, ensure};
use clap::Args;
use serde_json::{Value, json};
use std::{fs, os::unix::fs::PermissionsExt, path::PathBuf};

#[derive(Args)]
pub struct Options {
    #[arg(long)]
    pub binary: PathBuf,
    #[arg(long)]
    pub home: PathBuf,
    #[arg(long)]
    pub fake: bool,
    #[arg(long)]
    pub instances: Option<PathBuf>,
    #[arg(long)]
    pub image_cache: Option<PathBuf>,
    #[arg(long)]
    pub job: Option<PathBuf>,
    #[arg(long)]
    pub rolling: bool,
    #[arg(long)]
    pub backup_restore: bool,
    #[arg(long)]
    pub history: bool,
    #[arg(long)]
    pub storage_lifecycle: bool,
    #[arg(long)]
    pub baseline_binary: Option<PathBuf>,
}
pub fn rolling_reclaimed(status: &Value, input_bytes: u64) -> bool {
    status["workers"].as_array().is_some_and(|rows| {
        !rows.is_empty()
            && rows.iter().all(|r| {
                let report = &r["report"];
                report["checkpoints"].as_object().is_some_and(|c| {
                    !c.is_empty() && c.values().all(|c| c["live_checkpoints"] == 2)
                }) && report["blob_payload_bytes"]
                    .as_u64()
                    .is_some_and(|n| n <= input_bytes + 2 * 65536)
            })
    })
}
fn real_guard(a: &Options) -> Result<()> {
    if a.fake {
        return Ok(());
    }
    ensure!(
        a.instances.is_some() && a.image_cache.is_some(),
        "real Tart needs --instances and --image-cache"
    );
    let home = PathBuf::from(std::env::var("HOME")?);
    for config in [
        home.join("fleet/replicated/service.json"),
        home.join("fleet/single-m4/service.json"),
    ] {
        if config.is_file() {
            let config = read(&config)?;
            if let Some(client) = config["client_home"].as_str() {
                let binary = home.join("fleet/bin/fleet");
                let queue = api_result(&binary, client.as_ref(), json!({"op":"queue"}))?;
                ensure!(
                    queue.as_array().unwrap().iter().all(|j| matches!(
                        j["phase"].as_str(),
                        Some("succeeded" | "failed" | "cancelled" | "suspended")
                    )),
                    "installed jobs active"
                );
                let instances = api_result(&binary, client.as_ref(), json!({"op":"instances"}))?;
                ensure!(
                    instances.as_array().unwrap().iter().all(|i| [
                        json!("Stopped"),
                        json!("Deleted"),
                        json!({"Instance":"Stopped"}),
                        json!({"Instance":"Deleted"})
                    ]
                    .contains(&i["observed"][1]["phase"])),
                    "installed VMs active"
                );
            }
        }
    }
    if let Ok(jobs) = fs::read_dir(home.join(".fleet/jobs")) {
        for job in jobs {
            let job = job?;
            ensure!(
                !job.path().join("job.json").exists() || job.path().join("exit.json").exists(),
                "legacy training active"
            );
        }
    }
    Ok(())
}
fn settings(l: &Lab, a: &Options) -> Result<Value> {
    let mut s = if a.fake {
        fs::write(
            l.root.join("softnet"),
            "#!/bin/sh\necho 'softnet 0.24.0-test'\n",
        )?;
        fs::set_permissions(l.root.join("softnet"), fs::Permissions::from_mode(0o700))?;
        json!({"tart":l.s("fleet-fake-tart"),"softnet":l.s("softnet"),"images":{"macos":format!("registry.test/macos@sha256:{}","a".repeat(64))},"block_cidrs":[]})
    } else {
        read(a.instances.as_ref().context("instances required")?)?
    };
    s["memory_budget_mb"] = json!(if a.fake { 4096 } else { 8192 });
    s["disk_budget_gb"] = 100.into();
    s["min_free_disk_gb"] = 5.into();
    Ok(s)
}
fn worker(l: &Lab, a: &Options, id: &str, settings: &Value) -> Result<()> {
    save(&l.root.join(format!("{id}-instances.json")), settings)?;
    l.cli(&[
        "cluster",
        "prepare-worker",
        "--home",
        &l.s(id),
        "--config",
        &l.s("client/config.json"),
        "--id",
        id,
        "--cpu-slots",
        if a.fake && settings["memory_budget_mb"] != 8192 {
            "1"
        } else {
            "4"
        },
        "--memory-mb",
        &settings["memory_budget_mb"].to_string(),
        "--instances",
        &l.s(&format!("{id}-instances.json")),
    ])?;
    if !a.fake {
        let cache = l.root.join(format!("{id}/backend/instance-tart/cache"));
        fs::create_dir_all(cache.parent().unwrap())?;
        let out = output(
            "/bin/cp".as_ref(),
            &[
                "-cR".into(),
                a.image_cache.as_ref().unwrap().display().to_string(),
                cache.display().to_string(),
            ],
            None,
            180,
        )?;
        ensure!(out.status.success(), "private image cache copy failed");
    }
    l.cli(&[
        "service",
        "configure",
        "--config",
        &l.s(&format!("{id}.json")),
        "--worker-home",
        &l.s(id),
    ])?;
    Ok(())
}
fn storage(l: &Lab, id: &str) -> Result<()> {
    l.cli(&[
        "cluster",
        "prepare-storage",
        "--home",
        &l.s(id),
        "--config",
        &l.s("client/config.json"),
        "--id",
        id,
        "--failure-domain",
        "same-qualification-machine",
        "--capacity-bytes",
        &(64u64 << 20).to_string(),
    ])?;
    l.cli(&[
        "service",
        "configure",
        "--config",
        &l.s(&format!("{id}.json")),
        "--storage-home",
        &l.s(id),
    ])?;
    Ok(())
}
fn enroll(l: &Lab, id: &str, store: bool) -> Result<()> {
    let mut args = vec!["--home".into(), l.s("client")];
    if store {
        args.push("storage".into());
    }
    args.extend([
        "enroll".into(),
        l.s(&format!(
            "{id}/{}",
            if store {
                "registration.json"
            } else {
                "enroll.json"
            }
        )),
        "--request-id".into(),
        format!("enroll-{id}"),
    ]);
    checked(&l.binary, &args)?;
    Ok(())
}
fn phase(l: &Lab, id: &str, phase: &str) -> Result<Value> {
    wait(
        || l.request(json!({"op":"job","id":id})),
        |j| j["phase"] == phase,
        300,
    )
}
fn deleted_vms(l: &Lab, workers: &[&str]) -> Result<usize> {
    let mut count = 0;
    for worker in workers {
        let path = l.root.join(format!("{worker}/backend/instances"));
        if !path.exists() {
            continue;
        }
        for entry in fs::read_dir(path)? {
            let path = entry?.path().join("observed.json");
            if path.exists() {
                ensure!(
                    read(&path)?["state"] == "deleted",
                    "fixture VM not deleted: {}",
                    path.display()
                );
                count += 1;
            }
        }
    }
    Ok(count)
}
fn copied(rows: &Value, kind: &Value, store: &str) -> bool {
    rows.as_array().is_some_and(|rows| {
        rows.iter().any(|r| {
            r["artifact"]["manifest"]["kind"] == *kind
                && r["artifact"]["copies"][format!("storage/{store}")]["phase"] == "Ready"
        })
    })
}
fn find_artifact(rows: &Value, kind: &Value) -> Result<Value> {
    rows.as_array()
        .context("artifact rows")?
        .iter()
        .find(|r| r["artifact"]["manifest"]["kind"] == *kind)
        .map(|r| r["artifact"].clone())
        .context("artifact missing")
}
fn delete_all(l: &Lab) -> Result<()> {
    for inputs in [false, true] {
        for row in l.request(json!({"op":"artifacts"}))?.as_array().unwrap() {
            let mut art = row["artifact"].clone();
            if (art["manifest"]["kind"] == "Inputs") != inputs || art["deleted"] == true {
                continue;
            }
            let id = art["manifest"]["id"].clone();
            if art["pinned"] == true {
                l.request(json!({"op":"artifact_retention","id":id,"expected_revision":art["revision"],"pinned":false,"expires_at":null,"request_id":format!("unpin-{}",id.as_str().unwrap())}))?;
                art = l
                    .request(json!({"op":"artifacts"}))?
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|r| r["artifact"]["manifest"]["id"] == id)
                    .unwrap()["artifact"]
                    .clone();
            }
            l.request(json!({"op":"delete_artifact","id":id,"expected_revision":art["revision"],"request_id":format!("delete-{}",id.as_str().unwrap())}))?;
        }
    }
    wait(
        || l.request(json!({"op":"artifacts"})),
        |rows| {
            rows.as_array().unwrap().iter().all(|r| {
                r["artifact"]["deleted"] == true
                    && r["artifact"]["copies"]
                        .as_object()
                        .unwrap()
                        .values()
                        .all(|c| matches!(c["phase"].as_str(), Some("Deleted" | "Lost")))
            })
        },
        240,
    )?;
    Ok(())
}
pub fn checkpoint(a: Options) -> Result<()> {
    real_guard(&a)?;
    ensure!(
        !(a.job.is_some() && (a.rolling || a.backup_restore)),
        "custom job cannot use built-in rolling/backup assertions"
    );
    let mut l = Lab::new(&a.home, &a.binary)?;
    l.init(1)?;
    let mut s = settings(&l, &a)?;
    let platform = json!({"os":"macos","arch":"aarch64","capabilities":[]});
    let runtime = json!({"platform":platform,"image_digest":s["images"]["macos"].as_str().unwrap().split('@').nth(1).unwrap()});
    if a.backup_restore {
        s["platforms"] = json!({"macos":platform});
    }
    worker(&l, &a, "worker", &s)?;
    let mut configure = vec![
        "service".into(),
        "configure".into(),
        "--config".into(),
        l.s("service.json"),
        "--controller-home".into(),
        l.s("seed/node-1"),
        "--worker-home".into(),
        l.s("worker"),
        "--client-home".into(),
        l.s("client"),
        "--bootstrap".into(),
    ];
    if a.backup_restore {
        storage(&l, "backup")?;
        configure.extend(["--storage-home".into(), l.s("backup")]);
    }
    checked(&l.binary, &configure)?;
    let trainer = l.s("fleet-test-workload");
    let input_bytes = fs::metadata(&trainer)?.len();
    let mut job = if let Some(path) = &a.job {
        read(path)?
    } else {
        json!({"name":"Checkpoint qualification","argv":["./trainer",if a.rolling{"rolling"}else{"checkpoint"},if a.rolling{"24"}else{"7"},"root=checkpoints"],"inputs":{"trainer":trainer},"publish":["result.txt"],"checkpoint":{"manifests":"checkpoints/*/fleet.json","resume_argv":["./trainer","resume","{checkpoint}/state"]}})
    };
    if a.rolling {
        job["checkpoint"]["retention"] = json!({"keep_last":2,"max_live_checkpoints":6,"max_live_bytes":2<<20,"transfer_bytes_per_hour":8<<20});
    }
    job["keep_awake"] = false.into();
    job["settle_seconds"] = 0.into();
    job["max_seconds"] = 600.into();
    job["threads"] = json!(if a.fake { 1 } else { 2 });
    job["vm"] = json!({"image":"macos","cpus":if a.fake{1}else{2},"memory_mb":if a.fake{4096}else{6144},"disk_gb":50,"network":"offline","gpu":"exclusive"});
    if a.backup_restore {
        job["vm"]["runtime"] = runtime;
    }
    l.start("service", None)?;
    enroll(&l, "worker", false)?;
    if a.backup_restore {
        enroll(&l, "backup", true)?;
        l.request(json!({"op":"storage_policy","request_id":"policy","expected_revision":0,"policy":{"checkpoints":["backup"],"outputs":["backup"],"inputs":["backup"],"history":[]}}))?;
    }
    l.request(json!({"op":"submit","id":"resumable","request_id":"submit","worker":"worker","memory_mb":if a.fake{4096}else{6144},"job":job}))?;
    let cp = if a.rolling {
        let captures = wait(
            || l.request(json!({"op":"checkpoints","id":"resumable"})),
            |rows| rows.as_array().unwrap().len() == 24,
            480,
        )?;
        let status = wait(
            || l.request(json!({"op":"storage"})),
            |s| rolling_reclaimed(s, input_bytes),
            240,
        )?;
        save(&l.root.join("rolling-storage.json"), &status)?;
        let artifacts = l.request(json!({"op":"artifacts"}))?;
        ensure!(
            artifacts
                .as_array()
                .unwrap()
                .iter()
                .filter(|r| r["artifact"]["deleted"] != true
                    && r["artifact"]["manifest"]["kind"]
                        .get("Checkpoint")
                        .is_some())
                .count()
                == 2,
            "rolling retention failed"
        );
        ensure!(
            captures.as_array().unwrap().len() == 24,
            "missing checkpoints"
        );
        l.phase("rolling_24_captures_two_live_and_bounded_physical_cache");
        l.request(json!({"op":"job","id":"resumable"}))?["latest_checkpoint"].clone()
    } else {
        wait(
            || l.request(json!({"op":"checkpoints","id":"resumable"})),
            |v| v.as_array().is_some_and(|v| !v.is_empty()),
            240,
        )?[0]
            .clone()
    };
    let mut backup = Value::Null;
    if a.backup_restore {
        let rows = wait(
            || l.request(json!({"op":"artifacts"})),
            |rows| copied(rows, &json!({"Checkpoint":cp["id"]}), "backup"),
            240,
        )?;
        backup = find_artifact(&rows, &json!({"Checkpoint":cp["id"]}))?;
    }
    l.request(
        json!({"op":"fetch_checkpoint","id":cp["id"],"destination":l.s("checkpoint-download")}),
    )?;
    ensure!(
        l.root.join("checkpoint-download/checkpoint.json").is_file(),
        "checkpoint export missing"
    );
    let running = l.request(json!({"op":"job","id":"resumable"}))?;
    l.request(json!({"op":"suspend","id":"resumable","checkpoint":cp["id"],"expected_revision":running["revision"],"request_id":"suspend"}))?;
    ensure!(
        l.request(json!({"op":"retry","request_id":"suspend"}))?["reply"]["Accepted"]["job"]
            == "resumable",
        "suspend replay changed"
    );
    phase(&l, "resumable", "suspended")?;
    let attempts = l.request(json!({"op":"attempts","id":"resumable"}))?;
    ensure!(
        attempts.as_array().unwrap().len() == 1,
        "wrong attempt count"
    );
    deleted_vms(&l, &["worker"])?;
    l.stop("service", true)?;
    if a.backup_restore {
        fs::rename(
            l.root.join("worker/blobs"),
            l.root.join("unavailable-worker-blobs"),
        )?;
        let source = l
            .root
            .join("worker/backend/jobs")
            .join(attempts[0]["execution"].as_str().unwrap())
            .join("checkpoints")
            .join(cp["content"]["id"].as_str().unwrap())
            .join("files");
        fs::rename(source, l.root.join("unavailable-checkpoint-files"))?;
    }
    l.start("service", None)?;
    let suspended = phase(&l, "resumable", "suspended")?;
    let expected = if a.rolling { "24" } else { "7" };
    if a.backup_restore {
        wait(
            || l.request(json!({"op":"storage"})),
            |s| {
                s["storage_reports"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|r| r["storage"] == "backup" && !r["report"].is_null())
            },
            240,
        )?;
        l.request(
            json!({"op":"fetch_checkpoint","id":cp["id"],"destination":l.s("backup-only-export")}),
        )?;
        ensure!(
            fs::read_to_string(l.root.join("backup-only-export/files/state"))?
                .lines()
                .next()
                == Some(expected),
            "backup export corrupted"
        );
    }
    // Both the checkpoint and executable are restored through the existing durable artifact path.
    l.request(json!({"op":"resume","id":"resumable","expected_revision":suspended["revision"],"request_id":"resume"}))?;
    l.request(json!({"op":"retry","request_id":"resume"}))?;
    let final_state = wait(
        || l.request(json!({"op":"job","id":"resumable"})),
        |j| {
            matches!(
                j["phase"].as_str(),
                Some("succeeded" | "failed" | "cancelled")
            )
        },
        300,
    )?;
    ensure!(
        final_state["phase"] == "succeeded",
        "resume failed {final_state}"
    );
    let attempts = l.request(json!({"op":"attempts","id":"resumable"}))?;
    ensure!(
        attempts.as_array().unwrap().len() == 2
            && attempts[0]["execution"] != attempts[1]["execution"],
        "attempt identities changed"
    );
    for n in [1, 2] {
        l.cli(&[
            "--home",
            &l.s("client"),
            "logs",
            "resumable",
            "--attempt",
            &n.to_string(),
        ])?;
        save(
            &l.root.join(format!("attempt-{n}-logs.json")),
            &l.request(json!({"op":"logs","id":"resumable","attempt":n}))?,
        )?;
    }
    l.request(json!({"op":"fetch","id":"resumable","destination":l.s("outputs"),"attempt":2}))?;
    if a.job.is_none() {
        ensure!(
            fs::read_to_string(l.root.join("outputs/result.txt"))?
                == if a.rolling { "25" } else { "8" },
            "wrong resumed result"
        );
        for (n, needle) in [(1, "initial-once"), (2, "resumed-once")] {
            ensure!(
                l.request(json!({"op":"logs","id":"resumable","attempt":n}))?["Logs"]["data"]
                    .as_str()
                    .unwrap()
                    .matches(needle)
                    .count()
                    == 1,
                "work replayed"
            );
        }
    }
    if a.backup_restore {
        ensure!(
            fs::read_to_string(
                l.root
                    .join("worker/restored")
                    .join(backup["manifest"]["id"].as_str().unwrap())
                    .join("state")
            )?
            .lines()
            .next()
                == Some(expected),
            "worker restore corrupted"
        );
        wait(
            || l.request(json!({"op":"artifacts"})),
            |rows| copied(rows, &json!("FinalOutputs"), "backup"),
            240,
        )?;
        delete_all(&l)?;
        ensure!(
            l.request(json!({"op":"attempts","id":"resumable"}))?
                .as_array()
                .unwrap()
                .len()
                == 2,
            "cleanup removed history"
        );
    }
    let vm_count = deleted_vms(&l, &["worker"])?;
    l.phase("suspend_delete_restart_backup_restore_and_fresh_vm_resume");
    l.report(json!({"success":true,"real_tart":!a.fake,"fixture_vms_created":vm_count,"all_fixture_vms_deleted":true,"unique_attempts":2,"off_worker_restore_verified":a.backup_restore,"rolling_reclamation_verified":a.rolling,"executable_input_bytes":input_bytes,"physical_failure_domain":"one machine; no physical host-loss claim"}))
}

fn backed(l: &Lab, number: usize) -> Result<Value> {
    let h = l.request(json!({"op":"history","id":"portable"}))?;
    Ok(json!(
        h["attempts"][number - 1]["streams"]
            .as_array()
            .is_some_and(|streams| streams.iter().all(|s| !s["progress"].is_null()
                && s["progress"]["closed"] == true
                && s["progress"]["published"] == s["progress"]["observed"]
                && s["backed_up_bytes"] == s["retained_bytes"]
                && s["backed_up_bytes"].as_u64().is_some_and(|n| n > 0)))
    ))
}
fn stream(l: &Lab, number: usize, op: &str, stderr: bool) -> Result<String> {
    let mut offset = 0;
    let mut text = String::new();
    loop {
        let mut request =
            json!({"op":op,"id":"portable","attempt":number,"from":offset,"max":1<<20});
        if op == "logs" {
            request["stderr"] = stderr.into();
        }
        let mut p = l.request(request)?;
        if op == "logs" {
            p = p["Logs"].clone();
        }
        let data = p["data"].as_str().context("history data required")?;
        ensure!(
            p["gap"].is_null() && (p["archived"] == true || data.is_empty()),
            "history not fully archived: {p}"
        );
        text.push_str(data);
        if p["done"] == true {
            return Ok(text);
        }
        let next = p["next"].as_u64().context("history cursor required")?;
        ensure!(next > offset, "history cursor stalled");
        offset = next;
    }
}
fn metric_steps(text: &str) -> Result<Vec<u64>> {
    let rows = text
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(rows
        .iter()
        .filter(|m| m["kind"] == "jsonl")
        .map(|m| m["record"]["step"].as_u64().unwrap())
        .collect())
}

pub fn portable(mut a: Options) -> Result<()> {
    real_guard(&a)?;
    ensure!(
        a.baseline_binary.is_none() || a.fake,
        "mixed-version fixture must use simulated Tart"
    );
    a.history |= a.storage_lifecycle;
    let mut l = Lab::new(&a.home, &a.binary)?;
    let candidate = l.binary.clone();
    if let Some(baseline) = &a.baseline_binary {
        fs::copy(baseline, l.root.join("fleet-baseline"))?;
        l.binary = l.root.join("fleet-baseline");
    }
    l.init(1)?;
    let mut s = settings(&l, &a)?;
    let platform = json!({"os":"macos","arch":"aarch64","capabilities":[]});
    s["platforms"] = json!({"macos":platform});
    let runtime = json!({"platform":platform,"image_digest":s["images"]["macos"].as_str().unwrap().split('@').nth(1).unwrap()});
    worker(&l, &a, "source", &s)?;
    let mut initial = s.clone();
    initial["platforms"] = json!({});
    worker(&l, &a, "destination", &initial)?;
    storage(&l, "backup")?;
    if a.storage_lifecycle {
        for id in ["replacement", "reserve", "recovery"] {
            storage(&l, id)?;
        }
    }
    l.cli(&[
        "service",
        "configure",
        "--config",
        &l.s("control.json"),
        "--controller-home",
        &l.s("seed/node-1"),
        "--bootstrap",
    ])?;
    fs::create_dir(l.root.join("original-inputs"))?;
    fs::copy(
        l.root.join("fleet-test-workload"),
        l.root.join("original-inputs/trainer"),
    )?;
    fs::write(
        l.root.join("original-inputs/dataset"),
        "durable original data",
    )?;
    let mut argv = vec!["bin/trainer", "portable"];
    if a.history {
        argv.push("history");
    }
    let mut resume = vec!["bin/trainer", "resume", "{checkpoint}/state"];
    if a.history {
        resume.push("history");
    }
    let mut job = json!({"name":"Portable qualification","argv":argv,"inputs":{"bin/trainer":l.s("original-inputs/trainer"),"dataset":l.s("original-inputs/dataset")},"publish":["result.txt"],"keep_awake":false,"settle_seconds":0,"max_seconds":600,"threads":if a.fake{1}else{2},"vm":{"image":"macos","runtime":runtime,"cpus":if a.fake{1}else{2},"memory_mb":if a.fake{4096}else{6144},"disk_gb":50,"network":"offline","gpu":"exclusive"},"checkpoint":{"manifests":"cp/*/fleet.json","resume_argv":resume}});
    if a.history {
        job["metric_files"] = json!(["metrics.jsonl"]);
    }
    l.start("control", None)?;
    l.start("backup", None)?;
    if a.baseline_binary.is_some() {
        super::rolling::activate_to(&l, fleet::control::version::MIN)?;
    }
    for id in ["source", "destination"] {
        enroll(&l, id, false)?;
    }
    enroll(&l, "backup", true)?;
    l.request(json!({"op":"storage_policy","request_id":"policy","expected_revision":0,"policy":{"inputs":["backup"],"checkpoints":["backup"],"outputs":["backup"],"history":if a.history{vec!["backup"]}else{vec![]}}}))?;
    l.request(
        json!({"op":"drain","worker":"destination","drain":true,"request_id":"drain-destination"}),
    )?;
    let mut old = read(&l.root.join("destination/worker.json"))?["registration"].clone();
    save(&l.root.join("destination/backend/instances.json"), &s)?;
    let prepared = l.cli(&[
        "cluster",
        "prepare-worker-runtimes",
        "--home",
        &l.s("destination"),
    ])?;
    ensure!(
        l.cli(&[
            "cluster",
            "prepare-worker-runtimes",
            "--home",
            &l.s("destination")
        ])? == prepared,
        "runtime preparation not idempotent"
    );
    for _ in 0..2 {
        ensure!(
            l.cli(&[
                "cluster",
                "request",
                "--key",
                &l.s("client/node.key"),
                "--config",
                &l.s("client/config.json"),
                "--request",
                &l.s("destination/runtimes-request.json")
            ])? == "Recorded",
            "runtime update rejected"
        );
    }
    let mut updated = read(&l.root.join("destination/worker.json"))?["registration"].clone();
    ensure!(
        updated.as_object_mut().unwrap().remove("runtimes") == Some(json!({"macos":runtime})),
        "runtime changed"
    );
    old.as_object_mut().unwrap().remove("runtimes");
    ensure!(old == updated, "worker identity changed");
    l.start("source", None)?;
    l.start("destination", None)?;
    l.request(json!({"op":"submit","id":"portable","request_id":"portable-submit","memory_mb":0,"job":job}))?;
    let initial = wait(
        || {
            l.cli(&["--home", &l.s("client"), "stage", "portable"])?;
            l.request(json!({"op":"job","id":"portable"}))
        },
        |j| !j["latest_checkpoint"].is_null(),
        240,
    )?;
    let checkpoint = initial["latest_checkpoint"].clone();
    if a.baseline_binary.is_some() {
        let supervisor = l
            .root
            .join("source/backend/jobs/exec-portable-a1/supervisor.json");
        let original = fs::read(&supervisor)?;
        l.binary = candidate;
        l.stop("control", false)?;
        l.start("control", None)?;
        let v = l.request(json!({"op":"versions"}))?["version"].clone();
        l.request(json!({"op":"prepare_version","expected_revision":v["revision"],"target":fleet::control::version::MAX,"request_id":"vm-upgrade-prepare"}))?;
        let v = wait(
            || Ok(l.request(json!({"op":"versions"}))?["version"].clone()),
            |v| v["pending"]["ready"] == json!([1]),
            240,
        )?;
        l.request(json!({"op":"activate_version","expected_revision":v["revision"],"request_id":"vm-upgrade-activate"}))?;
        phase(&l, "portable", "running")?;
        for role in ["source", "destination", "backup"] {
            l.stop(role, false)?;
            l.start(role, None)?;
            phase(&l, "portable", "running")?;
            ensure!(
                fs::read(&supervisor)? == original,
                "supervisor changed during upgrade"
            );
        }
        ensure!(
            l.request(json!({"op":"versions"}))?["version"]["active"]
                == fleet::control::version::MAX,
            "generation not activated"
        );
        l.phase("rolling_vm_agent_upgrade_preserves_running_supervisor");
    }
    ensure!(
        initial["assignment"]["worker"] == "source",
        "wrong initial worker"
    );
    let bundle = initial["input_bundle"].clone();
    wait(
        || l.request(json!({"op":"artifacts"})),
        |rows| copied(rows, &json!({"Checkpoint":checkpoint["id"]}), "backup"),
        240,
    )?;
    save(
        &l.root.join("before-suspend.json"),
        &l.request(json!({"op":"job","id":"portable"}))?,
    )?;
    l.request(json!({"op":"suspend","id":"portable","checkpoint":checkpoint["id"],"expected_revision":initial["revision"],"request_id":"suspend"}))?;
    let suspended = phase(&l, "portable", "suspended")?;
    deleted_vms(&l, &["source"])?;
    if a.history {
        wait(|| backed(&l, 1), |b| b == true, 240)?;
    }
    l.stop("source", false)?;
    fs::rename(l.root.join("source"), l.root.join("unavailable-source"))?;
    fs::rename(l.root.join("client"), l.root.join("unavailable-client"))?;
    fs::remove_dir_all(l.root.join("original-inputs"))?;
    l.cli(&[
        "--home",
        &l.s("client"),
        "client",
        "import",
        "--config",
        &l.s("seed/client/config.json"),
        "--key",
        &l.s("seed/client/node.key"),
    ])?;
    l.stop("control", false)?;
    l.start("control", None)?;
    wait(
        || l.request(json!({"op":"storage"})),
        |s| {
            s["storage_reports"]
                .as_array()
                .unwrap()
                .iter()
                .any(|r| !r["report"].is_null())
        },
        240,
    )?;
    let mut backup_id = "backup";
    if a.storage_lifecycle {
        for id in ["replacement", "reserve", "recovery"] {
            enroll(&l, id, true)?;
        }
        let cp = find_artifact(
            &l.request(json!({"op":"artifacts"}))?,
            &json!({"Checkpoint":checkpoint["id"]}),
        )?;
        l.request(json!({"op":"artifact_retention","id":cp["manifest"]["id"],"expected_revision":cp["revision"],"pinned":true,"expires_at":null,"request_id":"pin-during-replacement"}))?;
        l.cli(&[
            "--home",
            &l.s("client"),
            "storage",
            "replace",
            "backup",
            "replacement",
            "--revision",
            "0",
            "--request-id",
            "replace-backup",
        ])?;
        l.request(json!({"op":"retry","request_id":"replace-backup"}))?;
        let plan = l.request(json!({"op":"storage_plan","storage":"backup"}))?;
        ensure!(
            !plan["awaiting_replacement"].as_array().unwrap().is_empty(),
            "replacement already ready"
        );
        let refused=l.envelope(json!({"op":"retire_storage","storage":"backup","expected_revision":1,"expected_inventory":plan["inventory"],"request_id":"too-early-retirement"}))?;
        ensure!(
            refused["ok"] == true && refused["result"]["reply"].get("Rejected").is_some(),
            "premature retirement accepted"
        );
        l.start("replacement", None)?;
        wait(
            || l.request(json!({"op":"storage_plan","storage":"backup"})),
            |p| {
                p["awaiting_replacement"]
                    .as_array()
                    .is_some_and(Vec::is_empty)
            },
            240,
        )?;
        l.stop("replacement", false)?;
        l.start("replacement", None)?;
        let p = wait(
            || l.request(json!({"op":"storage_plan","storage":"backup"})),
            |p| {
                p["awaiting_replacement"]
                    .as_array()
                    .is_some_and(Vec::is_empty)
            },
            240,
        )?;
        l.cli(&[
            "--home",
            &l.s("client"),
            "storage",
            "retire",
            "backup",
            "--revision",
            "1",
            "--inventory",
            p["inventory"].as_str().unwrap(),
            "--request-id",
            "retire-backup",
        ])?;
        wait(
            || l.request(json!({"op":"storage_plan","storage":"backup"})),
            |p| p["awaiting_cleanup"].as_array().is_some_and(Vec::is_empty),
            240,
        )?;
        l.cli(&[
            "--home",
            &l.s("client"),
            "storage",
            "finish-retirement",
            "backup",
            "--revision",
            "2",
            "--request-id",
            "finish-backup",
        ])?;
        l.stop("backup", false)?;
        l.start("backup", None)?;
        ensure!(
            l.request(json!({"op":"storage_plan","storage":"backup"}))?["lifecycle"]["phase"]
                == "retired",
            "retired identity returned"
        );
        l.stop("backup", false)?;
        ensure!(
            fs::read_dir(l.root.join("backup/objects"))?
                .next()
                .is_none(),
            "retired payloads remain"
        );
        backup_id = "replacement";
        let cp = find_artifact(
            &l.request(json!({"op":"artifacts"}))?,
            &json!({"Checkpoint":checkpoint["id"]}),
        )?;
        ensure!(
            cp["pinned"] == true && cp["deleted"] == false,
            "replacement lost retention"
        );
        l.phase("storage_replacement_retirement_restart_and_pins_with_source_offline");
    }
    if a.history {
        ensure!(
            stream(&l, 1, "logs", false)?.ends_with("initial-once\n")
                && stream(&l, 1, "logs", true)? == "diagnostic-start\n",
            "archived logs changed"
        );
        ensure!(
            metric_steps(&stream(&l, 1, "metrics", false)?)? == (0..2000).collect::<Vec<_>>(),
            "metric records duplicated or lost"
        );
        l.phase("archived_large_logs_and_exact_metrics_with_source_worker_offline");
    }
    l.request(json!({"op":"drain","worker":"destination","drain":false,"request_id":"enable-destination"}))?;
    l.cli(&[
        "--home",
        &l.s("client"),
        "resume",
        "portable",
        "--worker",
        "destination",
        "--request-id",
        "resume-on-destination",
        "--revision",
        &suspended["revision"].to_string(),
    ])?;
    l.request(json!({"op":"retry","request_id":"resume-on-destination"}))?;
    let final_state = wait(
        || l.request(json!({"op":"job","id":"portable"})),
        |j| {
            matches!(
                j["phase"].as_str(),
                Some("succeeded" | "failed" | "cancelled")
            )
        },
        300,
    )?;
    ensure!(
        final_state["phase"] == "succeeded"
            && final_state["assignment"]["worker"] == "destination"
            && final_state["attempts"].as_array().unwrap().len() == 2,
        "portable resume failed {final_state}"
    );
    wait(
        || l.request(json!({"op":"artifacts"})),
        |rows| {
            rows.as_array().unwrap().iter().any(|r| {
                r["artifact"]["manifest"]["kind"] == "FinalOutputs"
                    && r["artifact"]["manifest"]["origin"]["Attempt"]["attempt"]
                        == final_state["assignment"]["id"]
                    && r["artifact"]["copies"][format!("storage/{backup_id}")]["phase"] == "Ready"
            })
        },
        240,
    )?;
    l.request(json!({"op":"fetch","id":"portable","destination":l.s("outputs")}))?;
    ensure!(
        fs::read_to_string(l.root.join("outputs/result.txt"))? == "8",
        "resumed output changed"
    );
    ensure!(
        l.request(json!({"op":"logs","id":"portable"}))?["Logs"]["data"]
            .as_str()
            .unwrap()
            .matches("resumed-once")
            .count()
            == 1,
        "resume replayed"
    );
    if a.history {
        wait(|| backed(&l, 2), |b| b == true, 240)?;
        ensure!(
            stream(&l, 2, "logs", false)? == "resumed-once\n"
                && stream(&l, 2, "logs", true)? == "diagnostic-resume\n",
            "second attempt logs changed"
        );
        ensure!(
            metric_steps(&stream(&l, 2, "metrics", false)?)? == (0..2000).collect::<Vec<_>>(),
            "second attempt metric records changed"
        );
        let h = l.request(json!({"op":"history","id":"portable"}))?;
        ensure!(
            h["attempts"][0]["worker"] == "source" && h["attempts"][1]["worker"] == "destination",
            "attempt workers changed"
        );
        let mut policy = h["retention"]["policy"].clone();
        policy["log_days"] = 7.into();
        ensure!(l.request(json!({"op":"history_retention","id":"portable","expected_revision":h["retention"]["revision"],"policy":policy,"request_id":"history-retention"}))?["reply"]=="Recorded","history policy update failed");
    }
    ensure!(
        final_state["attempts"][0] == suspended["attempts"][0],
        "original attempt changed"
    );
    save(&l.root.join("after-resume.json"), &final_state)?;
    l.phase("cross_worker_resume_with_original_worker_client_and_inputs_unavailable");
    if a.storage_lifecycle {
        let status = l.request(json!({"op":"storage"}))?;
        l.request(json!({"op":"storage_policy","expected_revision":status["revision"],"policy":{"inputs":["replacement","reserve"],"checkpoints":["replacement","reserve"],"outputs":["replacement","reserve"],"history":["replacement","reserve"]},"request_id":"redundant-backups"}))?;
        l.start("reserve", None)?;
        wait(
            || l.request(json!({"op":"artifacts"})),
            |rows| {
                rows.as_array()
                    .unwrap()
                    .iter()
                    .all(|r| r["artifact"]["copies"]["storage/reserve"]["phase"] == "Ready")
            },
            240,
        )?;
        l.stop("replacement", false)?;
        fs::rename(l.root.join("replacement"), l.root.join("lost-replacement"))?;
        let p = l.request(json!({"op":"storage_plan","storage":"replacement"}))?;
        ensure!(
            p["unavailable_without_agent"]
                .as_array()
                .is_some_and(Vec::is_empty),
            "no surviving backup"
        );
        l.cli(&[
            "--home",
            &l.s("client"),
            "storage",
            "declare-lost",
            "replacement",
            "recovery",
            "--revision",
            "0",
            "--inventory",
            p["inventory"].as_str().unwrap(),
            "--request-id",
            "declare-lost-replacement",
        ])?;
        l.request(json!({"op":"retry","request_id":"declare-lost-replacement"}))?;
        l.start("recovery", None)?;
        wait(
            || l.request(json!({"op":"storage_plan","storage":"replacement"})),
            |p| {
                p["awaiting_replacement"]
                    .as_array()
                    .is_some_and(Vec::is_empty)
            },
            240,
        )?;
        fs::rename(l.root.join("lost-replacement"), l.root.join("replacement"))?;
        l.start("replacement", None)?;
        ensure!(
            l.request(json!({"op":"storage_plan","storage":"replacement"}))?["lifecycle"]["phase"]
                == "lost",
            "lost identity returned"
        );
        l.stop("replacement", false)?;
        ensure!(
            fs::read_dir(l.root.join("replacement/objects"))?
                .next()
                .is_some(),
            "lost disk incorrectly erased"
        );
        backup_id = "recovery";
        l.stop("destination", false)?;
        ensure!(
            stream(&l, 2, "logs", false)? == "resumed-once\n",
            "history unavailable without workers"
        );
        l.request(json!({"op":"fetch","id":"portable","destination":l.s("recovered-outputs")}))?;
        ensure!(
            fs::read_to_string(l.root.join("recovered-outputs/result.txt"))? == "8",
            "lost-agent recovery corrupted output"
        );
        l.start("destination", None)?;
        l.phase("lost_agent_repaired_from_survivor_and_returning_identity_fenced");
    }
    let artifacts = l.request(json!({"op":"artifacts"}))?;
    let input = &artifacts
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["artifact"]["manifest"]["id"] == bundle)
        .context("input bundle missing")?["artifact"];
    let refused=l.envelope(json!({"op":"delete_artifact","id":bundle,"expected_revision":input["revision"],"request_id":"cannot-delete-dependent-inputs"}))?;
    ensure!(
        refused["ok"] == true && refused["result"]["reply"].get("Rejected").is_some(),
        "dependent input deletion accepted"
    );
    fs::rename(l.root.join("unavailable-source"), l.root.join("source"))?;
    l.start("source", None)?;
    wait(
        || l.request(json!({"op":"artifacts"})),
        |rows| copied(rows, &json!("FinalOutputs"), backup_id),
        240,
    )?;
    delete_all(&l)?;
    let count = deleted_vms(&l, &["source", "destination"])?;
    l.phase("dependent_artifact_retention_and_cleanup");
    l.report(json!({"success":true,"real_tart":!a.fake,"fixture_vms_created":count,"all_fixture_vms_deleted":true,"rolling_vm_agent_upgrade":a.baseline_binary.is_some(),"cross_worker_resume":true,"durable_executable_and_inputs":true,"storage_replacement":a.storage_lifecycle,"lost_agent_recovery":a.storage_lifecycle,"durable_history":a.history,"physical_failure_domain":"one machine; no physical host-loss claim"}))
}

pub fn normal(a: Options) -> Result<()> {
    real_guard(&a)?;
    let mut l = Lab::new(&a.home, &a.binary)?;
    l.init(3)?;
    let mut s = settings(&l, &a)?;
    s["memory_budget_mb"] = 8192.into();
    worker(&l, &a, "worker", &s)?;
    for n in 1..=3 {
        let mut args = vec![
            "service".into(),
            "configure".into(),
            "--config".into(),
            l.s(&format!("node-{n}.json")),
            "--controller-home".into(),
            l.s(&format!("seed/node-{n}")),
        ];
        if n == 1 {
            args.extend(["--worker-home".into(), l.s("worker"), "--bootstrap".into()]);
        }
        checked(&l.binary, &args)?;
        l.start(&format!("node-{n}"), None)?;
    }
    enroll(&l, "worker", false)?;
    fs::write(l.root.join("input.txt"), "normal queue verified input\n")?;
    let vm = json!({"image":"macos","cpus":2,"memory_mb":4096,"disk_gb":50,"network":"offline","gpu":"exclusive"});
    l.request(json!({"op":"submit","id":"vm-canary","request_id":"vm-submit","worker":"worker","memory_mb":4096,"job":{"name":"Normal VM canary","argv":["./trainer","canary"],"inputs":{"trainer":l.s("fleet-test-workload"),"input.txt":l.s("input.txt")},"publish":["result.txt","metrics.jsonl"],"keep_awake":false,"settle_seconds":0,"max_seconds":240,"vm":vm,"probes":[{"jsonl_field":{"name":"step","file":"metrics.jsonl","field":"step","total":180}}]}}))?;
    let active = phase(&l, "vm-canary", "running")?;
    l.request(json!({"op":"submit","id":"native-canary","request_id":"native-submit","worker":"worker","memory_mb":128,"job":{"name":"Alongside VM","argv":["/bin/sh","-c","echo native-once"],"keep_awake":false,"settle_seconds":0,"max_seconds":20}}))?;
    phase(&l, "native-canary", "succeeded")?;
    wait(
        || l.request(json!({"op":"outputs","id":"vm-canary"})),
        |v| {
            v["Outputs"]
                .as_array()
                .is_some_and(|a| a.iter().any(|v| v["path"] == "result.txt"))
        },
        180,
    )?;
    l.request(json!({"op":"fetch","id":"vm-canary","destination":l.s("live-output")}))?;
    ensure!(
        fs::read(l.root.join("live-output/result.txt"))? == fs::read(l.root.join("input.txt"))?,
        "live output changed"
    );
    let events = wait(
        || {
            l.request(
                json!({"op":"data","worker":"worker","request":{"Events":{"since":0,"wait_ms":0}}}),
            )
        },
        |v| {
            v["Events"]["events"].as_array().is_some_and(|a| {
                a.iter()
                    .any(|e| e["kind"]["Progress"]["job"] == active["assignment"]["execution"])
            })
        },
        180,
    )?;
    save(&l.root.join("live-events.json"), &events)?;
    let inspect = l.cli(&[
        "cluster",
        "inspect",
        "--key",
        &l.s("client/node.key"),
        "--config",
        &l.s("client/config.json"),
        "--replica",
        "1",
    ])?;
    let leader = inspect["leader"].as_u64().unwrap();
    l.stop(&format!("node-{leader}"), true)?;
    phase(&l, "vm-canary", "running")?;
    l.start(&format!("node-{leader}"), None)?;
    l.stop("node-1", true)?;
    l.start("node-1", None)?;
    let running = phase(&l, "vm-canary", "running")?;
    l.request(json!({"op":"cancel","id":"vm-canary","request_id":"vm-cancel","expected_revision":running["revision"]}))?;
    phase(&l, "vm-canary", "cancelled")?;
    ensure!(
        l.request(json!({"op":"logs","id":"vm-canary"}))?["Logs"]["data"]
            .as_str()
            .unwrap()
            .matches("guest-started-once")
            .count()
            == 1,
        "guest execution duplicated"
    );
    l.phase("live_vm_outputs_metrics_native_coexistence_and_leader_worker_restart");
    save(&l.root.join("vm-spec.json"), &vm)?;
    l.instances.push("persistent".into());
    l.cli(&[
        "--home",
        &l.s("client"),
        "instance",
        "create",
        "persistent",
        "--worker",
        "worker",
        "--spec",
        &l.s("vm-spec.json"),
        "--request-id",
        "vm-create",
    ])?;
    for (action, expected) in [
        (None, "Running"),
        (Some("stop"), "Stopped"),
        (Some("delete"), "Deleted"),
    ] {
        if let Some(action) = action {
            l.cli(&[
                "--home",
                &l.s("client"),
                "instance",
                action,
                "persistent",
                "--request-id",
                &format!("vm-{action}"),
            ])?;
        }
        wait(
            || l.request(json!({"op":"instances"})),
            |rows| {
                rows.as_array().unwrap().iter().any(|i| {
                    i["id"] == "persistent"
                        && i["observed"][0] == i["revision"]
                        && i["observed"][1]["phase"]["Instance"] == expected
                })
            },
            180,
        )?;
    }
    let count = deleted_vms(&l, &["worker"])?;
    l.phase("persistent_instance_create_stop_delete");
    l.report(json!({"success":true,"real_tart":!a.fake,"fixture_vms_created":count,"all_fixture_vms_deleted":true,"native_coexistence":true,"guest_started_once":true}))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn reclamation_waits_for_cache_after_checkpoint_cleanup() {
        let mut status = json!({"workers":[{"report":{"checkpoints":{"attempt":{"live_checkpoints":2}},"blob_payload_bytes":3*65536}}]});
        assert!(!rolling_reclaimed(&status, 0));
        status["workers"][0]["report"]["blob_payload_bytes"] = json!(2 * 65536);
        assert!(rolling_reclaimed(&status, 0));
        status["workers"][0]["report"]["checkpoints"]["attempt"]["live_checkpoints"] = 3.into();
        assert!(!rolling_reclaimed(&status, 0));
    }
    #[test]
    fn missing_reports_never_confirm_reclamation() {
        for status in [
            json!({"workers":[]}),
            json!({"workers":[{"report":null}]}),
            json!({"workers":[{"report":{"checkpoints":{},"blob_payload_bytes":0}}]}),
        ] {
            assert!(!rolling_reclaimed(&status, 0));
        }
    }
}
