use super::support::*;
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::{
    fs,
    path::{Path, PathBuf},
};
const BASE: u32 = fleet::control::version::MIN;
const NEXT: u32 = fleet::control::version::MAX;

/// Use the previous executable for every preparation/activation before installing
/// the candidate. Installing binaries must never silently upgrade stored behavior.
pub fn activate_to(l: &Lab, target: u32) -> Result<()> {
    let v = l.state()?["version"].clone();
    if v["active"] == target {
        return Ok(());
    }
    ensure!(
        v["active"].as_u64() == Some(target as u64 - 1),
        "baseline must activate one generation at a time"
    );
    l.request(json!({"op":"prepare_version","request_id":format!("baseline-prepare-{target}"),"expected_revision":v["revision"],"target":target}))?;
    let v = wait(
        || Ok(l.state()?["version"].clone()),
        |v| v["pending"]["members"] == v["pending"]["ready"],
        60,
    )?;
    l.request(json!({"op":"activate_version","request_id":format!("baseline-activate-{target}"),"expected_revision":v["revision"]}))?;
    ensure!(
        l.state()?["version"]["active"] == target,
        "baseline activation failed"
    );
    Ok(())
}

fn inspect(l: &Lab, n: u64) -> Result<Value> {
    l.cli(&[
        "cluster",
        "inspect",
        "--key",
        &l.s("client/node.key"),
        "--config",
        &l.s("client/config.json"),
        "--replica",
        &n.to_string(),
    ])
}
fn caught(l: &Lab, nodes: &[u64]) -> Result<()> {
    let barrier = l.state()?["last_applied"]["index"].as_u64().unwrap();
    wait(
        || {
            nodes
                .iter()
                .map(|n| inspect(l, *n))
                .collect::<Result<Vec<_>>>()
                .map(Value::from)
        },
        |xs| {
            xs.as_array().unwrap().iter().all(|s| {
                s["last_applied"]["index"]
                    .as_u64()
                    .is_some_and(|i| i >= barrier)
            })
        },
        60,
    )?;
    Ok(())
}
fn rejected(l: &Lab, r: Value) -> Result<()> {
    let answer = l.envelope(r)?;
    ensure!(
        answer["ok"] == true && answer["result"]["reply"].get("Rejected").is_some(),
        "expected rejection: {answer}"
    );
    Ok(())
}
fn unchanged(l: &Lab, n: u64, binary: &Path) -> Result<()> {
    let db = l.root.join(format!("seed/node-{n}/control.redb"));
    let before = digest(&db)?;
    for args in [
        vec![
            "service".into(),
            "run".into(),
            "--config".into(),
            l.s(&format!("node-{n}.json")),
        ],
        vec![
            "cluster".into(),
            "upgrade".into(),
            "--controller-home".into(),
            l.s(&format!("seed/node-{n}")),
        ],
    ] {
        let out = output(binary, &args, None, 60)?;
        ensure!(
            !out.status.success() && {
                let error = String::from_utf8_lossy(&out.stderr);
                error.contains("incompatible")
                    || error.contains("format mismatch")
                    || error.contains("unsupported source format")
            },
            "expected incompatible startup: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        ensure!(digest(&db)? == before, "rejection changed database");
    }
    Ok(())
}
fn live(l: &Lab, supervisor: &[u8], reply: &Value) -> Result<()> {
    ensure!(
        l.request(json!({"op":"job","id":"continuity"}))?["phase"] == "running",
        "canary stopped"
    );
    ensure!(
        fs::read(
            l.root
                .join("worker/backend/jobs/exec-continuity-a1/supervisor.json")
        )? == supervisor,
        "supervisor changed"
    );
    ensure!(
        l.request(json!({"op":"retry","request_id":"submit-continuity"}))?["reply"] == *reply,
        "receipt changed"
    );
    Ok(())
}
pub fn run(root: &Path, baseline: &Path, candidate: &Path, unsupported: &Path) -> Result<()> {
    let mut l = Lab::new(root, candidate)?;
    for binary in [baseline, candidate, unsupported] {
        ensure!(
            binary.is_absolute() && binary.is_file(),
            "explicit binary required"
        );
    }
    let current = l.binary.clone();
    l.binary = baseline.into();
    l.init(3)?;
    l.cli(&[
        "cluster",
        "prepare-worker",
        "--home",
        &l.s("worker"),
        "--config",
        &l.s("client/config.json"),
        "--id",
        "canary",
        "--cpu-slots",
        "1",
        "--memory-mb",
        "256",
    ])?;
    l.cli(&[
        "cluster",
        "prepare-storage",
        "--home",
        &l.s("storage"),
        "--config",
        &l.s("client/config.json"),
        "--id",
        "backup",
        "--failure-domain",
        "one-qualification-host",
        "--capacity-bytes",
        &(32u64 << 20).to_string(),
    ])?;
    for n in 1..=3 {
        let name = format!("node-{n}");
        let mut args = vec![
            "service".into(),
            "configure".into(),
            "--config".into(),
            l.s(&format!("{name}.json")),
            "--controller-home".into(),
            l.s(&format!("seed/{name}")),
        ];
        if n == 1 {
            args.extend([
                "--worker-home".into(),
                l.s("worker"),
                "--storage-home".into(),
                l.s("storage"),
                "--bootstrap".into(),
            ]);
        }
        checked(baseline, &args)?;
        l.start(&name, Some(baseline))?;
    }
    activate_to(&l, BASE)?;
    let initial_revision = l.state()?["version"]["revision"].as_u64().unwrap();
    l.binary = current;
    caught(&l, &[1, 2, 3])?;
    let denied = l.envelope(json!({"op":"metadata","query":{"kind":"stats"}}))?;
    ensure!(
        denied["ok"] == false
            && denied["outcome_unknown"] == false
            && denied.to_string().contains("activate generation 5"),
        "future variant reached an older leader: {denied}"
    );
    for n in 1..=3 {
        ensure!(
            inspect(&l, n)?["software"] == json!({"min":BASE-1,"max":BASE}),
            "baseline range changed"
        );
    }
    l.cli(&[
        "--home",
        &l.s("client"),
        "enroll",
        &l.s("worker/enroll.json"),
        "--request-id",
        "enroll",
    ])?;
    l.cli(&[
        "--home",
        &l.s("client"),
        "storage",
        "enroll",
        &l.s("storage/registration.json"),
        "--request-id",
        "enroll-store",
    ])?;
    l.request(json!({"op":"storage_policy","request_id":"policy","expected_revision":0,"policy":{"checkpoints":[],"outputs":["backup"],"inputs":[],"history":["backup"]}}))?;
    let submitted=l.request(json!({"op":"submit","request_id":"submit-continuity","id":"continuity","worker":"canary","memory_mb":128,"job":{"name":"isolated rolling upgrade canary","argv":["/bin/sh","-c","echo started-once; while [ ! -f \"$1\" ]; do sleep .1; done; echo finished-once; echo preserved > result.txt","canary",l.s("finish-canary")],"threads":1,"keep_awake":false,"max_seconds":900,"shutdown_grace_seconds":0,"settle_seconds":0,"publish":["result.txt"]}}))?;
    wait(
        || l.request(json!({"op":"job","id":"continuity"})),
        |j| j["phase"] == "running",
        60,
    )?;
    let supervisor = l
        .root
        .join("worker/backend/jobs/exec-continuity-a1/supervisor.json");
    wait(|| Ok(json!(supervisor.exists())), |v| v == true, 60)?;
    let original = fs::read(&supervisor)?;
    l.phase("baseline_three_replicas_with_combined_worker_and_storage");
    let answer = api(
        unsupported,
        &l.root.join("client"),
        json!({"op":"prepare_version","request_id":"future-write","expected_revision":initial_revision,"target":NEXT}),
    )?;
    ensure!(
        answer["ok"] == false && answer.to_string().contains("compatib"),
        "future request was accepted: {answer}"
    );
    ensure!(
        !l.state()?["receipts"]
            .as_object()
            .unwrap()
            .keys()
            .any(|k| k.ends_with("future-write\"]")),
        "incompatible receipt recorded"
    );
    rejected(
        &l,
        json!({"op":"prepare_version","request_id":"skip-generation","expected_revision":initial_revision,"target":NEXT+1}),
    )?;
    ensure!(
        l.state()?["version"]["active"] == BASE,
        "unexpected generation"
    );
    l.phase("unsupported_network_client_and_generation_jump_rejected");
    checked(
        unsupported,
        &[
            "cluster".into(),
            "init".into(),
            "--home".into(),
            l.s("future-seed"),
            "--replicas".into(),
            "1".into(),
        ],
    )?;
    checked(
        unsupported,
        &[
            "service".into(),
            "configure".into(),
            "--config".into(),
            l.s("node-4.json"),
            "--controller-home".into(),
            l.s("future-seed/node-1"),
            "--bootstrap".into(),
        ],
    )?;
    l.start("node-4", Some(unsupported))?;
    let future = checked(
        unsupported,
        &[
            "cluster".into(),
            "read".into(),
            "--key".into(),
            l.s("future-seed/client/node.key"),
            "--config".into(),
            l.s("future-seed/client/config.json"),
        ],
    )?;
    ensure!(
        future["version"]["active"] == NEXT + 1 && l.state()?["version"]["active"] == BASE,
        "fresh cluster changed existing generation"
    );
    l.stop("node-4", false)?;
    l.phase("fresh_future_cluster_uses_its_floor_without_upgrading_existing_state");
    let leader = inspect(&l, 1)?["leader"].as_u64().unwrap();
    let follower = if leader == 2 { 3 } else { 2 };
    let name = format!("node-{follower}");
    l.stop(&name, true)?;
    unchanged(&l, follower, unsupported)?;
    for n in 0..110 {
        l.request(json!({"op":"drain","request_id":format!("snapshot-{n}"),"worker":"canary","drain":n%2==1}))?;
    }
    l.request(json!({"op":"drain","request_id":"undrain","worker":"canary","drain":false}))?;
    wait(|| inspect(&l, leader), |v| !v["purged"].is_null(), 60)?;
    l.start(&name, None)?;
    caught(&l, &[1, 2, 3])?;
    ensure!(
        inspect(&l, follower)?["software"] == json!({"min":BASE,"max":NEXT})
            && !inspect(&l, follower)?["snapshot"].is_null(),
        "mixed-version snapshot missing"
    );
    live(&l, &original, &submitted["reply"])?;
    l.phase("dirty_database_rejection_and_mixed_version_snapshot_catchup");
    l.request(json!({"op":"prepare_version","request_id":"prepare-first","expected_revision":initial_revision,"target":NEXT}))?;
    wait(
        || Ok(l.state()?["version"].clone()),
        |v| {
            v["pending"]["ready"]
                .as_array()
                .is_some_and(|a| a.contains(&json!(follower)))
        },
        60,
    )?;
    rejected(
        &l,
        json!({"op":"activate_version","request_id":"early-activate","expected_revision":initial_revision+1}),
    )?;
    l.stop(&name, true)?;
    unchanged(&l, follower, baseline)?;
    l.request(json!({"op":"abort_version","request_id":"abort-first","expected_revision":initial_revision+1}))?;
    unchanged(&l, follower, baseline)?;
    l.start(&name, None)?;
    caught(&l, &[1, 2, 3])?;
    l.stop(&name, false)?;
    l.start(&name, Some(baseline))?;
    caught(&l, &[1, 2, 3])?;
    live(&l, &original, &submitted["reply"])?;
    l.phase("preparation_blocks_unsafe_rollback_and_abort_enables_safe_rollback");
    let leader = inspect(&l, 1)?["leader"].as_u64().unwrap();
    let order = (1..=3)
        .filter(|n| *n != leader)
        .chain([leader])
        .collect::<Vec<_>>();
    for n in order {
        let name = format!("node-{n}");
        l.stop(&name, false)?;
        l.start(&name, None)?;
        caught(&l, &[1, 2, 3])?;
        live(&l, &original, &submitted["reply"])?;
    }
    let revision = l.state()?["version"]["revision"].as_u64().unwrap();
    l.request(json!({"op":"prepare_version","request_id":"prepare-final","expected_revision":revision,"target":NEXT}))?;
    wait(
        || Ok(l.state()?["version"].clone()),
        |v| v["pending"]["ready"] == json!([1, 2, 3]),
        60,
    )?;
    let leader = inspect(&l, 1)?["leader"].as_u64().unwrap();
    let name = format!("node-{leader}");
    l.stop(&name, true)?;
    l.request(json!({"op":"activate_version","request_id":"activate-final","expected_revision":revision+1}))?;
    unchanged(&l, leader, baseline)?;
    l.start(&name, None)?;
    caught(&l, &[1, 2, 3])?;
    ensure!(
        l.state()?["version"]["active"] == NEXT,
        "generation not activated"
    );
    ensure!(
        l.request(json!({"op":"metadata","query":{"kind":"stats"}}))?["indexed"] == true,
        "metadata layout was not activated"
    );
    ensure!(l.request(json!({"op":"vm_manage","request_id":"activated-vm","action":{"kind":"sweep","as_of":0}}))?["reply"]=="Recorded","activated VM mutation failed");
    ensure!(
        api_result(
            baseline,
            &l.root.join("client"),
            json!({"op":"job","id":"continuity"})
        )?["phase"]
            == "running",
        "old client failed"
    );
    ensure!(
        l.request(json!({"op":"retry","request_id":"activate-final"}))?["reply"] == "Recorded",
        "activation receipt changed"
    );
    live(&l, &original, &submitted["reply"])?;
    l.phase("rolling_combined_services_leader_loss_and_idempotent_activation");
    for n in 1..=3 {
        l.stop(&format!("node-{n}"), true)?;
    }
    for n in 1..=3 {
        unchanged(&l, n, baseline)?;
        unchanged(&l, n, unsupported)?;
    }
    for n in 1..=3 {
        l.start(&format!("node-{n}"), None)?;
    }
    caught(&l, &[1, 2, 3])?;
    live(&l, &original, &submitted["reply"])?;
    l.phase("cold_quorum_restart_and_post_activation_downgrade_refusal");
    fs::write(l.root.join("finish-canary"), "")?;
    wait(
        || l.request(json!({"op":"job","id":"continuity"})),
        |j| j["phase"] == "succeeded",
        60,
    )?;
    let artifacts = wait(
        || l.request(json!({"op":"artifacts"})),
        |rows| {
            rows.as_array().unwrap().iter().any(|r| {
                r["artifact"]["manifest"]["kind"] == "FinalOutputs"
                    && r["artifact"]["copies"]["storage/backup"]["phase"] == "Ready"
            })
        },
        90,
    )?;
    let artifact = &artifacts
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["artifact"]["manifest"]["kind"] == "FinalOutputs")
        .unwrap()["artifact"];
    l.request(json!({"op":"fetch_artifact","id":artifact["manifest"]["id"],"destination":l.s("restored")}))?;
    ensure!(
        fs::read_to_string(l.root.join("restored/result.txt"))? == "preserved\n",
        "output changed"
    );
    let expected = "started-once\nfinished-once\n";
    wait(
        || l.request(json!({"op":"history","id":"continuity"})),
        |h| {
            h["attempts"][0]["streams"]
                .as_array()
                .unwrap()
                .iter()
                .any(|s| {
                    s["stream"] == "stdout"
                        && s["progress"]["closed"] == true
                        && s["backed_up_bytes"] == expected.len()
                })
        },
        90,
    )?;
    let mut offset = 0;
    let mut text = String::new();
    loop {
        let page = l.request(json!({"op":"logs","id":"continuity","from":offset}))?["Logs"].clone();
        ensure!(page["gap"].is_null(), "history gap {page}");
        let data = page["data"].as_str().unwrap();
        text.push_str(data);
        ensure!(expected.starts_with(&text), "log changed");
        if !data.is_empty() {
            ensure!(page["archived"] == true, "logs not archived");
        }
        if page["done"] == true {
            break;
        }
        let next = page["next"].as_u64().unwrap();
        ensure!(next > offset, "log cursor stalled");
        offset = next;
    }
    ensure!(
        text == expected
            && l.request(json!({"op":"attempts","id":"continuity"}))?
                .as_array()
                .unwrap()
                .len()
                == 1,
        "duplicate or incomplete execution"
    );
    l.phase("verified_output_backup_and_exactly_once_native_execution");
    l.report(json!({"success":true,"physical_failure_domain":"one host; process faults only","binary_sha256":{"baseline":digest(baseline)?,"candidate":digest(candidate)?,"unsupported":digest(unsupported)?}}))
}

fn copy_tree(from: &Path, to: &Path) -> Result<()> {
    fs::create_dir_all(to)?;
    for e in fs::read_dir(from)? {
        let e = e?;
        ensure!(
            !e.file_type()?.is_symlink(),
            "build source contains symlink"
        );
        if e.file_type()?.is_dir() {
            copy_tree(&e.path(), &to.join(e.file_name()))?;
        } else {
            fs::copy(e.path(), to.join(e.file_name()))?;
        }
    }
    Ok(())
}
pub fn build(output_dir: &Path) -> Result<()> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let output_dir = resolve(output_dir)?;
    fs::create_dir_all(&output_dir)?;
    let previous = read(&root.join("contrib/upgrade_baseline.json"))?;
    let scratch = std::env::temp_dir().join(format!(
        "fleet-control-upgrade-build-{:016x}",
        rand::random::<u64>()
    ));
    fs::create_dir(&scratch)?;
    let target = root.join("target");
    let build_one = |source: &Path, name: &str, all: bool| -> Result<()> {
        for args in [
            vec!["clean", "-p", "fleet", "--profile", "dev"],
            if all {
                vec!["build", "--all-features", "--bins", "--locked"]
            } else {
                vec!["build", "--all-features", "--bin", "fleet", "--locked"]
            },
        ] {
            let status = std::process::Command::new("cargo")
                .args(args)
                .current_dir(source)
                .env("CARGO_TARGET_DIR", &target)
                .status()?;
            ensure!(status.success(), "qualification binary build failed");
        }
        fs::copy(target.join("debug/fleet"), output_dir.join(name))?;
        Ok(())
    };
    let result = (|| -> Result<()> {
        let archive = scratch.join("baseline.tar");
        let status = std::process::Command::new("git")
            .args(["archive", "--format=tar", "--output"])
            .arg(&archive)
            .arg(previous["revision"].as_str().unwrap())
            .current_dir(&root)
            .status()?;
        ensure!(status.success(), "baseline source unavailable");
        let baseline = scratch.join("baseline");
        fs::create_dir(&baseline)?;
        let status = std::process::Command::new("tar")
            .arg("-xf")
            .arg(&archive)
            .arg("-C")
            .arg(&baseline)
            .status()?;
        ensure!(status.success(), "baseline extraction failed");
        build_one(&baseline, "fleet-baseline", false)?;
        let adapter_previous = read(&root.join("contrib/service_adapter_baseline.json"))?;
        let adapter_source = scratch.join("service-baseline");
        fs::create_dir(&adapter_source)?;
        let status = std::process::Command::new("git")
            .args(["archive", "--format=tar", "--output"])
            .arg(&archive)
            .arg(
                adapter_previous["revision"]
                    .as_str()
                    .context("service adapter revision required")?,
            )
            .current_dir(&root)
            .status()?;
        ensure!(status.success(), "service adapter baseline unavailable");
        let status = std::process::Command::new("tar")
            .arg("-xf")
            .arg(&archive)
            .arg("-C")
            .arg(&adapter_source)
            .status()?;
        ensure!(
            status.success(),
            "service adapter baseline extraction failed"
        );
        build_one(&adapter_source, "fleet-service-baseline", false)?;
        let future = scratch.join("unsupported");
        fs::create_dir(&future)?;
        for dir in ["src", "crates", "contrib/rust", "tests/fixtures"] {
            copy_tree(&root.join(dir), &future.join(dir))?;
        }
        for name in ["Cargo.toml", "Cargo.lock", "rust-toolchain.toml"] {
            fs::copy(root.join(name), future.join(name))?;
        }
        let file = future.join("src/control/version.rs");
        let source = fs::read_to_string(&file)?;
        ensure!(
            source.contains(&format!("pub const MIN: u32 = {BASE};"))
                && source.contains(&format!("pub const MAX: u32 = {NEXT};")),
            "update unsupported build fixture for new baseline"
        );
        fs::write(
            file,
            source
                .replace(
                    &format!("pub const MIN: u32 = {BASE};"),
                    &format!("pub const MIN: u32 = {};", NEXT + 1),
                )
                .replace(
                    &format!("pub const MAX: u32 = {NEXT};"),
                    &format!("pub const MAX: u32 = {};", NEXT + 2),
                ),
        )?;
        build_one(&future, "fleet-unsupported", false)?;
        build_one(&root, "fleet-candidate", true)?;
        for name in ["fleet-guest", "fleet-fake-tart", "fleet-test-workload"] {
            fs::copy(target.join("debug").join(name), output_dir.join(name))?;
        }
        let hashes = ["fleet-baseline", "fleet-candidate", "fleet-unsupported"]
            .map(|n| digest(&output_dir.join(n)))
            .into_iter()
            .collect::<Result<Vec<_>>>()?;
        ensure!(
            hashes[0] != hashes[1] && hashes[0] != hashes[2] && hashes[1] != hashes[2],
            "builds must be distinct"
        );
        let report = json!({"baseline":previous,"service_adapter_baseline":adapter_previous,"unsupported_is_test_fixture":true,"sha256":{"fleet-baseline":hashes[0],"fleet-candidate":hashes[1],"fleet-unsupported":hashes[2],"fleet-service-baseline":digest(&output_dir.join("fleet-service-baseline"))?}});
        save(&output_dir.join("builds.json"), &report)?;
        println!("{report}");
        Ok(())
    })();
    // Always leave the candidate build intact on success; retain failed build evidence otherwise.
    if result.is_ok() {
        fs::remove_dir_all(scratch)?;
    }
    result
}
