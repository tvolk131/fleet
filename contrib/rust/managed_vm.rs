//! Public client qualification for worker-owned VM networking and durable operations.
use super::{support::*, tart};
use anyhow::{Result, ensure};
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    fs,
    net::{TcpListener, TcpStream},
    time::Duration,
};
fn port() -> Result<String> {
    Ok(TcpListener::bind("127.0.0.1:0")?.local_addr()?.to_string())
}
fn observed(l: &Lab, id: &str, wanted: &str) -> Result<()> {
    wait(
        || l.request(json!({"op":"instances"})),
        |v| {
            v.as_array().is_some_and(|rows| {
                rows.iter().any(|a| {
                    a["id"] == id
                        && a["observed"][0] == a["revision"]
                        && a["observed"][1]["phase"]["Instance"] == wanted
                })
            })
        },
        180,
    )?;
    Ok(())
}
fn op(l: &Lab, id: &str) -> Result<Value> {
    Ok(l.request(json!({"op":"vm"}))?["catalog"]["operations"][id].clone())
}
fn finished(l: &Lab, id: &str) -> Result<Value> {
    let v = wait(
        || op(l, id),
        |v| {
            matches!(
                v["outcome"]["phase"].as_str(),
                Some("succeeded" | "failed" | "interrupted")
            )
        },
        180,
    )?;
    ensure!(
        v["outcome"]["phase"] == "succeeded",
        "operation {id} failed: {v}"
    );
    Ok(v)
}
fn exec(l: &Lab, vm: &str, id: &str, argv: Value, complete: bool) -> Result<Value> {
    let revision = l.state()?["assignments"][vm]["revision"].clone();
    let reply=l.request(json!({"op":"vm_manage","request_id":format!("request-{id}"),"action":{"kind":"start","id":id,"instance":vm,"expected_revision":revision,"spec":{"kind":"exec","argv":argv,"cwd":null,"env":{},"timeout_seconds":600},"retention_seconds":604800}}))?;
    ensure!(
        reply["reply"] == "Recorded" && reply["operation"] == id,
        "command acceptance: {reply}"
    );
    if complete { finished(l, id) } else { Ok(reply) }
}
fn connections(l: &Lab, vm: &str, revision: u64, exports: Value, routes: Value) -> Result<()> {
    let v=l.request(json!({"op":"vm_manage","request_id":format!("connections-{vm}-{revision}"),"action":{"kind":"connections","instance":vm,"expected_revision":revision,"config":{"exports":exports,"routes":routes}}}))?;
    ensure!(v["reply"] == "Recorded", "connection update failed: {v}");
    Ok(())
}
fn enforced(l: &Lab) -> Result<()> {
    wait(
        || l.request(json!({"op":"vm"})),
        |v| {
            v["catalog"]["bindings"]
                .as_object()
                .is_some_and(|m| m.len() == 2)
        },
        90,
    )?;
    let revision = l.state()?["tunnels"]["revision"].clone();
    let v = l.request(json!({"op":"wait_tunnels","revision":revision,"timeout_seconds":90}))?;
    ensure!(
        v["confirmed"] == true,
        "network enforcement unconfirmed: {v}"
    );
    Ok(())
}
fn recover_pending_launch(l: &mut Lab, worker: &str) -> Result<()> {
    // Confined fault injection: persist the backend request but replace its launcher
    // with a successful no-op, reproducing a crash before supervisor execution.
    l.stop(worker, false)?;
    let config = l.root.join(worker).join("worker.json");
    let original = read(&config)?;
    let mut broken = original.clone();
    broken["supervisor"] = json!("/usr/bin/true");
    save(&config, &broken)?;
    l.start(worker, None)?;
    let marker = l.s("guest-a/pending-once");
    let staged = (|| -> Result<_> {
        exec(
            l,
            "a",
            "pending-launch",
            json!(["/bin/sh", "-c", format!("printf 'once\\n' >> '{marker}'")]),
            false,
        )?;
        let operation: fleet::control::vm::Operation =
            serde_json::from_value(op(l, "pending-launch")?)?;
        let dir = fleet::instances::guest::operation_dir(
            &l.root.join(worker).join("backend"),
            &operation.execution,
            &operation.backend_id(),
        )?;
        wait(
            || {
                let path = dir.join("operation.json");
                if path.exists() {
                    read(&path)
                } else {
                    Ok(Value::Null)
                }
            },
            |v| v["state"] == "pending",
            60,
        )?;
        Ok(dir)
    })();
    l.stop(worker, false)?;
    let staged = staged.and_then(|dir| {
        ensure!(
            !dir.join("attempted").exists(),
            "fault ran the guest command"
        );
        Ok(())
    });
    // Always restore the launcher so fixture cleanup can still delete its VMs.
    save(&config, &original)?;
    l.start(worker, None)?;
    staged?;
    finished(l, "pending-launch")?;
    l.request(json!({"op":"retry","request_id":"request-pending-launch"}))?;
    l.cli(&[
        "--home",
        &l.s("client"),
        "instance",
        "download",
        "a",
        &marker,
        "--operation",
        "pending-marker",
    ])?;
    finished(l, "pending-marker")?;
    l.request(json!({"op":"vm_fetch","id":"pending-marker","destination":l.s("pending-capture")}))?;
    ensure!(
        fs::read(l.root.join("pending-capture/download"))? == b"once\n",
        "pending launch was lost or replayed"
    );
    l.phase("pending_backend_launch_recovers_after_worker_restart_without_replay");
    Ok(())
}
pub fn run(a: tart::Options) -> Result<()> {
    tart::real_guard(&a)?;
    let mut l = Lab::new(&a.home, &a.binary)?;
    l.init(1)?;
    l.cli(&[
        "service",
        "configure",
        "--config",
        &l.s("controller.json"),
        "--controller-home",
        &l.s("seed/node-1"),
        "--bootstrap",
    ])?;
    l.start("controller", None)?;
    wait(
        || l.request(json!({"op":"versions"})),
        |v| v["version"]["active"] == fleet::control::version::MIN,
        30,
    )?;
    super::rolling::activate_to(&l, fleet::control::vm::GENERATION)?;
    let mut settings = tart::settings(&l, &a)?;
    settings["memory_budget_mb"] = json!(if a.fake { 4096 } else { 8192 });
    let workers = if a.fake {
        ["worker-a", "worker-b"]
    } else {
        ["worker", "worker"]
    };
    for worker in workers.iter().copied().collect::<BTreeSet<_>>() {
        tart::worker(&l, &a, worker, &settings)?;
        tart::enroll(&l, worker, false)?;
        l.start(worker, None)?;
    }
    wait(
        || l.request(json!({"op":"vm"})),
        |v| {
            v["catalog"]["capable"]
                .as_array()
                .is_some_and(|v| v.len() == if a.fake { 2 } else { 1 })
        },
        60,
    )?;
    save(
        &l.root.join("vm.json"),
        &json!({"image":"macos","cpus":if a.fake{1}else{2},"memory_mb":4096,"disk_gb":50,"network":"offline","gpu":"shared"}),
    )?;
    for (vm, worker) in ["a", "b"].into_iter().zip(workers) {
        l.instances.push(vm.into());
        l.cli(&[
            "--home",
            &l.s("client"),
            "instance",
            "create",
            vm,
            "--worker",
            worker,
            "--spec",
            &l.s("vm.json"),
            "--request-id",
            &format!("create-{vm}"),
        ])?;
        observed(&l, vm, "Running")?;
        let upload = format!("upload-{vm}");
        let reply=l.request(json!({"op":"vm_upload","request_id":format!("request-{upload}"),"id":upload,"instance":vm,"expected_revision":1,"source":l.s("fleet-test-workload"),"path":l.s(&format!("guest-{vm}/workload")),"executable":true}))?;
        ensure!(
            reply["staging"]["staged"] == true,
            "upload not staged: {reply}"
        );
        finished(&l, &upload)?;
    }
    l.phase("public_uploads_are_staged_and_execute_without_host_guest_mounts");
    let data = l.root.join("input.bin");
    fs::write(&data, vec![0x85; 1024 * 1024])?;
    let upload = json!({"op":"vm_upload","request_id":"upload-data-request","id":"upload-data","instance":"a","expected_revision":1,"source":data,"path":l.s("guest-a/data.bin")});
    let accepted = l.request(upload)?;
    ensure!(
        accepted["staging"]["staged"] == true,
        "data staging: {accepted}"
    );
    fs::write(&data, b"changed client source")?;
    let retry = l.request(json!({"op":"retry","request_id":"upload-data-request"}))?;
    ensure!(retry["reply"] == accepted["reply"], "retry differs");
    finished(&l, "upload-data")?;
    l.cli(&[
        "--home",
        &l.s("client"),
        "instance",
        "download",
        "a",
        &l.s("guest-a/data.bin"),
        "--operation",
        "download-data",
        "--request-id",
        "capture-data",
    ])?;
    finished(&l, "download-data")?;
    l.request(
        json!({"op":"vm_fetch","id":"download-data","destination":l.s("capture-before-delete")}),
    )?;
    ensure!(
        fs::read(l.root.join("capture-before-delete/download"))? == vec![0x85; 1024 * 1024],
        "download changed or retry used new source bytes"
    );
    l.phase("upload_retry_uses_saved_bytes_and_download_result_is_verified");
    recover_pending_launch(&mut l, workers[0])?;
    let marker = l.s("guest-a/once");
    exec(
        &l,
        "a",
        "once",
        json!([
            "/bin/sh",
            "-c",
            format!("printf 'once\\n' >> '{marker}'; sleep 5; printf 'finished\\n'")
        ]),
        false,
    )?;
    wait(
        || op(&l, "once"),
        |o| o["outcome"]["phase"] == "running",
        60,
    )?;
    l.stop("controller", false)?;
    l.stop(workers[0], false)?;
    if let Some(baseline) = &a.baseline_binary {
        let database = l.root.join(format!("{}/worker.redb", workers[0]));
        let before = digest(&database)?;
        let old = output(
            baseline,
            &[
                "service".into(),
                "run".into(),
                "--config".into(),
                l.s(&format!("{}.json", workers[0])),
            ],
            None,
            15,
        )?;
        ensure!(
            !old.status.success()
                && String::from_utf8_lossy(&old.stderr).contains("worker metadata"),
            "previous binary accepted generation-four worker journal"
        );
        ensure!(
            digest(&database)? == before,
            "old binary mutated the worker journal"
        );
        l.phase("previous_binary_refuses_worker_journal_without_mutation");
    }
    l.start(workers[0], None)?;
    std::thread::sleep(Duration::from_secs(6));
    l.start("controller", None)?;
    finished(&l, "once")?;
    l.request(json!({"op":"retry","request_id":"request-once"}))?;
    l.cli(&[
        "--home",
        &l.s("client"),
        "instance",
        "download",
        "a",
        &marker,
        "--operation",
        "once-marker",
    ])?;
    finished(&l, "once-marker")?;
    l.request(json!({"op":"vm_fetch","id":"once-marker","destination":l.s("once-capture")}))?;
    ensure!(
        fs::read(l.root.join("once-capture/download"))? == b"once\n",
        "command replayed after worker/controller restart"
    );
    l.phase("controller_outage_worker_restart_and_client_retry_do_not_reexecute_commands");
    let target = port()?;
    let listen = port()?;
    exec(
        &l,
        "b",
        "echo",
        json!([l.s("guest-b/workload"), "service-echo", target]),
        false,
    )?;
    wait(
        || l.request(json!({"op":"vm_logs","id":"echo"})),
        |v| {
            v["data"]
                .as_str()
                .is_some_and(|s| s.contains("service-ready"))
        },
        60,
    )?;
    connections(&l, "b", 0, json!({"echo":target}), json!([]))?;
    connections(
        &l,
        "a",
        0,
        json!({}),
        json!([{"service":{"kind":"vm","instance":"b","service":"echo"},"listen":listen}]),
    )?;
    enforced(&l)?;
    let keys = l.request(json!({"op":"vm"}))?["catalog"]["bindings"].clone();
    exec(
        &l,
        "a",
        "roundtrip",
        json!([l.s("guest-a/workload"), "service-roundtrip", listen]),
        true,
    )?;
    if !a.fake {
        ensure!(
            TcpStream::connect_timeout(&listen.parse()?, Duration::from_secs(1)).is_err(),
            "guest listener leaked onto host"
        );
    }
    l.phase("worker_creates_private_identities_and_enforces_named_vm_connections");
    let old_reports = l.request(json!({"op":"vm"}))?["enforcement"]["adapters"].clone();
    let restarted: BTreeSet<_> = ["a", "b"]
        .into_iter()
        .zip(workers)
        .filter(|(_, worker)| *worker == workers[0])
        .map(|(id, _)| fleet::control::vm::adapter_id(id))
        .collect();
    l.stop(workers[0], false)?;
    l.start(workers[0], None)?;
    // A recently stopped process's last report can remain fresh for its bounded
    // freshness window. This test knows it restarted the worker: require each
    // affected adapter's new boot before waiting for applied policy.
    wait(
        || l.request(json!({"op":"vm"})),
        |v| {
            restarted.iter().all(|id| {
                let old = old_reports
                    .as_array()
                    .and_then(|rows| rows.iter().find(|r| r["adapter"] == *id))
                    .and_then(|r| r["observation"]["report"]["boot"].as_u64())
                    .unwrap_or(0);
                v["enforcement"]["adapters"].as_array().is_some_and(|rows| {
                    rows.iter().any(|r| {
                        r["adapter"] == *id
                            && r["observation"]["report"]["boot"]
                                .as_u64()
                                .is_some_and(|boot| boot > old)
                    })
                })
            })
        },
        90,
    )?;
    enforced(&l)?;
    ensure!(
        l.request(json!({"op":"vm"}))?["catalog"]["bindings"] == keys,
        "worker restart rotated VM identity"
    );
    exec(
        &l,
        "a",
        "after-restart",
        json!([l.s("guest-a/workload"), "service-roundtrip", listen]),
        true,
    )?;
    connections(&l, "a", 1, json!({}), json!([]))?;
    enforced(&l)?;
    exec(
        &l,
        "a",
        "denied",
        json!([l.s("guest-a/workload"), "service-denied", listen]),
        true,
    )?;
    l.phase("identity_survives_worker_restart_and_permission_removal_is_confirmed");
    for vm in ["a", "b"] {
        l.cli(&[
            "--home",
            &l.s("client"),
            "instance",
            "delete",
            vm,
            "--request-id",
            &format!("delete-{vm}"),
        ])?;
        observed(&l, vm, "Deleted")?;
    }
    enforced(&l)?;
    let denied=l.envelope(json!({"op":"vm_manage","request_id":"late-exec","action":{"kind":"start","id":"late","instance":"a","expected_revision":1,"spec":{"kind":"exec","argv":["/bin/echo","late"],"cwd":null,"env":{},"timeout_seconds":30}}}))?;
    ensure!(
        denied["result"]["reply"].get("Rejected").is_some(),
        "deleted VM accepted command"
    );
    l.request(
        json!({"op":"vm_fetch","id":"download-data","destination":l.s("capture-after-delete")}),
    )?;
    ensure!(
        fs::read(l.root.join("capture-after-delete/download"))? == vec![0x85; 1024 * 1024],
        "result lost with VM disk"
    );
    l.phase("deletion_revokes_services_and_preserves_retained_operation_results");
    let revision = op(&l, "download-data")?["revision"].clone();
    l.request(json!({"op":"vm_manage","request_id":"release-download","action":{"kind":"release","id":"download-data","expected_revision":revision}}))?;
    wait(|| op(&l, "download-data"), |o| o["reclaimed"] == true, 60)?;
    let fetch = l.envelope(
        json!({"op":"vm_fetch","id":"download-data","destination":l.s("expired-fetch")}),
    )?;
    ensure!(
        fetch["ok"] == false && !l.root.join("expired-fetch").exists(),
        "released result still available"
    );
    l.phase("explicit_retention_release_waits_for_worker_reclamation");
    l.report(json!({"success":true,"real_tart":!a.fake,"physical_failure_domain":"one host; independent workers simulated when fake","public_client_interfaces":true,"fault_injection":"private worker launcher exits before guest execution","binary_sha256":digest(&l.binary)?}))?;
    for name in workers.iter().copied().collect::<BTreeSet<_>>() {
        l.stop(name, false)?;
    }
    l.stop("controller", false)?;

    Ok(())
}
