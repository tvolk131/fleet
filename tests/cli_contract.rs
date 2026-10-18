//! Public JSON CLI contracts, ported from the former Python black-box tests.
#[allow(dead_code)]
#[path = "../contrib/rust/host.rs"]
mod host;
#[allow(dead_code)]
#[path = "../contrib/rust/support.rs"]
mod support;
use anyhow::{Result, ensure};
use serde_json::{Value, json};
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
};
use support::*;

#[test]
fn simulated_guest_exits_when_its_vm_stops_or_reboots() -> Result<()> {
    let root = PathBuf::from(format!(
        "/tmp/fleet-control-vm-lifetime-{:016x}",
        rand::random::<u64>()
    ));
    fs::create_dir(&root)?;
    for reboot in [false, true] {
        let pid = root.join("pid");
        fs::write(&pid, "first-launcher")?;
        let child = std::process::Command::new(env!("CARGO_BIN_EXE_fleet-test-workload"))
            .args(["sleep", "60"])
            .env("FLEET_TEST_VM_LIFETIME", &pid)
            .spawn()?;
        let ready = root.join(format!("guest-{}.ready", child.id()));
        let mut child = Process { child };
        wait(|| Ok(json!(ready.exists())), |r| r == true, 5)?;
        if reboot {
            fs::write(&pid, "next-launcher")?;
        } else {
            fs::remove_file(&pid)?;
        }
        let status = wait(
            || Ok(json!(child.child.try_wait()?.map(|s| s.success()))),
            |r| !r.is_null(),
            5,
        )?;
        ensure!(status == true, "simulated guest did not exit with its VM");
    }
    fs::remove_dir_all(root)?;
    Ok(())
}

fn lab() -> Result<Lab> {
    let root = PathBuf::from(format!(
        "/tmp/fleet-control-contract-{:016x}",
        rand::random::<u64>()
    ));
    let mut l = Lab::new(&root, Path::new(env!("CARGO_BIN_EXE_fleet")))?;
    l.init(1)?;
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
        "1024",
    ])?;
    for (role, home) in [
        ("controller", "seed/node-1"),
        ("worker", "worker"),
        ("stager", "client"),
    ] {
        let flag = match role {
            "controller" => "--controller-home",
            "worker" => "--worker-home",
            _ => "--client-home",
        };
        let cfg = l.s(&format!("{role}.json"));
        let h = l.s(home);
        let mut args = vec!["service", "configure", "--config", &cfg, flag, &h];
        if role == "controller" {
            args.push("--bootstrap");
        }
        l.cli(&args)?;
    }
    l.start("controller", None)?;
    l.cli(&[
        "--home",
        &l.s("client"),
        "enroll",
        &l.s("worker/enroll.json"),
        "--request-id",
        "enroll-canary",
    ])?;
    l.start("worker", None)?;
    Ok(l)
}
fn phase(l: &Lab, id: &str, phase: &str) -> Result<Value> {
    wait(
        || l.request(json!({"op":"job","id":id})),
        |j| j["phase"] == phase,
        60,
    )
}
fn submit(l: &Lab, id: &str, job: Value, extras: Value) -> Result<Value> {
    let mut request = json!({"op":"submit","id":id,"request_id":format!("{id}-submit"),"memory_mb":128,"job":job});
    request
        .as_object_mut()
        .unwrap()
        .extend(extras.as_object().unwrap().clone());
    l.request(request)
}
fn cancel(l: &Lab, id: &str) -> Result<()> {
    let j = l.request(json!({"op":"job","id":id}))?;
    l.request(json!({"op":"cancel","id":id,"request_id":format!("{id}-cancel"),"expected_revision":j["revision"]}))?;
    phase(l, id, "cancelled")?;
    Ok(())
}

#[test]
fn tunnel_cli_distinguishes_committed_policy_from_confirmed_enforcement() -> Result<()> {
    let mut l = lab()?;
    // Status remains readable before generation-three activation.
    ensure!(
        l.request(json!({"op":"tunnels"}))?["revision"] == 0,
        "initial catalog"
    );
    l.request(json!({"op":"prepare_version","request_id":"tunnel-prepare","expected_revision":0,"target":fleet::control::tunnels::GENERATION}))?;
    wait(
        || l.request(json!({"op":"versions"})),
        |v| v["version"]["pending"]["ready"] == json!([1]),
        10,
    )?;
    l.request(
        json!({"op":"activate_version","request_id":"tunnel-activate","expected_revision":1}),
    )?;
    let port = std::net::UdpSocket::bind("127.0.0.1:0")?;
    save(
        &l.root.join("network.json"),
        &json!({"bind":port.local_addr()?.to_string(),"relays":[]}),
    )?;
    drop(port);
    let adapter = l.cli(&[
        "tunnels",
        "prepare",
        "--adapter-home",
        &l.s("adapter"),
        "--adapter",
        "a",
        "--authority",
        &l.s("client/config.json"),
        "--network",
        &l.s("network.json"),
    ])?;
    save(
        &l.root.join("topology.json"),
        &json!({"adapters":{"a":adapter},"services":{},"routes":[]}),
    )?;
    l.cli(&[
        "--home",
        &l.s("client"),
        "tunnels",
        "apply",
        &l.s("topology.json"),
        "--revision",
        "0",
        "--request-id",
        "routes",
    ])?;
    let unknown = l.cli(&["--home", &l.s("client"), "tunnels", "status"])?;
    ensure!(
        unknown["revision"] == 1 && unknown["adapters"][0]["state"] == "unknown",
        "commit was confused with enforcement: {unknown}"
    );
    let unconfirmed = l.cli(&[
        "--home",
        &l.s("client"),
        "tunnels",
        "wait",
        "--revision",
        "1",
        "--timeout-seconds",
        "1",
    ])?;
    ensure!(
        unconfirmed["confirmed"] == false && unconfirmed["outcome"] == "unconfirmed",
        "missing explicit timeout: {unconfirmed}"
    );
    l.cli(&[
        "service",
        "configure",
        "--config",
        &l.s("adapter.json"),
        "--tunnel-home",
        &l.s("adapter"),
    ])?;
    l.start("adapter", None)?;
    let confirmed = l.request(json!({"op":"wait_tunnels","revision":1,"timeout_seconds":10}))?;
    ensure!(
        confirmed["confirmed"] == true && confirmed["scope"] == "all_known_identities",
        "missing confirmation: {confirmed}"
    );
    let identity = unknown["adapters"][0]["identity"].as_str().unwrap();
    let selected = l.cli(&[
        "--home",
        &l.s("client"),
        "tunnels",
        "wait",
        "--revision",
        "1",
        "--timeout-seconds",
        "2",
        "--identity",
        identity,
    ])?;
    ensure!(
        selected["confirmed"] == true && selected["scope"] == "selected_identities",
        "missing selected scope: {selected}"
    );
    l.stop("adapter", false)?;
    l.stop("controller", false)?;
    l.start("controller", None)?;
    let restarted = l.request(json!({"op":"tunnels"}))?;
    ensure!(
        restarted["adapters"][0]["state"] == "unknown"
            && restarted["adapters"][0]["observation"]["last_applied"]["revision"] == 1,
        "restart resurrected freshness: {restarted}"
    );
    Ok(())
}

#[test]
fn storage_reports_are_timestamped_and_offline_is_explicit() -> Result<()> {
    let mut l = lab()?;
    let s = wait(
        || l.request(json!({"op":"storage"})),
        |s| {
            s["workers"]
                .as_array()
                .unwrap()
                .iter()
                .any(|w| w["worker"] == "canary" && !w["report"].is_null())
        },
        60,
    )?;
    let r = &s["workers"][0]["report"];
    ensure!(
        r["observed_at"].as_u64().unwrap() > 0
            && r.get("blob_payload_bytes").is_some()
            && s.get("retention").is_some(),
        "incomplete storage report"
    );
    l.stop("worker", false)?;
    let row = l.request(json!({"op":"storage"}))?["workers"][0].clone();
    ensure!(
        row["report"].is_null() && !row["error"].is_null(),
        "offline report missing"
    );
    Ok(())
}
#[test]
fn queue_inputs_logs_outputs_and_durable_retry() -> Result<()> {
    let l = lab()?;
    fs::write(l.root.join("input.txt"), "original input\n")?;
    submit(
        &l,
        "file",
        json!({"name":"file","argv":["/bin/sh","-c","echo started-once; cp input.txt result.txt"],"inputs":{"input.txt":l.s("input.txt")},"publish":["result.txt"],"keep_awake":false,"max_seconds":20,"settle_seconds":0}),
        json!({}),
    )?;
    phase(&l, "file", "succeeded")?;
    fs::write(l.root.join("input.txt"), "changed source\n")?;
    ensure!(
        l.request(json!({"op":"retry","request_id":"file-submit"}))?["reply"]["Accepted"]["job"]
            == "file",
        "retry changed"
    );
    ensure!(
        l.request(json!({"op":"logs","id":"file"}))?["Logs"]["data"] == "started-once\n",
        "logs changed"
    );
    let outputs = l.request(json!({"op":"outputs","id":"file"}))?;
    ensure!(
        outputs["Outputs"].as_array().unwrap().len() == 1
            && outputs["Outputs"][0]["path"] == "result.txt",
        "outputs changed"
    );
    l.request(json!({"op":"fetch","id":"file","destination":l.s("capture")}))?;
    ensure!(
        fs::read_to_string(l.root.join("capture/result.txt"))? == "original input\n",
        "input not saved"
    );
    ensure!(
        l.request(json!({"op":"queue"}))?
            .as_array()
            .unwrap()
            .iter()
            .filter(|j| j["id"] == "file")
            .count()
            == 1,
        "duplicate job"
    );
    ensure!(
        l.request(json!({"op":"requests"}))?
            .as_array()
            .unwrap()
            .iter()
            .any(|r| r["id"] == "file-submit"),
        "receipt absent"
    );
    fs::write(l.root.join("capture/result.txt"), "user edit")?;
    ensure!(
        l.envelope(json!({"op":"fetch","id":"file","destination":l.s("capture")}))?["ok"] == false,
        "overwrote export"
    );
    ensure!(
        fs::read_to_string(l.root.join("capture/result.txt"))? == "user edit",
        "export changed"
    );
    ensure!(l.envelope(json!({"op":"submit","id":"file","request_id":"file-submit","memory_mb":128,"job":{"name":"different","argv":["false"]}}))?["ok"]==false,"accepted conflict");
    ensure!(
        l.request(json!({"op":"attempts","id":"file"}))?
            .as_array()
            .unwrap()
            .len()
            == 1,
        "duplicate attempt"
    );
    Ok(())
}
#[test]
fn executable_inputs_and_log_cursor_survive_service_restarts() -> Result<()> {
    let mut l = lab()?;
    fs::write(
        l.root.join("canary.sh"),
        "#!/bin/sh\necho before-restart\nwhile [ ! -f \"$1\" ]; do sleep .1; done\necho after-restart\n",
    )?;
    fs::set_permissions(l.root.join("canary.sh"), fs::Permissions::from_mode(0o700))?;
    submit(
        &l,
        "restart",
        json!({"name":"canary","argv":["./bin/canary",l.s("gate")],"inputs":{"bin/canary":l.s("canary.sh")},"keep_awake":false,"max_seconds":60}),
        json!({}),
    )?;
    phase(&l, "restart", "running")?;
    let first = wait(
        || Ok(l.request(json!({"op":"logs","id":"restart"}))?["Logs"].clone()),
        |v| v["data"] == "before-restart\n",
        60,
    )?;
    l.stop("controller", false)?;
    l.stop("worker", false)?;
    l.start("controller", None)?;
    l.start("worker", None)?;
    phase(&l, "restart", "running")?;
    fs::write(l.root.join("gate"), "go")?;
    phase(&l, "restart", "succeeded")?;
    let tail = l.request(json!({"op":"logs","id":"restart","from":first["next"]}))?["Logs"].clone();
    ensure!(
        tail["data"] == "after-restart\n" && tail["done"] == true,
        "cursor lost: {tail}"
    );
    let mut offset = 0;
    let mut text = String::new();
    loop {
        let p = l.request(json!({"op":"logs","id":"restart","from":offset}))?["Logs"].clone();
        ensure!(p["gap"].is_null(), "log gap");
        text.push_str(p["data"].as_str().unwrap());
        if p["done"] == true {
            break;
        }
        let next = p["next"].as_u64().unwrap();
        ensure!(next > offset, "cursor stalled");
        offset = next;
    }
    ensure!(
        text == "before-restart\nafter-restart\n"
            && l.request(json!({"op":"attempts","id":"restart"}))?
                .as_array()
                .unwrap()
                .len()
                == 1,
        "duplicate/lost execution"
    );
    Ok(())
}
#[test]
fn dependency_and_cancel_before_placement() -> Result<()> {
    let l = lab()?;
    submit(
        &l,
        "first",
        json!({"name":"first","argv":["/bin/sh","-c","sleep 1"],"keep_awake":false,"max_seconds":15}),
        json!({}),
    )?;
    submit(
        &l,
        "second",
        json!({"name":"second","argv":["/bin/sh","-c","echo dependent"],"keep_awake":false,"max_seconds":15}),
        json!({"after":["first"]}),
    )?;
    phase(&l, "second", "succeeded")?;
    phase(&l, "first", "succeeded")?;
    submit(
        &l,
        "blocked",
        json!({"name":"blocked","argv":["true"],"keep_awake":false,"max_seconds":10}),
        json!({"requires":["qualification:no-such-capability"]}),
    )?;
    phase(&l, "blocked", "queued")?;
    cancel(&l, "blocked")?;
    ensure!(
        l.request(json!({"op":"job","id":"blocked"}))?["assignment"].is_null(),
        "cancelled job assigned"
    );
    Ok(())
}
#[test]
fn request_conflict_is_not_a_second_job() -> Result<()> {
    let l = lab()?;
    submit(
        &l,
        "conflict",
        json!({"name":"one","argv":["true"]}),
        json!({"requires":["never"]}),
    )?;
    let a=l.envelope(json!({"op":"submit","id":"conflict","request_id":"conflict-submit","requires":["never"],"memory_mb":128,"job":{"name":"different","argv":["false"]}}))?;
    ensure!(
        a["ok"] == false && a["outcome_unknown"] == false && a["request_id"] == "conflict-submit",
        "wrong conflict: {a}"
    );
    cancel(&l, "conflict")
}
#[test]
fn unknown_request_survives_controller_restart() -> Result<()> {
    let mut l = lab()?;
    l.stop("controller", false)?;
    let a=l.envelope(json!({"op":"submit","id":"outage","request_id":"outage-submit","requires":["never"],"memory_mb":128,"job":{"name":"outage","argv":["true"]}}))?;
    ensure!(
        a["ok"] == false && a["outcome_unknown"] == true && a["request_id"] == "outage-submit",
        "wrong unknown outcome: {a}"
    );
    l.start("controller", None)?;
    let r = l.request(json!({"op":"retry","request_id":"outage-submit"}))?;
    ensure!(
        r["reply"]["Accepted"]["job"] == "outage",
        "retry lost request"
    );
    phase(&l, "outage", "queued")?;
    cancel(&l, "outage")
}
#[test]
fn committed_rejection_is_a_reply_and_retry_preserves_it() -> Result<()> {
    let l = lab()?;
    let a=l.envelope(json!({"op":"submit","id":"rejected","request_id":"rejected-submit","after":["missing"],"memory_mb":128,"job":{"name":"rejected","argv":["true"]}}))?;
    ensure!(
        a["ok"] == true && a["result"]["reply"].get("Rejected").is_some(),
        "missing rejection"
    );
    ensure!(
        l.envelope(json!({"op":"retry","request_id":"rejected-submit"}))? == a,
        "retry changed rejection"
    );
    ensure!(
        !l.request(json!({"op":"queue"}))?
            .as_array()
            .unwrap()
            .iter()
            .any(|j| j["id"] == "rejected"),
        "rejected job queued"
    );
    Ok(())
}
#[test]
fn staging_service_delivers_saved_inputs_when_worker_returns() -> Result<()> {
    let mut l = lab()?;
    l.stop("worker", false)?;
    fs::write(l.root.join("input"), "saved while offline\n")?;
    submit(
        &l,
        "offline",
        json!({"name":"offline","argv":["/bin/sh","-c","cp input result"],"inputs":{"input":l.s("input")},"publish":["result"],"keep_awake":false,"max_seconds":20,"settle_seconds":0}),
        json!({}),
    )?;
    phase(&l, "offline", "queued")?;
    fs::remove_file(l.root.join("input"))?;
    l.start("worker", None)?;
    l.start("stager", None)?;
    phase(&l, "offline", "succeeded")?;
    l.stop("stager", false)?;
    l.request(json!({"op":"fetch","id":"offline","destination":l.s("capture")}))?;
    ensure!(
        fs::read_to_string(l.root.join("capture/result"))? == "saved while offline\n",
        "offline input lost"
    );
    Ok(())
}
#[test]
fn service_configuration_rendering_and_client_identity() -> Result<()> {
    let l = lab()?;
    l.cli(&[
        "service",
        "render",
        "--config",
        &l.s("controller.json"),
        "--kind",
        "launchd",
        "--name",
        "local.fleet.qualification",
        "--user",
        "testuser",
        "--user-home",
        &l.s("user"),
        "--output",
        &l.s("service.plist"),
    ])?;
    let text = fs::read_to_string(l.root.join("service.plist"))?;
    ensure!(
        text.contains("<key>AbandonProcessGroup</key>")
            && text.contains("<true/>")
            && text.contains("<string>service</string>")
            && text.contains("<string>run</string>"),
        "wrong launchd rendering"
    );
    ensure!(
        l.request(json!({"op":"status"}))?["quorum"] == true,
        "quorum missing"
    );
    ensure!(
        fs::metadata(l.root.join("client/node.key"))?
            .permissions()
            .mode()
            & 0o777
            == 0o600,
        "identity permissions"
    );
    Ok(())
}

fn fixture() -> Result<PathBuf> {
    let root = PathBuf::from(format!(
        "/tmp/fleet-control-helper-{:016x}",
        rand::random::<u64>()
    ));
    host::handle(&json!({"root":root,"op":"create"}))?;
    Ok(root.canonicalize()?)
}
#[test]
fn fixture_paths_and_unrelated_pid_are_refused() -> Result<()> {
    let root = fixture()?;
    for p in ["../escaped", "/tmp/escaped"] {
        ensure!(
            host::handle(&json!({"root":root,"op":"write","path":p,"data":[120]})).is_err(),
            "path escaped"
        );
    }
    save(
        &root.join("processes/foreign.json"),
        &json!({"pid":std::process::id(),"identity":"not this process"}),
    )?;
    ensure!(
        host::handle(&json!({"root":root,"op":"stop","name":"foreign"})).is_err(),
        "foreign PID accepted"
    );
    ensure!(
        host::handle(&json!({"root":root,"op":"create"})).is_err(),
        "existing fixture accepted"
    );
    fs::remove_dir_all(root)?;
    Ok(())
}
#[test]
fn fixture_service_role_outside_fixture_is_refused() -> Result<()> {
    let root = fixture()?;
    for cfg in [
        json!({"controller_home":"/tmp/not-this-fixture"}),
        json!({"controller_home":root.join("controller"),"worker_home":"/tmp/foreign"}),
        json!({"client_home":"../escape"}),
        json!({"controller_home":root.join("controller"),"storage_home":"/tmp/foreign"}),
        json!({}),
    ] {
        save(&root.join("service.json"), &cfg)?;
        let cmd = json!([
            env!("CARGO_BIN_EXE_fleet"),
            "service",
            "run",
            "--config",
            root.join("service.json")
        ]);
        ensure!(
            host::handle(&json!({"root":root,"op":"spawn","name":"invalid","command":cmd}))
                .is_err(),
            "foreign role accepted"
        );
    }
    ensure!(
        !root.join("processes/invalid.json").exists(),
        "unsafe process spawned"
    );
    fs::remove_dir_all(root)?;
    Ok(())
}
#[test]
fn fixture_only_recorded_child_is_stopped() -> Result<()> {
    let root = fixture()?;
    let (_, child) = host::handle(
        &json!({"root":root,"op":"spawn","name":"child","command":[env!("CARGO_BIN_EXE_fleet-test-workload"),"sleep","30","--home",root.join("worker")]}),
    )?;
    let mut process = Process {
        child: child.unwrap(),
    };
    ensure!(
        host::handle(&json!({"root":root,"op":"alive","name":"child"}))?.0 == true,
        "child not alive"
    );
    host::handle(&json!({"root":root,"op":"stop","name":"child"}))?;
    process.child.wait()?;
    ensure!(
        host::handle(&json!({"root":root,"op":"alive","name":"child"}))?.0 == false,
        "child survived"
    );
    fs::remove_dir_all(root)?;
    Ok(())
}
#[test]
fn fixture_held_session_lives_until_child_stops() -> Result<()> {
    use std::io::{BufRead, Write};
    let root = fixture()?;
    let mut session = std::process::Command::new(env!("CARGO_BIN_EXE_fleet-qualify"))
        .arg("host")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()?;
    let request = json!({"root":root,"op":"spawn","name":"held","hold_session":true,"command":[env!("CARGO_BIN_EXE_fleet-test-workload"),"sleep","30","--home",root.join("worker")]});
    session
        .stdin
        .take()
        .unwrap()
        .write_all(request.to_string().as_bytes())?;
    let mut line = String::new();
    std::io::BufReader::new(session.stdout.take().unwrap()).read_line(&mut line)?;
    let record: Value = serde_json::from_str(&line)?;
    ensure!(
        record["pid"].as_u64().unwrap() > 0 && session.try_wait()?.is_none(),
        "session not held"
    );
    host::handle(&json!({"root":root,"op":"stop","name":"held"}))?;
    ensure!(session.wait()?.success(), "held session failed");
    ensure!(
        host::handle(&json!({"root":root,"op":"alive","name":"held"}))?.0 == false,
        "child survived"
    );
    fs::remove_dir_all(root)?;
    Ok(())
}

#[test]
fn rendered_systemd_unit_validates_and_unsafe_variants_are_rejected() -> Result<()> {
    let root = fixture()?;
    save(
        &root.join("service.json"),
        &json!({"controller_home":root.join("controller"),"worker_home":null,"client_home":null,"bootstrap":true}),
    )?;
    let unit = fleet::control::service::render(
        "systemd",
        "fleet-qualification",
        "testuser",
        &root,
        &root.join("release with spaces/fleet"),
        &root.join("service.json"),
    )?;
    fleet::control::service::validate_unit(&unit)?;
    for changed in [
        unit.replace("User=testuser", "User=root"),
        unit.replace("User=testuser", "User=0"),
        unit.replace("KillMode=process", "KillMode=control-group"),
        unit.replace("User=testuser", "User=testuser\nUser=root"),
        unit.replace(" service run --config ", " daemon "),
    ] {
        ensure!(
            fleet::control::service::validate_unit(&changed).is_err(),
            "unsafe unit accepted"
        );
    }
    fs::remove_dir_all(root)?;
    Ok(())
}
