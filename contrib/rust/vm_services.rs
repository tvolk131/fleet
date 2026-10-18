//! End-to-end CLI qualification, using either real offline Tart VMs or the
//! process-level test double. The latter does not prove a hypervisor boundary.
use super::{support::*, tart};
use anyhow::{Context, Result, ensure};
use fleet::instances::{self, guest};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::{Read, Write},
    net::{TcpListener, TcpStream, UdpSocket},
    process::{Command, Stdio},
    time::{Duration, Instant},
};

fn port() -> Result<String> {
    Ok(TcpListener::bind("127.0.0.1:0")?.local_addr()?.to_string())
}
struct Vm {
    id: &'static str,
    worker: &'static str,
}
fn backend(l: &Lab, vm: &Vm) -> std::path::PathBuf {
    l.root.join(vm.worker).join("backend")
}
fn state(l: &mut Lab, id: &str, phase: &str) -> Result<Value> {
    wait(
        || {
            for (name, process) in &mut l.processes {
                if name.starts_with("worker")
                    && let Some(status) = process.child.try_wait()?
                {
                    let log =
                        fs::read_to_string(l.root.join(format!("{name}.log"))).unwrap_or_default();
                    anyhow::bail!(
                        "fixture {name} exited ({status}): {}",
                        log.chars().take(2048).collect::<String>()
                    );
                }
            }
            l.request(json!({"op":"instances"}))
        },
        |rows| {
            rows.as_array().is_some_and(|r| {
                r.iter().any(|r| {
                    r["id"] == id
                        && r["observed"][0] == r["revision"]
                        && r["observed"][1]["phase"]["Instance"] == phase
                })
            })
        },
        180,
    )
}
fn operation(l: &Lab, vm: &Vm, name: &str, spec: guest::Spec, finish: bool) -> Result<()> {
    let backend = backend(l, vm);
    let vm = format!("vm-{}", vm.id);
    let revision = instances::info(&backend, &vm)?.record.revision;
    guest::start(&backend, &vm, name, revision, spec, &l.binary)?;
    if finish {
        let deadline = Instant::now() + Duration::from_secs(90);
        loop {
            let op = guest::inspect(&backend, &vm, name)?;
            if op.state.finished() {
                let dir = guest::operation_dir(&backend, &vm, name)?;
                ensure!(
                    op.exit_code == Some(0),
                    "guest operation failed: {op:?}; {}",
                    String::from_utf8_lossy(&fs::read(dir.join("stderr")).unwrap_or_default())
                );
                break;
            }
            ensure!(Instant::now() < deadline, "guest operation timed out");
            pause();
        }
    }
    Ok(())
}
fn executable(l: &Lab, id: &str) -> String {
    l.s(&format!("guest-{id}/workload"))
}
fn upload(l: &Lab, vm: &Vm, name: &str) -> Result<()> {
    // Tart reports a running hypervisor before the guest agent has booted.
    // Only this side-effect-free readiness probe is retried, never the upload.
    wait(
        || {
            Ok(serde_json::to_value(guest::readiness(
                &backend(l, vm),
                &format!("vm-{}", vm.id),
            )?)?)
        },
        |r| r["ready"] == true,
        180,
    )?;
    let bytes = fs::read(l.root.join("fleet-test-workload"))?;
    let dir = guest::operation_dir(&backend(l, vm), &format!("vm-{}", vm.id), name)?;
    fs::create_dir_all(&dir)?;
    fs::write(dir.join("input"), &bytes)?;
    operation(
        l,
        vm,
        name,
        guest::Spec::Upload {
            path: executable(l, vm.id),
            hash: iroh_blobs::Hash::new(&bytes),
            size: bytes.len() as u64,
            sha256: digest(&l.root.join("fleet-test-workload"))?,
            executable: true,
        },
        true,
    )
}
fn exec(l: &Lab, vm: &Vm, name: &str, mode: &str, address: &str, finish: bool) -> Result<()> {
    operation(
        l,
        vm,
        name,
        guest::Spec::Exec {
            argv: vec![executable(l, vm.id), mode.into(), address.into()],
            cwd: None,
            env: BTreeMap::new(),
            timeout_seconds: if finish { 40 } else { 600 },
        },
        finish,
    )?;
    if mode == "service-echo" && !finish {
        let path =
            guest::operation_dir(&backend(l, vm), &format!("vm-{}", vm.id), name)?.join("stdout");
        wait(
            || Ok(json!(fs::read_to_string(&path).unwrap_or_default())),
            |v| v.as_str().is_some_and(|s| s.contains("service-ready")),
            30,
        )?;
    }
    Ok(())
}
fn probe(l: &Lab, vm: &Vm, name: &str, address: &str) -> Result<()> {
    exec(l, vm, name, "service-roundtrip", address, true)
}
fn confirmed(l: &Lab, revision: u64) -> Result<()> {
    let result =
        l.request(json!({"op":"wait_tunnels","revision":revision,"timeout_seconds":60}))?;
    ensure!(
        result["confirmed"] == true,
        "service policy unconfirmed: {result}"
    );
    Ok(())
}
fn apply(l: &Lab, topology: &Value, previous: u64) -> Result<()> {
    l.request(json!({"op":"replace_tunnels","request_id":format!("services-{previous}"),"expected_revision":previous,"topology":topology}))?;
    confirmed(l, previous + 1)
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
    l.request(json!({"op":"prepare_version","request_id":"prepare","expected_revision":0,"target":fleet::control::tunnels::GENERATION}))?;
    wait(
        || l.request(json!({"op":"versions"})),
        |v| v["version"]["pending"]["ready"] == json!([1]),
        30,
    )?;
    l.request(json!({"op":"activate_version","request_id":"activate","expected_revision":1}))?;
    let mut settings = tart::settings(&l, &a)?;
    settings["memory_budget_mb"] = if a.fake { 4096 } else { 8192 }.into();
    // Fake VMs allocate no RAM. Separate small logical workers let CI exercise
    // their network identities without relaxing real host admission checks.
    let vms = [
        Vm {
            id: "a",
            worker: if a.fake { "worker-a" } else { "worker" },
        },
        Vm {
            id: "b",
            worker: if a.fake { "worker-b" } else { "worker" },
        },
    ];
    let [vm_a, vm_b] = &vms;
    for worker in vms.iter().map(|vm| vm.worker).collect::<BTreeSet<_>>() {
        tart::worker(&l, &a, worker, &settings)?;
        tart::enroll(&l, worker, false)?;
        l.start(worker, None)?;
    }
    save(
        &l.root.join("vm.json"),
        &json!({"image":"macos","cpus":if a.fake {1} else {2},"memory_mb":4096,"disk_gb":50,"network":"offline","gpu":"shared"}),
    )?;
    for vm in &vms {
        let id = vm.id;
        l.instances.push(id.into());
        l.cli(&[
            "--home",
            &l.s("client"),
            "instance",
            "create",
            id,
            "--worker",
            vm.worker,
            "--spec",
            &l.s("vm.json"),
            "--request-id",
            &format!("create-{id}"),
        ])?;
        state(&mut l, id, "Running")?;
        upload(&l, vm, "upload")?;
    }
    let host_target = port()?;
    let a_target = port()?;
    let b_target = port()?;
    let a_to_b = port()?;
    let a_to_host = port()?;
    let b_to_a = port()?;
    let host_to_b = port()?;
    let child = Command::new(l.root.join("fleet-test-workload"))
        .args(["service-echo", &host_target])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    l.processes.insert("host-echo".into(), Process { child });
    exec(&l, vm_a, "echo", "service-echo", &a_target, false)?;
    exec(&l, vm_b, "echo", "service-echo", &b_target, false)?;
    let mut topology = json!({"adapters":{},"services":{
        "host-echo":{"owner":"host","target":host_target,"allowed":["a"]},
        "a-echo":{"owner":"a","target":a_target,"allowed":["b"]},
        "b-echo":{"owner":"b","target":b_target,"allowed":["a","host"]}
    },"routes":[
        {"adapter":"a","service":"b-echo","listen":a_to_b},
        {"adapter":"a","service":"host-echo","listen":a_to_host},
        {"adapter":"b","service":"a-echo","listen":b_to_a},
        {"adapter":"host","service":"b-echo","listen":host_to_b}
    ]});
    for id in ["host", "a", "b"] {
        let udp = UdpSocket::bind("127.0.0.1:0")?.local_addr()?;
        save(
            &l.root.join(format!("{id}-network.json")),
            &json!({"bind":udp,"relays":[]}),
        )?;
        let mut args = vec![
            "tunnels".into(),
            "prepare".into(),
            "--adapter-home".into(),
            l.s(&format!("adapter-{id}")),
            "--adapter".into(),
            id.into(),
            "--authority".into(),
            l.s("client/config.json"),
            "--network".into(),
            l.s(&format!("{id}-network.json")),
        ];
        if id != "host" {
            let vm = vms.iter().find(|vm| vm.id == id).unwrap();
            args.extend([
                "--worker-home".into(),
                l.s(vm.worker),
                "--instance".into(),
                format!("vm-{id}"),
            ]);
        }
        topology["adapters"][id] = checked(&l.binary, &args)?;
        l.cli(&[
            "service",
            "configure",
            "--config",
            &l.s(&format!("adapter-{id}.json")),
            "--tunnel-home",
            &l.s(&format!("adapter-{id}")),
        ])?;
        l.start(&format!("adapter-{id}"), None)?;
    }
    apply(&l, &topology, 0)?;
    probe(&l, vm_a, "vm-to-vm", &a_to_b)?;
    probe(&l, vm_b, "reverse-vm-to-vm", &b_to_a)?;
    probe(&l, vm_a, "vm-to-host", &a_to_host)?;
    let local = output(
        &l.root.join("fleet-test-workload"),
        &["service-roundtrip".into(), host_to_b.clone()],
        None,
        30,
    )?;
    ensure!(local.status.success(), "host to VM roundtrip failed");
    l.phase("vm_to_vm_vm_to_host_and_host_to_vm_large_half_closed_streams");
    if !a.fake {
        // A guest listener must not appear on the physical host or in another VM.
        let _no_host_listener =
            TcpListener::bind(&a_to_host).context("VM route leaked a host listener")?;
        exec(
            &l,
            vm_b,
            "other-vm-port-denied",
            "service-denied",
            &a_to_host,
            true,
        )?;
        // The physical host's service is absent from VM loopback without a grant.
        exec(
            &l,
            vm_b,
            "host-port-denied",
            "service-denied",
            &host_target,
            true,
        )?;
        l.phase("real_vm_namespaces_keep_ports_private");
    }
    // A second host adapter cannot silently become another identity for this VM.
    let duplicate = output(
        &l.binary,
        &[
            "tunnels".into(),
            "run".into(),
            "--adapter-home".into(),
            l.s("adapter-a"),
        ],
        None,
        10,
    )?;
    ensure!(
        !duplicate.status.success(),
        "duplicate adapter unexpectedly started"
    );
    save(
        &l.root.join("shadow-network.json"),
        &json!({"bind":UdpSocket::bind("127.0.0.1:0")?.local_addr()?,"relays":[]}),
    )?;
    l.cli(&[
        "tunnels",
        "prepare",
        "--adapter-home",
        &l.s("adapter-shadow"),
        "--adapter",
        "shadow",
        "--authority",
        &l.s("client/config.json"),
        "--network",
        &l.s("shadow-network.json"),
        "--worker-home",
        &l.s(vm_a.worker),
        "--instance",
        "vm-a",
    ])?;
    let shadow = output(
        &l.binary,
        &[
            "tunnels".into(),
            "run".into(),
            "--adapter-home".into(),
            l.s("adapter-shadow"),
        ],
        None,
        10,
    )?;
    ensure!(
        !shadow.status.success()
            && String::from_utf8_lossy(&shadow.stderr)
                .contains("VM already has a running service adapter"),
        "a second identity attached to the same VM"
    );
    // Test downgrade with the current process stopped: a held lock must not
    // hide whether the old binary would incorrectly interpret this home.
    l.stop("controller", true)?;
    let cached_restart = (|| -> Result<()> {
        l.stop("adapter-a", false)?;
        if let Some(baseline) = &a.baseline_binary {
            let old = output(
                baseline,
                &[
                    "tunnels".into(),
                    "run".into(),
                    "--adapter-home".into(),
                    l.s("adapter-a"),
                ],
                None,
                10,
            )?;
            ensure!(
                !old.status.success()
                    && String::from_utf8_lossy(&old.stderr).contains("No such file"),
                "old adapter accepted VM configuration"
            );
            l.phase("previous_host_adapter_refuses_vm_attachment_home");
        }
        // Cached policy survives both controller loss and an adapter restart.
        l.start("adapter-a", None)?;
        // Tart may keep the old guest command's stdin open after the host dies.
        // Allow its heartbeat expiry and the replacement adapter's bind retry.
        std::thread::sleep(fleet_guest::tunnel::DEADLINE + Duration::from_secs(5));
        probe(&l, vm_a, "cached-policy-restart", &a_to_b)?;
        Ok(())
    })();
    // Restore the fixture controller even on failure so Lab can delete its VMs.
    l.start("controller", None)?;
    cached_restart?;
    confirmed(&l, 1)?;
    l.phase("cached_policy_survives_controller_outage_and_adapter_restart");
    let mut active = TcpStream::connect(&host_to_b)?;
    active.set_read_timeout(Some(Duration::from_secs(10)))?;
    active.write_all(b"before-revoke")?;
    let mut marker = [0; 13];
    active.read_exact(&mut marker)?;
    topology["services"]["b-echo"]["allowed"] = json!([]);
    topology["routes"]
        .as_array_mut()
        .unwrap()
        .retain(|r| r["service"] != "b-echo");
    apply(&l, &topology, 1)?;
    let closed = active.read(&mut marker);
    ensure!(
        matches!(&closed, Ok(0))
            || closed.as_ref().err().is_some_and(|e| matches!(
                e.kind(),
                std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::BrokenPipe
                    | std::io::ErrorKind::UnexpectedEof
            )),
        "revoked stream remains open: {closed:?}"
    );
    exec(
        &l,
        vm_a,
        "revoked-port-denied",
        "service-denied",
        &a_to_b,
        true,
    )?;
    probe(&l, vm_a, "other-service-still-allowed", &a_to_host)?;
    l.phase("confirmed_revocation_closes_existing_connections_and_unbinds_route");
    let conflict = port()?;
    exec(
        &l,
        vm_a,
        "conflicting-listener",
        "service-echo",
        &conflict,
        false,
    )?;
    probe(&l, vm_a, "conflict-ready", &conflict)?;
    topology["routes"]
        .as_array_mut()
        .unwrap()
        .push(json!({"adapter":"a","service":"host-echo","listen":conflict}));
    l.request(json!({"op":"replace_tunnels","request_id":"conflict","expected_revision":2,"topology":topology}))?;
    wait(
        || l.request(json!({"op":"tunnels"})),
        |s| {
            s["adapters"].as_array().is_some_and(|a| {
                a.iter()
                    .any(|a| a["adapter"] == "a" && a["state"] == "failed")
            })
        },
        30,
    )?;
    ensure!(
        l.request(json!({"op":"wait_tunnels","revision":3,"timeout_seconds":1}))?["confirmed"]
            == false,
        "partial guest bind was acknowledged"
    );
    exec(
        &l,
        vm_a,
        "partial-bind-denied",
        "service-denied",
        &a_to_host,
        true,
    )?;
    topology["routes"]
        .as_array_mut()
        .unwrap()
        .retain(|r| r["listen"] != conflict);
    apply(&l, &topology, 3)?;
    probe(&l, vm_a, "after-bind-repair", &a_to_host)?;
    l.phase("partial_guest_bind_fails_closed_and_recovers_after_policy_repair");
    l.cli(&[
        "--home",
        &l.s("client"),
        "instance",
        "stop",
        "a",
        "--request-id",
        "stop-a",
    ])?;
    state(&mut l, "a", "Stopped")?;
    wait(
        || l.request(json!({"op":"tunnels"})),
        |s| {
            s["adapters"].as_array().is_some_and(|a| {
                a.iter()
                    .any(|a| a["adapter"] == "a" && a["state"] == "failed")
            })
        },
        30,
    )?;
    l.cli(&[
        "--home",
        &l.s("client"),
        "instance",
        "start",
        "a",
        "--request-id",
        "restart-a",
    ])?;
    state(&mut l, "a", "Running")?;
    confirmed(&l, 4)?;
    upload(&l, vm_a, "upload-after-restart")?;
    probe(&l, vm_a, "after-vm-restart", &a_to_host)?;
    l.phase("vm_stop_fails_closed_and_restart_reattaches");
    l.cli(&[
        "--home",
        &l.s("client"),
        "instance",
        "stop",
        "a",
        "--request-id",
        "stop-a-final",
    ])?;
    state(&mut l, "a", "Stopped")?;
    apply(&l, &json!({"adapters":{},"services":{},"routes":[]}), 4)?;
    l.phase("retired_stopped_vm_acknowledges_empty_policy");
    l.report(json!({"success":true,"real_tart":!a.fake,"isolated_guest_namespaces":!a.fake,"service_protocol":"fleet-service-channel-1","physical_failure_domain":"one machine","all_fixture_vms_deleted":true}))
}
