//! Process-level Tart double. Never opens a hypervisor or the normal Tart home.
use anyhow::{Context, Result, bail, ensure};
use serde_json::{Value, json};
use std::{
    fs,
    io::{Read, Write},
    os::unix::process::CommandExt,
    path::{Path, PathBuf},
    process::Command,
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, Instant},
};
static STOP: AtomicBool = AtomicBool::new(false);
extern "C" fn stop(_: i32) {
    STOP.store(true, Ordering::Relaxed);
}
fn sleep() {
    std::thread::sleep(Duration::from_millis(20));
}
fn alive(vm: &Path) -> bool {
    fs::read_to_string(vm.join("pid"))
        .ok()
        .and_then(|p| p.parse::<i32>().ok())
        .is_some_and(|pid| unsafe { libc::kill(pid, 0) } == 0)
}
fn helper(root: &Path) -> Result<PathBuf> {
    let executable = std::env::current_exe()?.canonicalize()?;
    let path = root.join("fleet-guest");
    if path.is_file() {
        return Ok(path);
    }
    let path = executable
        .parent()
        .context("missing executable parent")?
        .join("fleet-guest");
    ensure!(path.is_file(), "fixture needs companion fleet-guest");
    Ok(path)
}
fn translate(value: &mut Value, vm: &Path, name: &str, helper: &Path) {
    match value {
        Value::String(s) => {
            *s = s.replace(
                &format!("/tmp/{name}/work"),
                &vm.join("guest-work").to_string_lossy(),
            );
            if s.starts_with("/tmp/fleet-guest-") {
                *s = helper.to_string_lossy().into();
            }
        }
        Value::Array(values) => {
            for v in values {
                translate(v, vm, name, helper);
            }
        }
        Value::Object(values) => {
            for v in values.values_mut() {
                translate(v, vm, name, helper);
            }
        }
        _ => (),
    }
    // The simulated platform applies only to Fleet's fixed observation request.
    if value["kind"] == "prepare" && value["platform"].is_object() {
        value["platform"]["os"] = std::env::consts::OS.into();
        value["platform"]["arch"] = std::env::consts::ARCH.into();
    }
}
fn main() {
    if let Err(e) = run() {
        eprintln!("{e:#}");
        std::process::exit(1);
    }
}
fn run() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args == ["--version"] {
        println!("2.40.1");
        return Ok(());
    }
    let root = PathBuf::from(std::env::var("TART_HOME")?);
    let root = if root.exists() {
        root.canonicalize()?
    } else {
        root.parent()
            .context("Tart parent required")?
            .canonicalize()?
            .join(root.file_name().context("Tart directory required")?)
    };
    ensure!(
        root.is_absolute() && root.starts_with(std::env::temp_dir().canonicalize()?)
            || root.is_absolute() && root.starts_with(Path::new("/tmp").canonicalize()?),
        "fixture Tart home must be temporary"
    );
    fs::create_dir_all(root.join("vms"))?;
    fs::OpenOptions::new()
        .append(true)
        .create(true)
        .open(root.join("history"))?
        .write_all(format!("{}\n", args.join(" ")).as_bytes())?;
    let op = args.first().context("missing operation")?;
    match op.as_str() {
        "list" => {
            if root.join("fail-launcher-during-list").exists() {
                fs::write(root.join("release-launcher-failure"), "")?;
                let deadline = Instant::now() + Duration::from_secs(10);
                while fs::read_dir(root.join("vms"))?
                    .filter_map(Result::ok)
                    .any(|v| v.path().join("pid").exists())
                {
                    ensure!(Instant::now() < deadline, "fixture launcher did not exit");
                    sleep();
                }
            }
            let rows = fs::read_dir(root.join("vms"))?.map(|entry| { let e=entry?; Ok(json!({"Name":e.file_name().to_string_lossy(),"State":if alive(&e.path()) {"running"} else {"stopped"}})) }).collect::<Result<Vec<_>>>()?;
            println!("{}", json!(rows));
        }
        "clone" => {
            if root.join("slow-clone").exists() {
                std::thread::sleep(Duration::from_secs(30));
            }
            let vm = root.join("vms").join(&args[2]);
            fs::create_dir(&vm)?;
            fs::write(vm.join("disk"), "initial")?;
        }
        "set" => ensure!(root.join("vms").join(&args[1]).is_dir(), "missing VM"),
        "run" => {
            if root.join("fail-run").exists() {
                eprintln!("The maximum supported number of virtual machines has been reached.");
                std::process::exit(42);
            }
            ensure!(
                args.contains(&"--net-softnet".into())
                    && args.contains(&"--net-softnet-block".into()),
                "network isolation flags missing"
            );
            ensure!(
                !args.iter().any(|a| [
                    "--dir",
                    "--net-bridged",
                    "--net-host",
                    "--net-softnet-allow",
                    "--net-softnet-expose"
                ]
                .iter()
                .any(|p| a.starts_with(p))),
                "unsafe test configuration"
            );
            let vm = root.join("vms").join(&args[1]);
            let lock = fs::File::create(vm.join("lock"))?;
            lock.try_lock()?;
            fs::write(vm.join("pid.tmp"), std::process::id().to_string())?;
            fs::rename(vm.join("pid.tmp"), vm.join("pid"))?;
            unsafe {
                libc::signal(libc::SIGTERM, stop as *const () as libc::sighandler_t);
                libc::signal(libc::SIGINT, stop as *const () as libc::sighandler_t);
            }
            if root.join("fail-after-lock").exists() {
                if root.join("gate-launcher-failure").exists() {
                    while !root.join("release-launcher-failure").exists()
                        && !STOP.load(Ordering::Relaxed)
                    {
                        sleep();
                    }
                } else {
                    std::thread::sleep(Duration::from_secs(3));
                }
                fs::remove_file(vm.join("pid"))?;
                eprintln!("Late virtual machine capacity error");
                std::process::exit(42);
            }
            while !STOP.load(Ordering::Relaxed) {
                sleep();
            }
            fs::remove_file(vm.join("pid"))?;
        }
        "exec" if args[1] == "-i" => {
            let name = &args[2];
            let vm = root.join("vms").join(name);
            ensure!(alive(&vm), "VM is stopped");
            let helper = helper(&root)?;
            if args[3] == "/bin/sh" {
                ensure!(
                    args.get(6).is_some_and(|s| s == "fleet-guest-install"),
                    "unexpected bootstrap"
                );
                // Consume and compare the exact binary the host sends, without executing shell text.
                let mut bytes = vec![];
                std::io::stdin().take(64 << 20).read_to_end(&mut bytes)?;
                ensure!(
                    bytes == fs::read(&helper)?,
                    "bootstrap binary differs from fixture helper"
                );
                let name = Path::new(args.last().unwrap())
                    .file_name()
                    .context("helper path required")?;
                fs::create_dir_all(vm.join("guest-helpers"))?;
                fs::write(vm.join("guest-helpers").join(name), b"installed")?;
                println!("{}", fleet_guest::READY);
                return Ok(());
            }
            let legacy = args[3] == "/usr/bin/python3";
            if !legacy {
                let name = Path::new(&args[3])
                    .file_name()
                    .context("helper executable required")?;
                ensure!(
                    vm.join("guest-helpers").join(name).is_file(),
                    "guest helper disappeared after reboot"
                );
            }
            if args.get(4).is_some_and(|arg| arg == "--services") {
                let error = Command::new(helper)
                    .args([
                        "--services",
                        args.last().context("service configuration required")?,
                    ])
                    .exec();
                return Err(error.into());
            }
            let mut request: Value =
                serde_json::from_str(args.last().context("request required")?)?;
            if legacy && request["kind"] == "ready" {
                println!("fleet-guest-v1");
                return Ok(());
            }
            // The immutable historical baseline has a scripted guest envelope. At this mock
            // boundary its fixed helper is emulated by Rust; no interpreter is executed.
            if request["kind"] == "exec" {
                let argv = request["argv"].as_array_mut().context("argv required")?;
                if argv.len() == 5
                    && argv[0] == "/usr/bin/python3"
                    && argv[1] == "-I"
                    && argv[2] == "-c"
                {
                    let data = argv[4].clone();
                    *argv = vec![json!(helper), json!("--job"), data];
                }
                if argv.len() == 3 && argv[1] == "--job" {
                    let mut observation: Value =
                        serde_json::from_str(argv[2].as_str().context("observation required")?)?;
                    translate(&mut observation, &vm, name, &helper);
                    argv[2] = observation.to_string().into();
                }
            }
            translate(&mut request, &vm, name, &helper);
            let error = Command::new(helper)
                .arg(request.to_string())
                .env("FLEET_TEST_VM_LIFETIME", vm.join("pid"))
                .exec();
            return Err(error.into());
        }
        "exec" | "stop" => {
            if op == "exec" {
                ensure!(
                    args[2..] == ["/usr/bin/sudo", "-n", "/sbin/shutdown", "-h", "now"],
                    "unexpected shutdown"
                );
                if root.join("refuse-shutdown").exists() {
                    std::process::exit(1);
                }
            }
            let vm = root.join("vms").join(&args[1]);
            if let Ok(pid) = fs::read_to_string(vm.join("pid")) {
                unsafe {
                    libc::kill(pid.parse()?, libc::SIGTERM);
                }
            }
            for _ in 0..50 {
                if !vm.join("pid").exists() {
                    break;
                }
                sleep();
            }
        }
        "delete" => fs::remove_dir_all(root.join("vms").join(&args[1]))?,
        _ => bail!("unknown fake Tart request: {args:?}"),
    }
    Ok(())
}
