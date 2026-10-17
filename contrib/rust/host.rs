//! Guarded local/SSH fixture endpoint. All destructive paths and PID identities are checked here.
use super::support::*;
use anyhow::{Context, Result, bail, ensure};
use serde_json::{Value, json};
use std::{
    fs,
    io::Write,
    net::UdpSocket,
    os::unix::process::CommandExt,
    path::Path,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

pub fn identity(pid: u32) -> Result<Option<String>> {
    let out = Command::new("ps")
        .args([
            "-p",
            &pid.to_string(),
            "-o",
            "stat=",
            "-o",
            "lstart=",
            "-o",
            "command=",
        ])
        .output()?;
    if !out.status.success() {
        return Ok(None);
    }
    let text = String::from_utf8(out.stdout)?;
    let text = text.trim();
    let Some(n) = text.find(char::is_whitespace) else {
        return Ok(None);
    };
    if text[..n].starts_with('Z') {
        return Ok(None);
    }
    Ok(Some(text[n..].trim().into()))
}
pub fn handle(request: &Value) -> Result<(Value, Option<Child>)> {
    let root = fixture_root(Path::new(
        request["root"].as_str().context("root required")?,
    ))?;
    let op = request["op"].as_str().context("operation required")?;
    if op == "create" {
        ensure!(!root.exists(), "fixture already exists");
        fs::create_dir_all(&root)?;
        fs::write(
            root.join(".fleet-qualification"),
            "isolated control-plane fixture\n",
        )?;
        let out = Command::new("hostname").output()?;
        return Ok((
            json!({"hostname":String::from_utf8(out.stdout)?.trim(),"system":std::env::consts::OS,"architecture":std::env::consts::ARCH,"preservation":preservation()?}),
            None,
        ));
    }
    ensure!(
        root.join(".fleet-qualification").is_file(),
        "not an initialized fixture home"
    );
    match op {
        "preservation" => return Ok((preservation()?, None)),
        "write" => {
            let p = confined(
                &root,
                Path::new(request["path"].as_str().context("path required")?),
            )?;
            fs::create_dir_all(p.parent().unwrap())?;
            let data: Vec<u8> = serde_json::from_value(request["data"].clone())?;
            let temp = p.with_extension("fixture-tmp");
            fs::write(&temp, data)?;
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&temp, fs::Permissions::from_mode(0o600))?;
            fs::rename(temp, p)?;
            return Ok((true.into(), None));
        }
        "read" => {
            let p = confined(
                &root,
                Path::new(request["path"].as_str().context("path required")?),
            )?;
            return Ok((
                if p.exists() {
                    json!(fs::read(p)?)
                } else {
                    Value::Null
                },
                None,
            ));
        }
        "port" => {
            let address = request["address"].as_str().context("address required")?;
            let port = request["port"].as_u64().context("port required")?;
            ensure!(port <= 65535, "invalid port");
            let _socket = UdpSocket::bind((address, port as u16))?;
            return Ok((true.into(), None));
        }
        "digest" => {
            let path = Path::new(request["path"].as_str().context("binary path required")?);
            ensure!(path.is_absolute(), "absolute binary path required");
            return Ok((digest(path)?.into(), None));
        }
        _ => (),
    }
    let name = request["name"].as_str().context("process name required")?;
    ensure!(
        !name.is_empty()
            && name
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-'),
        "invalid process name"
    );
    let marker = confined(&root, Path::new(&format!("processes/{name}.json")))?;
    if op == "spawn" {
        if marker.exists() {
            let old = read(&marker)?;
            ensure!(
                identity(old["pid"].as_u64().context("PID required")? as u32)?.is_none(),
                "fixture process already exists"
            );
        }
        let args: Vec<String> = serde_json::from_value(request["command"].clone())?;
        ensure!(
            !args.is_empty() && Path::new(&args[0]).is_absolute(),
            "explicit executable required"
        );
        if args.iter().any(|s| s == "service") && args.iter().any(|s| s == "run") {
            validate_service(&root, &args)?;
        } else {
            let n = args
                .iter()
                .position(|s| s == "--home")
                .context("fixture home required")?;
            confined(
                &root,
                Path::new(args.get(n + 1).context("home argument required")?),
            )?;
        }
        let log = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(root.join(format!("{name}.log")))?;
        let mut child = Command::new(&args[0])
            .args(&args[1..])
            .current_dir(&root)
            .stdin(Stdio::null())
            .stdout(log.try_clone()?)
            .stderr(log)
            .process_group(0)
            .spawn()?;
        std::thread::sleep(Duration::from_millis(100));
        ensure!(
            child.try_wait()?.is_none(),
            "fixture process exited: {name}"
        );
        let recorded = identity(child.id())?.context("fixture process identity unavailable")?;
        let record = json!({"pid":child.id(),"identity":recorded,"command":args});
        save(&marker, &record)?;
        return Ok((record, Some(child)));
    }
    let record = read(&marker)?;
    let pid = record["pid"].as_u64().context("PID required")? as u32;
    let Some(actual) = identity(pid)? else {
        return Ok((false.into(), None));
    };
    ensure!(
        record["identity"] == actual,
        "PID identity changed; refusing to signal it"
    );
    if op == "alive" {
        return Ok((true.into(), None));
    }
    ensure!(op == "stop", "unknown fixture operation");
    unsafe {
        libc::kill(pid as i32, libc::SIGCONT);
        libc::kill(
            pid as i32,
            if request["kill"] == true {
                libc::SIGKILL
            } else {
                libc::SIGTERM
            },
        );
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    while let Some(actual) = identity(pid)? {
        if Instant::now() >= deadline {
            // A dying macOS process can briefly lose its command text before it becomes a
            // zombie. Wait without signaling again; check identity before any escalation.
            ensure!(
                record["identity"] == actual,
                "PID identity changed during stop"
            );
            unsafe {
                libc::kill(pid as i32, libc::SIGKILL);
            }
            std::thread::sleep(Duration::from_millis(300));
            break;
        }
        pause();
    }
    ensure!(identity(pid)?.is_none(), "fixture process did not exit");
    Ok((true.into(), None))
}
pub fn stdin() -> Result<()> {
    let request: Value = serde_json::from_reader(std::io::stdin().lock())?;
    let (result, child) = handle(&request)?;
    println!("{result}");
    std::io::stdout().flush()?;
    if request["hold_session"] == true {
        let Some(mut child) = child else {
            bail!("only spawn can hold a session")
        };
        child.wait()?;
    }
    Ok(())
}
