//! Black-box fixtures: CLI requests are passed unchanged, never reimplemented or implicitly retried.
use anyhow::{Context, Result, bail, ensure};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::Write,
    os::unix::{
        fs::{OpenOptionsExt, PermissionsExt},
        process::CommandExt,
    },
    path::{Component, Path, PathBuf},
    process::{Child, Command, Output, Stdio},
    time::{Duration, Instant},
};

pub fn pause() {
    std::thread::sleep(Duration::from_millis(100));
}
pub fn digest(path: &Path) -> Result<String> {
    Ok(format!("{:x}", Sha256::digest(fs::read(path)?)))
}
pub fn read(path: &Path) -> Result<Value> {
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}
pub fn save(path: &Path, value: &Value) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(&serde_json::to_vec_pretty(value)?)?;
    file.write_all(b"\n")?;
    Ok(())
}
pub fn resolve(path: &Path) -> Result<PathBuf> {
    ensure!(path.is_absolute(), "absolute path required");
    let mut result = PathBuf::new();
    for component in path.components() {
        match component {
            Component::ParentDir => {
                ensure!(result.pop(), "path escapes root");
            }
            Component::CurDir => (),
            other => result.push(other.as_os_str()),
        }
        if result.exists() || fs::symlink_metadata(&result).is_ok() {
            result = result.canonicalize()?;
        }
    }
    Ok(result)
}
pub fn fixture_root(path: &Path) -> Result<PathBuf> {
    let path = resolve(path)?;
    let temp = Path::new("/tmp").canonicalize()?;
    ensure!(
        path.starts_with(&temp)
            && path != temp
            && path.components().any(|c| c
                .as_os_str()
                .to_string_lossy()
                .starts_with("fleet-control-")),
        "fixture must be under /tmp/fleet-control-*"
    );
    Ok(path)
}
pub fn confined(root: &Path, path: &Path) -> Result<PathBuf> {
    let path = resolve(&root.join(path))?;
    ensure!(
        path.starts_with(root) && path != root,
        "path escapes fixture home"
    );
    Ok(path)
}
pub fn validate_service(root: &Path, args: &[String]) -> Result<()> {
    let n = args
        .iter()
        .position(|s| s == "--config")
        .context("service configuration required")?;
    let path = confined(
        root,
        Path::new(args.get(n + 1).context("configuration path required")?),
    )?;
    let config = read(&path)?;
    let mut count = 0;
    for key in [
        "controller_home",
        "worker_home",
        "storage_home",
        "client_home",
    ] {
        if let Some(home) = config[key].as_str() {
            ensure!(
                Path::new(home).is_absolute(),
                "absolute service home required"
            );
            confined(root, Path::new(home))?;
            count += 1;
        }
    }
    ensure!(count > 0, "service has no roles");
    Ok(())
}
pub fn output(
    binary: &Path,
    args: &[String],
    input: Option<&[u8]>,
    seconds: u64,
) -> Result<Output> {
    let dir =
        std::env::temp_dir().join(format!("fleet-cli-output-{:032x}", rand::random::<u128>()));
    fs::create_dir(&dir)?;
    struct Scratch(PathBuf);
    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    let _scratch = Scratch(dir.clone());
    let file = |name: &str| -> Result<File> {
        Ok(OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(dir.join(name))?)
    };
    let mut command = Command::new(binary);
    command
        .args(args)
        .stdout(file("stdout")?)
        .stderr(file("stderr")?)
        .process_group(0);
    if let Some(input) = input {
        let mut f = file("stdin")?;
        f.write_all(input)?;
        drop(f);
        command.stdin(File::open(dir.join("stdin"))?);
    } else {
        command.stdin(Stdio::null());
    }
    let mut child = command
        .spawn()
        .with_context(|| format!("launch {}", binary.display()))?;
    let deadline = Instant::now() + Duration::from_secs(seconds);
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if Instant::now() >= deadline {
            unsafe {
                libc::kill(-(child.id() as i32), libc::SIGKILL);
            }
            let _ = child.wait();
            bail!("command timed out: {} {args:?}", binary.display());
        }
        pause();
    };
    Ok(Output {
        status,
        stdout: fs::read(dir.join("stdout"))?,
        stderr: fs::read(dir.join("stderr"))?,
    })
}
pub fn checked(binary: &Path, args: &[String]) -> Result<Value> {
    let out = output(binary, args, None, 120)?;
    ensure!(
        out.status.success(),
        "{} {args:?}: {} {}",
        binary.display(),
        String::from_utf8_lossy(&out.stderr),
        String::from_utf8_lossy(&out.stdout)
    );
    if out.stdout.iter().all(u8::is_ascii_whitespace) {
        Ok(Value::Null)
    } else {
        serde_json::from_slice(&out.stdout).context("CLI did not return JSON")
    }
}
pub fn api(binary: &Path, home: &Path, request: Value) -> Result<Value> {
    let out = output(
        binary,
        &["--home".into(), home.display().to_string(), "api".into()],
        Some(&serde_json::to_vec(&request)?),
        90,
    )?;
    ensure!(
        out.status.success(),
        "API process failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let answer: Value = serde_json::from_slice(&out.stdout)?;
    ensure!(answer["ok"].is_boolean(), "invalid API envelope {answer}");
    Ok(answer)
}
pub fn api_result(binary: &Path, home: &Path, request: Value) -> Result<Value> {
    let answer = api(binary, home, request)?;
    ensure!(answer["ok"] == true, "API error: {answer}");
    let result = answer["result"].clone();
    ensure!(
        !["Rejected", "RecoveryUnknown", "Expired"]
            .iter()
            .any(|k| result["reply"].get(k).is_some()),
        "request rejected: {result}"
    );
    Ok(result)
}
pub fn wait(
    mut call: impl FnMut() -> Result<Value>,
    predicate: impl Fn(&Value) -> bool,
    seconds: u64,
) -> Result<Value> {
    let deadline = Instant::now() + Duration::from_secs(seconds);
    loop {
        let last = call()?;
        if predicate(&last) {
            return Ok(last);
        }
        ensure!(
            Instant::now() < deadline,
            "timed out waiting for state: {last}"
        );
        std::thread::sleep(Duration::from_millis(200));
    }
}
pub fn preservation() -> Result<Value> {
    let home = PathBuf::from(std::env::var("HOME")?);
    let old = home.join(".fleet");
    let mut result = serde_json::Map::new();
    for (kind, names) in [
        ("jobs", vec!["job.json", "supervisor.json", "exit.json"]),
        ("instances", vec!["desired.json", "observed.json"]),
    ] {
        if let Ok(entries) = fs::read_dir(old.join(kind)) {
            for entry in entries {
                let entry = entry?;
                for name in &names {
                    let p = entry.path().join(name);
                    if p.is_file() {
                        result.insert(
                            p.strip_prefix(&old)?.display().to_string(),
                            digest(&p)?.into(),
                        );
                    }
                }
            }
        }
    }
    // Read bytes only, including any journal. Never open or migrate the installed database.
    for name in ["fleet.db", "fleet.db-wal", "fleet.db-journal"] {
        let p = old.join(name);
        if p.is_file() {
            result.insert(name.into(), digest(&p)?.into());
        }
    }
    let binary = home.join("fleet/bin/fleet");
    if binary.is_file() {
        result.insert("installed_binary_sha256".into(), digest(&binary)?.into());
    }
    Ok(result.into())
}
pub struct Process {
    pub child: Child,
}
impl Process {
    pub fn stop(&mut self, kill: bool) -> Result<()> {
        if self.child.try_wait()?.is_some() {
            return Ok(());
        }
        unsafe {
            libc::kill(
                self.child.id() as i32,
                if kill { libc::SIGKILL } else { libc::SIGTERM },
            );
        }
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if self.child.try_wait()?.is_some() {
                return Ok(());
            }
            if Instant::now() >= deadline {
                self.child.kill()?;
                self.child.wait()?;
                return Ok(());
            }
            pause();
        }
    }
}
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.stop(false);
    }
}
pub struct Lab {
    pub root: PathBuf,
    pub binary: PathBuf,
    pub processes: BTreeMap<String, Process>,
    pub before: Value,
    pub phases: Vec<String>,
    pub instances: Vec<String>,
}
impl Lab {
    pub fn new(root: &Path, binary: &Path) -> Result<Self> {
        let root = fixture_root(root)?;
        ensure!(!root.exists(), "use a fresh fixture home");
        ensure!(
            binary.is_absolute() && binary.is_file(),
            "explicit Fleet binary required"
        );
        fs::create_dir_all(&root)?;
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700))?;
        fs::write(
            root.join(".fleet-qualification"),
            "isolated control-plane fixture\n",
        )?;
        let source = binary.canonicalize()?;
        fs::copy(&source, root.join("fleet"))?;
        for name in ["fleet-guest", "fleet-fake-tart", "fleet-test-workload"] {
            let p = source.parent().unwrap().join(name);
            if p.is_file() {
                fs::copy(p, root.join(name))?;
            }
        }
        Ok(Self {
            binary: root.join("fleet"),
            root,
            processes: BTreeMap::new(),
            before: preservation()?,
            phases: vec![],
            instances: vec![],
        })
    }
    pub fn s(&self, path: &str) -> String {
        self.root.join(path).display().to_string()
    }
    pub fn cli(&self, args: &[&str]) -> Result<Value> {
        checked(
            &self.binary,
            &args.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
        )
    }
    pub fn request(&self, request: Value) -> Result<Value> {
        api_result(&self.binary, &self.root.join("client"), request)
    }
    pub fn envelope(&self, request: Value) -> Result<Value> {
        api(&self.binary, &self.root.join("client"), request)
    }
    pub fn state(&self) -> Result<Value> {
        self.cli(&[
            "cluster",
            "read",
            "--key",
            &self.s("client/node.key"),
            "--config",
            &self.s("client/config.json"),
        ])
    }
    pub fn init(&self, replicas: u64) -> Result<Value> {
        let value = self.cli(&[
            "cluster",
            "init",
            "--home",
            &self.s("seed"),
            "--replicas",
            &replicas.to_string(),
        ])?;
        self.cli(&[
            "--home",
            &self.s("client"),
            "client",
            "import",
            "--config",
            &self.s("seed/client/config.json"),
            "--key",
            &self.s("seed/client/node.key"),
        ])?;
        Ok(value)
    }
    pub fn start(&mut self, name: &str, binary: Option<&Path>) -> Result<()> {
        ensure!(
            !self.processes.contains_key(name),
            "process already started"
        );
        let args = vec![
            "service".into(),
            "run".into(),
            "--config".into(),
            self.s(&format!("{name}.json")),
        ];
        validate_service(&self.root, &args)?;
        let log = OpenOptions::new()
            .create(true)
            .append(true)
            .mode(0o600)
            .open(self.root.join(format!("{name}.log")))?;
        let child = Command::new(binary.unwrap_or(&self.binary))
            .args(args)
            .stdin(Stdio::null())
            .stdout(log.try_clone()?)
            .stderr(log)
            .process_group(0)
            .spawn()?;
        self.processes.insert(name.into(), Process { child });
        Ok(())
    }
    pub fn stop(&mut self, name: &str, kill: bool) -> Result<()> {
        if let Some(mut p) = self.processes.remove(name) {
            p.stop(kill)?;
        }
        Ok(())
    }
    pub fn phase(&mut self, name: &str) {
        self.phases.push(name.into());
        println!("{}", json!({"phase":name}));
    }
    pub fn report(&mut self, mut result: Value) -> Result<()> {
        self.cancel_owned()?;
        self.delete_owned_instances()?;
        for name in self.processes.keys().cloned().collect::<Vec<_>>() {
            self.stop(&name, false)?;
        }
        ensure!(preservation()? == self.before, "installed state changed");
        result["installed_state_unchanged"] = true.into();
        result["fixture_processes_stopped"] = true.into();
        result["phases"] = json!(self.phases);
        save(&self.root.join("qualification.json"), &result)?;
        println!("{result}");
        Ok(())
    }
    pub fn cancel_owned(&self) -> Result<()> {
        let mut pending = vec![];
        fn visit(path: &Path, pending: &mut Vec<PathBuf>) -> Result<()> {
            if !path.is_dir() || fs::symlink_metadata(path)?.file_type().is_symlink() {
                return Ok(());
            }
            if path.join("job.json").is_file()
                && path.join("supervisor.json").exists()
                && !path.join("exit.json").exists()
            {
                fs::write(path.join("cancel"), "isolated qualification cleanup\n")?;
                pending.push(path.to_owned());
                return Ok(());
            }
            for entry in fs::read_dir(path)? {
                let entry = entry?;
                if entry.file_type()?.is_dir() {
                    visit(&entry.path(), pending)?;
                }
            }
            Ok(())
        }
        visit(&self.root, &mut pending)?;
        let deadline = Instant::now() + Duration::from_secs(40);
        while pending.iter().any(|p| !p.join("exit.json").exists()) {
            ensure!(
                Instant::now() < deadline,
                "fixture workload cleanup timed out"
            );
            pause();
        }
        Ok(())
    }
    fn delete_owned_instances(&mut self) -> Result<()> {
        for id in &self.instances {
            let rows = self.request(json!({"op":"instances"}))?;
            let row = rows
                .as_array()
                .context("instance inventory missing")?
                .iter()
                .find(|r| r["id"] == *id);
            let Some(row) = row else { continue };
            if row["observed"][1]["phase"]["Instance"] == "Deleted" {
                continue;
            }
            for (action, phase) in [("stop", "Stopped"), ("delete", "Deleted")] {
                self.cli(&[
                    "--home",
                    &self.s("client"),
                    "instance",
                    action,
                    id,
                    "--request-id",
                    &format!("fixture-cleanup-{action}-{id}"),
                ])?;
                wait(
                    || self.request(json!({"op":"instances"})),
                    |rows| {
                        rows.as_array().is_some_and(|rows| {
                            rows.iter().any(|r| {
                                r["id"] == *id
                                    && r["observed"][0] == r["revision"]
                                    && r["observed"][1]["phase"]["Instance"] == phase
                            })
                        })
                    },
                    90,
                )?;
            }
        }
        self.instances.clear();
        Ok(())
    }
}
impl Drop for Lab {
    fn drop(&mut self) {
        if let Err(e) = self.cancel_owned() {
            eprintln!("fixture cleanup: {e:#}");
        }
        // Supervisors and instance controllers deliberately outlive workers.
        // Delete only explicitly registered fixture instances while services
        // are still alive; stopping the worker alone would leave a VM running.
        if let Err(e) = self.delete_owned_instances() {
            eprintln!("fixture instance cleanup: {e:#}");
        }
    }
}
