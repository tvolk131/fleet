use super::{host, support::*};
use anyhow::{Context, Result, ensure};
use clap::Args;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::{BufRead, Write},
    os::fd::AsRawFd,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};
#[derive(Args)]
pub struct Options {
    #[arg(long, conflicts_with = "local", required_unless_present = "local")]
    inventory: Option<PathBuf>,
    #[arg(long)]
    local: bool,
    #[arg(long, required_if_eq("local", "true"))]
    binary: Option<PathBuf>,
    #[arg(long)]
    launchd: bool,
    #[arg(long)]
    home: PathBuf,
    #[arg(long, default_value_t = 28900)]
    port: u16,
}
#[derive(Clone, Deserialize)]
struct Spec {
    name: String,
    address: String,
    port: u16,
    binary: PathBuf,
    #[serde(default)]
    ssh: Vec<String>,
    #[serde(default)]
    qualifier: Option<PathBuf>,
}
struct Host {
    spec: Spec,
    root: PathBuf,
    started: BTreeSet<String>,
    children: BTreeMap<String, Child>,
    before: Option<Value>,
    launchd: bool,
    labels: BTreeMap<String, String>,
    generation: u64,
    configs: std::cell::RefCell<BTreeMap<String, String>>,
}
fn quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\"'\"'"))
}
impl Host {
    fn s(&self, path: &str) -> String {
        self.root.join(path).display().to_string()
    }
    fn argv(&self, binary: &Path, args: &[String]) -> (PathBuf, Vec<String>) {
        if self.spec.ssh.is_empty() {
            (binary.into(), args.to_vec())
        } else {
            let mut ssh = self.spec.ssh[1..].to_vec();
            let cmd = std::iter::once(binary.display().to_string())
                .chain(args.iter().cloned())
                .map(|s| quote(&s))
                .collect::<Vec<_>>()
                .join(" ");
            ssh.push(cmd);
            (self.spec.ssh[0].clone().into(), ssh)
        }
    }
    fn raw(
        &self,
        binary: &Path,
        args: &[String],
        input: Option<&[u8]>,
    ) -> Result<std::process::Output> {
        let (b, a) = self.argv(binary, args);
        output(&b, &a, input, 120)
    }
    fn cli(&self, args: &[&str]) -> Result<Value> {
        let args = args.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        let out = self.raw(&self.spec.binary, &args, None)?;
        ensure!(
            out.status.success(),
            "{} {args:?}: {} {}",
            self.spec.name,
            String::from_utf8_lossy(&out.stderr),
            String::from_utf8_lossy(&out.stdout)
        );
        if out.stdout.iter().all(u8::is_ascii_whitespace) {
            Ok(Value::Null)
        } else {
            Ok(serde_json::from_slice(&out.stdout)?)
        }
    }
    fn qualifier(&self) -> PathBuf {
        self.spec
            .qualifier
            .clone()
            .unwrap_or_else(|| self.spec.binary.parent().unwrap().join("fleet-qualify"))
    }
    fn helper(&self, mut r: Value) -> Result<Value> {
        r["root"] = json!(self.root);
        if self.spec.ssh.is_empty() {
            Ok(host::handle(&r)?.0)
        } else {
            let out = self.raw(
                &self.qualifier(),
                &["host".into()],
                Some(&serde_json::to_vec(&r)?),
            )?;
            ensure!(
                out.status.success(),
                "{} fixture: {}",
                self.spec.name,
                String::from_utf8_lossy(&out.stderr)
            );
            Ok(serde_json::from_slice(&out.stdout)?)
        }
    }
    fn write(&self, path: &str, value: &Value) -> Result<()> {
        self.bytes(path, &serde_json::to_vec_pretty(value)?)
    }
    fn bytes(&self, path: &str, data: &[u8]) -> Result<()> {
        self.helper(json!({"op":"write","path":path,"data":data}))?;
        Ok(())
    }
    fn read(&self, path: &str) -> Result<Value> {
        let bytes = self.helper(json!({"op":"read","path":path}))?;
        if bytes.is_null() {
            Ok(Value::Null)
        } else {
            Ok(serde_json::from_slice(&serde_json::from_value::<Vec<u8>>(
                bytes,
            )?)?)
        }
    }
    fn control(&self, args: &[&str], check: bool) -> Result<String> {
        let out = output(
            "/bin/launchctl".as_ref(),
            &args.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
            None,
            30,
        )?;
        ensure!(
            !check || out.status.success(),
            "launchctl {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        Ok(String::from_utf8(out.stdout)?)
    }
    fn pid(&self, name: &str) -> Result<Option<u32>> {
        let target = format!("gui/{}/{}", unsafe { libc::getuid() }, self.labels[name]);
        let text = self.control(&["print", &target], false)?;
        Ok(text.lines().find_map(|line| {
            line.trim()
                .strip_prefix("pid = ")
                .and_then(|s| s.parse().ok())
        }))
    }
    fn start(&mut self, name: &str) -> Result<()> {
        let args = vec![
            "service".into(),
            "run".into(),
            "--config".into(),
            self.configs
                .borrow()
                .get(name)
                .cloned()
                .unwrap_or_else(|| self.s(&format!("{name}.json"))),
        ];
        if self.launchd {
            ensure!(self.spec.ssh.is_empty(), "launchd fixture is local only");
            validate_service(&self.root, &args)?;
            self.generation += 1;
            let label = format!(
                "local.fleet.qualification.{}.{}.{}",
                std::process::id(),
                self.spec.name,
                self.generation
            );
            let definition = self.s(&format!("{name}-{}.plist", self.generation));
            let username = std::env::var("USER")?;
            let userhome = std::env::var("HOME")?;
            self.cli(&[
                "service",
                "render",
                "--kind",
                "launchd",
                "--name",
                &label,
                "--user",
                &username,
                "--user-home",
                &userhome,
                "--executable",
                self.spec.binary.to_str().unwrap(),
                "--config",
                &args[3],
                "--output",
                &definition,
            ])?;
            for args in [
                vec!["-remove".into(), "UserName".into(), definition.clone()],
                vec![
                    "-replace".into(),
                    "StandardOutPath".into(),
                    "-string".into(),
                    self.s(&format!("{name}.log")),
                    definition.clone(),
                ],
                vec![
                    "-replace".into(),
                    "StandardErrorPath".into(),
                    "-string".into(),
                    self.s(&format!("{name}.log")),
                    definition.clone(),
                ],
            ] {
                ensure!(
                    output("/usr/bin/plutil".as_ref(), &args, None, 10)?
                        .status
                        .success(),
                    "launchd fixture render adjustment failed"
                );
            }
            self.labels.insert(name.into(), label);
            self.started.insert(name.into());
            self.control(
                &[
                    "bootstrap",
                    &format!("gui/{}", unsafe { libc::getuid() }),
                    &definition,
                ],
                true,
            )?;
            return Ok(());
        }
        let command = std::iter::once(self.spec.binary.display().to_string())
            .chain(args)
            .collect::<Vec<_>>();
        let mut request = json!({"root":self.root,"op":"spawn","name":name,"command":command});
        if self.spec.ssh.is_empty() {
            let (_, child) = host::handle(&request)?;
            self.children.insert(name.into(), child.unwrap());
            self.started.insert(name.into());
            return Ok(());
        }
        request["hold_session"] = true.into();
        let (b, args) = self.argv(&self.qualifier(), &["host".into()]);
        let mut child = Command::new(b)
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?;
        let pipe = child.stdout.take().unwrap();
        let input = child.stdin.take().unwrap();
        // Own the SSH session before any fallible handshake work. On failure,
        // cleanup still checks the remote PID marker and stops the owned child.
        self.children.insert(name.into(), child);
        self.started.insert(name.into());
        let mut input = input;
        input.write_all(&serde_json::to_vec(&request)?)?;
        drop(input);
        let mut poll = libc::pollfd {
            fd: pipe.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        let ready = unsafe { libc::poll(&mut poll, 1, 15000) };
        if ready <= 0 {
            anyhow::bail!("SSH launch did not become ready");
        }
        let mut line = String::new();
        std::io::BufReader::new(pipe).read_line(&mut line)?;
        let _: Value = serde_json::from_str(&line)?;
        Ok(())
    }
    fn stop(&mut self, name: &str, kill: bool) -> Result<()> {
        if !self.started.contains(name) {
            return Ok(());
        }
        if self.launchd {
            let target = format!("gui/{}/{}", unsafe { libc::getuid() }, self.labels[name]);
            let old_pid = self.pid(name)?;
            let old = old_pid.map(host::identity).transpose()?.flatten();
            if kill {
                self.control(&["kill", "SIGKILL", &target], false)?;
            }
            self.control(&["bootout", &target], true)?;
            let end = Instant::now() + Duration::from_secs(15);
            while self.pid(name)?.is_some()
                || old_pid.is_some_and(|pid| host::identity(pid).ok().flatten().is_some())
            {
                ensure!(
                    Instant::now() < end,
                    "launchd failed to unload service {old:?}"
                );
                pause();
            }
        } else {
            let remote = self.helper(json!({"op":"stop","name":name,"kill":kill}));
            if let Some(child) = self.children.remove(name) {
                let mut p = Process { child };
                p.stop(false)?;
            }
            remote?;
        }
        self.started.remove(name);
        Ok(())
    }
    fn crash_restart(&self, name: &str) -> Result<()> {
        let old = self.pid(name)?.context("launchd PID missing")?;
        let target = format!("gui/{}/{}", unsafe { libc::getuid() }, self.labels[name]);
        self.control(&["kill", "SIGKILL", &target], true)?;
        wait(
            || Ok(json!(self.pid(name)?)),
            |p| p.as_u64().is_some_and(|p| p != old as u64),
            30,
        )?;
        Ok(())
    }
}
impl Drop for Host {
    fn drop(&mut self) {
        for name in self.started.clone() {
            if let Err(e) = self.stop(&name, false) {
                eprintln!("fixture service cleanup: {e:#}");
            }
        }
    }
}
struct Scenario {
    hosts: Vec<Host>,
    root: PathBuf,
    replicas: BTreeMap<u64, (usize, String, Value)>,
    jobs: Vec<(usize, String)>,
    phases: Vec<String>,
}
impl Scenario {
    fn normal(&self, args: &[&str]) -> Result<Value> {
        let home = self.root.join("client").display().to_string();
        let mut a = vec!["--home", &home];
        a.extend(args);
        self.hosts[0].cli(&a)
    }
    fn admin(&self, args: &[&str]) -> Result<Value> {
        let mut a = vec!["cluster"];
        a.extend(args);
        self.hosts[0].cli(&a)
    }
    fn state(&self) -> Result<Value> {
        self.admin(&[
            "read",
            "--key",
            &self.root.join("client/node.key").display().to_string(),
            "--config",
            &self.root.join("client/config.json").display().to_string(),
        ])
    }
    fn request(&self, r: Value) -> Result<Value> {
        api_result(&self.hosts[0].spec.binary, &self.root.join("client"), r)
    }
    fn configure(
        &self,
        h: usize,
        name: &str,
        controller: Option<&str>,
        worker: bool,
        bootstrap: bool,
    ) -> Result<()> {
        let h = &self.hosts[h];
        let cfg = h.s(&format!("{name}-{:016x}.json", rand::random::<u64>()));
        h.configs.borrow_mut().insert(name.into(), cfg.clone());
        let cp = controller.map(|s| h.s(s));
        let wh = h.s("worker");
        let mut args = vec!["service", "configure", "--config", &cfg];
        if let Some(cp) = &cp {
            args.extend(["--controller-home", cp]);
        }
        if worker {
            args.extend(["--worker-home", &wh]);
        }
        if bootstrap {
            args.push("--bootstrap");
        }
        h.cli(&args)?;
        Ok(())
    }
    fn start(&mut self, n: u64) -> Result<()> {
        let h = self.replicas[&n].0;
        self.hosts[h].start(&format!("node-{n}"))
    }
    fn stop(&mut self, n: u64, kill: bool) -> Result<()> {
        let h = self.replicas[&n].0;
        self.hosts[h].stop(&format!("node-{n}"), kill)
    }
    fn inspect(&self, n: u64) -> Result<Value> {
        let (h, dir, _) = &self.replicas[&n];
        let cfg = self.hosts[*h].read(&format!("{dir}/config.json"))?;
        self.hosts[0].write(&format!("inspect-{n}.json"), &cfg)?;
        self.admin(&[
            "inspect",
            "--key",
            &self.root.join("client/node.key").display().to_string(),
            "--config",
            &self
                .root
                .join(format!("inspect-{n}.json"))
                .display()
                .to_string(),
            "--replica",
            &n.to_string(),
        ])
    }
    fn caught(&self, ids: &[u64]) -> Result<()> {
        let barrier = self.state()?["last_applied"]["index"].as_u64().unwrap();
        wait(
            || {
                ids.iter()
                    .map(|i| self.inspect(*i))
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
            90,
        )?;
        Ok(())
    }
    fn prepare(&mut self, n: u64, h: usize, base: &Value, port: u16) -> Result<()> {
        let host = &self.hosts[h];
        host.write(&format!("base-{n}.json"), base)?;
        host.helper(json!({"op":"port","address":host.spec.address,"port":port+n as u16}))?;
        let peers = host.cli(&[
            "cluster",
            "prepare-replica",
            "--home",
            &host.s(&format!("node-{n}")),
            "--config",
            &host.s(&format!("base-{n}.json")),
            "--id",
            &n.to_string(),
            "--bind",
            &format!("{}:{}", host.spec.address, port + n as u16),
        ])?;
        self.replicas
            .insert(n, (h, format!("node-{n}"), peers[n.to_string()].clone()));
        self.hosts[0].write(&format!("peer-{n}.json"), &peers)?;
        self.configure(
            h,
            &format!("node-{n}"),
            Some(&format!("node-{n}")),
            false,
            false,
        )?;
        self.start(n)
    }
    fn membership(&self, name: &str, add: &[u64], remove: &[u64]) -> Result<()> {
        let mut args = vec!["replicas".into(), "--request-id".into(), name.into()];
        for i in add {
            args.extend([
                "--add".into(),
                self.root
                    .join(format!("peer-{i}.json"))
                    .display()
                    .to_string(),
            ]);
        }
        for i in remove {
            args.extend(["--remove".into(), i.to_string()]);
        }
        let a = self.normal(&args.iter().map(String::as_str).collect::<Vec<_>>())?;
        let op = a["reply"]["MembershipAccepted"]["operation"]
            .as_str()
            .context("membership acceptance")?;
        wait(
            || self.normal(&["operation", op]),
            |o| o["stage"] == "Complete",
            120,
        )?;
        ensure!(
            self.normal(&["retry", name])?["reply"] == a["reply"],
            "membership receipt changed"
        );
        Ok(())
    }
    fn submit(&mut self, h: usize, name: &str, duration: u64, after: &[&str]) -> Result<()> {
        self.hosts[0].bytes(
            &format!("{name}-input.txt"),
            format!("{name}-verified\n").as_bytes(),
        )?;
        let j = json!({"name":name,"argv":["/bin/sh","-c",format!("echo started-once; cat input.txt > result.txt; sleep {duration}")],"threads":1,"keep_awake":false,"max_seconds":duration+120,"shutdown_grace_seconds":0,"settle_seconds":0,"inputs":{"input.txt":self.root.join(format!("{name}-input.txt"))},"publish":["result.txt"]});
        self.request(json!({"op":"submit","id":name,"request_id":format!("submit-{name}"),"worker":self.hosts[h].spec.name,"memory_mb":128,"after":after,"job":j}))?;
        self.jobs.push((h, name.into()));
        Ok(())
    }
    fn phase_job(&self, name: &str, phase: &str) -> Result<Value> {
        wait(|| self.normal(&["job", name]), |j| j["phase"] == phase, 90)
    }
    fn supervisor(&self, h: usize, name: &str) -> Result<Value> {
        let v = self.hosts[h].read(&format!(
            "worker/backend/jobs/exec-{name}-a1/supervisor.json"
        ))?;
        ensure!(!v.is_null(), "supervisor missing");
        Ok(v)
    }
    fn fetch(&self, name: &str) -> Result<()> {
        let dest = self.root.join(format!("capture-{name}"));
        self.normal(&["fetch", name, dest.to_str().unwrap()])?;
        ensure!(
            fs::read_to_string(dest.join("result.txt"))? == format!("{name}-verified\n"),
            "output changed"
        );
        ensure!(
            self.normal(&["logs", name])?["Logs"]["data"] == "started-once\n",
            "execution duplicated"
        );
        Ok(())
    }
    fn phase(&mut self, name: &str) {
        self.phases.push(name.into());
        println!("{}", json!({"phase":name}));
    }
    fn cleanup(&mut self) -> Result<()> {
        let mut errors = vec![];
        for (h, name) in &self.jobs {
            let dir = format!("worker/backend/jobs/exec-{name}-a1");
            let r = (|| -> Result<()> {
                if !self.hosts[*h].read(&format!("{dir}/job.json"))?.is_null()
                    && self.hosts[*h].read(&format!("{dir}/exit.json"))?.is_null()
                {
                    self.hosts[*h].bytes(&format!("{dir}/cancel"), b"qualification cleanup\n")?;
                    wait(
                        || self.hosts[*h].read(&format!("{dir}/exit.json")),
                        |v| !v.is_null(),
                        30,
                    )?;
                }
                Ok(())
            })();
            if let Err(e) = r {
                errors.push(e.to_string());
            }
        }
        for h in &mut self.hosts {
            for name in h.started.clone() {
                if let Err(e) = h.stop(&name, false) {
                    errors.push(e.to_string());
                }
            }
            if let Some(before) = &h.before {
                match h.helper(json!({"op":"preservation"})) {
                    Ok(after) if after == *before => (),
                    Ok(_) => errors.push(format!("installed state changed on {}", h.spec.name)),
                    Err(e) => errors.push(e.to_string()),
                }
            }
        }
        ensure!(errors.is_empty(), "cleanup failed: {errors:?}");
        Ok(())
    }
}
pub fn run(a: Options) -> Result<()> {
    let root = fixture_root(&a.home)?;
    ensure!(!root.exists(), "fresh fixture home required");
    ensure!(!a.launchd || a.local, "launchd requires local mode");
    let specs: Vec<Spec> = if a.local {
        let binary = a
            .binary
            .context("local mode needs binary")?
            .canonicalize()?;
        (1..=3)
            .map(|i| Spec {
                name: format!("local-{i}"),
                address: "127.0.0.1".into(),
                port: a.port + 100 * (i - 1),
                binary: binary.clone(),
                ssh: vec![],
                qualifier: None,
            })
            .collect()
    } else {
        serde_json::from_value(
            read(a.inventory.as_ref().context("inventory required")?)?["hosts"].clone(),
        )?
    };
    ensure!(
        specs.len() == 3 && specs[0].ssh.is_empty(),
        "three hosts; first must be local"
    );
    if !a.local {
        ensure!(
            specs
                .iter()
                .map(|s| &s.address)
                .collect::<BTreeSet<_>>()
                .len()
                == 3,
            "physical mode requires different addresses"
        );
    }
    let hosts = specs
        .into_iter()
        .enumerate()
        .map(|(i, spec)| {
            let r = if a.local && i > 0 {
                root.join(&spec.name)
            } else {
                root.clone()
            };
            Host {
                spec,
                root: r,
                started: BTreeSet::new(),
                children: BTreeMap::new(),
                before: None,
                launchd: a.launchd,
                labels: BTreeMap::new(),
                generation: 0,
                configs: Default::default(),
            }
        })
        .collect();
    let mut s = Scenario {
        hosts,
        root: root.clone(),
        replicas: BTreeMap::new(),
        jobs: vec![],
        phases: vec![],
    };
    let mut hosts_report = vec![];
    let result = (|| -> Result<()> {
        for h in &mut s.hosts {
            let mut info = h.helper(json!({"op":"create"}))?;
            h.before = info.as_object_mut().unwrap().remove("preservation");
            if a.local {
                let copied = h.root.join("fleet");
                fs::copy(&h.spec.binary, &copied)?;
                h.spec.binary = copied;
            }
            info["name"] = h.spec.name.clone().into();
            info["binary_sha256"] = h.helper(json!({"op":"digest","path":h.spec.binary}))?;
            h.helper(json!({"op":"port","address":h.spec.address,"port":h.spec.port}))?;
            hosts_report.push(info);
        }
        if !a.local {
            ensure!(
                hosts_report
                    .iter()
                    .map(|h| h["hostname"].as_str().unwrap())
                    .collect::<BTreeSet<_>>()
                    .len()
                    == 3,
                "three physical hosts required"
            );
        }
        let mut args = vec![
            "init".into(),
            "--home".into(),
            root.join("seed").display().to_string(),
            "--replicas".into(),
            "3".into(),
        ];
        for h in &s.hosts {
            args.extend([
                "--address".into(),
                format!("{}:{}", h.spec.address, h.spec.port),
            ]);
        }
        let base = s.admin(&args.iter().map(String::as_str).collect::<Vec<_>>())?;
        s.normal(&[
            "client",
            "import",
            "--config",
            root.join("seed/client/config.json").to_str().unwrap(),
            "--key",
            root.join("seed/client/node.key").to_str().unwrap(),
        ])?;
        for i in 0..3 {
            let n = i as u64 + 1;
            let h = &s.hosts[i];
            for file in ["config.json", "node.key"] {
                h.bytes(
                    &format!("node-{n}/{file}"),
                    &fs::read(root.join(format!("seed/node-{n}/{file}")))?,
                )?;
            }
            s.replicas.insert(
                n,
                (i, format!("node-{n}"), base["peers"][n.to_string()].clone()),
            );
            if i < 2 {
                h.write("base.json", &base)?;
                h.cli(&[
                    "cluster",
                    "prepare-worker",
                    "--home",
                    &h.s("worker"),
                    "--config",
                    &h.s("base.json"),
                    "--id",
                    &h.spec.name,
                    "--cpu-slots",
                    "1",
                    "--memory-mb",
                    "256",
                    "--bind",
                    &format!("{}:{}", h.spec.address, h.spec.port + 20),
                ])?;
            }
            s.configure(
                i,
                &format!("node-{n}"),
                Some(&format!("node-{n}")),
                i < 2,
                i == 0,
            )?;
            s.start(n)?;
        }
        s.caught(&[1, 2, 3])?;
        for i in 0..2 {
            let h = &s.hosts[i];
            s.hosts[0].write(
                &format!("enroll-{}.json", h.spec.name),
                &h.read("worker/enroll.json")?,
            )?;
            s.normal(&[
                "enroll",
                root.join(format!("enroll-{}.json", h.spec.name))
                    .to_str()
                    .unwrap(),
                "--request-id",
                &format!("enroll-{}", h.spec.name),
            ])?;
        }
        ensure!(s.normal(&["status"])?["quorum"] == true, "quorum absent");
        s.phase("normal_combined_workers_and_controller_services");
        if a.launchd {
            for i in 0..3 {
                s.hosts[i].crash_restart(&format!("node-{}", i + 1))?;
                s.caught(&[1, 2, 3])?;
            }
            s.phase("launchd_automatically_restarts_crashed_replicas");
        }
        s.submit(1, "continuity", 1500, &[])?;
        s.phase_job("continuity", "running")?;
        let original = s.supervisor(1, "continuity")?;
        s.submit(0, "short", 1, &[])?;
        s.phase_job("short", "succeeded")?;
        s.fetch("short")?;
        s.request(json!({"op":"submit","id":"json-canary","request_id":"json-submit","worker":s.hosts[0].spec.name,"memory_mb":128,"job":{"name":"JSON qualification canary","argv":["/bin/sh","-c","echo json-once"],"max_seconds":20,"keep_awake":false,"settle_seconds":0}}))?;
        s.jobs.push((0, "json-canary".into()));
        s.phase_job("json-canary", "succeeded")?;
        ensure!(
            s.request(json!({"op":"logs","id":"json-canary"}))?["Logs"]["data"] == "json-once\n",
            "JSON job changed"
        );
        ensure!(
            s.normal(&["retry", "submit-short"])?["reply"]["Accepted"]["job"] == "short",
            "retry failed"
        );
        ensure!(
            s.normal(&["queue"])?
                .as_array()
                .unwrap()
                .iter()
                .filter(|j| j["id"] == "short")
                .count()
                == 1,
            "duplicate queue entry"
        );
        s.phase("normal_cli_json_queue_inputs_outputs_and_idempotent_retry");
        s.submit(1, "dependent", 1, &["continuity"])?;
        ensure!(
            s.normal(&["job", "dependent"])?["assignment"].is_null(),
            "dependent occupied capacity"
        );
        s.normal(&["cancel", "dependent", "--request-id", "cancel-dependent"])?;
        s.phase_job("dependent", "cancelled")?;
        let leader = s.inspect(1)?["leader"].as_u64().unwrap();
        s.stop(leader, true)?;
        ensure!(
            s.normal(&["status"])?["quorum"] == true && s.supervisor(1, "continuity")? == original,
            "leader loss interrupted work"
        );
        s.start(leader)?;
        s.caught(&[1, 2, 3])?;
        s.stop(2, true)?;
        s.start(2)?;
        s.phase_job("continuity", "running")?;
        ensure!(
            s.supervisor(1, "continuity")? == original,
            "combined restart duplicated work"
        );
        s.phase("combined_service_crash_preserves_supervisor_and_queue");
        for i in 0..2 {
            let n = i as u64 + 1;
            s.stop(n, false)?;
            s.configure(
                i,
                &format!("node-{n}"),
                Some(&format!("node-{n}")),
                false,
                i == 0,
            )?;
            s.configure(i, "worker-service", None, true, false)?;
            s.start(n)?;
            s.hosts[i].start("worker-service")?;
        }
        s.caught(&[1, 2, 3])?;
        s.prepare(4, 2, &base, a.port)?;
        s.membership("rotate-pi", &[4], &[3])?;
        s.stop(3, false)?;
        s.membership("shrink", &[], &[2, 4])?;
        s.stop(2, false)?;
        s.stop(4, false)?;
        let mut public = read(&root.join("client/config.json"))?;
        public["peers"] = json!({"1":s.replicas[&1].2});
        s.prepare(5, 1, &public, a.port)?;
        s.prepare(6, 2, &public, a.port)?;
        s.membership("regrow", &[5, 6], &[])?;
        s.caught(&[1, 5, 6])?;
        s.phase("normal_replica_rotation_three_to_one_to_three");
        s.stop(5, true)?;
        s.stop(6, true)?;
        let unknown = api(
            &s.hosts[0].spec.binary,
            &root.join("client"),
            json!({"op":"submit","id":"outage","request_id":"outage-submit","memory_mb":128,"requires":["never"],"job":{"name":"during outage","argv":["true"],"threads":1,"keep_awake":false}}),
        )?;
        ensure!(
            unknown["ok"] == false && unknown["outcome_unknown"] == true,
            "outage wrongly reported: {unknown}"
        );
        s.start(5)?;
        s.start(6)?;
        ensure!(
            s.normal(&["retry", "outage-submit"])?["reply"]["Accepted"]["job"] == "outage",
            "journal lost intent"
        );
        s.normal(&["cancel", "outage"])?;
        ensure!(
            s.supervisor(1, "continuity")? == original,
            "outage duplicated work"
        );
        s.phase("unknown_submission_resolved_from_durable_client_journal");
        s.caught(&[1, 5, 6])?;
        s.stop(1, false)?;
        s.submit(0, "forgotten", 600, &[])?;
        s.phase_job("forgotten", "running")?;
        let forgotten = s.supervisor(0, "forgotten")?;
        for i in 0..2 {
            s.hosts[i].stop("worker-service", false)?;
        }
        s.stop(5, true)?;
        s.stop(6, true)?;
        let manifest = s.admin(&[
            "recover",
            "--source-home",
            root.join("node-1").to_str().unwrap(),
            "--config",
            root.join("node-1/config.json").to_str().unwrap(),
            "--key",
            root.join("client/node.key").to_str().unwrap(),
            "--home",
            root.join("recovered").to_str().unwrap(),
            "--bind",
            &format!("{}:{}", s.hosts[0].spec.address, a.port + 15),
            "--accept-data-loss",
            "--old-controllers-fenced",
        ])?;
        s.replicas.insert(
            1,
            (
                0,
                "recovered".into(),
                manifest["plan"]["config"]["peers"]["1"].clone(),
            ),
        );
        s.configure(0, "node-1", Some("recovered"), false, true)?;
        s.start(1)?;
        s.normal(&[
            "accept-recovery",
            root.join("recovered/recovery.json").to_str().unwrap(),
        ])?;
        ensure!(
            !s.normal(&["queue"])?
                .as_array()
                .unwrap()
                .iter()
                .any(|j| j["id"] == "forgotten"),
            "survivor unexpectedly knows forgotten work"
        );
        let replay = api(
            &s.hosts[0].spec.binary,
            &root.join("client"),
            json!({"op":"retry","request_id":"submit-forgotten"}),
        )?;
        ensure!(
            replay.to_string().contains("RecoveryUnknown"),
            "forgotten intent was replayed: {replay}"
        );
        for i in 0..2 {
            let h = &s.hosts[i];
            h.write("recovery.json", &manifest)?;
            h.cli(&[
                "cluster",
                "rebind-worker",
                "--home",
                &h.s("worker"),
                "--manifest",
                &h.s("recovery.json"),
            ])?;
            s.hosts[i].start("worker-service")?;
        }
        wait(
            || s.state(),
            |v| {
                v["recovery"]["inventories"]
                    .as_object()
                    .is_some_and(|v| v.len() == 2)
            },
            90,
        )?;
        ensure!(
            s.normal(&["job", "forgotten"])?["assignment"]["execution"] == "exec-forgotten-a1",
            "inventory lost execution"
        );
        ensure!(
            s.supervisor(0, "forgotten")? == forgotten
                && s.supervisor(1, "continuity")? == original,
            "recovery duplicated work"
        );
        s.normal(&["resume-recovery", "--request-id", "resume"])?;
        for name in ["continuity", "forgotten"] {
            s.normal(&["cancel", name, "--request-id", &format!("cancel-{name}")])?;
            s.phase_job(name, "cancelled")?;
            s.fetch(name)?;
        }
        s.phase("normal_recovery_client_safeguard_inventory_visibility_and_cancellation");
        let config = read(&root.join("client/config.json"))?;
        s.prepare(7, 1, &config, a.port)?;
        s.prepare(8, 2, &config, a.port)?;
        s.membership("regrow-recovered", &[7, 8], &[])?;
        s.caught(&[1, 7, 8])?;
        for i in [1, 7, 8] {
            s.stop(i, true)?;
        }
        s.start(7)?;
        s.start(8)?;
        ensure!(
            s.normal(&["status"])?["quorum"] == true
                && s.normal(&["job", "forgotten"])?["phase"] == "cancelled",
            "cold recovered majority failed"
        );
        s.phase("recovered_queue_survives_majority_cold_restart");
        Ok(())
    })();
    let cleanup = s.cleanup();
    let report = json!({"success":result.is_ok()&&cleanup.is_ok(),"error":result.as_ref().err().map(|e|format!("{e:#}")),"cleanup_error":cleanup.as_ref().err().map(|e|format!("{e:#}")),"phases":s.phases,"hosts":hosts_report,"topology":if a.local{"local-three"}else{"physical-three"},"service_manager":if a.launchd{"user-launchd"}else{"fixture-processes"},"installed_state_unchanged":cleanup.is_ok(),"fixture_processes_stopped":s.hosts.iter().all(|h|h.started.is_empty()),"limits":["process crashes; no physical power-off","tiny native canaries; no ML training or host networking changes"]});
    if root.exists() {
        save(&root.join("qualification.json"), &report)?;
    }
    println!("{report}");
    result?;
    cleanup
}
