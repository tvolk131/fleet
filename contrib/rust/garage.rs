//! Stock Garage qualification in distinct network namespaces. No host services or
//! host state mounts are used; every Docker resource is owned by this invocation.
use super::support::*;
use anyhow::{Context, Result, ensure};
use clap::Args;
use serde_json::{Value, json};
use std::{
    fs,
    path::{Path, PathBuf},
    time::Instant,
};

const GARAGE: &str =
    "dxflrs/garage@sha256:9c96caa2612d3411acc5b0e6701fb238dbfba33e533a6d7d3d811a4b12d0d020";
const RUNTIME: &str =
    "ubuntu@sha256:68ee22004a9109b44973313b02ff7b1de250f1de0f5ecb628d73f579aaff33ac";
const KEY: &str = "GK010101010101010101010101";
const SECRET: &str = "0202020202020202020202020202020202020202020202020202020202020202";
const CURL: &str =
    "curlimages/curl@sha256:d43bdb28bae0be0998f3be83199bfb2b81e0a30b034b6d7586ce7e05de34c3fd";

fn wait(
    mut call: impl FnMut() -> Result<Value>,
    predicate: impl Fn(&Value) -> bool,
    seconds: u64,
) -> Result<Value> {
    let start = Instant::now();
    loop {
        let value = call();
        if let Ok(ref v) = value
            && predicate(v)
        {
            return value;
        }
        ensure!(
            start.elapsed().as_secs() < seconds,
            "qualification convergence timed out: {value:?}"
        );
        std::thread::sleep(std::time::Duration::from_millis(200));
    }
}

#[derive(Args)]
pub struct Options {
    #[arg(long)]
    pub home: PathBuf,
    /// Linux Fleet binary for the Docker daemon's architecture.
    #[arg(long)]
    pub linux_binary: PathBuf,
}
struct Lab {
    home: PathBuf,
    network: String,
    containers: Vec<String>,
}
impl Lab {
    fn collect_logs(&self) {
        for (n, container) in self.containers.iter().enumerate() {
            for role in ["control", "garage", "adapter"] {
                let _ = self.docker(&[
                    "cp",
                    &format!("{container}:/state/{role}.log"),
                    self.home
                        .join(format!("node-{n}-{role}.log"))
                        .to_str()
                        .unwrap(),
                ]);
            }
        }
    }
    fn cleanup(&mut self) -> Result<()> {
        while let Some(container) = self.containers.last() {
            self.docker(&["rm", "-f", container])?;
            self.containers.pop();
        }
        if !self.network.is_empty() {
            self.docker(&["network", "rm", &self.network])?;
            self.network.clear();
        }
        Ok(())
    }
    fn docker(&self, args: &[&str]) -> Result<String> {
        let out = output(
            Path::new("docker"),
            &args.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
            None,
            120,
        )?;
        ensure!(
            out.status.success(),
            "isolated docker command {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        Ok(String::from_utf8(out.stdout)?.trim().into())
    }
    fn exec(&self, node: usize, args: &[&str]) -> Result<String> {
        let mut call = vec!["exec", self.containers[node].as_str()];
        call.extend(args);
        self.docker(&call)
    }
    fn copy(&self, node: usize, local: &Path, destination: &str) -> Result<()> {
        self.docker(&[
            "cp",
            local.to_str().context("path")?,
            &format!("{}:{destination}", self.containers[node]),
        ])?;
        Ok(())
    }
    fn file(&self, node: usize, name: &str, contents: &str) -> Result<()> {
        let local = self.home.join(format!("node-{node}-{name}"));
        fs::write(&local, contents)?;
        self.copy(node, &local, &format!("/state/{name}"))
    }
    fn json_file(&self, node: usize, name: &str, value: Value) -> Result<()> {
        self.file(node, name, &serde_json::to_string(&value)?)
    }
    fn spawn(&self, node: usize, role: &str, command: &str) -> Result<()> {
        ensure!(
            ["garage", "adapter", "control"].contains(&role),
            "invalid fixture role"
        );
        let script =
            format!("echo $$ > /state/{role}.pid; exec {command} >>/state/{role}.log 2>&1");
        self.docker(&["exec", "-d", &self.containers[node], "sh", "-c", &script])?;
        Ok(())
    }
    fn stop(&self, node: usize, role: &str, signal: &str) -> Result<()> {
        ensure!(
            ["garage", "adapter", "control"].contains(&role) && ["TERM", "KILL"].contains(&signal),
            "invalid fixture process"
        );
        self.exec(
            node,
            &[
                "sh",
                "-c",
                &format!("kill -{signal} \"$(cat /state/{role}.pid)\""),
            ],
        )?;
        Ok(())
    }
    fn api(&self, value: Value) -> Result<Value> {
        let out = output(
            Path::new("docker"),
            &[
                "exec".into(),
                "-i".into(),
                self.containers[0].clone(),
                "/fleet".into(),
                "--home".into(),
                "/state/seed/client".into(),
                "api".into(),
            ],
            Some(&serde_json::to_vec(&value)?),
            30,
        )?;
        ensure!(
            out.status.success(),
            "fixture CLI failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let envelope: Value = serde_json::from_slice(&out.stdout)?;
        ensure!(envelope["ok"] == true, "fixture API: {envelope}");
        let result = envelope["result"].clone();
        ensure!(
            result["reply"].get("Rejected").is_none(),
            "fixture mutation rejected: {result}"
        );
        Ok(result)
    }
    fn fleet(&self, node: usize, args: &[&str]) -> Result<Value> {
        let mut call = vec!["/fleet"];
        call.extend(args);
        Ok(serde_json::from_str(&self.exec(node, &call)?)?)
    }
    fn healthy(&self, ids: &[String]) -> Result<()> {
        wait(
            || {
                let status = self.exec(0, &["/garage", "status"])?;
                Ok(json!(status))
            },
            |status| {
                ids.iter()
                    .all(|id| status.as_str().unwrap().contains(&id[..16]))
                    && !status.as_str().unwrap().contains("UNAVAILABLE")
            },
            60,
        )?;
        Ok(())
    }
    fn s3(&self, node: usize, upload: bool, object: &str, file: &Path) -> Result<f64> {
        // The client joins only the fixture's internal bridge; no host ports or
        // external routes are needed, and no fixture credentials leave this lab.
        let name = format!("{}-curl", self.network);
        let mut args: Vec<String> = vec![
            "run",
            "--rm",
            "-i",
            "--name",
            &name,
            "--network",
            &self.network,
            "--memory",
            "64m",
            "--cpus",
            "1",
            CURL,
        ]
        .into_iter()
        .map(str::to_owned)
        .collect();
        args.extend([
            "--fail".into(),
            "--silent".into(),
            "--show-error".into(),
            "--max-time".into(),
            "30".into(),
            "--aws-sigv4".into(),
            "aws:amz:garage:s3".into(),
            "--user".into(),
            format!("{KEY}:{SECRET}"),
        ]);
        if upload {
            args.extend(["--upload-file".into(), "-".into()]);
        }
        args.push(format!(
            "http://{}:3900/fleet-qualification/{object}",
            self.containers[node]
        ));
        let input = if upload { Some(fs::read(file)?) } else { None };
        let start = Instant::now();
        let result = output(Path::new("docker"), &args, input.as_deref(), 35);
        let _ = self.docker(&["rm", "-f", &name]);
        let result = result?;
        ensure!(
            result.status.success(),
            "S3 fixture transfer: {}",
            String::from_utf8_lossy(&result.stderr)
        );
        if !upload {
            fs::write(file, &result.stdout)?;
        }
        Ok(start.elapsed().as_secs_f64())
    }
}
impl Drop for Lab {
    fn drop(&mut self) {
        self.collect_logs();
        let _ = self.cleanup();
    }
}

pub fn run(options: Options) -> Result<()> {
    let home = fixture_root(&options.home)?;
    ensure!(!home.exists(), "qualification home must be new");
    fs::create_dir(&home)?;
    ensure!(
        options.linux_binary.is_absolute() && options.linux_binary.is_file(),
        "explicit Linux Fleet binary required"
    );
    let token = format!("fleet-tunnel-{:016x}", rand::random::<u64>());
    let mut lab = Lab {
        home: home.clone(),
        network: token.clone(),
        containers: vec![],
    };
    // Pin both upstream payload and namespace image. No public service ports.
    lab.docker(&["pull", GARAGE])?;
    lab.docker(&["pull", RUNTIME])?;
    lab.docker(&["pull", CURL])?;
    lab.docker(&[
        "network",
        "create",
        "--internal",
        "--label",
        "fleet.qualification=service-tunnels",
        &token,
    ])?;
    let source = format!("{token}-garage-source");
    lab.docker(&["create", "--name", &source, GARAGE])?;
    lab.containers.push(source.clone());
    lab.docker(&[
        "cp",
        &format!("{source}:/garage"),
        home.join("garage").to_str().unwrap(),
    ])?;
    lab.docker(&["rm", &source])?;
    lab.containers.clear();
    let mut ips = Vec::new();
    for n in 0..3 {
        let name = format!("{token}-{n}");
        lab.docker(&[
            "run",
            "-d",
            "--name",
            &name,
            "--label",
            "fleet.qualification=service-tunnels",
            "--network",
            &token,
            "--memory",
            "384m",
            "--cpus",
            "1",
            RUNTIME,
            "sleep",
            "3600",
        ])?;
        lab.containers.push(name.clone());
        let inspected: Value = serde_json::from_str(&lab.docker(&["inspect", &name])?)?;
        ips.push(
            inspected[0]["NetworkSettings"]["Networks"][&token]["IPAddress"]
                .as_str()
                .context("container address")?
                .to_owned(),
        );
        lab.exec(n, &["mkdir", "-p", "/state/meta", "/state/data"])?;
        lab.copy(n, &options.linux_binary, "/fleet")?;
        lab.copy(n, &home.join("garage"), "/garage")?;
    }
    lab.fleet(
        0,
        &[
            "cluster",
            "init",
            "--home",
            "/state/seed",
            "--replicas",
            "1",
            "--address",
            &format!("{}:40000", ips[0]),
        ],
    )?;
    let authority = lab.exec(0, &["cat", "/state/seed/client/config.json"])?;
    lab.fleet(
        0,
        &[
            "service",
            "configure",
            "--config",
            "/state/control.json",
            "--controller-home",
            "/state/seed/node-1",
            "--bootstrap",
        ],
    )?;
    lab.spawn(
        0,
        "control",
        "/fleet service run --config /state/control.json",
    )?;
    wait(
        || lab.api(json!({"op":"versions"})),
        |v| v["version"]["active"] == fleet::control::version::MIN,
        60,
    )?;
    lab.api(json!({"op":"prepare_version","request_id":"prepare","expected_revision":0,"target":fleet::control::tunnels::GENERATION}))?;
    wait(
        || lab.api(json!({"op":"versions"})),
        |v| v["version"]["pending"]["ready"] == json!([1]),
        60,
    )?;
    lab.api(json!({"op":"activate_version","request_id":"activate","expected_revision":1}))?;
    let mut topology = json!({"adapters":{},"services":{},"routes":[]});
    for (n, ip) in ips.iter().enumerate() {
        let id = format!("node-{n}");
        lab.file(n, "authority.json", &authority)?;
        lab.json_file(
            n,
            "network.json",
            json!({"bind":format!("{ip}:41000"),"relays":[]}),
        )?;
        topology["adapters"][&id] = lab.fleet(
            n,
            &[
                "tunnels",
                "prepare",
                "--adapter-home",
                "/state/adapter",
                "--adapter",
                &id,
                "--authority",
                "/state/authority.json",
                "--network",
                "/state/network.json",
            ],
        )?;
        topology["services"][&id] = json!({"owner":id,"target":format!("127.0.0.1:{}",31001+n),"allowed":(0..3).filter(|i|*i!=n).map(|i|format!("node-{i}")).collect::<Vec<_>>()});
        for remote in 0..3 {
            if remote != n {
                topology["routes"].as_array_mut().unwrap().push(json!({"adapter":id,"service":format!("node-{remote}"),"listen":format!("127.0.0.1:{}",31001+remote)}));
            }
        }
        let conf = format!(
            "metadata_dir=\"/state/meta\"\ndata_dir=\"/state/data\"\ndb_engine=\"sqlite\"\nreplication_factor=3\nmetadata_fsync=true\ndata_fsync=true\nrpc_bind_addr=\"127.0.0.1:{}\"\nrpc_public_addr=\"127.0.0.1:{}\"\nrpc_secret=\"{}\"\n[s3_api]\ns3_region=\"garage\"\napi_bind_addr=\"0.0.0.0:3900\"\n",
            31001 + n,
            31001 + n,
            "01".repeat(32)
        );
        lab.file(n, "garage.toml", &conf)?;
        lab.exec(n, &["cp", "/state/garage.toml", "/etc/garage.toml"])?;
        lab.fleet(
            n,
            &[
                "service",
                "configure",
                "--config",
                "/state/adapter-service.json",
                "--tunnel-home",
                "/state/adapter",
            ],
        )?;
        lab.spawn(n, "garage", "/garage server")?;
    }
    lab.api(json!({"op":"replace_tunnels","request_id":"routes","expected_revision":0,"topology":topology}))?;
    for n in 0..3 {
        lab.spawn(
            n,
            "adapter",
            "/fleet service run --config /state/adapter-service.json",
        )?;
    }
    let ids = (0..3)
        .map(|n| lab.exec(n, &["/garage", "node", "id"]))
        .collect::<Result<Vec<_>>>()?;
    // Seed only a chain; discovery must find the third peer via its advertised alias.
    for n in 0..2 {
        wait(
            || {
                lab.exec(n, &["/garage", "node", "connect", &ids[n + 1]])
                    .map(Value::from)
            },
            |_| true,
            60,
        )?;
    }
    lab.healthy(&ids)?;
    for (n, id) in ids.iter().enumerate() {
        lab.exec(
            0,
            &[
                "/garage",
                "layout",
                "assign",
                "-z",
                &format!("zone-{n}"),
                "-c",
                "256M",
                id.split('@').next().unwrap(),
            ],
        )?;
    }
    lab.exec(0, &["/garage", "layout", "apply", "--version", "1"])?;
    lab.exec(0, &["/garage", "bucket", "create", "fleet-qualification"])?;
    lab.exec(
        0,
        &[
            "/garage",
            "key",
            "import",
            KEY,
            SECRET,
            "--yes",
            "-n",
            "qualification",
        ],
    )?;
    lab.exec(
        0,
        &[
            "/garage",
            "bucket",
            "allow",
            "--read",
            "--write",
            "--owner",
            "fleet-qualification",
            "--key",
            "qualification",
        ],
    )?;
    // No Garage socket is bound to an inter-container address. /dev/tcp checks
    // each directed pair and must fail; a successful direct connection aborts.
    for n in 0..3 {
        for (remote, ip) in ips.iter().enumerate() {
            if remote != n {
                let positive = output(
                    Path::new("docker"),
                    &[
                        "exec".into(),
                        lab.containers[n].clone(),
                        "bash".into(),
                        "-c".into(),
                        format!("timeout 2 bash -c 'echo >/dev/tcp/{ip}/3900'"),
                    ],
                    None,
                    5,
                )?;
                ensure!(
                    positive.status.success(),
                    "direct-RPC probe cannot reach the peer's S3 control socket"
                );
                let out = output(
                    Path::new("docker"),
                    &[
                        "exec".into(),
                        lab.containers[n].clone(),
                        "bash".into(),
                        "-c".into(),
                        format!("timeout 2 bash -c 'echo >/dev/tcp/{ip}/{}'", 31001 + remote),
                    ],
                    None,
                    5,
                )?;
                ensure!(
                    !out.status.success(),
                    "direct Garage RPC bypassed tunnel isolation"
                );
            }
        }
    }
    let payload = home.join("payload.bin");
    let bytes: Vec<u8> = rand::random_iter::<u8>().take(8 << 20).collect();
    fs::write(&payload, &bytes)?;
    let put = lab.s3(0, true, "payload", &payload)?;
    let downloaded = home.join("download.bin");
    let get = lab.s3(2, false, "payload", &downloaded)?;
    ensure!(fs::read(&downloaded)? == bytes, "S3 bytes differ");
    // Restart Garage using only its persisted peer list, then restart an adapter
    // while the controller is stopped. All data must continue to use iroh.
    lab.stop(2, "garage", "KILL")?;
    lab.spawn(2, "garage", "/garage server")?;
    lab.healthy(&ids)?;
    wait(
        || lab.s3(2, false, "payload", &downloaded).map(|s| json!(s)),
        |_| true,
        60,
    )?;
    ensure!(
        fs::read(&downloaded)? == bytes,
        "bytes differ after Garage restart"
    );
    lab.stop(0, "control", "TERM")?;
    lab.stop(2, "adapter", "TERM")?;
    wait(
        || {
            lab.exec(
                2,
                &[
                    "sh",
                    "-c",
                    "! kill -0 \"$(cat /state/adapter.pid)\" 2>/dev/null",
                ],
            )
            .map(Value::from)
        },
        |_| true,
        30,
    )?;
    lab.spawn(
        2,
        "adapter",
        "/fleet service run --config /state/adapter-service.json",
    )?;
    wait(
        || {
            lab.s3(0, true, "during-controller-outage", &payload)
                .map(|v| json!(v))
        },
        |_| true,
        60,
    )?;
    lab.s3(2, false, "during-controller-outage", &downloaded)?;
    ensure!(
        fs::read(&downloaded)? == bytes,
        "bytes differ during controller outage"
    );
    lab.spawn(
        0,
        "control",
        "/fleet service run --config /state/control.json",
    )?;
    wait(
        || lab.api(json!({"op":"tunnels"})),
        |v| v["revision"] == 1,
        60,
    )?;
    // Removing all routes must actually terminate replication, not only change
    // a database field. A fresh PUT cannot obtain Garage's two-of-three quorum.
    lab.api(json!({"op":"replace_tunnels","request_id":"revoke","expected_revision":1,"topology":{"adapters":{},"services":{},"routes":[]}}))?;
    for n in 0..3 {
        wait(
            || {
                lab.exec(n, &["cat", "/state/adapter/policy.json"])
                    .and_then(|s| Ok(serde_json::from_str(&s)?))
            },
            |v| v["revision"] == 2,
            30,
        )?;
    }
    ensure!(
        lab.s3(0, true, "must-fail-after-revocation", &payload)
            .is_err(),
        "Garage still replicated after all service access was revoked"
    );
    let report = json!({"success":true,"garage_image":GARAGE,"runtime_image":RUNTIME,"fleet_sha256":digest(&options.linux_binary)?,"distinct_network_namespaces":3,"direct_rpc_blocked_pairs":6,"gossip_discovery":true,"garage_restart":true,"adapter_restart_without_controller":true,"revocation_breaks_quorum":true,"bytes":bytes.len(),"sha256":digest(&payload)?,"put_seconds":put,"get_seconds":get,"physical_failure_domains":1,"transport":"direct iroh; relay-only transport covered by Rust integration test"});
    lab.collect_logs();
    lab.cleanup()?;
    save(&home.join("result.json"), &report)?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}
