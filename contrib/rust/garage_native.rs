//! Native Garage feasibility check. Loopback-only fixture, never a deployment.
use super::support::*;
use anyhow::{Context, Result, ensure};
use clap::Args;
use serde_json::json;
use std::{
    fs,
    net::{TcpListener, UdpSocket},
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

#[derive(Args)]
pub struct Options {
    #[arg(long)]
    pub binary: PathBuf,
    #[arg(long)]
    pub garage: PathBuf,
    #[arg(long)]
    pub home: PathBuf,
}
const KEY: &str = "GK010101010101010101010101";
const SECRET: &str = "0202020202020202020202020202020202020202020202020202020202020202";
fn command(l: &Lab, args: &[&str]) -> Result<String> {
    let mut call = vec!["-c".into(), l.s("garage.toml")];
    call.extend(args.iter().map(|s| s.to_string()));
    let result = output(&l.root.join("garage"), &call, None, 30)?;
    ensure!(
        result.status.success(),
        "Garage command failed: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    Ok(String::from_utf8(result.stdout)?.trim().into())
}
fn start(l: &mut Lab) -> Result<()> {
    let log = fs::File::create(l.root.join("garage.log"))?;
    let child = Command::new(l.root.join("garage"))
        .args(["-c", &l.s("garage.toml"), "server"])
        .stdin(Stdio::null())
        .stdout(log.try_clone()?)
        .stderr(log)
        .spawn()?;
    l.processes.insert("garage".into(), Process { child });
    wait(
        || {
            Ok(command(l, &["status"])
                .ok()
                .map(Into::into)
                .unwrap_or(serde_json::Value::Null))
        },
        |v| v.is_string(),
        60,
    )?;
    Ok(())
}
fn curl(l: &Lab, url: &str, args: &[&str]) -> Result<Vec<u8>> {
    let mut call = vec![
        "--fail".into(),
        "--silent".into(),
        "--show-error".into(),
        "--max-time".into(),
        "30".into(),
        "--aws-sigv4".into(),
        "aws:amz:garage:s3".into(),
        "--user".into(),
        format!("{KEY}:{SECRET}"),
        url.into(),
    ];
    call.extend(args.iter().map(|s| s.to_string()));
    let result = output(Path::new("/usr/bin/curl"), &call, None, 40)?;
    ensure!(
        result.status.success(),
        "S3 request failed: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    let _ = l;
    Ok(result.stdout)
}
pub fn run(options: Options) -> Result<()> {
    let mut l = Lab::new(&options.home, &options.binary)?;
    fs::copy(options.garage.canonicalize()?, l.root.join("garage"))?;
    let version = output(&l.root.join("garage"), &["--version".into()], None, 10)?;
    ensure!(version.status.success(), "Garage executable failed");
    let port =
        || -> Result<String> { Ok(TcpListener::bind("127.0.0.1:0")?.local_addr()?.to_string()) };
    let rpc = port()?;
    let s3 = port()?;
    let listen = port()?;
    fs::write(
        l.root.join("garage.toml"),
        format!(
            "metadata_dir={:?}\ndata_dir={:?}\ndb_engine=\"sqlite\"\nreplication_factor=1\nmetadata_fsync=true\ndata_fsync=true\nrpc_bind_addr={rpc:?}\nrpc_public_addr={rpc:?}\nrpc_secret={:?}\n[s3_api]\ns3_region=\"garage\"\napi_bind_addr={s3:?}\n",
            l.s("meta"),
            l.s("data"),
            "01".repeat(32)
        ),
    )?;
    start(&mut l)?;
    let node = command(&l, &["node", "id"])?;
    command(
        &l,
        &[
            "layout",
            "assign",
            "-z",
            "local-feasibility",
            "-c",
            "256M",
            node.split('@').next().context("node ID")?,
        ],
    )?;
    command(&l, &["layout", "apply", "--version", "1"])?;
    command(&l, &["bucket", "create", "fleet-qualification"])?;
    command(
        &l,
        &["key", "import", KEY, SECRET, "--yes", "-n", "qualification"],
    )?;
    command(
        &l,
        &[
            "bucket",
            "allow",
            "--read",
            "--write",
            "fleet-qualification",
            "--key",
            "qualification",
        ],
    )?;
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
    super::rolling::activate_to(
        &l,
        fleet::control::tunnels::GENERATION.max(fleet::control::version::MIN),
    )?;
    let mut topology = json!({"adapters":{},"services":{"s3":{"owner":"storage","target":s3,"allowed":["client"]}},"routes":[{"adapter":"client","service":"s3","listen":listen}]});
    for id in ["storage", "client"] {
        save(
            &l.root.join(format!("{id}-network.json")),
            &json!({"bind":UdpSocket::bind("127.0.0.1:0")?.local_addr()?,"relays":[]}),
        )?;
        topology["adapters"][id] = l.cli(&[
            "tunnels",
            "prepare",
            "--adapter-home",
            &l.s(&format!("adapter-{id}")),
            "--adapter",
            id,
            "--authority",
            &l.s("client/config.json"),
            "--network",
            &l.s(&format!("{id}-network.json")),
        ])?;
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
    l.request(json!({"op":"replace_tunnels","request_id":"routes","expected_revision":0,"topology":topology}))?;
    ensure!(
        l.request(json!({"op":"wait_tunnels","revision":1,"timeout_seconds":30}))?["confirmed"]
            == true,
        "tunnels not applied"
    );
    let data: Vec<u8> = (0..8 * 1024 * 1024).map(|n| (n % 251) as u8).collect();
    fs::write(l.root.join("payload"), &data)?;
    let url = format!("http://{listen}/fleet-qualification/payload");
    curl(&l, &url, &["--upload-file", &l.s("payload")])?;
    ensure!(curl(&l, &url, &[])? == data, "S3 bytes differ");
    l.phase("native_garage_signed_s3_roundtrip_over_iroh");
    l.stop("garage", false)?;
    start(&mut l)?;
    ensure!(
        curl(&l, &url, &[])? == data,
        "S3 bytes changed after restart"
    );
    l.phase("native_garage_sqlite_and_object_persistence_after_restart");
    l.report(json!({"success":true,"garage":String::from_utf8(version.stdout)?.trim(),"os":std::env::consts::OS,"arch":std::env::consts::ARCH,"bytes":data.len(),"payload_sha256":digest(&l.root.join("payload"))?,"durability_scope":"clean process restart only; no power-loss or multi-host claim"}))
}
