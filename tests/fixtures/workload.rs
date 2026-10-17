//! Small real processes for lifecycle tests and qualification. No production commands live here.
use anyhow::{Result, bail, ensure};
use sha2::{Digest, Sha256};
use std::{
    fs,
    io::Write,
    path::Path,
    sync::atomic::{AtomicUsize, Ordering},
    time::{Duration, Instant},
};
static TERMS: AtomicUsize = AtomicUsize::new(0);
extern "C" fn term(_: i32) {
    TERMS.fetch_add(1, Ordering::Relaxed);
}
fn sleep(ms: u64) {
    std::thread::sleep(Duration::from_millis(ms));
}
fn checkpoint(root: &str, n: u64, size: usize) -> Result<()> {
    let path = Path::new(root).join(format!("{n:03}"));
    fs::create_dir_all(&path)?;
    let mut data = if size == 1 {
        n.to_string()
    } else {
        format!("{n}\n")
    }
    .into_bytes();
    if size > data.len() {
        data.resize(size, b'x');
    }
    fs::write(path.join("state"), &data)?;
    let manifest = serde_json::json!({"version":1,"position":n,"files":{"state":{"size":data.len(),"sha256":format!("{:x}",Sha256::digest(&data))}}});
    fs::write(path.join("ready.tmp"), manifest.to_string())?;
    fs::rename(path.join("ready.tmp"), path.join("fleet.json"))?;
    Ok(())
}
fn metrics() -> Result<()> {
    let mut f = fs::File::create("metrics.jsonl")?;
    for i in 0..2000 {
        writeln!(
            f,
            "{}",
            serde_json::json!({"step":i,"loss":1.0/(i+1) as f64})
        )?;
    }
    Ok(())
}
fn main() {
    if let Err(e) = run() {
        eprintln!("{e:#}");
        std::process::exit(1);
    }
}
fn run() -> Result<()> {
    // This executable also runs as a durable guest input. In a real VM the OS
    // terminates it at shutdown; the process-level Tart double needs the same
    // lifetime even when its command-channel parent has already disappeared.
    if let Some(path) = std::env::var_os("FLEET_TEST_VM_LIFETIME") {
        let path = std::path::PathBuf::from(path);
        let generation = fs::read(&path)?;
        fs::write(
            path.with_file_name(format!("guest-{}.ready", std::process::id())),
            b"ready",
        )?;
        std::thread::spawn(move || {
            loop {
                if fs::read(&path).ok().as_ref() != Some(&generation) {
                    std::process::exit(0);
                }
                sleep(20);
            }
        });
    }
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mode = args.first().map(String::as_str).unwrap_or("checkpoint");
    match mode {
        "graceful" | "stubborn" | "descendant" => {
            if mode == "descendant" {
                let pid = unsafe { libc::fork() };
                ensure!(pid >= 0, "fork failed");
                if pid == 0 {
                    unsafe {
                        libc::signal(libc::SIGTERM, libc::SIG_IGN);
                    }
                    fs::write("descendant-ready", "")?;
                    let at = Instant::now();
                    loop {
                        fs::write("heartbeat", at.elapsed().as_nanos().to_string())?;
                        sleep(30);
                    }
                }
                while !Path::new("descendant-ready").exists() {
                    sleep(10);
                }
            }
            unsafe {
                libc::signal(
                    libc::SIGTERM,
                    if mode == "stubborn" {
                        libc::SIG_IGN
                    } else {
                        term as *const () as libc::sighandler_t
                    },
                );
            }
            fs::write("ready", "")?;
            let mut seen = 0;
            let mut end = None;
            loop {
                let count = TERMS.load(Ordering::Relaxed);
                if count > seen {
                    if mode == "descendant" {
                        return Ok(());
                    }
                    for _ in seen..count {
                        writeln!(
                            fs::OpenOptions::new()
                                .create(true)
                                .append(true)
                                .open("terms")?,
                            "term"
                        )?;
                    }
                    seen = count;
                    end.get_or_insert(Instant::now() + Duration::from_millis(400));
                }
                if end.is_some_and(|e| Instant::now() >= e) {
                    break;
                }
                sleep(10);
            }
            fs::write("flushed", "")?;
        }
        "checkpoint" | "rolling" | "portable" => {
            if mode == "portable" {
                ensure!(
                    fs::read_to_string("dataset")? == "durable original data",
                    "input changed"
                );
            }
            let history = args.iter().any(|a| a == "history");
            if history {
                metrics()?;
                std::io::stdout().write_all(&vec![b'x'; 9 << 20])?;
                println!();
                eprintln!("diagnostic-start");
            }
            let count = if mode == "rolling" {
                args.get(1).map(|s| s.parse()).transpose()?.unwrap_or(28)
            } else {
                1
            };
            let root = args
                .iter()
                .find_map(|s| s.strip_prefix("root="))
                .unwrap_or("cp");
            for n in 1..=count {
                checkpoint(
                    root,
                    if mode == "rolling" { n } else { 7 },
                    if mode == "rolling" { 65536 } else { 1 },
                )?;
            }
            println!("initial-once");
            std::io::stdout().flush()?;
            sleep(500_000);
        }
        "corrupt-checkpoints" => {
            for (name, contents, declared) in [
                ("a-bad", b"bad".as_slice(), b"yes".as_slice()),
                ("b-partial", b"".as_slice(), b"complete".as_slice()),
                ("c-good", b"9".as_slice(), b"9".as_slice()),
            ] {
                let dir = Path::new("cp").join(name);
                fs::create_dir_all(&dir)?;
                fs::write(dir.join("state"), contents)?;
                fs::write(dir.join("fleet.json"),serde_json::json!({"version":1,"position":9,"files":{"state":{"size":declared.len(),"sha256":format!("{:x}",Sha256::digest(declared))}}}).to_string())?;
            }
            println!("still-running");
            std::io::stdout().flush()?;
            sleep(150_000);
        }
        "resume" => {
            if args.iter().any(|s| s == "immutable") {
                use std::os::unix::fs::PermissionsExt;
                ensure!(
                    fs::metadata("trainer")?.permissions().mode() & 0o111 != 0,
                    "trainer lost executable permission"
                );
                ensure!(
                    fs::read("dataset")? == b"immutable training data",
                    "dataset changed"
                );
            }
            let n = fs::read_to_string(&args[1])?
                .lines()
                .next()
                .unwrap()
                .parse::<u64>()?;
            fs::write("result.txt", (n + 1).to_string())?;
            if args.iter().any(|s| s == "history") {
                metrics()?;
                eprintln!("diagnostic-resume");
            }
            println!("resumed-once");
        }
        "observe" => {
            fs::create_dir("out")?;
            fs::write("out/result", "v1")?;
            fs::write(
                "metrics.jsonl",
                "{\"step\":1,\"loss\":0.8}\n{\"step\":2,\"loss\":0.6}\n",
            )?;
            println!("started-once");
            while !Path::new("advance").exists() {
                sleep(100);
            }
            fs::write("out/result", "v2")?;
            writeln!(
                fs::OpenOptions::new().append(true).open("metrics.jsonl")?,
                "{{\"step\":3,\"loss\":0.4}}"
            )?;
            println!("advanced-once");
            sleep(180_000);
        }
        "canary" => {
            println!("guest-started-once");
            fs::copy("input.txt", "result.txt")?;
            for n in 0..180 {
                writeln!(
                    fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open("metrics.jsonl")?,
                    "{{\"step\":{n}}}"
                )?;
                sleep(1000);
            }
        }
        "metrics" => {
            metrics()?;
            std::io::stdout().write_all(&vec![b'x'; 9 << 20])?;
        }
        "streams" => {
            ensure!(
                unsafe { libc::getsid(0) == libc::getpid() && libc::getpgrp() == libc::getpid() },
                "guest command did not receive its own session and process group"
            );
            let mut f = fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open("count")?;
            f.write_all(b"x")?;
            ensure!(
                std::env::var("LITERAL")? == "$(touch bad); x",
                "environment changed"
            );
            let data: Vec<u8> = (0..=255).cycle().take(256 * 300).collect();
            std::io::stdout().write_all(&data)?;
            eprint!("error stream");
            std::process::exit(23);
        }
        "excess" => {
            std::io::stdout().write_all(&vec![b'x'; 9 << 20])?;
            std::io::stdout().flush()?;
        }
        "once" => {
            fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&args[1])?
                .write_all(b"x")?;
            sleep(1000);
        }
        "sleep" => sleep(
            args.get(1)
                .map(|s| s.parse::<u64>())
                .transpose()?
                .unwrap_or(30)
                * 1000,
        ),
        "print" => println!("{}", args[1]),
        "pass" => (),
        _ => bail!("unknown fixture workload {mode}"),
    }
    Ok(())
}
