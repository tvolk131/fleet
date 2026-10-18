//! The unprivileged guest-side protocol. Host-side validation and VM isolation remain authoritative.
use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    os::unix::{
        fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
        process::{CommandExt, ExitStatusExt},
    },
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

pub const READY: &str = "fleet-guest-v2";
pub mod tunnel;
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Request {
    Ready,
    Exec {
        argv: Vec<String>,
        cwd: Option<String>,
        env: BTreeMap<String, String>,
        timeout_seconds: u64,
    },
    Upload {
        path: String,
        size: u64,
        sha256: String,
        executable: bool,
    },
    Download {
        path: String,
        max_bytes: u64,
        version: Option<String>,
    },
}

pub fn main_args(mut args: impl Iterator<Item = String>) -> Result<i32> {
    let first = args.next().context("guest request required")?;
    if first == "--services" {
        let config: tunnel::Config =
            serde_json::from_str(&args.next().context("service configuration required")?)?;
        ensure!(args.next().is_none(), "unexpected arguments");
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()?;
        let result = runtime.block_on(tunnel::guest(config));
        // Tokio's stdin reader may be blocked in the OS after a protocol failure.
        // main exits immediately; never wait for another byte from a failed host.
        runtime.shutdown_background();
        result?;
        return Ok(0);
    }
    if first == "--job" {
        let request: Observation =
            serde_json::from_str(&args.next().context("job request required")?)?;
        ensure!(args.next().is_none(), "unexpected arguments");
        println!("{}", observe(request)?);
        return Ok(0);
    }
    ensure!(args.next().is_none(), "unexpected arguments");
    let request: Request = serde_json::from_str(&first)?;
    match request {
        Request::Ready => println!("{READY}"),
        Request::Exec {
            argv,
            cwd,
            env,
            timeout_seconds,
        } => {
            ensure!(
                !argv.is_empty() && (1..=86400).contains(&timeout_seconds),
                "invalid command/timeout"
            );
            let mut command = Command::new(&argv[0]);
            command.args(&argv[1..]).envs(env).stdin(Stdio::null());
            // Match the previous helper's start_new_session: the workload has
            // its own session and process group, independent of Tart's channel.
            // setsid is async-signal-safe and this closure does not allocate.
            unsafe {
                command.pre_exec(|| {
                    if libc::setsid() == -1 {
                        return Err(std::io::Error::last_os_error());
                    }
                    Ok(())
                });
            }
            if let Some(cwd) = cwd {
                command.current_dir(cwd);
            }
            let mut child = command.spawn()?;
            let deadline = Instant::now() + Duration::from_secs(timeout_seconds);
            loop {
                if let Some(status) = child.try_wait()? {
                    return Ok(status
                        .code()
                        .unwrap_or(128 + status.signal().unwrap_or(127)));
                }
                if Instant::now() >= deadline {
                    break;
                }
                std::thread::sleep(Duration::from_millis(20));
            }
            unsafe {
                libc::kill(-(child.id() as i32), libc::SIGTERM);
            }
            let grace = Instant::now() + Duration::from_secs(5);
            while Instant::now() < grace {
                if child.try_wait()?.is_some() {
                    eprintln!("Fleet guest command timed out");
                    return Ok(124);
                }
                std::thread::sleep(Duration::from_millis(20));
            }
            unsafe {
                libc::kill(-(child.id() as i32), libc::SIGKILL);
            }
            child.wait()?;
            eprintln!("Fleet guest command timed out");
            return Ok(124);
        }
        Request::Upload {
            path,
            size,
            sha256,
            executable,
        } => {
            ensure!(size <= 1 << 30, "upload exceeds byte limit");
            let path = Path::new(&path);
            let parent = path.parent().context("upload parent required")?;
            fs::create_dir_all(parent)?;
            let (temporary, mut out) = temporary(parent)?;
            let mut input = std::io::stdin().lock();
            let mut remaining = size;
            let mut digest = Sha256::new();
            let mut buffer = [0u8; 65536];
            while remaining > 0 {
                let n = input.read(&mut buffer[..remaining.min(65536) as usize])?;
                ensure!(n > 0, "incomplete upload");
                out.write_all(&buffer[..n])?;
                digest.update(&buffer[..n]);
                remaining -= n as u64;
            }
            ensure!(
                input.read(&mut buffer[..1])? == 0,
                "upload exceeded declared length"
            );
            ensure!(
                format!("{:x}", digest.finalize()) == sha256,
                "upload SHA-256 mismatch"
            );
            out.set_permissions(fs::Permissions::from_mode(if executable {
                0o700
            } else {
                0o600
            }))?;
            out.sync_all()?;
            match fs::hard_link(&temporary.0, path) {
                Ok(()) => (),
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    let mut current = regular(path)?;
                    let meta = current.metadata()?;
                    ensure!(
                        meta.len() == size && digest_reader(&mut current)? == sha256,
                        "destination exists with different content"
                    );
                    ensure!(
                        (meta.mode() & 0o111 != 0) == executable,
                        "destination exists with different executable permissions"
                    );
                }
                Err(e) => return Err(e.into()),
            }
            File::open(parent)?.sync_all()?;
            println!("{}", serde_json::json!({"bytes":size,"sha256":sha256}));
        }
        Request::Download {
            path,
            max_bytes,
            version: expected,
        } => {
            let mut source = match regular(Path::new(&path)) {
                Err(e)
                    if expected.is_some()
                        && e.downcast_ref::<std::io::Error>()
                            .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound) =>
                {
                    return Ok(75);
                }
                result => result?,
            };
            let before = source.metadata()?;
            if expected.as_ref().is_some_and(|v| *v != version(&before)) {
                return Ok(75);
            }
            ensure!(before.len() <= max_bytes, "download exceeds byte limit");
            let mut output = std::io::stdout().lock();
            let count = std::io::copy(&mut (&mut source).take(before.len()), &mut output)?;
            if count != before.len() || version(&source.metadata()?) != version(&before) {
                return Ok(75);
            }
        }
    }
    Ok(0)
}

fn regular(path: &Path) -> Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    ensure!(
        file.metadata()?.is_file(),
        "expected regular file, not a symlink or device"
    );
    Ok(file)
}
pub fn version(meta: &fs::Metadata) -> String {
    format!(
        "{}:{}:{}:{}:{}",
        meta.dev(),
        meta.ino(),
        meta.len(),
        meta.mtime() as i128 * 1_000_000_000 + meta.mtime_nsec() as i128,
        meta.ctime() as i128 * 1_000_000_000 + meta.ctime_nsec() as i128
    )
}
fn digest_reader(reader: &mut impl Read) -> Result<String> {
    let mut hash = Sha256::new();
    let mut buffer = [0u8; 65536];
    loop {
        let n = reader.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
    }
    Ok(format!("{:x}", hash.finalize()))
}
struct Temporary(PathBuf);
impl Drop for Temporary {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}
fn temporary(parent: &Path) -> Result<(Temporary, File)> {
    for n in 0..100 {
        let path = parent.join(format!(
            ".fleet-upload-{}-{}-{n}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        ));
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)
        {
            Ok(file) => return Ok((Temporary(path), file)),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => (),
            Err(e) => return Err(e.into()),
        }
    }
    bail!("unable to create upload staging file")
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Platform {
    pub os: String,
    pub arch: String,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct Observation {
    pub kind: String,
    pub root: PathBuf,
    #[serde(default)]
    pub platform: Option<Platform>,
    #[serde(default)]
    pub patterns: Vec<String>,
    #[serde(default)]
    pub probes: Vec<serde_json::Value>,
    #[serde(default)]
    pub settle_seconds: u64,
    #[serde(default = "yes", rename = "final")]
    pub final_pass: bool,
    #[serde(default)]
    pub metric_files: Vec<String>,
    #[serde(default)]
    pub history_cursors: BTreeMap<String, Cursor>,
}
fn yes() -> bool {
    true
}
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Cursor {
    #[serde(default)]
    pub identity: String,
    #[serde(default)]
    pub offset: u64,
    #[serde(default)]
    pub generation: u64,
    #[serde(default)]
    pub discarding: bool,
}
fn matches(pattern: &str, path: &str, exact: bool) -> bool {
    let parts: Vec<_> = pattern.split('/').collect();
    let names: Vec<_> = path.split('/').collect();
    if if exact {
        parts.len() != names.len()
    } else {
        parts.len() > names.len()
    } {
        return false;
    }
    parts.iter().zip(names).all(|(p, n)| {
        regex::Regex::new(&format!("\\A{}\\z", regex::escape(p).replace("\\*", ".*")))
            .is_ok_and(|r| r.is_match(n))
    })
}
fn walk(root: &Path, parent: &Path, result: &mut Vec<(String, fs::Metadata)>) -> Result<()> {
    if !parent.exists() {
        return Ok(());
    }
    let mut entries = fs::read_dir(parent)?.collect::<std::io::Result<Vec<_>>>()?;
    entries.sort_by_key(|e| e.file_name());
    for entry in entries {
        let path = entry.path();
        let meta = match fs::symlink_metadata(&path) {
            Ok(m) => m,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e.into()),
        };
        // Match the previous inventory: never traverse symlinks, including directory aliases.
        if meta.file_type().is_symlink() && path.is_dir() {
            continue;
        }
        let directory = meta.is_dir();
        result.push((
            path.strip_prefix(root)?
                .to_str()
                .context("non-UTF-8 output path")?
                .to_owned(),
            meta,
        ));
        ensure!(
            result.len() <= 100000,
            "output inventory exceeded its scan limit"
        );
        if directory {
            walk(root, &path, result)?;
        }
    }
    Ok(())
}
fn values(path: &Path, field: &str, points: usize) -> Result<Vec<f64>> {
    if points == 0 {
        return Ok(vec![]);
    }
    let mut file = match regular(path) {
        Ok(f) => f,
        Err(e)
            if e.downcast_ref::<std::io::Error>().is_some_and(|e| {
                matches!(
                    e.kind(),
                    std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
                )
            }) =>
        {
            return Ok(vec![]);
        }
        Err(e) => return Err(e),
    };
    let start = file.metadata()?.len().saturating_sub(512 * 1024);
    file.seek(SeekFrom::Start(start))?;
    let mut data = vec![];
    file.take(512 * 1024).read_to_end(&mut data)?;
    let first = if start > 0 {
        data.iter()
            .position(|b| *b == b'\n')
            .map_or(data.len(), |n| n + 1)
    } else {
        0
    };
    let end = data.iter().rposition(|b| *b == b'\n').map_or(0, |n| n + 1);
    let mut result = vec![];
    if first < end {
        for line in data[first..end].split(|b| *b == b'\n').rev() {
            if let Ok(mut value) = serde_json::from_slice::<serde_json::Value>(line) {
                for key in field.split('.') {
                    value = value.get(key).cloned().unwrap_or_default();
                }
                if let Some(v) = value.as_f64().filter(|v| v.is_finite()) {
                    result.push(v);
                    if result.len() == points {
                        break;
                    }
                }
            }
        }
    }
    result.reverse();
    Ok(result)
}
fn history_batch(
    root: &Path,
    path: &str,
    old: &Cursor,
    final_pass: bool,
) -> Result<Option<serde_json::Value>> {
    let mut source = match regular(&root.join(path)) {
        Ok(f) => f,
        Err(e)
            if !final_pass
                && e.downcast_ref::<std::io::Error>()
                    .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound) =>
        {
            return Ok(None);
        }
        Err(e) => return Err(e),
    };
    let meta = source.metadata()?;
    let identity = format!("{}:{}", meta.dev(), meta.ino());
    let reset = identity != old.identity || meta.len() < old.offset;
    let start = if reset { 0 } else { old.offset };
    let generation = old.generation + u64::from(reset && !old.identity.is_empty());
    source.seek(SeekFrom::Start(start))?;
    let mut data = vec![];
    source.take(65536).read_to_end(&mut data)?;
    if data.is_empty() {
        return Ok(None);
    }
    let discarding = old.discarding && !reset;
    let end = data
        .iter()
        .enumerate()
        .filter(|(_, b)| **b == b'\n')
        .map(|(n, _)| n + 1)
        .take(if discarding { 1 } else { 256 })
        .last();
    let mut skipped = discarding || (end.is_none() && (data.len() == 65536 || final_pass));
    if end.is_none() && !skipped {
        return Ok(None);
    }
    let count = end.unwrap_or(data.len());
    let text = if skipped {
        ""
    } else {
        match std::str::from_utf8(&data[..count]) {
            Ok(t) => t,
            Err(_) => {
                skipped = true;
                ""
            }
        }
    };
    Ok(Some(
        serde_json::json!({"path":path,"identity":identity,"generation":generation,"from":start,"next":start+count as u64,"length":meta.len(),"text":text,"skipped":skipped,"discarding":end.is_none()}),
    ))
}
pub fn observe(spec: Observation) -> Result<serde_json::Value> {
    use serde_json::json;
    if spec.kind == "prepare" {
        if let Some(p) = spec.platform {
            ensure!(
                p.os == std::env::consts::OS && p.arch == std::env::consts::ARCH,
                "guest OS/architecture differs from portable runtime"
            );
        }
        fs::create_dir_all(&spec.root)?;
        fs::set_permissions(&spec.root, fs::Permissions::from_mode(0o700))?;
        return Ok(serde_json::Value::Null);
    }
    ensure!(
        matches!(spec.kind.as_str(), "observe" | "manifest"),
        "unknown job helper operation"
    );
    let mut paths = vec![];
    walk(&spec.root, &spec.root, &mut paths)?;
    let mut files = vec![];
    let mut total = 0u64;
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos() as i128;
    for (path, meta) in &paths {
        if meta.is_dir() || !spec.patterns.iter().any(|p| matches(p, path, false)) {
            continue;
        }
        ensure!(
            meta.is_file(),
            "published output must be a regular file: {path}"
        );
        let changed = (meta.mtime() as i128 * 1_000_000_000 + meta.mtime_nsec() as i128)
            .max(meta.ctime() as i128 * 1_000_000_000 + meta.ctime_nsec() as i128);
        if !spec.final_pass && now - changed < spec.settle_seconds as i128 * 1_000_000_000 {
            continue;
        }
        total += meta.len();
        ensure!(
            meta.len() <= 1 << 30 && total <= 4 << 30 && files.len() < 256,
            "output size/file-count limit exceeded"
        );
        files.push(json!({"path":path,"size":meta.len(),"version":version(meta)}));
    }
    files.sort_by(|a, b| a["path"].as_str().cmp(&b["path"].as_str()));
    if spec.kind == "manifest" {
        return Ok(files.into());
    }
    let mut metrics = vec![];
    for probe in spec.probes {
        let (kind, settings) = probe
            .as_object()
            .and_then(|p| p.iter().next())
            .context("invalid probe")?;
        metrics.push(match kind.as_str() {
            "jsonl_field" | "jsonl_series" => {
                let pattern = settings["file"].as_str().context("probe file required")?;
                let points = if kind == "jsonl_field" { 1 } else { settings["points"].as_u64().unwrap_or(60).min(500) as usize };
                let latest = paths.iter().filter(|(p,m)| m.is_file() && matches(pattern,p,true)).max_by_key(|(p,m)| (m.mtime(),m.mtime_nsec(),p));
                json!({"values":match latest { Some((p,_)) => values(&spec.root.join(p),settings["field"].as_str().context("probe field required")?,points)?, None => vec![] }})
            }
            "glob_count" => json!({"count":paths.iter().filter(|(p,_)| matches(settings["pattern"].as_str().unwrap_or(""),p,true)).count()}),
            _ => serde_json::Value::Null,
        });
    }
    let mut history = vec![];
    let mut errors = vec![];
    for path in spec.metric_files {
        match history_batch(
            &spec.root,
            &path,
            spec.history_cursors
                .get(&path)
                .unwrap_or(&Cursor::default()),
            spec.final_pass,
        ) {
            Ok(Some(batch)) => history.push(batch),
            Ok(None) => (),
            Err(e) => errors.push(format!("{path}: {e}").chars().take(512).collect::<String>()),
        }
    }
    Ok(json!({"files":files,"metrics":metrics,"history":history,"history_errors":errors}))
}
