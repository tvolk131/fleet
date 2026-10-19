//! Key files and the allowlist, under `~/.fleet` by default.
//!
//! A key file holds a secret key as 64 hex characters and is created with mode 0600. The allowlist holds one endpoint
//! ID per line; `#` starts a comment.

use std::{
    collections::HashSet,
    fs,
    io::Write,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use iroh::{EndpointId, SecretKey};

/// `~/.fleet`, or `$FLEET_HOME` when set.
pub fn fleet_home() -> Result<PathBuf> {
    if let Some(home) = std::env::var_os("FLEET_HOME") {
        return Ok(PathBuf::from(home));
    }
    let home = std::env::var_os("HOME").context("HOME is not set")?;
    Ok(PathBuf::from(home).join(".fleet"))
}

/// Reads the secret key at `path`, creating a new one (mode 0600, in a mode-0700 folder) if there is none.
pub fn load_or_create(path: &Path) -> Result<SecretKey> {
    if path.exists() {
        let mode = fs::metadata(path)?.permissions().mode();
        if mode & 0o077 != 0 {
            bail!(
                "{} is readable by other users (mode {:o}); run chmod 600 on it",
                path.display(),
                mode & 0o777
            );
        }
        let text = fs::read_to_string(path)?;
        let bytes = from_hex(text.trim())
            .with_context(|| format!("{} is not a fleet key file", path.display()))?;
        return Ok(SecretKey::from_bytes(&bytes));
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
        fs::set_permissions(parent, fs::Permissions::from_mode(0o700))?;
    }
    let key = SecretKey::generate();
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    writeln!(file, "{}", to_hex(&key.to_bytes()))?;
    file.sync_all()?;
    if let Some(parent) = path.parent() {
        fs::File::open(parent)?.sync_all()?;
    }
    Ok(key)
}

/// The endpoint IDs in an allowlist file; none if the file does not exist.
pub fn read_allowlist(path: &Path) -> Result<HashSet<EndpointId>> {
    if !path.exists() {
        return Ok(HashSet::new());
    }
    let mut ids = HashSet::new();
    for (n, line) in fs::read_to_string(path)?.lines().enumerate() {
        let line = line.split('#').next().unwrap_or("").trim();
        if line.is_empty() {
            continue;
        }
        let id = line
            .parse()
            .with_context(|| format!("{} line {}: not an endpoint ID", path.display(), n + 1))?;
        ids.insert(id);
    }
    Ok(ids)
}

/// Adds an endpoint ID to an allowlist file (mode 0600), unless it is already there. Returns whether it was added.
pub fn allow(path: &Path, id: EndpointId, note: &str) -> Result<bool> {
    if read_allowlist(path)?.contains(&id) {
        return Ok(false);
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = fs::OpenOptions::new()
        .append(true)
        .create(true)
        .mode(0o600)
        .open(path)?;
    let note = if note.is_empty() {
        String::new()
    } else {
        format!("  # {note}")
    };
    writeln!(file, "{id}{note}")?;
    Ok(true)
}

/// Exclusive use of a client identity while a CLI/GUI request or staging task is connected.
/// The existing `coordinator.lock` filename is retained for installed client homes.
/// A crash releases the OS lock; a client never takes over controller duties.
#[derive(Debug)]
pub struct KeyLock(#[allow(dead_code)] fs::File);

/// Takes the client's identity lock, or `None` if another process holds it.
pub fn try_lock_client(home: &Path) -> Result<Option<KeyLock>> {
    use std::os::fd::AsRawFd;
    fs::create_dir_all(home)?;
    let file = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .mode(0o600)
        .open(home.join("coordinator.lock"))?;
    let r = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if r == 0 {
        Ok(Some(KeyLock(file)))
    } else if std::io::Error::last_os_error().raw_os_error() == Some(libc::EWOULDBLOCK) {
        Ok(None)
    } else {
        Err(std::io::Error::last_os_error().into())
    }
}

/// Takes the client's identity lock, waiting up to `patience` for another holder to let go.
pub fn lock_client(home: &Path, patience: std::time::Duration) -> Result<KeyLock> {
    let start = std::time::Instant::now();
    loop {
        if let Some(lock) = try_lock_client(home)? {
            return Ok(lock);
        }
        if start.elapsed() >= patience {
            bail!("the client identity is in use by another Fleet request or staging task");
        }
        std::thread::sleep(std::time::Duration::from_millis(200));
    }
}

fn to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn from_hex(text: &str) -> Result<[u8; 32]> {
    if text.len() != 64 || !text.is_ascii() {
        bail!("expected 64 hex characters");
    }
    let mut out = [0u8; 32];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&text[2 * i..2 * i + 2], 16)?;
    }
    Ok(out)
}

#[cfg(feature = "control-plane")]
pub(crate) fn backup_version(path: &Path, version: u32) -> Result<()> {
    let mut backup = path.with_extension(format!(
        "{}.v{version}-backup",
        path.extension().and_then(|s| s.to_str()).unwrap_or("file")
    ));
    if backup.exists() {
        backup = backup.with_extension(format!("v{version}-backup-{:016x}", rand::random::<u64>()));
    }
    let mut target = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&backup)
        .with_context(|| format!("create immutable upgrade backup {}", backup.display()))?;
    std::io::copy(&mut std::fs::File::open(path)?, &mut target)?;
    target.sync_all()?;
    std::fs::File::open(path.parent().context("backup parent")?)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("fleet-test-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    #[test]
    fn a_created_key_reads_back_and_is_private() {
        let dir = temp_dir("key");
        let path = dir.join("agent.key");
        let key = load_or_create(&path).unwrap();
        assert_eq!(load_or_create(&path).unwrap().public(), key.public());
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(
            load_or_create(&path).is_err(),
            "a world-readable key is refused"
        );
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn the_allowlist_adds_once_and_ignores_comments() {
        let dir = temp_dir("allow");
        let path = dir.join("allowed");
        let id = SecretKey::generate().public();
        assert!(allow(&path, id, "laptop").unwrap());
        assert!(!allow(&path, id, "laptop").unwrap());
        fs::write(&path, format!("# administrators\n{id}  # laptop\n\n")).unwrap();
        assert_eq!(read_allowlist(&path).unwrap(), HashSet::from([id]));
        fs::remove_dir_all(dir).unwrap();
    }
}
