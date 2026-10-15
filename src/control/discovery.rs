//! Local authenticated-peer cache. This is a client file, never replicated cluster authority.
use super::membership::MembershipView;
use anyhow::{Result, ensure};
use std::{
    fs,
    io::Write,
    os::{fd::AsRawFd, unix::fs::OpenOptionsExt},
    path::{Path, PathBuf},
};

pub fn save_cache(path: &Path, view: &MembershipView) -> Result<()> {
    let mut lock_path = path.as_os_str().to_owned();
    lock_path.push(".lock");
    let lock = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .mode(0o600)
        .open(lock_path)?;
    ensure!(
        unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX) } == 0,
        "cache lock failed"
    );
    if path.exists() {
        let old: MembershipView = serde_json::from_slice(&fs::read(path)?)?;
        ensure!(old.cluster == view.cluster, "cache cluster mismatch");
        if old.log_id.map(|l| l.index) >= view.log_id.map(|l| l.index) {
            return Ok(());
        }
    }
    let temp = path.with_extension(format!("{:032x}.tmp", rand::random::<u128>()));
    let result = (|| -> Result<()> {
        let mut file = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(&temp)?;
        file.write_all(&serde_json::to_vec_pretty(view)?)?;
        file.sync_all()?;
        fs::rename(&temp, path)?;
        if let Some(parent) = path.parent() {
            fs::File::open(parent)?.sync_all()?;
        }
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(temp);
    }
    result
}

/// Recovery histories have incomparable log positions and never share discovery caches.
pub fn incarnation_cache(config: &Path, cluster: &str) -> PathBuf {
    let mut path = config.as_os_str().to_owned();
    path.push(format!(
        ".{}.members.json",
        blake3::hash(cluster.as_bytes()).to_hex()
    ));
    path.into()
}
