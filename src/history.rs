//! Bounded, restart-readable local diagnostic streams. Positions include discarded bytes.
//!
//! Capture never removes an unarchived prefix to make room: a full spool drains and discards new
//! output. Only a verified archive acknowledgement (or committed deletion) releases local blocks.
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::{
    fs,
    io::{Read, Seek, SeekFrom, Write},
    os::fd::AsRawFd,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

pub const BLOCK_BYTES: u64 = 1 << 20;
pub const SPOOL_BYTES: u64 = 32 << 20;
pub const MAX_BLOCKS: usize = 256;
pub const READ_BYTES: usize = 1 << 20;
pub mod jsonl;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum Stream {
    Stdout,
    Stderr,
    Metrics,
}
impl Stream {
    pub fn name(self) -> &'static str {
        match self {
            Self::Stdout => "stdout",
            Self::Stderr => "stderr",
            Self::Metrics => "metrics",
        }
    }
    pub fn log(stderr: bool) -> Self {
        if stderr { Self::Stderr } else { Self::Stdout }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct Policy {
    pub log_bytes: u64,
    pub metric_bytes: u64,
    pub log_days: Option<u32>,
    pub metric_days: Option<u32>,
}
impl Default for Policy {
    fn default() -> Self {
        Self {
            log_bytes: 1 << 30,
            metric_bytes: 128 << 20,
            log_days: Some(30),
            metric_days: Some(90),
        }
    }
}
impl Policy {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            (BLOCK_BYTES..=64 << 30).contains(&self.log_bytes),
            "history log budget must be 1 MiB–64 GiB"
        );
        ensure!(
            (BLOCK_BYTES..=4 << 30).contains(&self.metric_bytes),
            "history metric budget must be 1 MiB–4 GiB"
        );
        ensure!(
            [self.log_days, self.metric_days]
                .iter()
                .all(|n| n.is_none_or(|n| (1..=36500).contains(&n))),
            "history retention must be 1–36500 days or null"
        );
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Block {
    pub start: u64,
    pub end: u64,
}
impl Block {
    pub fn name(&self) -> String {
        format!("{:020}.bin", self.start)
    }
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Local {
    #[serde(default)]
    pub revision: u64,
    #[serde(default)]
    pub sources: std::collections::BTreeMap<String, jsonl::Cursor>,
    pub observed: u64,
    #[serde(default)]
    pub captured_end: u64,
    pub dropped: u64,
    pub released: u64,
    pub closed: bool,
    pub blocks: Vec<Block>,
    pub error: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReadResult {
    pub from: u64,
    pub next: u64,
    pub observed: u64,
    pub data: Vec<u8>,
    /// A range known to exist but not available in this local spool.
    pub gap: Option<Block>,
    pub closed: bool,
}

pub fn root(job: &Path, stream: Stream) -> PathBuf {
    job.join("history").join(stream.name())
}
pub fn state(job: &Path, stream: Stream) -> Result<Local> {
    Ok(crate::jobs::read_json(&root(job, stream).join("state.json"))?.unwrap_or_default())
}
fn locked(job: &Path, stream: Stream) -> Result<(PathBuf, fs::File)> {
    let root = root(job, stream);
    fs::create_dir_all(&root)?;
    let lock = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(root.join("capture.lock"))?;
    ensure!(
        unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX) } == 0,
        "history lock failed"
    );
    Ok((root, lock))
}

/// A safety floor for diagnostics, separate from job/storage reservations. Concurrent non-Fleet
/// writers can still consume this space; IO failures also degrade capture instead of the job.
#[allow(clippy::unnecessary_cast)]
pub fn has_disk_headroom(path: &Path, additional: u64) -> Result<bool> {
    use std::os::unix::ffi::OsStrExt;
    let path = std::ffi::CString::new(path.as_os_str().as_bytes())?;
    let mut stats = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    ensure!(
        unsafe { libc::statvfs(path.as_ptr(), stats.as_mut_ptr()) } == 0,
        "history filesystem statistics failed"
    );
    let stats = unsafe { stats.assume_init() };
    Ok((stats.f_bavail as u64).saturating_mul(stats.f_frsize as u64) >= (1 << 30) + additional)
}

pub fn append(job: &Path, stream: Stream, bytes: &[u8]) -> Result<()> {
    append_with_budget(job, stream, bytes, SPOOL_BYTES)
}
fn append_with_budget(job: &Path, stream: Stream, bytes: &[u8], budget: u64) -> Result<()> {
    let (root, _lock) = locked(job, stream)?;
    let mut s = state(job, stream)?;
    append_locked(&root, &mut s, bytes, budget)
}
fn append_locked(root: &Path, s: &mut Local, bytes: &[u8], budget: u64) -> Result<()> {
    prune_orphans(root, s)?;
    ensure!(!s.closed, "history stream is closed");
    let start = s.observed;
    let end = start
        .checked_add(bytes.len() as u64)
        .context("history offset overflow")?;
    let used: u64 = s.blocks.iter().map(|b| b.end - b.start).sum();
    let mut keep = bytes.len().min(budget.saturating_sub(used) as usize);
    if !has_disk_headroom(root, keep as u64)? {
        keep = 0;
        s.error = Some("Capture paused: less than 1 GiB filesystem headroom".into());
    }
    let mut written = 0;
    while written < keep {
        let offset = start + written as u64;
        let extend = s
            .blocks
            .last()
            .is_some_and(|b| b.end == offset && b.end - b.start < BLOCK_BYTES);
        if !extend {
            if s.blocks.len() == MAX_BLOCKS {
                keep = written;
                break;
            }
            s.blocks.push(Block {
                start: offset,
                end: offset,
            });
        }
        let b = s.blocks.last_mut().unwrap();
        let count = (keep - written).min((BLOCK_BYTES - (b.end - b.start)) as usize);
        let mut f = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(root.join(b.name()))?;
        // An interrupted append can leave uncommitted bytes. Never reinterpret them at new offsets.
        f.set_len(b.end - b.start)?;
        f.seek(SeekFrom::Start(b.end - b.start))?;
        f.write_all(&bytes[written..written + count])?;
        f.sync_data()?;
        b.end += count as u64;
        written += count;
    }
    s.observed = end;
    if keep > 0 {
        s.captured_end = start + keep as u64;
    }
    s.revision = s
        .revision
        .checked_add(1)
        .context("history revision overflow")?;
    s.dropped = s.dropped.saturating_add((bytes.len() - keep) as u64);
    crate::instances::write(&root.join("state.json"), s)
}

/// Used after IO recovers, so dropped output is represented even when its bytes could not be saved.
pub fn record_loss(job: &Path, stream: Stream, count: u64, error: &str) -> Result<()> {
    let (root, _lock) = locked(job, stream)?;
    let mut s = state(job, stream)?;
    if count == 0 && s.error.as_deref() == Some(error) {
        return Ok(());
    }
    s.observed = s
        .observed
        .checked_add(count)
        .context("history offset overflow")?;
    s.dropped = s.dropped.saturating_add(count);
    s.error = Some(error.chars().take(256).collect());
    s.revision = s
        .revision
        .checked_add(1)
        .context("history revision overflow")?;
    crate::instances::write(&root.join("state.json"), &s)
}
pub fn clear_archive_error(job: &Path, stream: Stream) -> Result<()> {
    let (root, _lock) = locked(job, stream)?;
    let mut s = state(job, stream)?;
    if s.error
        .as_deref()
        .is_some_and(|e| e.starts_with("Archive:"))
    {
        s.error = None;
        s.revision = s
            .revision
            .checked_add(1)
            .context("history revision overflow")?;
        crate::instances::write(&root.join("state.json"), &s)?;
    }
    Ok(())
}
pub fn close(job: &Path, stream: Stream) -> Result<()> {
    let (root, _lock) = locked(job, stream)?;
    let mut s = state(job, stream)?;
    if s.closed {
        return Ok(());
    }
    s.closed = true;
    s.revision = s
        .revision
        .checked_add(1)
        .context("history revision overflow")?;
    crate::instances::write(&root.join("state.json"), &s)
}

/// Release whole blocks only. The active partial block stays bounded until it fills or closes.
pub fn release(job: &Path, stream: Stream, through: u64) -> Result<()> {
    let (root, _lock) = locked(job, stream)?;
    let mut s = state(job, stream)?;
    ensure!(
        through <= s.observed,
        "history acknowledgement beyond capture"
    );
    let removed: Vec<_> = s
        .blocks
        .iter()
        .filter(|b| b.end <= through)
        .cloned()
        .collect();
    s.blocks.retain(|b| b.end > through);
    s.released = s.released.max(through);
    // Publish the removal before unlinking; a crash can leave an orphan, never a false readable range.
    crate::instances::write(&root.join("state.json"), &s)?;
    for b in removed {
        if root.join(b.name()).exists() {
            fs::remove_file(root.join(b.name()))?;
        }
    }
    prune_orphans(&root, &s)?;
    fs::File::open(&root)?.sync_all()?;
    Ok(())
}

fn prune_orphans(root: &Path, s: &Local) -> Result<()> {
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.len() == 24
            && name.ends_with(".bin")
            && name.as_bytes()[..20].iter().all(|b| b.is_ascii_digit())
            && !s.blocks.iter().any(|b| b.name() == name)
        {
            ensure!(entry.file_type()?.is_file(), "invalid history spool object");
            fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}

pub fn read(job: &Path, stream: Stream, from: u64, max: usize) -> Result<ReadResult> {
    let (root, _lock) = locked(job, stream)?;
    let s = state(job, stream)?;
    ensure!(from <= s.observed, "history offset beyond capture");
    let mut r = ReadResult {
        from,
        next: from,
        observed: s.observed,
        data: vec![],
        gap: None,
        closed: s.closed,
    };
    if from == s.observed {
        return Ok(r);
    }
    let Some(b) = s.blocks.iter().find(|b| b.end > from) else {
        r.gap = Some(Block {
            start: from,
            end: s.observed,
        });
        r.next = s.observed;
        return Ok(r);
    };
    if b.start > from {
        r.gap = Some(Block {
            start: from,
            end: b.start,
        });
        r.next = b.start;
        return Ok(r);
    }
    for block in s.blocks.iter().filter(|b| b.end > from) {
        if block.start > r.next || r.data.len() >= max.clamp(1, READ_BYTES) {
            break;
        }
        let mut f = fs::File::open(root.join(block.name()))?;
        f.seek(SeekFrom::Start(r.next - block.start))?;
        let n = (block.end - r.next).min((max.clamp(1, READ_BYTES) - r.data.len()) as u64);
        let before = r.data.len();
        f.take(n).read_to_end(&mut r.data)?;
        ensure!(
            (r.data.len() - before) as u64 == n,
            "history spool truncated"
        );
        r.next += n;
    }
    Ok(r)
}

/// Bounded readers continue draining after disk errors or budget exhaustion. A descendant retaining
/// a pipe cannot prevent supervisor cleanup once the child group has been stopped.
pub fn collect(
    mut input: impl Read + AsRawFd,
    job: &Path,
    stream: Stream,
    stopped: Arc<AtomicBool>,
) -> Result<()> {
    let fd = input.as_raw_fd();
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    ensure!(
        flags >= 0 && unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } >= 0,
        "history pipe setup failed"
    );
    let mut buf = [0u8; 65536];
    let mut lost = 0u64;
    let mut error = String::new();
    loop {
        let n = match input.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                if stopped.load(Ordering::Relaxed) {
                    break;
                }
                std::thread::sleep(Duration::from_millis(20));
                continue;
            }
            Err(e) => {
                error = e.to_string();
                break;
            }
        };
        if lost > 0 {
            if record_loss(job, stream, lost, &error).is_err() {
                lost = lost.saturating_add(n as u64);
                continue;
            }
            lost = 0;
        }
        if let Err(e) = append(job, stream, &buf[..n]) {
            lost += n as u64;
            error = e.to_string();
        }
    }
    if lost > 0 || !error.is_empty() {
        record_loss(job, stream, lost, &error)?;
    }
    close(job, stream)
}

#[cfg(test)]
mod tests {
    use super::*;
    pub(super) fn dir() -> PathBuf {
        let p = std::env::temp_dir().join(format!("fleet-history-{:032x}", rand::random::<u128>()));
        fs::create_dir(&p).unwrap();
        p
    }
    #[test]
    fn bounded_capture_preserves_positions_and_resumes_after_verified_release() {
        let p = dir();
        let stream = Stream::Stdout;
        append_with_budget(&p, stream, b"abcdef", 4).unwrap();
        assert_eq!(read(&p, stream, 0, 20).unwrap().data, b"abcd");
        let gap = read(&p, stream, 4, 20).unwrap();
        assert_eq!(gap.gap, Some(Block { start: 4, end: 6 }));
        assert_eq!(state(&p, stream).unwrap().dropped, 2);
        release(&p, stream, 4).unwrap();
        append_with_budget(&p, stream, b"gh", 4).unwrap();
        assert_eq!(read(&p, stream, 6, 20).unwrap().data, b"gh");
        close(&p, stream).unwrap();
        assert!(read(&p, stream, 8, 20).unwrap().closed);
        assert!(append(&p, stream, b"i").is_err());
        fs::remove_dir_all(p).unwrap();
    }
    #[test]
    fn utf8_split_across_blocks_and_discarded_tail_keeps_byte_offsets() {
        let p = dir();
        let stream = Stream::Stdout;
        let mut bytes = vec![b'x'; BLOCK_BYTES as usize - 1];
        bytes.extend_from_slice("🌍".as_bytes());
        append(&p, stream, &bytes).unwrap();
        let r = read(&p, stream, BLOCK_BYTES - 1, 4).unwrap();
        assert_eq!(r.data, "🌍".as_bytes());
        assert_eq!(r.next, BLOCK_BYTES + 3);
        release(&p, stream, r.next).unwrap();
        append_with_budget(&p, stream, b"abcdef", 2).unwrap();
        let s = state(&p, stream).unwrap();
        assert_eq!(s.observed - s.captured_end, 4);
        fs::remove_dir_all(p).unwrap();
    }
    #[test]
    fn partial_append_after_crash_is_not_published_or_shifted() {
        let p = dir();
        let stream = Stream::Stderr;
        append(&p, stream, b"abc").unwrap();
        let b = state(&p, stream).unwrap().blocks[0].clone();
        fs::OpenOptions::new()
            .append(true)
            .open(root(&p, stream).join(b.name()))
            .unwrap()
            .write_all(b"uncommitted")
            .unwrap();
        assert_eq!(read(&p, stream, 0, 100).unwrap().data, b"abc");
        append(&p, stream, b"d").unwrap();
        assert_eq!(read(&p, stream, 0, 100).unwrap().data, b"abcd");
        fs::remove_dir_all(p).unwrap();
    }
}
