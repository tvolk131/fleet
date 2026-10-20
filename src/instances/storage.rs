//! On-demand storage inventory. APFS allocation may count shared extents in
//! several files; it is never presented as unique or reclaimable disk usage.
use super::{Config, State};
use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use std::{
    fs,
    os::unix::{ffi::OsStrExt, fs::MetadataExt},
    path::Path,
    time::{Duration, Instant},
};

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Usage {
    pub logical_bytes: u64,
    pub allocated_bytes: u64,
    pub files: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Instance {
    pub id: String,
    pub capacity_gb: u32,
    pub disk: Usage,
    pub management: Usage,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Report {
    pub checked_at: u64,
    pub free_bytes: u64,
    pub disk_budget_gb: u32,
    pub reserved_capacity_gb: u64,
    pub min_free_disk_gb: u32,
    pub instances: Vec<Instance>,
    pub image_cache: Usage,
    pub blob_store: Usage,
    pub note: String,
}
pub fn free_bytes(home: &Path) -> Result<u64> {
    let path = std::ffi::CString::new(home.as_os_str().as_bytes())?;
    let mut stats: libc::statvfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statvfs(path.as_ptr(), &mut stats) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok((u128::from(stats.f_bavail) * u128::from(stats.f_frsize)).min(u128::from(u64::MAX)) as u64)
}
pub fn guard(home: &Path) -> Result<()> {
    if free_bytes(home)? < u64::from(Config::load(home)?.min_free_disk_gb) * (1 << 30) {
        bail!("worker's VM free disk guard reached");
    }
    Ok(())
}
fn usage(path: &Path, skip: Option<&Path>, deadline: Instant, entries: &mut u64) -> Result<Usage> {
    let mut total = Usage::default();
    let mut pending = vec![path.to_owned()];
    while let Some(path) = pending.pop() {
        if Some(path.as_path()) == skip {
            continue;
        }
        *entries += 1;
        if *entries > 200_000 || Instant::now() > deadline {
            bail!("storage inventory exceeded its scan budget");
        }
        let meta = match fs::symlink_metadata(&path) {
            Ok(m) => m,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e.into()),
        };
        if meta.file_type().is_symlink() {
            continue;
        }
        if meta.is_dir() {
            for e in fs::read_dir(path)? {
                pending.push(e?.path());
            }
        } else if meta.is_file() {
            total.logical_bytes = total.logical_bytes.saturating_add(meta.len());
            total.allocated_bytes = total
                .allocated_bytes
                .saturating_add(meta.blocks().saturating_mul(512));
            total.files += 1;
        }
    }
    Ok(total)
}
pub fn report(home: &Path) -> Result<Report> {
    let config = Config::load(home)?;
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut entries = 0;
    let mut rows = vec![];
    let mut reserved = 0;
    for i in super::list(home)? {
        if i.observed.state != State::Deleted {
            reserved += u64::from(i.record.spec.disk_gb);
        }
        let dir = super::directory(home, &i.record.id)?;
        let vm = dir.join("driver");
        rows.push(Instance {
            id: i.record.id.clone(),
            capacity_gb: if i.observed.state == State::Deleted {
                0
            } else {
                i.record.spec.disk_gb
            },
            disk: usage(&vm, None, deadline, &mut entries)?,
            management: usage(&dir, Some(&vm), deadline, &mut entries)?,
        });
    }
    let mut images = Usage::default();
    // Aliases may share a bundle.
    let bundles: std::collections::BTreeSet<_> =
        config.images.values().map(|i| &i.bundle).collect();
    for bundle in bundles {
        let u = usage(bundle, None, deadline, &mut entries)?;
        images.logical_bytes = images.logical_bytes.saturating_add(u.logical_bytes);
        images.allocated_bytes = images.allocated_bytes.saturating_add(u.allocated_bytes);
        images.files += u.files;
    }
    Ok(Report{checked_at:crate::jobs::now(),free_bytes:free_bytes(home)?,disk_budget_gb:config.disk_budget_gb,
        reserved_capacity_gb:reserved,min_free_disk_gb:config.min_free_disk_gb,instances:rows,
        image_cache:images,
        blob_store:usage(&home.join("blobs"),None,deadline,&mut entries)?,
        note:"Logical sizes include sparse capacity. Allocated bytes are filesystem-reported per-file blocks; APFS clones can share those blocks, so sums are neither unique physical use nor reclaimable space. No cleanup is performed.".into()})
}
