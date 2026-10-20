//! An image bundle's identity: the iroh-blobs collection hash of its files, so the digest a
//! worker's catalog pins is the hash another worker can later fetch the whole bundle by.
//!
//! A bundle is a flat directory. Every regular file not starting with `.` is part of it, named
//! by its file name, in byte order; anything else in it is an error.

use std::{
    collections::BTreeMap,
    fs,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail, ensure};
use iroh_blobs::{Hash, format::collection::Collection};
use serde::{Deserialize, Serialize};

pub const PREFIX: &str = "blake3:";

/// `blake3:` followed by the bundle's collection hash in lowercase hex.
pub fn digest(bundle: &Path) -> Result<String> {
    Ok(compute(bundle, &files(bundle)?, &|| false)?.0)
}

/// Checks a digest's format, not what it names.
pub fn validate(digest: &str) -> Result<()> {
    let hex = digest
        .strip_prefix(PREFIX)
        .context("image digest must start with blake3:")?;
    ensure!(
        hex.len() == 64
            && hex
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "invalid image digest"
    );
    Ok(())
}

/// Fails unless `bundle` has digest `expected`. Hashing a disk takes a while, so a match is
/// remembered in `cache` against each file's identity; any write to a file changes its ctime.
/// Hashing stops with an error soon after `cancelled` returns true.
pub fn verify(
    bundle: &Path,
    expected: &str,
    cache: &Path,
    cancelled: &dyn Fn() -> bool,
) -> Result<()> {
    validate(expected)?;
    let bundle = bundle.canonicalize()?;
    let files = files(&bundle)?;
    let before = stamp(&files)?;
    let mut known: BTreeMap<PathBuf, Cached> = crate::jobs::read_json(cache)?.unwrap_or_default();
    if known
        .get(&bundle)
        .is_some_and(|c| c.digest == expected && c.files == before)
    {
        return Ok(());
    }
    let (actual, _) = compute(&bundle, &files, cancelled)?;
    ensure!(
        actual == expected,
        "image bundle {} has digest {actual}, not the catalog's {expected}",
        bundle.display()
    );
    // Only a stamp taken before hashing that still holds afterwards describes what was hashed.
    if stamp(&files)? == before {
        known.insert(
            bundle,
            Cached {
                digest: actual,
                files: before,
            },
        );
        crate::instances::write(cache, &known)?;
    }
    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
struct Cached {
    digest: String,
    files: Vec<Stamp>,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Stamp {
    name: String,
    dev: u64,
    ino: u64,
    size: u64,
    mtime_ns: i128,
    ctime_ns: i128,
}

fn stamp(files: &[(String, PathBuf)]) -> Result<Vec<Stamp>> {
    files
        .iter()
        .map(|(name, path)| {
            let m = fs::symlink_metadata(path)?;
            Ok(Stamp {
                name: name.clone(),
                dev: m.dev(),
                ino: m.ino(),
                size: m.size(),
                mtime_ns: i128::from(m.mtime()) * 1_000_000_000 + i128::from(m.mtime_nsec()),
                ctime_ns: i128::from(m.ctime()) * 1_000_000_000 + i128::from(m.ctime_nsec()),
            })
        })
        .collect()
}

fn files(bundle: &Path) -> Result<Vec<(String, PathBuf)>> {
    let mut files = Vec::new();
    for entry in
        fs::read_dir(bundle).with_context(|| format!("reading bundle {}", bundle.display()))?
    {
        let entry = entry?;
        let name = entry
            .file_name()
            .into_string()
            .map_err(|n| anyhow::anyhow!("non-UTF-8 file name {n:?} in bundle"))?;
        if name.starts_with('.') {
            continue;
        }
        if !entry.file_type()?.is_file() {
            bail!(
                "{} in bundle {} is not a regular file",
                name,
                bundle.display()
            );
        }
        files.push((name, entry.path()));
    }
    ensure!(!files.is_empty(), "bundle {} is empty", bundle.display());
    files.sort();
    Ok(files)
}

/// Each file is hashed in slices of this size, checking for cancellation in between.
const SLICE: usize = 1 << 30;

fn compute(
    bundle: &Path,
    files: &[(String, PathBuf)],
    cancelled: &dyn Fn() -> bool,
) -> Result<(String, Collection)> {
    let mut collection = Collection::default();
    for (name, path) in files {
        let file = fs::File::open(path)
            .with_context(|| format!("hashing {} in {}", name, bundle.display()))?;
        let mut hasher = blake3::Hasher::new();
        if file.metadata()?.len() > 0 {
            // A writer changing the file while it is mapped can only produce a wrong hash,
            // which then fails verification.
            let map = unsafe { memmap2::Mmap::map(&file)? };
            for slice in map.chunks(SLICE) {
                ensure!(!cancelled(), "cancelled while hashing {}", bundle.display());
                hasher.update_rayon(slice);
            }
        }
        collection.push(name.clone(), Hash::from(hasher.finalize()));
    }
    let root = collection.to_blobs().last().context("empty collection")?;
    Ok((format!("{PREFIX}{}", Hash::new(&root).to_hex()), collection))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn digest_is_the_iroh_collection_hash_and_tracks_every_file() {
        let dir = std::env::temp_dir().join(format!("fleet-bundle-{:016x}", rand::random::<u64>()));
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("image.json"), b"{}").unwrap();
        fs::write(dir.join("disk.img"), b"disk").unwrap();
        fs::write(dir.join(".DS_Store"), b"ignored").unwrap();
        let first = digest(&dir).unwrap();
        validate(&first).unwrap();

        let expected: Collection = [
            ("disk.img", Hash::new(b"disk")),
            ("image.json", Hash::new(b"{}")),
        ]
        .into_iter()
        .collect();
        let root = expected.to_blobs().last().unwrap();
        assert_eq!(first, format!("blake3:{}", Hash::new(&root).to_hex()));

        let cache = dir.join(".cache.json");
        let no = || false;
        assert!(verify(&dir, &first, &cache, &|| true).is_err());
        verify(&dir, &first, &cache, &no).unwrap();
        // Cached: a cancelled check that hashes nothing still succeeds.
        verify(&dir, &first, &cache, &|| true).unwrap();
        fs::write(dir.join("disk.img"), b"diSk").unwrap();
        assert!(verify(&dir, &first, &cache, &no).is_err());
        fs::write(dir.join("disk.img"), b"disk").unwrap();
        verify(&dir, &first, &cache, &no).unwrap();
        fs::write(dir.join("extra"), b"").unwrap();
        assert!(verify(&dir, &first, &cache, &no).is_err());
        fs::remove_dir_all(&dir).unwrap();
    }
}
