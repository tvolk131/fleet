//! Offset-aware capture of explicitly declared append-only metric files. Every batch records its
//! source cursor in the same local transaction as its diagnostic bytes, so replay is harmless.
use super::*;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
pub const BATCH: usize = 64 << 10;
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Cursor {
    pub identity: String,
    pub offset: u64,
    pub generation: u64,
    #[serde(default)]
    pub discarding: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Batch {
    pub path: String,
    pub identity: String,
    pub generation: u64,
    pub from: u64,
    pub next: u64,
    pub length: u64,
    pub text: String,
    pub skipped: bool,
    pub discarding: bool,
}

pub fn validate_paths(paths: &[String]) -> Result<()> {
    ensure!(paths.len() <= 4, "at most four JSONL metric files");
    let unique: std::collections::BTreeSet<_> = paths.iter().collect();
    ensure!(unique.len() == paths.len(), "duplicate metric path");
    for path in paths {
        crate::checkpoint::canonical(path)?;
        ensure!(path.len() <= 512, "metric path too long");
    }
    Ok(())
}

pub fn read_batch(
    root: &Path,
    path: &str,
    old: &Cursor,
    final_pass: bool,
) -> Result<Option<Batch>> {
    crate::checkpoint::canonical(path)?;
    let mut current = root.to_path_buf();
    for part in Path::new(path).components() {
        current.push(part);
        match fs::symlink_metadata(&current) {
            Ok(m) => ensure!(!m.file_type().is_symlink(), "metric path contains symlink"),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound && !final_pass => return Ok(None),
            Err(e) => return Err(e.into()),
        }
    }
    let mut f = fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(current)?;
    let meta = f.metadata()?;
    ensure!(meta.is_file(), "metric source is not regular");
    let identity = format!("{}:{}", meta.dev(), meta.ino());
    let reset = identity != old.identity || meta.len() < old.offset;
    let from = if reset { 0 } else { old.offset };
    let generation = old.generation + u64::from(reset && !old.identity.is_empty());
    f.seek(SeekFrom::Start(from))?;
    let mut bytes = vec![];
    f.take(BATCH as u64).read_to_end(&mut bytes)?;
    if bytes.is_empty() {
        return Ok(None);
    }
    let discarding = old.discarding && !reset;
    let newline = if discarding {
        bytes.iter().position(|b| *b == b'\n').map(|n| n + 1)
    } else {
        bytes
            .iter()
            .enumerate()
            .filter(|(_, b)| **b == b'\n')
            .take(256)
            .last()
            .map(|(n, _)| n + 1)
    };
    let mut skipped = discarding || (newline.is_none() && (bytes.len() == BATCH || final_pass));
    let keep = match newline {
        Some(n) => n,
        None if skipped => bytes.len(),
        None => return Ok(None),
    };
    bytes.truncate(keep);
    let text = if skipped {
        String::new()
    } else {
        match String::from_utf8(bytes) {
            Ok(text) => text,
            Err(_) => {
                skipped = true;
                String::new()
            }
        }
    };
    Ok(Some(Batch {
        path: path.into(),
        identity,
        generation,
        from,
        next: from + keep as u64,
        length: meta.len(),
        text,
        skipped,
        discarding: newline.is_none(),
    }))
}

pub fn ingest(job: &Path, allowed: &[String], batch: &Batch) -> Result<()> {
    ensure!(
        allowed.contains(&batch.path),
        "undeclared JSONL metric source"
    );
    ensure!(
        batch.identity.len() <= 128
            && !batch.identity.is_empty()
            && batch.next > batch.from
            && batch.next - batch.from <= BATCH as u64
            && batch.next <= batch.length
            && batch.text.len() <= BATCH * 3,
        "invalid metric batch"
    );
    ensure!(
        if batch.skipped {
            batch.text.is_empty()
        } else {
            batch.text.len() as u64 == batch.next - batch.from
                && batch.text.ends_with('\n')
                && batch.text.lines().count() <= 256
                && !batch.discarding
        },
        "invalid metric framing"
    );
    let (root, _lock) = locked(job, Stream::Metrics)?;
    let mut s = state(job, Stream::Metrics)?;
    let old = s.sources.get(&batch.path).cloned().unwrap_or_default();
    if batch.generation < old.generation
        || (batch.generation == old.generation
            && batch.identity == old.identity
            && batch.next <= old.offset)
    {
        return Ok(());
    }
    if batch.generation == old.generation && batch.identity == old.identity {
        ensure!(batch.from == old.offset, "metric cursor gap");
    } else {
        ensure!(
            batch.from == 0
                && batch.generation == old.generation + u64::from(!old.identity.is_empty()),
            "metric rotation cursor mismatch"
        );
    }
    let mut records = Vec::new();
    if old.identity != batch.identity || batch.generation != old.generation {
        records.push(serde_json::json!({"kind":"jsonl_source","file":batch.path,"generation":batch.generation,"identity":batch.identity,"previous_offset":old.offset}));
    }
    if batch.skipped {
        records.push(serde_json::json!({"kind":"jsonl_gap","file":batch.path,"from":batch.from,"next":batch.next,"reason":"oversized or incomplete final line"}));
    } else {
        let mut offset = batch.from;
        for line in batch.text.split_inclusive('\n') {
            let next = offset + line.len() as u64;
            let record = if line.len() > 16 << 10 {
                None
            } else {
                serde_json::from_str::<serde_json::Value>(line)
                    .ok()
                    .filter(|v| v.is_object())
            };
            records.push(match record {
                Some(value)=>serde_json::json!({"kind":"jsonl","at":crate::jobs::now(),"file":batch.path,"identity":batch.identity,"generation":batch.generation,"offset":offset,"next":next,"record":value}),
                None=>serde_json::json!({"kind":"jsonl_gap","file":batch.path,"from":offset,"next":next,"reason":"invalid or oversized metric record"}),
            });
            offset = next;
        }
    }
    let mut bytes = Vec::new();
    for record in records {
        serde_json::to_writer(&mut bytes, &record)?;
        bytes.push(b'\n');
    }
    s.sources.insert(
        batch.path.clone(),
        Cursor {
            identity: batch.identity.clone(),
            generation: batch.generation,
            offset: batch.next,
            discarding: batch.discarding,
        },
    );
    append_locked(&root, &mut s, &bytes, SPOOL_BYTES)
}

pub fn capture(job: &Path, paths: &[String], final_pass: bool) -> Result<bool> {
    let mut complete = true;
    for path in paths {
        // A final pass drains bounded batches over subsequent agent polls rather than monopolizing IO.
        let old = state(job, Stream::Metrics)?
            .sources
            .get(path)
            .cloned()
            .unwrap_or_default();
        match read_batch(&job.join("work"), path, &old, final_pass) {
            Ok(Some(batch)) => {
                complete &= batch.next == batch.length;
                ingest(job, paths, &batch)?;
            }
            Ok(None) => (),
            Err(error) => {
                record_loss(job, Stream::Metrics, 0, &format!("JSONL {path}: {error}"))?;
            }
        }
    }
    Ok(complete)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn append_rotation_partial_records_and_replay_are_explicit() {
        let dir = super::super::tests::dir();
        fs::create_dir(dir.join("work")).unwrap();
        let path = dir.join("work/metrics.jsonl");
        fs::write(&path, b"{\"loss\":1}\n{\"loss\":").unwrap();
        let paths: Vec<String> = vec!["metrics.jsonl".into()];
        let batch = read_batch(&dir.join("work"), &paths[0], &Cursor::default(), false)
            .unwrap()
            .unwrap();
        ingest(&dir, &paths, &batch).unwrap();
        let before = state(&dir, Stream::Metrics).unwrap().observed;
        ingest(&dir, &paths, &batch).unwrap();
        assert_eq!(state(&dir, Stream::Metrics).unwrap().observed, before);
        fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"2}\n")
            .unwrap();
        assert!(capture(&dir, &paths, false).unwrap());
        fs::rename(&path, dir.join("work/old")).unwrap();
        fs::write(&path, b"{\"loss\":3}\n").unwrap();
        capture(&dir, &paths, false).unwrap();
        let s = state(&dir, Stream::Metrics).unwrap();
        assert_eq!(s.sources[&paths[0]].generation, 1);
        let text =
            String::from_utf8(read(&dir, Stream::Metrics, 0, READ_BYTES).unwrap().data).unwrap();
        assert_eq!(text.matches("\"kind\":\"jsonl\"").count(), 3);
        fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn oversized_line_tail_cannot_be_misinterpreted_as_a_new_record() {
        let dir = super::super::tests::dir();
        fs::create_dir(dir.join("work")).unwrap();
        let paths = vec![String::from("m.jsonl")];
        let mut bytes = vec![b'x'; BATCH];
        bytes.extend_from_slice(b"{\"fake\":1}\n{\"real\":2}\n");
        fs::write(dir.join("work/m.jsonl"), bytes).unwrap();
        assert!(!capture(&dir, &paths, false).unwrap());
        assert!(state(&dir, Stream::Metrics).unwrap().sources[&paths[0]].discarding);
        assert!(!capture(&dir, &paths, false).unwrap());
        assert!(capture(&dir, &paths, false).unwrap());
        let text =
            String::from_utf8(read(&dir, Stream::Metrics, 0, READ_BYTES).unwrap().data).unwrap();
        assert!(!text.contains("fake"));
        assert!(text.contains("real"));
        assert!(text.contains("jsonl_gap"));
        fs::remove_dir_all(dir).unwrap();
    }
}
