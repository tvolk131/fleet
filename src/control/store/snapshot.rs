//! Disk-backed Raft snapshots. Generation four retains its JSON wire format; generation five
//! streams a header and sorted rows. Snapshot chunks and applied state publish atomically in redb.
use super::*;
use std::io::{BufRead, BufReader, BufWriter, Read, Seek, Write};
use std::os::unix::fs::OpenOptionsExt;

const MAGIC: &[u8] = b"FLEET14\n";
const RECORD_LIMIT: u64 = 16 << 20;
const CHUNKS: TableDefinition<u64, &[u8]> = TableDefinition::new("snapshot_chunks");
const CHUNK: usize = 64 << 10;
pub const MAX_STREAM: u64 = 64 << 30;

#[derive(Serialize, Deserialize)]
struct Saved {
    meta: SnapshotMeta<u64, BasicNode>,
    bytes: u64,
}

/// Unlinked, private temporary file: bounded memory, no abandoned pathname after a crash.
pub fn temporary() -> Result<std::fs::File> {
    let path = std::env::temp_dir().join(format!("fleet-snapshot-{:032x}", rand::random::<u128>()));
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&path)?;
    std::fs::remove_file(path)?;
    Ok(file)
}

fn digest(file: &mut std::fs::File) -> Result<String> {
    file.rewind()?;
    let mut hash = blake3::Hasher::new();
    let mut buf = vec![0; CHUNK];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hash.update(&buf[..n]);
    }
    file.rewind()?;
    Ok(hash.finalize().to_hex().to_string())
}

fn line(file: &mut impl Write, value: &impl Serialize) -> Result<()> {
    let data = serde_json::to_vec(value)?;
    ensure!(
        data.len() < RECORD_LIMIT as usize,
        "snapshot record exceeds limit"
    );
    file.write_all(&data)?;
    file.write_all(b"\n")?;
    Ok(())
}

fn row(file: &mut impl Write, kind: u8, key: &str, value: &[u8]) -> Result<()> {
    let key = serde_json::to_vec(key)?;
    ensure!(
        key.len() + value.len() + 16 < RECORD_LIMIT as usize,
        "snapshot record exceeds limit"
    );
    // Values were validated at application/installation. Preserve their bytes instead of parsing
    // an entire permanent history merely to wrap each record for transfer.
    write!(file, "[{kind},")?;
    file.write_all(&key)?;
    file.write_all(b",")?;
    file.write_all(value)?;
    file.write_all(b"]\n")?;
    Ok(())
}

fn next(reader: &mut BufReader<std::fs::File>) -> Result<Option<Vec<u8>>> {
    let mut data = Vec::new();
    let n = reader.take(RECORD_LIMIT + 1).read_until(b'\n', &mut data)?;
    if n == 0 {
        return Ok(None);
    }
    ensure!(
        n <= RECORD_LIMIT as usize && data.last() == Some(&b'\n'),
        "oversized or truncated snapshot record"
    );
    data.pop();
    Ok(Some(data))
}

fn save(
    tx: &redb::WriteTransaction,
    file: &mut std::fs::File,
    meta: &SnapshotMeta<u64, BasicNode>,
    indexed: bool,
) -> Result<()> {
    file.rewind()?;
    let bytes = file.metadata()?.len();
    ensure!(bytes <= MAX_STREAM, "snapshot disk budget exceeded");
    let mut chunks = tx.open_table(CHUNKS)?;
    chunks.retain(|_, _| false)?;
    if !indexed {
        // Before activation, the previous executable must still be able to reopen this database.
        ensure!(
            bytes <= MAX_SNAPSHOT as u64,
            "legacy snapshot exceeds limit"
        );
        let mut data = Vec::new();
        file.read_to_end(&mut data)?;
        let mut table = tx.open_table(META)?;
        put(
            &mut table,
            "snapshot",
            &SavedSnapshot {
                meta: meta.clone(),
                hash: meta.snapshot_id.clone(),
                data,
            },
        )?;
        table.remove("stream_snapshot")?;
        file.rewind()?;
        return Ok(());
    }
    let mut buffer = vec![0; CHUNK];
    let mut offset = 0;
    while offset < bytes {
        let size = usize::try_from((bytes - offset).min(CHUNK as u64))?;
        file.read_exact(&mut buffer[..size])?;
        chunks.insert(offset, &buffer[..size])?;
        offset += size as u64;
    }
    let mut table = tx.open_table(META)?;
    put(
        &mut table,
        "stream_snapshot",
        &Saved {
            meta: meta.clone(),
            bytes,
        },
    )?;
    table.remove("snapshot")?;
    file.rewind()?;
    Ok(())
}

pub(super) fn build(db: &Database) -> Result<Snapshot<TypeConfig>> {
    let read_tx = db.begin_read()?;
    let table = read_tx.open_table(META)?;
    let header: State = read(&table, "state")?.context("missing state")?;
    let (format, _, _): (u32, String, u64) =
        read(&table, "identity")?.context("missing identity")?;
    let mut file = temporary()?;
    let mut writer = BufWriter::with_capacity(CHUNK, &mut file);
    if format == indexed::FORMAT {
        writer.write_all(MAGIC)?;
        line(&mut writer, &header)?;
        for record in read_tx.open_table(indexed::RECEIPTS)?.iter()? {
            let (key, data) = record?;
            row(&mut writer, 0, key.value(), data.value())?;
        }
        for record in read_tx.open_table(indexed::ROWS)?.iter()? {
            let (key, data) = record?;
            let (kind, key) = key.value();
            row(&mut writer, kind, key, data.value())?;
        }
    } else {
        serde_json::to_writer(&mut writer, &header)?;
    }
    writer.flush()?;
    drop(writer);
    if format != indexed::FORMAT {
        ensure!(
            file.metadata()?.len() <= MAX_SNAPSHOT as u64,
            "legacy snapshot exceeds limit; activate indexed metadata"
        );
    }
    let meta = SnapshotMeta {
        last_log_id: header.last_applied,
        last_membership: header.membership,
        snapshot_id: digest(&mut file)?,
    };
    drop(table);
    drop(read_tx);
    let mut tx = db.begin_write()?;
    tx.set_durability(Durability::Immediate)?;
    {
        let table = tx.open_table(META)?;
        let existing = read::<Saved>(&table, "stream_snapshot")?
            .map(|s| s.meta)
            .or(read::<SavedSnapshot>(&table, "snapshot")?.map(|s| s.meta));
        if existing.is_some_and(|s| s.last_log_id > meta.last_log_id) {
            drop(table);
            drop(tx);
            return current(db)?.context("newer snapshot disappeared");
        }
    }
    save(&tx, &mut file, &meta, format == indexed::FORMAT)?;
    tx.commit()?;
    Ok(Snapshot {
        meta,
        snapshot: Box::new(tokio::fs::File::from_std(file)),
    })
}

pub(super) fn install(
    db: &Database,
    meta: SnapshotMeta<u64, BasicNode>,
    mut file: std::fs::File,
    fail: &AtomicBool,
) -> Result<()> {
    ensure!(
        file.metadata()?.len() <= MAX_STREAM,
        "snapshot disk budget exceeded"
    );
    ensure!(
        digest(&mut file)? == meta.snapshot_id,
        "snapshot digest mismatch"
    );
    let mut reader = BufReader::new(file);
    let stream = reader.fill_buf()?.starts_with(MAGIC);
    let state: State = if stream {
        reader.consume(MAGIC.len());
        serde_json::from_slice(&next(&mut reader)?.context("missing snapshot header")?)?
    } else {
        ensure!(
            reader.get_ref().metadata()?.len() <= MAX_SNAPSHOT as u64,
            "legacy snapshot exceeds limit"
        );
        serde_json::from_reader(&mut reader)?
    };
    let indexed = state.version.active >= indexed::GENERATION;
    ensure!(
        state.format == FORMAT
            && state.last_applied == meta.last_log_id
            && state.membership == meta.last_membership,
        "snapshot metadata mismatch"
    );
    ensure!(
        !stream || state.version.active >= indexed::GENERATION,
        "indexed snapshot before activation"
    );
    let mut tx = db.begin_write()?;
    tx.set_durability(Durability::Immediate)?;
    {
        let mut table = tx.open_table(META)?;
        let (_, cluster, node): (u32, String, u64) =
            read(&table, "identity")?.context("missing identity")?;
        ensure!(cluster == state.cluster, "snapshot cluster mismatch");
        let old: State = read(&table, "state")?.context("missing state")?;
        ensure!(
            state.version.active >= old.version.active
                && state.version.revision >= old.version.revision,
            "snapshot cannot roll back the cluster generation or upgrade revision"
        );
        super::super::version::Software::current().require(state.version.required(node))?;
        let prepared: Option<(u64, u32)> = read(&table, "version_prepared")?;
        if prepared.is_some_and(|(r, t)| {
            state.version.revision != r
                || state.version.pending.as_ref().is_none_or(|p| p.target != t)
        }) {
            table.remove("version_prepared")?;
        }
        let format = if state.version.active >= indexed::GENERATION {
            indexed::FORMAT
        } else {
            FORMAT
        };
        put(&mut table, "identity", &(format, cluster, node))?;
        put(&mut table, "state", &state)?;
    }
    // Replacement, not merge: a snapshot must never retain rows from a different applied image.
    tx.open_table(indexed::ROWS)?.retain(|_, _| false)?;
    tx.open_table(indexed::RECEIPTS)?.retain(|_, _| false)?;
    if stream {
        ensure!(
            state.receipts.is_empty(),
            "receipt rows embedded in indexed header"
        );
        let value = serde_json::to_value(&state)?;
        for collection in indexed::COLLECTIONS {
            ensure!(
                value
                    .pointer(collection.path())
                    .and_then(|v| v.as_object())
                    .is_some_and(|m| m.is_empty()),
                "resource rows embedded in indexed header"
            );
        }
        let mut previous: Option<(u8, String)> = None;
        while let Some(data) = next(&mut reader)? {
            let (kind, key, value): (u8, String, serde_json::Value) =
                serde_json::from_slice(&data)?;
            let order = (kind, key.clone());
            ensure!(
                previous.as_ref().is_none_or(|p| p < &order),
                "duplicate or unordered snapshot row"
            );
            previous = Some(order);
            if kind == 0 {
                let _: (String, String) = serde_json::from_str(&key)?;
                let receipt: super::super::model::Receipt = serde_json::from_value(value)?;
                put(&mut tx.open_table(indexed::RECEIPTS)?, &key, &receipt)?;
            } else {
                ensure!(
                    indexed::COLLECTIONS.iter().any(|c| *c as u8 == kind),
                    "unknown snapshot collection"
                );
                tx.open_table(indexed::ROWS)?
                    .insert((kind, key.as_str()), serde_json::to_vec(&value)?.as_slice())?;
            }
        }
        // Type-check the resource image before publishing. Permanent receipts are never hydrated.
        indexed::hydrate(state, &tx.open_table(indexed::ROWS)?)?;
    } else if state.version.active >= indexed::GENERATION {
        indexed::persist(&tx, &state)?;
    }
    let mut file = reader.into_inner();
    save(&tx, &mut file, &meta, indexed)?;
    ensure!(
        !fail.swap(false, Ordering::SeqCst),
        "injected snapshot commit failure"
    );
    tx.commit()?;
    Ok(())
}

pub(super) fn current(db: &Database) -> Result<Option<Snapshot<TypeConfig>>> {
    let tx = db.begin_read()?;
    let table = tx.open_table(META)?;
    let mut file = temporary()?;
    let meta = if let Some(saved) = read::<Saved>(&table, "stream_snapshot")? {
        let mut offset = 0;
        for chunk in tx.open_table(CHUNKS)?.iter()? {
            let (key, value) = chunk?;
            ensure!(key.value() == offset, "snapshot chunk missing");
            file.write_all(value.value())?;
            offset += value.value().len() as u64;
        }
        ensure!(offset == saved.bytes, "snapshot length mismatch");
        saved.meta
    } else if let Some(saved) = read::<SavedSnapshot>(&table, "snapshot")? {
        ensure!(
            saved.hash == snapshot_id(&saved.data),
            "snapshot digest mismatch"
        );
        file.write_all(&saved.data)?;
        saved.meta
    } else {
        return Ok(None);
    };
    ensure!(
        digest(&mut file)? == meta.snapshot_id,
        "snapshot digest mismatch"
    );
    Ok(Some(Snapshot {
        meta,
        snapshot: Box::new(tokio::fs::File::from_std(file)),
    }))
}
