//! Snapshot-backed worker change delivery. This cache is not a second authority or Raft log:
//! losing/compacting it only costs a full snapshot. A worker confirms the current head, then
//! commits its verified complete image and cursor together before allowing new local effects.
use super::workload::WorkerView;
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, VecDeque};

#[cfg(test)]
pub(crate) mod tests;

pub const PAGE_BYTES: usize = 64 << 10;
const KEEP_IMAGES: usize = 3;
const KEEP_TRANSFERS: usize = 2;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Cursor {
    pub revision: u64,
    pub digest: String,
}

/// Historical resources remain in the catalog, but unchanged records don't travel each poll.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct Image(BTreeMap<String, Value>);

impl Image {
    pub fn from_view(view: &WorkerView) -> Result<Self> {
        let mut header = serde_json::to_value(view)?;
        let mut rows = BTreeMap::new();
        for field in ["assignments", "artifacts"] {
            let values = header[field]
                .take()
                .as_array()
                .context("invalid worker array")?
                .clone();
            for value in values {
                let id = if field == "artifacts" {
                    &value["manifest"]["id"]
                } else {
                    &value["id"]
                };
                let id = id.as_str().context("worker record has no ID")?;
                rows.insert(format!("{field}/{id}"), value);
            }
        }
        for field in ["history", "stores", "resume_checkpoints"] {
            let values = header[field]
                .take()
                .as_object()
                .context("invalid worker map")?
                .clone();
            for (id, value) in values {
                rows.insert(format!("{field}/{id}"), value);
            }
        }
        for field in ["operations", "networks", "bindings"] {
            let values = header["vm"][field]
                .take()
                .as_object()
                .context("invalid VM map")?
                .clone();
            for (id, value) in values {
                rows.insert(format!("vm/{field}/{id}"), value);
            }
        }
        rows.insert("header".into(), header);
        Ok(Self(rows))
    }

    pub fn view(&self) -> Result<WorkerView> {
        let mut value = self.0.get("header").context("missing sync header")?.clone();
        for field in ["assignments", "artifacts"] {
            value[field] = Value::Array(vec![]);
        }
        for field in ["history", "stores", "resume_checkpoints"] {
            value[field] = serde_json::json!({});
        }
        for field in ["operations", "networks", "bindings"] {
            value["vm"][field] = serde_json::json!({});
        }
        for (key, row) in &self.0 {
            if key == "header" {
                continue;
            }
            let (field, id) = key.split_once('/').context("invalid sync key")?;
            match field {
                "assignments" | "artifacts" => value[field]
                    .as_array_mut()
                    .context("worker array")?
                    .push(row.clone()),
                "history" | "stores" | "resume_checkpoints" => {
                    value[field][id] = row.clone();
                }
                "vm" => {
                    let (field, id) = id.split_once('/').context("invalid VM key")?;
                    ensure!(
                        ["operations", "networks", "bindings"].contains(&field),
                        "unknown VM collection"
                    );
                    value["vm"][field][id] = row.clone();
                }
                _ => anyhow::bail!("unknown sync collection"),
            }
        }
        Ok(serde_json::from_value(value)?)
    }

    fn digest(&self) -> Result<String> {
        Ok(blake3::hash(&serde_json::to_vec(self)?)
            .to_hex()
            .to_string())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Saved {
    pub cursor: Cursor,
    pub image: Image,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Request {
    Poll { cursor: Option<Cursor> },
    Page { token: String, offset: u64 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Reply {
    Current {
        cursor: Cursor,
    },
    Page {
        token: String,
        offset: u64,
        total: u64,
        bytes: Vec<u8>,
    },
    /// Lost leader cache / compacted transfer: restart from the last durable image.
    Reset,
}

#[derive(Debug, Serialize, Deserialize)]
struct Batch {
    base: Option<Cursor>,
    target: Cursor,
    changes: BTreeMap<String, Option<Value>>,
}

#[derive(Default)]
struct History {
    images: VecDeque<Saved>,
    transfers: VecDeque<(String, Vec<u8>)>,
}

#[derive(Default)]
pub struct Cache(BTreeMap<String, History>);

impl Cache {
    pub fn continuation(&self, worker: &str, token: &str, offset: u64) -> Reply {
        self.0
            .get(worker)
            .map_or(Reply::Reset, |h| Self::page(h, token, offset))
    }
    /// Caller has authenticated this worker incarnation and performed a quorum-confirmed read.
    pub fn answer(
        &mut self,
        worker: &str,
        revision: u64,
        view: &WorkerView,
        request: Request,
    ) -> Result<Reply> {
        let history = self.0.entry(worker.into()).or_default();
        match request {
            Request::Page { token, offset } => Ok(Self::page(history, &token, offset)),
            Request::Poll { cursor } => {
                if let Some(cursor) = &cursor {
                    ensure!(
                        cursor.revision <= revision,
                        "worker sync revision regressed"
                    );
                }
                let image = Image::from_view(view)?;
                let digest = image.digest()?;
                let target = Cursor { revision, digest };
                ensure!(
                    cursor
                        .as_ref()
                        .is_none_or(|c| c.revision != revision || c.digest == target.digest),
                    "worker image changed at the same committed revision"
                );
                let base = cursor
                    .as_ref()
                    .and_then(|c| history.images.iter().find(|s| &s.cursor == c));
                let current = cursor.as_ref().is_some_and(|c| c.digest == target.digest);
                let bytes = if current {
                    None
                } else {
                    let mut changes = BTreeMap::new();
                    for (key, value) in &image.0 {
                        if base.is_none_or(|b| b.image.0.get(key) != Some(value)) {
                            changes.insert(key.clone(), Some(value.clone()));
                        }
                    }
                    if let Some(base) = base {
                        for key in base.image.0.keys().filter(|k| !image.0.contains_key(*k)) {
                            changes.insert(key.clone(), None);
                        }
                    }
                    Some(serde_json::to_vec(&Batch {
                        base: base.map(|b| b.cursor.clone()),
                        target: target.clone(),
                        changes,
                    })?)
                };
                // Unrelated cluster activity doesn't change this worker's delivery position.
                let target = if current {
                    cursor.expect("current cursor")
                } else {
                    target
                };
                history.images.retain(|s| s.cursor != target);
                history.images.push_back(Saved {
                    cursor: target.clone(),
                    image,
                });
                while history.images.len() > KEEP_IMAGES {
                    history.images.pop_front();
                }
                if let Some(bytes) = bytes {
                    let token = blake3::hash(&bytes).to_hex().to_string();
                    history.transfers.retain(|(old, _)| old != &token);
                    history.transfers.push_back((token.clone(), bytes));
                    while history.transfers.len() > KEEP_TRANSFERS {
                        history.transfers.pop_front();
                    }
                    Ok(Self::page(history, &token, 0))
                } else {
                    Ok(Reply::Current { cursor: target })
                }
            }
        }
    }

    fn page(history: &History, token: &str, offset: u64) -> Reply {
        let Some((_, bytes)) = history.transfers.iter().find(|(id, _)| id == token) else {
            return Reply::Reset;
        };
        let Ok(start) = usize::try_from(offset) else {
            return Reply::Reset;
        };
        if start >= bytes.len() || start % PAGE_BYTES != 0 {
            return Reply::Reset;
        }
        let end = start.saturating_add(PAGE_BYTES).min(bytes.len());
        Reply::Page {
            token: token.into(),
            offset,
            total: bytes.len() as u64,
            bytes: bytes[start..end].to_vec(),
        }
    }
}

#[derive(Default)]
struct Download {
    token: String,
    total: u64,
    bytes: Vec<u8>,
}

/// Partial downloads are expendable. The accepted image/cursor is durable in worker.redb;
/// a process restart resumes from that image and cannot execute a partially received update.
#[derive(Default)]
pub struct Receiver {
    accepted: Option<Saved>,
    staged: Option<Saved>,
    download: Option<Download>,
    reset: bool,
}

impl Receiver {
    pub fn new(accepted: Option<Saved>) -> Result<Self> {
        if let Some(saved) = &accepted {
            ensure!(
                saved.image.digest()? == saved.cursor.digest,
                "corrupt durable sync image"
            );
            saved.image.view()?;
        }
        Ok(Self {
            accepted,
            ..Self::default()
        })
    }
    pub fn request(&self) -> Request {
        if let Some(download) = &self.download {
            Request::Page {
                token: download.token.clone(),
                offset: download.bytes.len() as u64,
            }
        } else {
            Request::Poll {
                cursor: if self.reset {
                    None
                } else {
                    self.staged
                        .as_ref()
                        .or(self.accepted.as_ref())
                        .map(|s| s.cursor.clone())
                },
            }
        }
    }
    pub fn pending(&self) -> bool {
        self.download.is_some() || self.staged.is_some() || self.reset
    }
    pub fn require_refresh(&mut self) {
        self.download = None;
        self.staged = None;
        self.reset = true;
    }
    pub fn commit(&mut self, saved: Saved) {
        self.accepted = Some(saved);
        self.staged = None;
        self.download = None;
        self.reset = false;
    }
    pub fn receive(&mut self, reply: Reply) -> Result<Option<Saved>> {
        match reply {
            Reply::Reset => {
                self.download = None;
                self.staged = None;
                self.reset = true;
                Ok(None)
            }
            Reply::Current { cursor } => {
                ensure!(
                    self.download.is_none() && !self.reset,
                    "unexpected sync head confirmation"
                );
                let saved = self
                    .staged
                    .as_ref()
                    .or(self.accepted.as_ref())
                    .context("sync head without image")?;
                ensure!(saved.cursor == cursor, "sync head cursor mismatch");
                Ok(Some(saved.clone()))
            }
            Reply::Page {
                token,
                offset,
                total,
                bytes,
            } => {
                ensure!(
                    !bytes.is_empty() && bytes.len() <= PAGE_BYTES,
                    "invalid sync page size"
                );
                let download = self.download.get_or_insert_with(|| Download {
                    token: token.clone(),
                    total,
                    bytes: vec![],
                });
                ensure!(
                    download.token == token
                        && download.total == total
                        && download.bytes.len() as u64 == offset,
                    "sync page changed or out of order"
                );
                ensure!(
                    offset
                        .checked_add(bytes.len() as u64)
                        .is_some_and(|end| end <= total),
                    "invalid sync page extent"
                );
                download.bytes.extend(bytes);
                if download.bytes.len() as u64 != total {
                    return Ok(None);
                }
                ensure!(
                    blake3::hash(&download.bytes).to_hex().as_str() == token,
                    "sync transfer hash mismatch"
                );
                let batch: Batch = serde_json::from_slice(&download.bytes)?;
                let old = self.staged.as_ref().or(self.accepted.as_ref());
                ensure!(
                    old.is_none_or(
                        |s| batch.target.revision > s.cursor.revision || batch.target == s.cursor
                    ),
                    "sync image regressed"
                );
                let mut image = if let Some(base) = &batch.base {
                    let old = old.context("delta has no base")?;
                    ensure!(&old.cursor == base && !self.reset, "delta base mismatch");
                    old.image.clone()
                } else {
                    Image::default()
                };
                for (key, value) in batch.changes {
                    match value {
                        Some(value) => {
                            image.0.insert(key, value);
                        }
                        None => {
                            image.0.remove(&key);
                        }
                    }
                }
                ensure!(
                    image.digest()? == batch.target.digest,
                    "sync image hash mismatch"
                );
                image.view()?;
                self.staged = Some(Saved {
                    cursor: batch.target,
                    image,
                });
                self.download = None;
                self.reset = false;
                // Always poll the current head after a transfer. Changes during pagination
                // (especially cancellation/recovery holds) are folded in before effects.
                Ok(None)
            }
        }
    }
}
