use super::*;
use crate::control::artifact::{CopyPhase, Kind, Manifest};
use crate::history::{self, Stream};

#[derive(Debug, Default, Serialize, Deserialize)]
struct Sealed {
    offsets: BTreeMap<Stream, u64>,
    at: BTreeMap<Stream, u64>,
}
impl Worker {
    pub(super) async fn reconcile_history(&mut self) -> Result<()> {
        let view = self.storage_view.clone().context("no history view")?;
        let saved = self.journal.read()?;
        if view.cleanup_allowed {
            let retired:Vec<_>=saved.artifacts.values().filter(|m|saved.deleted_artifacts.contains(&m.id)
                && !view.artifacts.iter().any(|a|a.manifest.id==m.id)
                && matches!(m.kind,Kind::History{stream,end,..} if view.history.get(m.attempt().unwrap_or("")).and_then(|s|s.get(&stream)).is_some_and(|p|p.published>=end)))
                .map(|m|m.id.clone()).collect();
            for id in &retired {
                let root = self.artifact_root(&saved.artifacts[id])?;
                if root.exists() {
                    std::fs::remove_dir_all(root)?;
                }
            }
            if !retired.is_empty() {
                self.journal.change(|s| {
                    for id in &retired {
                        s.artifacts.remove(id);
                        s.deleted_artifacts.remove(id);
                    }
                    Ok(())
                })?;
            }
        }
        for a in &view.assignments {
            if a.attempt.is_none() {
                continue;
            }
            let dir = jobs::jobs_dir(&self.home.join("backend")).join(&a.execution);
            if !dir.join("job.json").exists() {
                continue;
            }
            let index = dir.join("history-sealed.json");
            let mut sealed: Sealed = jobs::read_json(&index)?.unwrap_or_default();
            // Journal publication precedes the local cursor. Repair an interrupted cursor update.
            for m in saved
                .artifacts
                .values()
                .filter(|m| m.attempt() == Some(a.id.as_str()))
            {
                if let Kind::History { stream, end, .. } = m.kind {
                    let offset = sealed.offsets.entry(stream).or_default();
                    *offset = (*offset).max(end);
                }
            }
            for stream in [Stream::Stdout, Stream::Stderr, Stream::Metrics] {
                if !history::root(&dir, stream).join("state.json").exists() {
                    continue;
                }
                let local = history::state(&dir, stream)?;
                let report = super::super::history::Report {
                    captured_end: local.captured_end,
                    revision: local.revision,
                    error: local.error.clone(),
                    sources: local.sources.clone(),
                    worker: a.worker.clone(),
                    incarnation: self.config.registration.incarnation.clone(),
                    attempt: a.id.clone(),
                    stream,
                    observed: local.observed,
                    dropped: local.dropped,
                    closed: local.closed,
                };
                let old = view.history.get(&a.id).and_then(|s| s.get(&stream));
                if old.is_none_or(|p| {
                    p.observed != local.observed
                        || p.dropped != local.dropped
                        || p.closed != local.closed
                        || p.error != local.error
                        || p.sources != local.sources
                }) {
                    let reply = self
                        .client
                        .write(
                            Request {
                                origin: self.config.cluster.clone(),
                                id: format!(
                                    "history-report-{}",
                                    blake3::hash(&serde_json::to_vec(&report)?).to_hex()
                                ),
                                mutation: Mutation::HistoryReport(report),
                            },
                            Duration::from_secs(2),
                        )
                        .await?;
                    ensure!(
                        reply == Reply::Recorded,
                        "history report refused: {reply:?}"
                    );
                }
                // Only committed backup readiness or deletion permits source reclamation. Prefix
                // progression stops at the first unpublished/unprotected range.
                if view.cleanup_allowed {
                    let mut through = local.released;
                    let mut chunks: Vec<_> = view
                        .artifacts
                        .iter()
                        .filter(|m| {
                            m.manifest.attempt() == Some(a.id.as_str())
                                && matches!(m.manifest.kind,Kind::History{stream:s,..} if s==stream)
                        })
                        .collect();
                    chunks.sort_by_key(|m| {
                        if let Kind::History { start, .. } = m.manifest.kind {
                            start
                        } else {
                            0
                        }
                    });
                    for chunk in chunks {
                        let Kind::History { start, end, .. } = chunk.manifest.kind else {
                            unreachable!()
                        };
                        if end <= through {
                            continue;
                        }
                        // A gap before the next chunk is discarded capture, not outstanding bytes.
                        if local
                            .blocks
                            .iter()
                            .any(|b| b.end > through && b.start.max(through) < start)
                        {
                            break;
                        }
                        let ready = !chunk.targets.is_empty()
                            && chunk.targets.iter().all(|s| {
                                chunk.copies.get(&format!("storage/{s}")).is_some_and(|c| {
                                    c.phase == CopyPhase::Ready
                                        && c.authority == self.config.cluster
                                })
                            });
                        if !chunk.deleted && !ready {
                            break;
                        }
                        through = end;
                        if ready && !chunk.deleted {
                            let root = dir.join("history-archive").join(&chunk.manifest.id);
                            super::super::artifact_io::remove_owned_file(&root, "data")?;
                        }
                    }
                    if through > local.released {
                        history::release(&dir, stream, through)?;
                    }
                }
                let from = sealed.offsets.get(&stream).copied().unwrap_or(0);
                // Publish each stream in order, including after a lost controller reply.
                let published = old.map_or(0, |p| p.published);
                if saved.artifacts.values().any(|m| m.attempt()==Some(a.id.as_str()) && matches!(m.kind,Kind::History{stream:s,end,..} if s==stream && end>published)) {continue;}
                if !history::has_disk_headroom(&dir, history::BLOCK_BYTES)? {
                    let _ = history::record_loss(
                        &dir,
                        stream,
                        0,
                        "Archive: less than 1 GiB filesystem headroom",
                    );
                    continue;
                }
                if from >= local.observed {
                    continue;
                }
                let now = jobs::now();
                if !a.terminal()
                    && !local.closed
                    && local.observed - from < history::BLOCK_BYTES
                    && now.saturating_sub(sealed.at.get(&stream).copied().unwrap_or(0)) < 60
                {
                    continue;
                }
                // Pending payloads also count against a finite source budget; outages backpressure
                // archival while the producer's own bounded spool continues draining application IO.
                if super::super::storage::usage(&dir.join("history-archive"))?.file_bytes
                    >= history::SPOOL_BYTES * 3
                {
                    continue;
                }
                let mut read = history::read(&dir, stream, from, history::READ_BYTES)?;
                if read.gap.is_some() {
                    sealed.offsets.insert(stream, read.next);
                    instances::write(&index, &sealed)?;
                    continue;
                }
                if read.data.is_empty() {
                    continue;
                }
                let (_, mut end) = crate::agent::log_text(
                    &read.data,
                    read.next,
                    if local.closed {
                        local.observed
                    } else {
                        local.observed.saturating_add(1)
                    },
                );
                if end < read.next
                    && read.next < local.observed
                    && history::read(&dir, stream, read.next, 1)?.gap.is_some()
                {
                    end = read.next;
                }
                if end == from {
                    continue;
                }
                read.data.truncate((end - from) as usize);
                read.next = end;
                let m = Manifest::new(
                    a.worker.clone(),
                    a.id.clone(),
                    Kind::History {
                        stream,
                        start: from,
                        end: read.next,
                    },
                    vec![jobs::OutputFile {
                        path: "data".into(),
                        size: read.data.len() as u64,
                        hash: iroh_blobs::Hash::from_bytes(*blake3::hash(&read.data).as_bytes()),
                    }],
                )?;
                let root = dir.join("history-archive").join(&m.id);
                std::fs::create_dir_all(&root)?;
                let path = root.join("data");
                if !path.exists() {
                    use std::io::Write;
                    let temporary = root.join("data.tmp");
                    let mut f = std::fs::OpenOptions::new()
                        .create(true)
                        .truncate(true)
                        .write(true)
                        .open(&temporary)?;
                    f.write_all(&read.data)?;
                    f.sync_all()?;
                    std::fs::rename(temporary, &path)?;
                    std::fs::File::open(&root)?.sync_all()?;
                }
                super::super::artifact_io::verify(&root, &m)?;
                self.journal.change(|s| {
                    s.artifacts.insert(m.id.clone(), m.clone());
                    Ok(())
                })?;
                sealed.offsets.insert(stream, read.next);
                sealed.at.insert(stream, now);
                instances::write(&index, &sealed)?;
            }
        }
        Ok(())
    }
}
