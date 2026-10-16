use super::{
    State,
    artifact::Kind,
    client::AppClient,
    runtime::{DataRequest, data_call},
    workload::Assignment,
};
use crate::history::{Block, Stream};
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};

pub(super) fn attempt<'a>(
    state: &'a State,
    id: &str,
    number: Option<u32>,
) -> Result<&'a Assignment> {
    let j = state.jobs.get(id).context("unknown job")?;
    if let Some(n) = number {
        ensure!(n > 0, "attempt numbers begin at 1");
        j.attempts
            .get((n - 1) as usize)
            .and_then(|id| state.assignments.get(id.as_ref()))
    } else {
        j.current(state)
    }
    .context("job has no such attempt")
}
pub(super) fn summary(state: &State, id: &str) -> Result<Value> {
    let job = state.jobs.get(id).context("unknown job")?;
    let attempts:Vec<_>=job.attempts.iter().filter_map(|id|state.assignments.get(id.as_ref())).map(|a|{
        let streams:Vec<_>=[Stream::Stdout,Stream::Stderr,Stream::Metrics].into_iter().map(|stream|{
            let mut chunks:Vec<_>=state.storage.artifacts.values().filter(|m|m.manifest.attempt()==Some(a.id.as_str()) && matches!(m.manifest.kind,Kind::History{stream:s,..} if s==stream)).collect();
            chunks.sort_by_key(|m|if let Kind::History{start,..}=m.manifest.kind{start}else{0});
            let retained:u64=chunks.iter().filter(|m|!m.deleted).map(|m|m.manifest.bytes()).sum();
            let backed:u64=chunks.iter().filter(|m|state.durable_artifact(m)).map(|m|m.manifest.bytes()).sum();
            json!({"stream":stream,"progress":state.history.attempts.get(&a.id).and_then(|s|s.get(&stream)),"retained_bytes":retained,"backed_up_bytes":backed,
                "chunks":chunks.into_iter().map(|m|json!({"id":m.manifest.id,"range":m.manifest.kind,"revision":m.revision,"pinned":m.pinned,"expires_at":m.expires_at,"deleted":m.deleted,"backed_up":state.durable_artifact(m),"copies":m.copies})).collect::<Vec<_>>()})
        }).collect();
        json!({"id":a.id,"attempt":a.attempt,"worker":a.worker,"execution":a.execution,"observed":a.observed,"streams":streams})
    }).collect();
    Ok(
        json!({"job":id,"retention":state.history_settings(id),"attempts":attempts,"storage_targets":state.storage.policy.history}),
    )
}

impl AppClient {
    pub(super) async fn history_read(
        &self,
        state: &State,
        a: &Assignment,
        stream: Stream,
        from: u64,
        max: u32,
    ) -> Result<Value> {
        ensure!(max >= 4, "history page size must be at least four bytes");
        let progress = state
            .history
            .attempts
            .get(&a.id)
            .and_then(|s| s.get(&stream));
        let published = progress.map_or(0, |p| p.published);
        let observed = progress.map_or(0, |p| p.observed.max(p.published));
        let closed = progress.is_some_and(|p| p.closed);
        let mut chunks:Vec<_>=state.storage.artifacts.values().filter(|m|m.manifest.attempt()==Some(a.id.as_str()) && matches!(m.manifest.kind,Kind::History{stream:s,end,..} if s==stream && end>from)).collect();
        chunks.sort_by_key(|m| {
            if let Kind::History { start, .. } = m.manifest.kind {
                start
            } else {
                0
            }
        });
        if let Some(chunk) = chunks.first() {
            let Kind::History { start, end, .. } = chunk.manifest.kind else {
                unreachable!()
            };
            if start <= from && !chunk.deleted {
                let destination = self
                    .home
                    .join("downloads")
                    .join(format!("history-{:032x}", rand::random::<u128>()));
                std::fs::create_dir_all(destination.parent().expect("history download parent"))?;
                let result=async {
                    super::artifact_io::fetch_available(&self.transport.endpoint,state,chunk,&destination).await?;
                    let bytes=std::fs::read(destination.join("data"))?;
                    let offset=(from-start) as usize;
                    let n=(max as usize).clamp(1,crate::history::READ_BYTES).min(bytes.len()-offset);
                    let (data,next)=crate::agent::log_text(&bytes[offset..offset+n],from+n as u64,end);
                    Ok::<_,anyhow::Error>(json!({"data":data,"next":next,"done":closed && next>=observed,"gap":null,"archived":state.durable_artifact(chunk),"observed":observed}))
                }.await;
                if destination.exists() {
                    std::fs::remove_dir_all(destination)?;
                }
                return result;
            }
            if from < published {
                let next = if start > from { start } else { end };
                return Ok(
                    json!({"data":"","next":next,"done":closed && next>=observed,"gap":Block{start:from,end:next},"reason":"history not retained","observed":observed}),
                );
            }
        }
        if from < published {
            return Ok(
                json!({"data":"","next":published,"done":closed && published>=observed,"gap":Block{start:from,end:published},"reason":"history not retained","observed":observed}),
            );
        }
        if closed
            && from >= progress.map_or(0, |p| p.captured_end).max(published)
            && from < observed
        {
            return Ok(
                json!({"data":"","next":observed,"done":true,"gap":Block{start:from,end:observed},"reason":"discarded during capture","observed":observed}),
            );
        }
        if closed && from == observed {
            return Ok(json!({"data":"","next":from,"done":true,"gap":null,"observed":observed}));
        }
        let worker = state
            .workers
            .get(&a.worker)
            .context("unknown attempt worker")?;
        let crate::proto::Response::History(r) = data_call(
            &self.transport.endpoint,
            &state.cluster,
            &worker.peer,
            DataRequest::History {
                execution: a.execution.clone(),
                stream,
                from,
                max,
            },
        )
        .await
        .context("unarchived history is unavailable on its worker")?
        else {
            anyhow::bail!("unexpected history response")
        };
        let (data, next) = crate::agent::log_text(
            &r.data,
            r.next,
            if r.data.len() < (max as usize).clamp(1, crate::history::READ_BYTES) {
                r.next
            } else {
                r.observed
            },
        );
        Ok(
            json!({"data":data,"next":next,"done":r.closed && next>=r.observed,"gap":r.gap,"archived":false,"observed":r.observed}),
        )
    }
}
