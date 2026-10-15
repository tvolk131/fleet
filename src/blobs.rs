//! Content-addressed files over `iroh-blobs`: each side keeps a store; the agent's accepts pushes (inputs) and serves
//! outputs only to allowlisted coordinators; the coordinator pushes inputs and fetches outputs.

use std::{collections::HashSet, path::Path, sync::Arc};

use anyhow::{Context, Result};
use iroh::{Endpoint, EndpointAddr, EndpointId, endpoint::Connection};
use iroh_blobs::{
    BlobsProtocol, Hash, HashAndFormat,
    api::Store,
    protocol::{ChunkRangesSeq, PushRequest},
    provider::events::{
        AbortReason, ConnectMode, EventMask, EventSender, ProviderMessage, RequestMode,
    },
    store::fs::FsStore,
};

pub async fn open(path: &Path) -> Result<Store> {
    std::fs::create_dir_all(path)?;
    let store = FsStore::load(path)
        .await
        .with_context(|| format!("opening the blob store {}", path.display()))?;
    Ok(store.into())
}

/// The blobs protocol for an agent: connections from keys not on the allowlist are refused before any request, and
/// allowlisted coordinators may both fetch and push.
pub fn protocol(store: &Store, allow: Arc<HashSet<EndpointId>>) -> BlobsProtocol {
    let mask = EventMask {
        connected: ConnectMode::Intercept,
        push: RequestMode::None,
        ..EventMask::DEFAULT
    };
    let (events, mut rx) = EventSender::channel(32, mask);
    tokio::spawn(async move {
        while let Some(message) = rx.recv().await {
            if let ProviderMessage::ClientConnected(message) = message {
                let allowed = message.endpoint_id.is_some_and(|id| allow.contains(&id));
                if !allowed {
                    tracing::warn!(peer = ?message.endpoint_id, "refused a blobs connection from a key not on the allowlist");
                }
                message
                    .tx
                    .send(if allowed {
                        Ok(())
                    } else {
                        Err(AbortReason::Permission)
                    })
                    .await
                    .ok();
            }
        }
    });
    BlobsProtocol::new(store, Some(events))
}

/// Pushes blobs from the local store to a peer's store. Returns the connection, which must stay open until the peer
/// has finished importing (closing it early aborts the import): confirm arrival, then close it.
pub async fn push(
    store: &Store,
    endpoint: &Endpoint,
    to: EndpointAddr,
    hashes: &[Hash],
) -> Result<Option<Connection>> {
    if hashes.is_empty() {
        return Ok(None);
    }
    let connection = endpoint
        .connect(to, iroh_blobs::ALPN)
        .await
        .map_err(crate::proto::transport)?;
    for &hash in hashes {
        store
            .remote()
            .execute_push(
                connection.clone(),
                PushRequest::new(hash, ChunkRangesSeq::root()),
            )
            .await
            .with_context(|| format!("pushing {hash}"))?;
    }
    Ok(Some(connection))
}

/// Fetches blobs from a peer into the local store; each is verified against its hash as it arrives.
pub async fn fetch(
    store: &Store,
    endpoint: &Endpoint,
    from: EndpointAddr,
    hashes: &[Hash],
) -> Result<()> {
    if hashes.is_empty() {
        return Ok(());
    }
    let connection = endpoint
        .connect(from, iroh_blobs::ALPN)
        .await
        .map_err(crate::proto::transport)?;
    for &hash in hashes {
        store
            .remote()
            .fetch(connection.clone(), HashAndFormat::raw(hash))
            .await
            .map_err(|e| {
                if connection.close_reason().is_some() {
                    crate::proto::refusal(&connection, e.into())
                } else {
                    anyhow::Error::from(e)
                }
            })
            .with_context(|| format!("fetching {hash}"))?;
    }
    connection.close(0u32.into(), b"done");
    Ok(())
}

/// The BLAKE3 hash of a file, as `iroh-blobs` computes it, read in a streaming way.
pub fn hash_file(path: &Path) -> Result<Hash> {
    let mut hasher = blake3::Hasher::new();
    hasher.update_reader(std::fs::File::open(path)?)?;
    Ok(Hash::from_bytes(*hasher.finalize().as_bytes()))
}
