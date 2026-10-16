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

/// Reserve the known content hash before import. If an earlier sweep missed this tag, the later
/// import is still protected as a write; if it preceded the sweep, the durable tag is a root.
pub async fn import_named(store: &Store, path: &Path, hash: Hash, tag: String) -> Result<()> {
    store.tags().set(&tag, HashAndFormat::raw(hash)).await?;
    let imported = store.blobs().add_path(path).with_named_tag(tag).await?;
    anyhow::ensure!(
        imported.hash == hash,
        "file changed while importing into the blob cache"
    );
    Ok(())
}

/// Managed stores collect only explicitly retired artifact hashes. Other cache entries remain
/// protected, including inputs reserved before network transfer. Restart begins with no candidates.
pub struct Reclamation {
    pub candidates: Arc<std::sync::RwLock<HashSet<Hash>>>,
    _store: Arc<Store>,
}

pub async fn open_managed(path: &Path) -> Result<(Store, Reclamation)> {
    use iroh_blobs::store::{GcConfig, ProtectOutcome, fs::options::Options};
    let candidates = Arc::new(std::sync::RwLock::new(HashSet::<Hash>::new()));
    let holder = Arc::new(std::sync::Mutex::new(std::sync::Weak::<Store>::new()));
    let mut options = Options::new(path);
    options.gc = Some(GcConfig {
        interval: std::time::Duration::from_secs(2),
        add_protected: Some(Arc::new({
            let candidates = candidates.clone();
            let holder = holder.clone();
            move |live| {
                let candidates = candidates.read().expect("GC candidates").clone();
                let store = holder.lock().expect("GC store").upgrade();
                Box::pin(async move {
                    let Some(store) = store.filter(|_| !candidates.is_empty()) else {
                        return ProtectOutcome::Abort;
                    };
                    match tokio::spawn(async move { store.blobs().list().hashes().await }).await {
                        Ok(Ok(hashes)) => {
                            live.extend(hashes.into_iter().filter(|h| !candidates.contains(h)));
                            ProtectOutcome::Continue
                        }
                        _ => ProtectOutcome::Abort,
                    }
                })
            }
        })),
    });
    std::fs::create_dir_all(path)?;
    let store: Store = FsStore::load_with_opts(path.join("blobs.db"), options)
        .await?
        .into();
    let held = Arc::new(store.clone());
    *holder.lock().expect("GC store") = Arc::downgrade(&held);
    Ok((
        store,
        Reclamation {
            candidates,
            _store: held,
        },
    ))
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

#[cfg(test)]
mod reclamation_tests {
    use super::*;
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn managed_gc_reclaims_retired_hashes_but_preserves_shared_reserved_and_unknown_blobs() {
        let path =
            std::env::temp_dir().join(format!("fleet-blob-gc-{:032x}", rand::random::<u128>()));
        let (store, gc) = open_managed(&path).await.unwrap();
        let retired = store
            .blobs()
            .add_slice(vec![1; 128 << 10])
            .with_named_tag("checkpoint/old")
            .await
            .unwrap();
        let shared = store
            .blobs()
            .add_slice(vec![2; 128 << 10])
            .with_named_tag("checkpoint/shared")
            .await
            .unwrap();
        store
            .tags()
            .set("output/live", HashAndFormat::raw(shared.hash))
            .await
            .unwrap();
        let input = store
            .blobs()
            .add_slice(vec![3; 128 << 10])
            .with_named_tag("input/reserved")
            .await
            .unwrap();
        let unknown = store.blobs().add_slice(vec![4; 128 << 10]).await.unwrap();
        store.tags().delete(unknown.name).await.unwrap();
        store.tags().delete("checkpoint/old").await.unwrap();
        store.tags().delete("checkpoint/shared").await.unwrap();
        gc.candidates
            .write()
            .unwrap()
            .extend([retired.hash, shared.hash, input.hash]);
        let end = tokio::time::Instant::now() + std::time::Duration::from_secs(12);
        while store.blobs().has(retired.hash).await.unwrap() {
            assert!(
                tokio::time::Instant::now() < end,
                "GC did not reclaim retired bytes"
            );
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        for hash in [shared.hash, input.hash, unknown.hash] {
            assert!(store.blobs().has(hash).await.unwrap());
        }
        // A new input with a previously collected hash is reserved before uploading and remains
        // safe while periodic GC and the upload overlap, even with an older candidate snapshot.
        store
            .tags()
            .set("input/new", HashAndFormat::raw(retired.hash))
            .await
            .unwrap();
        let imported = store
            .blobs()
            .add_slice(vec![1; 128 << 10])
            .temp_tag()
            .await
            .unwrap();
        assert_eq!(imported.hash(), retired.hash);
        drop(imported);
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        assert!(store.blobs().has(retired.hash).await.unwrap());
        drop(gc);
        drop(store);
        let _ = std::fs::remove_dir_all(path);
    }
}
