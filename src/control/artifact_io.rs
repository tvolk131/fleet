//! Artifact-scoped authenticated streaming. Bulk bytes never pass through Raft.
use super::{
    artifact::{MAX_BYTES, Manifest},
    network::Peer,
};
use anyhow::{Context, Result, ensure};
use iroh::{
    Endpoint, EndpointId,
    endpoint::Connection,
    protocol::{AcceptError, ProtocolHandler},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
    time::Duration,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub const ALPN: &[u8] = b"fleet/artifact/1";
#[derive(Clone, Default, Debug)]
pub struct Access {
    pub readers: BTreeSet<EndpointId>,
    pub objects: BTreeMap<String, (Manifest, PathBuf)>,
}
#[derive(Clone, Debug)]
pub struct Handler {
    pub cluster: String,
    pub access: Arc<RwLock<Access>>,
    pub slots: Arc<tokio::sync::Semaphore>,
    pub authorization: Authorization,
}
/// Cached directory views cannot authorize a revoked agent. Each new transfer gets a quorum read.
#[derive(Clone)]
pub struct Authorization(Authority);
#[derive(Clone)]
enum Authority {
    Controller {
        client: Arc<tokio::sync::Mutex<super::Client>>,
        discovery: PathBuf,
    },
    /// The isolated upload/quota unit test stubs its controller dependency. Integration tests
    /// exercise real quorum authorization; this variant is absent from every production build.
    #[cfg(test)]
    UploadUnitTest,
}
impl std::fmt::Debug for Authorization {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ArtifactAuthorization")
    }
}
impl Authorization {
    pub fn new(
        endpoint: Endpoint,
        cluster: String,
        peers: BTreeMap<u64, Peer>,
        role_config: &Path,
    ) -> Result<Self> {
        let mut client = super::Client::from_endpoint(endpoint, cluster.clone(), peers)?;
        let discovery = super::discovery::incarnation_cache(role_config, &cluster);
        client.use_cache(discovery.clone())?;
        Ok(Self(Authority::Controller {
            client: Arc::new(tokio::sync::Mutex::new(client)),
            discovery,
        }))
    }
    #[cfg(test)]
    pub(super) fn upload_unit_test() -> Self {
        Self(Authority::UploadUnitTest)
    }
    pub async fn check(&self, reader: EndpointId, artifact: &str, upload: bool) -> Result<()> {
        #[cfg(not(test))]
        let Authority::Controller { client, discovery } = &self.0;
        #[cfg(test)]
        let (client, discovery) = match &self.0 {
            Authority::Controller { client, discovery } => (client, discovery),
            #[cfg(test)]
            Authority::UploadUnitTest => return Ok(()),
        };
        let answer = tokio::time::timeout(Duration::from_secs(5), async {
            let mut client = client.lock().await;
            // The role's regular polls learn replica rotations even when no artifact has been
            // served yet. Share that authenticated, authority-scoped cache before making a fresh
            // quorum read, rather than stranding this client on retired bootstrap replicas.
            if discovery.exists() {
                client.learn(serde_json::from_slice(&std::fs::read(discovery)?)?)?;
            }
            client
                .call(
                    super::network::Message::AuthorizeArtifact {
                        reader,
                        artifact: artifact.into(),
                        upload,
                    },
                    Duration::from_secs(3),
                )
                .await
        })
        .await??;
        ensure!(
            matches!(answer, super::network::Answer::AuthorizeArtifact(Ok(true))),
            "artifact access is no longer authorized"
        );
        Ok(())
    }
}
#[derive(Serialize, Deserialize)]
struct Request {
    cluster: String,
    artifact: String,
    path: String,
}

pub fn verify_file(root: &Path, file: &crate::jobs::OutputFile) -> Result<()> {
    crate::checkpoint::canonical(&file.path)?;
    let root_meta = std::fs::symlink_metadata(root)?;
    ensure!(
        root_meta.is_dir() && !root_meta.file_type().is_symlink(),
        "artifact root is not a private directory"
    );
    let path = root.join(&file.path);
    for p in path.ancestors().take_while(|p| *p != root) {
        ensure!(
            !std::fs::symlink_metadata(p)?.file_type().is_symlink(),
            "artifact contains a symlink"
        );
    }
    let meta = std::fs::symlink_metadata(&path)?;
    ensure!(
        meta.is_file() && meta.len() == file.size,
        "artifact size/type mismatch"
    );
    ensure!(
        crate::blobs::hash_file(&path)? == file.hash,
        "artifact hash mismatch"
    );
    Ok(())
}
/// Remove a manifest-owned path even when its bytes are corrupt. Never traverse a symlinked directory.
pub fn remove_owned_file(root: &Path, relative: &str) -> Result<()> {
    crate::checkpoint::canonical(relative)?;
    let path = root.join(relative);
    match std::fs::symlink_metadata(&path) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e.into()),
        Ok(meta) => ensure!(!meta.is_dir(), "artifact file became a directory"),
    }
    for parent in path.parent().context("file parent")?.ancestors() {
        ensure!(
            !std::fs::symlink_metadata(parent)?.file_type().is_symlink(),
            "refusing cleanup through a symlinked directory"
        );
        if parent == root {
            break;
        }
    }
    std::fs::remove_file(&path)?;
    std::fs::File::open(path.parent().context("file parent")?)?.sync_all()?;
    Ok(())
}
pub fn verify(root: &Path, manifest: &Manifest) -> Result<()> {
    manifest.validate()?;
    for file in &manifest.files {
        verify_file(root, file)?;
    }
    Ok(())
}

impl ProtocolHandler for Handler {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        if !self
            .access
            .read()
            .expect("artifact access")
            .readers
            .contains(&connection.remote_id())
        {
            connection.close(403u32.into(), b"not authorized");
            return Ok(());
        }
        // One request per connection; finite admission and transfer deadlines bound resources.
        let Ok(_slot) = self.slots.clone().try_acquire_owned() else {
            connection.close(429u32.into(), b"busy");
            return Ok(());
        };
        let result = tokio::time::timeout(Duration::from_secs(1800), async {
            let (mut send, mut recv) =
                tokio::time::timeout(Duration::from_secs(10), connection.accept_bi()).await??;
            let bytes =
                tokio::time::timeout(Duration::from_secs(10), recv.read_to_end(8192)).await??;
            let request: Request = serde_json::from_slice(&bytes)?;
            self.authorization
                .check(connection.remote_id(), &request.artifact, false)
                .await?;
            let selected = (|| -> Result<_> {
                ensure!(
                    request.cluster == self.cluster,
                    "artifact authority mismatch"
                );
                let access = self.access.read().expect("artifact access");
                ensure!(
                    access.readers.contains(&connection.remote_id()),
                    "reader no longer authorized"
                );
                let (manifest, root) = access
                    .objects
                    .get(&request.artifact)
                    .context("artifact unavailable")?;
                let file = manifest
                    .files
                    .iter()
                    .find(|f| f.path == request.path)
                    .context("file not in artifact")?;
                Ok((root.clone(), file.clone()))
            })();
            let selected = match selected {
                Ok((root, file)) => {
                    tokio::task::spawn_blocking(move || {
                        verify_file(&root, &file)?;
                        Ok::<_, anyhow::Error>((root.join(&file.path), file.size))
                    })
                    .await?
                }
                Err(e) => Err(e),
            };
            let header: std::result::Result<(), String> =
                selected.as_ref().map(|_| ()).map_err(|e| e.to_string());
            let bytes = serde_json::to_vec(&header)?;
            send.write_all(&(bytes.len() as u32).to_be_bytes()).await?;
            send.write_all(&bytes).await?;
            if let Ok((path, size)) = selected {
                let mut file = tokio::fs::File::open(path).await?;
                let copied = tokio::io::copy(&mut (&mut file).take(size), &mut send).await?;
                ensure!(copied == size, "artifact changed during transfer");
            }
            send.finish()?;
            // Ensure the final stream data is acknowledged before dropping the connection.
            let _ = tokio::time::timeout(Duration::from_secs(10), send.stopped()).await;
            Ok::<_, anyhow::Error>(())
        })
        .await;
        if !matches!(result, Ok(Ok(()))) {
            tracing::debug!(?result, "artifact stream interrupted");
        }
        Ok(())
    }
}

/// Download only into a fresh private directory. On failure discard all partial payloads.
pub async fn fetch(
    endpoint: &Endpoint,
    peer: &Peer,
    cluster: &str,
    manifest: &Manifest,
    destination: &Path,
) -> Result<()> {
    manifest.validate()?;
    std::fs::create_dir(destination).context("artifact destination must be new")?;
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(destination, std::fs::Permissions::from_mode(0o700))?;
    let result = tokio::time::timeout(Duration::from_secs(1800), async {
        let mut total = 0u64;
        for file in &manifest.files {
            let connection =
                tokio::time::timeout(Duration::from_secs(10), endpoint.connect(peer.addr(), ALPN))
                    .await??;
            let result = async {
                let (mut send, mut recv) = connection.open_bi().await?;
                send.write_all(&serde_json::to_vec(&Request {
                    cluster: cluster.into(),
                    artifact: manifest.id.clone(),
                    path: file.path.clone(),
                })?)
                .await?;
                send.finish()?;
                let mut length = [0; 4];
                tokio::time::timeout(Duration::from_secs(15), recv.read_exact(&mut length))
                    .await??;
                let n = u32::from_be_bytes(length) as usize;
                ensure!(n <= 4096, "artifact response header too large");
                let mut header = vec![0; n];
                tokio::time::timeout(Duration::from_secs(15), recv.read_exact(&mut header))
                    .await??;
                serde_json::from_slice::<std::result::Result<(), String>>(&header)?
                    .map_err(anyhow::Error::msg)?;
                let path = destination.join(&file.path);
                tokio::fs::create_dir_all(path.parent().context("parent")?).await?;
                let mut output = tokio::fs::OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .open(&path)
                    .await?;
                let mut hash = blake3::Hasher::new();
                let mut written = 0u64;
                let mut chunk = vec![0; 65536];
                while let Some(n) =
                    tokio::time::timeout(Duration::from_secs(30), recv.read(&mut chunk)).await??
                {
                    written += n as u64;
                    total += n as u64;
                    ensure!(
                        written <= file.size && total <= MAX_BYTES,
                        "artifact stream exceeds manifest size"
                    );
                    hash.update(&chunk[..n]);
                    output.write_all(&chunk[..n]).await?;
                }
                ensure!(
                    written == file.size
                        && iroh_blobs::Hash::from_bytes(*hash.finalize().as_bytes()) == file.hash,
                    "artifact stream hash/size mismatch"
                );
                output.sync_all().await?;
                sync_parents(path.parent().expect("parent"), destination)?;
                Ok::<_, anyhow::Error>(())
            }
            .await;
            connection.close(0u32.into(), b"done");
            result?;
        }
        std::fs::File::open(destination)?.sync_all()?;
        std::fs::File::open(destination.parent().context("destination parent")?)?.sync_all()?;
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("artifact transfer deadline exceeded")
    .and_then(|r| r);
    if result.is_err() {
        let _ = std::fs::remove_dir_all(destination);
    }
    result
}
pub fn sync_parents(path: &Path, root: &Path) -> Result<()> {
    for p in path.ancestors() {
        std::fs::File::open(p)?.sync_all()?;
        if p == root {
            break;
        }
    }
    Ok(())
}

/// Prefer verified retained copies; source workers are a fallback, never a required coordinator.
pub async fn fetch_available(
    endpoint: &Endpoint,
    state: &super::State,
    artifact: &super::artifact::Artifact,
    destination: &Path,
) -> Result<()> {
    use super::artifact::{CopyPhase, Kind};
    ensure!(!artifact.deleted, "artifact was deleted");
    ensure!(!destination.exists(), "artifact destination must be new");
    let mut sources = Vec::new();
    for id in &artifact.targets {
        if state.storage_readable(id)
            && artifact
                .copies
                .get(&format!("storage/{id}"))
                .is_some_and(|c| c.phase == CopyPhase::Ready)
            && let Some(store) = state.storage.stores.get(id)
        {
            sources.push(&store.peer);
        }
    }
    if let Some(worker) = artifact
        .manifest
        .worker()
        .and_then(|id| state.workers.get(id))
    {
        sources.push(&worker.peer);
    }
    let mut errors = Vec::new();
    for source in sources {
        match fetch(
            endpoint,
            source,
            &state.cluster,
            &artifact.manifest,
            destination,
        )
        .await
        {
            Ok(()) => {
                let verified = (|| -> Result<()> {
                    if let Kind::Checkpoint(id) = &artifact.manifest.kind {
                        let cp = state
                            .checkpoints
                            .get(id)
                            .context("checkpoint metadata missing")?;
                        for file in &cp.content.files {
                            cp.content.verify_file(destination, file)?;
                        }
                    }
                    Ok(())
                })();
                match verified {
                    Ok(()) => return Ok(()),
                    Err(e) => {
                        std::fs::remove_dir_all(destination)?;
                        errors.push(e.to_string());
                    }
                }
            }
            Err(e) => errors.push(e.to_string()),
        }
    }
    anyhow::bail!(
        "no verified artifact copy is available: {}",
        errors.join("; ")
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn owned_cleanup_removes_corrupt_payload_without_following_symlinked_parents() {
        let root = std::env::temp_dir().join(format!(
            "fleet-artifact-cleanup-{:032x}",
            rand::random::<u128>()
        ));
        std::fs::create_dir_all(root.join("files")).unwrap();
        std::fs::create_dir_all(root.join("outside")).unwrap();
        std::fs::write(root.join("files/data"), b"corrupt").unwrap();
        std::fs::write(root.join("outside/data"), b"retain").unwrap();
        remove_owned_file(&root.join("files"), "data").unwrap();
        assert!(!root.join("files/data").exists());
        std::os::unix::fs::symlink(root.join("outside"), root.join("files/link")).unwrap();
        assert!(remove_owned_file(&root.join("files"), "link/data").is_err());
        assert_eq!(std::fs::read(root.join("outside/data")).unwrap(), b"retain");
        assert!(remove_owned_file(&root.join("files"), "../outside/data").is_err());
        std::fs::remove_dir_all(root).unwrap();
    }
}
