//! Administrator upload into a committed job input artifact. One storage gate covers quota,
//! transfer, publication, repair and deletion; an interrupted transfer never becomes Ready.
use super::{
    artifact::{Kind, Manifest},
    artifact_io,
    network::Peer,
};
use anyhow::{Context, Result, ensure};
use iroh::{
    Endpoint, EndpointId,
    endpoint::{Connection, RecvStream, SendStream},
    protocol::{AcceptError, ProtocolHandler},
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
    time::Duration,
};
use tokio::io::AsyncWriteExt;

pub const ALPN: &[u8] = b"fleet/input-upload/1";
#[derive(Debug, Clone)]
pub struct Handler {
    pub cluster: String,
    pub administrators: BTreeSet<EndpointId>,
    pub allowed: Arc<RwLock<BTreeMap<String, Manifest>>>,
    pub gate: Arc<tokio::sync::Mutex<()>>,
    pub home: PathBuf,
    pub capacity: u64,
    pub slots: Arc<tokio::sync::Semaphore>,
}
#[derive(Serialize, Deserialize)]
struct Request {
    cluster: String,
    artifact: String,
}
async fn write<T: Serialize>(send: &mut SendStream, value: &T) -> Result<()> {
    let bytes = serde_json::to_vec(value)?;
    send.write_all(&(bytes.len() as u32).to_be_bytes()).await?;
    send.write_all(&bytes).await?;
    Ok(())
}
async fn read<T: DeserializeOwned>(recv: &mut RecvStream) -> Result<T> {
    let mut len = [0; 4];
    recv.read_exact(&mut len).await?;
    let len = u32::from_be_bytes(len) as usize;
    ensure!(len <= 8192, "upload header too large");
    let mut bytes = vec![0; len];
    recv.read_exact(&mut bytes).await?;
    Ok(serde_json::from_slice(&bytes)?)
}
struct Pending(PathBuf);
impl Drop for Pending {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

impl ProtocolHandler for Handler {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        if !self.administrators.contains(&connection.remote_id()) {
            connection.close(403u32.into(), b"not authorized");
            return Ok(());
        }
        let Ok(_slot) = self.slots.clone().try_acquire_owned() else {
            connection.close(429u32.into(), b"busy");
            return Ok(());
        };
        let result = tokio::time::timeout(Duration::from_secs(1800), async {
            let (mut send, mut recv) =
                tokio::time::timeout(Duration::from_secs(10), connection.accept_bi()).await??;
            let request: Request =
                tokio::time::timeout(Duration::from_secs(10), read(&mut recv)).await??;
            let _gate = self.gate.lock().await;
            let selected = (|| -> Result<(Manifest, bool)> {
                ensure!(request.cluster == self.cluster, "upload authority mismatch");
                let m = self
                    .allowed
                    .read()
                    .expect("input upload access")
                    .get(&request.artifact)
                    .cloned()
                    .context(
                        "input artifact not accepting uploads; wait for storage reconciliation",
                    )?;
                m.validate()?;
                ensure!(m.kind == Kind::Inputs, "only job inputs accept upload");
                ensure!(
                    !self
                        .home
                        .join("deleted")
                        .join(format!("{}.json", m.id))
                        .exists(),
                    "artifact has a deletion tombstone"
                );
                let root = self.home.join("objects").join(&m.id);
                if root.exists() {
                    let saved: Manifest =
                        serde_json::from_slice(&std::fs::read(root.join("manifest.json"))?)?;
                    ensure!(saved == m, "stored manifest mismatch");
                    artifact_io::verify(&root.join("files"), &m)?;
                    return Ok((m, true));
                }
                super::storage::reserve(&self.home, self.capacity, &m)?;
                Ok((m, false))
            })();
            write(
                &mut send,
                &selected
                    .as_ref()
                    .map(|(_, already)| *already)
                    .map_err(|e| e.to_string()),
            )
            .await?;
            if let Ok((m, false)) = selected {
                let stage = self.home.join("staging").join(format!("upload-{}", m.id));
                std::fs::create_dir(&stage)?;
                let _pending = Pending(stage.clone());
                let files = stage.join("files");
                std::fs::create_dir(&files)?;
                let result = async {
                    let mut chunk = vec![0; 65536];
                    for f in &m.files {
                        let path = files.join(&f.path);
                        tokio::fs::create_dir_all(path.parent().context("input parent")?).await?;
                        let mut output = tokio::fs::OpenOptions::new()
                            .write(true)
                            .create_new(true)
                            .open(&path)
                            .await?;
                        let mut hash = blake3::Hasher::new();
                        let mut remaining = f.size;
                        while remaining > 0 {
                            let n = (remaining as usize).min(chunk.len());
                            tokio::time::timeout(
                                Duration::from_secs(30),
                                recv.read_exact(&mut chunk[..n]),
                            )
                            .await??;
                            hash.update(&chunk[..n]);
                            output.write_all(&chunk[..n]).await?;
                            remaining -= n as u64;
                        }
                        ensure!(
                            iroh_blobs::Hash::from_bytes(*hash.finalize().as_bytes()) == f.hash,
                            "input upload hash mismatch"
                        );
                        output.sync_all().await?;
                        artifact_io::sync_parents(path.parent().context("parent")?, &files)?;
                    }
                    tokio::time::timeout(Duration::from_secs(10), recv.read_to_end(0)).await??;
                    artifact_io::verify(&files, &m)?;
                    crate::instances::write(&stage.join("manifest.json"), &m)?;
                    std::fs::rename(&stage, self.home.join("objects").join(&m.id))?;
                    std::fs::File::open(self.home.join("objects"))?.sync_all()?;
                    Ok::<_, anyhow::Error>(())
                }
                .await;
                write(&mut send, &result.map_err(|e| e.to_string())).await?;
            }
            send.finish()?;
            let _ = tokio::time::timeout(Duration::from_secs(10), send.stopped()).await;
            Ok::<_, anyhow::Error>(())
        })
        .await;
        if !matches!(result, Ok(Ok(()))) {
            tracing::debug!(?result, "input upload interrupted");
        }
        Ok(())
    }
}

pub async fn upload(
    endpoint: &Endpoint,
    peer: &Peer,
    cluster: &str,
    manifest: &Manifest,
    root: &Path,
) -> Result<()> {
    artifact_io::verify(root, manifest)?;
    tokio::time::timeout(Duration::from_secs(1800), async {
        let connection =
            tokio::time::timeout(Duration::from_secs(10), endpoint.connect(peer.addr(), ALPN))
                .await??;
        let result = async {
            let (mut send, mut recv) = connection.open_bi().await?;
            write(
                &mut send,
                &Request {
                    cluster: cluster.into(),
                    artifact: manifest.id.clone(),
                },
            )
            .await?;
            let already: std::result::Result<bool, String> = read(&mut recv).await?;
            if !already.map_err(anyhow::Error::msg)? {
                for f in &manifest.files {
                    let mut file = tokio::fs::File::open(root.join(&f.path)).await?;
                    ensure!(
                        tokio::io::copy(&mut file, &mut send).await? == f.size,
                        "input changed while uploading"
                    );
                }
                send.finish()?;
                let result: std::result::Result<(), String> = read(&mut recv).await?;
                result.map_err(anyhow::Error::msg)?;
            } else {
                send.finish()?;
            }
            Ok::<_, anyhow::Error>(())
        }
        .await;
        connection.close(0u32.into(), b"upload complete");
        result
    })
    .await?
}

#[cfg(test)]
mod tests {
    use super::*;
    use iroh::{SecretKey, protocol::Router};
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn input_upload_rejects_uncommitted_unauthorized_partial_and_corrupt_bytes_and_serializes_quota()
     {
        let home = std::env::temp_dir().join(format!(
            "fleet-input-upload-{:032x}",
            rand::random::<u128>()
        ));
        for name in ["objects", "staging", "deleted", "client"] {
            std::fs::create_dir_all(home.join(name)).unwrap();
        }
        let data = vec![7; 128 << 10];
        std::fs::write(home.join("client/input"), &data).unwrap();
        let file = crate::jobs::OutputFile {
            path: "input".into(),
            hash: iroh_blobs::Hash::new(&data),
            size: data.len() as u64,
        };
        let first = Manifest::inputs("first".into(), vec![file.clone()]).unwrap();
        let second = Manifest::inputs("second".into(), vec![file]).unwrap();
        let server = super::super::network::connect_endpoint(
            SecretKey::generate(),
            "127.0.0.1:0".parse().unwrap(),
        )
        .await
        .unwrap();
        let client = super::super::network::connect_endpoint(
            SecretKey::generate(),
            "127.0.0.1:0".parse().unwrap(),
        )
        .await
        .unwrap();
        let outsider = super::super::network::connect_endpoint(
            SecretKey::generate(),
            "127.0.0.1:0".parse().unwrap(),
        )
        .await
        .unwrap();
        let peer = Peer {
            identity: server.id(),
            address: server.bound_sockets()[0],
        };
        let allowed = Arc::new(RwLock::new(BTreeMap::from([
            (first.id.clone(), first.clone()),
            (second.id.clone(), second.clone()),
        ])));
        let router = Router::builder(server)
            .accept(
                ALPN,
                Handler {
                    cluster: "cluster".into(),
                    administrators: BTreeSet::from([client.id()]),
                    allowed,
                    gate: Arc::new(tokio::sync::Mutex::new(())),
                    home: home.clone(),
                    capacity: (1 << 20) + (200 << 10),
                    slots: Arc::new(tokio::sync::Semaphore::new(4)),
                },
            )
            .spawn();
        assert!(
            upload(&outsider, &peer, "cluster", &first, &home.join("client"))
                .await
                .is_err()
        );
        assert!(
            upload(
                &client,
                &peer,
                "wrong-cluster",
                &first,
                &home.join("client")
            )
            .await
            .is_err()
        );
        let unknown = Manifest::inputs("unknown".into(), first.files.clone()).unwrap();
        assert!(
            upload(&client, &peer, "cluster", &unknown, &home.join("client"))
                .await
                .is_err()
        );
        for bytes in [vec![1; 3], vec![1; data.len()]] {
            let connection = client.connect(peer.addr(), ALPN).await.unwrap();
            let (mut send, mut recv) = connection.open_bi().await.unwrap();
            write(
                &mut send,
                &Request {
                    cluster: "cluster".into(),
                    artifact: first.id.clone(),
                },
            )
            .await
            .unwrap();
            assert_eq!(
                read::<std::result::Result<bool, String>>(&mut recv)
                    .await
                    .unwrap(),
                Ok(false)
            );
            send.write_all(&bytes).await.unwrap();
            send.finish().unwrap();
            assert!(
                read::<std::result::Result<(), String>>(&mut recv)
                    .await
                    .unwrap()
                    .is_err()
            );
            connection.close(0u32.into(), b"test complete");
        }
        assert!(!home.join("objects").join(&first.id).exists());
        let root = home.join("client");
        let (a, b) = tokio::join!(
            upload(&client, &peer, "cluster", &first, &root),
            upload(&client, &peer, "cluster", &second, &root)
        );
        assert_ne!(
            a.is_ok(),
            b.is_ok(),
            "quota must admit exactly one concurrent upload: {a:?} {b:?}"
        );
        let accepted = if a.is_ok() { &first } else { &second };
        upload(&client, &peer, "cluster", accepted, &root)
            .await
            .unwrap();
        artifact_io::verify(
            &home.join("objects").join(&accepted.id).join("files"),
            accepted,
        )
        .unwrap();
        router.shutdown().await.unwrap();
        client.close().await;
        outsider.close().await;
        assert_eq!(
            super::super::storage::usage(&home.join("staging"))
                .unwrap()
                .files,
            0
        );
        std::fs::remove_dir_all(home).unwrap();
    }
}
