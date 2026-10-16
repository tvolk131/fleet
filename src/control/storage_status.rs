//! Administrator-only, timestamped local storage observations. These never authorize deletion.
use super::network::Peer;
use anyhow::{Result, ensure};
use iroh::{
    Endpoint, EndpointId,
    endpoint::Connection,
    protocol::{AcceptError, ProtocolHandler},
};
use serde_json::Value;
use std::{
    collections::BTreeSet,
    sync::{Arc, RwLock},
    time::Duration,
};

pub const ALPN: &[u8] = b"fleet/storage-status/1";
#[derive(Clone, Debug)]
pub struct Handler {
    pub cluster: String,
    pub administrators: BTreeSet<EndpointId>,
    pub report: Arc<RwLock<Value>>,
}
impl ProtocolHandler for Handler {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        if !self.administrators.contains(&connection.remote_id()) {
            connection.close(403u32.into(), b"not authorized");
            return Ok(());
        }
        let result = tokio::time::timeout(Duration::from_secs(5), async {
            let (mut send, mut recv) = connection.accept_bi().await?;
            let cluster: String = serde_json::from_slice(&recv.read_to_end(1024).await?)?;
            ensure!(cluster == self.cluster, "storage authority mismatch");
            let bytes = serde_json::to_vec(&*self.report.read().expect("storage report"))?;
            send.write_all(&bytes).await?;
            send.finish()?;
            let _ = send.stopped().await;
            Ok::<_, anyhow::Error>(())
        })
        .await;
        if !matches!(result, Ok(Ok(()))) {
            connection.close(400u32.into(), b"invalid status request");
        }
        Ok(())
    }
}
pub async fn read(endpoint: &Endpoint, peer: &Peer, cluster: &str) -> Result<Value> {
    tokio::time::timeout(Duration::from_secs(3), async {
        let connection = endpoint.connect(peer.addr(), ALPN).await?;
        let (mut send, mut recv) = connection.open_bi().await?;
        send.write_all(&serde_json::to_vec(cluster)?).await?;
        send.finish()?;
        let report = serde_json::from_slice(&recv.read_to_end(65536).await?)?;
        connection.close(0u32.into(), b"status received");
        Ok(report)
    })
    .await?
}
