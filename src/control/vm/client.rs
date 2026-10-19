use super::*;
use crate::{
    blobs,
    control::{
        client::AppClient,
        runtime::{self, DataRequest},
    },
    worker_data,
};
use serde_json::{Value, json};
use std::path::{Path, PathBuf};

pub async fn prepare_upload(
    home: &Path,
    source: &Path,
    path: &str,
    executable: bool,
) -> Result<guest::Spec> {
    ensure!(source.is_file(), "upload source must be a regular file");
    let size = std::fs::metadata(source)?.len();
    ensure!(size <= guest::MAX_FILE, "upload exceeds 1 GiB");
    let _lock =
        instances::try_lock(&home.join("blobs.lock"))?.context("client input storage is busy")?;
    let store = blobs::open(&home.join("blobs")).await?;
    let result = async {
        let hash = blobs::hash_file(source)?;
        let sha256 = guest::sha256(source)?;
        let spec = guest::Spec::Upload {
            path: path.into(),
            hash,
            size,
            sha256,
            executable,
        };
        spec.validate()?;
        blobs::import_named(&store, source, hash, format!("vm-upload/{hash}")).await?;
        store.sync_db().await?;
        Ok::<_, anyhow::Error>(spec)
    }
    .await;
    let closed = store.shutdown().await;
    let spec = result?;
    closed?;
    Ok(spec)
}
impl AppClient {
    pub(crate) async fn stage_vm(&mut self, id: &str) -> Result<Value> {
        let state = self.read().await?;
        let op = state.vm.operations.get(id).context("unknown operation")?;
        ensure!(!op.released, "operation input was released");
        let guest::Spec::Upload { hash, .. } = &op.spec else {
            anyhow::bail!("operation is not an upload");
        };
        let worker = &state.workers[&op.worker].peer;
        let _lock = instances::try_lock(&self.home.join("blobs.lock"))?
            .context("client input storage is busy")?;
        let store = blobs::open(&self.home.join("blobs")).await?;
        let result=async {
            ensure!(store.blobs().has(*hash).await?,"saved upload bytes are unavailable on this client");
            // Wait only for the assigned worker to observe the committed operation.
            let deadline=tokio::time::Instant::now()+std::time::Duration::from_secs(30);
            loop {
                let response=runtime::data_call(&self.transport.endpoint,&state.cluster,worker,DataRequest::VmInput{id:id.into()}).await;
                if matches!(response,Ok(worker_data::Response::Missing(_))) {break;}
                ensure!(tokio::time::Instant::now()<deadline,"upload is accepted but staging is unconfirmed; retry instance stage with the same operation ID");
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
            let connection=blobs::push(&store,&self.transport.endpoint,worker.addr(),&[*hash]).await?;
            loop {
                let r=runtime::data_call(&self.transport.endpoint,&state.cluster,worker,DataRequest::VmInput{id:id.into()}).await?;
                if matches!(r,worker_data::Response::Missing(ref missing) if missing.is_empty()) {break;}
                ensure!(tokio::time::Instant::now()<deadline,"upload staging unconfirmed; retry instance stage");
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
            if let Some(c)=connection {c.close(0u32.into(),b"upload staged");}
            Ok::<_,anyhow::Error>(json!({"operation":id,"staged":true,"hash":hash,"durability":"worker-local; execution is observed separately"}))
        }.await;
        let closed = store.shutdown().await;
        let r = result?;
        closed?;
        Ok(r)
    }
    pub(crate) async fn fetch_vm(&mut self, id: &str, destination: &Path) -> Result<Value> {
        let state = self.read().await?;
        let op = state.vm.operations.get(id).context("unknown operation")?;
        ensure!(
            op.outcome.phase.terminal() && !op.released,
            "operation unfinished or output released"
        );
        ensure!(
            destination.is_absolute() && !destination.exists(),
            "destination must be a new absolute directory"
        );
        let worker = &state.workers[&op.worker].peer;
        let _lock = instances::try_lock(&self.home.join("blobs.lock"))?
            .context("client file storage is busy")?;
        let store = blobs::open(&self.home.join("blobs")).await?;
        struct Staging(PathBuf);
        impl Drop for Staging {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let stage = Staging(
            destination.with_file_name(format!(".fleet-capture-{:032x}", rand::random::<u128>())),
        );
        let result = async {
            use std::os::unix::fs::PermissionsExt;
            std::fs::create_dir(&stage.0)?;
            std::fs::set_permissions(&stage.0, std::fs::Permissions::from_mode(0o700))?;
            blobs::fetch(
                &store,
                &self.transport.endpoint,
                worker.addr(),
                &op.outcome
                    .files
                    .values()
                    .map(|f| f.hash)
                    .collect::<Vec<_>>(),
            )
            .await?;
            for (name, file) in &op.outcome.files {
                ensure!(
                    ["stdout", "stderr", "download"].contains(&name.as_str()),
                    "invalid result file"
                );
                let path = stage.0.join(name);
                store.blobs().export(file.hash, &path).await?;
                guest::verify_input(&path, file.hash, file.size)?;
                std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
            }
            instances::write(&stage.0.join("operation.json"), op)?;
            // create_dir refuses a collision; never overwrite a caller's existing directory.
            std::fs::create_dir(destination)?;
            std::fs::set_permissions(destination, std::fs::Permissions::from_mode(0o700))?;
            let move_files = (|| -> Result<()> {
                for e in std::fs::read_dir(&stage.0)? {
                    let e = e?;
                    std::fs::rename(e.path(), destination.join(e.file_name()))?;
                }
                Ok(())
            })();
            if let Err(e) = move_files {
                let _ = std::fs::remove_dir_all(destination);
                return Err(e);
            }
            Ok(json!({"operation":id,"destination":destination,"files":op.outcome.files}))
        }
        .await;
        let closed = store.shutdown().await;
        let r = result?;
        closed?;
        Ok(r)
    }
    pub(crate) async fn vm_logs(&mut self, id: &str, stderr: bool, from: u64) -> Result<Value> {
        let state = self.read().await?;
        let op = state.vm.operations.get(id).context("unknown operation")?;
        ensure!(!op.released, "operation output released");
        if op.outcome.phase == Phase::Pending {
            return Ok(json!({"data":"", "next":from, "done":false, "pending":true}));
        }
        let r = runtime::data_call(
            &self.transport.endpoint,
            &state.cluster,
            &state.workers[&op.worker].peer,
            DataRequest::VmLogs {
                id: id.into(),
                stderr,
                from,
            },
        )
        .await?;
        match r {
            worker_data::Response::Error(error) => anyhow::bail!("{error}"),
            worker_data::Response::InstanceGuest(guest::Response::Logs(log)) => Ok(
                json!({"data":String::from_utf8_lossy(&log.data),"next":log.next,"done":log.done}),
            ),
            other => anyhow::bail!("unexpected guest log response: {other:?}"),
        }
    }
}
