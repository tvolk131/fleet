//! Reconcile committed VM intent using the existing isolated guest backend.
use super::*;
use crate::control::{
    tunnels,
    vm::{self, Outcome, Phase, Update},
};
use instances::guest;

pub(super) fn supported(format: u32) -> bool {
    format == super::super::FORMAT
}
pub(super) fn input_tag(id: &str) -> String {
    format!("vm-input/{}", blake3::hash(id.as_bytes()).to_hex())
}
pub(super) fn result_tag(id: &str, name: &str) -> String {
    format!("vm-result/{}/{name}", blake3::hash(id.as_bytes()).to_hex())
}

impl Worker {
    async fn report_vm(&mut self, update: Update) -> Result<()> {
        let r = vm::Report {
            worker: self.config.registration.id.clone(),
            incarnation: self.config.registration.incarnation.clone(),
            update,
        };
        let id = format!(
            "vm-report-{}",
            blake3::hash(&serde_json::to_vec(&r)?).to_hex()
        );
        let reply = self
            .client
            .write(
                Request {
                    origin: self.config.cluster.clone(),
                    id,
                    mutation: Mutation::VmReport(r),
                },
                Duration::from_secs(2),
            )
            .await?;
        ensure!(reply == Reply::Recorded, "VM report rejected: {reply:?}");
        Ok(())
    }
    pub(super) async fn reconcile_vm(&mut self, online: bool) -> Result<()> {
        let saved = self.journal.read()?;
        if saved.generation < vm::GENERATION || saved.recovery_inventory.is_some() {
            return Ok(());
        }
        if online
            && !saved
                .vm
                .capable
                .contains(&self.config.registration.cursor())
        {
            self.report_vm(Update::Capable).await?;
        }
        // A broken attachment or unavailable result must not starve other VMs.
        let mut errors = Vec::new();
        for id in saved.vm.networks.keys() {
            if let Err(e) = self.reconcile_vm_adapter(&saved, id, online).await {
                errors.push(format!("{id}: {e:#}"));
            }
        }
        for op in saved.vm.operations.values() {
            if let Err(e) = self.reconcile_vm_operation(&saved, op, online).await {
                errors.push(format!("{}: {e:#}", op.id));
            }
        }
        ensure!(errors.is_empty(), "{}", errors.join("; "));
        Ok(())
    }
    async fn reconcile_vm_adapter(
        &mut self,
        saved: &JournalState,
        id: &str,
        online: bool,
    ) -> Result<()> {
        let Some(a) = saved.assignments.get(id) else {
            return Ok(());
        };
        if saved.held.contains(id) || !saved.starts_allowed {
            return Ok(());
        }
        let home = self
            .home
            .join("managed-vm/adapters")
            .join(
                blake3::hash(self.config.cluster.as_bytes())
                    .to_hex()
                    .as_str(),
            )
            .join(vm::adapter_id(id));
        let deleted = a.desired.terminating();
        if !home.exists() && !deleted {
            let attachment = match tunnels::attachment::Attachment::prepare(
                &self.home,
                &a.execution,
                &super::super::Config {
                    cluster: self.config.cluster.clone(),
                    id: 0,
                    peers: self.config.controllers.clone(),
                    administrators: self.config.administrators.clone(),
                    bootstrap_allowed: false,
                },
            ) {
                Ok(a) => a,
                Err(_) => return Ok(()),
            }; // VM backend may not have been created yet.
            std::fs::create_dir_all(home.parent().expect("adapter parent"))?;
            let socket = std::net::UdpSocket::bind(SocketAddr::new(
                self.config.registration.peer.address.ip(),
                0,
            ))?;
            let address = socket.local_addr()?;
            drop(socket);
            tunnels::agent::Agent::prepare_attached(
                &home,
                vm::adapter_id(id),
                super::super::Config {
                    cluster: self.config.cluster.clone(),
                    id: 0,
                    peers: self.config.controllers.clone(),
                    administrators: self.config.administrators.clone(),
                    bootstrap_allowed: false,
                },
                tunnels::transport::Network {
                    bind: Some(address),
                    relays: vec![],
                },
                attachment,
            )
            .await?;
        }
        if !home.exists() {
            return Ok(());
        }
        let adapter: tunnels::Adapter =
            serde_json::from_slice(&std::fs::read(home.join("adapter.json"))?)?;
        if !saved.vm.bindings.contains_key(id) && !deleted && online {
            self.report_vm(Update::Bound {
                instance: id.into(),
                adapter,
            })
            .await?;
        }
        if !self.vm_adapters.contains_key(id) {
            self.vm_adapters
                .insert(id.into(), tunnels::agent::Agent::start(&home).await?);
        }
        if online {
            // A fresh empty policy must be acknowledged even for a deleted/stopped VM.
            if let Some(agent) = self.vm_adapters.get_mut(id) {
                agent.tick().await?;
            }
        }
        Ok(())
    }
    async fn reconcile_vm_operation(
        &mut self,
        saved: &JournalState,
        op: &vm::Operation,
        online: bool,
    ) -> Result<()> {
        if saved.held.contains(&op.instance) || !saved.starts_allowed {
            return Ok(());
        }
        if op.released {
            if online && !op.reclaimed {
                self.reclaim_vm_operation(op).await?;
                self.report_vm(Update::Reclaimed {
                    id: op.id.clone(),
                    expected_revision: op.revision,
                })
                .await?;
            }
            return Ok(());
        }
        if op.outcome.phase.terminal() {
            return Ok(());
        }
        let outcome = self.vm_operation(op).await?;
        if online
            && let Some(result) = outcome
            && result != op.outcome
        {
            self.report_vm(Update::Operation {
                id: op.id.clone(),
                expected_revision: op.revision,
                result,
            })
            .await?;
        }
        Ok(())
    }
    async fn vm_operation(&self, op: &vm::Operation) -> Result<Option<Outcome>> {
        let backend = self.home.join("backend");
        let backend_id = op.backend_id();
        let dir = guest::operation_dir(&backend, &op.execution, &backend_id)?;
        let result_path = self
            .home
            .join("managed-vm/outcomes")
            .join(format!("{backend_id}.json"));
        if let Some(result) = jobs::read_json(&result_path)? {
            return Ok(Some(result));
        }
        let saved = self.journal.read()?;
        let a = saved
            .assignments
            .get(&op.instance)
            .context("operation assignment missing")?;
        let mut failure = None;
        if !dir.join("operation.json").exists() {
            if self.journal.attempted(true, &op.id)? {
                failure=Some("worker attempted this operation but backend records are missing; guest effects are unknown; not replayed".to_string());
            } else if a.revision != op.instance_revision || !a.desired.running() {
                failure = Some("VM changed before execution; command was not started".to_string());
            } else {
                // Readiness and blob checks have no application side effects. Don't mark an
                // attempt merely because the VM is still booting or input delivery is pending.
                let Ok(i) = instances::info(&backend, &a.execution) else {
                    return Ok(None);
                };
                if i.observed.state != instances::State::Running
                    || i.observed.revision != i.record.revision
                {
                    return Ok(None);
                }
                if let guest::Spec::Upload { hash, size, .. } = &op.spec
                    && !matches!(self.store.blobs().status(*hash).await?,iroh_blobs::api::blobs::BlobStatus::Complete{size:stored} if stored==*size)
                {
                    return Ok(None);
                }
                let ready = self
                    .backend
                    .guest(
                        self.config.registration.peer.identity,
                        guest::Request::Ready {
                            id: a.execution.clone(),
                        },
                    )
                    .await;
                if !matches!(ready,Ok(guest::Response::Ready(r)) if r.ready) {
                    return Ok(None);
                }
                self.journal.mark_attempted(true, &op.id, true)?;
                let started = self
                    .backend
                    .guest(
                        self.config.registration.peer.identity,
                        guest::Request::Start {
                            id: op.execution.clone(),
                            operation: backend_id.clone(),
                            revision: i.record.revision,
                            spec: op.spec.clone(),
                        },
                    )
                    .await;
                if let Err(e) = started
                    && !dir.join("operation.json").exists()
                {
                    failure = Some(format!("operation attempt failed; not replayed: {e:#}"));
                }
            }
        } else {
            let local = guest::inspect(&backend, &op.execution, &backend_id)?;
            ensure!(
                local.id == backend_id && local.instance == op.execution && local.spec == op.spec,
                "backend operation differs from committed request"
            );
            if local.state == guest::OperationState::Pending {
                if a.revision != op.instance_revision || !a.desired.running() {
                    failure = Some("VM changed before execution; command was not started".into());
                } else {
                    // The backend durably writes Pending before spawning its supervisor.
                    // Recover that launch gap only: inspect/start honor the supervisor lock
                    // and attempted marker, so uncertain guest effects are never replayed.
                    self.journal.mark_attempted(true, &op.id, true)?;
                    self.backend
                        .guest(
                            self.config.registration.peer.identity,
                            guest::Request::Start {
                                id: op.execution.clone(),
                                operation: backend_id.clone(),
                                revision: local.revision,
                                spec: op.spec.clone(),
                            },
                        )
                        .await?;
                }
            }
        }
        if let Some(error) = failure {
            let outcome = Outcome {
                phase: Phase::Interrupted,
                error: Some(error.chars().take(256).collect()),
                finished_at: Some(jobs::now()),
                ..Outcome::default()
            };
            instances::write(&result_path, &outcome)?;
            return Ok(Some(outcome));
        }
        let local = guest::inspect(&backend, &op.execution, &backend_id)?;
        let phase = match local.state {
            guest::OperationState::Pending | guest::OperationState::Running => Phase::Running,
            guest::OperationState::Succeeded => Phase::Succeeded,
            guest::OperationState::Failed => Phase::Failed,
            guest::OperationState::Interrupted => Phase::Interrupted,
        };
        let mut outcome = Outcome {
            phase,
            exit_code: local.exit_code,
            error: local.error.map(|e| e.chars().take(256).collect()),
            finished_at: local.finished_at,
            files: BTreeMap::new(),
        };
        if outcome.phase.terminal() {
            let _gate = self.backend.blob_guard().await;
            for name in ["stdout", "stderr"] {
                let path = dir.join(name);
                if !path.exists() {
                    continue;
                }
                let name = if name == "stdout" && matches!(op.spec, guest::Spec::Download { .. }) {
                    if outcome.phase != Phase::Succeeded {
                        continue;
                    }
                    "download"
                } else {
                    name
                };
                let size = std::fs::metadata(&path)?.len();
                let hash = blobs::hash_file(&path)?;
                blobs::import_named(&self.store, &path, hash, result_tag(&op.id, name)).await?;
                outcome.files.insert(name.into(), vm::File { hash, size });
            }
            self.store.sync_db().await?;
            instances::write(&result_path, &outcome)?;
        }
        Ok(Some(outcome))
    }
    async fn reclaim_vm_operation(&self, op: &vm::Operation) -> Result<()> {
        ensure!(
            op.outcome.phase.terminal() && op.released,
            "cannot reclaim unfinished operation"
        );
        let _gate = self.backend.blob_guard().await;
        let dir =
            guest::operation_dir(&self.home.join("backend"), &op.execution, &op.backend_id())?;
        // An operation fenced before start has no backend directory yet.
        std::fs::create_dir_all(&dir)?;
        let _lock = instances::try_lock(&dir.join("operation.lock"))?
            .context("operation supervisor still holds its lock")?;
        // Preserve operation.json and attempted: they fence a replay after cleanup.
        for name in ["stdout", "stderr", "input", "supervisor.log"] {
            match std::fs::remove_file(dir.join(name)) {
                Ok(()) => (),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
                Err(e) => return Err(e.into()),
            }
        }
        self.store.tags().delete(input_tag(&op.id)).await?;
        for name in ["stdout", "stderr", "download"] {
            self.store.tags().delete(result_tag(&op.id, name)).await?;
        }
        self.store.sync_db().await?;
        Ok(())
    }
}
use std::net::SocketAddr;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn journal_reopen_keeps_attempt_fences_and_rejects_generation_or_result_rollback() {
        let home = std::env::temp_dir().join(format!(
            "fleet-control-vm-journal-{}",
            rand::random::<u64>()
        ));
        let key = iroh::SecretKey::generate().public();
        let cfg: WorkerConfig = serde_json::from_value(json!({
            "cluster":"vm-test", "registration":{
                "id":"worker", "incarnation":"disk-1", "peer":{"identity":key,"address":"127.0.0.1:29001"},
                "budget":{"cpu_slots":2,"memory_mb":4096,"disk_gb":100,"vm_slots":1},
                "images":["macos"],"runtimes":{},"tags":[]
            }, "controllers":{},"administrators":[key],"supervisor":"/fixture/fleet"
        })).unwrap();
        let j = Journal::open(&home, &cfg, true).unwrap();
        let mut view: WorkerView = serde_json::from_value(json!({
            "generation":1,"vm":vm::Catalog::default(),"history":{},"artifacts":[],"stores":{},"cleanup_allowed":true,
            "starts_allowed":true,"checkpoints":[],"resume_checkpoints":{},"recovery_required":false,
            "held":[],"registration":cfg.registration,"assignments":[],"acknowledged":0
        })).unwrap();
        let op: vm::Operation = serde_json::from_value(json!({
            "id":"once","ordinal":1,"instance":"a","execution":"vm-a","worker":"worker","incarnation":"disk-1",
            "instance_revision":1,"revision":2,"spec":{"kind":"exec","argv":["/bin/true"],"cwd":null,"env":{},"timeout_seconds":30},
            "retention_seconds":60,"outcome":{"phase":"succeeded","exit_code":0,"error":null,"finished_at":10,"files":{}},
            "released":false,"reclaimed":false
        })).unwrap();
        view.vm.operations.insert(op.id.clone(), op);
        j.accept(view.clone()).unwrap();
        j.mark_attempted(true, "once", true).unwrap();
        assert_eq!(j.read().unwrap().format, crate::control::FORMAT);
        drop(j);
        let j = Journal::open(&home, &cfg, false).unwrap();
        assert!(j.attempted(true, "once").unwrap());
        let mut bad = view.clone();
        bad.generation = 0;
        assert!(j.accept(bad).is_err());
        let mut bad = view.clone();
        bad.vm.operations.get_mut("once").unwrap().outcome.phase = Phase::Running;
        assert!(j.accept(bad).is_err());
        let o = view.vm.operations.get_mut("once").unwrap();
        o.released = true;
        o.revision += 1;
        j.accept(view.clone()).unwrap();
        view.vm.operations.get_mut("once").unwrap().released = false;
        assert!(j.accept(view).is_err());
        assert!(j.read().unwrap().vm.operations["once"].released);
        // Both execution domains survive reopen; marker writes avoid the resource journal.
        let mut view = serde_json::from_value::<WorkerView>(json!({
            "generation":1,"vm":j.read().unwrap().vm,"history":{},"artifacts":[],"stores":{},"cleanup_allowed":true,
            "starts_allowed":true,"checkpoints":[],"resume_checkpoints":{},"recovery_required":false,
            "held":[],"registration":cfg.registration,"assignments":[],"acknowledged":0
        })).unwrap();
        j.mark_attempted(false, "native-before", true).unwrap();
        j.accept(view.clone()).unwrap();
        assert_eq!(
            j.read().unwrap().format,
            crate::control::store::indexed::FORMAT
        );
        j.mark_attempted(true, "after", true).unwrap();
        j.mark_attempted(false, "not-published", true).unwrap();
        j.mark_attempted(false, "not-published", false).unwrap();
        view.generation = 0;
        assert!(j.accept(view.clone()).is_err());
        // Emergency rebinding resets the observed authority generation. A surviving older
        // controller image must not downgrade the worker's physical execution-fence format.
        j.change(|s| {
            s.generation = 0;
            Ok(())
        })
        .unwrap();
        view.generation = 1;
        j.accept(view).unwrap();
        assert_eq!(
            j.read().unwrap().format,
            crate::control::store::indexed::FORMAT
        );
        drop(j);
        let j = Journal::open(&home, &cfg, false).unwrap();
        assert!(j.attempted(true, "once").unwrap());
        assert!(j.attempted(true, "after").unwrap());
        assert!(j.attempted(false, "native-before").unwrap());
        assert!(!j.attempted(false, "not-published").unwrap());
        assert!(!j.attempted(false, "once").unwrap());
        drop(j);
        // Number one alone is not compatibility, and an unsupported accepted generation must
        // be refused before redb can repair or modify the original file.
        let path = home.join("worker.redb");
        for (field, bad) in [
            ("family", json!("foreign")),
            ("generation", json!(2)),
            ("format", json!(4294967297u64)),
        ] {
            let original = {
                let db = Database::open(&path).unwrap();
                let tx = db.begin_write().unwrap();
                let original;
                {
                    let mut table = tx.open_table(JOURNAL).unwrap();
                    let mut state: serde_json::Value =
                        serde_json::from_slice(table.get("state").unwrap().unwrap().value())
                            .unwrap();
                    original = state.clone();
                    state[field] = bad;
                    table
                        .insert("state", serde_json::to_vec(&state).unwrap().as_slice())
                        .unwrap();
                }
                tx.commit().unwrap();
                original
            };
            let bytes = std::fs::read(&path).unwrap();
            assert!(Journal::open(&home, &cfg, false).is_err());
            assert_eq!(std::fs::read(&path).unwrap(), bytes);
            let db = Database::open(&path).unwrap();
            let tx = db.begin_write().unwrap();
            tx.open_table(JOURNAL)
                .unwrap()
                .insert("state", serde_json::to_vec(&original).unwrap().as_slice())
                .unwrap();
            tx.commit().unwrap();
        }
        {
            let db = Database::open(home.join("worker.redb")).unwrap();
            let tx = db.begin_write().unwrap();
            tx.delete_table(EXECUTIONS).unwrap();
            tx.commit().unwrap();
        }
        assert!(Journal::open(&home, &cfg, false).is_err());
        std::fs::remove_dir_all(home).unwrap();
    }
}
