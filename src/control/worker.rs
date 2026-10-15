//! A durable simulated worker. Effects are counters in redb, never processes or VMs.
use super::model::{Job, Mutation, Phase};
use anyhow::{Context, Result, ensure};
use redb::{Database, Durability, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::Path};

const DATA: TableDefinition<&str, &[u8]> = TableDefinition::new("simulated_worker");

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Execution {
    pub desired: Job,
    pub starts: u64,
    pub event: Mutation,
}

#[derive(Debug, Serialize, Deserialize)]
struct WorkerState {
    format: u32,
    cluster: String,
    worker: String,
    incarnation: String,
    sequence: u64,
    executions: BTreeMap<String, Execution>,
    events: Vec<Mutation>,
}

pub struct SimulatedWorker {
    db: Database,
}

impl SimulatedWorker {
    pub fn open(path: &Path, cluster: &str, worker: &str) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let db = Database::create(path)?;
        let mut tx = db.begin_write()?;
        tx.set_durability(Durability::Immediate)?;
        {
            let mut table = tx.open_table(DATA)?;
            let old = table
                .get("state")?
                .map(|v| serde_json::from_slice::<WorkerState>(v.value()))
                .transpose()?;
            if let Some(old) = old {
                ensure!(
                    old.format == super::FORMAT && old.cluster == cluster && old.worker == worker,
                    "worker identity mismatch"
                );
            } else {
                let state = WorkerState {
                    format: super::FORMAT,
                    cluster: cluster.into(),
                    worker: worker.into(),
                    incarnation: format!("{:032x}", rand::random::<u128>()),
                    sequence: 0,
                    executions: BTreeMap::new(),
                    events: Vec::new(),
                };
                table.insert("state", serde_json::to_vec(&state)?.as_slice())?;
            }
        }
        tx.commit()?;
        Ok(Self { db })
    }

    pub fn ensure(&self, desired: &Job) -> Result<Execution> {
        let mut tx = self.db.begin_write()?;
        tx.set_durability(Durability::Immediate)?;
        let execution = {
            let mut table = tx.open_table(DATA)?;
            let mut state: WorkerState = serde_json::from_slice(
                table.get("state")?.context("missing worker state")?.value(),
            )?;
            ensure!(desired.worker == state.worker, "wrong assigned worker");
            let old = state.executions.get(&desired.execution);
            if let Some(old) = old {
                ensure!(
                    old.desired.job == desired.job && old.desired.payload == desired.payload,
                    "execution contents conflict"
                );
                if desired.revision == old.desired.revision {
                    ensure!(
                        desired.cancelled == old.desired.cancelled,
                        "revision contents conflict"
                    );
                    return Ok(old.clone());
                }
                if desired.revision < old.desired.revision || old.desired.cancelled {
                    return Ok(old.clone());
                }
            }
            let starts = old.map_or(u64::from(!desired.cancelled), |o| o.starts);
            state.sequence += 1;
            let event = Mutation::Observe {
                worker: state.worker.clone(),
                incarnation: state.incarnation.clone(),
                sequence: state.sequence,
                execution: desired.execution.clone(),
                revision: desired.revision,
                phase: if desired.cancelled {
                    Phase::Cancelled
                } else {
                    Phase::Running
                },
            };
            let execution = Execution {
                desired: desired.clone(),
                starts,
                event: event.clone(),
            };
            state
                .executions
                .insert(desired.execution.clone(), execution.clone());
            state.events.push(event);
            table.insert("state", serde_json::to_vec(&state)?.as_slice())?;
            execution
        };
        tx.commit()?;
        Ok(execution)
    }

    pub fn events(&self) -> Result<Vec<Mutation>> {
        use redb::ReadableDatabase;
        let tx = self.db.begin_read()?;
        let table = tx.open_table(DATA)?;
        let state: WorkerState =
            serde_json::from_slice(table.get("state")?.context("missing worker state")?.value())?;
        Ok(state.events)
    }
}
