//! A durable simulated worker. Effects are counters in redb, never processes or VMs.
use anyhow::{Context, Result, ensure};
use fleet::control::{
    Mutation,
    workload::{Assignment, AttemptPhase, Desired, JobDesired, Observation, Report, WorkPhase},
};
use redb::{Database, Durability, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::Path};

const DATA: TableDefinition<&str, &[u8]> = TableDefinition::new("simulated_worker");

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Execution {
    pub desired: Assignment,
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
                    old.format == fleet::control::FORMAT
                        && old.cluster == cluster
                        && old.worker == worker,
                    "worker identity mismatch"
                );
            } else {
                let state = WorkerState {
                    format: fleet::control::FORMAT,
                    cluster: cluster.into(),
                    worker: worker.into(),
                    incarnation: "simulated-journal".into(),
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

    pub fn ensure(&self, desired: &Assignment) -> Result<Execution> {
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
                    old.desired.id == desired.id && old.desired.workload == desired.workload,
                    "execution contents conflict"
                );
                if desired.revision == old.desired.revision {
                    ensure!(
                        desired.desired == old.desired.desired,
                        "revision contents conflict"
                    );
                    return Ok(old.clone());
                }
                if desired.revision < old.desired.revision
                    || old.desired.desired == Desired::Job(JobDesired::Cancelled)
                {
                    return Ok(old.clone());
                }
            }
            let starts = old.map_or(
                u64::from(desired.desired != Desired::Job(JobDesired::Cancelled)),
                |o| o.starts,
            );
            state.sequence += 1;
            let event = Mutation::WorkerReport(Report {
                worker: state.worker.clone(),
                incarnation: state.incarnation.clone(),
                sequence: state.sequence,
                assignment: desired.id.clone(),
                revision: desired.revision,
                observation: Observation {
                    phase: if desired.desired == Desired::Job(JobDesired::Cancelled) {
                        WorkPhase::Job(AttemptPhase::Cancelled)
                    } else {
                        WorkPhase::Job(AttemptPhase::Running)
                    },
                    detail: None,
                },
            });
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
