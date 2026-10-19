//! Worker data-plane replies and hardware observations. Mutations use replicated intent.
use crate::{
    events::Event,
    jobs::{JobInfo, OutputFile},
};
use iroh_blobs::Hash;
use serde::{Deserialize, Serialize};
use serde_json::Value;
pub const MAX_LOG_CHUNK: u32 = 512 * 1024;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum Response {
    StorageReport(Value),
    InstanceGuest(crate::instances::guest::Response),
    Instances(Vec<crate::instances::Info>),
    Status(Status),
    Missing(Vec<Hash>),
    Jobs(Vec<JobInfo>),
    /// Log text from a byte offset: cut at a character boundary, so `next` may stop short of the bytes read.
    Logs {
        data: String,
        next: u64,
        done: bool,
        #[serde(default)]
        gap: Option<crate::history::Block>,
    },
    History(crate::history::ReadResult),
    Outputs(Vec<OutputFile>),
    Events {
        events: Vec<Event>,
        last: u64,
        first: u64,
    },
    Error(String),
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Hello {
    pub agent_version: String,
    pub hostname: String,
    pub os: String,
    pub cpu: String,
    pub cores: u32,
    pub memory_bytes: u64,
    /// The GPU and its graphics API, if the machine reports one.
    #[serde(default)]
    pub gpu: Option<String>,
    /// What placement can ask for, for example `gpu:metal` (plus any in `$FLEET_HOME/tags`).
    #[serde(default)]
    pub tags: Vec<String>,
    /// The machine's idle-sleep timer in minutes (0: never), if known.
    #[serde(default)]
    pub sleep_minutes: Option<u32>,
    /// How many CPU threads fleet may give jobs at once (its cores minus 2, unless `$FLEET_HOME/cpu-slots` says
    /// otherwise).
    pub cpu_slots: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
#[serde(default)]
pub struct Status {
    pub vm_capacity: Option<crate::instances::Capacity>,
    pub instances: Vec<crate::instances::Info>,
    /// The 1-, 5- and 15-minute load averages.
    pub load: Option<[f64; 3]>,
    pub on_ac_power: Option<bool>,
    pub battery_percent: Option<u8>,
    /// Free space on the volume holding the agent's home folder.
    pub disk_free_bytes: Option<u64>,
    pub uptime_seconds: Option<u64>,
    /// IDs of the jobs running now.
    pub running_jobs: Vec<String>,
    /// A GPU process started outside fleet (named in `$FLEET_HOME/gpu-processes`), if one is running.
    pub gpu_busy_external: Option<String>,
}
