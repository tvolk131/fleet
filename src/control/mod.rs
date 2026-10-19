//! Replicated control plane and opt-in workers, with explicit isolated state homes.

pub mod admin;
pub mod artifact;
pub mod artifact_io;
pub mod checkpoint;
pub mod client;
pub mod discovery;
pub mod history;
mod history_read;
pub mod ids;
pub mod input_upload;
pub mod membership;
pub mod model;
pub mod network;
pub mod portable;
pub mod queue;
pub mod recovery;
mod retention;
pub mod runtime;
pub mod service;
pub mod storage;
pub mod storage_lifecycle;
mod storage_status;
pub mod store;
pub mod tunnels;
pub mod upgrade;
pub mod version;
pub mod vm;
pub mod workload;

pub use model::{Command, Mutation, Reply, Request, State};
pub use network::{Client, Config, Replica};
pub use store::Store;

openraft::declare_raft_types!(
    pub TypeConfig:
        D = Command,
        R = Reply,
        NodeId = u64,
        Node = openraft::BasicNode,
        SnapshotData = tokio::fs::File,
);

pub type Raft = openraft::Raft<TypeConfig>;
pub type Entry = openraft::Entry<TypeConfig>;
// Physical metadata layout. Independent of wire and cluster behavior generations.
pub const FORMAT: u32 = 12;
