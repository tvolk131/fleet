//! Replicated control plane and opt-in workers, with explicit isolated state homes.

pub mod admin;
pub mod checkpoint;
pub mod client;
pub mod discovery;
pub mod ids;
pub mod membership;
pub mod model;
pub mod network;
pub mod queue;
pub mod recovery;
pub mod runtime;
pub mod service;
pub mod store;
pub mod upgrade;
pub mod workload;

pub use model::{Command, Mutation, Reply, Request, State};
pub use network::{Client, Config, Replica};
use std::io::Cursor;
pub use store::Store;

openraft::declare_raft_types!(
    pub TypeConfig:
        D = Command,
        R = Reply,
        NodeId = u64,
        Node = openraft::BasicNode,
);

pub type Raft = openraft::Raft<TypeConfig>;
pub type Entry = openraft::Entry<TypeConfig>;
pub const FORMAT: u32 = 6;
