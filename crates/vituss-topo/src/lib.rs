//! # vituss-topo
//!
//! The cluster's metadata: which keyspaces exist, how they are sharded, which
//! tablets serve which shard, and which database engine backs each one.
//!
//! The store underneath is pluggable ([`TopoStore`]): memory for `vituss combo`
//! and tests, a directory of JSON files for single-node and development use, and
//! room for etcd, Consul or ZooKeeper without touching anything above.
//!
//! The typed API ([`TopoServer`]) is what the rest of Vituss uses.

pub mod file;
pub mod memory;
pub mod records;
pub mod server;
pub mod store;

pub use file::FileStore;
pub use memory::MemoryStore;
pub use records::{
    BackendConfig, CellInfo, Keyspace, KeyspacePartition, KeyspaceType, Shard, ShardReference,
    SourceShard, SrvKeyspace, Tablet, TabletControl,
};
pub use server::{set_dialect_validator, RoutingRules, TopoServer};
pub use store::{LockHandle, TopoStore, Version, Versioned, WatchEvent};
