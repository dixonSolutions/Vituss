//! # vituss-ctl
//!
//! The control plane. Everything that changes what the cluster *is*, as opposed
//! to what is in it: creating keyspaces and shards, publishing VSchemas,
//! rebuilding the serving graph, planning a reshard.
//!
//! Its centrepiece is [`ClusterConfig`]: the whole cluster in one reviewable
//! file, applied idempotently. That includes which engine each keyspace runs on,
//! which is the only place a Vituss deployment has to say so.

pub mod config;
pub mod ops;

pub use config::{ClusterConfig, KeyspaceConfig, ShardSpec, TabletConfig};
pub use ops::{ApplyReport, Ctl};
