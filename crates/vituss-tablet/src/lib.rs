//! # vituss-tablet
//!
//! The shard-side server. One tablet owns one database — one shard of one
//! keyspace — and is the only thing in Vituss that holds a connection to it.
//!
//! Its job is narrow on purpose: execute what it is given, hold transactions
//! open across statements, know its own schema, and tell the truth about its
//! health. All the routing intelligence is in the gate; a tablet never decides
//! where a query should go.

pub mod schema;
pub mod server;
pub mod service;

pub use schema::{ColumnSchema, IndexSchema, TableSchema};
pub use server::TabletServer;
pub use service::{QueryService, TabletHealth, TransactionId};
