//! # vituss-backend
//!
//! The drivers. Where `vituss-dialect` says what an engine's SQL looks like, this
//! crate says how to connect to one, run a statement and get rows back as neutral
//! [`Value`](vituss_core::Value)s.
//!
//! Everything above here works against [`Backend`] and [`Connection`] and never
//! names a driver. That is what allows two shards of the same keyspace to run
//! different engines: the gate renders each shard's SQL through that shard's
//! dialect and executes it through that shard's driver.
//!
//! Drivers are feature-gated so a deployment only compiles what it uses:
//!
//! | feature | engines |
//! |---|---|
//! | `sqlite` (default) | SQLite / libSQL |
//! | `mysql` | MySQL, MariaDB, Percona |
//! | `postgres` | PostgreSQL, CockroachDB |
//! | `mssql` | SQL Server, Azure SQL |
//! | `all-engines` | all of the above |

pub mod backend;
pub mod common;
pub mod drivers;
pub mod fake;
pub mod registry;

pub use backend::{Backend, Connection, Health, PoolStats};
pub use fake::{FakeBackend, FakeRule};
pub use registry::{open, register, registered, BackendBuilder};

pub use vituss_core::BackendConfig;
