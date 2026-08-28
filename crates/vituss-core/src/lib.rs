//! # vituss-core
//!
//! The vocabulary types every other Vituss crate speaks: values, result sets,
//! keyspace IDs / key ranges, routing destinations, tablet targets, sessions
//! and errors.
//!
//! Nothing in this crate knows about a specific database engine. Engine-specific
//! behaviour lives behind the traits in `vituss-dialect` (SQL surface) and
//! `vituss-backend` (drivers).

pub mod config;
pub mod error;
pub mod key;
pub mod result;
pub mod session;
pub mod target;
pub mod value;

pub use config::BackendConfig;
pub use error::{Code, Error, Result};
pub use key::{KeyRange, KeyspaceId, ShardDestination};
pub use result::{Field, QueryResult, Row};
pub use session::{Session, ShardSession, TransactionMode};
pub use target::{Target, TabletAlias, TabletType};
pub use value::{BindVars, SqlType, Value};
