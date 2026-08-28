//! The driver plug-in traits.
//!
//! `vituss-dialect` describes what an engine's SQL looks like; this describes how
//! to *talk* to one. The split matters: a shard could be reached over a proxy, a
//! connection pooler or a completely different transport while still speaking the
//! same dialect.

use async_trait::async_trait;

use vituss_core::{QueryResult, Result, Value};
use vituss_dialect::DialectRef;

/// A live connection to one shard's database.
///
/// Connections are stateful on purpose. A transaction, a session variable or an
/// advisory lock lives on a specific connection, and Vituss must be able to keep
/// hold of that connection for as long as the client's session needs it — that is
/// what a *reserved* connection is.
#[async_trait]
pub trait Connection: Send + Sync {
    /// Run a statement with positional parameters already rendered for this engine.
    async fn execute(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult>;

    /// Run a statement that takes no parameters — `BEGIN`, `SAVEPOINT`, `XA PREPARE`.
    async fn execute_raw(&mut self, sql: &str) -> Result<QueryResult> {
        self.execute(sql, &[]).await
    }

    async fn begin(&mut self, isolation: Option<&str>) -> Result<()>;
    async fn commit(&mut self) -> Result<()>;
    async fn rollback(&mut self) -> Result<()>;

    async fn savepoint(&mut self, name: &str) -> Result<()>;
    async fn rollback_to_savepoint(&mut self, name: &str) -> Result<()>;
    async fn release_savepoint(&mut self, name: &str) -> Result<()>;

    /// Prepare this connection's transaction for a distributed commit.
    ///
    /// Fails on engines whose capabilities say they cannot do it, rather than
    /// pretending to and losing atomicity silently.
    async fn prepare_two_pc(&mut self, xid: &str) -> Result<()>;
    async fn commit_prepared(&mut self, xid: &str) -> Result<()>;
    async fn rollback_prepared(&mut self, xid: &str) -> Result<()>;

    async fn ping(&mut self) -> Result<()>;

    fn in_transaction(&self) -> bool;

    /// False once the connection has hit an error that makes its state unknown.
    /// Such a connection is closed rather than returned to the pool.
    fn is_healthy(&self) -> bool {
        true
    }
}

/// Health of a backend, as reported to the discovery layer.
#[derive(Debug, Clone, PartialEq)]
pub struct Health {
    pub reachable: bool,
    pub server_version: Option<String>,
    /// Position in the engine's change stream, when it has one. Used to compute
    /// replication lag and to decide whether a replica may serve reads.
    pub replication_position: Option<String>,
    /// Seconds this replica is behind its primary, when known.
    pub replication_lag_secs: Option<u64>,
    pub error: Option<String>,
}

impl Health {
    pub fn unreachable(err: impl Into<String>) -> Self {
        Self {
            reachable: false,
            server_version: None,
            replication_position: None,
            replication_lag_secs: None,
            error: Some(err.into()),
        }
    }
}

impl std::fmt::Debug for dyn Backend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Backend({})", self.describe())
    }
}

impl std::fmt::Debug for dyn Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Connection(in_tx={}, healthy={})", self.in_transaction(), self.is_healthy())
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize)]
pub struct PoolStats {
    pub size: u32,
    pub idle: u32,
    pub in_use: u32,
    pub max: u32,
}

/// A connection pool for one shard's database.
#[async_trait]
pub trait Backend: Send + Sync {
    /// The SQL surface this backend speaks. The gate renders each shard's SQL
    /// through this, so shards of one keyspace may legitimately differ.
    fn dialect(&self) -> &DialectRef;

    /// Human-readable target, for logs. Must not contain credentials.
    fn describe(&self) -> String;

    async fn acquire(&self) -> Result<Box<dyn Connection>>;

    async fn health(&self) -> Result<Health>;

    fn stats(&self) -> PoolStats;

    /// Close the pool. Idempotent.
    async fn close(&self);
}
