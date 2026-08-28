//! What a tablet offers to the gate.
//!
//! Modelled as a trait so the gate does not care whether the tablet is in the
//! same process (as in `vituss combo`), across a network, or a test double.

use async_trait::async_trait;

use vituss_core::{QueryResult, Result, Target, Value};

/// A transaction the tablet is holding open on behalf of a session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TransactionId(pub u64);

impl TransactionId {
    pub fn none() -> Self {
        Self(0)
    }
    pub fn is_none(self) -> bool {
        self.0 == 0
    }
}

/// The tablet's view of its own health, published to the gate.
#[derive(Debug, Clone, PartialEq)]
pub struct TabletHealth {
    pub target: Target,
    pub serving: bool,
    pub replication_lag_secs: Option<u64>,
    pub server_version: Option<String>,
    pub error: Option<String>,
}

#[async_trait]
pub trait QueryService: Send + Sync {
    fn target(&self) -> &Target;

    /// Run a statement, optionally inside a transaction this tablet is holding.
    async fn execute(
        &self,
        sql: &str,
        params: &[Value],
        transaction: TransactionId,
    ) -> Result<QueryResult>;

    /// Open a transaction and return its handle.
    async fn begin(&self) -> Result<TransactionId>;

    /// Open a transaction and run the first statement in one round trip.
    ///
    /// Worth a dedicated call because the common case is a single-shard
    /// transaction, where this halves the latency.
    async fn begin_execute(
        &self,
        sql: &str,
        params: &[Value],
        ) -> Result<(QueryResult, TransactionId)> {
        let tx = self.begin().await?;
        let r = self.execute(sql, params, tx).await?;
        Ok((r, tx))
    }

    async fn commit(&self, transaction: TransactionId) -> Result<()>;
    async fn rollback(&self, transaction: TransactionId) -> Result<()>;

    /// Prepare a transaction for a distributed commit.
    async fn prepare(&self, transaction: TransactionId, xid: &str) -> Result<()>;
    async fn commit_prepared(&self, xid: &str) -> Result<()>;
    async fn rollback_prepared(&self, xid: &str) -> Result<()>;

    async fn health(&self) -> TabletHealth;

    /// Tables this tablet serves, as discovered from the database itself.
    async fn schema(&self) -> Result<Vec<crate::schema::TableSchema>>;

    /// Release everything. Open transactions are rolled back.
    async fn close(&self);
}
