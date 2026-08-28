//! The execution hook lookup vindexes use.
//!
//! A lookup vindex has to run a query of its own — `SELECT toid FROM lookup
//! WHERE fromid IN (…)` — in the middle of planning or executing the user's
//! query. It must do so inside the caller's session so that the lookup row and
//! the row it describes commit or roll back together.
//!
//! The gate implements this trait; the vindex crate only declares it, which keeps
//! the dependency pointing the right way.

use async_trait::async_trait;

use vituss_core::{BindVars, QueryResult, Result};

/// When a vindex's own write must be committed relative to the user's transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CommitOrder {
    /// Part of the user's transaction.
    #[default]
    Normal,
    /// Committed *before* the user's transaction. Lookup rows are inserted here so
    /// that a duplicate-key clash is detected before the owning row is written.
    Pre,
    /// Committed *after* the user's transaction. Lookup rows are deleted here so
    /// that a rolled-back delete does not lose the mapping.
    Post,
    /// Committed immediately on its own connection, outside the transaction.
    Autocommit,
}

#[async_trait]
pub trait VCursor: Send + Sync {
    /// Run a query against the vindex's own keyspace.
    async fn execute(
        &self,
        method: &str,
        query: &str,
        bind_vars: &BindVars,
        rollback_on_error: bool,
        commit_order: CommitOrder,
    ) -> Result<QueryResult>;

    /// Run a query against one specific keyspace ID — used by `consistent_lookup`
    /// to write the lookup row on the shard that owns it.
    async fn execute_keyspace_id(
        &self,
        keyspace: &str,
        keyspace_id: &[u8],
        query: &str,
        bind_vars: &BindVars,
        rollback_on_error: bool,
        autocommit: bool,
    ) -> Result<QueryResult>;

    /// True when the cursor sits inside a transaction that is performing a DML.
    /// `consistent_lookup` takes stronger locks in that case.
    fn in_transaction_and_is_dml(&self) -> bool {
        false
    }
}
