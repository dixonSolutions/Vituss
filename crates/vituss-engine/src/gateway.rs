//! The seam between "decide what to do" and "actually talk to a shard".
//!
//! The engine knows how to fan a plan out and put the answers back together. It
//! does not know where shards live, how they are discovered, or how a connection
//! to one is obtained — that is the gateway's job, and it is why the engine can be
//! tested against a handful of in-memory shards and run unchanged against a real
//! cluster.

use std::sync::Arc;

use async_trait::async_trait;

use vituss_core::{QueryResult, Result, Session, ShardDestination, TabletType, Target, Value};
use vituss_dialect::DialectRef;

/// The session, shared for the duration of one request.
///
/// Shared rather than borrowed because a scattered query touches several shards
/// concurrently and each of them may need to open — and record — its own
/// transaction on the session.
pub type SessionRef = Arc<tokio::sync::Mutex<Session>>;

#[async_trait]
pub trait ShardGateway: Send + Sync {
    /// Shard names that serve a destination, in key-range order.
    async fn shards_for(
        &self,
        keyspace: &str,
        destination: &ShardDestination,
        tablet_type: TabletType,
    ) -> Result<Vec<String>>;

    /// The SQL surface a specific shard speaks.
    ///
    /// Per shard, not per keyspace: during an engine migration the shards of one
    /// keyspace legitimately differ, and each one's statement is rendered for
    /// whatever it is actually running.
    async fn dialect_for(&self, keyspace: &str, shard: &str) -> Result<DialectRef>;

    /// Run a statement on one shard, joining the session's transaction if it has
    /// one there and starting one if it should.
    async fn execute(
        &self,
        target: &Target,
        sql: &str,
        params: &[Value],
        session: &SessionRef,
    ) -> Result<QueryResult>;

    /// Commit every shard the session has open.
    async fn commit(&self, session: &SessionRef) -> Result<()>;

    /// Roll back every shard the session has open.
    async fn rollback(&self, session: &SessionRef) -> Result<()>;

    /// Take the next `count` values from a sequence.
    ///
    /// Sequences live in an unsharded keyspace and are read with their own
    /// transaction, so a rolled-back insert does not hand the same id out twice.
    async fn next_sequence_values(&self, keyspace: &str, table: &str, count: u64) -> Result<Vec<Value>>;
}
