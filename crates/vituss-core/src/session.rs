//! Connection session state.
//!
//! The gate is stateless across connections; everything that must persist for a
//! client lives here and travels with each request. That includes the open
//! per-shard transactions, which is what makes cross-shard transactions possible
//! without pinning a client to one gate.

use std::collections::BTreeMap;

use crate::target::{Target, TabletType};
use crate::value::Value;

/// How far Vituss will go to keep a multi-shard transaction atomic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransactionMode {
    /// Reject any transaction that would touch more than one shard.
    Single,
    /// Allow multi-shard transactions, committing shards one by one on a
    /// best-effort basis. A failure part-way leaves shards inconsistent; this is
    /// the pragmatic default and matches Vitess's `MULTI`.
    #[default]
    Multi,
    /// Two-phase commit via the backend's own distributed-transaction support
    /// (MySQL XA, PostgreSQL prepared transactions, MS DTC). Correct but slow,
    /// and only available when every shard in the transaction supports it.
    TwoPc,
}

/// An open transaction on one shard.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ShardSession {
    pub target: Target,
    /// Opaque handle the owning tablet uses to find the reserved connection.
    pub transaction_id: u64,
    /// Set when the connection is *reserved* (held open across statements because
    /// the client set a session system variable or took a lock) even outside a
    /// transaction.
    #[serde(default)]
    pub reserved_id: u64,
    /// Whether this shard has done any write. A read-only participant can be
    /// released early instead of joining the commit.
    #[serde(default)]
    pub rows_affected: bool,
}

/// Everything Vituss remembers about a client connection.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Session {
    /// Current default keyspace, from `USE <ks>` or the connect-time database.
    pub target_keyspace: Option<String>,
    /// Shard pinned by `USE ks:-80`, if any.
    pub target_shard: Option<String>,
    /// Tablet type selected by `USE ks@replica` or the connect string.
    pub tablet_type: TabletType,
    pub in_transaction: bool,
    pub autocommit: bool,
    pub transaction_mode: TransactionMode,
    pub shard_sessions: Vec<ShardSession>,
    /// Pre-sessions run before the main transaction commits (used to insert
    /// lookup-vindex rows so that a failed main commit does not orphan them).
    pub pre_sessions: Vec<ShardSession>,
    /// Post-sessions run after commit (used to delete lookup rows).
    pub post_sessions: Vec<ShardSession>,
    pub system_variables: BTreeMap<String, String>,
    pub user_defined_variables: BTreeMap<String, Value>,
    pub last_insert_id: Option<u64>,
    pub rows_affected: u64,
    pub found_rows: u64,
    pub warnings: Vec<String>,
    /// Session UUID, used in logs and in 2PC transaction identifiers.
    pub session_uuid: String,
    /// Authenticated user, for table ACLs.
    pub user: Option<String>,
}

impl Default for Session {
    fn default() -> Self {
        Self {
            target_keyspace: None,
            target_shard: None,
            tablet_type: TabletType::Primary,
            in_transaction: false,
            autocommit: true,
            transaction_mode: TransactionMode::Multi,
            shard_sessions: Vec::new(),
            pre_sessions: Vec::new(),
            post_sessions: Vec::new(),
            system_variables: BTreeMap::new(),
            user_defined_variables: BTreeMap::new(),
            last_insert_id: None,
            rows_affected: 0,
            found_rows: 0,
            warnings: Vec::new(),
            session_uuid: uuid_v4(),
            user: None,
        }
    }
}

fn uuid_v4() -> String {
    uuid::Uuid::new_v4().to_string()
}

impl Session {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn find_shard_session(&self, target: &Target) -> Option<&ShardSession> {
        self.shard_sessions
            .iter()
            .find(|s| s.target.keyspace == target.keyspace && s.target.shard == target.shard)
    }

    pub fn add_shard_session(&mut self, s: ShardSession) {
        if let Some(existing) = self
            .shard_sessions
            .iter_mut()
            .find(|e| e.target.keyspace == s.target.keyspace && e.target.shard == s.target.shard)
        {
            *existing = s;
        } else {
            self.shard_sessions.push(s);
        }
    }

    /// Clear all transaction state. Called after commit or rollback.
    pub fn reset_transaction(&mut self) {
        self.in_transaction = false;
        self.shard_sessions.clear();
        self.pre_sessions.clear();
        self.post_sessions.clear();
    }

    /// True when the open transaction spans more than one shard.
    pub fn is_multi_shard(&self) -> bool {
        self.shard_sessions.len() > 1
    }
}
