//! The hook that lets a lookup vindex run its own query.
//!
//! A lookup vindex has to consult a table in the middle of routing the user's
//! query. That table is itself a Vituss table — possibly sharded, possibly on a
//! different engine — so the lookup goes back through the planner and the
//! executor rather than to a fixed connection.

use async_trait::async_trait;

use vituss_core::{BindVars, Error, QueryResult, Result, Session, ShardDestination, Value};
use vituss_planner::{Planner, Primitive, RouteOpcode};
use vituss_vindex::cursor::{CommitOrder, VCursor};

use crate::exec::Executor;
use crate::gateway::SessionRef;

pub struct EngineCursor {
    executor: Executor,
    session: SessionRef,
}

impl EngineCursor {
    pub fn new(executor: Executor, session: SessionRef) -> Self {
        Self { executor, session }
    }

    /// The session a vindex write should run in.
    ///
    /// `Pre` and `Post` writes deliberately do *not* join the user's transaction:
    /// a lookup row must exist before the row it describes is written, and must
    /// survive until after that row is gone. Committing them separately is what
    /// makes a crash between the two recoverable — the lookup table may briefly
    /// name a row that does not exist yet, which a reader treats as a miss, but
    /// it never fails to name a row that does.
    async fn session_for(&self, order: CommitOrder) -> SessionRef {
        match order {
            CommitOrder::Normal => self.session.clone(),
            _ => {
                let base = self.session.lock().await;
                let mut s = Session::new();
                s.target_keyspace = base.target_keyspace.clone();
                s.user = base.user.clone();
                s.autocommit = true;
                crate::exec::new_session(s)
            }
        }
    }
}

#[async_trait]
impl VCursor for EngineCursor {
    async fn execute(
        &self,
        method: &str,
        query: &str,
        bind_vars: &BindVars,
        _rollback_on_error: bool,
        commit_order: CommitOrder,
    ) -> Result<QueryResult> {
        let planner = Planner::new(
            self.executor.vschema(),
            None,
            self.executor.client_dialect().clone(),
        );
        let plan = planner.plan(query).map_err(|e| {
            Error::new(e.code, format!("{method}: {} (query: {query})", e.message))
        })?;

        let session = self.session_for(commit_order).await;
        let result = self.executor.execute(&plan, &session, bind_vars).await;

        // A separately-committed vindex write has to be committed here; nothing
        // else will.
        if commit_order != CommitOrder::Normal {
            match &result {
                Ok(_) => self.executor.gateway().commit(&session).await?,
                Err(_) => {
                    let _ = self.executor.gateway().rollback(&session).await;
                }
            }
        }
        result
    }

    async fn execute_keyspace_id(
        &self,
        keyspace_or_table: &str,
        keyspace_id: &[u8],
        query: &str,
        bind_vars: &BindVars,
        _rollback_on_error: bool,
        autocommit: bool,
    ) -> Result<QueryResult> {
        // A vindex knows the name of its lookup *table*, not the keyspace it lives
        // in, so accept either and resolve.
        let keyspace = match self.executor.vschema().keyspace(keyspace_or_table) {
            Ok(k) => k.name.clone(),
            Err(_) => self
                .executor
                .vschema()
                .find_table(None, keyspace_or_table)
                .map(|t| t.keyspace.clone())?,
        };

        let planner = Planner::new(
            self.executor.vschema(),
            Some(&keyspace),
            self.executor.client_dialect().clone(),
        );
        let mut plan = planner.plan(query)?;

        // Override the routing: this statement must land on the shard that owns
        // the given keyspace id, whatever the planner would otherwise have chosen.
        match &mut plan.primitive {
            Primitive::Route(r) => {
                r.opcode = RouteOpcode::ByDestination;
                r.destination = Some(ShardDestination::KeyspaceId(vituss_core::KeyspaceId(
                    keyspace_id.to_vec(),
                )));
            }
            Primitive::Insert(i) => {
                i.opcode = RouteOpcode::ByDestination;
                i.pinned = Some(vituss_core::KeyspaceId(keyspace_id.to_vec()));
            }
            Primitive::Dml(d) => {
                d.route.opcode = RouteOpcode::ByDestination;
                d.route.destination = Some(ShardDestination::KeyspaceId(vituss_core::KeyspaceId(
                    keyspace_id.to_vec(),
                )));
            }
            other => {
                return Err(Error::internal(format!(
                    "a keyspace-id-targeted vindex query planned as {other:?}, which cannot be targeted"
                )))
            }
        }

        let session = if autocommit {
            self.session_for(CommitOrder::Autocommit).await
        } else {
            self.session.clone()
        };
        let result = self.executor.execute(&plan, &session, bind_vars).await;
        if autocommit {
            match &result {
                Ok(_) => self.executor.gateway().commit(&session).await?,
                Err(_) => {
                    let _ = self.executor.gateway().rollback(&session).await;
                }
            }
        }
        result
    }

    fn in_transaction_and_is_dml(&self) -> bool {
        // Checked without blocking: the caller only uses it to decide how strong a
        // lock to take, and a conservative answer is safe.
        self.session
            .try_lock()
            .map(|s| s.in_transaction)
            .unwrap_or(true)
    }
}

/// Unused-value guard so the module's intent stays readable.
#[allow(dead_code)]
fn _value_marker(_: Value) {}
