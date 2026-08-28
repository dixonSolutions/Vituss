//! The gate.
//!
//! This is what a client connects to. It looks like a single database server; it
//! is actually a stateless router in front of many. Everything that makes that
//! illusion hold — planning, routing, transaction bookkeeping, result merging —
//! meets here.
//!
//! The gate holds no data and no durable state. Every fact it uses comes from the
//! topology, so a gate can be started, stopped or replaced at any time, and a
//! client's session survives because the session travels with the request.

use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::RwLock;

use vituss_core::{
    BindVars, Error, QueryResult, Result, ShardDestination, ShardSession, TabletType, Target,
    TransactionMode, Value,
};
use vituss_dialect::DialectRef;
use vituss_engine::{Executor, SessionRef, ShardGateway};
use vituss_planner::{Plan, Planner};
use vituss_tablet::{QueryService, TabletServer, TransactionId};
use vituss_topo::TopoServer;
use vituss_vschema::{VSchema, VSchemaSpec};

use crate::discovery::{Discovery, TabletEntry};
use crate::resolver::Resolver;

pub struct Gate {
    topo: TopoServer,
    cell: Option<String>,
    resolver: Resolver,
    discovery: Discovery,
    vschema: RwLock<Arc<VSchema>>,
    /// The SQL surface clients speak to this gate. Independent of what the shards
    /// run: a gate can accept MySQL syntax and serve it from PostgreSQL shards.
    client_dialect: DialectRef,
}

impl Gate {
    /// Build a gate from the topology: load the serving graph, the VSchema and
    /// the tablets, and open a connection pool per shard.
    pub async fn bootstrap(
        topo: TopoServer,
        cell: Option<String>,
        client_dialect: DialectRef,
    ) -> Result<Arc<Self>> {
        let cells = topo.list_cells().await?;
        let cell = cell.or_else(|| cells.first().cloned());

        let gate = Arc::new(Self {
            topo,
            cell: cell.clone(),
            resolver: Resolver::new(),
            discovery: Discovery::new(cell.clone()),
            vschema: RwLock::new(Arc::new(VSchema::default())),
            client_dialect,
        });

        gate.reload_serving_graph().await?;
        gate.reload_vschema().await?;
        gate.reload_tablets().await?;
        Ok(gate)
    }

    pub fn vschema(&self) -> Arc<VSchema> {
        self.vschema.read().clone()
    }

    pub fn client_dialect(&self) -> &DialectRef {
        &self.client_dialect
    }

    pub fn topo(&self) -> &TopoServer {
        &self.topo
    }

    pub fn discovery(&self) -> &Discovery {
        &self.discovery
    }

    pub fn resolver(&self) -> &Resolver {
        &self.resolver
    }

    /// Re-read the serving graph for this gate's cell.
    pub async fn reload_serving_graph(&self) -> Result<()> {
        let Some(cell) = &self.cell else {
            return Err(Error::failed_precondition(
                "the topology has no cells; create one before starting a gate",
            ));
        };
        for keyspace in self.topo.list_srv_keyspaces(cell).await? {
            let srv = self.topo.get_srv_keyspace(cell, &keyspace).await?;
            self.resolver.update(keyspace, srv);
        }
        Ok(())
    }

    /// Re-read every keyspace's VSchema and rebuild the resolved form.
    ///
    /// Rebuilt as a whole rather than patched: a VSchema is a web of cross
    /// references (a lookup vindex names a table in another keyspace, a sequence
    /// lives in a third), and validating it piecewise would let an inconsistent
    /// intermediate state go live.
    pub async fn reload_vschema(&self) -> Result<()> {
        let mut spec = VSchemaSpec::default();
        for keyspace in self.topo.list_keyspaces().await? {
            let ks = self.topo.get_keyspace(&keyspace).await?;
            let mut ks_spec: vituss_vschema::KeyspaceSpec = match self
                .topo
                .get_vschema_json(&keyspace)
                .await?
            {
                Some(v) => serde_json::from_value(v).map_err(|e| {
                    Error::invalid(format!("keyspace {keyspace}: invalid VSchema: {e}"))
                })?,
                None => Default::default(),
            };
            // The topology is authoritative about which engine a keyspace runs on.
            ks_spec.dialect = Some(ks.dialect.clone());
            spec.keyspaces.insert(keyspace, ks_spec);
        }

        let rules = self.topo.get_routing_rules().await?;
        spec.routing_rules = rules.rules.into_iter().collect();

        let built = VSchema::build(&spec)?;
        *self.vschema.write() = Arc::new(built);
        Ok(())
    }

    /// Open (or re-open) a tablet for every tablet record in the topology.
    pub async fn reload_tablets(&self) -> Result<()> {
        self.discovery.remove_all();
        for record in self.topo.get_tablets().await? {
            if let Some(cell) = &self.cell {
                // A gate serves its own cell; tablets elsewhere are reached
                // through the gate that is local to them.
                if &record.alias.cell != cell {
                    continue;
                }
            }
            let target = Target {
                keyspace: record.keyspace.clone(),
                shard: record.shard.clone(),
                tablet_type: record.tablet_type,
                cell: Some(record.alias.cell.clone()),
            };
            let server = match TabletServer::open(target.clone(), record.backend.clone()).await {
                Ok(s) => s,
                Err(e) => {
                    // One unreachable shard must not stop the gate: the rest of
                    // the keyspace can still be served, and queries that need
                    // this shard will say so.
                    tracing::error!(
                        tablet = %record.alias,
                        target = %target,
                        error = %e.message,
                        "could not open the tablet's database; it will not serve"
                    );
                    continue;
                }
            };
            let health = server.health().await;
            self.discovery.add(TabletEntry {
                service: server,
                cell: record.alias.cell.clone(),
                health,
            });
        }
        Ok(())
    }

    /// Register an already-built tablet. Used by `vituss combo`, which runs the
    /// tablets in the same process, and by tests.
    pub async fn register_tablet(&self, service: Arc<dyn QueryService>, cell: impl Into<String>) {
        let health = service.health().await;
        self.discovery.add(TabletEntry { service, cell: cell.into(), health });
    }

    /// Refresh every tablet's health.
    pub async fn refresh_health(&self) {
        for entry in self.discovery.all() {
            let health = entry.service.health().await;
            self.discovery.update_health(health);
        }
    }

    fn executor(self: &Arc<Self>) -> Executor {
        Executor::new(
            self.clone() as Arc<dyn ShardGateway>,
            self.vschema(),
            self.client_dialect.clone(),
        )
    }

    /// Plan a statement without running it.
    pub fn plan(self: &Arc<Self>, sql: &str, default_keyspace: Option<&str>) -> Result<Plan> {
        let vschema = self.vschema();
        Planner::new(&vschema, default_keyspace, self.client_dialect.clone()).plan(sql)
    }

    /// Plan and execute a statement for a client session.
    pub async fn execute(
        self: &Arc<Self>,
        sql: &str,
        session: &SessionRef,
        bind_vars: &BindVars,
    ) -> Result<QueryResult> {
        // `VEXPLAIN <query>` returns the plan instead of running it — the fastest
        // way to find out why a query is scattering.
        let trimmed = sql.trim_start();
        if let Some(rest) = strip_prefix_ci(trimmed, "vexplain ") {
            let default_keyspace = session.lock().await.target_keyspace.clone();
            let plan = self.plan(rest, default_keyspace.as_deref())?;
            return Ok(QueryResult::from_rows(
                vec![vituss_core::Field::new("Plan", vituss_core::SqlType::Text)],
                plan.explain()
                    .lines()
                    .map(|l| vec![Value::Text(l.to_string())])
                    .collect(),
            ));
        }

        // Session-setup chatter is answered here, before the planner sees it:
        // `SET NAMES utf8mb4` is not a query, and no shard can answer it.
        if let Some(result) = crate::sysvars::try_handle(sql, session).await {
            return result;
        }

        let default_keyspace = session.lock().await.target_keyspace.clone();
        let plan = self.plan(sql, default_keyspace.as_deref())?;

        {
            let s = session.lock().await;
            // Reads inside a transaction go to the primary, or the client would
            // not see its own uncommitted writes.
            if s.in_transaction && s.tablet_type != TabletType::Primary {
                drop(s);
                session.lock().await.tablet_type = TabletType::Primary;
            }
        }

        self.executor().execute(&plan, session, bind_vars).await
    }

    /// Where to send a statement for a target, resolving the tablet.
    fn tablet_for(&self, target: &Target) -> Result<Arc<dyn QueryService>> {
        self.discovery.pick(target)
    }
}

fn strip_prefix_ci<'a>(s: &'a str, prefix: &str) -> Option<&'a str> {
    if s.len() >= prefix.len() && s[..prefix.len()].eq_ignore_ascii_case(prefix) {
        Some(&s[prefix.len()..])
    } else {
        None
    }
}

#[async_trait]
impl ShardGateway for Gate {
    async fn shards_for(
        &self,
        keyspace: &str,
        destination: &ShardDestination,
        tablet_type: TabletType,
    ) -> Result<Vec<String>> {
        self.resolver.resolve(keyspace, destination, tablet_type)
    }

    async fn dialect_for(&self, keyspace: &str, shard: &str) -> Result<DialectRef> {
        // Per shard, because a keyspace being migrated between engines has shards
        // on both at once. Read from the topology rather than from the tablet:
        // that answer is the same whether the tablet is local or remote.
        let shard_record = self.topo.get_shard(keyspace, shard).await.ok();
        let name = match shard_record.and_then(|s| s.dialect) {
            Some(d) => d,
            None => self.topo.get_keyspace(keyspace).await?.dialect,
        };
        vituss_dialect::get(&name)
    }

    async fn execute(
        &self,
        target: &Target,
        sql: &str,
        params: &[Value],
        session: &SessionRef,
    ) -> Result<QueryResult> {
        let tablet = self.tablet_for(target)?;

        let (existing, in_transaction) = {
            let s = session.lock().await;
            (
                s.find_shard_session(target).map(|ss| ss.transaction_id),
                s.in_transaction,
            )
        };

        if !in_transaction {
            return tablet.execute(sql, params, TransactionId::none()).await;
        }

        if let Some(id) = existing {
            return tablet.execute(sql, params, TransactionId(id)).await;
        }

        // First statement on this shard in this transaction. Refuse to spread the
        // transaction if the session asked for single-shard semantics — silently
        // widening it would break the atomicity the client is relying on.
        {
            let s = session.lock().await;
            if s.transaction_mode == TransactionMode::Single && !s.shard_sessions.is_empty() {
                return Err(Error::failed_precondition(format!(
                    "this statement would add shard {}/{} to a transaction that already spans {}. \
                     The session's transaction mode is 'single'; set vituss_transaction_mode='multi' \
                     or 'two_pc' to allow multi-shard transactions.",
                    target.keyspace,
                    target.shard,
                    s.shard_sessions
                        .iter()
                        .map(|ss| ss.target.shard.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                )));
            }
        }

        let (result, tx) = tablet.begin_execute(sql, params).await?;
        session.lock().await.add_shard_session(ShardSession {
            target: target.clone(),
            transaction_id: tx.0,
            reserved_id: 0,
            rows_affected: result.rows_affected > 0,
        });
        Ok(result)
    }

    async fn commit(&self, session: &SessionRef) -> Result<()> {
        let (shard_sessions, mode) = {
            let s = session.lock().await;
            (s.shard_sessions.clone(), s.transaction_mode)
        };
        if shard_sessions.is_empty() {
            return Ok(());
        }

        if shard_sessions.len() == 1 || mode != TransactionMode::TwoPc {
            return self.commit_sequentially(&shard_sessions).await;
        }
        self.commit_two_phase(&shard_sessions, session).await
    }

    async fn rollback(&self, session: &SessionRef) -> Result<()> {
        let shard_sessions = session.lock().await.shard_sessions.clone();
        let mut first_error = None;
        for ss in &shard_sessions {
            let Ok(tablet) = self.tablet_for(&ss.target) else { continue };
            // Every shard is attempted even if one fails: a shard left with an
            // open transaction holds locks that block everyone else.
            if let Err(e) = tablet.rollback(TransactionId(ss.transaction_id)).await {
                first_error.get_or_insert(e);
            }
        }
        match first_error {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    async fn next_sequence_values(&self, keyspace: &str, table: &str, count: u64) -> Result<Vec<Value>> {
        let shards = self
            .resolver
            .resolve(keyspace, &ShardDestination::AllShards, TabletType::Primary)?;
        let shard = shards.first().ok_or_else(|| {
            Error::unavailable(format!("sequence keyspace {keyspace} has no serving shard"))
        })?;
        let target = Target::new(keyspace, shard, TabletType::Primary);
        let tablet = self.tablet_for(&target)?;
        let dialect = self.dialect_for(keyspace, shard).await?;

        // Reserve a block under a row lock. The lock is what makes two gates
        // handing out ids at the same time safe; without it they would both read
        // the same next_id.
        let tx = tablet.begin().await?;
        let result = async {
            let select = dialect.select_for_update(
                &dialect.quote_ident("next_id"),
                table,
                &format!("{} = 0", dialect.quote_ident("id")),
            );
            let current = tablet.execute(&select, &[], tx).await?;
            let next = current
                .rows
                .first()
                .and_then(|r| r.first())
                .and_then(Value::as_uint)
                .ok_or_else(|| {
                    Error::failed_precondition(format!(
                        "sequence table {keyspace}.{table} has no row with id = 0; \
                         initialise it with: INSERT INTO {table} (id, next_id) VALUES (0, 1)"
                    ))
                })?;

            let update = format!(
                "UPDATE {} SET {} = {} WHERE {} = 0",
                dialect.quote_ident(table),
                dialect.quote_ident("next_id"),
                next + count,
                dialect.quote_ident("id")
            );
            tablet.execute(&update, &[], tx).await?;
            Ok::<u64, Error>(next)
        }
        .await;

        match result {
            Ok(next) => {
                tablet.commit(tx).await?;
                Ok((0..count).map(|i| Value::Uint(next + i)).collect())
            }
            Err(e) => {
                let _ = tablet.rollback(tx).await;
                Err(e)
            }
        }
    }
}

impl Gate {
    /// Commit shards one at a time.
    ///
    /// Not atomic across shards, and deliberately loud about it: if a later shard
    /// fails, the earlier ones have already committed and the error says exactly
    /// which. This is the same trade Vitess makes for its default transaction
    /// mode — two-phase commit is available when the cost is worth paying.
    async fn commit_sequentially(&self, shard_sessions: &[ShardSession]) -> Result<()> {
        let mut committed: Vec<String> = Vec::new();
        for ss in shard_sessions {
            let tablet = self.tablet_for(&ss.target)?;
            match tablet.commit(TransactionId(ss.transaction_id)).await {
                Ok(()) => committed.push(ss.target.shard.clone()),
                Err(e) => {
                    // Undo what is still undoable.
                    for remaining in shard_sessions.iter().skip(committed.len() + 1) {
                        if let Ok(t) = self.tablet_for(&remaining.target) {
                            let _ = t.rollback(TransactionId(remaining.transaction_id)).await;
                        }
                    }
                    return Err(Error::aborted(format!(
                        "commit failed on shard {}/{}: {}. {} shard(s) had already committed ({}); \
                         the transaction is partially applied.",
                        ss.target.keyspace,
                        ss.target.shard,
                        e.message,
                        committed.len(),
                        if committed.is_empty() { "none".to_string() } else { committed.join(", ") }
                    )));
                }
            }
        }
        Ok(())
    }

    /// Commit atomically, using the engines' own distributed-transaction support.
    async fn commit_two_phase(&self, shard_sessions: &[ShardSession], session: &SessionRef) -> Result<()> {
        let xid = format!("vituss:{}", session.lock().await.session_uuid);

        // Every participant must speak the same 2PC protocol. Mixing XA and
        // prepared transactions across engines has no recovery story, so it is
        // refused rather than attempted.
        let mut protocol: Option<&'static str> = None;
        for ss in shard_sessions {
            let dialect = self.dialect_for(&ss.target.keyspace, &ss.target.shard).await?;
            let caps = dialect.capabilities();
            if !caps.can_two_pc() {
                return Err(Error::unimplemented(format!(
                    "shard {}/{} runs {}, which cannot take part in a two-phase commit. \
                     Use vituss_transaction_mode='multi' and accept non-atomic multi-shard commits, \
                     or keep the transaction on one shard.",
                    ss.target.keyspace,
                    ss.target.shard,
                    dialect.name()
                )));
            }
            let style = match caps.two_pc {
                vituss_dialect::TwoPcStyle::Xa => "xa",
                vituss_dialect::TwoPcStyle::PreparedTransaction => "prepared-transaction",
                _ => unreachable!("can_two_pc was checked"),
            };
            match protocol {
                None => protocol = Some(style),
                Some(p) if p == style => {}
                Some(p) => {
                    return Err(Error::unimplemented(format!(
                        "this transaction spans engines using different two-phase commit protocols \
                         ({p} and {style}); Vituss will not coordinate across them"
                    )))
                }
            }
        }

        // Phase one: everybody prepares. Any failure here is still safe to undo.
        for ss in shard_sessions {
            let tablet = self.tablet_for(&ss.target)?;
            if let Err(e) = tablet.prepare(TransactionId(ss.transaction_id), &xid).await {
                for other in shard_sessions {
                    if let Ok(t) = self.tablet_for(&other.target) {
                        let _ = t.rollback(TransactionId(other.transaction_id)).await;
                        let _ = t.rollback_prepared(&xid).await;
                    }
                }
                return Err(Error::aborted(format!(
                    "two-phase commit aborted while preparing shard {}/{}: {}. \
                     Nothing was committed.",
                    ss.target.keyspace, ss.target.shard, e.message
                )));
            }
        }

        // Phase two: everybody commits. A failure here cannot be undone — the
        // transaction is decided — so it is retried by the resolver, not rolled back.
        let mut failures = Vec::new();
        for ss in shard_sessions {
            let tablet = self.tablet_for(&ss.target)?;
            if let Err(e) = tablet.commit_prepared(&xid).await {
                failures.push(format!("{}/{}: {}", ss.target.keyspace, ss.target.shard, e.message));
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(Error::internal(format!(
                "transaction {xid} was prepared everywhere but could not be committed on {} shard(s): {}. \
                 It is still prepared there and must be committed — it will not roll back.",
                failures.len(),
                failures.join("; ")
            )))
        }
    }
}
