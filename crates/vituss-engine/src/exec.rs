//! The executor: runs a plan.
//!
//! Every primitive the planner can emit is handled here. The shape of the code
//! follows the shape of the plan — a `Route` fans out and gathers, everything
//! else takes what a child produced and does one thing to it.

use std::collections::BTreeMap;
use std::sync::Arc;

use futures::future::BoxFuture;
use sqlparser::ast::{Expr, Statement, Values};

use vituss_core::{
    BindVars, Error, KeyspaceId, QueryResult, Result, Session, ShardDestination, TabletType, Target, Value,
};
use vituss_dialect::DialectRef;
use vituss_planner::{
    DmlPlan, InsertPlan, Join, JoinKind, Plan, Primitive, Route, RouteOpcode, RouteValue,
    SessionOp, SetScope, ShowPlan,
};
use vituss_vschema::VSchema;

use crate::combine;
use crate::cursor::EngineCursor;
use crate::gateway::{SessionRef, ShardGateway};

/// Runs plans against a cluster.
#[derive(Clone)]
pub struct Executor {
    pub(crate) gateway: Arc<dyn ShardGateway>,
    pub(crate) vschema: Arc<VSchema>,
    /// The dialect the client speaks, used to render statements Vituss builds
    /// itself before they are re-rendered for each shard.
    pub(crate) client_dialect: DialectRef,
}

impl Executor {
    pub fn new(gateway: Arc<dyn ShardGateway>, vschema: Arc<VSchema>, client_dialect: DialectRef) -> Self {
        Self { gateway, vschema, client_dialect }
    }

    pub fn vschema(&self) -> &Arc<VSchema> {
        &self.vschema
    }

    pub fn gateway(&self) -> &Arc<dyn ShardGateway> {
        &self.gateway
    }

    pub fn client_dialect(&self) -> &DialectRef {
        &self.client_dialect
    }

    /// Execute a plan.
    pub async fn execute(&self, plan: &Plan, session: &SessionRef, bind_vars: &BindVars) -> Result<QueryResult> {
        self.run(&plan.primitive, session, bind_vars).await
    }

    fn run<'a>(
        &'a self,
        p: &'a Primitive,
        session: &'a SessionRef,
        bv: &'a BindVars,
    ) -> BoxFuture<'a, Result<QueryResult>> {
        Box::pin(async move {
            match p {
                Primitive::Route(r) => self.run_route(r, session, bv).await,
                Primitive::Insert(i) => self.run_insert(i, session, bv).await,
                Primitive::Dml(d) => self.run_dml(d, session, bv).await,
                Primitive::Join(j) => self.run_join(j, session, bv).await,
                Primitive::Limit(l) => {
                    let mut out = self.run(&l.input, session, bv).await?;
                    let count = l.count.as_ref().and_then(|v| self.value_of(v, bv).ok()).and_then(|v| v.as_uint());
                    let offset = l.offset.as_ref().and_then(|v| self.value_of(v, bv).ok()).and_then(|v| v.as_uint());
                    combine::limit(&mut out, count.map(|c| c as usize), offset.map(|o| o as usize));
                    Ok(out)
                }
                Primitive::Sort(s) => {
                    let mut out = self.run(&s.input, session, bv).await?;
                    combine::sort(&mut out, &s.order_by, s.limit);
                    Ok(out)
                }
                Primitive::Distinct(d) => {
                    let mut out = self.run(&d.input, session, bv).await?;
                    combine::distinct(&mut out, &d.columns);
                    Ok(out)
                }
                Primitive::Aggregate(a) => {
                    let input = self.run(&a.input, session, bv).await?;
                    let mut out = combine::aggregate(input, &a.aggregates, &a.group_by, a.ordered)?;
                    if a.truncate_columns > 0 {
                        out.truncate_columns(a.truncate_columns);
                    }
                    Ok(out)
                }
                Primitive::Truncate { input, columns } => {
                    let mut out = self.run(input, session, bv).await?;
                    out.truncate_columns(*columns);
                    Ok(out)
                }
                Primitive::Concatenate { inputs, distinct } => {
                    let mut results = Vec::with_capacity(inputs.len());
                    for i in inputs {
                        results.push(self.run(i, session, bv).await?);
                    }
                    let mut out = combine::concat(results);
                    if *distinct {
                        combine::distinct(&mut out, &[]);
                    }
                    Ok(out)
                }
                Primitive::SingleRow => Ok(QueryResult { rows: vec![vec![]], ..Default::default() }),
                Primitive::Ddl(d) => self.run_ddl(d, session).await,
                Primitive::Session(op) => self.run_session(op, session).await,
                Primitive::Show(s) => self.run_show(s, session).await,
            }
        })
    }

    // -- Route ---------------------------------------------------------------

    async fn run_route(&self, route: &Route, session: &SessionRef, bv: &BindVars) -> Result<QueryResult> {
        let tablet_type = session.lock().await.tablet_type;
        let destination = self.destination_for(route, session, bv).await?;

        if matches!(destination, ShardDestination::None) {
            // The predicate cannot match anything, so no shard is contacted at all.
            return Ok(QueryResult::default());
        }

        let shards = self
            .gateway
            .shards_for(&route.keyspace, &destination, tablet_type)
            .await?;
        if shards.is_empty() {
            return Err(Error::unavailable(format!(
                "no shard of keyspace {} is serving {} queries for destination {destination}",
                route.keyspace, tablet_type
            )));
        }

        let mut merged = route.bind_vars.clone();
        merged.extend(bv.clone());

        let results = self
            .fan_out(&route.keyspace, &shards, tablet_type, &route.statement, &merged, session)
            .await?;

        let mut out = if route.order_by.is_empty() {
            combine::concat(results)
        } else {
            // Each shard sorted its own rows, so this is a merge, not a re-sort.
            combine::merge_sorted(results, &route.order_by)
        };
        if route.truncate_columns > 0 {
            out.truncate_columns(route.truncate_columns);
        }
        Ok(out)
    }

    /// Send one statement to several shards, rendering it separately for each.
    ///
    /// The per-shard rendering is the point: two shards of the same keyspace can
    /// be running different engines, and each gets SQL with its own placeholder
    /// syntax and identifier quoting.
    async fn fan_out(
        &self,
        keyspace: &str,
        shards: &[String],
        tablet_type: TabletType,
        statement: &Statement,
        bind_vars: &BindVars,
        session: &SessionRef,
    ) -> Result<Vec<QueryResult>> {
        let in_transaction = session.lock().await.in_transaction;

        let mut futures = Vec::with_capacity(shards.len());
        for shard in shards {
            let target = Target::new(keyspace, shard, tablet_type);
            let dialect = self.gateway.dialect_for(keyspace, shard).await?;
            let rendered =
                vituss_dialect::render::render_for(statement, bind_vars, self.client_dialect.as_ref(), dialect.as_ref())?;
            futures.push(async move {
                self.gateway
                    .execute(&target, &rendered.sql, &rendered.params, session)
                    .await
            });
        }

        if in_transaction {
            // Inside a transaction the order shards are touched in decides the
            // lock order, and a deterministic one is what keeps concurrent
            // multi-shard transactions from deadlocking against each other.
            let mut out = Vec::with_capacity(futures.len());
            for f in futures {
                out.push(f.await?);
            }
            Ok(out)
        } else {
            futures::future::try_join_all(futures).await
        }
    }

    /// Turn a route's opcode and values into a concrete destination.
    async fn destination_for(
        &self,
        route: &Route,
        session: &SessionRef,
        bv: &BindVars,
    ) -> Result<ShardDestination> {
        match route.opcode {
            RouteOpcode::Unsharded | RouteOpcode::Scatter => Ok(ShardDestination::AllShards),
            RouteOpcode::AnyShard | RouteOpcode::Reference => Ok(ShardDestination::AnyShard),
            RouteOpcode::None => Ok(ShardDestination::None),
            RouteOpcode::ByDestination => route
                .destination
                .clone()
                .ok_or_else(|| Error::internal("ByDestination route carries no destination")),
            RouteOpcode::NextSequenceValue => Ok(ShardDestination::AnyShard),
            _ => {
                let cv = route.vindex.as_ref().ok_or_else(|| {
                    Error::internal(format!("{} route carries no vindex", route.opcode))
                })?;
                let rows = self.vindex_rows(&route.values, bv)?;
                let cursor = EngineCursor::new(self.clone(), session.clone());
                let destinations = cv.vindex.map(Some(&cursor), &rows).await?;
                Ok(merge_destinations(destinations))
            }
        }
    }

    /// Resolve the plan's route values into concrete rows of vindex inputs.
    fn vindex_rows(&self, values: &[RouteValue], bv: &BindVars) -> Result<Vec<Vec<Value>>> {
        values
            .iter()
            .map(|v| match v {
                RouteValue::Tuple(items) => items.iter().map(|i| self.value_of(i, bv)).collect(),
                other => Ok(vec![self.value_of(other, bv)?]),
            })
            .collect()
    }

    fn value_of(&self, v: &RouteValue, bv: &BindVars) -> Result<Value> {
        match v {
            RouteValue::Literal(x) => Ok(x.clone()),
            RouteValue::BindVar(name) | RouteValue::BindVarList(name) => bv
                .get(name)
                .cloned()
                .ok_or_else(|| Error::invalid(format!("missing bind variable {name:?}"))),
            RouteValue::Tuple(items) => {
                // A tuple where a single value was expected: take the first, which
                // is what a one-column vindex means by it.
                items
                    .first()
                    .ok_or_else(|| Error::internal("empty routing tuple"))
                    .and_then(|i| self.value_of(i, bv))
            }
        }
    }

    // -- INSERT --------------------------------------------------------------

    async fn run_insert(&self, plan: &InsertPlan, session: &SessionRef, bv: &BindVars) -> Result<QueryResult> {
        let mut rows: Vec<Vec<Value>> = plan
            .rows
            .iter()
            .map(|r| r.values.iter().map(|v| self.value_of(v, bv)).collect::<Result<Vec<_>>>())
            .collect::<Result<_>>()?;

        // Fill in generated keys before routing: the sharding column may be the
        // one the sequence supplies.
        let mut first_generated: Option<u64> = None;
        if let Some(seq) = &plan.sequence {
            let needed: Vec<usize> = rows
                .iter()
                .enumerate()
                .filter(|(_, r)| match seq.column_position {
                    Some(p) => r.get(p).is_none_or(Value::is_null),
                    None => true,
                })
                .map(|(i, _)| i)
                .collect();
            if !needed.is_empty() {
                let values = self
                    .gateway
                    .next_sequence_values(&seq.keyspace, &seq.table, needed.len() as u64)
                    .await?;
                first_generated = values.first().and_then(Value::as_uint);
                for (slot, idx) in needed.iter().enumerate() {
                    let value = values
                        .get(slot)
                        .cloned()
                        .ok_or_else(|| Error::internal("sequence returned too few values"))?;
                    match seq.column_position {
                        Some(p) => rows[*idx][p] = value,
                        None => {
                            // The column was not in the statement, so the plan's
                            // column list already includes it at the end.
                            rows[*idx].push(value);
                        }
                    }
                }
            }
        }

        let unsharded = !plan.table.sharded;
        let tablet_type = TabletType::Primary;

        // Work out where each row goes.
        let mut row_shards: Vec<String> = Vec::with_capacity(rows.len());
        let mut row_ksids: Vec<KeyspaceId> = Vec::with_capacity(rows.len());

        if unsharded {
            let shards = self
                .gateway
                .shards_for(&plan.keyspace, &ShardDestination::AllShards, tablet_type)
                .await?;
            let shard = shards
                .first()
                .cloned()
                .ok_or_else(|| Error::unavailable(format!("keyspace {} has no shard", plan.keyspace)))?;
            row_shards = vec![shard; rows.len()];
            row_ksids = vec![KeyspaceId::default(); rows.len()];
        } else if let Some(pinned) = &plan.pinned {
            // A reference table: one authoritative copy, at a fixed keyspace id.
            let dest = ShardDestination::KeyspaceId(pinned.clone());
            let shards = self.gateway.shards_for(&plan.keyspace, &dest, tablet_type).await?;
            let shard = shards.first().cloned().ok_or_else(|| {
                Error::unavailable(format!("no shard owns the pinned keyspace id for {}", plan.table.name))
            })?;
            row_shards = vec![shard; rows.len()];
            row_ksids = vec![pinned.clone(); rows.len()];
        } else {
            let primary = plan.table.primary_vindex.as_ref().ok_or_else(|| {
                Error::internal(format!("sharded table {} has no primary vindex", plan.table.name))
            })?;
            let vindex_rows: Vec<Vec<Value>> = rows
                .iter()
                .map(|r| plan.vindex_column_positions.iter().map(|p| r[*p].clone()).collect())
                .collect();
            let cursor = EngineCursor::new(self.clone(), session.clone());
            let destinations = primary.vindex.map(Some(&cursor), &vindex_rows).await?;

            for (i, dest) in destinations.into_iter().enumerate() {
                let ksid = match dest {
                    ShardDestination::KeyspaceId(k) => k,
                    other => {
                        return Err(Error::invalid(format!(
                            "row {} of the INSERT does not map to a single shard ({other}); \
                             the sharding column's value must be usable by the {} vindex",
                            i + 1,
                            primary.vindex.kind()
                        )))
                    }
                };
                let shards = self
                    .gateway
                    .shards_for(&plan.keyspace, &ShardDestination::KeyspaceId(ksid.clone()), tablet_type)
                    .await?;
                let shard = shards.first().cloned().ok_or_else(|| {
                    Error::unavailable(format!("no shard of {} owns keyspace id {ksid}", plan.keyspace))
                })?;
                row_shards.push(shard);
                row_ksids.push(ksid);
            }
        }

        // Maintain owned lookup vindexes before the rows themselves are written,
        // so a duplicate on the lookup is detected before anything is inserted.
        if !plan.owned_vindexes.is_empty() {
            let cursor = EngineCursor::new(self.clone(), session.clone());
            for cv in &plan.owned_vindexes {
                let positions: Vec<usize> = cv
                    .columns
                    .iter()
                    .filter_map(|c| plan.columns.iter().position(|pc| pc.eq_ignore_ascii_case(c)))
                    .collect();
                if positions.len() != cv.columns.len() {
                    // The statement does not set this vindex's columns, so there
                    // is nothing to record.
                    continue;
                }
                let vrows: Vec<Vec<Value>> = rows
                    .iter()
                    .map(|r| positions.iter().map(|p| r[*p].clone()).collect())
                    .collect();
                cv.vindex
                    .create(&cursor, &vrows, &row_ksids, plan.ignore_duplicates)
                    .await?;
            }
        }

        // Group by shard so each one gets a single multi-row INSERT.
        let mut by_shard: BTreeMap<String, Vec<usize>> = BTreeMap::new();
        for (i, shard) in row_shards.iter().enumerate() {
            by_shard.entry(shard.clone()).or_default().push(i);
        }

        let shard_count = by_shard.len();
        let mut total = QueryResult::default();
        for (shard, indexes) in by_shard {
            let target = Target::new(&plan.keyspace, &shard, tablet_type);
            let dialect = self.gateway.dialect_for(&plan.keyspace, &shard).await?;

            let (statement, bind_vars) =
                build_multi_row_insert(&plan.statement, plan.columns.len(), &indexes, &rows)?;
            let rendered = vituss_dialect::render::render_for(
                &statement,
                &bind_vars,
                self.client_dialect.as_ref(),
                dialect.as_ref(),
            )?;
            let result = self
                .gateway
                .execute(&target, &rendered.sql, &rendered.params, session)
                .await?;
            total.append(result);
        }

        // A generated key belongs to the client, and it came from the sequence,
        // not from whichever shard happened to answer last.
        match first_generated {
            Some(id) => {
                total.last_insert_id = Some(id);
                session.lock().await.last_insert_id = Some(id);
            }
            // Across several shards there is no single "the" id — reporting one
            // shard's would be arbitrary and misleading, so none is reported.
            None if shard_count > 1 => total.last_insert_id = None,
            None => {
                if let Some(id) = total.last_insert_id {
                    session.lock().await.last_insert_id = Some(id);
                }
            }
        }
        Ok(total)
    }

    // -- UPDATE / DELETE -----------------------------------------------------

    async fn run_dml(&self, plan: &DmlPlan, session: &SessionRef, bv: &BindVars) -> Result<QueryResult> {
        {
            let s = session.lock().await;
            if plan.route.opcode.is_multi_shard()
                && s.in_transaction
                && s.transaction_mode == vituss_core::TransactionMode::Single
            {
                return Err(Error::failed_precondition(format!(
                    "this {:?} would touch more than one shard, and the session's transaction mode \
                     is 'single'. Set vituss_transaction_mode='multi' to allow it, or add a \
                     predicate on the sharding column.",
                    plan.kind
                )));
            }
        }

        // Read the rows about to change, so their lookup entries can be removed
        // while they are still findable.
        let old_rows = match &plan.pre_query {
            None => None,
            Some(pre) => Some(self.run_route(pre, session, bv).await?),
        };

        if let (Some(old), false) = (&old_rows, plan.owned_vindexes.is_empty()) {
            let cursor = EngineCursor::new(self.clone(), session.clone());
            for cv in &plan.owned_vindexes {
                let positions: Vec<usize> = cv
                    .columns
                    .iter()
                    .filter_map(|c| old.column_index(c))
                    .collect();
                if positions.len() != cv.columns.len() {
                    continue;
                }
                let primary = plan.table.primary_vindex.as_ref();
                for row in &old.rows {
                    let vrow: Vec<Value> = positions.iter().map(|p| row[*p].clone()).collect();
                    // The keyspace id the lookup entry points at, recomputed from
                    // the row's own sharding column.
                    let ksid = match primary {
                        Some(p) => {
                            let cols: Vec<Value> = p
                                .columns
                                .iter()
                                .filter_map(|c| old.column_index(c))
                                .map(|i| row[i].clone())
                                .collect();
                            match p.vindex.map(Some(&cursor), &[cols]).await?.into_iter().next() {
                                Some(ShardDestination::KeyspaceId(k)) => k,
                                _ => KeyspaceId::default(),
                            }
                        }
                        None => KeyspaceId::default(),
                    };
                    cv.vindex.delete(&cursor, &[vrow], &ksid).await?;
                }
            }
        }

        let result = self.run_route(&plan.route, session, bv).await?;

        {
            let mut s = session.lock().await;
            s.rows_affected = result.rows_affected;
        }
        Ok(result)
    }

    // -- Join ----------------------------------------------------------------

    async fn run_join(&self, join: &Join, session: &SessionRef, bv: &BindVars) -> Result<QueryResult> {
        let left = self.run(&join.left, session, bv).await?;

        let mut out = QueryResult::default();
        for left_row in &left.rows {
            let mut inner_bv = bv.clone();
            for (name, column) in &join.vars {
                let value = left_row
                    .get(*column)
                    .cloned()
                    .ok_or_else(|| Error::internal(format!("join variable {name} has no source column")))?;
                inner_bv.insert(name.clone(), value);
            }
            let right = self.run(&join.right, session, &inner_bv).await?;

            if right.rows.is_empty() {
                if join.kind == JoinKind::Left {
                    out.rows.push(build_joined_row(left_row, &[], &join.column_map, true));
                }
                continue;
            }
            if out.fields.is_empty() {
                out.fields = joined_fields(&left, &right, &join.column_map);
            }
            for right_row in &right.rows {
                out.rows.push(build_joined_row(left_row, right_row, &join.column_map, false));
            }
        }
        if out.fields.is_empty() {
            out.fields = joined_fields(&left, &QueryResult::default(), &join.column_map);
        }
        Ok(out)
    }

    // -- DDL, session, show --------------------------------------------------

    async fn run_ddl(&self, ddl: &vituss_planner::Ddl, session: &SessionRef) -> Result<QueryResult> {
        let shards = self
            .gateway
            .shards_for(&ddl.keyspace, &ShardDestination::AllShards, TabletType::Primary)
            .await?;

        // Applied one shard at a time, and stopped at the first failure. A DDL
        // that half-succeeded is bad; one that half-succeeded and then kept going
        // is worse, because it hides which shard diverged.
        let mut out = QueryResult::default();
        for shard in &shards {
            let target = Target::new(&ddl.keyspace, shard, TabletType::Primary);
            let dialect = self.gateway.dialect_for(&ddl.keyspace, shard).await?;
            let rendered = vituss_dialect::render::render_for(
                &ddl.statement,
                &BindVars::new(),
                self.client_dialect.as_ref(),
                dialect.as_ref(),
            )?;
            let result = self
                .gateway
                .execute(&target, &rendered.sql, &rendered.params, session)
                .await
                .map_err(|e| {
                    Error::new(
                        e.code,
                        format!(
                            "DDL failed on shard {}/{}: {}. Shards applied before it have the change; \
                             the keyspace's schema is now inconsistent and needs repair.",
                            ddl.keyspace, shard, e.message
                        ),
                    )
                })?;
            out.rows_affected += result.rows_affected;
            // What the type translation could not carry across. Deduplicated
            // rather than repeated per shard: every shard on the same engine loses
            // the same thing. Each message names the engine it applies to, so a
            // keyspace mid-migration across two of them still reads correctly.
            for w in rendered.warnings {
                if !out.warnings.contains(&w) {
                    out.warnings.push(w);
                }
            }
        }
        Ok(out)
    }

    async fn run_session(&self, op: &SessionOp, session: &SessionRef) -> Result<QueryResult> {
        match op {
            SessionOp::Use { keyspace, shard, tablet_type } => {
                let mut s = session.lock().await;
                s.target_keyspace = Some(keyspace.clone());
                s.target_shard = shard.clone();
                if let Some(tt) = tablet_type {
                    s.tablet_type = tt.parse()?;
                }
            }
            SessionOp::Begin => {
                let mut s = session.lock().await;
                if s.in_transaction {
                    // An implicit commit here would silently discard the
                    // isolation the client thought it had.
                    return Err(Error::failed_precondition(
                        "a transaction is already open; COMMIT or ROLLBACK before starting another",
                    ));
                }
                s.in_transaction = true;
            }
            SessionOp::Commit => {
                self.gateway.commit(session).await?;
                session.lock().await.reset_transaction();
            }
            SessionOp::Rollback => {
                self.gateway.rollback(session).await?;
                session.lock().await.reset_transaction();
            }
            SessionOp::Savepoint(_) | SessionOp::RollbackTo(_) | SessionOp::Release(_) => {
                return Err(Error::unimplemented(
                    "savepoints inside a multi-shard transaction are not supported yet",
                ))
            }
            SessionOp::Set { name, value, scope } => {
                let mut s = session.lock().await;
                match scope {
                    SetScope::UserDefined => {
                        s.user_defined_variables.insert(name.clone(), value.clone());
                    }
                    SetScope::Vituss => match name.trim_start_matches('@').to_lowercase().as_str() {
                        "vituss_transaction_mode" => {
                            s.transaction_mode = match value.to_string().to_lowercase().as_str() {
                                "single" => vituss_core::TransactionMode::Single,
                                "multi" => vituss_core::TransactionMode::Multi,
                                "two_pc" | "twopc" => vituss_core::TransactionMode::TwoPc,
                                other => {
                                    return Err(Error::invalid(format!(
                                        "unknown transaction mode {other:?}; expected single, multi or two_pc"
                                    )))
                                }
                            };
                        }
                        other => {
                            return Err(Error::invalid(format!("unknown Vituss setting {other:?}")));
                        }
                    },
                    _ => {
                        // Session variables are remembered and replayed onto each
                        // shard connection the session reserves, so they mean the
                        // same thing wherever a later statement lands.
                        s.system_variables.insert(name.clone(), value.to_string());
                    }
                }
            }
        }
        Ok(QueryResult::default())
    }

    async fn run_show(&self, show: &ShowPlan, session: &SessionRef) -> Result<QueryResult> {
        use vituss_core::{Field, SqlType};
        let single = |name: &str, values: Vec<String>| QueryResult::from_rows(
            vec![Field::new(name, SqlType::VarChar)],
            values.into_iter().map(|v| vec![Value::Text(v)]).collect(),
        );

        match show {
            ShowPlan::Keyspaces | ShowPlan::Databases => {
                Ok(single("Database", self.vschema.keyspace_names()))
            }
            ShowPlan::Dialects => Ok(single("Dialect", vituss_dialect::registered())),
            ShowPlan::VSchemaTables { keyspace } => {
                let ks = match keyspace {
                    Some(k) => k.clone(),
                    None => session
                        .lock()
                        .await
                        .target_keyspace
                        .clone()
                        .ok_or_else(|| Error::failed_precondition("no keyspace selected"))?,
                };
                let mut names: Vec<String> =
                    self.vschema.keyspace(&ks)?.tables.keys().cloned().collect();
                names.sort();
                Ok(single("Table", names))
            }
            ShowPlan::VSchemaVindexes { keyspace } => {
                let ks = match keyspace {
                    Some(k) => k.clone(),
                    None => session
                        .lock()
                        .await
                        .target_keyspace
                        .clone()
                        .ok_or_else(|| Error::failed_precondition("no keyspace selected"))?,
                };
                let keyspace = self.vschema.keyspace(&ks)?;
                let mut rows: Vec<Vec<Value>> = keyspace
                    .vindexes
                    .iter()
                    .map(|(name, v)| {
                        vec![
                            Value::Text(name.clone()),
                            Value::Text(v.kind().to_string()),
                            Value::Bool(v.is_unique()),
                            Value::Int(v.cost() as i64),
                        ]
                    })
                    .collect();
                rows.sort_by(|a, b| a[0].compare(&b[0]));
                Ok(QueryResult::from_rows(
                    vec![
                        Field::new("Name", SqlType::VarChar),
                        Field::new("Type", SqlType::VarChar),
                        Field::new("Unique", SqlType::Bool),
                        Field::new("Cost", SqlType::Int64),
                    ],
                    rows,
                ))
            }
            ShowPlan::Tables { keyspace } => {
                let ks = match keyspace {
                    Some(k) => k.clone(),
                    None => session
                        .lock()
                        .await
                        .target_keyspace
                        .clone()
                        .ok_or_else(|| Error::failed_precondition("no keyspace selected"))?,
                };
                let mut names: Vec<String> =
                    self.vschema.keyspace(&ks)?.tables.keys().cloned().collect();
                names.sort();
                Ok(single(&format!("Tables_in_{ks}"), names))
            }
            ShowPlan::Shards { .. } | ShowPlan::Tablets => Err(Error::unimplemented(
                "SHOW SHARDS / SHOW TABLETS are served by the control plane, not the query path",
            )),
            ShowPlan::Passthrough => Err(Error::unimplemented(
                "this SHOW statement is not answered by Vituss; run it against a shard directly",
            )),
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Combine per-row destinations into one destination for the whole statement.
fn merge_destinations(destinations: Vec<ShardDestination>) -> ShardDestination {
    let mut ids: Vec<KeyspaceId> = Vec::new();
    let mut ranges = Vec::new();
    let mut any_scatter = false;

    for d in destinations {
        match d {
            ShardDestination::KeyspaceId(k) => ids.push(k),
            ShardDestination::KeyspaceIds(ks) => ids.extend(ks),
            ShardDestination::KeyRange(kr) | ShardDestination::ExactKeyRange(kr) => ranges.push(kr),
            ShardDestination::AllShards => any_scatter = true,
            // A row that maps nowhere simply contributes no shards.
            ShardDestination::None => {}
            other => return other,
        }
    }

    if any_scatter {
        return ShardDestination::AllShards;
    }
    match (ids.len(), ranges.len()) {
        (0, 0) => ShardDestination::None,
        (_, 0) => {
            ids.sort();
            ids.dedup();
            if ids.len() == 1 {
                ShardDestination::KeyspaceId(ids.pop().expect("length checked"))
            } else {
                ShardDestination::KeyspaceIds(ids)
            }
        }
        (0, 1) => ShardDestination::KeyRange(ranges.pop().expect("length checked")),
        // Several ranges, or a mix: the union is not expressible as one range, so
        // fall back to asking every shard rather than silently dropping any.
        _ => ShardDestination::AllShards,
    }
}

/// Expand the single-row INSERT template into one statement covering every row
/// bound for this shard.
fn build_multi_row_insert(
    template: &Statement,
    column_count: usize,
    indexes: &[usize],
    rows: &[Vec<Value>],
) -> Result<(Statement, BindVars)> {
    let mut stmt = template.clone();
    let Statement::Insert(insert) = &mut stmt else {
        return Err(Error::internal("insert plan template is not an INSERT"));
    };
    let source = insert
        .source
        .as_mut()
        .ok_or_else(|| Error::internal("insert plan template has no VALUES"))?;
    let sqlparser::ast::SetExpr::Values(Values { rows: value_rows, .. }) = &mut *source.body else {
        return Err(Error::internal("insert plan template is not a VALUES insert"));
    };

    let mut bind_vars = BindVars::new();
    let mut new_rows = Vec::with_capacity(indexes.len());
    for (n, idx) in indexes.iter().enumerate() {
        let mut tuple = Vec::with_capacity(column_count);
        for c in 0..column_count {
            let name = format!("r{n}c{c}");
            bind_vars.insert(
                name.clone(),
                rows[*idx].get(c).cloned().unwrap_or(Value::Null),
            );
            tuple.push(Expr::Value(sqlparser::ast::ValueWithSpan {
                value: sqlparser::ast::Value::Placeholder(format!(":{name}")),
                span: sqlparser::tokenizer::Span::empty(),
            }));
        }
        new_rows.push(sqlparser::ast::Parens::with_empty_span(tuple));
    }
    *value_rows = new_rows;
    Ok((stmt, bind_vars))
}

fn build_joined_row(left: &[Value], right: &[Value], map: &[i32], right_is_null: bool) -> Vec<Value> {
    map.iter()
        .map(|&m| {
            if m >= 0 {
                left.get(m as usize).cloned().unwrap_or(Value::Null)
            } else if right_is_null {
                Value::Null
            } else {
                right.get((-m - 1) as usize).cloned().unwrap_or(Value::Null)
            }
        })
        .collect()
}

fn joined_fields(left: &QueryResult, right: &QueryResult, map: &[i32]) -> Vec<vituss_core::Field> {
    map.iter()
        .map(|&m| {
            let f = if m >= 0 {
                left.fields.get(m as usize)
            } else {
                right.fields.get((-m - 1) as usize)
            };
            f.cloned()
                .unwrap_or_else(|| vituss_core::Field::new("?", vituss_core::SqlType::Unknown))
        })
        .collect()
}

/// The session as the executor hands it out at the start of a request.
pub fn new_session(session: Session) -> SessionRef {
    Arc::new(tokio::sync::Mutex::new(session))
}
