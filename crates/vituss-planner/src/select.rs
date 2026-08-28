//! SELECT planning.
//!
//! The governing rule: push work into the shards' own engines, and do as little
//! as possible in Vituss. A query that reaches one shard is sent verbatim — the
//! shard's optimiser handles the joins, the sort, the aggregate, the limit, all
//! of which it does better than a router could. Only when a query genuinely
//! spans shards does Vituss add primitives above the route, and then only the
//! ones the shards cannot finish themselves.

use std::sync::Arc;

use sqlparser::ast::{
    Expr, GroupByExpr, JoinConstraint, JoinOperator, ObjectName, OrderByKind, Query, Select,
    SelectItem, SetExpr, SetOperator, Statement, TableFactor,
};

use vituss_core::{Error, Result, Value};
use vituss_vschema::Table;

use crate::analyze::{constraints_for, route_table, Constraints, Routing};
use crate::normalize::Normalizer;
use crate::plan::{
    Aggregate, AggregateExpr, AggregateFunc, Distinct, Join, JoinKind, Limit, OrderBy, Primitive, Route,
    RouteOpcode, RouteValue,
};
use crate::util::{and_terms, as_column_ref};
use crate::Planner;

/// A table in the FROM clause, resolved against the VSchema.
#[derive(Clone)]
pub struct TableRef {
    /// The name the query refers to it by: the alias if there is one, else the
    /// table name.
    pub alias: String,
    pub table: Arc<Table>,
    /// The name as written, so the pushed-down statement can keep it.
    pub name: ObjectName,
}

impl Planner<'_> {
    /// Plan a `SELECT`, `VALUES` or `UNION`.
    pub fn plan_query(&self, query: &Query) -> Result<Primitive> {
        match &*query.body {
            SetExpr::Select(select) => self.plan_select(select, query),
            SetExpr::Query(inner) => self.plan_query(inner),
            SetExpr::SetOperation { op, set_quantifier, left, right } => {
                self.plan_set_operation(*op, set_quantifier, left, right, query)
            }
            SetExpr::Values(_) => {
                // `VALUES (1),(2)` needs no data; any engine can evaluate it.
                let mut stmt = Statement::Query(Box::new(query.clone()));
                let keyspace = self.any_keyspace()?;
                strip_qualifiers(&mut stmt);
                Ok(Primitive::Route(Route::new(RouteOpcode::AnyShard, keyspace, stmt)))
            }
            other => Err(Error::unsupported(format!(
                "this query form cannot be routed yet: {other}"
            ))),
        }
    }

    fn plan_set_operation(
        &self,
        op: SetOperator,
        quantifier: &sqlparser::ast::SetQuantifier,
        left: &SetExpr,
        right: &SetExpr,
        _outer: &Query,
    ) -> Result<Primitive> {
        if op != SetOperator::Union {
            return Err(Error::unsupported(format!(
                "{op} across shards is not supported; only UNION is"
            )));
        }
        let distinct = !matches!(quantifier, sqlparser::ast::SetQuantifier::All);
        let mut inputs = Vec::new();
        for side in [left, right] {
            let q = Query {
                with: None,
                body: Box::new(side.clone()),
                order_by: None,
                limit_clause: None,
                fetch: None,
                locks: Vec::new(),
                for_clause: None,
                settings: None,
                format_clause: None,
                pipe_operators: Vec::new(),
            };
            inputs.push(self.plan_query(&q)?);
        }
        Ok(Primitive::Concatenate { inputs, distinct })
    }

    fn plan_select(&self, select: &Select, query: &Query) -> Result<Primitive> {
        let tables = self.resolve_from(&select.from)?;

        if tables.is_empty() {
            // `SELECT 1`, `SELECT NOW()` — no data involved, so any shard will do.
            let keyspace = self.any_keyspace()?;
            let mut stmt = Statement::Query(Box::new(query.clone()));
            strip_qualifiers(&mut stmt);
            return Ok(Primitive::Route(Route::new(RouteOpcode::AnyShard, keyspace, stmt)));
        }

        let keyspaces: Vec<&str> = {
            let mut ks: Vec<&str> = tables.iter().map(|t| t.table.keyspace.as_str()).collect();
            ks.sort_unstable();
            ks.dedup();
            ks
        };

        // Everything in one keyspace, and either unsharded or provably collocated:
        // one route, and the shard's engine does the join.
        if keyspaces.len() == 1 {
            if let Some(route) = self.try_single_route(&tables, select, query)? {
                return Ok(route);
            }
        }

        // Otherwise the join has to happen here.
        self.plan_cross_shard_join(&tables, select, query)
    }

    /// Try to answer the whole query with a single route.
    ///
    /// Returns `None` when the tables are not collocated, which is the signal to
    /// fall back to a join executed by the gate.
    fn try_single_route(
        &self,
        tables: &[TableRef],
        select: &Select,
        query: &Query,
    ) -> Result<Option<Primitive>> {
        let keyspace = tables[0].table.keyspace.clone();

        // Every table's routing, computed independently.
        let routings: Vec<(usize, Routing, Constraints)> = tables
            .iter()
            .enumerate()
            .map(|(i, t)| {
                let quals = vec![t.alias.clone(), t.table.name.clone()];
                let c = constraints_for(select.selection.as_ref(), &quals);
                (i, route_table(&t.table, &c), c)
            })
            .collect();

        // Unsharded keyspace: nothing to collocate, push it all down.
        if tables.iter().all(|t| !t.table.sharded) {
            return Ok(Some(self.build_route(
                Routing { opcode: RouteOpcode::Unsharded, vindex: None, values: Vec::new() },
                &keyspace,
                tables,
                select,
                query,
            )?));
        }

        // A single table is always "collocated" with itself.
        if tables.len() == 1 {
            let routing = routings[0].1.clone();
            return Ok(Some(self.build_route(routing, &keyspace, tables, select, query)?));
        }

        // Several sharded tables: they can share a route only if every pair is
        // joined on the columns of the *same* vindex, so matching rows are
        // guaranteed to be on the same shard.
        if !self.tables_are_collocated(tables, select) {
            return Ok(None);
        }

        // Take the most selective routing any of them offers: a predicate on one
        // table narrows the whole join, since the others follow it.
        let best = routings
            .into_iter()
            .map(|(_, r, _)| r)
            .min_by_key(|r| match r.opcode {
                RouteOpcode::EqualUnique => 0,
                RouteOpcode::In | RouteOpcode::MultiEqual => 1,
                RouteOpcode::Equal => 2,
                RouteOpcode::Range => 3,
                _ => 9,
            })
            .unwrap_or_else(Routing::scatter);

        Ok(Some(self.build_route(best, &keyspace, tables, select, query)?))
    }

    /// True when every join in the query equates the primary-vindex columns of
    /// the tables it joins, using the same vindex.
    ///
    /// This is the condition that makes a sharded join free: rows that could ever
    /// match are, by construction, on the same shard, so each shard can compute
    /// its slice of the join locally and the gate only concatenates.
    fn tables_are_collocated(&self, tables: &[TableRef], select: &Select) -> bool {
        let vindex_of = |alias: &str| -> Option<(String, Vec<String>)> {
            let t = tables.iter().find(|t| t.alias.eq_ignore_ascii_case(alias))?;
            let pv = t.table.primary_vindex.as_ref()?;
            Some((pv.vindex.name().to_string(), pv.columns.clone()))
        };

        // Collect every equality between two qualified columns, from both the ON
        // clauses and the WHERE clause.
        let mut equalities: Vec<((String, String), (String, String))> = Vec::new();
        let mut collect = |e: &Expr| {
            for term in and_terms(e) {
                if let Expr::BinaryOp { left, op: sqlparser::ast::BinaryOperator::Eq, right } = term {
                    if let (Some((Some(lq), lc)), Some((Some(rq), rc))) =
                        (as_column_ref(left), as_column_ref(right))
                    {
                        equalities.push(((lq, lc), (rq, rc)));
                    }
                }
            }
        };
        if let Some(w) = &select.selection {
            collect(w);
        }
        for twj in &select.from {
            for join in &twj.joins {
                if let Some(JoinConstraint::On(e)) = constraint_of(&join.join_operator) {
                    collect(e);
                }
            }
        }

        // Every table beyond the first must be tied to one already covered.
        let mut connected: Vec<String> = vec![tables[0].alias.clone()];
        let mut progress = true;
        while progress && connected.len() < tables.len() {
            progress = false;
            for ((lq, lc), (rq, rc)) in &equalities {
                let (l_in, r_in) = (
                    connected.iter().any(|a| a.eq_ignore_ascii_case(lq)),
                    connected.iter().any(|a| a.eq_ignore_ascii_case(rq)),
                );
                if l_in == r_in {
                    continue;
                }
                let (Some((lv, lcols)), Some((rv, rcols))) = (vindex_of(lq), vindex_of(rq)) else {
                    continue;
                };
                // Same vindex, and both sides are that vindex's (single) column.
                let same_vindex = lv == rv;
                let on_vindex_columns = lcols.len() == 1
                    && rcols.len() == 1
                    && lcols[0].eq_ignore_ascii_case(lc)
                    && rcols[0].eq_ignore_ascii_case(rc);
                if same_vindex && on_vindex_columns {
                    connected.push(if l_in { rq.clone() } else { lq.clone() });
                    progress = true;
                }
            }
        }

        connected.len() == tables.len()
    }

    /// Build the route, adding whatever the shards cannot finish themselves.
    fn build_route(
        &self,
        routing: Routing,
        keyspace: &str,
        tables: &[TableRef],
        select: &Select,
        query: &Query,
    ) -> Result<Primitive> {
        let mut pushed = query.clone();
        let mut pushed_select = select.clone();

        let mut norm = Normalizer::new("v");
        norm.opt_expr(pushed_select.selection.as_mut());
        norm.opt_expr(pushed_select.having.as_mut());
        for twj in &mut pushed_select.from {
            for join in &mut twj.joins {
                if let Some(e) = constraint_of_mut(&mut join.join_operator) {
                    norm.expr(e);
                }
            }
        }

        let single_shard = routing.is_single_shard() || routing.opcode == RouteOpcode::Reference;
        let original_columns = select.projection.len();

        let mut route = Route::new(routing.opcode, keyspace, Statement::Query(Box::new(pushed.clone())));
        route.vindex = routing.vindex.clone();
        route.values = routing.values.clone();
        route.table = Some(tables[0].table.name.clone());

        if single_shard {
            // One shard: hand it the query as written and let its own planner do
            // the rest. Nothing is added above the route.
            *pushed.body = SetExpr::Select(Box::new(pushed_select));
            strip_keyspace_qualifiers(&mut pushed, tables);
            route.statement = Statement::Query(Box::new(pushed));
            route.bind_vars = norm.finish();
            return Ok(Primitive::Route(route));
        }

        // Multi-shard. Work out what must be recombined here.
        let has_aggregates = select.projection.iter().any(|i| item_aggregate(i).is_some());
        let group_by_exprs = group_by_expressions(select);
        let order_by_exprs = order_by_expressions(query);
        let is_distinct = select.distinct.is_some();
        let (limit, offset) = limit_and_offset(query);

        let mut extra_columns: Vec<Expr> = Vec::new();
        let mut aggregates: Vec<AggregateExpr> = Vec::new();

        // Rewrite the projection so each shard returns a *partial* aggregate that
        // can be combined. Notably AVG becomes SUM plus COUNT, because the mean of
        // per-shard means is not the mean.
        if has_aggregates {
            for (pos, item) in select.projection.iter().enumerate() {
                let Some((func, inner, distinct)) = item_aggregate(item) else { continue };
                let alias = item_alias(item).unwrap_or_else(|| format!("agg{pos}"));
                match func {
                    AggregateFunc::Avg => {
                        let inner = inner.clone().ok_or_else(|| {
                            Error::unsupported("AVG() across shards needs an argument")
                        })?;
                        pushed_select.projection[pos] =
                            SelectItem::UnnamedExpr(self.build_expr(&format!("SUM({inner})"))?);
                        let count_pos = original_columns + extra_columns.len();
                        extra_columns.push(self.build_expr(&format!("COUNT({inner})"))?);
                        aggregates.push(AggregateExpr {
                            func: AggregateFunc::Avg,
                            column: pos,
                            count_column: Some(count_pos),
                            distinct,
                            alias,
                        });
                    }
                    AggregateFunc::Count | AggregateFunc::CountStar => {
                        if distinct {
                            return Err(Error::unsupported(
                                "COUNT(DISTINCT ...) across shards is not supported: the shards \
                                 would each count their own distinct values and the totals cannot \
                                 be added. Route the query to one shard, or use an approximate count.",
                            ));
                        }
                        aggregates.push(AggregateExpr {
                            func,
                            column: pos,
                            count_column: None,
                            distinct,
                            alias,
                        });
                    }
                    other => aggregates.push(AggregateExpr {
                        func: other,
                        column: pos,
                        count_column: None,
                        distinct,
                        alias,
                    }),
                }
            }
        }

        // Group-by keys must come back from the shards, so add any that the
        // projection does not already contain.
        let mut group_by_positions = Vec::new();
        for e in &group_by_exprs {
            match find_in_projection(&select.projection, e) {
                Some(p) => group_by_positions.push(p),
                None => {
                    group_by_positions.push(original_columns + extra_columns.len());
                    extra_columns.push(e.clone());
                }
            }
        }

        // Same for sort keys.
        let mut order_by = Vec::new();
        for (e, desc, nulls_first) in &order_by_exprs {
            let column = match find_in_projection(&select.projection, e) {
                Some(p) => p,
                None => {
                    // A positional `ORDER BY 2` already refers to an output column.
                    if let Some(p) = positional(e) {
                        p.saturating_sub(1)
                    } else {
                        let p = original_columns + extra_columns.len();
                        extra_columns.push(e.clone());
                        p
                    }
                }
            };
            order_by.push(OrderBy { column, descending: *desc, nulls_first: *nulls_first });
        }

        for e in &extra_columns {
            pushed_select.projection.push(SelectItem::UnnamedExpr(e.clone()));
        }

        // A grouped aggregate can stream instead of buffering if the shards hand
        // back rows already ordered by the group key.
        let group_sorted = !group_by_positions.is_empty() && order_by.is_empty();
        if group_sorted {
            for p in &group_by_positions {
                order_by.push(OrderBy { column: *p, descending: false, nulls_first: true });
            }
        }

        *pushed.body = SetExpr::Select(Box::new(pushed_select));

        // Push a wider LIMIT down: any shard could hold all of the top N.
        if let (Some(l), off) = (limit, offset) {
            set_limit(&mut pushed, l + off.unwrap_or(0), None);
        }
        if group_sorted {
            // The shard must sort by the group key for the streaming aggregate
            // above to be correct.
            set_order_by(&mut pushed, &group_by_exprs);
        }

        strip_keyspace_qualifiers(&mut pushed, tables);
        route.statement = Statement::Query(Box::new(pushed));
        route.bind_vars = norm.finish();
        route.order_by = order_by.clone();

        let mut node = Primitive::Route(route);

        if has_aggregates || !group_by_positions.is_empty() {
            node = Primitive::Aggregate(Aggregate {
                input: Box::new(node),
                aggregates,
                group_by: group_by_positions,
                ordered: group_sorted,
                truncate_columns: 0,
            });
        } else if !order_by.is_empty() {
            // Without an aggregate the merge already happens inside the route, so
            // no separate sort is needed — but a distinct or a limit above it
            // still has to preserve that order.
        }

        if is_distinct {
            node = Primitive::Distinct(Distinct { input: Box::new(node), columns: (0..original_columns).collect() });
        }

        if limit.is_some() || offset.is_some() {
            node = Primitive::Limit(Limit {
                input: Box::new(node),
                count: limit.map(|l| RouteValue::Literal(Value::Uint(l))),
                offset: offset.map(|o| RouteValue::Literal(Value::Uint(o))),
            });
        }

        if !extra_columns.is_empty() {
            node = Primitive::Truncate { input: Box::new(node), columns: original_columns };
        }

        Ok(node)
    }

    /// Plan a join the shards cannot do themselves, as a nested loop.
    ///
    /// The left side runs first; each of its rows supplies bind variables to a
    /// fresh execution of the right side. That is O(left) round trips, which is
    /// why the planner works so hard to avoid getting here.
    fn plan_cross_shard_join(
        &self,
        tables: &[TableRef],
        select: &Select,
        query: &Query,
    ) -> Result<Primitive> {
        if tables.len() != 2 {
            return Err(Error::unsupported(format!(
                "cannot route a {}-table join whose tables are not collocated. \
                 Tables joined across shards must be joined on the columns of the same vindex; \
                 {} are not.",
                tables.len(),
                tables.iter().map(|t| t.alias.as_str()).collect::<Vec<_>>().join(" and ")
            )));
        }
        if query.limit_clause.is_some() || query.order_by.is_some() {
            return Err(Error::unsupported(
                "ORDER BY / LIMIT over a cross-shard join is not supported yet; \
                 the join must be collocated for those to be pushed down",
            ));
        }
        if select.distinct.is_some() || select.projection.iter().any(|i| item_aggregate(i).is_some()) {
            return Err(Error::unsupported(
                "DISTINCT / aggregates over a cross-shard join are not supported yet",
            ));
        }

        let (left_ref, right_ref) = (&tables[0], &tables[1]);
        let kind = join_kind(select);

        // The equality that ties the two sides together.
        let mut join_predicates = Vec::new();
        if let Some(w) = &select.selection {
            join_predicates.extend(and_terms(w).into_iter().cloned());
        }
        for twj in &select.from {
            for join in &twj.joins {
                if let Some(JoinConstraint::On(e)) = constraint_of(&join.join_operator) {
                    join_predicates.extend(and_terms(e).into_iter().cloned());
                }
            }
        }

        let mut vars: Vec<(String, usize)> = Vec::new();
        let mut left_extra: Vec<Expr> = Vec::new();
        let mut right_filters: Vec<String> = Vec::new();

        for p in &join_predicates {
            let Expr::BinaryOp { left, op: sqlparser::ast::BinaryOperator::Eq, right } = p else { continue };
            let (Some((Some(lq), lc)), Some((Some(rq), rc))) = (as_column_ref(left), as_column_ref(right))
            else {
                continue;
            };
            let (left_col, right_col) = if lq.eq_ignore_ascii_case(&left_ref.alias) && rq.eq_ignore_ascii_case(&right_ref.alias)
            {
                (lc, rc)
            } else if rq.eq_ignore_ascii_case(&left_ref.alias) && lq.eq_ignore_ascii_case(&right_ref.alias) {
                (rc, lc)
            } else {
                continue;
            };
            let var = format!("j_{}_{}", left_ref.alias, left_col).to_lowercase();
            let pos = select.projection.len() + left_extra.len();
            left_extra.push(self.build_expr(&format!("{}.{}", left_ref.alias, left_col))?);
            vars.push((var.clone(), pos));
            right_filters.push(format!("{right_col} = :{var}"));
        }

        if vars.is_empty() {
            return Err(Error::unsupported(format!(
                "cannot join {} and {} across shards: no equality between their columns to join on. \
                 A cross join over sharded tables would be the full cartesian product.",
                left_ref.alias, right_ref.alias
            )));
        }

        // Split the projection: items qualified with the right table's alias come
        // from the right side, everything else from the left.
        let mut column_map = Vec::new();
        let mut left_projection = Vec::new();
        let mut right_projection = Vec::new();
        for item in &select.projection {
            let from_right = item_qualifier(item)
                .is_some_and(|q| q.eq_ignore_ascii_case(&right_ref.alias));
            if from_right {
                column_map.push(-(right_projection.len() as i32 + 1));
                right_projection.push(item.clone());
            } else {
                column_map.push(left_projection.len() as i32);
                left_projection.push(item.clone());
            }
        }
        for e in &left_extra {
            left_projection.push(SelectItem::UnnamedExpr(e.clone()));
        }
        if right_projection.is_empty() {
            // The right side still has to run — it decides which left rows match —
            // so give it something cheap to return.
            right_projection.push(SelectItem::UnnamedExpr(self.build_expr("1")?));
        }

        let left_plan = self.plan_side(left_ref, left_projection, &join_predicates, select)?;
        let right_plan = self.plan_side_with_filters(right_ref, right_projection, &right_filters, select)?;

        Ok(Primitive::Join(Join {
            left: Box::new(left_plan),
            right: Box::new(right_plan),
            kind,
            column_map,
            vars,
        }))
    }

    fn plan_side(
        &self,
        table: &TableRef,
        projection: Vec<SelectItem>,
        _predicates: &[Expr],
        select: &Select,
    ) -> Result<Primitive> {
        self.plan_side_with_filters(table, projection, &[], select)
    }

    /// Build a single-table sub-plan for one side of a join.
    fn plan_side_with_filters(
        &self,
        table: &TableRef,
        projection: Vec<SelectItem>,
        extra_filters: &[String],
        select: &Select,
    ) -> Result<Primitive> {
        // Keep only the WHERE terms that mention this table alone.
        let mut terms: Vec<String> = Vec::new();
        if let Some(w) = &select.selection {
            for t in and_terms(w) {
                if mentions_only(t, &table.alias) {
                    terms.push(t.to_string());
                }
            }
        }
        terms.extend(extra_filters.iter().cloned());

        let cols = projection
            .iter()
            .map(|i| i.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let where_clause = if terms.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", terms.join(" AND "))
        };
        let sql = format!(
            "SELECT {cols} FROM {} AS {}{where_clause}",
            table.name, table.alias
        );
        let stmt = self.dialect.parse_one(&sql)?;
        let Statement::Query(q) = &stmt else {
            return Err(Error::internal("sub-plan did not parse back as a query"));
        };
        let SetExpr::Select(sub) = &*q.body else {
            return Err(Error::internal("sub-plan did not parse back as a select"));
        };
        let refs = vec![table.clone()];
        self.build_route(
            {
                let quals = vec![table.alias.clone(), table.table.name.clone()];
                let c = constraints_for(sub.selection.as_ref(), &quals);
                route_table(&table.table, &c)
            },
            &table.table.keyspace,
            &refs,
            sub,
            q,
        )
    }

    /// Resolve the FROM clause against the VSchema.
    pub(crate) fn resolve_from(&self, from: &[sqlparser::ast::TableWithJoins]) -> Result<Vec<TableRef>> {
        let mut out = Vec::new();
        for twj in from {
            self.resolve_factor(&twj.relation, &mut out)?;
            for join in &twj.joins {
                self.resolve_factor(&join.relation, &mut out)?;
            }
        }
        Ok(out)
    }

    fn resolve_factor(&self, factor: &TableFactor, out: &mut Vec<TableRef>) -> Result<()> {
        match factor {
            TableFactor::Table { name, alias, .. } => {
                let parts: Vec<String> = name.0.iter().map(|p| p.to_string().trim_matches(|c| c == '`' || c == '"' || c == '[' || c == ']').to_string()).collect();
                let (keyspace, table) = match parts.len() {
                    1 => (self.default_keyspace.map(str::to_string), parts[0].clone()),
                    2 => (Some(parts[0].clone()), parts[1].clone()),
                    _ => {
                        return Err(Error::invalid(format!(
                            "table name {name} has too many parts; expected [keyspace.]table"
                        )))
                    }
                };
                let resolved = self.vschema.find_table(keyspace.as_deref(), &table)?;
                let alias = alias
                    .as_ref()
                    .map(|a| a.name.value.clone())
                    .unwrap_or_else(|| table.clone());
                out.push(TableRef { alias, table: resolved, name: name.clone() });
                Ok(())
            }
            TableFactor::Derived { .. } => Err(Error::unsupported(
                "a subquery in FROM cannot be routed yet; give the subquery a keyspace-local form \
                 or query the tables directly",
            )),
            other => Err(Error::unsupported(format!("FROM clause form {other} cannot be routed yet"))),
        }
    }

    /// Parse a fragment as an expression, by round-tripping it through the
    /// dialect's own parser rather than hand-building a dozen AST fields.
    pub(crate) fn build_expr(&self, fragment: &str) -> Result<Expr> {
        let stmt = self.dialect.parse_one(&format!("SELECT {fragment}"))?;
        let Statement::Query(q) = stmt else {
            return Err(Error::internal("expression fragment did not parse as a query"));
        };
        let SetExpr::Select(s) = *q.body else {
            return Err(Error::internal("expression fragment did not parse as a select"));
        };
        match s.projection.into_iter().next() {
            Some(SelectItem::UnnamedExpr(e)) | Some(SelectItem::ExprWithAlias { expr: e, .. }) => Ok(e),
            _ => Err(Error::internal(format!("cannot build an expression from {fragment:?}"))),
        }
    }

    fn any_keyspace(&self) -> Result<String> {
        if let Some(ks) = self.default_keyspace {
            return Ok(ks.to_string());
        }
        self.vschema
            .keyspace_names()
            .into_iter()
            .next()
            .ok_or_else(|| Error::failed_precondition("no keyspace is selected and the VSchema is empty"))
    }
}

// ---------------------------------------------------------------------------
// AST helpers
// ---------------------------------------------------------------------------

fn constraint_of(op: &JoinOperator) -> Option<&JoinConstraint> {
    match op {
        JoinOperator::Join(c)
        | JoinOperator::Inner(c)
        | JoinOperator::Left(c)
        | JoinOperator::LeftOuter(c)
        | JoinOperator::Right(c)
        | JoinOperator::RightOuter(c)
        | JoinOperator::FullOuter(c)
        | JoinOperator::CrossJoin(c) => Some(c),
        _ => None,
    }
}

fn constraint_of_mut(op: &mut JoinOperator) -> Option<&mut Expr> {
    let c = match op {
        JoinOperator::Join(c)
        | JoinOperator::Inner(c)
        | JoinOperator::Left(c)
        | JoinOperator::LeftOuter(c)
        | JoinOperator::Right(c)
        | JoinOperator::RightOuter(c)
        | JoinOperator::FullOuter(c)
        | JoinOperator::CrossJoin(c) => c,
        _ => return None,
    };
    match c {
        JoinConstraint::On(e) => Some(e),
        _ => None,
    }
}

fn join_kind(select: &Select) -> JoinKind {
    for twj in &select.from {
        for join in &twj.joins {
            if matches!(join.join_operator, JoinOperator::Left(_) | JoinOperator::LeftOuter(_)) {
                return JoinKind::Left;
            }
        }
    }
    JoinKind::Inner
}

/// The aggregate an item computes, if any: `(function, argument, distinct)`.
fn item_aggregate(item: &SelectItem) -> Option<(AggregateFunc, Option<String>, bool)> {
    let expr = match item {
        SelectItem::UnnamedExpr(e) => e,
        SelectItem::ExprWithAlias { expr, .. } => expr,
        _ => return None,
    };
    let Expr::Function(f) = expr else { return None };
    let name = f.name.to_string().to_ascii_uppercase();
    let func = match name.as_str() {
        "COUNT" => AggregateFunc::Count,
        "SUM" => AggregateFunc::Sum,
        "MIN" => AggregateFunc::Min,
        "MAX" => AggregateFunc::Max,
        "AVG" => AggregateFunc::Avg,
        _ => return None,
    };

    let sqlparser::ast::FunctionArguments::List(list) = &f.args else {
        return Some((func, None, false));
    };
    let distinct = matches!(list.duplicate_treatment, Some(sqlparser::ast::DuplicateTreatment::Distinct));
    let arg = list.args.first().map(|a| a.to_string());
    let func = match (func, arg.as_deref()) {
        (AggregateFunc::Count, Some("*")) | (AggregateFunc::Count, None) => AggregateFunc::CountStar,
        (f, _) => f,
    };
    Some((func, arg.filter(|a| a != "*"), distinct))
}

fn item_alias(item: &SelectItem) -> Option<String> {
    match item {
        SelectItem::ExprWithAlias { alias, .. } => Some(alias.value.clone()),
        SelectItem::UnnamedExpr(e) => Some(e.to_string()),
        _ => None,
    }
}

fn item_qualifier(item: &SelectItem) -> Option<String> {
    let expr = match item {
        SelectItem::UnnamedExpr(e) => e,
        SelectItem::ExprWithAlias { expr, .. } => expr,
        SelectItem::QualifiedWildcard(kind, _) => {
            return match kind {
                sqlparser::ast::SelectItemQualifiedWildcardKind::ObjectName(n) => {
                    n.0.first().map(|p| p.to_string())
                }
                _ => None,
            }
        }
        _ => return None,
    };
    as_column_ref(expr).and_then(|(q, _)| q)
}

fn group_by_expressions(select: &Select) -> Vec<Expr> {
    match &select.group_by {
        GroupByExpr::Expressions(exprs, _) => exprs.clone(),
        GroupByExpr::All(_) => Vec::new(),
    }
}

/// `(expression, descending, nulls first)` for each ORDER BY term.
fn order_by_expressions(query: &Query) -> Vec<(Expr, bool, bool)> {
    let Some(ob) = &query.order_by else { return Vec::new() };
    let OrderByKind::Expressions(exprs) = &ob.kind else { return Vec::new() };
    exprs
        .iter()
        .map(|e| {
            let desc = e.options.asc == Some(false);
            // SQL's default puts NULLs first ascending and last descending, which
            // is what MySQL, PostgreSQL and SQL Server all do by default.
            let nulls_first = e.options.nulls_first.unwrap_or(!desc);
            (e.expr.clone(), desc, nulls_first)
        })
        .collect()
}

fn limit_and_offset(query: &Query) -> (Option<u64>, Option<u64>) {
    use sqlparser::ast::LimitClause;
    let as_u64 = |e: &Expr| -> Option<u64> {
        match e {
            Expr::Value(v) => match &v.value {
                sqlparser::ast::Value::Number(n, _) => n.parse().ok(),
                _ => None,
            },
            _ => None,
        }
    };
    match &query.limit_clause {
        Some(LimitClause::LimitOffset { limit, offset, .. }) => (
            limit.as_ref().and_then(as_u64),
            offset.as_ref().and_then(|o| as_u64(&o.value)),
        ),
        Some(LimitClause::OffsetCommaLimit { offset, limit }) => (as_u64(limit), as_u64(offset)),
        None => (None, None),
    }
}

fn set_limit(query: &mut Query, limit: u64, offset: Option<u64>) {
    use sqlparser::ast::{LimitClause, Offset, OffsetRows, Value as AstValue, ValueWithSpan};
    let num = |n: u64| {
        Expr::Value(ValueWithSpan {
            value: AstValue::Number(n.to_string(), false),
            span: sqlparser::tokenizer::Span::empty(),
        })
    };
    query.limit_clause = Some(LimitClause::LimitOffset {
        limit: Some(num(limit)),
        offset: offset.map(|o| Offset { value: num(o), rows: OffsetRows::None }),
        limit_by: Vec::new(),
    });
}

fn set_order_by(query: &mut Query, exprs: &[Expr]) {
    use sqlparser::ast::{OrderBy as AstOrderBy, OrderByExpr, OrderByOptions};
    if exprs.is_empty() {
        return;
    }
    query.order_by = Some(AstOrderBy {
        kind: OrderByKind::Expressions(
            exprs
                .iter()
                .map(|e| OrderByExpr {
                    expr: e.clone(),
                    options: OrderByOptions { asc: Some(true), nulls_first: None },
                    with_fill: None,
                })
                .collect(),
        ),
        interpolate: None,
    });
}

/// Position of an expression in the projection, matched by rendered form or alias.
fn find_in_projection(projection: &[SelectItem], e: &Expr) -> Option<usize> {
    let target = e.to_string();
    projection.iter().position(|item| match item {
        SelectItem::UnnamedExpr(x) => x.to_string() == target,
        SelectItem::ExprWithAlias { expr, alias } => {
            expr.to_string() == target || alias.value == target
        }
        _ => false,
    })
}

/// `ORDER BY 2` — the ordinal, if the expression is a bare number.
fn positional(e: &Expr) -> Option<usize> {
    match e {
        Expr::Value(v) => match &v.value {
            sqlparser::ast::Value::Number(n, _) => n.parse().ok(),
            _ => None,
        },
        _ => None,
    }
}

/// True when every qualified column in the expression belongs to `alias`.
fn mentions_only(e: &Expr, alias: &str) -> bool {
    let mut ok = true;
    let mut seen_any = false;
    visit_columns(e, &mut |q, _| {
        if let Some(q) = q {
            seen_any = true;
            if !q.eq_ignore_ascii_case(alias) {
                ok = false;
            }
        }
    });
    ok && seen_any
}

fn visit_columns(e: &Expr, f: &mut impl FnMut(Option<&str>, &str)) {
    if let Some((q, c)) = as_column_ref(e) {
        f(q.as_deref(), &c);
        return;
    }
    match e {
        Expr::BinaryOp { left, right, .. } => {
            visit_columns(left, f);
            visit_columns(right, f);
        }
        Expr::UnaryOp { expr, .. } | Expr::Nested(expr) | Expr::Cast { expr, .. } => visit_columns(expr, f),
        Expr::InList { expr, list, .. } => {
            visit_columns(expr, f);
            for i in list {
                visit_columns(i, f);
            }
        }
        Expr::Between { expr, low, high, .. } => {
            visit_columns(expr, f);
            visit_columns(low, f);
            visit_columns(high, f);
        }
        Expr::IsNull(x) | Expr::IsNotNull(x) => visit_columns(x, f),
        _ => {}
    }
}

/// Remove the keyspace qualifier from table names before the statement goes to a
/// shard: the shard's database is the keyspace, so `commerce.user` would be a
/// database that does not exist there.
fn strip_keyspace_qualifiers(query: &mut Query, tables: &[TableRef]) {
    let mut stmt = Statement::Query(Box::new(query.clone()));
    strip_qualifiers(&mut stmt);
    if let Statement::Query(q) = stmt {
        *query = *q;
    }
    let _ = tables;
}

fn strip_qualifiers(stmt: &mut Statement) {
    use sqlparser::ast::VisitMut;
    struct Strip;
    impl sqlparser::ast::VisitorMut for Strip {
        type Break = ();
        fn pre_visit_relation(&mut self, name: &mut ObjectName) -> std::ops::ControlFlow<()> {
            if name.0.len() > 1 {
                let last = name.0.last().cloned();
                if let Some(last) = last {
                    *name = ObjectName(vec![last]);
                }
            }
            std::ops::ControlFlow::Continue(())
        }
    }
    let _ = stmt.visit(&mut Strip);
}
