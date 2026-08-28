//! INSERT / UPDATE / DELETE planning.
//!
//! Writes are where sharding stops being transparent. A `SELECT` that goes to
//! the wrong shard returns too few rows; an `INSERT` that goes to the wrong shard
//! puts data where no query will ever look for it again. So the planner is strict
//! here: if it cannot prove where a row belongs, it refuses rather than guesses.

use sqlparser::ast::{
    Expr, Insert, ObjectName, Query, SetExpr, Statement, TableFactor, TableObject, Update, Values,
};

use vituss_core::{Error, Result};
use vituss_vschema::Table;

use crate::analyze::{constraints_for, route_table};
use crate::normalize::Normalizer;
use crate::plan::{
    DmlKind, DmlPlan, InsertPlan, InsertRow, Primitive, Route, RouteOpcode, RouteValue, SequencePlan,
};
use crate::util::expr_to_route_value;
use crate::Planner;

impl Planner<'_> {
    pub fn plan_insert(&self, insert: &Insert) -> Result<Primitive> {
        let TableObject::TableName(name) = &insert.table else {
            return Err(Error::unsupported("only INSERT INTO <table> is supported"));
        };
        let table = self.resolve_table_name(name)?;

        if !table.sharded {
            // One shard: send it as written.
            let mut stmt = Statement::Insert(insert.clone());
            let bind_vars = normalize_insert(&mut stmt);
            strip_table_qualifier(&mut stmt);
            let mut route = Route::new(RouteOpcode::Unsharded, &table.keyspace, stmt);
            route.bind_vars = bind_vars;
            route.table = Some(table.name.clone());
            return Ok(Primitive::Route(route));
        }

        if insert.source.is_none() {
            return Err(Error::unsupported(
                "INSERT ... SET is not supported for a sharded table; use INSERT ... VALUES so the \
                 sharding column can be read from the statement",
            ));
        }
        let source = insert.source.as_ref().expect("checked above");

        // Column names, from the statement or from an authoritative VSchema list.
        let declared_columns: Vec<String> = if !insert.columns.is_empty() {
            insert.columns.iter().map(object_name_last).collect()
        } else if table.column_list_authoritative {
            table.columns.iter().map(|c| c.name.clone()).collect()
        } else {
            return Err(Error::invalid(format!(
                "INSERT into sharded table {}.{} must list its columns: without them Vituss cannot \
                 tell which value is the sharding key",
                table.keyspace, table.name
            )));
        };

        let SetExpr::Values(Values { rows, .. }) = &*source.body else {
            return Err(Error::unsupported(
                "INSERT ... SELECT into a sharded table is not supported: the rows the SELECT \
                 produces would have to be routed one by one, which Vituss will not do implicitly. \
                 Read the rows and insert them explicitly.",
            ));
        };

        let primary = table.primary_vindex.as_ref().ok_or_else(|| {
            Error::internal(format!("sharded table {} has no primary vindex", table.name))
        })?;

        // A column the client omitted but a sequence supplies is added to the
        // statement here, so that by the time routing happens every sharding
        // column has a value and a known position.
        let mut columns = declared_columns.clone();
        if let Some(auto) = &table.auto_increment {
            if !columns.iter().any(|c| c.eq_ignore_ascii_case(&auto.column)) {
                columns.push(auto.column.clone());
            }
        }

        // Where the vindex's columns sit in the statement's column list.
        let mut vindex_positions = Vec::new();
        for want in &primary.columns {
            match columns.iter().position(|c| c.eq_ignore_ascii_case(want)) {
                Some(p) => vindex_positions.push(p),
                None => {
                    return Err(Error::invalid(format!(
                        "INSERT into {}.{} does not set {want:?}, which is the sharding column, and \
                         no sequence supplies it. Vituss cannot tell which shard the row belongs to.",
                        table.keyspace, table.name
                    )))
                }
            }
        }

        let sequence = table.auto_increment.as_ref().map(|a| SequencePlan {
            column: a.column.clone(),
            column_position: columns.iter().position(|c| c.eq_ignore_ascii_case(&a.column)),
            keyspace: a.sequence.keyspace.clone(),
            table: a.sequence.table.clone(),
        });

        // One InsertRow per VALUES tuple, each carrying the values the vindex needs.
        let mut plan_rows = Vec::with_capacity(rows.len());
        for (i, row) in rows.iter().enumerate() {
            if row.len() != declared_columns.len() {
                return Err(Error::invalid(format!(
                    "INSERT row {} has {} value(s) but {} column(s) were named",
                    i + 1,
                    row.len(),
                    declared_columns.len()
                )));
            }
            let mut values: Vec<RouteValue> = row
                .iter()
                .enumerate()
                .map(|(c, e)| {
                    expr_to_route_value(e).ok_or_else(|| {
                        Error::unsupported(format!(
                            "value for column {:?} in row {} is not a constant; Vituss must know it \
                             at planning time to route the row",
                            declared_columns[c],
                            i + 1
                        ))
                    })
                })
                .collect::<Result<_>>()?;
            // Placeholders for the columns added above; the executor fills them
            // from the sequence before it routes anything.
            values.resize(columns.len(), RouteValue::Literal(vituss_core::Value::Null));

            let vindex_values = vindex_positions.iter().map(|p| values[*p].clone()).collect();
            plan_rows.push(InsertRow { values, vindex_values });
        }

        // The per-shard template: one row of placeholders, which the engine
        // repeats once per row that lands on that shard.
        let template = self.insert_template(&table, &columns, insert.ignore || insert.or.is_some())?;

        Ok(Primitive::Insert(InsertPlan {
            keyspace: table.keyspace.clone(),
            table: table.clone(),
            opcode: if table.is_reference() { RouteOpcode::Reference } else { RouteOpcode::EqualUnique },
            statement: template,
            bind_vars: Default::default(),
            columns,
            rows: plan_rows,
            vindex_column_positions: vindex_positions,
            owned_vindexes: table.owned_vindexes().cloned().collect(),
            sequence,
            ignore_duplicates: insert.ignore || insert.or.is_some(),
            pinned: table.pinned.clone(),
        }))
    }

    fn insert_template(&self, table: &Table, columns: &[String], ignore: bool) -> Result<Statement> {
        let quoted: Vec<String> = columns.iter().map(|c| self.dialect.quote_ident(c)).collect();
        let placeholders: Vec<String> = (0..columns.len()).map(|i| format!(":c{i}")).collect();
        // `IGNORE` is spelled differently on every engine, so the intent is carried
        // as a hint and rewritten by the executor for the shard's dialect.
        let hint = if ignore { "/*vt+ IGNORE_DUPLICATE */ " } else { "" };
        let sql = format!(
            "INSERT {hint}INTO {} ({}) VALUES ({})",
            self.dialect.quote_ident(&table.name),
            quoted.join(", "),
            placeholders.join(", ")
        );
        self.dialect.parse_one(&sql)
    }

    pub fn plan_update(&self, update: &Update) -> Result<Primitive> {
        if update.from.is_some() {
            return Err(Error::unsupported("UPDATE ... FROM is not supported across shards"));
        }
        if !update.table.joins.is_empty() {
            return Err(Error::unsupported("multi-table UPDATE is not supported across shards"));
        }
        let TableFactor::Table { name, alias, .. } = &update.table.relation else {
            return Err(Error::unsupported("only UPDATE <table> is supported"));
        };
        let table = self.resolve_table_name(name)?;
        let alias_name = alias
            .as_ref()
            .map(|a| a.name.value.clone())
            .unwrap_or_else(|| table.name.clone());

        // Moving a row between shards is a delete plus an insert, not an update:
        // the row's physical location is derived from this column.
        if let Some(primary) = &table.primary_vindex {
            for a in &update.assignments {
                let target = assignment_column(a);
                if primary.columns.iter().any(|c| c.eq_ignore_ascii_case(&target)) {
                    return Err(Error::unsupported(format!(
                        "cannot UPDATE {target:?}: it is the sharding column of {}.{}, so changing \
                         it would move the row to another shard. Delete the row and insert it again.",
                        table.keyspace, table.name
                    )));
                }
            }
        }

        let quals = vec![alias_name, table.name.clone()];
        let constraints = constraints_for(update.selection.as_ref(), &quals);
        let routing = route_table(&table, &constraints);

        let mut stmt = Statement::Update(update.clone());
        let bind_vars = normalize_where(&mut stmt);
        strip_table_qualifier(&mut stmt);

        let mut route = Route::new(routing.opcode, &table.keyspace, stmt);
        route.vindex = routing.vindex.clone();
        route.values = routing.values.clone();
        route.table = Some(table.name.clone());
        route.bind_vars = bind_vars;

        // Only vindexes whose columns this statement actually changes need
        // maintaining; the rest are untouched by the update.
        let changed: Vec<String> = update.assignments.iter().map(assignment_column).collect();
        let owned: Vec<_> = table
            .owned_vindexes()
            .filter(|cv| {
                cv.columns
                    .iter()
                    .any(|c| changed.iter().any(|ch| ch.eq_ignore_ascii_case(c)))
            })
            .cloned()
            .collect();

        let pre_query = if owned.is_empty() {
            None
        } else {
            Some(Box::new(self.owned_vindex_pre_query(&table, &owned, update.selection.as_ref())?))
        };

        Ok(Primitive::Dml(DmlPlan {
            kind: DmlKind::Update,
            route,
            table,
            owned_vindexes: owned,
            pre_query,
            multi_shard_autocommit: false,
        }))
    }

    pub fn plan_delete(&self, delete: &sqlparser::ast::Delete) -> Result<Primitive> {
        let tables = match &delete.from {
            sqlparser::ast::FromTable::WithFromKeyword(t) | sqlparser::ast::FromTable::WithoutKeyword(t) => t,
        };
        if tables.len() != 1 || !tables[0].joins.is_empty() {
            return Err(Error::unsupported("multi-table DELETE is not supported across shards"));
        }
        let TableFactor::Table { name, alias, .. } = &tables[0].relation else {
            return Err(Error::unsupported("only DELETE FROM <table> is supported"));
        };
        let table = self.resolve_table_name(name)?;
        let alias_name = alias
            .as_ref()
            .map(|a| a.name.value.clone())
            .unwrap_or_else(|| table.name.clone());

        let quals = vec![alias_name, table.name.clone()];
        let constraints = constraints_for(delete.selection.as_ref(), &quals);
        let routing = route_table(&table, &constraints);

        let mut stmt = Statement::Delete(delete.clone());
        let bind_vars = normalize_where(&mut stmt);
        strip_table_qualifier(&mut stmt);

        let mut route = Route::new(routing.opcode, &table.keyspace, stmt);
        route.vindex = routing.vindex.clone();
        route.values = routing.values.clone();
        route.table = Some(table.name.clone());
        route.bind_vars = bind_vars;

        let owned: Vec<_> = table.owned_vindexes().cloned().collect();
        let pre_query = if owned.is_empty() {
            None
        } else {
            Some(Box::new(self.owned_vindex_pre_query(&table, &owned, delete.selection.as_ref())?))
        };

        Ok(Primitive::Dml(DmlPlan {
            kind: DmlKind::Delete,
            route,
            table,
            owned_vindexes: owned,
            pre_query,
            multi_shard_autocommit: false,
        }))
    }

    /// Read the rows a DML is about to change, so their lookup-vindex entries can
    /// be removed before the rows themselves are.
    ///
    /// Without this, deleting a row would leave a lookup entry pointing at
    /// nothing, and the next query routed through that vindex would go to a shard
    /// that no longer has the row.
    fn owned_vindex_pre_query(
        &self,
        table: &Table,
        owned: &[vituss_vschema::ColumnVindex],
        where_clause: Option<&Expr>,
    ) -> Result<Route> {
        let mut columns: Vec<String> = Vec::new();
        for cv in owned {
            for c in &cv.columns {
                if !columns.iter().any(|e| e.eq_ignore_ascii_case(c)) {
                    columns.push(c.clone());
                }
            }
        }
        // The primary vindex columns too: the lookup row records which keyspace id
        // the entry pointed at, and deleting it needs that value.
        if let Some(primary) = &table.primary_vindex {
            for c in &primary.columns {
                if !columns.iter().any(|e| e.eq_ignore_ascii_case(c)) {
                    columns.push(c.clone());
                }
            }
        }

        let cols = columns
            .iter()
            .map(|c| self.dialect.quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ");
        let where_sql = match where_clause {
            Some(w) => format!(" WHERE {w}"),
            None => String::new(),
        };
        // FOR UPDATE: the rows must not move between reading them and deleting
        // their lookup entries.
        let sql = format!(
            "SELECT {cols} FROM {}{where_sql} FOR UPDATE",
            self.dialect.quote_ident(&table.name)
        );
        let mut stmt = self.dialect.parse_one(&sql)?;
        let bind_vars = normalize_where(&mut stmt);

        let table_name = [table.name.clone()];
        let constraints = constraints_for(where_clause, &table_name);
        let routing = route_table(table, &constraints);
        let mut route = Route::new(routing.opcode, &table.keyspace, stmt);
        route.vindex = routing.vindex;
        route.values = routing.values;
        route.table = Some(table.name.clone());
        route.bind_vars = bind_vars;
        Ok(route)
    }

    pub(crate) fn resolve_table_name(&self, name: &ObjectName) -> Result<std::sync::Arc<Table>> {
        let parts: Vec<String> = name.0.iter().map(|p| unquote(&p.to_string())).collect();
        match parts.len() {
            1 => self.vschema.find_table(self.default_keyspace, &parts[0]),
            2 => self.vschema.find_table(Some(&parts[0]), &parts[1]),
            _ => Err(Error::invalid(format!(
                "table name {name} has too many parts; expected [keyspace.]table"
            ))),
        }
    }
}

fn unquote(s: &str) -> String {
    s.trim_matches(|c| c == '`' || c == '"' || c == '[' || c == ']').to_string()
}

fn object_name_last(n: &ObjectName) -> String {
    n.0.last().map(|p| unquote(&p.to_string())).unwrap_or_default()
}

fn assignment_column(a: &sqlparser::ast::Assignment) -> String {
    match &a.target {
        sqlparser::ast::AssignmentTarget::ColumnName(n) => object_name_last(n),
        sqlparser::ast::AssignmentTarget::Tuple(ns) => {
            ns.first().map(object_name_last).unwrap_or_default()
        }
    }
}

/// Lift literals out of a DML's WHERE clause and assignments.
fn normalize_where(stmt: &mut Statement) -> vituss_core::BindVars {
    let mut n = Normalizer::new("v");
    match stmt {
        Statement::Update(u) => {
            n.opt_expr(u.selection.as_mut());
            for a in &mut u.assignments {
                n.expr(&mut a.value);
            }
        }
        Statement::Delete(d) => n.opt_expr(d.selection.as_mut()),
        Statement::Query(q) => {
            if let SetExpr::Select(s) = &mut *q.body {
                n.opt_expr(s.selection.as_mut());
            }
        }
        _ => {}
    }
    n.finish()
}

/// Lift literals out of an unsharded INSERT's VALUES.
fn normalize_insert(stmt: &mut Statement) -> vituss_core::BindVars {
    let mut n = Normalizer::new("v");
    if let Statement::Insert(i) = stmt {
        if let Some(source) = &mut i.source {
            normalize_query_values(&mut n, source);
        }
        for a in &mut i.assignments {
            n.expr(&mut a.value);
        }
    }
    n.finish()
}

fn normalize_query_values(n: &mut Normalizer, q: &mut Query) {
    if let SetExpr::Values(v) = &mut *q.body {
        for row in &mut v.rows {
            for e in row.iter_mut() {
                n.expr(e);
            }
        }
    }
}

/// A shard's database *is* the keyspace, so `commerce.user` must become `user`
/// before the statement leaves the gate.
fn strip_table_qualifier(stmt: &mut Statement) {
    use sqlparser::ast::VisitMut;
    struct Strip;
    impl sqlparser::ast::VisitorMut for Strip {
        type Break = ();
        fn pre_visit_relation(&mut self, name: &mut ObjectName) -> std::ops::ControlFlow<()> {
            if name.0.len() > 1 {
                if let Some(last) = name.0.last().cloned() {
                    *name = ObjectName(vec![last]);
                }
            }
            std::ops::ControlFlow::Continue(())
        }
    }
    let _ = stmt.visit(&mut Strip);
}
