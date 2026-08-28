//! # vituss-planner
//!
//! Turns a parsed statement into a [`Plan`]: a tree describing which shards to
//! contact, what to send them, and what to do with what comes back.
//!
//! The planner never looks at data and never talks to a database. It works from
//! the VSchema (how tables are sharded) and the statement alone, which is why a
//! plan can be cached and reused for every execution of the same query shape.
//!
//! It is also engine-neutral. The statement arrives already parsed by the
//! keyspace's own dialect, and leaves as an AST that each shard's dialect renders
//! for itself — so the same plan can drive a MySQL shard and a PostgreSQL shard
//! in the same query.

pub mod analyze;
pub mod dml;
pub mod normalize;
pub mod plan;
pub mod select;
pub mod util;

use sqlparser::ast::Statement;

use vituss_core::{Error, Result, Value};
use vituss_dialect::DialectRef;
use vituss_vschema::VSchema;

pub use analyze::{Constraint, Constraints, Routing};
pub use plan::{
    Aggregate, AggregateExpr, AggregateFunc, Ddl, Distinct, DmlKind, DmlPlan, InsertPlan, InsertRow, Join,
    JoinKind, Limit, OrderBy, Plan, Primitive, Route, RouteOpcode, RouteValue, SequencePlan, SessionOp,
    SetScope, ShowPlan, Sort,
};
pub use normalize::{client_placeholder_name, number_client_placeholders};
pub use select::TableRef;

/// Plans statements against one VSchema.
pub struct Planner<'a> {
    pub vschema: &'a VSchema,
    /// The session's current keyspace, used to resolve unqualified table names.
    pub default_keyspace: Option<&'a str>,
    /// The dialect the client is speaking, used to parse and to build fragments.
    pub dialect: DialectRef,
}

impl<'a> Planner<'a> {
    pub fn new(vschema: &'a VSchema, default_keyspace: Option<&'a str>, dialect: DialectRef) -> Self {
        Self { vschema, default_keyspace, dialect }
    }

    /// Parse and plan a statement.
    pub fn plan(&self, sql: &str) -> Result<Plan> {
        let stmt = self.dialect.parse_one(sql)?;
        self.plan_statement(&stmt, sql)
    }

    /// Plan an already-parsed statement.
    pub fn plan_statement(&self, stmt: &Statement, sql: &str) -> Result<Plan> {
        let primitive = self.build(stmt)?;
        let mut keyspaces = Vec::new();
        collect_keyspaces(&primitive, &mut keyspaces);
        keyspaces.sort();
        keyspaces.dedup();
        Ok(Plan {
            is_dml: matches!(
                primitive,
                Primitive::Insert(_) | Primitive::Dml(_) | Primitive::Ddl(_)
            ),
            primitive,
            original_sql: sql.to_string(),
            keyspaces,
        })
    }

    fn build(&self, stmt: &Statement) -> Result<Primitive> {
        match stmt {
            Statement::Query(q) => self.plan_query(q),
            Statement::Insert(i) => self.plan_insert(i),
            Statement::Update(u) => self.plan_update(u),
            Statement::Delete(d) => self.plan_delete(d),

            Statement::StartTransaction { .. } => Ok(Primitive::Session(SessionOp::Begin)),
            Statement::Commit { .. } => Ok(Primitive::Session(SessionOp::Commit)),
            Statement::Rollback { savepoint, .. } => Ok(Primitive::Session(match savepoint {
                Some(name) => SessionOp::RollbackTo(name.value.clone()),
                None => SessionOp::Rollback,
            })),
            Statement::Savepoint { name } => Ok(Primitive::Session(SessionOp::Savepoint(name.value.clone()))),
            Statement::ReleaseSavepoint { name } => {
                Ok(Primitive::Session(SessionOp::Release(name.value.clone())))
            }

            Statement::Use(u) => self.plan_use(&u.to_string()),
            Statement::Set(s) => self.plan_set(s),

            Statement::ShowDatabases { .. } => Ok(Primitive::Show(ShowPlan::Databases)),
            Statement::ShowSchemas { .. } => Ok(Primitive::Show(ShowPlan::Keyspaces)),
            Statement::ShowTables { show_options, .. } => Ok(Primitive::Show(ShowPlan::Tables {
                keyspace: show_options
                    .show_in
                    .as_ref()
                    .and_then(|i| i.parent_name.as_ref())
                    .map(|n| n.to_string()),
            })),
            Statement::ShowVariables { .. }
            | Statement::ShowStatus { .. }
            | Statement::ShowCollation { .. }
            | Statement::ShowFunctions { .. }
            | Statement::ShowColumns { .. }
            | Statement::ShowCreate { .. } => Ok(Primitive::Show(ShowPlan::Passthrough)),

            // Everything that changes a table's shape goes to every shard: each
            // shard holds the same schema for the tables it stores.
            Statement::CreateTable(_)
            | Statement::AlterTable { .. }
            | Statement::Drop { .. }
            | Statement::CreateIndex(_)
            | Statement::CreateView(_)
            | Statement::Truncate(_)
            | Statement::AlterIndex { .. }
            | Statement::CreateSchema { .. }
            | Statement::CreateDatabase { .. } => self.plan_ddl(stmt),

            other => Err(Error::unsupported(format!(
                "Vituss cannot route this statement yet: {}",
                first_words(&other.to_string())
            ))),
        }
    }

    fn plan_ddl(&self, stmt: &Statement) -> Result<Primitive> {
        let keyspace = self.ddl_keyspace(stmt)?;
        let mut stripped = stmt.clone();
        strip_relation_qualifiers(&mut stripped);
        Ok(Primitive::Ddl(Ddl {
            keyspace,
            statement: stripped,
            is_database_level: matches!(
                stmt,
                Statement::CreateDatabase { .. } | Statement::CreateSchema { .. }
            ) || matches!(stmt, Statement::Drop { object_type, .. }
                if matches!(object_type, sqlparser::ast::ObjectType::Database | sqlparser::ast::ObjectType::Schema)),
        }))
    }

    /// Which keyspace a DDL applies to: the qualifier on the object name, else
    /// the session's keyspace.
    fn ddl_keyspace(&self, stmt: &Statement) -> Result<String> {
        let mut found: Option<String> = None;
        {
            use sqlparser::ast::{ObjectName, VisitMut};
            struct Find<'a>(&'a mut Option<String>);
            impl sqlparser::ast::VisitorMut for Find<'_> {
                type Break = ();
                fn pre_visit_relation(&mut self, name: &mut ObjectName) -> std::ops::ControlFlow<()> {
                    if self.0.is_none() && name.0.len() > 1 {
                        *self.0 = Some(name.0[name.0.len() - 2].to_string().trim_matches(|c| {
                            c == '`' || c == '"' || c == '[' || c == ']'
                        }).to_string());
                    }
                    std::ops::ControlFlow::Continue(())
                }
            }
            let mut c = stmt.clone();
            let _ = c.visit(&mut Find(&mut found));
        }
        found
            .or_else(|| self.default_keyspace.map(str::to_string))
            .ok_or_else(|| {
                Error::failed_precondition(
                    "no keyspace selected: qualify the object name or issue USE <keyspace> first",
                )
            })
    }

    fn plan_use(&self, raw: &str) -> Result<Primitive> {
        // `USE ks`, `USE ks@replica`, `USE ks:-80`. The last two are Vituss (and
        // Vitess) extensions that let a client aim at a specific tablet type or
        // shard without any protocol change.
        let target = raw.trim_start_matches("USE ").trim().trim_matches('`').trim_matches('"');
        let (rest, tablet_type) = match target.split_once('@') {
            Some((r, t)) => (r, Some(t.to_string())),
            None => (target, None),
        };
        let (keyspace, shard) = match rest.split_once(':') {
            Some((k, s)) => (k.to_string(), Some(s.to_string())),
            None => (rest.to_string(), None),
        };
        // Fail here rather than on the next query.
        self.vschema.keyspace(&keyspace)?;
        Ok(Primitive::Session(SessionOp::Use { keyspace, shard, tablet_type }))
    }

    fn plan_set(&self, set: &sqlparser::ast::Set) -> Result<Primitive> {
        let raw = set.to_string();
        // sqlparser models the many SET forms as separate variants; the parts
        // Vituss cares about are the name, the value and the scope, so it reads
        // them off the rendered statement rather than matching a dozen shapes.
        let body = raw.trim_start_matches("SET ").trim();
        let (scope, body) = if let Some(r) = body.strip_prefix("GLOBAL ") {
            (SetScope::Global, r)
        } else if let Some(r) = body.strip_prefix("SESSION ") {
            (SetScope::Session, r)
        } else if let Some(r) = body.strip_prefix("LOCAL ") {
            (SetScope::Session, r)
        } else {
            (SetScope::Session, body)
        };

        let (name, value) = body
            .split_once('=')
            .ok_or_else(|| Error::unsupported(format!("cannot interpret {raw:?}")))?;
        let name = name.trim().to_string();
        let value = value.trim().trim_matches('\'').trim_matches('"');

        let scope = if name.starts_with('@') && !name.starts_with("@@") {
            SetScope::UserDefined
        } else if name.trim_start_matches('@').starts_with("vituss_") {
            SetScope::Vituss
        } else {
            scope
        };

        let value = match value.parse::<i64>() {
            Ok(i) => Value::Int(i),
            Err(_) => Value::Text(value.to_string()),
        };
        Ok(Primitive::Session(SessionOp::Set { name, value, scope }))
    }
}

fn collect_keyspaces(p: &Primitive, out: &mut Vec<String>) {
    match p {
        Primitive::Route(r) => out.push(r.keyspace.clone()),
        Primitive::Insert(i) => out.push(i.keyspace.clone()),
        Primitive::Dml(d) => {
            out.push(d.route.keyspace.clone());
            if let Some(pre) = &d.pre_query {
                out.push(pre.keyspace.clone());
            }
        }
        Primitive::Ddl(d) => out.push(d.keyspace.clone()),
        Primitive::Join(j) => {
            collect_keyspaces(&j.left, out);
            collect_keyspaces(&j.right, out);
        }
        Primitive::Limit(l) => collect_keyspaces(&l.input, out),
        Primitive::Sort(s) => collect_keyspaces(&s.input, out),
        Primitive::Distinct(d) => collect_keyspaces(&d.input, out),
        Primitive::Aggregate(a) => collect_keyspaces(&a.input, out),
        Primitive::Truncate { input, .. } => collect_keyspaces(input, out),
        Primitive::Concatenate { inputs, .. } => {
            for i in inputs {
                collect_keyspaces(i, out);
            }
        }
        Primitive::SingleRow | Primitive::Session(_) | Primitive::Show(_) => {}
    }
}

fn strip_relation_qualifiers(stmt: &mut Statement) {
    use sqlparser::ast::{ObjectName, VisitMut};
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

fn first_words(s: &str) -> String {
    s.split_whitespace().take(6).collect::<Vec<_>>().join(" ")
}
