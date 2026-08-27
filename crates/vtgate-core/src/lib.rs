//! `vtgate-core`: the dialect-agnostic command/routing layer.
//!
//! This plays the role Vitess's VTGate plays today, but written only
//! against [`sql_ast::SqlDialectParser`] and [`sql_ast::StatementInfo`].
//! It never imports a dialect crate (`sql-dialect-mysql`,
//! `sql-dialect-postgres`, ...) — those are registered by whatever binary
//! wires the system together (see `vtgate-cli`), exactly the way Vitess's
//! VTGate is transport/storage-engine agnostic at the tablet layer.
//!
//! What's here is intentionally a stub: real routing needs a vschema
//! (keyspace/shard/vindex definitions), a topology client, and a planner
//! that turns `StatementInfo` + vschema into a scatter/gather plan. This
//! crate defines the seam those pieces would be built behind.

use std::collections::HashMap;

use sql_ast::{
    ParseError, ParsedStatement, SqlDialectParser, StatementInfo, StatementKind, TableRef,
};

/// Holds one [`SqlDialectParser`] per dialect name. A real deployment
/// would populate this once at startup from whatever dialects are
/// compiled in / licensed / configured, then look up the right one per
/// client connection (e.g. from the negotiated wire protocol, or a
/// per-keyspace config in the vschema).
#[derive(Default)]
pub struct DialectRegistry {
    parsers: HashMap<&'static str, Box<dyn SqlDialectParser>>,
}

impl DialectRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, parser: Box<dyn SqlDialectParser>) -> &mut Self {
        self.parsers.insert(parser.name(), parser);
        self
    }

    pub fn get(&self, dialect: &str) -> Option<&dyn SqlDialectParser> {
        self.parsers.get(dialect).map(|b| b.as_ref())
    }

    pub fn dialects(&self) -> impl Iterator<Item = &'static str> + '_ {
        self.parsers.keys().copied()
    }
}

/// Where a statement should be routed. A real implementation resolves
/// this from a vschema (keyspace -> shards -> vindexes) plus the
/// statement's tables/predicates; this stub always scatters to every
/// configured target, mirroring Vitess's routing decision points
/// (`EqualUnique`, `Scatter`, `IN`, ...) without implementing them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoutingDecision {
    /// Routes to a single shard/tablet because the statement's predicates
    /// pin it via a unique vindex.
    Single(String),
    /// No vindex could narrow the target; the statement must be sent to
    /// every shard and results merged (or an error, for a write).
    Scatter(Vec<String>),
}

#[derive(Debug)]
pub struct RoutedStatement {
    pub dialect: &'static str,
    pub kind: StatementKind,
    pub tables: Vec<TableRef>,
    pub sql: String,
    pub decision: RoutingDecision,
}

#[derive(Debug, thiserror::Error)]
pub enum RouteError {
    #[error("no parser registered for dialect {0:?}")]
    UnknownDialect(String),
    #[error(transparent)]
    Parse(#[from] ParseError),
    #[error("write statements must not be scattered without an explicit vindex: {0}")]
    UnsafeScatterWrite(String),
}

/// The stub router: parse via the registry, classify via `StatementInfo`,
/// and decide a target. Everything past "decide a target" (vschema
/// lookup, actual shard list, cross-shard transaction coordination,
/// result merging) is out of scope for this skeleton.
pub struct Router {
    pub registry: DialectRegistry,
    /// Placeholder "vschema": every keyspace just scatters to these shard
    /// names. A real router replaces this with per-table vindex-driven
    /// resolution.
    pub known_shards: Vec<String>,
}

impl Router {
    pub fn new(registry: DialectRegistry, known_shards: Vec<String>) -> Self {
        Self {
            registry,
            known_shards,
        }
    }

    pub fn route(&self, dialect: &str, sql: &str) -> Result<RoutedStatement, RouteError> {
        let parser = self
            .registry
            .get(dialect)
            .ok_or_else(|| RouteError::UnknownDialect(dialect.to_string()))?;
        let stmt = parser.parse(sql)?;
        self.route_parsed(stmt)
    }

    pub fn route_parsed(&self, stmt: ParsedStatement) -> Result<RoutedStatement, RouteError> {
        let kind = stmt.kind();
        let tables = stmt.tables();
        let sql = stmt.to_sql();
        let dialect = stmt.dialect;

        let decision = if kind.is_write() && self.known_shards.len() > 1 {
            return Err(RouteError::UnsafeScatterWrite(sql));
        } else {
            RoutingDecision::Scatter(self.known_shards.clone())
        };

        Ok(RoutedStatement {
            dialect,
            kind,
            tables,
            sql,
            decision,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn registry() -> DialectRegistry {
        let mut registry = DialectRegistry::new();
        registry
            .register(Box::new(sql_dialect_mysql::MySqlSqlParser))
            .register(Box::new(sql_dialect_postgres::PostgresSqlParser));
        registry
    }

    /// Same router logic, two structurally unrelated dialect backends
    /// (sqlparser-rs's AST vs. pg_query's protobuf tree) — proving
    /// `vtgate-core` genuinely only depends on `StatementInfo`.
    #[test]
    fn routes_reads_identically_across_dialects() {
        let router = Router::new(registry(), vec!["shard-0".into(), "shard-1".into()]);

        for dialect in ["mysql", "postgres"] {
            let routed = router
                .route(dialect, "SELECT id FROM users WHERE id = 5")
                .unwrap();
            assert_eq!(routed.kind, StatementKind::Select);
            assert_eq!(
                routed.tables,
                vec![TableRef {
                    schema: None,
                    name: "users".into()
                }]
            );
            assert_eq!(
                routed.decision,
                RoutingDecision::Scatter(vec!["shard-0".into(), "shard-1".into()])
            );
        }
    }

    #[test]
    fn rejects_multi_shard_write() {
        let router = Router::new(registry(), vec!["shard-0".into(), "shard-1".into()]);
        let err = router
            .route("mysql", "UPDATE users SET name = 'x' WHERE id = 1")
            .unwrap_err();
        assert!(matches!(err, RouteError::UnsafeScatterWrite(_)));
    }

    #[test]
    fn unknown_dialect_errors() {
        let router = Router::new(registry(), vec!["shard-0".into()]);
        let err = router.route("mssql", "SELECT 1").unwrap_err();
        assert!(matches!(err, RouteError::UnknownDialect(d) if d == "mssql"));
    }
}
