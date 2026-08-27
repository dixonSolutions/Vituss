//! `sql-ast` defines the boundary between dialect-specific SQL parsers
//! (MySQL, PostgreSQL, ANSI/generic, ...) and the dialect-agnostic command
//! layer (routing, planning, sharding) that sits on top of them.
//!
//! # Why not one shared AST?
//!
//! A single AST that faithfully represents MySQL *and* PostgreSQL *and*
//! ANSI SQL is not realistic without either (a) losing dialect-specific
//! fidelity, or (b) reinventing a grammar superset that drifts from every
//! real database's actual grammar. Real-world dialect parsers (the
//! PostgreSQL grammar wrapped by `pg_query`, `sqlparser-rs`'s grammar for
//! MySQL/ANSI/etc.) already disagree on AST shape.
//!
//! Instead this crate defines two separate boundaries:
//!
//! 1. [`SqlDialectParser`] — a trait each dialect crate implements to turn
//!    raw SQL text into a [`ParsedStatement`], a small enum that keeps each
//!    dialect's *native* AST intact (no lossy translation at parse time).
//! 2. [`StatementInfo`] — a trait each `ParsedStatement` variant implements
//!    to answer the small set of semantic questions the command/routing
//!    layer actually needs (statement kind, tables touched, is it a
//!    write, ...), *without* the command layer ever matching on a
//!    dialect-specific AST node.
//!
//! The command layer (see `vtgate-core`) is written entirely against
//! `StatementInfo` + `SqlDialectParser`, so it never depends on any
//! specific dialect crate. New dialects plug in by implementing these two
//! traits; nothing in the routing/planning layer changes.

use std::fmt;

/// Coarse statement classification the command layer needs to route,
/// plan, and account for a query, independent of dialect.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StatementKind {
    Select,
    Insert,
    Update,
    Delete,
    Ddl,
    Transaction,
    Other,
}

impl StatementKind {
    /// Whether this statement kind mutates data (used for read/write
    /// splitting and routing to primaries vs. replicas).
    pub fn is_write(self) -> bool {
        matches!(
            self,
            StatementKind::Insert
                | StatementKind::Update
                | StatementKind::Delete
                | StatementKind::Ddl
        )
    }
}

/// A table reference as seen by the routing layer: just enough to resolve
/// a keyspace/shard, not a full dialect-specific object reference.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableRef {
    pub schema: Option<String>,
    pub name: String,
}

impl fmt::Display for TableRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.schema {
            Some(schema) => write!(f, "{schema}.{}", self.name),
            None => write!(f, "{}", self.name),
        }
    }
}

/// The semantic surface every dialect's parsed statement must expose to
/// the command/routing layer. Each dialect crate implements this for its
/// own native AST node type(s) — the command layer never needs to know
/// what that underlying type is.
pub trait StatementInfo {
    fn kind(&self) -> StatementKind;
    /// Tables referenced by this statement (targets for DML, referenced
    /// tables for SELECT/joins). Best-effort: dialect crates should
    /// return everything they can resolve syntactically.
    fn tables(&self) -> Vec<TableRef>;
    /// Round-trippable SQL text for this statement, used for pushing the
    /// statement down to a shard/tablet unchanged.
    fn to_sql(&self) -> String;
}

/// A parsed statement from some dialect, wrapping the dialect's native
/// AST behind [`StatementInfo`]. Dialect crates provide a concrete boxed
/// value; the command layer only ever touches it through the trait.
pub struct ParsedStatement {
    pub dialect: &'static str,
    inner: Box<dyn StatementInfo + Send + Sync>,
}

impl ParsedStatement {
    pub fn new(dialect: &'static str, inner: Box<dyn StatementInfo + Send + Sync>) -> Self {
        Self { dialect, inner }
    }
}

impl StatementInfo for ParsedStatement {
    fn kind(&self) -> StatementKind {
        self.inner.kind()
    }

    fn tables(&self) -> Vec<TableRef> {
        self.inner.tables()
    }

    fn to_sql(&self) -> String {
        self.inner.to_sql()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("[{dialect}] syntax error: {message}")]
    Syntax {
        dialect: &'static str,
        message: String,
    },
    #[error("[{dialect}] statement not supported by the routing layer yet: {message}")]
    Unsupported {
        dialect: &'static str,
        message: String,
    },
}

/// Implemented once per SQL dialect. `vtgate-core` holds a registry of
/// `Box<dyn SqlDialectParser>` keyed by dialect name and dispatches to
/// whichever one a session/connection was negotiated for.
pub trait SqlDialectParser: Send + Sync {
    /// Short, stable identifier used for registry lookup and diagnostics
    /// (e.g. "mysql", "postgres", "ansi").
    fn name(&self) -> &'static str;

    /// Parse a single SQL statement in this dialect.
    fn parse(&self, sql: &str) -> Result<ParsedStatement, ParseError>;

    /// Parse a (possibly multi-statement) batch of SQL text.
    fn parse_batch(&self, sql: &str) -> Result<Vec<ParsedStatement>, ParseError>;
}
