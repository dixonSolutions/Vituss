//! The [`SqlDialect`] plug-in trait and its registry.
//!
//! Everything engine-specific in Vituss goes through this trait. A new engine is
//! added by implementing it and calling [`register`] — no changes to the planner,
//! the engine primitives, the gate or the tablet.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use once_cell::sync::Lazy;
use sqlparser::ast::Statement;
use sqlparser::dialect::Dialect as ParserDialect;
use sqlparser::parser::Parser;

use vituss_core::{Code, Error, Result, SqlType};

use crate::caps::{Capabilities, TwoPcStyle};
use crate::ddl::ColumnType;
use crate::introspect::Introspection;
use crate::render;

/// How an error should be reported to a client speaking this engine's protocol.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeError {
    pub code: u32,
    pub sql_state: String,
}

/// Statements that drive a distributed transaction on this engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TwoPcSql {
    pub start: String,
    pub end: Option<String>,
    pub prepare: String,
    pub commit: String,
    pub rollback: String,
    /// Lists transactions left prepared by a crashed coordinator, so the
    /// transaction resolver can finish or abandon them.
    pub recover: String,
}

/// A pluggable SQL engine.
///
/// Implementors describe an engine's surface: how to parse it, how to render SQL
/// back out for it, what it can do, how to introspect it and how to talk to it
/// about transactions. Query *execution* is a separate concern — see the
/// `Backend` trait in `vituss-backend`.
pub trait SqlDialect: Send + Sync + 'static {
    /// Canonical name, as used in configuration (`mysql`, `postgres`, `mssql`).
    fn name(&self) -> &'static str;

    /// Other names accepted in configuration for this engine.
    fn aliases(&self) -> &'static [&'static str] {
        &[]
    }

    /// The grammar to parse with.
    ///
    /// Vituss does not implement a SQL grammar. It borrows a maintained one per
    /// engine, which is the whole reason a query using engine-specific syntax
    /// parses correctly instead of being rejected by a hand-rolled subset.
    fn parser(&self) -> &dyn ParserDialect;

    fn capabilities(&self) -> &Capabilities;

    fn introspection(&self) -> &Introspection;

    /// TCP port the engine listens on by default.
    fn default_port(&self) -> u16;

    /// Parse a possibly multi-statement string.
    fn parse(&self, sql: &str) -> Result<Vec<Statement>> {
        Parser::parse_sql(self.parser(), sql).map_err(|e| {
            Error::new(Code::InvalidArgument, format!("syntax error ({}): {e}", self.name()))
        })
    }

    /// Parse exactly one statement, rejecting anything else.
    fn parse_one(&self, sql: &str) -> Result<Statement> {
        let mut stmts = self.parse(sql)?;
        match stmts.len() {
            1 => Ok(stmts.pop().expect("length checked")),
            0 => Err(Error::invalid("empty statement")),
            n => Err(Error::unsupported(format!(
                "expected a single statement, got {n}; multi-statement requests are rejected \
                 because they cannot be routed as a unit"
            ))),
        }
    }

    /// Quote an identifier for this engine.
    fn quote_ident(&self, ident: &str) -> String {
        render::quote_with(ident, self.capabilities().identifier_quote)
    }

    /// Normalise an unquoted identifier to the form the engine will store it as.
    /// Used when comparing a client-supplied name against the VSchema.
    fn fold_ident(&self, ident: &str) -> String {
        use crate::caps::IdentifierCase::*;
        match self.capabilities().identifier_case {
            FoldLower => ident.to_lowercase(),
            FoldUpper => ident.to_uppercase(),
            PreserveInsensitive | PreserveSensitive => ident.to_string(),
        }
    }

    /// Map an engine's own type name onto the neutral [`SqlType`].
    fn map_native_type(&self, native: &str) -> SqlType;

    /// Render a neutral column type as this engine's own declaration.
    ///
    /// The inverse of [`SqlDialect::map_native_type`], and the reason a
    /// `CREATE TABLE` written for one engine can be executed on another. Where
    /// this engine has no equivalent — no unsigned integers, no JSON type — the
    /// implementation picks the nearest type that holds the same values; the
    /// translation layer separately tells the operator that it did so.
    fn render_column_type(&self, column: &ColumnType) -> String;

    /// The column option that makes this engine generate the value, if it uses
    /// one.
    ///
    /// `None` means the engine expresses it some other way: PostgreSQL folds it
    /// into the type (`BIGSERIAL`), SQLite gets it for free on an
    /// `INTEGER PRIMARY KEY`.
    fn auto_increment_option(&self, _column: &ColumnType) -> Option<String> {
        None
    }

    /// Translate a Vituss error into what a client of this engine expects to see.
    fn native_error(&self, err: &Error) -> NativeError;

    /// `BEGIN` with the requested isolation level, if any.
    fn begin_sql(&self, isolation: Option<&str>) -> String {
        match isolation {
            Some(level) => format!("SET TRANSACTION ISOLATION LEVEL {level}; BEGIN"),
            None => "BEGIN".to_string(),
        }
    }

    fn savepoint_sql(&self, name: &str) -> String {
        format!("SAVEPOINT {}", self.quote_ident(name))
    }
    fn rollback_to_savepoint_sql(&self, name: &str) -> String {
        format!("ROLLBACK TO SAVEPOINT {}", self.quote_ident(name))
    }
    fn release_savepoint_sql(&self, name: &str) -> String {
        format!("RELEASE SAVEPOINT {}", self.quote_ident(name))
    }

    /// The distributed-transaction statements for a given transaction id, or
    /// `None` if this engine cannot do 2PC from SQL.
    fn two_pc_sql(&self, _xid: &str) -> Option<TwoPcSql> {
        None
    }

    /// Render a `LIMIT`/`OFFSET` clause. SQL Server needs `OFFSET … FETCH`, and
    /// requires an `ORDER BY` to go with it.
    fn limit_clause(&self, limit: Option<u64>, offset: Option<u64>) -> String {
        match (limit, offset) {
            (None, None) => String::new(),
            (Some(l), None) => format!(" LIMIT {l}"),
            (Some(l), Some(o)) => format!(" LIMIT {l} OFFSET {o}"),
            (None, Some(o)) => format!(" LIMIT 18446744073709551615 OFFSET {o}"),
        }
    }

    /// A `SELECT` that takes a row lock, for read-modify-write sequences.
    ///
    /// Spelled `FOR UPDATE` almost everywhere, but T-SQL has no such clause and
    /// uses table hints instead — a difference that would otherwise leak into
    /// every caller that needs to reserve a block of sequence values.
    fn select_for_update(&self, columns: &str, table: &str, where_clause: &str) -> String {
        format!(
            "SELECT {columns} FROM {} WHERE {where_clause} FOR UPDATE",
            self.quote_ident(table)
        )
    }

    /// Schemas that belong to the engine itself and are never sharded.
    fn system_schemas(&self) -> &'static [&'static str];

    /// True when `name` refers to one of this engine's own schemas.
    fn is_system_schema(&self, name: &str) -> bool {
        self.system_schemas().iter().any(|s| s.eq_ignore_ascii_case(name))
    }
}

impl std::fmt::Debug for dyn SqlDialect {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SqlDialect({})", self.name())
    }
}

/// A shareable handle to a dialect.
pub type DialectRef = Arc<dyn SqlDialect>;

static REGISTRY: Lazy<RwLock<HashMap<String, DialectRef>>> = Lazy::new(|| {
    let mut m: HashMap<String, DialectRef> = HashMap::new();
    for d in builtin_dialects() {
        for key in std::iter::once(d.name()).chain(d.aliases().iter().copied()) {
            m.insert(key.to_string(), d.clone());
        }
    }
    RwLock::new(m)
});

fn builtin_dialects() -> Vec<DialectRef> {
    vec![
        Arc::new(crate::mysql::MySql::new()),
        Arc::new(crate::postgres::Postgres::new()),
        Arc::new(crate::mssql::MsSql::new()),
        Arc::new(crate::sqlite::Sqlite::new()),
    ]
}

/// Register a dialect under its name and aliases, replacing any existing entry.
///
/// This is the extension point: a downstream crate can teach Vituss about
/// CockroachDB or Oracle without forking it.
pub fn register(dialect: DialectRef) {
    let mut reg = REGISTRY.write().expect("dialect registry poisoned");
    for key in std::iter::once(dialect.name()).chain(dialect.aliases().iter().copied()) {
        reg.insert(key.to_string(), dialect.clone());
    }
}

/// Look up a dialect by name or alias, case-insensitively.
pub fn get(name: &str) -> Result<DialectRef> {
    let reg = REGISTRY.read().expect("dialect registry poisoned");
    reg.get(&name.to_lowercase()).cloned().ok_or_else(|| {
        let mut known: Vec<&str> = reg.keys().map(String::as_str).collect();
        known.sort_unstable();
        Error::not_found(format!("unknown SQL dialect {name:?}; registered: {}", known.join(", ")))
    })
}

/// Every registered dialect name, sorted. Aliases included.
pub fn registered() -> Vec<String> {
    let reg = REGISTRY.read().expect("dialect registry poisoned");
    let mut v: Vec<String> = reg.keys().cloned().collect();
    v.sort();
    v
}

/// True when both engines can participate in the same 2PC transaction.
///
/// They cannot if either lacks SQL-level 2PC. Vituss does not attempt to bridge
/// XA and `PREPARE TRANSACTION` across engines in one transaction — the failure
/// modes are not recoverable without a real coordinator.
pub fn two_pc_compatible(a: &dyn SqlDialect, b: &dyn SqlDialect) -> bool {
    let (x, y) = (a.capabilities().two_pc, b.capabilities().two_pc);
    x == y && !matches!(x, TwoPcStyle::None | TwoPcStyle::ExternalCoordinator)
}
