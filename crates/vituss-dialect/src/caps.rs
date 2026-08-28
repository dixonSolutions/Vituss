//! What a backend engine can and cannot do.
//!
//! The planner consults these instead of branching on the engine name, so adding
//! a fourth engine is a matter of describing it rather than editing the planner.

/// How an engine treats an unquoted identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentifierCase {
    /// Stored as written, compared case-insensitively (MySQL on Windows/macOS,
    /// SQL Server with a CI collation).
    PreserveInsensitive,
    /// Stored as written, compared case-sensitively (MySQL on Linux).
    PreserveSensitive,
    /// Folded to lower case (PostgreSQL).
    FoldLower,
    /// Folded to upper case (Oracle-style).
    FoldUpper,
}

/// How the engine implements a distributed (two-phase) commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TwoPcStyle {
    /// `XA START/END/PREPARE/COMMIT` — MySQL, MariaDB.
    Xa,
    /// `PREPARE TRANSACTION` / `COMMIT PREPARED` — PostgreSQL.
    PreparedTransaction,
    /// Only via an external transaction coordinator (MS DTC). Vituss cannot drive
    /// it from SQL, so 2PC is refused for keyspaces on such engines.
    ExternalCoordinator,
    None,
}

/// How the engine expresses "lock these rows for update".
///
/// A read-modify-write — reserving a block of sequence values, reading the rows a
/// DML is about to change — has to hold a lock across the two statements. The
/// three engines spell that three different ways, and one of them cannot express
/// it in the statement at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowLock {
    /// `SELECT … FOR UPDATE` — MySQL, PostgreSQL.
    ForUpdate,
    /// A table hint: `FROM t WITH (UPDLOCK, HOLDLOCK)` — SQL Server.
    TableHint,
    /// No statement-level row locking. SQLite locks the whole database for the
    /// duration of a write transaction, so an explicit clause is both
    /// unsupported and unnecessary.
    None,
}

/// The placeholder syntax the engine's protocol expects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlaceholderStyle {
    /// `?` — MySQL, SQL Server (ODBC-style).
    Question,
    /// `$1`, `$2` … — PostgreSQL.
    DollarNumbered,
    /// `@p1`, `@p2` … — SQL Server (TDS named parameters).
    AtNumbered,
}

/// A static description of one engine's abilities.
#[derive(Debug, Clone)]
pub struct Capabilities {
    pub identifier_case: IdentifierCase,
    pub identifier_quote: char,
    pub max_identifier_len: usize,
    pub placeholder_style: PlaceholderStyle,
    pub two_pc: TwoPcStyle,
    pub row_lock: RowLock,

    /// `INSERT ... RETURNING` / `OUTPUT INSERTED.*`. When absent, the gate must
    /// find generated keys another way.
    pub supports_returning: bool,
    /// A protocol-level "last generated auto-increment" field.
    pub supports_last_insert_id: bool,
    /// `LIMIT n OFFSET m`. When false the dialect must emit `OFFSET … FETCH NEXT`.
    pub supports_limit_offset: bool,
    /// `SAVEPOINT` / `ROLLBACK TO SAVEPOINT`.
    pub supports_savepoints: bool,
    /// Native unsigned integer types. PostgreSQL and SQL Server have none, so
    /// a `UInt` sharding key must be widened when stored there.
    pub supports_unsigned: bool,
    /// `INSERT ... ON DUPLICATE KEY UPDATE` / `ON CONFLICT DO UPDATE` / `MERGE`.
    pub supports_upsert: bool,
    /// Multiple statements in a single round trip.
    pub supports_multi_statement: bool,
    /// Row-level change capture Vituss can tail for VReplication: MySQL binlog,
    /// PostgreSQL logical replication, SQL Server CDC.
    pub supports_change_capture: bool,
    /// Whether `CREATE DATABASE` inside a session is possible (PostgreSQL forbids
    /// it in a transaction; SQL Server needs a separate connection).
    pub supports_create_database_in_tx: bool,
    /// Transaction-scoped advisory locks, used to serialise lookup-vindex writes.
    pub supports_advisory_locks: bool,
}

impl Capabilities {
    /// True when Vituss can run a real 2PC transaction across shards on this engine.
    pub fn can_two_pc(&self) -> bool {
        matches!(self.two_pc, TwoPcStyle::Xa | TwoPcStyle::PreparedTransaction)
    }
}
