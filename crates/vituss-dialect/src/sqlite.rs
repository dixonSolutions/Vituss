//! SQLite dialect.
//!
//! SQLite is here for two reasons. It is a genuinely useful shard backend for
//! small, embedded or edge deployments — and, more importantly for the codebase,
//! it is proof that the plug-in layer works: it was added without touching the
//! planner, the gate or the tablet, and it exercises the awkward cases (no
//! distributed transactions, no unsigned integers, no schema concept) that a
//! MySQL-shaped abstraction would have hidden.
//!
//! It is also what lets the end-to-end tests run a real SQL engine per shard
//! without starting a server.

use sqlparser::dialect::SQLiteDialect;

use vituss_core::{Code, Error, SqlType};

use crate::caps::{Capabilities, IdentifierCase, PlaceholderStyle, RowLock, TwoPcStyle};
use crate::ddl::ColumnType;
use crate::dialect::{NativeError, SqlDialect};
use crate::introspect::Introspection;

pub struct Sqlite {
    parser: SQLiteDialect,
    caps: Capabilities,
    introspection: Introspection,
}

impl Default for Sqlite {
    fn default() -> Self {
        Self::new()
    }
}

impl Sqlite {
    pub fn new() -> Self {
        Self {
            parser: SQLiteDialect {},
            caps: Capabilities {
                identifier_case: IdentifierCase::PreserveInsensitive,
                identifier_quote: '"',
                max_identifier_len: 1_000_000,
                placeholder_style: PlaceholderStyle::Question,
                // No distributed transactions of any kind.
                row_lock: RowLock::None,
                two_pc: TwoPcStyle::None,
                supports_returning: true,
                supports_last_insert_id: true,
                supports_limit_offset: true,
                supports_savepoints: true,
                supports_unsigned: false,
                supports_upsert: true,
                supports_multi_statement: false,
                supports_change_capture: false,
                supports_create_database_in_tx: false,
                auto_increment_in_type: false,
                supports_advisory_locks: false,
            },
            // SQLite has no schema catalog, so each query carries a trivially
            // true use of the schema parameter to keep the one-parameter contract
            // the tablet relies on.
            introspection: Introspection {
                list_tables: "SELECT name, CASE type WHEN 'table' THEN 'BASE TABLE' ELSE 'VIEW' END \
                              FROM sqlite_master \
                              WHERE type IN ('table','view') AND name NOT LIKE 'sqlite_%' AND ?1 IS NOT NULL \
                              ORDER BY name",
                list_columns: "SELECT m.name, p.name, p.cid + 1, p.type, \
                                      CASE p.\"notnull\" WHEN 0 THEN 1 ELSE 0 END, p.dflt_value, \
                                      CASE WHEN p.pk = 1 AND UPPER(p.type) = 'INTEGER' THEN 1 ELSE 0 END \
                               FROM sqlite_master m JOIN pragma_table_info(m.name) p \
                               WHERE m.type = 'table' AND m.name NOT LIKE 'sqlite_%' AND ?1 IS NOT NULL \
                               ORDER BY m.name, p.cid",
                list_indexes: "SELECT m.name, il.name, ii.name, ii.seqno + 1, il.\"unique\", \
                                      CASE il.origin WHEN 'pk' THEN 1 ELSE 0 END \
                               FROM sqlite_master m \
                               JOIN pragma_index_list(m.name) il \
                               JOIN pragma_index_info(il.name) ii \
                               WHERE m.type = 'table' AND ?1 IS NOT NULL \
                               ORDER BY m.name, il.name, ii.seqno",
                list_foreign_keys: "SELECT m.name, 'fk_' || fk.id, fk.\"from\", fk.\"table\", fk.\"to\" \
                                    FROM sqlite_master m JOIN pragma_foreign_key_list(m.name) fk \
                                    WHERE m.type = 'table' AND ?1 IS NOT NULL \
                                    ORDER BY m.name, fk.id, fk.seq",
                current_schema: "SELECT 'main'",
                ping: "SELECT 1",
                server_version: "SELECT sqlite_version()",
                replication_position: None,
            },
        }
    }
}

impl SqlDialect for Sqlite {
    fn name(&self) -> &'static str {
        "sqlite"
    }
    fn aliases(&self) -> &'static [&'static str] {
        &["sqlite3", "libsql"]
    }
    fn parser(&self) -> &dyn sqlparser::dialect::Dialect {
        &self.parser
    }
    fn capabilities(&self) -> &Capabilities {
        &self.caps
    }
    fn introspection(&self) -> &Introspection {
        &self.introspection
    }
    fn default_port(&self) -> u16 {
        // File-backed; there is no port. Reported as 0 so a config that tries to
        // dial one fails loudly.
        0
    }

    fn map_native_type(&self, native: &str) -> SqlType {
        // SQLite has storage classes, not types: the declared type is advisory
        // and matched by substring, exactly as the engine's own affinity rules do.
        let n = native.to_ascii_uppercase();
        if n.contains("INT") {
            SqlType::Int64
        } else if n.contains("CHAR") || n.contains("CLOB") || n.contains("TEXT") {
            SqlType::Text
        } else if n.contains("BLOB") || n.is_empty() {
            SqlType::Blob
        } else if n.contains("REAL") || n.contains("FLOA") || n.contains("DOUB") {
            SqlType::Float64
        } else if n.contains("BOOL") {
            SqlType::Bool
        } else if n.contains("DATE") || n.contains("TIME") {
            SqlType::DateTime
        } else {
            SqlType::Decimal
        }
    }


    fn render_column_type(&self, c: &ColumnType) -> String {
        // SQLite has five storage classes and uses the declared type only to pick
        // an affinity, so the mapping collapses hard. The names chosen here are
        // the ones whose affinity matches how the driver decodes the values back.
        match c.base {
            SqlType::Bool | SqlType::Int8 | SqlType::Int16 | SqlType::Int32 | SqlType::Int64
            | SqlType::Uint64 => "INTEGER".to_string(),
            SqlType::Float32 | SqlType::Float64 => "REAL".to_string(),
            SqlType::Decimal => "NUMERIC".to_string(),
            SqlType::Char | SqlType::VarChar | SqlType::Text | SqlType::Json | SqlType::Uuid => {
                "TEXT".to_string()
            }
            SqlType::Binary | SqlType::VarBinary | SqlType::Blob => "BLOB".to_string(),
            // These carry no numeric affinity, so ISO-8601 text survives intact
            // while the column still reads back as temporal metadata.
            SqlType::Date => "DATE".to_string(),
            SqlType::Time => "TIME".to_string(),
            SqlType::DateTime | SqlType::Timestamp => "DATETIME".to_string(),
            SqlType::Null | SqlType::Unknown => c.source_text.clone(),
        }
    }

    // No option: an INTEGER PRIMARY KEY is already an alias for the rowid and
    // assigns itself. SQLite's `AUTOINCREMENT` keyword only suppresses id reuse,
    // and it is a syntax error anywhere but on such a column.

    fn native_error(&self, err: &Error) -> NativeError {
        if let Some(code) = err.native_code {
            return NativeError { code, sql_state: err.sql_state.clone().unwrap_or_else(|| "HY000".into()) };
        }
        // SQLite result codes.
        let code = match err.code {
            Code::InvalidArgument => 1,       // SQLITE_ERROR
            Code::PermissionDenied => 3,      // SQLITE_PERM
            Code::Aborted => 4,               // SQLITE_ABORT
            Code::ResourceExhausted => 5,     // SQLITE_BUSY
            Code::NotFound => 12,             // SQLITE_NOTFOUND
            Code::AlreadyExists => 19,        // SQLITE_CONSTRAINT
            Code::DeadlineExceeded => 9,      // SQLITE_INTERRUPT
            Code::Unimplemented | Code::Unsupported => 23, // SQLITE_AUTH-adjacent misuse
            _ => 1,
        };
        NativeError { code, sql_state: "HY000".to_string() }
    }

    fn begin_sql(&self, _isolation: Option<&str>) -> String {
        // SQLite has a single isolation level; asking for another is a no-op
        // rather than an error, matching what the engine itself does.
        "BEGIN".to_string()
    }

    fn select_for_update(&self, columns: &str, table: &str, where_clause: &str) -> String {
        // No row-level locking clause exists. A write transaction already holds
        // the whole database, so the read is protected by being inside one —
        // emitting `FOR UPDATE` would simply be a syntax error.
        format!(
            "SELECT {columns} FROM {} WHERE {where_clause}",
            self.quote_ident(table)
        )
    }

    fn system_schemas(&self) -> &'static [&'static str] {
        &["sqlite_master", "sqlite_temp_master", "_vt"]
    }
}
