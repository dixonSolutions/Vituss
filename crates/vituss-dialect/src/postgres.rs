//! PostgreSQL dialect.

use sqlparser::dialect::PostgreSqlDialect;

use vituss_core::{Code, Error, SqlType};

use crate::caps::{Capabilities, IdentifierCase, PlaceholderStyle, RowLock, TwoPcStyle};
use crate::ddl::{self, ColumnType};
use crate::dialect::{NativeError, SqlDialect, TwoPcSql};
use crate::introspect::Introspection;

pub struct Postgres {
    parser: PostgreSqlDialect,
    caps: Capabilities,
    introspection: Introspection,
}

impl Default for Postgres {
    fn default() -> Self {
        Self::new()
    }
}

impl Postgres {
    pub fn new() -> Self {
        Self {
            parser: PostgreSqlDialect {},
            caps: Capabilities {
                identifier_case: IdentifierCase::FoldLower,
                identifier_quote: '"',
                max_identifier_len: 63,
                placeholder_style: PlaceholderStyle::DollarNumbered,
                row_lock: RowLock::ForUpdate,
                two_pc: TwoPcStyle::PreparedTransaction,
                // RETURNING is how the Postgres backend reports generated keys,
                // since the protocol has no last-insert-id field.
                supports_returning: true,
                supports_last_insert_id: false,
                supports_limit_offset: true,
                supports_savepoints: true,
                supports_unsigned: false,
                supports_upsert: true,
                // The extended protocol carries one statement per message.
                supports_multi_statement: false,
                supports_change_capture: true,
                // CREATE DATABASE cannot run inside a transaction block.
                supports_create_database_in_tx: false,
                auto_increment_in_type: true,
                supports_advisory_locks: true,
            },
            introspection: Introspection {
                list_tables: "SELECT table_name, table_type \
                              FROM information_schema.tables \
                              WHERE table_schema = $1 \
                              ORDER BY table_name",
                list_columns: "SELECT table_name, column_name, ordinal_position, \
                                      COALESCE(domain_name, udt_name), \
                                      is_nullable = 'YES', column_default, \
                                      (is_identity = 'YES' OR column_default LIKE 'nextval(%') \
                               FROM information_schema.columns \
                               WHERE table_schema = $1 \
                               ORDER BY table_name, ordinal_position",
                // pg_index is used instead of information_schema because the
                // standard views do not expose uniqueness and primary-key flags
                // together with column order.
                list_indexes: "SELECT t.relname, i.relname, a.attname, \
                                      array_position(ix.indkey::int2[], a.attnum), \
                                      ix.indisunique, ix.indisprimary \
                               FROM pg_class t \
                               JOIN pg_namespace n ON n.oid = t.relnamespace \
                               JOIN pg_index ix ON t.oid = ix.indrelid \
                               JOIN pg_class i ON i.oid = ix.indexrelid \
                               JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey) \
                               WHERE n.nspname = $1 AND t.relkind = 'r' \
                               ORDER BY t.relname, i.relname, 4",
                list_foreign_keys: "SELECT k.table_name, k.constraint_name, k.column_name, \
                                           ccu.table_name, ccu.column_name \
                                    FROM information_schema.key_column_usage k \
                                    JOIN information_schema.table_constraints tc \
                                      ON tc.constraint_name = k.constraint_name \
                                     AND tc.table_schema = k.table_schema \
                                    JOIN information_schema.constraint_column_usage ccu \
                                      ON ccu.constraint_name = k.constraint_name \
                                     AND ccu.table_schema = k.table_schema \
                                    WHERE k.table_schema = $1 AND tc.constraint_type = 'FOREIGN KEY' \
                                    ORDER BY k.table_name, k.constraint_name, k.ordinal_position",
                current_schema: "SELECT current_schema()",
                ping: "SELECT 1",
                server_version: "SELECT version()",
                // WAL LSN on a primary. Comparable and monotonic, which is what
                // the change-capture stream needs to resume.
                replication_position: Some("SELECT pg_current_wal_lsn()::text"),
            },
        }
    }
}

impl SqlDialect for Postgres {
    fn name(&self) -> &'static str {
        "postgres"
    }
    fn aliases(&self) -> &'static [&'static str] {
        &["postgresql", "pgsql", "pg", "cockroach"]
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
        5432
    }

    fn map_native_type(&self, native: &str) -> SqlType {
        let n = native.to_ascii_lowercase();
        let base = n.split(['(', ' ']).next().unwrap_or("");
        match base.trim_start_matches('_') {
            "bool" | "boolean" => SqlType::Bool,
            "int2" | "smallint" | "smallserial" => SqlType::Int16,
            "int4" | "integer" | "int" | "serial" => SqlType::Int32,
            "int8" | "bigint" | "bigserial" => SqlType::Int64,
            "float4" | "real" => SqlType::Float32,
            "float8" | "double" => SqlType::Float64,
            "numeric" | "decimal" | "money" => SqlType::Decimal,
            "bpchar" | "char" | "character" => SqlType::Char,
            "varchar" => SqlType::VarChar,
            "text" | "citext" | "name" => SqlType::Text,
            "bytea" => SqlType::Blob,
            "date" => SqlType::Date,
            "time" | "timetz" => SqlType::Time,
            "timestamp" => SqlType::DateTime,
            "timestamptz" => SqlType::Timestamp,
            "json" | "jsonb" => SqlType::Json,
            "uuid" => SqlType::Uuid,
            _ => SqlType::Unknown,
        }
    }


    fn render_column_type(&self, c: &ColumnType) -> String {
        // A generated key is part of the type here, not a column option.
        if c.auto_increment {
            return match c.base {
                SqlType::Int8 | SqlType::Int16 => "SMALLSERIAL".to_string(),
                SqlType::Int32 => "SERIAL".to_string(),
                _ => "BIGSERIAL".to_string(),
            };
        }
        match c.base {
            SqlType::Bool => "BOOLEAN".to_string(),
            SqlType::Int8 | SqlType::Int16 => "SMALLINT".to_string(),
            // An unsigned 32-bit value does not fit in INTEGER, so it is widened
            // rather than silently truncated at the top of its range.
            SqlType::Int32 if c.unsigned => "BIGINT".to_string(),
            SqlType::Int32 => "INTEGER".to_string(),
            SqlType::Int64 if c.unsigned => "NUMERIC(20)".to_string(),
            SqlType::Int64 => "BIGINT".to_string(),
            // u64's range exceeds BIGINT; NUMERIC(20) is the smallest exact type
            // that holds all of it.
            SqlType::Uint64 => "NUMERIC(20)".to_string(),
            SqlType::Float32 => "REAL".to_string(),
            SqlType::Float64 => "DOUBLE PRECISION".to_string(),
            SqlType::Decimal => ddl::decimal(c, "NUMERIC"),
            SqlType::Char => ddl::sized(c, "CHAR", 1),
            SqlType::VarChar => ddl::sized_or(c, "VARCHAR", "TEXT"),
            SqlType::Text => "TEXT".to_string(),
            // One binary type, whatever the source called it.
            SqlType::Binary | SqlType::VarBinary | SqlType::Blob => "BYTEA".to_string(),
            SqlType::Date => "DATE".to_string(),
            SqlType::Time => "TIME".to_string(),
            SqlType::DateTime => "TIMESTAMP".to_string(),
            SqlType::Timestamp => "TIMESTAMPTZ".to_string(),
            SqlType::Json => "JSONB".to_string(),
            SqlType::Uuid => "UUID".to_string(),
            SqlType::Null | SqlType::Unknown => c.source_text.clone(),
        }
    }

    fn native_error(&self, err: &Error) -> NativeError {
        if let Some(state) = &err.sql_state {
            return NativeError { code: err.native_code.unwrap_or(0), sql_state: state.clone() };
        }
        // PostgreSQL clients switch on SQLSTATE, not on a numeric code.
        let state = match err.code {
            Code::InvalidArgument => "42601",       // syntax_error
            Code::NotFound => "42P01",              // undefined_table
            Code::AlreadyExists => "23505",         // unique_violation
            Code::PermissionDenied => "42501",      // insufficient_privilege
            Code::Unauthenticated => "28000",       // invalid_authorization_specification
            Code::ResourceExhausted => "53300",     // too_many_connections
            Code::DeadlineExceeded | Code::Canceled => "57014", // query_canceled
            Code::Aborted => "40001",               // serialization_failure
            Code::Unavailable => "08006",           // connection_failure
            Code::Unimplemented | Code::Unsupported => "0A000", // feature_not_supported
            Code::FailedPrecondition => "55000",    // object_not_in_prerequisite_state
            _ => "XX000",                           // internal_error
        };
        NativeError { code: 0, sql_state: state.to_string() }
    }

    fn two_pc_sql(&self, xid: &str) -> Option<TwoPcSql> {
        let x = format!("'{}'", xid.replace('\'', "''"));
        Some(TwoPcSql {
            // PostgreSQL promotes the *current* transaction; there is no
            // separate "start the distributed transaction" statement.
            start: "BEGIN".to_string(),
            end: None,
            prepare: format!("PREPARE TRANSACTION {x}"),
            commit: format!("COMMIT PREPARED {x}"),
            rollback: format!("ROLLBACK PREPARED {x}"),
            recover: "SELECT gid FROM pg_prepared_xacts".to_string(),
        })
    }

    fn system_schemas(&self) -> &'static [&'static str] {
        &["information_schema", "pg_catalog", "pg_toast", "_vt"]
    }
}
