//! MySQL / MariaDB dialect.

use sqlparser::dialect::MySqlDialect;

use vituss_core::{Code, Error, SqlType};

use crate::caps::{Capabilities, IdentifierCase, PlaceholderStyle, RowLock, TwoPcStyle};
use crate::ddl::{self, ColumnType};
use crate::dialect::{NativeError, SqlDialect, TwoPcSql};
use crate::introspect::Introspection;

pub struct MySql {
    parser: MySqlDialect,
    caps: Capabilities,
    introspection: Introspection,
}

impl Default for MySql {
    fn default() -> Self {
        Self::new()
    }
}

impl MySql {
    pub fn new() -> Self {
        Self {
            parser: MySqlDialect {},
            caps: Capabilities {
                identifier_case: IdentifierCase::PreserveSensitive,
                identifier_quote: '`',
                max_identifier_len: 64,
                placeholder_style: PlaceholderStyle::Question,
                row_lock: RowLock::ForUpdate,
                two_pc: TwoPcStyle::Xa,
                supports_returning: false,
                supports_last_insert_id: true,
                supports_limit_offset: true,
                supports_savepoints: true,
                supports_unsigned: true,
                supports_upsert: true,
                supports_multi_statement: true,
                supports_change_capture: true,
                supports_create_database_in_tx: true,
                auto_increment_in_type: false,
                supports_advisory_locks: true,
            },
            introspection: Introspection {
                list_tables: "SELECT table_name, table_type \
                              FROM information_schema.tables \
                              WHERE table_schema = ? \
                              ORDER BY table_name",
                list_columns: "SELECT table_name, column_name, ordinal_position, column_type, \
                                      is_nullable = 'YES', column_default, \
                                      extra LIKE '%auto_increment%' \
                               FROM information_schema.columns \
                               WHERE table_schema = ? \
                               ORDER BY table_name, ordinal_position",
                list_indexes: "SELECT table_name, index_name, column_name, seq_in_index, \
                                      non_unique = 0, index_name = 'PRIMARY' \
                               FROM information_schema.statistics \
                               WHERE table_schema = ? \
                               ORDER BY table_name, index_name, seq_in_index",
                list_foreign_keys: "SELECT k.table_name, k.constraint_name, k.column_name, \
                                           k.referenced_table_name, k.referenced_column_name \
                                    FROM information_schema.key_column_usage k \
                                    WHERE k.table_schema = ? AND k.referenced_table_name IS NOT NULL \
                                    ORDER BY k.table_name, k.constraint_name, k.ordinal_position",
                current_schema: "SELECT DATABASE()",
                ping: "SELECT 1",
                server_version: "SELECT VERSION()",
                // GTID set: globally comparable, which is what VReplication needs
                // to resume a stream after a primary failover.
                replication_position: Some("SELECT @@global.gtid_executed"),
            },
        }
    }
}

impl SqlDialect for MySql {
    fn name(&self) -> &'static str {
        "mysql"
    }
    fn aliases(&self) -> &'static [&'static str] {
        &["mariadb", "percona"]
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
        3306
    }

    fn map_native_type(&self, native: &str) -> SqlType {
        let n = native.to_ascii_lowercase();
        let base = n.split(['(', ' ']).next().unwrap_or("").to_string();
        let unsigned = n.contains("unsigned");
        match base.as_str() {
            "tinyint" if n.starts_with("tinyint(1)") => SqlType::Bool,
            "bool" | "boolean" => SqlType::Bool,
            "tinyint" => SqlType::Int8,
            "smallint" => SqlType::Int16,
            "mediumint" | "int" | "integer" => SqlType::Int32,
            "bigint" => {
                if unsigned {
                    SqlType::Uint64
                } else {
                    SqlType::Int64
                }
            }
            "float" => SqlType::Float32,
            "double" | "real" => SqlType::Float64,
            "decimal" | "numeric" => SqlType::Decimal,
            "char" => SqlType::Char,
            "varchar" => SqlType::VarChar,
            "tinytext" | "text" | "mediumtext" | "longtext" | "enum" | "set" => SqlType::Text,
            "binary" => SqlType::Binary,
            "varbinary" => SqlType::VarBinary,
            "tinyblob" | "blob" | "mediumblob" | "longblob" => SqlType::Blob,
            "date" | "year" => SqlType::Date,
            "time" => SqlType::Time,
            "datetime" => SqlType::DateTime,
            "timestamp" => SqlType::Timestamp,
            "json" => SqlType::Json,
            _ => SqlType::Unknown,
        }
    }


    fn render_column_type(&self, c: &ColumnType) -> String {
        let u = if c.unsigned { " UNSIGNED" } else { "" };
        match c.base {
            // MySQL has no boolean; TINYINT(1) is what every client library reads
            // back as one.
            SqlType::Bool => "TINYINT(1)".to_string(),
            SqlType::Int8 => format!("TINYINT{u}"),
            SqlType::Int16 => format!("SMALLINT{u}"),
            SqlType::Int32 => format!("INT{u}"),
            SqlType::Int64 => format!("BIGINT{u}"),
            SqlType::Uint64 => "BIGINT UNSIGNED".to_string(),
            SqlType::Float32 => "FLOAT".to_string(),
            SqlType::Float64 => "DOUBLE".to_string(),
            SqlType::Decimal => ddl::decimal(c, "DECIMAL"),
            SqlType::Char => ddl::sized(c, "CHAR", 1),
            SqlType::VarChar => ddl::sized(c, "VARCHAR", 255),
            SqlType::Text => "TEXT".to_string(),
            SqlType::Binary => ddl::sized(c, "BINARY", 1),
            SqlType::VarBinary => ddl::sized(c, "VARBINARY", 255),
            SqlType::Blob => "BLOB".to_string(),
            SqlType::Date => "DATE".to_string(),
            SqlType::Time => "TIME".to_string(),
            SqlType::DateTime => "DATETIME".to_string(),
            SqlType::Timestamp => "TIMESTAMP".to_string(),
            SqlType::Json => "JSON".to_string(),
            // No UUID type. CHAR(36) holds the canonical text form, which is what
            // applications compare and index on.
            SqlType::Uuid => "CHAR(36)".to_string(),
            SqlType::Null | SqlType::Unknown => c.source_text.clone(),
        }
    }

    fn auto_increment_option(&self, _column: &ColumnType) -> Option<String> {
        Some("AUTO_INCREMENT".to_string())
    }

    fn native_error(&self, err: &Error) -> NativeError {
        // Preserve the shard's own error number when we are forwarding one, so
        // application code that switches on e.g. 1062 (duplicate key) keeps working.
        if let Some(code) = err.native_code {
            return NativeError {
                code,
                sql_state: err.sql_state.clone().unwrap_or_else(|| "HY000".into()),
            };
        }
        let (code, state) = match err.code {
            Code::InvalidArgument => (1064, "42000"),   // ER_PARSE_ERROR
            Code::NotFound => (1146, "42S02"),          // ER_NO_SUCH_TABLE
            Code::AlreadyExists => (1062, "23000"),     // ER_DUP_ENTRY
            Code::PermissionDenied => (1045, "28000"),  // ER_ACCESS_DENIED_ERROR
            Code::Unauthenticated => (1045, "28000"),
            Code::ResourceExhausted => (1040, "08004"), // ER_CON_COUNT_ERROR
            Code::DeadlineExceeded => (1317, "70100"),  // ER_QUERY_INTERRUPTED
            Code::Canceled => (1317, "70100"),
            Code::Aborted => (1213, "40001"),           // ER_LOCK_DEADLOCK
            Code::Unavailable => (2003, "HY000"),       // CR_CONN_HOST_ERROR
            Code::Unimplemented | Code::Unsupported => (1235, "42000"), // ER_NOT_SUPPORTED_YET
            _ => (1105, "HY000"),                       // ER_UNKNOWN_ERROR
        };
        NativeError { code, sql_state: state.to_string() }
    }

    fn begin_sql(&self, isolation: Option<&str>) -> String {
        match isolation {
            Some(level) => format!("SET TRANSACTION ISOLATION LEVEL {level}"),
            None => "BEGIN".to_string(),
        }
    }

    fn two_pc_sql(&self, xid: &str) -> Option<TwoPcSql> {
        let x = format!("'{}'", xid.replace('\'', "''"));
        Some(TwoPcSql {
            start: format!("XA START {x}"),
            end: Some(format!("XA END {x}")),
            prepare: format!("XA PREPARE {x}"),
            commit: format!("XA COMMIT {x}"),
            rollback: format!("XA ROLLBACK {x}"),
            recover: "XA RECOVER".to_string(),
        })
    }

    fn system_schemas(&self) -> &'static [&'static str] {
        &["information_schema", "performance_schema", "mysql", "sys", "_vt"]
    }
}
