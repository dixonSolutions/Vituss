//! Microsoft SQL Server dialect.

use sqlparser::dialect::MsSqlDialect;

use vituss_core::{Code, Error, SqlType};

use crate::caps::{Capabilities, IdentifierCase, PlaceholderStyle, RowLock, TwoPcStyle};
use crate::ddl::{self, ColumnType};
use crate::dialect::{NativeError, SqlDialect};
use crate::introspect::Introspection;

pub struct MsSql {
    parser: MsSqlDialect,
    caps: Capabilities,
    introspection: Introspection,
}

impl Default for MsSql {
    fn default() -> Self {
        Self::new()
    }
}

impl MsSql {
    pub fn new() -> Self {
        Self {
            parser: MsSqlDialect {},
            caps: Capabilities {
                // Case behaviour actually follows the database collation; the
                // default installs are case-insensitive, so that is what we assume.
                identifier_case: IdentifierCase::PreserveInsensitive,
                identifier_quote: '[',
                max_identifier_len: 128,
                placeholder_style: PlaceholderStyle::AtNumbered,
                // Distributed transactions exist, but only under MS DTC — Vituss
                // cannot drive them from SQL, so 2PC keyspaces are refused here.
                row_lock: RowLock::TableHint,
                two_pc: TwoPcStyle::ExternalCoordinator,
                // OUTPUT INSERTED.* fills the same role as RETURNING.
                supports_returning: true,
                supports_last_insert_id: true,
                // No LIMIT: OFFSET … FETCH NEXT only, and it needs an ORDER BY.
                supports_limit_offset: false,
                supports_savepoints: true,
                supports_unsigned: false,
                supports_upsert: true,
                supports_multi_statement: true,
                // Change Data Capture / Change Tracking.
                supports_change_capture: true,
                supports_create_database_in_tx: false,
                auto_increment_in_type: false,
                supports_advisory_locks: true,
            },
            introspection: Introspection {
                list_tables: "SELECT table_name, table_type \
                              FROM information_schema.tables \
                              WHERE table_schema = @p1 \
                              ORDER BY table_name",
                list_columns: "SELECT c.table_name, c.column_name, c.ordinal_position, c.data_type, \
                                      CAST(CASE WHEN c.is_nullable = 'YES' THEN 1 ELSE 0 END AS bit), \
                                      c.column_default, \
                                      CAST(COLUMNPROPERTY(OBJECT_ID(QUOTENAME(c.table_schema) + '.' + \
                                           QUOTENAME(c.table_name)), c.column_name, 'IsIdentity') AS bit) \
                               FROM information_schema.columns c \
                               WHERE c.table_schema = @p1 \
                               ORDER BY c.table_name, c.ordinal_position",
                list_indexes: "SELECT t.name, i.name, c.name, ic.key_ordinal, i.is_unique, i.is_primary_key \
                               FROM sys.indexes i \
                               JOIN sys.tables t ON t.object_id = i.object_id \
                               JOIN sys.schemas s ON s.schema_id = t.schema_id \
                               JOIN sys.index_columns ic ON ic.object_id = i.object_id \
                                                        AND ic.index_id = i.index_id \
                               JOIN sys.columns c ON c.object_id = ic.object_id \
                                                 AND c.column_id = ic.column_id \
                               WHERE s.name = @p1 AND ic.is_included_column = 0 \
                               ORDER BY t.name, i.name, ic.key_ordinal",
                list_foreign_keys: "SELECT tp.name, fk.name, cp.name, tr.name, cr.name \
                                    FROM sys.foreign_keys fk \
                                    JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id \
                                    JOIN sys.tables tp ON tp.object_id = fkc.parent_object_id \
                                    JOIN sys.schemas s ON s.schema_id = tp.schema_id \
                                    JOIN sys.columns cp ON cp.object_id = fkc.parent_object_id \
                                                       AND cp.column_id = fkc.parent_column_id \
                                    JOIN sys.tables tr ON tr.object_id = fkc.referenced_object_id \
                                    JOIN sys.columns cr ON cr.object_id = fkc.referenced_object_id \
                                                       AND cr.column_id = fkc.referenced_column_id \
                                    WHERE s.name = @p1 \
                                    ORDER BY tp.name, fk.name, fkc.constraint_column_id",
                current_schema: "SELECT SCHEMA_NAME()",
                ping: "SELECT 1",
                server_version: "SELECT @@VERSION",
                // CDC max LSN. Returns NULL when CDC is not enabled on the database.
                replication_position: Some("SELECT CONVERT(varchar(42), sys.fn_cdc_get_max_lsn(), 1)"),
            },
        }
    }
}

impl SqlDialect for MsSql {
    fn name(&self) -> &'static str {
        "mssql"
    }
    fn aliases(&self) -> &'static [&'static str] {
        &["sqlserver", "tds", "azuresql"]
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
        1433
    }

    fn map_native_type(&self, native: &str) -> SqlType {
        let n = native.to_ascii_lowercase();
        let base = n.split(['(', ' ']).next().unwrap_or("");
        match base {
            "bit" => SqlType::Bool,
            "tinyint" => SqlType::Int8,
            "smallint" => SqlType::Int16,
            "int" => SqlType::Int32,
            "bigint" => SqlType::Int64,
            "real" => SqlType::Float32,
            "float" => SqlType::Float64,
            "decimal" | "numeric" | "money" | "smallmoney" => SqlType::Decimal,
            "char" | "nchar" => SqlType::Char,
            "varchar" | "nvarchar" => SqlType::VarChar,
            "text" | "ntext" | "xml" => SqlType::Text,
            "binary" => SqlType::Binary,
            "varbinary" | "image" | "timestamp" | "rowversion" => SqlType::VarBinary,
            "date" => SqlType::Date,
            "time" => SqlType::Time,
            "datetime" | "datetime2" | "smalldatetime" => SqlType::DateTime,
            "datetimeoffset" => SqlType::Timestamp,
            "uniqueidentifier" => SqlType::Uuid,
            _ => SqlType::Unknown,
        }
    }


    fn render_column_type(&self, c: &ColumnType) -> String {
        match c.base {
            SqlType::Bool => "BIT".to_string(),
            // TINYINT is unsigned 0-255 here, so a signed source needs SMALLINT.
            SqlType::Int8 if c.unsigned => "TINYINT".to_string(),
            SqlType::Int8 | SqlType::Int16 => "SMALLINT".to_string(),
            SqlType::Int32 if c.unsigned => "BIGINT".to_string(),
            SqlType::Int32 => "INT".to_string(),
            SqlType::Int64 if c.unsigned => "DECIMAL(20, 0)".to_string(),
            SqlType::Int64 => "BIGINT".to_string(),
            SqlType::Uint64 => "DECIMAL(20, 0)".to_string(),
            SqlType::Float32 => "REAL".to_string(),
            SqlType::Float64 => "FLOAT".to_string(),
            SqlType::Decimal => ddl::decimal(c, "DECIMAL"),
            // N-prefixed types store UTF-16, which is what makes a column able to
            // hold what a utf8mb4 column held.
            SqlType::Char => ddl::sized(c, "NCHAR", 1),
            SqlType::VarChar => ddl::sized_or(c, "NVARCHAR", "NVARCHAR(MAX)"),
            SqlType::Text => "NVARCHAR(MAX)".to_string(),
            SqlType::Binary => ddl::sized(c, "BINARY", 1),
            SqlType::VarBinary => ddl::sized_or(c, "VARBINARY", "VARBINARY(MAX)"),
            SqlType::Blob => "VARBINARY(MAX)".to_string(),
            SqlType::Date => "DATE".to_string(),
            SqlType::Time => "TIME".to_string(),
            SqlType::DateTime => "DATETIME2".to_string(),
            SqlType::Timestamp => "DATETIMEOFFSET".to_string(),
            // No JSON type before SQL Server 2025; NVARCHAR(MAX) is what the
            // JSON functions operate on anyway.
            SqlType::Json => "NVARCHAR(MAX)".to_string(),
            SqlType::Uuid => "UNIQUEIDENTIFIER".to_string(),
            SqlType::Null | SqlType::Unknown => c.source_text.clone(),
        }
    }

    fn auto_increment_option(&self, _column: &ColumnType) -> Option<String> {
        Some("IDENTITY(1,1)".to_string())
    }

    fn native_error(&self, err: &Error) -> NativeError {
        if let Some(code) = err.native_code {
            return NativeError {
                code,
                sql_state: err.sql_state.clone().unwrap_or_else(|| "42000".into()),
            };
        }
        // SQL Server error numbers below 50000 are reserved for the engine, so
        // errors Vituss originates use the 50000 user range.
        let (code, state) = match err.code {
            Code::InvalidArgument => (102, "42000"),      // Incorrect syntax
            Code::NotFound => (208, "42S02"),             // Invalid object name
            Code::AlreadyExists => (2627, "23000"),       // Violation of PRIMARY KEY
            Code::PermissionDenied => (229, "42000"),     // Permission denied
            Code::Unauthenticated => (18456, "28000"),    // Login failed
            Code::ResourceExhausted => (10928, "53000"),
            Code::DeadlineExceeded | Code::Canceled => (1222, "HY008"), // Lock request timeout
            Code::Aborted => (1205, "40001"),             // Deadlock victim
            Code::Unavailable => (10054, "08S01"),        // Transport-level error
            Code::Unimplemented | Code::Unsupported => (40515, "0A000"),
            _ => (50000, "42000"),
        };
        NativeError { code, sql_state: state.to_string() }
    }

    fn begin_sql(&self, isolation: Option<&str>) -> String {
        match isolation {
            Some(level) => format!("SET TRANSACTION ISOLATION LEVEL {level}; BEGIN TRANSACTION"),
            None => "BEGIN TRANSACTION".to_string(),
        }
    }

    fn rollback_to_savepoint_sql(&self, name: &str) -> String {
        // T-SQL spells it ROLLBACK TRANSACTION, and has no RELEASE at all.
        format!("ROLLBACK TRANSACTION {}", self.quote_ident(name))
    }

    fn release_savepoint_sql(&self, _name: &str) -> String {
        // No-op: T-SQL savepoints are released implicitly at commit.
        String::new()
    }

    fn savepoint_sql(&self, name: &str) -> String {
        format!("SAVE TRANSACTION {}", self.quote_ident(name))
    }

    fn limit_clause(&self, limit: Option<u64>, offset: Option<u64>) -> String {
        match (limit, offset) {
            (None, None) => String::new(),
            (Some(l), off) => format!(" OFFSET {} ROWS FETCH NEXT {l} ROWS ONLY", off.unwrap_or(0)),
            (None, Some(o)) => format!(" OFFSET {o} ROWS"),
        }
    }

    fn select_for_update(&self, columns: &str, table: &str, where_clause: &str) -> String {
        // UPDLOCK takes the write lock now; HOLDLOCK keeps it until the
        // transaction ends, which together are what `FOR UPDATE` means elsewhere.
        format!(
            "SELECT {columns} FROM {} WITH (UPDLOCK, HOLDLOCK) WHERE {where_clause}",
            self.quote_ident(table)
        )
    }

    fn system_schemas(&self) -> &'static [&'static str] {
        &["information_schema", "sys", "master", "msdb", "tempdb", "model", "_vt"]
    }
}
