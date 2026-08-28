//! SQLite driver, over `sqlx`.
//!
//! A file (or in-memory) database per shard. Useful in its own right for small
//! deployments, and it is what makes the end-to-end tests exercise real SQL
//! execution — real parsing, real joins, real constraint violations — without a
//! server to start.

use async_trait::async_trait;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions, SqliteRow};
use sqlx::{Column, Row, TypeInfo, ValueRef};

use vituss_core::{BackendConfig, Error, Field, QueryResult, Result, SqlType, Value};
use vituss_dialect::DialectRef;

use crate::backend::{Backend, Connection, Health, PoolStats};
use crate::common::returns_rows;
use crate::impl_connection;

pub struct SqliteBackend {
    pool: sqlx::SqlitePool,
    dialect: DialectRef,
    describe: String,
    config: BackendConfig,
}

impl SqliteBackend {
    pub async fn connect(config: &BackendConfig, dialect: DialectRef) -> Result<Self> {
        let dsn = config.resolved_dsn()?;
        let opts: SqliteConnectOptions = dsn
            .parse::<SqliteConnectOptions>()
            .map_err(|e| Error::invalid(format!("invalid SQLite DSN {}: {e}", config.redacted_dsn())))?
            .create_if_missing(true)
            // WAL lets readers run while a writer holds the database, which is
            // what makes concurrent shard queries usable at all.
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
            .busy_timeout(std::time::Duration::from_secs(config.query_timeout_secs));

        let pool = SqlitePoolOptions::new()
            .max_connections(config.max_connections.max(1))
            .acquire_timeout(std::time::Duration::from_secs(config.connect_timeout_secs))
            .connect_with(opts)
            .await
            .map_err(map_err)?;

        Ok(Self {
            pool,
            dialect,
            describe: format!("sqlite:{}", config.redacted_dsn()),
            config: config.clone(),
        })
    }
}

#[async_trait]
impl Backend for SqliteBackend {
    fn dialect(&self) -> &DialectRef {
        &self.dialect
    }

    fn describe(&self) -> String {
        self.describe.clone()
    }

    async fn acquire(&self) -> Result<Box<dyn Connection>> {
        let conn = self.pool.acquire().await.map_err(map_err)?;
        Ok(Box::new(SqliteConnection {
            conn,
            dialect: self.dialect.clone(),
            in_tx: false,
            healthy: true,
            timeout: std::time::Duration::from_secs(self.config.query_timeout_secs),
        }))
    }

    async fn health(&self) -> Result<Health> {
        let mut conn = match self.acquire().await {
            Ok(c) => c,
            Err(e) => return Ok(Health::unreachable(e.message)),
        };
        let version = conn
            .execute_raw(self.dialect.introspection().server_version)
            .await
            .ok()
            .and_then(|r| r.rows.first().and_then(|row| row.first().cloned()))
            .map(|v| v.to_string());
        Ok(Health {
            reachable: true,
            server_version: version,
            // SQLite has no replication of its own; a SQLite shard is always its
            // own primary.
            replication_position: None,
            replication_lag_secs: Some(0),
            error: None,
        })
    }

    fn stats(&self) -> PoolStats {
        PoolStats {
            size: self.pool.size(),
            idle: self.pool.num_idle() as u32,
            in_use: self.pool.size().saturating_sub(self.pool.num_idle() as u32),
            max: self.config.max_connections,
        }
    }

    async fn close(&self) {
        self.pool.close().await;
    }
}

pub struct SqliteConnection {
    conn: sqlx::pool::PoolConnection<sqlx::Sqlite>,
    dialect: DialectRef,
    in_tx: bool,
    healthy: bool,
    timeout: std::time::Duration,
}

impl SqliteConnection {
    async fn exec_impl(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult> {
        let fut = run(&mut self.conn, sql, params);
        match tokio::time::timeout(self.timeout, fut).await {
            Ok(Ok(r)) => Ok(r),
            Ok(Err(e)) => {
                // A failed statement inside a transaction leaves the connection in
                // an unknown state; mark it so the pool discards it.
                if self.in_tx {
                    self.healthy = false;
                }
                Err(e)
            }
            Err(_) => {
                self.healthy = false;
                Err(Error::new(
                    vituss_core::Code::DeadlineExceeded,
                    format!("query exceeded the {}s backend timeout", self.timeout.as_secs()),
                ))
            }
        }
    }
}

impl_connection!(SqliteConnection);

async fn run(
    conn: &mut sqlx::pool::PoolConnection<sqlx::Sqlite>,
    sql: &str,
    params: &[Value],
) -> Result<QueryResult> {
    let mut q = sqlx::query(sql);
    for p in params {
        q = bind(q, p);
    }

    if returns_rows(sql) {
        let rows = q.fetch_all(&mut **conn).await.map_err(map_err)?;
        Ok(rows_to_result(rows))
    } else {
        let done = q.execute(&mut **conn).await.map_err(map_err)?;
        Ok(QueryResult {
            rows_affected: done.rows_affected(),
            last_insert_id: Some(done.last_insert_rowid() as u64),
            ..Default::default()
        })
    }
}

type SqliteQuery<'a> = sqlx::query::Query<'a, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'a>>;

fn bind<'a>(q: SqliteQuery<'a>, v: &'a Value) -> SqliteQuery<'a> {
    match v {
        Value::Null => q.bind(Option::<i64>::None),
        Value::Bool(b) => q.bind(*b),
        Value::Int(i) => q.bind(*i),
        // SQLite integers are signed 64-bit; anything larger has to travel as
        // text or it would wrap silently.
        Value::Uint(u) => match i64::try_from(*u) {
            Ok(i) => q.bind(i),
            Err(_) => q.bind(u.to_string()),
        },
        Value::Float(f) => q.bind(*f),
        Value::Decimal(d) => q.bind(d.to_string()),
        Value::Text(s) => q.bind(s.as_str()),
        Value::Bytes(b) => q.bind(b.as_slice()),
        Value::Date(d) => q.bind(*d),
        Value::Time(t) => q.bind(*t),
        Value::DateTime(dt) => q.bind(*dt),
        Value::Timestamp(ts) => q.bind(*ts),
        Value::Json(j) => q.bind(j.to_string()),
        Value::Uuid(u) => q.bind(u.to_string()),
    }
}

fn rows_to_result(rows: Vec<SqliteRow>) -> QueryResult {
    let Some(first) = rows.first() else {
        return QueryResult::default();
    };
    let fields: Vec<Field> = first
        .columns()
        .iter()
        .map(|c| {
            let native = c.type_info().name().to_string();
            Field {
                name: c.name().to_string(),
                table: None,
                schema: None,
                sql_type: sqlite_type(&native),
                nullable: true,
                native_type: Some(native),
            }
        })
        .collect();

    let out_rows: Vec<Vec<Value>> = rows
        .iter()
        .map(|row| (0..row.columns().len()).map(|i| decode(row, i)).collect())
        .collect();

    // SQLite reports a *declared* column type, which for a computed column or an
    // untyped table is nothing at all. The values are authoritative, so where the
    // declaration says nothing, the first real value decides.
    let mut fields = fields;
    for (i, field) in fields.iter_mut().enumerate() {
        if !matches!(field.sql_type, SqlType::Null | SqlType::Unknown) {
            continue;
        }
        if let Some(v) = out_rows.iter().filter_map(|r| r.get(i)).find(|v| !v.is_null()) {
            field.sql_type = v.sql_type();
        }
    }

    QueryResult { fields, rows: out_rows, ..Default::default() }
}

fn sqlite_type(native: &str) -> SqlType {
    match native.to_ascii_uppercase().as_str() {
        "INTEGER" | "INT" | "BIGINT" => SqlType::Int64,
        "REAL" | "FLOAT" | "DOUBLE" => SqlType::Float64,
        "TEXT" | "VARCHAR" => SqlType::Text,
        "BLOB" => SqlType::Blob,
        "BOOLEAN" => SqlType::Bool,
        "DATETIME" | "TIMESTAMP" => SqlType::DateTime,
        "DATE" => SqlType::Date,
        "NULL" => SqlType::Null,
        _ => SqlType::Unknown,
    }
}

/// Decode one column by its *storage class*, not by its declared type.
///
/// SQLite stores whatever it was given, so the declared column type is only a
/// hint. Reading the value's actual class is the only way to avoid coercing a
/// text value to a zero integer.
fn decode(row: &SqliteRow, i: usize) -> Value {
    let Ok(raw) = row.try_get_raw(i) else { return Value::Null };
    if raw.is_null() {
        return Value::Null;
    }
    match raw.type_info().name().to_ascii_uppercase().as_str() {
        "INTEGER" | "INT" | "BIGINT" | "BOOLEAN" => {
            row.try_get::<i64, _>(i).map(Value::Int).unwrap_or(Value::Null)
        }
        "REAL" | "FLOAT" | "DOUBLE" => row.try_get::<f64, _>(i).map(Value::Float).unwrap_or(Value::Null),
        "BLOB" => row.try_get::<Vec<u8>, _>(i).map(Value::Bytes).unwrap_or(Value::Null),
        _ => row
            .try_get::<String, _>(i)
            .map(Value::Text)
            // A value whose class we did not recognise is still better carried as
            // bytes than dropped.
            .or_else(|_| row.try_get::<Vec<u8>, _>(i).map(Value::Bytes))
            .unwrap_or(Value::Null),
    }
}

/// Translate a driver error into a Vituss error, keeping the engine's own code.
pub(crate) fn map_err(e: sqlx::Error) -> Error {
    use vituss_core::Code;
    let (code, native, state) = match &e {
        sqlx::Error::RowNotFound => (Code::NotFound, None, None),
        sqlx::Error::PoolTimedOut => (Code::ResourceExhausted, None, None),
        sqlx::Error::PoolClosed => (Code::Unavailable, None, None),
        sqlx::Error::Io(_) => (Code::Unavailable, None, None),
        sqlx::Error::Database(db) => {
            let native = db.code().and_then(|c| c.parse::<u32>().ok());
            // SQLITE_CONSTRAINT (19) and its extended codes are all uniqueness or
            // integrity failures, which callers retry differently from real errors.
            let code = match native {
                Some(19) | Some(1555) | Some(2067) => Code::AlreadyExists,
                Some(5) | Some(6) => Code::ResourceExhausted,
                _ => Code::InvalidArgument,
            };
            (code, native, None)
        }
        _ => (Code::Internal, None, None),
    };
    Error::new(code, e.to_string()).with_native(native, state)
}
