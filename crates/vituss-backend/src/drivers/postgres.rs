//! PostgreSQL driver, over `sqlx`.

use async_trait::async_trait;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions, PgRow};
use sqlx::{Column, Row, TypeInfo, ValueRef};

use vituss_core::{BackendConfig, Code, Error, Field, QueryResult, Result, SqlType, Value};
use vituss_dialect::DialectRef;

use crate::backend::{Backend, Connection, Health, PoolStats};
use crate::common::returns_rows;
use crate::impl_connection;

pub struct PostgresBackend {
    pool: sqlx::PgPool,
    dialect: DialectRef,
    describe: String,
    config: BackendConfig,
}

impl PostgresBackend {
    pub async fn connect(config: &BackendConfig, dialect: DialectRef) -> Result<Self> {
        let dsn = config.resolved_dsn()?;
        let mut opts: PgConnectOptions = dsn
            .parse::<PgConnectOptions>()
            .map_err(|e| Error::invalid(format!("invalid PostgreSQL DSN {}: {e}", config.redacted_dsn())))?;
        if let Some(db) = &config.database {
            opts = opts.database(db);
        }
        // A shard may live in its own schema rather than its own database, which
        // is the cheaper layout when many shards share a server.
        if let Some(schema) = &config.schema {
            opts = opts.options([("search_path", schema.as_str())]);
        }

        let pool = PgPoolOptions::new()
            .max_connections(config.max_connections.max(1))
            .acquire_timeout(std::time::Duration::from_secs(config.connect_timeout_secs))
            .connect_with(opts)
            .await
            .map_err(map_err)?;

        Ok(Self {
            pool,
            dialect,
            describe: format!("postgres:{}", config.redacted_dsn()),
            config: config.clone(),
        })
    }
}

#[async_trait]
impl Backend for PostgresBackend {
    fn dialect(&self) -> &DialectRef {
        &self.dialect
    }

    fn describe(&self) -> String {
        self.describe.clone()
    }

    async fn acquire(&self) -> Result<Box<dyn Connection>> {
        let conn = self.pool.acquire().await.map_err(map_err)?;
        Ok(Box::new(PostgresConnection {
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
        let version = scalar(&mut conn, self.dialect.introspection().server_version).await;

        // On a standby `pg_current_wal_lsn()` errors, so ask whether this server
        // is in recovery first and take the standby's replay position instead.
        let in_recovery = scalar(&mut conn, "SELECT pg_is_in_recovery()")
            .await
            .map(|v| v == "true" || v == "t" || v == "1")
            .unwrap_or(false);

        let (position, lag) = if in_recovery {
            let pos = scalar(&mut conn, "SELECT pg_last_wal_replay_lsn()::text").await;
            let lag = scalar(
                &mut conn,
                "SELECT COALESCE(CEIL(EXTRACT(EPOCH FROM now() - pg_last_xact_replay_timestamp())), 0)::bigint",
            )
            .await
            .and_then(|v| v.parse::<f64>().ok())
            .map(|v| v.max(0.0) as u64);
            (pos, lag)
        } else {
            (scalar(&mut conn, "SELECT pg_current_wal_lsn()::text").await, Some(0))
        };

        Ok(Health {
            reachable: true,
            server_version: version,
            replication_position: position,
            replication_lag_secs: lag,
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

async fn scalar(conn: &mut Box<dyn Connection>, sql: &str) -> Option<String> {
    conn.execute_raw(sql)
        .await
        .ok()
        .and_then(|r| r.rows.first().and_then(|row| row.first().cloned()))
        .filter(|v| !v.is_null())
        .map(|v| v.to_string())
}

pub struct PostgresConnection {
    conn: sqlx::pool::PoolConnection<sqlx::Postgres>,
    dialect: DialectRef,
    in_tx: bool,
    healthy: bool,
    timeout: std::time::Duration,
}

impl PostgresConnection {
    async fn exec_impl(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult> {
        let fut = run(&mut self.conn, sql, params);
        match tokio::time::timeout(self.timeout, fut).await {
            Ok(Ok(r)) => Ok(r),
            Ok(Err(e)) => {
                // PostgreSQL aborts the whole transaction on any error, so the
                // connection genuinely cannot be reused for more statements.
                if self.in_tx {
                    self.healthy = false;
                }
                Err(e)
            }
            Err(_) => {
                self.healthy = false;
                Err(Error::new(
                    Code::DeadlineExceeded,
                    format!("query exceeded the {}s backend timeout", self.timeout.as_secs()),
                ))
            }
        }
    }
}

impl_connection!(PostgresConnection);

async fn run(
    conn: &mut sqlx::pool::PoolConnection<sqlx::Postgres>,
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
            // PostgreSQL has no last-insert-id; the planner adds RETURNING when a
            // generated key is needed, and that arrives as rows instead.
            last_insert_id: None,
            ..Default::default()
        })
    }
}

type PgQuery<'a> = sqlx::query::Query<'a, sqlx::Postgres, sqlx::postgres::PgArguments>;

fn bind<'a>(q: PgQuery<'a>, v: &'a Value) -> PgQuery<'a> {
    match v {
        Value::Null => q.bind(Option::<i64>::None),
        Value::Bool(b) => q.bind(*b),
        Value::Int(i) => q.bind(*i),
        // PostgreSQL has no unsigned types. Values that fit in i64 are sent as
        // bigint; anything larger goes as numeric text, which is lossless.
        Value::Uint(u) => match i64::try_from(*u) {
            Ok(i) => q.bind(i),
            Err(_) => q.bind(rust_decimal::Decimal::from(*u)),
        },
        Value::Float(f) => q.bind(*f),
        Value::Decimal(d) => q.bind(*d),
        Value::Text(s) => q.bind(s.as_str()),
        Value::Bytes(b) => q.bind(b.as_slice()),
        Value::Date(d) => q.bind(*d),
        Value::Time(t) => q.bind(*t),
        Value::DateTime(dt) => q.bind(*dt),
        Value::Timestamp(ts) => q.bind(*ts),
        Value::Json(j) => q.bind(j.clone()),
        Value::Uuid(u) => q.bind(*u),
    }
}

fn rows_to_result(rows: Vec<PgRow>) -> QueryResult {
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
                sql_type: pg_type(&native),
                nullable: true,
                native_type: Some(native),
            }
        })
        .collect();

    let out_rows = rows
        .iter()
        .map(|row| (0..row.columns().len()).map(|i| decode(row, i)).collect())
        .collect();

    QueryResult { fields, rows: out_rows, ..Default::default() }
}

fn pg_type(native: &str) -> SqlType {
    match native.to_ascii_uppercase().as_str() {
        "BOOL" => SqlType::Bool,
        "INT2" => SqlType::Int16,
        "INT4" => SqlType::Int32,
        "INT8" => SqlType::Int64,
        "FLOAT4" => SqlType::Float32,
        "FLOAT8" => SqlType::Float64,
        "NUMERIC" | "MONEY" => SqlType::Decimal,
        "CHAR" | "BPCHAR" => SqlType::Char,
        "VARCHAR" => SqlType::VarChar,
        "TEXT" | "NAME" | "CITEXT" => SqlType::Text,
        "BYTEA" => SqlType::Blob,
        "DATE" => SqlType::Date,
        "TIME" | "TIMETZ" => SqlType::Time,
        "TIMESTAMP" => SqlType::DateTime,
        "TIMESTAMPTZ" => SqlType::Timestamp,
        "JSON" | "JSONB" => SqlType::Json,
        "UUID" => SqlType::Uuid,
        _ => SqlType::Unknown,
    }
}

fn decode(row: &PgRow, i: usize) -> Value {
    let Ok(raw) = row.try_get_raw(i) else { return Value::Null };
    if raw.is_null() {
        return Value::Null;
    }
    let native = raw.type_info().name().to_string();
    match pg_type(&native) {
        SqlType::Bool => row.try_get::<bool, _>(i).map(Value::Bool).unwrap_or(Value::Null),
        SqlType::Int16 => row.try_get::<i16, _>(i).map(|v| Value::Int(v as i64)).unwrap_or(Value::Null),
        SqlType::Int32 => row.try_get::<i32, _>(i).map(|v| Value::Int(v as i64)).unwrap_or(Value::Null),
        SqlType::Int64 => row.try_get::<i64, _>(i).map(Value::Int).unwrap_or(Value::Null),
        SqlType::Float32 => row.try_get::<f32, _>(i).map(|v| Value::Float(v as f64)).unwrap_or(Value::Null),
        SqlType::Float64 => row.try_get::<f64, _>(i).map(Value::Float).unwrap_or(Value::Null),
        SqlType::Decimal => row
            .try_get::<rust_decimal::Decimal, _>(i)
            .map(Value::Decimal)
            .unwrap_or(Value::Null),
        SqlType::Date => row.try_get::<chrono::NaiveDate, _>(i).map(Value::Date).unwrap_or(Value::Null),
        SqlType::Time => row.try_get::<chrono::NaiveTime, _>(i).map(Value::Time).unwrap_or(Value::Null),
        SqlType::DateTime => row
            .try_get::<chrono::NaiveDateTime, _>(i)
            .map(Value::DateTime)
            .unwrap_or(Value::Null),
        SqlType::Timestamp => row
            .try_get::<chrono::DateTime<chrono::Utc>, _>(i)
            .map(Value::Timestamp)
            .unwrap_or(Value::Null),
        SqlType::Json => row.try_get::<serde_json::Value, _>(i).map(Value::Json).unwrap_or(Value::Null),
        SqlType::Uuid => row.try_get::<uuid::Uuid, _>(i).map(Value::Uuid).unwrap_or(Value::Null),
        SqlType::Blob => row.try_get::<Vec<u8>, _>(i).map(Value::Bytes).unwrap_or(Value::Null),
        _ => row
            .try_get::<String, _>(i)
            .map(Value::Text)
            .or_else(|_| row.try_get::<Vec<u8>, _>(i).map(Value::Bytes))
            .unwrap_or(Value::Null),
    }
}

pub(crate) fn map_err(e: sqlx::Error) -> Error {
    let (code, native, state) = match &e {
        sqlx::Error::RowNotFound => (Code::NotFound, None, None),
        sqlx::Error::PoolTimedOut => (Code::ResourceExhausted, None, None),
        sqlx::Error::PoolClosed | sqlx::Error::Io(_) => (Code::Unavailable, None, None),
        sqlx::Error::Database(db) => {
            let state = db.code().map(|c| c.to_string());
            // PostgreSQL identifies errors by SQLSTATE, and the class (first two
            // characters) is enough to decide how a caller should react.
            let code = match state.as_deref() {
                Some("23505") => Code::AlreadyExists,
                Some("40001") | Some("40P01") => Code::Aborted, // serialisation failure, deadlock
                Some("57014") => Code::DeadlineExceeded,
                Some("42P01") | Some("3D000") => Code::NotFound,
                Some("42501") | Some("28000") | Some("28P01") => Code::PermissionDenied,
                Some("53300") | Some("53200") => Code::ResourceExhausted,
                Some(s) if s.starts_with("08") => Code::Unavailable,
                Some(s) if s.starts_with("42") || s.starts_with("22") || s.starts_with("23") => {
                    Code::InvalidArgument
                }
                _ => Code::Internal,
            };
            (code, None, state)
        }
        _ => (Code::Internal, None, None),
    };
    Error::new(code, e.to_string()).with_native(native, state)
}
