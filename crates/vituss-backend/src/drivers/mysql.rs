//! MySQL / MariaDB driver, over `sqlx`.

use async_trait::async_trait;
use sqlx::mysql::{MySqlConnectOptions, MySqlPoolOptions, MySqlRow};
use sqlx::{Column, Row, TypeInfo, ValueRef};

use vituss_core::{BackendConfig, Code, Error, Field, QueryResult, Result, SqlType, Value};
use vituss_dialect::DialectRef;

use crate::backend::{Backend, Connection, Health, PoolStats};
use crate::common::returns_rows;
use crate::impl_connection;

pub struct MySqlBackend {
    pool: sqlx::MySqlPool,
    dialect: DialectRef,
    describe: String,
    config: BackendConfig,
}

impl MySqlBackend {
    pub async fn connect(config: &BackendConfig, dialect: DialectRef) -> Result<Self> {
        let dsn = config.resolved_dsn()?;
        let mut opts: MySqlConnectOptions = dsn
            .parse::<MySqlConnectOptions>()
            .map_err(|e| Error::invalid(format!("invalid MySQL DSN {}: {e}", config.redacted_dsn())))?;
        if let Some(db) = &config.database {
            opts = opts.database(db);
        }

        let pool = MySqlPoolOptions::new()
            .max_connections(config.max_connections.max(1))
            .acquire_timeout(std::time::Duration::from_secs(config.connect_timeout_secs))
            .connect_with(opts)
            .await
            .map_err(map_err)?;

        Ok(Self {
            pool,
            dialect,
            describe: format!("mysql:{}", config.redacted_dsn()),
            config: config.clone(),
        })
    }
}

#[async_trait]
impl Backend for MySqlBackend {
    fn dialect(&self) -> &DialectRef {
        &self.dialect
    }

    fn describe(&self) -> String {
        self.describe.clone()
    }

    async fn acquire(&self) -> Result<Box<dyn Connection>> {
        let conn = self.pool.acquire().await.map_err(map_err)?;
        Ok(Box::new(MySqlConnection {
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
        let intro = self.dialect.introspection();
        let version = scalar(&mut conn, intro.server_version).await;
        let position = match intro.replication_position {
            Some(sql) => scalar(&mut conn, sql).await,
            None => None,
        };
        // Seconds_Behind_Source is the number every MySQL operator already knows;
        // taking it from SHOW REPLICA STATUS keeps the meaning identical.
        let lag = conn
            .execute_raw("SHOW REPLICA STATUS")
            .await
            .ok()
            .and_then(|r| {
                let idx = r.column_index("Seconds_Behind_Source").or_else(|| r.column_index("Seconds_Behind_Master"))?;
                r.rows.first()?.get(idx)?.as_uint()
            })
            // No rows means this server is not a replica, i.e. it is the primary.
            .or(Some(0));

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

pub struct MySqlConnection {
    conn: sqlx::pool::PoolConnection<sqlx::MySql>,
    dialect: DialectRef,
    in_tx: bool,
    healthy: bool,
    timeout: std::time::Duration,
}

impl MySqlConnection {
    async fn exec_impl(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult> {
        let fut = run(&mut self.conn, sql, params);
        match tokio::time::timeout(self.timeout, fut).await {
            Ok(Ok(r)) => Ok(r),
            Ok(Err(e)) => {
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

impl_connection!(MySqlConnection);

async fn run(
    conn: &mut sqlx::pool::PoolConnection<sqlx::MySql>,
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
        let id = done.last_insert_id();
        Ok(QueryResult {
            rows_affected: done.rows_affected(),
            // MySQL reports 0 when the statement generated no id; carrying that
            // as `None` keeps it distinguishable from a genuine id of 0.
            last_insert_id: (id != 0).then_some(id),
            ..Default::default()
        })
    }
}

type MySqlQuery<'a> = sqlx::query::Query<'a, sqlx::MySql, sqlx::mysql::MySqlArguments>;

fn bind<'a>(q: MySqlQuery<'a>, v: &'a Value) -> MySqlQuery<'a> {
    match v {
        Value::Null => q.bind(Option::<i64>::None),
        Value::Bool(b) => q.bind(*b),
        Value::Int(i) => q.bind(*i),
        Value::Uint(u) => q.bind(*u),
        Value::Float(f) => q.bind(*f),
        Value::Decimal(d) => q.bind(*d),
        Value::Text(s) => q.bind(s.as_str()),
        Value::Bytes(b) => q.bind(b.as_slice()),
        Value::Date(d) => q.bind(*d),
        Value::Time(t) => q.bind(*t),
        Value::DateTime(dt) => q.bind(*dt),
        Value::Timestamp(ts) => q.bind(*ts),
        Value::Json(j) => q.bind(j.clone()),
        // MySQL has no UUID type; the canonical text form is what applications
        // store in CHAR(36), and BINARY(16) columns accept it via UUID_TO_BIN.
        Value::Uuid(u) => q.bind(u.to_string()),
    }
}

fn rows_to_result(rows: Vec<MySqlRow>) -> QueryResult {
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
                sql_type: mysql_type(&native),
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

fn mysql_type(native: &str) -> SqlType {
    let n = native.to_ascii_uppercase();
    let unsigned = n.contains("UNSIGNED");
    match n.split_whitespace().next().unwrap_or("") {
        "BOOLEAN" | "BOOL" => SqlType::Bool,
        "TINYINT" => SqlType::Int8,
        "SMALLINT" => SqlType::Int16,
        "INT" | "MEDIUMINT" | "INTEGER" => SqlType::Int32,
        "BIGINT" => {
            if unsigned {
                SqlType::Uint64
            } else {
                SqlType::Int64
            }
        }
        "FLOAT" => SqlType::Float32,
        "DOUBLE" => SqlType::Float64,
        "DECIMAL" | "NEWDECIMAL" => SqlType::Decimal,
        "CHAR" => SqlType::Char,
        "VARCHAR" => SqlType::VarChar,
        "TEXT" | "TINYTEXT" | "MEDIUMTEXT" | "LONGTEXT" | "ENUM" | "SET" => SqlType::Text,
        "BINARY" => SqlType::Binary,
        "VARBINARY" => SqlType::VarBinary,
        "BLOB" | "TINYBLOB" | "MEDIUMBLOB" | "LONGBLOB" => SqlType::Blob,
        "DATE" | "YEAR" => SqlType::Date,
        "TIME" => SqlType::Time,
        "DATETIME" => SqlType::DateTime,
        "TIMESTAMP" => SqlType::Timestamp,
        "JSON" => SqlType::Json,
        _ => SqlType::Unknown,
    }
}

fn decode(row: &MySqlRow, i: usize) -> Value {
    let Ok(raw) = row.try_get_raw(i) else { return Value::Null };
    if raw.is_null() {
        return Value::Null;
    }
    let native = raw.type_info().name().to_string();
    match mysql_type(&native) {
        SqlType::Bool => row.try_get::<bool, _>(i).map(Value::Bool).unwrap_or(Value::Null),
        SqlType::Int8 | SqlType::Int16 | SqlType::Int32 | SqlType::Int64 => {
            row.try_get::<i64, _>(i).map(Value::Int).unwrap_or(Value::Null)
        }
        SqlType::Uint64 => row.try_get::<u64, _>(i).map(Value::Uint).unwrap_or(Value::Null),
        SqlType::Float32 | SqlType::Float64 => {
            row.try_get::<f64, _>(i).map(Value::Float).unwrap_or(Value::Null)
        }
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
            // A zero timestamp is legal in MySQL and has no chrono equivalent;
            // falling back keeps it visible instead of turning it into NULL.
            .or_else(|_| row.try_get::<chrono::NaiveDateTime, _>(i).map(Value::DateTime))
            .unwrap_or(Value::Null),
        SqlType::Json => row
            .try_get::<serde_json::Value, _>(i)
            .map(Value::Json)
            .unwrap_or(Value::Null),
        SqlType::Binary | SqlType::VarBinary | SqlType::Blob => {
            row.try_get::<Vec<u8>, _>(i).map(Value::Bytes).unwrap_or(Value::Null)
        }
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
            let native = db.code().and_then(|c| c.parse::<u32>().ok());
            let state = db.code().map(|c| c.to_string());
            // These are the numbers applications actually branch on, so they are
            // mapped rather than flattened into a generic failure.
            let code = match native {
                Some(1062) | Some(1586) => Code::AlreadyExists,   // duplicate entry
                Some(1213) => Code::Aborted,                      // deadlock
                Some(1205) => Code::DeadlineExceeded,             // lock wait timeout
                Some(1146) | Some(1049) => Code::NotFound,        // no such table / database
                Some(1045) | Some(1044) => Code::PermissionDenied,
                Some(1040) | Some(1203) => Code::ResourceExhausted,
                Some(1064) => Code::InvalidArgument,              // parse error
                _ => Code::InvalidArgument,
            };
            (code, native, state)
        }
        _ => (Code::Internal, None, None),
    };
    Error::new(code, e.to_string()).with_native(native, state)
}
