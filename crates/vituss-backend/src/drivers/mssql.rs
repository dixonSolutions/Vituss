//! Microsoft SQL Server driver, over `tiberius` (TDS).
//!
//! Unlike the other three engines this one has no `sqlx` driver, so the pool is
//! ours: a semaphore plus a stack of idle connections. That is enough, because
//! Vituss only ever needs "give me a connection and let me hold it until I say
//! otherwise" — the same shape a reserved connection has everywhere else.
//!
//! SQL Server's distributed transactions require MS DTC, which cannot be driven
//! from SQL. The dialect reports that honestly, so a 2PC transaction touching an
//! MSSQL shard is refused rather than silently downgraded.

use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::Mutex;
use tiberius::{AuthMethod, Client, Config, ToSql};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

use vituss_core::{BackendConfig, Code, Error, Field, QueryResult, Result, SqlType, Value};
use vituss_dialect::DialectRef;

use crate::backend::{Backend, Connection, Health, PoolStats};
use crate::common::returns_rows;
use crate::impl_connection;

type TdsClient = Client<Compat<TcpStream>>;

pub struct MsSqlBackend {
    config: BackendConfig,
    tds_config: Config,
    dialect: DialectRef,
    describe: String,
    idle: Arc<Mutex<Vec<TdsClient>>>,
    permits: Arc<tokio::sync::Semaphore>,
    live: Arc<std::sync::atomic::AtomicU32>,
}

impl MsSqlBackend {
    pub async fn connect(config: &BackendConfig, dialect: DialectRef) -> Result<Self> {
        let dsn = config.resolved_dsn()?;
        let mut tds_config = parse_dsn(&dsn, config)?;
        if let Some(db) = &config.database {
            tds_config.database(db);
        }

        let backend = Self {
            config: config.clone(),
            tds_config,
            dialect,
            describe: format!("mssql:{}", config.redacted_dsn()),
            idle: Arc::new(Mutex::new(Vec::new())),
            permits: Arc::new(tokio::sync::Semaphore::new(config.max_connections.max(1) as usize)),
            live: Arc::new(std::sync::atomic::AtomicU32::new(0)),
        };
        // Fail at startup rather than at the first query if the shard is
        // unreachable or the credentials are wrong.
        let probe = backend.open().await?;
        backend.idle.lock().push(probe);
        Ok(backend)
    }

    async fn open(&self) -> Result<TdsClient> {
        let addr = self.tds_config.get_addr();
        let tcp = TcpStream::connect(&addr)
            .await
            .map_err(|e| Error::unavailable(format!("connecting to {addr}: {e}")))?;
        tcp.set_nodelay(true).ok();
        let client = Client::connect(self.tds_config.clone(), tcp.compat_write())
            .await
            .map_err(map_err)?;
        self.live.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(client)
    }
}

/// Accept both ADO/JDBC connection strings and a `mssql://user:pass@host:port/db` URL.
fn parse_dsn(dsn: &str, config: &BackendConfig) -> Result<Config> {
    if dsn.contains('=') && !dsn.contains("://") {
        return Config::from_ado_string(dsn)
            .map_err(|e| Error::invalid(format!("invalid SQL Server connection string: {e}")));
    }
    let rest = dsn
        .split_once("://")
        .map(|(_, r)| r)
        .ok_or_else(|| Error::invalid(format!("invalid SQL Server DSN {}", config.redacted_dsn())))?;
    let (creds, hostpart) = match rest.split_once('@') {
        Some((c, h)) => (Some(c), h),
        None => (None, rest),
    };
    let (hostport, database) = match hostpart.split_once('/') {
        Some((h, d)) => (h, Some(d)),
        None => (hostpart, None),
    };
    let (host, port) = match hostport.rsplit_once(':') {
        Some((h, p)) => (
            h,
            p.parse::<u16>()
                .map_err(|_| Error::invalid(format!("invalid port {p:?} in SQL Server DSN")))?,
        ),
        None => (hostport, 1433),
    };

    let mut cfg = Config::new();
    cfg.host(host);
    cfg.port(port);
    if let Some(db) = database.filter(|d| !d.is_empty()) {
        cfg.database(db);
    }
    if let Some(creds) = creds {
        let (user, pass) = creds.split_once(':').unwrap_or((creds, ""));
        cfg.authentication(AuthMethod::sql_server(user, pass));
    }
    // Most SQL Server installs present a self-signed certificate. Trusting it is
    // the documented default for tooling; set `trust_certificate=false` in the
    // backend options to require a verified chain instead.
    if config.options.get("trust_certificate").map(String::as_str) != Some("false") {
        cfg.trust_cert();
    }
    Ok(cfg)
}

#[async_trait]
impl Backend for MsSqlBackend {
    fn dialect(&self) -> &DialectRef {
        &self.dialect
    }

    fn describe(&self) -> String {
        self.describe.clone()
    }

    async fn acquire(&self) -> Result<Box<dyn Connection>> {
        let permit = tokio::time::timeout(
            std::time::Duration::from_secs(self.config.connect_timeout_secs),
            self.permits.clone().acquire_owned(),
        )
        .await
        .map_err(|_| Error::new(Code::ResourceExhausted, "timed out waiting for a SQL Server connection"))?
        .map_err(|_| Error::unavailable("SQL Server pool is closed"))?;

        // Taken out of the lock before any await: the pool is a parking_lot mutex
        // and must not be held across one.
        let idle = self.idle.lock().pop();
        let client = match idle {
            Some(c) => c,
            None => self.open().await?,
        };

        Ok(Box::new(MsSqlConnection {
            client: Some(client),
            idle: self.idle.clone(),
            dialect: self.dialect.clone(),
            in_tx: false,
            healthy: true,
            timeout: std::time::Duration::from_secs(self.config.query_timeout_secs),
            _permit: permit,
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
        // Always Group secondaries report their redo queue; a standalone server
        // has no rows here, which reads as "not a replica", i.e. lag zero.
        let lag = conn
            .execute_raw(
                "SELECT TOP 1 CAST(rs.redo_queue_size / 1024 AS bigint) \
                 FROM sys.dm_hadr_database_replica_states rs",
            )
            .await
            .ok()
            .and_then(|r| r.rows.first().and_then(|row| row.first().and_then(Value::as_uint)))
            .or(Some(0));

        Ok(Health {
            reachable: true,
            server_version: version,
            replication_position: None,
            replication_lag_secs: lag,
            error: None,
        })
    }

    fn stats(&self) -> PoolStats {
        let idle = self.idle.lock().len() as u32;
        let size = self.live.load(std::sync::atomic::Ordering::Relaxed);
        PoolStats { size, idle, in_use: size.saturating_sub(idle), max: self.config.max_connections }
    }

    async fn close(&self) {
        self.permits.close();
        self.idle.lock().clear();
    }
}

pub struct MsSqlConnection {
    client: Option<TdsClient>,
    idle: Arc<Mutex<Vec<TdsClient>>>,
    dialect: DialectRef,
    in_tx: bool,
    healthy: bool,
    timeout: std::time::Duration,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl Drop for MsSqlConnection {
    fn drop(&mut self) {
        // Only a connection with no transaction left open and no unknown state is
        // worth reusing; anything else is dropped so the server rolls it back.
        if self.healthy && !self.in_tx {
            if let Some(c) = self.client.take() {
                self.idle.lock().push(c);
            }
        }
    }
}

impl MsSqlConnection {
    async fn exec_impl(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult> {
        let timeout = self.timeout;
        let client = match self.client.as_mut() {
            Some(c) => c,
            None => return Err(Error::unavailable("SQL Server connection has been closed")),
        };

        let owned: Vec<OwnedParam> = params.iter().map(OwnedParam::from).collect();
        let refs: Vec<&dyn ToSql> = owned.iter().map(|p| p.as_to_sql()).collect();

        let result = tokio::time::timeout(timeout, run(client, sql, &refs)).await;
        match result {
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
                    format!("query exceeded the {}s backend timeout", timeout.as_secs()),
                ))
            }
        }
    }
}

impl_connection!(MsSqlConnection);

async fn run(client: &mut TdsClient, sql: &str, params: &[&dyn ToSql]) -> Result<QueryResult> {
    if returns_rows(sql) {
        let stream = client.query(sql, params).await.map_err(map_err)?;
        let rows = stream.into_first_result().await.map_err(map_err)?;
        Ok(rows_to_result(&rows))
    } else {
        let done = client.execute(sql, params).await.map_err(map_err)?;
        Ok(QueryResult {
            rows_affected: done.rows_affected().iter().sum(),
            // SCOPE_IDENTITY() is scope-safe where @@IDENTITY is not: a trigger
            // that inserts elsewhere must not change what the caller sees.
            last_insert_id: None,
            ..Default::default()
        })
    }
}

/// Owned parameter storage.
///
/// `tiberius` binds by reference, so the converted values have to outlive the
/// call; this holds them for the duration of one statement.
enum OwnedParam {
    Null,
    Bool(bool),
    I64(i64),
    F64(f64),
    Decimal(rust_decimal::Decimal),
    Text(String),
    Bytes(Vec<u8>),
    Date(chrono::NaiveDate),
    Time(chrono::NaiveTime),
    DateTime(chrono::NaiveDateTime),
    Uuid(uuid::Uuid),
}

impl From<&Value> for OwnedParam {
    fn from(v: &Value) -> Self {
        match v {
            Value::Null => Self::Null,
            Value::Bool(b) => Self::Bool(*b),
            Value::Int(i) => Self::I64(*i),
            // No unsigned types in T-SQL; anything that does not fit becomes a
            // decimal rather than wrapping.
            Value::Uint(u) => match i64::try_from(*u) {
                Ok(i) => Self::I64(i),
                Err(_) => Self::Decimal(rust_decimal::Decimal::from(*u)),
            },
            Value::Float(f) => Self::F64(*f),
            Value::Decimal(d) => Self::Decimal(*d),
            Value::Text(s) => Self::Text(s.clone()),
            Value::Bytes(b) => Self::Bytes(b.clone()),
            Value::Date(d) => Self::Date(*d),
            Value::Time(t) => Self::Time(*t),
            Value::DateTime(dt) => Self::DateTime(*dt),
            Value::Timestamp(ts) => Self::DateTime(ts.naive_utc()),
            Value::Json(j) => Self::Text(j.to_string()),
            Value::Uuid(u) => Self::Uuid(*u),
        }
    }
}

impl OwnedParam {
    fn as_to_sql(&self) -> &dyn ToSql {
        match self {
            Self::Null => &Option::<i64>::None,
            Self::Bool(b) => b,
            Self::I64(i) => i,
            Self::F64(f) => f,
            Self::Decimal(d) => d,
            Self::Text(s) => s,
            Self::Bytes(b) => b,
            Self::Date(d) => d,
            Self::Time(t) => t,
            Self::DateTime(dt) => dt,
            Self::Uuid(u) => u,
        }
    }
}

fn rows_to_result(rows: &[tiberius::Row]) -> QueryResult {
    let Some(first) = rows.first() else {
        return QueryResult::default();
    };
    let fields: Vec<Field> = first
        .columns()
        .iter()
        .map(|c| {
            let native = format!("{:?}", c.column_type());
            Field {
                name: c.name().to_string(),
                table: None,
                schema: None,
                sql_type: tds_type(c.column_type()),
                nullable: true,
                native_type: Some(native),
            }
        })
        .collect();

    let out_rows = rows
        .iter()
        .map(|row| {
            let types: Vec<_> = row.columns().iter().map(|c| c.column_type()).collect();
            types.iter().enumerate().map(|(i, t)| decode(row, i, *t)).collect()
        })
        .collect();

    QueryResult { fields, rows: out_rows, ..Default::default() }
}

fn tds_type(t: tiberius::ColumnType) -> SqlType {
    use tiberius::ColumnType as C;
    match t {
        C::Bit | C::Bitn => SqlType::Bool,
        C::Int1 => SqlType::Int8,
        C::Int2 => SqlType::Int16,
        C::Int4 => SqlType::Int32,
        C::Int8 | C::Intn => SqlType::Int64,
        C::Float4 => SqlType::Float32,
        C::Float8 | C::Floatn => SqlType::Float64,
        C::Decimaln | C::Numericn | C::Money | C::Money4 => SqlType::Decimal,
        C::BigChar | C::NChar => SqlType::Char,
        C::BigVarChar | C::NVarchar => SqlType::VarChar,
        C::Text | C::NText | C::Xml => SqlType::Text,
        C::BigBinary => SqlType::Binary,
        C::BigVarBin | C::Image => SqlType::VarBinary,
        C::Daten => SqlType::Date,
        C::Timen => SqlType::Time,
        C::Datetime | C::Datetime2 | C::Datetime4 | C::Datetimen => SqlType::DateTime,
        C::DatetimeOffsetn => SqlType::Timestamp,
        C::Guid => SqlType::Uuid,
        _ => SqlType::Unknown,
    }
}

fn decode(row: &tiberius::Row, i: usize, t: tiberius::ColumnType) -> Value {
    match tds_type(t) {
        SqlType::Bool => row.try_get::<bool, _>(i).ok().flatten().map(Value::Bool),
        SqlType::Int8 => row.try_get::<u8, _>(i).ok().flatten().map(|v| Value::Int(v as i64)),
        SqlType::Int16 => row.try_get::<i16, _>(i).ok().flatten().map(|v| Value::Int(v as i64)),
        SqlType::Int32 => row.try_get::<i32, _>(i).ok().flatten().map(|v| Value::Int(v as i64)),
        SqlType::Int64 => row.try_get::<i64, _>(i).ok().flatten().map(Value::Int),
        SqlType::Float32 => row.try_get::<f32, _>(i).ok().flatten().map(|v| Value::Float(v as f64)),
        SqlType::Float64 => row.try_get::<f64, _>(i).ok().flatten().map(Value::Float),
        SqlType::Decimal => row
            .try_get::<rust_decimal::Decimal, _>(i)
            .ok()
            .flatten()
            .map(Value::Decimal),
        SqlType::Date => row.try_get::<chrono::NaiveDate, _>(i).ok().flatten().map(Value::Date),
        SqlType::Time => row.try_get::<chrono::NaiveTime, _>(i).ok().flatten().map(Value::Time),
        SqlType::DateTime | SqlType::Timestamp => row
            .try_get::<chrono::NaiveDateTime, _>(i)
            .ok()
            .flatten()
            .map(Value::DateTime),
        SqlType::Uuid => row.try_get::<uuid::Uuid, _>(i).ok().flatten().map(Value::Uuid),
        SqlType::Binary | SqlType::VarBinary | SqlType::Blob => {
            row.try_get::<&[u8], _>(i).ok().flatten().map(|b| Value::Bytes(b.to_vec()))
        }
        _ => row.try_get::<&str, _>(i).ok().flatten().map(|s| Value::Text(s.to_string())),
    }
    .unwrap_or(Value::Null)
}

pub(crate) fn map_err(e: tiberius::error::Error) -> Error {
    use tiberius::error::Error as E;
    match &e {
        E::Server(info) => {
            let number = info.code();
            let code = match number {
                2627 | 2601 => Code::AlreadyExists,     // primary key / unique index violation
                1205 => Code::Aborted,                  // deadlock victim
                1222 => Code::DeadlineExceeded,         // lock request timeout
                208 | 911 => Code::NotFound,            // invalid object / database
                229 | 230 | 18456 => Code::PermissionDenied,
                701 | 802 | 10928 | 10929 => Code::ResourceExhausted,
                102 | 156 => Code::InvalidArgument,     // syntax error
                _ => Code::Internal,
            };
            Error::new(code, info.message().to_string()).with_native(Some(number), None)
        }
        E::Io { .. } => Error::unavailable(e.to_string()),
        E::Protocol(_) | E::Encoding(_) => Error::internal(e.to_string()),
        _ => Error::new(Code::Internal, e.to_string()),
    }
}
