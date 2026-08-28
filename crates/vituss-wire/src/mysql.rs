//! MySQL wire protocol server.
//!
//! Lets any MySQL client — the `mysql` CLI, JDBC, Go's `database/sql`, an ORM —
//! connect to Vituss as if it were a MySQL server. Nothing on the client side
//! changes, which is the point: sharding should be something you turn on, not
//! something every application has to be rewritten for.
//!
//! Note the asymmetry this makes possible. The protocol a client speaks and the
//! engine a shard runs are independent choices: a MySQL application can be served
//! from PostgreSQL shards, because the statement is re-rendered on its way down.

use std::collections::HashMap;
use std::sync::Arc;

use opensrv_mysql::{
    AsyncMysqlIntermediary, AsyncMysqlShim, Column, ColumnFlags, ColumnType, ErrorKind, InitWriter,
    OkResponse, ParamParser, QueryResultWriter, StatementMetaWriter,
};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;

use vituss_core::{BindVars, Error, QueryResult, Session, SqlType, Value};
use vituss_engine::SessionRef;
use vituss_gate::Gate;

/// One client connection.
pub struct MySqlConnection {
    gate: Arc<Gate>,
    session: SessionRef,
    /// Statements this connection has prepared, by id.
    prepared: HashMap<u32, Prepared>,
    next_statement_id: u32,
}

struct Prepared {
    sql: String,
    param_count: usize,
}

impl MySqlConnection {
    pub fn new(gate: Arc<Gate>, default_keyspace: Option<String>, user: Option<String>) -> Self {
        let mut session = Session::new();
        session.target_keyspace = default_keyspace;
        session.user = user;
        Self {
            gate,
            session: vituss_engine::new_session(session),
            prepared: HashMap::new(),
            next_statement_id: 1,
        }
    }
}

#[async_trait::async_trait]
impl<W: AsyncWrite + Send + Unpin> AsyncMysqlShim<W> for MySqlConnection {
    type Error = std::io::Error;

    fn version(&self) -> String {
        // Reported as a MySQL 8 server so clients enable the modern protocol
        // features, with the Vituss version appended so it is obvious what this
        // actually is when someone reads a connection log.
        format!("8.0.0-Vituss-{}", env!("CARGO_PKG_VERSION"))
    }

    async fn on_prepare<'a>(
        &'a mut self,
        query: &'a str,
        info: StatementMetaWriter<'a, W>,
    ) -> std::io::Result<()> {
        let dialect = self.gate.client_dialect().clone();
        // The client's `?` placeholders become `:p1`, `:p2` … and the *renamed*
        // statement is what gets stored: at execute time the parameters arrive
        // positionally, and named bind variables are what the planner and the
        // per-shard renderers work in.
        let (sql, count) = match dialect.parse_one(query) {
            Ok(mut stmt) => {
                let n = vituss_planner::number_client_placeholders(&mut stmt);
                (stmt.to_string(), n)
            }
            Err(e) => return info.error(ErrorKind::ER_PARSE_ERROR, e.message.as_bytes()).await,
        };

        let id = self.next_statement_id;
        self.next_statement_id += 1;
        self.prepared.insert(id, Prepared { sql, param_count: count });

        // Parameter types are reported as strings: MySQL clients send the actual
        // type with each execute, and the value is re-typed then. Columns are
        // reported empty because the shape is not known until the statement runs.
        let params: Vec<Column> = (0..count).map(|_| text_column("?")).collect();
        info.reply(id, &params, &[]).await
    }

    async fn on_execute<'a>(
        &'a mut self,
        id: u32,
        params: ParamParser<'a>,
        results: QueryResultWriter<'a, W>,
    ) -> std::io::Result<()> {
        let Some(prepared) = self.prepared.get(&id) else {
            return results
                .error(ErrorKind::ER_UNKNOWN_STMT_HANDLER, b"unknown prepared statement")
                .await;
        };
        let sql = prepared.sql.clone();
        let expected = prepared.param_count;

        let mut bind_vars = BindVars::new();
        let mut seen = 0usize;
        for (i, p) in params.into_iter().enumerate() {
            seen += 1;
            bind_vars.insert(
                vituss_planner::client_placeholder_name(i),
                param_to_value(&p),
            );
        }
        if seen != expected {
            let msg = format!("statement takes {expected} parameter(s) but {seen} were sent");
            return results.error(ErrorKind::ER_WRONG_ARGUMENTS, msg.as_bytes()).await;
        }

        self.run(&sql, &bind_vars, results).await
    }

    async fn on_close<'a>(&'a mut self, id: u32)
    where
        W: 'async_trait,
    {
        self.prepared.remove(&id);
    }

    async fn on_query<'a>(
        &'a mut self,
        query: &'a str,
        results: QueryResultWriter<'a, W>,
    ) -> std::io::Result<()> {
        self.run(query, &BindVars::new(), results).await
    }

    async fn on_init<'a>(&'a mut self, database: &'a str, writer: InitWriter<'a, W>) -> std::io::Result<()> {
        // `USE <db>` and the connect-time database both land here. A keyspace that
        // does not exist is rejected now rather than on the first query.
        match self.gate.vschema().keyspace(database) {
            Ok(ks) => {
                self.session.lock().await.target_keyspace = Some(ks.name.clone());
                writer.ok().await
            }
            Err(e) => writer.error(ErrorKind::ER_BAD_DB_ERROR, e.message.as_bytes()).await,
        }
    }
}

impl MySqlConnection {
    async fn run<'a, W: AsyncWrite + Send + Unpin>(
        &'a mut self,
        sql: &str,
        bind_vars: &BindVars,
        results: QueryResultWriter<'a, W>,
    ) -> std::io::Result<()> {
        match self.gate.execute(sql, &self.session, bind_vars).await {
            Ok(result) => write_result(result, results).await,
            Err(e) => {
                let native = self.gate.client_dialect().native_error(&e);
                tracing::debug!(sql, error = %e.message, "query failed");
                results
                    .error(error_kind(native.code), e.message.as_bytes())
                    .await
            }
        }
    }
}

/// Serve MySQL clients on `addr` until the process stops.
pub async fn serve(gate: Arc<Gate>, addr: &str, default_keyspace: Option<String>) -> vituss_core::Result<()> {
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|e| Error::internal(format!("cannot listen on {addr}: {e}")))?;
    tracing::info!(addr, "MySQL protocol server listening");

    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %e, "accept failed");
                continue;
            }
        };
        let gate = gate.clone();
        let ks = default_keyspace.clone();
        tokio::spawn(async move {
            let (reader, writer) = stream.into_split();
            let conn = MySqlConnection::new(gate, ks, None);
            if let Err(e) = AsyncMysqlIntermediary::run_on(conn, reader, writer).await {
                tracing::debug!(%peer, error = %e, "client connection ended");
            }
        });
    }
}

/// Serve one already-accepted connection. Used by `vituss combo` and by tests.
pub async fn serve_connection<S>(
    gate: Arc<Gate>,
    stream: S,
    default_keyspace: Option<String>,
) -> std::io::Result<()>
where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let (reader, writer) = tokio::io::split(stream);
    let conn = MySqlConnection::new(gate, default_keyspace, None);
    AsyncMysqlIntermediary::run_on(conn, reader, writer).await
}

// ---------------------------------------------------------------------------
// Conversions
// ---------------------------------------------------------------------------

async fn write_result<W: AsyncWrite + Send + Unpin>(
    result: QueryResult,
    writer: QueryResultWriter<'_, W>,
) -> std::io::Result<()> {
    if result.fields.is_empty() {
        return writer
            .completed(OkResponse {
                affected_rows: result.rows_affected,
                last_insert_id: result.last_insert_id.unwrap_or(0),
                info: result.info.unwrap_or_default(),
                ..Default::default()
            })
            .await;
    }

    let columns: Vec<Column> = effective_columns(&result);
    let mut rows = writer.start(&columns).await?;
    for row in &result.rows {
        for (i, value) in row.iter().enumerate() {
            // Written according to the column type, not the value's own type. The
            // binary protocol has no tolerance for a mismatch, and a shard can
            // legitimately hand back an integer for a column it declared as text.
            write_value(&mut rows, value, columns[i].coltype)?;
        }
        rows.end_row().await?;
    }
    rows.finish().await
}

/// Column definitions for a result, filling in types the shard could not report.
///
/// A shard that returned no rows, or an engine with no static type for a computed
/// column, leaves the type unknown. Guessing from the first real value is better
/// than declaring everything a string, because clients use these types to decide
/// how to decode.
fn effective_columns(result: &QueryResult) -> Vec<Column> {
    let mut columns: Vec<Column> = result.fields.iter().map(to_column).collect();
    for (i, field) in result.fields.iter().enumerate() {
        if !matches!(field.sql_type, SqlType::Null | SqlType::Unknown) {
            continue;
        }
        if let Some(v) = result.rows.iter().filter_map(|r| r.get(i)).find(|v| !v.is_null()) {
            let mut inferred = field.clone();
            inferred.sql_type = v.sql_type();
            columns[i] = to_column(&inferred);
        }
    }
    columns
}

fn write_value<W: AsyncWrite + Unpin>(
    rows: &mut opensrv_mysql::RowWriter<'_, W>,
    value: &Value,
    coltype: ColumnType,
) -> std::io::Result<()> {
    if value.is_null() {
        return rows.write_col(None::<i64>);
    }
    // Text-typed columns take the value's string form whatever it actually is.
    if matches!(
        coltype,
        ColumnType::MYSQL_TYPE_VAR_STRING
            | ColumnType::MYSQL_TYPE_STRING
            | ColumnType::MYSQL_TYPE_VARCHAR
            | ColumnType::MYSQL_TYPE_NEWDECIMAL
            | ColumnType::MYSQL_TYPE_DECIMAL
            | ColumnType::MYSQL_TYPE_JSON
    ) {
        return match value {
            Value::Bytes(b) => rows.write_col(b.as_slice()),
            other => rows.write_col(other.to_string()),
        };
    }
    match value {
        Value::Null => rows.write_col(None::<i64>),
        Value::Bool(b) => rows.write_col(*b as i64),
        Value::Int(i) => rows.write_col(*i),
        Value::Uint(u) => rows.write_col(*u),
        Value::Float(f) => rows.write_col(*f),
        // Decimals travel as text so no precision is lost on the way out.
        Value::Decimal(d) => rows.write_col(d.to_string()),
        Value::Text(s) => rows.write_col(s.as_str()),
        Value::Bytes(b) => rows.write_col(b.as_slice()),
        Value::Date(d) => rows.write_col(d.to_string()),
        Value::Time(t) => rows.write_col(t.to_string()),
        Value::DateTime(dt) => rows.write_col(dt.to_string()),
        Value::Timestamp(ts) => rows.write_col(ts.naive_utc().to_string()),
        Value::Json(j) => rows.write_col(j.to_string()),
        Value::Uuid(u) => rows.write_col(u.to_string()),
    }
}

fn to_column(field: &vituss_core::Field) -> Column {
    let coltype = match field.sql_type {
        SqlType::Bool => ColumnType::MYSQL_TYPE_TINY,
        SqlType::Int8 => ColumnType::MYSQL_TYPE_TINY,
        SqlType::Int16 => ColumnType::MYSQL_TYPE_SHORT,
        SqlType::Int32 => ColumnType::MYSQL_TYPE_LONG,
        SqlType::Int64 | SqlType::Uint64 => ColumnType::MYSQL_TYPE_LONGLONG,
        SqlType::Float32 => ColumnType::MYSQL_TYPE_FLOAT,
        SqlType::Float64 => ColumnType::MYSQL_TYPE_DOUBLE,
        SqlType::Decimal => ColumnType::MYSQL_TYPE_NEWDECIMAL,
        SqlType::Date => ColumnType::MYSQL_TYPE_DATE,
        SqlType::Time => ColumnType::MYSQL_TYPE_TIME,
        SqlType::DateTime => ColumnType::MYSQL_TYPE_DATETIME,
        SqlType::Timestamp => ColumnType::MYSQL_TYPE_TIMESTAMP,
        SqlType::Json => ColumnType::MYSQL_TYPE_JSON,
        SqlType::Binary | SqlType::VarBinary | SqlType::Blob => ColumnType::MYSQL_TYPE_BLOB,
        _ => ColumnType::MYSQL_TYPE_VAR_STRING,
    };
    let mut flags = ColumnFlags::empty();
    if field.sql_type == SqlType::Uint64 {
        flags |= ColumnFlags::UNSIGNED_FLAG;
    }
    if !field.nullable {
        flags |= ColumnFlags::NOT_NULL_FLAG;
    }
    if field.sql_type.is_binary() {
        flags |= ColumnFlags::BINARY_FLAG;
    }
    Column {
        table: field.table.clone().unwrap_or_default(),
        column: field.name.clone(),
        coltype,
        colflags: flags,
    }
}

fn text_column(name: &str) -> Column {
    Column {
        table: String::new(),
        column: name.to_string(),
        coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
        colflags: ColumnFlags::empty(),
    }
}

fn param_to_value(p: &opensrv_mysql::ParamValue<'_>) -> Value {
    use opensrv_mysql::ValueInner;
    match p.value.into_inner() {
        ValueInner::NULL => Value::Null,
        ValueInner::Bytes(b) => match std::str::from_utf8(b) {
            Ok(s) => Value::Text(s.to_string()),
            Err(_) => Value::Bytes(b.to_vec()),
        },
        ValueInner::Int(i) => Value::Int(i),
        ValueInner::UInt(u) => Value::Uint(u),
        ValueInner::Double(d) => Value::Float(d),
        // Temporal parameters arrive pre-formatted by the client library; carrying
        // them as text keeps them exact whichever engine finally parses them.
        ValueInner::Date(b) | ValueInner::Datetime(b) | ValueInner::Time(b) => Value::Bytes(b.to_vec()),
    }
}

/// Map a MySQL error number onto the protocol's error-kind enum.
fn error_kind(code: u32) -> ErrorKind {
    ErrorKind::from(code as u16)
}
