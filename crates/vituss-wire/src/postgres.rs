//! PostgreSQL wire protocol server.
//!
//! The other half of the symmetry: a client can speak PostgreSQL to Vituss and be
//! served from MySQL shards, or the reverse. Protocol and storage engine are
//! independent choices, and this is where that becomes visible.
//!
//! Implements the simple query protocol — enough for `psql`, for `SELECT`s and
//! DML from most drivers, and for tooling. The extended (parse/bind/execute)
//! protocol is not implemented yet; a client that insists on it gets a clear
//! error rather than a hang.

use std::sync::Arc;

use futures::stream;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::query::SimpleQueryHandler;
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::api::{ClientInfo, ClientPortalStore, PgWireServerHandlers, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;
use tokio::net::TcpListener;

use vituss_core::{Error, QueryResult, Session, SqlType, Value};
use vituss_engine::SessionRef;
use vituss_gate::Gate;

/// One PostgreSQL client connection.
pub struct PostgresConnection {
    gate: Arc<Gate>,
    session: SessionRef,
}

impl PostgresConnection {
    pub fn new(gate: Arc<Gate>, default_keyspace: Option<String>) -> Self {
        let mut session = Session::new();
        session.target_keyspace = default_keyspace;
        Self { gate, session: vituss_engine::new_session(session) }
    }
}

impl NoopStartupHandler for PostgresConnection {}

#[async_trait::async_trait]
impl SimpleQueryHandler for PostgresConnection {
    async fn do_query<C>(&self, _client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + futures::Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: pgwire::api::store::PortalStore,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as futures::Sink<PgWireBackendMessage>>::Error>,
    {
        match self.gate.execute(query, &self.session, &Default::default()).await {
            Ok(result) => Ok(vec![to_response(query, result)]),
            Err(e) => Ok(vec![Response::Error(Box::new(to_error_info(&self.gate, &e)))]),
        }
    }
}

/// Wires the connection up as pgwire's handler set.
pub struct Handlers(Arc<PostgresConnection>);

impl PgWireServerHandlers for Handlers {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.0.clone()
    }

    fn startup_handler(&self) -> Arc<impl pgwire::api::auth::StartupHandler> {
        self.0.clone()
    }
}

/// Serve PostgreSQL clients on `addr` until the process stops.
pub async fn serve(gate: Arc<Gate>, addr: &str, default_keyspace: Option<String>) -> vituss_core::Result<()> {
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|e| Error::internal(format!("cannot listen on {addr}: {e}")))?;
    tracing::info!(addr, "PostgreSQL protocol server listening");

    loop {
        let (socket, peer) = match listener.accept().await {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %e, "accept failed");
                continue;
            }
        };
        let gate = gate.clone();
        let ks = default_keyspace.clone();
        tokio::spawn(async move {
            let handlers = Handlers(Arc::new(PostgresConnection::new(gate, ks)));
            if let Err(e) = pgwire::tokio::process_socket(socket, None, handlers).await {
                tracing::debug!(%peer, error = %e, "client connection ended");
            }
        });
    }
}

// ---------------------------------------------------------------------------
// Conversions
// ---------------------------------------------------------------------------

fn to_response(query: &str, result: QueryResult) -> Response {
    if result.fields.is_empty() {
        // PostgreSQL's command tag carries the verb and the row count, and clients
        // parse it — `INSERT 0 3` is not decoration.
        let verb = command_verb(query);
        return Response::Execution(Tag::new(&verb).with_rows(result.rows_affected as usize));
    }

    let fields: Arc<Vec<FieldInfo>> = Arc::new(
        result
            .fields
            .iter()
            .map(|f| FieldInfo::new(f.name.clone(), None, None, pg_type(f.sql_type), FieldFormat::Text))
            .collect(),
    );

    // One encoder, reused: it is designed to be, and a fresh one per row would
    // reallocate the buffer for every row of every result.
    let mut encoder = DataRowEncoder::new(fields.clone());
    let rows: Vec<PgWireResult<pgwire::messages::data::DataRow>> = result
        .rows
        .iter()
        .map(|row| {
            for value in row {
                encode(&mut encoder, value)?;
            }
            Ok(encoder.take_row())
        })
        .collect();

    Response::Query(QueryResponse::new(fields, stream::iter(rows)))
}

fn encode(encoder: &mut DataRowEncoder, value: &Value) -> PgWireResult<()> {
    match value {
        Value::Null => encoder.encode_field(&None::<i64>),
        Value::Bool(b) => encoder.encode_field(b),
        Value::Int(i) => encoder.encode_field(i),
        // PostgreSQL has no unsigned type; values beyond i64 travel as text so
        // nothing is lost.
        Value::Uint(u) => match i64::try_from(*u) {
            Ok(i) => encoder.encode_field(&i),
            Err(_) => encoder.encode_field(&u.to_string()),
        },
        Value::Float(f) => encoder.encode_field(f),
        Value::Decimal(d) => encoder.encode_field(&d.to_string()),
        Value::Text(s) => encoder.encode_field(s),
        Value::Bytes(b) => encoder.encode_field(b),
        Value::Date(d) => encoder.encode_field(&d.to_string()),
        Value::Time(t) => encoder.encode_field(&t.to_string()),
        Value::DateTime(dt) => encoder.encode_field(&dt.to_string()),
        Value::Timestamp(ts) => encoder.encode_field(&ts.to_rfc3339()),
        Value::Json(j) => encoder.encode_field(&j.to_string()),
        Value::Uuid(u) => encoder.encode_field(&u.to_string()),
    }
}

fn pg_type(t: SqlType) -> Type {
    match t {
        SqlType::Bool => Type::BOOL,
        SqlType::Int8 | SqlType::Int16 => Type::INT2,
        SqlType::Int32 => Type::INT4,
        SqlType::Int64 | SqlType::Uint64 => Type::INT8,
        SqlType::Float32 => Type::FLOAT4,
        SqlType::Float64 => Type::FLOAT8,
        SqlType::Decimal => Type::NUMERIC,
        SqlType::Date => Type::DATE,
        SqlType::Time => Type::TIME,
        SqlType::DateTime => Type::TIMESTAMP,
        SqlType::Timestamp => Type::TIMESTAMPTZ,
        SqlType::Json => Type::JSONB,
        SqlType::Uuid => Type::UUID,
        SqlType::Binary | SqlType::VarBinary | SqlType::Blob => Type::BYTEA,
        _ => Type::VARCHAR,
    }
}

fn command_verb(query: &str) -> String {
    query
        .split_whitespace()
        .next()
        .unwrap_or("OK")
        .to_uppercase()
}

fn to_error_info(gate: &Gate, e: &Error) -> ErrorInfo {
    // The SQLSTATE comes from the client's dialect, so a PostgreSQL client sees a
    // PostgreSQL error class whatever engine actually raised it.
    let native = gate.client_dialect().native_error(e);
    ErrorInfo::new("ERROR".to_string(), native.sql_state, e.message.clone())
}
