//! The tablet server.
//!
//! One tablet fronts exactly one database — one shard of one keyspace. It owns
//! that database's connection pool, holds open transactions on behalf of gate
//! sessions, tracks its schema, and reports its health.
//!
//! Everything here is engine-agnostic: the tablet talks to its database through
//! [`Backend`] and describes it through [`SqlDialect`], so the same tablet code
//! serves a MySQL shard, a PostgreSQL shard or a SQL Server shard.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;

use vituss_backend::{Backend, Connection};
use vituss_core::{BackendConfig, Error, QueryResult, Result, Target, Value};
use vituss_dialect::DialectRef;

use crate::schema::{ColumnSchema, IndexSchema, TableSchema};
use crate::service::{QueryService, TabletHealth, TransactionId};

/// A transaction the tablet is holding for a session.
struct OpenTransaction {
    conn: Box<dyn Connection>,
    /// When it was opened, so a session that walks away does not pin a connection
    /// for ever.
    started: std::time::Instant,
}

pub struct TabletServer {
    target: Target,
    backend: Arc<dyn Backend>,
    dialect: DialectRef,
    config: BackendConfig,
    transactions: tokio::sync::Mutex<HashMap<u64, OpenTransaction>>,
    next_transaction_id: AtomicU64,
    schema: tokio::sync::RwLock<Vec<TableSchema>>,
    /// How long a transaction may stay open before the tablet reclaims it.
    transaction_timeout: std::time::Duration,
}

impl TabletServer {
    pub async fn open(target: Target, config: BackendConfig) -> Result<Arc<Self>> {
        let backend = vituss_backend::open(&config).await?;
        let dialect = backend.dialect().clone();
        let server = Arc::new(Self {
            target,
            backend,
            dialect,
            config,
            transactions: tokio::sync::Mutex::new(HashMap::new()),
            next_transaction_id: AtomicU64::new(1),
            schema: tokio::sync::RwLock::new(Vec::new()),
            transaction_timeout: std::time::Duration::from_secs(30),
        });
        // Load the schema once at startup so the first query does not pay for it.
        if let Err(e) = server.reload_schema().await {
            tracing::warn!(target = %server.target, error = %e.message, "initial schema load failed");
        }
        Ok(server)
    }

    /// Build a tablet over an already-open backend. Used by tests and by
    /// `vituss combo`, which shares one pool between components.
    pub fn with_backend(target: Target, backend: Arc<dyn Backend>, config: BackendConfig) -> Arc<Self> {
        let dialect = backend.dialect().clone();
        Arc::new(Self {
            target,
            backend,
            dialect,
            config,
            transactions: tokio::sync::Mutex::new(HashMap::new()),
            next_transaction_id: AtomicU64::new(1),
            schema: tokio::sync::RwLock::new(Vec::new()),
            transaction_timeout: std::time::Duration::from_secs(30),
        })
    }

    pub fn backend(&self) -> &Arc<dyn Backend> {
        &self.backend
    }

    pub fn dialect(&self) -> &DialectRef {
        &self.dialect
    }

    /// Re-read the schema from the database.
    pub async fn reload_schema(&self) -> Result<()> {
        let tables = self.introspect().await?;
        *self.schema.write().await = tables;
        Ok(())
    }

    /// Ask the database what it holds, using the dialect's own catalogue queries.
    async fn introspect(&self) -> Result<Vec<TableSchema>> {
        let intro = self.dialect.introspection();
        let mut conn = self.backend.acquire().await?;

        // Every introspection query takes exactly one parameter: the schema.
        let schema = match self.config.effective_schema() {
            Some(s) => Value::Text(s.to_string()),
            None => {
                let r = conn.execute_raw(intro.current_schema).await?;
                r.rows
                    .first()
                    .and_then(|row| row.first().cloned())
                    .unwrap_or(Value::Text(String::new()))
            }
        };
        let param = [schema];

        let tables = conn.execute(intro.list_tables, &param).await?;
        let columns = conn.execute(intro.list_columns, &param).await?;
        let indexes = conn.execute(intro.list_indexes, &param).await?;

        let mut by_name: HashMap<String, TableSchema> = HashMap::new();
        for row in &tables.rows {
            let name = row.first().map(ToString::to_string).unwrap_or_default();
            let kind = row.get(1).map(ToString::to_string).unwrap_or_default();
            by_name.insert(
                name.to_lowercase(),
                TableSchema {
                    name,
                    is_view: kind.eq_ignore_ascii_case("VIEW"),
                    columns: Vec::new(),
                    indexes: Vec::new(),
                },
            );
        }

        for row in &columns.rows {
            let table = row.first().map(ToString::to_string).unwrap_or_default();
            let Some(t) = by_name.get_mut(&table.to_lowercase()) else { continue };
            let native = row.get(3).map(ToString::to_string).unwrap_or_default();
            t.columns.push(ColumnSchema {
                name: row.get(1).map(ToString::to_string).unwrap_or_default(),
                ordinal: row.get(2).and_then(Value::as_uint).unwrap_or(0) as u32,
                sql_type: self.dialect.map_native_type(&native),
                native_type: native,
                nullable: truthy(row.get(4)),
                default: row.get(5).filter(|v| !v.is_null()).map(ToString::to_string),
                auto_generated: truthy(row.get(6)),
            });
        }

        for row in &indexes.rows {
            let table = row.first().map(ToString::to_string).unwrap_or_default();
            let Some(t) = by_name.get_mut(&table.to_lowercase()) else { continue };
            let index_name = row.get(1).map(ToString::to_string).unwrap_or_default();
            let column = row.get(2).map(ToString::to_string).unwrap_or_default();
            match t.indexes.iter_mut().find(|i| i.name == index_name) {
                // Rows arrive one column at a time, ordered by position, so
                // appending reconstructs the index's column order.
                Some(existing) => existing.columns.push(column),
                None => t.indexes.push(IndexSchema {
                    name: index_name,
                    columns: vec![column],
                    unique: truthy(row.get(4)),
                    primary: truthy(row.get(5)),
                }),
            }
        }

        let mut out: Vec<TableSchema> = by_name.into_values().collect();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }

    /// Drop transactions that have been open too long.
    ///
    /// A session that disappears mid-transaction would otherwise hold a
    /// connection — and its locks — until the process restarts.
    pub async fn reap_idle_transactions(&self) -> usize {
        let mut txs = self.transactions.lock().await;
        let expired: Vec<u64> = txs
            .iter()
            .filter(|(_, t)| t.started.elapsed() > self.transaction_timeout)
            .map(|(id, _)| *id)
            .collect();
        for id in &expired {
            if let Some(mut tx) = txs.remove(id) {
                tracing::warn!(target = %self.target, transaction = id, "reclaiming an abandoned transaction");
                let _ = tx.conn.rollback().await;
            }
        }
        expired.len()
    }

    pub async fn open_transaction_count(&self) -> usize {
        self.transactions.lock().await.len()
    }

    /// Look up an open transaction, with an error that says what probably
    /// happened if it is gone.
    async fn transaction_missing(&self, id: TransactionId) -> Error {
        Error::aborted(format!(
            "transaction {} is not open on {}; it may have been rolled back after a failure \
             or reclaimed after being idle",
            id.0, self.target
        ))
    }
}

fn truthy(v: Option<&Value>) -> bool {
    match v {
        Some(Value::Bool(b)) => *b,
        Some(other) => other.as_int().map(|i| i != 0).unwrap_or_else(|| {
            matches!(other.as_str().map(str::to_ascii_uppercase).as_deref(), Some("YES") | Some("TRUE") | Some("T"))
        }),
        None => false,
    }
}

#[async_trait]
impl QueryService for TabletServer {
    fn target(&self) -> &Target {
        &self.target
    }

    async fn execute(&self, sql: &str, params: &[Value], transaction: TransactionId) -> Result<QueryResult> {
        if transaction.is_none() {
            let mut conn = self.backend.acquire().await?;
            return conn.execute(sql, params).await;
        }
        let mut txs = self.transactions.lock().await;
        match txs.get_mut(&transaction.0) {
            Some(tx) => tx.conn.execute(sql, params).await,
            None => Err(self.transaction_missing(transaction).await),
        }
    }

    async fn begin(&self) -> Result<TransactionId> {
        // Refuse writes on a tablet that is not the shard's primary, rather than
        // letting the engine reject them one statement at a time.
        if !self.target.tablet_type.accepts_writes() {
            return Err(Error::failed_precondition(format!(
                "cannot start a transaction on {}: it is a {} tablet and only the primary accepts writes",
                self.target, self.target.tablet_type
            )));
        }
        let mut conn = self.backend.acquire().await?;
        conn.begin(None).await?;
        let id = self.next_transaction_id.fetch_add(1, Ordering::Relaxed);
        self.transactions
            .lock()
            .await
            .insert(id, OpenTransaction { conn, started: std::time::Instant::now() });
        Ok(TransactionId(id))
    }

    async fn commit(&self, transaction: TransactionId) -> Result<()> {
        let mut tx = self
            .transactions
            .lock()
            .await
            .remove(&transaction.0)
            .ok_or_else(|| Error::aborted(format!("transaction {} is not open on {}", transaction.0, self.target)))?;
        tx.conn.commit().await
    }

    async fn rollback(&self, transaction: TransactionId) -> Result<()> {
        let Some(mut tx) = self.transactions.lock().await.remove(&transaction.0) else {
            // Rolling back something already gone is what a client does after an
            // error; it is not itself an error.
            return Ok(());
        };
        tx.conn.rollback().await
    }

    async fn prepare(&self, transaction: TransactionId, xid: &str) -> Result<()> {
        // The connection stays in the map after preparing: the coordinator still
        // has to tell it to commit or roll back.
        let mut txs = self.transactions.lock().await;
        match txs.get_mut(&transaction.0) {
            Some(tx) => tx.conn.prepare_two_pc(xid).await,
            None => Err(self.transaction_missing(transaction).await),
        }
    }

    async fn commit_prepared(&self, xid: &str) -> Result<()> {
        let mut conn = self.backend.acquire().await?;
        conn.commit_prepared(xid).await
    }

    async fn rollback_prepared(&self, xid: &str) -> Result<()> {
        let mut conn = self.backend.acquire().await?;
        conn.rollback_prepared(xid).await
    }

    async fn health(&self) -> TabletHealth {
        match self.backend.health().await {
            Ok(h) => TabletHealth {
                target: self.target.clone(),
                serving: h.reachable && self.target.tablet_type.is_serving(),
                replication_lag_secs: h.replication_lag_secs,
                server_version: h.server_version,
                error: h.error,
            },
            Err(e) => TabletHealth {
                target: self.target.clone(),
                serving: false,
                replication_lag_secs: None,
                server_version: None,
                error: Some(e.message),
            },
        }
    }

    async fn schema(&self) -> Result<Vec<TableSchema>> {
        Ok(self.schema.read().await.clone())
    }

    async fn close(&self) {
        let mut txs = self.transactions.lock().await;
        for (id, mut tx) in txs.drain() {
            tracing::debug!(target = %self.target, transaction = id, "rolling back on shutdown");
            let _ = tx.conn.rollback().await;
        }
        self.backend.close().await;
    }
}
