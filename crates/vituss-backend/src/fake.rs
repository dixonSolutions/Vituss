//! A programmable in-process backend.
//!
//! Not a SQL engine — it matches statements against rules and returns canned
//! results, and records everything it was asked to run. That is what makes it
//! possible to assert on *routing*: which shard received which query, with which
//! bind values, in which order. Tests that need real SQL semantics use the SQLite
//! driver instead.

use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::Mutex;

use vituss_core::{Error, QueryResult, Result, Value};
use vituss_dialect::DialectRef;

use crate::backend::{Backend, Connection, Health, PoolStats};
use crate::impl_connection;

/// One statement the fake backend was asked to run.
#[derive(Debug, Clone, PartialEq)]
pub struct RecordedQuery {
    pub sql: String,
    pub params: Vec<Value>,
}

/// A canned answer for statements containing `pattern`.
#[derive(Clone)]
pub struct FakeRule {
    pub pattern: String,
    pub result: std::result::Result<QueryResult, Error>,
}

impl FakeRule {
    pub fn ok(pattern: impl Into<String>, result: QueryResult) -> Self {
        Self { pattern: pattern.into(), result: Ok(result) }
    }
    pub fn err(pattern: impl Into<String>, error: Error) -> Self {
        Self { pattern: pattern.into(), result: Err(error) }
    }
}

#[derive(Default)]
struct FakeState {
    rules: Vec<FakeRule>,
    log: Vec<RecordedQuery>,
    /// Fails every acquire, to exercise the gate's unavailable-shard handling.
    down: bool,
}

/// A backend that answers from rules and records what it was asked.
#[derive(Clone)]
pub struct FakeBackend {
    state: Arc<Mutex<FakeState>>,
    dialect: DialectRef,
    name: String,
}

impl FakeBackend {
    pub fn new(name: impl Into<String>, dialect: DialectRef) -> Self {
        Self { state: Arc::new(Mutex::new(FakeState::default())), dialect, name: name.into() }
    }

    /// Add a rule. Rules are matched in insertion order; the first whose pattern
    /// is a substring of the statement wins.
    pub fn add_rule(&self, rule: FakeRule) -> &Self {
        self.state.lock().rules.push(rule);
        self
    }

    /// Statements this backend has been asked to run, in order.
    pub fn log(&self) -> Vec<RecordedQuery> {
        self.state.lock().log.clone()
    }

    /// Just the SQL, for readable assertions.
    pub fn queries(&self) -> Vec<String> {
        self.state.lock().log.iter().map(|q| q.sql.clone()).collect()
    }

    pub fn clear_log(&self) {
        self.state.lock().log.clear();
    }

    /// Make every connection attempt fail, simulating a dead shard.
    pub fn set_down(&self, down: bool) {
        self.state.lock().down = down;
    }
}

#[async_trait]
impl Backend for FakeBackend {
    fn dialect(&self) -> &DialectRef {
        &self.dialect
    }

    fn describe(&self) -> String {
        format!("fake:{}", self.name)
    }

    async fn acquire(&self) -> Result<Box<dyn Connection>> {
        if self.state.lock().down {
            return Err(Error::unavailable(format!("fake backend {} is down", self.name)));
        }
        Ok(Box::new(FakeConnection {
            state: self.state.clone(),
            dialect: self.dialect.clone(),
            in_tx: false,
            healthy: true,
        }))
    }

    async fn health(&self) -> Result<Health> {
        if self.state.lock().down {
            return Ok(Health::unreachable("fake backend is down"));
        }
        Ok(Health {
            reachable: true,
            server_version: Some(format!("fake-{}", self.dialect.name())),
            replication_position: None,
            replication_lag_secs: Some(0),
            error: None,
        })
    }

    fn stats(&self) -> PoolStats {
        PoolStats { size: 1, idle: 1, in_use: 0, max: 1 }
    }

    async fn close(&self) {}
}

struct FakeConnection {
    state: Arc<Mutex<FakeState>>,
    dialect: DialectRef,
    in_tx: bool,
    healthy: bool,
}

impl FakeConnection {
    async fn exec_impl(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult> {
        let mut state = self.state.lock();
        state.log.push(RecordedQuery { sql: sql.to_string(), params: params.to_vec() });
        let matched = state
            .rules
            .iter()
            .find(|r| sql.contains(&r.pattern))
            .map(|r| r.result.clone());
        match matched {
            Some(Ok(r)) => Ok(r),
            Some(Err(e)) => Err(e),
            // No rule: succeed with an empty result. Tests that care assert on the
            // log; tests that do not should not have to write a rule per statement.
            None => Ok(QueryResult::default()),
        }
    }
}

impl_connection!(FakeConnection);
