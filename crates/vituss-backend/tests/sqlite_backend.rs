//! The SQLite driver against a real (in-memory) database.

#![cfg(feature = "sqlite")]

use vituss_backend::{open, BackendConfig};
use vituss_core::Value;

async fn shard() -> std::sync::Arc<dyn vituss_backend::Backend> {
    // A private in-memory database per pool connection would give each connection
    // its own data, so the tests use a shared cache instead.
    let cfg = BackendConfig {
        max_connections: 1,
        ..BackendConfig::new("sqlite", "sqlite::memory:")
    };
    open(&cfg).await.expect("open sqlite backend")
}

#[tokio::test]
async fn executes_real_sql_and_returns_typed_values() {
    let backend = shard().await;
    let mut conn = backend.acquire().await.unwrap();

    conn.execute_raw("CREATE TABLE user (id INTEGER PRIMARY KEY, name TEXT, score REAL)")
        .await
        .unwrap();

    let r = conn
        .execute(
            "INSERT INTO user (id, name, score) VALUES (?, ?, ?)",
            &[Value::Int(1), Value::Text("ada".into()), Value::Float(9.5)],
        )
        .await
        .unwrap();
    assert_eq!(r.rows_affected, 1);
    assert_eq!(r.last_insert_id, Some(1));

    let r = conn.execute("SELECT id, name, score FROM user WHERE id = ?", &[Value::Int(1)]).await.unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Value::Int(1));
    assert_eq!(r.rows[0][1], Value::Text("ada".into()));
    assert_eq!(r.rows[0][2], Value::Float(9.5));
    assert_eq!(r.fields[0].name, "id");
}

#[tokio::test]
async fn transactions_commit_and_roll_back() {
    let backend = shard().await;
    let mut conn = backend.acquire().await.unwrap();
    conn.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY)").await.unwrap();

    conn.begin(None).await.unwrap();
    assert!(conn.in_transaction());
    conn.execute("INSERT INTO t VALUES (?)", &[Value::Int(1)]).await.unwrap();
    conn.rollback().await.unwrap();
    assert!(!conn.in_transaction());

    let r = conn.execute_raw("SELECT COUNT(*) FROM t").await.unwrap();
    assert_eq!(r.rows[0][0], Value::Int(0));

    conn.begin(None).await.unwrap();
    conn.execute("INSERT INTO t VALUES (?)", &[Value::Int(2)]).await.unwrap();
    conn.commit().await.unwrap();
    let r = conn.execute_raw("SELECT COUNT(*) FROM t").await.unwrap();
    assert_eq!(r.rows[0][0], Value::Int(1));
}

#[tokio::test]
async fn savepoints_nest_inside_a_transaction() {
    let backend = shard().await;
    let mut conn = backend.acquire().await.unwrap();
    conn.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY)").await.unwrap();

    conn.begin(None).await.unwrap();
    conn.execute("INSERT INTO t VALUES (?)", &[Value::Int(1)]).await.unwrap();
    conn.savepoint("sp1").await.unwrap();
    conn.execute("INSERT INTO t VALUES (?)", &[Value::Int(2)]).await.unwrap();
    conn.rollback_to_savepoint("sp1").await.unwrap();
    conn.commit().await.unwrap();

    let r = conn.execute_raw("SELECT id FROM t").await.unwrap();
    assert_eq!(r.rows.len(), 1, "only the pre-savepoint row should survive");
    assert_eq!(r.rows[0][0], Value::Int(1));
}

#[tokio::test]
async fn two_phase_commit_is_refused_rather_than_faked() {
    let backend = shard().await;
    let mut conn = backend.acquire().await.unwrap();
    conn.begin(None).await.unwrap();
    let err = conn.prepare_two_pc("vituss:1").await.unwrap_err();
    assert_eq!(err.code, vituss_core::Code::Unimplemented);
    assert!(err.message.contains("no distributed transaction support"), "{}", err.message);
}

#[tokio::test]
async fn constraint_violations_map_to_already_exists() {
    let backend = shard().await;
    let mut conn = backend.acquire().await.unwrap();
    conn.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY)").await.unwrap();
    conn.execute("INSERT INTO t VALUES (?)", &[Value::Int(1)]).await.unwrap();

    let err = conn.execute("INSERT INTO t VALUES (?)", &[Value::Int(1)]).await.unwrap_err();
    assert_eq!(err.code, vituss_core::Code::AlreadyExists, "{}", err.message);
    // The engine's own code survives, so an application switching on it still works.
    assert!(err.native_code.is_some());
}

#[tokio::test]
async fn health_reports_the_engine_version() {
    let backend = shard().await;
    let health = backend.health().await.unwrap();
    assert!(health.reachable);
    assert!(health.server_version.is_some());
    assert_eq!(health.replication_lag_secs, Some(0));
}

#[tokio::test]
async fn an_unknown_dialect_names_the_drivers_that_are_compiled_in() {
    let err = open(&BackendConfig::new("oracle", "oracle://x")).await.unwrap_err();
    // Reported by the dialect registry, before a driver is even looked for.
    assert!(err.message.contains("oracle"), "{}", err.message);
}
