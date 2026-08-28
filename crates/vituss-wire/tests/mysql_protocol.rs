//! A real MySQL client, over TCP, against a real sharded cluster.
//!
//! The client here is `sqlx`'s MySQL driver — it does the handshake, the
//! capability negotiation, the prepared-statement round trips. Nothing about the
//! protocol is faked, which is the only way to know a MySQL application would
//! actually work against Vituss unmodified.

#![cfg(feature = "mysql-server")]

use std::sync::Arc;

use sqlx::{Column, Row};

use vituss_core::{KeyRange, TabletAlias, TabletType};
use vituss_gate::Gate;
use vituss_topo::{BackendConfig, CellInfo, Keyspace, Shard, Tablet, TopoServer};

const VSCHEMA: &str = r#"
{
  "sharded": true,
  "vindexes": { "hash": { "type": "hash" } },
  "tables": { "user": { "column_vindexes": [ { "column": "user_id", "name": "hash" } ] } }
}
"#;

async fn start() -> (String, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let topo = TopoServer::memory();
    topo.create_cell(&CellInfo { name: "zone1".into(), topo_address: None, region: None })
        .await
        .unwrap();
    topo.create_keyspace(&Keyspace::new("commerce", "sqlite")).await.unwrap();
    topo.save_vschema_json("commerce", &serde_json::from_str(VSCHEMA).unwrap())
        .await
        .unwrap();

    for (i, name) in ["-80", "80-"].iter().enumerate() {
        topo.create_shard(&Shard::new("commerce", *name).unwrap()).await.unwrap();
        topo.create_tablet(&Tablet {
            alias: TabletAlias::new("zone1", 100 + i as u32),
            hostname: "localhost".into(),
            port_map: Default::default(),
            keyspace: "commerce".into(),
            shard: name.to_string(),
            key_range: KeyRange::parse(name).unwrap(),
            tablet_type: TabletType::Primary,
            backend: BackendConfig {
                max_connections: 1,
                ..BackendConfig::new(
                    "sqlite",
                    format!("sqlite:{}", dir.path().join(format!("s{i}.db")).display()),
                )
            },
            tags: Default::default(),
        })
        .await
        .unwrap();
    }
    topo.rebuild_all().await.unwrap();

    let gate: Arc<Gate> = Gate::bootstrap(topo, Some("zone1".into()), vituss_dialect::get("mysql").unwrap())
        .await
        .unwrap();

    // Port 0 lets the OS pick a free one, so tests never collide.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else { break };
            let gate = gate.clone();
            tokio::spawn(async move {
                let _ = vituss_wire::mysql::serve_connection(gate, stream, Some("commerce".into())).await;
            });
        }
    });

    (format!("mysql://vituss@127.0.0.1:{}/commerce", addr.port()), dir)
}

#[tokio::test]
async fn a_real_mysql_client_can_use_a_sharded_cluster() {
    let (url, _dir) = start().await;
    let pool = sqlx::MySqlPool::connect(&url).await.expect("connect");

    sqlx::query("CREATE TABLE user (user_id BIGINT PRIMARY KEY, name VARCHAR(64))")
        .execute(&pool)
        .await
        .expect("create table");

    // Ids 1 and 4 hash to different shards, so this insert spans the cluster.
    for (id, name) in [(1i64, "ada"), (4, "grace"), (2, "alan")] {
        sqlx::query("INSERT INTO user (user_id, name) VALUES (?, ?)")
            .bind(id)
            .bind(name)
            .execute(&pool)
            .await
            .unwrap_or_else(|e| panic!("insert {id}: {e}"));
    }

    // A prepared statement with a parameter: the client sends the value
    // separately, and Vituss routes on it.
    let row = sqlx::query("SELECT name FROM user WHERE user_id = ?")
        .bind(4i64)
        .fetch_one(&pool)
        .await
        .expect("routed select");
    assert_eq!(row.get::<String, _>(0), "grace");

    // A scatter with a global sort.
    let rows = sqlx::query("SELECT user_id, name FROM user ORDER BY name")
        .fetch_all(&pool)
        .await
        .expect("scatter select");
    let names: Vec<String> = rows.iter().map(|r| r.get::<String, _>(1)).collect();
    assert_eq!(names, vec!["ada", "alan", "grace"]);

    // Column metadata survives the trip.
    assert_eq!(rows[0].columns()[0].name(), "user_id");

    // A cross-shard aggregate.
    let row = sqlx::query("SELECT COUNT(*) FROM user").fetch_one(&pool).await.unwrap();
    assert_eq!(row.get::<i64, _>(0), 3);

    // Errors arrive as MySQL errors, not as a dropped connection.
    let err = sqlx::query("SELECT * FROM nonexistent").fetch_all(&pool).await.unwrap_err();
    assert!(err.to_string().contains("nonexistent"), "{err}");

    // And the connection is still usable afterwards.
    let row = sqlx::query("SELECT COUNT(*) FROM user").fetch_one(&pool).await.unwrap();
    assert_eq!(row.get::<i64, _>(0), 3);
}

#[tokio::test]
async fn transactions_work_through_the_protocol() {
    let (url, _dir) = start().await;
    let pool = sqlx::MySqlPool::connect(&url).await.expect("connect");
    sqlx::query("CREATE TABLE user (user_id BIGINT PRIMARY KEY, name VARCHAR(64))")
        .execute(&pool)
        .await
        .unwrap();

    // Held on one connection, so the gate sees one session throughout.
    let mut conn = pool.acquire().await.unwrap();
    sqlx::query("BEGIN").execute(&mut *conn).await.unwrap();
    sqlx::query("INSERT INTO user (user_id, name) VALUES (1, 'ada')")
        .execute(&mut *conn)
        .await
        .unwrap();
    sqlx::query("INSERT INTO user (user_id, name) VALUES (4, 'grace')")
        .execute(&mut *conn)
        .await
        .unwrap();
    sqlx::query("ROLLBACK").execute(&mut *conn).await.unwrap();

    let row = sqlx::query("SELECT COUNT(*) FROM user").fetch_one(&mut *conn).await.unwrap();
    assert_eq!(row.get::<i64, _>(0), 0, "a rollback must undo both shards");
}
