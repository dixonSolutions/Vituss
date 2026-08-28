//! End-to-end: a real sharded cluster, in one process, over real SQL engines.
//!
//! Each shard is a genuine SQLite database — real parsing, real constraints, real
//! transactions — so what these tests exercise is the whole stack: plan, route,
//! render per shard, execute, merge. Nothing about the query path is stubbed.

use std::sync::Arc;

use vituss_core::{KeyRange, Session, TabletAlias, TabletType, Value};
use vituss_engine::new_session;
use vituss_gate::Gate;
use vituss_topo::{BackendConfig, CellInfo, Keyspace, Shard, Tablet, TopoServer};

const VSCHEMA: &str = r#"
{
  "sharded": true,
  "vindexes": { "hash": { "type": "hash" } },
  "tables": {
    "user":   { "column_vindexes": [ { "column": "user_id", "name": "hash" } ] },
    "corder": { "column_vindexes": [ { "column": "user_id", "name": "hash" } ] }
  }
}
"#;

/// A second layout, adding the pieces that need an unsharded keyspace: a
/// sequence for generated ids and a lookup table for a secondary index.
const SHARDED_VSCHEMA_WITH_LOOKUP: &str = r#"
{
  "sharded": true,
  "vindexes": {
    "hash": { "type": "hash" },
    "email_idx": {
      "type": "lookup_unique",
      "owner": "user",
      "params": { "table": "email_lookup", "from": "email", "to": "keyspace_id" }
    }
  },
  "tables": {
    "user": {
      "column_vindexes": [
        { "column": "user_id", "name": "hash" },
        { "column": "email", "name": "email_idx" }
      ],
      "auto_increment": { "column": "user_id", "sequence": "main.user_seq" }
    }
  }
}
"#;

const UNSHARDED_VSCHEMA: &str = r#"
{
  "sharded": false,
  "tables": { "user_seq": { "type": "sequence" }, "email_lookup": {} }
}
"#;

struct Cluster {
    gate: Arc<Gate>,
    _dir: tempfile::TempDir,
}

async fn cluster(shards: &[&str]) -> Cluster {
    let dir = tempfile::tempdir().expect("tempdir");
    let topo = TopoServer::memory();

    topo.create_cell(&CellInfo { name: "zone1".into(), topo_address: None, region: None })
        .await
        .unwrap();
    topo.create_keyspace(&Keyspace::new("commerce", "sqlite")).await.unwrap();
    topo.save_vschema_json("commerce", &serde_json::from_str(VSCHEMA).unwrap())
        .await
        .unwrap();

    for (i, name) in shards.iter().enumerate() {
        topo.create_shard(&Shard::new("commerce", *name).unwrap()).await.unwrap();
        let path = dir.path().join(format!("shard{i}.db"));
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
                ..BackendConfig::new("sqlite", format!("sqlite:{}", path.display()))
            },
            tags: Default::default(),
        })
        .await
        .unwrap();
    }
    topo.rebuild_all().await.unwrap();

    // The client speaks MySQL; the shards run SQLite. Every statement is
    // translated on its way to a shard.
    let dialect = vituss_dialect::get("mysql").unwrap();
    let gate = Gate::bootstrap(topo, Some("zone1".into()), dialect).await.unwrap();
    Cluster { gate, _dir: dir }
}

fn session() -> vituss_engine::SessionRef {
    let mut s = Session::new();
    s.target_keyspace = Some("commerce".into());
    new_session(s)
}

async fn run(c: &Cluster, s: &vituss_engine::SessionRef, sql: &str) -> vituss_core::QueryResult {
    c.gate
        .execute(sql, s, &Default::default())
        .await
        .unwrap_or_else(|e| panic!("{sql}\n  failed: {}", e.message))
}

async fn run_err(c: &Cluster, s: &vituss_engine::SessionRef, sql: &str) -> String {
    match c.gate.execute(sql, s, &Default::default()).await {
        Ok(_) => panic!("expected {sql:?} to fail"),
        Err(e) => e.message,
    }
}

async fn seeded() -> (Cluster, vituss_engine::SessionRef) {
    let c = cluster(&["-80", "80-"]).await;
    let s = session();
    run(&c, &s, "CREATE TABLE user (user_id BIGINT PRIMARY KEY, name VARCHAR(64), country VARCHAR(2))").await;
    run(&c, &s, "CREATE TABLE corder (order_id BIGINT PRIMARY KEY, user_id BIGINT, price BIGINT)").await;
    (c, s)
}

#[tokio::test]
async fn ddl_reaches_every_shard() {
    let (c, s) = seeded().await;
    // Both shards have the table, so an insert to either works.
    run(&c, &s, "INSERT INTO user (user_id, name) VALUES (1, 'a'), (2, 'b')").await;
    let r = run(&c, &s, "SELECT COUNT(*) FROM user").await;
    assert_eq!(r.rows[0][0].as_int(), Some(2));
}

#[tokio::test]
async fn rows_are_placed_on_the_shard_their_vindex_chooses() {
    let (c, s) = seeded().await;
    // hash(1) starts 0x16 → shard -80. hash(4) starts 0xd2 → shard 80-.
    run(&c, &s, "INSERT INTO user (user_id, name) VALUES (1, 'ada'), (4, 'grace')").await;

    let low = run(&c, &s, "SELECT name FROM user WHERE user_id = 1").await;
    assert_eq!(low.rows.len(), 1);
    assert_eq!(low.rows[0][0], Value::Text("ada".into()));

    let high = run(&c, &s, "SELECT name FROM user WHERE user_id = 4").await;
    assert_eq!(high.rows[0][0], Value::Text("grace".into()));

    // And the scatter finds both, wherever they went.
    let all = run(&c, &s, "SELECT name FROM user ORDER BY name").await;
    assert_eq!(all.rows.len(), 2);
    assert_eq!(all.rows[0][0], Value::Text("ada".into()));
    assert_eq!(all.rows[1][0], Value::Text("grace".into()));
}

#[tokio::test]
async fn a_scattered_order_by_is_globally_sorted() {
    let (c, s) = seeded().await;
    for i in 1..=12 {
        run(
            &c,
            &s,
            &format!("INSERT INTO user (user_id, name) VALUES ({i}, 'user{:02}')", 13 - i),
        )
        .await;
    }
    let r = run(&c, &s, "SELECT name FROM user ORDER BY name").await;
    let names: Vec<String> = r.rows.iter().map(|x| x[0].to_string()).collect();
    let mut sorted = names.clone();
    sorted.sort();
    assert_eq!(names, sorted, "rows from different shards must come back in order");
    assert_eq!(names.len(), 12);
}

#[tokio::test]
async fn a_scattered_limit_returns_the_global_top_n() {
    let (c, s) = seeded().await;
    for i in 1..=12 {
        run(&c, &s, &format!("INSERT INTO user (user_id, name) VALUES ({i}, 'u{:02}')", i)).await;
    }
    let r = run(&c, &s, "SELECT name FROM user ORDER BY name LIMIT 3").await;
    let names: Vec<String> = r.rows.iter().map(|x| x[0].to_string()).collect();
    assert_eq!(names, vec!["u01", "u02", "u03"]);
}

#[tokio::test]
async fn aggregates_are_combined_across_shards() {
    let (c, s) = seeded().await;
    for i in 1..=10 {
        run(
            &c,
            &s,
            &format!("INSERT INTO corder (order_id, user_id, price) VALUES ({i}, {i}, {})", i * 10),
        )
        .await;
    }

    let count = run(&c, &s, "SELECT COUNT(*) FROM corder").await;
    assert_eq!(count.rows[0][0].as_int(), Some(10));

    let sum = run(&c, &s, "SELECT SUM(price) FROM corder").await;
    assert_eq!(sum.rows[0][0].as_int(), Some(550));

    let minmax = run(&c, &s, "SELECT MIN(price), MAX(price) FROM corder").await;
    assert_eq!(minmax.rows[0][0].as_int(), Some(10));
    assert_eq!(minmax.rows[0][1].as_int(), Some(100));

    // The mean of the per-shard means would be wrong unless the shards are
    // evenly split; this is the case that catches it.
    let avg = run(&c, &s, "SELECT AVG(price) FROM corder").await;
    assert_eq!(avg.rows[0][0].as_f64(), Some(55.0));
    assert_eq!(avg.rows[0].len(), 1, "the helper COUNT column must not reach the client");
}

#[tokio::test]
async fn group_by_folds_groups_that_span_shards() {
    let (c, s) = seeded().await;
    let rows = [(1, "gb"), (2, "fr"), (3, "gb"), (4, "fr"), (5, "gb"), (6, "de")];
    for (id, country) in rows {
        run(
            &c,
            &s,
            &format!("INSERT INTO user (user_id, name, country) VALUES ({id}, 'n{id}', '{country}')"),
        )
        .await;
    }
    let r = run(&c, &s, "SELECT country, COUNT(*) FROM user GROUP BY country").await;
    let counts: std::collections::BTreeMap<String, i64> = r
        .rows
        .iter()
        .map(|row| (row[0].to_string(), row[1].as_int().unwrap()))
        .collect();
    assert_eq!(counts["gb"], 3);
    assert_eq!(counts["fr"], 2);
    assert_eq!(counts["de"], 1);
}

#[tokio::test]
async fn a_collocated_join_runs_inside_each_shard() {
    let (c, s) = seeded().await;
    for i in 1..=6 {
        run(&c, &s, &format!("INSERT INTO user (user_id, name) VALUES ({i}, 'u{i}')")).await;
        run(
            &c,
            &s,
            &format!("INSERT INTO corder (order_id, user_id, price) VALUES ({}, {i}, {})", 100 + i, i * 5),
        )
        .await;
    }
    let r = run(
        &c,
        &s,
        "SELECT u.name, o.price FROM user u JOIN corder o ON u.user_id = o.user_id ORDER BY u.name",
    )
    .await;
    assert_eq!(r.rows.len(), 6);
    assert_eq!(r.rows[0][0], Value::Text("u1".into()));
    assert_eq!(r.rows[0][1].as_int(), Some(5));
}

#[tokio::test]
async fn updates_and_deletes_route_the_same_way_reads_do() {
    let (c, s) = seeded().await;
    run(&c, &s, "INSERT INTO user (user_id, name) VALUES (1, 'ada'), (4, 'grace')").await;

    let r = run(&c, &s, "UPDATE user SET name = 'Ada' WHERE user_id = 1").await;
    assert_eq!(r.rows_affected, 1);
    let check = run(&c, &s, "SELECT name FROM user WHERE user_id = 1").await;
    assert_eq!(check.rows[0][0], Value::Text("Ada".into()));

    let r = run(&c, &s, "DELETE FROM user WHERE user_id = 4").await;
    assert_eq!(r.rows_affected, 1);
    let left = run(&c, &s, "SELECT COUNT(*) FROM user").await;
    assert_eq!(left.rows[0][0].as_int(), Some(1));
}

#[tokio::test]
async fn a_scattered_delete_touches_every_shard() {
    let (c, s) = seeded().await;
    for i in 1..=8 {
        run(&c, &s, &format!("INSERT INTO corder (order_id, user_id, price) VALUES ({i}, {i}, 100)")).await;
    }
    let r = run(&c, &s, "DELETE FROM corder WHERE price = 100").await;
    assert_eq!(r.rows_affected, 8, "the affected count is summed over shards");
    assert_eq!(run(&c, &s, "SELECT COUNT(*) FROM corder").await.rows[0][0].as_int(), Some(0));
}

#[tokio::test]
async fn a_transaction_spanning_shards_commits_everywhere() {
    let (c, s) = seeded().await;
    run(&c, &s, "BEGIN").await;
    // These two ids hash to different shards, so this is genuinely a two-shard
    // transaction.
    run(&c, &s, "INSERT INTO user (user_id, name) VALUES (1, 'ada')").await;
    run(&c, &s, "INSERT INTO user (user_id, name) VALUES (4, 'grace')").await;
    assert_eq!(s.lock().await.shard_sessions.len(), 2);
    run(&c, &s, "COMMIT").await;

    assert!(!s.lock().await.in_transaction);
    assert_eq!(run(&c, &s, "SELECT COUNT(*) FROM user").await.rows[0][0].as_int(), Some(2));
}

#[tokio::test]
async fn rolling_back_undoes_every_shard() {
    let (c, s) = seeded().await;
    run(&c, &s, "BEGIN").await;
    run(&c, &s, "INSERT INTO user (user_id, name) VALUES (1, 'ada')").await;
    run(&c, &s, "INSERT INTO user (user_id, name) VALUES (4, 'grace')").await;
    run(&c, &s, "ROLLBACK").await;

    assert_eq!(run(&c, &s, "SELECT COUNT(*) FROM user").await.rows[0][0].as_int(), Some(0));
}

#[tokio::test]
async fn single_shard_transaction_mode_refuses_to_widen() {
    let (c, s) = seeded().await;
    run(&c, &s, "SET vituss_transaction_mode = 'single'").await;
    run(&c, &s, "BEGIN").await;
    run(&c, &s, "INSERT INTO user (user_id, name) VALUES (1, 'ada')").await;

    let msg = run_err(&c, &s, "INSERT INTO user (user_id, name) VALUES (4, 'grace')").await;
    assert!(msg.contains("transaction mode is 'single'"), "{msg}");
    run(&c, &s, "ROLLBACK").await;
}

#[tokio::test]
async fn two_phase_commit_is_refused_on_an_engine_that_cannot_do_it() {
    let (c, s) = seeded().await;
    run(&c, &s, "SET vituss_transaction_mode = 'two_pc'").await;
    run(&c, &s, "BEGIN").await;
    run(&c, &s, "INSERT INTO user (user_id, name) VALUES (1, 'ada')").await;
    run(&c, &s, "INSERT INTO user (user_id, name) VALUES (4, 'grace')").await;

    // SQLite has no distributed transactions, and the gate says so rather than
    // committing shard by shard and calling it atomic.
    let msg = run_err(&c, &s, "COMMIT").await;
    assert!(msg.contains("cannot take part in a two-phase commit"), "{msg}");
}

#[tokio::test]
async fn constraint_violations_come_back_from_the_shard_that_raised_them() {
    let (c, s) = seeded().await;
    run(&c, &s, "INSERT INTO user (user_id, name) VALUES (1, 'ada')").await;
    let msg = run_err(&c, &s, "INSERT INTO user (user_id, name) VALUES (1, 'again')").await;
    assert!(msg.to_lowercase().contains("unique") || msg.to_lowercase().contains("constraint"), "{msg}");
}

#[tokio::test]
async fn an_unsharded_keyspace_needs_no_vindex_at_all() {
    let c = cluster(&["-"]).await;
    let s = session();
    run(&c, &s, "CREATE TABLE user (user_id BIGINT PRIMARY KEY, name VARCHAR(64), country VARCHAR(2))").await;
    run(&c, &s, "INSERT INTO user (user_id, name) VALUES (1, 'ada'), (2, 'grace')").await;
    assert_eq!(run(&c, &s, "SELECT COUNT(*) FROM user").await.rows[0][0].as_int(), Some(2));
}

#[tokio::test]
async fn vexplain_shows_the_plan_without_running_it() {
    let (c, s) = seeded().await;
    let r = run(&c, &s, "VEXPLAIN SELECT name FROM user WHERE user_id = 1").await;
    let text = r.rows.iter().map(|x| x[0].to_string()).collect::<Vec<_>>().join("\n");
    assert!(text.contains("Route(EqualUnique)"), "{text}");
}

#[tokio::test]
async fn mysql_syntax_is_translated_for_sqlite_shards() {
    let (c, s) = seeded().await;
    // Backtick-quoted identifiers are MySQL-only; the shards are SQLite and must
    // receive double quotes instead.
    run(&c, &s, "INSERT INTO `user` (`user_id`, `name`) VALUES (1, 'ada')").await;
    let r = run(&c, &s, "SELECT `name` FROM `user` WHERE `user_id` = 1").await;
    assert_eq!(r.rows[0][0], Value::Text("ada".into()));
}

#[tokio::test]
async fn show_statements_are_answered_from_the_vschema() {
    let (c, s) = seeded().await;
    let r = run(&c, &s, "SHOW TABLES").await;
    let names: Vec<String> = r.rows.iter().map(|x| x[0].to_string()).collect();
    assert!(names.contains(&"user".to_string()), "{names:?}");
    assert!(names.contains(&"corder".to_string()), "{names:?}");
}

// ---------------------------------------------------------------------------
// Sequences and lookup vindexes, which need a second keyspace
// ---------------------------------------------------------------------------

/// A two-keyspace cluster: `commerce` sharded, `main` unsharded and holding the
/// sequence and the lookup table.
async fn cluster_with_lookup() -> (Cluster, vituss_engine::SessionRef) {
    let dir = tempfile::tempdir().expect("tempdir");
    let topo = TopoServer::memory();
    topo.create_cell(&CellInfo { name: "zone1".into(), topo_address: None, region: None })
        .await
        .unwrap();

    for (ks, vschema, shards) in [
        ("commerce", SHARDED_VSCHEMA_WITH_LOOKUP, vec!["-80", "80-"]),
        ("main", UNSHARDED_VSCHEMA, vec!["-"]),
    ] {
        topo.create_keyspace(&Keyspace::new(ks, "sqlite")).await.unwrap();
        topo.save_vschema_json(ks, &serde_json::from_str(vschema).unwrap())
            .await
            .unwrap();
        for (i, name) in shards.iter().enumerate() {
            topo.create_shard(&Shard::new(ks, *name).unwrap()).await.unwrap();
            let path = dir.path().join(format!("{ks}_{i}.db"));
            topo.create_tablet(&Tablet {
                alias: TabletAlias::new("zone1", next_uid()),
                hostname: "localhost".into(),
                port_map: Default::default(),
                keyspace: ks.into(),
                shard: name.to_string(),
                key_range: KeyRange::parse(name).unwrap(),
                tablet_type: TabletType::Primary,
                backend: BackendConfig {
                    max_connections: 1,
                    ..BackendConfig::new("sqlite", format!("sqlite:{}", path.display()))
                },
                tags: Default::default(),
            })
            .await
            .unwrap();
        }
    }
    topo.rebuild_all().await.unwrap();

    let gate = Gate::bootstrap(topo, Some("zone1".into()), vituss_dialect::get("mysql").unwrap())
        .await
        .unwrap();
    let c = Cluster { gate, _dir: dir };
    let s = session();

    run(&c, &s, "CREATE TABLE user (user_id BIGINT PRIMARY KEY, email VARCHAR(128), name VARCHAR(64))").await;
    run(&c, &s, "USE main").await;
    run(&c, &s, "CREATE TABLE email_lookup (email VARCHAR(128) PRIMARY KEY, keyspace_id VARBINARY(8))").await;
    run(&c, &s, "CREATE TABLE user_seq (id BIGINT PRIMARY KEY, next_id BIGINT)").await;
    run(&c, &s, "INSERT INTO user_seq (id, next_id) VALUES (0, 100)").await;
    run(&c, &s, "USE commerce").await;
    (c, s)
}

static NEXT_UID: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(200);

fn next_uid() -> u32 {
    NEXT_UID.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

#[tokio::test]
async fn a_sequence_supplies_the_sharding_column_the_client_omitted() {
    let (c, s) = cluster_with_lookup().await;

    // No user_id given: it comes from the sequence, and it is also the column
    // that decides which shard the row lands on — so it has to be generated
    // before routing, not after.
    run(&c, &s, "INSERT INTO user (email, name) VALUES ('ada@x.io', 'Ada')").await;
    run(&c, &s, "INSERT INTO user (email, name) VALUES ('grace@x.io', 'Grace')").await;

    let r = run(&c, &s, "SELECT user_id, name FROM user ORDER BY user_id").await;
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0].as_uint(), Some(100));
    assert_eq!(r.rows[1][0].as_uint(), Some(101));

    // The generated ids hash to different shards, so this also proves the rows
    // were routed by the value the sequence produced.
    let one = run(&c, &s, "SELECT name FROM user WHERE user_id = 100").await;
    assert_eq!(one.rows.len(), 1);
    assert_eq!(one.rows[0][0], Value::Text("Ada".into()));
}

#[tokio::test]
async fn an_owned_lookup_vindex_is_maintained_and_then_used_for_routing() {
    let (c, s) = cluster_with_lookup().await;
    run(&c, &s, "INSERT INTO user (user_id, email, name) VALUES (1, 'ada@x.io', 'Ada')").await;
    run(&c, &s, "INSERT INTO user (user_id, email, name) VALUES (4, 'grace@x.io', 'Grace')").await;

    // Vituss wrote the lookup rows on the way in...
    run(&c, &s, "USE main").await;
    let lookup = run(&c, &s, "SELECT email FROM email_lookup ORDER BY email").await;
    assert_eq!(lookup.rows.len(), 2);
    run(&c, &s, "USE commerce").await;

    // ...so a query on the secondary column routes instead of scattering.
    let plan = c.gate.plan("SELECT name FROM user WHERE email = 'grace@x.io'", Some("commerce")).unwrap();
    assert!(plan.explain().contains("EqualUnique"), "{}", plan.explain());

    let r = run(&c, &s, "SELECT name FROM user WHERE email = 'grace@x.io'").await;
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Value::Text("Grace".into()));
}

#[tokio::test]
async fn deleting_a_row_removes_its_lookup_entry() {
    let (c, s) = cluster_with_lookup().await;
    run(&c, &s, "INSERT INTO user (user_id, email, name) VALUES (1, 'ada@x.io', 'Ada')").await;

    // The DELETE reads the affected rows under a lock first, so it knows which
    // lookup entries to remove. That read is `FOR UPDATE` on MySQL, a table hint
    // on SQL Server, and nothing at all on SQLite — translated per shard.
    run(&c, &s, "DELETE FROM user WHERE user_id = 1").await;

    run(&c, &s, "USE main").await;
    let lookup = run(&c, &s, "SELECT email FROM email_lookup").await;
    assert!(lookup.rows.is_empty(), "a deleted row must not leave its lookup entry behind");
}
