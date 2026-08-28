//! Topology behaviour, exercised against every store implementation.

use vituss_core::{KeyRange, TabletAlias, TabletType};
use vituss_topo::*;

async fn stores() -> Vec<(&'static str, TopoServer, Option<tempfile::TempDir>)> {
    let dir = tempfile::tempdir().expect("tempdir");
    let file = TopoServer::file(dir.path()).expect("file store");
    vec![("memory", TopoServer::memory(), None), ("file", file, Some(dir))]
}

async fn seed(topo: &TopoServer, shards: &[&str], dialect: &str) {
    topo.create_cell(&CellInfo { name: "zone1".into(), topo_address: None, region: None })
        .await
        .unwrap();
    topo.create_keyspace(&Keyspace::new("commerce", dialect)).await.unwrap();
    for (i, name) in shards.iter().enumerate() {
        let mut shard = Shard::new("commerce", *name).unwrap();
        let alias = TabletAlias::new("zone1", 100 + i as u32);
        shard.primary_alias = Some(alias.clone());
        topo.create_shard(&shard).await.unwrap();
        topo.create_tablet(&Tablet {
            alias,
            hostname: format!("shard{i}.internal"),
            port_map: [("grpc".to_string(), 15991 + i as u16)].into_iter().collect(),
            keyspace: "commerce".into(),
            shard: name.to_string(),
            key_range: KeyRange::parse(name).unwrap(),
            tablet_type: TabletType::Primary,
            backend: BackendConfig::new(dialect, format!("{dialect}://localhost/commerce_{i}")),
            tags: Default::default(),
        })
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn records_round_trip_through_every_store() {
    for (name, topo, _dir) in stores().await {
        seed(&topo, &["-80", "80-"], "mysql").await;

        assert_eq!(topo.list_keyspaces().await.unwrap(), vec!["commerce"], "{name}");
        assert_eq!(topo.list_shards("commerce").await.unwrap(), vec!["-80", "80-"], "{name}");
        assert_eq!(topo.list_cells().await.unwrap(), vec!["zone1"], "{name}");

        let ks = topo.get_keyspace("commerce").await.unwrap();
        assert_eq!(ks.dialect, "mysql", "{name}");
        assert_eq!(ks.sidecar_database, "_vt", "{name}");

        let tablets = topo.get_shard_tablets("commerce", "-80", Some("zone1")).await.unwrap();
        assert_eq!(tablets.len(), 1, "{name}");
        assert_eq!(tablets[0].backend.dialect, "mysql", "{name}");
        assert_eq!(tablets[0].address(), "shard0.internal:15991", "{name}");
    }
}

#[tokio::test]
async fn a_keyspace_may_span_engines() {
    // One shard on MySQL, one on PostgreSQL. This is what a live engine
    // migration looks like: move a shard at a time while the gate serves both.
    let topo = TopoServer::memory();
    topo.create_cell(&CellInfo { name: "zone1".into(), topo_address: None, region: None })
        .await
        .unwrap();
    topo.create_keyspace(&Keyspace::new("commerce", "mysql")).await.unwrap();

    let mut a = Shard::new("commerce", "-80").unwrap();
    a.dialect = Some("mysql".into());
    let mut b = Shard::new("commerce", "80-").unwrap();
    b.dialect = Some("postgres".into());
    topo.create_shard(&a).await.unwrap();
    topo.create_shard(&b).await.unwrap();

    let shards = topo.get_shards("commerce").await.unwrap();
    assert_eq!(shards[0].dialect.as_deref(), Some("mysql"));
    assert_eq!(shards[1].dialect.as_deref(), Some("postgres"));

    // The serving graph does not care which engine is underneath.
    let srv = topo.rebuild_srv_keyspace("commerce", &["zone1".to_string()]).await.unwrap();
    assert_eq!(srv.shards(TabletType::Primary).len(), 2);
}

#[tokio::test]
async fn overlapping_shards_are_rejected_at_creation() {
    let topo = TopoServer::memory();
    topo.create_keyspace(&Keyspace::new("commerce", "mysql")).await.unwrap();
    topo.create_shard(&Shard::new("commerce", "-80").unwrap()).await.unwrap();

    let err = topo.create_shard(&Shard::new("commerce", "40-c0").unwrap()).await.unwrap_err();
    assert!(err.message.contains("overlaps"), "{}", err.message);
}

#[tokio::test]
async fn a_serving_graph_with_a_hole_is_refused() {
    let topo = TopoServer::memory();
    topo.create_cell(&CellInfo { name: "zone1".into(), topo_address: None, region: None })
        .await
        .unwrap();
    topo.create_keyspace(&Keyspace::new("commerce", "mysql")).await.unwrap();
    // -40 and 80- leave 40-80 uncovered.
    topo.create_shard(&Shard::new("commerce", "-40").unwrap()).await.unwrap();
    topo.create_shard(&Shard::new("commerce", "80-").unwrap()).await.unwrap();

    let err = topo
        .rebuild_srv_keyspace("commerce", &["zone1".to_string()])
        .await
        .unwrap_err();
    assert!(err.message.contains("do not meet"), "{}", err.message);
}

#[tokio::test]
async fn the_serving_graph_is_rebuilt_from_shard_records() {
    let topo = TopoServer::memory();
    seed(&topo, &["-40", "40-80", "80-c0", "c0-"], "mysql").await;
    topo.rebuild_all().await.unwrap();

    let srv = topo.get_srv_keyspace("zone1", "commerce").await.unwrap();
    let names: Vec<&str> = srv.shards(TabletType::Primary).iter().map(|s| s.name.as_str()).collect();
    assert_eq!(names, vec!["-40", "40-80", "80-c0", "c0-"]);
    // Replicas and rdonly get their own partition, so a shard can stop serving
    // reads without stopping writes.
    assert_eq!(srv.partitions.len(), 3);

    // Take a shard out of primary service, as a cutover would.
    let mut s = topo.get_shard("commerce", "-40").await.unwrap();
    s.is_primary_serving = false;
    topo.update_shard(&s).await.unwrap();
    // The remaining shards no longer cover the key range, so the rebuild refuses
    // rather than publishing a graph that drops writes.
    assert!(topo.rebuild_srv_keyspace("commerce", &["zone1".into()]).await.is_err());
}

#[tokio::test]
async fn keyspace_locks_are_exclusive() {
    let topo = TopoServer::memory();
    topo.create_keyspace(&Keyspace::new("commerce", "mysql")).await.unwrap();

    let held = topo.lock_keyspace("commerce", "resharding").await.unwrap();
    held.check().await.unwrap();
    let err = topo.lock_keyspace("commerce", "another reshard").await.unwrap_err();
    assert!(err.message.contains("already locked"), "{}", err.message);

    held.unlock().await.unwrap();
    // Released, so the next operation can proceed.
    topo.lock_keyspace("commerce", "retry").await.unwrap();
}

#[tokio::test]
async fn deleting_a_keyspace_with_shards_is_refused() {
    let topo = TopoServer::memory();
    seed(&topo, &["-"], "mysql").await;
    let err = topo.delete_keyspace("commerce").await.unwrap_err();
    assert!(err.message.contains("still has"), "{}", err.message);

    topo.delete_shard("commerce", "-").await.unwrap();
    topo.delete_keyspace("commerce").await.unwrap();
    assert!(topo.list_keyspaces().await.unwrap().is_empty());
}

#[tokio::test]
async fn watching_replays_existing_state_then_streams_changes() {
    let topo = TopoServer::memory();
    seed(&topo, &["-"], "mysql").await;

    let mut rx = topo.watch_tablets().await.unwrap();
    // Replay: the tablet that already existed.
    let first = rx.recv().await.expect("replay event");
    assert!(matches!(first, WatchEvent::Put { .. }));

    let mut t = topo.get_tablet(&TabletAlias::new("zone1", 100)).await.unwrap();
    t.tablet_type = TabletType::Replica;
    topo.update_tablet(&t).await.unwrap();

    let second = rx.recv().await.expect("update event");
    match second {
        WatchEvent::Put { path, .. } => assert!(path.contains("zone1-0000000100")),
        other => panic!("expected a put, got {other:?}"),
    }
}

#[tokio::test]
async fn routing_rules_survive_a_round_trip() {
    let topo = TopoServer::memory();
    let mut rules = RoutingRules::default();
    rules
        .rules
        .insert("commerce.orders".into(), vec!["orders_ks.orders".into()]);
    topo.save_routing_rules(&rules).await.unwrap();
    assert_eq!(topo.get_routing_rules().await.unwrap(), rules);
}
