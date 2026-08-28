//! Administrative operations.
//!
//! Everything here changes cluster metadata rather than data. Each operation
//! takes the topology lock it needs and leaves the serving graph consistent, so
//! that a half-finished operation is never visible to a gate.

use vituss_core::{BackendConfig, Error, KeyRange, Result, TabletAlias, TabletType, Target};
use vituss_topo::{CellInfo, Keyspace, RoutingRules, Shard, Tablet, TopoServer};

use crate::config::{ClusterConfig, KeyspaceConfig};

pub struct Ctl {
    topo: TopoServer,
}

/// What an `apply` changed, so the operator can see it before and after.
#[derive(Debug, Default, PartialEq)]
pub struct ApplyReport {
    pub cells_created: Vec<String>,
    pub keyspaces_created: Vec<String>,
    pub keyspaces_updated: Vec<String>,
    pub shards_created: Vec<String>,
    pub tablets_created: Vec<String>,
    pub vschemas_applied: Vec<String>,
    pub serving_graphs_rebuilt: Vec<String>,
}

impl ApplyReport {
    pub fn is_empty(&self) -> bool {
        self.cells_created.is_empty()
            && self.keyspaces_created.is_empty()
            && self.keyspaces_updated.is_empty()
            && self.shards_created.is_empty()
            && self.tablets_created.is_empty()
            && self.vschemas_applied.is_empty()
    }

    pub fn summary(&self) -> String {
        let mut parts = Vec::new();
        let add = |parts: &mut Vec<String>, label: &str, items: &[String]| {
            if !items.is_empty() {
                parts.push(format!("{} {label}: {}", items.len(), items.join(", ")));
            }
        };
        add(&mut parts, "cell(s) created", &self.cells_created);
        add(&mut parts, "keyspace(s) created", &self.keyspaces_created);
        add(&mut parts, "keyspace(s) updated", &self.keyspaces_updated);
        add(&mut parts, "shard(s) created", &self.shards_created);
        add(&mut parts, "tablet(s) created", &self.tablets_created);
        add(&mut parts, "VSchema(s) applied", &self.vschemas_applied);
        add(&mut parts, "serving graph(s) rebuilt", &self.serving_graphs_rebuilt);
        if parts.is_empty() {
            "no changes".to_string()
        } else {
            parts.join("\n")
        }
    }
}

impl Ctl {
    pub fn new(topo: TopoServer) -> Self {
        Self { topo }
    }

    pub fn topo(&self) -> &TopoServer {
        &self.topo
    }

    /// Make the cluster match the configuration.
    ///
    /// Idempotent: applying the same file twice reports no changes the second
    /// time. Existing shards are never removed — shrinking a keyspace destroys
    /// data, and that is not something a config apply should do implicitly.
    pub async fn apply(&self, config: &ClusterConfig) -> Result<ApplyReport> {
        config.validate()?;
        let mut report = ApplyReport::default();

        for cell in &config.cells {
            if self.topo.get_cell(cell).await.is_err() {
                self.topo
                    .create_cell(&CellInfo { name: cell.clone(), topo_address: None, region: None })
                    .await?;
                report.cells_created.push(cell.clone());
            }
        }

        for ks in &config.keyspaces {
            self.apply_keyspace(ks, &mut report).await?;
        }

        if !config.routing_rules.is_empty() {
            self.topo
                .save_routing_rules(&RoutingRules { rules: config.routing_rules.clone() })
                .await?;
        }

        for ks in &config.keyspaces {
            self.topo.rebuild_srv_keyspace(&ks.name, &config.cells).await?;
            report.serving_graphs_rebuilt.push(ks.name.clone());
        }

        Ok(report)
    }

    async fn apply_keyspace(&self, ks: &KeyspaceConfig, report: &mut ApplyReport) -> Result<()> {
        let record = Keyspace::new(&ks.name, &ks.dialect);
        match self.topo.get_keyspace(&ks.name).await {
            Err(_) => {
                self.topo.create_keyspace(&record).await?;
                report.keyspaces_created.push(ks.name.clone());
            }
            Ok(existing) if existing.dialect != ks.dialect => {
                // Changing a keyspace's engine is legitimate — that is what an
                // engine migration is — but it must be deliberate, and the shards
                // have to be moved separately.
                self.topo.update_keyspace(&record).await?;
                report.keyspaces_updated.push(format!(
                    "{} ({} -> {})",
                    ks.name, existing.dialect, ks.dialect
                ));
            }
            Ok(_) => {}
        }

        let _lock = self.topo.lock_keyspace(&ks.name, "apply cluster config").await?;

        for shard_name in ks.shards.shard_names(ks.sharded)? {
            if self.topo.get_shard(&ks.name, &shard_name).await.is_err() {
                self.topo.create_shard(&Shard::new(&ks.name, &shard_name)?).await?;
                report.shards_created.push(format!("{}/{}", ks.name, shard_name));
            }
        }

        let existing_tablets = self.topo.get_tablets().await?;
        let mut next_uid = existing_tablets.iter().map(|t| t.alias.uid).max().unwrap_or(99) + 1;

        for t in ks.resolved_tablets()? {
            let already = existing_tablets.iter().any(|e| {
                e.keyspace == ks.name
                    && e.shard == t.shard
                    && e.tablet_type == t.tablet_type
                    && e.backend.dsn == t.dsn
            });
            if already {
                continue;
            }
            let mut backend = BackendConfig::new(t.dialect.clone().unwrap_or_else(|| ks.dialect.clone()), &t.dsn);
            backend.database = t.database.clone();
            backend.schema = t.schema.clone();
            if let Some(n) = t.max_connections {
                backend.max_connections = n;
            }

            let alias = TabletAlias::new(&t.cell, next_uid);
            next_uid += 1;
            self.topo
                .create_tablet(&Tablet {
                    alias: alias.clone(),
                    hostname: t.hostname.clone().unwrap_or_else(|| "localhost".to_string()),
                    port_map: Default::default(),
                    keyspace: ks.name.clone(),
                    shard: t.shard.clone(),
                    key_range: KeyRange::parse(&t.shard)?,
                    tablet_type: t.tablet_type,
                    backend,
                    tags: Default::default(),
                })
                .await?;
            report.tablets_created.push(alias.to_string());

            // A shard's primary is named in the shard record, which is what makes
            // failover a metadata change rather than a reconfiguration.
            if t.tablet_type == TabletType::Primary {
                let mut shard = self.topo.get_shard(&ks.name, &t.shard).await?;
                if shard.primary_alias.is_none() {
                    shard.primary_alias = Some(alias);
                    self.topo.update_shard(&shard).await?;
                }
            }
        }

        let mut spec = ks.vschema.clone();
        spec.sharded = ks.sharded;
        spec.dialect = Some(ks.dialect.clone());
        let json = serde_json::to_value(&spec).map_err(|e| Error::internal(e.to_string()))?;
        self.topo.save_vschema_json(&ks.name, &json).await?;
        report.vschemas_applied.push(ks.name.clone());
        Ok(())
    }

    /// Rebuild every keyspace's serving graph.
    pub async fn rebuild(&self) -> Result<Vec<String>> {
        let cells = self.topo.list_cells().await?;
        let mut done = Vec::new();
        for ks in self.topo.list_keyspaces().await? {
            self.topo.rebuild_srv_keyspace(&ks, &cells).await?;
            done.push(ks);
        }
        Ok(done)
    }

    /// Replace one keyspace's VSchema, after checking it builds.
    pub async fn apply_vschema(&self, keyspace: &str, spec: &vituss_vschema::KeyspaceSpec) -> Result<()> {
        // Validated against the whole cluster, because a lookup vindex or a
        // sequence may point at another keyspace.
        let mut whole = vituss_vschema::VSchemaSpec::default();
        for name in self.topo.list_keyspaces().await? {
            let s: vituss_vschema::KeyspaceSpec = if name == keyspace {
                spec.clone()
            } else {
                match self.topo.get_vschema_json(&name).await? {
                    Some(v) => serde_json::from_value(v).map_err(|e| Error::invalid(e.to_string()))?,
                    None => Default::default(),
                }
            };
            whole.keyspaces.insert(name, s);
        }
        vituss_vschema::VSchema::build(&whole)?;

        let json = serde_json::to_value(spec).map_err(|e| Error::internal(e.to_string()))?;
        self.topo.save_vschema_json(keyspace, &json).await
    }

    /// Check that every shard of a keyspace agrees on the schema.
    ///
    /// A keyspace whose shards have drifted apart will answer scattered queries
    /// with mismatched columns, which surfaces as baffling client errors. This
    /// finds it deliberately instead.
    pub async fn validate_schema(&self, keyspace: &str) -> Result<Vec<String>> {
        let shards = self.topo.get_shards(keyspace).await?;
        let mut reference: Option<(String, Vec<vituss_tablet::TableSchema>)> = None;
        let mut problems = Vec::new();

        for shard in &shards {
            let tablets = self
                .topo
                .get_shard_tablets(keyspace, &shard.name, None)
                .await?;
            let Some(record) = tablets.iter().find(|t| t.tablet_type == TabletType::Primary) else {
                problems.push(format!("shard {} has no primary tablet", shard.name));
                continue;
            };
            let target = Target::new(keyspace, &shard.name, TabletType::Primary);
            let server = match vituss_tablet::TabletServer::open(target, record.backend.clone()).await {
                Ok(s) => s,
                Err(e) => {
                    problems.push(format!("shard {}: unreachable ({})", shard.name, e.message));
                    continue;
                }
            };
            server.reload_schema().await?;
            let schema = <vituss_tablet::TabletServer as vituss_tablet::QueryService>::schema(&server).await?;

            match &reference {
                None => reference = Some((shard.name.clone(), schema)),
                Some((ref_shard, ref_schema)) => {
                    for diff in diff_schemas(ref_schema, &schema) {
                        problems.push(format!("shard {} differs from {}: {diff}", shard.name, ref_shard));
                    }
                }
            }
        }
        Ok(problems)
    }

    /// Split one shard into several, as a metadata operation.
    ///
    /// This creates the target shards and records where their data comes from.
    /// It does **not** move any rows: that is VReplication's job, and it is not
    /// implemented yet. The new shards are left not-serving so that the cluster
    /// keeps working until they are filled and cut over.
    pub async fn plan_split(&self, keyspace: &str, source_shard: &str, parts: u32) -> Result<Vec<String>> {
        if parts < 2 {
            return Err(Error::invalid("a split must produce at least two shards"));
        }
        let _lock = self.topo.lock_keyspace(keyspace, "plan a shard split").await?;
        let source = self.topo.get_shard(keyspace, source_shard).await?;

        let targets = split_range(&source.key_range, parts)?;
        let mut created = Vec::new();
        for range in targets {
            let mut shard = Shard::new(keyspace, range.name())?;
            shard.source_shards = vec![vituss_topo::SourceShard {
                uid: 0,
                keyspace: keyspace.to_string(),
                shard: source_shard.to_string(),
                key_range: source.key_range.clone(),
                tables: Vec::new(),
            }];
            // Not serving until its data has been copied and verified.
            shard.is_primary_serving = false;
            self.topo.create_shard(&shard).await?;
            created.push(shard.name);
        }
        Ok(created)
    }
}

/// Divide a key range into `parts` equal pieces.
fn split_range(range: &KeyRange, parts: u32) -> Result<Vec<KeyRange>> {
    let to_u64 = |b: &[u8], pad: u8| -> u64 {
        let mut buf = [pad; 8];
        for (i, x) in b.iter().take(8).enumerate() {
            buf[i] = *x;
        }
        u64::from_be_bytes(buf)
    };
    let start = if range.start.is_empty() { 0u64 } else { to_u64(&range.start, 0) };
    let end = if range.end.is_empty() { u64::MAX } else { to_u64(&range.end, 0) };
    if end <= start {
        return Err(Error::invalid(format!("cannot split empty range {range}")));
    }
    let step = (end - start) / parts as u64;
    if step == 0 {
        return Err(Error::invalid(format!(
            "range {range} is too narrow to split into {parts} parts"
        )));
    }

    let trim = |v: u64| -> Vec<u8> {
        let mut b = v.to_be_bytes().to_vec();
        while b.last() == Some(&0) {
            b.pop();
        }
        b
    };

    Ok((0..parts)
        .map(|i| {
            let s = if i == 0 { range.start.clone() } else { trim(start + step * i as u64) };
            let e = if i == parts - 1 { range.end.clone() } else { trim(start + step * (i + 1) as u64) };
            KeyRange::new(s, e)
        })
        .collect())
}

/// Differences that matter for query correctness.
fn diff_schemas(a: &[vituss_tablet::TableSchema], b: &[vituss_tablet::TableSchema]) -> Vec<String> {
    let mut out = Vec::new();
    for table in a {
        match b.iter().find(|t| t.name.eq_ignore_ascii_case(&table.name)) {
            None => out.push(format!("table {} is missing", table.name)),
            Some(other) => {
                for col in &table.columns {
                    match other.column(&col.name) {
                        None => out.push(format!("{}.{} is missing", table.name, col.name)),
                        Some(o) if o.sql_type != col.sql_type => out.push(format!(
                            "{}.{} is {:?} here but {:?} there",
                            table.name, col.name, o.sql_type, col.sql_type
                        )),
                        _ => {}
                    }
                }
            }
        }
    }
    for table in b {
        if !a.iter().any(|t| t.name.eq_ignore_ascii_case(&table.name)) {
            out.push(format!("table {} is unexpected", table.name));
        }
    }
    out
}
