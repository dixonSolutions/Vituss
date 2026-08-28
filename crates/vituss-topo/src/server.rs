//! The typed topology API.
//!
//! Everything above this layer talks in keyspaces, shards and tablets; only this
//! module knows they are JSON blobs at particular paths. Swapping the backing
//! store therefore changes nothing for the rest of Vituss.

use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde::Serialize;

use vituss_core::{Error, KeyRange, Result, TabletAlias, TabletType};

use crate::records::*;
use crate::store::{LockHandle, TopoStore, WatchEvent};

/// Path layout. Kept in one place so a store's on-disk shape is documented and
/// stable.
mod paths {
    pub fn cell(name: &str) -> String {
        format!("/cells/{name}/CellInfo")
    }
    pub fn cells_prefix() -> String {
        "/cells/".to_string()
    }
    pub fn keyspace(ks: &str) -> String {
        format!("/keyspaces/{ks}/Keyspace")
    }
    pub fn keyspaces_prefix() -> String {
        "/keyspaces/".to_string()
    }
    pub fn shard(ks: &str, shard: &str) -> String {
        format!("/keyspaces/{ks}/shards/{}/Shard", super::escape(shard))
    }
    pub fn shards_prefix(ks: &str) -> String {
        format!("/keyspaces/{ks}/shards/")
    }
    pub fn vschema(ks: &str) -> String {
        format!("/keyspaces/{ks}/VSchema")
    }
    pub fn tablet(alias: &str) -> String {
        format!("/tablets/{alias}/Tablet")
    }
    pub fn tablets_prefix() -> String {
        "/tablets/".to_string()
    }
    pub fn srv_keyspace(cell: &str, ks: &str) -> String {
        format!("/cells/{cell}/srvkeyspaces/{ks}/SrvKeyspace")
    }
    pub fn srv_keyspaces_prefix(cell: &str) -> String {
        format!("/cells/{cell}/srvkeyspaces/")
    }
    pub fn routing_rules() -> String {
        "/global/RoutingRules".to_string()
    }
}

/// Shard names contain `-`, which is fine, but the full range is `-`, which is
/// an awkward path segment. Escape it to a name that is unambiguous everywhere.
fn escape(shard: &str) -> String {
    if shard == "-" {
        "ALL".to_string()
    } else {
        shard.replace('/', "_")
    }
}

/// Query-level routing overrides: `from` table or keyspace → `to` targets.
///
/// This is how a table move is made atomic for clients: flip the rule and every
/// gate starts sending `commerce.orders` to `orders_ks.orders` on its next read
/// of the topology, with no client reconnect.
#[derive(Debug, Clone, Default, PartialEq, Serialize, serde::Deserialize)]
pub struct RoutingRules {
    #[serde(default)]
    pub rules: std::collections::BTreeMap<String, Vec<String>>,
}

/// The typed topology server.
#[derive(Clone)]
pub struct TopoServer {
    store: Arc<dyn TopoStore>,
}

impl TopoServer {
    pub fn new(store: Arc<dyn TopoStore>) -> Self {
        Self { store }
    }

    /// An in-memory topology. Used by `vituss combo` and by tests.
    pub fn memory() -> Self {
        Self::new(Arc::new(crate::memory::MemoryStore::new()))
    }

    /// A topology backed by a directory of JSON files.
    pub fn file(root: impl Into<std::path::PathBuf>) -> Result<Self> {
        Ok(Self::new(Arc::new(crate::file::FileStore::open(root)?)))
    }

    pub fn store(&self) -> &Arc<dyn TopoStore> {
        &self.store
    }

    // -- generic helpers -----------------------------------------------------

    async fn read<T: DeserializeOwned>(&self, path: &str) -> Result<Option<T>> {
        match self.store.get(path).await? {
            None => Ok(None),
            Some(v) => serde_json::from_slice(&v.data)
                .map(Some)
                .map_err(|e| Error::internal(format!("corrupt record at {path}: {e}"))),
        }
    }

    async fn read_required<T: DeserializeOwned>(&self, path: &str, what: &str) -> Result<T> {
        self.read(path).await?.ok_or_else(|| Error::not_found(format!("{what} not found")))
    }

    async fn write<T: Serialize>(&self, path: &str, value: &T) -> Result<()> {
        let data = serde_json::to_vec_pretty(value).map_err(|e| Error::internal(e.to_string()))?;
        self.store.put(path, &data, None).await.map(|_| ())
    }

    async fn create<T: Serialize>(&self, path: &str, value: &T) -> Result<()> {
        let data = serde_json::to_vec_pretty(value).map_err(|e| Error::internal(e.to_string()))?;
        self.store.create(path, &data).await.map(|_| ())
    }

    // -- cells ---------------------------------------------------------------

    pub async fn create_cell(&self, cell: &CellInfo) -> Result<()> {
        self.create(&paths::cell(&cell.name), cell).await
    }

    pub async fn get_cell(&self, name: &str) -> Result<CellInfo> {
        self.read_required(&paths::cell(name), &format!("cell {name:?}")).await
    }

    pub async fn list_cells(&self) -> Result<Vec<String>> {
        Ok(self
            .store
            .list(&paths::cells_prefix())
            .await?
            .into_iter()
            .filter_map(|p| p.strip_suffix("/CellInfo").map(str::to_string))
            .filter_map(|p| p.rsplit('/').next().map(str::to_string))
            .collect())
    }

    // -- keyspaces -----------------------------------------------------------

    pub async fn create_keyspace(&self, ks: &Keyspace) -> Result<()> {
        // Fail early on an unknown engine rather than at the first query.
        vituss_dialect_check(&ks.dialect)?;
        self.create(&paths::keyspace(&ks.name), ks).await
    }

    pub async fn get_keyspace(&self, name: &str) -> Result<Keyspace> {
        self.read_required(&paths::keyspace(name), &format!("keyspace {name:?}")).await
    }

    pub async fn update_keyspace(&self, ks: &Keyspace) -> Result<()> {
        self.write(&paths::keyspace(&ks.name), ks).await
    }

    pub async fn list_keyspaces(&self) -> Result<Vec<String>> {
        let mut out: Vec<String> = self
            .store
            .list(&paths::keyspaces_prefix())
            .await?
            .into_iter()
            .filter(|p| p.ends_with("/Keyspace"))
            .filter_map(|p| {
                p.strip_prefix("/keyspaces/")
                    .and_then(|r| r.strip_suffix("/Keyspace"))
                    .map(str::to_string)
            })
            .collect();
        out.sort();
        out.dedup();
        Ok(out)
    }

    pub async fn delete_keyspace(&self, name: &str) -> Result<()> {
        let shards = self.list_shards(name).await?;
        if !shards.is_empty() {
            return Err(Error::failed_precondition(format!(
                "keyspace {name:?} still has {} shard(s); delete them first",
                shards.len()
            )));
        }
        self.store.delete_prefix(&format!("/keyspaces/{name}/")).await
    }

    // -- shards --------------------------------------------------------------

    pub async fn create_shard(&self, shard: &Shard) -> Result<()> {
        // A shard whose range overlaps an existing one would make routing
        // ambiguous, so reject it here rather than discover it at query time.
        for existing in self.get_shards(&shard.keyspace).await? {
            if existing.name != shard.name && existing.key_range.intersects(&shard.key_range) {
                return Err(Error::failed_precondition(format!(
                    "shard {} overlaps existing shard {} in keyspace {}",
                    shard.name, existing.name, shard.keyspace
                )));
            }
        }
        self.create(&paths::shard(&shard.keyspace, &shard.name), shard).await
    }

    pub async fn get_shard(&self, ks: &str, shard: &str) -> Result<Shard> {
        self.read_required(&paths::shard(ks, shard), &format!("shard {ks}/{shard}")).await
    }

    pub async fn update_shard(&self, shard: &Shard) -> Result<()> {
        self.write(&paths::shard(&shard.keyspace, &shard.name), shard).await
    }

    pub async fn list_shards(&self, ks: &str) -> Result<Vec<String>> {
        Ok(self.get_shards(ks).await?.into_iter().map(|s| s.name).collect())
    }

    pub async fn get_shards(&self, ks: &str) -> Result<Vec<Shard>> {
        let mut out = Vec::new();
        for path in self.store.list(&paths::shards_prefix(ks)).await? {
            if !path.ends_with("/Shard") {
                continue;
            }
            if let Some(s) = self.read::<Shard>(&path).await? {
                out.push(s);
            }
        }
        // Key-range order, which is also the order the serving graph wants.
        out.sort_by(|a, b| a.key_range.start.cmp(&b.key_range.start));
        Ok(out)
    }

    pub async fn delete_shard(&self, ks: &str, shard: &str) -> Result<()> {
        self.store.delete(&paths::shard(ks, shard), None).await
    }

    // -- tablets -------------------------------------------------------------

    pub async fn create_tablet(&self, tablet: &Tablet) -> Result<()> {
        vituss_dialect_check(&tablet.backend.dialect)?;
        self.create(&paths::tablet(&tablet.alias.to_string()), tablet).await
    }

    pub async fn get_tablet(&self, alias: &TabletAlias) -> Result<Tablet> {
        self.read_required(&paths::tablet(&alias.to_string()), &format!("tablet {alias}")).await
    }

    pub async fn update_tablet(&self, tablet: &Tablet) -> Result<()> {
        self.write(&paths::tablet(&tablet.alias.to_string()), tablet).await
    }

    pub async fn delete_tablet(&self, alias: &TabletAlias) -> Result<()> {
        self.store.delete_prefix(&format!("/tablets/{alias}/")).await
    }

    pub async fn get_tablets(&self) -> Result<Vec<Tablet>> {
        let mut out = Vec::new();
        for path in self.store.list(&paths::tablets_prefix()).await? {
            if !path.ends_with("/Tablet") {
                continue;
            }
            if let Some(t) = self.read::<Tablet>(&path).await? {
                out.push(t);
            }
        }
        out.sort_by(|a, b| a.alias.cmp(&b.alias));
        Ok(out)
    }

    /// Tablets serving one shard, optionally restricted to a cell.
    pub async fn get_shard_tablets(&self, ks: &str, shard: &str, cell: Option<&str>) -> Result<Vec<Tablet>> {
        Ok(self
            .get_tablets()
            .await?
            .into_iter()
            .filter(|t| t.keyspace == ks && t.shard == shard)
            .filter(|t| cell.is_none_or(|c| t.alias.cell == c))
            .collect())
    }

    // -- serving graph -------------------------------------------------------

    pub async fn get_srv_keyspace(&self, cell: &str, ks: &str) -> Result<SrvKeyspace> {
        self.read_required(&paths::srv_keyspace(cell, ks), &format!("serving graph for {ks} in cell {cell}"))
            .await
    }

    pub async fn list_srv_keyspaces(&self, cell: &str) -> Result<Vec<String>> {
        Ok(self
            .store
            .list(&paths::srv_keyspaces_prefix(cell))
            .await?
            .into_iter()
            .filter(|p| p.ends_with("/SrvKeyspace"))
            .filter_map(|p| {
                p.strip_suffix("/SrvKeyspace")
                    .and_then(|r| r.rsplit('/').next())
                    .map(str::to_string)
            })
            .collect())
    }

    /// Recompute the serving graph for a keyspace from its shard records.
    ///
    /// The graph is derived, never hand-edited: that is what guarantees a
    /// half-finished reshard cannot be observed. Rebuilding is idempotent, so it
    /// is safe to call after any shard change.
    pub async fn rebuild_srv_keyspace(&self, ks: &str, cells: &[String]) -> Result<SrvKeyspace> {
        let shards = self.get_shards(ks).await?;
        if shards.is_empty() {
            return Err(Error::failed_precondition(format!("keyspace {ks:?} has no shards to serve")));
        }

        let mut partitions = Vec::new();
        for tablet_type in [TabletType::Primary, TabletType::Replica, TabletType::Rdonly] {
            let refs: Vec<ShardReference> = shards
                .iter()
                .filter(|s| s.is_serving(tablet_type))
                .map(|s| ShardReference { name: s.name.clone(), key_range: s.key_range.clone() })
                .collect();
            if refs.is_empty() {
                continue;
            }
            validate_coverage(ks, tablet_type, &refs)?;
            partitions.push(KeyspacePartition { tablet_type, shard_references: refs });
        }

        let srv = SrvKeyspace { partitions, served_from: None };
        for cell in cells {
            self.write(&paths::srv_keyspace(cell, ks), &srv).await?;
        }
        Ok(srv)
    }

    /// Rebuild every keyspace's serving graph in every cell.
    pub async fn rebuild_all(&self) -> Result<()> {
        let cells = self.list_cells().await?;
        for ks in self.list_keyspaces().await? {
            self.rebuild_srv_keyspace(&ks, &cells).await?;
        }
        Ok(())
    }

    // -- vschema -------------------------------------------------------------

    /// The VSchema is stored as opaque JSON so that `vituss-topo` does not need
    /// to depend on `vituss-vschema`, which in turn depends on the vindexes.
    pub async fn get_vschema_json(&self, ks: &str) -> Result<Option<serde_json::Value>> {
        self.read(&paths::vschema(ks)).await
    }

    pub async fn save_vschema_json(&self, ks: &str, vschema: &serde_json::Value) -> Result<()> {
        self.write(&paths::vschema(ks), vschema).await
    }

    // -- routing rules -------------------------------------------------------

    pub async fn get_routing_rules(&self) -> Result<RoutingRules> {
        Ok(self.read(&paths::routing_rules()).await?.unwrap_or_default())
    }

    pub async fn save_routing_rules(&self, rules: &RoutingRules) -> Result<()> {
        self.write(&paths::routing_rules(), rules).await
    }

    // -- locks and watches ---------------------------------------------------

    /// Take the keyspace lock. Every operation that changes the shard layout must
    /// hold it — resharding, primary election, serving-graph rebuilds.
    pub async fn lock_keyspace(&self, ks: &str, reason: &str) -> Result<Box<dyn LockHandle>> {
        self.store.lock(&format!("/keyspaces/{ks}"), reason).await
    }

    pub async fn lock_shard(&self, ks: &str, shard: &str, reason: &str) -> Result<Box<dyn LockHandle>> {
        self.store.lock(&format!("/keyspaces/{ks}/shards/{}", escape(shard)), reason).await
    }

    /// Watch the serving graph for a cell. The gate keeps a live copy from this.
    pub async fn watch_srv_keyspaces(&self, cell: &str) -> Result<tokio::sync::mpsc::Receiver<WatchEvent>> {
        self.store.watch(&paths::srv_keyspaces_prefix(cell)).await
    }

    pub async fn watch_tablets(&self) -> Result<tokio::sync::mpsc::Receiver<WatchEvent>> {
        self.store.watch(&paths::tablets_prefix()).await
    }
}

/// Reject a serving graph with a hole or an overlap in it.
///
/// Vitess discovers these at query time as a "no shard for keyspace id" error.
/// Catching it during the rebuild means a bad reshard fails at the operation that
/// caused it, next to the person who can fix it.
fn validate_coverage(ks: &str, tablet_type: TabletType, refs: &[ShardReference]) -> Result<()> {
    if refs.len() == 1 && refs[0].key_range.is_full() {
        return Ok(());
    }
    let mut sorted: Vec<&ShardReference> = refs.iter().collect();
    sorted.sort_by(|a, b| a.key_range.start.cmp(&b.key_range.start));

    if !sorted[0].key_range.start.is_empty() {
        return Err(Error::failed_precondition(format!(
            "keyspace {ks} ({tablet_type}): no shard covers the start of the key range; \
             the lowest is {}",
            sorted[0].name
        )));
    }
    if !sorted[sorted.len() - 1].key_range.end.is_empty() {
        return Err(Error::failed_precondition(format!(
            "keyspace {ks} ({tablet_type}): no shard covers the end of the key range; \
             the highest is {}",
            sorted[sorted.len() - 1].name
        )));
    }
    for w in sorted.windows(2) {
        if w[0].key_range.end != w[1].key_range.start {
            return Err(Error::failed_precondition(format!(
                "keyspace {ks} ({tablet_type}): shards {} and {} do not meet — \
                 {} ends at {} but {} starts at {}",
                w[0].name,
                w[1].name,
                w[0].name,
                hex(&w[0].key_range.end),
                w[1].name,
                hex(&w[1].key_range.start),
            )));
        }
    }
    Ok(())
}

fn hex(b: &[u8]) -> String {
    if b.is_empty() {
        "(unbounded)".to_string()
    } else {
        b.iter().map(|x| format!("{x:02x}")).collect()
    }
}

/// Check the named dialect is registered.
///
/// `vituss-topo` deliberately does not depend on `vituss-dialect`, so this is a
/// hook the binaries install at startup. Until then it accepts everything, which
/// keeps the topology usable by tools that do not link the dialects.
fn vituss_dialect_check(name: &str) -> Result<()> {
    match DIALECT_VALIDATOR.get() {
        Some(f) => f(name),
        None => Ok(()),
    }
}

static DIALECT_VALIDATOR: std::sync::OnceLock<fn(&str) -> Result<()>> = std::sync::OnceLock::new();

/// Install the dialect-name validator. Called once at process start.
pub fn set_dialect_validator(f: fn(&str) -> Result<()>) {
    let _ = DIALECT_VALIDATOR.set(f);
}

/// Convenience: the key range a shard name denotes.
pub fn key_range_of(shard: &str) -> Result<KeyRange> {
    KeyRange::parse(shard)
}
