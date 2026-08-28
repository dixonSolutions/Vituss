//! The records stored in the topology.
//!
//! These are the cluster's source of truth: what keyspaces exist, how they are
//! sharded, which tablets serve which shard, and — the part Vitess has no
//! equivalent for — *which database engine* each tablet is backed by.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use vituss_core::{KeyRange, TabletAlias, TabletType};

/// Re-exported from `vituss-core`: both the topology and the drivers need it.
pub use vituss_core::BackendConfig;

/// What kind of keyspace this is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KeyspaceType {
    #[default]
    Normal,
    /// A point-in-time copy of another keyspace, used to build a new one before
    /// cutting traffic over.
    Snapshot,
}

/// A keyspace: one logical database, possibly spread over many shards.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Keyspace {
    pub name: String,
    #[serde(default)]
    pub keyspace_type: KeyspaceType,
    /// Source keyspace, for snapshots.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_keyspace: Option<String>,
    /// Default engine for shards that do not name one. Also decides which
    /// grammar the gate uses to parse queries aimed at this keyspace.
    pub dialect: String,
    /// How many replicas must acknowledge a write before a primary may be
    /// considered durable enough to fail over from.
    #[serde(default = "default_durability")]
    pub durability_policy: String,
    /// Schema Vituss uses for its own bookkeeping inside each shard
    /// (2PC transaction log, VReplication state, sequence tables).
    #[serde(default = "default_sidecar")]
    pub sidecar_database: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

fn default_durability() -> String {
    "semi_sync".to_string()
}
fn default_sidecar() -> String {
    "_vt".to_string()
}

impl Keyspace {
    pub fn new(name: impl Into<String>, dialect: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            keyspace_type: KeyspaceType::Normal,
            base_keyspace: None,
            dialect: dialect.into(),
            durability_policy: default_durability(),
            sidecar_database: default_sidecar(),
            description: None,
        }
    }
}

/// A source shard being copied from, during a reshard or a move.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SourceShard {
    pub uid: u32,
    pub keyspace: String,
    pub shard: String,
    #[serde(default)]
    pub key_range: KeyRange,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tables: Vec<String>,
}

/// Per-tablet-type serving overrides, used to stop serving a shard mid-cutover.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TabletControl {
    pub tablet_type: TabletType,
    /// Stop answering queries of this type from this shard.
    #[serde(default)]
    pub denied: bool,
    /// Tables that are specifically denied, for a partial move.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub denied_tables: Vec<String>,
}

/// A shard: one key range of a keyspace.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Shard {
    pub keyspace: String,
    /// Canonical name, which *is* the key range (`-80`, `80-`, `-`).
    pub name: String,
    #[serde(default)]
    pub key_range: KeyRange,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary_alias: Option<TabletAlias>,
    /// Unix seconds when the current primary took over. Used to break ties when
    /// two tablets both believe they are primary.
    #[serde(default)]
    pub primary_term_start: i64,
    /// False while a reshard is in progress and this shard's data has been
    /// handed to its successors.
    #[serde(default = "default_true")]
    pub is_primary_serving: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub source_shards: Vec<SourceShard>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tablet_controls: Vec<TabletControl>,
    /// Engine override for this shard. Absent means the keyspace default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dialect: Option<String>,
}

fn default_true() -> bool {
    true
}

impl Shard {
    pub fn new(keyspace: impl Into<String>, name: impl Into<String>) -> vituss_core::Result<Self> {
        let name = name.into();
        let key_range = KeyRange::parse(&name)?;
        Ok(Self {
            keyspace: keyspace.into(),
            name,
            key_range,
            primary_alias: None,
            primary_term_start: 0,
            is_primary_serving: true,
            source_shards: Vec::new(),
            tablet_controls: Vec::new(),
            dialect: None,
        })
    }

    /// True when a query of this type may be sent to this shard.
    pub fn is_serving(&self, tablet_type: TabletType) -> bool {
        if tablet_type == TabletType::Primary && !self.is_primary_serving {
            return false;
        }
        !self
            .tablet_controls
            .iter()
            .any(|c| c.tablet_type == tablet_type && c.denied && c.denied_tables.is_empty())
    }
}

/// A tablet: one database server plus the Vituss process in front of it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Tablet {
    pub alias: TabletAlias,
    pub hostname: String,
    /// Named ports the tablet listens on, e.g. `{"grpc": 15991, "http": 15100}`.
    #[serde(default)]
    pub port_map: BTreeMap<String, u16>,
    pub keyspace: String,
    pub shard: String,
    #[serde(default)]
    pub key_range: KeyRange,
    #[serde(default)]
    pub tablet_type: TabletType,
    /// How this tablet reaches its database. Carries the engine, so the gate can
    /// render each shard's SQL for the engine that shard actually runs.
    pub backend: BackendConfig,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tags: BTreeMap<String, String>,
}

impl Tablet {
    pub fn address(&self) -> String {
        match self.port_map.get("grpc") {
            Some(p) => format!("{}:{}", self.hostname, p),
            None => self.hostname.clone(),
        }
    }
}

/// One shard as it appears in the serving graph.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ShardReference {
    pub name: String,
    #[serde(default)]
    pub key_range: KeyRange,
}

/// The set of shards serving one tablet type.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KeyspacePartition {
    pub tablet_type: TabletType,
    pub shard_references: Vec<ShardReference>,
}

/// The serving graph for one keyspace in one cell.
///
/// This is the hot-path structure: the gate reads it on every query to turn a
/// keyspace ID into a shard name. It is derived from the shard records rather
/// than edited directly, so that a half-finished reshard never becomes visible.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct SrvKeyspace {
    pub partitions: Vec<KeyspacePartition>,
    /// When set, queries for this keyspace are answered from another one.
    /// Used to make a keyspace rename or a move atomic from the client's view.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub served_from: Option<BTreeMap<String, String>>,
}

impl SrvKeyspace {
    pub fn partition(&self, tablet_type: TabletType) -> Option<&KeyspacePartition> {
        self.partitions.iter().find(|p| p.tablet_type == tablet_type)
    }

    /// Shards serving this tablet type, in key-range order.
    pub fn shards(&self, tablet_type: TabletType) -> &[ShardReference] {
        self.partition(tablet_type).map(|p| p.shard_references.as_slice()).unwrap_or(&[])
    }
}

/// A cell (failure domain / data centre).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CellInfo {
    pub name: String,
    /// Address of the topology store serving this cell, when cells have their own.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topo_address: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
}
