//! The on-disk VSchema format.
//!
//! Deliberately close to Vitess's, so an existing `vschema.json` can be used
//! unchanged, with two additions: a table may declare its column types (useful
//! before schema tracking has run) and a keyspace may name its engine.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// One vindex definition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VindexSpec {
    /// Registered vindex kind: `hash`, `xxhash`, `lookup_unique`, …
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub params: BTreeMap<String, String>,
    /// Table that owns this vindex's rows. An owned lookup vindex is maintained
    /// automatically: inserting into the owner inserts the lookup row too.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
}

/// A vindex applied to one or more columns of a table.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnVindexSpec {
    /// Vindex name, as defined in the keyspace's `vindexes` map.
    pub name: String,
    /// Single-column form.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub column: Option<String>,
    /// Multi-column form. Takes precedence over `column`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<String>,
}

impl ColumnVindexSpec {
    pub fn column_names(&self) -> Vec<String> {
        if !self.columns.is_empty() {
            self.columns.clone()
        } else {
            self.column.iter().cloned().collect()
        }
    }
}

/// Where a table's generated primary keys come from.
///
/// Auto-increment does not work across shards — every shard would generate the
/// same values — so a sharded table draws its ids from a sequence table in an
/// unsharded keyspace instead.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AutoIncrementSpec {
    pub column: String,
    /// Sequence table, `keyspace.table` or just `table` in this keyspace.
    pub sequence: String,
}

/// How a table participates in sharding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TableKind {
    /// A normal sharded or unsharded table.
    #[default]
    Normal,
    /// A small table copied to every shard, so it can be joined locally instead
    /// of forcing a cross-shard join.
    Reference,
    /// A sequence generator table living in an unsharded keyspace.
    Sequence,
}

/// A declared column. Optional: schema tracking fills these in from the live
/// database, and this is only the fallback for tables Vituss has not seen yet.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnSpec {
    pub name: String,
    #[serde(rename = "type")]
    pub sql_type: vituss_core::SqlType,
    #[serde(default)]
    pub nullable: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TableSpec {
    #[serde(rename = "type", default)]
    pub kind: TableKind,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub column_vindexes: Vec<ColumnVindexSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auto_increment: Option<AutoIncrementSpec>,
    /// For a reference table: the authoritative copy, as `keyspace.table`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<ColumnSpec>,
    /// When true, `columns` is the complete list and the planner may expand
    /// `SELECT *` without asking a shard.
    #[serde(default)]
    pub column_list_authoritative: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub primary_key: Vec<String>,
}

/// One keyspace's VSchema.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct KeyspaceSpec {
    #[serde(default)]
    pub sharded: bool,
    /// Engine backing this keyspace. Optional here because the topology already
    /// records it; when both are present they must agree.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dialect: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub vindexes: BTreeMap<String, VindexSpec>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tables: BTreeMap<String, TableSpec>,
    /// Allow queries that Vituss cannot route, by sending the raw SQL to every
    /// shard. Off by default: a scatter that silently returns a partial answer is
    /// worse than an error.
    #[serde(default)]
    pub require_explicit_routing: bool,
}

/// The whole cluster's VSchema: every keyspace, keyed by name.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct VSchemaSpec {
    #[serde(default)]
    pub keyspaces: BTreeMap<String, KeyspaceSpec>,
    /// Table-level routing overrides, `from` → `to`, used during moves.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub routing_rules: BTreeMap<String, Vec<String>>,
}

impl VSchemaSpec {
    pub fn from_json(s: &str) -> vituss_core::Result<Self> {
        serde_json::from_str(s).map_err(|e| vituss_core::Error::invalid(format!("invalid VSchema JSON: {e}")))
    }
    pub fn from_yaml(s: &str) -> vituss_core::Result<Self> {
        serde_yaml::from_str(s).map_err(|e| vituss_core::Error::invalid(format!("invalid VSchema YAML: {e}")))
    }
}
