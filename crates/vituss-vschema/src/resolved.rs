//! The runtime VSchema: specs with their vindexes instantiated and cross
//! references resolved.

use std::collections::HashMap;
use std::sync::Arc;

use vituss_core::KeyspaceId;
use vituss_vindex::VindexRef;

use crate::model::{ColumnSpec, TableKind};

/// A fully-qualified table name.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TableName {
    pub keyspace: String,
    pub table: String,
}

impl TableName {
    pub fn new(keyspace: impl Into<String>, table: impl Into<String>) -> Self {
        Self { keyspace: keyspace.into(), table: table.into() }
    }

    /// Parse `keyspace.table`, or `table` against a default keyspace.
    pub fn parse(qualified: &str, default_keyspace: Option<&str>) -> vituss_core::Result<Self> {
        match qualified.split_once('.') {
            Some((ks, t)) => Ok(Self::new(ks.trim(), t.trim())),
            None => default_keyspace
                .map(|ks| Self::new(ks, qualified.trim()))
                .ok_or_else(|| {
                    vituss_core::Error::invalid(format!(
                        "table {qualified:?} is not qualified and no keyspace is selected"
                    ))
                }),
        }
    }
}

impl std::fmt::Display for TableName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.keyspace, self.table)
    }
}

/// A vindex bound to specific columns of a table.
#[derive(Clone)]
pub struct ColumnVindex {
    pub columns: Vec<String>,
    pub vindex: VindexRef,
    /// True when this table is responsible for keeping the vindex's rows up to
    /// date. Only owned lookup vindexes are written on INSERT/DELETE.
    pub owned: bool,
}

impl std::fmt::Debug for ColumnVindex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ColumnVindex")
            .field("columns", &self.columns)
            .field("vindex", &self.vindex.name())
            .field("kind", &self.vindex.kind())
            .field("owned", &self.owned)
            .finish()
    }
}

/// Where a table's generated ids come from.
#[derive(Debug, Clone)]
pub struct AutoIncrement {
    pub column: String,
    pub sequence: TableName,
}

/// A table as the planner sees it.
#[derive(Debug, Clone)]
pub struct Table {
    pub name: String,
    pub keyspace: String,
    pub kind: TableKind,
    /// Whether the keyspace this table lives in is sharded.
    pub sharded: bool,
    /// Every vindex on the table, cheapest first — so the planner picks the best
    /// available route by taking the first one whose columns are constrained.
    pub column_vindexes: Vec<ColumnVindex>,
    /// The vindex that decides which shard a row is stored on. Every sharded
    /// table has exactly one; it must be unique and it must be first.
    pub primary_vindex: Option<ColumnVindex>,
    pub auto_increment: Option<AutoIncrement>,
    pub columns: Vec<ColumnSpec>,
    pub column_list_authoritative: bool,
    pub primary_key: Vec<String>,
    /// For a reference table, the keyspace holding the authoritative copy.
    pub source: Option<TableName>,
    /// For a table pinned to one keyspace ID rather than sharded.
    pub pinned: Option<KeyspaceId>,
}

impl Table {
    pub fn qualified_name(&self) -> TableName {
        TableName::new(&self.keyspace, &self.name)
    }

    /// Vindexes this table maintains rows for.
    pub fn owned_vindexes(&self) -> impl Iterator<Item = &ColumnVindex> {
        self.column_vindexes.iter().filter(|cv| cv.owned)
    }

    /// A reference table can be read from any shard, so a join against it never
    /// needs to cross shards.
    pub fn is_reference(&self) -> bool {
        self.kind == TableKind::Reference
    }

    pub fn is_sequence(&self) -> bool {
        self.kind == TableKind::Sequence
    }

    /// The vindex covering exactly these columns, if any.
    pub fn vindex_for(&self, columns: &[String]) -> Option<&ColumnVindex> {
        self.column_vindexes.iter().find(|cv| {
            cv.columns.len() == columns.len()
                && cv
                    .columns
                    .iter()
                    .zip(columns)
                    .all(|(a, b)| a.eq_ignore_ascii_case(b))
        })
    }

    /// The best vindex whose columns are all present in `available`.
    ///
    /// "Best" is lowest cost, then most columns matched: a two-column vindex that
    /// is fully constrained beats a one-column one, because it narrows further.
    pub fn best_vindex<'a>(&'a self, available: &[String]) -> Option<&'a ColumnVindex> {
        self.column_vindexes
            .iter()
            .filter(|cv| {
                cv.columns
                    .iter()
                    .all(|c| available.iter().any(|a| a.eq_ignore_ascii_case(c)))
            })
            .min_by_key(|cv| (cv.vindex.cost(), usize::MAX - cv.columns.len()))
    }
}

/// One keyspace's resolved schema.
#[derive(Debug, Clone)]
pub struct Keyspace {
    pub name: String,
    pub sharded: bool,
    /// Engine this keyspace runs on. The gate uses it to choose a grammar.
    pub dialect: String,
    pub tables: HashMap<String, Arc<Table>>,
    pub vindexes: HashMap<String, VindexRef>,
    pub require_explicit_routing: bool,
}

impl Keyspace {
    pub fn table(&self, name: &str) -> Option<&Arc<Table>> {
        self.tables
            .get(name)
            .or_else(|| self.tables.iter().find(|(k, _)| k.eq_ignore_ascii_case(name)).map(|(_, v)| v))
    }
}
