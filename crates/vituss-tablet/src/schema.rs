//! Schema tracking.
//!
//! The planner works from the VSchema, which says how tables are *sharded*. It
//! also needs to know what columns a table has — to expand `SELECT *`, to check
//! that an INSERT names real columns, to know which predicates hit a unique key.
//! That comes from the database itself, through the dialect's introspection
//! queries, because only the database knows what is actually there.

use vituss_core::SqlType;

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ColumnSchema {
    pub name: String,
    pub ordinal: u32,
    /// The engine's own type name, kept verbatim for diagnostics.
    pub native_type: String,
    pub sql_type: SqlType,
    pub nullable: bool,
    pub default: Option<String>,
    /// True when the engine generates this column's value.
    pub auto_generated: bool,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct IndexSchema {
    pub name: String,
    pub columns: Vec<String>,
    pub unique: bool,
    pub primary: bool,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TableSchema {
    pub name: String,
    pub is_view: bool,
    pub columns: Vec<ColumnSchema>,
    pub indexes: Vec<IndexSchema>,
}

impl TableSchema {
    pub fn column(&self, name: &str) -> Option<&ColumnSchema> {
        self.columns.iter().find(|c| c.name.eq_ignore_ascii_case(name))
    }

    /// The primary key's columns, in order.
    pub fn primary_key(&self) -> Vec<String> {
        self.indexes
            .iter()
            .find(|i| i.primary)
            .map(|i| i.columns.clone())
            .unwrap_or_default()
    }

    /// True when every column of some unique index is constrained — which is how
    /// the planner knows a predicate selects at most one row.
    pub fn is_uniquely_constrained(&self, constrained: &[String]) -> bool {
        self.indexes.iter().filter(|i| i.unique).any(|i| {
            i.columns
                .iter()
                .all(|c| constrained.iter().any(|k| k.eq_ignore_ascii_case(c)))
        })
    }
}
