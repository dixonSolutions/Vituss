//! Result sets.

use crate::value::{SqlType, Value};

/// Column metadata for one output column.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Field {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    pub sql_type: SqlType,
    #[serde(default = "default_true")]
    pub nullable: bool,
    /// The engine's own type name (`bigint unsigned`, `int4`, `nvarchar`), kept so
    /// that a client asking for metadata sees what the shard actually reported.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_type: Option<String>,
}

fn default_true() -> bool {
    true
}

impl Field {
    pub fn new(name: impl Into<String>, sql_type: SqlType) -> Self {
        Self {
            name: name.into(),
            table: None,
            schema: None,
            sql_type,
            nullable: true,
            native_type: None,
        }
    }
}

pub type Row = Vec<Value>;

/// The result of executing one statement.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct QueryResult {
    pub fields: Vec<Field>,
    pub rows: Vec<Row>,
    pub rows_affected: u64,
    /// Present only when the statement generated one. PostgreSQL has no native
    /// equivalent, so the Postgres backend fills this from `RETURNING` when the
    /// planner injected it.
    pub last_insert_id: Option<u64>,
    pub info: Option<String>,
    /// Non-fatal diagnostics gathered from shards (`SHOW WARNINGS` equivalents).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

impl QueryResult {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn affected(n: u64) -> Self {
        Self { rows_affected: n, ..Default::default() }
    }

    pub fn from_rows(fields: Vec<Field>, rows: Vec<Row>) -> Self {
        Self { fields, rows, ..Default::default() }
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Index of a column by name, case-insensitively (all three engines fold
    /// unquoted identifiers, though not to the same case).
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.fields.iter().position(|f| f.name.eq_ignore_ascii_case(name))
    }

    /// Append `other`'s rows to `self`. Used by scatter gather.
    ///
    /// Field metadata is taken from whichever result first reported some, since a
    /// shard that matched no rows may legitimately return no field list.
    pub fn append(&mut self, mut other: QueryResult) {
        if self.fields.is_empty() {
            self.fields = std::mem::take(&mut other.fields);
        }
        self.rows.append(&mut other.rows);
        self.rows_affected += other.rows_affected;
        if self.last_insert_id.is_none() {
            self.last_insert_id = other.last_insert_id;
        }
        self.warnings.extend(other.warnings);
    }

    /// Truncate to the first `n` columns.
    ///
    /// The planner adds columns that the client never asked for — ORDER BY keys
    /// pulled up for a cross-shard merge sort, vindex columns needed to route a
    /// DML. They are stripped here before the result leaves the gate.
    pub fn truncate_columns(&mut self, n: usize) {
        if n == 0 || n >= self.fields.len() {
            return;
        }
        self.fields.truncate(n);
        for row in &mut self.rows {
            row.truncate(n);
        }
    }
}
