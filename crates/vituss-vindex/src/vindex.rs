//! The [`Vindex`] trait.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;

use vituss_core::{Error, KeyspaceId, Result, ShardDestination, Value};

use crate::cursor::VCursor;

/// Parameters from the VSchema, e.g. `{"table": "user_lookup", "from": "id", "to": "keyspace_id"}`.
pub type VindexParams = BTreeMap<String, String>;

/// A shareable vindex instance.
pub type VindexRef = Arc<dyn Vindex>;

/// One row's worth of vindex input: one value per vindex column.
///
/// Single-column vindexes get a one-element slice. Modelling both cases the same
/// way is a deliberate simplification over Vitess, which needs two traits and a
/// runtime downcast to tell them apart.
pub type VindexRow = Vec<Value>;

#[async_trait]
pub trait Vindex: Send + Sync {
    /// Instance name from the VSchema (`user_index`), not the kind.
    fn name(&self) -> &str;

    /// Kind, as registered (`hash`, `lookup_unique`).
    fn kind(&self) -> &'static str;

    /// How expensive this vindex is, used by the planner to choose between
    /// several usable vindexes on the same table.
    ///
    /// 0 — the value already *is* the keyspace ID.
    /// 1 — a pure computation.
    /// ≥2 — requires a lookup query.
    fn cost(&self) -> u32;

    /// True when one input maps to exactly one keyspace ID. Only unique vindexes
    /// may be a table's primary vindex.
    fn is_unique(&self) -> bool;

    /// Number of columns this vindex consumes.
    fn column_count(&self) -> usize {
        1
    }

    /// True when the vindex can map a *prefix* of its columns.
    ///
    /// Composite vindexes that lay their columns out in order can answer a query
    /// that constrains only the leading ones, narrowing to a key range instead of
    /// scattering. The planner checks this before it tries.
    fn accepts_partial_columns(&self) -> bool {
        false
    }

    /// True when [`Vindex::map`] needs a live [`VCursor`].
    ///
    /// Such vindexes cannot be used by VReplication, which has no session to
    /// execute in.
    fn needs_cursor(&self) -> bool {
        false
    }

    /// Map input rows to destinations.
    async fn map(&self, cursor: Option<&dyn VCursor>, rows: &[VindexRow]) -> Result<Vec<ShardDestination>>;

    /// Check that each row really does map to the given keyspace ID.
    ///
    /// Used on UPDATE/INSERT to reject a row whose sharding column disagrees with
    /// the shard it is being written to.
    async fn verify(
        &self,
        cursor: Option<&dyn VCursor>,
        rows: &[VindexRow],
        ksids: &[KeyspaceId],
    ) -> Result<Vec<bool>>;

    /// Record a new mapping. Only *owned* lookup vindexes implement this; for
    /// functional vindexes there is nothing to store.
    async fn create(
        &self,
        _cursor: &dyn VCursor,
        _rows: &[VindexRow],
        _ksids: &[KeyspaceId],
        _ignore_on_duplicate: bool,
    ) -> Result<()> {
        Ok(())
    }

    /// Remove a mapping.
    async fn delete(&self, _cursor: &dyn VCursor, _rows: &[VindexRow], _ksid: &KeyspaceId) -> Result<()> {
        Ok(())
    }

    /// Replace a mapping in place.
    async fn update(
        &self,
        cursor: &dyn VCursor,
        old_row: &VindexRow,
        ksid: &KeyspaceId,
        new_row: &VindexRow,
    ) -> Result<()> {
        self.delete(cursor, std::slice::from_ref(old_row), ksid).await?;
        self.create(cursor, std::slice::from_ref(new_row), std::slice::from_ref(ksid), false).await
    }

    /// Recover the original value from a keyspace ID, when the vindex is
    /// invertible. Lets the gate fill in a sharding column the client omitted.
    fn reverse_map(&self, _ksids: &[KeyspaceId]) -> Option<Result<Vec<Value>>> {
        None
    }

    /// The raw hash of a single value, when this vindex exposes one.
    /// Multi-column vindexes compose their columns out of these.
    fn hash(&self, _value: &Value) -> Option<Result<Vec<u8>>> {
        None
    }

    /// Map a contiguous `BETWEEN lo AND hi` range to a destination, when the
    /// vindex preserves order. Turns a range scan into one key range instead of
    /// a scatter.
    fn range_map(&self, _lo: &Value, _hi: &Value) -> Option<Result<ShardDestination>> {
        None
    }

    /// Map a value prefix to a destination, for vindexes like `cfc` whose
    /// keyspace IDs share a prefix when the inputs do.
    fn prefix_map(&self, _prefix: &Value) -> Option<Result<ShardDestination>> {
        None
    }

    /// VSchema params this vindex understands. Anything else is reported as a
    /// configuration mistake rather than silently ignored.
    fn known_params(&self) -> &'static [&'static str] {
        &[]
    }
}

impl std::fmt::Debug for dyn Vindex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}({})", self.kind(), self.name())
    }
}

/// Reject params the vindex does not understand.
///
/// Vitess only warns about these; Vituss refuses, because a typo in a vindex
/// param silently changes how data is sharded.
pub fn validate_params(kind: &str, params: &VindexParams, known: &[&str]) -> Result<()> {
    let unknown: Vec<&str> = params
        .keys()
        .map(String::as_str)
        .filter(|k| !known.contains(k))
        .collect();
    if unknown.is_empty() {
        return Ok(());
    }
    Err(Error::invalid(format!(
        "vindex {kind:?}: unknown parameter(s) {}; known parameters are {}",
        unknown.join(", "),
        if known.is_empty() { "(none)".to_string() } else { known.join(", ") }
    )))
}

/// Fetch a required param.
pub fn required<'a>(kind: &str, params: &'a VindexParams, key: &str) -> Result<&'a str> {
    params
        .get(key)
        .map(String::as_str)
        .ok_or_else(|| Error::invalid(format!("vindex {kind:?} requires the {key:?} parameter")))
}

/// Fetch a boolean param, defaulting when absent.
pub fn bool_param(params: &VindexParams, key: &str, default: bool) -> bool {
    params
        .get(key)
        .map(|v| matches!(v.to_ascii_lowercase().as_str(), "true" | "1" | "yes"))
        .unwrap_or(default)
}
