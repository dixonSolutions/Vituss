//! `multicol` — a composite vindex over several columns.
//!
//! Each column contributes a fixed slice of the keyspace ID, so a query that
//! constrains only the *leading* columns still narrows to a key range instead of
//! scattering. That is the whole point: it gives you a compound sharding key with
//! useful prefix behaviour.

use std::sync::Arc;

use async_trait::async_trait;

use vituss_core::{Error, KeyRange, KeyspaceId, Result, ShardDestination, Value};

use crate::cursor::VCursor;
use crate::vindex::{required, validate_params, Vindex, VindexParams, VindexRow};

const KNOWN_PARAMS: &[&str] = &["column_count", "column_bytes", "column_vindex"];

/// Total keyspace-ID width. Fixed at 8 bytes to match every other vindex, so a
/// keyspace can mix `multicol` and `hash` tables and share shard boundaries.
const KSID_LEN: usize = 8;

pub struct MultiCol {
    name: String,
    /// Per-column sub-vindex used to hash that column's value.
    columns: Vec<Arc<dyn Vindex>>,
    /// How many bytes of each sub-hash land in the keyspace ID. Sums to 8.
    widths: Vec<usize>,
}

impl MultiCol {
    pub fn new(name: &str, params: &VindexParams) -> Result<Self> {
        validate_params("multicol", params, KNOWN_PARAMS)?;
        let count: usize = required("multicol", params, "column_count")?
            .parse()
            .map_err(|_| Error::invalid("multicol: 'column_count' must be a number"))?;
        if count == 0 || count > KSID_LEN {
            return Err(Error::invalid(format!(
                "multicol: 'column_count' must be between 1 and {KSID_LEN}, got {count}"
            )));
        }

        let widths = match params.get("column_bytes") {
            Some(spec) => {
                let w: Vec<usize> = spec
                    .split(',')
                    .map(|s| {
                        s.trim()
                            .parse()
                            .map_err(|_| Error::invalid(format!("multicol: invalid byte width {s:?}")))
                    })
                    .collect::<Result<_>>()?;
                if w.len() != count {
                    return Err(Error::invalid(format!(
                        "multicol: 'column_bytes' lists {} widths but 'column_count' is {count}",
                        w.len()
                    )));
                }
                if w.iter().sum::<usize>() != KSID_LEN {
                    return Err(Error::invalid(format!(
                        "multicol: 'column_bytes' must sum to {KSID_LEN}, got {}",
                        w.iter().sum::<usize>()
                    )));
                }
                w
            }
            // Default: split evenly, giving any remainder to the leading columns
            // so the most selective prefix gets the most resolution.
            None => {
                let base = KSID_LEN / count;
                let extra = KSID_LEN % count;
                (0..count).map(|i| base + usize::from(i < extra)).collect()
            }
        };

        let kinds: Vec<String> = match params.get("column_vindex") {
            Some(spec) => spec.split(',').map(|s| s.trim().to_string()).collect(),
            None => vec!["hash".to_string(); count],
        };
        if kinds.len() != count {
            return Err(Error::invalid(format!(
                "multicol: 'column_vindex' lists {} vindexes but 'column_count' is {count}",
                kinds.len()
            )));
        }

        let columns = kinds
            .iter()
            .enumerate()
            .map(|(i, k)| {
                let sub = crate::registry::create(k, &format!("{name}_col{i}"), &VindexParams::new())?;
                if sub.hash(&Value::Int(0)).is_none() {
                    return Err(Error::invalid(format!(
                        "multicol: sub-vindex {k:?} cannot be used for a column because it exposes no hash"
                    )));
                }
                Ok(sub)
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Self { name: name.to_string(), columns, widths })
    }

    /// Build the keyspace ID from however many leading columns are present.
    ///
    /// Trailing bytes are left unset, which is exactly what makes a partial key
    /// a *range*: everything sharing the known prefix.
    fn compose(&self, row: &VindexRow) -> Result<Vec<u8>> {
        let mut out = Vec::with_capacity(KSID_LEN);
        for (i, value) in row.iter().enumerate().take(self.columns.len()) {
            let hashed = self.columns[i]
                .hash(value)
                .ok_or_else(|| Error::internal("multicol sub-vindex lost its hash"))??;
            let w = self.widths[i];
            let mut part = hashed;
            part.resize(w, 0);
            out.extend_from_slice(&part[..w]);
        }
        Ok(out)
    }
}

#[async_trait]
impl Vindex for MultiCol {
    fn name(&self) -> &str {
        &self.name
    }
    fn kind(&self) -> &'static str {
        "multicol"
    }
    fn cost(&self) -> u32 {
        1
    }
    fn is_unique(&self) -> bool {
        true
    }
    fn column_count(&self) -> usize {
        self.columns.len()
    }
    fn known_params(&self) -> &'static [&'static str] {
        KNOWN_PARAMS
    }

    async fn map(&self, _c: Option<&dyn VCursor>, rows: &[VindexRow]) -> Result<Vec<ShardDestination>> {
        rows.iter()
            .map(|row| {
                let prefix = self.compose(row)?;
                Ok(if prefix.len() == KSID_LEN {
                    ShardDestination::KeyspaceId(KeyspaceId(prefix))
                } else {
                    // Partial key: every keyspace ID starting with this prefix.
                    let mut end = prefix.clone();
                    while let Some(b) = end.pop() {
                        if b != 0xff {
                            end.push(b + 1);
                            break;
                        }
                    }
                    ShardDestination::KeyRange(KeyRange::new(prefix, end))
                })
            })
            .collect()
    }

    async fn verify(&self, _c: Option<&dyn VCursor>, rows: &[VindexRow], ksids: &[KeyspaceId]) -> Result<Vec<bool>> {
        rows.iter()
            .zip(ksids)
            .map(|(row, k)| Ok(self.compose(row)? == k.0))
            .collect()
    }

    /// A `multicol` vindex accepts a prefix of its columns, which is what lets a
    /// query constraining only the first column still avoid a scatter.
    fn accepts_partial_columns(&self) -> bool {
        true
    }
}

pub(crate) fn register(m: &mut std::collections::HashMap<&'static str, crate::registry::VindexBuilder>) {
    m.insert("multicol", |n, p| Ok(Arc::new(MultiCol::new(n, p)?)));
}
