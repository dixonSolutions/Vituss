//! Lookup vindexes: the mapping lives in a table instead of in a function.
//!
//! A lookup vindex lets you route by a column that is not the sharding key —
//! `SELECT * FROM user WHERE email = ?` on a keyspace sharded by `user_id`. The
//! price is a round trip to the lookup table before the real query can be routed.
//!
//! The lookup table is itself a Vituss table in some keyspace, so the query goes
//! back through the gate via a [`VCursor`] rather than to a fixed database. That
//! is what allows the lookup table to be sharded, and to live on a different
//! engine than the table it indexes.

use std::sync::Arc;

use async_trait::async_trait;

use vituss_core::{BindVars, Error, KeyspaceId, Result, ShardDestination, Value};

use crate::cursor::{CommitOrder, VCursor};
use crate::vindex::{bool_param, required, validate_params, Vindex, VindexParams, VindexRow};

const KNOWN_PARAMS: &[&str] = &[
    "table",
    "from",
    "to",
    "autocommit",
    "multi_shard_autocommit",
    "write_only",
    "no_verify",
    "batch_lookup",
    "ignore_nulls",
    "read_lock",
    // Injected by the VSchema loader from the vindex's `owner:` field. A lookup
    // vindex with an owner is maintained by Vituss on the owner's writes; one
    // without is read-only, pointing at a table someone else keeps up to date.
    "owner",
];

/// How the `to` column is interpreted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ToEncoding {
    /// The column stores the keyspace ID bytes directly.
    Raw,
    /// The column stores a number that must be run through the `hash` vindex to
    /// become a keyspace ID. Used when the lookup table points at a keyspace
    /// sharded by `hash`, so the stored value stays human-readable.
    Hashed,
}

struct LookupInternal {
    name: String,
    table: String,
    /// One name per vindex column.
    from: Vec<String>,
    to: String,
    to_encoding: ToEncoding,
    unique: bool,
    autocommit: bool,
    /// While a lookup vindex is being backfilled, `map` must not be trusted:
    /// the table is incomplete, so every query scatters instead of silently
    /// missing rows.
    write_only: bool,
    no_verify: bool,
    ignore_nulls: bool,
    /// `consistent_lookup` writes the lookup row on the shard that owns it and
    /// keeps it consistent with the owning row through pre/post sessions.
    consistent: bool,
}

impl LookupInternal {
    fn parse(kind: &'static str, name: &str, params: &VindexParams, unique: bool, to_encoding: ToEncoding, consistent: bool) -> Result<Self> {
        validate_params(kind, params, KNOWN_PARAMS)?;
        let from: Vec<String> = required(kind, params, "from")?
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        if from.is_empty() {
            return Err(Error::invalid(format!("vindex {name:?}: 'from' must name at least one column")));
        }
        Ok(Self {
            name: name.to_string(),
            table: required(kind, params, "table")?.to_string(),
            from,
            to: required(kind, params, "to")?.to_string(),
            to_encoding,
            unique,
            autocommit: bool_param(params, "autocommit", false),
            write_only: bool_param(params, "write_only", false),
            no_verify: bool_param(params, "no_verify", false),
            ignore_nulls: bool_param(params, "ignore_nulls", false),
            consistent,
        })
    }

    fn commit_order(&self, write: bool) -> CommitOrder {
        match (self.consistent, write) {
            // Consistent lookups insert before and delete after the owning write,
            // so a rollback on either side cannot leave a dangling mapping.
            (true, true) => CommitOrder::Pre,
            _ if self.autocommit => CommitOrder::Autocommit,
            _ => CommitOrder::Normal,
        }
    }

    /// `SELECT from…, to FROM table WHERE (from…) IN ((…),(…))`
    ///
    /// Written with Vituss's `:name` placeholders and no engine-specific syntax:
    /// the gate re-plans this query, so it may end up on any engine.
    fn select_sql(&self, rows: &[VindexRow], bind_vars: &mut BindVars) -> String {
        let cols = self.from.join(", ");
        let mut tuples = Vec::with_capacity(rows.len());
        for (i, row) in rows.iter().enumerate() {
            let mut parts = Vec::with_capacity(row.len());
            for (j, v) in row.iter().enumerate() {
                let key = format!("k{i}_{j}");
                bind_vars.insert(key.clone(), v.clone());
                parts.push(format!(":{key}"));
            }
            tuples.push(format!("({})", parts.join(", ")));
        }
        format!(
            "SELECT {cols}, {to} FROM {table} WHERE ({cols}) IN ({tuples})",
            to = self.to,
            table = self.table,
            tuples = tuples.join(", ")
        )
    }

    fn insert_sql(&self, rows: &[VindexRow], ksids: &[KeyspaceId], bind_vars: &mut BindVars, ignore: bool) -> String {
        let cols = format!("{}, {}", self.from.join(", "), self.to);
        let mut tuples = Vec::with_capacity(rows.len());
        for (i, (row, ksid)) in rows.iter().zip(ksids).enumerate() {
            let mut parts = Vec::with_capacity(row.len() + 1);
            for (j, v) in row.iter().enumerate() {
                let key = format!("f{i}_{j}");
                bind_vars.insert(key.clone(), v.clone());
                parts.push(format!(":{key}"));
            }
            let key = format!("t{i}");
            bind_vars.insert(key.clone(), self.encode_to(ksid));
            parts.push(format!(":{key}"));
            tuples.push(format!("({})", parts.join(", ")));
        }
        // `IGNORE` is spelled differently on every engine; the gate's planner
        // rewrites this hint into the target engine's upsert form.
        let ignore = if ignore { " /*vt+ IGNORE_DUPLICATE */" } else { "" };
        format!(
            "INSERT{ignore} INTO {table}({cols}) VALUES {tuples}",
            table = self.table,
            tuples = tuples.join(", ")
        )
    }

    fn delete_sql(&self, rows: &[VindexRow], ksid: &KeyspaceId, bind_vars: &mut BindVars) -> String {
        let cols = self.from.join(", ");
        let mut tuples = Vec::with_capacity(rows.len());
        for (i, row) in rows.iter().enumerate() {
            let mut parts = Vec::with_capacity(row.len());
            for (j, v) in row.iter().enumerate() {
                let key = format!("d{i}_{j}");
                bind_vars.insert(key.clone(), v.clone());
                parts.push(format!(":{key}"));
            }
            tuples.push(format!("({})", parts.join(", ")));
        }
        bind_vars.insert("dksid".into(), self.encode_to(ksid));
        format!(
            "DELETE FROM {table} WHERE ({cols}) IN ({tuples}) AND {to} = :dksid",
            table = self.table,
            to = self.to,
            tuples = tuples.join(", ")
        )
    }

    /// Turn a keyspace ID into the value stored in the `to` column.
    fn encode_to(&self, ksid: &KeyspaceId) -> Value {
        match self.to_encoding {
            ToEncoding::Raw => Value::Bytes(ksid.0.clone()),
            // `lookup_hash` stores the pre-hash number, so it must be un-hashed
            // on the way in.
            ToEncoding::Hashed => match unhash(ksid.as_bytes()) {
                Ok(n) => Value::Uint(n),
                Err(_) => Value::Bytes(ksid.0.clone()),
            },
        }
    }

    /// Turn a value read from the `to` column into a keyspace ID.
    fn decode_to(&self, v: &Value) -> Result<KeyspaceId> {
        match self.to_encoding {
            ToEncoding::Raw => Ok(KeyspaceId(v.to_vindex_bytes())),
            ToEncoding::Hashed => {
                let n = v
                    .as_uint()
                    .or_else(|| v.as_int().map(|i| i as u64))
                    .ok_or_else(|| Error::invalid(format!("lookup column {} holds a non-numeric value {v}", self.to)))?;
                Ok(KeyspaceId(hash_u64(n)))
            }
        }
    }

    /// Group the lookup result by the `from` tuple, preserving input order.
    async fn query(&self, cursor: &dyn VCursor, rows: &[VindexRow]) -> Result<Vec<Vec<KeyspaceId>>> {
        let mut bind_vars = BindVars::new();
        let sql = self.select_sql(rows, &mut bind_vars);
        let result = cursor
            .execute("VindexLookup", &sql, &bind_vars, false, self.commit_order(false))
            .await?;

        let ncols = self.from.len();
        let mut grouped: Vec<Vec<KeyspaceId>> = vec![Vec::new(); rows.len()];
        for res_row in &result.rows {
            if res_row.len() < ncols + 1 {
                return Err(Error::internal(format!(
                    "lookup vindex {}: expected {} columns from {}, got {}",
                    self.name,
                    ncols + 1,
                    self.table,
                    res_row.len()
                )));
            }
            let key = &res_row[..ncols];
            let ksid = self.decode_to(&res_row[ncols])?;
            // Linear scan: the batch is a single query's worth of rows, and
            // hashing Value would need a total Eq we deliberately do not define.
            for (i, input) in rows.iter().enumerate() {
                if input.len() == ncols && input.iter().zip(key).all(|(a, b)| a == b) {
                    grouped[i].push(ksid.clone());
                }
            }
        }
        Ok(grouped)
    }
}

fn hash_u64(n: u64) -> Vec<u8> {
    use cipher::{BlockEncrypt, KeyInit};
    let cipher = des::Des::new(&[0u8; 8].into());
    let mut block = n.to_be_bytes().into();
    cipher.encrypt_block(&mut block);
    block.to_vec()
}

fn unhash(ksid: &[u8]) -> Result<u64> {
    use cipher::{BlockDecrypt, KeyInit};
    let b = <[u8; 8]>::try_from(ksid).map_err(|_| Error::invalid("keyspace id is not 8 bytes"))?;
    let cipher = des::Des::new(&[0u8; 8].into());
    let mut block = b.into();
    cipher.decrypt_block(&mut block);
    Ok(u64::from_be_bytes(block.into()))
}

/// The lookup vindex family. One type covers unique/non-unique, raw/hashed and
/// consistent/eventual variants, because the differences are all data.
pub struct Lookup {
    inner: LookupInternal,
    kind: &'static str,
    owned: bool,
}

impl Lookup {
    fn need_cursor<'a>(&self, cursor: Option<&'a dyn VCursor>) -> Result<&'a dyn VCursor> {
        cursor.ok_or_else(|| {
            Error::unsupported(format!(
                "vindex {} is a lookup vindex and needs to run a query; it cannot be used here \
                 (VReplication and offline tooling have no session to execute in)",
                self.inner.name
            ))
        })
    }
}

#[async_trait]
impl Vindex for Lookup {
    fn name(&self) -> &str {
        &self.inner.name
    }
    fn kind(&self) -> &'static str {
        self.kind
    }
    fn cost(&self) -> u32 {
        // Deliberately above every functional vindex: when a table has both, the
        // planner must prefer the one that does not need a round trip.
        if self.inner.unique {
            10
        } else {
            20
        }
    }
    fn is_unique(&self) -> bool {
        self.inner.unique
    }
    fn needs_cursor(&self) -> bool {
        true
    }
    fn column_count(&self) -> usize {
        self.inner.from.len()
    }
    fn known_params(&self) -> &'static [&'static str] {
        KNOWN_PARAMS
    }

    async fn map(&self, cursor: Option<&dyn VCursor>, rows: &[VindexRow]) -> Result<Vec<ShardDestination>> {
        if self.inner.write_only {
            // Backfill in progress: the table cannot be trusted to be complete,
            // so scatter rather than return a wrong — and silently empty — answer.
            return Ok(rows.iter().map(|_| ShardDestination::AllShards).collect());
        }
        let cursor = self.need_cursor(cursor)?;
        let grouped = self.inner.query(cursor, rows).await?;
        Ok(grouped
            .into_iter()
            .map(|ksids| match (ksids.len(), self.inner.unique) {
                (0, _) => ShardDestination::None,
                (_, true) => ShardDestination::KeyspaceId(ksids.into_iter().next().expect("non-empty")),
                (_, false) => ShardDestination::KeyspaceIds(ksids),
            })
            .collect())
    }

    async fn verify(&self, cursor: Option<&dyn VCursor>, rows: &[VindexRow], ksids: &[KeyspaceId]) -> Result<Vec<bool>> {
        if self.inner.write_only || self.inner.no_verify {
            return Ok(vec![true; rows.len()]);
        }
        let cursor = self.need_cursor(cursor)?;
        let grouped = self.inner.query(cursor, rows).await?;
        Ok(grouped
            .into_iter()
            .zip(ksids)
            .map(|(found, want)| found.iter().any(|k| k == want))
            .collect())
    }

    async fn create(
        &self,
        cursor: &dyn VCursor,
        rows: &[VindexRow],
        ksids: &[KeyspaceId],
        ignore_on_duplicate: bool,
    ) -> Result<()> {
        if !self.owned {
            // An unowned lookup vindex reads a table someone else maintains.
            return Ok(());
        }
        let (rows, ksids) = if self.inner.ignore_nulls {
            let kept: Vec<usize> = rows
                .iter()
                .enumerate()
                .filter(|(_, r)| !r.iter().any(Value::is_null))
                .map(|(i, _)| i)
                .collect();
            (
                kept.iter().map(|&i| rows[i].clone()).collect::<Vec<_>>(),
                kept.iter().map(|&i| ksids[i].clone()).collect::<Vec<_>>(),
            )
        } else {
            if let Some(bad) = rows.iter().find(|r| r.iter().any(Value::is_null)) {
                return Err(Error::invalid(format!(
                    "lookup vindex {}: cannot insert a NULL value ({} columns) — set ignore_nulls=true to skip such rows",
                    self.inner.name,
                    bad.len()
                )));
            }
            (rows.to_vec(), ksids.to_vec())
        };
        if rows.is_empty() {
            return Ok(());
        }

        let mut bind_vars = BindVars::new();
        let sql = self.inner.insert_sql(&rows, &ksids, &mut bind_vars, ignore_on_duplicate);

        if self.inner.consistent {
            // Write the lookup row on the shard that owns it, so the insert and
            // the uniqueness check happen on the same shard.
            for (row, ksid) in rows.iter().zip(&ksids) {
                let mut bv = BindVars::new();
                let sql = self.inner.insert_sql(
                    std::slice::from_ref(row),
                    std::slice::from_ref(ksid),
                    &mut bv,
                    ignore_on_duplicate,
                );
                cursor
                    .execute_keyspace_id(&self.inner.table, ksid.as_bytes(), &sql, &bv, true, self.inner.autocommit)
                    .await?;
            }
            return Ok(());
        }

        cursor
            .execute("VindexCreate", &sql, &bind_vars, true, self.inner.commit_order(true))
            .await
            .map(|_| ())
    }

    async fn delete(&self, cursor: &dyn VCursor, rows: &[VindexRow], ksid: &KeyspaceId) -> Result<()> {
        if !self.owned || rows.is_empty() {
            return Ok(());
        }
        let mut bind_vars = BindVars::new();
        let sql = self.inner.delete_sql(rows, ksid, &mut bind_vars);
        let order = if self.inner.consistent { CommitOrder::Post } else { self.inner.commit_order(true) };
        cursor.execute("VindexDelete", &sql, &bind_vars, true, order).await.map(|_| ())
    }
}

// ---------------------------------------------------------------------------
// Constructors
// ---------------------------------------------------------------------------

fn build(
    kind: &'static str,
    name: &str,
    params: &VindexParams,
    unique: bool,
    to_encoding: ToEncoding,
    consistent: bool,
) -> Result<Arc<dyn Vindex>> {
    Ok(Arc::new(Lookup {
        inner: LookupInternal::parse(kind, name, params, unique, to_encoding, consistent)?,
        kind,
        owned: params.contains_key("owner"),
    }))
}

pub(crate) fn register(m: &mut std::collections::HashMap<&'static str, crate::registry::VindexBuilder>) {
    m.insert("lookup", |n, p| build("lookup", n, p, false, ToEncoding::Raw, false));
    m.insert("lookup_unique", |n, p| build("lookup_unique", n, p, true, ToEncoding::Raw, false));
    m.insert("lookup_hash", |n, p| build("lookup_hash", n, p, false, ToEncoding::Hashed, false));
    m.insert("lookup_hash_unique", |n, p| build("lookup_hash_unique", n, p, true, ToEncoding::Hashed, false));
    m.insert("consistent_lookup", |n, p| build("consistent_lookup", n, p, false, ToEncoding::Raw, true));
    m.insert("consistent_lookup_unique", |n, p| {
        build("consistent_lookup_unique", n, p, true, ToEncoding::Raw, true)
    });
}
