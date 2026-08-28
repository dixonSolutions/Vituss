//! Functional vindexes: the keyspace ID is computed from the value, with no state
//! and no round trip.

use async_trait::async_trait;
use cipher::{BlockDecrypt, BlockEncrypt, KeyInit};
use des::Des;
use md5::{Digest, Md5};

use vituss_core::{Error, KeyspaceId, Result, ShardDestination, Value};

use crate::cursor::VCursor;
use crate::vindex::{validate_params, Vindex, VindexParams, VindexRow};

/// Interpret a value as the 64-bit number a numeric vindex hashes.
///
/// Negative signed values are taken as their two's-complement bit pattern, which
/// is what makes `-1` and `18446744073709551615` land on the same shard — the
/// same rule Vitess uses, and one that must not change or existing data moves.
fn as_u64(v: &Value) -> Result<u64> {
    match v {
        Value::Int(i) => Ok(*i as u64),
        Value::Uint(u) => Ok(*u),
        Value::Bool(b) => Ok(*b as u64),
        Value::Text(s) => {
            let t = s.trim();
            t.parse::<u64>()
                .or_else(|_| t.parse::<i64>().map(|i| i as u64))
                .map_err(|_| Error::invalid(format!("cannot use {t:?} as a numeric sharding key")))
        }
        Value::Null => Err(Error::invalid("NULL is not a valid numeric sharding key")),
        other => Err(Error::invalid(format!(
            "cannot use a {:?} value as a numeric sharding key",
            other.sql_type()
        ))),
    }
}

fn one_col<'a>(kind: &str, row: &'a VindexRow) -> Result<&'a Value> {
    row.first()
        .ok_or_else(|| Error::internal(format!("vindex {kind} received a row with no columns")))
}

/// Compare a computed keyspace ID against an expected one.
fn same(a: &[u8], b: &KeyspaceId) -> bool {
    a == b.as_bytes()
}

// ---------------------------------------------------------------------------
// hash
// ---------------------------------------------------------------------------

/// `hash` — DES-ECB of the big-endian 64-bit value under an all-zero key.
///
/// This is not a good hash by cryptographic standards and it was never meant to
/// be: it is a cheap bijection that spreads sequential IDs across the keyspace.
/// It is kept bit-for-bit compatible with Vitess so that an existing Vitess
/// keyspace can be pointed at Vituss without moving a single row.
pub struct Hash {
    name: String,
}

impl Hash {
    pub fn new(name: &str, params: &VindexParams) -> Result<Self> {
        validate_params("hash", params, &[])?;
        Ok(Self { name: name.to_string() })
    }

    fn encrypt(n: u64) -> Vec<u8> {
        let cipher = Des::new(&[0u8; 8].into());
        let mut block = n.to_be_bytes().into();
        cipher.encrypt_block(&mut block);
        block.to_vec()
    }

    fn decrypt(ksid: &[u8]) -> Result<u64> {
        if ksid.len() != 8 {
            return Err(Error::invalid(format!("invalid keyspace id {}", hex::encode(ksid))));
        }
        let cipher = Des::new(&[0u8; 8].into());
        let mut block = <[u8; 8]>::try_from(ksid).expect("length checked").into();
        cipher.decrypt_block(&mut block);
        Ok(u64::from_be_bytes(block.into()))
    }
}

#[async_trait]
impl Vindex for Hash {
    fn name(&self) -> &str {
        &self.name
    }
    fn kind(&self) -> &'static str {
        "hash"
    }
    fn cost(&self) -> u32 {
        1
    }
    fn is_unique(&self) -> bool {
        true
    }

    async fn map(&self, _c: Option<&dyn VCursor>, rows: &[VindexRow]) -> Result<Vec<ShardDestination>> {
        rows.iter()
            .map(|r| {
                let v = one_col("hash", r)?;
                Ok(match as_u64(v) {
                    // An unusable value matches no shard rather than every shard:
                    // scattering here would turn a typo into a full table scan.
                    Err(_) => ShardDestination::None,
                    Ok(n) => ShardDestination::KeyspaceId(KeyspaceId(Self::encrypt(n))),
                })
            })
            .collect()
    }

    async fn verify(&self, _c: Option<&dyn VCursor>, rows: &[VindexRow], ksids: &[KeyspaceId]) -> Result<Vec<bool>> {
        rows.iter()
            .zip(ksids)
            .map(|(r, k)| {
                let n = as_u64(one_col("hash", r)?)?;
                Ok(same(&Self::encrypt(n), k))
            })
            .collect()
    }

    fn reverse_map(&self, ksids: &[KeyspaceId]) -> Option<Result<Vec<Value>>> {
        Some(
            ksids
                .iter()
                .map(|k| Self::decrypt(k.as_bytes()).map(Value::Uint))
                .collect(),
        )
    }

    fn hash(&self, value: &Value) -> Option<Result<Vec<u8>>> {
        Some(as_u64(value).map(Self::encrypt))
    }
}

// ---------------------------------------------------------------------------
// xxhash
// ---------------------------------------------------------------------------

/// `xxhash` — xxHash64 of the value's canonical bytes, little-endian.
///
/// The recommended default for new keyspaces: it accepts any type, not just
/// integers, and it is fast enough to be free.
pub struct XxHash {
    name: String,
}

impl XxHash {
    pub fn new(name: &str, params: &VindexParams) -> Result<Self> {
        validate_params("xxhash", params, &[])?;
        Ok(Self { name: name.to_string() })
    }

    fn digest(v: &Value) -> Vec<u8> {
        xxhash_rust::xxh64::xxh64(&v.to_vindex_bytes(), 0).to_le_bytes().to_vec()
    }
}

#[async_trait]
impl Vindex for XxHash {
    fn name(&self) -> &str {
        &self.name
    }
    fn kind(&self) -> &'static str {
        "xxhash"
    }
    fn cost(&self) -> u32 {
        1
    }
    fn is_unique(&self) -> bool {
        true
    }

    async fn map(&self, _c: Option<&dyn VCursor>, rows: &[VindexRow]) -> Result<Vec<ShardDestination>> {
        rows.iter()
            .map(|r| Ok(ShardDestination::KeyspaceId(KeyspaceId(Self::digest(one_col("xxhash", r)?)))))
            .collect()
    }

    async fn verify(&self, _c: Option<&dyn VCursor>, rows: &[VindexRow], ksids: &[KeyspaceId]) -> Result<Vec<bool>> {
        rows.iter()
            .zip(ksids)
            .map(|(r, k)| Ok(same(&Self::digest(one_col("xxhash", r)?), k)))
            .collect()
    }

    fn hash(&self, value: &Value) -> Option<Result<Vec<u8>>> {
        Some(Ok(Self::digest(value)))
    }
}

// ---------------------------------------------------------------------------
// binary / binary_md5
// ---------------------------------------------------------------------------

/// `binary` — the value's bytes *are* the keyspace ID.
///
/// Cost 0, fully reversible, and order-preserving, so range predicates on the
/// sharding column narrow to a key range instead of scattering.
pub struct Binary {
    name: String,
}

impl Binary {
    pub fn new(name: &str, params: &VindexParams) -> Result<Self> {
        validate_params("binary", params, &[])?;
        Ok(Self { name: name.to_string() })
    }
}

#[async_trait]
impl Vindex for Binary {
    fn name(&self) -> &str {
        &self.name
    }
    fn kind(&self) -> &'static str {
        "binary"
    }
    fn cost(&self) -> u32 {
        0
    }
    fn is_unique(&self) -> bool {
        true
    }

    async fn map(&self, _c: Option<&dyn VCursor>, rows: &[VindexRow]) -> Result<Vec<ShardDestination>> {
        rows.iter()
            .map(|r| Ok(ShardDestination::KeyspaceId(KeyspaceId(one_col("binary", r)?.to_vindex_bytes()))))
            .collect()
    }

    async fn verify(&self, _c: Option<&dyn VCursor>, rows: &[VindexRow], ksids: &[KeyspaceId]) -> Result<Vec<bool>> {
        rows.iter()
            .zip(ksids)
            .map(|(r, k)| Ok(same(&one_col("binary", r)?.to_vindex_bytes(), k)))
            .collect()
    }

    fn reverse_map(&self, ksids: &[KeyspaceId]) -> Option<Result<Vec<Value>>> {
        Some(Ok(ksids.iter().map(|k| Value::Bytes(k.0.clone())).collect()))
    }

    fn hash(&self, value: &Value) -> Option<Result<Vec<u8>>> {
        Some(Ok(value.to_vindex_bytes()))
    }

    fn range_map(&self, lo: &Value, hi: &Value) -> Option<Result<ShardDestination>> {
        // Identity mapping preserves order, so a BETWEEN maps to one key range.
        Some(Ok(ShardDestination::KeyRange(vituss_core::KeyRange::new(
            lo.to_vindex_bytes(),
            hi.to_vindex_bytes(),
        ))))
    }

    fn prefix_map(&self, prefix: &Value) -> Option<Result<ShardDestination>> {
        let start = prefix.to_vindex_bytes();
        let mut end = start.clone();
        // The successor of a byte prefix: increment the last byte that is not 0xff.
        while let Some(last) = end.pop() {
            if last != 0xff {
                end.push(last + 1);
                break;
            }
        }
        Some(Ok(ShardDestination::KeyRange(vituss_core::KeyRange::new(start, end))))
    }
}

/// `binary_md5` — MD5 of the value's bytes.
///
/// Not order-preserving, but it accepts any value and spreads long or skewed
/// string keys evenly. MD5 is used as a mixing function only; nothing here
/// depends on it being collision resistant.
pub struct BinaryMd5 {
    name: String,
}

impl BinaryMd5 {
    pub fn new(name: &str, params: &VindexParams) -> Result<Self> {
        validate_params("binary_md5", params, &[])?;
        Ok(Self { name: name.to_string() })
    }
    fn digest(v: &Value) -> Vec<u8> {
        Md5::digest(v.to_vindex_bytes()).to_vec()
    }
}

#[async_trait]
impl Vindex for BinaryMd5 {
    fn name(&self) -> &str {
        &self.name
    }
    fn kind(&self) -> &'static str {
        "binary_md5"
    }
    fn cost(&self) -> u32 {
        1
    }
    fn is_unique(&self) -> bool {
        true
    }

    async fn map(&self, _c: Option<&dyn VCursor>, rows: &[VindexRow]) -> Result<Vec<ShardDestination>> {
        rows.iter()
            .map(|r| Ok(ShardDestination::KeyspaceId(KeyspaceId(Self::digest(one_col("binary_md5", r)?)))))
            .collect()
    }

    async fn verify(&self, _c: Option<&dyn VCursor>, rows: &[VindexRow], ksids: &[KeyspaceId]) -> Result<Vec<bool>> {
        rows.iter()
            .zip(ksids)
            .map(|(r, k)| Ok(same(&Self::digest(one_col("binary_md5", r)?), k)))
            .collect()
    }

    fn hash(&self, value: &Value) -> Option<Result<Vec<u8>>> {
        Some(Ok(Self::digest(value)))
    }
}

// ---------------------------------------------------------------------------
// numeric / reverse_bits / null
// ---------------------------------------------------------------------------

/// `numeric` — the 64-bit value big-endian, unhashed.
///
/// Order-preserving, so sequential IDs cluster: fine when you want range scans on
/// the sharding key, bad when you want write spread. Choose deliberately.
pub struct Numeric {
    name: String,
}

impl Numeric {
    pub fn new(name: &str, params: &VindexParams) -> Result<Self> {
        validate_params("numeric", params, &[])?;
        Ok(Self { name: name.to_string() })
    }
}

#[async_trait]
impl Vindex for Numeric {
    fn name(&self) -> &str {
        &self.name
    }
    fn kind(&self) -> &'static str {
        "numeric"
    }
    fn cost(&self) -> u32 {
        0
    }
    fn is_unique(&self) -> bool {
        true
    }

    async fn map(&self, _c: Option<&dyn VCursor>, rows: &[VindexRow]) -> Result<Vec<ShardDestination>> {
        rows.iter()
            .map(|r| {
                Ok(match as_u64(one_col("numeric", r)?) {
                    Ok(n) => ShardDestination::KeyspaceId(KeyspaceId(n.to_be_bytes().to_vec())),
                    Err(_) => ShardDestination::None,
                })
            })
            .collect()
    }

    async fn verify(&self, _c: Option<&dyn VCursor>, rows: &[VindexRow], ksids: &[KeyspaceId]) -> Result<Vec<bool>> {
        rows.iter()
            .zip(ksids)
            .map(|(r, k)| {
                let n = as_u64(one_col("numeric", r)?)?;
                Ok(same(&n.to_be_bytes(), k))
            })
            .collect()
    }

    fn reverse_map(&self, ksids: &[KeyspaceId]) -> Option<Result<Vec<Value>>> {
        Some(
            ksids
                .iter()
                .map(|k| {
                    <[u8; 8]>::try_from(k.as_bytes())
                        .map(|b| Value::Uint(u64::from_be_bytes(b)))
                        .map_err(|_| Error::invalid(format!("invalid keyspace id {k}")))
                })
                .collect(),
        )
    }

    fn hash(&self, value: &Value) -> Option<Result<Vec<u8>>> {
        Some(as_u64(value).map(|n| n.to_be_bytes().to_vec()))
    }

    fn range_map(&self, lo: &Value, hi: &Value) -> Option<Result<ShardDestination>> {
        let r = (|| {
            Ok(ShardDestination::KeyRange(vituss_core::KeyRange::new(
                as_u64(lo)?.to_be_bytes().to_vec(),
                as_u64(hi)?.to_be_bytes().to_vec(),
            )))
        })();
        Some(r)
    }
}

/// `reverse_bits` — the 64-bit value with its bits reversed.
///
/// Turns a monotonically increasing ID into a uniformly spread keyspace ID while
/// staying perfectly invertible, so no lookup table is needed to go back.
pub struct ReverseBits {
    name: String,
}

impl ReverseBits {
    pub fn new(name: &str, params: &VindexParams) -> Result<Self> {
        validate_params("reverse_bits", params, &[])?;
        Ok(Self { name: name.to_string() })
    }
    fn encode(n: u64) -> Vec<u8> {
        n.reverse_bits().to_be_bytes().to_vec()
    }
}

#[async_trait]
impl Vindex for ReverseBits {
    fn name(&self) -> &str {
        &self.name
    }
    fn kind(&self) -> &'static str {
        "reverse_bits"
    }
    fn cost(&self) -> u32 {
        1
    }
    fn is_unique(&self) -> bool {
        true
    }

    async fn map(&self, _c: Option<&dyn VCursor>, rows: &[VindexRow]) -> Result<Vec<ShardDestination>> {
        rows.iter()
            .map(|r| {
                Ok(match as_u64(one_col("reverse_bits", r)?) {
                    Ok(n) => ShardDestination::KeyspaceId(KeyspaceId(Self::encode(n))),
                    Err(_) => ShardDestination::None,
                })
            })
            .collect()
    }

    async fn verify(&self, _c: Option<&dyn VCursor>, rows: &[VindexRow], ksids: &[KeyspaceId]) -> Result<Vec<bool>> {
        rows.iter()
            .zip(ksids)
            .map(|(r, k)| Ok(same(&Self::encode(as_u64(one_col("reverse_bits", r)?)?), k)))
            .collect()
    }

    fn reverse_map(&self, ksids: &[KeyspaceId]) -> Option<Result<Vec<Value>>> {
        Some(
            ksids
                .iter()
                .map(|k| {
                    <[u8; 8]>::try_from(k.as_bytes())
                        .map(|b| Value::Uint(u64::from_be_bytes(b).reverse_bits()))
                        .map_err(|_| Error::invalid(format!("invalid keyspace id {k}")))
                })
                .collect(),
        )
    }

    fn hash(&self, value: &Value) -> Option<Result<Vec<u8>>> {
        Some(as_u64(value).map(Self::encode))
    }
}

/// `null` — every row maps to keyspace ID zero.
///
/// Used for a table that must live entirely on the first shard of an otherwise
/// sharded keyspace (reference data, sequences).
pub struct Null {
    name: String,
}

const NULL_KSID: [u8; 8] = [0; 8];

impl Null {
    pub fn new(name: &str, params: &VindexParams) -> Result<Self> {
        validate_params("null", params, &[])?;
        Ok(Self { name: name.to_string() })
    }
}

#[async_trait]
impl Vindex for Null {
    fn name(&self) -> &str {
        &self.name
    }
    fn kind(&self) -> &'static str {
        "null"
    }
    fn cost(&self) -> u32 {
        100
    }
    fn is_unique(&self) -> bool {
        true
    }

    async fn map(&self, _c: Option<&dyn VCursor>, rows: &[VindexRow]) -> Result<Vec<ShardDestination>> {
        Ok(rows
            .iter()
            .map(|_| ShardDestination::KeyspaceId(KeyspaceId(NULL_KSID.to_vec())))
            .collect())
    }

    async fn verify(&self, _c: Option<&dyn VCursor>, rows: &[VindexRow], ksids: &[KeyspaceId]) -> Result<Vec<bool>> {
        Ok(rows.iter().zip(ksids).map(|(_, k)| same(&NULL_KSID, k)).collect())
    }
}

// ---------------------------------------------------------------------------
// numeric_static_map
// ---------------------------------------------------------------------------

/// `numeric_static_map` — an explicit value → keyspace-ID table from the VSchema.
///
/// For the case where a handful of tenant IDs must be pinned to named shards and
/// no hash function would put them there.
pub struct NumericStaticMap {
    name: String,
    map: std::collections::HashMap<u64, u64>,
    /// What to do with a value that is not in the map.
    fallback_hash: bool,
}

impl NumericStaticMap {
    pub fn new(name: &str, params: &VindexParams) -> Result<Self> {
        validate_params("numeric_static_map", params, &["json", "json_path", "fallback_type"])?;
        let raw = match (params.get("json"), params.get("json_path")) {
            (Some(j), _) => j.clone(),
            (None, Some(path)) => std::fs::read_to_string(path)
                .map_err(|e| Error::invalid(format!("numeric_static_map: cannot read {path:?}: {e}")))?,
            (None, None) => {
                return Err(Error::invalid(
                    "numeric_static_map requires either the 'json' or the 'json_path' parameter",
                ))
            }
        };
        let parsed: std::collections::HashMap<String, u64> = serde_json::from_str(&raw)
            .map_err(|e| Error::invalid(format!("numeric_static_map: invalid JSON: {e}")))?;
        let map = parsed
            .into_iter()
            .map(|(k, v)| {
                k.parse::<u64>()
                    .map(|k| (k, v))
                    .map_err(|_| Error::invalid(format!("numeric_static_map: key {k:?} is not a number")))
            })
            .collect::<Result<_>>()?;
        let fallback_hash = params.get("fallback_type").map(String::as_str) == Some("hash");
        Ok(Self { name: name.to_string(), map, fallback_hash })
    }

    fn lookup(&self, v: &Value) -> Result<Option<Vec<u8>>> {
        let n = as_u64(v)?;
        match self.map.get(&n) {
            Some(k) => Ok(Some(k.to_be_bytes().to_vec())),
            None if self.fallback_hash => Ok(Some(Hash::encrypt(n))),
            None => Ok(None),
        }
    }
}

#[async_trait]
impl Vindex for NumericStaticMap {
    fn name(&self) -> &str {
        &self.name
    }
    fn kind(&self) -> &'static str {
        "numeric_static_map"
    }
    fn cost(&self) -> u32 {
        1
    }
    fn is_unique(&self) -> bool {
        true
    }
    fn known_params(&self) -> &'static [&'static str] {
        &["json", "json_path", "fallback_type"]
    }

    async fn map(&self, _c: Option<&dyn VCursor>, rows: &[VindexRow]) -> Result<Vec<ShardDestination>> {
        rows.iter()
            .map(|r| {
                Ok(match self.lookup(one_col("numeric_static_map", r)?) {
                    Ok(Some(k)) => ShardDestination::KeyspaceId(KeyspaceId(k)),
                    _ => ShardDestination::None,
                })
            })
            .collect()
    }

    async fn verify(&self, _c: Option<&dyn VCursor>, rows: &[VindexRow], ksids: &[KeyspaceId]) -> Result<Vec<bool>> {
        rows.iter()
            .zip(ksids)
            .map(|(r, k)| {
                Ok(self
                    .lookup(one_col("numeric_static_map", r)?)?
                    .is_some_and(|computed| same(&computed, k)))
            })
            .collect()
    }

    fn hash(&self, value: &Value) -> Option<Result<Vec<u8>>> {
        Some(self.lookup(value).and_then(|o| {
            o.ok_or_else(|| Error::invalid(format!("{value} is not present in the numeric_static_map")))
        }))
    }
}

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

pub(crate) fn register(m: &mut std::collections::HashMap<&'static str, crate::registry::VindexBuilder>) {
    m.insert("hash", |n, p| Ok(std::sync::Arc::new(Hash::new(n, p)?)));
    m.insert("xxhash", |n, p| Ok(std::sync::Arc::new(XxHash::new(n, p)?)));
    m.insert("binary", |n, p| Ok(std::sync::Arc::new(Binary::new(n, p)?)));
    m.insert("binary_md5", |n, p| Ok(std::sync::Arc::new(BinaryMd5::new(n, p)?)));
    m.insert("numeric", |n, p| Ok(std::sync::Arc::new(Numeric::new(n, p)?)));
    m.insert("reverse_bits", |n, p| Ok(std::sync::Arc::new(ReverseBits::new(n, p)?)));
    m.insert("null", |n, p| Ok(std::sync::Arc::new(Null::new(n, p)?)));
    m.insert("numeric_static_map", |n, p| Ok(std::sync::Arc::new(NumericStaticMap::new(n, p)?)));
}
