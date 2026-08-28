//! Keyspace IDs, key ranges and routing destinations.
//!
//! A *keyspace ID* is an opaque byte string produced by a vindex from a row's
//! sharding key. A *key range* is a half-open interval `[start, end)` over those
//! byte strings, and a shard is named after the range it owns (`-80`, `80-c0`,
//! `-` for unsharded). Comparison is prefix-extended, so `0x80` and `0x8000...`
//! denote the same boundary — this is what makes shard splits a pure metadata
//! operation.
//!
//! This scheme is engine-independent: it is arithmetic on bytes, not on anything
//! MySQL provides. That is why the same sharding layout works over PostgreSQL and
//! SQL Server unchanged.

use std::fmt;

use crate::error::{Error, Result};

/// An opaque keyspace ID.
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize)]
pub struct KeyspaceId(pub Vec<u8>);

impl KeyspaceId {
    pub fn new(bytes: impl Into<Vec<u8>>) -> Self {
        Self(bytes.into())
    }
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
    pub fn to_hex(&self) -> String {
        hex::encode(&self.0)
    }
    pub fn from_hex(s: &str) -> Result<Self> {
        hex::decode(s)
            .map(Self)
            .map_err(|e| Error::invalid(format!("invalid keyspace id {s:?}: {e}")))
    }
}

impl fmt::Display for KeyspaceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

/// Compare two key boundaries with prefix extension (missing bytes read as 0x00).
fn cmp_prefix(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
    let n = a.len().max(b.len());
    for i in 0..n {
        let (x, y) = (a.get(i).copied().unwrap_or(0), b.get(i).copied().unwrap_or(0));
        match x.cmp(&y) {
            std::cmp::Ordering::Equal => continue,
            other => return other,
        }
    }
    std::cmp::Ordering::Equal
}

/// A half-open key range `[start, end)`.
///
/// An empty `start` means unbounded-below and an empty `end` means
/// unbounded-above, so the full range is `KeyRange { start: [], end: [] }`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct KeyRange {
    #[serde(default, with = "hex_bytes")]
    pub start: Vec<u8>,
    #[serde(default, with = "hex_bytes")]
    pub end: Vec<u8>,
}

mod hex_bytes {
    use serde::{Deserialize, Deserializer, Serializer};
    pub fn serialize<S: Serializer>(v: &Vec<u8>, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&hex::encode(v))
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        let s = String::deserialize(d)?;
        hex::decode(&s).map_err(serde::de::Error::custom)
    }
}

impl KeyRange {
    /// The range covering every keyspace ID — i.e. an unsharded keyspace.
    pub fn full() -> Self {
        Self::default()
    }

    pub fn new(start: impl Into<Vec<u8>>, end: impl Into<Vec<u8>>) -> Self {
        Self { start: start.into(), end: end.into() }
    }

    pub fn is_full(&self) -> bool {
        self.start.is_empty() && self.end.is_empty()
    }

    /// Parse a shard name such as `-80`, `80-c0`, `c0-` or `-`/`0` (full range).
    pub fn parse(name: &str) -> Result<Self> {
        let name = name.trim();
        if name == "0" || name == "-" || name.is_empty() {
            return Ok(Self::full());
        }
        let (s, e) = name
            .split_once('-')
            .ok_or_else(|| Error::invalid(format!("invalid shard name {name:?}: expected 'start-end'")))?;
        let decode = |p: &str, which: &str| -> Result<Vec<u8>> {
            if p.is_empty() {
                return Ok(Vec::new());
            }
            // Odd-length hex is accepted the way Vitess accepts it: `-8` == `-80`.
            let padded = if p.len() % 2 == 1 { format!("{p}0") } else { p.to_string() };
            hex::decode(&padded)
                .map_err(|err| Error::invalid(format!("invalid {which} {p:?} in shard name {name:?}: {err}")))
        };
        Ok(Self { start: decode(s, "range start")?, end: decode(e, "range end")? })
    }

    /// The canonical shard name for this range.
    pub fn name(&self) -> String {
        if self.is_full() {
            "-".to_string()
        } else {
            format!("{}-{}", hex::encode(&self.start), hex::encode(&self.end))
        }
    }

    pub fn contains(&self, ksid: &[u8]) -> bool {
        let after_start = self.start.is_empty() || cmp_prefix(ksid, &self.start) != std::cmp::Ordering::Less;
        let before_end = self.end.is_empty() || cmp_prefix(ksid, &self.end) == std::cmp::Ordering::Less;
        after_start && before_end
    }

    /// True when the two ranges share at least one keyspace ID.
    pub fn intersects(&self, other: &KeyRange) -> bool {
        let start_before_other_end =
            other.end.is_empty() || self.start.is_empty() || cmp_prefix(&self.start, &other.end) == std::cmp::Ordering::Less;
        let other_start_before_end =
            self.end.is_empty() || other.start.is_empty() || cmp_prefix(&other.start, &self.end) == std::cmp::Ordering::Less;
        start_before_other_end && other_start_before_end
    }

    /// Split the full range into `n` equal shards, the way `vtctld CreateKeyspace
    /// --shards=n` does. Returns canonical shard names.
    pub fn even_split(n: u32) -> Result<Vec<KeyRange>> {
        if n == 0 {
            return Err(Error::invalid("shard count must be > 0"));
        }
        if n == 1 {
            return Ok(vec![KeyRange::full()]);
        }
        // Boundaries are taken from the top 64 bits, which is enough resolution for
        // any practical shard count and keeps names short.
        let span = u64::MAX / n as u64 + 1;
        let mut out = Vec::with_capacity(n as usize);
        for i in 0..n as u64 {
            let start = if i == 0 { Vec::new() } else { trim_zeros(&(span * i).to_be_bytes()) };
            let end = if i == n as u64 - 1 { Vec::new() } else { trim_zeros(&(span * (i + 1)).to_be_bytes()) };
            out.push(KeyRange { start, end });
        }
        Ok(out)
    }
}

fn trim_zeros(b: &[u8]) -> Vec<u8> {
    let mut v = b.to_vec();
    while v.last() == Some(&0) {
        v.pop();
    }
    v
}

impl fmt::Display for KeyRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

/// Where a query (or one row of it) should be sent.
///
/// Vindexes produce these; the resolver turns them into concrete shard names by
/// consulting the serving key-range map in the topology.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShardDestination {
    /// Every serving shard of the keyspace.
    AllShards,
    /// Any single shard — used for queries whose result is shard-independent
    /// (`SELECT 1`, most `SHOW` statements).
    AnyShard,
    /// A shard named explicitly, e.g. by `USE ks:-80`.
    Shard(String),
    /// Exactly one keyspace ID; resolves to the one shard that owns it.
    KeyspaceId(KeyspaceId),
    /// A batch of keyspace IDs; resolves to the union of owning shards, and the
    /// engine keeps the id→shard mapping so DML rows go to the right place.
    KeyspaceIds(Vec<KeyspaceId>),
    /// Every shard overlapping the range.
    KeyRange(KeyRange),
    /// Like `KeyRange`, but errors unless the range aligns with shard boundaries.
    /// Used by resharding tooling that must not read partial shards.
    ExactKeyRange(KeyRange),
    /// Route nowhere: the predicate is unsatisfiable (`WHERE id IN ()`).
    None,
}

impl fmt::Display for ShardDestination {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AllShards => write!(f, "AllShards"),
            Self::AnyShard => write!(f, "AnyShard"),
            Self::Shard(s) => write!(f, "Shard({s})"),
            Self::KeyspaceId(k) => write!(f, "KeyspaceId({k})"),
            Self::KeyspaceIds(ks) => {
                write!(f, "KeyspaceIds({})", ks.iter().map(|k| k.to_hex()).collect::<Vec<_>>().join(","))
            }
            Self::KeyRange(kr) => write!(f, "KeyRange({kr})"),
            Self::ExactKeyRange(kr) => write!(f, "ExactKeyRange({kr})"),
            Self::None => write!(f, "None"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_and_renders_shard_names() {
        assert_eq!(KeyRange::parse("-").unwrap(), KeyRange::full());
        assert_eq!(KeyRange::parse("0").unwrap(), KeyRange::full());
        let kr = KeyRange::parse("40-80").unwrap();
        assert_eq!(kr.start, vec![0x40]);
        assert_eq!(kr.end, vec![0x80]);
        assert_eq!(kr.name(), "40-80");
        // Odd-length hex is right-padded, matching Vitess.
        assert_eq!(KeyRange::parse("-8").unwrap().end, vec![0x80]);
    }

    #[test]
    fn containment_is_prefix_extended() {
        let kr = KeyRange::parse("-80").unwrap();
        assert!(kr.contains(&[0x00]));
        assert!(kr.contains(&[0x7f, 0xff, 0xff]));
        assert!(!kr.contains(&[0x80]));
        // 0x80 and 0x8000 are the same boundary.
        assert!(!kr.contains(&[0x80, 0x00, 0x00]));
    }

    #[test]
    fn even_split_covers_the_space_without_gaps() {
        for n in [1u32, 2, 4, 8, 3] {
            let shards = KeyRange::even_split(n).unwrap();
            assert_eq!(shards.len() as u32, n);
            assert!(shards[0].start.is_empty());
            assert!(shards[n as usize - 1].end.is_empty());
            for w in shards.windows(2) {
                assert_eq!(w[0].end, w[1].start, "gap between {} and {}", w[0], w[1]);
            }
        }
    }

    #[test]
    fn intersection_matches_containment() {
        let a = KeyRange::parse("-80").unwrap();
        let b = KeyRange::parse("80-").unwrap();
        assert!(!a.intersects(&b));
        assert!(a.intersects(&KeyRange::parse("40-c0").unwrap()));
        assert!(a.intersects(&KeyRange::full()));
    }
}
