//! Built-in vindex implementations.
//!
//! ## Ported from Vitess
//!
//! `hash`, `xxhash`, `binary`, `binary_md5`, `numeric`, `numeric_static_map`,
//! `reverse_bits`, `null`, `multicol`, and the lookup family (`lookup`,
//! `lookup_unique`, `lookup_hash`, `lookup_hash_unique`, `consistent_lookup`,
//! `consistent_lookup_unique`).
//!
//! The functional vindexes are bit-for-bit compatible with Vitess, so an existing
//! Vitess keyspace maps to the same shards under Vituss.
//!
//! ## Not yet ported
//!
//! `cfc`, `region_experimental`, `region_json`, and the `unicode_loose_*` family.
//! The unicode ones depend on MySQL's UCA 4.0.0 collation weight tables; a
//! close-but-different implementation would silently place rows on the wrong
//! shard, so they are absent rather than approximated.

use std::collections::HashMap;

use crate::registry::VindexBuilder;

pub mod functional;
pub mod lookup;
pub mod multicol;

pub use functional::{Binary, BinaryMd5, Hash, Null, Numeric, NumericStaticMap, ReverseBits, XxHash};
pub use lookup::Lookup;
pub use multicol::MultiCol;

pub(crate) fn register_builtins(m: &mut HashMap<&'static str, VindexBuilder>) {
    functional::register(m);
    lookup::register(m);
    multicol::register(m);
}
