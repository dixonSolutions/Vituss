//! # vituss-vindex
//!
//! A *vindex* is the function that decides which shard a row belongs to. It maps
//! one or more column values to a keyspace ID, and a keyspace ID falls inside
//! exactly one shard's key range.
//!
//! Vindexes are the reason Vituss can shard over any engine: the function runs in
//! Vituss, on neutral [`Value`]s, and produces plain bytes. Nothing about it
//! depends on MySQL, PostgreSQL or SQL Server — so a keyspace can be migrated
//! between engines without re-sharding.
//!
//! ## Kinds
//!
//! * **Functional** vindexes (`hash`, `xxhash`, `numeric`, …) compute the
//!   keyspace ID from the value. They are cheap and need no state.
//! * **Lookup** vindexes (`lookup`, `consistent_lookup`, …) store the mapping in
//!   a table and consult it through a [`VCursor`]. They cost a round trip but let
//!   you shard by a column that is not the primary sharding key.
//!
//! ## Adding one
//!
//! Implement [`Vindex`] and call [`register`] with a constructor. The planner
//! discovers it through the VSchema by name; nothing else changes.

pub mod cursor;
pub mod impls;
pub mod registry;
pub mod vindex;

pub use cursor::VCursor;
pub use registry::{create, register, registered_kinds, VindexBuilder};
pub use vindex::{Vindex, VindexParams, VindexRef};

pub use vituss_core::{KeyspaceId, ShardDestination, Value};
