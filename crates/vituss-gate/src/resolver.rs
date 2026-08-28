//! Destination → shard names.
//!
//! Turning a keyspace ID into a shard is a lookup in the serving graph, which the
//! gate keeps a live copy of. It is on the hot path of every routed query, so it
//! is a plain in-memory scan over a handful of key ranges and nothing more.

use std::collections::HashMap;

use parking_lot::RwLock;

use vituss_core::{Error, KeyRange, Result, ShardDestination, TabletType};
use vituss_topo::SrvKeyspace;

#[derive(Default)]
pub struct Resolver {
    serving: RwLock<HashMap<String, SrvKeyspace>>,
}

impl Resolver {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn update(&self, keyspace: impl Into<String>, srv: SrvKeyspace) {
        self.serving.write().insert(keyspace.into(), srv);
    }

    pub fn keyspaces(&self) -> Vec<String> {
        let mut v: Vec<String> = self.serving.read().keys().cloned().collect();
        v.sort();
        v
    }

    pub fn shard_names(&self, keyspace: &str, tablet_type: TabletType) -> Result<Vec<String>> {
        let serving = self.serving.read();
        let srv = serving.get(keyspace).ok_or_else(|| {
            Error::not_found(format!(
                "keyspace {keyspace:?} is not in this cell's serving graph; \
                 it may not exist, or the serving graph may not have been rebuilt since it was created"
            ))
        })?;
        let shards = srv.shards(tablet_type);
        if shards.is_empty() {
            return Err(Error::unavailable(format!(
                "no shard of keyspace {keyspace} is serving {tablet_type} queries"
            )));
        }
        Ok(shards.iter().map(|s| s.name.clone()).collect())
    }

    /// The shards a destination resolves to, in key-range order and without
    /// duplicates.
    pub fn resolve(
        &self,
        keyspace: &str,
        destination: &ShardDestination,
        tablet_type: TabletType,
    ) -> Result<Vec<String>> {
        let serving = self.serving.read();
        let srv = serving
            .get(keyspace)
            .ok_or_else(|| Error::not_found(format!("keyspace {keyspace:?} is not in the serving graph")))?;
        let shards = srv.shards(tablet_type);
        if shards.is_empty() {
            return Err(Error::unavailable(format!(
                "no shard of keyspace {keyspace} is serving {tablet_type} queries"
            )));
        }

        let all = || -> Vec<String> { shards.iter().map(|s| s.name.clone()).collect() };

        let out = match destination {
            ShardDestination::AllShards => all(),
            // Deterministically the first, not a random one: a query that could
            // go anywhere should still be reproducible, and the first shard is as
            // good a choice as any.
            ShardDestination::AnyShard => vec![shards[0].name.clone()],
            ShardDestination::None => Vec::new(),
            ShardDestination::Shard(name) => {
                if !shards.iter().any(|s| &s.name == name) {
                    return Err(Error::not_found(format!(
                        "shard {name:?} is not serving in keyspace {keyspace}; serving shards are: {}",
                        all().join(", ")
                    )));
                }
                vec![name.clone()]
            }
            ShardDestination::KeyspaceId(k) => {
                let owner = shards
                    .iter()
                    .find(|s| s.key_range.contains(k.as_bytes()))
                    .ok_or_else(|| {
                        Error::internal(format!(
                            "no shard of {keyspace} covers keyspace id {k}; the serving graph has a gap"
                        ))
                    })?;
                vec![owner.name.clone()]
            }
            ShardDestination::KeyspaceIds(ids) => {
                let mut out = Vec::new();
                for id in ids {
                    if let Some(owner) = shards.iter().find(|s| s.key_range.contains(id.as_bytes())) {
                        if !out.contains(&owner.name) {
                            out.push(owner.name.clone());
                        }
                    }
                }
                out
            }
            ShardDestination::KeyRange(kr) => shards
                .iter()
                .filter(|s| s.key_range.intersects(kr))
                .map(|s| s.name.clone())
                .collect(),
            ShardDestination::ExactKeyRange(kr) => {
                let matching: Vec<&vituss_topo::ShardReference> =
                    shards.iter().filter(|s| s.key_range.intersects(kr)).collect();
                // Used by resharding tooling, which must not read part of a shard.
                if !covers_exactly(kr, &matching) {
                    return Err(Error::invalid(format!(
                        "key range {kr} does not align with shard boundaries in keyspace {keyspace}"
                    )));
                }
                matching.iter().map(|s| s.name.clone()).collect()
            }
        };
        Ok(out)
    }
}

fn covers_exactly(kr: &KeyRange, shards: &[&vituss_topo::ShardReference]) -> bool {
    if shards.is_empty() {
        return false;
    }
    let mut sorted: Vec<&&vituss_topo::ShardReference> = shards.iter().collect();
    sorted.sort_by(|a, b| a.key_range.start.cmp(&b.key_range.start));
    sorted[0].key_range.start == kr.start
        && sorted[sorted.len() - 1].key_range.end == kr.end
        && sorted
            .windows(2)
            .all(|w| w[0].key_range.end == w[1].key_range.start)
}
