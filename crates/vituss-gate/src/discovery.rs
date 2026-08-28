//! Finding a tablet to send a query to.
//!
//! The gate keeps a live view of which tablets are serving which shard, and picks
//! one per query. The interesting decisions are all about *not* picking: a
//! replica that has fallen too far behind, or a tablet in another cell when a
//! local one exists, is worse than no choice at all.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

use vituss_core::{Error, Result, TabletType, Target};
use vituss_tablet::{QueryService, TabletHealth};

/// One tablet the gate can route to.
#[derive(Clone)]
pub struct TabletEntry {
    pub service: Arc<dyn QueryService>,
    pub cell: String,
    pub health: TabletHealth,
}

/// Everything the gate knows about where tablets are.
#[derive(Default)]
pub struct Discovery {
    /// Keyed by `(keyspace, shard, tablet_type)`.
    tablets: RwLock<HashMap<(String, String, TabletType), Vec<TabletEntry>>>,
    /// The gate's own cell, preferred when several tablets could serve.
    local_cell: RwLock<Option<String>>,
    /// Replicas lagging by more than this are not used for reads.
    max_replica_lag_secs: RwLock<u64>,
}

impl Discovery {
    pub fn new(local_cell: Option<String>) -> Self {
        Self {
            tablets: RwLock::new(HashMap::new()),
            local_cell: RwLock::new(local_cell),
            max_replica_lag_secs: RwLock::new(30),
        }
    }

    pub fn set_max_replica_lag(&self, secs: u64) {
        *self.max_replica_lag_secs.write() = secs;
    }

    pub fn add(&self, entry: TabletEntry) {
        let t = entry.service.target().clone();
        self.tablets
            .write()
            .entry((t.keyspace, t.shard, t.tablet_type))
            .or_default()
            .push(entry);
    }

    pub fn remove_all(&self) {
        self.tablets.write().clear();
    }

    /// Update a tablet's health, which decides whether it stays eligible.
    pub fn update_health(&self, health: TabletHealth) {
        let key = (
            health.target.keyspace.clone(),
            health.target.shard.clone(),
            health.target.tablet_type,
        );
        if let Some(entries) = self.tablets.write().get_mut(&key) {
            for e in entries.iter_mut() {
                if e.service.target() == &health.target {
                    e.health = health.clone();
                }
            }
        }
    }

    pub fn all(&self) -> Vec<TabletEntry> {
        self.tablets.read().values().flatten().cloned().collect()
    }

    /// Pick a tablet for a target.
    ///
    /// Falls back from the requested tablet type to the primary for reads: a
    /// keyspace with no replicas should still answer `@replica` queries rather
    /// than fail, and the primary is always correct, just more loaded.
    pub fn pick(&self, target: &Target) -> Result<Arc<dyn QueryService>> {
        let candidates = self.eligible(target);
        if let Some(picked) = self.choose(candidates) {
            return Ok(picked);
        }

        if target.tablet_type != TabletType::Primary {
            let fallback = Target {
                tablet_type: TabletType::Primary,
                ..target.clone()
            };
            if let Some(picked) = self.choose(self.eligible(&fallback)) {
                tracing::debug!(%target, "no healthy replica; serving the read from the primary");
                return Ok(picked);
            }
        }

        Err(Error::unavailable(format!(
            "no healthy tablet is serving {target}{}",
            self.diagnose(target)
        )))
    }

    fn eligible(&self, target: &Target) -> Vec<TabletEntry> {
        let max_lag = *self.max_replica_lag_secs.read();
        let tablets = self.tablets.read();
        let Some(entries) = tablets.get(&(
            target.keyspace.clone(),
            target.shard.clone(),
            target.tablet_type,
        )) else {
            return Vec::new();
        };
        entries
            .iter()
            .filter(|e| e.health.serving)
            .filter(|e| target.cell.as_ref().is_none_or(|c| &e.cell == c))
            .filter(|e| {
                // A stale replica silently returns old data, which is worse than
                // an error, so it is taken out of rotation.
                target.tablet_type == TabletType::Primary
                    || e.health.replication_lag_secs.is_none_or(|l| l <= max_lag)
            })
            .cloned()
            .collect()
    }

    fn choose(&self, mut candidates: Vec<TabletEntry>) -> Option<Arc<dyn QueryService>> {
        if candidates.is_empty() {
            return None;
        }
        // Prefer the gate's own cell: a cross-datacentre hop costs more than any
        // load imbalance it would fix.
        if let Some(local) = self.local_cell.read().clone() {
            let local_only: Vec<TabletEntry> =
                candidates.iter().filter(|e| e.cell == local).cloned().collect();
            if !local_only.is_empty() {
                candidates = local_only;
            }
        }
        // Random rather than round-robin: no shared counter, and it spreads load
        // evenly across many gates without any of them coordinating.
        let i = rand::random::<usize>() % candidates.len();
        Some(candidates[i].service.clone())
    }

    /// Explain why a target has no tablet, since "unavailable" alone is not
    /// actionable at three in the morning.
    fn diagnose(&self, target: &Target) -> String {
        let tablets = self.tablets.read();
        let key = (target.keyspace.clone(), target.shard.clone(), target.tablet_type);
        match tablets.get(&key) {
            None => format!(
                ": no tablet of type {} is registered for shard {}/{}",
                target.tablet_type, target.keyspace, target.shard
            ),
            Some(entries) => {
                let reasons: Vec<String> = entries
                    .iter()
                    .map(|e| match (&e.health.error, e.health.replication_lag_secs) {
                        (Some(err), _) => format!("{}: {err}", e.cell),
                        (None, Some(lag)) if lag > *self.max_replica_lag_secs.read() => {
                            format!("{}: {lag}s behind", e.cell)
                        }
                        _ => format!("{}: not serving", e.cell),
                    })
                    .collect();
                format!(" ({} tablet(s) rejected — {})", entries.len(), reasons.join("; "))
            }
        }
    }
}
