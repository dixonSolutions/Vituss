//! The pluggable key/value store the topology is built on.
//!
//! Vituss needs very little from a topology store: versioned reads and writes,
//! prefix listing, compare-and-swap, a lock, and a change feed. Keeping that
//! surface small is what allows the same cluster metadata to sit in etcd,
//! Consul, ZooKeeper, a file tree or memory — chosen per deployment rather than
//! baked in.

use async_trait::async_trait;

use vituss_core::Result;

/// Opaque version of a stored value, used for compare-and-swap.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize)]
pub struct Version(pub u64);

/// A value together with the version it was read at.
#[derive(Debug, Clone, PartialEq)]
pub struct Versioned {
    pub data: Vec<u8>,
    pub version: Version,
}

/// A change observed on a watched prefix.
#[derive(Debug, Clone, PartialEq)]
pub enum WatchEvent {
    Put { path: String, data: Vec<u8>, version: Version },
    Delete { path: String },
}

/// A held topology lock. Dropping it releases the lock.
///
/// Locks exist so that two `vtctld` operations cannot reshard the same keyspace
/// at once. They are advisory: every writer must take them, nothing enforces it.
#[async_trait]
pub trait LockHandle: Send + Sync {
    /// Confirm the lock is still held. Long operations must check before they
    /// act on anything they read while holding it.
    async fn check(&self) -> Result<()>;
    /// Release early rather than at drop.
    async fn unlock(self: Box<Self>) -> Result<()>;
}

impl std::fmt::Debug for dyn LockHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("LockHandle")
    }
}

#[async_trait]
pub trait TopoStore: Send + Sync + 'static {
    /// Human-readable implementation name, for logs and `vituss ctl status`.
    fn name(&self) -> &'static str;

    async fn get(&self, path: &str) -> Result<Option<Versioned>>;

    /// Every path under `prefix`, sorted. Prefix is a `/`-separated directory
    /// path; the result contains full paths, not basenames.
    async fn list(&self, prefix: &str) -> Result<Vec<String>>;

    /// Write `data`. When `expected` is `Some`, the write fails unless the
    /// current version matches — that is how concurrent `vtctld`s avoid
    /// clobbering each other. `Some(None)` semantics (create-only) are expressed
    /// by [`TopoStore::create`].
    async fn put(&self, path: &str, data: &[u8], expected: Option<Version>) -> Result<Version>;

    /// Write only if the path does not exist.
    async fn create(&self, path: &str, data: &[u8]) -> Result<Version>;

    async fn delete(&self, path: &str, expected: Option<Version>) -> Result<()>;

    /// Delete everything under a prefix.
    async fn delete_prefix(&self, prefix: &str) -> Result<()>;

    async fn lock(&self, path: &str, reason: &str) -> Result<Box<dyn LockHandle>>;

    /// Stream of changes under a prefix. The first messages replay the current
    /// contents, so a subscriber never misses state that existed before it
    /// subscribed.
    async fn watch(&self, prefix: &str) -> Result<tokio::sync::mpsc::Receiver<WatchEvent>>;
}
