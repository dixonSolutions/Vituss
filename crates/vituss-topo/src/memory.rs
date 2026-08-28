//! In-memory topology store.
//!
//! Used by `vituss combo` (the whole cluster in one process) and by tests. It is
//! a complete implementation, not a stub: it versions, locks and watches exactly
//! like the persistent stores, so behaviour under test matches production.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::Mutex;
use tokio::sync::mpsc;

use vituss_core::{Error, Result};

use crate::store::{LockHandle, TopoStore, Versioned, Version, WatchEvent};

#[derive(Default)]
struct Inner {
    data: BTreeMap<String, Versioned>,
    next_version: u64,
    locks: BTreeMap<String, String>,
    watchers: Vec<(String, mpsc::Sender<WatchEvent>)>,
}

#[derive(Clone, Default)]
pub struct MemoryStore {
    inner: Arc<Mutex<Inner>>,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }

    fn notify(inner: &mut Inner, event: WatchEvent) {
        let path = match &event {
            WatchEvent::Put { path, .. } | WatchEvent::Delete { path } => path.clone(),
        };
        // Drop watchers whose receiver has gone away, so a long-lived store does
        // not accumulate dead senders.
        inner.watchers.retain(|(prefix, tx)| {
            if !path.starts_with(prefix.as_str()) {
                return !tx.is_closed();
            }
            tx.try_send(event.clone()).is_ok() || !tx.is_closed()
        });
    }
}

struct MemoryLock {
    store: Arc<Mutex<Inner>>,
    path: String,
    token: String,
}

#[async_trait]
impl LockHandle for MemoryLock {
    async fn check(&self) -> Result<()> {
        let inner = self.store.lock();
        match inner.locks.get(&self.path) {
            Some(t) if *t == self.token => Ok(()),
            _ => Err(Error::aborted(format!("lost the topology lock on {}", self.path))),
        }
    }

    async fn unlock(self: Box<Self>) -> Result<()> {
        let mut inner = self.store.lock();
        if inner.locks.get(&self.path).is_some_and(|t| *t == self.token) {
            inner.locks.remove(&self.path);
        }
        Ok(())
    }
}

impl Drop for MemoryLock {
    fn drop(&mut self) {
        let mut inner = self.store.lock();
        if inner.locks.get(&self.path).is_some_and(|t| *t == self.token) {
            inner.locks.remove(&self.path);
        }
    }
}

#[async_trait]
impl TopoStore for MemoryStore {
    fn name(&self) -> &'static str {
        "memory"
    }

    async fn get(&self, path: &str) -> Result<Option<Versioned>> {
        Ok(self.inner.lock().data.get(path).cloned())
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        Ok(self
            .inner
            .lock()
            .data
            .range(prefix.to_string()..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .map(|(k, _)| k.clone())
            .collect())
    }

    async fn put(&self, path: &str, data: &[u8], expected: Option<Version>) -> Result<Version> {
        let mut inner = self.inner.lock();
        if let Some(want) = expected {
            let have = inner.data.get(path).map(|v| v.version);
            if have != Some(want) {
                return Err(Error::aborted(format!(
                    "concurrent modification of {path}: expected version {want:?}, found {have:?}"
                )));
            }
        }
        inner.next_version += 1;
        let version = Version(inner.next_version);
        inner.data.insert(path.to_string(), Versioned { data: data.to_vec(), version });
        Self::notify(&mut inner, WatchEvent::Put { path: path.to_string(), data: data.to_vec(), version });
        Ok(version)
    }

    async fn create(&self, path: &str, data: &[u8]) -> Result<Version> {
        let mut inner = self.inner.lock();
        if inner.data.contains_key(path) {
            return Err(Error::already_exists(format!("{path} already exists")));
        }
        inner.next_version += 1;
        let version = Version(inner.next_version);
        inner.data.insert(path.to_string(), Versioned { data: data.to_vec(), version });
        Self::notify(&mut inner, WatchEvent::Put { path: path.to_string(), data: data.to_vec(), version });
        Ok(version)
    }

    async fn delete(&self, path: &str, expected: Option<Version>) -> Result<()> {
        let mut inner = self.inner.lock();
        match inner.data.get(path) {
            None => return Err(Error::not_found(format!("{path} does not exist"))),
            Some(v) if expected.is_some_and(|w| w != v.version) => {
                return Err(Error::aborted(format!("concurrent modification of {path}")))
            }
            _ => {}
        }
        inner.data.remove(path);
        Self::notify(&mut inner, WatchEvent::Delete { path: path.to_string() });
        Ok(())
    }

    async fn delete_prefix(&self, prefix: &str) -> Result<()> {
        let mut inner = self.inner.lock();
        let paths: Vec<String> = inner
            .data
            .range(prefix.to_string()..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .map(|(k, _)| k.clone())
            .collect();
        for p in paths {
            inner.data.remove(&p);
            Self::notify(&mut inner, WatchEvent::Delete { path: p });
        }
        Ok(())
    }

    async fn lock(&self, path: &str, reason: &str) -> Result<Box<dyn LockHandle>> {
        let token = uuid::Uuid::new_v4().to_string();
        {
            let mut inner = self.inner.lock();
            if let Some(holder) = inner.locks.get(path) {
                return Err(Error::aborted(format!("{path} is already locked (holder {holder})")));
            }
            inner.locks.insert(path.to_string(), token.clone());
        }
        tracing::debug!(path, reason, "topology lock acquired");
        Ok(Box::new(MemoryLock { store: self.inner.clone(), path: path.to_string(), token }))
    }

    async fn watch(&self, prefix: &str) -> Result<mpsc::Receiver<WatchEvent>> {
        let (tx, rx) = mpsc::channel(256);
        let mut inner = self.inner.lock();
        // Replay current state first so the subscriber starts from a complete view.
        for (path, v) in inner.data.range(prefix.to_string()..).take_while(|(k, _)| k.starts_with(prefix)) {
            let _ = tx.try_send(WatchEvent::Put {
                path: path.clone(),
                data: v.data.clone(),
                version: v.version,
            });
        }
        inner.watchers.push((prefix.to_string(), tx));
        Ok(rx)
    }
}
