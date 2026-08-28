//! Filesystem topology store.
//!
//! One file per record under a root directory, which makes the whole cluster
//! state readable with `cat` and diffable in git. Suitable for single-machine
//! deployments, development clusters and disaster-recovery snapshots of a real
//! store; not for a multi-node cluster, where the locks would not be shared.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::mpsc;

use vituss_core::{Error, Result};

use crate::store::{LockHandle, TopoStore, Version, Versioned, WatchEvent};

/// Stored envelope: the version travels with the data so a restart does not reset it.
#[derive(serde::Serialize, serde::Deserialize)]
struct Envelope {
    version: u64,
    #[serde(with = "base64_bytes")]
    data: Vec<u8>,
}

mod base64_bytes {
    use serde::{Deserialize, Deserializer, Serializer};

    /// Records are JSON, so they are stored as readable text where possible and
    /// only escaped when they are not valid UTF-8.
    pub fn serialize<S: Serializer>(v: &[u8], s: S) -> Result<S::Ok, S::Error> {
        match std::str::from_utf8(v) {
            Ok(text) => s.serialize_str(text),
            Err(_) => s.serialize_str(&format!("\u{0}hex:{}", hex_encode(v))),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        let s = String::deserialize(d)?;
        match s.strip_prefix("\u{0}hex:") {
            Some(h) => hex_decode(h).map_err(serde::de::Error::custom),
            None => Ok(s.into_bytes()),
        }
    }

    fn hex_encode(v: &[u8]) -> String {
        v.iter().map(|b| format!("{b:02x}")).collect()
    }

    fn hex_decode(s: &str) -> Result<Vec<u8>, String> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|e| e.to_string()))
            .collect()
    }
}

pub struct FileStore {
    root: PathBuf,
    counter: Arc<parking_lot::Mutex<u64>>,
}

impl FileStore {
    /// Open (creating if needed) a store rooted at `root`.
    pub fn open(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        std::fs::create_dir_all(&root)
            .map_err(|e| Error::internal(format!("cannot create topology root {}: {e}", root.display())))?;
        Ok(Self { root, counter: Arc::new(parking_lot::Mutex::new(0)) })
    }

    /// Reject paths that would escape the root. Topology paths are built by
    /// Vituss, but keyspace and shard names come from users.
    fn resolve(&self, path: &str) -> Result<PathBuf> {
        let rel = path.trim_start_matches('/');
        if rel.split('/').any(|c| c == ".." || c == "." || c.is_empty()) {
            return Err(Error::invalid(format!("invalid topology path {path:?}")));
        }
        Ok(self.root.join(format!("{rel}.json")))
    }

    fn next_version(&self) -> u64 {
        let mut c = self.counter.lock();
        *c += 1;
        // Mixed with the wall clock so versions keep increasing across restarts.
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        now.wrapping_mul(1_000_000).wrapping_add(*c)
    }

    fn read(&self, file: &Path) -> Result<Option<Versioned>> {
        match std::fs::read_to_string(file) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(Error::internal(format!("reading {}: {e}", file.display()))),
            Ok(text) => {
                let env: Envelope = serde_json::from_str(&text)
                    .map_err(|e| Error::internal(format!("corrupt topology record {}: {e}", file.display())))?;
                Ok(Some(Versioned { data: env.data, version: Version(env.version) }))
            }
        }
    }

    fn write(&self, file: &Path, data: &[u8], version: u64) -> Result<()> {
        if let Some(dir) = file.parent() {
            std::fs::create_dir_all(dir)
                .map_err(|e| Error::internal(format!("creating {}: {e}", dir.display())))?;
        }
        let env = Envelope { version, data: data.to_vec() };
        let text = serde_json::to_string_pretty(&env).map_err(|e| Error::internal(e.to_string()))?;
        // Write-then-rename: a crash never leaves a half-written record.
        let tmp = file.with_extension("json.tmp");
        std::fs::write(&tmp, text).map_err(|e| Error::internal(format!("writing {}: {e}", tmp.display())))?;
        std::fs::rename(&tmp, file)
            .map_err(|e| Error::internal(format!("renaming into {}: {e}", file.display())))
    }
}

struct FileLock {
    path: PathBuf,
}

#[async_trait]
impl LockHandle for FileLock {
    async fn check(&self) -> Result<()> {
        if self.path.exists() {
            Ok(())
        } else {
            Err(Error::aborted(format!("lost the topology lock {}", self.path.display())))
        }
    }
    async fn unlock(self: Box<Self>) -> Result<()> {
        let _ = std::fs::remove_file(&self.path);
        Ok(())
    }
}

impl Drop for FileLock {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

#[async_trait]
impl TopoStore for FileStore {
    fn name(&self) -> &'static str {
        "file"
    }

    async fn get(&self, path: &str) -> Result<Option<Versioned>> {
        self.read(&self.resolve(path)?)
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let mut out = Vec::new();
        let start = self.root.join(prefix.trim_start_matches('/'));
        let base = if start.is_dir() { start } else { start.parent().map(Path::to_path_buf).unwrap_or_default() };
        let mut stack = vec![base];
        while let Some(dir) = stack.pop() {
            let Ok(entries) = std::fs::read_dir(&dir) else { continue };
            for entry in entries.flatten() {
                let p = entry.path();
                if p.is_dir() {
                    stack.push(p);
                } else if p.extension().is_some_and(|e| e == "json") {
                    if let Ok(rel) = p.strip_prefix(&self.root) {
                        let logical = format!("/{}", rel.with_extension("").to_string_lossy());
                        if logical.starts_with(prefix) {
                            out.push(logical);
                        }
                    }
                }
            }
        }
        out.sort();
        Ok(out)
    }

    async fn put(&self, path: &str, data: &[u8], expected: Option<Version>) -> Result<Version> {
        let file = self.resolve(path)?;
        if let Some(want) = expected {
            let have = self.read(&file)?.map(|v| v.version);
            if have != Some(want) {
                return Err(Error::aborted(format!(
                    "concurrent modification of {path}: expected version {want:?}, found {have:?}"
                )));
            }
        }
        let version = self.next_version();
        self.write(&file, data, version)?;
        Ok(Version(version))
    }

    async fn create(&self, path: &str, data: &[u8]) -> Result<Version> {
        let file = self.resolve(path)?;
        if file.exists() {
            return Err(Error::already_exists(format!("{path} already exists")));
        }
        let version = self.next_version();
        self.write(&file, data, version)?;
        Ok(Version(version))
    }

    async fn delete(&self, path: &str, expected: Option<Version>) -> Result<()> {
        let file = self.resolve(path)?;
        let have = self.read(&file)?.ok_or_else(|| Error::not_found(format!("{path} does not exist")))?;
        if expected.is_some_and(|w| w != have.version) {
            return Err(Error::aborted(format!("concurrent modification of {path}")));
        }
        std::fs::remove_file(&file).map_err(|e| Error::internal(format!("deleting {path}: {e}")))
    }

    async fn delete_prefix(&self, prefix: &str) -> Result<()> {
        for p in self.list(prefix).await? {
            let _ = std::fs::remove_file(self.resolve(&p)?);
        }
        Ok(())
    }

    async fn lock(&self, path: &str, reason: &str) -> Result<Box<dyn LockHandle>> {
        let file = self.resolve(path)?.with_extension("lock");
        if let Some(dir) = file.parent() {
            let _ = std::fs::create_dir_all(dir);
        }
        // create_new is the atomic primitive: exactly one caller wins.
        match std::fs::OpenOptions::new().write(true).create_new(true).open(&file) {
            Ok(_) => {
                let _ = std::fs::write(&file, reason);
                Ok(Box::new(FileLock { path: file }))
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                let holder = std::fs::read_to_string(&file).unwrap_or_default();
                Err(Error::aborted(format!("{path} is already locked ({holder})")))
            }
            Err(e) => Err(Error::internal(format!("locking {path}: {e}"))),
        }
    }

    async fn watch(&self, prefix: &str) -> Result<mpsc::Receiver<WatchEvent>> {
        // Polled rather than inotify-based: the file store is for development and
        // single-node use, where a one-second delay in noticing a topology change
        // is not worth a platform-specific dependency.
        let (tx, rx) = mpsc::channel(256);
        let root = self.root.clone();
        let prefix = prefix.to_string();
        tokio::spawn(async move {
            let store = match FileStore::open(&root) {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!(%e, "file topology watcher could not open the store");
                    return;
                }
            };
            let mut seen: std::collections::HashMap<String, Version> = std::collections::HashMap::new();
            loop {
                let Ok(paths) = store.list(&prefix).await else { break };
                let mut current = std::collections::HashMap::new();
                for path in paths {
                    if let Ok(Some(v)) = store.get(&path).await {
                        current.insert(path.clone(), v.version);
                        if seen.get(&path) != Some(&v.version)
                            && tx.send(WatchEvent::Put { path, data: v.data, version: v.version }).await.is_err()
                        {
                            return;
                        }
                    }
                }
                for gone in seen.keys().filter(|k| !current.contains_key(*k)) {
                    if tx.send(WatchEvent::Delete { path: gone.clone() }).await.is_err() {
                        return;
                    }
                }
                seen = current;
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        });
        Ok(rx)
    }
}
