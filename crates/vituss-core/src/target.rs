//! Tablet identity and query targets.

use std::fmt;
use std::str::FromStr;

use crate::error::Error;

/// The role a tablet plays for its shard.
///
/// Vituss keeps Vitess's vocabulary because the routing rules depend on it: reads
/// marked `@replica` must never land on the primary, and only the primary accepts
/// writes. How the role is *enforced* differs per engine (MySQL `super_read_only`,
/// PostgreSQL hot standby, SQL Server AG read-only routing) and is the backend's
/// job, not the router's.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TabletType {
    /// Read-write. Exactly one per shard.
    #[default]
    Primary,
    /// Replica eligible for promotion; serves OLTP reads.
    Replica,
    /// Replica not eligible for promotion; serves analytics / batch reads.
    Rdonly,
    /// Being backed up or restored; not serving.
    Backup,
    /// Restoring from a backup; not serving.
    Restore,
    /// Present in topology but deliberately not serving.
    Drained,
    /// Reachable but unhealthy.
    Unhealthy,
}

impl TabletType {
    pub fn is_serving(self) -> bool {
        matches!(self, Self::Primary | Self::Replica | Self::Rdonly)
    }
    pub fn accepts_writes(self) -> bool {
        self == Self::Primary
    }
}

impl fmt::Display for TabletType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::Primary => "primary",
            Self::Replica => "replica",
            Self::Rdonly => "rdonly",
            Self::Backup => "backup",
            Self::Restore => "restore",
            Self::Drained => "drained",
            Self::Unhealthy => "unhealthy",
        };
        f.write_str(s)
    }
}

impl FromStr for TabletType {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            // "master" is accepted for compatibility with older Vitess configs.
            "primary" | "master" => Ok(Self::Primary),
            "replica" => Ok(Self::Replica),
            "rdonly" | "batch" => Ok(Self::Rdonly),
            "backup" => Ok(Self::Backup),
            "restore" => Ok(Self::Restore),
            "drained" => Ok(Self::Drained),
            other => Err(Error::invalid(format!("unknown tablet type {other:?}"))),
        }
    }
}

/// A globally unique tablet name: `<cell>-<uid>`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
pub struct TabletAlias {
    pub cell: String,
    pub uid: u32,
}

impl TabletAlias {
    pub fn new(cell: impl Into<String>, uid: u32) -> Self {
        Self { cell: cell.into(), uid }
    }
}

impl fmt::Display for TabletAlias {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{:010}", self.cell, self.uid)
    }
}

impl FromStr for TabletAlias {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (cell, uid) = s
            .rsplit_once('-')
            .ok_or_else(|| Error::invalid(format!("invalid tablet alias {s:?}: expected <cell>-<uid>")))?;
        Ok(Self {
            cell: cell.to_string(),
            uid: uid.parse().map_err(|_| Error::invalid(format!("invalid tablet uid in {s:?}")))?,
        })
    }
}

/// Where a query is bound: keyspace + shard + tablet role.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default, serde::Serialize, serde::Deserialize)]
pub struct Target {
    pub keyspace: String,
    pub shard: String,
    pub tablet_type: TabletType,
    /// Restricts routing to one cell (data centre). `None` means any cell,
    /// subject to the gate's cell preference.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cell: Option<String>,
}

impl Target {
    pub fn new(keyspace: impl Into<String>, shard: impl Into<String>, tablet_type: TabletType) -> Self {
        Self { keyspace: keyspace.into(), shard: shard.into(), tablet_type, cell: None }
    }
}

impl fmt::Display for Target {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}@{}", self.keyspace, self.shard, self.tablet_type)
    }
}
