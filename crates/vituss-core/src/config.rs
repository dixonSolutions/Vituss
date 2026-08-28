//! How to reach a database.
//!
//! Lives in `vituss-core` because both the topology (which stores it) and the
//! backend drivers (which act on it) need it, and neither should depend on the
//! other.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

/// Connection details for one shard's database.
///
/// The `dialect` field is the pivot of the whole design: it names which SQL
/// surface and which driver to use, so two shards of the same keyspace can run
/// different engines.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BackendConfig {
    /// Registered dialect name: `mysql`, `postgres`, `mssql`, `sqlite`, or a
    /// custom one registered by a downstream crate.
    pub dialect: String,
    /// Driver connection string. Its syntax belongs to the driver.
    ///
    /// `${VAR}` references are expanded from the environment at connect time, so
    /// the topology can be committed to version control without secrets in it.
    pub dsn: String,
    /// Database holding the shard's tables.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,
    /// Schema within the database, where the engine has both concepts
    /// (PostgreSQL, SQL Server).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    #[serde(default = "default_pool_size")]
    pub max_connections: u32,
    /// Connections reserved for statements inside a transaction. Kept separate so
    /// a flood of read queries cannot starve in-flight transactions and force
    /// them to roll back.
    #[serde(default = "default_pool_size")]
    pub max_transaction_connections: u32,
    #[serde(default = "default_query_timeout")]
    pub query_timeout_secs: u64,
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout_secs: u64,
    /// Extra driver options, passed through verbatim.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub options: BTreeMap<String, String>,
}

fn default_pool_size() -> u32 {
    20
}
fn default_query_timeout() -> u64 {
    30
}
fn default_connect_timeout() -> u64 {
    10
}

impl BackendConfig {
    pub fn new(dialect: impl Into<String>, dsn: impl Into<String>) -> Self {
        Self {
            dialect: dialect.into(),
            dsn: dsn.into(),
            database: None,
            schema: None,
            max_connections: default_pool_size(),
            max_transaction_connections: default_pool_size(),
            query_timeout_secs: default_query_timeout(),
            connect_timeout_secs: default_connect_timeout(),
            options: BTreeMap::new(),
        }
    }

    pub fn with_database(mut self, db: impl Into<String>) -> Self {
        self.database = Some(db.into());
        self
    }

    pub fn with_schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    /// The DSN with `${VAR}` references expanded from the environment.
    pub fn resolved_dsn(&self) -> Result<String> {
        expand_env(&self.dsn)
    }

    /// The DSN with any password removed, for logs and status pages.
    pub fn redacted_dsn(&self) -> String {
        redact(&self.dsn)
    }

    /// The schema Vituss should introspect: the explicit `schema`, else the
    /// database, else the engine's default.
    pub fn effective_schema(&self) -> Option<&str> {
        self.schema.as_deref().or(self.database.as_deref())
    }
}

/// Expand `${NAME}` from the environment, erroring on anything unset rather than
/// connecting with an empty password and getting a confusing auth failure.
fn expand_env(s: &str) -> Result<String> {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(start) = rest.find("${") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let end = after
            .find('}')
            .ok_or_else(|| Error::invalid(format!("unterminated ${{...}} in DSN {}", redact(s))))?;
        let name = &after[..end];
        let value = std::env::var(name).map_err(|_| {
            Error::invalid(format!("DSN references environment variable {name:?}, which is not set"))
        })?;
        out.push_str(&value);
        rest = &after[end + 1..];
    }
    out.push_str(rest);
    Ok(out)
}

/// Strip the password out of a `scheme://user:pass@host/db` style DSN.
fn redact(dsn: &str) -> String {
    let Some(scheme_end) = dsn.find("://") else { return dsn.to_string() };
    let (scheme, rest) = dsn.split_at(scheme_end + 3);
    let Some(at) = rest.find('@') else { return dsn.to_string() };
    let (creds, host) = rest.split_at(at);
    match creds.split_once(':') {
        Some((user, _)) => format!("{scheme}{user}:***{host}"),
        None => dsn.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn passwords_never_reach_the_logs() {
        let c = BackendConfig::new("mysql", "mysql://app:hunter2@db1:3306/commerce");
        assert_eq!(c.redacted_dsn(), "mysql://app:***@db1:3306/commerce");
        assert!(!c.redacted_dsn().contains("hunter2"));
    }

    #[test]
    fn env_references_are_expanded_and_missing_ones_are_an_error() {
        std::env::set_var("VITUSS_TEST_PW", "s3cret");
        let c = BackendConfig::new("mysql", "mysql://app:${VITUSS_TEST_PW}@db1/commerce");
        assert_eq!(c.resolved_dsn().unwrap(), "mysql://app:s3cret@db1/commerce");

        let missing = BackendConfig::new("mysql", "mysql://app:${VITUSS_NOT_SET_ANYWHERE}@db1/c");
        let err = missing.resolved_dsn().unwrap_err();
        assert!(err.message.contains("VITUSS_NOT_SET_ANYWHERE"), "{}", err.message);
    }
}
