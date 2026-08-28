//! Driver registry: dialect name → backend constructor.
//!
//! The last piece of the plug-in story. A new engine needs a [`SqlDialect`]
//! (its SQL surface), a [`Backend`] (its driver) and one call to [`register`];
//! nothing above this crate has to know it exists.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, RwLock};

use once_cell::sync::Lazy;

use vituss_core::{BackendConfig, Error, Result};
use vituss_dialect::DialectRef;

use crate::backend::Backend;

/// Builds a connection pool from a config and the dialect it named.
pub type BackendBuilder = fn(
    BackendConfig,
    DialectRef,
) -> Pin<Box<dyn Future<Output = Result<Arc<dyn Backend>>> + Send>>;

static REGISTRY: Lazy<RwLock<HashMap<String, BackendBuilder>>> = Lazy::new(|| {
    let mut m: HashMap<String, BackendBuilder> = HashMap::new();
    register_builtins(&mut m);
    RwLock::new(m)
});

fn register_builtins(m: &mut HashMap<String, BackendBuilder>) {
    #[cfg(feature = "sqlite")]
    {
        let b: BackendBuilder = |cfg, d| {
            Box::pin(async move {
                Ok(Arc::new(crate::drivers::sqlite::SqliteBackend::connect(&cfg, d).await?) as Arc<dyn Backend>)
            })
        };
        for name in ["sqlite", "sqlite3", "libsql"] {
            m.insert(name.to_string(), b);
        }
    }
    #[cfg(feature = "mysql")]
    {
        let b: BackendBuilder = |cfg, d| {
            Box::pin(async move {
                Ok(Arc::new(crate::drivers::mysql::MySqlBackend::connect(&cfg, d).await?) as Arc<dyn Backend>)
            })
        };
        for name in ["mysql", "mariadb", "percona"] {
            m.insert(name.to_string(), b);
        }
    }
    #[cfg(feature = "postgres")]
    {
        let b: BackendBuilder = |cfg, d| {
            Box::pin(async move {
                Ok(Arc::new(crate::drivers::postgres::PostgresBackend::connect(&cfg, d).await?) as Arc<dyn Backend>)
            })
        };
        for name in ["postgres", "postgresql", "pgsql", "pg", "cockroach"] {
            m.insert(name.to_string(), b);
        }
    }
    #[cfg(feature = "mssql")]
    {
        let b: BackendBuilder = |cfg, d| {
            Box::pin(async move {
                Ok(Arc::new(crate::drivers::mssql::MsSqlBackend::connect(&cfg, d).await?) as Arc<dyn Backend>)
            })
        };
        for name in ["mssql", "sqlserver", "tds", "azuresql"] {
            m.insert(name.to_string(), b);
        }
    }
    let _ = m;
}

/// Register a driver for a dialect name, replacing any existing one.
pub fn register(dialect_name: impl Into<String>, builder: BackendBuilder) {
    REGISTRY
        .write()
        .expect("backend registry poisoned")
        .insert(dialect_name.into(), builder);
}

/// Open a pool for a shard.
pub async fn open(config: &BackendConfig) -> Result<Arc<dyn Backend>> {
    let dialect = vituss_dialect::get(&config.dialect)?;
    let builder = {
        let reg = REGISTRY.read().expect("backend registry poisoned");
        reg.get(&config.dialect.to_lowercase()).copied()
    };
    match builder {
        Some(b) => b(config.clone(), dialect).await,
        None => {
            let mut have = registered();
            have.sort();
            Err(Error::not_found(format!(
                "no driver compiled in for dialect {:?}; available drivers: {}. \
                 Enable the matching feature on vituss-backend (e.g. --features {})",
                config.dialect,
                if have.is_empty() { "(none)".to_string() } else { have.join(", ") },
                config.dialect
            )))
        }
    }
}

/// Every dialect name a driver is compiled in for.
pub fn registered() -> Vec<String> {
    REGISTRY.read().expect("backend registry poisoned").keys().cloned().collect()
}
