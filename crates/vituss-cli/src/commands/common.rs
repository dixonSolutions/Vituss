//! Shared plumbing for the subcommands.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{anyhow, Context};

use vituss_core::{QueryResult, Session};
use vituss_ctl::{ClusterConfig, Ctl};
use vituss_engine::SessionRef;
use vituss_gate::Gate;
use vituss_topo::TopoServer;

/// Open the topology a command should work against.
///
/// A directory gives a persistent, human-readable topology; no directory gives an
/// in-memory one, which is what makes `vituss up --config` a single command with
/// nothing left behind.
pub fn open_topo(dir: Option<&Path>) -> anyhow::Result<TopoServer> {
    match dir {
        Some(d) => TopoServer::file(d)
            .map_err(|e| anyhow!("cannot open the topology at {}: {}", d.display(), e.message)),
        None => Ok(TopoServer::memory()),
    }
}

/// Load and validate a cluster config.
pub fn load_config(path: &Path) -> anyhow::Result<ClusterConfig> {
    let config = ClusterConfig::load(path)
        .map_err(|e| anyhow!("{}: {}", path.display(), e.message))
        .with_context(|| format!("reading {}", path.display()))?;
    config
        .validate()
        .map_err(|e| anyhow!("{}: {}", path.display(), e.message))?;
    Ok(config)
}

/// Bring a cluster up from a config file and return a ready gate.
pub async fn gate_from_config(
    config_path: &Path,
    topo_dir: Option<&PathBuf>,
    client_dialect: &str,
) -> anyhow::Result<(Arc<Gate>, ClusterConfig)> {
    let config = load_config(config_path)?;
    let topo = open_topo(topo_dir.map(|p| p.as_path()))?;

    let ctl = Ctl::new(topo.clone());
    let report = ctl
        .apply(&config)
        .await
        .map_err(|e| anyhow!("applying the cluster config: {}", e.message))?;
    if !report.is_empty() {
        tracing::info!("{}", report.summary().replace('\n', "; "));
    }

    let dialect = vituss_dialect::get(client_dialect).map_err(|e| anyhow!("{}", e.message))?;
    let cell = config.cells.first().cloned();
    let gate = Gate::bootstrap(topo, cell, dialect)
        .await
        .map_err(|e| anyhow!("starting the gate: {}", e.message))?;
    Ok((gate, config))
}

/// A fresh client session, optionally pinned to a keyspace.
pub fn session(keyspace: Option<String>) -> SessionRef {
    let mut s = Session::new();
    s.target_keyspace = keyspace;
    vituss_engine::new_session(s)
}

/// Print a result set as an aligned table, the way a SQL shell would.
pub fn print_result(result: &QueryResult) {
    if result.fields.is_empty() {
        println!(
            "OK, {} row(s) affected{}",
            result.rows_affected,
            result
                .last_insert_id
                .map(|id| format!(" (last insert id {id})"))
                .unwrap_or_default()
        );
        return;
    }

    let headers: Vec<String> = result.fields.iter().map(|f| f.name.clone()).collect();
    let rows: Vec<Vec<String>> = result
        .rows
        .iter()
        .map(|r| r.iter().map(|v| v.to_string()).collect())
        .collect();

    let widths: Vec<usize> = headers
        .iter()
        .enumerate()
        .map(|(i, h)| {
            rows.iter()
                .filter_map(|r| r.get(i))
                .map(|c| c.chars().count())
                .chain(std::iter::once(h.chars().count()))
                .max()
                .unwrap_or(0)
        })
        .collect();

    let rule: String = widths
        .iter()
        .map(|w| format!("+{}", "-".repeat(w + 2)))
        .collect::<String>()
        + "+";

    println!("{rule}");
    println!(
        "|{}|",
        headers
            .iter()
            .zip(&widths)
            .map(|(h, w)| format!(" {h:<w$} ", w = w))
            .collect::<Vec<_>>()
            .join("|")
    );
    println!("{rule}");
    for row in &rows {
        println!(
            "|{}|",
            widths
                .iter()
                .enumerate()
                .map(|(i, w)| {
                    let cell = row.get(i).map(String::as_str).unwrap_or("");
                    format!(" {cell:<w$} ", w = w)
                })
                .collect::<Vec<_>>()
                .join("|")
        );
    }
    println!("{rule}");
    println!("{} row(s)", rows.len());
    for w in &result.warnings {
        eprintln!("warning: {w}");
    }
}
