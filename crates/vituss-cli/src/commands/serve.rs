//! `vituss serve` — the gateway alone, against an existing topology.

use std::path::PathBuf;

use anyhow::anyhow;
use clap::Args;

use vituss_gate::Gate;

use super::common;

#[derive(Args)]
pub struct Serve {
    /// Directory holding the topology.
    #[arg(long, env = "VITUSS_TOPO")]
    topo: PathBuf,

    /// Cell this gate serves. Defaults to the first cell in the topology.
    #[arg(long, env = "VITUSS_CELL")]
    cell: Option<String>,

    #[arg(long, default_value = "mysql", env = "VITUSS_CLIENT_DIALECT")]
    client_dialect: String,

    #[arg(long, default_value = "127.0.0.1:15306")]
    mysql_addr: String,

    #[arg(long, default_value = "127.0.0.1:15432")]
    postgres_addr: String,

    #[arg(long)]
    keyspace: Option<String>,
}

impl Serve {
    pub async fn run(self) -> anyhow::Result<()> {
        let topo = common::open_topo(Some(&self.topo))?;
        let dialect = vituss_dialect::get(&self.client_dialect).map_err(|e| anyhow!("{}", e.message))?;
        let gate = Gate::bootstrap(topo, self.cell, dialect)
            .await
            .map_err(|e| anyhow!("starting the gate: {}", e.message))?;

        #[cfg(feature = "wire")]
        {
            let mut servers = Vec::new();
            if self.mysql_addr != "off" {
                let (g, addr, ks) = (gate.clone(), self.mysql_addr.clone(), self.keyspace.clone());
                servers.push(tokio::spawn(async move {
                    if let Err(e) = vituss_wire::mysql::serve(g, &addr, ks).await {
                        tracing::error!(error = %e.message, "MySQL server stopped");
                    }
                }));
            }
            if self.postgres_addr != "off" {
                let (g, addr, ks) = (gate.clone(), self.postgres_addr.clone(), self.keyspace.clone());
                servers.push(tokio::spawn(async move {
                    if let Err(e) = vituss_wire::postgres::serve(g, &addr, ks).await {
                        tracing::error!(error = %e.message, "PostgreSQL server stopped");
                    }
                }));
            }
            if servers.is_empty() {
                return Err(anyhow!("both protocol servers are disabled; nothing to do"));
            }

            // The topology is the source of truth and it changes underneath a
            // running gate — a reshard, a failover, a new VSchema. Re-reading it
            // is what makes those take effect without a restart.
            let refresher = gate.clone();
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(std::time::Duration::from_secs(5));
                loop {
                    ticker.tick().await;
                    if let Err(e) = refresher.reload_serving_graph().await {
                        tracing::warn!(error = %e.message, "could not refresh the serving graph");
                    }
                    if let Err(e) = refresher.reload_vschema().await {
                        tracing::warn!(error = %e.message, "could not refresh the VSchema");
                    }
                    refresher.refresh_health().await;
                }
            });

            tokio::signal::ctrl_c().await.ok();
            for s in servers {
                s.abort();
            }
            Ok(())
        }

        #[cfg(not(feature = "wire"))]
        Err(anyhow!("this build has no protocol servers (the `wire` feature is off)"))
    }
}
