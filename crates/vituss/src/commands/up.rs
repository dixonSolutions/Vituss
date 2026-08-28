//! `vituss up` — the whole cluster in one process.

use std::path::PathBuf;

use anyhow::anyhow;
use clap::Args;

use super::common;

#[derive(Args)]
pub struct Up {
    /// Cluster configuration file (YAML or JSON).
    #[arg(long, short = 'c', env = "VITUSS_CONFIG")]
    config: PathBuf,

    /// Directory for a persistent topology. Omit to keep the topology in memory,
    /// which leaves nothing behind when the process exits.
    #[arg(long, env = "VITUSS_TOPO")]
    topo: Option<PathBuf>,

    /// The SQL dialect clients will speak to this gate. Independent of what the
    /// shards run: a MySQL client can be served from PostgreSQL shards.
    #[arg(long, default_value = "mysql", env = "VITUSS_CLIENT_DIALECT")]
    client_dialect: String,

    /// Address for the MySQL protocol server. Pass `off` to disable it.
    #[arg(long, default_value = "127.0.0.1:15306")]
    mysql_addr: String,

    /// Address for the PostgreSQL protocol server. Pass `off` to disable it.
    #[arg(long, default_value = "127.0.0.1:15432")]
    postgres_addr: String,

    /// Keyspace new connections start in.
    #[arg(long)]
    keyspace: Option<String>,

    /// Statements to run once the cluster is up, before accepting clients.
    /// Repeatable. Useful for creating tables in a throwaway cluster.
    #[arg(long = "exec", short = 'e')]
    exec: Vec<String>,

    /// Run the `--exec` statements and exit instead of serving.
    #[arg(long)]
    once: bool,
}

impl Up {
    pub async fn run(self) -> anyhow::Result<()> {
        let (gate, config) =
            common::gate_from_config(&self.config, self.topo.as_ref(), &self.client_dialect).await?;

        let default_keyspace = self
            .keyspace
            .clone()
            .or_else(|| config.keyspaces.first().map(|k| k.name.clone()));

        for (name, engines) in summarise(&config) {
            tracing::info!(keyspace = %name, engines = %engines, "keyspace ready");
        }

        if !self.exec.is_empty() {
            let session = common::session(default_keyspace.clone());
            for sql in &self.exec {
                let result = gate
                    .execute(sql, &session, &Default::default())
                    .await
                    .map_err(|e| anyhow!("{sql}\n  {}", e.message))?;
                common::print_result(&result);
            }
        }

        if self.once {
            return Ok(());
        }

        #[cfg(feature = "wire")]
        {
            let mut servers: Vec<tokio::task::JoinHandle<()>> = Vec::new();

            if self.mysql_addr != "off" {
                let gate = gate.clone();
                let addr = self.mysql_addr.clone();
                let ks = default_keyspace.clone();
                println!("MySQL clients:      mysql -h 127.0.0.1 -P {} -u vituss", port_of(&addr));
                servers.push(tokio::spawn(async move {
                    if let Err(e) = vituss_wire::mysql::serve(gate, &addr, ks).await {
                        tracing::error!(error = %e.message, "MySQL server stopped");
                    }
                }));
            }

            if self.postgres_addr != "off" {
                let gate = gate.clone();
                let addr = self.postgres_addr.clone();
                let ks = default_keyspace.clone();
                println!("PostgreSQL clients: psql -h 127.0.0.1 -p {} -U vituss", port_of(&addr));
                servers.push(tokio::spawn(async move {
                    if let Err(e) = vituss_wire::postgres::serve(gate, &addr, ks).await {
                        tracing::error!(error = %e.message, "PostgreSQL server stopped");
                    }
                }));
            }

            if servers.is_empty() {
                return Err(anyhow!("both protocol servers are disabled; nothing to do"));
            }

            // Health is re-checked in the background so a shard that comes back
            // after a blip starts serving again without a restart.
            let health_gate = gate.clone();
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(std::time::Duration::from_secs(5));
                loop {
                    ticker.tick().await;
                    health_gate.refresh_health().await;
                }
            });

            println!("Press Ctrl-C to stop.");
            tokio::signal::ctrl_c().await.ok();
            println!("\nShutting down.");
            for s in servers {
                s.abort();
            }
        }

        #[cfg(not(feature = "wire"))]
        {
            return Err(anyhow!(
                "this build has no protocol servers (the `wire` feature is off); \
                 use --once with --exec, or rebuild with --features wire"
            ));
        }

        #[cfg(feature = "wire")]
        Ok(())
    }
}

fn port_of(addr: &str) -> &str {
    addr.rsplit(':').next().unwrap_or(addr)
}

/// One line per keyspace: which engines its shards actually run.
fn summarise(config: &vituss_ctl::ClusterConfig) -> Vec<(String, String)> {
    config
        .keyspaces
        .iter()
        .map(|ks| {
            let shards = ks.shards.shard_names(ks.sharded).unwrap_or_default();
            let mut engines: Vec<String> = ks
                .tablets
                .iter()
                .map(|t| t.dialect.clone().unwrap_or_else(|| ks.dialect.clone()))
                .collect();
            if engines.is_empty() {
                engines.push(ks.dialect.clone());
            }
            engines.sort();
            engines.dedup();
            (
                ks.name.clone(),
                format!("{} shard(s) on {}", shards.len(), engines.join(" + ")),
            )
        })
        .collect()
}
