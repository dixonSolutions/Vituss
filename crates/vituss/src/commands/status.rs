//! `vituss status` — what the cluster looks like right now.

use std::path::PathBuf;

use anyhow::anyhow;
use clap::Args;

use vituss_core::TabletType;

use super::common;

#[derive(Args)]
pub struct Status {
    /// Directory holding the topology. Omit to read a cluster config instead.
    #[arg(long, env = "VITUSS_TOPO")]
    topo: Option<PathBuf>,

    /// Cluster configuration file, when there is no persistent topology.
    #[arg(long, short = 'c', env = "VITUSS_CONFIG")]
    config: Option<PathBuf>,

    /// Also connect to every tablet and report whether it is reachable.
    #[arg(long)]
    probe: bool,
}

impl Status {
    pub async fn run(self) -> anyhow::Result<()> {
        let topo = match (&self.topo, &self.config) {
            (Some(dir), _) => common::open_topo(Some(dir))?,
            (None, Some(cfg)) => {
                // No persistent topology: build one in memory from the config so
                // `status` describes what `vituss up` would create.
                let config = common::load_config(cfg)?;
                let topo = common::open_topo(None)?;
                vituss_ctl::Ctl::new(topo.clone())
                    .apply(&config)
                    .await
                    .map_err(|e| anyhow!("{}", e.message))?;
                topo
            }
            (None, None) => {
                return Err(anyhow!("pass --topo <dir> or --config <file>"));
            }
        };

        let cells = topo.list_cells().await.map_err(|e| anyhow!("{}", e.message))?;
        println!("cells: {}", if cells.is_empty() { "(none)".into() } else { cells.join(", ") });

        let keyspaces = topo.list_keyspaces().await.map_err(|e| anyhow!("{}", e.message))?;
        if keyspaces.is_empty() {
            println!("no keyspaces");
            return Ok(());
        }

        for name in keyspaces {
            let ks = topo.get_keyspace(&name).await.map_err(|e| anyhow!("{}", e.message))?;
            let shards = topo.get_shards(&name).await.map_err(|e| anyhow!("{}", e.message))?;
            println!(
                "\nkeyspace {}  engine={}  {}  {} shard(s)",
                ks.name,
                ks.dialect,
                if shards.len() > 1 { "sharded" } else { "unsharded" },
                shards.len()
            );

            for shard in &shards {
                let tablets = topo
                    .get_shard_tablets(&name, &shard.name, None)
                    .await
                    .map_err(|e| anyhow!("{}", e.message))?;
                let serving = if shard.is_primary_serving { "serving" } else { "NOT SERVING" };
                println!("  shard {:<10} {:<12} {} tablet(s)", shard.name, serving, tablets.len());

                for t in &tablets {
                    // The engine is per tablet, not per keyspace: this is what a
                    // partly-migrated keyspace looks like.
                    let engine = &t.backend.dialect;
                    let mut line = format!(
                        "    {:<22} {:<8} {:<10} {}",
                        t.alias.to_string(),
                        t.tablet_type.to_string(),
                        engine,
                        t.backend.redacted_dsn()
                    );
                    if self.probe {
                        line.push_str(&match probe(t).await {
                            Ok(v) => format!("   [up: {v}]"),
                            Err(e) => format!("   [DOWN: {e}]"),
                        });
                    }
                    println!("{line}");
                }
                if !shard.source_shards.is_empty() {
                    println!(
                        "    (filling from {})",
                        shard
                            .source_shards
                            .iter()
                            .map(|s| s.shard.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    );
                }
            }
        }
        Ok(())
    }
}

async fn probe(tablet: &vituss_topo::Tablet) -> Result<String, String> {
    let target = vituss_core::Target::new(&tablet.keyspace, &tablet.shard, TabletType::Primary);
    let server = vituss_tablet::TabletServer::open(target, tablet.backend.clone())
        .await
        .map_err(|e| e.message)?;
    let health = <vituss_tablet::TabletServer as vituss_tablet::QueryService>::health(&server).await;
    match (health.serving, health.server_version) {
        (true, Some(v)) => Ok(v.lines().next().unwrap_or("").trim().to_string()),
        (true, None) => Ok("reachable".to_string()),
        (false, _) => Err(health.error.unwrap_or_else(|| "not serving".to_string())),
    }
}
