//! `vituss apply` and `vituss validate`.

use std::path::PathBuf;

use anyhow::anyhow;
use clap::Args;

use vituss_ctl::Ctl;

use super::common;

#[derive(Args)]
pub struct Apply {
    #[arg(long, short = 'c', env = "VITUSS_CONFIG")]
    config: PathBuf,

    /// Directory holding the topology to update.
    #[arg(long, env = "VITUSS_TOPO")]
    topo: PathBuf,

    /// Report what would change without changing anything.
    #[arg(long)]
    dry_run: bool,
}

impl Apply {
    pub async fn run(self) -> anyhow::Result<()> {
        let config = common::load_config(&self.config)?;
        if self.dry_run {
            // The configuration is fully validated — engines, vindexes, shard
            // coverage, cross-keyspace references — without writing anything.
            println!("Configuration is valid. Not applied (--dry-run).");
            return Ok(());
        }

        let topo = common::open_topo(Some(&self.topo))?;
        let report = Ctl::new(topo)
            .apply(&config)
            .await
            .map_err(|e| anyhow!("{}", e.message))?;
        println!("{}", report.summary());
        Ok(())
    }
}

#[derive(Args)]
pub struct Validate {
    #[arg(long, short = 'c', env = "VITUSS_CONFIG")]
    config: PathBuf,
}

impl Validate {
    pub async fn run(self) -> anyhow::Result<()> {
        let config = common::load_config(&self.config)?;
        println!(
            "OK: {} cell(s), {} keyspace(s).",
            config.cells.len(),
            config.keyspaces.len()
        );
        for ks in &config.keyspaces {
            let shards = ks.shards.shard_names(ks.sharded).map_err(|e| anyhow!("{}", e.message))?;
            println!(
                "  {:<20} {:<10} {:>2} shard(s)  {:>2} table(s)  {:>2} vindex(es)",
                ks.name,
                ks.dialect,
                shards.len(),
                ks.vschema.tables.len(),
                ks.vschema.vindexes.len()
            );
        }
        Ok(())
    }
}
