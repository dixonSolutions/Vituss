//! `vituss query` and `vituss explain` — one-shot access to a cluster.

use std::path::PathBuf;

use anyhow::anyhow;
use clap::Args;

use super::common;

#[derive(Args)]
pub struct Query {
    #[arg(long, short = 'c', env = "VITUSS_CONFIG")]
    config: PathBuf,

    #[arg(long, env = "VITUSS_TOPO")]
    topo: Option<PathBuf>,

    /// Keyspace to run in. Defaults to the first in the configuration.
    #[arg(long, short = 'k')]
    keyspace: Option<String>,

    #[arg(long, default_value = "mysql")]
    client_dialect: String,

    /// The statements to run, in order. All of them share one session, so a
    /// `BEGIN` in the first applies to the rest.
    #[arg(required = true)]
    sql: Vec<String>,
}

impl Query {
    pub async fn run(self) -> anyhow::Result<()> {
        let (gate, config) =
            common::gate_from_config(&self.config, self.topo.as_ref(), &self.client_dialect).await?;
        let keyspace = self
            .keyspace
            .or_else(|| config.keyspaces.first().map(|k| k.name.clone()));
        let session = common::session(keyspace);

        for sql in &self.sql {
            let result = gate
                .execute(sql, &session, &Default::default())
                .await
                .map_err(|e| anyhow!("{sql}\n  {}", e.message))?;
            common::print_result(&result);
        }
        Ok(())
    }
}

#[derive(Args)]
pub struct Explain {
    #[arg(long, short = 'c', env = "VITUSS_CONFIG")]
    config: PathBuf,

    #[arg(long, env = "VITUSS_TOPO")]
    topo: Option<PathBuf>,

    #[arg(long, short = 'k')]
    keyspace: Option<String>,

    #[arg(long, default_value = "mysql")]
    client_dialect: String,

    /// The statement to explain.
    sql: String,
}

impl Explain {
    pub async fn run(self) -> anyhow::Result<()> {
        let (gate, config) =
            common::gate_from_config(&self.config, self.topo.as_ref(), &self.client_dialect).await?;
        let keyspace = self
            .keyspace
            .or_else(|| config.keyspaces.first().map(|k| k.name.clone()));

        // Planning alone, with no execution: this shows the routing decision, and
        // it is what you read when a query is unexpectedly slow.
        let plan = gate
            .plan(&self.sql, keyspace.as_deref())
            .map_err(|e| anyhow!("{}", e.message))?;

        println!("{}", plan.explain());
        println!("keyspaces touched: {}", plan.keyspaces.join(", "));
        if plan.is_dml {
            println!("this statement writes");
        }
        Ok(())
    }
}
