//! The `vituss` command-line tool.
//!
//! One binary that can be a whole cluster (`vituss up`), just the query gateway
//! (`vituss serve`), or the administrative client (`vituss apply`, `status`,
//! `query`, `explain`).

mod commands;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "vituss",
    version,
    about = "Vituss — sharding, routing and cluster management for MySQL, PostgreSQL, SQL Server and SQLite",
    long_about = "Vituss puts a routing and cluster-management layer in front of database servers \
                  you already run. It does not implement a storage engine or a SQL grammar of its \
                  own: queries are parsed with the engine's own grammar and executed by the engine \
                  itself. What Vituss adds is sharding, routing, cross-shard query assembly, \
                  transactions and cluster topology."
)]
struct Cli {
    /// Log level: error, warn, info, debug, trace.
    #[arg(long, short = 'v', global = true, default_value = "info", env = "VITUSS_LOG")]
    log: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run a whole cluster in this process: topology, tablets, gate and the
    /// client-facing protocol servers. The fastest way to try Vituss.
    Up(commands::up::Up),

    /// Run only the query gateway, against an existing topology.
    Serve(commands::serve::Serve),

    /// Make a cluster's topology match a configuration file.
    Apply(commands::apply::Apply),

    /// Check a configuration file without touching a cluster.
    Validate(commands::apply::Validate),

    /// Show what the cluster looks like: keyspaces, shards, tablets, engines.
    Status(commands::status::Status),

    /// Run one query against a cluster and print the result.
    Query(commands::query::Query),

    /// Show how a query would be routed, without running it.
    Explain(commands::query::Explain),

    /// List the SQL engines and sharding functions this build supports.
    Capabilities(commands::capabilities::Capabilities),
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| format!("vituss={},warn", cli.log).into()),
        )
        .with_target(false)
        .init();

    match cli.command {
        Command::Up(c) => c.run().await,
        Command::Serve(c) => c.run().await,
        Command::Apply(c) => c.run().await,
        Command::Validate(c) => c.run().await,
        Command::Status(c) => c.run().await,
        Command::Query(c) => c.run().await,
        Command::Explain(c) => c.run().await,
        Command::Capabilities(c) => c.run().await,
    }
}
