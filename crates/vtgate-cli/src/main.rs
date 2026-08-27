//! Minimal demo binary: registers all three dialect parsers with
//! `vtgate-core`'s `Router` and runs one query through it, to show the
//! plumbing end-to-end. This is not VTGate's wire protocol server —
//! there's no listener, no session/connection state, no real vschema.

use clap::Parser as ClapParser;
use vtgate_core::{DialectRegistry, Router};

#[derive(ClapParser)]
struct Args {
    /// Which dialect to parse the query as.
    #[arg(long, default_value = "mysql")]
    dialect: String,

    /// The SQL query to route.
    query: String,

    /// Comma-separated shard names the stub router scatters reads to.
    #[arg(long, default_value = "shard-0,shard-1")]
    shards: String,
}

fn build_registry() -> DialectRegistry {
    let mut registry = DialectRegistry::new();
    registry
        .register(Box::new(sql_dialect_mysql::MySqlSqlParser))
        .register(Box::new(sql_dialect_postgres::PostgresSqlParser))
        .register(Box::new(sql_dialect_generic::GenericSqlParser));
    registry
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let registry = build_registry();

    let available: Vec<_> = registry.dialects().collect();
    if !available.contains(&args.dialect.as_str()) {
        anyhow::bail!(
            "unknown dialect {:?}; available: {}",
            args.dialect,
            available.join(", ")
        );
    }

    let shards: Vec<String> = args
        .shards
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();
    let router = Router::new(registry, shards);

    let routed = router.route(&args.dialect, &args.query)?;

    println!("dialect:  {}", routed.dialect);
    println!("kind:     {:?}", routed.kind);
    println!(
        "tables:   {}",
        routed
            .tables
            .iter()
            .map(|t| t.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );
    println!("decision: {:?}", routed.decision);
    println!("sql:      {}", routed.sql);

    Ok(())
}
