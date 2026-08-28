//! `vituss capabilities` — what this build supports.

use clap::Args;

use vituss_dialect::TwoPcStyle;

#[derive(Args)]
pub struct Capabilities {
    /// Print as JSON.
    #[arg(long)]
    json: bool,
}

impl Capabilities {
    pub async fn run(self) -> anyhow::Result<()> {
        // Names, not aliases, so each engine appears once.
        let mut engines: Vec<String> = vituss_dialect::registered()
            .into_iter()
            .filter_map(|n| vituss_dialect::get(&n).ok().map(|d| d.name().to_string()))
            .collect();
        engines.sort();
        engines.dedup();

        let drivers = {
            let mut d: Vec<String> = vituss_backend::registered()
                .into_iter()
                .filter_map(|n| vituss_dialect::get(&n).ok().map(|d| d.name().to_string()))
                .collect();
            d.sort();
            d.dedup();
            d
        };

        let mut vindexes = vituss_vindex::registered_kinds();
        vindexes.sort_unstable();

        if self.json {
            let value = serde_json::json!({
                "engines": engines,
                "drivers": drivers,
                "vindexes": vindexes,
            });
            println!("{}", serde_json::to_string_pretty(&value)?);
            return Ok(());
        }

        println!("SQL engines (dialects):");
        println!();
        println!(
            "  {:<12} {:<7} {:<12} {:<8} {:<9} driver",
            "engine", "port", "placeholder", "quote", "2PC"
        );
        for name in &engines {
            let d = vituss_dialect::get(name).expect("just listed");
            let caps = d.capabilities();
            let two_pc = match caps.two_pc {
                TwoPcStyle::Xa => "XA",
                TwoPcStyle::PreparedTransaction => "PREPARE",
                TwoPcStyle::ExternalCoordinator => "external",
                TwoPcStyle::None => "none",
            };
            let placeholder = vituss_dialect::render::placeholder_for(caps.placeholder_style, 0);
            let quote = format!("{}x{}", caps.identifier_quote, matching_quote(caps.identifier_quote));
            let driver = if drivers.contains(name) { "compiled in" } else { "NOT COMPILED IN" };
            println!(
                "  {:<12} {:<7} {:<12} {:<8} {:<9} {}",
                d.name(),
                d.default_port(),
                placeholder,
                quote,
                two_pc,
                driver
            );
        }

        println!("\nSharding functions (vindexes):\n");
        for kind in &vindexes {
            println!("  {kind}");
        }

        println!(
            "\nA driver marked NOT COMPILED IN can still be planned for, but not connected to. \
             \nRebuild with the matching feature to enable it."
        );
        Ok(())
    }
}

fn matching_quote(open: char) -> char {
    match open {
        '[' => ']',
        c => c,
    }
}
