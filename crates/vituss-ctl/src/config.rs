//! Declarative cluster configuration.
//!
//! A whole cluster — cells, keyspaces, shards, tablets, VSchemas — in one file
//! that can be reviewed, diffed and committed. Applying it is idempotent, so the
//! same file describes both "create this cluster" and "make the cluster look like
//! this".
//!
//! This is the one place where a Vituss deployment says which engine it runs on.
//! Change `dialect: mysql` to `dialect: postgres`, point the DSNs elsewhere, and
//! nothing else in the configuration moves.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use vituss_core::{Error, KeyRange, Result, TabletType};

/// The whole cluster.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// Cells (failure domains). Defaults to a single cell named `zone1`.
    #[serde(default = "default_cells")]
    pub cells: Vec<String>,
    pub keyspaces: Vec<KeyspaceConfig>,
    /// Table-level routing overrides, applied cluster-wide.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub routing_rules: BTreeMap<String, Vec<String>>,
}

fn default_cells() -> Vec<String> {
    vec!["zone1".to_string()]
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyspaceConfig {
    pub name: String,
    /// Which engine this keyspace runs on: `mysql`, `postgres`, `mssql`,
    /// `sqlite`, or any dialect a plug-in has registered.
    pub dialect: String,
    #[serde(default)]
    pub sharded: bool,
    /// Shard layout. Either a count (split evenly) or explicit ranges.
    #[serde(default)]
    pub shards: ShardSpec,
    /// The logical schema for this keyspace, in VSchema form.
    #[serde(default)]
    pub vschema: vituss_vschema::KeyspaceSpec,
    /// Tablets. When omitted, one primary per shard is generated from
    /// [`KeyspaceConfig::dsn_template`].
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tablets: Vec<TabletConfig>,
    /// Template for generated DSNs. `{keyspace}`, `{shard}` and `{index}` are
    /// substituted; `{shard}` is the shard name with `-` replaced by `_` so it is
    /// usable as a file or database name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dsn_template: Option<String>,
    /// Database name for generated tablets, with the same substitutions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub database_template: Option<String>,
}

/// How a keyspace is divided.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ShardSpec {
    /// `shards: 4` — split the key space into four equal ranges.
    Count(u32),
    /// `shards: ["-80", "80-"]` — explicit ranges.
    Ranges(Vec<String>),
}

impl Default for ShardSpec {
    fn default() -> Self {
        Self::Count(1)
    }
}

impl ShardSpec {
    /// The shard names this spec describes.
    pub fn shard_names(&self, sharded: bool) -> Result<Vec<String>> {
        match self {
            Self::Count(n) => {
                if !sharded && *n > 1 {
                    return Err(Error::invalid(
                        "an unsharded keyspace cannot have more than one shard",
                    ));
                }
                Ok(KeyRange::even_split(*n)?.iter().map(|k| k.name()).collect())
            }
            Self::Ranges(names) => {
                if names.is_empty() {
                    return Err(Error::invalid("'shards' must list at least one shard"));
                }
                // Parsed now so a typo is a configuration error rather than a
                // routing failure at the first query.
                for n in names {
                    KeyRange::parse(n)?;
                }
                Ok(names.clone())
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TabletConfig {
    pub shard: String,
    #[serde(default = "default_cell")]
    pub cell: String,
    #[serde(default)]
    pub tablet_type: TabletType,
    /// Connection string for this tablet's database.
    pub dsn: String,
    /// Engine override, when this shard runs something different from the rest of
    /// its keyspace — which is what a shard-by-shard engine migration looks like.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dialect: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hostname: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_connections: Option<u32>,
}

fn default_cell() -> String {
    "zone1".to_string()
}

impl ClusterConfig {
    pub fn from_yaml(s: &str) -> Result<Self> {
        serde_yaml::from_str(s).map_err(|e| Error::invalid(format!("invalid cluster config: {e}")))
    }

    pub fn from_json(s: &str) -> Result<Self> {
        serde_json::from_str(s).map_err(|e| Error::invalid(format!("invalid cluster config: {e}")))
    }

    /// Load from a file, choosing the format by extension.
    pub fn load(path: &std::path::Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| Error::invalid(format!("cannot read {}: {e}", path.display())))?;
        match path.extension().and_then(|e| e.to_str()) {
            Some("json") => Self::from_json(&text),
            _ => Self::from_yaml(&text),
        }
    }

    /// Check the configuration without touching a cluster.
    ///
    /// Every one of these is a mistake that would otherwise surface as a confusing
    /// runtime failure hours later, so they are all caught here.
    pub fn validate(&self) -> Result<()> {
        if self.cells.is_empty() {
            return Err(Error::invalid("at least one cell is required"));
        }
        if self.keyspaces.is_empty() {
            return Err(Error::invalid("at least one keyspace is required"));
        }

        let mut seen = std::collections::HashSet::new();
        for ks in &self.keyspaces {
            if !seen.insert(&ks.name) {
                return Err(Error::invalid(format!("keyspace {:?} is defined twice", ks.name)));
            }
            // Fail on an unknown engine here, not when a query needs it.
            vituss_dialect::get(&ks.dialect)?;

            let shards = ks.shards.shard_names(ks.sharded)?;
            if ks.sharded && shards.len() == 1 && shards[0] == "-" {
                return Err(Error::invalid(format!(
                    "keyspace {:?} is marked sharded but has a single full-range shard; \
                     set 'shards' to a count or a list of ranges",
                    ks.name
                )));
            }
            if !ks.sharded && !ks.vschema.vindexes.is_empty() {
                return Err(Error::invalid(format!(
                    "keyspace {:?} is unsharded but declares vindexes",
                    ks.name
                )));
            }

            for t in &ks.tablets {
                if !shards.contains(&t.shard) {
                    return Err(Error::invalid(format!(
                        "keyspace {:?}: tablet for shard {:?}, which is not one of {}",
                        ks.name,
                        t.shard,
                        shards.join(", ")
                    )));
                }
                if let Some(d) = &t.dialect {
                    vituss_dialect::get(d)?;
                }
                if !self.cells.contains(&t.cell) {
                    return Err(Error::invalid(format!(
                        "keyspace {:?}: tablet in cell {:?}, which is not declared",
                        ks.name, t.cell
                    )));
                }
            }

            if ks.tablets.is_empty() && ks.dsn_template.is_none() {
                return Err(Error::invalid(format!(
                    "keyspace {:?} has neither 'tablets' nor 'dsn_template'; Vituss has no way to \
                     reach its databases",
                    ks.name
                )));
            }

            // Every shard needs a primary, or it cannot accept writes.
            if !ks.tablets.is_empty() {
                for shard in &shards {
                    let has_primary = ks
                        .tablets
                        .iter()
                        .any(|t| &t.shard == shard && t.tablet_type == TabletType::Primary);
                    if !has_primary {
                        return Err(Error::invalid(format!(
                            "keyspace {:?}: shard {shard} has no primary tablet, so it cannot accept writes",
                            ks.name
                        )));
                    }
                }
            }
        }

        // Building the VSchema is the real validation: it instantiates every
        // vindex and resolves every cross reference.
        let mut spec = vituss_vschema::VSchemaSpec::default();
        for ks in &self.keyspaces {
            let mut k = ks.vschema.clone();
            k.sharded = ks.sharded;
            k.dialect = Some(ks.dialect.clone());
            spec.keyspaces.insert(ks.name.clone(), k);
        }
        spec.routing_rules = self.routing_rules.clone();
        vituss_vschema::VSchema::build(&spec)?;
        Ok(())
    }
}

impl KeyspaceConfig {
    /// The tablets to create, generating them from the template when none are
    /// listed explicitly.
    pub fn resolved_tablets(&self) -> Result<Vec<TabletConfig>> {
        if !self.tablets.is_empty() {
            return Ok(self.tablets.clone());
        }
        let template = self.dsn_template.as_ref().ok_or_else(|| {
            Error::invalid(format!("keyspace {:?} needs 'tablets' or 'dsn_template'", self.name))
        })?;
        let shards = self.shards.shard_names(self.sharded)?;
        Ok(shards
            .iter()
            .enumerate()
            .map(|(i, shard)| TabletConfig {
                shard: shard.clone(),
                cell: default_cell(),
                tablet_type: TabletType::Primary,
                dsn: substitute(template, &self.name, shard, i),
                dialect: None,
                database: self
                    .database_template
                    .as_ref()
                    .map(|t| substitute(t, &self.name, shard, i)),
                schema: None,
                hostname: None,
                max_connections: None,
            })
            .collect())
    }
}

fn substitute(template: &str, keyspace: &str, shard: &str, index: usize) -> String {
    template
        .replace("{keyspace}", keyspace)
        // Shard names contain `-`, which is not legal in most database names.
        .replace("{shard}", &shard.replace('-', "_"))
        .replace("{index}", &index.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    const EXAMPLE: &str = r#"
cells: [zone1]
keyspaces:
  - name: commerce
    dialect: sqlite
    sharded: true
    shards: 4
    dsn_template: "sqlite:/tmp/{keyspace}_{shard}.db"
    vschema:
      vindexes:
        hash: { type: hash }
      tables:
        user:
          column_vindexes:
            - { column: user_id, name: hash }
"#;

    #[test]
    fn a_shard_count_expands_to_even_ranges() {
        let c = ClusterConfig::from_yaml(EXAMPLE).unwrap();
        c.validate().unwrap();
        let names = c.keyspaces[0].shards.shard_names(true).unwrap();
        assert_eq!(names.len(), 4);
        assert_eq!(names[0], "-40");
        assert_eq!(names[3], "c0-");
    }

    #[test]
    fn tablets_are_generated_from_the_template() {
        let c = ClusterConfig::from_yaml(EXAMPLE).unwrap();
        let tablets = c.keyspaces[0].resolved_tablets().unwrap();
        assert_eq!(tablets.len(), 4);
        // The shard's `-` is not legal in a filename, so it is substituted.
        assert_eq!(tablets[0].dsn, "sqlite:/tmp/commerce__40.db");
        assert!(tablets.iter().all(|t| t.tablet_type == TabletType::Primary));
    }

    #[test]
    fn an_unknown_engine_is_caught_before_anything_is_created() {
        let bad = EXAMPLE.replace("dialect: sqlite", "dialect: oracle");
        let err = ClusterConfig::from_yaml(&bad).unwrap().validate().unwrap_err();
        assert!(err.message.contains("unknown SQL dialect"), "{}", err.message);
    }

    #[test]
    fn a_sharded_table_without_a_vindex_is_caught_here_too() {
        let bad = EXAMPLE.replace(
            "          column_vindexes:\n            - { column: user_id, name: hash }\n",
            "",
        );
        let err = ClusterConfig::from_yaml(&bad).unwrap().validate().unwrap_err();
        assert!(err.message.contains("needs a unique column_vindex"), "{}", err.message);
    }

    #[test]
    fn a_shard_with_no_primary_is_refused() {
        let cfg = r#"
cells: [zone1]
keyspaces:
  - name: main
    dialect: sqlite
    shards: ["-"]
    tablets:
      - { shard: "-", tablet_type: replica, dsn: "sqlite::memory:" }
"#;
        let err = ClusterConfig::from_yaml(cfg).unwrap().validate().unwrap_err();
        assert!(err.message.contains("no primary tablet"), "{}", err.message);
    }
}
