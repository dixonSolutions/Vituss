//! # vituss-vschema
//!
//! The *logical* schema, as opposed to the physical one each shard's engine
//! holds. It answers: which keyspace does this table live in, is that keyspace
//! sharded, and which vindex decides where a row goes.
//!
//! The VSchema knows nothing about MySQL, PostgreSQL or SQL Server beyond the
//! name of the engine a keyspace runs on. Sharding is expressed entirely in terms
//! of vindexes over neutral values, which is what makes a keyspace portable
//! between engines.

pub mod model;
pub mod resolved;

use std::collections::HashMap;
use std::sync::Arc;

use vituss_core::{Error, KeyspaceId, Result};
use vituss_vindex::{create as create_vindex, VindexParams};

pub use model::{
    AutoIncrementSpec, ColumnSpec, ColumnVindexSpec, KeyspaceSpec, TableKind, TableSpec, VSchemaSpec,
    VindexSpec,
};
pub use resolved::{AutoIncrement, ColumnVindex, Keyspace, Table, TableName};

/// The resolved, cluster-wide VSchema.
#[derive(Debug, Clone, Default)]
pub struct VSchema {
    keyspaces: HashMap<String, Arc<Keyspace>>,
    /// Unqualified table name → every table with that name. A name that appears
    /// in more than one keyspace cannot be used unqualified.
    global_tables: HashMap<String, Vec<Arc<Table>>>,
    routing_rules: HashMap<String, Vec<TableName>>,
}

impl VSchema {
    /// Build a runtime VSchema from its declaration, instantiating every vindex.
    ///
    /// All validation happens here rather than at query time: an unknown vindex
    /// kind, a sharded table without a primary vindex, or a sequence in a sharded
    /// keyspace is a configuration error, and it should surface when the
    /// configuration is loaded.
    pub fn build(spec: &VSchemaSpec) -> Result<Self> {
        let mut keyspaces = HashMap::new();

        for (ks_name, ks_spec) in &spec.keyspaces {
            let mut vindexes: HashMap<String, VindexRefWithOwner> = HashMap::new();
            for (vname, vspec) in &ks_spec.vindexes {
                let mut params: VindexParams = vspec.params.clone();
                if let Some(owner) = &vspec.owner {
                    params.insert("owner".to_string(), owner.clone());
                }
                let vindex = create_vindex(&vspec.kind, vname, &params).map_err(|e| {
                    Error::invalid(format!("keyspace {ks_name}: vindex {vname:?}: {}", e.message))
                })?;
                vindexes.insert(vname.clone(), VindexRefWithOwner { vindex, owner: vspec.owner.clone() });
            }

            let mut tables = HashMap::new();
            for (tname, tspec) in &ks_spec.tables {
                let table = build_table(ks_name, ks_spec, tname, tspec, &vindexes)?;
                tables.insert(tname.clone(), Arc::new(table));
            }

            keyspaces.insert(
                ks_name.clone(),
                Arc::new(Keyspace {
                    name: ks_name.clone(),
                    sharded: ks_spec.sharded,
                    dialect: ks_spec.dialect.clone().unwrap_or_else(|| "mysql".to_string()),
                    tables,
                    vindexes: vindexes.into_iter().map(|(k, v)| (k, v.vindex)).collect(),
                    require_explicit_routing: ks_spec.require_explicit_routing,
                }),
            );
        }

        // An owned vindex must name a table that exists, or writes to it would
        // silently never happen.
        for (ks_name, ks_spec) in &spec.keyspaces {
            for (vname, vspec) in &ks_spec.vindexes {
                if let Some(owner) = &vspec.owner {
                    if !ks_spec.tables.contains_key(owner) {
                        return Err(Error::invalid(format!(
                            "keyspace {ks_name}: vindex {vname:?} is owned by table {owner:?}, \
                             which is not defined in this keyspace"
                        )));
                    }
                }
            }
        }

        let mut global_tables: HashMap<String, Vec<Arc<Table>>> = HashMap::new();
        for ks in keyspaces.values() {
            for table in ks.tables.values() {
                global_tables
                    .entry(table.name.to_lowercase())
                    .or_default()
                    .push(table.clone());
            }
        }

        let mut routing_rules = HashMap::new();
        for (from, to) in &spec.routing_rules {
            let targets = to
                .iter()
                .map(|t| TableName::parse(t, None))
                .collect::<Result<Vec<_>>>()
                .map_err(|e| Error::invalid(format!("routing rule {from:?}: {}", e.message)))?;
            routing_rules.insert(from.to_lowercase(), targets);
        }

        let vschema = Self { keyspaces, global_tables, routing_rules };
        vschema.validate_cross_references(spec)?;
        Ok(vschema)
    }

    pub fn from_json(s: &str) -> Result<Self> {
        Self::build(&VSchemaSpec::from_json(s)?)
    }

    pub fn from_yaml(s: &str) -> Result<Self> {
        Self::build(&VSchemaSpec::from_yaml(s)?)
    }

    pub fn keyspace(&self, name: &str) -> Result<&Arc<Keyspace>> {
        self.keyspaces
            .get(name)
            .or_else(|| {
                self.keyspaces
                    .iter()
                    .find(|(k, _)| k.eq_ignore_ascii_case(name))
                    .map(|(_, v)| v)
            })
            .ok_or_else(|| {
                let mut known: Vec<&str> = self.keyspaces.keys().map(String::as_str).collect();
                known.sort_unstable();
                Error::not_found(format!(
                    "unknown keyspace {name:?}; known keyspaces: {}",
                    if known.is_empty() { "(none)".into() } else { known.join(", ") }
                ))
            })
    }

    pub fn keyspaces(&self) -> impl Iterator<Item = &Arc<Keyspace>> {
        self.keyspaces.values()
    }

    pub fn keyspace_names(&self) -> Vec<String> {
        let mut v: Vec<String> = self.keyspaces.keys().cloned().collect();
        v.sort();
        v
    }

    /// Resolve a table reference to its keyspace.
    ///
    /// `keyspace` is the qualifier the query used, or the session's current
    /// keyspace. An unqualified name resolves globally only when it is
    /// unambiguous — otherwise the query is rejected rather than guessed at.
    pub fn find_table(&self, keyspace: Option<&str>, table: &str) -> Result<Arc<Table>> {
        // Routing rules take priority: they are how a table move is made visible
        // to clients without changing their SQL.
        let rule_key = match keyspace {
            Some(ks) => format!("{ks}.{table}").to_lowercase(),
            None => table.to_lowercase(),
        };
        if let Some(targets) = self.routing_rules.get(&rule_key) {
            match targets.len() {
                0 => {
                    return Err(Error::failed_precondition(format!(
                        "table {rule_key} is denied by a routing rule (a move is in progress)"
                    )))
                }
                1 => {
                    let t = &targets[0];
                    return self.lookup_exact(&t.keyspace, &t.table);
                }
                _ => {
                    return Err(Error::unsupported(format!(
                        "table {rule_key} routes to {} targets; a query must name one explicitly",
                        targets.len()
                    )))
                }
            }
        }

        match keyspace {
            Some(ks) => self.lookup_exact(ks, table),
            None => {
                let candidates = self
                    .global_tables
                    .get(&table.to_lowercase())
                    .map(Vec::as_slice)
                    .unwrap_or(&[]);
                match candidates.len() {
                    1 => Ok(candidates[0].clone()),
                    0 => Err(Error::not_found(format!("table {table:?} is not in the VSchema"))),
                    _ => Err(Error::invalid(format!(
                        "table {table:?} exists in {} keyspaces ({}); qualify it as <keyspace>.{table}",
                        candidates.len(),
                        candidates.iter().map(|t| t.keyspace.as_str()).collect::<Vec<_>>().join(", ")
                    ))),
                }
            }
        }
    }

    fn lookup_exact(&self, keyspace: &str, table: &str) -> Result<Arc<Table>> {
        let ks = self.keyspace(keyspace)?;
        ks.table(table)
            .cloned()
            .ok_or_else(|| Error::not_found(format!("table {keyspace}.{table} is not in the VSchema")))
    }

    /// True when the VSchema knows about this table at all.
    pub fn has_table(&self, keyspace: Option<&str>, table: &str) -> bool {
        self.find_table(keyspace, table).is_ok()
    }

    pub fn routing_rules(&self) -> &HashMap<String, Vec<TableName>> {
        &self.routing_rules
    }

    /// Every sequence table referenced by an auto-increment, so `vtctld` can
    /// check they exist before a keyspace goes live.
    pub fn sequences(&self) -> Vec<TableName> {
        let mut out: Vec<TableName> = self
            .keyspaces
            .values()
            .flat_map(|ks| ks.tables.values())
            .filter_map(|t| t.auto_increment.as_ref().map(|a| a.sequence.clone()))
            .collect();
        out.sort();
        out.dedup();
        out
    }

    fn validate_cross_references(&self, _spec: &VSchemaSpec) -> Result<()> {
        for ks in self.keyspaces.values() {
            for table in ks.tables.values() {
                if let Some(auto) = &table.auto_increment {
                    let seq = self.lookup_exact(&auto.sequence.keyspace, &auto.sequence.table).map_err(|_| {
                        Error::invalid(format!(
                            "table {}.{}: auto_increment names sequence {} which is not in the VSchema",
                            ks.name, table.name, auto.sequence
                        ))
                    })?;
                    if !seq.is_sequence() {
                        return Err(Error::invalid(format!(
                            "table {}.{}: auto_increment names {} but that table is not of type 'sequence'",
                            ks.name, table.name, auto.sequence
                        )));
                    }
                    if seq.sharded {
                        return Err(Error::invalid(format!(
                            "sequence {} lives in sharded keyspace {}; a sequence must be unsharded \
                             or every shard would hand out the same ids",
                            auto.sequence, seq.keyspace
                        )));
                    }
                }
                if let Some(source) = &table.source {
                    self.lookup_exact(&source.keyspace, &source.table).map_err(|_| {
                        Error::invalid(format!(
                            "reference table {}.{} names source {} which is not in the VSchema",
                            ks.name, table.name, source
                        ))
                    })?;
                }
            }
        }
        Ok(())
    }
}

struct VindexRefWithOwner {
    vindex: vituss_vindex::VindexRef,
    owner: Option<String>,
}

fn build_table(
    ks_name: &str,
    ks_spec: &KeyspaceSpec,
    tname: &str,
    tspec: &TableSpec,
    vindexes: &HashMap<String, VindexRefWithOwner>,
) -> Result<Table> {
    let mut column_vindexes = Vec::new();
    for cv in &tspec.column_vindexes {
        let entry = vindexes.get(&cv.name).ok_or_else(|| {
            Error::invalid(format!(
                "table {ks_name}.{tname}: column_vindex names {:?}, which is not defined in this keyspace's vindexes",
                cv.name
            ))
        })?;
        let columns = cv.column_names();
        if columns.is_empty() {
            return Err(Error::invalid(format!(
                "table {ks_name}.{tname}: column_vindex {:?} names no columns",
                cv.name
            )));
        }
        let expected = entry.vindex.column_count();
        if columns.len() != expected && !entry.vindex.accepts_partial_columns() {
            return Err(Error::invalid(format!(
                "table {ks_name}.{tname}: vindex {:?} takes {expected} column(s) but {} were given",
                cv.name,
                columns.len()
            )));
        }
        column_vindexes.push(ColumnVindex {
            columns,
            vindex: entry.vindex.clone(),
            owned: entry.owner.as_deref() == Some(tname),
        });
    }

    // Cheapest first: the planner takes the first usable one, so ordering here is
    // what makes it prefer a computation over a lookup round trip.
    column_vindexes.sort_by_key(|cv| cv.vindex.cost());

    let primary_vindex = column_vindexes.iter().find(|cv| cv.vindex.is_unique()).cloned();

    if ks_spec.sharded {
        match tspec.kind {
            TableKind::Normal => {
                if primary_vindex.is_none() {
                    return Err(Error::invalid(format!(
                        "table {ks_name}.{tname}: a table in a sharded keyspace needs a unique \
                         column_vindex to decide which shard its rows live on"
                    )));
                }
            }
            TableKind::Sequence => {
                return Err(Error::invalid(format!(
                    "table {ks_name}.{tname}: a sequence cannot live in a sharded keyspace"
                )))
            }
            // A reference table is copied to every shard, so it needs no vindex.
            TableKind::Reference => {}
        }
    } else if !column_vindexes.is_empty() {
        return Err(Error::invalid(format!(
            "table {ks_name}.{tname}: keyspace {ks_name} is unsharded, so its tables must not \
             declare column_vindexes"
        )));
    }

    let auto_increment = tspec
        .auto_increment
        .as_ref()
        .map(|a| -> Result<AutoIncrement> {
            Ok(AutoIncrement {
                column: a.column.clone(),
                sequence: TableName::parse(&a.sequence, Some(ks_name))?,
            })
        })
        .transpose()?;

    let source = tspec
        .source
        .as_ref()
        .map(|s| TableName::parse(s, Some(ks_name)))
        .transpose()?;

    Ok(Table {
        name: tname.to_string(),
        keyspace: ks_name.to_string(),
        kind: tspec.kind,
        sharded: ks_spec.sharded,
        column_vindexes,
        primary_vindex,
        auto_increment,
        columns: tspec.columns.clone(),
        column_list_authoritative: tspec.column_list_authoritative,
        primary_key: tspec.primary_key.clone(),
        source,
        // Reference tables in a sharded keyspace are readable from any shard;
        // writes are pinned to keyspace id 0 so there is one authoritative copy.
        pinned: if ks_spec.sharded && tspec.kind == TableKind::Reference {
            Some(KeyspaceId(vec![0; 8]))
        } else {
            None
        },
    })
}
