//! Working out which shards a statement needs.
//!
//! This is the step that makes Vituss more than a proxy: given a table's vindexes
//! and the predicates a query applies to it, decide whether the query can be
//! answered by one shard, a few, or all of them.

use std::collections::HashMap;

use sqlparser::ast::{BinaryOperator, Expr};

use vituss_vschema::{ColumnVindex, Table};

use crate::plan::{RouteOpcode, RouteValue};
use crate::util::{and_terms, as_column_ref, expr_to_route_value, or_terms};

/// What a query says about one column.
#[derive(Debug, Clone, PartialEq)]
pub enum Constraint {
    /// `col = <value>`
    Equal(RouteValue),
    /// `col IN (<values>)`
    In(Vec<RouteValue>),
    /// `col BETWEEN <lo> AND <hi>`, or a pair of inequalities that amount to one.
    Range { lo: Option<RouteValue>, hi: Option<RouteValue> },
    /// `col LIKE 'prefix%'`
    Prefix(RouteValue),
}

/// Everything the WHERE clause constrains, keyed by lower-cased column name.
///
/// Only top-level `AND` terms are collected: a predicate under an `OR` does not
/// hold for every row the query returns, so it cannot narrow the routing.
#[derive(Debug, Clone, Default)]
pub struct Constraints {
    pub by_column: HashMap<String, Constraint>,
    /// Alternative constraint sets from a top-level `OR`, each of which routes
    /// independently. `WHERE id = 1 OR id = 2` becomes two of these.
    pub alternatives: Vec<HashMap<String, Constraint>>,
}

impl Constraints {
    pub fn get(&self, column: &str) -> Option<&Constraint> {
        self.by_column.get(&column.to_lowercase())
    }

    pub fn columns(&self) -> Vec<String> {
        self.by_column.keys().cloned().collect()
    }
}

/// Extract routing constraints from a WHERE clause.
///
/// `qualifiers` names the aliases that refer to the table in question; a
/// predicate qualified with anything else belongs to a different table and is
/// ignored here.
pub fn constraints_for(where_clause: Option<&Expr>, qualifiers: &[String]) -> Constraints {
    let Some(expr) = where_clause else {
        return Constraints::default();
    };

    let mut out = Constraints::default();
    for term in and_terms(expr) {
        if let Some((col, c)) = constraint_from(term, qualifiers) {
            merge(&mut out.by_column, col, c);
        }
    }

    // A top-level OR of equality predicates on the same column still routes: it
    // is the union of the destinations. `and_terms` returned the OR untouched, so
    // look inside it here.
    let terms = and_terms(expr);
    if terms.len() == 1 {
        let branches = or_terms(terms[0]);
        if branches.len() > 1 {
            let mut alts = Vec::with_capacity(branches.len());
            for branch in branches {
                let mut set = HashMap::new();
                for term in and_terms(branch) {
                    if let Some((col, c)) = constraint_from(term, qualifiers) {
                        merge(&mut set, col, c);
                    }
                }
                // One unroutable branch makes the whole OR unroutable: those rows
                // could be anywhere.
                if set.is_empty() {
                    alts.clear();
                    break;
                }
                alts.push(set);
            }
            out.alternatives = alts;
        }
    }

    out
}

fn merge(map: &mut HashMap<String, Constraint>, column: String, c: Constraint) {
    use std::collections::hash_map::Entry;
    match map.entry(column) {
        Entry::Vacant(e) => {
            e.insert(c);
        }
        Entry::Occupied(mut e) => {
            // An equality is strictly more selective than a range, so it wins.
            if matches!(c, Constraint::Equal(_)) {
                e.insert(c);
            } else if let (Constraint::Range { lo: lo1, hi: hi1 }, Constraint::Range { lo: lo2, hi: hi2 }) =
                (e.get().clone(), c)
            {
                e.insert(Constraint::Range { lo: lo1.or(lo2), hi: hi1.or(hi2) });
            }
        }
    }
}

fn constraint_from(expr: &Expr, qualifiers: &[String]) -> Option<(String, Constraint)> {
    let matches_table = |qual: &Option<String>| match qual {
        // Unqualified: assume it belongs to the table being considered. Safe
        // because the caller only asks about one table at a time, and a genuinely
        // ambiguous column would have been rejected by the engine anyway.
        None => true,
        Some(q) => qualifiers.iter().any(|a| a.eq_ignore_ascii_case(q)),
    };

    match expr {
        Expr::BinaryOp { left, op, right } => {
            // Accept the predicate written either way round.
            let (col_expr, val_expr, op) = match (as_column_ref(left), as_column_ref(right)) {
                (Some(_), None) => (left, right, op.clone()),
                (None, Some(_)) => (right, left, flip(op.clone())),
                _ => return None,
            };
            let (qual, column) = as_column_ref(col_expr)?;
            if !matches_table(&qual) {
                return None;
            }
            let value = expr_to_route_value(val_expr)?;
            let c = match op {
                BinaryOperator::Eq => Constraint::Equal(value),
                BinaryOperator::Gt | BinaryOperator::GtEq => {
                    Constraint::Range { lo: Some(value), hi: None }
                }
                BinaryOperator::Lt | BinaryOperator::LtEq => {
                    Constraint::Range { lo: None, hi: Some(value) }
                }
                _ => return None,
            };
            Some((column.to_lowercase(), c))
        }
        Expr::InList { expr, list, negated: false } => {
            let (qual, column) = as_column_ref(expr)?;
            if !matches_table(&qual) {
                return None;
            }
            let values: Vec<RouteValue> = list.iter().map(expr_to_route_value).collect::<Option<_>>()?;
            Some((column.to_lowercase(), Constraint::In(values)))
        }
        Expr::Between { expr, negated: false, low, high } => {
            let (qual, column) = as_column_ref(expr)?;
            if !matches_table(&qual) {
                return None;
            }
            Some((
                column.to_lowercase(),
                Constraint::Range { lo: expr_to_route_value(low), hi: expr_to_route_value(high) },
            ))
        }
        Expr::Like { expr, pattern, negated: false, .. } => {
            let (qual, column) = as_column_ref(expr)?;
            if !matches_table(&qual) {
                return None;
            }
            // Only an anchored prefix narrows anything: `LIKE '%foo'` still has to
            // look everywhere.
            let RouteValue::Literal(vituss_core::Value::Text(p)) = expr_to_route_value(pattern)? else {
                return None;
            };
            let prefix = p.split(['%', '_']).next()?.to_string();
            if prefix.is_empty() {
                return None;
            }
            Some((
                column.to_lowercase(),
                Constraint::Prefix(RouteValue::Literal(vituss_core::Value::Text(prefix))),
            ))
        }
        Expr::Nested(inner) => constraint_from(inner, qualifiers),
        _ => None,
    }
}

fn flip(op: BinaryOperator) -> BinaryOperator {
    match op {
        BinaryOperator::Gt => BinaryOperator::Lt,
        BinaryOperator::Lt => BinaryOperator::Gt,
        BinaryOperator::GtEq => BinaryOperator::LtEq,
        BinaryOperator::LtEq => BinaryOperator::GtEq,
        other => other,
    }
}

/// How a table should be routed, given what the query constrains.
#[derive(Debug, Clone)]
pub struct Routing {
    pub opcode: RouteOpcode,
    pub vindex: Option<ColumnVindex>,
    pub values: Vec<RouteValue>,
}

impl Routing {
    pub fn scatter() -> Self {
        Self { opcode: RouteOpcode::Scatter, vindex: None, values: Vec::new() }
    }

    /// True when the route provably reaches exactly one shard.
    pub fn is_single_shard(&self) -> bool {
        matches!(
            self.opcode,
            RouteOpcode::Unsharded | RouteOpcode::EqualUnique | RouteOpcode::AnyShard | RouteOpcode::None
        )
    }
}

/// Choose the cheapest routing for a table.
///
/// The order of the checks *is* the cost model: an exact keyspace ID beats a set
/// of them, which beats a key range, which beats asking every shard.
pub fn route_table(table: &Table, constraints: &Constraints) -> Routing {
    if !table.sharded {
        return Routing { opcode: RouteOpcode::Unsharded, vindex: None, values: Vec::new() };
    }
    if table.is_reference() {
        // Copied to every shard, so any one of them can answer.
        return Routing { opcode: RouteOpcode::Reference, vindex: None, values: Vec::new() };
    }

    // Try every vindex, cheapest first (the VSchema sorted them), and take the
    // first that the query has enough information to use.
    let mut best: Option<Routing> = None;
    for cv in &table.column_vindexes {
        let Some(candidate) = routing_for_vindex(cv, constraints) else { continue };
        let better = match &best {
            None => true,
            Some(b) => rank(candidate.opcode) < rank(b.opcode),
        };
        if better {
            best = Some(candidate);
        }
    }

    best.unwrap_or_else(Routing::scatter)
}

/// Lower is better.
fn rank(op: RouteOpcode) -> u8 {
    match op {
        RouteOpcode::None => 0,
        RouteOpcode::Unsharded | RouteOpcode::EqualUnique => 1,
        RouteOpcode::In | RouteOpcode::MultiEqual => 2,
        RouteOpcode::Equal => 3,
        RouteOpcode::Range => 4,
        _ => 9,
    }
}

fn routing_for_vindex(cv: &ColumnVindex, constraints: &Constraints) -> Option<Routing> {
    let ncols = cv.columns.len();

    // Every column equality-constrained: the strongest case.
    let equals: Option<Vec<RouteValue>> = cv
        .columns
        .iter()
        .map(|c| match constraints.get(c) {
            Some(Constraint::Equal(v)) => Some(v.clone()),
            _ => None,
        })
        .collect();
    if let Some(values) = equals {
        return Some(Routing {
            opcode: if cv.vindex.is_unique() { RouteOpcode::EqualUnique } else { RouteOpcode::Equal },
            vindex: Some(cv.clone()),
            values: vec![RouteValue::Tuple(values)],
        });
    }

    // A leading prefix of the columns, for a vindex that accepts one.
    if ncols > 1 && cv.vindex.accepts_partial_columns() {
        let mut prefix = Vec::new();
        for c in &cv.columns {
            match constraints.get(c) {
                Some(Constraint::Equal(v)) => prefix.push(v.clone()),
                _ => break,
            }
        }
        if !prefix.is_empty() {
            return Some(Routing {
                opcode: RouteOpcode::Equal,
                vindex: Some(cv.clone()),
                values: vec![RouteValue::Tuple(prefix)],
            });
        }
    }

    if ncols != 1 {
        return None;
    }
    let column = &cv.columns[0];

    match constraints.get(column) {
        Some(Constraint::In(values)) => Some(Routing {
            opcode: RouteOpcode::In,
            vindex: Some(cv.clone()),
            values: values.iter().map(|v| RouteValue::Tuple(vec![v.clone()])).collect(),
        }),
        Some(Constraint::Range { lo, hi }) => {
            // Only worth it if the vindex preserves order; a hash vindex turns a
            // range into a scatter anyway.
            let probe = vituss_core::Value::Int(0);
            cv.vindex.range_map(&probe, &probe)?.ok()?;
            Some(Routing {
                opcode: RouteOpcode::Range,
                vindex: Some(cv.clone()),
                values: vec![RouteValue::Tuple(vec![
                    lo.clone().unwrap_or(RouteValue::Literal(vituss_core::Value::Null)),
                    hi.clone().unwrap_or(RouteValue::Literal(vituss_core::Value::Null)),
                ])],
            })
        }
        Some(Constraint::Prefix(p)) => {
            let probe = vituss_core::Value::Text(String::new());
            cv.vindex.prefix_map(&probe)?.ok()?;
            Some(Routing {
                opcode: RouteOpcode::Range,
                vindex: Some(cv.clone()),
                values: vec![RouteValue::Tuple(vec![p.clone()])],
            })
        }
        _ => {
            // A top-level OR whose every branch pins this column: the union of
            // those destinations, which is still better than a scatter.
            if constraints.alternatives.is_empty() {
                return None;
            }
            let mut values = Vec::with_capacity(constraints.alternatives.len());
            for alt in &constraints.alternatives {
                match alt.get(&column.to_lowercase()) {
                    Some(Constraint::Equal(v)) => values.push(RouteValue::Tuple(vec![v.clone()])),
                    Some(Constraint::In(vs)) => {
                        values.extend(vs.iter().map(|v| RouteValue::Tuple(vec![v.clone()])))
                    }
                    _ => return None,
                }
            }
            Some(Routing { opcode: RouteOpcode::MultiEqual, vindex: Some(cv.clone()), values })
        }
    }
}
