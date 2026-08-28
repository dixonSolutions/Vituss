//! The plan IR.
//!
//! A plan is a tree of primitives. Leaves are [`Route`]s — a statement plus the
//! shards to send it to; everything above them combines shard results. The shape
//! mirrors Vitess's engine primitives, because the shape is the interesting part:
//! it is what decides how much work is pushed into the shards' own engines and
//! how much Vituss has to do itself.
//!
//! Vituss pushes down as much as it can. A `SELECT ... ORDER BY ... LIMIT` that
//! routes to one shard is one `Route` and nothing else: the shard's own optimiser
//! does the sorting. The same query across shards becomes `Limit(Sort(Route))`,
//! and only then does Vituss sort anything.

use std::fmt;

use sqlparser::ast::Statement;

use vituss_core::{BindVars, KeyspaceId, ShardDestination, Value};
use vituss_vschema::{ColumnVindex, Table};

/// How a [`Route`] chooses its shards.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteOpcode {
    /// The keyspace has exactly one shard.
    Unsharded,
    /// A unique vindex was fully constrained: exactly one shard.
    EqualUnique,
    /// A non-unique vindex was constrained: a known, usually small, set of shards.
    Equal,
    /// `IN (...)` over a vindex column: one shard per distinct value, deduplicated.
    In,
    /// Several independent equality predicates (`a = 1 OR a = 2`): the union.
    MultiEqual,
    /// A range predicate over an order-preserving vindex: one key range.
    Range,
    /// No usable predicate: every shard.
    Scatter,
    /// Any single shard — the answer does not depend on which.
    AnyShard,
    /// A reference table: readable from any shard, written to the pinned one.
    Reference,
    /// The client named the destination (`USE ks:-80`).
    ByDestination,
    /// The predicate is unsatisfiable; no shard is contacted at all.
    None,
    /// `SELECT NEXT n VALUES FROM seq` against a sequence table.
    NextSequenceValue,
}

impl RouteOpcode {
    /// True when the route may touch more than one shard.
    pub fn is_multi_shard(self) -> bool {
        matches!(self, Self::Scatter | Self::In | Self::MultiEqual | Self::Equal | Self::Range)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unsharded => "Unsharded",
            Self::EqualUnique => "EqualUnique",
            Self::Equal => "Equal",
            Self::In => "IN",
            Self::MultiEqual => "MultiEqual",
            Self::Range => "Range",
            Self::Scatter => "Scatter",
            Self::AnyShard => "AnyShard",
            Self::Reference => "Reference",
            Self::ByDestination => "ByDestination",
            Self::None => "None",
            Self::NextSequenceValue => "NextValue",
        }
    }
}

impl fmt::Display for RouteOpcode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One input to a vindex, resolved at execution time.
///
/// Values are not baked into the plan: they come from bind variables, which is
/// what lets one plan serve every execution of the same query shape — including
/// the joined case, where the value comes from the other side's current row.
#[derive(Debug, Clone, PartialEq)]
pub enum RouteValue {
    /// A constant known at planning time.
    Literal(Value),
    /// A named bind variable.
    BindVar(String),
    /// A bind variable holding a list, from `IN (:list)`.
    BindVarList(String),
    /// One value per vindex column, for a multi-column vindex.
    Tuple(Vec<RouteValue>),
}

/// A sort key, expressed as a column position in the shard's result.
#[derive(Debug, Clone, PartialEq)]
pub struct OrderBy {
    /// Index into the row the shards return.
    pub column: usize,
    pub descending: bool,
    /// SQL's default is NULLs first ascending; a query may ask for the opposite.
    pub nulls_first: bool,
}

/// Send a statement to a set of shards.
#[derive(Debug, Clone)]
pub struct Route {
    pub opcode: RouteOpcode,
    pub keyspace: String,
    /// The statement to run on each shard, with `:name` placeholders.
    pub statement: Statement,
    /// Literals lifted out of the statement during normalisation.
    pub bind_vars: BindVars,
    /// The vindex used to pick shards, when the opcode needs one.
    pub vindex: Option<ColumnVindex>,
    /// Inputs to that vindex.
    pub values: Vec<RouteValue>,
    /// An explicit destination, for `ByDestination` and `Reference` routes.
    pub destination: Option<ShardDestination>,
    /// Sort order the shards return rows in. When set, the gate merges the
    /// per-shard streams instead of concatenating and re-sorting.
    pub order_by: Vec<OrderBy>,
    /// Columns the client actually asked for. The planner may have added more
    /// (sort keys, vindex columns); they are trimmed after merging.
    pub truncate_columns: usize,
    /// The table this route reads, for diagnostics and ACL checks.
    pub table: Option<String>,
}

impl Route {
    pub fn new(opcode: RouteOpcode, keyspace: impl Into<String>, statement: Statement) -> Self {
        Self {
            opcode,
            keyspace: keyspace.into(),
            statement,
            bind_vars: BindVars::new(),
            vindex: None,
            values: Vec::new(),
            destination: None,
            order_by: Vec::new(),
            truncate_columns: 0,
            table: None,
        }
    }
}

/// What a DML does about the vindexes it owns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DmlKind {
    Insert,
    Update,
    Delete,
}

/// One row to insert, already resolved to a destination.
#[derive(Debug, Clone)]
pub struct InsertRow {
    /// The row's values, in the statement's column order.
    pub values: Vec<RouteValue>,
    /// Inputs to the primary vindex, taken from those values.
    pub vindex_values: Vec<RouteValue>,
}

/// An INSERT, which is the one statement that must decide a *destination per row*
/// rather than per statement.
#[derive(Debug, Clone)]
pub struct InsertPlan {
    pub keyspace: String,
    pub table: std::sync::Arc<Table>,
    pub opcode: RouteOpcode,
    /// The per-shard statement template.
    pub statement: Statement,
    pub bind_vars: BindVars,
    /// Column names in the statement's order.
    pub columns: Vec<String>,
    pub rows: Vec<InsertRow>,
    /// Position of the primary vindex's columns within `columns`.
    pub vindex_column_positions: Vec<usize>,
    /// Lookup vindexes this table owns, which must be written before the row is.
    pub owned_vindexes: Vec<ColumnVindex>,
    /// Sequence to draw the primary key from when the client omitted it.
    pub sequence: Option<SequencePlan>,
    /// `INSERT IGNORE` / `ON CONFLICT DO NOTHING`.
    pub ignore_duplicates: bool,
    /// For a reference table in a sharded keyspace: the single pinned location.
    pub pinned: Option<KeyspaceId>,
}

#[derive(Debug, Clone)]
pub struct SequencePlan {
    /// Column the generated value fills.
    pub column: String,
    /// Position of that column in `columns`, if it is present in the statement.
    pub column_position: Option<usize>,
    pub keyspace: String,
    pub table: String,
}

/// An UPDATE or DELETE.
#[derive(Debug, Clone)]
pub struct DmlPlan {
    pub kind: DmlKind,
    pub route: Route,
    pub table: std::sync::Arc<Table>,
    /// Lookup vindexes that must be maintained. Empty for a table that owns none,
    /// which is the fast path.
    pub owned_vindexes: Vec<ColumnVindex>,
    /// Query that fetches the rows about to change, so their lookup rows can be
    /// removed. Only present when `owned_vindexes` is non-empty.
    pub pre_query: Option<Box<Route>>,
    /// True when a multi-shard DML is allowed. Vituss refuses one by default
    /// because it cannot be made atomic without 2PC.
    pub multi_shard_autocommit: bool,
}

/// A cross-shard join.
///
/// Executed as a nested loop: run the left side, then for each left row run the
/// right side with values from it bound in. That is the only join Vituss performs
/// itself; anything that can be answered inside one shard is pushed down as a
/// single [`Route`] and joined by the shard's own engine.
#[derive(Debug, Clone)]
pub struct Join {
    pub left: Box<Primitive>,
    pub right: Box<Primitive>,
    pub kind: JoinKind,
    /// How to build the output row: positive index takes from the left row,
    /// negative (encoded as `-(i+1)`) takes from the right.
    pub column_map: Vec<i32>,
    /// Bind variables the right side reads, filled from the left row:
    /// `(bind var name, left column index)`.
    pub vars: Vec<(String, usize)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    Left,
}

#[derive(Debug, Clone)]
pub struct Limit {
    pub input: Box<Primitive>,
    pub count: Option<RouteValue>,
    pub offset: Option<RouteValue>,
}

#[derive(Debug, Clone)]
pub struct Sort {
    pub input: Box<Primitive>,
    pub order_by: Vec<OrderBy>,
    /// Stop after this many rows. Set when the query has a LIMIT, so the sort can
    /// keep only what it needs instead of buffering every shard's rows.
    pub limit: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct Distinct {
    pub input: Box<Primitive>,
    /// Columns that make a row distinct. Empty means every column.
    pub columns: Vec<usize>,
}

/// One aggregate to compute above the shards.
#[derive(Debug, Clone, PartialEq)]
pub struct AggregateExpr {
    pub func: AggregateFunc,
    /// Column in the shard result holding this aggregate's partial value.
    pub column: usize,
    /// For AVG, the column holding the partial count that goes with the sum.
    pub count_column: Option<usize>,
    pub distinct: bool,
    pub alias: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFunc {
    Count,
    Sum,
    Min,
    Max,
    /// AVG is split into SUM and COUNT before being pushed down, because the
    /// average of per-shard averages is not the average.
    Avg,
    CountStar,
}

#[derive(Debug, Clone)]
pub struct Aggregate {
    pub input: Box<Primitive>,
    pub aggregates: Vec<AggregateExpr>,
    /// GROUP BY columns, as positions in the shard result.
    pub group_by: Vec<usize>,
    /// True when the input already arrives grouped (the shards sorted by the
    /// group key), which lets the aggregate stream instead of buffering.
    pub ordered: bool,
    pub truncate_columns: usize,
}

/// A statement that changes session state rather than data.
#[derive(Debug, Clone, PartialEq)]
pub enum SessionOp {
    Use { keyspace: String, shard: Option<String>, tablet_type: Option<String> },
    Begin,
    Commit,
    Rollback,
    Savepoint(String),
    RollbackTo(String),
    Release(String),
    Set { name: String, value: Value, scope: SetScope },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetScope {
    Session,
    Global,
    /// A `@user` variable, held by the gate and never sent to a shard.
    UserDefined,
    /// A Vituss setting such as `vituss_transaction_mode`.
    Vituss,
}

/// A DDL statement, broadcast to every shard of a keyspace.
#[derive(Debug, Clone)]
pub struct Ddl {
    pub keyspace: String,
    pub statement: Statement,
    /// True when the DDL creates or drops the keyspace's database itself, which
    /// must not run inside a transaction on some engines.
    pub is_database_level: bool,
}

/// A `SHOW` statement Vituss answers from its own metadata.
#[derive(Debug, Clone, PartialEq)]
pub enum ShowPlan {
    Keyspaces,
    Databases,
    Tables { keyspace: Option<String> },
    VSchemaTables { keyspace: Option<String> },
    VSchemaVindexes { keyspace: Option<String> },
    Shards { keyspace: Option<String> },
    Tablets,
    Dialects,
    /// Anything else: forwarded to one shard and returned verbatim.
    Passthrough,
}

#[derive(Debug, Clone)]
pub enum Primitive {
    Route(Route),
    Insert(InsertPlan),
    Dml(DmlPlan),
    Join(Join),
    Limit(Limit),
    Sort(Sort),
    Distinct(Distinct),
    Aggregate(Aggregate),
    /// UNION / UNION ALL of several inputs.
    Concatenate { inputs: Vec<Primitive>, distinct: bool },
    /// A single empty row, the identity for `SELECT 1` and for the right side of
    /// a left join that matched nothing.
    SingleRow,
    /// Drop the trailing columns the planner added for its own use — sort keys
    /// and group-by keys the client never asked for. Always the outermost node,
    /// because the nodes below it need those columns.
    Truncate { input: Box<Primitive>, columns: usize },
    Ddl(Ddl),
    Session(SessionOp),
    Show(ShowPlan),
}

/// A planned statement, ready to execute.
#[derive(Debug, Clone)]
pub struct Plan {
    pub primitive: Primitive,
    /// The original SQL, for logs and for `EXPLAIN`.
    pub original_sql: String,
    /// Keyspaces this plan touches, for ACL checks and metrics.
    pub keyspaces: Vec<String>,
    /// True when the plan writes.
    pub is_dml: bool,
}

impl Plan {
    /// A one-line-per-node rendering, as `VEXPLAIN` produces.
    pub fn explain(&self) -> String {
        let mut out = String::new();
        render(&self.primitive, 0, &mut out);
        out
    }
}

fn render(p: &Primitive, depth: usize, out: &mut String) {
    let pad = "  ".repeat(depth);
    match p {
        Primitive::Route(r) => {
            out.push_str(&format!(
                "{pad}Route({}) keyspace={} table={}\n{pad}  query: {}\n",
                r.opcode,
                r.keyspace,
                r.table.as_deref().unwrap_or("-"),
                r.statement
            ));
            if !r.order_by.is_empty() {
                out.push_str(&format!("{pad}  merge-sort on {:?}\n", r.order_by));
            }
        }
        Primitive::Insert(i) => {
            out.push_str(&format!(
                "{pad}Insert({}) {}.{} rows={}\n{pad}  query: {}\n",
                i.opcode,
                i.keyspace,
                i.table.name,
                i.rows.len(),
                i.statement
            ));
        }
        Primitive::Dml(d) => {
            out.push_str(&format!("{pad}{:?}\n", d.kind));
            render(&Primitive::Route(d.route.clone()), depth + 1, out);
            if let Some(pre) = &d.pre_query {
                out.push_str(&format!("{pad}  (reads rows first to maintain owned vindexes)\n"));
                render(&Primitive::Route((**pre).clone()), depth + 1, out);
            }
        }
        Primitive::Join(j) => {
            out.push_str(&format!("{pad}{:?}Join vars={:?}\n", j.kind, j.vars));
            render(&j.left, depth + 1, out);
            render(&j.right, depth + 1, out);
        }
        Primitive::Limit(l) => {
            out.push_str(&format!("{pad}Limit\n"));
            render(&l.input, depth + 1, out);
        }
        Primitive::Sort(s) => {
            out.push_str(&format!("{pad}Sort {:?}\n", s.order_by));
            render(&s.input, depth + 1, out);
        }
        Primitive::Distinct(d) => {
            out.push_str(&format!("{pad}Distinct\n"));
            render(&d.input, depth + 1, out);
        }
        Primitive::Aggregate(a) => {
            out.push_str(&format!(
                "{pad}Aggregate {:?} group_by={:?}\n",
                a.aggregates.iter().map(|x| x.func).collect::<Vec<_>>(),
                a.group_by
            ));
            render(&a.input, depth + 1, out);
        }
        Primitive::Concatenate { inputs, distinct } => {
            out.push_str(&format!("{pad}Concatenate(distinct={distinct})\n"));
            for i in inputs {
                render(i, depth + 1, out);
            }
        }
        Primitive::Truncate { input, columns } => {
            out.push_str(&format!("{pad}Truncate to {columns} column(s)\n"));
            render(input, depth + 1, out);
        }
        Primitive::SingleRow => out.push_str(&format!("{pad}SingleRow\n")),
        Primitive::Ddl(d) => out.push_str(&format!("{pad}DDL keyspace={} all shards\n", d.keyspace)),
        Primitive::Session(s) => out.push_str(&format!("{pad}Session {s:?}\n")),
        Primitive::Show(s) => out.push_str(&format!("{pad}Show {s:?}\n")),
    }
}
