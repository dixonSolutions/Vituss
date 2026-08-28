# Adding a database engine

Vituss reaches an engine through two traits. Implement both, register them, and
the planner, the gate, the tablet and the engine primitives all work against it
unchanged — none of them is edited, and none of them learns the engine's name.

SQLite was added this way on purpose. It is the worked example: no distributed
transactions, no unsigned integers, no schema catalogue, dynamic typing. If the
abstraction had been MySQL-shaped, SQLite would have broken it.

---

## 1. `SqlDialect` — what the SQL looks like

```rust
use vituss_dialect::{Capabilities, IdentifierCase, PlaceholderStyle, SqlDialect, TwoPcStyle};

pub struct CockroachDb {
    parser: sqlparser::dialect::PostgreSqlDialect,
    caps: Capabilities,
    introspection: vituss_dialect::Introspection,
}

impl SqlDialect for CockroachDb {
    fn name(&self) -> &'static str { "cockroach" }
    fn aliases(&self) -> &'static [&'static str] { &["crdb"] }

    // Borrow a maintained grammar. Vituss never writes one.
    fn parser(&self) -> &dyn sqlparser::dialect::Dialect { &self.parser }

    fn capabilities(&self) -> &Capabilities { &self.caps }
    fn introspection(&self) -> &vituss_dialect::Introspection { &self.introspection }
    fn default_port(&self) -> u16 { 26257 }

    // The engine's type names → the neutral type system.
    fn map_native_type(&self, native: &str) -> vituss_core::SqlType { /* … */ }

    // …and back out again, which is what lets a CREATE TABLE written for another
    // engine run here. See "Column types" below.
    fn render_column_type(&self, c: &vituss_dialect::ColumnType) -> String { /* … */ }
    fn native_error(&self, err: &vituss_core::Error) -> vituss_dialect::NativeError { /* … */ }
    fn system_schemas(&self) -> &'static [&'static str] { &["information_schema", "crdb_internal"] }
}
```

### Capabilities

These are what the planner consults *instead of* branching on the engine name.
Describe the engine honestly — especially where it cannot do something.

```rust
Capabilities {
    identifier_case:   IdentifierCase::FoldLower,
    identifier_quote:  '"',
    max_identifier_len: 63,
    placeholder_style: PlaceholderStyle::DollarNumbered,
    two_pc:            TwoPcStyle::None,   // CockroachDB has no PREPARE TRANSACTION

    supports_returning: true,
    supports_last_insert_id: false,
    supports_limit_offset: true,
    supports_savepoints: true,
    supports_unsigned: false,
    supports_upsert: true,
    supports_multi_statement: false,
    supports_change_capture: true,
    supports_create_database_in_tx: false,
    supports_advisory_locks: false,
}
```

Understating a capability costs performance. Overstating one costs correctness:
claiming 2PC an engine cannot do turns a refused transaction into a silently
non-atomic one.

### Introspection

Seven queries the tablet uses to learn its own schema. Each takes exactly one
parameter — the schema name — and must return the documented columns
**positionally**:

| Query | Returns |
|---|---|
| `list_tables` | `(table_name, table_type)` |
| `list_columns` | `(table, column, ordinal, native_type, nullable, default, auto_generated)` |
| `list_indexes` | `(table, index, column, ordinal, is_unique, is_primary)` |
| `list_foreign_keys` | `(table, constraint, column, ref_table, ref_column)` |
| `current_schema` | one value |
| `ping` | anything cheap |
| `server_version` | one value |
| `replication_position` | one value, or `None` if the engine has no change stream |

If the engine has no schema catalogue, the query still has to *use* the parameter
— SQLite appends `AND ?1 IS NOT NULL`, which keeps the one-parameter contract
without changing the result.

### Column types

`map_native_type` and `render_column_type` are inverses, and both are required.
The second is where a schema written for another engine becomes a schema this one
can execute:

```rust
fn render_column_type(&self, c: &ColumnType) -> String {
    match c.base {
        SqlType::Bool   => "BOOLEAN".into(),
        // No unsigned types here, so widen rather than truncate the range.
        SqlType::Int32 if c.unsigned => "BIGINT".into(),
        SqlType::Int32  => "INTEGER".into(),
        SqlType::Uint64 => "NUMERIC(20)".into(),
        SqlType::Decimal => ddl::decimal(c, "NUMERIC"),
        SqlType::VarChar => ddl::sized_or(c, "VARCHAR", "TEXT"),
        SqlType::Json   => "JSONB".into(),
        // A type this layer could not classify. Passing it through is the least
        // wrong option, and the caller is warned.
        SqlType::Null | SqlType::Unknown => c.source_text.clone(),
        // …
    }
}
```

If the engine declares a generated key as a column option, return it from
`auto_increment_option` (`AUTO_INCREMENT`, `IDENTITY(1,1)`). If it folds the key
into the type instead — PostgreSQL's `SERIAL` — check `c.auto_increment` at the
top of `render_column_type`, return `None` from `auto_increment_option`, and set
`auto_increment_in_type: true` in the capabilities so the translator knows the key
is accounted for.

### Overrides

Default implementations cover the common spelling; override where the engine
differs. SQL Server needs all four of these:

```rust
fn begin_sql(&self, isolation: Option<&str>) -> String { "BEGIN TRANSACTION".into() }
fn savepoint_sql(&self, name: &str) -> String { format!("SAVE TRANSACTION {name}") }
fn limit_clause(&self, limit: Option<u64>, offset: Option<u64>) -> String {
    format!(" OFFSET {} ROWS FETCH NEXT {} ROWS ONLY", offset.unwrap_or(0), limit.unwrap())
}
fn select_for_update(&self, cols: &str, table: &str, w: &str) -> String {
    format!("SELECT {cols} FROM [{table}] WITH (UPDLOCK, HOLDLOCK) WHERE {w}")
}
```

---

## 2. `Backend` and `Connection` — how to talk to it

```rust
#[async_trait]
impl Backend for CockroachBackend {
    fn dialect(&self) -> &DialectRef { &self.dialect }
    fn describe(&self) -> String { self.redacted.clone() }   // never include a password
    async fn acquire(&self) -> Result<Box<dyn Connection>> { /* … */ }
    async fn health(&self) -> Result<Health> { /* … */ }
    fn stats(&self) -> PoolStats { /* … */ }
    async fn close(&self) { /* … */ }
}
```

`Connection` has one method to write by hand:

```rust
impl CockroachConnection {
    async fn exec_impl(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult> { /* … */ }
}

// Generates the whole Connection impl: transaction control, savepoints, 2PC,
// ping — all expressed as the dialect's own SQL.
vituss_backend::impl_connection!(CockroachConnection);
```

The macro needs three fields on the type: `dialect: DialectRef`, `in_tx: bool`,
`healthy: bool`.

Two things `exec_impl` must get right:

- **Mark the connection unhealthy after a failure inside a transaction.** Its
  state is then unknown, and it must be dropped rather than pooled.
- **Preserve the engine's native error code and SQLSTATE.** Applications branch
  on `1062` and `23505`; flattening them into a generic error breaks that.

---

## 3. Register

```rust
vituss_dialect::register(Arc::new(CockroachDb::new()));

vituss_backend::register("cockroach", |cfg, dialect| {
    Box::pin(async move {
        Ok(Arc::new(CockroachBackend::connect(&cfg, dialect).await?) as Arc<dyn Backend>)
    })
});
```

That is the whole integration. The engine is now usable in a cluster config:

```yaml
keyspaces:
  - name: commerce
    dialect: cockroach
    shards: 4
    dsn_template: "postgres://root@crdb:26257/{keyspace}_{shard}"
```

and `vituss capabilities` will list it.

---

## Adding a sharding function

Same pattern, one trait:

```rust
#[async_trait]
impl Vindex for GeoHash {
    fn name(&self) -> &str { &self.name }
    fn kind(&self) -> &'static str { "geohash" }
    fn cost(&self) -> u32 { 1 }             // 0 = the value is the keyspace ID
    fn is_unique(&self) -> bool { true }    // only unique vindexes can be primary

    async fn map(&self, _: Option<&dyn VCursor>, rows: &[VindexRow])
        -> Result<Vec<ShardDestination>> { /* … */ }

    async fn verify(&self, _: Option<&dyn VCursor>, rows: &[VindexRow], ksids: &[KeyspaceId])
        -> Result<Vec<bool>> { /* … */ }

    // Optional, and each one buys the planner something:
    //   hash()          → usable as a column of a composite vindex
    //   reverse_map()   → the gate can fill in an omitted sharding column
    //   range_map()     → BETWEEN narrows to a key range instead of scattering
    //   prefix_map()    → LIKE 'abc%' narrows too
}

vituss_vindex::register("geohash", |name, params| Ok(Arc::new(GeoHash::new(name, params)?)));
```

Two rules that are not negotiable:

1. **The mapping must be stable for ever.** It decides which shard every row
   lives on; changing it re-shards the data.
2. **It must depend only on the value**, never on the engine. That is what makes
   a keyspace portable.

Declare `known_params()` and validate them. An unrecognised parameter is an
error, not a warning: a typo in a vindex param silently changes how data is
sharded, which is the worst class of bug this system can have.

---

## Where the topology lives

The fourth plug-in, if a deployment needs etcd, Consul or ZooKeeper:

```rust
#[async_trait]
impl TopoStore for EtcdStore {
    fn name(&self) -> &'static str { "etcd" }
    async fn get(&self, path: &str) -> Result<Option<Versioned>>;
    async fn list(&self, prefix: &str) -> Result<Vec<String>>;
    async fn put(&self, path: &str, data: &[u8], expected: Option<Version>) -> Result<Version>;
    async fn create(&self, path: &str, data: &[u8]) -> Result<Version>;
    async fn delete(&self, path: &str, expected: Option<Version>) -> Result<()>;
    async fn delete_prefix(&self, prefix: &str) -> Result<()>;
    async fn lock(&self, path: &str, reason: &str) -> Result<Box<dyn LockHandle>>;
    async fn watch(&self, prefix: &str) -> Result<Receiver<WatchEvent>>;
}
```

Versioned reads and writes, prefix listing, compare-and-swap, a lock and a change
feed. Nothing else — which is why a directory of JSON files is a complete
implementation, not a stub.
