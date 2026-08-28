# Vituss

**Sharding, routing and cluster management for databases you already run.**

Vituss puts a horizontal-scaling layer in front of MySQL, PostgreSQL, SQL Server
or SQLite. Applications keep talking to what looks like a single database server,
using their existing driver; behind it, the data is spread across as many shards
as you need.

It is a Rust reimagining of [Vitess](https://vitess.io), with one deliberate
change of premise: **Vituss is not tied to MySQL.**

---

## The two things it does not do

Both are load-bearing, so they are worth stating before anything else.

**Vituss does not implement a SQL grammar.** Vitess hand-wrote a MySQL parser
because it targeted exactly one engine and needed byte-level control of its
syntax. Vituss targets several, so it borrows a maintained grammar per engine
([`sqlparser`](https://crates.io/crates/sqlparser)) and confines everything else
that differs — quoting, placeholders, capabilities, introspection, error codes,
transaction control — behind one trait. A query using engine-specific syntax
parses because that engine's own grammar parsed it.

**Vituss does not implement a storage or execution engine.** Shards are real
MySQL, PostgreSQL, SQL Server or SQLite servers. A query that reaches one shard
is sent to it verbatim and that shard's own optimiser does the joins, the sort,
the aggregate, the limit. Vituss only does the work no single shard can: choosing
shards, merging their answers, and coordinating transactions across them.

What is left is the part that is genuinely hard, and the part Vitess proved is
worth having.

---

## Quick start

No database server required — the example cluster uses SQLite files.

```bash
cargo build --release
./target/release/vituss up --config examples/commerce-sqlite.yaml
```

That brings up a four-shard `commerce` keyspace and an unsharded `main`
keyspace, and starts both protocol servers:

```
MySQL clients:      mysql -h 127.0.0.1 -P 15306 -u vituss
PostgreSQL clients: psql -h 127.0.0.1 -p 15432 -U vituss
```

Note what that means: the shards are SQLite, and you can talk to them with either
a MySQL client or a PostgreSQL client. Protocol and storage engine are
independent choices.

```sql
-- Routed to exactly one shard: the vindex knows where user 4 lives.
SELECT name FROM user WHERE user_id = 4;

-- Scattered, then merged in order by the gate.
SELECT user_id, name FROM user ORDER BY name LIMIT 10;

-- Each shard counts its own rows; the counts are added here.
SELECT country, COUNT(*) FROM user GROUP BY country;

-- Shows the routing decision without running anything.
VEXPLAIN SELECT name FROM user WHERE user_id = 4;
```

### From the command line

```bash
vituss capabilities                                   # engines, drivers, vindexes
vituss validate  -c examples/commerce-sqlite.yaml     # check a config
vituss explain   -c examples/commerce-sqlite.yaml "SELECT * FROM user"
vituss query     -c examples/commerce-sqlite.yaml "SELECT COUNT(*) FROM user"
vituss status    -c examples/commerce-sqlite.yaml --probe
```

---

## Engine independence, concretely

A shard's engine is a property of the shard, not of the deployment:

```yaml
keyspaces:
  - name: commerce
    dialect: postgres        # this keyspace runs on PostgreSQL
    sharded: true
    shards: 4
    tablets:
      - { shard: "-40",   dsn: "postgres://…/commerce_0" }
      - { shard: "40-80", dsn: "postgres://…/commerce_1" }
      - { shard: "80-c0", dsn: "mysql://…/commerce_2", dialect: mysql }   # mid-migration
      - { shard: "c0-",   dsn: "postgres://…/commerce_3" }
```

The third shard is on MySQL while the rest are on PostgreSQL. That is what a live
engine migration looks like, and the router does not care: each shard's statement
is rendered for the engine that shard actually runs.

This works because sharding is expressed in terms that belong to neither engine.
A *vindex* maps column values to an opaque keyspace ID; a keyspace ID falls in
exactly one shard's byte range. It is arithmetic on bytes, not on anything MySQL
provides.

| | MySQL | PostgreSQL | SQL Server | SQLite |
|---|---|---|---|---|
| Placeholders | `?` | `$1` | `@p1` | `?` |
| Identifier quoting | `` `x` `` | `"x"` | `[x]` | `"x"` |
| Identifier case | preserved | folded lower | preserved, insensitive | preserved |
| Row limiting | `LIMIT`/`OFFSET` | `LIMIT`/`OFFSET` | `OFFSET … FETCH` | `LIMIT`/`OFFSET` |
| Generated keys | last-insert-id | `RETURNING` | `OUTPUT INSERTED` | last-insert-rowid |
| Distributed commit | XA | `PREPARE TRANSACTION` | MS DTC only | none |
| Row lock | `FOR UPDATE` | `FOR UPDATE` | `WITH (UPDLOCK)` | none needed |
| Generated key | `AUTO_INCREMENT` | `BIGSERIAL` | `IDENTITY(1,1)` | implicit rowid |
| Unsigned integers | yes | no | no | no |

Every one of those is described by the dialect, not branched on by the planner —
including the column types themselves. A `CREATE TABLE` written by a MySQL client
arrives at a PostgreSQL shard as idiomatic PostgreSQL:

```sql
-- what the client wrote
CREATE TABLE user (user_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                   payload JSON, PRIMARY KEY (user_id)) ENGINE=InnoDB

-- what the PostgreSQL shard receives
CREATE TABLE user (user_id BIGSERIAL NOT NULL, payload JSONB, PRIMARY KEY (user_id))
```

Where something cannot survive the crossing — a `CHARACTER SET`, an
`ON UPDATE CURRENT_TIMESTAMP`, an unsigned range that has to widen — the statement
still runs and the result carries a warning saying what was dropped and why.

Where an engine genuinely cannot do something — SQL Server's distributed
transactions need an external coordinator, SQLite has none at all — Vituss
**refuses** the operation rather than silently degrading it. A caller who asked
for atomicity finds out that they are not getting it.

---

## Architecture

```
        MySQL client                          PostgreSQL client
             │                                       │
      ┌──────┴───────────────────────────────────────┴──────┐
      │  vituss-wire     protocol servers                   │
      ├─────────────────────────────────────────────────────┤
      │  vituss-gate     session, routing, transactions      │
      │  vituss-planner  statement → plan (no data touched)  │
      │  vituss-engine   fan out, merge, sort, aggregate     │
      │  vituss-vindex   sharding functions                  │
      │  vituss-vschema  logical schema                      │
      ├─────────────────────────────────────────────────────┤
      │  vituss-tablet   one per shard: pools, transactions  │
      │  vituss-backend  drivers      │  vituss-dialect  SQL │
      └────────┬──────────────────────┴──────┬──────────────┘
               │                             │
        ┌──────┴──────┐               ┌──────┴──────┐
        │   MySQL     │               │ PostgreSQL  │   … real database servers
        └─────────────┘               └─────────────┘

      vituss-topo   cluster metadata (memory / files / pluggable)
      vituss-ctl    declarative configuration and admin operations
```

Full detail in [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

### Plug-in points

Four traits, each the sole extension point for its concern:

| Trait | Crate | Adds |
|---|---|---|
| `SqlDialect` | `vituss-dialect` | a new SQL engine's surface |
| `Backend` / `Connection` | `vituss-backend` | a driver for it |
| `Vindex` | `vituss-vindex` | a new sharding function |
| `TopoStore` | `vituss-topo` | where cluster metadata lives |

Adding an engine touches none of the planner, the gate, the tablet or the engine
primitives — see [docs/ADDING-AN-ENGINE.md](docs/ADDING-AN-ENGINE.md). SQLite was
added that way, deliberately, as proof.

---

## What is ported, and what is not

Coming from Vitess, see [docs/PORTING-FROM-VITESS.md](docs/PORTING-FROM-VITESS.md)
for the concept-by-concept mapping and the honest gap list.

**Working:** keyspaces, shards, tablets and the serving graph; VSchema with
lookup vindexes and sequences; routing (equal / IN / multi-equal / range /
scatter / reference / unsharded); cross-shard merge-sort, limit, distinct, and
aggregation including a correct `AVG`; collocated joins pushed into the shard and
non-collocated ones executed as nested loops; INSERT routed per row with owned
lookup-vindex maintenance; UPDATE and DELETE with lookup upkeep; DDL broadcast;
multi-shard transactions with optional two-phase commit; cross-engine DDL type
translation; MySQL and PostgreSQL protocol servers; a declarative control plane.

**Not yet:** VReplication and resharding *data movement* (the metadata side of a
split is there; nothing copies rows yet), VTOrc's automatic failover, online DDL,
backups, the `cfc` / `region_*` / `unicode_loose_*` vindexes, the PostgreSQL
extended query protocol, and gRPC between components — today a gate speaks to
tablets in-process.

The gaps are listed rather than stubbed. A vindex that is *nearly* compatible
would silently place rows on the wrong shard, which is worse than not having it.

### Vitess compatibility

The functional vindexes are bit-for-bit identical to Vitess's, verified against
vectors taken from Vitess's own test suite
([`crates/vituss-vindex/tests/vitess_compat.rs`](crates/vituss-vindex/tests/vitess_compat.rs)).
An existing Vitess keyspace maps to the same shards under Vituss, so a migration
does not move a single row.

---

## Development

```bash
cargo test --workspace          # 164 tests, no external services needed
cargo build --release
```

The integration tests run a real multi-shard cluster over real SQLite databases
and drive it with a real MySQL client, so the query path is exercised end to end
rather than mocked:

- `crates/vituss-gate/tests/end_to_end.rs` — routing, merging, transactions
- `crates/vituss-wire/tests/mysql_protocol.rs` — a real driver over TCP
- `crates/vituss-planner/tests/planning.rs` — every routing decision
- `crates/vituss-vindex/tests/vitess_compat.rs` — Vitess byte compatibility
- `crates/vituss-dialect/tests/ddl_translation.rs` — schemas across type systems

To build with only the engines you need:

```bash
cargo build --no-default-features --features sqlite-only -p vituss
```

Published on crates.io as of 0.1.0:

```bash
cargo install vituss                # the binary, all four engines
```

See [docs/PUBLISHING.md](docs/PUBLISHING.md) for how releases are cut.

## Licence

Apache-2.0, as Vitess is.
