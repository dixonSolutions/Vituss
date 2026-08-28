# Architecture

Vituss is a router, not a database. Everything below follows from that.

## The path of a query

```
  client statement
        │
   1.   ├─ vituss-wire      protocol decode; session chatter answered here
        │
   2.   ├─ vituss-gate      is this SET/@@variable? if so, answer and stop
        │
   3.   ├─ vituss-dialect   parse with the *client's* grammar
        │
   4.   ├─ vituss-planner   VSchema + predicates → a Plan. No data is touched.
        │
   5.   ├─ vituss-engine    walk the plan
        │                     ├─ vituss-vindex   values → keyspace IDs
        │                     ├─ vituss-gate     keyspace IDs → shard names
        │                     ├─ vituss-dialect  render per shard's engine
        │                     ├─ vituss-tablet   execute
        │                     └─ combine         merge / aggregate / limit
        │
   6.   └─ vituss-wire      encode for the client's protocol
```

Steps 3 and 5 are the ones that make engine independence work, and they are
deliberately separate. The statement is parsed **once**, with the grammar of the
engine the *client* speaks; it is rendered **per shard**, for the engine that
shard runs. A MySQL client's backtick-quoted identifiers become double quotes on
a PostgreSQL shard and brackets on a SQL Server one, and its `?` placeholders
become `$1` or `@p1`, because rendering is a translation rather than a
`to_string()`.

## Crates

| Crate | Responsibility |
|---|---|
| `vituss-core` | Values, result sets, key ranges, destinations, sessions, errors. Knows nothing about any engine. |
| `vituss-dialect` | The `SqlDialect` plug-in: grammar, quoting, placeholders, capabilities, introspection SQL, error mapping, transaction control. |
| `vituss-backend` | The `Backend`/`Connection` plug-in: drivers. `sqlx` for MySQL / PostgreSQL / SQLite, `tiberius` for SQL Server, plus a programmable fake for tests. |
| `vituss-topo` | Cluster metadata over a pluggable `TopoStore` (memory, files, room for etcd). Keyspaces, shards, tablets, the serving graph, locks, watches. |
| `vituss-vschema` | The logical schema: which tables live where and how they are sharded. Validates itself at load, not at query time. |
| `vituss-vindex` | The `Vindex` plug-in: sharding functions. Bit-compatible with Vitess's. |
| `vituss-planner` | Statement → `Plan`. Pure: no I/O, no data, cacheable. |
| `vituss-engine` | Executes a plan through the `ShardGateway` trait. Fan-out, merge-sort, aggregation, joins, limits. |
| `vituss-tablet` | The shard-side server: one database, its pool, its open transactions, its schema, its health. |
| `vituss-gate` | The gateway: sessions, discovery, resolution, transaction coordination. Stateless and replaceable. |
| `vituss-wire` | MySQL and PostgreSQL protocol servers. |
| `vituss-ctl` | Declarative cluster config and administrative operations. |
| `vituss-cli` | The `vituss` binary. |

Dependencies point one way: `core → dialect → vindex/vschema → planner → engine
→ gate → wire`. No cycles, and no crate below the planner knows a cluster exists.

## Sharding

A **keyspace** is one logical database. A **shard** owns a half-open range of
byte strings, and is named after it: `-80`, `80-c0`, `c0-`. A **vindex** maps
column values to a *keyspace ID* — an opaque byte string — and the keyspace ID
falls in exactly one shard's range.

```
   vindex("user_id" = 4)  →  keyspace id 0xd2fd8867d50d2dfe
                                     │
   shards:  [ -40 ] [ 40-80 ] [ 80-c0 ] [ c0-  ]
                                            ▲
                                       this one
```

Because the mapping is arithmetic on bytes, it is identical whichever engine
stores the row. That is why a keyspace can be migrated between engines without
re-sharding, and why the same layout works over four engines that agree on
almost nothing else.

Splitting a shard is a metadata operation: `-80` becomes `-40` and `40-80`, and
every keyspace ID keeps the same value. (Moving the *rows* is VReplication's job
and is not implemented yet — see the README's gap list.)

## Planning

The planner's job is to push as much work into the shards as possible.

A query that provably reaches **one shard** is sent verbatim, with nothing added
above it. The shard's own optimiser handles the joins, the sort, the aggregate
and the limit — all of which it does better than a router could, because it has
the indexes and the statistics.

A query that spans shards gets exactly the primitives it needs and no more:

```
  SELECT country, COUNT(*) FROM user GROUP BY country     -- scattered

  Truncate to 1 column          ← drops the helper columns the planner added
    Aggregate [CountStar] group_by=[0] ordered=true
      Route(Scatter) keyspace=commerce
        query: SELECT country, COUNT(*) FROM user GROUP BY country ORDER BY country
        merge-sort on [0]
```

Note what was pushed down: the `GROUP BY` and the `COUNT` run on every shard, and
so does an `ORDER BY` the user never wrote — added so the gate's aggregate can
*stream* over sorted input instead of buffering every row.

Three rewrites are worth knowing about, because they are where naive routing goes
wrong:

- **`AVG` is split into `SUM` and `COUNT`.** The mean of per-shard means is not
  the mean unless the shards happen to be evenly filled.
- **`LIMIT n OFFSET m` is widened to `LIMIT n+m` on each shard**, then applied
  again at the gate. Any single shard could hold all of the top *n*.
- **Sort and group keys missing from the projection are added**, then trimmed
  after merging. The gate cannot merge on a column the shards did not return.

And one refusal: `COUNT(DISTINCT x)` across shards is rejected with a reason,
because per-shard distinct counts cannot be added. Routed to one shard, it works
fine.

### Joins

Two tables sharded by the same vindex on the columns being joined are
*collocated*: matching rows are, by construction, on the same shard. The whole
join is pushed down and the gate only concatenates.

```
  SELECT u.name, o.price FROM user u JOIN corder o ON u.user_id = o.user_id

  Route(Scatter)              ← one primitive; each shard joins its own rows
```

When they are not collocated, the gate runs a nested loop: the left side, then
the right side once per left row with values bound in. Correct, and O(left) round
trips — which is why the planner works so hard to avoid it, and why the VSchema
lets you shard `corder` by `user_id` rather than by `order_id`.

## Transactions

Session state, including the open per-shard transactions, travels with each
request rather than living in the gate. Gates are therefore interchangeable.

Three modes:

- **`single`** — reject anything that would touch a second shard. Strictest, and
  the right default for workloads that can be designed for it.
- **`multi`** (default) — commit shards one at a time. Not atomic; if a later
  shard fails, the error says exactly which shards had already committed.
- **`two_pc`** — the engines' own distributed commit: XA on MySQL,
  `PREPARE TRANSACTION` on PostgreSQL. Refused when any participant cannot do it,
  and refused when participants disagree on the protocol — bridging XA and
  prepared transactions has no recovery story.

Reads inside a transaction are routed to the primary, so a client sees its own
uncommitted writes.

## Lookup vindexes

A lookup vindex routes on a column that is *not* the sharding key, by keeping the
mapping in a table:

```
  SELECT * FROM user WHERE email = 'ada@example.com'

  1. email → keyspace_id      via a query against the lookup table
  2. keyspace_id → shard      via the serving graph
  3. the real query           to that one shard
```

The lookup table is itself a Vituss table, so it can be sharded and can live on a
different engine. Its query goes back through the planner via the `VCursor`
trait.

When a table *owns* a lookup vindex, Vituss maintains it: inserts write the
lookup row first (so a duplicate is caught before anything else happens), and
deletes remove it after (so a rolled-back delete does not lose the mapping).

While a lookup vindex is being backfilled it is marked `write_only`, and routing
through it **scatters** rather than trusting an incomplete table. A wrong answer
that looks right is the failure mode worth engineering against.

## Failure behaviour

The recurring principle: prefer a loud failure to a plausible wrong answer.

- A serving graph with a gap or an overlap is refused at rebuild time, not
  discovered as a "no shard for keyspace id" error at 3am.
- A DDL that fails part-way through a keyspace reports which shards already
  applied it, and stops rather than continuing.
- A sum that would overflow `i64` widens to a decimal rather than wrapping.
- An unknown vindex parameter is an error, not a warning — a typo in a vindex
  param silently changes how data is sharded.
- A replica lagging beyond the threshold is taken out of rotation; stale data
  returned as fresh is worse than an error.
- Passwords never appear in a DSN that reaches a log, and `${VAR}` references in
  a DSN are expanded at connect time so the topology can be committed to git.
