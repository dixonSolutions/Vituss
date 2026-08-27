# Vitess-in-Rust: architecture and scope

## What this is

A **design document and a running skeleton**, not a Vitess replacement.
Vitess (https://vitess.io) is a large, mature Go system: VTGate (query
routing/planning), VTTablet (per-shard proxy + query rewriting), VTOrc
(replication management), a topology service, an online-DDL engine, and
its own hand-written `sqlparser` for MySQL's grammar. Rewriting all of
that in Rust, correctly, is a multi-year, multi-person effort — not
something to attempt speculatively in one session. What's here instead:

- A real architectural boundary between **dialect-specific SQL parsing**
  and the **dialect-agnostic command/routing layer**, proven out with
  three actually-different parser backends (MySQL and ANSI/generic via
  `sqlparser-rs`, PostgreSQL via `pg_query` which wraps the real
  PostgreSQL grammar).
- A minimal, compiling, runnable Cargo workspace that demonstrates the
  seam: `sql-ast` (the trait boundary) → dialect crates (parser
  backends) → `vtgate-core` (routing stub) → `vtgate-cli` (demo binary).
- An honest list of what a real system needs next, and where it plugs in.

## Why "one AST for all dialects" is the wrong goal

The instinct is to define a single SQL AST and have every dialect parser
translate into it, the way Vitess's own `sqlparser` package defines one
MySQL-flavored AST. That works *because* Vitess only supports MySQL. The
moment you add PostgreSQL, the premise breaks down:

- PostgreSQL's real grammar (what `pg_query`/`libpg_query` implements) has
  constructs with no MySQL equivalent (e.g. `RETURNING`, native array
  types, `LATERAL`, window frame variations, its own DDL surface) and
  disagrees with MySQL on things as basic as identifier quoting and upsert
  syntax.
- A hand-rolled "superset" grammar drifts from what any real engine
  actually accepts, and every new dialect added means renegotiating the
  shared AST's shape — which is exactly the maintenance trap this
  project should avoid.
- Existing high-quality dialect parsers already disagree on AST
  representation: `sqlparser-rs`'s `Statement` enum vs. `pg_query`'s
  protobuf tree (a direct mirror of Postgres's own internal parse nodes)
  are not reconcilable without a lossy translation layer, maintained
  forever, for every dialect added.

## The actual boundary: two small traits

`sql-ast` (crate) defines the contract every dialect plugs into:

```rust
trait SqlDialectParser: Send + Sync {
    fn name(&self) -> &'static str;
    fn parse(&self, sql: &str) -> Result<ParsedStatement, ParseError>;
    fn parse_batch(&self, sql: &str) -> Result<Vec<ParsedStatement>, ParseError>;
}

trait StatementInfo {
    fn kind(&self) -> StatementKind;      // Select/Insert/Update/Delete/Ddl/...
    fn tables(&self) -> Vec<TableRef>;    // what the command layer needs to route
    fn to_sql(&self) -> String;           // push the statement down unchanged
}
```

- **`SqlDialectParser`** is implemented once per dialect. It owns parsing
  and keeps the dialect's *native* AST fully intact internally — no
  translation happens here.
- **`StatementInfo`** is implemented once per dialect's parsed-statement
  wrapper. It answers only the questions the command/routing layer
  actually needs. This is deliberately small: statement kind (for
  read/write splitting), tables touched (for vindex/shard resolution),
  and a way to get SQL text back out (for pushing the statement down to
  a tablet, since VTGate/VTTablet-style architectures forward SQL text,
  not a rewritten AST, to the underlying engine in the common case).

`vtgate-core`, the command/routing layer, is written **only** against
these two traits. It never imports `sql-dialect-mysql`,
`sql-dialect-postgres`, or any other dialect crate — those are wired in
by whichever binary assembles the system (`vtgate-cli` here; a real
server binary in a full implementation). New dialects — SQLite, T-SQL,
Snowflake, whatever — plug in by adding a crate that implements these two
traits. Nothing in `vtgate-core` changes.

This mirrors how Vitess itself is layered (VTGate is protocol/topology
generic; VTTablet's `TabletType`/query-service abstraction is generic
over the backing MySQL instance) — just moving the dialect seam one level
higher, from "MySQL vs. no other option" to "any dialect with a
`SqlDialectParser` impl."

## Workspace layout

```
crates/
  sql-ast/               the trait boundary (this doc's core contribution)
  sql-dialect-mysql/      MySQL grammar, via sqlparser-rs's MySqlDialect
  sql-dialect-postgres/   real PostgreSQL grammar, via pg_query (libpg_query)
  sql-dialect-generic/    ANSI/generic fallback, via sqlparser-rs's GenericDialect
  vtgate-core/            dialect-agnostic registry + stub router
  vtgate-cli/             demo binary wiring all three dialects into the router
```

Try it:

```
cargo run -p vtgate-cli -- --dialect mysql    "SELECT id FROM users WHERE id = 5"
cargo run -p vtgate-cli -- --dialect postgres "SELECT id FROM users WHERE id = 5"
cargo run -p vtgate-cli -- --dialect generic --shards shard-0 \
    "INSERT INTO users (id, name) VALUES (1, 'a')"
```

Note `sql-dialect-mysql` and `sql-dialect-generic` share their
`StatementInfo` implementation (`classify`/`extract_tables` in the
generic crate) because both happen to parse into `sqlparser-rs`'s AST —
that's an implementation convenience between two crates that chose the
same backend, not a requirement the trait imposes. `sql-dialect-postgres`
implements `StatementInfo` completely independently, directly over
`pg_query`'s protobuf tree, which is the case that actually proves the
boundary holds.

## What existing crates this leans on, and why

Per the "use existing crates, don't hand-roll grammars" decision:

- **`sqlparser-rs`** — actively maintained, dialect-parameterized SQL
  parser used in production by DataFusion, GreptimeDB, and others.
  Covers MySQL, Postgres-flavored, ANSI, Snowflake, BigQuery, Hive,
  ClickHouse, and more dialects out of the box — each new
  `sql-dialect-*` crate for one of those dialects is mostly plumbing.
- **`pg_query` (pg_query.rs)** — wraps `libpg_query`, which is extracted
  directly from the PostgreSQL server source. This is the real grammar,
  not a reimplementation, so Postgres compatibility is bounded by
  PostgreSQL's own release cadence rather than by parser maintainer
  effort.

This intentionally avoids what Vitess's own `sqlparser` package does
(hand-maintain a grammar via goyacc), which is exactly the kind of
per-dialect maintenance burden pluggable dialects via mature upstream
crates are meant to sidestep.

## What a real implementation still needs

Ordered roughly by how soon it'd block real use, not by difficulty:

1. **A real vschema and planner.** `vtgate-core::Router` today has no
   concept of keyspaces, shards, or vindexes — it either targets one
   configured shard list or refuses multi-shard writes outright. Vitess's
   actual planner (`go/vt/vtgate/planbuilder`) resolves routing from
   table/column predicates against defined vindexes (hash, lookup,
   consistent-lookup, ...); that's a substantial component on its own,
   and it's dialect-specific in ways `StatementInfo` doesn't yet expose
   (e.g. WHERE-clause predicate extraction per vindex column).
2. **Per-dialect predicate/expression extraction.** `StatementInfo::tables()`
   is enough to know *what's touched*; real routing needs *which column
   values* pin a row to a shard, which means walking each dialect's
   expression tree — necessarily dialect-specific work living inside
   each `sql-dialect-*` crate, exposed through an extended `StatementInfo`.
3. **A topology service equivalent.** Vitess's topology (etcd/ZooKeeper/
   Consul-backed) tracks live tablets, shard ownership, and serving
   graphs. Nothing here talks to one yet.
4. **A tablet-side proxy.** Vitess's VTTablet does connection pooling,
   query rewriting for its own MySQL-specific rules, replication-aware
   read routing, and online DDL. None of that exists here; `to_sql()` is
   as far as this skeleton goes toward "send the query somewhere."
5. **Wire protocol servers.** MySQL and PostgreSQL wire protocols are
   different enough (auth handshake, prepared-statement binary formats,
   COPY protocol for Postgres) that "one server, N dialects" likely means
   one wire-protocol crate per frontend protocol, still funneling into
   the same `vtgate-core` router. `vtgate-cli` here has no listener at
   all — it's a one-shot CLI over the router, not a server.
6. **Correctness/compatibility testing against real engines.** Any of
   this claiming MySQL or Postgres compatibility needs to be validated
   against the databases themselves (e.g. running each engine's own SQL
   logic test suite through the relevant dialect crate), not just
   "compiles and parses example queries."

None of this is a reason not to start — it's the reason the first
deliverable is the trait boundary and a skeleton proving it holds against
two structurally different parsers, rather than a half-built router with
no clear seam for what comes next.
