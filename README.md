# Vite

A design skeleton for a Vitess-style query router with **pluggable SQL
dialect parsers** (MySQL, PostgreSQL, ANSI/generic), instead of Vitess's
current MySQL-only grammar.

This is a proof of the architectural boundary, not a Vitess
replacement — see [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the
full design rationale, what's implemented, and what a production system
still needs.

## Layout

```
crates/
  sql-ast/               dialect-agnostic trait boundary
  sql-dialect-mysql/      MySQL grammar (sqlparser-rs)
  sql-dialect-postgres/   real PostgreSQL grammar (pg_query / libpg_query)
  sql-dialect-generic/    ANSI/generic fallback (sqlparser-rs)
  vtgate-core/            dialect-agnostic routing stub
  vtgate-cli/             demo CLI wiring all dialects into the router
```

## Try it

```sh
cargo run -p vtgate-cli -- --dialect mysql    "SELECT id FROM users WHERE id = 5"
cargo run -p vtgate-cli -- --dialect postgres "SELECT id FROM users WHERE id = 5"
cargo run -p vtgate-cli -- --dialect generic --shards shard-0 \
    "INSERT INTO users (id, name) VALUES (1, 'a')"
```

```sh
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets
```
