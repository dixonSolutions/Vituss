# Publishing to crates.io

Vituss is not published yet. Everything needed to publish is in place — each
crate has a description, licence, keywords, categories and a version on its
internal dependencies — so it is a decision, not a task.

## Before the first publish

**Set the real repository URL.** `Cargo.toml` currently says
`https://github.com/kingspan/vituss`, which was a placeholder. crates.io shows it
on every crate page and `cargo` warns without it.

```toml
[workspace.package]
repository = "https://github.com/<your-org>/vituss"
```

**The names are free.** `vituss`, `vituss-core` and the rest were unclaimed on
crates.io at the time of writing. Publishing `vituss` reserves the prefix in
practice, since crates.io blocks confusingly similar names.

## Order matters

crates.io verifies that every dependency already exists, so the workspace has to
go out in dependency order. One failure part-way leaves the earlier crates
published and irreversible — versions cannot be re-used, only yanked — so do a
dry run of the whole sequence first.

```bash
# Verify every crate packages and builds in isolation.
for c in vituss-core vituss-dialect vituss-topo vituss-vschema vituss-vindex \
         vituss-backend vituss-planner vituss-engine vituss-tablet \
         vituss-gate vituss-wire vituss-ctl vituss; do
  cargo publish --dry-run -p "$c" || break
done
```

The dry run for anything past `vituss-core` will report that its dependencies are
not on crates.io. That is expected and is the one thing a dry run cannot check;
it only proves the packaging.

Then, for real, in the same order, waiting for the index to update between each:

```bash
cargo publish -p vituss-core        # no internal dependencies
cargo publish -p vituss-dialect
cargo publish -p vituss-topo
cargo publish -p vituss-vschema
cargo publish -p vituss-vindex
cargo publish -p vituss-backend
cargo publish -p vituss-planner
cargo publish -p vituss-engine
cargo publish -p vituss-tablet
cargo publish -p vituss-gate
cargo publish -p vituss-wire
cargo publish -p vituss-ctl
cargo publish -p vituss             # the CLI
```

## What users get

```bash
cargo install vituss                # the `vituss` binary, all four engines
cargo install vituss --no-default-features --features sqlite-only
```

```toml
# embedding the router in your own service
[dependencies]
vituss-gate = "0.1"
vituss-backend = { version = "0.1", features = ["mysql", "postgres"] }
```

## Should it be published?

**Reasons to.** `cargo install vituss` is how a Rust tool is expected to arrive.
The plug-in story only works if a third party can depend on `vituss-dialect` and
`vituss-backend` from their own crate to add an engine — which is the whole point
of the design, and it is awkward against a git dependency.

**Reasons to wait.** Publishing 13 crates fixes their names and their public APIs
in a way that a git dependency does not. The traits are the product here, and
they are one release old. The gaps in the README — no VReplication, no failover,
no gRPC between components — also set expectations that a crates.io listing tends
to raise.

**A middle path.** Publish `vituss-core`, `vituss-dialect` and `vituss-vindex`
first. They are the stable, genuinely reusable pieces: a neutral SQL value type
with per-engine rendering, and a set of Vitess-compatible sharding functions.
Both are useful on their own to anyone building something adjacent, and neither
commits you to the shape of the rest.

## Versioning

The internal dependencies pin exact minor versions (`version = "0.1.0"`), so a
breaking change to one crate needs a coordinated bump across the workspace. Use
`cargo release` or `cargo-workspaces` rather than editing thirteen manifests by
hand.
