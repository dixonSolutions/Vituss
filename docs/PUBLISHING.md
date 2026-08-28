# Publishing to crates.io

Vituss is on crates.io as of 0.1.0 — all thirteen crates, published together.
`.github/workflows/publish.yml` handles releases from here.

Two things caught the first attempt out, both worth knowing before a release
from a fresh machine or a fresh account:

- **crates.io needs a verified email on the account.** Not just set, verified.
  Without it every upload is rejected with a 400 before any bytes move.
- **New crate names are rate limited**, roughly one per ten minutes once the
  initial burst is spent. Publishing thirteen new names took about an hour of
  waiting. This only bites the first release; subsequent versions of an existing
  crate are not affected.

## How a release happens

`publish.yml` runs on every push to `main` and gates on one question: is the
version in `[workspace.package]` already on crates.io? Almost always it is, and
the run stops at the gate having done nothing. So:

1. Bump `version` in the root `Cargo.toml` and run `cargo update --workspace`
   so `Cargo.lock` agrees.
2. Merge to `main`.
3. The gate sees a version crates.io has not got, the full test suite runs, then
   `cargo publish --workspace` goes out and the commit is tagged `v<version>`.

The token lives in the `CARGO_REGISTRY_TOKEN` repository secret and is read only
by the `publish` job, which runs in the `crates-io` environment. That environment
currently has no protection rules, so a version bump merged to `main` releases
without asking. Adding a required reviewer there puts a human back in front of
the irreversible step without touching the workflow.

Nothing about this is undoable. A crates.io version can be yanked but never
replaced or deleted, and the name is claimed forever the first time it goes out.
The gate exists so that an ordinary commit to `main` cannot trigger a release;
it does not make the release itself reversible.

## Doing it by hand instead

If you would rather not go through CI, order matters — crates.io verifies that
every dependency already exists, so the workspace has to go out in dependency
order. One failure part-way leaves the earlier crates published and
irreversible, so do a dry run of the whole sequence first.

Since Cargo 1.90, `cargo publish --workspace` works out that order itself and
waits for the index between crates; that is what the workflow runs. The
explicit sequence below is the fallback.

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
