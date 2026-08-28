# Coming from Vitess

Vituss keeps Vitess's model — it is a good one — and changes the premise that it
is a MySQL system. This is the concept-by-concept mapping, followed by an honest
account of what is not here yet.

## Vocabulary

| Vitess | Vituss | Notes |
|---|---|---|
| `vtgate` | `vituss-gate` + `vituss-wire` | Same role. Protocol servers are a separate crate, so a deployment picks which it exposes. |
| `vttablet` | `vituss-tablet` | Same role. Owns one database, its pool, its transactions, its schema. |
| `vtctld` / `vtctl` | `vituss-ctl` + `vituss` CLI | Adds a declarative cluster config; the imperative operations are still there. |
| `vtcombo` | `vituss up` | Whole cluster in one process. |
| `topo` (etcd/consul/zk) | `vituss-topo` | Same shape, behind a `TopoStore` trait. Memory and file stores ship; etcd is a plug-in away. |
| VSchema | VSchema | Same JSON. An existing `vschema.json` loads unchanged. |
| vindex | vindex | Functional ones are bit-compatible — see below. |
| keyspace / shard / tablet type | identical | Including `-80` shard naming and `primary`/`replica`/`rdonly`. |
| `VExplain` | `VEXPLAIN` | Same idea, same purpose. |
| sqlparser (hand-written) | `sqlparser` crate, per dialect | The main structural change. |
| `sqltypes.Value` (type + bytes) | `vituss_core::Value` (decoded enum) | Vitess's form follows MySQL's text-first wire encoding; Vituss must interoperate with engines whose encodings disagree. |

## Vindex compatibility

The functional vindexes produce **byte-identical** keyspace IDs to Vitess's,
verified against vectors lifted from Vitess's own test suite:

| Vindex | Status |
|---|---|
| `hash` | Identical — DES-ECB of the big-endian u64 under an all-zero key |
| `xxhash` | Identical — xxHash64 of the canonical bytes, little-endian |
| `binary`, `binary_md5` | Identical |
| `numeric`, `numeric_static_map` | Identical |
| `reverse_bits`, `null` | Identical |
| `multicol` | Compatible; the partial-key case narrows to a key range |
| `lookup`, `lookup_unique`, `lookup_hash`, `lookup_hash_unique` | Compatible |
| `consistent_lookup`, `consistent_lookup_unique` | Compatible |
| `cfc`, `region_experimental`, `region_json` | **Not ported** |
| `unicode_loose_md5`, `unicode_loose_xxhash` | **Not ported** — see below |

The unicode vindexes depend on MySQL's UCA 4.0.0 collation weight tables. An
implementation that is close but not exact would place rows on the wrong shard
without any error, so they are absent rather than approximated. If you use them,
Vituss cannot serve that keyspace yet — and that is the right answer.

An existing Vitess keyspace using the compatible vindexes maps to the same shards
under Vituss. A migration moves no rows.

## Query support

Working:

- Routing: `Unsharded`, `EqualUnique`, `Equal`, `IN`, `MultiEqual`, `Range`,
  `Scatter`, `Reference`, `AnyShard`, `ByDestination`, `None`
- Cross-shard merge-sort, `LIMIT`/`OFFSET`, `DISTINCT`
- Aggregation: `COUNT`, `SUM`, `MIN`, `MAX`, and `AVG` split into `SUM`+`COUNT`
- `GROUP BY`, streaming when the shards return sorted input
- Collocated joins pushed into the shard; non-collocated ones as nested loops
- `INSERT` routed per row, with sequences and owned lookup-vindex maintenance
- `UPDATE` / `DELETE` with lookup upkeep and a locked pre-read
- DDL broadcast to every shard of a keyspace
- `UNION` / `UNION ALL`
- `USE ks`, `USE ks@replica`, `USE ks:-80`

Refused, with a reason rather than a wrong answer:

- `COUNT(DISTINCT x)` across shards — per-shard distinct counts cannot be added
- `INSERT ... SELECT` into a sharded table — the rows would have to be routed one
  by one, and Vituss will not do that implicitly
- Changing a row's sharding column — that is a delete plus an insert
- A cross-shard join with no equality to join on — it is a cartesian product
- `ORDER BY` / `LIMIT` / aggregates *over* a non-collocated join

Not implemented:

- Subqueries in `FROM`
- Correlated subqueries
- `EXCEPT` / `INTERSECT` across shards
- Window functions across shards (they work fine routed to one shard)
- Savepoints inside a multi-shard transaction

## Operations

| Vitess | Vituss |
|---|---|
| `vtctld CreateKeyspace` etc. | `vituss apply -c cluster.yaml`, idempotent |
| Serving graph rebuild | Automatic on apply; refused if it would leave a gap |
| `VtctldClient GetTablets` | `vituss status --probe` |
| MoveTables / routing rules | Routing rules work; the data movement does not |
| Reshard | Metadata side only — `Ctl::plan_split` creates the target shards not-serving |
| VTOrc failover | Not ported |
| Online DDL | Not ported; DDL is applied directly to every shard |
| Backups | Not ported |
| VReplication / VStream | Not ported |

## Deliberate differences

**Configuration is declarative.** A cluster is one reviewable file, applied
idempotently, rather than a sequence of `vtctl` commands. The imperative
operations still exist for the things that are genuinely imperative.

**Validation happens at load, not at query time.** An unknown vindex kind, a
sharded table with no primary vindex, a sequence in a sharded keyspace, a serving
graph with a hole — all rejected when the configuration is applied, next to the
person who can fix it.

**Unknown vindex parameters are errors.** Vitess warns. A typo in a vindex param
silently changes how data is sharded, which is not a warning-shaped problem.

**Two-phase commit is refused rather than degraded.** If a participant's engine
cannot do a real distributed commit, the transaction is rejected. Vituss will not
commit shard-by-shard and call it atomic.

**Components share a process today.** A gate talks to tablets in-process; there
is no gRPC layer yet. The `QueryService` trait is the seam where one goes, and
nothing above it would change.

## What a migration would look like

1. Point a Vituss cluster config at your existing MySQL shards, with the same
   VSchema. The vindexes agree, so the routing agrees.
2. Run both in parallel; compare results.
3. Cut clients over to the Vituss gate — the wire protocol is the same.
4. If you then want a different engine, move one shard at a time: a shard's
   `dialect` is a per-shard property, and the gate serves a keyspace whose shards
   disagree.

Step 4 is the reason Vituss exists.
