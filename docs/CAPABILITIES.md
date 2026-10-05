# CoreTexDB — Capabilities and Design

> A companion to the operational `README.md`. This document explains **what the
> system does, why it works that way, and what it deliberately does not do**.
>
> - Operating it: [`README.md`](../README.md), [operations runbook](OPERATIONS.md)
> - Recovering from failure: [recovery runbook](RECOVERY.md)
> - Internals: [architecture](architecture.md) · [roadmap](roadmap.md)

CoreTexDB is a vector database for AI applications: storage, approximate and
exact search, hybrid retrieval, and the operational surface that production
deployments need — replication, snapshots, clustering, observability.

---

## 1. What works

| Capability | State |
|---|---|
| Vector storage, CRUD, TTL | Working |
| Indexes: brute-force (exact), HNSW, IVF, scalar, PQ (quantised) | Working |
| Search: vector, metadata-filtered, hybrid (vector + BM25, RRF), reranked | Working |
| SQL subset | Working |
| Durability: write-ahead log, atomic snapshots, restart recovery | Working |
| Replication: full + incremental sync, read-only replicas | Working (library + REST for the data plane) |
| Clustering: slot routing, node discovery, collection migration | Working (library) |
| Pub/Sub: change events, WebSocket bridge | Working (library) |
| Observability: Prometheus metrics, tracing, INFO | Working (library) |
| Interfaces: REST, gRPC, CLI, C ABI, Python | Working |

## 2. What is deliberately absent

This is the interesting part. These are decisions, not gaps.

**Vector-level sharding.** A collection is the unit of placement. An index is
built per collection, so splitting one collection across nodes would turn every
query into a merge of partial ANN results — a distributed retrieval problem this
design does not attempt. Routing at collection granularity keeps every query a
local search.

**Transactions in the replication log.** Transactional writes (`*_tx`) do not
enter the write-ahead log, so they are neither replicated nor recovered from it.
This is a pre-existing limitation, recorded rather than hidden.

**Automatic renames in replication.** `rename_collection` is not journalled; a
replica needs a full resync to follow one.

**Parallel full scans.** Measured, then rejected: splitting a brute-force scan
across 8 cores was consistently *slower* than the inline loop (20k × 128 dims:
4.2 ms parallel against 2.1 ms inline), because the per-candidate work is a
single ~80 ns distance and the split costs more than the work it parallelises.
The numbers are in the source so nobody retries it blind.

**Cluster HTTP endpoints and `INFO` over REST.** The library API is complete and
tested; the endpoints are not wired because the REST and CLI modules are under
active development elsewhere. See [operations §7](OPERATIONS.md#7-not-wired-yet).

---

## 3. Durability, and what a power cut costs

Three layers, each with its own contract:

- **`FileStorage`** — an append-only log of length-prefixed, CRC-checked
  records across segments. A torn tail is discarded; a broken checksum stops
  recovery there. It never misreads a partial write.
- **The write-ahead log** — `checksum|json` lines. A corrupt line is skipped and
  counted; its neighbours replay.
- **The manifest** — collection schemas. Losing it costs nothing: schemas are
  journalled, so they are rebuilt from the log with their metric and index type
  intact.

Writes land as WAL → storage → memory → index. A crash can therefore leave the
log *ahead* of storage, never behind — recovery replays confirmed writes.

A bulk write fsyncs **once per batch**. Per-row fsync made a 1000-row insert
take 51.9 s; batched it takes 49.5 ms. The contract stays all-or-nothing:
success means the whole batch is durable.

Tested, not asserted: `tests/fault_injection.rs` truncates files at byte
offsets, flips checksums, deletes segments and appends partial lines, then
requires that recovery never invents data and never panics.

---

## 4. Search

### Indexes

| Index | Use |
|---|---|
| `brute_force` | Exact search. The default, and the right answer when recall matters more than latency. |
| `hnsw` | Approximate, in-memory graph. Fast at high recall. |
| `ivf` | Approximate, clustered. |
| `scalar` | Metadata-only indexing. |
| `pq` | Product quantisation. Big memory saving, lossy. |

An unrecognised index type falls back to exact search, so a typo cannot silently
trade accuracy for speed.

### Distances

`metric_distance` is the single function every index routes through, so the
vector kernels apply everywhere at once. AVX/FMA paths run behind a
one-time-per-process detection, in `#[target_feature]` functions — intrinsics in
an ordinary function degrade to opaque calls, which measured 5.5× *slower* than
scalar code before that was fixed.

On 8 cores at 128 dimensions: euclidean 4.9×, cosine 5.4× versus the scalar
reference. Vectorised accumulation orders differ, so results agree to
floating-point tolerance rather than bit-for-bit; a test cross-checks every
kernel against a scalar reference at lengths that exercise lane tails and
degenerate inputs.

### Filters

Metadata filtering never shortens a result. The engine oversamples the index,
re-checks each candidate, and falls back to an exact scan when the proposals
cannot fill `k`. A metadata inverted index (`data_version`-validated against the
write lock) makes selective filters sub-linear while the linear predicate stays
exact — an inverted index narrows to a candidate **superset**, never to an
answer on its own.

Supported: `$eq`, `$gt`/`$gte`/`$lt`/`$lte`, `$in`, `$ne`, `$exists`, `$regex`,
`$and`, `$or`, `$not`.

### Hybrid retrieval

Vector results and BM25 matches over `metadata` are fused by reciprocal rank
fusion. Either side may be omitted. Reranking is a second pass over the fused
candidates; with no text query it passes the fused ordering through unchanged
rather than degrading it.

### Determinism

Results at equal distance are ordered by id. Without that, the order came from
hash-map iteration order — so a restart could return identical rows in a
different order, and two replicas fed the same data could disagree on their
rankings. Found by the differential suite; it is the kind of defect no single
component's own tests can surface.

---

## 5. Replication

A replica pulls. First contact is a full snapshot; after that it applies the
log tail. It refuses writes at the data layer — REST, gRPC, C ABI and direct
library calls are all covered, not just the ones someone remembered.

The snapshot read order is load-bearing: log position, then schemas, then rows.
Record writes hold the data write lock across their WAL append, and schema
changes hold the collections lock across theirs, so anything absent from a
snapshot was written after its position and arrives in the tail. Anything
present may be replayed as well, and every apply is idempotent — which is what
makes the overlap harmless.

The replica's applied position is persisted (`replica_state.json`, written by
atomic rename). If the primary can no longer answer continuously from that
position — segments discarded, or the log reset — the replica is told so and
falls back to a full snapshot rather than silently skipping history.

Restoring a primary from a backup moves its log position backwards, so every
replica resyncs once. That is correct: the alternative is a replica quietly
missing writes.

---

## 6. Clustering

16384 slots, CRC16, `{hashtag}` pinning — the Redis Cluster model, so operators
can reuse slot tooling. Routing, health probing and collection migration are
library-level.

Migration is copy-then-route: the slot moves only after the target has the
data. A failed target leaves the route on the node that still holds it. The
source keeps its copy, because cleanup should be an explicit act rather than a
side effect of a routing change.

**Scale the fleet, not the collections.** Sharding one collection across nodes
is a different problem (see [§2](#2-what-is-deliberately-absent)).

---

## 7. Observability

Three metric stacks had accumulated side by side, and `tracing` events were
emitted with no subscriber configured. There is now one switch
(`Telemetry::init`) that installs the subscriber, owns the metrics the
`/metrics` endpoint renders, wires command statistics onto the database, and
folds replication position, cluster slots, per-command counters and slow-query
counts into the same exposition — under names that cannot collide with the
metrics already there.

Command statistics are optional to the last instruction: with no observer
attached, an entry point does no clock reads and takes no locks, and the
parameter description sits behind a closure so it is not even built.

---

## 8. Building and testing

```bash
cargo build --release                    # one binary: `coretex`
cargo test --lib                         # unit tests
cargo test                               # everything
```

Feature flags:

| Feature | Contents |
|---|---|
| default | `tokio`, `serde`, `compression`, `metrics` |
| `python` | PyO3 bindings |
| `full` | rocksdb, s3, pyo3, wasm |

`onnx` and `tantivy` are **not** in `full`: their code never compiled against the
pinned dependency versions, so enabling them would ship a broken build. A
per-feature compile gate in CI keeps that from rotting.

Test layout:

| Suite | Covers |
|---|---|
| `src/**` unit tests | Index maths, WAL, filters, SIMD kernels, recovery |
| `tests/persistence_and_search.rs` | Storage, restart, index persistence |
| `tests/filtered_search.rs` | Filter semantics and the no-short-results guarantee |
| `tests/hybrid_search.rs`, `rerank_search.rs` | Hybrid and reranked retrieval |
| `tests/replication.rs` | Full/incremental sync, read-only refusal, resumption |
| `tests/cluster.rs` | Slot routing, migration, failure handling |
| `tests/snapshot.rs` | Snapshot round-trip, corruption refusal, log compaction |
| `tests/pubsub.rs` | Change events, failure silence, WebSocket bridge |
| `tests/differential.rs` | Five restore paths agreeing, field by field |
| `tests/fault_injection.rs` | Truncation, checksum damage, segment loss, restarts |
| `tests/ffi_api.rs` | C ABI surface, header/source symbol agreement |

The last two are the ones that catch what per-component tests cannot:
cross-path divergence, and behaviour when the layer below lies.

---

## 9. Interfaces

- **REST** — the primary HTTP surface, plus GraphQL and WebSocket.
- **gRPC** — with rate limiting and auth.
- **CLI** — `coretex server | doctor | backup | vector | index | search | sql | …`
- **C ABI** — `include/coretexdb.h`, 13 functions; `tests/ffi_api.rs` guards that
  the header and the source agree, so the header cannot drift.
- **Python** — `pip install coretexdb`; an independent 1.0.x version line.

---

## 10. Where to look next

| Question | Document |
|---|---|
| How do I run it? | [`README.md`](../README.md) |
| How do I operate replicas, clusters, snapshots? | [OPERATIONS.md](OPERATIONS.md) |
| What happens when it crashes? | [RECOVERY.md](RECOVERY.md) |
| How is it built internally? | [architecture.md](architecture.md) |
| What is planned? | [roadmap.md](roadmap.md) |