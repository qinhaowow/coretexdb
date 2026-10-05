# Operations Runbook

Day-to-day operation of CoreTexDB: replication, clustering, snapshots,
observability. Each section states what the system does on its own, what you
must configure, and how to tell the difference between healthy and broken.

> Recovery from data loss has its own document: [`RECOVERY.md`](RECOVERY.md).
> Architecture: [`architecture.md`](architecture.md).

---

## 1. The shape of a deployment

A single node is the default and needs no configuration:

```bash
coretex server --data-dir /opt/CoreTexDB
```

Two capabilities change what you must set up:

| Capability | You need | Section |
|---|---|---|
| Replication (a read replica) | WAL enabled on the primary | [§2](#2-replication) |
| Clustering (slot routing) | An explicit routing table | [§3](#3-clustering) |

Nothing else in this document is required for a working single node.

---

## 2. Replication

### What it does

A replica pulls from a primary: a full snapshot on first contact, then the
write-ahead log incrementally. It **refuses all writes** while attached — not
as a convention, but enforced at the data layer, so REST, gRPC, FFI and direct
library calls are all covered.

### Setting up a primary

Replication reads the WAL, so the primary must have it on:

```toml
# config/coretex.toml — the primary
wal_enabled = true
```

Without it, a replica can still take a full snapshot, but never an incremental
one — it would re-snapshot on every cycle.

### Setting up a replica

```rust
use coretexdb::{CoreTexDB, DbConfig, HttpTransport, ReplicaSync};
use std::{sync::Arc, time::Duration};

let db = Arc::new(CoreTexDB::with_config(DbConfig::new("/opt/CoreTexDB-replica")));
db.init().await?;

let transport = Arc::new(HttpTransport::new("http://primary:8080"));
let sync = ReplicaSync::new(
    db,
    transport,
    "/opt/CoreTexDB-replica/replica_state.json",
);

// Take the first full copy now, then keep up in the background.
sync.sync_once().await?;
let (_task, stop) = sync.spawn_loop(Duration::from_secs(1));

// Later, to stop cleanly:
stop.send(true);
```

`ReplicaSync::new` switches the database to read-only. From that moment every
mutation fails with `database is in read-only mode (replication replica)` —
including the replica's own replication replay, which is exempt by design.

### Checking that it works

```bash
curl localhost:8080/replication/status | jq
```

```json
{ "lsn": 4821, "read_only": false, "collections": 3, "records": 90210 }
```

`lsn` is the primary's log position. The replica's `/replication/status` shows
the same field **after** it has applied that much — comparing the two numbers
is the health check. They will differ by the writes of one cycle; a growing gap
means the replica is falling behind, a gap that keeps resetting means the
replica is resyncing from scratch every cycle.

### When it deliberately falls back to a full sync

Two situations force it, both reported rather than hidden:

| Situation | Signal | What happens |
|---|---|---|
| The replica's position predates what the primary still holds | `read_entries_since` returns `truncated` | Full snapshot, position reset |
| The primary's log was reset | position ahead of everything the log produced | Full snapshot, position reset |

The second is worth knowing about operationally: **restoring the primary from
a backup makes its log position go backwards**, so every replica will resync
once. That is correct behaviour — the alternative is a replica silently missing
writes.

### Replica restart

A replica's local WAL records what it *replayed*, not what it received, so
restarting does not make it current. On start it replays its own log to recover
memory, then you must sync again to catch up. `replica_state.json` decides
whether that is incremental or a full copy:

- position usable → incremental catch-up
- position no longer available → full snapshot

### Security note

`/replication/*` is exempt from authentication, like `/raft/*`. It carries the
full dataset. Restrict it at the network layer; do not expose it publicly.

---

## 3. Clustering

### What it does

Collections are assigned to nodes through a 16384-slot table, Redis-style.
The hash is CRC16 over the collection name, and `{hashtag}` pins related
collections to one slot:

```rust
use coretexdb::{slot_of, ClusterRouter, NodeInfo};

let router = ClusterRouter::new(vec![
    NodeInfo::new("n1", "http://n1:8080"),
    NodeInfo::new("n2", "http://n2:8080"),
])?;

// Same slot — they always travel together.
assert_eq!(slot_of("{user:42}:profile"), slot_of("{user:42}:orders"));

// Assign a collection, or a whole range at once.
router.assign_collection("docs", "n1").await?;
router.assign_range("n2", 0, 8191).await?;

// Look up where a collection is served.
let node = router.lookup("docs").await?;   // errors when unassigned
```

The error from `lookup` names the slot, so an HTTP layer can turn it into a
`MOVED` redirect. That endpoint is not wired yet (see §7).

### Design constraint worth understanding

**A collection is never split across nodes.** An index is built per collection,
so splitting one would turn every query into a merge of partial ANN results — a
distributed retrieval problem this phase does not attempt. Route at collection
granularity.

### Migrating a collection

```rust
use coretexdb::ClusterMigrator;

let migrator = ClusterMigrator::new(router, transports);
let outcome = migrator.migrate("docs", "n1", "n2").await?;
assert_eq!(outcome.records, 90210);
assert!(outcome.source_retained);
```

The order is the contract: **copy first, route second.** If the export or the
import fails, the route never moves, so clients keep reaching the node that
still has the data. Once it succeeds, `docs` resolves to `n2`.

`source_retained: true` is deliberate. Migration copies; it does not delete.
Cleaning up the old copy is a separate, explicit `delete_collection` — never a
silent side effect of a routing change.

**Quiesce writes during a migration.** Rows written to the source after its
export are not carried over. For a maintenance window this is trivial; for a
live system, plan for it rather than assuming the migration caught everything.

### Checking node health

```rust
let health = router.probe_all(&transports).await;
for node in health {
    println!(
        "{}: {} ({:?})",
        node.node.id,
        if node.alive { "up" } else { "down" },
        node.error
    );
}
```

A node with no registered transport reports **down**, not skipped — a silent
gap would look like a healthy cluster.

---

## 4. Snapshots and background save

### Taking one

```rust
use coretexdb::SnapshotArchive;

let archive = SnapshotArchive::open("/var/backups/coretexdb").await?;
let meta = archive.save_auto(&db).await?;
println!("{} collections, {} records, {} bytes", meta.collections, meta.records, meta.bytes);
```

A snapshot of a running database is consistent by construction — the same read
ordering replication uses. Serialisation and disk writes happen after the locks
are released, so a running node is never blocked for the length of a write.

### Background snapshots

```rust
use coretexdb::BackgroundSnapshotter;

let snapshotter = BackgroundSnapshotter::spawn(
    db,
    archive,
    Duration::from_secs(3600),   // hourly
    24,                          // keep the newest 24
);
```

Failures are logged and retried on the next tick — a snapshot that cannot be
taken never takes the database with it. Old files are pruned after each success.

### Restoring

```rust
archive.restore_into(&db, "snapshot-1735689600").await?;
```

This wipes the target and replays the image through the same path startup
recovery uses, then persists the manifest — so the restored database survives
its own restart. Restore drill: see [`RECOVERY.md`](RECOVERY.md).

### Log compaction

The WAL grows forever unless something trims it. `compact_wal` folds it to each
key's final state and writes a **new directory**:

```rust
use coretexdb::compact_wal;

let report = compact_wal(&db, Path::new("/var/lib/coretexdb/compacted")).await?;
println!("{} → {} entries", report.entries_before, report.entries_after);
```

Two deliberate properties:

- **The live log is never rewritten.** Retiring it is your decision, at a moment
  you choose — not something a background task does mid-incident.
- **A log with gaps is refused.** If history is already missing, folding what
  survives would bake the loss into a tidy-looking log. Restore from a snapshot
  or a repaired directory instead.

To adopt a compacted log, stop the node, point `wal_dir` at it, and start.

---

## 5. Observability

### One switch

```rust
use coretexdb::{Telemetry, TelemetryConfig};

let telemetry = Telemetry::init(TelemetryConfig {
    service_name: "coretexdb".to_string(),
    filter_directives: Some("info,coretexdb=debug".to_string()),
    ..Default::default()
})?;
telemetry.attach_database(&db)?;
```

That installs the tracing subscriber (idempotent — a second call keeps the
first), wires command statistics and the slow-query log, and owns the metrics
instance. Hand `telemetry.database_metrics()` to your HTTP layer; it renders the
`/metrics` exposition.

### Metrics worth alerting on

| Metric | Meaning | Alert when |
|---|---|---|
| `coretexdb_replication_lsn` | Log position | diverges between primary and replica |
| `coretexdb_read_only` | Replica guard | non-zero on a node you intend to write to |
| `coretexdb_slow_queries` | Queries over the threshold | grows steadily |
| `coretexdb_command_errors{command=…}` | Failures per operation | non-zero for a sustained period |
| `coretexdb_cluster_unassigned_slots` | Slots with no owner | non-zero in steady state |

Command statistics break down by operation name (`search`, `insert_vectors`,
`delete_vectors`, `get_vector`) with calls, errors and mean duration.

### INFO

```rust
let info = coretexdb::collect_info(&db, Some(&telemetry.commands()), None).await;
println!("{}", info.to_text());
```

Sections: `# Server` (version, mode, uptime, read-only), `# Replication` (WAL
on/off, log position), `# Keyspace` (per-collection row counts), `# Stats`
(per-command counters, slow-query summary), `# Cluster` (slots per node, when
routing information is supplied). The `read_only` flag comes from the
replication guard, not from guessing at configuration.

---

## 6. Pub/Sub

```rust
use coretexdb::EventBus;

let bus = std::sync::Arc::new(coretexdb::EventBus::new(1024));
db.data_manager.set_event_bus(bus.clone())?;
let mut events = bus.subscribe();
```

Every successful mutation then announces itself — inserts, updates, deletes
(including the transaction variants), and collection create/delete/rename. A
failed write stays silent, and a delete announces only the ids that actually
existed.

Cost when no bus is attached: one `Option` check per entry point. Publishing
never fails and never blocks — a subscriber too slow to keep up gets told it
lagged rather than back-pressuring the write that already succeeded.

To push those events to WebSocket subscribers:

```rust
use coretexdb::coretex_websocket::{WebSocketConfig, WebSocketServer};

let mut config = WebSocketConfig::default();
config.ping_interval_secs = 30;
let server = Arc::new(WebSocketServer::new(config));
server.subscribe_connection("conn-1", "docs").await;
server.attach_event_bus(bus);
let mut messages = server.event_receiver();   // per-connection loop
```

---

## 7. Not wired yet

Three capabilities exist at the library layer and have no HTTP surface, because
the REST and CLI modules are being edited elsewhere and mixing changes into
those files has caused problems before. The library API is complete and tested:

| Capability | Library API | Waiting on |
|---|---|---|
| Cluster node endpoints, `MOVED` responses | `ClusterTransport`, `ClusterRouter::lookup` | `coretex_api/rest/mod.rs` |
| WebSocket accept route | `WebSocketServer` | `coretex_api/rest/mod.rs` |
| `INFO` endpoint, `coretex info` CLI | `collect_info` | `rest/mod.rs`, `coretex_cli/mod.rs` |

Until then, drive these from Rust, or over the `/replication/*` endpoints that
do exist.

---

## 8. Routine checklist

**Daily**
- Replication: compare `lsn` between primary and replica; the gap should stay
  within one cycle.
- `coretex doctor --data-dir /opt/CoreTexDB` — reports WAL corruption counts,
  stale index checksums, manifest/collection mismatches.

**Weekly**
- Confirm background snapshots are landing: check the newest file's timestamp
  and that `coretexdb_slowest_query_ms` is not climbing.

**Monthly**
- Check WAL growth. If it is large and the primary has been up a long time,
  plan a compaction window ([§4](#4-snapshots-and-background-save)).
- Rehearse a restore into a scratch directory ([`RECOVERY.md`](RECOVERY.md)).

**After any restore of the primary**
- Expect every replica to resync once. The primary's log position moved
  backwards; that is correct, and it happens once.