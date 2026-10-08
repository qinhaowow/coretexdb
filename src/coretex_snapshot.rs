//! C4 — snapshots and log compaction: consistent point-in-time images, and
//! a way to shrink an ever-growing write-ahead log.
//!
//! `coretex_backup` copies files under `data/`, which is the right tool for a
//! stopped database and the wrong one for a running one: copying live files
//! can catch a collection half-written. A snapshot here is taken through the
//! same door replication uses — [`DataManager::replication_snapshot`] — whose
//! read order (log position → schemas → records, under the locks the write
//! path takes) makes the image consistent by construction. Serialisation and
//! disk I/O happen after the locks are released, so a running database is
//! never blocked for the length of a write.
//!
//! File format, deliberately close to the WAL's own conventions so one
//! checksum routine serves both:
//!
//! ```text
//! "CTSNAP01"  magic, 8 bytes
//! u32         payload length, little endian
//! u32         CRC32 of the payload
//! payload     bincode-serialised ReplicationSnapshot
//! ```
//!
//! Writes are atomic (temp file + rename), so a snapshot is either a
//! complete image or absent — never half a file. Reads verify magic,
//! length and checksum before decoding: a corrupt snapshot is refused with a
//! message that says which check failed, instead of producing a plausible
//! half-dataset.
//!
//! Compaction ([`compact_wal`]) is the other half: it folds the log down to
//! each key's final state and writes a *new* log directory. It never rewrites
//! or truncates the live log — deciding when to retire it is an operator's
//! call, not something a background task should do mid-incident.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::coretex_core::{CoreTexError, Result};
use crate::coretex_replication::ReplicationSnapshot;
use crate::coretex_utils::wal::{crc32, WalEntryType, WriteAheadLog};
use crate::CoreTexDB;

/// Container magic; the trailing digit is the format version.
const MAGIC: &[u8; 8] = b"CTSNAP01";
const MAGIC_LEN: usize = 8;
const HEADER_LEN: usize = MAGIC_LEN + 4 + 4;

/// What a saved snapshot contains, recorded next to the file so listing and
/// restore decisions do not require decoding it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotMeta {
    /// File name inside the store directory.
    pub name: String,
    /// Source log position the image is consistent with (0 = no WAL).
    pub lsn: u64,
    /// Collections in the image.
    pub collections: usize,
    /// Rows in the image.
    pub records: usize,
    /// Size of the file on disk.
    pub bytes: u64,
    /// Unix seconds when it was taken.
    pub created_at: i64,
}

/// The serialised form actually written to disk.
///
/// bincode cannot *deserialize* `serde_json::Value` (it would need
/// `deserialize_any`), and metadata is exactly that — so metadata and schemas
/// travel as their JSON text, parsed back on load, while the vectors stay
/// packed binary. That keeps snapshots small without giving up exactness:
/// JSON round-trips numbers and strings unchanged.
#[derive(Serialize, Deserialize)]
struct SnapshotPayload {
    lsn: u64,
    /// Collection name → schema as JSON text.
    schemas: HashMap<String, String>,
    /// `collection → id → (vector, metadata as JSON text)`.
    records: HashMap<String, HashMap<String, (Vec<f32>, String)>>,
}

fn to_payload(snapshot: &ReplicationSnapshot) -> Result<SnapshotPayload> {
    let mut schemas = HashMap::new();
    for schema in &snapshot.collections {
        schemas.insert(schema.name.clone(), serde_json::to_string(schema)?);
    }
    let mut records = HashMap::new();
    for (collection, rows) in &snapshot.records {
        let mut encoded = HashMap::new();
        for (id, record) in rows {
            encoded.insert(
                id.clone(),
                (record.vector.clone(), serde_json::to_string(&record.metadata)?),
            );
        }
        records.insert(collection.clone(), encoded);
    }
    Ok(SnapshotPayload {
        lsn: snapshot.lsn,
        schemas,
        records,
    })
}

fn from_payload(payload: SnapshotPayload) -> Result<ReplicationSnapshot> {
    let mut collections = Vec::new();
    for json in payload.schemas.values() {
        collections.push(serde_json::from_str(json)?);
    }
    let mut records = HashMap::new();
    for (collection, rows) in payload.records {
        let mut decoded = HashMap::new();
        for (id, (vector, metadata)) in rows {
            decoded.insert(
                id,
                crate::coretex_data::VectorRecord {
                    vector,
                    metadata: serde_json::from_str(&metadata)?,
                },
            );
        }
        records.insert(collection, decoded);
    }
    Ok(ReplicationSnapshot {
        lsn: payload.lsn,
        collections,
        records,
    })
}

/// Encode a snapshot into the on-disk container.
pub fn encode(snapshot: &ReplicationSnapshot) -> Result<Vec<u8>> {
    let payload = bincode::serialize(&to_payload(snapshot)?).map_err(CoreTexError::Bincode)?;
    let mut out = Vec::with_capacity(HEADER_LEN + payload.len());
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&crc32(&payload).to_le_bytes());
    out.extend_from_slice(&payload);
    Ok(out)
}

/// Decode a snapshot container, verifying every header field before touching
/// the payload.
pub fn decode(bytes: &[u8]) -> Result<ReplicationSnapshot> {
    if bytes.len() < HEADER_LEN {
        return Err(CoreTexError::Other(format!(
            "snapshot truncated: {} bytes, header alone needs {HEADER_LEN}",
            bytes.len()
        )));
    }
    if &bytes[..MAGIC_LEN] != MAGIC {
        return Err(CoreTexError::Other(format!(
            "not a snapshot file: magic {:?}",
            String::from_utf8_lossy(&bytes[..MAGIC_LEN])
        )));
    }
    let len = u32::from_le_bytes(bytes[MAGIC_LEN..MAGIC_LEN + 4].try_into().unwrap()) as usize;
    let expected = u32::from_le_bytes(bytes[MAGIC_LEN + 4..HEADER_LEN].try_into().unwrap());
    let payload = &bytes[HEADER_LEN..];
    if payload.len() != len {
        return Err(CoreTexError::Other(format!(
            "snapshot length mismatch: header says {len}, file carries {}",
            payload.len()
        )));
    }
    let actual = crc32(payload);
    if actual != expected {
        return Err(CoreTexError::Other(format!(
            "snapshot checksum mismatch: header {expected:#010x}, payload {actual:#010x}"
        )));
    }
    bincode::deserialize(payload)
        .map_err(CoreTexError::Bincode)
        .and_then(|payload: SnapshotPayload| from_payload(payload))
}

/// A directory of snapshots.
#[derive(Debug, Clone)]
pub struct SnapshotArchive {
    dir: PathBuf,
}

impl SnapshotArchive {
    /// Use (and create) `dir` for snapshot files.
    pub async fn open(dir: impl Into<PathBuf>) -> Result<Self> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir)?;
        Ok(Self { dir })
    }

    /// Directory in use.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    fn path_for(&self, name: &str) -> PathBuf {
        self.dir.join(format!("{name}.snapshot"))
    }

    /// Take a snapshot of `db` and store it as `name`, replacing any
    /// previous file of that name. Returns what the image contains.
    pub async fn save(&self, db: &CoreTexDB, name: &str) -> Result<SnapshotMeta> {
        // Consistent by construction: schemas and rows copied under the locks
        // the write path takes, position read first.
        let snapshot = db.data_manager.replication_snapshot().await;
        let bytes = encode(&snapshot)?;

        // Atomic: temp file then rename, so a reader (or a crash) never sees
        // a partial image.
        let final_path = self.path_for(name);
        let tmp = final_path.with_extension("snapshot.tmp");
        std::fs::write(&tmp, &bytes)?;
        std::fs::rename(&tmp, &final_path)?;

        Ok(SnapshotMeta {
            name: name.to_string(),
            lsn: snapshot.lsn,
            collections: snapshot.collections.len(),
            records: snapshot.records.values().map(|c| c.len()).sum(),
            bytes: bytes.len() as u64,
            created_at: now_secs(),
        })
    }

    /// Take a snapshot under a timestamp-derived name.
    pub async fn save_auto(&self, db: &CoreTexDB) -> Result<SnapshotMeta> {
        let name = format!("snapshot-{}", now_secs());
        self.save(db, &name).await
    }

    /// Read one snapshot back.
    pub async fn load(&self, name: &str) -> Result<ReplicationSnapshot> {
        let path = self.path_for(name);
        let bytes = std::fs::read(&path).map_err(CoreTexError::Io)?;
        decode(&bytes)
    }

    /// Restore a stored snapshot into `db`, replacing everything it holds.
    ///
    /// The image goes through the replication apply path and the manifest is
    /// persisted, so the restored database is one a restart can read back —
    /// which is the point of rehearsing recovery.
    pub async fn restore_into(&self, db: &CoreTexDB, name: &str) -> Result<usize> {
        let snapshot = self.load(name).await?;
        let restored = db.data_manager.apply_replication_snapshot(&snapshot).await?;
        db.persist_manifest().await?;
        Ok(restored)
    }

    /// Stored snapshots, newest name first.
    pub fn list(&self) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(&self.dir)
            .map(|entries| {
                entries
                    .flatten()
                    .filter_map(|e| e.file_name().to_str().map(str::to_string))
                    .filter(|n| n.ends_with(".snapshot"))
                    .map(|n| n.trim_end_matches(".snapshot").to_string())
                    .collect()
            })
            .unwrap_or_default();
        // Timestamp-derived names sort lexicographically by time.
        names.sort_by(|a, b| b.cmp(a));
        names
    }

    /// The newest stored snapshot, if any.
    pub fn latest(&self) -> Option<String> {
        self.list().into_iter().next()
    }

    /// Delete a stored snapshot.
    pub fn delete(&self, name: &str) -> Result<bool> {
        let path = self.path_for(name);
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(CoreTexError::Io(e)),
        }
    }

    /// Keep only the `keep` newest snapshots; returns the names removed.
    pub fn prune(&self, keep: usize) -> Result<Vec<String>> {
        let names = self.list();
        let mut removed = Vec::new();
        for name in names.into_iter().skip(keep) {
            if self.delete(&name)? {
                removed.push(name);
            }
        }
        Ok(removed)
    }
}

/// Periodic background snapshots ("BGSAVE"): the interval loop, plus the
/// store it writes into.
pub struct BackgroundSnapshotter {
    db: Arc<CoreTexDB>,
    store: SnapshotArchive,
    interval: Duration,
    keep: usize,
    stop: tokio::sync::watch::Sender<bool>,
}

impl BackgroundSnapshotter {
    /// Start a background loop taking a snapshot every `interval`, keeping
    /// the `keep` newest files. Errors are logged, never fatal — a snapshot
    /// that cannot be taken must not take the database with it.
    pub fn spawn(
        db: Arc<CoreTexDB>,
        store: SnapshotArchive,
        interval: Duration,
        keep: usize,
    ) -> Self {
        let (stop, mut stop_rx) = tokio::sync::watch::channel(false);
        let loop_store = store.clone();
        let loop_db = db.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(interval) => {
                        match loop_store.save_auto(&loop_db).await {
                            Ok(meta) => {
                                if let Err(e) = loop_store.prune(keep) {
                                    log::warn!("snapshot prune failed: {e}");
                                }
                                log::info!(
                                    "background snapshot {} ({} collections, {} records, {} bytes)",
                                    meta.name, meta.collections, meta.records, meta.bytes
                                );
                            }
                            Err(e) => log::warn!("background snapshot failed: {e}"),
                        }
                    }
                    changed = stop_rx.changed() => {
                        if changed.is_err() || *stop_rx.borrow() {
                            break;
                        }
                    }
                }
            }
        });
        Self {
            db,
            store,
            interval,
            keep,
            stop,
        }
    }

    /// Take one right now instead of waiting for the next tick.
    pub async fn save_now(&self) -> Result<SnapshotMeta> {
        let meta = self.store.save_auto(&self.db).await?;
        self.store.prune(self.keep)?;
        Ok(meta)
    }

    /// The store this loop writes into.
    pub fn store(&self) -> &SnapshotArchive {
        &self.store
    }

    /// Configured interval, for callers that want to report it.
    pub fn interval(&self) -> Duration {
        self.interval
    }

    /// Stop the loop after the current tick.
    pub fn stop(&self) {
        let _ = self.stop.send(true);
    }
}

/// What a compaction produced.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactionReport {
    /// Entries in the live log.
    pub entries_before: usize,
    /// Entries written to the compacted log.
    pub entries_after: usize,
    /// Entries dropped as superseded or bookkeeping-only.
    pub entries_dropped: usize,
    /// Bytes written.
    pub bytes: u64,
    /// Target directory the compacted log lives in.
    pub dir: PathBuf,
}

/// Fold a write-ahead log down to each key's final state, into a new log
/// directory (`target_dir`, created fresh).
///
/// Superseded row entries collapse to the last write per storage key;
/// schema and transaction bookkeeping entries are carried through in
/// sequence order, checkpoints are dropped. The live log is left untouched —
/// the caller decides when to retire it.
pub async fn compact_wal(db: &CoreTexDB, target_dir: &Path) -> Result<CompactionReport> {
    let (entries, truncated) = db.data_manager.read_replication_entries(0).await?;
    if truncated {
        // Gaps mean history is already gone: folding what survives would
        // quietly bake the loss into the compacted log.
        return Err(CoreTexError::Other(
            "cannot compact: the live log has gaps (older segments discarded or the log was reset); \
             restore from a snapshot or from a repaired directory instead"
                .to_string(),
        ));
    }
    let entries_before = entries.len();

    // Last write wins per storage key; everything that is not a row write is
    // kept in order.
    let mut final_row: HashMap<String, usize> = HashMap::new();
    let mut keep = vec![false; entries.len()];
    for (index, entry) in entries.iter().enumerate() {
        match entry.entry_type {
            WalEntryType::Insert | WalEntryType::Update | WalEntryType::Delete => {
                let key = format!("{}:{}", entry.collection, entry.key);
                final_row.insert(key, index);
            }
            WalEntryType::Checkpoint => {}
            _ => keep[index] = true,
        }
    }
    for index in final_row.values() {
        keep[*index] = true;
    }

    std::fs::create_dir_all(target_dir)?;
    let wal = WriteAheadLog::new(&target_dir.to_string_lossy());
    wal.init().await.map_err(CoreTexError::Io)?;
    for (index, entry) in entries.iter().enumerate() {
        if !keep[index] {
            continue;
        }
        let mut copy = entry.clone();
        wal.append(&mut copy).await.map_err(CoreTexError::Io)?;
    }

    let bytes = wal.stats().await.total_bytes;
    Ok(CompactionReport {
        entries_before,
        entries_after: keep.iter().filter(|k| **k).count(),
        entries_dropped: entries_before - keep.iter().filter(|k| **k).count(),
        bytes,
        dir: target_dir.to_path_buf(),
    })
}

fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> ReplicationSnapshot {
        let mut records = std::collections::HashMap::new();
        let mut rows = std::collections::HashMap::new();
        rows.insert(
            "a".to_string(),
            crate::coretex_data::VectorRecord {
                vector: vec![0.5, 1.5],
                metadata: serde_json::json!({"k": "v"}),
            },
        );
        records.insert("docs".to_string(), rows);
        ReplicationSnapshot {
            lsn: 42,
            collections: Vec::new(),
            records,
        }
    }

    #[test]
    fn round_trip_preserves_content() {
        let original = sample();
        let decoded = decode(&encode(&original).unwrap()).unwrap();
        assert_eq!(decoded.lsn, 42);
        assert_eq!(decoded.records["docs"]["a"].vector, vec![0.5, 1.5]);
        assert_eq!(
            decoded.records["docs"]["a"].metadata,
            serde_json::json!({"k": "v"})
        );
    }

    #[test]
    fn corruption_and_truncation_are_refused_with_a_reason() {
        let bytes = encode(&sample()).unwrap();

        // A flipped payload byte must fail the checksum, not decode into
        // something plausible.
        let mut corrupt = bytes.clone();
        let last = corrupt.len() - 1;
        corrupt[last] ^= 0xff;
        let err = decode(&corrupt).unwrap_err().to_string();
        assert!(err.contains("checksum mismatch"), "got: {err}");

        // Truncation is caught by the length field, before any decode.
        let err = decode(&bytes[..bytes.len() - 3]).unwrap_err().to_string();
        assert!(err.contains("length mismatch"), "got: {err}");

        let err = decode(&bytes[..4]).unwrap_err().to_string();
        assert!(err.contains("truncated"), "got: {err}");

        // A file that is not a snapshot at all.
        let err = decode(b"just some bytes, not a snapshot file").unwrap_err().to_string();
        assert!(err.contains("not a snapshot file"), "got: {err}");
    }
}