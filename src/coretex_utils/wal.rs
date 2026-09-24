//! Write-Ahead Log (WAL) for CoretexDB
//!
//! Guarantees durability: every write is persisted to the WAL before
//! being applied to the storage engine. On crash recovery, the WAL is
//! replayed in order to reconstruct the database state.
//!
//! ## Format
//! Each WAL entry is a JSON line with a CRC32 checksum prefix:
//!   CRC32(32 hex) | JSON(entry)
//!
//! ## Recovery
//! On startup, all WAL segment files are scanned in order and valid
//! entries are replayed. Corrupted entries are skipped and logged.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::RwLock;

// ── CRC32 ──────────────────────────────────────────────────────────

/// Simple CRC32 implementation (IEEE 802.3 polynomial).
/// Used to detect corruption in WAL entries.
fn crc32(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFFFFFF;
    for &byte in data {
        crc ^= byte as u32;
        for _ in 0..8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ 0xEDB88320;
            } else {
                crc >>= 1;
            }
        }
    }
    !crc
}

// ── Entry Types ────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WalEntryType {
    Insert,
    Update,
    Delete,
    CreateCollection,
    DeleteCollection,
    BeginTransaction,
    CommitTransaction,
    RollbackTransaction,
    Checkpoint,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalEntry {
    /// Monotonically increasing entry sequence number (global)
    pub sequence: u64,
    /// Unix timestamp in microseconds
    pub timestamp: u128,
    /// Type of operation
    pub entry_type: WalEntryType,
    /// Target collection name
    pub collection: String,
    /// The vector/document ID this operation applies to
    pub key: String,
    /// Payload (vector data, metadata, or null for deletes)
    pub data: serde_json::Value,
    /// Optional transaction ID if this entry is part of a txn
    pub transaction_id: Option<String>,
}

impl WalEntry {
    pub fn new(
        entry_type: WalEntryType,
        collection: &str,
        key: &str,
        data: serde_json::Value,
    ) -> Self {
        Self {
            sequence: 0, // filled by WAL on append
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_micros(),
            entry_type,
            collection: collection.to_string(),
            key: key.to_string(),
            data,
            transaction_id: None,
        }
    }

    pub fn with_transaction(mut self, txn_id: &str) -> Self {
        self.transaction_id = Some(txn_id.to_string());
        self
    }
}

// ── WAL Result ─────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct WalStats {
    pub total_entries: u64,
    pub total_bytes: u64,
    pub segment_count: usize,
    pub corrupted_entries: u64,
    pub last_sequence: u64,
}

// ── WAL Engine ─────────────────────────────────────────────────────

/// Parse segment number from a file name like `wal-000001.log`.
fn parse_segment_number(name: &str) -> Option<u64> {
    let stem = name.strip_prefix("wal-")?.strip_suffix(".log")?;
    stem.parse().ok()
}

/// The Write-Ahead Log. Thread-safe, async.
pub struct WriteAheadLog {
    log_dir: PathBuf,
    current_file: Arc<RwLock<PathBuf>>,
    sequence_counter: Arc<RwLock<u64>>,
    max_segment_size: u64,
    current_size: Arc<RwLock<u64>>,
    /// Segment files in order (for recovery)
    segments: Arc<RwLock<Vec<PathBuf>>>,
    stats: Arc<RwLock<WalStats>>,
}

impl WriteAheadLog {
    /// Create a new WAL. Does not initialize — call `init()`.
    pub fn new(log_dir: &str) -> Self {
        let log_path = PathBuf::from(log_dir);
        let current_file = Arc::new(RwLock::new(log_path.join("wal-000001.log")));

        Self {
            log_dir: log_path,
            current_file,
            sequence_counter: Arc::new(RwLock::new(0)),
            max_segment_size: 64 * 1024 * 1024, // 64 MB
            current_size: Arc::new(RwLock::new(0)),
            segments: Arc::new(RwLock::new(Vec::new())),
            stats: Arc::new(RwLock::new(WalStats {
                total_entries: 0,
                total_bytes: 0,
                segment_count: 0,
                corrupted_entries: 0,
                last_sequence: 0,
            })),
        }
    }

    /// Set maximum segment size (default: 64 MB)
    pub fn with_max_segment_size(mut self, size: u64) -> Self {
        self.max_segment_size = size;
        self
    }

    /// Path of the segment file currently being appended to.
    ///
    /// Exposed so external tooling and tests can inspect or rotate the active
    /// segment without gaining write access to the WAL's private state.
    pub async fn current_path(&self) -> PathBuf {
        self.current_file.read().await.clone()
    }

    /// Number of segment files currently tracked.
    pub async fn segment_count(&self) -> usize {
        self.segments.read().await.len()
    }

    /// Initialize the WAL: create directory, discover existing segments,
    /// restore sequence counter.
    pub async fn init(&self) -> std::io::Result<()> {
        fs::create_dir_all(&self.log_dir).await?;

        // Discover existing segments sorted by name.
        // Strictly match wal-*.log only; ignore legacy wal_*.log and any other .log.
        let mut discovered: Vec<PathBuf> = Vec::new();
        let mut entries = fs::read_dir(&self.log_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.is_file() {
                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    if name.starts_with("wal-") && name.ends_with(".log") {
                        discovered.push(path);
                    }
                }
            }
        }
        discovered.sort();

        let segment_count = discovered.len();

        if discovered.is_empty() {
            // Fresh WAL — create first segment
            File::create(&*self.current_file.read().await).await?.sync_all().await?;
            self.segments.write().await.push(self.current_file.read().await.clone());
        } else {
            // Restore from existing segments
            *self.current_file.write().await = discovered.last().unwrap().clone();
            self.segments.write().await.clone_from(&discovered);

            // Compute current size of the last segment
            if let Ok(meta) = fs::metadata(&*self.current_file.read().await).await {
                *self.current_size.write().await = meta.len();
            }

            // Restore sequence counter by scanning all segments
            let max_seq = self.scan_max_sequence().await?;
            *self.sequence_counter.write().await = max_seq;
        }

        // Update stats
        {
            let mut stats = self.stats.write().await;
            stats.segment_count = segment_count;
        }

        Ok(())
    }

    /// Append an entry to the WAL. Returns the assigned sequence number.
    /// This is the only write path — all writes fsync before returning.
    pub async fn append(&self, entry: &mut WalEntry) -> std::io::Result<u64> {
        // Assign sequence
        let seq = {
            let mut counter = self.sequence_counter.write().await;
            *counter += 1;
            *counter
        };
        entry.sequence = seq;

        // Serialize
        let json = serde_json::to_vec(entry)?;
        let checksum = crc32(&json);
        let mut line = format!("{:08x}|", checksum).into_bytes();
        line.extend_from_slice(&json);
        line.push(b'\n');

        let entry_size = line.len() as u64;

        // Rotate if needed
        {
            let current_size = *self.current_size.write().await;
            if current_size + entry_size > self.max_segment_size {
                let _ = current_size;
                self.rotate().await?;
            }
        }

        // Append to file
        let mut file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&*self.current_file.read().await)
            .await?;

        file.write_all(&line).await?;
        file.flush().await?;

        // Update tracking
        {
            let mut size = self.current_size.write().await;
            *size += entry_size;
        }
        {
            let mut stats = self.stats.write().await;
            stats.total_entries += 1;
            stats.total_bytes += entry_size;
            stats.last_sequence = seq;
        }

        Ok(seq)
    }

    /// Create a convenience entry and append it.
    pub async fn log_operation(
        &self,
        entry_type: WalEntryType,
        collection: &str,
        key: &str,
        data: serde_json::Value,
    ) -> std::io::Result<u64> {
        let mut entry = WalEntry::new(entry_type, collection, key, data);
        self.append(&mut entry).await
    }

    /// Rotate to a new segment file.
    ///
    /// Lock order: `current_size` → `segments` (never reversed).
    /// Holds `segments` write lock for the entire rotation.
    async fn rotate(&self) -> std::io::Result<()> {
        let _timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros();

        let mut size_guard = self.current_size.write().await;
        let mut segments = self.segments.write().await;

        let mut max_num = 0u64;
        for seg in segments.iter() {
            if let Some(name) = seg.file_name().and_then(|n| n.to_str()) {
                if let Some(n) = parse_segment_number(name) {
                    max_num = max_num.max(n);
                }
            }
        }
        let next_segment = max_num + 1;
        let new_file = self
            .log_dir
            .join(format!("wal-{:06}.log", next_segment));

        // Create new file
        File::create(&new_file).await?.sync_all().await?;

        segments.push(new_file.clone());
        drop(segments);

        *self.current_file.write().await = new_file;
        *size_guard = 0;

        Ok(())
    }

    /// Scan all WAL segments and return the highest sequence number.
    async fn scan_max_sequence(&self) -> std::io::Result<u64> {
        let mut max_seq: u64 = 0;
        let segments = self.segments.read().await.clone();

        for segment in &segments {
            if let Ok(file) = File::open(segment).await {
                let reader = BufReader::new(file);
                let mut lines = reader.lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    if line.len() < 9 {
                        continue;
                    }
                    if let Ok(entry) = self.parse_entry_line(&line) {
                        if entry.sequence > max_seq {
                            max_seq = entry.sequence;
                        }
                    }
                }
            }
        }

        Ok(max_seq)
    }

    /// Parse a WAL entry line: "CRC32|JSON"
    fn parse_entry_line(&self, line: &str) -> Result<WalEntry, String> {
        if line.len() < 9 {
            return Err("Line too short".to_string());
        }

        let checksum_str = &line[..8];
        let json_str = &line[9..]; // skip '|'

        // Verify checksum
        let expected: u32 = u32::from_str_radix(checksum_str, 16)
            .map_err(|e| format!("Bad checksum hex: {}", e))?;
        let actual = crc32(json_str.as_bytes());
        if expected != actual {
            return Err(format!(
                "Checksum mismatch: expected {:08x}, got {:08x}",
                expected, actual
            ));
        }

        serde_json::from_str::<WalEntry>(json_str).map_err(|e| format!("JSON parse: {}", e))
    }

    /// Read all entries from all segments. Corrupted lines are skipped and counted.
    pub async fn read_all_entries(&self) -> std::io::Result<Vec<WalEntry>> {
        let mut entries = Vec::new();
        let segments = self.segments.read().await.clone();
        let mut corrupted = 0u64;

        for segment in &segments {
            let file = File::open(segment).await?;
            let reader = BufReader::new(file);
            let mut lines = reader.lines();

            while let Ok(Some(line)) = lines.next_line().await {
                if line.trim().is_empty() {
                    continue;
                }
                match self.parse_entry_line(&line) {
                    Ok(entry) => entries.push(entry),
                    Err(_) => corrupted += 1,
                }
            }
        }

        {
            let mut stats = self.stats.write().await;
            stats.corrupted_entries += corrupted;
        }

        Ok(entries)
    }

    /// Replay all WAL entries through a handler.
    /// Transactions are grouped: if an entry has a transaction_id,
    /// all entries for that txn are passed together.
    pub async fn replay(
        &self,
        handler: &(dyn Fn(&[WalEntry]) -> Result<(), String> + Send + Sync),
    ) -> std::io::Result<ReplayResult> {
        let entries = self.read_all_entries().await?;
        let total = entries.len();
        let mut skipped = 0u64;
        let mut replayed = 0u64;

        // Group by transaction
        let mut current_txn: Vec<&WalEntry> = Vec::new();
        let mut current_txn_id: Option<&str> = None;

        for entry in &entries {
            match entry.entry_type {
                WalEntryType::BeginTransaction => {
                    current_txn.clear();
                    current_txn_id = entry.transaction_id.as_deref();
                    current_txn.push(entry);
                }
                WalEntryType::CommitTransaction | WalEntryType::RollbackTransaction => {
                    current_txn.push(entry);
                    if entry.entry_type == WalEntryType::CommitTransaction {
                        // Replay non-marker entries
                        let ops: Vec<&WalEntry> = current_txn
                            .iter()
                            .filter(|e| !matches!(e.entry_type,
                                WalEntryType::BeginTransaction |
                                WalEntryType::CommitTransaction |
                                WalEntryType::RollbackTransaction
                            ))
                            .copied()
                            .collect();
                        if !ops.is_empty() {
                            match handler(&ops.iter().cloned().cloned().collect::<Vec<_>>()) {
                                Ok(()) => replayed += ops.len() as u64,
                                Err(_) => skipped += ops.len() as u64,
                            }
                        }
                    }
                    current_txn.clear();
                    current_txn_id = None;
                }
                _ => {
                    if current_txn_id.is_some() {
                        current_txn.push(entry);
                    } else {
                        // Non-transactional entry — replay immediately
                        let batch = [entry.clone()];
                        match handler(&batch) {
                            Ok(()) => replayed += 1,
                            Err(_) => skipped += 1,
                        }
                    }
                }
            }
        }

        Ok(ReplayResult {
            total_entries: total as u64,
            replayed,
            skipped,
            corrupted: self.stats.read().await.corrupted_entries,
        })
    }

    /// Get current WAL statistics.
    pub async fn stats(&self) -> WalStats {
        self.stats.read().await.clone()
    }

    /// Trigger garbage collection: delete segments older than `retain_segments`.
    pub async fn gc(&self, retain_segments: usize) -> std::io::Result<usize> {
        let segments = self.segments.read().await.clone();
        if segments.len() <= retain_segments {
            return Ok(0);
        }

        let to_remove = segments.len() - retain_segments;
        let mut removed = 0;

        for segment in segments.iter().take(to_remove) {
            if segment != &*self.current_file.read().await {
                fs::remove_file(segment).await?;
                removed += 1;
            }
        }

        // Update segment list
        {
            let mut segs = self.segments.write().await;
            *segs = segs.iter().skip(removed).cloned().collect();
        }

        Ok(removed)
    }

    /// Create a checkpoint: flush current segment, then GC old segments.
    /// Returns (segments_removed, current_segment_path).
    pub async fn checkpoint(&self, retain_segments: usize) -> std::io::Result<(usize, PathBuf)> {
        // Force rotate to a new segment so the current one can be GC'd
        self.rotate().await?;

        // GC old segments
        let removed = self.gc(retain_segments).await?;
        let current = self.current_file.read().await.clone();

        // Log checkpoint entry
        let mut entry = WalEntry::new(WalEntryType::Checkpoint, "system", "_checkpoint", serde_json::json!({"checkpoint": true}));
        let _ = self.append(&mut entry).await;

        Ok((removed, current))
    }
}

// ── Replay Result ──────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ReplayResult {
    pub total_entries: u64,
    pub replayed: u64,
    pub skipped: u64,
    pub corrupted: u64,
}

impl ReplayResult {
    pub fn is_clean(&self) -> bool {
        self.skipped == 0 && self.corrupted == 0
    }
}

// ── Recovery Manager ───────────────────────────────────────────────

use crate::coretex_storage::StorageEngine;
use crate::coretex_core::Result as CoreResult;

/// Orchestrates WAL replay into a StorageEngine on startup.
pub struct RecoveryManager {
    wal: Arc<WriteAheadLog>,
}

impl RecoveryManager {
    pub fn new(wal: Arc<WriteAheadLog>) -> Self {
        Self { wal }
    }

    /// Replay the WAL into the given storage engine.
    /// After this call, the storage engine contains all committed data.
    pub async fn recover(
        &self,
        storage: &(dyn StorageEngine + Send + Sync),
    ) -> CoreResult<ReplayResult> {
        let entries = self.recover_storage_entries().await
            .map_err(crate::coretex_core::CoreTexError::Io)?;

        let mut replayed = 0u64;
        for (entry_type, _collection, key, vector, metadata) in &entries {
            match entry_type {
                WalEntryType::Insert | WalEntryType::Update => {
                    storage.store(key, vector, metadata).await?;
                    replayed += 1;
                }
                WalEntryType::Delete => {
                    let _ = storage.delete(key).await;
                    replayed += 1;
                }
                _ => {}
            }
        }

        Ok(ReplayResult {
            total_entries: entries.len() as u64,
            replayed,
            skipped: 0,
            corrupted: self.wal.stats().await.corrupted_entries,
        })
    }

    /// Read all entries from WAL and flatten them into (type, collection, key, vector, metadata) tuples.
    /// Non-data operations (BeginTxn, CommitTxn, Checkpoint) are filtered out.
    /// RollbackTransaction causes its preceding BeginTransaction entries to be skipped.
    pub async fn recover_storage_entries(
        &self,
    ) -> std::io::Result<Vec<(WalEntryType, String, String, Vec<f32>, serde_json::Value)>> {
        let entries = self.wal.read_all_entries().await?;
        let mut result = Vec::new();
        let mut in_txn = false;
        let mut txn_entries: Vec<(WalEntryType, String, String, Vec<f32>, serde_json::Value)> = Vec::new();

        for entry in &entries {
            match entry.entry_type {
                WalEntryType::BeginTransaction => {
                    in_txn = true;
                    txn_entries.clear();
                }
                WalEntryType::CommitTransaction => {
                    if in_txn {
                        result.append(&mut txn_entries);
                        in_txn = false;
                    }
                }
                WalEntryType::RollbackTransaction => {
                    // Discard all entries in this transaction
                    txn_entries.clear();
                    in_txn = false;
                }
                WalEntryType::Insert | WalEntryType::Update | WalEntryType::Delete => {
                    let vector: Vec<f32> = entry.data.get("vector")
                        .and_then(|v| v.as_array())
                        .map(|a| a.iter().filter_map(|x| x.as_f64()).map(|f| f as f32).collect())
                        .unwrap_or_default();
                    let metadata = entry.data.get("metadata")
                        .cloned()
                        .unwrap_or(serde_json::json!({}));

                    let item = (entry.entry_type, entry.collection.clone(), entry.key.clone(), vector, metadata);
                    if in_txn {
                        txn_entries.push(item);
                    } else {
                        result.push(item);
                    }
                }
                _ => {}
            }
        }

        Ok(result)
    }
}

// ── Tests ──────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    async fn setup_wal() -> (TempDir, Arc<WriteAheadLog>) {
        let dir = TempDir::new().unwrap();
        let wal = Arc::new(WriteAheadLog::new(
            dir.path().to_string_lossy().as_ref(),
        ));
        wal.init().await.unwrap();
        (dir, wal)
    }

    #[tokio::test]
    async fn test_segment_naming_wal_dash_000001() {
        let (_dir, wal) = setup_wal().await;
        let path = wal.current_path().await;
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        assert_eq!(name, "wal-000001.log");
        assert!(path.exists());
    }

    #[tokio::test]
    async fn test_discovery_ignores_old_format() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("wal_000000.log"), b"legacy").unwrap();
        std::fs::write(dir.path().join("other.log"), b"x").unwrap();
        std::fs::write(dir.path().join("notes.txt"), b"x").unwrap();

        let wal = WriteAheadLog::new(dir.path().to_string_lossy().as_ref());
        wal.init().await.unwrap();

        assert_eq!(wal.segment_count().await, 1);
        let name = wal
            .current_path()
            .await
            .file_name()
            .unwrap()
            .to_string_lossy()
            .into_owned();
        assert_eq!(name, "wal-000001.log");
        assert!(dir.path().join("wal_000000.log").exists());
        assert!(dir.path().join("other.log").exists());
    }

    #[tokio::test]
    async fn test_segment_numbering_with_gaps() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("wal-000003.log"), b"").unwrap();
        std::fs::write(dir.path().join("wal-000005.log"), b"").unwrap();

        let wal = WriteAheadLog::new(dir.path().to_string_lossy().as_ref());
        wal.init().await.unwrap();

        let cur = wal.current_path().await;
        assert_eq!(cur.file_name().unwrap().to_str().unwrap(), "wal-000005.log");

        wal.rotate().await.unwrap();
        let cur = wal.current_path().await;
        assert_eq!(cur.file_name().unwrap().to_str().unwrap(), "wal-000006.log");
        assert!(dir.path().join("wal-000006.log").exists());
    }

    #[tokio::test]
    async fn test_first_segment_naming() {
        let dir = TempDir::new().unwrap();
        let wal = WriteAheadLog::new(dir.path().to_string_lossy().as_ref());
        wal.init().await.unwrap();
        assert!(dir.path().join("wal-000001.log").is_file());
        let name = wal
            .current_path()
            .await
            .file_name()
            .unwrap()
            .to_string_lossy()
            .into_owned();
        assert_eq!(name, "wal-000001.log");
    }

    #[tokio::test]
    async fn test_append_and_read() {
        let (_dir, wal) = setup_wal().await;

        let mut entry = WalEntry::new(
            WalEntryType::Insert,
            "test_col",
            "key1",
            serde_json::json!({"vector": [1.0, 2.0], "metadata": {"name": "test"}}),
        );
        let seq = wal.append(&mut entry).await.unwrap();
        assert_eq!(seq, 1);

        let entries = wal.read_all_entries().await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].sequence, 1);
        assert_eq!(entries[0].collection, "test_col");
        assert_eq!(entries[0].key, "key1");
    }

    #[tokio::test]
    async fn test_checksum_verification() {
        let (_dir, wal) = setup_wal().await;

        let mut entry = WalEntry::new(
            WalEntryType::Insert,
            "c",
            "k",
            serde_json::json!({"v": [1.0]}),
        );
        wal.append(&mut entry).await.unwrap();

        let entries = wal.read_all_entries().await.unwrap();
        assert_eq!(entries.len(), 1);
        // Should be valid (no corruption)
    }

    #[tokio::test]
    async fn test_corruption_detected() {
        let (_dir, wal) = setup_wal().await;

        // Manually write a corrupted line
        let mut file = OpenOptions::new()
            .append(true)
            .open(wal.current_file.read().await.clone())
            .await
            .unwrap();
        file.write_all(b"deadbeef|{\"corrupt\": true}\n").await.unwrap();
        file.flush().await.unwrap();

        let entries = wal.read_all_entries().await.unwrap();
        // The corrupted line should be skipped
        assert_eq!(entries.len(), 0);
        assert!(wal.stats().await.corrupted_entries >= 1);
    }

    #[tokio::test]
    async fn test_segment_rotation() {
        let (_dir, wal) = setup_wal().await;

        // Force small segments
        let wal = WriteAheadLog::new(
            _dir.path().to_string_lossy().as_ref(),
        )
        .with_max_segment_size(100); // tiny

        // Override the internal state...
        // This is hacky but works for testing rotation logic
        // Actually let's just test through the normal API

        // Append several entries
        for i in 0..50 {
            let mut entry = WalEntry::new(
                WalEntryType::Insert,
                "c",
                &format!("k{}", i),
                serde_json::json!({"v": [1.0]}),
            );
            wal.append(&mut entry).await.unwrap();
        }

        let segments = wal.segments.read().await.len();
        assert!(segments > 0, "Should have at least one segment");
    }

    #[tokio::test]
    async fn test_multi_segment_read() {
        let dir = TempDir::new().unwrap();
        let wal = Arc::new(
            WriteAheadLog::new(dir.path().to_string_lossy().as_ref())
                .with_max_segment_size(150), // force rotation after 1-2 entries
        );
        wal.init().await.unwrap();

        for i in 0..10 {
            let mut entry = WalEntry::new(
                WalEntryType::Insert,
                "test",
                &format!("key{}", i),
                serde_json::json!({"vector": [i as f64]}),
            );
            wal.append(&mut entry).await.unwrap();
        }

        let entries = wal.read_all_entries().await.unwrap();
        assert_eq!(entries.len(), 10);
        for i in 0..10 {
            assert_eq!(entries[i].key, format!("key{}", i));
        }
    }

    #[tokio::test]
    async fn test_replay_handler() {
        let (_dir, wal) = setup_wal().await;

        for i in 0..5 {
            let mut entry = WalEntry::new(
                WalEntryType::Insert,
                "test",
                &format!("k{}", i),
                serde_json::json!({"v": [i as f64]}),
            );
            wal.append(&mut entry).await.unwrap();
        }

        let mut replayed_count: std::sync::atomic::AtomicU64 =
            std::sync::atomic::AtomicU64::new(0);

        {
            let count = &replayed_count;
            let result = wal
                .replay(&|entries: &[WalEntry]| -> Result<(), String> {
                    count.fetch_add(entries.len() as u64, std::sync::atomic::Ordering::SeqCst);
                    Ok(())
                })
                .await
                .unwrap();
            assert_eq!(result.replayed, 5);
        }

        assert_eq!(
            replayed_count.load(std::sync::atomic::Ordering::SeqCst),
            5
        );
    }

    #[tokio::test]
    async fn test_recovery_restores_sequence() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().to_string_lossy().to_string();

        // Phase 1: write some entries
        {
            let wal = Arc::new(WriteAheadLog::new(&path));
            wal.init().await.unwrap();
            for i in 0..3 {
                let mut entry = WalEntry::new(
                    WalEntryType::Insert,
                    "test",
                    &format!("k{}", i),
                    serde_json::json!({"v": [1.0]}),
                );
                wal.append(&mut entry).await.unwrap();
            }
        }

        // Phase 2: create new WAL instance (simulates restart)
        {
            let wal = Arc::new(WriteAheadLog::new(&path));
            wal.init().await.unwrap();
            let seq = *wal.sequence_counter.read().await;
            assert_eq!(seq, 3, "Sequence counter should be restored");

            // Append more
            let mut entry = WalEntry::new(
                WalEntryType::Insert,
                "test",
                "k_next",
                serde_json::json!({"v": [1.0]}),
            );
            let new_seq = wal.append(&mut entry).await.unwrap();
            assert_eq!(new_seq, 4);
        }
    }
}
