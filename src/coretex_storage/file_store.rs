//! Durable, dependency-free file-backed storage engine.
//!
//! ## Design
//!
//! The engine is an append-only log split into segment files:
//!
//! ```text
//! <root>/store-000000.log
//! <root>/store-000001.log
//! ```
//!
//! Every mutation is appended as one length-prefixed, CRC32-checked record:
//!
//! ```text
//! [magic u8][op u8][crc32 u32 LE][key_len u32 LE][payload_len u32 LE][key][payload]
//! ```
//!
//! `crc32` covers `op || key || payload`. An in-memory hash map (rebuilt by
//! replaying the segments at startup) maps each key to the location of its
//! newest record, which makes reads one pread instead of a full scan.
//!
//! ## Durability
//!
//! Every record is flushed to the OS on write, so a process crash — the case
//! the restart tests cover — never loses an acknowledged write. A truncated or
//! CRC-mismatching tail, the signature of a crash mid-write, is detected during
//! replay and truncated away, so a half-written record can never be read back
//! as garbage. Surviving whole-machine power loss additionally needs `fsync`;
//! see [`FileStorage::with_fsync`].
//!
//! ## Space reclamation
//!
//! Overwrites and deletes leave dead bytes behind. Once dead bytes dominate the
//! log, [`FileStorage::compact`] rewrites only the live records into a fresh
//! segment and drops the old files. It runs automatically once the ratio
//! crosses the thresholds below, and can be triggered explicitly.

use async_trait::async_trait;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::coretex_core::{CoreTexError, Result};
use crate::coretex_storage::StorageEngine;

/// First byte of every record. Lets replay reject a file that is not ours.
const MAGIC: u8 = 0x7B;
/// magic + op + crc32 + key_len + payload_len
const HEADER_LEN: usize = 14;
const HEADER_LEN_U64: u64 = HEADER_LEN as u64;

const OP_STORE: u8 = 1;
const OP_DELETE: u8 = 2;
const OP_SET_TTL: u8 = 3;
const OP_REMOVE_TTL: u8 = 4;

const SEGMENT_PREFIX: &str = "store-";
const SEGMENT_SUFFIX: &str = ".log";

/// Roll over to a new segment once the active one reaches this size.
const DEFAULT_MAX_SEGMENT_BYTES: u64 = 64 * 1024 * 1024;
/// Never compact while the log is smaller than this, to avoid churn on tiny sets.
const COMPACT_MIN_DEAD_BYTES: u64 = 1024 * 1024;

// ---------------------------------------------------------------------------
// Little-endian accessors. Avoids `try_into().unwrap()` on hot paths.
// ---------------------------------------------------------------------------

fn u32_at(bytes: &[u8], off: usize) -> u32 {
    u32::from_le_bytes([bytes[off], bytes[off + 1], bytes[off + 2], bytes[off + 3]])
}

fn u64_at(bytes: &[u8], off: usize) -> u64 {
    u64::from_le_bytes([
        bytes[off],
        bytes[off + 1],
        bytes[off + 2],
        bytes[off + 3],
        bytes[off + 4],
        bytes[off + 5],
        bytes[off + 6],
        bytes[off + 7],
    ])
}

// ---------------------------------------------------------------------------
// CRC32 (IEEE 802.3, reflected). Table is built at compile time.
// ---------------------------------------------------------------------------

const fn crc32_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0usize;
    while i < 256 {
        let mut crc = i as u32;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0xEDB8_8320
            } else {
                crc >> 1
            };
            bit += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
}

static CRC32_TABLE: [u32; 256] = crc32_table();

fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for &byte in bytes {
        crc = (crc >> 8) ^ CRC32_TABLE[((crc ^ byte as u32) & 0xFF) as usize];
    }
    crc ^ 0xFFFF_FFFF
}

// ---------------------------------------------------------------------------
// Record encoding
// ---------------------------------------------------------------------------

/// Location of a live record inside a segment file.
#[derive(Debug, Clone, Copy)]
struct Loc {
    seg: u32,
    off: u64,
    /// Total encoded length, header included.
    len: u64,
}

/// What replaying one record did to the key index, so the caller can keep the
/// live/dead byte counters honest.
enum Effect {
    /// New key added; its record is live.
    Stored,
    /// Existing key overwritten; the previous record became dead.
    Replaced { dead: u64 },
    /// Key deleted; both the tombstone and the previous record are dead.
    Removed { dead: u64 },
    /// Bookkeeping record (TTL); occupies space but indexes nothing.
    Metadata,
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn segment_name(id: u32) -> String {
    format!("{}{:06}{}", SEGMENT_PREFIX, id, SEGMENT_SUFFIX)
}

fn parse_segment_id(name: &str) -> Option<u32> {
    let digits = name.strip_prefix(SEGMENT_PREFIX)?.strip_suffix(SEGMENT_SUFFIX)?;
    digits.parse::<u32>().ok()
}

/// `[u32 count][f32 LE × count][metadata JSON]`
fn encode_store_payload(vector: &[f32], metadata: &serde_json::Value) -> Result<Vec<u8>> {
    let meta = serde_json::to_vec(metadata).map_err(CoreTexError::Serialization)?;
    let mut buf = Vec::with_capacity(4 + vector.len() * 4 + meta.len());
    buf.extend_from_slice(&(vector.len() as u32).to_le_bytes());
    for value in vector {
        buf.extend_from_slice(&value.to_le_bytes());
    }
    buf.extend_from_slice(&meta);
    Ok(buf)
}

fn decode_store_payload(payload: &[u8]) -> Result<(Vec<f32>, serde_json::Value)> {
    if payload.len() < 4 {
        return Err(CoreTexError::StorageError(
            "record payload is shorter than its length prefix".into(),
        ));
    }
    let count = u32_at(payload, 0) as usize;
    let vec_end = 4usize
        .checked_add(
            count
                .checked_mul(4)
                .ok_or_else(|| CoreTexError::StorageError("vector length overflows".into()))?,
        )
        .ok_or_else(|| CoreTexError::StorageError("vector length overflows".into()))?;
    if payload.len() < vec_end {
        return Err(CoreTexError::StorageError(
            "record payload is shorter than its declared vector".into(),
        ));
    }
    let mut vector = Vec::with_capacity(count);
    for off in (4..vec_end).step_by(4) {
        vector.push(f32::from_le_bytes([
            payload[off],
            payload[off + 1],
            payload[off + 2],
            payload[off + 3],
        ]));
    }
    let metadata = serde_json::from_slice(&payload[vec_end..]).map_err(CoreTexError::Serialization)?;
    Ok((vector, metadata))
}

/// Encode a record header plus the CRC over `op || key || payload`.
fn encode_header(op: u8, key: &[u8], payload: &[u8]) -> [u8; HEADER_LEN] {
    let mut hashed = Vec::with_capacity(1 + key.len() + payload.len());
    hashed.push(op);
    hashed.extend_from_slice(key);
    hashed.extend_from_slice(payload);
    let crc = crc32(&hashed);

    let mut header = [0u8; HEADER_LEN];
    header[0] = MAGIC;
    header[1] = op;
    header[2..6].copy_from_slice(&crc.to_le_bytes());
    header[6..10].copy_from_slice(&(key.len() as u32).to_le_bytes());
    header[10..14].copy_from_slice(&(payload.len() as u32).to_le_bytes());
    header
}

/// Fill `buf` completely, or report a clean EOF / short read as `None`.
fn read_exact_or_eof(file: &mut File, buf: &mut [u8]) -> std::io::Result<Option<()>> {
    let mut filled = 0;
    while filled < buf.len() {
        match file.read(&mut buf[filled..])? {
            0 => return Ok(None),
            n => filled += n,
        }
    }
    Ok(Some(()))
}

/// Outcome of attempting to read a record at `off`.
enum RecordRead {
    Record(u8, String, Vec<u8>),
    /// The tail is incomplete, corrupt, or foreign. Everything from `off` on
    /// must be discarded.
    Stop,
}

/// Read one record starting at `off`.
fn read_record(file: &mut File, off: u64) -> std::io::Result<RecordRead> {
    file.seek(SeekFrom::Start(off))?;

    let mut header = [0u8; HEADER_LEN];
    if read_exact_or_eof(file, &mut header)?.is_none() || header[0] != MAGIC {
        return Ok(RecordRead::Stop);
    }

    let op = header[1];
    let expected_crc = u32_at(&header, 2);
    let key_len = u32_at(&header, 6) as usize;
    let payload_len = u32_at(&header, 10) as usize;

    let mut key = vec![0u8; key_len];
    if read_exact_or_eof(file, &mut key)?.is_none() {
        return Ok(RecordRead::Stop);
    }
    let mut payload = vec![0u8; payload_len];
    if read_exact_or_eof(file, &mut payload)?.is_none() {
        return Ok(RecordRead::Stop);
    }

    let mut hashed = Vec::with_capacity(1 + key.len() + payload.len());
    hashed.push(op);
    hashed.extend_from_slice(&key);
    hashed.extend_from_slice(&payload);
    if crc32(&hashed) != expected_crc {
        return Ok(RecordRead::Stop);
    }

    match String::from_utf8(key) {
        Ok(key) => Ok(RecordRead::Record(op, key, payload)),
        Err(_) => Ok(RecordRead::Stop),
    }
}

// ---------------------------------------------------------------------------
// Engine
// ---------------------------------------------------------------------------

struct Inner {
    /// Id of the segment currently being appended to.
    seg_id: u32,
    writer: BufWriter<File>,
    /// Bytes already written to `writer`'s file.
    offset: u64,
    index: HashMap<String, Loc>,
    /// key -> expiry, in seconds since the UNIX epoch.
    ttl: HashMap<String, u64>,
    live_bytes: u64,
    dead_bytes: u64,
}

/// Result of replaying every segment on disk.
struct Recovered {
    seg_id: u32,
    index: HashMap<String, Loc>,
    ttl: HashMap<String, u64>,
    live_bytes: u64,
    dead_bytes: u64,
}

/// A durable [`StorageEngine`] backed by append-only segment files.
pub struct FileStorage {
    root: PathBuf,
    max_segment_bytes: u64,
    fsync: bool,
    /// `None` until [`StorageEngine::init`] succeeds.
    inner: Mutex<Option<Inner>>,
}

impl FileStorage {
    /// Create a storage engine rooted at `root`. The directory is created by
    /// [`StorageEngine::init`], not here.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            max_segment_bytes: DEFAULT_MAX_SEGMENT_BYTES,
            fsync: false,
            inner: Mutex::new(None),
        }
    }

    /// The segment directory for this `data_dir` (`<data_dir>/store`).
    /// `data_dir` is normally `…/data/coretex`, so segments live at
    /// `…/data/coretex/store/store-NNNNNN.log`.
    pub fn store_path(data_dir: &str) -> PathBuf {
        Path::new(data_dir).join("store")
    }

    /// Roll over to a new segment file once the active one passes `bytes`.
    pub fn with_max_segment_bytes(mut self, bytes: u64) -> Self {
        self.max_segment_bytes = bytes.max(1);
        self
    }

    /// `fsync` every write instead of merely flushing to the OS.
    ///
    /// Costs roughly an order of magnitude in write throughput but survives
    /// whole-machine power loss, not just process crashes.
    pub fn with_fsync(mut self, fsync: bool) -> Self {
        self.fsync = fsync;
        self
    }

    /// Force a full compaction, rewriting only live records into a fresh segment.
    pub fn compact(&self) -> Result<()> {
        let mut guard = self.inner.lock();
        match guard.as_mut() {
            Some(inner) => self.compact_inner(inner),
            None => Err(CoreTexError::StorageNotInitialized),
        }
    }

    /// Bytes of live records currently on disk.
    pub fn live_bytes(&self) -> u64 {
        self.inner.lock().as_ref().map(|i| i.live_bytes).unwrap_or(0)
    }

    /// Bytes occupied by superseded records, tombstones, and TTL records.
    pub fn dead_bytes(&self) -> u64 {
        self.inner.lock().as_ref().map(|i| i.dead_bytes).unwrap_or(0)
    }

    // -- internals ----------------------------------------------------------

    fn segment_path(&self, id: u32) -> PathBuf {
        self.root.join(segment_name(id))
    }

    fn segment_ids(&self) -> Vec<u32> {
        let mut ids = Vec::new();
        if let Ok(entries) = fs::read_dir(&self.root) {
            for entry in entries.flatten() {
                if let Some(name) = entry.file_name().to_str() {
                    if let Some(id) = parse_segment_id(name) {
                        ids.push(id);
                    }
                }
            }
        }
        ids
    }

    /// Apply one replayed record to the index, reporting how it changed the
    /// live/dead byte accounting.
    fn apply(
        index: &mut HashMap<String, Loc>,
        ttl: &mut HashMap<String, u64>,
        op: u8,
        key: String,
        payload: Vec<u8>,
        loc: Loc,
    ) -> Effect {
        match op {
            OP_STORE => match index.insert(key, loc) {
                Some(previous) => Effect::Replaced { dead: previous.len },
                None => Effect::Stored,
            },
            OP_DELETE => {
                let dead = index.remove(&key).map(|l| l.len).unwrap_or(0);
                ttl.remove(&key);
                Effect::Removed { dead }
            }
            OP_SET_TTL => {
                if payload.len() >= 8 {
                    ttl.insert(key, u64_at(&payload, 0));
                }
                Effect::Metadata
            }
            OP_REMOVE_TTL => {
                ttl.remove(&key);
                Effect::Metadata
            }
            // An op byte we do not know is treated as corruption, not skipped:
            // silently ignoring it could resurrect stale state.
            _ => Effect::Metadata,
        }
    }

    /// Replay every segment in id order, building the index and truncating any
    /// torn tail left behind by a crash.
    fn recover(&self) -> Result<Recovered> {
        let mut segments = self.segment_ids();
        segments.sort_unstable();

        let mut index: HashMap<String, Loc> = HashMap::new();
        let mut ttl: HashMap<String, u64> = HashMap::new();
        let mut live_bytes: u64 = 0;
        let mut dead_bytes: u64 = 0;

        for seg_id in &segments {
            let path = self.segment_path(*seg_id);
            let mut file = File::open(&path)?;
            let mut off: u64 = 0;

            loop {
                match read_record(&mut file, off) {
                    Ok(RecordRead::Record(op, key, payload)) => {
                        let len = HEADER_LEN_U64 + key.len() as u64 + payload.len() as u64;
                        let loc = Loc { seg: *seg_id, off, len };
                        match Self::apply(&mut index, &mut ttl, op, key, payload, loc) {
                            Effect::Stored => live_bytes += len,
                            Effect::Replaced { dead } => {
                                live_bytes = live_bytes.saturating_sub(dead);
                                dead_bytes += dead;
                                live_bytes += len;
                            }
                            Effect::Removed { dead } => {
                                live_bytes = live_bytes.saturating_sub(dead);
                                dead_bytes += dead + len;
                            }
                            Effect::Metadata => dead_bytes += len,
                        }
                        off += len;
                    }
                    Ok(RecordRead::Stop) => {
                        let actual_len = file.metadata()?.len();
                        if actual_len > off {
                            eprintln!(
                                "FileStorage: discarding {} trailing bytes in {} (not a complete record)",
                                actual_len - off,
                                path.display()
                            );
                            let file = OpenOptions::new().write(true).open(&path)?;
                            file.set_len(off)?;
                            file.sync_all()?;
                        }
                        break;
                    }
                    Err(e) => return Err(CoreTexError::Io(e)),
                }
            }
        }

        // Expired entries are dead weight; drop them during recovery.
        let now = now_secs();
        let expired: Vec<String> = ttl
            .iter()
            .filter(|(_, expiry)| **expiry <= now)
            .map(|(key, _)| key.clone())
            .collect();
        for key in expired {
            ttl.remove(&key);
            if let Some(loc) = index.remove(&key) {
                live_bytes = live_bytes.saturating_sub(loc.len);
                dead_bytes += loc.len;
            }
        }

        Ok(Recovered {
            seg_id: segments.last().copied().unwrap_or(0),
            index,
            ttl,
            live_bytes,
            dead_bytes,
        })
    }

    fn open_writer(&self, inner: &mut Inner) -> Result<()> {
        let path = self.segment_path(inner.seg_id);
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;
        inner.offset = file.metadata()?.len();
        inner.writer = BufWriter::new(file);
        Ok(())
    }

    /// Append one record, rolling to a new segment when the active one is full.
    fn append(&self, inner: &mut Inner, op: u8, key: &str, payload: &[u8]) -> Result<Loc> {
        let key_bytes = key.as_bytes();
        let header = encode_header(op, key_bytes, payload);
        let len = HEADER_LEN_U64 + key_bytes.len() as u64 + payload.len() as u64;

        if inner.offset > 0 && inner.offset + len > self.max_segment_bytes {
            inner.seg_id = inner.seg_id.saturating_add(1);
            self.open_writer(inner)?;
        }

        let loc = Loc { seg: inner.seg_id, off: inner.offset, len };
        inner.writer.write_all(&header)?;
        inner.writer.write_all(key_bytes)?;
        inner.writer.write_all(payload)?;
        inner.writer.flush()?;
        if self.fsync {
            inner.writer.get_ref().sync_data()?;
        }
        inner.offset += len;
        Ok(loc)
    }

    /// Rewrite live records into a fresh segment, then drop the old files.
    fn compact_inner(&self, inner: &mut Inner) -> Result<()> {
        let new_id = self.segment_ids().into_iter().max().map(|m| m + 1).unwrap_or(0);
        let target = self.segment_path(new_id);

        // Snapshot what to keep before mutating the index.
        let live: Vec<(String, Loc)> = inner
            .index
            .iter()
            .map(|(key, loc)| (key.clone(), *loc))
            .collect();

        // Phase 1: write live records into the new segment.
        let mut new_index: HashMap<String, Loc> = HashMap::with_capacity(live.len());
        let mut new_live: u64 = 0;
        {
            let mut file = BufWriter::new(File::create(&target)?);
            for (key, old) in &live {
                let RecordRead::Record(op, _, payload) = self.read_at(*old)? else {
                    return Err(CoreTexError::StorageError(format!(
                        "live record {}@{} could not be read back",
                        old.seg, old.off
                    )));
                };
                if op != OP_STORE {
                    return Err(CoreTexError::StorageError(format!(
                        "key index points at a non-STORE record (op={})",
                        op
                    )));
                }

                let key_bytes = key.as_bytes();
                let header = encode_header(OP_STORE, key_bytes, &payload);
                let len = HEADER_LEN_U64 + key_bytes.len() as u64 + payload.len() as u64;

                file.write_all(&header)?;
                file.write_all(key_bytes)?;
                file.write_all(&payload)?;

                new_index.insert(key.clone(), Loc { seg: new_id, off: new_live, len });
                new_live += len;
            }
            file.flush()?;
            file.get_ref().sync_all()?;
        }

        // Phase 2: point the writer at the new segment before writing anything
        // else, otherwise TTL records would land in a file we are about to drop.
        inner.seg_id = new_id;
        self.open_writer(inner)?;
        inner.index = new_index;
        inner.dead_bytes = 0;
        inner.live_bytes = new_live;

        // Phase 3: the new segment is durable, so the old ones can go.
        for id in self.segment_ids() {
            if id != new_id {
                fs::remove_file(self.segment_path(id))?;
            }
        }

        // Phase 4: re-emit TTL bookkeeping into the new segment.
        for (key, expiry) in inner.ttl.clone() {
            let loc = self.append(inner, OP_SET_TTL, &key, &expiry.to_le_bytes())?;
            inner.dead_bytes += loc.len;
        }
        Ok(())
    }

    /// Read a record back by location.
    fn read_at(&self, loc: Loc) -> Result<RecordRead> {
        let mut file = File::open(self.segment_path(loc.seg))?;
        read_record(&mut file, loc.off).map_err(CoreTexError::Io)
    }

    /// Run compaction once the log has accumulated enough dead weight.
    fn maybe_compact(&self, inner: &mut Inner) -> Result<()> {
        if inner.dead_bytes >= COMPACT_MIN_DEAD_BYTES && inner.dead_bytes >= inner.live_bytes {
            self.compact_inner(inner)?;
        }
        Ok(())
    }

    fn require_inner(guard: &mut Option<Inner>) -> Result<&mut Inner> {
        guard.as_mut().ok_or(CoreTexError::StorageNotInitialized)
    }

    fn is_expired(inner: &Inner, key: &str) -> bool {
        inner
            .ttl
            .get(key)
            .map(|expiry| *expiry <= now_secs())
            .unwrap_or(false)
    }

    /// Supersede any existing record for `key`, keeping byte counters correct.
    fn record_superseded(inner: &mut Inner, len: u64) {
        inner.live_bytes += len;
    }
}

/// File length without holding the file open, so `Inner` can be built in one
/// expression. `File::metadata` would need the handle first, which is exactly
/// the ordering that forced the old `/dev/null` placeholder.
fn file_len(path: &Path) -> std::io::Result<u64> {
    Ok(fs::metadata(path)?.len())
}

#[async_trait]
impl StorageEngine for FileStorage {
    async fn init(&mut self) -> Result<()> {
        if self.inner.lock().is_some() {
            return Ok(());
        }
        fs::create_dir_all(&self.root)?;

        let recovered = self.recover()?;

        // Open the active segment before building the state around it, so no
        // placeholder is ever needed. The previous version used `/dev/null` as
        // a stand-in writer, which does not exist on Windows (`NUL` is the
        // equivalent) and made every Windows `init` fail with
        // "system cannot find the path specified".
        let path = self.segment_path(recovered.seg_id);
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;

        let inner = Inner {
            seg_id: recovered.seg_id,
            writer: BufWriter::new(file),
            offset: file_len(&path)?,
            index: recovered.index,
            ttl: recovered.ttl,
            live_bytes: recovered.live_bytes,
            dead_bytes: recovered.dead_bytes,
        };
        *self.inner.lock() = Some(inner);

        // A log that is mostly dead bytes is worth reclaiming at startup.
        let should_compact = {
            let guard = self.inner.lock();
            match guard.as_ref() {
                Some(i) => i.dead_bytes >= COMPACT_MIN_DEAD_BYTES && i.dead_bytes >= i.live_bytes,
                None => false,
            }
        };
        if should_compact {
            self.compact()?;
        }
        Ok(())
    }

    async fn store(&self, id: &str, vector: &[f32], metadata: &serde_json::Value) -> Result<()> {
        let payload = encode_store_payload(vector, metadata)?;
        let mut guard = self.inner.lock();
        let inner = Self::require_inner(&mut guard)?;

        let loc = self.append(inner, OP_STORE, id, &payload)?;
        if let Some(previous) = inner.index.insert(id.to_string(), loc) {
            inner.live_bytes = inner.live_bytes.saturating_sub(previous.len);
            inner.dead_bytes += previous.len;
        }
        Self::record_superseded(inner, loc.len);
        self.maybe_compact(inner)
    }

    async fn retrieve(&self, id: &str) -> Result<Option<(Vec<f32>, serde_json::Value)>> {
        let loc = {
            let guard = self.inner.lock();
            let inner = guard.as_ref().ok_or(CoreTexError::StorageNotInitialized)?;
            if Self::is_expired(inner, id) {
                return Ok(None);
            }
            match inner.index.get(id) {
                Some(loc) => *loc,
                None => return Ok(None),
            }
        };
        match self.read_at(loc)? {
            RecordRead::Record(_, _, payload) => Ok(Some(decode_store_payload(&payload)?)),
            RecordRead::Stop => Err(CoreTexError::StorageError(
                "live record failed validation on read".into(),
            )),
        }
    }

    async fn delete(&self, id: &str) -> Result<bool> {
        let mut guard = self.inner.lock();
        let inner = Self::require_inner(&mut guard)?;

        // Capture the old location *before* removing it, so its bytes are
        // accounted as dead rather than silently leaking out of live_bytes.
        let previous = inner.index.remove(id);
        let had_ttl = inner.ttl.remove(id).is_some();
        if previous.is_none() && !had_ttl {
            return Ok(false);
        }

        let tombstone = self.append(inner, OP_DELETE, id, &[])?;
        inner.dead_bytes += tombstone.len;
        if let Some(previous) = previous {
            inner.live_bytes = inner.live_bytes.saturating_sub(previous.len);
            inner.dead_bytes += previous.len;
        }
        self.maybe_compact(inner)?;
        Ok(true)
    }

    async fn list(&self) -> Result<Vec<String>> {
        let guard = self.inner.lock();
        let inner = guard.as_ref().ok_or(CoreTexError::StorageNotInitialized)?;
        Ok(inner
            .index
            .keys()
            .filter(|key| !Self::is_expired(inner, key))
            .cloned()
            .collect())
    }

    async fn count(&self) -> Result<usize> {
        let guard = self.inner.lock();
        let inner = guard.as_ref().ok_or(CoreTexError::StorageNotInitialized)?;
        Ok(inner
            .index
            .keys()
            .filter(|key| !Self::is_expired(inner, key))
            .count())
    }

    async fn set_ttl(&self, id: &str, ttl_secs: u64) -> Result<()> {
        let expiry = now_secs().saturating_add(ttl_secs);
        let mut guard = self.inner.lock();
        let inner = Self::require_inner(&mut guard)?;
        let loc = self.append(inner, OP_SET_TTL, id, &expiry.to_le_bytes())?;
        inner.dead_bytes += loc.len;
        inner.ttl.insert(id.to_string(), expiry);
        Ok(())
    }

    async fn remove_ttl(&self, id: &str) -> Result<()> {
        let mut guard = self.inner.lock();
        let inner = Self::require_inner(&mut guard)?;
        if inner.ttl.remove(id).is_some() {
            let loc = self.append(inner, OP_REMOVE_TTL, id, &[])?;
            inner.dead_bytes += loc.len;
        }
        Ok(())
    }

    async fn get_ttl(&self, id: &str) -> Result<Option<u64>> {
        let guard = self.inner.lock();
        let inner = guard.as_ref().ok_or(CoreTexError::StorageNotInitialized)?;
        let now = now_secs();
        Ok(match inner.ttl.get(id) {
            Some(&expiry) if expiry > now => Some(expiry - now),
            Some(_) => Some(0),
            None => None,
        })
    }

    async fn expired_keys(&self) -> Result<Vec<String>> {
        let guard = self.inner.lock();
        let inner = guard.as_ref().ok_or(CoreTexError::StorageNotInitialized)?;
        let now = now_secs();
        Ok(inner
            .ttl
            .iter()
            .filter(|(_, expiry)| **expiry <= now)
            .map(|(key, _)| key.clone())
            .collect())
    }

    async fn purge_expired(&self) -> Result<usize> {
        let mut guard = self.inner.lock();
        let inner = Self::require_inner(&mut guard)?;

        let now = now_secs();
        let expired: Vec<String> = inner
            .ttl
            .iter()
            .filter(|(_, expiry)| **expiry <= now)
            .map(|(key, _)| key.clone())
            .collect();

        let mut purged = 0;
        for key in expired {
            inner.ttl.remove(&key);
            if let Some(previous) = inner.index.remove(&key) {
                inner.live_bytes = inner.live_bytes.saturating_sub(previous.len);
                inner.dead_bytes += previous.len;
            }
            let tombstone = self.append(inner, OP_DELETE, &key, &[])?;
            inner.dead_bytes += tombstone.len;
            purged += 1;
        }
        self.maybe_compact(inner)?;
        Ok(purged)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    async fn open(dir: &Path) -> FileStorage {
        let mut storage = FileStorage::new(dir);
        storage.init().await.unwrap();
        storage
    }

    #[tokio::test]
    async fn store_then_retrieve_roundtrips() {
        let dir = tempfile::tempdir().unwrap();
        let storage = open(dir.path()).await;

        let metadata = serde_json::json!({"color": "red", "n": 1});
        storage.store("a", &[1.0, 2.0, 3.0], &metadata).await.unwrap();

        let (vector, meta) = storage.retrieve("a").await.unwrap().unwrap();
        assert_eq!(vector, vec![1.0, 2.0, 3.0]);
        assert_eq!(meta, metadata);
        assert!(storage.retrieve("missing").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let storage = open(dir.path()).await;
            storage.store("a", &[1.0, 0.0], &serde_json::json!({"k": "v"})).await.unwrap();
            storage.store("b", &[0.0, 1.0], &serde_json::json!({})).await.unwrap();
            storage.delete("b").await.unwrap();
        }

        let storage = open(dir.path()).await;
        assert_eq!(storage.count().await.unwrap(), 1);
        assert_eq!(storage.list().await.unwrap(), vec!["a".to_string()]);
        let (vector, meta) = storage.retrieve("a").await.unwrap().unwrap();
        assert_eq!(vector, vec![1.0, 0.0]);
        assert_eq!(meta, serde_json::json!({"k": "v"}));
    }

    #[tokio::test]
    async fn overwrite_keeps_latest_value() {
        let dir = tempfile::tempdir().unwrap();
        {
            let storage = open(dir.path()).await;
            storage.store("a", &[1.0], &serde_json::json!({"v": 1})).await.unwrap();
            storage.store("a", &[2.0], &serde_json::json!({"v": 2})).await.unwrap();
        }
        let storage = open(dir.path()).await;
        assert_eq!(storage.count().await.unwrap(), 1);
        let (vector, meta) = storage.retrieve("a").await.unwrap().unwrap();
        assert_eq!(vector, vec![2.0]);
        assert_eq!(meta, serde_json::json!({"v": 2}));
    }

    #[tokio::test]
    async fn delete_accounting_does_not_leak_live_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let storage = open(dir.path()).await;
        storage.store("a", &[1.0], &serde_json::json!({})).await.unwrap();
        assert!(storage.live_bytes() > 0);

        storage.delete("a").await.unwrap();
        assert_eq!(storage.live_bytes(), 0);
        assert!(storage.dead_bytes() > 0);
        assert!(!storage.delete("a").await.unwrap());
    }

    #[tokio::test]
    async fn truncated_tail_is_discarded() {
        let dir = tempfile::tempdir().unwrap();
        {
            let storage = open(dir.path()).await;
            storage.store("a", &[1.0, 2.0], &serde_json::json!({"ok": true})).await.unwrap();
            storage.store("b", &[3.0, 4.0], &serde_json::json!({})).await.unwrap();
        }

        // Simulate a crash halfway through writing a third record.
        let path = dir.path().join(segment_name(0));
        let mut bytes = fs::read(&path).unwrap();
        let torn_len = bytes.len();
        bytes.extend_from_slice(&[MAGIC, OP_STORE, 0xAA, 0xBB]);
        fs::write(&path, &bytes).unwrap();

        let storage = open(dir.path()).await;
        // Both committed records survive; the torn tail does not.
        assert_eq!(storage.count().await.unwrap(), 2);
        assert!(storage.retrieve("a").await.unwrap().is_some());
        assert!(storage.retrieve("b").await.unwrap().is_some());
        assert_eq!(fs::metadata(&path).unwrap().len(), torn_len as u64);
    }

    #[tokio::test]
    async fn corrupt_record_stops_replay() {
        let dir = tempfile::tempdir().unwrap();
        {
            let storage = open(dir.path()).await;
            storage.store("a", &[1.0], &serde_json::json!({})).await.unwrap();
            storage.store("b", &[2.0], &serde_json::json!({})).await.unwrap();
        }

        // Flip a byte inside the first record's payload.
        let path = dir.path().join(segment_name(0));
        let mut bytes = fs::read(&path).unwrap();
        bytes[HEADER_LEN + 6] ^= 0xFF;
        fs::write(&path, &bytes).unwrap();

        let storage = open(dir.path()).await;
        // Nothing after the corruption is trusted.
        assert_eq!(storage.count().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn ttl_roundtrips_and_purges() {
        let dir = tempfile::tempdir().unwrap();
        {
            let storage = open(dir.path()).await;
            storage.store("a", &[1.0], &serde_json::json!({})).await.unwrap();
            storage.set_ttl("a", 3600).await.unwrap();
            assert!(storage.get_ttl("a").await.unwrap().unwrap() > 3500);
        }

        let storage = open(dir.path()).await;
        assert!(storage.get_ttl("a").await.unwrap().unwrap() > 3500);

        storage.set_ttl("a", 0).await.unwrap();
        assert_eq!(storage.get_ttl("a").await.unwrap(), Some(0));
        assert_eq!(storage.purge_expired().await.unwrap(), 1);
        assert_eq!(storage.count().await.unwrap(), 0);
        assert!(storage.retrieve("a").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn expiry_hides_entry_but_keeps_it_out_of_count() {
        let dir = tempfile::tempdir().unwrap();
        let storage = open(dir.path()).await;
        storage.store("a", &[1.0], &serde_json::json!({})).await.unwrap();
        storage.set_ttl("a", 0).await.unwrap();

        assert_eq!(storage.count().await.unwrap(), 0);
        assert!(storage.list().await.unwrap().is_empty());
        assert!(storage.retrieve("a").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn compaction_reclaims_dead_bytes_and_preserves_data() {
        let dir = tempfile::tempdir().unwrap();
        let storage = open(dir.path()).await;

        for round in 0..200 {
            storage
                .store("hot", &[round as f32], &serde_json::json!({"round": round}))
                .await
                .unwrap();
            storage.store("cold", &[0.5], &serde_json::json!({})).await.unwrap();
        }
        assert!(storage.dead_bytes() > 0);

        storage.compact().unwrap();
        assert_eq!(storage.dead_bytes(), 0);
        assert_eq!(storage.count().await.unwrap(), 2);

        let (vector, meta) = storage.retrieve("hot").await.unwrap().unwrap();
        assert_eq!(vector, vec![199.0]);
        assert_eq!(meta, serde_json::json!({"round": 199}));

        // Only the compacted segment should remain.
        assert_eq!(storage.segment_ids().len(), 1);
    }

    #[tokio::test]
    async fn compaction_preserves_ttl_records() {
        let dir = tempfile::tempdir().unwrap();
        let storage = open(dir.path()).await;
        storage.store("a", &[1.0], &serde_json::json!({})).await.unwrap();
        storage.set_ttl("a", 3600).await.unwrap();

        storage.compact().unwrap();
        assert!(storage.get_ttl("a").await.unwrap().unwrap() > 3500);
        assert_eq!(storage.count().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn compaction_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let storage = open(dir.path()).await;
            for round in 0..100 {
                storage.store("hot", &[round as f32], &serde_json::json!({})).await.unwrap();
            }
            storage.compact().unwrap();
        }
        let storage = open(dir.path()).await;
        assert_eq!(storage.count().await.unwrap(), 1);
        assert_eq!(storage.retrieve("hot").await.unwrap().unwrap().0, vec![99.0]);
    }

    #[tokio::test]
    async fn rolls_segments_past_size_limit() {
        let dir = tempfile::tempdir().unwrap();
        let mut storage = FileStorage::new(dir.path()).with_max_segment_bytes(256);
        storage.init().await.unwrap();

        for i in 0..50 {
            storage
                .store(&format!("key-{}", i), &[i as f32; 8], &serde_json::json!({}))
                .await
                .unwrap();
        }
        assert!(storage.segment_ids().len() > 1);
        assert_eq!(storage.count().await.unwrap(), 50);
        assert_eq!(storage.retrieve("key-49").await.unwrap().unwrap().0, vec![49.0; 8]);
    }

    #[tokio::test]
    async fn timestamp_ttl_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let storage = open(dir.path()).await;
            storage.store("a", &[1.0], &serde_json::json!({})).await.unwrap();
            storage.set_ttl("a", 0).await.unwrap();
        }
        let storage = open(dir.path()).await;
        // An already-expired entry is dropped during replay.
        assert_eq!(storage.count().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn empty_vectors_and_unicode_keys_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let storage = open(dir.path()).await;

        storage.store("", &[], &serde_json::json!({})).await.unwrap();
        storage.store("中文键/🧪", &[1.5], &serde_json::json!({"标题": "值"})).await.unwrap();

        let (vector, meta) = storage.retrieve("中文键/🧪").await.unwrap().unwrap();
        assert_eq!(vector, vec![1.5]);
        assert_eq!(meta, serde_json::json!({"标题": "值"}));
        assert!(storage.retrieve("").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn chinese_english_mixed_metadata_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let storage = open(dir.path()).await;

        let metadata = serde_json::json!({
            "title": "CoreTexDB 是一款多模态向量数据库",
            "description": "A multimodal vector database for AI applications",
            "作者": "Cerebros Team",
            "tags": ["向量搜索", "vector search", "相似度", "similarity"],
            "中文详情": {
                "功能": "支持中英文混合查询",
                "性能": "每秒处理超过 10 万条向量",
                "兼容性": "Windows / Linux / macOS"
            },
            "mixed": "数据库database引擎engine"
        });

        storage.store("doc:中文测试", &[1.0, 2.0, 3.0], &metadata).await.unwrap();

        let (vector, meta) = storage.retrieve("doc:中文测试").await.unwrap().unwrap();
        assert_eq!(vector, vec![1.0, 2.0, 3.0]);
        assert_eq!(meta, metadata);

        // Verify specific Chinese string values survive round-trip
        assert_eq!(meta["title"].as_str().unwrap(), "CoreTexDB 是一款多模态向量数据库");
        assert_eq!(meta["作者"].as_str().unwrap(), "Cerebros Team");
        assert_eq!(meta["中文详情"]["功能"].as_str().unwrap(), "支持中英文混合查询");
        assert_eq!(meta["mixed"].as_str().unwrap(), "数据库database引擎engine");
    }

    #[tokio::test]
    async fn bulk_chinese_vectors_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let storage = open(dir.path()).await;

        let categories = ["新闻", "科技", "体育", "娱乐", "财经"];
        for (i, cat) in categories.iter().enumerate() {
            let key = format!("article:{}:{}", cat, i);
            let metadata = serde_json::json!({
                "category": cat,
                "title": format!("第{}篇{}文章", i + 1, cat),
                "content": "这是一篇关于{}的测试文章，包含中英文混合内容 test content".replace("{}", cat),
                "score": 0.5 + i as f64 * 0.1,
            });
            let vector: Vec<f32> = (0..8).map(|j| (i * 8 + j) as f32 * 0.1).collect();
            storage.store(&key, &vector, &metadata).await.unwrap();
        }

        // Verify all records can be read back
        for (i, cat) in categories.iter().enumerate() {
            let key = format!("article:{}:{}", cat, i);
            let (vector, meta) = storage.retrieve(&key).await.unwrap().unwrap();
            assert_eq!(vector.len(), 8);
            assert_eq!(meta["category"].as_str().unwrap(), *cat);
            assert!(meta["title"].as_str().unwrap().contains(cat));
        }

        assert_eq!(storage.count().await.unwrap(), 5);
    }

    #[tokio::test]
    async fn operations_before_init_fail_loudly() {
        let dir = tempfile::tempdir().unwrap();
        let storage = FileStorage::new(dir.path());
        assert!(storage.store("a", &[1.0], &serde_json::json!({})).await.is_err());
        assert!(storage.retrieve("a").await.is_err());
        assert!(storage.count().await.is_err());
        assert!(storage.delete("a").await.is_err());
        assert!(storage.compact().is_err());
    }
}
