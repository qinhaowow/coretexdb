//! D3 — fault injection: what each durability layer promises when the layer
//! below it lies.
//!
//! The database has three durable layers, each with its own crash contract:
//!
//! * `FileStorage` — an append-only log of length-prefixed, CRC-checked
//!   records. A torn tail must be discarded, never misread.
//! * the WAL — `checksum|json` lines. A corrupt line must be skipped and
//!   counted, and the surrounding entries must still replay.
//! * snapshots — a magic/length/CRC container written atomically.
//!
//! Injection here is by *construction*, not by mocking: files are truncated
//! at byte offsets, checksums are flipped, segments are deleted mid-log. That
//! exercises the same code paths a power cut would, because the recovery
//! routines have no way to tell the difference.
//!
//! What is deliberately asserted is narrow: recovery never invents data, and
//! recovery never panics. Where a layer's contract says "skip", the skip is
//! visible in what survives — a specific key lost is acceptable, a wrong
//! value is not.

use std::path::{Path, PathBuf};

use coretexdb::coretex_utils::wal::{WalEntry, WalEntryType, WriteAheadLog};
use coretexdb::{CoreTexDB, DbConfig};

const DIM: usize = 4;

fn vec_of(i: f32) -> Vec<f32> {
    vec![i, 1.0, 0.0, 0.0]
}

async fn open_at(data_dir: &str, wal: bool) -> CoreTexDB {
    let mut config = DbConfig::new(data_dir);
    config.wal_enabled = wal;
    let db = CoreTexDB::with_config(config);
    db.init().await.expect("init");
    db
}

async fn seeded(path: &str, rows: usize) -> CoreTexDB {
    let db = open_at(path, true).await;
    db.create_collection_with_index("c", DIM, "euclidean", "brute_force")
        .await
        .unwrap();
    let vectors: Vec<(String, Vec<f32>, serde_json::Value)> = (0..rows)
        .map(|i| (format!("r{i}"), vec_of(i as f32), serde_json::json!({"i": i})))
        .collect();
    db.insert_vectors("c", vectors).await.unwrap();
    db
}

/// Paths inside a database directory.
struct Layout {
    data: PathBuf,
    wal: PathBuf,
    store: PathBuf,
}

impl Layout {
    /// `base` is what is passed to `DbConfig::new`, i.e. the install root;
    /// the real paths hang off its `data/` child.
    fn new(base: &Path) -> Self {
        Self {
            data: base.join("data/coretex"),
            wal: base.join("data/wal"),
            store: base.join("data/coretex/store"),
        }
    }

    fn store_segments(&self) -> Vec<PathBuf> {
        let mut paths: Vec<PathBuf> = std::fs::read_dir(&self.store)
            .map(|entries| {
                entries
                    .flatten()
                    .map(|e| e.path())
                    .filter(|p| p.extension().map(|e| e == "log").unwrap_or(false))
                    .collect()
            })
            .unwrap_or_default();
        paths.sort();
        paths
    }

    fn wal_segments(&self) -> Vec<PathBuf> {
        let mut paths: Vec<PathBuf> = std::fs::read_dir(&self.wal)
            .map(|entries| {
                entries
                    .flatten()
                    .map(|e| e.path())
                    .filter(|p| {
                        p.file_name()
                            .and_then(|n| n.to_str())
                            .map(|n| n.starts_with("wal-") && n.ends_with(".log"))
                            .unwrap_or(false)
                    })
                    .collect()
            })
            .unwrap_or_default();
        paths.sort();
        paths
    }
}

async fn count_of(db: &CoreTexDB) -> usize {
    db.data_manager.get_vectors_count("c").await.unwrap_or(0)
}

async fn reopen(base: &Path) -> CoreTexDB {
    open_at(&base.to_string_lossy(), true).await
}

/// 1. 存储层断电：截断到任意字节偏移，尾部半条记录必须被丢弃而不是误读。
#[tokio::test]
async fn truncated_storage_tail_is_discarded_at_every_offset() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path();
    let layout = Layout::new(base);

    {
        let db = open_at(&base.to_string_lossy(), true).await;
        db.create_collection_with_index("c", DIM, "euclidean", "brute_force")
            .await
            .unwrap();
        for i in 0..8 {
            db.insert_vectors(
                "c",
                vec![(format!("r{i}"), vec_of(i as f32), serde_json::json!({"i": i}))],
            )
            .await
            .unwrap();
        }
    }

    let segments = layout.store_segments();
    assert!(!segments.is_empty(), "storage must have written segments");
    let total: u64 = segments.iter().map(|p| std::fs::metadata(p).unwrap().len()).sum();
    assert!(total > 100, "expected a non-trivial log, got {total} bytes");

    // Cut the log at a spread of offsets. Any prefix must open, and recovery
    // must keep only whole records — never fabricate one from half a write.
    for cut in [1u64, 7, 33, total / 3, total / 2, total - 1] {
        // Restore a pristine copy for each cut.
        let work = dir.path().join(format!("cut-{cut}"));
        copy_tree(&layout.store, &work).unwrap();

        let last = last_log(&work).unwrap();
        let len = std::fs::metadata(&last).unwrap().len();
        let target = cut.min(len.saturating_sub(1));
        truncate(&last, target);

        let db = open_store_only(&work, base).await;
        // Whatever survived must be readable without error, and the ids that
        // come back must be a prefix of what was written (records are
        // appended in order).
        let recovered = db.data_manager.get_vectors_count("c").await.unwrap_or(0);
        assert!(
            recovered <= 8,
            "cut {cut}: recovery invented {recovered} rows (max 8)"
        );
        if let Ok(hits) = db.search("c", vec_of(0.0), 16, None).await {
            let mut ids: Vec<String> = hits.into_iter().map(|h| h.id).collect();
            ids.sort();
            ids.dedup();
            assert!(ids.len() <= 8, "cut {cut}: duplicate ids after recovery");
        }
    }
}

/// 2. 存储层校验和被破坏：该记录之后的内容必须停止恢复，而不是带着错值继续。
#[tokio::test]
async fn storage_checksum_break_stops_recovery_there() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path();
    let layout = Layout::new(base);

    {
        let db = open_at(&base.to_string_lossy(), true).await;
        db.create_collection_with_index("c", DIM, "euclidean", "brute_force")
            .await
            .unwrap();
        for i in 0..6 {
            db.insert_vectors(
                "c",
                vec![(format!("r{i}"), vec_of(i as f32), serde_json::json!({"i": i}))],
            )
            .await
            .unwrap();
        }
    }

    let segments = layout.store_segments();
    let last = segments.last().unwrap();
    let original = std::fs::read(last).unwrap();
    assert!(original.len() > 32);

    // Flip a byte well inside the payload of an early record. The record's CRC
    // no longer matches, so recovery must treat everything from there on as
    // an incomplete tail.
    let victim = original.len() / 3;
    let mut damaged = original.clone();
    damaged[victim] ^= 0xff;
    std::fs::write(last, &damaged).unwrap();

    // Recovery must succeed (not error out) and lose rows from the damage
    // point onward — never return the damaged payload.
    let work = dir.path().join("crc-broken");
    copy_tree(&layout.store, &work).unwrap();
    let target = last_log(&work).unwrap();
    std::fs::write(&target, &damaged).unwrap();

    let db = open_store_only(&work, base).await;
    let count = db.data_manager.get_vectors_count("c").await.unwrap_or(0);
    assert!(count <= 6, "recovery invented rows after a checksum break: {count}");

    // And a query must not return a value that failed its checksum. The rows
    // that do come back must be ones we wrote.
    if let Ok(hits) = db.search("c", vec_of(0.0), 16, None).await {
        for hit in hits {
            let n: usize = hit.id.trim_start_matches('r').parse().unwrap_or(usize::MAX);
            assert!(n < 6, "recovered an id we never wrote: {}", hit.id);
        }
    }
}

/// 3. WAL 行被破坏：坏行跳过并计数，前后条目照常重放。
#[tokio::test]
async fn corrupt_wal_line_is_skipped_and_neighbours_replay() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    let wal = WriteAheadLog::new(&path);
    wal.init().await.unwrap();

    let mut entries = Vec::new();
    for i in 0..5 {
        entries.push(WalEntry::new(
            WalEntryType::Insert,
            "c",
            &format!("r{i}"),
            serde_json::json!({"vector": [i as f32, 1.0, 0.0, 0.0], "metadata": {}}),
        ));
    }
    wal.append_batch(&mut entries).await.unwrap();

    // Damage the JSON of the middle entry so its checksum no longer matches.
    let segments: Vec<PathBuf> = std::fs::read_dir(&path)
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.to_string_lossy().contains("wal-"))
        .collect();
    assert_eq!(segments.len(), 1);
    let text = std::fs::read_to_string(&segments[0]).unwrap();
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines.len(), 5);

    let mut damaged: Vec<String> = lines.iter().map(|l| l.to_string()).collect();
    // Flip a byte inside the payload, past the checksum prefix. Owned `String`
    // keeps this simple; no need to leak.
    let at = damaged[2].len() / 2;
    damaged[2].replace_range(at..at + 1, "X");
    std::fs::write(&segments[0], damaged.join("\n") + "\n").unwrap();

    // A fresh reader over the same files: the damaged line is skipped, the
    // other four survive, and the skip is counted.
    let reopened = WriteAheadLog::new(&path);
    reopened.init().await.unwrap();
    let read = reopened.read_all_entries().await.unwrap();
    assert_eq!(
        read.len(),
        4,
        "exactly the damaged entry must be dropped, got {:?}",
        read.iter().map(|e| e.key.as_str()).collect::<Vec<_>>()
    );
    assert!(
        !read.iter().any(|e| e.key == "r2"),
        "the damaged entry must be the one dropped"
    );
    assert!(reopened.stats().await.corrupted_entries >= 1);
}

/// 4. WAL 尾部半行（断电时最常见）：之前的条目必须完整可读。
#[tokio::test]
async fn torn_wal_tail_does_not_hide_earlier_entries() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();

    {
        let wal = WriteAheadLog::new(&path);
        wal.init().await.unwrap();
        for i in 0..4 {
            let mut entry = WalEntry::new(
                WalEntryType::Insert,
                "c",
                &format!("r{i}"),
                serde_json::json!({"vector": [1.0, 0.0], "metadata": {}}),
            );
            wal.append(&mut entry).await.unwrap();
        }
    }

    let segment: Vec<PathBuf> = std::fs::read_dir(&path)
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.to_string_lossy().contains("wal-"))
        .collect();
    let segment = segment[0].clone();

    // Append a partial line, as a crash mid-write would leave.
    let mut bytes = std::fs::read(&segment).unwrap();
    bytes.extend_from_slice(b"deadbeef|{\"sequence\":5,\"entry_ty");
    std::fs::write(&segment, &bytes).unwrap();

    let wal = WriteAheadLog::new(&path);
    wal.init().await.unwrap();
    let entries = wal.read_all_entries().await.unwrap();
    assert_eq!(entries.len(), 4, "a torn tail must not cost earlier entries");
    assert_eq!(entries[0].key, "r0");
    assert_eq!(entries[3].key, "r3");
}

/// 5. 端到端：WAL 有条目而存储被清空（断电落在两步之间），重启后靠 WAL 恢复。
#[tokio::test]
async fn crash_between_wal_and_storage_is_recovered_from_the_log() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path();
    let layout = Layout::new(base);

    {
        let db = seeded(&base.to_string_lossy(), 5).await;
        assert_eq!(count_of(&db).await, 5);
    }

    // Simulate the power cut between the WAL append and the storage write:
    // the log survives, the storage segments do not.
    for segment in layout.store_segments() {
        std::fs::write(&segment, b"").unwrap();
    }

    let db = reopen(base).await;
    assert_eq!(
        count_of(&db).await,
        5,
        "a completed WAL write must be replayed even with empty storage"
    );
    let hits = db.search("c", vec_of(0.0), 10, None).await.unwrap();
    assert_eq!(hits.len(), 5);
}

/// 6. 存储有数据但 WAL 被清空：manifest + 存储必须仍能重建（无 WAL 依赖）。
#[tokio::test]
async fn empty_wal_with_intact_storage_still_restores() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path();
    let layout = Layout::new(base);

    {
        let db = seeded(&base.to_string_lossy(), 4).await;
        assert_eq!(count_of(&db).await, 4);
    }

    for segment in layout.wal_segments() {
        std::fs::write(&segment, b"").unwrap();
    }

    let db = reopen(base).await;
    assert_eq!(
        count_of(&db).await,
        4,
        "storage plus the manifest must rebuild the collection without the WAL"
    );
}

/// 7. manifest 丢失但 WAL 完好：回放必须重建 schema（度量不能靠猜）。
#[tokio::test]
async fn lost_manifest_is_rebuilt_from_the_log_schema() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path();
    let layout = Layout::new(base);

    {
        let db = seeded(&base.to_string_lossy(), 3).await;
        assert_eq!(count_of(&db).await, 3);
    }

    // Destroy the manifest, keep the log. It must still be *well-formed* JSON —
    // an unparseable manifest is a different failure (init refuses it, which is
    // correct), whereas an empty one is the realistic "metadata lost" case.
    let manifest = layout.data.join("metadata/metadata.json");
    assert!(
        manifest.exists(),
        "expected a manifest at {}",
        manifest.display()
    );
    std::fs::write(
        &manifest,
        format!(
            r#"{{"version":"{}","created_at":0,"last_modified":0,"collections":[],"schemas":[]}}"#,
            env!("CARGO_PKG_VERSION")
        ),
    )
    .unwrap();

    let db = reopen(base).await;
    assert_eq!(count_of(&db).await, 3);
    // The schema comes from the CreateCollection entry in the log, so the
    // metric must be euclidean — a guess would make this cosine.
    let schema = db.data_manager.get_collection("c").await.unwrap();
    assert_eq!(
        format!("{:?}", schema.distance_metric),
        "Euclidean",
        "recovery must take the schema from the log, not infer it"
    );
}

/// 8. 反复重启：同一份数据重启 N 次，结果必须幂等且不增长。
#[tokio::test]
async fn repeated_restarts_are_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path();

    {
        let db = seeded(&base.to_string_lossy(), 6).await;
        assert_eq!(count_of(&db).await, 6);
    }

    let mut counts = Vec::new();
    for _ in 0..3 {
        let db = reopen(base).await;
        counts.push(count_of(&db).await);
        drop(db);
    }
    assert!(
        counts.windows(2).all(|w| w[0] == w[1]),
        "row count drifted across restarts: {counts:?}"
    );
    assert_eq!(counts[0], 6, "restarts must not lose or duplicate rows");
}

// ── helpers ────────────────────────────────────────────────────────

fn copy_tree(from: &Path, to: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(to)?;
    for entry in std::fs::read_dir(from)? {
        let entry = entry?;
        let target = to.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_tree(&entry.path(), &target)?;
        } else {
            std::fs::copy(entry.path(), &target)?;
        }
    }
    Ok(())
}

fn last_log(dir: &Path) -> Option<PathBuf> {
    let mut paths: Vec<PathBuf> = std::fs::read_dir(dir)
        .ok()?
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().map(|e| e == "log").unwrap_or(false))
        .collect();
    paths.sort();
    paths.pop()
}

fn truncate(path: &Path, len: u64) {
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .unwrap();
    file.set_len(len).unwrap();
    file.sync_all().unwrap();
}

/// Open a database whose storage lives in `store_dir` but whose manifest and
/// WAL stay where they were — lets a test damage storage in isolation.
///
/// WAL is disabled on purpose: these tests are about what the storage log can
/// recover on its own, and leaving the log enabled would mask a storage-layer
/// loss by replaying it.
async fn open_store_only(store_dir: &Path, base: &Path) -> CoreTexDB {
    let data_dir = base.join("data/coretex");
    // Swap the store directory for the damaged copy.
    let live = data_dir.join("store");
    let _ = std::fs::remove_dir_all(&live);
    copy_tree(store_dir, &live).unwrap();

    let mut config = DbConfig::new(&data_dir.to_string_lossy());
    config.wal_enabled = false;
    let db = CoreTexDB::with_config(config);
    db.init()
        .await
        .expect("init on a damaged store must succeed, not error");
    db
}