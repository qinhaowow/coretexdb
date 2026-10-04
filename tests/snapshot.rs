//! C4 — snapshots and log compaction: what a running node hands you, and
//! what a compacted log replays to.
//!
//! A snapshot must restore the database to a point in time, not to "roughly
//! now"; it must refuse a damaged file rather than decode something
//! plausible; a restored database must survive its own restart (or the
//! rehearsal proves nothing); and compaction must fold history without
//! changing what a replay produces.

use coretexdb::{compact_wal, CoreTexDB, DbConfig, SnapshotArchive};

const DIM: usize = 4;
const QUERY: [f32; DIM] = [0.0, 1.0, 0.0, 0.0];

async fn open_at(path: &str) -> CoreTexDB {
    let mut config = DbConfig::new(path);
    config.wal_enabled = true;
    let db = CoreTexDB::with_config(config);
    db.init().await.expect("init");
    db
}

async fn open_plain(path: &str) -> CoreTexDB {
    let db = CoreTexDB::with_config(DbConfig::new(path));
    db.init().await.expect("init");
    db
}

async fn seed(db: &CoreTexDB, collection: &str, rows: usize) {
    db.create_collection_with_index(collection, DIM, "euclidean", "brute_force")
        .await
        .unwrap();
    let vectors: Vec<(String, Vec<f32>, serde_json::Value)> = (0..rows)
        .map(|i| (format!("r{i}"), vec![i as f32, 1.0, 0.0, 0.0], serde_json::json!({"i": i})))
        .collect();
    db.insert_vectors(collection, vectors).await.unwrap();
}

async fn search_ids(db: &CoreTexDB, collection: &str) -> Vec<String> {
    db.search(collection, QUERY.to_vec(), 32, None)
        .await
        .unwrap()
        .into_iter()
        .map(|h| h.id)
        .collect()
}

/// 1. 快照恢复的是"那一刻"，不是"现在"：快照后的变更不出现在恢复出来的库里。
#[tokio::test]
async fn snapshot_restores_an_earlier_state() {
    let src_dir = tempfile::tempdir().unwrap();
    let snap_dir = tempfile::tempdir().unwrap();
    let dst_dir = tempfile::tempdir().unwrap();

    let src = open_at(&src_dir.path().to_str().unwrap().to_string()).await;
    seed(&src, "docs", 3).await;

    let archive = SnapshotArchive::open(snap_dir.path()).await.unwrap();
    let meta = archive.save(&src, "s1").await.unwrap();
    assert_eq!(meta.collections, 1);
    assert_eq!(meta.records, 3);
    assert!(meta.lsn > 0, "snapshot records the source log position");
    assert!(meta.bytes > 0);

    // The source keeps moving after the image was taken.
    src.insert_vectors("docs", vec![("r3".into(), vec![3.0, 1.0, 0.0, 0.0], serde_json::json!({}))])
        .await
        .unwrap();
    src.delete_vectors("docs", &["r0".to_string()]).await.unwrap();
    assert_eq!(src.data_manager.get_vectors_count("docs").await.unwrap(), 3);

    // The restored database reflects the image, not the present.
    let dst = open_plain(&dst_dir.path().to_str().unwrap().to_string()).await;
    let restored = archive.restore_into(&dst, "s1").await.unwrap();
    assert_eq!(restored, 3);
    assert_eq!(dst.data_manager.get_vectors_count("docs").await.unwrap(), 3);
    assert_eq!(search_ids(&dst, "docs").await, vec!["r0", "r1", "r2"]);
}

/// 2. 磁盘上的损坏文件被拒绝，并说清是哪一项校验失败。
#[tokio::test]
async fn damaged_snapshot_file_is_refused() {
    let src_dir = tempfile::tempdir().unwrap();
    let snap_dir = tempfile::tempdir().unwrap();
    let src = open_at(&src_dir.path().to_str().unwrap().to_string()).await;
    seed(&src, "docs", 2).await;

    let archive = SnapshotArchive::open(snap_dir.path()).await.unwrap();
    archive.save(&src, "s1").await.unwrap();
    let path = snap_dir.path().join("s1.snapshot");
    let good = std::fs::read(&path).unwrap();

    // Flip a payload byte: the checksum must catch it.
    let mut corrupt = good.clone();
    let last = corrupt.len() - 1;
    corrupt[last] ^= 0x01;
    std::fs::write(&path, &corrupt).unwrap();
    let err = archive.load("s1").await.unwrap_err().to_string();
    assert!(err.contains("checksum mismatch"), "got: {err}");

    // Truncated file: caught by the length field.
    std::fs::write(&path, &good[..good.len() - 5]).unwrap();
    let err = archive.load("s1").await.unwrap_err().to_string();
    assert!(err.contains("length mismatch"), "got: {err}");

    // Restore the good file: it loads again.
    std::fs::write(&path, &good).unwrap();
    assert_eq!(archive.load("s1").await.unwrap().records["docs"].len(), 2);
}

/// 3. 归档管理：列举、找最新、按上限裁剪、删除。
#[tokio::test]
async fn archive_lists_prunes_and_deletes() {
    let src_dir = tempfile::tempdir().unwrap();
    let snap_dir = tempfile::tempdir().unwrap();
    let src = open_at(&src_dir.path().to_str().unwrap().to_string()).await;
    seed(&src, "docs", 1).await;

    let archive = SnapshotArchive::open(snap_dir.path()).await.unwrap();
    for name in ["a", "b", "c"] {
        archive.save(&src, name).await.unwrap();
    }
    assert_eq!(archive.list(), vec!["c", "b", "a"]);
    assert_eq!(archive.latest().as_deref(), Some("c"));

    // Re-saving the same name replaces the file, it does not accumulate.
    archive.save(&src, "a").await.unwrap();
    assert_eq!(archive.list().len(), 3);

    let removed = archive.prune(2).unwrap();
    assert_eq!(removed, vec!["a"]);
    assert_eq!(archive.list(), vec!["c", "b"]);

    assert!(archive.delete("b").unwrap());
    assert!(!archive.delete("b").unwrap(), "deleting twice is not an error");
    assert_eq!(archive.list(), vec!["c"]);
}

/// 4. 恢复出来的库能扛自己的重启——否则恢复演练没意义。
#[tokio::test]
async fn restored_database_survives_its_own_restart() {
    let src_dir = tempfile::tempdir().unwrap();
    let snap_dir = tempfile::tempdir().unwrap();
    let dst_dir = tempfile::tempdir().unwrap();

    let src = open_at(&src_dir.path().to_str().unwrap().to_string()).await;
    seed(&src, "docs", 4).await;
    let archive = SnapshotArchive::open(snap_dir.path()).await.unwrap();
    archive.save(&src, "s1").await.unwrap();

    {
        let dst = open_plain(&dst_dir.path().to_str().unwrap().to_string()).await;
        archive.restore_into(&dst, "s1").await.unwrap();
        assert_eq!(dst.data_manager.get_vectors_count("docs").await.unwrap(), 4);
    }

    // Restart the restored database: schemas come from the manifest the
    // restore persisted, rows from storage.
    let dst2 = open_plain(&dst_dir.path().to_str().unwrap().to_string()).await;
    assert_eq!(dst2.data_manager.get_vectors_count("docs").await.unwrap(), 4);
    assert_eq!(search_ids(&dst2, "docs").await, vec!["r0", "r1", "r2", "r3"]);
}

/// 5. 日志压实：折叠历史但回放结果不变——新库拿压实日志恢复出同样状态。
#[tokio::test]
async fn wal_compaction_preserves_state() {
    let src_dir = tempfile::tempdir().unwrap();
    let compact_dir = tempfile::tempdir().unwrap();
    let dst_dir = tempfile::tempdir().unwrap();

    let src = open_at(&src_dir.path().to_str().unwrap().to_string()).await;
    seed(&src, "docs", 5).await;
    // History that compaction should fold: an overwrite and a delete.
    src.update_vector("docs", "r1", vec![9.0, 1.0, 0.0, 0.0], None)
        .await
        .unwrap();
    src.update_vector("docs", "r1", vec![8.0, 1.0, 0.0, 0.0], None)
        .await
        .unwrap();
    src.delete_vectors("docs", &["r4".to_string()]).await.unwrap();

    let target = compact_dir.path().join("wal-compacted");
    let report = compact_wal(&src, &target).await.unwrap();
    assert!(report.entries_before > report.entries_after, "{report:?}");
    assert!(report.entries_dropped > 0);
    assert!(report.bytes > 0);

    // A fresh node whose WAL is the compacted log replays to the same state.
    let mut config = DbConfig::new(&dst_dir.path().to_str().unwrap().to_string());
    config.wal_enabled = true;
    config.wal_dir = target.to_string_lossy().to_string();
    let dst = CoreTexDB::with_config(config);
    dst.init().await.expect("init");

    assert_eq!(dst.data_manager.get_vectors_count("docs").await.unwrap(), 4);
    // Same rows *and* same ranking as the source: recovery now rebuilds the
    // collection from the schema in the log instead of guessing a metric.
    assert_eq!(search_ids(&dst, "docs").await, search_ids(&src, "docs").await);
    assert_eq!(
        format!("{:?}", dst.data_manager.get_collection("docs").await.unwrap().distance_metric),
        "Euclidean"
    );

    // The overwrite survived as its final value, not its first one.
    let record = dst.data_manager.get_vector("docs", "r1").await.unwrap().unwrap();
    assert_eq!(record.vector[0], 8.0);

    // The live log is untouched by compaction.
    assert_eq!(src.data_manager.get_vectors_count("docs").await.unwrap(), 4);
}