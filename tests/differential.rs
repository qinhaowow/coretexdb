//! D3 — differential testing: the same data restored by every available route
//! must arrive in the same state.
//!
//! Five mechanisms can populate a database — the manifest plus storage, WAL
//! replay, a snapshot restore, a replication sync, and a compacted-log replay.
//! Each was written against a different assumption about what the others
//! guarantee, and a divergence between them would not show up in any
//! mechanism's own tests: a replica could faithfully replay a primary whose
//! own recovery was subtly wrong, and a snapshot could preserve that error.
//!
//! So the assertion is cross-cutting. One dataset, many write shapes, then
//! every restore path produces the same collections, the same ids, the same
//! vectors and metadata, and the same search ranking — compared field by
//! field rather than by hash, so a mismatch says what changed.

use std::collections::BTreeMap;
use std::sync::Arc;

use coretexdb::coretex_replication::{InProcessTransport, ReplicaSync};
use coretexdb::{compact_wal, CoreTexDB, DbConfig, SnapshotArchive};

const DIM: usize = 8;

/// The whole observable state of a collection, in a form that compares
/// cleanly: ids sorted, vectors exact, metadata compared structurally.
type State = BTreeMap<String, (Vec<f32>, serde_json::Value)>;



async fn open_at(data_dir: &str, wal: bool) -> CoreTexDB {
    let mut config = DbConfig::new(data_dir);
    config.wal_enabled = wal;
    let db = CoreTexDB::with_config(config);
    db.init().await.expect("init");
    db
}

/// Snapshot a collection's full contents through the public read path.
///
/// Enumeration goes through a search wide enough to cover the collection, then
/// each id is read back individually — so vectors and metadata come from the
/// same storage path a user would see, not from internal state.
async fn observe(db: &CoreTexDB, collection: &str) -> State {
    let schema = db
        .data_manager
        .get_collection(collection)
        .await
        .expect("collection exists");
    let mut out = State::new();
    let hits = db
        .search(collection, vec![0.0; schema.dimension], 65_536, None)
        .await
        .expect("search over the collection");
    for hit in hits {
        // `get_vector` on CoreTexDB already reports a failure as `Err`, and a
        // missing row as `None`; both mean the same thing here — search
        // returned an id that is not readable.
        let (vector, metadata) = match db.get_vector(collection, &hit.id).await {
            Ok(Some(row)) => row,
            other => panic!("id {} came from search but is not readable: {other:?}", hit.id),
        };
        out.insert(
            hit.id,
            (
                vector.iter().map(|v| (v * 1e6).round() / 1e6).collect(),
                metadata,
            ),
        );
    }
    out
}

/// Every metric, so a wrong reconstruction cannot hide behind a lucky search.
const METRICS: [&str; 4] = ["euclidean", "cosine", "dotproduct", "manhattan"];

/// Write one dataset with a deliberately awkward shape: overwrites, deletes,
/// an empty collection, and metadata of several JSON types.
async fn write_dataset(db: &CoreTexDB, collection: &str, metric: &str) {
    db.create_collection_with_index(collection, DIM, metric, "brute_force")
        .await
        .unwrap();

    let rows: Vec<(String, Vec<f32>, serde_json::Value)> = (0..40)
        .map(|i| {
            (
                format!("r{i:03}"),
                (0..DIM).map(|d| ((i * 7 + d * 3) % 23) as f32 / 23.0).collect(),
                match i % 4 {
                    0 => serde_json::json!({"n": i, "flag": true}),
                    1 => serde_json::json!({"n": i, "nested": {"a": [1, 2, 3]}}),
                    2 => serde_json::json!({"n": i, "text": format!("row {i}")}),
                    _ => serde_json::json!({"n": i, "ratio": i as f64 / 4.0}),
                },
            )
        })
        .collect();
    db.insert_vectors(collection, rows).await.unwrap();

    // Overwrite a few, delete a few. The surviving set must be what every
    // restore path reproduces.
    for i in [3usize, 11, 29] {
        db.update_vector(
            collection,
            &format!("r{i:03}"),
            (0..DIM).map(|d| ((i * d) % 17) as f32 / 17.0).collect(),
            Some(serde_json::json!({"n": i, "revised": true})),
        )
        .await
        .unwrap();
    }
    for i in [7usize, 19] {
        db.delete_vectors(collection, &[format!("r{i:03}")])
            .await
            .unwrap();
    }
    db.delete_vectors(collection, &["never-existed".to_string()])
        .await
        .unwrap();
}

/// The ranking a query must produce, as ids in order.
async fn ranking(db: &CoreTexDB, collection: &str, query: Vec<f32>) -> Vec<String> {
    db.search(collection, query, 4096, None)
        .await
        .expect("search")
        .into_iter()
        .map(|h| h.id)
        .collect()
}

fn assert_same(label: &str, expected: &State, actual: &State) {
    assert_eq!(
        expected.keys().collect::<Vec<_>>(),
        actual.keys().collect::<Vec<_>>(),
        "{label}: the set of ids differs"
    );
    for (id, (vector, metadata)) in expected {
        let (other_vector, other_metadata) = actual.get(id).expect("id present");
        assert_eq!(
            vector, other_vector,
            "{label}: vector differs for {id}"
        );
        assert_eq!(
            metadata, other_metadata,
            "{label}: metadata differs for {id}"
        );
    }
}

/// 1. 五条恢复路径对同一份数据必须给出同一状态：本地重启、快照恢复、
///    复制同步、压实日志回放、以及无 WAL 的纯 manifest+存储恢复。
#[tokio::test]
async fn every_restore_path_agrees() {
    for metric in METRICS {
        let label = format!("metric {metric}");
        let root = tempfile::tempdir().unwrap();
        let base = root.path();
        let primary_dir = base.join("primary");
        let primary_dir = primary_dir.to_str().unwrap();

        // The reference: write, then read back from the same live instance.
        let live = open_at(primary_dir, true).await;
        write_dataset(&live, "c", metric).await;
        let expected_state = observe(&live, "c").await;
        let expected_rank = ranking(&live, "c", vec![0.5; DIM]).await;
        assert_eq!(expected_state.len(), 38, "{label}: dataset shape changed");

        // Route 1: plain restart (manifest + storage + WAL replay).
        drop(live);
        let restarted = open_at(primary_dir, true).await;
        assert_same(
            &format!("{label} / restart"),
            &expected_state,
            &observe(&restarted, "c").await,
        );
        assert_eq!(
            expected_rank,
            ranking(&restarted, "c", vec![0.5; DIM]).await,
            "{label}: restart changed the ranking"
        );

        // Route 2: snapshot restore into a fresh node.
        let snap_root = tempfile::tempdir().unwrap();
        let archive = SnapshotArchive::open(snap_root.path()).await.unwrap();
        archive.save(&restarted, "s1").await.unwrap();
        let snapshot_dir = base.join("from-snapshot");
        let from_snapshot = open_at(snapshot_dir.to_str().unwrap(), true).await;
        archive.restore_into(&from_snapshot, "s1").await.unwrap();
        assert_same(
            &format!("{label} / snapshot"),
            &expected_state,
            &observe(&from_snapshot, "c").await,
        );
        assert_eq!(
            expected_rank,
            ranking(&from_snapshot, "c", vec![0.5; DIM]).await,
            "{label}: snapshot changed the ranking"
        );

        // Route 3: replication into a fresh replica.
        let replica_dir = base.join("replica");
        let replica = Arc::new(open_at(replica_dir.to_str().unwrap(), true).await);
        let sync = ReplicaSync::new(
            replica.clone(),
            Arc::new(InProcessTransport::new(Arc::new(
                open_at(primary_dir, true).await,
            ))),
            replica_dir.join("replica_state.json"),
        );
        sync.sync_once().await.expect("full sync");
        assert_same(
            &format!("{label} / replication"),
            &expected_state,
            &observe(&replica, "c").await,
        );
        assert_eq!(
            expected_rank,
            ranking(&replica, "c", vec![0.5; DIM]).await,
            "{label}: replication changed the ranking"
        );

        // Route 4: replay a compacted log into a fresh node.
        let compact_root = tempfile::tempdir().unwrap();
        let compact_target = compact_root.path().join("wal");
        let report = compact_wal(&restarted, &compact_target)
            .await
            .unwrap_or_else(|e| panic!("{label}: compaction failed: {e}"));
        assert!(
            report.entries_after <= report.entries_before,
            "{label}: compaction grew the log"
        );

        let replay_dir = base.join("from-compacted");
        let mut replay_config = DbConfig::new(replay_dir.to_str().unwrap());
        replay_config.wal_enabled = true;
        replay_config.wal_dir = compact_target.to_string_lossy().to_string();
        let replayed = CoreTexDB::with_config(replay_config);
        replayed.init().await.expect("replay init");
        assert_same(
            &format!("{label} / compacted log"),
            &expected_state,
            &observe(&replayed, "c").await,
        );

        // Route 5: manifest + storage with no WAL at all.
        let nowal_dir = base.join("no-wal");
        let no_wal = open_at(nowal_dir.to_str().unwrap(), false).await;
        write_dataset(&no_wal, "c", metric).await;
        let no_wal_state = observe(&no_wal, "c").await;
        drop(no_wal);
        let no_wal_reopened = open_at(nowal_dir.to_str().unwrap(), false).await;
        assert_same(
            &format!("{label} / no WAL"),
            &no_wal_state,
            &observe(&no_wal_reopened, "c").await,
        );
        assert_same(
            &format!("{label} / WAL vs no WAL"),
            &expected_state,
            &no_wal_state,
        );
    }
}

/// 2. 写入形状的差分：同样的最终状态，不管用什么写法达成，恢复后必须一致。
#[tokio::test]
async fn different_write_shapes_recover_identically() {
    let root = tempfile::tempdir().unwrap();

    // (a) one bulk insert.
    let bulk_dir = root.path().join("bulk");
    let bulk = open_at(bulk_dir.to_str().unwrap(), true).await;
    bulk.create_collection_with_index("c", DIM, "cosine", "brute_force")
        .await
        .unwrap();
    let rows: Vec<(String, Vec<f32>, serde_json::Value)> = (0..20)
        .map(|i| {
            (
                format!("r{i:02}"),
                (0..DIM).map(|d| ((i + d) % 11) as f32 / 11.0).collect(),
                serde_json::json!({"i": i}),
            )
        })
        .collect();
    bulk.insert_vectors("c", rows.clone()).await.unwrap();
    let expected = observe(&bulk, "c").await;

    // (b) the same rows one at a time.
    let single_dir = root.path().join("single");
    let single = open_at(single_dir.to_str().unwrap(), true).await;
    single.create_collection_with_index("c", DIM, "cosine", "brute_force")
        .await
        .unwrap();
    for row in &rows {
        single
            .insert_vectors("c", vec![(row.0.clone(), row.1.clone(), row.2.clone())])
            .await
            .unwrap();
    }
    assert_same(
        "bulk vs single writes",
        &expected,
        &observe(&single, "c").await,
    );

    // (c) the same rows via upsert.
    let upsert_dir = root.path().join("upsert");
    let upsert = open_at(upsert_dir.to_str().unwrap(), true).await;
    upsert.create_collection_with_index("c", DIM, "cosine", "brute_force")
        .await
        .unwrap();
    upsert.upsert_vectors("c", rows.clone()).await.unwrap();
    assert_same(
        "bulk vs upsert writes",
        &expected,
        &observe(&upsert, "c").await,
    );

    // Each must also survive its own restart identically.
    drop(bulk);
    drop(single);
    drop(upsert);
    for (name, dir) in [
        ("bulk", &bulk_dir),
        ("single", &single_dir),
        ("upsert", &upsert_dir),
    ] {
        let reopened = open_at(dir.to_str().unwrap(), true).await;
        assert_same(
            &format!("{name} after restart"),
            &expected,
            &observe(&reopened, "c").await,
        );
    }
}

/// 3. 增量复制对拍：逐条同步与批量同步必须收敛到同一状态。
#[tokio::test]
async fn incremental_and_bulk_replication_converge() {
    let root = tempfile::tempdir().unwrap();
    let primary = Arc::new(open_at(root.path().join("primary").to_str().unwrap(), true).await);
    primary
        .create_collection_with_index("c", DIM, "euclidean", "brute_force")
        .await
        .unwrap();

    // Replica A syncs after every write; replica B syncs once at the end.
    let bulk_dir = root.path().join("replica-bulk");
    let bulk = Arc::new(open_at(bulk_dir.to_str().unwrap(), true).await);
    let bulk_sync = ReplicaSync::new(
        bulk.clone(),
        Arc::new(InProcessTransport::new(primary.clone())),
        bulk_dir.join("state.json"),
    );
    bulk_sync.sync_once().await.unwrap();

    let stepwise_dir = root.path().join("replica-stepwise");
    let stepwise = Arc::new(open_at(stepwise_dir.to_str().unwrap(), true).await);
    let stepwise_sync = ReplicaSync::new(
        stepwise.clone(),
        Arc::new(InProcessTransport::new(primary.clone())),
        stepwise_dir.join("state.json"),
    );

    for i in 0..15 {
        primary
            .insert_vectors(
                "c",
                vec![(
                    format!("r{i:02}"),
                    (0..DIM).map(|d| ((i * d) % 13) as f32 / 13.0).collect(),
                    serde_json::json!({"i": i}),
                )],
            )
            .await
            .unwrap();
        stepwise_sync.sync_once().await.unwrap();
    }
    // A delete in the middle, also applied stepwise.
    primary
        .delete_vectors("c", &["r05".to_string()])
        .await
        .unwrap();
    stepwise_sync.sync_once().await.unwrap();

    bulk_sync.sync_once().await.unwrap();

    let expected = observe(&primary, "c").await;
    assert_same("stepwise", &expected, &observe(&stepwise, "c").await);
    assert_same("bulk", &expected, &observe(&bulk, "c").await);
    assert_eq!(
        ranking(&stepwise, "c", vec![0.4; DIM]).await,
        ranking(&bulk, "c", vec![0.4; DIM]).await,
        "replicas disagree on the ranking"
    );
}

