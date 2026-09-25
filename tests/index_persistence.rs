//! End-to-end tests for ANN index persistence: an `hnsw`/`ivf`/`pq` index
//! written by `save_indexes` must survive a restart, while a stale one must be
//! transparently rebuilt from storage.

use coretexdb::{CoreTexDB, DbConfig};

const QUERY: [f32; 3] = [1.0, 0.1, 0.0];

async fn open(path: &str) -> CoreTexDB {
    let db = CoreTexDB::with_config(DbConfig::new(path));
    db.init().await.expect("init");
    db
}

async fn seed_hnsw(db: &CoreTexDB) {
    db.create_collection_with_index("c", 3, "cosine", "hnsw")
        .await
        .unwrap();
    db.insert_vectors(
        "c",
        vec![
            ("p".into(), vec![1.0, 0.0, 0.0], serde_json::json!({})),
            ("q".into(), vec![0.0, 1.0, 0.0], serde_json::json!({})),
            ("r".into(), vec![1.0, 1.0, 0.0], serde_json::json!({})),
        ],
    )
    .await
    .unwrap();
}

fn index_files(data_dir: &str) -> Vec<std::path::PathBuf> {
    // `data_dir` is the install root; the config maps it to `<root>/data/coretex`.
    let dir = std::path::Path::new(data_dir)
        .join("data")
        .join("coretex")
        .join("indexes")
        .join("vector");
    std::fs::read_dir(&dir)
        .map(|entries| entries.flatten().map(|e| e.path()).collect())
        .unwrap_or_default()
}

/// The saved index must be *loaded* on restart, not rebuilt. Proven by
/// tampering the persisted vectors: the checksum is derived from storage (not
/// from the index file), so the tampered file still validates; if search then
/// reflects the tampered data, the file was used.
#[tokio::test]
async fn hnsw_index_is_loaded_on_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();

    {
        let db = open(&path).await;
        seed_hnsw(&db).await;
        assert_eq!(db.save_indexes().await.unwrap(), 1, "one hnsw index written");
    }

    let files = index_files(&path);
    assert_eq!(files.len(), 1, "expected exactly one index file");

    let file = &files[0];
    let mut root: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(file).unwrap()).unwrap();
    // Make `q` the exact query vector. A rebuilt index would rank `p` first.
    root["data"]["vectors"]["q"] = serde_json::json!(QUERY);
    std::fs::write(file, serde_json::to_string(&root).unwrap()).unwrap();

    let db = open(&path).await;
    let hits = db.search("c", QUERY.to_vec(), 1, None).await.unwrap();
    assert_eq!(
        hits[0].id, "q",
        "persisted index must be loaded instead of rebuilt"
    );
}

/// A stale index (data changed since it was written) has a mismatching
/// checksum and must be rebuilt — so a vector added after the save is still
/// searchable.
#[tokio::test]
async fn stale_index_is_rebuilt_on_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();

    {
        let db = open(&path).await;
        seed_hnsw(&db).await;
        assert_eq!(db.save_indexes().await.unwrap(), 1);

        // Mutate the data after persisting: the file is now stale.
        db.insert_vectors(
            "c",
            vec![("late".into(), QUERY.to_vec(), serde_json::json!({}))],
        )
        .await
        .unwrap();
    }

    let db = open(&path).await;
    assert_eq!(db.get_vectors_count("c").await.unwrap(), 4);

    let hits = db.search("c", QUERY.to_vec(), 4, None).await.unwrap();
    assert!(
        hits.iter().any(|h| h.id == "late"),
        "stale index must be rebuilt from storage, got {:?}",
        hits.iter().map(|h| &h.id).collect::<Vec<_>>()
    );
}

/// The exact default index has nothing worth persisting, so it writes no file.
#[tokio::test]
async fn brute_force_collection_persists_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();

    let db = open(&path).await;
    db.create_collection("c", 3, "cosine").await.unwrap();
    db.insert_vectors(
        "c",
        vec![("p".into(), vec![1.0, 0.0, 0.0], serde_json::json!({}))],
    )
    .await
    .unwrap();

    assert_eq!(db.save_indexes().await.unwrap(), 0);
    assert!(index_files(&path).is_empty());
    // And a restart still returns correct results via the rebuild path.
    drop(db);
    let db = open(&path).await;
    let hits = db.search("c", vec![1.0, 0.0, 0.0], 1, None).await.unwrap();
    assert_eq!(hits[0].id, "p");
}
