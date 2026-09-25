//! End-to-end tests for the `pq` (product quantization) index type: it must be
//! selectable for a collection, accept inserts and answer searches both before
//! and after the lazy training run.

use coretexdb::{CoreTexDB, DbConfig};

const DIM: usize = 8;

/// An 8-dim vector whose weight lives in the first component.
fn vector(i: f32) -> Vec<f32> {
    let mut v = vec![0.0; DIM];
    v[0] = i;
    v[1] = 1.0;
    v
}

async fn open(path: &str) -> CoreTexDB {
    let db = CoreTexDB::with_config(DbConfig::new(path));
    db.init().await.expect("init");
    db
}

async fn seed(db: &CoreTexDB, count: u32) {
    let rows: Vec<(String, Vec<f32>, serde_json::Value)> = (0..count)
        .map(|i| (format!("v{i}"), vector(i as f32), serde_json::json!({})))
        .collect();
    db.insert_vectors("c", rows).await.unwrap();
}

/// `"pq"` must select the PQ engine instead of silently falling back to brute
/// force, and the collection must accept writes: the old index rejected every
/// insert with "Index not trained", because nothing ever called `train()`.
#[tokio::test]
async fn pq_collection_accepts_inserts_and_searches() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    let db = open(&path).await;

    db.create_collection_with_index("c", DIM, "cosine", "pq")
        .await
        .unwrap();

    seed(&db, 24).await;
    assert_eq!(db.get_vectors_count("c").await.unwrap(), 24);

    let hits = db.search("c", vector(7.0), 3, None).await.unwrap();
    assert_eq!(hits.len(), 3, "search must return k rows");
    assert_eq!(hits[0].id, "v7", "the exact match must rank first");
}

/// A small collection never reaches the training threshold — it must still
/// answer correctly by scanning the buffered vectors.
#[tokio::test]
async fn pq_collection_with_few_vectors_is_still_searchable() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    let db = open(&path).await;

    db.create_collection_with_index("c", DIM, "cosine", "pq")
        .await
        .unwrap();

    seed(&db, 3).await;

    let hits = db.search("c", vector(1.0), 3, None).await.unwrap();
    assert_eq!(hits.len(), 3);
    assert_eq!(hits[0].id, "v1");
}

/// The chosen engine must reach the persisted file: `parse_index_type` used to
/// map every unknown name to brute force, so `"pq"` never reached PQ at all.
#[tokio::test]
async fn pq_collection_persists_a_pq_index() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();

    {
        let db = open(&path).await;
        db.create_collection_with_index("c", DIM, "cosine", "pq")
            .await
            .unwrap();
        seed(&db, 24).await;
        assert_eq!(db.save_indexes().await.unwrap(), 1, "one pq index written");
    }

    let index_dir = std::path::Path::new(&path)
        .join("data")
        .join("coretex")
        .join("indexes")
        .join("vector");
    let file = std::fs::read_dir(&index_dir)
        .unwrap()
        .flatten()
        .next()
        .expect("an index file must exist")
        .path();

    let raw = std::fs::read_to_string(&file).unwrap();
    let envelope: serde_json::Value = serde_json::from_str(&raw).unwrap();
    assert_eq!(
        envelope["index_type"].as_str(),
        Some("pq"),
        "the file must record the pq engine, got: {raw}"
    );
}
