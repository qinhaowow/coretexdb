//! End-to-end tests for the durable core: persistence across restarts, and
//! search correctness measured against an independent brute-force reference.
//!
//! The reference implementation below deliberately shares no code with the
//! engine. If both were wrong in the same way the tests would pass; by
//! recomputing the expected neighbours from first principles we get a real
//! differential check (the "对拍基线").

use coretexdb::{CoreTexDB, DbConfig, IndexType};

/// Query vector used throughout.
const QUERY: [f32; 3] = [1.0, 0.1, 0.0];

/// The corpus. Chosen so that every metric produces a strict, tie-free
/// ordering, and so that cosine and dotproduct disagree about the winner —
/// which is what makes the differential check able to catch a metric that is
/// being ignored.
fn corpus() -> Vec<(&'static str, [f32; 3], serde_json::Value)> {
    vec![
        ("p", [1.0, 0.0, 0.0], serde_json::json!({"group": "x", "rank": 1})),
        ("q", [0.0, 1.0, 0.0], serde_json::json!({"group": "y", "rank": 2})),
        ("r", [1.0, 1.0, 0.0], serde_json::json!({"group": "x", "rank": 3})),
        ("s", [1.5, 0.0, 0.5], serde_json::json!({"group": "x", "rank": 4})),
    ]
}

// ---------------------------------------------------------------------------
// Independent reference implementation
// ---------------------------------------------------------------------------

fn reference_distance(metric: &str, a: &[f32], b: &[f32]) -> f32 {
    match metric {
        "cosine" => {
            let dot: f32 = a.iter().zip(b).map(|(x, y)| x * y).sum();
            let na: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
            let nb: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
            1.0 - dot / (na * nb)
        }
        "euclidean" => a
            .iter()
            .zip(b)
            .map(|(x, y)| (x - y) * (x - y))
            .sum::<f32>()
            .sqrt(),
        "dotproduct" => -a.iter().zip(b).map(|(x, y)| x * y).sum::<f32>(),
        "manhattan" => a.iter().zip(b).map(|(x, y)| (x - y).abs()).sum(),
        other => panic!("unknown metric {}", other),
    }
}

/// Expected ranking of the corpus for `metric`, best first.
fn expected_order(metric: &str) -> Vec<&'static str> {
    let mut scored: Vec<(&'static str, f32)> = corpus()
        .into_iter()
        .map(|(id, vector, _)| (id, reference_distance(metric, &QUERY, &vector)))
        .collect();
    scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
    scored.into_iter().map(|(id, _)| id).collect()
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn open(path: &str) -> CoreTexDB {
    let db = CoreTexDB::with_config(DbConfig::new(path));
    db.init().await.expect("init");
    db
}

async fn seed_for_metric(db: &CoreTexDB, metric: &str) -> String {
    let collection = format!("c_{}", metric);
    db.create_collection(&collection, 3, metric).await.unwrap();
    let vectors: Vec<(String, Vec<f32>, serde_json::Value)> = corpus()
        .into_iter()
        .map(|(id, vector, metadata)| (id.to_string(), vector.to_vec(), metadata))
        .collect();
    db.insert_vectors(&collection, vectors).await.unwrap();
    collection
}

// ---------------------------------------------------------------------------
// Persistence
// ---------------------------------------------------------------------------

#[tokio::test]
async fn collections_and_vectors_survive_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();

    {
        let db = open(&path).await;
        let collection = seed_for_metric(&db, "cosine").await;
        assert_eq!(db.get_vectors_count(&collection).await.unwrap(), 4);
    }

    // A brand new process-equivalent: only the directory is shared.
    let db = open(&path).await;
    assert_eq!(db.list_collections().await.unwrap(), vec!["c_cosine".to_string()]);

    let schema = db.get_collection("c_cosine").await.unwrap();
    assert_eq!(schema.dimension, 3);
    assert_eq!(db.get_vectors_count("c_cosine").await.unwrap(), 4);

    let (vector, metadata) = db.get_vector("c_cosine", "r").await.unwrap().unwrap();
    assert_eq!(vector, vec![1.0, 1.0, 0.0]);
    assert_eq!(metadata, serde_json::json!({"group": "x", "rank": 3}));
}

#[tokio::test]
async fn metadata_json_is_a_readable_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();

    let db = open(&path).await;
    db.create_collection("alpha", 8, "euclidean").await.unwrap();

    let raw = std::fs::read_to_string(dir.path().join("data").join("metadata.json")).unwrap();
    let manifest: serde_json::Value = serde_json::from_str(&raw).unwrap();

    assert_eq!(manifest["collections"], serde_json::json!(["alpha"]));
    assert_eq!(manifest["schemas"][0]["name"], "alpha");
    assert_eq!(manifest["schemas"][0]["dimension"], 8);
    assert_eq!(manifest["schemas"][0]["distance_metric"], "Euclidean");
}

#[tokio::test]
async fn deleting_a_collection_drops_its_vectors_for_good() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();

    {
        let db = open(&path).await;
        seed_for_metric(&db, "cosine").await;
        db.delete_collection("c_cosine").await.unwrap();
        assert!(db.list_collections().await.unwrap().is_empty());
    }

    // Recreating the same name must not resurrect the deleted vectors.
    let db = open(&path).await;
    db.create_collection("c_cosine", 3, "cosine").await.unwrap();
    assert_eq!(db.get_vectors_count("c_cosine").await.unwrap(), 0);
}

#[tokio::test]
async fn renames_are_persisted() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();

    {
        let db = open(&path).await;
        seed_for_metric(&db, "cosine").await;
        db.rename_collection("c_cosine", "renamed").await.unwrap();
    }

    let db = open(&path).await;
    assert_eq!(db.list_collections().await.unwrap(), vec!["renamed".to_string()]);
    assert_eq!(db.get_vectors_count("renamed").await.unwrap(), 4);
    let (vector, _) = db.get_vector("renamed", "s").await.unwrap().unwrap();
    assert_eq!(vector, vec![1.5, 0.0, 0.5]);
}

#[tokio::test]
async fn dimension_mismatch_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path().to_str().unwrap()).await;
    db.create_collection("c", 3, "cosine").await.unwrap();

    let bad = vec![("x".to_string(), vec![1.0, 2.0], serde_json::json!({}))];
    assert!(db.insert_vectors("c", bad).await.is_err());
}

// ---------------------------------------------------------------------------
// Search correctness (differential vs the reference implementation)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn indexed_search_matches_brute_force_for_every_metric() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path().to_str().unwrap()).await;

    for metric in ["cosine", "euclidean", "dotproduct", "manhattan"] {
        let collection = seed_for_metric(&db, metric).await;
        let expected = expected_order(metric);

        let results = db
            .search(&collection, QUERY.to_vec(), 4, None)
            .await
            .unwrap();
        let got: Vec<&str> = results.iter().map(|r| r.id.as_str()).collect();

        assert_eq!(
            got,
            expected,
            "{}: index path disagrees with the brute-force reference",
            metric
        );
    }
}

#[tokio::test]
async fn scan_path_matches_brute_force_for_every_metric() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path().to_str().unwrap()).await;

    for metric in ["cosine", "euclidean", "dotproduct", "manhattan"] {
        let collection = seed_for_metric(&db, metric).await;
        let expected = expected_order(metric);

        // A non-selective filter routes the query through the exact scan.
        let results = db
            .search(&collection, QUERY.to_vec(), 4, Some(serde_json::json!({})))
            .await
            .unwrap();
        let got: Vec<&str> = results.iter().map(|r| r.id.as_str()).collect();

        assert_eq!(
            got,
            expected,
            "{}: scan path disagrees with the brute-force reference",
            metric
        );
    }
}

#[tokio::test]
async fn search_correctness_is_preserved_across_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();

    {
        let db = open(&path).await;
        for metric in ["cosine", "euclidean", "dotproduct", "manhattan"] {
            seed_for_metric(&db, metric).await;
        }
    }

    // The indexes are rebuilt from the log, so this is where a restore bug hides.
    let db = open(&path).await;
    for metric in ["cosine", "euclidean", "dotproduct", "manhattan"] {
        let collection = format!("c_{}", metric);
        let expected = expected_order(metric);

        let results = db
            .search(&collection, QUERY.to_vec(), 4, None)
            .await
            .unwrap();
        let got: Vec<&str> = results.iter().map(|r| r.id.as_str()).collect();

        assert_eq!(
            got,
            expected,
            "{}: results after restart disagree with the reference",
            metric
        );
    }
}

#[tokio::test]
async fn filter_is_honoured_and_returns_k_matches_when_they_exist() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path().to_str().unwrap()).await;
    db.create_collection("c", 3, "euclidean").await.unwrap();

    let vectors: Vec<(String, Vec<f32>, serde_json::Value)> = corpus()
        .into_iter()
        .map(|(id, vector, metadata)| (id.to_string(), vector.to_vec(), metadata))
        .collect();
    db.insert_vectors("c", vectors).await.unwrap();

    // group "x" holds 3 of the 4 vectors.
    let results = db
        .search("c", QUERY.to_vec(), 3, Some(serde_json::json!({"group": "x"})))
        .await
        .unwrap();
    assert_eq!(results.len(), 3, "a non-selective filter must still fill k");

    let mut ids: Vec<&str> = results.iter().map(|r| r.id.as_str()).collect();
    ids.sort_unstable();
    assert_eq!(ids, vec!["p", "r", "s"]);

    // A selective filter must not be diluted by non-matching neighbours.
    let results = db
        .search("c", QUERY.to_vec(), 5, Some(serde_json::json!({"group": "y"})))
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].id, "q");

    // Comparison operators work too.
    let results = db
        .search("c", QUERY.to_vec(), 5, Some(serde_json::json!({"rank": {"$gte": 3}})))
        .await
        .unwrap();
    let mut ids: Vec<&str> = results.iter().map(|r| r.id.as_str()).collect();
    ids.sort_unstable();
    assert_eq!(ids, vec!["r", "s"]);
}

#[tokio::test]
async fn updates_and_deletes_are_visible_to_search() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path().to_str().unwrap()).await;
    db.create_collection("c", 3, "euclidean").await.unwrap();

    let vectors: Vec<(String, Vec<f32>, serde_json::Value)> = corpus()
        .into_iter()
        .map(|(id, vector, metadata)| (id.to_string(), vector.to_vec(), metadata))
        .collect();
    db.insert_vectors("c", vectors).await.unwrap();

    // Move "q" (the worst match) to be an exact copy of the query.
    db.update_vector("c", "q", QUERY.to_vec(), None).await.unwrap();
    let results = db.search("c", QUERY.to_vec(), 1, None).await.unwrap();
    assert_eq!(results[0].id, "q");

    db.delete_vectors("c", &["q".to_string()]).await.unwrap();
    assert_eq!(db.get_vectors_count("c").await.unwrap(), 3);
    let results = db.search("c", QUERY.to_vec(), 4, None).await.unwrap();
    assert!(results.iter().all(|r| r.id != "q"));
}

#[tokio::test]
async fn hnsw_matches_brute_force_on_a_small_collection() {
    // HNSW is approximate in general, but when the whole dataset fits inside the
    // search's candidate set it must agree with an exact scan exactly. This
    // guards the graph-construction invariants (closest-`m` neighbour selection,
    // highest-level entry point, geometric level distribution).
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path().to_str().unwrap()).await;

    for metric in ["cosine", "euclidean", "dotproduct", "manhattan"] {
        let collection = format!("h_{}", metric);
        db.create_collection_with_index(&collection, 3, metric, "hnsw")
            .await
            .unwrap();

        let vectors: Vec<(String, Vec<f32>, serde_json::Value)> = corpus()
            .into_iter()
            .map(|(id, vector, metadata)| (id.to_string(), vector.to_vec(), metadata))
            .collect();
        db.insert_vectors(&collection, vectors).await.unwrap();

        let results = db.search(&collection, QUERY.to_vec(), 4, None).await.unwrap();
        let got: Vec<&str> = results.iter().map(|r| r.id.as_str()).collect();
        assert_eq!(
            got,
            expected_order(metric),
            "{}: hnsw disagrees with the brute-force reference",
            metric
        );
    }
}

#[tokio::test]
async fn index_type_is_recorded_and_survives_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();

    {
        let db = open(&path).await;
        db.create_collection_with_index("h", 3, "cosine", "hnsw").await.unwrap();
        db.create_collection_with_index("b", 3, "cosine", "brute_force")
            .await
            .unwrap();
        assert_eq!(
            db.get_collection("h").await.unwrap().indexes[0].index_type,
            IndexType::HNSW
        );
    }

    // The choice is read back from the manifest, so the rebuilt index is the
    // same kind the collection was created with.
    let db = open(&path).await;
    assert_eq!(
        db.get_collection("h").await.unwrap().indexes[0].index_type,
        IndexType::HNSW
    );
    assert_eq!(
        db.get_collection("b").await.unwrap().indexes[0].index_type,
        IndexType::BruteForce
    );
}

#[tokio::test]
async fn create_collection_defaults_to_the_exact_index() {
    // G6: the default must be exact, so a collection created without naming an
    // index never silently loses accuracy. The CLI resolves its `--index`
    // default from the same constant, so this also pins the CLI's behaviour.
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path().to_str().unwrap()).await;
    db.create_collection("c", 3, "cosine").await.unwrap();
    assert_eq!(
        db.get_collection("c").await.unwrap().indexes[0].index_type,
        IndexType::BruteForce
    );
}

#[tokio::test]
async fn an_unknown_index_type_falls_back_to_the_exact_index() {
    // A typo must not silently trade accuracy for speed.
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path().to_str().unwrap()).await;
    db.create_collection_with_index("c", 3, "cosine", "hnsww").await.unwrap();
    assert_eq!(
        db.get_collection("c").await.unwrap().indexes[0].index_type,
        IndexType::BruteForce
    );
}

#[tokio::test]
async fn persists_across_many_restarts() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();

    for round in 0..5 {
        let db = open(&path).await;
        if round == 0 {
            db.create_collection("c", 3, "euclidean").await.unwrap();
        }
        db.insert_vectors(
            "c",
            vec![(format!("v{}", round), vec![round as f32, 0.0, 0.0], serde_json::json!({"round": round}))],
        )
        .await
        .unwrap();
        assert_eq!(db.get_vectors_count("c").await.unwrap(), round + 1);
    }
}
