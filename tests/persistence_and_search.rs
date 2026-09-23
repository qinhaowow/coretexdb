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

    let raw = std::fs::read_to_string(
        dir.path()
            .join("data")
            .join("coretex")
            .join("metadata")
            .join("metadata.json"),
    )
    .unwrap();
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

// ---------------------------------------------------------------------------
// Data operations: update / upsert / bulk
//
// These paths used to write only into the in-memory map, so a bulk write was
// invisible to search and gone after a restart. Each test therefore asserts
// durability (a fresh instance reads it back) *and* index visibility (search
// finds it), because passing one without the other is exactly the old bug.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn updates_survive_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();

    {
        let db = open(&path).await;
        seed_for_metric(&db, "euclidean").await;

        // Replace "q" with an exact copy of the query and rewrite its metadata.
        let changed = db
            .update_vector(
                "c_euclidean",
                "q",
                QUERY.to_vec(),
                Some(serde_json::json!({"group": "rewritten", "rank": 9})),
            )
            .await
            .unwrap();
        assert!(changed);
    }

    let db = open(&path).await;
    let (vector, metadata) = db.get_vector("c_euclidean", "q").await.unwrap().unwrap();
    assert_eq!(vector, QUERY.to_vec(), "updated vector must be durable");
    assert_eq!(
        metadata,
        serde_json::json!({"group": "rewritten", "rank": 9}),
        "updated metadata must be durable"
    );

    // The replayed log must end up with the newest record for the key, so the
    // updated vector wins the ranking instead of the one it replaced.
    let results = db.search("c_euclidean", QUERY.to_vec(), 1, None).await.unwrap();
    assert_eq!(results[0].id, "q");
}

#[tokio::test]
async fn upsert_updates_existing_and_inserts_new() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path().to_str().unwrap()).await;
    let collection = seed_for_metric(&db, "euclidean").await;

    let (inserted, updated) = db
        .upsert_vectors(
            &collection,
            vec![
                ("brand_new".to_string(), QUERY.to_vec(), serde_json::json!({"n": 1})),
                ("p".to_string(), QUERY.to_vec(), serde_json::json!({"n": 2})),
            ],
        )
        .await
        .unwrap();

    assert_eq!(inserted, vec!["brand_new".to_string()]);
    assert_eq!(updated, vec!["p".to_string()]);
    assert_eq!(db.get_vectors_count(&collection).await.unwrap(), 5);

    // Both halves must reach the index, not just the in-memory map.
    let results = db.search(&collection, QUERY.to_vec(), 2, None).await.unwrap();
    let ids: std::collections::HashSet<&str> = results.iter().map(|r| r.id.as_str()).collect();
    assert_eq!(ids.len(), 2, "upsert must be visible to search");
    assert!(ids.contains("brand_new"));
    assert!(ids.contains("p"));
}

#[tokio::test]
async fn bulk_insert_is_searchable_and_durable() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();

    {
        let db = open(&path).await;
        db.create_collection("c", 3, "euclidean").await.unwrap();

        let ids = db
            .bulk_insert(
                "c",
                vec![
                    ("a".to_string(), vec![1.0, 0.0, 0.0], serde_json::json!({"v": 1})),
                    ("b".to_string(), vec![0.0, 1.0, 0.0], serde_json::json!({"v": 1})),
                ],
            )
            .await
            .unwrap();
        assert_eq!(ids, vec!["a".to_string(), "b".to_string()]);

        let results = db.search("c", vec![1.0, 0.0, 0.0], 1, None).await.unwrap();
        assert_eq!(results[0].id, "a", "bulk_insert must reach the index");
    }

    let db = open(&path).await;
    assert_eq!(db.get_vectors_count("c").await.unwrap(), 2);
    assert_eq!(
        db.get_vector("c", "b").await.unwrap().unwrap().1,
        serde_json::json!({"v": 1})
    );
}

#[tokio::test]
async fn bulk_update_skips_missing_ids_and_is_durable() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();

    {
        let db = open(&path).await;
        seed_for_metric(&db, "euclidean").await;

        let updated = db
            .bulk_update(
                "c_euclidean",
                vec![
                    ("q".to_string(), QUERY.to_vec(), serde_json::json!({"group": "moved"})),
                    ("nope".to_string(), vec![0.0, 0.0, 1.0], serde_json::json!({})),
                ],
            )
            .await
            .unwrap();
        assert_eq!(updated, vec!["q".to_string()], "missing ids must be skipped");

        let results = db.search("c_euclidean", QUERY.to_vec(), 1, None).await.unwrap();
        assert_eq!(results[0].id, "q");
    }

    let db = open(&path).await;
    let (vector, metadata) = db.get_vector("c_euclidean", "q").await.unwrap().unwrap();
    assert_eq!(vector, QUERY.to_vec(), "bulk_update must be durable");
    assert_eq!(metadata, serde_json::json!({"group": "moved"}));
    assert_eq!(db.get_vectors_count("c_euclidean").await.unwrap(), 4);
}

#[tokio::test]
async fn bulk_delete_reports_present_ids_and_is_durable() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();

    {
        let db = open(&path).await;
        seed_for_metric(&db, "euclidean").await;

        let deleted = db
            .bulk_delete(
                "c_euclidean",
                vec!["q".to_string(), "nope".to_string(), "q".to_string()],
            )
            .await
            .unwrap();
        assert_eq!(deleted, vec!["q".to_string()]);
        assert_eq!(db.get_vectors_count("c_euclidean").await.unwrap(), 3);

        let results = db.search("c_euclidean", QUERY.to_vec(), 4, None).await.unwrap();
        assert!(results.iter().all(|r| r.id != "q"), "bulk_delete must reach the index");
    }

    let db = open(&path).await;
    assert_eq!(db.get_vectors_count("c_euclidean").await.unwrap(), 3);
    assert!(db.get_vector("c_euclidean", "q").await.unwrap().is_none());
}

#[tokio::test]
async fn bulk_upsert_partitions_and_is_durable() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();

    {
        let db = open(&path).await;
        seed_for_metric(&db, "euclidean").await;

        let result = db
            .bulk_upsert(
                "c_euclidean",
                vec![
                    ("p".to_string(), QUERY.to_vec(), serde_json::json!({"n": 1})),
                    ("fresh".to_string(), vec![2.0, 2.0, 2.0], serde_json::json!({"n": 1})),
                    ("fresh".to_string(), vec![3.0, 3.0, 3.0], serde_json::json!({"n": 2})),
                ],
            )
            .await
            .unwrap();

        assert_eq!(result.inserted, vec!["fresh".to_string()]);
        assert_eq!(
            result.updated,
            vec!["p".to_string(), "fresh".to_string()],
            "a repeated id in one batch inserts once then updates"
        );
        assert_eq!(db.get_vectors_count("c_euclidean").await.unwrap(), 5);
    }

    let db = open(&path).await;
    let (vector, metadata) = db.get_vector("c_euclidean", "fresh").await.unwrap().unwrap();
    assert_eq!(vector, vec![3.0, 3.0, 3.0], "bulk_upsert must be durable");
    assert_eq!(metadata, serde_json::json!({"n": 2}));

    let (vector, _) = db.get_vector("c_euclidean", "p").await.unwrap().unwrap();
    assert_eq!(vector, QUERY.to_vec());
}

// ---------------------------------------------------------------------------
// Non-ASCII text
//
// Collection names, vector ids and metadata values all become part of a log
// record key and of the manifest, so any byte/char confusion shows up here as a
// panic or as mojibake rather than as a quietly wrong answer.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn chinese_text_round_trips_without_mojibake() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();

    {
        let db = open(&path).await;
        db.create_collection("商品库", 3, "euclidean").await.unwrap();
        db.insert_vectors(
            "商品库",
            vec![
                (
                    "苹果-001".to_string(),
                    vec![1.0, 0.0, 0.0],
                    serde_json::json!({"名称": "红富士苹果", "类别": "水果"}),
                ),
                (
                    "绿茶-002".to_string(),
                    vec![0.0, 1.0, 0.0],
                    serde_json::json!({"名称": "龙井绿茶", "类别": "饮料"}),
                ),
            ],
        )
        .await
        .unwrap();
    }

    let db = open(&path).await;
    assert_eq!(db.list_collections().await.unwrap(), vec!["商品库".to_string()]);

    let (vector, metadata) = db.get_vector("商品库", "苹果-001").await.unwrap().unwrap();
    assert_eq!(vector, vec![1.0, 0.0, 0.0]);
    assert_eq!(metadata["名称"], "红富士苹果");

    // Filtering on a Chinese value must work through the exact-scan path.
    let results = db
        .search(
            "商品库",
            vec![1.0, 0.0, 0.0],
            5,
            Some(serde_json::json!({"类别": "水果"})),
        )
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].id, "苹果-001");

    // A delete reports the multibyte id back verbatim.
    let deleted = db.bulk_delete("商品库", vec!["绿茶-002".to_string()]).await.unwrap();
    assert_eq!(deleted, vec!["绿茶-002".to_string()]);

    // The manifest must be valid, unescaped UTF-8 on disk.
    let raw = std::fs::read(
        dir.path()
            .join("data")
            .join("coretex")
            .join("metadata")
            .join("metadata.json"),
    )
    .unwrap();
    let text = std::str::from_utf8(&raw).expect("manifest must be valid UTF-8");
    let manifest: serde_json::Value = serde_json::from_str(text).unwrap();
    assert_eq!(manifest["collections"][0], "商品库");
}

#[tokio::test]
async fn mixed_ascii_and_chinese_ids_stay_distinct() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path().to_str().unwrap()).await;
    db.create_collection("mixed", 3, "cosine").await.unwrap();

    // A multibyte id and an ASCII id must not collide in the key index, which
    // would happen if lengths were counted in characters instead of bytes.
    let ids: Vec<String> = vec!["a".to_string(), "中文".to_string(), "a中".to_string()];
    let vectors: Vec<(String, Vec<f32>, serde_json::Value)> = ids
        .iter()
        .enumerate()
        .map(|(i, id)| (id.clone(), vec![i as f32 + 1.0, 0.0, 0.0], serde_json::json!({})))
        .collect();
    db.insert_vectors("mixed", vectors).await.unwrap();

    assert_eq!(db.get_vectors_count("mixed").await.unwrap(), 3);
    for (i, id) in ids.iter().enumerate() {
        let (vector, _) = db.get_vector("mixed", id).await.unwrap().unwrap();
        assert_eq!(vector[0], i as f32 + 1.0, "id {id} must map to its own record");
    }
}

// ---------------------------------------------------------------------------
// 清空 / 条件删除：必须持久，且不得误伤
// ---------------------------------------------------------------------------

/// 清空集合必须落盘。`clear_collection` 曾经只清内存与索引、不写任何存储
/// （没有 tombstone、没有 WAL），于是重启后向量全部"复活"。
#[tokio::test]
async fn clearing_a_collection_is_durable() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();

    {
        let db = open(&path).await;
        db.create_collection("c", 3, "euclidean").await.unwrap();
        let vectors: Vec<(String, Vec<f32>, serde_json::Value)> = corpus()
            .into_iter()
            .map(|(id, vector, metadata)| (id.to_string(), vector.to_vec(), metadata))
            .collect();
        db.insert_vectors("c", vectors).await.unwrap();

        let removed = db.clear_collection("c").await.unwrap();
        assert_eq!(removed, 4, "clear 必须报告真正清掉的条数");
        assert_eq!(db.get_vectors_count("c").await.unwrap(), 0);
    }

    // 新进程：只共享目录。
    let db = open(&path).await;
    assert_eq!(
        db.get_vectors_count("c").await.unwrap(),
        0,
        "清空必须落盘 —— 重启后不得复活"
    );
    let results = db.search("c", QUERY.to_vec(), 10, None).await.unwrap();
    assert!(results.is_empty(), "清空后检索必须为空");
}

/// 按条件批量删除：只删匹配的，不匹配的一条都不能动，且跨重启生效。
#[tokio::test]
async fn delete_by_filter_removes_only_matching_vectors_and_is_durable() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();

    {
        let db = open(&path).await;
        db.create_collection("c", 3, "euclidean").await.unwrap();
        let vectors: Vec<(String, Vec<f32>, serde_json::Value)> = corpus()
            .into_iter()
            .map(|(id, vector, metadata)| (id.to_string(), vector.to_vec(), metadata))
            .collect();
        db.insert_vectors("c", vectors).await.unwrap();

        // corpus 里 group == "x" 的是 p / r / s，group == "y" 的是 q。
        let mut deleted = db
            .delete_vectors_where("c", &serde_json::json!({"group": "x"}))
            .await
            .unwrap();
        deleted.sort();
        assert_eq!(deleted, vec!["p", "r", "s"]);
        assert_eq!(db.get_vectors_count("c").await.unwrap(), 1);

        // 比较算符也要能用。
        let removed = db
            .delete_vectors_where("c", &serde_json::json!({"rank": {"$gte": 99}}))
            .await
            .unwrap();
        assert!(removed.is_empty(), "没有匹配项时不得删除任何东西");
        assert_eq!(db.get_vectors_count("c").await.unwrap(), 1);
    }

    let db = open(&path).await;
    assert_eq!(db.get_vectors_count("c").await.unwrap(), 1);
    let (_, metadata) = db.get_vector("c", "q").await.unwrap().unwrap();
    assert_eq!(metadata["group"], "y");
    assert!(db.get_vector("c", "p").await.unwrap().is_none());
}

/// upsert 的语义：新 id 算插入、老 id 算更新，且两者都落盘。
#[tokio::test]
async fn upsert_inserts_or_replaces_and_is_durable() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();

    {
        let db = open(&path).await;
        db.create_collection("c", 3, "euclidean").await.unwrap();
        db.insert_vectors(
            "c",
            vec![("keep".to_string(), vec![1.0, 0.0, 0.0], serde_json::json!({"v": 1}))],
        )
        .await
        .unwrap();

        let (inserted, updated) = db
            .upsert_vectors(
                "c",
                vec![
                    ("keep".to_string(), vec![0.0, 1.0, 0.0], serde_json::json!({"v": 2})),
                    ("new".to_string(), vec![0.0, 0.0, 1.0], serde_json::json!({"v": 3})),
                ],
            )
            .await
            .unwrap();

        assert_eq!(inserted, vec!["new"]);
        assert_eq!(updated, vec!["keep"]);
        assert_eq!(db.get_vectors_count("c").await.unwrap(), 2);
    }

    let db = open(&path).await;
    assert_eq!(db.get_vectors_count("c").await.unwrap(), 2);
    let (vector, metadata) = db.get_vector("c", "keep").await.unwrap().unwrap();
    assert_eq!(vector, vec![0.0, 1.0, 0.0], "upsert 必须替换向量");
    assert_eq!(metadata["v"], 2, "upsert 必须替换元数据");
}
