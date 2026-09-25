/// Tests for vector indexes

use super::*;

#[tokio::test]
async fn test_brute_force_index() {
    // Create a new brute force index
    let index = BruteForceIndex::new("cosine");
    
    // Add vectors
    index.add("vec1", &[1.0, 0.0, 0.0]).await.unwrap();
    index.add("vec2", &[0.0, 1.0, 0.0]).await.unwrap();
    index.add("vec3", &[0.0, 0.0, 1.0]).await.unwrap();
    
    // Search for similar vectors
    let query = &[1.0, 0.0, 0.0];
    let results = index.search(query, 2).await.unwrap();
    
    // Verify results
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].id, "vec1");
    assert!(results[0].distance < results[1].distance);
    
    // Remove a vector
    let removed = index.remove("vec2").await.unwrap();
    assert!(removed);
    
    // Search again after removal
    let results = index.search(query, 2).await.unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].id, "vec1");
    assert_eq!(results[1].id, "vec3");
    
    // Clear the index
    index.clear().await.unwrap();
    
    // Search after clearing
    let results = index.search(query, 2).await.unwrap();
    assert_eq!(results.len(), 0);
}

#[tokio::test]
async fn test_hnsw_index() {
    // Create a new HNSW index
    let index = HNSWIndex::new("cosine");
    
    // Add vectors
    index.add("vec1", &[1.0, 0.0, 0.0]).await.unwrap();
    index.add("vec2", &[0.0, 1.0, 0.0]).await.unwrap();
    index.add("vec3", &[0.0, 0.0, 1.0]).await.unwrap();
    
    // Search for similar vectors
    let query = &[1.0, 0.0, 0.0];
    let results = index.search(query, 2).await.unwrap();
    
    // Verify results
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].id, "vec1");
    
    // Remove a vector
    let removed = index.remove("vec2").await.unwrap();
    assert!(removed);
    
    // Clear the index
    index.clear().await.unwrap();
}

/// Regression: `remove` used to promote an arbitrary `graph.keys().next()` to
/// entry point. The entry point must remain the *highest-level* node, otherwise
/// the hierarchy stops being navigable from the top.
#[tokio::test]
async fn test_hnsw_remove_reassigns_highest_level_entry_point() {
    let index = HNSWIndex::new("cosine");
    for i in 0..64 {
        let angle = i as f32 * 0.1;
        index
            .add(&format!("v{i}"), &[angle.cos(), angle.sin(), 0.0])
            .await
            .unwrap();
    }

    let old_ep = index.entry_point.read().await.clone();
    let old_ep = old_ep.expect("entry point set after inserts");

    index.remove(&old_ep).await.unwrap();

    let new_ep = index.entry_point.read().await.clone();
    let graph = index.graph.read().await;
    if let Some(ep) = new_ep {
        assert_ne!(ep, old_ep, "removed entry point must not stay selected");
        let ep_level = graph.get(&ep).map(|l| l.len()).unwrap_or(0);
        let max_level = graph.values().map(|l| l.len()).max().unwrap_or(0);
        assert_eq!(ep_level, max_level, "entry point must stay the highest-level node");
    }
}

/// Regression: `search_layer` used to `unwrap()` the vector for a graph
/// neighbour, so a single dangling backlink panicked the whole search.
#[tokio::test]
async fn test_hnsw_search_tolerates_orphan_graph_neighbor() {
    let index = HNSWIndex::new("cosine");
    index.add("a", &[1.0, 0.0, 0.0]).await.unwrap();
    index.add("b", &[0.0, 1.0, 0.0]).await.unwrap();

    // Simulate a legacy orphan: a backlink to an id with no vector.
    {
        let mut vectors = index.vectors.write().await;
        vectors.remove("b");
    }

    // Must not panic.
    let _ = index.search(&[1.0, 0.0, 0.0], 5).await.unwrap();
}

/// Concurrent add/remove/search must not deadlock (AB-BA between the
/// `vectors → entry_point → graph` and the old `vectors → graph → entry_point`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_hnsw_concurrent_add_remove_no_deadlock() {
    use std::sync::Arc;
    let index = Arc::new(HNSWIndex::new("cosine"));
    for i in 0..16 {
        index.add(&format!("seed{i}"), &[i as f32, 1.0, 0.0]).await.unwrap();
    }

    let idx = index.clone();
    let writer = tokio::spawn(async move {
        for i in 0..200 {
            idx.add(&format!("x{i}"), &[i as f32, 0.0, 1.0]).await.unwrap();
        }
    });
    let idx = index.clone();
    let remover = tokio::spawn(async move {
        for i in 0..200 {
            let _ = idx.remove(&format!("seed{}", i % 16)).await;
        }
    });
    let idx = index.clone();
    let searcher = tokio::spawn(async move {
        for _ in 0..200 {
            let _ = idx.search(&[1.0, 1.0, 0.0], 5).await;
        }
    });

    // Reaching the joins means every lock pair progressed; a deadlock would
    // hang the test (caught by the harness timeout) instead.
    writer.await.unwrap();
    remover.await.unwrap();
    searcher.await.unwrap();
}

#[tokio::test]
async fn test_ivf_index() {
    // Create a new IVF index
    let index = IVFIndex::new("cosine");
    
    // Add vectors
    index.add("vec1", &[1.0, 0.0, 0.0]).await.unwrap();
    index.add("vec2", &[0.0, 1.0, 0.0]).await.unwrap();
    index.add("vec3", &[0.0, 0.0, 1.0]).await.unwrap();
    
    // Search for similar vectors
    let query = &[1.0, 0.0, 0.0];
    let results = index.search(query, 2).await.unwrap();
    
    // Verify results
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].id, "vec1");
    
    // Remove a vector
    let removed = index.remove("vec2").await.unwrap();
    assert!(removed);
    
    // Clear the index
    index.clear().await.unwrap();
}

#[tokio::test]
async fn test_scalar_index() {
    // Create a new scalar index
    let index = ScalarIndex::new();
    
    // Add vectors (scalar values are the first element)
    index.add("vec1", &[1.0]).await.unwrap();
    index.add("vec2", &[2.0]).await.unwrap();
    index.add("vec3", &[3.0]).await.unwrap();
    
    // Search for similar vectors
    let query = &[2.0];
    let results = index.search(query, 2).await.unwrap();
    
    // Verify results
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].id, "vec2");
    
    // Remove a vector
    let removed = index.remove("vec2").await.unwrap();
    assert!(removed);
    
    // Clear the index
    index.clear().await.unwrap();
}

#[tokio::test]
async fn test_index_manager() {
    // Create a new index manager
    let manager = IndexManager::new();
    
    // Create an index
    manager.create_index("test-index", "brute_force", "cosine").await.unwrap();
    
    // Get the index
    let index = manager.get_index("test-index").await.unwrap();
    assert!(index.is_some());
    
    // Delete the index
    let deleted = manager.delete_index("test-index").await.unwrap();
    assert!(deleted);
    
    // Verify deletion
    let index = manager.get_index("test-index").await.unwrap();
    assert!(index.is_none());
}

/// A persisted HNSW index must round-trip: the loaded index answers the same
/// query and reports the same checksum-bound data.
#[tokio::test]
async fn test_hnsw_persist_and_load_roundtrip() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("demo.json");

    let index = HNSWIndex::new("cosine");
    let mut pairs = Vec::new();
    for i in 0..32u32 {
        let vector = vec![i as f32, 1.0, 0.0];
        index.add(&format!("v{i}"), &vector).await.unwrap();
        pairs.push((format!("v{i}"), vector));
    }
    let checksum = vectors_checksum(&pairs);

    assert!(index.persist(&path, &checksum).await.unwrap());
    assert!(path.exists(), "index file must be written");

    // A fresh manager loads it and gets a working index.
    let manager = IndexManager::new();
    manager
        .create_index("demo_index", "hnsw", "cosine")
        .await
        .unwrap();
    assert!(
        manager
            .load_index("demo_index", "hnsw", "cosine", &path, &checksum)
            .await
            .unwrap(),
        "matching checksum must install the persisted index"
    );

    let loaded = manager.get_index("demo_index").await.unwrap().unwrap();
    let hits = loaded.search(&[0.0, 1.0, 0.0], 3).await.unwrap();
    assert!(!hits.is_empty());
    assert_eq!(hits[0].id, "v0");
}

/// A stale index (data changed after it was written) must be rejected, so the
/// caller falls back to rebuilding instead of serving incomplete results.
#[tokio::test]
async fn test_load_index_rejects_stale_checksum() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("demo.json");

    let index = HNSWIndex::new("cosine");
    index.add("a", &[1.0, 0.0]).await.unwrap();
    let written = vectors_checksum(&[("a".to_string(), vec![1.0, 0.0])]);
    index.persist(&path, &written).await.unwrap();

    let manager = IndexManager::new();
    manager.create_index("i", "hnsw", "cosine").await.unwrap();

    // Correct checksum loads…
    assert!(manager
        .load_index("i", "hnsw", "cosine", &path, &written)
        .await
        .unwrap());
    // …a checksum from a different data set does not.
    let stale = vectors_checksum(&[("a".to_string(), vec![9.0, 9.0])]);
    assert_ne!(stale, written);
    assert!(!manager
        .load_index("i", "hnsw", "cosine", &path, &stale)
        .await
        .unwrap());
}

/// Type and metric must match too — otherwise a cosine index could be loaded
/// under a euclidean collection.
#[tokio::test]
async fn test_load_index_rejects_type_and_metric_mismatch() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("demo.json");

    let index = HNSWIndex::new("cosine");
    index.add("a", &[1.0, 0.0]).await.unwrap();
    let checksum = vectors_checksum(&[("a".to_string(), vec![1.0, 0.0])]);
    index.persist(&path, &checksum).await.unwrap();

    let manager = IndexManager::new();
    manager.create_index("i", "hnsw", "cosine").await.unwrap();

    assert!(!manager
        .load_index("i", "ivf", "cosine", &path, &checksum)
        .await
        .unwrap());
    assert!(!manager
        .load_index("i", "hnsw", "euclidean", &path, &checksum)
        .await
        .unwrap());
    // Its own type/metric still works.
    assert!(manager
        .load_index("i", "hnsw", "cosine", &path, &checksum)
        .await
        .unwrap());
}
