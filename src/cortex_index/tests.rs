//! Tests for vector indexes

use super::*;
use tokio::test;

#[test]
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

#[test]
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

#[test]
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

#[test]
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

#[test]
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
