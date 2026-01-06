//! Tests for storage engines

use super::*;
use tokio::test;

#[test]
async fn test_memory_storage() {
    // Create a new memory storage
    let mut storage = MemoryStorage::new();
    
    // Initialize storage
    storage.init().await.unwrap();
    
    // Store a vector
    let id = "test-1";
    let vector = vec![1.0, 2.0, 3.0];
    let metadata = serde_json::json!({"category": "test"});
    
    storage.store(id, &vector, &metadata).await.unwrap();
    
    // Retrieve the vector
    let result = storage.retrieve(id).await.unwrap();
    assert!(result.is_some());
    
    let (retrieved_vector, retrieved_metadata) = result.unwrap();
    assert_eq!(retrieved_vector, vector);
    assert_eq!(retrieved_metadata, metadata);
    
    // List all vectors
    let ids = storage.list().await.unwrap();
    assert!(ids.contains(&id.to_string()));
    
    // Count vectors
    let count = storage.count().await.unwrap();
    assert_eq!(count, 1);
    
    // Delete the vector
    let deleted = storage.delete(id).await.unwrap();
    assert!(deleted);
    
    // Verify deletion
    let result = storage.retrieve(id).await.unwrap();
    assert!(result.is_none());
    
    // Count vectors after deletion
    let count = storage.count().await.unwrap();
    assert_eq!(count, 0);
}

#[test]
async fn test_persistent_storage() {
    // Create a temporary directory for testing
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().to_str().unwrap();
    
    // Create a new persistent storage
    let mut storage = PersistentStorage::new(db_path);
    
    // Initialize storage
    storage.init().await.unwrap();
    
    // Store a vector
    let id = "test-1";
    let vector = vec![1.0, 2.0, 3.0];
    let metadata = serde_json::json!({"category": "test"});
    
    storage.store(id, &vector, &metadata).await.unwrap();
    
    // Retrieve the vector
    let result = storage.retrieve(id).await.unwrap();
    assert!(result.is_some());
    
    let (retrieved_vector, retrieved_metadata) = result.unwrap();
    assert_eq!(retrieved_vector, vector);
    assert_eq!(retrieved_metadata, metadata);
    
    // List all vectors
    let ids = storage.list().await.unwrap();
    assert!(ids.contains(&id.to_string()));
    
    // Count vectors
    let count = storage.count().await.unwrap();
    assert_eq!(count, 1);
    
    // Delete the vector
    let deleted = storage.delete(id).await.unwrap();
    assert!(deleted);
    
    // Verify deletion
    let result = storage.retrieve(id).await.unwrap();
    assert!(result.is_none());
    
    // Count vectors after deletion
    let count = storage.count().await.unwrap();
    assert_eq!(count, 0);
}
