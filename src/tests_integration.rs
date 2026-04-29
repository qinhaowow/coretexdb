//! Integration tests for CoreTexDB

use crate::{CoreTexDB, DbConfig};

#[tokio::test]
async fn test_full_workflow() {
    let db = CoreTexDB::new();
    db.init().await.unwrap();
    
    db.create_collection("test_workflow", 4, "cosine").await.unwrap();
    
    let vectors = vec![
        ("v1".to_string(), vec![1.0, 0.0, 0.0, 0.0], serde_json::json!({"label": "a"})),
        ("v2".to_string(), vec![0.0, 1.0, 0.0, 0.0], serde_json::json!({"label": "b"})),
        ("v3".to_string(), vec![0.0, 0.0, 1.0, 0.0], serde_json::json!({"label": "c"})),
    ];
    
    db.insert_vectors("test_workflow", vectors).await.unwrap();
    
    let count = db.get_vectors_count("test_workflow").await.unwrap();
    assert_eq!(count, 3);
    
    let results = db.search("test_workflow", vec![1.0, 0.0, 0.0, 0.0], 2, None).await.unwrap();
    assert!(!results.is_empty());
    
    db.delete_collection("test_workflow").await.unwrap();
    
    let collections = db.list_collections().await.unwrap();
    assert!(!collections.contains(&"test_workflow".to_string()));
}

#[tokio::test]
async fn test_persistent_config() {
    let config = DbConfig {
        data_dir: "./test_data".to_string(),
        memory_only: false,
        max_vectors_per_collection: 10000,
    };
    
    let db = CoreTexDB::with_config(config);
    db.init().await.unwrap();
    
    db.create_collection("persist_test", 8, "euclidean").await.unwrap();
    
    let collections = db.list_collections().await.unwrap();
    assert!(collections.contains(&"persist_test".to_string()));
    
    let _ = std::fs::remove_dir_all("./test_data");
}

#[tokio::test]
async fn test_multiple_collections() {
    let db = CoreTexDB::new();
    db.init().await.unwrap();
    
    db.create_collection("col1", 4, "cosine").await.unwrap();
    db.create_collection("col2", 8, "euclidean").await.unwrap();
    db.create_collection("col3", 16, "dotproduct").await.unwrap();
    
    let collections = db.list_collections().await.unwrap();
    assert_eq!(collections.len(), 3);
    
    db.insert_vectors("col1", vec![("v1".to_string(), vec![1.0, 0.0, 0.0, 0.0], serde_json::json!({}))]).await.unwrap();
    db.insert_vectors("col2", vec![("v2".to_string(), vec![1.0; 8], serde_json::json!({}))]).await.unwrap();
    
    let count1 = db.get_vectors_count("col1").await.unwrap();
    let count2 = db.get_vectors_count("col2").await.unwrap();
    let count3 = db.get_vectors_count("col3").await.unwrap();
    
    assert_eq!(count1, 1);
    assert_eq!(count2, 1);
    assert_eq!(count3, 0);
}
