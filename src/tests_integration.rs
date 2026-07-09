//! Integration tests for CoreTexDB

use std::collections::HashMap;
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

// ═══════════════════════════════════════════════════════════
// SQL JOIN tests
// ═══════════════════════════════════════════════════════════

use crate::coretex_sql::{SQLExecutor, CollectionData, SQLValue};

fn make_collection(name: &str, data: Vec<(&str, HashMap<&str, SQLValue>)>) -> CollectionData {
    let mut vectors = HashMap::new();
    for (id, meta) in data {
        let converted: HashMap<String, SQLValue> = meta.into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect();
        vectors.insert(id.to_string(), (vec![0.0_f32; 4], converted));
    }
    CollectionData {
        name: name.to_string(),
        vectors,
    }
}

fn s(val: &str) -> SQLValue { SQLValue::String(val.to_string()) }
fn n(val: f64) -> SQLValue { SQLValue::Number(val) }

#[tokio::test]
async fn test_sql_inner_join() {
    let executor = SQLExecutor::new();

    executor.register_collection("users", make_collection("users", vec![
        ("u1", HashMap::from([("name", s("Alice")), ("dept_id", n(1.0))])),
        ("u2", HashMap::from([("name", s("Bob")),   ("dept_id", n(2.0))])),
        ("u3", HashMap::from([("name", s("Carol")), ("dept_id", n(1.0))])),
    ])).await;

    executor.register_collection("depts", make_collection("depts", vec![
        ("d1", HashMap::from([("dept_name", s("Engineering")), ("dept_id", n(1.0))])),
        ("d2", HashMap::from([("dept_name", s("Marketing")),   ("dept_id", n(2.0))])),
    ])).await;

    let result = executor.execute(
        "SELECT name, dept_name FROM users JOIN depts ON dept_id = dept_id"
    ).await.unwrap();

    match result {
        SQLResult::Select(rows) => {
            assert_eq!(rows.len(), 4, "Should have 4 joined rows (3 users × matching depts)");
            // u1→d1, u2→d2, u3→d1
            let has_alice_eng = rows.iter().any(|r|
                r.get("name") == Some(&s("Alice")) && r.get("dept_name") == Some(&s("Engineering"))
            );
            assert!(has_alice_eng);
        }
        other => panic!("Expected Select, got {:?}", other),
    }
}

#[tokio::test]
async fn test_sql_left_join_preserves_unmatched() {
    let executor = SQLExecutor::new();

    executor.register_collection("users", make_collection("users", vec![
        ("u1", HashMap::from([("name", s("Alice")), ("dept_id", n(1.0))])),
        ("u2", HashMap::from([("name", s("Bob")),   ("dept_id", n(99.0))])), // no matching dept
    ])).await;

    executor.register_collection("depts", make_collection("depts", vec![
        ("d1", HashMap::from([("dept_name", s("Engineering")), ("dept_id", n(1.0))])),
    ])).await;

    let result = executor.execute(
        "SELECT name, dept_name FROM users LEFT JOIN depts ON dept_id = dept_id"
    ).await.unwrap();

    match result {
        SQLResult::Select(rows) => {
            assert_eq!(rows.len(), 2, "LEFT JOIN should preserve both users");
            let bob_row = rows.iter().find(|r| r.get("name") == Some(&s("Bob"))).unwrap();
            assert_eq!(bob_row.get("dept_name"), Some(&SQLValue::Null),
                "Bob's dept_name should be NULL (no match)");
        }
        other => panic!("Expected Select, got {:?}", other),
    }
}

#[tokio::test]
async fn test_sql_join_with_where() {
    let executor = SQLExecutor::new();

    executor.register_collection("users", make_collection("users", vec![
        ("u1", HashMap::from([("name", s("Alice")), ("age", n(30.0)), ("dept_id", n(1.0))])),
        ("u2", HashMap::from([("name", s("Bob")),   ("age", n(25.0)), ("dept_id", n(1.0))])),
    ])).await;

    executor.register_collection("depts", make_collection("depts", vec![
        ("d1", HashMap::from([("dept_name", s("Engineering")), ("dept_id", n(1.0))])),
    ])).await;

    let result = executor.execute(
        "SELECT name, dept_name FROM users JOIN depts ON dept_id = dept_id WHERE age = 25"
    ).await.unwrap();

    match result {
        SQLResult::Select(rows) => {
            assert_eq!(rows.len(), 1, "Only Bob should match age=25");
            assert_eq!(rows[0].get("name"), Some(&s("Bob")));
        }
        other => panic!("Expected Select, got {:?}", other),
    }
}

#[tokio::test]
async fn test_sql_inner_join_explicit_keyword() {
    let executor = SQLExecutor::new();

    executor.register_collection("a", make_collection("a", vec![
        ("k1", HashMap::from([("x", s("foo")), ("join_key", n(1.0))])),
    ])).await;
    executor.register_collection("b", make_collection("b", vec![
        ("k2", HashMap::from([("y", s("bar")), ("join_key", n(1.0))])),
    ])).await;

    let result = executor.execute(
        "SELECT x, y FROM a INNER JOIN b ON join_key = join_key"
    ).await.unwrap();

    match result {
        SQLResult::Select(rows) => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].get("x"), Some(&s("foo")));
            assert_eq!(rows[0].get("y"), Some(&s("bar")));
        }
        other => panic!("Expected Select, got {:?}", other),
    }
}

#[tokio::test]
async fn test_sql_multiple_joins() {
    let executor = SQLExecutor::new();

    executor.register_collection("users", make_collection("users", vec![
        ("u1", HashMap::from([("name", s("Alice")), ("dept_id", n(1.0)), ("city_id", n(10.0))])),
    ])).await;
    executor.register_collection("depts", make_collection("depts", vec![
        ("d1", HashMap::from([("dept_name", s("Eng")), ("dept_id", n(1.0))])),
    ])).await;
    executor.register_collection("cities", make_collection("cities", vec![
        ("c1", HashMap::from([("city_name", s("NYC")), ("city_id", n(10.0))])),
    ])).await;

    let result = executor.execute(
        "SELECT name, dept_name, city_name FROM users JOIN depts ON dept_id = dept_id JOIN cities ON city_id = city_id"
    ).await.unwrap();

    match result {
        SQLResult::Select(rows) => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].get("name"), Some(&s("Alice")));
            assert_eq!(rows[0].get("dept_name"), Some(&s("Eng")));
            assert_eq!(rows[0].get("city_name"), Some(&s("NYC")));
        }
        other => panic!("Expected Select, got {:?}", other),
    }
}

#[tokio::test]
async fn test_sql_join_no_matches_inner() {
    let executor = SQLExecutor::new();

    executor.register_collection("a", make_collection("a", vec![
        ("k1", HashMap::from([("val", s("left")), ("key", n(1.0))])),
    ])).await;
    executor.register_collection("b", make_collection("b", vec![
        ("k2", HashMap::from([("val", s("right")), ("key", n(999.0))])),
    ])).await;

    let result = executor.execute(
        "SELECT a.val FROM a JOIN b ON key = key"
    ).await.unwrap();

    match result {
        SQLResult::Select(rows) => {
            assert_eq!(rows.len(), 0, "INNER JOIN with no matches should return 0 rows");
        }
        other => panic!("Expected Select, got {:?}", other),
    }
}

#[tokio::test]
async fn test_sql_join_with_limit() {
    let executor = SQLExecutor::new();

    executor.register_collection("users", make_collection("users", vec![
        ("u1", HashMap::from([("name", s("A")), ("dept_id", n(1.0))])),
        ("u2", HashMap::from([("name", s("B")), ("dept_id", n(1.0))])),
        ("u3", HashMap::from([("name", s("C")), ("dept_id", n(1.0))])),
    ])).await;
    executor.register_collection("depts", make_collection("depts", vec![
        ("d1", HashMap::from([("dept_name", s("Eng")), ("dept_id", n(1.0))])),
    ])).await;

    let result = executor.execute(
        "SELECT name, dept_name FROM users JOIN depts ON dept_id = dept_id LIMIT 2"
    ).await.unwrap();

    match result {
        SQLResult::Select(rows) => {
            assert_eq!(rows.len(), 2, "LIMIT 2 should apply after join");
        }
        other => panic!("Expected Select, got {:?}", other),
    }
}
