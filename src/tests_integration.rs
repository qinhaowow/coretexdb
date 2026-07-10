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

// ═══════════════════════════════════════════════════════════
// End-to-end: WAL recovery × SQL (JOIN + Aggregate)
// ═══════════════════════════════════════════════════════════

#[cfg(test)]
mod wal_sql_e2e {
    use std::sync::Arc;
    use tokio::sync::RwLock;
    use tempfile::TempDir;

    use crate::coretex_core::{CollectionSchema, DistanceMetric};
    use crate::coretex_data::DataManager;
    use crate::coretex_index::IndexManager;
    use crate::coretex_storage::{MemoryStorage, StorageEngine};
    use crate::coretex_utils::wal::{WriteAheadLog, WalEntryType, RecoveryManager};
    use crate::coretex_sql::{SQLExecutor, SQLResult, SQLValue};

    fn s(v: &str) -> SQLValue { SQLValue::String(v.to_string()) }
    fn n(v: f64) -> SQLValue { SQLValue::Number(v) }

    async fn setup_dm_sql(dir: &TempDir) -> (Arc<DataManager>, SQLExecutor) {
        let storage: Arc<RwLock<Box<dyn StorageEngine>>> =
            Arc::new(RwLock::new(Box::new(MemoryStorage::new())));
        let index_manager = Arc::new(IndexManager::new(Arc::clone(&storage)));
        let wal = Arc::new(WriteAheadLog::new(dir.path().to_string_lossy().as_ref()));
        wal.init().await.unwrap();
        let dm = Arc::new(DataManager::new(Arc::clone(&storage), index_manager)
            .with_wal(Arc::clone(&wal)));
        let executor = SQLExecutor::with_data_manager(Arc::clone(&dm));
        (dm, executor)
    }

    async fn insert_via_dm(dm: &DataManager, coll: &str, entries: Vec<(&str, serde_json::Value)>) {
        let vectors: Vec<_> = entries.into_iter().map(|(id, meta)| {
            (id.to_string(), vec![0.0_f32; 4], meta)
        }).collect();
        dm.insert_vectors(coll, vectors).await.unwrap();
    }

    #[tokio::test]
    async fn test_wal_recovery_sql_select_after_crash() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().to_string_lossy().to_string();

        // ── Phase 1: Write data, then crash ──
        {
            let (dm, executor) = setup_dm_sql(&dir).await;
            dm.create_collection("users", 4, "cosine").await.unwrap();

            insert_via_dm(&dm, "users", vec![
                ("u1", serde_json::json!({"name": "Alice", "age": 30})),
                ("u2", serde_json::json!({"name": "Bob",   "age": 25})),
            ]).await;

            // Verify pre-crash
            let r = executor.execute("SELECT name, age FROM users ORDER BY age ASC").await.unwrap();
            if let SQLResult::Select(rows) = r {
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0].get("name"), Some(&s("Bob")));
            } else { panic!("Expected Select"); }
        } // crash — everything dropped

        // ── Phase 2: Recover and query ──
        {
            let storage: Arc<RwLock<Box<dyn StorageEngine>>> =
                Arc::new(RwLock::new(Box::new(MemoryStorage::new())));
            let index_manager = Arc::new(IndexManager::new(Arc::clone(&storage)));
            let wal = Arc::new(WriteAheadLog::new(&wal_path));
            wal.init().await.unwrap();

            let recovery = RecoveryManager::new(Arc::clone(&wal));
            let entries = recovery.recover_storage_entries().await.unwrap();

            // Replay into fresh storage
            {
                let s = storage.write().await;
                for (_etype, _coll, key, vector, metadata) in &entries {
                    let _ = s.store(key, vector, metadata).await;
                }
            }

            // Re-create DataManager pointing at recovered storage
            let dm = Arc::new(DataManager::new(Arc::clone(&storage), index_manager));
            // Re-create the collection so DM knows about it
            dm.create_collection("users", 4, "cosine").await.unwrap();

            // Replay WAL entries through DataManager to populate its internal state
            for (etype, coll, key, _vec, meta) in &entries {
                match etype {
                    WalEntryType::Insert => {
                        dm.insert_vectors(coll, vec![
                            (key.clone(), vec![0.0; 4], meta.clone())
                        ]).await.unwrap();
                    }
                    _ => {}
                }
            }

            let executor = SQLExecutor::with_data_manager(Arc::clone(&dm));
            let r = executor.execute("SELECT name, age FROM users WHERE age > 20").await.unwrap();
            if let SQLResult::Select(rows) = r {
                assert_eq!(rows.len(), 2, "Both users should survive recovery");
            } else { panic!("Expected Select"); }
        }
    }

    #[tokio::test]
    async fn test_wal_recovery_sql_join_after_crash() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().to_string_lossy().to_string();

        {
            let (dm, _executor) = setup_dm_sql(&dir).await;
            dm.create_collection("users", 4, "cosine").await.unwrap();
            dm.create_collection("depts", 4, "cosine").await.unwrap();

            insert_via_dm(&dm, "users", vec![
                ("u1", serde_json::json!({"name": "Alice", "dept_id": 1})),
                ("u2", serde_json::json!({"name": "Bob",   "dept_id": 2})),
            ]).await;
            insert_via_dm(&dm, "depts", vec![
                ("d1", serde_json::json!({"dept_name": "Eng", "dept_id": 1})),
                ("d2", serde_json::json!({"dept_name": "Mkt", "dept_id": 2})),
            ]).await;
        }

        {
            let storage: Arc<RwLock<Box<dyn StorageEngine>>> =
                Arc::new(RwLock::new(Box::new(MemoryStorage::new())));
            let index_manager = Arc::new(IndexManager::new(Arc::clone(&storage)));
            let wal = Arc::new(WriteAheadLog::new(&wal_path));
            wal.init().await.unwrap();

            let recovery = RecoveryManager::new(Arc::clone(&wal));
            let entries = recovery.recover_storage_entries().await.unwrap();

            {
                let s = storage.write().await;
                for (_etype, _coll, key, vector, metadata) in &entries {
                    let _ = s.store(key, vector, metadata).await;
                }
            }

            let dm = Arc::new(DataManager::new(Arc::clone(&storage), index_manager));
            dm.create_collection("users", 4, "cosine").await.unwrap();
            dm.create_collection("depts", 4, "cosine").await.unwrap();

            for (etype, coll, key, _vec, meta) in &entries {
                if *etype == WalEntryType::Insert {
                    dm.insert_vectors(coll, vec![(key.clone(), vec![0.0; 4], meta.clone())])
                        .await.unwrap();
                }
            }

            let executor = SQLExecutor::with_data_manager(Arc::clone(&dm));
            let r = executor.execute(
                "SELECT name, dept_name FROM users JOIN depts ON dept_id = dept_id"
            ).await.unwrap();

            if let SQLResult::Select(rows) = r {
                assert_eq!(rows.len(), 2);
                let alice = rows.iter().find(|r| r.get("name") == Some(&s("Alice"))).unwrap();
                assert_eq!(alice.get("dept_name"), Some(&s("Eng")));
            } else { panic!("Expected Select"); }
        }
    }

    #[tokio::test]
    async fn test_wal_recovery_sql_group_by_after_crash() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().to_string_lossy().to_string();

        {
            let (dm, _executor) = setup_dm_sql(&dir).await;
            dm.create_collection("orders", 4, "cosine").await.unwrap();

            insert_via_dm(&dm, "orders", vec![
                ("o1", serde_json::json!({"dept": "A", "amount": 10})),
                ("o2", serde_json::json!({"dept": "A", "amount": 20})),
                ("o3", serde_json::json!({"dept": "B", "amount": 5})),
            ]).await;
        }

        {
            let storage: Arc<RwLock<Box<dyn StorageEngine>>> =
                Arc::new(RwLock::new(Box::new(MemoryStorage::new())));
            let index_manager = Arc::new(IndexManager::new(Arc::clone(&storage)));
            let wal = Arc::new(WriteAheadLog::new(&wal_path));
            wal.init().await.unwrap();

            let recovery = RecoveryManager::new(Arc::clone(&wal));
            let entries = recovery.recover_storage_entries().await.unwrap();

            {
                let s = storage.write().await;
                for (_etype, _coll, key, vector, metadata) in &entries {
                    let _ = s.store(key, vector, metadata).await;
                }
            }

            let dm = Arc::new(DataManager::new(Arc::clone(&storage), index_manager));
            dm.create_collection("orders", 4, "cosine").await.unwrap();

            for (etype, coll, key, _vec, meta) in &entries {
                if *etype == WalEntryType::Insert {
                    dm.insert_vectors(coll, vec![(key.clone(), vec![0.0; 4], meta.clone())])
                        .await.unwrap();
                }
            }

            let executor = SQLExecutor::with_data_manager(Arc::clone(&dm));
            let r = executor.execute(
                "SELECT dept, SUM(amount) AS total, COUNT(*) AS cnt FROM orders GROUP BY dept ORDER BY dept"
            ).await.unwrap();

            if let SQLResult::Select(rows) = r {
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0].get("dept"), Some(&s("A")));
                assert_eq!(rows[0].get("total"), Some(&n(30.0)));
                assert_eq!(rows[0].get("cnt"), Some(&n(2.0)));
                assert_eq!(rows[1].get("dept"), Some(&s("B")));
                assert_eq!(rows[1].get("total"), Some(&n(5.0)));
            } else { panic!("Expected Select"); }
        }
    }

    #[tokio::test]
    async fn test_wal_recovery_full_pipeline() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().to_string_lossy().to_string();

        {
            let (dm, _executor) = setup_dm_sql(&dir).await;
            dm.create_collection("orders", 4, "cosine").await.unwrap();
            dm.create_collection("products", 4, "cosine").await.unwrap();

            insert_via_dm(&dm, "orders", vec![
                ("o1", serde_json::json!({"product_id": 1, "qty": 3, "price": 10})),
                ("o2", serde_json::json!({"product_id": 1, "qty": 2, "price": 10})),
                ("o3", serde_json::json!({"product_id": 2, "qty": 5, "price": 8})),
                ("o4", serde_json::json!({"product_id": 2, "qty": 1, "price": 8})),
            ]).await;
            insert_via_dm(&dm, "products", vec![
                ("p1", serde_json::json!({"product_id": 1, "name": "Widget"})),
                ("p2", serde_json::json!({"product_id": 2, "name": "Gadget"})),
            ]).await;
        }

        {
            let storage: Arc<RwLock<Box<dyn StorageEngine>>> =
                Arc::new(RwLock::new(Box::new(MemoryStorage::new())));
            let index_manager = Arc::new(IndexManager::new(Arc::clone(&storage)));
            let wal = Arc::new(WriteAheadLog::new(&wal_path));
            wal.init().await.unwrap();

            let recovery = RecoveryManager::new(Arc::clone(&wal));
            let entries = recovery.recover_storage_entries().await.unwrap();

            {
                let s = storage.write().await;
                for (_etype, _coll, key, vector, metadata) in &entries {
                    let _ = s.store(key, vector, metadata).await;
                }
            }

            let dm = Arc::new(DataManager::new(Arc::clone(&storage), index_manager));
            dm.create_collection("orders", 4, "cosine").await.unwrap();
            dm.create_collection("products", 4, "cosine").await.unwrap();

            for (etype, coll, key, _vec, meta) in &entries {
                if *etype == WalEntryType::Insert {
                    dm.insert_vectors(coll, vec![(key.clone(), vec![0.0; 4], meta.clone())])
                        .await.unwrap();
                }
            }

            let executor = SQLExecutor::with_data_manager(Arc::clone(&dm));

            // JOIN + GROUP BY + HAVING + ORDER BY + LIMIT — all after recovery
            let r = executor.execute(
                "SELECT name, SUM(qty) AS total_qty, COUNT(*) AS order_count \
                 FROM orders JOIN products ON product_id = product_id \
                 GROUP BY name \
                 HAVING SUM(qty) > 4 \
                 ORDER BY total_qty DESC \
                 LIMIT 1"
            ).await.unwrap();

            if let SQLResult::Select(rows) = r {
                assert_eq!(rows.len(), 1, "Only Gadget has qty > 4 (6 total)");
                assert_eq!(rows[0].get("name"), Some(&s("Gadget")));
                assert_eq!(rows[0].get("total_qty"), Some(&n(6.0)));
            } else { panic!("Expected Select"); }
        }
    }

    #[tokio::test]
    async fn test_wal_recovery_delete_then_select() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().to_string_lossy().to_string();

        {
            let (dm, _executor) = setup_dm_sql(&dir).await;
            dm.create_collection("items", 4, "cosine").await.unwrap();
            insert_via_dm(&dm, "items", vec![
                ("keep",   serde_json::json!({"name": "keep-me"})),
                ("remove", serde_json::json!({"name": "delete-me"})),
            ]).await;
            dm.delete_vectors("items", &["remove"]).await.unwrap();
        }

        {
            let storage: Arc<RwLock<Box<dyn StorageEngine>>> =
                Arc::new(RwLock::new(Box::new(MemoryStorage::new())));
            let index_manager = Arc::new(IndexManager::new(Arc::clone(&storage)));
            let wal = Arc::new(WriteAheadLog::new(&wal_path));
            wal.init().await.unwrap();

            let recovery = RecoveryManager::new(Arc::clone(&wal));
            let entries = recovery.recover_storage_entries().await.unwrap();

            {
                let s = storage.write().await;
                for (_etype, _coll, key, vector, metadata) in &entries {
                    let _ = s.store(key, vector, metadata).await;
                }
            }

            let dm = Arc::new(DataManager::new(Arc::clone(&storage), index_manager));
            dm.create_collection("items", 4, "cosine").await.unwrap();

            for (etype, coll, key, _vec, meta) in &entries {
                match etype {
                    WalEntryType::Insert => {
                        dm.insert_vectors(coll, vec![(key.clone(), vec![0.0; 4], meta.clone())])
                            .await.unwrap();
                    }
                    WalEntryType::Delete => {
                        let _ = dm.delete_vectors(coll, &[key]).await;
                    }
                    _ => {}
                }
            }

            let executor = SQLExecutor::with_data_manager(Arc::clone(&dm));
            let r = executor.execute("SELECT name FROM items").await.unwrap();
            if let SQLResult::Select(rows) = r {
                assert_eq!(rows.len(), 1, "Only 'keep' should survive delete recovery");
                assert_eq!(rows[0].get("name"), Some(&s("keep-me")));
            } else { panic!("Expected Select"); }
        }
    }

    #[tokio::test]
    async fn test_wal_recovery_count_star_after_crash() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().to_string_lossy().to_string();

        {
            let (dm, _executor) = setup_dm_sql(&dir).await;
            dm.create_collection("events", 4, "cosine").await.unwrap();
            for i in 0..5 {
                insert_via_dm(&dm, "events", vec![
                    (format!("e{}", i).as_str(), serde_json::json!({"val": i})),
                ]).await;
            }
        }

        {
            let storage: Arc<RwLock<Box<dyn StorageEngine>>> =
                Arc::new(RwLock::new(Box::new(MemoryStorage::new())));
            let index_manager = Arc::new(IndexManager::new(Arc::clone(&storage)));
            let wal = Arc::new(WriteAheadLog::new(&wal_path));
            wal.init().await.unwrap();

            let recovery = RecoveryManager::new(Arc::clone(&wal));
            let entries = recovery.recover_storage_entries().await.unwrap();

            {
                let s = storage.write().await;
                for (_etype, _coll, key, vector, metadata) in &entries {
                    let _ = s.store(key, vector, metadata).await;
                }
            }

            let dm = Arc::new(DataManager::new(Arc::clone(&storage), index_manager));
            dm.create_collection("events", 4, "cosine").await.unwrap();

            for (etype, coll, key, _vec, meta) in &entries {
                if *etype == WalEntryType::Insert {
                    dm.insert_vectors(coll, vec![(key.clone(), vec![0.0; 4], meta.clone())])
                        .await.unwrap();
                }
            }

            let executor = SQLExecutor::with_data_manager(Arc::clone(&dm));
            let r = executor.execute("SELECT COUNT(*) FROM events").await.unwrap();
            if let SQLResult::Select(rows) = r {
                assert_eq!(rows[0].get("count(*)"), Some(&n(5.0)), "All 5 events should survive");
            } else { panic!("Expected Select"); }
        }
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

// ═══════════════════════════════════════════════════════════
// SQL Aggregate tests
// ═══════════════════════════════════════════════════════════

#[tokio::test]
async fn test_sql_count_star() {
    let executor = SQLExecutor::new();
    executor.register_collection("orders", make_collection("orders", vec![
        ("o1", HashMap::from([("amount", n(100.0))])),
        ("o2", HashMap::from([("amount", n(200.0))])),
        ("o3", HashMap::from([("amount", n(300.0))])),
    ])).await;

    let result = executor.execute("SELECT COUNT(*) FROM orders").await.unwrap();
    match result {
        SQLResult::Select(rows) => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].get("count(*)"), Some(&n(3.0)));
        }
        other => panic!("Expected Select, got {:?}", other),
    }
}

#[tokio::test]
async fn test_sql_count_column() {
    let executor = SQLExecutor::new();
    executor.register_collection("t", make_collection("t", vec![
        ("a", HashMap::from([("status", s("active"))])),
        ("b", HashMap::from([("status", s("active"))])),
        ("c", HashMap::new()), // no "status" field
    ])).await;

    let result = executor.execute("SELECT COUNT(status) FROM t").await.unwrap();
    match result {
        SQLResult::Select(rows) => {
            assert_eq!(rows[0].get("count(status)"), Some(&n(2.0)),
                "Only rows with status field should be counted");
        }
        other => panic!("Expected Select, got {:?}", other),
    }
}

#[tokio::test]
async fn test_sql_sum_avg() {
    let executor = SQLExecutor::new();
    executor.register_collection("sales", make_collection("sales", vec![
        ("s1", HashMap::from([("amount", n(10.0))])),
        ("s2", HashMap::from([("amount", n(20.0))])),
        ("s3", HashMap::from([("amount", n(30.0))])),
    ])).await;

    let result = executor.execute("SELECT SUM(amount), AVG(amount) FROM sales").await.unwrap();
    match result {
        SQLResult::Select(rows) => {
            assert_eq!(rows[0].get("sum(amount)"), Some(&n(60.0)));
            assert_eq!(rows[0].get("avg(amount)"), Some(&n(20.0)));
        }
        other => panic!("Expected Select, got {:?}", other),
    }
}

#[tokio::test]
async fn test_sql_min_max() {
    let executor = SQLExecutor::new();
    executor.register_collection("prices", make_collection("prices", vec![
        ("p1", HashMap::from([("price", n(5.0))])),
        ("p2", HashMap::from([("price", n(99.0))])),
        ("p3", HashMap::from([("price", n(42.0))])),
    ])).await;

    let result = executor.execute("SELECT MIN(price), MAX(price) FROM prices").await.unwrap();
    match result {
        SQLResult::Select(rows) => {
            assert_eq!(rows[0].get("min(price)"), Some(&n(5.0)));
            assert_eq!(rows[0].get("max(price)"), Some(&n(99.0)));
        }
        other => panic!("Expected Select, got {:?}", other),
    }
}

#[tokio::test]
async fn test_sql_group_by() {
    let executor = SQLExecutor::new();
    executor.register_collection("orders", make_collection("orders", vec![
        ("o1", HashMap::from([("dept", s("A")), ("amount", n(10.0))])),
        ("o2", HashMap::from([("dept", s("A")), ("amount", n(20.0))])),
        ("o3", HashMap::from([("dept", s("B")), ("amount", n(5.0))])),
        ("o4", HashMap::from([("dept", s("B")), ("amount", n(15.0))])),
    ])).await;

    let result = executor.execute(
        "SELECT dept, SUM(amount) FROM orders GROUP BY dept ORDER BY SUM(amount) DESC"
    ).await.unwrap();

    match result {
        SQLResult::Select(rows) => {
            assert_eq!(rows.len(), 2);
            // B=20, A=30. DESC puts A first.
            assert_eq!(rows[0].get("dept"), Some(&s("A")));
            assert_eq!(rows[0].get("sum(amount)"), Some(&n(30.0)));
            assert_eq!(rows[1].get("dept"), Some(&s("B")));
            assert_eq!(rows[1].get("sum(amount)"), Some(&n(20.0)));
        }
        other => panic!("Expected Select, got {:?}", other),
    }
}

#[tokio::test]
async fn test_sql_having() {
    let executor = SQLExecutor::new();
    executor.register_collection("orders", make_collection("orders", vec![
        ("o1", HashMap::from([("dept", s("A")), ("amount", n(10.0))])),
        ("o2", HashMap::from([("dept", s("A")), ("amount", n(5.0))])),
        ("o3", HashMap::from([("dept", s("B")), ("amount", n(100.0))])),
    ])).await;

    let result = executor.execute(
        "SELECT dept, SUM(amount) FROM orders GROUP BY dept HAVING SUM(amount) > 20"
    ).await.unwrap();

    match result {
        SQLResult::Select(rows) => {
            assert_eq!(rows.len(), 1, "Only dept B has sum > 20");
            assert_eq!(rows[0].get("dept"), Some(&s("B")));
        }
        other => panic!("Expected Select, got {:?}", other),
    }
}

#[tokio::test]
async fn test_sql_aggregate_with_alias() {
    let executor = SQLExecutor::new();
    executor.register_collection("t", make_collection("t", vec![
        ("x", HashMap::from([("val", n(42.0))])),
    ])).await;

    let result = executor.execute("SELECT COUNT(*) AS cnt, AVG(val) AS average FROM t").await.unwrap();
    match result {
        SQLResult::Select(rows) => {
            assert_eq!(rows[0].get("cnt"), Some(&n(1.0)));
            assert_eq!(rows[0].get("average"), Some(&n(42.0)));
        }
        other => panic!("Expected Select, got {:?}", other),
    }
}

#[tokio::test]
async fn test_sql_order_by_asc() {
    let executor = SQLExecutor::new();
    executor.register_collection("items", make_collection("items", vec![
        ("i1", HashMap::from([("name", s("Z")), ("score", n(10.0))])),
        ("i2", HashMap::from([("name", s("A")), ("score", n(30.0))])),
        ("i3", HashMap::from([("name", s("M")), ("score", n(20.0))])),
    ])).await;

    let result = executor.execute("SELECT name, score FROM items ORDER BY score ASC").await.unwrap();
    match result {
        SQLResult::Select(rows) => {
            assert_eq!(rows[0].get("name"), Some(&s("Z"))); // score=10
            assert_eq!(rows[1].get("name"), Some(&s("M"))); // score=20
            assert_eq!(rows[2].get("name"), Some(&s("A"))); // score=30
        }
        other => panic!("Expected Select, got {:?}", other),
    }
}

#[tokio::test]
async fn test_sql_order_by_desc() {
    let executor = SQLExecutor::new();
    executor.register_collection("items", make_collection("items", vec![
        ("i1", HashMap::from([("name", s("A")), ("score", n(10.0))])),
        ("i2", HashMap::from([("name", s("B")), ("score", n(30.0))])),
    ])).await;

    let result = executor.execute("SELECT name, score FROM items ORDER BY score DESC").await.unwrap();
    match result {
        SQLResult::Select(rows) => {
            assert_eq!(rows[0].get("score"), Some(&n(30.0)));
            assert_eq!(rows[1].get("score"), Some(&n(10.0)));
        }
        other => panic!("Expected Select, got {:?}", other),
    }
}

// ═══════════════════════════════════════════════════════════
// End-to-end scenario tests (JOIN + Aggregate + CRUD)
// ═══════════════════════════════════════════════════════════

#[tokio::test]
async fn test_e2e_insert_select_update_delete() {
    // Full CRUD lifecycle on a single collection
    let executor = SQLExecutor::new();

    // CREATE (implicit via INSERT)
    let r = executor.execute("INSERT INTO products (name, price, stock) VALUES ('Widget', 9.99, 100)").await.unwrap();
    assert!(matches!(r, SQLResult::Insert(1)));

    let r = executor.execute("INSERT INTO products (name, price, stock) VALUES ('Gadget', 19.99, 50)").await.unwrap();
    assert!(matches!(r, SQLResult::Insert(1)));

    // SELECT with WHERE
    let r = executor.execute("SELECT name, price FROM products WHERE name = 'Widget'").await.unwrap();
    match r {
        SQLResult::Select(rows) => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].get("name"), Some(&s("Widget")));
            assert_eq!(rows[0].get("price"), Some(&n(9.99)));
        }
        other => panic!("Expected Select, got {:?}", other),
    }

    // UPDATE
    let r = executor.execute("UPDATE products SET price = 8.99 WHERE name = 'Widget'").await.unwrap();
    assert!(matches!(r, SQLResult::Update(1)));

    // Verify UPDATE
    let r = executor.execute("SELECT price FROM products WHERE name = 'Widget'").await.unwrap();
    match r {
        SQLResult::Select(rows) => {
            assert_eq!(rows[0].get("price"), Some(&n(8.99)), "Price should be updated");
        }
        other => panic!("Expected Select, got {:?}", other),
    }

    // DELETE
    let r = executor.execute("DELETE FROM products WHERE name = 'Gadget'").await.unwrap();
    assert!(matches!(r, SQLResult::Delete(1)));

    // Verify DELETE
    let r = executor.execute("SELECT COUNT(*) FROM products").await.unwrap();
    match r {
        SQLResult::Select(rows) => {
            assert_eq!(rows[0].get("count(*)"), Some(&n(1.0)), "Only Widget should remain");
        }
        other => panic!("Expected Select, got {:?}", other),
    }
}

#[tokio::test]
async fn test_e2e_aggregate_after_inserts() {
    let executor = SQLExecutor::new();

    executor.execute("INSERT INTO sales (region, amount) VALUES ('East', 100)").await.unwrap();
    executor.execute("INSERT INTO sales (region, amount) VALUES ('East', 200)").await.unwrap();
    executor.execute("INSERT INTO sales (region, amount) VALUES ('West', 50)").await.unwrap();
    executor.execute("INSERT INTO sales (region, amount) VALUES ('West', 150)").await.unwrap();
    executor.execute("INSERT INTO sales (region, amount) VALUES ('North', 300)").await.unwrap();

    let r = executor.execute(
        "SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM sales GROUP BY region ORDER BY total DESC"
    ).await.unwrap();

    match r {
        SQLResult::Select(rows) => {
            assert_eq!(rows.len(), 3, "Three regions");
            // North=300, East=300, West=200 — DESC
            assert_eq!(rows[0].get("total"), Some(&n(300.0))); // North
            assert_eq!(rows[0].get("cnt"), Some(&n(1.0)));
            assert_eq!(rows[1].get("total"), Some(&n(300.0))); // East
            assert_eq!(rows[1].get("cnt"), Some(&n(2.0)));
            assert_eq!(rows[2].get("total"), Some(&n(200.0))); // West
        }
        other => panic!("Expected Select, got {:?}", other),
    }
}

#[tokio::test]
async fn test_e2e_join_with_aggregate_data() {
    // JOIN two tables with data suitable for aggregation
    let executor = SQLExecutor::new();

    executor.register_collection("employees", make_collection("employees", vec![
        ("e1", HashMap::from([("name", s("Alice")), ("dept_id", n(1.0))])),
        ("e2", HashMap::from([("name", s("Bob")),   ("dept_id", n(1.0))])),
        ("e3", HashMap::from([("name", s("Carol")), ("dept_id", n(2.0))])),
    ])).await;

    executor.register_collection("departments", make_collection("departments", vec![
        ("d1", HashMap::from([("dept_name", s("Engineering")), ("dept_id", n(1.0)), ("budget", n(500.0))])),
        ("d2", HashMap::from([("dept_name", s("Sales")),       ("dept_id", n(2.0)), ("budget", n(300.0))])),
    ])).await;

    // JOIN + WHERE + ORDER BY
    let r = executor.execute(
        "SELECT name, dept_name, budget FROM employees JOIN departments ON dept_id = dept_id WHERE budget > 400 ORDER BY name ASC"
    ).await.unwrap();

    match r {
        SQLResult::Select(rows) => {
            // Only Engineering has budget > 400
            assert_eq!(rows.len(), 2, "Alice and Bob in Engineering");
            assert!(rows.iter().all(|r| r.get("dept_name") == Some(&s("Engineering"))));
            assert!(rows.iter().all(|r| r.get("budget") == Some(&n(500.0))));
            assert_eq!(rows[0].get("name"), Some(&s("Alice"))); // ASC
            assert_eq!(rows[1].get("name"), Some(&s("Bob")));
        }
        other => panic!("Expected Select, got {:?}", other),
    }
}

#[tokio::test]
async fn test_e2e_multiple_aggregates_same_query() {
    let executor = SQLExecutor::new();

    executor.register_collection("grades", make_collection("grades", vec![
        ("g1", HashMap::from([("subject", s("Math")), ("score", n(85.0))])),
        ("g2", HashMap::from([("subject", s("Math")), ("score", n(95.0))])),
        ("g3", HashMap::from([("subject", s("Math")), ("score", n(75.0))])),
        ("g4", HashMap::from([("subject", s("English")), ("score", n(80.0))])),
        ("g5", HashMap::from([("subject", s("English")), ("score", n(90.0))])),
    ])).await;

    let r = executor.execute(
        "SELECT subject, COUNT(*) AS cnt, MIN(score) AS lowest, MAX(score) AS highest, AVG(score) AS average FROM grades GROUP BY subject"
    ).await.unwrap();

    match r {
        SQLResult::Select(rows) => {
            assert_eq!(rows.len(), 2);

            let math = rows.iter().find(|r| r.get("subject") == Some(&s("Math"))).unwrap();
            assert_eq!(math.get("cnt"), Some(&n(3.0)));
            assert_eq!(math.get("lowest"), Some(&n(75.0)));
            assert_eq!(math.get("highest"), Some(&n(95.0)));
            assert_eq!(math.get("average"), Some(&n(85.0))); // (85+95+75)/3

            let eng = rows.iter().find(|r| r.get("subject") == Some(&s("English"))).unwrap();
            assert_eq!(eng.get("cnt"), Some(&n(2.0)));
            assert_eq!(eng.get("average"), Some(&n(85.0))); // (80+90)/2
        }
        other => panic!("Expected Select, got {:?}", other),
    }
}

#[tokio::test]
async fn test_e2e_where_orderby_limit_combined() {
    let executor = SQLExecutor::new();

    for i in 1..=10 {
        let sql = format!("INSERT INTO logs (level, msg) VALUES ('info', 'msg{}')", i);
        executor.execute(&sql).await.unwrap();
    }
    executor.execute("INSERT INTO logs (level, msg) VALUES ('error', 'critical')").await.unwrap();
    executor.execute("INSERT INTO logs (level, msg) VALUES ('error', 'fatal')").await.unwrap();

    // WHERE + ORDER BY + LIMIT (non-aggregate)
    let r = executor.execute(
        "SELECT level, msg FROM logs WHERE level = 'error' ORDER BY msg ASC LIMIT 1"
    ).await.unwrap();

    match r {
        SQLResult::Select(rows) => {
            assert_eq!(rows.len(), 1, "LIMIT 1");
            assert_eq!(rows[0].get("msg"), Some(&s("critical")), "ASC: critical < fatal");
        }
        other => panic!("Expected Select, got {:?}", other),
    }

    // Aggregate: count by level
    let r = executor.execute(
        "SELECT level, COUNT(*) AS total FROM logs GROUP BY level HAVING COUNT(*) >= 2"
    ).await.unwrap();

    match r {
        SQLResult::Select(rows) => {
            assert_eq!(rows.len(), 2, "Both info (10) and error (2) have count >= 2");
        }
        other => panic!("Expected Select, got {:?}", other),
    }
}

#[tokio::test]
async fn test_e2e_empty_result_graceful() {
    let executor = SQLExecutor::new();

    executor.register_collection("empty_tbl", make_collection("empty_tbl", vec![])).await;

    // SELECT on empty
    let r = executor.execute("SELECT * FROM empty_tbl").await.unwrap();
    match r {
        SQLResult::Select(rows) => assert_eq!(rows.len(), 0),
        other => panic!("Expected empty Select, got {:?}", other),
    }

    // COUNT on empty
    let r = executor.execute("SELECT COUNT(*) FROM empty_tbl").await.unwrap();
    match r {
        SQLResult::Select(rows) => {
            assert_eq!(rows[0].get("count(*)"), Some(&n(0.0)));
        }
        other => panic!("Expected Select, got {:?}", other),
    }

    // No-match WHERE
    executor.execute("INSERT INTO empty_tbl (x) VALUES ('hello')").await.unwrap();
    let r = executor.execute("SELECT * FROM empty_tbl WHERE x = 'nope'").await.unwrap();
    match r {
        SQLResult::Select(rows) => assert_eq!(rows.len(), 0),
        other => panic!("Expected empty Select, got {:?}", other),
    }
}

#[tokio::test]
async fn test_e2e_type_handling() {
    let executor = SQLExecutor::new();

    executor.register_collection("mixed", make_collection("mixed", vec![
        ("m1", HashMap::from([
            ("int_val", n(42.0)),
            ("float_val", n(3.14)),
            ("str_val", s("hello")),
        ])),
        ("m2", HashMap::from([
            ("int_val", n(100.0)),
            ("float_val", n(2.71)),
            ("str_val", s("world")),
        ])),
    ])).await;

    // Sum numeric, min/max string
    let r = executor.execute(
        "SELECT SUM(int_val) AS si, AVG(float_val) AS af, MIN(str_val) AS ms, MAX(str_val) AS xs FROM mixed"
    ).await.unwrap();

    match r {
        SQLResult::Select(rows) => {
            assert_eq!(rows[0].get("si"), Some(&n(142.0)));
            // AVG：3.14+2.71 = 5.85 / 2 = 2.925, but floating point may vary slightly
            let af_val = rows[0].get("af").unwrap();
            if let SQLValue::Number(v) = af_val {
                assert!((v - 2.925).abs() < 0.001, "avg should be ~2.925, got {}", v);
            } else { panic!("Expected Number, got {:?}", af_val); }
            assert_eq!(rows[0].get("ms"), Some(&s("hello")));
            assert_eq!(rows[0].get("xs"), Some(&s("world")));
        }
        other => panic!("Expected Select, got {:?}", other),
    }
}
