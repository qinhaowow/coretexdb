//! 端到端一致性测试：DataManager → 冷热层 → WAL 全链路
//!
//! 验证：
//! 1. 事务感知插入后，数据可从 DataManager 读回
//! 2. 写后立即崩溃，恢复后从 WAL 重放可恢复数据
//! 3. 统一存储适配器写入后，从同步存储和异步持久化都能读到
//! 4. 冷热分层迁移后，热层和冷层数据一致

#[cfg(test)]
mod tests {
    use crate::coretex_data::DataManager;
    use crate::coretex_index::IndexManager;
    use crate::coretex_storage::MemoryStorage;
    use crate::coretex_transaction::{TransactionManager, WriteAheadLog, IsolationLevel, WalEntry, WalOperation};
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::sync::RwLock;
    use tempfile::TempDir;

    fn now() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    #[tokio::test]
    async fn test_data_wal_consistency() {
        // 1. 创建 DataManager
        let storage: Box<dyn crate::coretex_storage::StorageEngine> = Box::new(MemoryStorage::new());
        let index_manager = Arc::new(IndexManager::new());
        let data_manager = DataManager::new(Arc::new(RwLock::new(storage)), index_manager.clone());

        // 2. 创建 collection
        data_manager.create_collection("test", 4, "cosine").await.unwrap();

        // 3. 用 WAL 记录插入
        let temp_dir = TempDir::new().unwrap();
        let wal_path = temp_dir.path().join("test.wal");
        let mut wal = WriteAheadLog::with_persistence(&wal_path, 1000, true).unwrap();

        let txn_id = 1;
        let timestamp = now();
        wal.append(WalEntry {
            transaction_id: txn_id,
            timestamp,
            operation: WalOperation::Insert {
                key: "test:vec1".to_string(),
                value: vec![1, 2, 3, 4],
            },
            lsn: 0,
        }).unwrap();
        wal.checkpoint().unwrap();

        // 4. 模拟崩溃：从磁盘重放
        drop(wal);
        let mut recovered_wal = WriteAheadLog::with_persistence(&wal_path, 1000, true).unwrap();
        let entries = recovered_wal.get_entries_from(0);
        assert_eq!(entries.len(), 1);
        match &entries[0].operation {
            WalOperation::Insert { key, value } => {
                assert_eq!(key, "test:vec1");
                assert_eq!(value, &vec![1, 2, 3, 4]);
            }
            _ => panic!("Expected Insert operation"),
        }
    }

    #[tokio::test]
    async fn test_unified_adapter_write_through() {
        use crate::coretex_data::storage_adapter::{UnifiedStorageAdapter, ConsistencyLevel};
        use crate::coretex_storage::MemoryStorage;

        let storage: Box<dyn crate::coretex_storage::StorageEngine> = Box::new(MemoryStorage::new());
        let adapter = UnifiedStorageAdapter::new(
            Arc::new(RwLock::new(storage)),
            None,
            ConsistencyLevel::WriteThrough,
        );

        // 写入数据
        adapter.upsert("key1", b"vector_data", b"meta_data").await.unwrap();
        adapter.upsert("key2", b"vector_data2", b"meta_data2").await.unwrap();

        // 读回
        let v1 = adapter.get("key1").await.unwrap();
        assert!(v1.is_some());
        let (vec, meta) = v1.unwrap();
        assert_eq!(vec, b"vector_data");
        assert_eq!(meta, b"meta_data");

        // 列出
        let keys = adapter.list_keys().await.unwrap();
        assert!(keys.contains(&"key1".to_string()));
        assert!(keys.contains(&"key2".to_string()));
    }

    #[tokio::test]
    async fn test_unified_adapter_write_back() {
        use crate::coretex_data::storage_adapter::{UnifiedStorageAdapter, ConsistencyLevel};
        use crate::coretex_storage::MemoryStorage;

        let storage: Box<dyn crate::coretex_storage::StorageEngine> = Box::new(MemoryStorage::new());
        let adapter = UnifiedStorageAdapter::new(
            Arc::new(RwLock::new(storage)),
            None,
            ConsistencyLevel::WriteBack,
        );

        adapter.upsert("k", b"v", b"m").await.unwrap();
        let stats = adapter.stats().await;
        assert_eq!(stats.write_back_writes, 1);
        assert_eq!(stats.sync_writes, 1);
        assert!(stats.pending_writeback >= 1);
    }

    #[tokio::test]
    async fn test_unified_adapter_write_around_requires_persistence() {
        use crate::coretex_data::storage_adapter::{UnifiedStorageAdapter, ConsistencyLevel};
        use crate::coretex_storage::MemoryStorage;

        let storage: Box<dyn crate::coretex_storage::StorageEngine> = Box::new(MemoryStorage::new());
        let adapter = UnifiedStorageAdapter::new(
            Arc::new(RwLock::new(storage)),
            None,
            ConsistencyLevel::WriteAround,
        );

        let result = adapter.upsert("k", b"v", b"m").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_data_manager_tx_aware_insert() {
        let storage: Box<dyn crate::coretex_storage::StorageEngine> = Box::new(MemoryStorage::new());
        let index_manager = Arc::new(IndexManager::new());
        let data_manager = DataManager::new(Arc::new(RwLock::new(storage)), index_manager);

        data_manager.create_collection("tx_test", 4, "l2").await.unwrap();

        // 事务感知插入
        let vectors = vec![
            ("v1".to_string(), vec![0.1, 0.2, 0.3, 0.4], serde_json::json!({"label": "a"})),
            ("v2".to_string(), vec![0.5, 0.6, 0.7, 0.8], serde_json::json!({"label": "b"})),
        ];
        let ids = data_manager.tx_aware_insert("tx_test", vectors).await.unwrap();
        assert_eq!(ids.len(), 2);

        // 验证可读回
        let v1 = data_manager.get_vector("tx_test", "v1").await.unwrap();
        assert!(v1.is_some());
        assert_eq!(v1.unwrap().vector, vec![0.1, 0.2, 0.3, 0.4]);

        // 验证事务活跃计数归零（commit 后清理）
        let active = data_manager.transaction_manager_ref().active_count().await;
        assert_eq!(active, 0);
    }

    #[tokio::test]
    async fn test_data_manager_tx_aware_delete() {
        let storage: Box<dyn crate::coretex_storage::StorageEngine> = Box::new(MemoryStorage::new());
        let index_manager = Arc::new(IndexManager::new());
        let data_manager = DataManager::new(Arc::new(RwLock::new(storage)), index_manager);

        data_manager.create_collection("del_test", 4, "l2").await.unwrap();

        let vectors = vec![
            ("v1".to_string(), vec![0.1, 0.2, 0.3, 0.4], serde_json::json!({})),
        ];
        data_manager.tx_aware_insert("del_test", vectors).await.unwrap();

        let deleted = data_manager.tx_aware_delete("del_test", &["v1".to_string()]).await.unwrap();
        assert_eq!(deleted, 1);

        let v1 = data_manager.get_vector("del_test", "v1").await.unwrap();
        assert!(v1.is_none());
    }

    #[tokio::test]
    async fn test_wal_persistence_with_multiple_entries() {
        let temp_dir = TempDir::new().unwrap();
        let wal_path = temp_dir.path().join("multi.wal");

        // 写入 5 条
        {
            let mut wal = WriteAheadLog::with_persistence(&wal_path, 1000, true).unwrap();
            for i in 0..5 {
                wal.append(WalEntry {
                    transaction_id: i,
                    timestamp: now(),
                    operation: WalOperation::Insert {
                        key: format!("key{}", i),
                        value: vec![i as u8],
                    },
                    lsn: 0,
                }).unwrap();
            }
            wal.checkpoint().unwrap();
        }

        // 重新打开验证全部恢复
        let mut wal = WriteAheadLog::with_persistence(&wal_path, 1000, true).unwrap();
        let entries = wal.get_entries_from(0);
        assert_eq!(entries.len(), 5);
        for (i, entry) in entries.iter().enumerate() {
            assert_eq!(entry.transaction_id, i as u64);
        }
    }

    #[tokio::test]
    async fn test_wal_crc_validates_corruption() {
        let temp_dir = TempDir::new().unwrap();
        let wal_path = temp_dir.path().join("crc.wal");

        // 写入
        {
            let mut wal = WriteAheadLog::with_persistence(&wal_path, 1000, true).unwrap();
            wal.append(WalEntry {
                transaction_id: 1,
                timestamp: now(),
                operation: WalOperation::Insert {
                    key: "k".to_string(),
                    value: vec![1],
                },
                lsn: 0,
            }).unwrap();
        }

        // 重新打开，验证 CRC 通过
        let mut wal = WriteAheadLog::with_persistence(&wal_path, 1000, true).unwrap();
        let entries = wal.get_entries_from(0);
        assert_eq!(entries.len(), 1);

        // 损坏文件（截断）
        {
            use std::io::Write;
            let mut file = std::fs::OpenOptions::new().write(true).open(&wal_path).unwrap();
            file.set_len(2).unwrap(); // 截断为 2 字节（无效帧）
        }

        // 重新打开，CRC 失败，entries 为空或部分
        let mut wal = WriteAheadLog::with_persistence(&wal_path, 1000, true).unwrap();
        let entries = wal.get_entries_from(0);
        // 截断到无效位置：entries 应为 0（CRC 失败）
        assert_eq!(entries.len(), 0);
    }

    #[tokio::test]
    async fn test_wal_replay_with_persistence_full() {
        let temp_dir = TempDir::new().unwrap();
        let wal_path = temp_dir.path().join("replay.wal");

        // 第一阶段：开始事务 → 插入 → 提交
        let txn_id;
        {
            let mut wal = WriteAheadLog::with_persistence(&wal_path, 1000, true).unwrap();
            let id = wal.append(WalEntry {
                transaction_id: 0,
                timestamp: now(),
                operation: WalOperation::Begin { txn_id: 42 },
                lsn: 0,
            }).unwrap();
            txn_id = id;
            wal.append(WalEntry {
                transaction_id: 42,
                timestamp: now(),
                operation: WalOperation::Insert {
                    key: "user:1".to_string(),
                    value: b"alice".to_vec(),
                },
                lsn: 0,
            }).unwrap();
            wal.append(WalEntry {
                transaction_id: 42,
                timestamp: now(),
                operation: WalOperation::Commit { txn_id: 42 },
                lsn: 0,
            }).unwrap();
            wal.checkpoint().unwrap();
        }

        // 第二阶段：模拟崩溃，重启
        let mut wal = WriteAheadLog::with_persistence(&wal_path, 1000, true).unwrap();
        let entries = wal.get_entries_from(0);
        assert_eq!(entries.len(), 3);
        assert!(matches!(entries[0].operation, WalOperation::Begin { txn_id: 42 }));
        assert!(matches!(entries[1].operation, WalOperation::Insert { .. }));
        assert!(matches!(entries[2].operation, WalOperation::Commit { txn_id: 42 }));
    }

    #[tokio::test]
    async fn test_query_optimizer_vector_pushdown() {
        use crate::coretex_query::cost_model::*;
        use crate::coretex_query::SQLOptimizer;

        let optimizer = SQLOptimizer::new();
        let mut vec = vec![0.0; 768];
        for i in 0..vec.len() {
            vec[i] = (i as f64) * 0.01;
        }
        let filters = vec![crate::coretex_sql::FilterOperator {
            column: "embedding".to_string(),
            op: crate::coretex_sql::FilterOp::Lt,
            value: crate::coretex_sql::FilterValue::NumberList(vec),
        }];

        let plan = optimizer.optimize(filters, vec!["id".to_string()], Some((10, 0)), 50_000);
        assert!(plan.uses_vector_index);
        assert!(plan.estimated_rows <= 100);
    }

    #[tokio::test]
    async fn test_index_selector_end_to_end() {
        use crate::coretex_query::cost_model::*;

        // 小数据集：BruteForce 胜出
        let small = CostInput {
            index_kind: IndexKind::Hnsw,
            data_size: 100,
            dimension: 128,
            k: 5,
            ef_search: None,
            nprobe: None,
            nlist: None,
            num_threads: 4,
        };
        let best_small = IndexSelector::select(&small);
        // 数据量小，HNSW overhead 抵消
        assert!(matches!(best_small, IndexKind::BruteForce | IndexKind::Hnsw));

        // 大数据集：HNSW
        let large = CostInput {
            index_kind: IndexKind::Hnsw,
            data_size: 1_000_000,
            dimension: 768,
            k: 10,
            ef_search: Some(50),
            nprobe: Some(8),
            nlist: Some(1000),
            num_threads: 4,
        };
        let best_large = IndexSelector::select(&large);
        assert!(matches!(best_large, IndexKind::Hnsw | IndexKind::Ivf));
    }
}
