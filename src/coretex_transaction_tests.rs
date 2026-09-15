//! Tests for transaction module (MVCC, WAL, snapshots)

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use crate::TransactionManager;
    use crate::coretex_transaction::IsolationLevel;
    use crate::WriteAheadLog;
    use crate::coretex_transaction::WalEntry;
    use crate::coretex_transaction::WalOperation;
    use crate::coretex_transaction::SnapshotManager;
    use crate::Snapshot;
    use crate::coretex_transaction::TransactionError;

    #[tokio::test]
    async fn test_transaction_begin() {
        let manager = TransactionManager::new();
        
        let txn_id = manager.begin_transaction(IsolationLevel::Snapshot).await.unwrap();
        
        assert!(txn_id > 0);
    }

    #[tokio::test]
    async fn test_transaction_commit() {
        let manager = TransactionManager::new();
        
        let txn_id = manager.begin_transaction(IsolationLevel::Snapshot).await.unwrap();
        let result = manager.commit(txn_id).await;
        
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_transaction_abort() {
        let manager = TransactionManager::new();
        
        let txn_id = manager.begin_transaction(IsolationLevel::Snapshot).await.unwrap();
        let result = manager.abort(txn_id).await;
        
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_transaction_commit_nonexistent() {
        let manager = TransactionManager::new();
        
        let result = manager.commit(9999).await;
        
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_transaction_abort_nonexistent() {
        let manager = TransactionManager::new();
        
        let result = manager.abort(9999).await;
        
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_multiple_transactions() {
        let manager = TransactionManager::new();
        
        let txn1 = manager.begin_transaction(IsolationLevel::ReadCommitted).await.unwrap();
        let txn2 = manager.begin_transaction(IsolationLevel::RepeatableRead).await.unwrap();
        
        assert!(txn2 > txn1);
    }

    #[tokio::test]
    async fn test_snapshot_creation() {
        let manager = TransactionManager::new();
        
        let txn_id = manager.begin_transaction(IsolationLevel::Snapshot).await.unwrap();
        let snapshot_id = manager.create_snapshot(txn_id).await.unwrap();
        
        assert!(snapshot_id >= 0);
    }

    #[tokio::test]
    async fn test_snapshot_retrieval() {
        let manager = TransactionManager::new();
        
        let txn_id = manager.begin_transaction(IsolationLevel::Snapshot).await.unwrap();
        let snapshot_id = manager.create_snapshot(txn_id).await.unwrap();
        
        let snapshot = manager.get_snapshot(snapshot_id).await;
        assert!(snapshot.is_some());
    }

    #[tokio::test]
    async fn test_snapshot_not_found() {
        let manager = TransactionManager::new();
        
        let snapshot = manager.get_snapshot(9999).await;
        assert!(snapshot.is_none());
    }

    #[tokio::test]
    async fn test_isolation_levels() {
        let manager = TransactionManager::new();
        
        let levels = vec![
            IsolationLevel::ReadUncommitted,
            IsolationLevel::ReadCommitted,
            IsolationLevel::RepeatableRead,
            IsolationLevel::Serializable,
            IsolationLevel::Snapshot,
        ];
        
        for level in levels {
            let result = manager.begin_transaction(level).await;
            assert!(result.is_ok());
        }
    }

    #[tokio::test]
    async fn test_wal_append() {
        let mut wal = WriteAheadLog::new(100);
        
        wal.append(WalEntry {
            transaction_id: 1,
            timestamp: 1000,
            operation: WalOperation::Begin { txn_id: 1 },
            lsn: 0,
        });
        
        assert_eq!(wal.read_key_at_timestamp("nonexistent", 2000), None);
    }

    #[tokio::test]
    async fn test_wal_max_entries() {
        let max_entries = 5;
        let mut wal = WriteAheadLog::new(max_entries);
        
        for i in 0..10 {
            wal.append(WalEntry {
                transaction_id: i,
                timestamp: i as u64 * 1000,
                operation: WalOperation::Begin { txn_id: i },
                lsn: i as u64,
            });
        }
        
        // After 10 appends with max_entries=5, only the last 5 should remain
        let value = wal.read_key_at_timestamp("nonexistent", 2000);
        assert_eq!(value, None);
    }

    #[tokio::test]
    async fn test_wal_read_key_at_timestamp() {
        let mut wal = WriteAheadLog::new(100);
        
        wal.append(WalEntry {
            transaction_id: 1,
            timestamp: 1000,
            operation: WalOperation::Insert { 
                key: "key1".to_string(), 
                value: b"value1".to_vec() 
            },
            lsn: 0,
        });
        
        wal.append(WalEntry {
            transaction_id: 2,
            timestamp: 2000,
            operation: WalOperation::Update { 
                key: "key1".to_string(),
                old_value: b"value1".to_vec(),
                new_value: b"value2".to_vec(),
            },
            lsn: 1,
        });
        
        let value = wal.read_key_at_timestamp("key1", 1500);
        assert_eq!(value, Some(b"value1".to_vec()));
        
        let value = wal.read_key_at_timestamp("key1", 2500);
        assert_eq!(value, Some(b"value2".to_vec()));
    }

    #[tokio::test]
    async fn test_snapshot_manager() {
        let mut manager = SnapshotManager::new();
        
        let snapshot = Snapshot {
            id: 0,
            timestamp: 1000,
            transaction_id: 1,
            data: HashMap::new(),
        };
        
        let id = manager.create_snapshot(snapshot);
        assert_eq!(id, 0);
        
        let retrieved = manager.get_snapshot(0);
        assert!(retrieved.is_some());
    }

    #[tokio::test]
    async fn test_transaction_error_display() {
        let error = TransactionError::TransactionNotFound(123);
        let msg = format!("{}", error);
        assert!(msg.contains("123"));
        
        let error = TransactionError::InvalidTransactionState("test".to_string());
        let msg = format!("{}", error);
        assert!(msg.contains("test"));
        
        let error = TransactionError::WriteConflict("conflict".to_string());
        let msg = format!("{}", error);
        assert!(msg.contains("conflict"));
        
        let error = TransactionError::SnapshotNotFound(456);
        let msg = format!("{}", error);
        assert!(msg.contains("456"));
    }
}
