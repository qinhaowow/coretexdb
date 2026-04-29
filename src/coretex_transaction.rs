//! Transaction and Version Control for CortexDB
//! Implements MVCC and WAL for ACID transactions and time-travel queries

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;

pub struct TransactionManager {
    active_transactions: Arc<RwLock<HashMap<TransactionId, Transaction>>>,
    wal: Arc<RwLock<WriteAheadLog>>,
    snapshot_manager: Arc<RwLock<SnapshotManager>>,
    current_txn_id: Arc<RwLock<TransactionId>>,
}

pub type TransactionId = u64;

#[derive(Debug, Clone)]
pub enum TransactionState {
    Active,
    Committed,
    Aborted,
}

#[derive(Debug)]
pub struct Transaction {
    pub id: TransactionId,
    pub state: TransactionState,
    pub start_timestamp: u64,
    pub write_set: Vec<WriteOperation>,
    pub read_set: Vec<ReadOperation>,
    pub isolation_level: IsolationLevel,
}

#[derive(Debug, Clone)]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
    Snapshot,
}

#[derive(Debug, Clone)]
pub enum WriteOperation {
    Insert { key: String, value: Vec<u8> },
    Update { key: String, old_value: Vec<u8>, new_value: Vec<u8> },
    Delete { key: String, old_value: Vec<u8> },
}

#[derive(Debug, Clone)]
pub struct ReadOperation {
    pub key: String,
    pub timestamp: u64,
}

#[derive(Debug, Clone)]
pub struct WriteAheadLog {
    entries: Vec<WalEntry>,
    max_entries: usize,
}

#[derive(Debug, Clone)]
pub struct WalEntry {
    pub transaction_id: TransactionId,
    pub timestamp: u64,
    pub operation: WalOperation,
    pub lsn: u64,
}

#[derive(Debug, Clone)]
pub enum WalOperation {
    Begin { txn_id: TransactionId },
    Insert { key: String, value: Vec<u8> },
    Update { key: String, old_value: Vec<u8>, new_value: Vec<u8> },
    Delete { key: String, value: Vec<u8> },
    Commit { txn_id: TransactionId },
    Abort { txn_id: TransactionId },
}

pub struct SnapshotManager {
    snapshots: HashMap<SnapshotId, Snapshot>,
    next_snapshot_id: SnapshotId,
}

pub type SnapshotId = u64;

#[derive(Debug, Clone)]
pub struct Snapshot {
    pub id: SnapshotId,
    pub timestamp: u64,
    pub transaction_id: TransactionId,
    pub data: HashMap<String, Vec<u8>>,
}

impl TransactionManager {
    pub fn new() -> Self {
        Self {
            active_transactions: Arc::new(RwLock::new(HashMap::new())),
            wal: Arc::new(RwLock::new(WriteAheadLog::new(10000))),
            snapshot_manager: Arc::new(RwLock::new(SnapshotManager::new())),
            current_txn_id: Arc::new(RwLock::new(0)),
        }
    }

    pub async fn begin_transaction(&self, isolation_level: IsolationLevel) -> Result<TransactionId, TransactionError> {
        let txn_id = {
            let mut counter = self.current_txn_id.write().await;
            *counter += 1;
            *counter
        };

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let transaction = Transaction {
            id: txn_id,
            state: TransactionState::Active,
            start_timestamp: timestamp,
            write_set: Vec::new(),
            read_set: Vec::new(),
            isolation_level: isolation_level.clone(),
        };

        {
            let mut active = self.active_transactions.write().await;
            active.insert(txn_id, transaction);
        }

        {
            let mut wal = self.wal.write().await;
            wal.append(WalEntry {
                transaction_id: txn_id,
                timestamp,
                operation: WalOperation::Begin { txn_id },
                lsn: wal.entries.len() as u64,
            });
        }

        Ok(txn_id)
    }

    pub async fn commit(&self, txn_id: TransactionId) -> Result<(), TransactionError> {
        let mut active = self.active_transactions.write().await;
        
        let transaction = active.get_mut(&txn_id)
            .ok_or(TransactionError::TransactionNotFound(txn_id))?;

        if transaction.state != TransactionState::Active {
            return Err(TransactionError::InvalidTransactionState(
                "Transaction is not active".to_string()
            ));
        }

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        {
            let mut wal = self.wal.write().await;
            wal.append(WalEntry {
                transaction_id: txn_id,
                timestamp,
                operation: WalOperation::Commit { txn_id },
                lsn: wal.entries.len() as u64,
            });
        }

        transaction.state = TransactionState::Committed;

        Ok(())
    }

    pub async fn abort(&self, txn_id: TransactionId) -> Result<(), TransactionError> {
        let mut active = self.active_transactions.write().await;
        
        let transaction = active.get_mut(&txn_id)
            .ok_or(TransactionError::TransactionNotFound(txn_id))?;

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        {
            let mut wal = self.wal.write().await;
            wal.append(WalEntry {
                transaction_id: txn_id,
                timestamp,
                operation: WalOperation::Abort { txn_id },
                lsn: wal.entries.len() as u64,
            });
        }

        transaction.state = TransactionState::Aborted;

        Ok(())
    }

    pub async fn create_snapshot(&self, txn_id: TransactionId) -> Result<SnapshotId, TransactionError> {
        let active = self.active_transactions.read().await;
        
        let transaction = active.get(&txn_id)
            .ok_or(TransactionError::TransactionNotFound(txn_id))?;

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let snapshot = Snapshot {
            id: 0,
            timestamp,
            transaction_id: txn_id,
            data: HashMap::new(),
        };

        let mut manager = self.snapshot_manager.write().await;
        let snapshot_id = manager.create_snapshot(snapshot);

        Ok(snapshot_id)
    }

    pub async fn get_snapshot(&self, snapshot_id: SnapshotId) -> Option<Snapshot> {
        let manager = self.snapshot_manager.read().await;
        manager.get_snapshot(snapshot_id)
    }

    pub async fn read_at_timestamp(&self, key: &str, timestamp: u64) -> Option<Vec<u8>> {
        let wal = self.wal.read().await;
        wal.read_key_at_timestamp(key, timestamp)
    }

    pub async fn get_transaction_history(&self, key: &str) -> Vec<TransactionHistoryEntry> {
        let wal = self.wal.read().await;
        wal.get_history(key)
    }

    pub async fn get_wal_entries(&self, from_lsn: u64) -> Vec<WalEntry> {
        let wal = self.wal.read().await;
        wal.get_entries_from(from_lsn)
    }
}

impl WriteAheadLog {
    pub fn new(max_entries: usize) -> Self {
        Self {
            entries: Vec::new(),
            max_entries,
        }
    }

    pub fn append(&mut self, entry: WalEntry) {
        if self.entries.len() >= self.max_entries {
            self.entries.remove(0);
        }
        self.entries.push(entry);
    }

    pub fn read_key_at_timestamp(&self, key: &str, timestamp: u64) -> Option<Vec<u8>> {
        let mut value: Option<Vec<u8>> = None;
        
        for entry in &self.entries {
            if entry.timestamp > timestamp {
                break;
            }
            
            match &entry.operation {
                WalOperation::Insert { key: k, value: v } if k == key => {
                    value = Some(v.clone());
                },
                WalOperation::Update { key: k, new_value: v, .. } if k == key => {
                    value = Some(v.clone());
                },
                WalOperation::Delete { key: k, .. } if k == key => {
                    value = None;
                },
                _ => {},
            }
        }
        
        value
    }

    pub fn get_history(&self, key: &str) -> Vec<TransactionHistoryEntry> {
        let mut history = Vec::new();
        
        for entry in &self.entries {
            match &entry.operation {
                WalOperation::Insert { key: k, value } if k == key => {
                    history.push(TransactionHistoryEntry {
                        timestamp: entry.timestamp,
                        operation: "INSERT".to_string(),
                        value: value.clone(),
                    });
                },
                WalOperation::Update { key: k, new_value, .. } if k == key => {
                    history.push(TransactionHistoryEntry {
                        timestamp: entry.timestamp,
                        operation: "UPDATE".to_string(),
                        value: new_value.clone(),
                    });
                },
                WalOperation::Delete { key: k, value } if k == key => {
                    history.push(TransactionHistoryEntry {
                        timestamp: entry.timestamp,
                        operation: "DELETE".to_string(),
                        value: value.clone(),
                    });
                },
                _ => {},
            }
        }
        
        history
    }

    pub fn get_entries_from(&self, from_lsn: u64) -> Vec<WalEntry> {
        self.entries
            .iter()
            .filter(|e| e.lsn >= from_lsn)
            .cloned()
            .collect()
    }
}

impl SnapshotManager {
    pub fn new() -> Self {
        Self {
            snapshots: HashMap::new(),
            next_snapshot_id: 0,
        }
    }

    pub fn create_snapshot(&mut self, snapshot: Snapshot) -> SnapshotId {
        let id = self.next_snapshot_id;
        self.next_snapshot_id += 1;
        
        let mut s = snapshot;
        s.id = id;
        self.snapshots.insert(id, s);
        
        id
    }

    pub fn get_snapshot(&self, id: SnapshotId) -> Option<Snapshot> {
        self.snapshots.get(&id).cloned()
    }
}

#[derive(Debug, Clone)]
pub struct TransactionHistoryEntry {
    pub timestamp: u64,
    pub operation: String,
    pub value: Vec<u8>,
}

#[derive(Debug)]
pub enum TransactionError {
    TransactionNotFound(TransactionId),
    InvalidTransactionState(String),
    WriteConflict(String),
    SnapshotNotFound(SnapshotId),
}

impl std::fmt::Display for TransactionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransactionError::TransactionNotFound(id) => {
                write!(f, "Transaction {} not found", id)
            },
            TransactionError::InvalidTransactionState(msg) => {
                write!(f, "Invalid transaction state: {}", msg)
            },
            TransactionError::WriteConflict(msg) => {
                write!(f, "Write conflict: {}", msg)
            },
            TransactionError::SnapshotNotFound(id) => {
                write!(f, "Snapshot {} not found", id)
            },
        }
    }
}

impl std::error::Error for TransactionError {}
