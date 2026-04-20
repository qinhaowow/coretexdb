//! Transaction support for CortexDB

use std::sync::Arc;
use tokio::sync::RwLock;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub enum TransactionState {
    Pending,
    Running,
    Committed,
    Aborted,
}

pub struct Transaction {
    pub id: u64,
    pub state: TransactionState,
    pub operations: Vec<TransactionOperation>,
    pub start_time: u64,
    pub commit_time: Option<u64>,
}

#[derive(Debug, Clone)]
pub enum TransactionOperation {
    Insert { collection: String, id: String, vector: Vec<f32>, metadata: serde_json::Value },
    Update { collection: String, id: String, vector: Option<Vec<f32>>, metadata: Option<serde_json::Value> },
    Delete { collection: String, id: String },
    CreateCollection { name: String, dimension: usize },
    DeleteCollection { name: String },
}

pub struct TransactionManager {
    transactions: Arc<RwLock<HashMap<u64, Transaction>>>,
    active_transactions: Arc<RwLock<HashMap<String, u64>>>,
    transaction_counter: Arc<RwLock<u64>>,
    lock_manager: Arc<LockManager>,
}

impl TransactionManager {
    pub fn new() -> Self {
        Self {
            transactions: Arc::new(RwLock::new(HashMap::new())),
            active_transactions: Arc::new(RwLock::new(HashMap::new())),
            transaction_counter: Arc::new(RwLock::new(0)),
            lock_manager: Arc::new(LockManager::new()),
        }
    }

    pub async fn begin_transaction(&self) -> u64 {
        let mut counter = self.transaction_counter.write().await;
        *counter += 1;
        let tx_id = *counter;
        
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        let transaction = Transaction {
            id: tx_id,
            state: TransactionState::Pending,
            operations: Vec::new(),
            start_time: timestamp,
            commit_time: None,
        };
        
        let mut transactions = self.transactions.write().await;
        transactions.insert(tx_id, transaction);
        
        tx_id
    }

    pub async fn add_operation(&self, tx_id: u64, operation: TransactionOperation) -> Result<(), String> {
        let mut transactions = self.transactions.write().await;
        
        if let Some(transaction) = transactions.get_mut(&tx_id) {
            if matches!(transaction.state, TransactionState::Pending) {
                transaction.operations.push(operation);
                return Ok(());
            }
        }
        
        Err("Transaction not found or already started".to_string())
    }

    pub async fn commit(&self, tx_id: u64) -> Result<(), String> {
        let mut transactions = self.transactions.write().await;
        
        if let Some(transaction) = transactions.get_mut(&tx_id) {
            if matches!(transaction.state, TransactionState::Pending) {
                for op in &transaction.operations {
                    match op {
                        TransactionOperation::Insert { collection, id, .. } => {
                            let lock_key = format!("{}:{}", collection, id);
                            self.lock_manager.unlock(&lock_key, tx_id).await;
                        }
                        TransactionOperation::Delete { collection, id, .. } => {
                            let lock_key = format!("{}:{}", collection, id);
                            self.lock_manager.unlock(&lock_key, tx_id).await;
                        }
                        _ => {}
                    }
                }
                
                transaction.state = TransactionState::Committed;
                
                let timestamp = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                transaction.commit_time = Some(timestamp);
                
                let mut active = self.active_transactions.write().await;
                active.remove(&tx_id.to_string());
                
                return Ok(());
            }
        }
        
        Err("Transaction not found or cannot be committed".to_string())
    }

    pub async fn abort(&self, tx_id: u64) -> Result<(), String> {
        let mut transactions = self.transactions.write().await;
        
        if let Some(transaction) = transactions.get_mut(&tx_id) {
            if matches!(transaction.state, TransactionState::Pending) {
                for op in &transaction.operations {
                    match op {
                        TransactionOperation::Insert { collection, id, .. } => {
                            let lock_key = format!("{}:{}", collection, id);
                            self.lock_manager.unlock(&lock_key, tx_id).await;
                        }
                        TransactionOperation::Delete { collection, id, .. } => {
                            let lock_key = format!("{}:{}", collection, id);
                            self.lock_manager.unlock(&lock_key, tx_id).await;
                        }
                        _ => {}
                    }
                }
                
                transaction.state = TransactionState::Aborted;
                
                let mut active = self.active_transactions.write().await;
                active.remove(&tx_id.to_string());
                
                return Ok(());
            }
        }
        
        Err("Transaction not found or cannot be aborted".to_string())
    }

    pub async fn get_transaction(&self, tx_id: u64) -> Option<Transaction> {
        let transactions = self.transactions.read().await;
        transactions.get(&tx_id).cloned()
    }
}

pub struct LockManager {
    locks: Arc<RwLock<HashMap<String, Lock>>>,
}

#[derive(Debug, Clone)]
pub struct Lock {
    pub key: String,
    pub owner: u64,
    pub lock_type: LockType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockType {
    Read,
    Write,
}

impl LockManager {
    pub fn new() -> Self {
        Self {
            locks: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn lock(&self, key: &str, owner: u64, lock_type: LockType) -> bool {
        let mut locks = self.locks.write().await;
        
        if let Some(existing) = locks.get(key) {
            if existing.owner == owner || existing.lock_type == LockType::Read && lock_type == LockType::Read {
                return true;
            }
            return false;
        }
        
        locks.insert(key.to_string(), Lock {
            key: key.to_string(),
            owner,
            lock_type,
        });
        
        true
    }

    pub async fn unlock(&self, key: &str, _owner: u64) {
        let mut locks = self.locks.write().await;
        locks.remove(key);
    }

    pub async fn is_locked(&self, key: &str) -> bool {
        let locks = self.locks.read().await;
        locks.contains_key(key)
    }
}
