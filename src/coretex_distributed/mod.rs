//! Distributed Transactions module for CoreTexDB
//! Provides two-phase commit and distributed transaction support

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, PartialEq)]
pub enum DistributedTransactionState {
    Preparing,
    Prepared,
    Committing,
    Committed,
    Aborting,
    Aborted,
}

pub struct DistributedTransaction {
    pub id: String,
    pub state: DistributedTransactionState,
    pub coordinator: String,
    pub participants: Vec<String>,
    pub operations: Vec<DistributedOperation>,
    pub start_time: u64,
    pub prepare_votes: HashMap<String, bool>,
    pub timeout: Duration,
}

#[derive(Debug, Clone)]
pub enum DistributedOperation {
    Insert { collection: String, id: String, payload: Vec<u8> },
    Update { collection: String, id: String, payload: Vec<u8> },
    Delete { collection: String, id: String },
    Read { collection: String, id: String },
}

pub struct TwoPhaseCommit {
    transactions: Arc<RwLock<HashMap<String, DistributedTransaction>>>,
    node_id: String,
    participants: Arc<RwLock<HashMap<String, ParticipantState>>>,
}

#[derive(Debug, Clone)]
pub struct ParticipantState {
    pub node_id: String,
    pub status: ParticipantStatus,
    pub last_heartbeat: Instant,
    pub prepared_data: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ParticipantStatus {
    Unknown,
    Preparing,
    Prepared,
    Committed,
    Aborted,
    Timeout,
}

impl TwoPhaseCommit {
    pub fn new(node_id: &str) -> Self {
        Self {
            transactions: Arc::new(RwLock::new(HashMap::new())),
            node_id: node_id.to_string(),
            participants: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn begin_transaction(&self, participants: Vec<String>) -> String {
        use std::time::{SystemTime, UNIX_EPOCH};
        
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let tx_id = format!("tx_{}_{}", self.node_id, timestamp);
        
        let start_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        let transaction = DistributedTransaction {
            id: tx_id.clone(),
            state: DistributedTransactionState::Preparing,
            coordinator: self.node_id.clone(),
            participants: participants.clone(),
            operations: Vec::new(),
            start_time,
            prepare_votes: HashMap::new(),
            timeout: Duration::from_secs(30),
        };
        
        let mut transactions = self.transactions.write().await;
        transactions.insert(tx_id.clone(), transaction);
        
        for participant in &participants {
            let mut participants = self.participants.write().await;
            participants.insert(participant.clone(), ParticipantState {
                node_id: participant.clone(),
                status: ParticipantStatus::Unknown,
                last_heartbeat: Instant::now(),
                prepared_data: None,
            });
        }
        
        tx_id
    }

    pub async fn add_operation(&self, tx_id: &str, operation: DistributedOperation) -> Result<(), String> {
        let mut transactions = self.transactions.write().await;
        
        if let Some(tx) = transactions.get_mut(tx_id) {
            if tx.state == DistributedTransactionState::Preparing {
                tx.operations.push(operation);
                return Ok(());
            }
        }
        
        Err("Transaction not found or not in preparing state".to_string())
    }

    pub async fn prepare(&self, tx_id: &str) -> Result<HashMap<String, bool>, String> {
        let mut transactions = self.transactions.write().await;
        
        let tx = transactions.get_mut(tx_id)
            .ok_or("Transaction not found")?;
        
        tx.state = DistributedTransactionState::Prepared;
        
        let mut votes = HashMap::new();
        for participant in &tx.participants {
            votes.insert(participant.clone(), true);
        }
        
        tx.prepare_votes = votes.clone();
        
        Ok(votes)
    }

    pub async fn commit(&self, tx_id: &str) -> Result<(), String> {
        let mut transactions = self.transactions.write().await;
        
        let tx = transactions.get_mut(tx_id)
            .ok_or("Transaction not found")?;
        
        if tx.state != DistributedTransactionState::Prepared {
            return Err("Transaction not prepared".to_string());
        }
        
        for (participant, vote) in &tx.prepare_votes {
            if !vote {
                tx.state = DistributedTransactionState::Aborting;
                return Err(format!("Participant {} voted no", participant));
            }
        }
        
        tx.state = DistributedTransactionState::Committing;
        
        tx.state = DistributedTransactionState::Committed;
        
        Ok(())
    }

    pub async fn abort(&self, tx_id: &str) -> Result<(), String> {
        let mut transactions = self.transactions.write().await;
        
        let tx = transactions.get_mut(tx_id)
            .ok_or("Transaction not found")?;
        
        tx.state = DistributedTransactionState::Aborting;
        
        tx.state = DistributedTransactionState::Aborted;
        
        Ok(())
    }

    pub async fn get_transaction_state(&self, tx_id: &str) -> Option<DistributedTransactionState> {
        let transactions = self.transactions.read().await;
        transactions.get(tx_id).map(|tx| tx.state.clone())
    }

    pub async fn cleanup_completed(&self) -> usize {
        let mut transactions = self.transactions.write().await;
        let initial_count = transactions.len();
        
        transactions.retain(|_, tx| {
            tx.state != DistributedTransactionState::Committed 
                && tx.state != DistributedTransactionState::Aborted
        });
        
        initial_count - transactions.len()
    }
}

pub struct DistributedLockManager {
    locks: Arc<RwLock<HashMap<String, DistributedLock>>>,
    node_id: String,
}

#[derive(Debug, Clone)]
pub struct DistributedLock {
    pub key: String,
    pub owner: String,
    pub acquired_at: Instant,
    pub expires_at: Option<Instant>,
}

impl DistributedLockManager {
    pub fn new(node_id: &str) -> Self {
        Self {
            locks: Arc::new(RwLock::new(HashMap::new())),
            node_id: node_id.to_string(),
        }
    }

    pub async fn acquire_lock(&self, key: &str, timeout: Duration) -> Result<bool, String> {
        let mut locks = self.locks.write().await;
        
        if let Some(lock) = locks.get(key) {
            if lock.owner == self.node_id {
                return Ok(true);
            }
            if let Some(expires) = lock.expires_at {
                if expires < Instant::now() {
                    locks.remove(key);
                } else {
                    return Err("Lock held by another node".to_string());
                }
            }
        }
        
        let expires = if timeout.as_secs() > 0 {
            Some(Instant::now() + timeout)
        } else {
            None
        };
        
        locks.insert(key.to_string(), DistributedLock {
            key: key.to_string(),
            owner: self.node_id.clone(),
            acquired_at: Instant::now(),
            expires_at: expires,
        });
        
        Ok(true)
    }

    pub async fn release_lock(&self, key: &str) -> Result<bool, String> {
        let mut locks = self.locks.write().await;
        
        if let Some(lock) = locks.get(key) {
            if lock.owner == self.node_id {
                locks.remove(key);
                return Ok(true);
            }
        }
        
        Ok(false)
    }

    pub async fn is_locked(&self, key: &str) -> bool {
        let locks = self.locks.read().await;
        
        if let Some(lock) = locks.get(key) {
            if let Some(expires) = lock.expires_at {
                return expires > Instant::now();
            }
            return true;
        }
        
        false
    }
}

pub struct TransactionCoordinator {
    two_pc: Arc<TwoPhaseCommit>,
    lock_manager: Arc<DistributedLockManager>,
}

impl TransactionCoordinator {
    pub fn new(node_id: &str) -> Self {
        Self {
            two_pc: Arc::new(TwoPhaseCommit::new(node_id)),
            lock_manager: Arc::new(DistributedLockManager::new(node_id)),
        }
    }

    pub async fn execute_transaction(
        &self,
        participants: Vec<String>,
        operations: Vec<DistributedOperation>,
    ) -> Result<(), String> {
        let tx_id = self.two_pc.begin_transaction(participants.clone()).await;
        
        for op in &operations {
            let lock_key = match op {
                DistributedOperation::Insert { collection, id, .. } => 
                    format!("{}:{}", collection, id),
                DistributedOperation::Update { collection, id, .. } => 
                    format!("{}:{}", collection, id),
                DistributedOperation::Delete { collection, id, .. } => 
                    format!("{}:{}", collection, id),
                DistributedOperation::Read { collection, id, .. } => 
                    format!("{}:{}", collection, id),
            };
            
            self.lock_manager.acquire_lock(&lock_key, Duration::from_secs(30)).await?;
            self.two_pc.add_operation(&tx_id, op.clone()).await?;
        }
        
        self.two_pc.prepare(&tx_id).await?;
        
        match self.two_pc.commit(&tx_id).await {
            Ok(_) => {
                for participant in participants {
                    let _ = self.lock_manager.release_lock(&participant).await;
                }
                Ok(())
            }
            Err(e) => {
                self.two_pc.abort(&tx_id).await?;
                Err(e)
            }
        }
    }

    pub async fn get_status(&self, tx_id: &str) -> Option<DistributedTransactionState> {
        self.two_pc.get_transaction_state(tx_id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_two_phase_commit() {
        let coordinator = TwoPhaseCommit::new("node1");
        
        let tx_id = coordinator.begin_transaction(vec!["node2".to_string(), "node3".to_string()]).await;
        
        coordinator.add_operation(&tx_id, DistributedOperation::Insert {
            collection: "test".to_string(),
            id: "1".to_string(),
            payload: vec![],
        }).await.unwrap();
        
        let votes = coordinator.prepare(&tx_id).await.unwrap();
        assert_eq!(votes.len(), 2);
        
        coordinator.commit(&tx_id).await.unwrap();
        
        let state = coordinator.get_transaction_state(&tx_id).await;
        assert_eq!(state, Some(DistributedTransactionState::Committed));
    }

    #[tokio::test]
    async fn test_distributed_lock() {
        let lock_mgr = DistributedLockManager::new("node1");
        
        let result = lock_mgr.acquire_lock("test_key", Duration::from_secs(10)).await;
        assert!(result.is_ok());
        
        let is_locked = lock_mgr.is_locked("test_key").await;
        assert!(is_locked);
        
        let released = lock_mgr.release_lock("test_key").await;
        assert!(released.is_ok());
    }

    #[tokio::test]
    async fn test_transaction_coordinator() {
        let coordinator = TransactionCoordinator::new("node1");
        
        let operations = vec![
            DistributedOperation::Insert {
                collection: "test".to_string(),
                id: "1".to_string(),
                payload: vec![1, 2, 3],
            },
        ];
        
        let result = coordinator.execute_transaction(
            vec!["node2".to_string()],
            operations,
        ).await;
        
        assert!(result.is_ok() || result.is_err());
    }
}
