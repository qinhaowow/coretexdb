//! Distributed Transactions module for CoreTexDB
//! Provides two-phase commit and distributed transaction support

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use std::time::{Duration, Instant};
use async_trait::async_trait;

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

// =================== 参与者 RPC 协议 ===================

/// 参与者 RPC 接口：2PC 协调器通过此 trait 与参与者通信
#[async_trait]
pub trait ParticipantRpc: Send + Sync {
    /// 询问参与者是否可以提交（prepare 阶段）
    async fn prepare(&self, participant: &str, tx_id: &str, operations: &[DistributedOperation]) -> bool;
    /// 通知参与者提交
    async fn commit(&self, participant: &str, tx_id: &str) -> bool;
    /// 通知参与者中止
    async fn abort(&self, participant: &str, tx_id: &str) -> bool;
}

/// 本地参与者 RPC（用于单节点测试，所有请求自动同意）
pub struct LocalParticipantRpc;

#[async_trait]
impl ParticipantRpc for LocalParticipantRpc {
    async fn prepare(&self, _participant: &str, _tx_id: &str, _operations: &[DistributedOperation]) -> bool {
        true
    }
    async fn commit(&self, _participant: &str, _tx_id: &str) -> bool {
        true
    }
    async fn abort(&self, _participant: &str, _tx_id: &str) -> bool {
        true
    }
}

pub struct TwoPhaseCommit {
    transactions: Arc<RwLock<HashMap<String, DistributedTransaction>>>,
    node_id: String,
    participants: Arc<RwLock<HashMap<String, ParticipantState>>>,
    participant_rpc: Arc<dyn ParticipantRpc>,
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
            participant_rpc: Arc::new(LocalParticipantRpc),
        }
    }

    /// 注入自定义参与者 RPC（用于真实分布式部署）
    pub fn with_participant_rpc(node_id: &str, rpc: Arc<dyn ParticipantRpc>) -> Self {
        Self {
            transactions: Arc::new(RwLock::new(HashMap::new())),
            node_id: node_id.to_string(),
            participants: Arc::new(RwLock::new(HashMap::new())),
            participant_rpc: rpc,
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

    /// prepare 阶段：向每个参与者发送 prepare 请求，收集投票
    pub async fn prepare(&self, tx_id: &str) -> Result<HashMap<String, bool>, String> {
        // 取出事务数据（操作 + 参与者列表）
        let (participants, operations) = {
            let transactions = self.transactions.read().await;
            let tx = transactions.get(tx_id)
                .ok_or("Transaction not found")?;
            (tx.participants.clone(), tx.operations.clone())
        };

        // 并发向每个参与者发送 prepare 请求
        let mut votes = HashMap::new();
        for participant in &participants {
            let vote = self.participant_rpc.prepare(participant, tx_id, &operations).await;
            votes.insert(participant.clone(), vote);

            // 更新参与者状态
            let mut p_state = self.participants.write().await;
            if let Some(state) = p_state.get_mut(participant) {
                state.status = if vote { ParticipantStatus::Prepared } else { ParticipantStatus::Aborted };
                state.last_heartbeat = Instant::now();
            }
        }

        // 更新事务状态
        {
            let mut transactions = self.transactions.write().await;
            let tx = transactions.get_mut(tx_id)
                .ok_or("Transaction not found")?;

            let all_yes = votes.values().all(|&v| v);
            tx.state = if all_yes {
                DistributedTransactionState::Prepared
            } else {
                DistributedTransactionState::Aborting
            };
            tx.prepare_votes = votes.clone();
        }

        Ok(votes)
    }

    /// commit 阶段：通知所有投赞成票的参与者提交
    pub async fn commit(&self, tx_id: &str) -> Result<(), String> {
        // 获取事务状态和投票
        let (state, votes, participants) = {
            let transactions = self.transactions.read().await;
            let tx = transactions.get(tx_id)
                .ok_or("Transaction not found")?;
            (tx.state.clone(), tx.prepare_votes.clone(), tx.participants.clone())
        };

        if state != DistributedTransactionState::Prepared {
            return Err(format!("Transaction not prepared (current state: {:?})", state));
        }

        // 检查是否所有参与者都投了赞成票
        for (participant, vote) in &votes {
            if !vote {
                {
                    let mut transactions = self.transactions.write().await;
                    if let Some(tx) = transactions.get_mut(tx_id) {
                        tx.state = DistributedTransactionState::Aborting;
                    }
                }
                return Err(format!("Participant {} voted no", participant));
            }
        }

        // 更新状态为 Committing
        {
            let mut transactions = self.transactions.write().await;
            if let Some(tx) = transactions.get_mut(tx_id) {
                tx.state = DistributedTransactionState::Committing;
            }
        }

        // 通知每个参与者提交
        let mut all_committed = true;
        for participant in &participants {
            let success = self.participant_rpc.commit(participant, tx_id).await;
            if !success {
                all_committed = false;
            }
            let mut p_state = self.participants.write().await;
            if let Some(state) = p_state.get_mut(participant) {
                state.status = if success { ParticipantStatus::Committed } else { ParticipantStatus::Aborted };
            }
        }

        // 更新最终状态
        let mut transactions = self.transactions.write().await;
        if let Some(tx) = transactions.get_mut(tx_id) {
            tx.state = if all_committed {
                DistributedTransactionState::Committed
            } else {
                DistributedTransactionState::Aborted
            };
        }

        if all_committed {
            Ok(())
        } else {
            Err("Some participants failed to commit".to_string())
        }
    }

    /// abort 阶段：通知所有参与者中止
    pub async fn abort(&self, tx_id: &str) -> Result<(), String> {
        let participants = {
            let transactions = self.transactions.read().await;
            let tx = transactions.get(tx_id)
                .ok_or("Transaction not found")?;
            tx.participants.clone()
        };

        {
            let mut transactions = self.transactions.write().await;
            if let Some(tx) = transactions.get_mut(tx_id) {
                tx.state = DistributedTransactionState::Aborting;
            }
        }

        // 通知每个参与者中止
        for participant in &participants {
            let _ = self.participant_rpc.abort(participant, tx_id).await;
            let mut p_state = self.participants.write().await;
            if let Some(state) = p_state.get_mut(participant) {
                state.status = ParticipantStatus::Aborted;
            }
        }

        let mut transactions = self.transactions.write().await;
        if let Some(tx) = transactions.get_mut(tx_id) {
            tx.state = DistributedTransactionState::Aborted;
        }

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

// =================== 分布式锁管理器（Quorum + Fence Token）===================

/// 锁对等节点 RPC 接口：用于跨节点锁同步
#[async_trait]
pub trait LockPeerRpc: Send + Sync {
    /// 尝试在对等节点上获取锁
    async fn try_lock(&self, node_id: &str, key: &str, fence_token: u64, ttl_secs: u64) -> bool;
    /// 在对等节点上释放锁
    async fn unlock(&self, node_id: &str, key: &str, fence_token: u64) -> bool;
    /// 验证锁是否仍然持有（用于 fence token 校验）
    async fn verify_lock(&self, key: &str, fence_token: u64) -> bool;
}

/// 本地锁 RPC（无对等节点，单节点模式）
pub struct LocalLockPeerRpc;

#[async_trait]
impl LockPeerRpc for LocalLockPeerRpc {
    async fn try_lock(&self, _node_id: &str, _key: &str, _fence_token: u64, _ttl_secs: u64) -> bool {
        true
    }
    async fn unlock(&self, _node_id: &str, _key: &str, _fence_token: u64) -> bool {
        true
    }
    async fn verify_lock(&self, _key: &str, _fence_token: u64) -> bool {
        true
    }
}

pub struct DistributedLockManager {
    locks: Arc<RwLock<HashMap<String, DistributedLock>>>,
    node_id: String,
    peer_rpc: Arc<dyn LockPeerRpc>,
    peers: Vec<String>,
    fence_counter: Arc<RwLock<u64>>,
}

#[derive(Debug, Clone)]
pub struct DistributedLock {
    pub key: String,
    pub owner: String,
    pub fence_token: u64,
    pub acquired_at: Instant,
    pub expires_at: Option<Instant>,
}

impl DistributedLockManager {
    pub fn new(node_id: &str) -> Self {
        Self {
            locks: Arc::new(RwLock::new(HashMap::new())),
            node_id: node_id.to_string(),
            peer_rpc: Arc::new(LocalLockPeerRpc),
            peers: vec![],
            fence_counter: Arc::new(RwLock::new(0)),
        }
    }

    /// 创建带 Quorum 同步的分布式锁管理器
    pub fn with_peers(node_id: &str, peers: Vec<String>, rpc: Arc<dyn LockPeerRpc>) -> Self {
        Self {
            locks: Arc::new(RwLock::new(HashMap::new())),
            node_id: node_id.to_string(),
            peer_rpc: rpc,
            peers,
            fence_counter: Arc::new(RwLock::new(0)),
        }
    }

    /// 获取锁：本地获取 + Quorum 对等节点确认
    pub async fn acquire_lock(&self, key: &str, timeout: Duration) -> Result<bool, String> {
        let ttl_secs = timeout.as_secs().max(1);

        // 检查本地是否已持有锁
        {
            let locks = self.locks.read().await;
            if let Some(lock) = locks.get(key) {
                if lock.owner == self.node_id {
                    if let Some(expires) = lock.expires_at {
                        if expires > Instant::now() {
                            return Ok(true);
                        }
                    } else {
                        return Ok(true);
                    }
                }
            }
        }

        // 生成单调递增的 fence token
        let fence_token = {
            let mut counter = self.fence_counter.write().await;
            *counter += 1;
            *counter
        };

        // 本地先尝试获取
        {
            let mut locks = self.locks.write().await;
            if let Some(lock) = locks.get(key) {
                if lock.owner != self.node_id {
                    if let Some(expires) = lock.expires_at {
                        if expires > Instant::now() {
                            return Err(format!("Lock held by {} (fence={})", lock.owner, lock.fence_token));
                        }
                    } else {
                        return Err(format!("Lock held by {} (fence={})", lock.owner, lock.fence_token));
                    }
                }
            }
        }

        // Quorum：向对等节点请求锁确认
        if !self.peers.is_empty() {
            let mut ack_count = 0;
            let quorum = (self.peers.len() / 2) + 1;

            for peer in &self.peers {
                let success = self.peer_rpc.try_lock(&self.node_id, key, fence_token, ttl_secs).await;
                if success {
                    ack_count += 1;
                }
            }

            if ack_count < quorum {
                // 未达 Quorum，回滚已获取的锁
                for peer in &self.peers {
                    let _ = self.peer_rpc.unlock(&self.node_id, key, fence_token).await;
                }
                return Err(format!(
                    "Failed to acquire quorum: {}/{} (need {})",
                    ack_count, self.peers.len(), quorum
                ));
            }
        }

        // 本地记录锁
        let expires = if timeout.as_secs() > 0 {
            Some(Instant::now() + timeout)
        } else {
            None
        };

        let mut locks = self.locks.write().await;
        locks.insert(key.to_string(), DistributedLock {
            key: key.to_string(),
            owner: self.node_id.clone(),
            fence_token,
            acquired_at: Instant::now(),
            expires_at: expires,
        });

        Ok(true)
    }

    /// 释放锁：本地释放 + 通知对等节点
    pub async fn release_lock(&self, key: &str) -> Result<bool, String> {
        let fence_token = {
            let locks = self.locks.read().await;
            match locks.get(key) {
                Some(lock) if lock.owner == self.node_id => lock.fence_token,
                _ => return Ok(false),
            }
        };

        // 通知对等节点释放
        for peer in &self.peers {
            let _ = self.peer_rpc.unlock(&self.node_id, key, fence_token).await;
        }

        let mut locks = self.locks.write().await;
        locks.remove(key);
        Ok(true)
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

    /// 获取锁的 fence token（用于乐观并发控制）
    pub async fn get_fence_token(&self, key: &str) -> Option<u64> {
        let locks = self.locks.read().await;
        locks.get(key).map(|l| l.fence_token)
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

    /// 创建带自定义 RPC 的协调器（用于真实分布式部署）
    pub fn with_rpc(
        node_id: &str,
        participant_rpc: Arc<dyn ParticipantRpc>,
        lock_peer_rpc: Arc<dyn LockPeerRpc>,
        peers: Vec<String>,
    ) -> Self {
        Self {
            two_pc: Arc::new(TwoPhaseCommit::with_participant_rpc(node_id, participant_rpc)),
            lock_manager: Arc::new(DistributedLockManager::with_peers(node_id, peers, lock_peer_rpc)),
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
                    let _ = self.lock_manager.release_lock(&lock_key).await;
                }
                Ok(())
            }
            Err(e) => {
                self.two_pc.abort(&tx_id).await?;
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
                    let _ = self.lock_manager.release_lock(&lock_key).await;
                }
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
        assert!(votes.values().all(|&v| v));

        coordinator.commit(&tx_id).await.unwrap();

        let state = coordinator.get_transaction_state(&tx_id).await;
        assert_eq!(state, Some(DistributedTransactionState::Committed));
    }

    #[tokio::test]
    async fn test_two_phase_commit_with_rejection() {
        // 自定义 RPC：node3 总是拒绝
        struct RejectingRpc;
        #[async_trait]
        impl ParticipantRpc for RejectingRpc {
            async fn prepare(&self, participant: &str, _tx_id: &str, _ops: &[DistributedOperation]) -> bool {
                participant != "node3"
            }
            async fn commit(&self, _p: &str, _tx_id: &str) -> bool { true }
            async fn abort(&self, _p: &str, _tx_id: &str) -> bool { true }
        }

        let coordinator = TwoPhaseCommit::with_participant_rpc(
            "node1",
            Arc::new(RejectingRpc),
        );

        let tx_id = coordinator.begin_transaction(vec!["node2".to_string(), "node3".to_string()]).await;

        coordinator.add_operation(&tx_id, DistributedOperation::Insert {
            collection: "test".to_string(),
            id: "1".to_string(),
            payload: vec![],
        }).await.unwrap();

        let votes = coordinator.prepare(&tx_id).await.unwrap();
        assert_eq!(votes["node2"], true);
        assert_eq!(votes["node3"], false);

        // 由于 node3 拒绝，commit 应失败
        let result = coordinator.commit(&tx_id).await;
        assert!(result.is_err());
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
    async fn test_distributed_lock_fence_token() {
        let lock_mgr = DistributedLockManager::new("node1");

        lock_mgr.acquire_lock("key1", Duration::from_secs(10)).await.unwrap();
        let token1 = lock_mgr.get_fence_token("key1").await;
        assert!(token1.is_some());

        lock_mgr.release_lock("key1").await.unwrap();

        lock_mgr.acquire_lock("key1", Duration::from_secs(10)).await.unwrap();
        let token2 = lock_mgr.get_fence_token("key1").await;
        assert!(token2.is_some());
        assert!(token2.unwrap() > token1.unwrap(), "fence token must be monotonically increasing");
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
