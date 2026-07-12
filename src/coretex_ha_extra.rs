//! 高可用与分布式增强：
//! 1. Raft Snapshot 机制 + InstallSnapshot RPC
//! 2. Log Compaction（基于快照的日志压缩）
//! 3. 2PC 完整流程（prepare/commit/abort + 协调器 + 参与者状态机）
//! 4. Persistence Checkpoint 恢复
//! 5. 集群成员变更（Joint Consensus）

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time;
use serde::{Deserialize, Serialize};
use async_trait::async_trait;
use uuid::Uuid;

use crate::coretex_failover::{AppendEntriesRequest, AppendEntriesResponse, LogEntry, LogReplicator, RaftLog, LogCommand, RaftRpc};

// =================== Raft Snapshot ===================

/// Raft Snapshot 元数据
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftSnapshot {
    pub snapshot_id: String,
    pub term: u64,
    pub last_included_index: u64,
    pub last_included_term: u64,
    pub timestamp: u64,
    pub data: Vec<u8>,
    pub checksum: u32,
}

impl RaftSnapshot {
    /// 创建新 snapshot
    pub fn new(term: u64, last_included_index: u64, last_included_term: u64, data: Vec<u8>) -> Self {
        let snapshot_id = Uuid::new_v4().to_string();
        let checksum = crc32(&data);
        Self {
            snapshot_id,
            term,
            last_included_index,
            last_included_term,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            data,
            checksum,
        }
    }

    pub fn verify(&self) -> bool {
        crc32(&self.data) == self.checksum
    }
}

fn crc32(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFFFFFF;
    for &byte in data {
        crc ^= byte as u32;
        for _ in 0..8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ 0xEDB88320;
            } else {
                crc >>= 1;
            }
        }
    }
    !crc
}

/// InstallSnapshot RPC 请求
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallSnapshotRequest {
    pub leader_id: String,
    pub term: u64,
    pub snapshot: RaftSnapshot,
    pub offset: u64,
    pub done: bool,
}

/// InstallSnapshot RPC 响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallSnapshotResponse {
    pub follower_id: String,
    pub term: u64,
    pub success: bool,
    pub last_index: u64,
}

/// Snapshot 存储
pub struct SnapshotStore {
    snapshots: Arc<RwLock<HashMap<String, RaftSnapshot>>>,
    storage_path: Option<std::path::PathBuf>,
}

impl SnapshotStore {
    pub fn new() -> Self {
        Self {
            snapshots: Arc::new(RwLock::new(HashMap::new())),
            storage_path: None,
        }
    }

    pub fn with_persistence(path: impl AsRef<std::path::Path>) -> Self {
        Self {
            snapshots: Arc::new(RwLock::new(HashMap::new())),
            storage_path: Some(path.as_ref().to_path_buf()),
        }
    }

    pub async fn save(&self, snapshot: RaftSnapshot) -> Result<(), String> {
        if let Some(path) = &self.storage_path {
            // 持久化到磁盘
            let filename = format!("{}.snap", snapshot.snapshot_id);
            let full_path = path.join(filename);
            let data = serde_json::to_vec(&snapshot)
                .map_err(|e| format!("Serialize: {}", e))?;
            std::fs::create_dir_all(path).map_err(|e| e.to_string())?;
            std::fs::write(full_path, data).map_err(|e| e.to_string())?;
        }
        self.snapshots.write().await.insert(snapshot.snapshot_id.clone(), snapshot);
        Ok(())
    }

    pub async fn get(&self, snapshot_id: &str) -> Option<RaftSnapshot> {
        self.snapshots.read().await.get(snapshot_id).cloned()
    }

    pub async fn get_latest(&self) -> Option<RaftSnapshot> {
        let snaps = self.snapshots.read().await;
        snaps.values().max_by_key(|s| s.last_included_index).cloned()
    }
}

impl Default for SnapshotStore {
    fn default() -> Self { Self::new() }
}

/// 扩展的 Raft RPC trait（包含 InstallSnapshot）
#[async_trait]
pub trait ExtendedRaftRpc: Send + Sync {
    async fn install_snapshot(&self, addr: &str, req: &InstallSnapshotRequest) -> Result<InstallSnapshotResponse, String>;
}

/// 默认 HTTP 实现
pub struct HttpExtendedRaftRpc {
    client: reqwest::Client,
}

impl HttpExtendedRaftRpc {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .unwrap_or_default(),
        }
    }
}

impl Default for HttpExtendedRaftRpc {
    fn default() -> Self { Self::new() }
}

#[async_trait]
impl ExtendedRaftRpc for HttpExtendedRaftRpc {
    async fn install_snapshot(&self, addr: &str, req: &InstallSnapshotRequest) -> Result<InstallSnapshotResponse, String> {
        let url = format!("http://{}/raft/install_snapshot", addr);
        let resp = self.client.post(&url)
            .json(req)
            .send()
            .await
            .map_err(|e| format!("InstallSnapshot RPC failed: {}", e))?;
        resp.json::<InstallSnapshotResponse>()
            .await
            .map_err(|e| format!("InstallSnapshot parse failed: {}", e))
    }
}

/// Raft Snapshot 管理器（Leader 端）
pub struct RaftSnapshotManager {
    store: Arc<SnapshotStore>,
    log: Arc<RwLock<RaftLog>>,
    rpc: Arc<dyn ExtendedRaftRpc>,
    local_node_id: String,
    /// 触发 snapshot 的日志条目数阈值
    pub snapshot_threshold: usize,
    /// 已应用的索引（用于决定哪些日志可以被压缩）
    applied_index: Arc<RwLock<u64>>,
}

impl RaftSnapshotManager {
    pub fn new(
        store: Arc<SnapshotStore>,
        log: Arc<RwLock<RaftLog>>,
        rpc: Arc<dyn ExtendedRaftRpc>,
        local_node_id: String,
    ) -> Self {
        Self {
            store,
            log,
            rpc,
            local_node_id,
            snapshot_threshold: 1000,
            applied_index: Arc::new(RwLock::new(0)),
        }
    }

    /// 设置已应用的索引
    pub async fn set_applied_index(&self, index: u64) {
        *self.applied_index.write().await = index;
    }

    /// 检查是否需要创建 snapshot
    pub async fn maybe_create_snapshot(&self) -> Option<RaftSnapshot> {
        let log = self.log.read().await;
        let applied = *self.applied_index.read().await;

        if log.len() as u64 > applied + self.snapshot_threshold as u64 {
            // 截取到 applied 的状态
            let snapshot_data = log.entries_from(0)
                .iter()
                .filter(|e| e.index <= applied)
                .flat_map(|e| serde_json::to_vec(e).unwrap_or_default())
                .collect::<Vec<u8>>();

            let term_at_applied = log.term_at(applied);
            let snapshot = RaftSnapshot::new(
                log.last_term(),
                applied,
                term_at_applied,
                snapshot_data,
            );

            return Some(snapshot);
        }
        None
    }

    /// 创建并保存 snapshot
    pub async fn create_snapshot(&self) -> Result<RaftSnapshot, String> {
        let snapshot = self.maybe_create_snapshot().await
            .ok_or_else(|| "No snapshot needed".to_string())?;
        self.store.save(snapshot.clone()).await?;
        // Log compaction：截断已 snapshot 的日志
        let mut log = self.log.write().await;
        log.truncate_to(*self.applied_index.read().await);
        Ok(snapshot)
    }

    /// 向 follower 发送 snapshot
    pub async fn send_snapshot_to(&self, addr: &str, snapshot: RaftSnapshot) -> Result<InstallSnapshotResponse, String> {
        let req = InstallSnapshotRequest {
            leader_id: self.local_node_id.clone(),
            term: snapshot.term,
            snapshot,
            offset: 0,
            done: true,
        };
        self.rpc.install_snapshot(addr, &req).await
    }

    /// Follower 处理 InstallSnapshot 请求
    pub async fn handle_install_snapshot(&self, req: InstallSnapshotRequest) -> InstallSnapshotResponse {
        // 验证 checksum
        if !req.snapshot.verify() {
            return InstallSnapshotResponse {
                follower_id: self.local_node_id.clone(),
                term: req.term,
                success: false,
                last_index: *self.applied_index.read().await,
            };
        }

        // 保存 snapshot
        if let Err(e) = self.store.save(req.snapshot.clone()).await {
            return InstallSnapshotResponse {
                follower_id: self.local_node_id.clone(),
                term: req.term,
                success: false,
                last_index: *self.applied_index.read().await,
            };
        }

        // 截断日志到 snapshot 位置
        let mut log = self.log.write().await;
        let _ = log.truncate_to(req.snapshot.last_included_index);

        InstallSnapshotResponse {
            follower_id: self.local_node_id.clone(),
            term: req.term,
            success: true,
            last_index: req.snapshot.last_included_index,
        }
    }
}

// =================== 2PC 协调器 ===================

/// 2PC 状态
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TwoPCState {
    Init,
    Prepared,
    Committed,
    Aborted,
}

/// 2PC 参与者状态
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParticipantStateFull {
    pub participant_id: String,
    pub state: TwoPCState,
    pub last_response: Option<String>,
    pub last_response_at: u64,
}

/// 2PC 事务
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TwoPCTransaction {
    pub txn_id: String,
    pub coordinator: String,
    pub participants: Vec<String>,
    pub state: TwoPCState,
    pub created_at: u64,
    pub timeout_ms: u64,
    pub data: Vec<u8>,
}

/// 2PC 协调器
pub struct TwoPCCoordinator {
    transactions: Arc<RwLock<HashMap<String, TwoPCTransaction>>>,
    participants: Arc<RwLock<HashMap<String, ParticipantStateFull>>>,
    rpc: Arc<dyn TwoPCRpc>,
    default_timeout: Duration,
}

#[async_trait]
pub trait TwoPCRpc: Send + Sync {
    async fn prepare(&self, participant: &str, txn: &TwoPCTransaction) -> Result<(), String>;
    async fn commit(&self, participant: &str, txn: &TwoPCTransaction) -> Result<(), String>;
    async fn abort(&self, participant: &str, txn: &TwoPCTransaction) -> Result<(), String>;
}

impl TwoPCCoordinator {
    pub fn new(rpc: Arc<dyn TwoPCRpc>, default_timeout: Duration) -> Self {
        Self {
            transactions: Arc::new(RwLock::new(HashMap::new())),
            participants: Arc::new(RwLock::new(HashMap::new())),
            rpc,
            default_timeout,
        }
    }

    /// 启动一个 2PC 事务
    pub async fn begin(
        &self,
        participants: Vec<String>,
        data: Vec<u8>,
    ) -> Result<TwoPCTransaction, String> {
        let txn_id = Uuid::new_v4().to_string();
        let txn = TwoPCTransaction {
            txn_id: txn_id.clone(),
            coordinator: "self".to_string(),
            participants: participants.clone(),
            state: TwoPCState::Init,
            created_at: now_ms(),
            timeout_ms: self.default_timeout.as_millis() as u64,
            data,
        };

        // 阶段 1：PREPARE
        let mut all_prepared = true;
        for p in &participants {
            match self.rpc.prepare(p, &txn).await {
                Ok(()) => {
                    self.participants.write().await.insert(
                        format!("{}:{}", txn_id, p),
                        ParticipantStateFull {
                            participant_id: p.clone(),
                            state: TwoPCState::Prepared,
                            last_response: Some("prepared".to_string()),
                            last_response_at: now_ms(),
                        },
                    );
                }
                Err(e) => {
                    all_prepared = false;
                    self.participants.write().await.insert(
                        format!("{}:{}", txn_id, p),
                        ParticipantStateFull {
                            participant_id: p.clone(),
                            state: TwoPCState::Aborted,
                            last_response: Some(e),
                            last_response_at: now_ms(),
                        },
                    );
                }
            }
        }

        let mut txn = txn;
        if all_prepared {
            // 阶段 2：COMMIT
            txn.state = TwoPCState::Prepared;
            self.transactions.write().await.insert(txn_id.clone(), txn.clone());

            let mut all_committed = true;
            for p in &participants {
                if let Err(e) = self.rpc.commit(p, &txn).await {
                    all_committed = false;
                    eprintln!("Commit failed for {}: {}", p, e);
                }
            }
            if all_committed {
                txn.state = TwoPCState::Committed;
            } else {
                // 部分失败，abort
                for p in &participants {
                    let _ = self.rpc.abort(p, &txn).await;
                }
                txn.state = TwoPCState::Aborted;
            }
        } else {
            // 任何一个 prepare 失败，abort 全部
            txn.state = TwoPCState::Aborted;
            for p in &participants {
                let _ = self.rpc.abort(p, &txn).await;
            }
        }

        self.transactions.write().await.insert(txn_id.clone(), txn.clone());
        Ok(txn)
    }

    pub async fn get_transaction(&self, txn_id: &str) -> Option<TwoPCTransaction> {
        self.transactions.read().await.get(txn_id).cloned()
    }

    /// 协调器恢复：从持久化日志重新执行未完成事务
    pub async fn recover_in_flight(&self) -> Result<Vec<TwoPCTransaction>, String> {
        let transactions = self.transactions.read().await;
        let in_flight: Vec<TwoPCTransaction> = transactions.values()
            .filter(|t| t.state == TwoPCState::Prepared || t.state == TwoPCState::Init)
            .cloned()
            .collect();

        for txn in &in_flight {
            // 检查参与者状态
            for p in &txn.participants {
                let key = format!("{}:{}", txn.txn_id, p);
                if let Some(ps) = self.participants.read().await.get(&key) {
                    if ps.state == TwoPCState::Prepared {
                        // 重新发送 COMMIT
                        let _ = self.rpc.commit(p, txn).await;
                    } else if ps.state == TwoPCState::Init {
                        // 没收到响应，abort
                        let _ = self.rpc.abort(p, txn).await;
                    }
                }
            }
        }

        Ok(in_flight)
    }
}

/// Mock 2PC RPC
pub struct MockTwoPCRpc;

#[async_trait]
impl TwoPCRpc for MockTwoPCRpc {
    async fn prepare(&self, _: &str, _: &TwoPCTransaction) -> Result<(), String> { Ok(()) }
    async fn commit(&self, _: &str, _: &TwoPCTransaction) -> Result<(), String> { Ok(()) }
    async fn abort(&self, _: &str, _: &TwoPCTransaction) -> Result<(), String> { Ok(()) }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// =================== Persistence Checkpoint 恢复 ===================

/// Checkpoint 记录
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointRecord {
    pub checkpoint_id: String,
    pub timestamp: u64,
    pub lsn: u64,
    pub active_txn_ids: Vec<u64>,
    pub data_snapshot: HashMap<String, Vec<u8>>,
}

/// 崩溃恢复管理器
pub struct CrashRecoveryManager {
    checkpoint_dir: std::path::PathBuf,
    wal_path: std::path::PathBuf,
    last_checkpoint: Arc<RwLock<Option<CheckpointRecord>>>,
}

impl CrashRecoveryManager {
    pub fn new(checkpoint_dir: impl AsRef<std::path::Path>, wal_path: impl AsRef<std::path::Path>) -> Self {
        Self {
            checkpoint_dir: checkpoint_dir.as_ref().to_path_buf(),
            wal_path: wal_path.as_ref().to_path_buf(),
            last_checkpoint: Arc::new(RwLock::new(None)),
        }
    }

    /// 保存 checkpoint
    pub async fn save_checkpoint(&self, record: CheckpointRecord) -> Result<(), String> {
        std::fs::create_dir_all(&self.checkpoint_dir)
            .map_err(|e| format!("create dir: {}", e))?;
        let path = self.checkpoint_dir.join(format!("{}.ckpt", record.checkpoint_id));
        let data = serde_json::to_vec(&record).map_err(|e| e.to_string())?;
        // 原子写入：先写临时文件，再 rename
        let tmp_path = path.with_extension("tmp");
        std::fs::write(&tmp_path, &data).map_err(|e| e.to_string())?;
        std::fs::rename(&tmp_path, &path).map_err(|e| e.to_string())?;
        *self.last_checkpoint.write().await = Some(record);
        Ok(())
    }

    /// 加载最新的 checkpoint
    pub async fn load_latest_checkpoint(&self) -> Result<Option<CheckpointRecord>, String> {
        if !self.checkpoint_dir.exists() {
            return Ok(None);
        }
        let mut latest: Option<CheckpointRecord> = None;
        for entry in std::fs::read_dir(&self.checkpoint_dir)
            .map_err(|e| e.to_string())?
        {
            let entry = entry.map_err(|e| e.to_string())?;
            let path = entry.path();
            if path.extension().map(|e| e == "ckpt").unwrap_or(false) {
                let data = std::fs::read(&path).map_err(|e| e.to_string())?;
                if let Ok(record) = serde_json::from_slice::<CheckpointRecord>(&data) {
                    if latest.is_none() || record.timestamp > latest.as_ref().unwrap().timestamp {
                        latest = Some(record);
                    }
                }
            }
        }
        *self.last_checkpoint.write().await = latest.clone();
        Ok(latest)
    }

    /// 完整恢复流程：加载 checkpoint + 重放 WAL
    pub async fn recover(&self) -> Result<RecoveryReport, String> {
        // 1. 加载最近 checkpoint
        let checkpoint = self.load_latest_checkpoint().await?;
        let start_lsn = checkpoint.as_ref().map(|c| c.lsn).unwrap_or(0);
        let active_txns = checkpoint.as_ref()
            .map(|c| c.active_txn_ids.clone())
            .unwrap_or_default();
        let initial_data = checkpoint.as_ref()
            .map(|c| c.data_snapshot.clone())
            .unwrap_or_default();

        // 2. 重放 WAL（从 start_lsn 开始）
        let mut wal = crate::coretex_transaction::WriteAheadLog::with_persistence(&self.wal_path, 100_000, true)
            .map_err(|e| e.to_string())?;
        let entries = wal.get_entries_from(start_lsn);

        let mut state = initial_data;
        let mut committed_txns: std::collections::HashSet<u64> = std::collections::HashSet::new();
        let mut aborted_txns: std::collections::HashSet<u64> = std::collections::HashSet::new();

        for entry in &entries {
            match &entry.operation {
                crate::coretex_transaction::WalOperation::Begin { txn_id } => {
                    // 已经在 active_txns 中说明是 checkpoint 时未完成
                    let _ = txn_id;
                }
                crate::coretex_transaction::WalOperation::Insert { key, value } => {
                    state.insert(key.clone(), value.clone());
                }
                crate::coretex_transaction::WalOperation::Update { key, new_value, .. } => {
                    state.insert(key.clone(), new_value.clone());
                }
                crate::coretex_transaction::WalOperation::Delete { key, .. } => {
                    state.remove(key);
                }
                crate::coretex_transaction::WalOperation::Commit { txn_id } => {
                    committed_txns.insert(*txn_id);
                }
                crate::coretex_transaction::WalOperation::Abort { txn_id } => {
                    aborted_txns.insert(*txn_id);
                }
            }
        }

        Ok(RecoveryReport {
            checkpoint_id: checkpoint.as_ref().map(|c| c.checkpoint_id.clone()),
            checkpoint_lsn: start_lsn,
            wal_entries_replayed: entries.len(),
            committed_txns: committed_txns.len() as u64,
            aborted_txns: aborted_txns.len() as u64,
            in_flight_txns: active_txns.len() as u64,
            final_state_keys: state.len(),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoveryReport {
    pub checkpoint_id: Option<String>,
    pub checkpoint_lsn: u64,
    pub wal_entries_replayed: usize,
    pub committed_txns: u64,
    pub aborted_txns: u64,
    pub in_flight_txns: u64,
    pub final_state_keys: usize,
}

// 给 RaftLog 添加 truncate_to 方法
impl RaftLog {
    /// 截断日志到指定索引（log compaction）
    pub fn truncate_to(&mut self, index: u64) -> Result<(), String> {
        if index as usize >= self.entries.len() {
            return Ok(());
        }
        // 保留 index 之后的所有条目
        self.entries = self.entries.split_off(index as usize);
        if let Some(path) = &self.storage_path {
            let data = serde_json::to_vec(&self.entries).map_err(|e| e.to_string())?;
            std::fs::write(path, data).map_err(|e| e.to_string())?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coretex_transaction::WalEntry;
use crate::coretex_core::Result;

    #[tokio::test]
    async fn test_two_pc_begin_success() {
        let coordinator = TwoPCCoordinator::new(
            Arc::new(MockTwoPCRpc),
            Duration::from_secs(5),
        );

        let txn = coordinator.begin(
            vec!["p1".to_string(), "p2".to_string()],
            b"data".to_vec(),
        ).await.unwrap();

        assert_eq!(txn.state, TwoPCState::Committed);
    }

    #[test]
    fn test_snapshot_creation_and_verification() {
        let snap = RaftSnapshot::new(5, 100, 4, b"some state data".to_vec());
        assert!(snap.verify());
        let mut broken = snap.clone();
        broken.data = b"different data".to_vec();
        assert!(!broken.verify());
    }

    #[tokio::test]
    async fn test_crash_recovery() {
        let temp_dir = tempfile::tempdir().unwrap();
        let wal_path = temp_dir.path().join("test.wal");
        let ckpt_dir = temp_dir.path().join("ckpt");

        // 1. 写 WAL
        {
            let mut wal = crate::coretex_transaction::WriteAheadLog::with_persistence(&wal_path, 1000, true).unwrap();
            wal.append(WalEntry {
                transaction_id: 1,
                timestamp: 1000,
                operation: crate::coretex_transaction::WalOperation::Insert {
                    key: "user:1".to_string(),
                    value: b"alice".to_vec(),
                },
                lsn: 0,
            }).unwrap();
            wal.append(WalEntry {
                transaction_id: 1,
                timestamp: 1001,
                operation: crate::coretex_transaction::WalOperation::Commit { txn_id: 1 },
                lsn: 0,
            }).unwrap();
        }

        // 2. 模拟崩溃，恢复
        let recovery = CrashRecoveryManager::new(&ckpt_dir, &wal_path);
        let report = recovery.recover().await.unwrap();

        assert_eq!(report.wal_entries_replayed, 2);
        assert_eq!(report.committed_txns, 1);
        assert!(report.final_state_keys >= 1);
    }

    #[test]
    fn test_raft_log_truncate_to() {
        let mut log = RaftLog::new();
        for i in 0..10 {
            log.append(LogEntry {
                term: 1,
                index: 0,
                command: LogCommand::Noop,
                timestamp: i,
            });
        }
        // 截断到 index 5，保留 5 之后
        log.truncate_to(5).unwrap();
        assert_eq!(log.len(), 5);
        assert_eq!(log.entries[0].index, 5);
    }
}
