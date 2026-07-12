//! Transaction and Version Control for CortexDB
//! Implements MVCC and WAL for ACID transactions and time-travel queries

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;

pub struct TransactionManager {
    pub active_transactions: Arc<RwLock<HashMap<TransactionId, Transaction>>>,
    pub wal: Arc<RwLock<WriteAheadLog>>,
    pub snapshot_manager: Arc<RwLock<SnapshotManager>>,
    current_txn_id: Arc<RwLock<TransactionId>>,
}

pub type TransactionId = u64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransactionState {
    Active,
    Committed,
    Aborted,
}

#[derive(Debug, Clone)]
pub struct Transaction {
    pub id: TransactionId,
    pub state: TransactionState,
    pub start_timestamp: u64,
    pub write_set: Vec<WriteOperation>,
    pub read_set: Vec<ReadOperation>,
    pub isolation_level: IsolationLevel,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    pub entries: Vec<WalEntry>,
    max_entries: usize,
    log_path: Option<PathBuf>,
    fsync_on_commit: bool,
    next_lsn: u64,
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
    /// 公开方法：直接向 WAL 追加条目（供 DataManager 的事务感知写入使用）
    pub async fn append_wal(&self, entry: WalEntry) -> Result<u64, TransactionError> {
        let mut wal = self.wal.write().await;
        wal.append(entry)
    }

    /// 获取当前活跃事务数
    pub async fn active_count(&self) -> usize {
        self.active_transactions.read().await.len()
    }
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
            let lsn = wal.entries.len() as u64;
            wal.append(WalEntry {
                transaction_id: txn_id,
                timestamp,
                operation: WalOperation::Begin { txn_id },
                lsn,
            })?;
        }

        Ok(txn_id)
    }

    pub async fn commit(&self, txn_id: TransactionId) -> Result<(), TransactionError> {
        // 提交前验证隔离级别约束（Serializable 检查读-写冲突）
        self.validate_commit(txn_id).await?;

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
            let lsn = wal.entries.len() as u64;
            wal.append(WalEntry {
                transaction_id: txn_id,
                timestamp,
                operation: WalOperation::Commit { txn_id },
                lsn,
            })?;
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
            let lsn = wal.entries.len() as u64;
            wal.append(WalEntry {
                transaction_id: txn_id,
                timestamp,
                operation: WalOperation::Abort { txn_id },
                lsn,
            })?;
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

        // 从 WAL 构建快照数据：收集所有已提交的 key 及其在快照时间点的值
        let snapshot_data = {
            let wal = self.wal.read().await;
            wal.build_snapshot_data(timestamp)
        };

        let snapshot = Snapshot {
            id: 0,
            timestamp,
            transaction_id: txn_id,
            data: snapshot_data,
        };

        let mut manager = self.snapshot_manager.write().await;
        let snapshot_id = manager.create_snapshot(snapshot);

        Ok(snapshot_id)
    }

    /// 创建带显式数据的快照（供 DataManager 在创建快照时传入当前数据）
    pub async fn create_snapshot_with_data(
        &self,
        txn_id: TransactionId,
        data: HashMap<String, Vec<u8>>,
    ) -> Result<SnapshotId, TransactionError> {
        let active = self.active_transactions.read().await;

        let _transaction = active.get(&txn_id)
            .ok_or(TransactionError::TransactionNotFound(txn_id))?;

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let snapshot = Snapshot {
            id: 0,
            timestamp,
            transaction_id: txn_id,
            data,
        };

        let mut manager = self.snapshot_manager.write().await;
        let snapshot_id = manager.create_snapshot(snapshot);

        Ok(snapshot_id)
    }

    /// 记录事务的读操作（用于隔离级别冲突检测）
    pub async fn record_read(&self, txn_id: TransactionId, key: &str) -> Result<(), TransactionError> {
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

        transaction.read_set.push(ReadOperation {
            key: key.to_string(),
            timestamp,
        });

        Ok(())
    }

    /// 检查写冲突：根据隔离级别判断是否允许写入
    pub async fn check_write_conflict(&self, txn_id: TransactionId, key: &str) -> Result<(), TransactionError> {
        let active = self.active_transactions.read().await;
        let transaction = active.get(&txn_id)
            .ok_or(TransactionError::TransactionNotFound(txn_id))?;

        if transaction.state != TransactionState::Active {
            return Err(TransactionError::InvalidTransactionState(
                "Transaction is not active".to_string()
            ));
        }

        let my_start = transaction.start_timestamp;
        let my_level = &transaction.isolation_level;

        // ReadUncommitted：不做任何检查
        if *my_level == IsolationLevel::ReadUncommitted {
            return Ok(());
        }

        // 检查其他活跃事务是否已修改同一个 key
        for (other_id, other_txn) in active.iter() {
            if *other_id == txn_id {
                continue;
            }
            if other_txn.state != TransactionState::Active {
                continue;
            }

            let other_writes_key = other_txn.write_set.iter().any(|op| match op {
                WriteOperation::Insert { key: k, .. } => k == key,
                WriteOperation::Update { key: k, .. } => k == key,
                WriteOperation::Delete { key: k, .. } => k == key,
            });

            if other_writes_key {
                // RepeatableRead / Serializable / Snapshot：检测写-写冲突
                if *my_level == IsolationLevel::RepeatableRead
                    || *my_level == IsolationLevel::Serializable
                    || *my_level == IsolationLevel::Snapshot
                {
                    return Err(TransactionError::WriteConflict(format!(
                        "Key '{}' is being modified by active transaction {}", key, other_id
                    )));
                }
            }
        }

        // ReadCommitted 及以上：检查是否有已提交事务在当前事务开始后修改了该 key
        if *my_level != IsolationLevel::ReadUncommitted {
            let wal = self.wal.read().await;
            for entry in &wal.entries {
                if entry.timestamp <= my_start {
                    continue;
                }
                // 只检查已提交事务的写入
                let is_committed = wal.entries.iter().any(|e| {
                    e.transaction_id == entry.transaction_id
                        && matches!(e.operation, WalOperation::Commit { .. })
                });
                if !is_committed {
                    continue;
                }

                let writes_key = match &entry.operation {
                    WalOperation::Insert { key: k, .. } => k == key,
                    WalOperation::Update { key: k, .. } => k == key,
                    WalOperation::Delete { key: k, .. } => k == key,
                    _ => false,
                };

                if writes_key {
                    return Err(TransactionError::WriteConflict(format!(
                        "Key '{}' was modified by committed transaction {} after this transaction started",
                        key, entry.transaction_id
                    )));
                }
            }
        }

        Ok(())
    }

    /// 在提交前验证隔离级别约束（Serializable 检查读-写冲突）
    pub async fn validate_commit(&self, txn_id: TransactionId) -> Result<(), TransactionError> {
        let active = self.active_transactions.read().await;
        let transaction = active.get(&txn_id)
            .ok_or(TransactionError::TransactionNotFound(txn_id))?;

        if transaction.state != TransactionState::Active {
            return Err(TransactionError::InvalidTransactionState(
                "Transaction is not active".to_string()
            ));
        }

        // Serializable：检查读集中的 key 是否被其他已提交事务修改（读-写冲突）
        if transaction.isolation_level == IsolationLevel::Serializable {
            let my_start = transaction.start_timestamp;
            let read_keys: Vec<String> = transaction.read_set.iter().map(|r| r.key.clone()).collect();
            let wal = self.wal.read().await;

            for read_key in &read_keys {
                for entry in &wal.entries {
                    if entry.timestamp <= my_start {
                        continue;
                    }
                    if entry.transaction_id == txn_id {
                        continue;
                    }

                    let is_committed = wal.entries.iter().any(|e| {
                        e.transaction_id == entry.transaction_id
                            && matches!(e.operation, WalOperation::Commit { .. })
                    });
                    if !is_committed {
                        continue;
                    }

                    let writes_key = match &entry.operation {
                        WalOperation::Insert { key: k, .. } => k == read_key,
                        WalOperation::Update { key: k, .. } => k == read_key,
                        WalOperation::Delete { key: k, .. } => k == read_key,
                        _ => false,
                    };

                    if writes_key {
                        return Err(TransactionError::WriteConflict(format!(
                            "Serializable conflict: key '{}' was read and later modified by transaction {}",
                            read_key, entry.transaction_id
                        )));
                    }
                }
            }
        }

        Ok(())
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

impl Default for WriteAheadLog {
    fn default() -> Self {
        Self::new(10000)
    }
}

impl WriteAheadLog {
    /// 创建仅内存的 WAL（无持久化）
    pub fn new(max_entries: usize) -> Self {
        Self {
            entries: Vec::new(),
            max_entries,
            log_path: None,
            fsync_on_commit: false,
            next_lsn: 0,
        }
    }

    /// 创建持久化到磁盘的 WAL（启动时自动从磁盘恢复）
    pub fn with_persistence(log_path: impl AsRef<Path>, max_entries: usize, fsync_on_commit: bool) -> std::io::Result<Self> {
        let path = log_path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        if !path.exists() {
            File::create(&path)?;
        }

        let mut wal = Self {
            entries: Vec::new(),
            max_entries,
            log_path: Some(path.clone()),
            fsync_on_commit,
            next_lsn: 0,
        };

        wal.replay_from_disk().map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::Other, format!("replay failed: {}", e))
        })?;

        Ok(wal)
    }

    /// 追加 WAL 条目，立即持久化到磁盘（如果启用了持久化）
    pub fn append(&mut self, mut entry: WalEntry) -> Result<u64, TransactionError> {
        entry.lsn = self.next_lsn;
        self.next_lsn += 1;

        if self.entries.len() >= self.max_entries {
            self.entries.remove(0);
        }
        self.entries.push(entry.clone());

        if self.log_path.is_some() {
            self.write_entry_to_disk(&entry)?;
            if self.fsync_on_commit {
                self.fsync()?;
            }
        }

        Ok(entry.lsn)
    }

    fn write_entry_to_disk(&self, entry: &WalEntry) -> Result<(), TransactionError> {
        let path = match &self.log_path {
            Some(p) => p,
            None => return Ok(()),
        };

        let serialized = serialize_wal_entry(entry);
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|e| TransactionError::IoError(e.to_string()))?;

        let mut writer = BufWriter::new(file);
        let len = serialized.len() as u32;
        let crc = crc32_fast(&serialized);
        writer.write_all(&len.to_le_bytes())
            .and_then(|_| writer.write_all(&crc.to_le_bytes()))
            .and_then(|_| writer.write_all(&serialized))
            .map_err(|e| TransactionError::IoError(e.to_string()))?;
        writer.flush().map_err(|e| TransactionError::IoError(e.to_string()))?;
        Ok(())
    }

    fn fsync(&self) -> Result<(), TransactionError> {
        let path = match &self.log_path {
            Some(p) => p,
            None => return Ok(()),
        };
        let file = OpenOptions::new()
            .append(true)
            .open(path)
            .map_err(|e| TransactionError::IoError(e.to_string()))?;
        file.sync_all().map_err(|e| TransactionError::IoError(e.to_string()))?;
        Ok(())
    }

    /// 从磁盘重放 WAL（崩溃恢复入口）
    pub fn replay_from_disk(&mut self) -> Result<(), TransactionError> {
        let path = match &self.log_path.clone() {
            Some(p) => p.clone(),
            None => return Ok(()),
        };

        if !path.exists() {
            return Ok(());
        }

        self.entries.clear();

        let file = File::open(&path).map_err(|e| TransactionError::IoError(e.to_string()))?;
        let mut reader = BufReader::new(file);
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).map_err(|e| TransactionError::IoError(e.to_string()))?;

        let mut offset = 0usize;
        while offset + 8 <= buf.len() {
            let len = u32::from_le_bytes([buf[offset], buf[offset + 1], buf[offset + 2], buf[offset + 3]]) as usize;
            let crc_stored = u32::from_le_bytes([buf[offset + 4], buf[offset + 5], buf[offset + 6], buf[offset + 7]]);
            offset += 8;

            if offset + len > buf.len() {
                break;
            }

            let payload = &buf[offset..offset + len];
            let crc_computed = crc32_fast(payload);
            if crc_computed != crc_stored {
                break;
            }

            match deserialize_wal_entry(payload) {
                Ok(entry) => {
                    if entry.lsn >= self.next_lsn {
                        self.next_lsn = entry.lsn + 1;
                    }
                    self.entries.push(entry);
                }
                Err(_) => break,
            }
            offset += len;
        }

        Ok(())
    }

    /// 截断 WAL（checkpoint 后调用）
    pub fn truncate(&mut self) -> Result<(), TransactionError> {
        self.entries.clear();
        if let Some(path) = &self.log_path {
            File::create(path).map_err(|e| TransactionError::IoError(e.to_string()))?;
        }
        Ok(())
    }

    /// 强制将当前 WAL 刷到磁盘
    pub fn checkpoint(&mut self) -> Result<(), TransactionError> {
        if self.log_path.is_some() {
            self.fsync()?;
        }
        Ok(())
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

    /// 收集 WAL 中所有出现过的 key（来自 Insert/Update/Delete 操作）
    pub fn get_all_keys(&self) -> std::collections::HashSet<String> {
        let mut keys = std::collections::HashSet::new();
        for entry in &self.entries {
            match &entry.operation {
                WalOperation::Insert { key, .. } => { keys.insert(key.clone()); },
                WalOperation::Update { key, .. } => { keys.insert(key.clone()); },
                WalOperation::Delete { key, .. } => { keys.insert(key.clone()); },
                _ => {},
            }
        }
        keys
    }

    /// 从 WAL 构建快照数据：收集所有已提交事务写入的 key 在指定时间点的值
    pub fn build_snapshot_data(&self, timestamp: u64) -> HashMap<String, Vec<u8>> {
        // 找出所有已提交事务的 ID
        let committed_txns: std::collections::HashSet<TransactionId> = self.entries.iter()
            .filter_map(|e| {
                if let WalOperation::Commit { txn_id } = e.operation {
                    Some(txn_id)
                } else {
                    None
                }
            })
            .collect();

        let mut data = HashMap::new();
        for key in self.get_all_keys() {
            // 只从已提交事务的 WAL 条目中重建 key 的值
            let mut value: Option<Vec<u8>> = None;
            for entry in &self.entries {
                if entry.timestamp > timestamp {
                    break;
                }
                if !committed_txns.contains(&entry.transaction_id) {
                    continue;
                }

                match &entry.operation {
                    WalOperation::Insert { key: k, value: v } if k == &key => {
                        value = Some(v.clone());
                    },
                    WalOperation::Update { key: k, new_value: v, .. } if k == &key => {
                        value = Some(v.clone());
                    },
                    WalOperation::Delete { key: k, .. } if k == &key => {
                        value = None;
                    },
                    _ => {},
                }
            }
            if let Some(v) = value {
                data.insert(key, v);
            }
        }
        data
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

// =============== WAL 持久化辅助函数 ===============

/// CRC32 (IEEE 多项式) - 用于 WAL 帧校验
fn crc32_fast(data: &[u8]) -> u32 {
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

/// 序列化 WAL 条目为字节序列（自定义二进制格式）
fn serialize_wal_entry(entry: &WalEntry) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&entry.lsn.to_le_bytes());
    buf.extend_from_slice(&entry.transaction_id.to_le_bytes());
    buf.extend_from_slice(&entry.timestamp.to_le_bytes());
    match &entry.operation {
        WalOperation::Begin { txn_id } => {
            buf.push(0);
            buf.extend_from_slice(&txn_id.to_le_bytes());
        }
        WalOperation::Insert { key, value } => {
            buf.push(1);
            let kb = key.as_bytes();
            buf.extend_from_slice(&(kb.len() as u32).to_le_bytes());
            buf.extend_from_slice(kb);
            buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
            buf.extend_from_slice(value);
        }
        WalOperation::Update { key, old_value, new_value } => {
            buf.push(2);
            let kb = key.as_bytes();
            buf.extend_from_slice(&(kb.len() as u32).to_le_bytes());
            buf.extend_from_slice(kb);
            buf.extend_from_slice(&(old_value.len() as u32).to_le_bytes());
            buf.extend_from_slice(old_value);
            buf.extend_from_slice(&(new_value.len() as u32).to_le_bytes());
            buf.extend_from_slice(new_value);
        }
        WalOperation::Delete { key, value } => {
            buf.push(3);
            let kb = key.as_bytes();
            buf.extend_from_slice(&(kb.len() as u32).to_le_bytes());
            buf.extend_from_slice(kb);
            buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
            buf.extend_from_slice(value);
        }
        WalOperation::Commit { txn_id } => {
            buf.push(4);
            buf.extend_from_slice(&txn_id.to_le_bytes());
        }
        WalOperation::Abort { txn_id } => {
            buf.push(5);
            buf.extend_from_slice(&txn_id.to_le_bytes());
        }
    }
    buf
}

fn read_kv(data: &[u8], mut offset: usize) -> Result<(Vec<u8>, usize), String> {
    if offset + 4 > data.len() {
        return Err("klen out of range".to_string());
    }
    let klen = u32::from_le_bytes([data[offset], data[offset + 1], data[offset + 2], data[offset + 3]]) as usize;
    offset += 4;
    if offset + klen > data.len() {
        return Err("key bytes out of range".to_string());
    }
    let kb = data[offset..offset + klen].to_vec();
    offset += klen;
    Ok((kb, offset))
}

fn read_u64(data: &[u8], offset: usize) -> Result<u64, String> {
    if offset + 8 > data.len() {
        return Err("u64 out of range".to_string());
    }
    Ok(u64::from_le_bytes([
        data[offset], data[offset + 1], data[offset + 2], data[offset + 3],
        data[offset + 4], data[offset + 5], data[offset + 6], data[offset + 7],
    ]))
}

fn read_u32(data: &[u8], offset: usize) -> Result<u32, String> {
    if offset + 4 > data.len() {
        return Err("u32 out of range".to_string());
    }
    Ok(u32::from_le_bytes([data[offset], data[offset + 1], data[offset + 2], data[offset + 3]]))
}

fn read_bytes(data: &[u8], offset: usize) -> Result<(Vec<u8>, usize), String> {
    let len = read_u32(data, offset)? as usize;
    let start = offset + 4;
    if start + len > data.len() {
        return Err("bytes out of range".to_string());
    }
    Ok((data[start..start + len].to_vec(), start + len))
}

/// 反序列化 WAL 条目
fn deserialize_wal_entry(data: &[u8]) -> Result<WalEntry, String> {
    if data.len() < 25 {
        return Err("payload too short".to_string());
    }
    let lsn = read_u64(data, 0)?;
    let txn_id = read_u64(data, 8)?;
    let timestamp = read_u64(data, 16)?;
    let op_tag = data[24];
    let mut offset = 25;

    let operation = match op_tag {
        0 => {
            let txn_id = read_u64(data, offset)?;
            WalOperation::Begin { txn_id }
        }
        1 => {
            let (kb, off) = read_kv(data, offset)?;
            offset = off;
            let (value, _) = read_bytes(data, offset)?;
            WalOperation::Insert {
                key: String::from_utf8(kb).map_err(|e| e.to_string())?,
                value,
            }
        }
        2 => {
            let (kb, off) = read_kv(data, offset)?;
            offset = off;
            let (old_value, off) = read_bytes(data, offset)?;
            offset = off;
            let (new_value, _) = read_bytes(data, offset)?;
            WalOperation::Update {
                key: String::from_utf8(kb).map_err(|e| e.to_string())?,
                old_value,
                new_value,
            }
        }
        3 => {
            let (kb, off) = read_kv(data, offset)?;
            offset = off;
            let (value, _) = read_bytes(data, offset)?;
            WalOperation::Delete {
                key: String::from_utf8(kb).map_err(|e| e.to_string())?,
                value,
            }
        }
        4 => {
            let txn_id = read_u64(data, offset)?;
            WalOperation::Commit { txn_id }
        }
        5 => {
            let txn_id = read_u64(data, offset)?;
            WalOperation::Abort { txn_id }
        }
        _ => return Err(format!("unknown op tag {}", op_tag)),
    };

    Ok(WalEntry {
        transaction_id: txn_id,
        timestamp,
        operation,
        lsn,
    })
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
    IoError(String),
    SerializationError(String),
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
            TransactionError::IoError(msg) => {
                write!(f, "I/O error: {}", msg)
            },
            TransactionError::SerializationError(msg) => {
                write!(f, "Serialization error: {}", msg)
            },
        }
    }
}

impl std::error::Error for TransactionError {}

// ============== 锁管理与死锁检测 ==============

/// 锁模式
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LockMode {
    Shared,
    Exclusive,
}

/// 锁请求（用于等待图）
#[derive(Debug, Clone)]
pub struct LockRequest {
    pub txn_id: TransactionId,
    pub resource: String,
    pub mode: LockMode,
    pub requested_at: std::time::Instant,
}

/// 锁条目
#[derive(Debug, Clone)]
struct LockEntry {
    txn_id: TransactionId,
    mode: LockMode,
    granted_at: std::time::Instant,
}

/// 死锁信息
#[derive(Debug, Clone)]
pub struct DeadlockInfo {
    pub cycle: Vec<TransactionId>,
    pub victim: TransactionId,
    pub detection_time: std::time::Instant,
}

/// 锁等待图节点
#[derive(Debug, Clone)]
struct WaitsFor {
    waiting_txn: TransactionId,
    holding_txn: TransactionId,
    resource: String,
    waiting_since: std::time::Instant,
}

/// 锁管理器：支持超时和死锁检测
pub struct LockManager {
    /// 资源 -> 当前持有者列表
    locks: Arc<RwLock<HashMap<String, Vec<LockEntry>>>>,
    /// 等待图：resource -> 等待的事务
    waiters: Arc<RwLock<HashMap<String, Vec<LockRequest>>>>,
    /// 全局等待图（用于死锁检测）：waiting -> holding
    waits_for_graph: Arc<RwLock<HashMap<TransactionId, Vec<WaitsFor>>>>,
    /// 锁超时（默认 5 秒）
    default_timeout: std::time::Duration,
    /// 死锁检测周期
    detection_interval: std::time::Duration,
    /// 死锁事件回调
    deadlock_callbacks: Arc<RwLock<Vec<Box<dyn Fn(DeadlockInfo) + Send + Sync>>>>,
}

impl LockManager {
    pub fn new() -> Self {
        Self {
            locks: Arc::new(RwLock::new(HashMap::new())),
            waiters: Arc::new(RwLock::new(HashMap::new())),
            waits_for_graph: Arc::new(RwLock::new(HashMap::new())),
            default_timeout: std::time::Duration::from_secs(5),
            detection_interval: std::time::Duration::from_millis(500),
            deadlock_callbacks: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub fn with_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.default_timeout = timeout;
        self
    }

    /// 注册死锁回调
    pub async fn on_deadlock<F>(&self, f: F)
    where
        F: Fn(DeadlockInfo) + Send + Sync + 'static,
    {
        let mut callbacks = self.deadlock_callbacks.write().await;
        callbacks.push(Box::new(f));
    }

    /// 尝试立即获取锁
    pub async fn try_acquire(
        &self,
        txn_id: TransactionId,
        resource: &str,
        mode: LockMode,
    ) -> Result<(), TransactionError> {
        let mut locks = self.locks.write().await;
        let entries = locks.entry(resource.to_string()).or_default();

        // 检查兼容性
        if !entries.is_empty() {
            // 已有锁持有者
            if entries.iter().any(|e| e.mode == LockMode::Exclusive) {
                return Err(TransactionError::WriteConflict(format!(
                    "Resource '{}' is exclusively locked by another transaction",
                    resource
                )));
            }
            if mode == LockMode::Exclusive {
                return Err(TransactionError::WriteConflict(format!(
                    "Resource '{}' is shared-locked, cannot acquire exclusive",
                    resource
                )));
            }
        }

        entries.push(LockEntry {
            txn_id,
            mode,
            granted_at: std::time::Instant::now(),
        });
        Ok(())
    }

    /// 获取锁（带超时和死锁检测）
    pub async fn acquire(
        &self,
        txn_id: TransactionId,
        resource: &str,
        mode: LockMode,
    ) -> Result<(), TransactionError> {
        // 1. 先尝试立即获取
        if self.try_acquire(txn_id, resource, mode).await.is_ok() {
            return Ok(());
        }

        // 2. 加入等待队列
        {
            let mut waiters = self.waiters.write().await;
            waiters.entry(resource.to_string()).or_default().push(LockRequest {
                txn_id,
                resource: resource.to_string(),
                mode,
                requested_at: std::time::Instant::now(),
            });
        }

        // 3. 更新等待图
        self.update_waits_for_graph(txn_id, resource).await;

        // 4. 轮询等待（带超时）
        let deadline = std::time::Instant::now() + self.default_timeout;
        loop {
            // 检查死锁
            if let Some(deadlock) = self.detect_deadlock_for(txn_id).await {
                self.remove_from_waiters(txn_id, resource).await;
                self.remove_from_waits_for_graph(txn_id).await;
                self.notify_deadlock(deadlock.clone()).await;
                return Err(TransactionError::WriteConflict(format!(
                    "Deadlock detected involving transaction {}: cycle {:?}",
                    deadlock.victim, deadlock.cycle
                )));
            }

            // 再次尝试获取
            if self.try_acquire(txn_id, resource, mode).await.is_ok() {
                self.remove_from_waiters(txn_id, resource).await;
                self.remove_from_waits_for_graph(txn_id).await;
                return Ok(());
            }

            // 检查超时
            if std::time::Instant::now() >= deadline {
                self.remove_from_waiters(txn_id, resource).await;
                self.remove_from_waits_for_graph(txn_id).await;
                return Err(TransactionError::WriteConflict(format!(
                    "Lock acquisition timeout for resource '{}'",
                    resource
                )));
            }

            // 短暂休眠后重试
            tokio::time::sleep(self.detection_interval).await;
        }
    }

    /// 释放锁
    pub async fn release(&self, txn_id: TransactionId, resource: &str) {
        let mut locks = self.locks.write().await;
        if let Some(entries) = locks.get_mut(resource) {
            entries.retain(|e| e.txn_id != txn_id);
            if entries.is_empty() {
                locks.remove(resource);
            }
        }
    }

    /// 释放事务持有的所有锁
    pub async fn release_all(&self, txn_id: TransactionId) {
        let mut locks = self.locks.write().await;
        let mut empty_resources = Vec::new();
        for (resource, entries) in locks.iter_mut() {
            entries.retain(|e| e.txn_id != txn_id);
            if entries.is_empty() {
                empty_resources.push(resource.clone());
            }
        }
        for r in empty_resources {
            locks.remove(&r);
        }
    }

    /// 更新等待图：txn_id 等待 resource，对应持有者是当前所有持有该 resource 的事务
    async fn update_waits_for_graph(&self, txn_id: TransactionId, resource: &str) {
        let mut graph = self.waits_for_graph.write().await;
        let locks = self.locks.read().await;
        let entries = locks.get(resource).cloned().unwrap_or_default();
        let waiting_since = std::time::Instant::now();
        let mut edges = Vec::new();
        for entry in entries {
            if entry.txn_id != txn_id {
                edges.push(WaitsFor {
                    waiting_txn: txn_id,
                    holding_txn: entry.txn_id,
                    resource: resource.to_string(),
                    waiting_since,
                });
            }
        }
        graph.insert(txn_id, edges);
    }

    async fn remove_from_waiters(&self, txn_id: TransactionId, resource: &str) {
        let mut waiters = self.waiters.write().await;
        if let Some(queue) = waiters.get_mut(resource) {
            queue.retain(|w| w.txn_id != txn_id);
            if queue.is_empty() {
                waiters.remove(resource);
            }
        }
    }

    async fn remove_from_waits_for_graph(&self, txn_id: TransactionId) {
        let mut graph = self.waits_for_graph.write().await;
        graph.remove(&txn_id);
    }

    /// 检测指定事务是否在死锁环中（DFS 查找）
    async fn detect_deadlock_for(&self, start_txn: TransactionId) -> Option<DeadlockInfo> {
        let graph = self.waits_for_graph.read().await;
        let mut visited = std::collections::HashSet::new();
        let mut path = Vec::new();
        let mut path_set = std::collections::HashSet::new();

        if self.dfs_cycle(&graph, start_txn, &mut visited, &mut path, &mut path_set) {
            // 找到一个环，选择最年轻的事务作为受害者
            let victim = *path.iter().max().unwrap_or(&start_txn);
            Some(DeadlockInfo {
                cycle: path,
                victim,
                detection_time: std::time::Instant::now(),
            })
        } else {
            None
        }
    }

    fn dfs_cycle(
        &self,
        graph: &HashMap<TransactionId, Vec<WaitsFor>>,
        node: TransactionId,
        visited: &mut std::collections::HashSet<TransactionId>,
        path: &mut Vec<TransactionId>,
        path_set: &mut std::collections::HashSet<TransactionId>,
    ) -> bool {
        if path_set.contains(&node) {
            // 找到环
            if let Some(start_idx) = path.iter().position(|&x| x == node) {
                path.drain(..start_idx);
            }
            path.push(node);
            return true;
        }
        if visited.contains(&node) {
            return false;
        }

        path.push(node);
        path_set.insert(node);

        if let Some(edges) = graph.get(&node) {
            for edge in edges {
                if self.dfs_cycle(graph, edge.holding_txn, visited, path, path_set) {
                    return true;
                }
            }
        }

        path.pop();
        path_set.remove(&node);
        visited.insert(node);
        false
    }

    async fn notify_deadlock(&self, info: DeadlockInfo) {
        let callbacks = self.deadlock_callbacks.read().await;
        for cb in callbacks.iter() {
            cb(info.clone());
        }
    }

    /// 获取当前活跃锁数量
    pub async fn lock_count(&self) -> usize {
        self.locks.read().await.len()
    }

    /// 获取等待队列长度
    pub async fn waiter_count(&self) -> usize {
        self.waiters.read().await.values().map(|v| v.len()).sum()
    }

    /// 启动后台死锁检测任务
    pub async fn start_deadlock_detector(self: Arc<Self>) {
        let self_clone = self.clone();
        let interval = self.detection_interval;
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                // 扫描所有等待中的事务，检测死锁
                let waiters = self_clone.waiters.read().await;
                let mut all_waiting_txns: Vec<TransactionId> = Vec::new();
                for requests in waiters.values() {
                    for r in requests {
                        all_waiting_txns.push(r.txn_id);
                    }
                }
                drop(waiters);

                for txn in all_waiting_txns {
                    if let Some(info) = self_clone.detect_deadlock_for(txn).await {
                        eprintln!("[LockManager] Deadlock detected: cycle={:?}, victim={}",
                            info.cycle, info.victim);
                        self_clone.notify_deadlock(info).await;
                    }
                }
            }
        });
    }
}

impl Default for LockManager {
    fn default() -> Self {
        Self::new()
    }
}
