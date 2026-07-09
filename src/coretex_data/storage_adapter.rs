//! 统一抽象层：将同步 `StorageEngine`（coretex_storage）和异步 `PersistenceManager`（coretex_persistence）
//! 封装为统一的 `UnifiedStorageAdapter`，让上层（DataManager、DataLakehouse）只看到一种 API。
//!
//! 设计目标：
//! 1. 消除双写风险：所有写操作经由统一的 `write_through` 方法，按一致性级别（WriteThrough / WriteBack / WriteAround）
//!    决定是否同步落盘
//! 2. 统一错误类型：`AdapterError` 包装同步/异步两边的错误
//! 3. 统一接口：upsert/get/delete/list_keys 四个核心操作，async 接口签名

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use serde::{Deserialize, Serialize};

use crate::coretex_storage::StorageEngine;
use crate::coretex_persistence::PersistenceManager;

/// 一致性级别
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConsistencyLevel {
    /// 同时写入同步存储（StorageEngine）和异步持久化层（PersistenceManager）
    WriteThrough,
    /// 只写同步存储，后台异步刷到持久化层
    WriteBack,
    /// 跳过同步存储，只写持久化层（适合大对象）
    WriteAround,
}

impl Default for ConsistencyLevel {
    fn default() -> Self {
        Self::WriteThrough
    }
}

/// 统一存储错误
#[derive(Debug, Clone)]
pub enum AdapterError {
    SyncStorageError(String),
    AsyncPersistenceError(String),
    SerializationError(String),
    NotFound(String),
    Timeout(String),
}

impl std::fmt::Display for AdapterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AdapterError::SyncStorageError(m) => write!(f, "sync storage error: {}", m),
            AdapterError::AsyncPersistenceError(m) => write!(f, "async persistence error: {}", m),
            AdapterError::SerializationError(m) => write!(f, "serialization error: {}", m),
            AdapterError::NotFound(k) => write!(f, "key not found: {}", k),
            AdapterError::Timeout(m) => write!(f, "operation timeout: {}", m),
        }
    }
}

impl std::error::Error for AdapterError {}

/// 写入统计
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct AdapterStats {
    pub sync_writes: u64,
    pub async_writes: u64,
    pub write_back_writes: u64,
    pub write_around_writes: u64,
    pub write_through_writes: u64,
    pub read_misses: u64,
    pub read_hits: u64,
    pub pending_writeback: u64,
    pub errors: u64,
    pub avg_write_latency_us: u64,
    pub avg_read_latency_us: u64,
}

/// WriteBack 后台任务的待刷数据条目
struct WriteBackEntry {
    key: String,
    vector: Vec<u8>,
    metadata: Vec<u8>,
    enqueued_at: Instant,
}

/// 统一存储适配器：封装同步 StorageEngine + 异步 PersistenceManager
pub struct UnifiedStorageAdapter {
    sync_storage: Arc<RwLock<Box<dyn StorageEngine>>>,
    async_persistence: Option<Arc<PersistenceManager>>,
    consistency: ConsistencyLevel,
    writeback_queue: Arc<RwLock<Vec<WriteBackEntry>>>,
    stats: Arc<RwLock<AdapterStats>>,
    writeback_interval: Duration,
    writeback_running: Arc<RwLock<bool>>,
}

impl UnifiedStorageAdapter {
    /// 创建新适配器。async_persistence 可选为 None（仅同步模式）
    pub fn new(
        sync_storage: Arc<RwLock<Box<dyn StorageEngine>>>,
        async_persistence: Option<Arc<PersistenceManager>>,
        consistency: ConsistencyLevel,
    ) -> Self {
        Self {
            sync_storage,
            async_persistence,
            consistency,
            writeback_queue: Arc::new(RwLock::new(Vec::new())),
            stats: Arc::new(RwLock::new(AdapterStats::default())),
            writeback_interval: Duration::from_millis(100),
            writeback_running: Arc::new(RwLock::new(false)),
        }
    }

    /// 启动后台 writeback 刷盘任务
    pub async fn start_writeback_worker(&self) {
        let mut running = self.writeback_running.write().await;
        if *running {
            return;
        }
        *running = true;
        drop(running);

        let queue = self.writeback_queue.clone();
        let persistence = self.async_persistence.clone();
        let interval = self.writeback_interval;
        let running = self.writeback_running.clone();
        let stats = self.stats.clone();

        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;

                let entries: Vec<WriteBackEntry> = {
                    let mut q = queue.write().await;
                    let drained: Vec<_> = q.drain(..).collect();
                    drained
                };

                if entries.is_empty() {
                    let r = running.read().await;
                    if !*r { break; }
                    continue;
                }

                if let Some(pm) = &persistence {
                    for entry in entries {
                        let start = Instant::now();
                        // 异步写入
                        let key = entry.key.clone();
                        let vec_data = entry.vector.clone();
                        let meta_data = entry.metadata.clone();
                        let result = pm.put(&key, &vec_data, &meta_data).await;
                        let elapsed = start.elapsed().as_micros() as u64;

                        let mut s = stats.write().await;
                        s.async_writes += 1;
                        s.avg_write_latency_us = update_avg(s.avg_write_latency_us, elapsed, s.async_writes as u64);
                        if result.is_err() {
                            s.errors += 1;
                        }
                    }
                }

                let r = running.read().await;
                if !*r { break; }
            }
        });
    }

    /// 停止后台任务
    pub async fn stop_writeback_worker(&self) {
        *self.writeback_running.write().await = false;
    }

    /// 统一写入接口
    pub async fn upsert(
        &self,
        key: &str,
        vector: &[u8],
        metadata: &[u8],
    ) -> Result<(), AdapterError> {
        let start = Instant::now();
        let mut stats = self.stats.write().await;

        match self.consistency {
            ConsistencyLevel::WriteThrough => {
                // 同步存储
                {
                    let storage = self.sync_storage.read().await;
                    storage
                        .store(key, vector, metadata)
                        .await
                        .map_err(|e| AdapterError::SyncStorageError(e.to_string()))?;
                }
                stats.write_through_writes += 1;
                stats.sync_writes += 1;

                // 异步持久化
                if let Some(pm) = &self.async_persistence {
                    pm.put(key, vector, metadata).await
                        .map_err(|e| AdapterError::AsyncPersistenceError(e.to_string()))?;
                    stats.async_writes += 1;
                }
            }
            ConsistencyLevel::WriteBack => {
                // 只写同步存储，排队等后台刷
                {
                    let storage = self.sync_storage.read().await;
                    storage
                        .store(key, vector, metadata)
                        .await
                        .map_err(|e| AdapterError::SyncStorageError(e.to_string()))?;
                }
                stats.sync_writes += 1;
                stats.write_back_writes += 1;
                drop(stats);

                let mut queue = self.writeback_queue.write().await;
                queue.push(WriteBackEntry {
                    key: key.to_string(),
                    vector: vector.to_vec(),
                    metadata: metadata.to_vec(),
                    enqueued_at: Instant::now(),
                });

                let mut s = self.stats.write().await;
                s.pending_writeback = queue.len() as u64;
            }
            ConsistencyLevel::WriteAround => {
                // 跳过同步存储，直接写异步持久化
                if let Some(pm) = &self.async_persistence {
                    pm.put(key, vector, metadata).await
                        .map_err(|e| AdapterError::AsyncPersistenceError(e.to_string()))?;
                } else {
                    return Err(AdapterError::AsyncPersistenceError(
                        "WriteAround requires async_persistence".to_string()
                    ));
                }
                stats.async_writes += 1;
                stats.write_around_writes += 1;
            }
        }

        let elapsed = start.elapsed().as_micros() as u64;
        stats.avg_write_latency_us = update_avg(stats.avg_write_latency_us, elapsed, stats.sync_writes + stats.async_writes);
        Ok(())
    }

    /// 统一读取：先查同步存储，再回退到异步持久化
    pub async fn get(&self, key: &str) -> Result<Option<(Vec<u8>, Vec<u8>)>, AdapterError> {
        let start = Instant::now();
        // 优先从同步存储读取
        let from_sync = {
            let storage = self.sync_storage.read().await;
            storage.retrieve(key).await
                .map_err(|e| AdapterError::SyncStorageError(e.to_string()))?
        };

        if let Some((vec_bytes, meta_bytes)) = from_sync {
            let mut stats = self.stats.write().await;
            stats.read_hits += 1;
            stats.avg_read_latency_us = update_avg(stats.avg_read_latency_us, start.elapsed().as_micros() as u64, stats.read_hits);
            return Ok(Some((vec_bytes, meta_bytes)));
        }

        // 回退到异步持久化
        if let Some(pm) = &self.async_persistence {
            let result = pm.get(key).await
                .map_err(|e| AdapterError::AsyncPersistenceError(e.to_string()))?;
            let mut stats = self.stats.write().await;
            if result.is_none() {
                stats.read_misses += 1;
            } else {
                stats.read_hits += 1;
            }
            stats.avg_read_latency_us = update_avg(stats.avg_read_latency_us, start.elapsed().as_micros() as u64, stats.read_hits + stats.read_misses);
            return Ok(result);
        }

        let mut stats = self.stats.write().await;
        stats.read_misses += 1;
        Ok(None)
    }

    /// 统一删除
    pub async fn delete(&self, key: &str) -> Result<bool, AdapterError> {
        let mut deleted = false;
        {
            let storage = self.sync_storage.read().await;
            if storage.delete(key).await.unwrap_or(false) {
                deleted = true;
            }
        }
        if let Some(pm) = &self.async_persistence {
            if pm.delete(key).await.unwrap_or(false) {
                deleted = true;
            }
        }
        Ok(deleted)
    }

    /// 列出所有键（合并两个存储）
    pub async fn list_keys(&self) -> Result<Vec<String>, AdapterError> {
        let mut all: Vec<String> = Vec::new();
        {
            let storage = self.sync_storage.read().await;
            if let Ok(keys) = storage.list().await {
                all.extend(keys);
            }
        }
        let mut unique: std::collections::HashSet<String> = all.into_iter().collect();
        if let Some(pm) = &self.async_persistence {
            if let Ok(persist_keys) = pm.list_keys().await {
                unique.extend(persist_keys);
            }
        }
        Ok(unique.into_iter().collect())
    }

    /// 强制刷盘（writeback 模式下调用）
    pub async fn flush(&self) -> Result<(), AdapterError> {
        if let Some(pm) = &self.async_persistence {
            pm.flush().await
                .map_err(|e| AdapterError::AsyncPersistenceError(e.to_string()))?;
        }
        Ok(())
    }

    /// 获取统计
    pub async fn stats(&self) -> AdapterStats {
        self.stats.read().await.clone()
    }
}

fn update_avg(current: u64, new_sample: u64, total: u64) -> u64 {
    if total == 0 { return new_sample; }
    ((current * (total - 1)) + new_sample) / total
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coretex_storage::MemoryStorage;
use crate::coretex_core::Result;

    #[tokio::test]
    async fn test_unified_adapter_write_through() {
        let storage: Box<dyn StorageEngine> = Box::new(MemoryStorage::new());
        let adapter = UnifiedStorageAdapter::new(
            Arc::new(RwLock::new(storage)),
            None,
            ConsistencyLevel::WriteThrough,
        );

        adapter.upsert("key1", b"vec1", b"meta1").await.unwrap();
        let result = adapter.get("key1").await.unwrap();
        assert!(result.is_some());
    }

    #[tokio::test]
    async fn test_unified_adapter_write_around_requires_persistence() {
        let storage: Box<dyn StorageEngine> = Box::new(MemoryStorage::new());
        let adapter = UnifiedStorageAdapter::new(
            Arc::new(RwLock::new(storage)),
            None,
            ConsistencyLevel::WriteAround,
        );

        let result = adapter.upsert("key1", b"vec1", b"meta1").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_unified_adapter_delete() {
        let storage: Box<dyn StorageEngine> = Box::new(MemoryStorage::new());
        let adapter = UnifiedStorageAdapter::new(
            Arc::new(RwLock::new(storage)),
            None,
            ConsistencyLevel::WriteThrough,
        );

        adapter.upsert("k1", b"v1", b"m1").await.unwrap();
        assert!(adapter.delete("k1").await.unwrap());
        assert!(adapter.get("k1").await.unwrap().is_none());
    }
}
