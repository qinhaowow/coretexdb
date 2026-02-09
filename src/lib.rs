//! CortexDB - 企业级多模态向量数据库
//! 
//! # 项目架构
//!
//! 本项目采用分层架构设计，包含以下核心模块：
//!
//! ## 核心层 (Core)
//! - [`cortex_core`] - 核心类型、配置、模式定义
//! - [`cortex_storage`] - 存储引擎实现
//! - [`cortex_index`] - 索引管理
//! - [`cortex_query`] - 查询处理
//!
//! ## API 层 (API)
//! - [`cortex_api`] - REST/gRPC API
//! - [`api`] - GraphQL/PostgreSQL 协议
//!
//! ## 分布式层 (Distributed)
//! - [`cortex_distributed`] - 分布式协调
//! - [`distributed`] - 集群管理
//!
//! ## 工具层 (Utilities)
//! - [`cortex_security`] - 认证与安全
//! - [`cortex_monitor`] - 监控与日志
//! - [`cortex_backup`] - 备份与恢复
//!
//! ## 高级特性 (Advanced)
//! - [`ai`] - AI/ML 特性
//! - [`clustering`] - 聚类算法
//! - [`compute`] - GPU/SIMD 计算
//!
//! ## 实验特性 (Experimental)
//! - 实验功能位于 [`experiments/`] 目录
//!
//! # 示例
//!
//! ```ignore
//! use cortexdb::CortexDB;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let db = CortexDB::new().await?;
//!     Ok(())
//! }
//! ```
//!

#![doc(html_root_url = "https://docs.rs/cortexdb/0.1.0")]

pub mod cortex_core;
pub mod cortex_storage;
pub mod cortex_index;
pub mod cortex_query;
pub mod cortex_api;
pub mod cortex_backup;
pub mod cortex_cli;
pub mod cortex_distributed;
pub mod cortex_monitor;
pub mod cortex_security;
pub mod cortex_perf;
pub mod cortex_utils;

pub mod ai;
pub mod api;
pub mod compute;
pub mod core;
pub mod distributed;
pub mod index;
pub mod query;
pub mod storage;
pub mod transaction;

pub mod scheduler;
pub mod worker;
pub mod clustering;
pub mod sharding;
pub mod discovery;
pub mod coordinator;

#[cfg(feature = "python")]
pub mod python;

#[cfg(feature = "grpc")]
pub mod grpc;

#[cfg(feature = "websocket")]
pub mod websocket;

#[cfg(feature = "distributed")]
pub mod raft;

#[cfg(feature = "postgres")]
pub mod postgres;

pub use cortex_core::{Vector, Document, CollectionSchema, DistanceMetric, IndexConfig, IndexType, CortexError, Result};
pub use cortex_storage::{StorageEngine, MemoryStorage, PersistentStorage};
pub use cortex_index::{VectorIndex, BruteForceIndex, IndexManager, SearchResult};
pub use cortex_query::{QueryType, QueryParams, QueryResult, QueryProcessor, QueryPlanner};
pub use cortex_api::rest::{start_server, ApiConfig};

#[cfg(feature = "grpc")]
pub use cortex_api::grpc::start_server as start_grpc_server;

#[cfg(feature = "distributed")]
pub use cortex_distributed::{ClusterManager, QueryCoordinator, ShardingStrategy, MetadataManager};

#[cfg(feature = "distributed")]
pub use cortex_monitor::{MetricsCollector, HealthChecker, MonitoringDashboard};

#[cfg(feature = "distributed")]
pub use cortex_backup::{BackupManager, RestoreManager, ReplicationManager};

pub use cortex_security::{AuthManager, TokenManager, PermissionManager};
pub use cortex_perf::{QueryCache, QueryRouter, ParallelQueryExecutor, QueryOptimizer};
pub use cortex_cli::run_cli;

pub use scheduler::{Scheduler, SchedulerConfig, SchedulerStats, PriorityQueue, SchedulingPolicy};
pub use worker::{TaskExecutor, ExecutorConfig, HeartbeatManager, WorkerRegistration};
pub use clustering::{StreamingKMeans, BIRCH, ANNIndex, AnnoyIndex, HNSWIndex, LSHIndex};
pub use sharding::{ConsistentHash, HashRing, ShardBalancer, LoadBalancer};
pub use discovery::{EtcdClient, ConsulClient, ServiceRegistration};
pub use coordinator::{LeaderElection, ElectionConfig, ElectionState};

pub mod prelude {
    pub use crate::cortex_core::*;
    pub use crate::cortex_storage::*;
    pub use crate::cortex_index::*;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_basic_structure() {
        let db = CortexDB::new().await.unwrap();
        assert!(db.storage.is_ready().await);
    }

    #[test]
    fn test_vector_creation() {
        let vector = Vector::new(vec![1.0, 2.0, 3.0]);
        assert_eq!(vector.dimension(), 3);
    }
}

pub struct CortexDB {
    storage: Box<dyn StorageEngine>,
    index_manager: IndexManager,
    config: CortexConfig,
}

impl CortexDB {
    pub async fn new() -> Result<Self, CortexError> {
        let config = CortexConfig::default();
        let storage = Box::new(MemoryStorage::new());
        storage.init().await?;
        
        let index_manager = IndexManager::new();
        
        Ok(Self {
            storage,
            index_manager,
            config,
        })
    }

    pub async fn with_config(config: CortexConfig) -> Result<Self, CortexError> {
        let storage: Box<dyn StorageEngine> = match config.storage.engine.as_str() {
            "memory" => Box::new(MemoryStorage::new()),
            "persistent" => Box::new(PersistentStorage::new(&config.storage.directory)),
            _ => Box::new(MemoryStorage::new()),
        };
        storage.init().await?;
        
        let index_manager = IndexManager::new();
        
        Ok(Self {
            storage,
            index_manager,
            config,
        })
    }

    pub async fn store(&self, id: &str, vector: &[f32], metadata: &serde_json::Value) -> Result<(), CortexError> {
        self.storage.store(id, vector, metadata).await
    }

    pub async fn retrieve(&self, id: &str) -> Result<Option<(Vec<f32>, serde_json::Value)>, CortexError> {
        self.storage.retrieve(id).await
    }

    pub async fn delete(&self, id: &str) -> Result<bool, CortexError> {
        self.storage.delete(id).await
    }

    pub async fn search(&self, query: &[f32], k: usize) -> Result<Vec<SearchResult>, CortexError> {
        self.index_manager.search(query, k).await
    }

    pub fn index_manager(&self) -> &IndexManager {
        &self.index_manager
    }

    pub fn config(&self) -> &CortexConfig {
        &self.config
    }
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct CortexConfig {
    pub storage: StorageConfig,
    pub api: ApiConfig,
    pub distributed: DistributedConfig,
    pub security: SecurityConfig,
    pub monitoring: MonitoringConfig,
}

impl Default for CortexConfig {
    fn default() -> Self {
        Self {
            storage: StorageConfig::default(),
            api: ApiConfig::default(),
            distributed: DistributedConfig::default(),
            security: SecurityConfig::default(),
            monitoring: MonitoringConfig::default(),
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct StorageConfig {
    pub engine: String,
    pub directory: String,
    pub memory_limit: usize,
    pub enable_compression: bool,
    pub compression_algorithm: String,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            engine: "memory".to_string(),
            directory: "./data".to_string(),
            memory_limit: 1024 * 1024 * 1024,
            enable_compression: true,
            compression_algorithm: "lz4".to_string(),
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct ApiConfig {
    pub host: String,
    pub port: u16,
    pub workers: usize,
    pub tls_enabled: bool,
    pub cors_enabled: bool,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            port: 8080,
            workers: 4,
            tls_enabled: false,
            cors_enabled: true,
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct DistributedConfig {
    pub enabled: bool,
    pub cluster_name: String,
    pub node_id: String,
    pub seed_nodes: Vec<String>,
    pub heartbeat_interval: Duration,
    pub gossip_interval: Duration,
}

impl Default for DistributedConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            cluster_name: "cortexdb".to_string(),
            node_id: uuid::Uuid::new_v4().to_string(),
            seed_nodes: Vec::new(),
            heartbeat_interval: Duration::from_secs(5),
            gossip_interval: Duration::from_secs(30),
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct SecurityConfig {
    pub authentication_enabled: bool,
    pub authorization_enabled: bool,
    pub jwt_secret: String,
    pub jwt_expiry: Duration,
    pub encryption_key: String,
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            authentication_enabled: false,
            authorization_enabled: false,
            jwt_secret: "secret".to_string(),
            jwt_expiry: Duration::from_hours(24),
            encryption_key: "key".to_string(),
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct MonitoringConfig {
    pub metrics_enabled: bool,
    pub tracing_enabled: bool,
    pub log_level: String,
    pub metrics_port: u16,
    pub health_check_interval: Duration,
}

impl Default for MonitoringConfig {
    fn default() -> Self {
        Self {
            metrics_enabled: true,
            tracing_enabled: true,
            log_level: "info".to_string(),
            metrics_port: 9090,
            health_check_interval: Duration::from_secs(10),
        }
    }
}

#[cfg(feature = "python")]
pub mod python {
    use pyo3::prelude::*;
    use crate::CortexDB;
    use std::sync::Arc;
    use tokio::runtime::Runtime;

    #[pyclass]
    pub struct PyCortexDB {
        db: Arc<CortexDB>,
        rt: Runtime,
    }

    #[pymethods]
    impl PyCortexDB {
        #[new]
        fn new() -> PyResult<Self> {
            let rt = Runtime::new().map_err(|e| PyErr::from(std::io::Error::new(std::io::ErrorKind::Other, e)))?;
            let db = rt.block_on(async { CortexDB::new().await.map_err(|e| PyErr::from(std::io::Error::new(std::io::ErrorKind::Other, e)))? });
            Ok(Self {
                db: Arc::new(db),
                rt,
            })
        }

        fn store(&self, id: &str, vector: Vec<f32>, metadata: String) -> PyResult<()> {
            let metadata: serde_json::Value = serde_json::from_str(&metadata)
                .map_err(|e| PyErr::from(std::io::Error::new(std::io::ErrorKind::Other, e)))?;
            self.rt.block_on(async {
                self.db.store(id, &vector, &metadata).await
                    .map_err(|e| PyErr::from(std::io::Error::new(std::io::ErrorKind::Other, e)))
            })
        }

        fn search(&self, query: Vec<f32>, k: usize) -> PyResult<Vec<String>> {
            self.rt.block_on(async {
                self.db.search(&query, k).await
                    .map_err(|e| PyErr::from(std::io::Error::new(std::io::ErrorKind::Other, e)))
            }).map(|r| r.into_iter().map(|s| format!("{:?}", s)).collect())
        }
    }
}
