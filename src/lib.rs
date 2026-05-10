//! CoreTexDB - A multimodal vector database for AI applications 

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use serde::{Deserialize, Serialize};

pub const DB_VERSION: &str = env!("CARGO_PKG_VERSION");

pub mod coretex_core; 
pub mod coretex_storage; 
pub mod coretex_index; 
pub mod coretex_query; 
pub mod coretex_api; 
pub mod coretex_cli; 
pub mod coretex_utils; 
pub mod coretex_embedding; 
pub mod coretex_grpc;
pub mod coretex_gis;
pub mod coretex_timeseries;
pub mod coretex_export;
pub mod coretex_ann;
pub mod coretex_distributed;
pub mod coretex_auth;
pub mod coretex_monitoring;
pub mod coretex_sql;
pub mod coretex_compression;
pub mod coretex_security; 
#[cfg(feature = "python")]
pub mod coretex_python;
pub mod coretex_onnx;
pub mod coretex_bm25;
pub mod coretex_incremental;
pub mod coretex_cdc;
pub mod coretex_transaction;
pub mod coretex_edge;
pub mod coretex_simd;
pub mod coretex_websocket;
pub mod coretex_failover;
pub mod coretex_permissions;
pub mod coretex_tracing;
pub mod coretex_persistence;
pub mod coretex_backup;
pub mod coretex_monitoring_v2;
// pub mod coretex_tantivy;
pub mod coretex_graph;
pub mod coretex_hybrid;
pub mod coretex_rerank;
pub mod coretex_lakehouse;
pub mod coretex_document;
pub mod coretex_data;
pub mod coretex_sentinel;
pub mod coretex_grpo;

#[cfg(test)]
mod coretex_bm25_tests;
#[cfg(test)]
mod coretex_security_tests;
#[cfg(test)]
mod coretex_transaction_tests;
#[cfg(test)]
mod coretex_embedding_tests;

#[cfg(feature = "python")]
pub use coretex_python::{PyCortexDB, PySearchResult, PyCollectionInfo, PyCoreTexError};
pub use coretex_incremental::{IncrementalIndex, IndexUpdate};
pub use coretex_cdc::{CdcEngine, CdcEvent, CdcConfig};
pub use coretex_transaction::{TransactionManager, TransactionId, Snapshot, WriteAheadLog, IsolationLevel, TransactionError, WalEntry, WalOperation};
pub use coretex_edge::{EdgeDB, EdgeConfig, EdgeStats, EdgeSearchResult}; 

pub use coretex_core::{Vector, Document, CollectionSchema, IndexConfig, IndexType, CoreTexError, Result};
pub use coretex_storage::{StorageEngine, MemoryStorage};
#[cfg(feature = "rocksdb")]
pub use coretex_storage::PersistentStorage; 
pub use coretex_index::{VectorIndex, BruteForceIndex, IndexManager, SearchResult, HNSWIndex, IVFIndex, ScalarIndex}; 
pub use coretex_query::{QueryType, QueryParams, QueryResult as CoreTexQueryResult, DefaultQueryProcessor, QueryPlanner, QueryItem}; 
pub use coretex_bm25::{BM25Index, BM25Result, HybridQueryEngine, HybridSearchResult, MetadataFilter, FilterCondition}; 
pub use coretex_api::rest::{start_server, ApiConfig};
pub use coretex_api::graphql::{AppSchema, build_schema}; 
pub use coretex_cli::run_cli; 
pub use coretex_utils::{
    LockManager, Transaction, TransactionOperation, TransactionState,
    ClusterManager, ClusterNode, NodeRole, NodeState, Shard,
    BackupManager, MonitoringService, Metrics,
    cosine_similarity, euclidean_distance, normalize_vector, parse_vector, random_vector,
    LRUCache, TimedLRUCache, AsyncLRUCache, MultiLevelCache, CacheStats, MultiLevelCacheStats
}; 
pub use coretex_embedding::{
    TextEmbeddingService, ImageEmbeddingService, AudioEmbeddingService, 
    VideoEmbeddingService, PointCloudEmbeddingService, EmbeddingRouter,
    EmbeddingRequest, EmbeddingResponse, DataType, EmbeddingConfig,
    StreamingEmbedder, StreamItem, StreamResult, EmbeddingStream, StreamingStats,
    BatchedStreamEmbedder, WindowedStreamEmbedder, BackpressureStreamEmbedder, BackpressureSignal
}; 
pub use coretex_grpc::{CoretexService, start_grpc_server}; 
pub use coretex_gis::{GeoIndex, GeoPoint, GeoBoundingBox, GeoPolygon, GeoLineString, GeoQuery}; 
pub use coretex_timeseries::{TimeSeriesIndex, TimeSeries, TimeSeriesPoint, TimeSeriesStats, Aggregation, RollingWindow, ExponentialMovingAverage};
pub use coretex_export::{DataExporter, VectorExporter, BatchExporter, CollectionExporter, ExportResult, ExportFormat};
pub use coretex_ann::{ANNConfig, ANNAlgorithm, ANNParameters, HNSWParameters, IVFParameters, PQParameters, NSGParameters, SearchParameters, ANNTuner, IndexOptimizer, PerformanceRecord};
pub use coretex_distributed::{TwoPhaseCommit, DistributedTransaction, DistributedOperation, DistributedTransactionState, TransactionCoordinator, DistributedLockManager, DistributedLock, ParticipantState, ParticipantStatus};
pub use coretex_auth::{AuthService, User, Role, Permission, JWTConfig, TokenClaims, AuthToken, UserInfo, RateLimiter};
pub use coretex_monitoring::{PrometheusMetrics, DatabaseMetrics, AlertManager, AlertRule, AlertCondition, AlertSeverity, Alert, GrafanaConfig, GrafanaClient, SlowQueryConfig, SlowQueryEntry, SlowQueryLogger};
pub use coretex_sql::{SQLExecutor, SQLStatement, SQLSelect, SQLInsert, SQLDelete, SQLResult, SQLValue, SQLLexer, SQLParser};
pub use coretex_compression::{VectorCompressor, CompressedVector, CompressionAlgorithm, CompressionStats, RunLengthEncoding, DeltaCoding, QuantizationCompressor};
pub use coretex_security::{TlsConfig, TlsServer, TlsClient, EncryptionService, EncryptedData, EncryptionKey, KeyManager, AuditLogger, AuditEvent, AuditLevel, AuditAction, ACLEngine, ACLPolicy, Subject, SubjectType, Resource, ResourceType, Action, Effect, ACLValidator, VaultKMS, KMSConfig, KMSProvider, ExternalKey, KeyRotationManager, InputValidator, RateLimitValidator, NetworkIsolation, NetworkPolicy, IpRange, PolicyAction, IPRangeManager}; 
pub use coretex_simd::{simd_utils, SimdCapabilities};
pub use coretex_websocket::{WebSocketServer, WebSocketClient, WebSocketConfig, WebSocketMessage, WebSocketStats}; 
// pub use coretex_tantivy::{TantivySearcher, TantivyDocumentResult};
pub use coretex_graph::{GraphDatabase, GraphNode, GraphEdge, GraphPath, GraphError};
pub use coretex_hybrid::{
    MultiModalDocument, VectorData, TextData, ScalarValue, TimeSeriesData, GeoLocation,
    HybridQuery, VectorQuery, TextQuery, ScalarFilter, FilterOperator, QueryWeights, DistanceMetric,
    ScoreFusion, ScoreFusionEngine, MultiModalResult, FusedResult,
    HybridRetriever, VectorRetriever, TextRetriever,
};
pub use coretex_rerank::{
    CoarseRanker, CoarseRankerConfig, CoarseResult,
    FineRanker, FineRankerConfig, FineResult, RerankDocument, RerankModel, TwoStageSearchPipeline,
};
pub use coretex_lakehouse::{
    StorageTier, TierConfig, DocumentMeta,
    VectorLakehouse, MigrationReport, LakehouseStats,
    LRUTieringPolicy, TTLTieringPolicy, HybridTieringPolicy,
};
pub use coretex_document::{
    ParsedDocument, ImageData, TableData,
    DocumentParser, DocumentParserRegistry, PdfParser, ImageParser, AudioParser,
    HighDimVector, HighDimVectorStore, PQCompressor,
}; 
pub use coretex_data::{DataManager, VectorRecord, BulkResult};
pub use coretex_failover::{FailoverManager, FailoverConfig, FailoverEvent, NodeHealth, NodeStatus, ClusterStats, ConnectionPool, RaftRpc, HttpRaftRpc, VoteRequest, VoteResponse, HeartbeatRequest, HeartbeatResponse};

pub struct CoreTexDB {
    pub data_manager: DataManager,
    pub config: DbConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbConfig {
    pub data_dir: String,
    pub bin_dir: String,
    pub log_dir: String,
    pub wal_dir: String,
    pub backup_dir: String,
    pub include_dir: String,
    pub memory_only: bool,
    pub max_vectors_per_collection: usize,
    pub create_dirs_on_init: bool,
}

impl Default for DbConfig {
    fn default() -> Self {
        let base_dir = "./coretex_data".to_string();
        Self {
            data_dir: format!("{}/data", base_dir),
            bin_dir: format!("{}/bin", base_dir),
            log_dir: format!("{}/logs", base_dir),
            wal_dir: format!("{}/wal", base_dir),
            backup_dir: format!("{}/backup", base_dir),
            include_dir: format!("{}/include", base_dir),
            memory_only: false,
            max_vectors_per_collection: 1000000,
            create_dirs_on_init: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseMetadata {
    pub version: String,
    pub created_at: u64,
    pub last_modified: u64,
    pub collections: Vec<String>,
}

impl Default for DatabaseMetadata {
    fn default() -> Self {
        Self {
            version: DB_VERSION.to_string(),
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            last_modified: 0,
            collections: vec![],
        }
    }
}

impl DbConfig {
    pub fn new(base_dir: &str) -> Self {
        Self {
            data_dir: format!("{}/data", base_dir),
            bin_dir: format!("{}/bin", base_dir),
            log_dir: format!("{}/logs", base_dir),
            wal_dir: format!("{}/wal", base_dir),
            backup_dir: format!("{}/backup", base_dir),
            include_dir: format!("{}/include", base_dir),
            memory_only: false,
            max_vectors_per_collection: 1000000,
            create_dirs_on_init: true,
        }
    }
}

impl CoreTexDB {
    pub fn new() -> Self {
        let storage: Box<dyn StorageEngine> = Box::new(MemoryStorage::new());
        let storage = Arc::new(RwLock::new(storage));
        let index_manager = Arc::new(IndexManager::new());
        let data_manager = DataManager::new(storage, index_manager);
        
        Self {
            data_manager,
            config: DbConfig::default(),
        }
    }

    pub fn with_config(config: DbConfig) -> Self {
        let storage: Box<dyn StorageEngine> = if config.memory_only {
            Box::new(MemoryStorage::new())
        } else {
            #[cfg(feature = "rocksdb")]
            { Box::new(PersistentStorage::new(&config.data_dir)) }
            #[cfg(not(feature = "rocksdb"))]
            { panic!("memory_only=false requires the 'rocksdb' feature. Enable it in Cargo.toml or set memory_only=true") }
        };
        let storage = Arc::new(RwLock::new(storage));
        let index_manager = Arc::new(IndexManager::new());
        let data_manager = DataManager::new(storage, index_manager);
        
        Self {
            data_manager,
            config,
        }
    }

    pub async fn init(&self) -> Result<()> {
        if self.config.create_dirs_on_init && !self.config.memory_only {
            self.create_directories().await?;
        }
        
        if !self.config.memory_only {
            self.init_metadata().await?;
        }
        
        Ok(())
    }
    
    pub async fn create_directories(&self) -> Result<()> {
        let dirs = vec![
            &self.config.data_dir,
            &self.config.bin_dir,
            &self.config.log_dir,
            &self.config.wal_dir,
            &self.config.backup_dir,
            &self.config.include_dir,
        ];
        
        for dir in dirs {
            let path = PathBuf::from(dir);
            if !path.exists() {
                fs::create_dir_all(&path)
                    .map_err(|e| CoreTexError::Io(e))?;
            }
        }
        
        let collections_dir = PathBuf::from(&self.config.data_dir).join("collections");
        if !collections_dir.exists() {
            fs::create_dir_all(&collections_dir)
                .map_err(|e| CoreTexError::Io(e))?;
        }
        
        Ok(())
    }
    
    pub async fn init_metadata(&self) -> Result<()> {
        let metadata_path = PathBuf::from(&self.config.data_dir).join("metadata.json");
        
        if metadata_path.exists() {
            let content = fs::read_to_string(&metadata_path)
                .map_err(|e| CoreTexError::Io(e))?;
            
            let _metadata: DatabaseMetadata = serde_json::from_str(&content)
                .map_err(|e| CoreTexError::ValidationError(format!("Invalid metadata format: {}", e)))?;
        } else {
            let metadata = DatabaseMetadata::default();
            let content = serde_json::to_string_pretty(&metadata)
                .map_err(|e| CoreTexError::Serialization(e))?;
            fs::write(&metadata_path, content)
                .map_err(|e| CoreTexError::Io(e))?;
        }
        
        Ok(())
    }
    
    pub async fn load_metadata(&self) -> Result<DatabaseMetadata> {
        let metadata_path = PathBuf::from(&self.config.data_dir).join("metadata.json");
        
        if !metadata_path.exists() {
            return Ok(DatabaseMetadata::default());
        }
        
        let content = fs::read_to_string(&metadata_path)
            .map_err(|e| CoreTexError::Io(e))?;
        
        let metadata: DatabaseMetadata = serde_json::from_str(&content)
            .map_err(|e| CoreTexError::ValidationError(format!("Invalid metadata format: {}", e)))?;
        
        Ok(metadata)
    }
    
    pub async fn save_metadata(&self, metadata: &DatabaseMetadata) -> Result<()> {
        let metadata_path = PathBuf::from(&self.config.data_dir).join("metadata.json");
        let content = serde_json::to_string_pretty(metadata)
            .map_err(|e| CoreTexError::Serialization(e))?;
        fs::write(&metadata_path, content)
            .map_err(|e| CoreTexError::Io(e))?;
        Ok(())
    }

    pub async fn create_collection(&self, name: &str, dimension: usize, metric: &str) -> Result<()> {
        self.data_manager.create_collection(name, dimension, metric).await
    }

    pub async fn delete_collection(&self, name: &str) -> Result<()> {
        self.data_manager.delete_collection(name).await
    }

    pub async fn list_collections(&self) -> Result<Vec<String>> {
        self.data_manager.list_collections().await
    }

    pub async fn get_collection(&self, name: &str) -> Result<CollectionSchema> {
        self.data_manager.get_collection(name).await
    }

    pub async fn insert_vectors(&self, collection: &str, vectors: Vec<(String, Vec<f32>, serde_json::Value)>) -> Result<Vec<String>> {
        self.data_manager.insert_vectors(collection, vectors).await
    }

    pub async fn get_vector(&self, collection: &str, id: &str) -> Result<Option<(Vec<f32>, serde_json::Value)>> {
        self.data_manager.get_vector(collection, id).await.map(|opt| {
            opt.map(|r| (r.vector, r.metadata))
        })
    }

    pub async fn delete_vectors(&self, collection: &str, ids: &[String]) -> Result<usize> {
        self.data_manager.delete_vectors(collection, ids).await
    }

    pub async fn search(&self, collection: &str, query: Vec<f32>, k: usize, filter: Option<serde_json::Value>) -> Result<Vec<SearchResult>> {
        self.data_manager.search(collection, query, k, filter).await
    }

    pub async fn get_vectors_count(&self, collection: &str) -> Result<usize> {
        self.data_manager.get_vectors_count(collection).await
    }

    pub async fn update_vector(
        &self,
        collection: &str,
        id: &str,
        vector: Vec<f32>,
        metadata: Option<serde_json::Value>,
    ) -> Result<bool> {
        self.data_manager.update_vector(collection, id, vector, metadata).await
    }

    pub async fn upsert_vectors(
        &self,
        collection: &str,
        vectors: Vec<(String, Vec<f32>, serde_json::Value)>,
    ) -> Result<(Vec<String>, Vec<String>)> {
        self.data_manager.upsert_vectors(collection, vectors).await
    }

    pub async fn bulk_insert(
        &self,
        collection: &str,
        vectors: Vec<(String, Vec<f32>, serde_json::Value)>,
    ) -> Result<Vec<String>> {
        self.data_manager.bulk_insert(collection, vectors).await
    }

    pub async fn bulk_update(
        &self,
        collection: &str,
        vectors: Vec<(String, Vec<f32>, serde_json::Value)>,
    ) -> Result<Vec<String>> {
        self.data_manager.bulk_update(collection, vectors).await
    }

    pub async fn bulk_delete(
        &self,
        collection: &str,
        ids: Vec<String>,
    ) -> Result<Vec<String>> {
        self.data_manager.bulk_delete(collection, ids).await
    }

    pub async fn bulk_upsert(
        &self,
        collection: &str,
        vectors: Vec<(String, Vec<f32>, serde_json::Value)>,
    ) -> Result<BulkResult> {
        self.data_manager.bulk_upsert(collection, vectors).await
    }

    pub async fn get_vectors_by_ids(
        &self,
        collection: &str,
        ids: &[String],
    ) -> Result<Vec<(String, (Vec<f32>, serde_json::Value))>> {
        self.data_manager.get_vectors_by_ids(collection, ids).await
            .map(|vec| vec.into_iter().map(|(id, r)| (id, (r.vector, r.metadata))).collect())
    }
}

impl Default for CoreTexDB {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_and_list_collection() {
        let db = CoreTexDB::new();
        db.init().await.unwrap();

        db.create_collection("test", 128, "cosine").await.unwrap();

        let collections = db.list_collections().await.unwrap();
        assert!(collections.contains(&"test".to_string()));
    }

    #[tokio::test]
    async fn test_insert_and_search() {
        let db = CoreTexDB::new();
        db.init().await.unwrap();

        db.create_collection("test", 4, "cosine").await.unwrap();

        let vectors = vec![
            ("vec1".to_string(), vec![1.0, 0.0, 0.0, 0.0], serde_json::json!({"text": "hello"})),
            ("vec2".to_string(), vec![0.0, 1.0, 0.0, 0.0], serde_json::json!({"text": "world"})),
            ("vec3".to_string(), vec![0.9, 0.1, 0.0, 0.0], serde_json::json!({"text": "hi"})),
        ];

        db.insert_vectors("test", vectors).await.unwrap();

        let results = db.search("test", vec![1.0, 0.0, 0.0, 0.0], 2, None).await.unwrap();

        assert!(!results.is_empty());
        assert_eq!(results[0].id, "vec1");
    }

    #[tokio::test]
    async fn test_delete_collection() {
        let db = CoreTexDB::new();
        db.init().await.unwrap();

        db.create_collection("test", 128, "cosine").await.unwrap();
        db.delete_collection("test").await.unwrap();

        let collections = db.list_collections().await.unwrap();
        assert!(!collections.contains(&"test".to_string()));
    }

    #[tokio::test]
    async fn test_full_workflow() {
        let db = CoreTexDB::new();
        db.init().await.unwrap();

        db.create_collection("test_workflow", 4, "cosine").await.unwrap();

        let vectors = vec![
            ("v1".to_string(), vec![1.0, 0.0, 0.0, 0.0], serde_json::json!({"label": "a"})),
            ("v2".to_string(), vec![0.0, 1.0, 0.0, 0.0], serde_json::json!({"label": "b"})),
            ("v3".to_string(), vec![0.0, 0.0, 1.0, 0.0], serde_json::json!({"label": "c"})),
        ];

        db.insert_vectors("test_workflow", vectors).await.unwrap();

        let count = db.get_vectors_count("test_workflow").await.unwrap();
        assert_eq!(count, 3);

        let results = db.search("test_workflow", vec![1.0, 0.0, 0.0, 0.0], 2, None).await.unwrap();
        assert!(!results.is_empty());

        db.delete_collection("test_workflow").await.unwrap();

        let collections = db.list_collections().await.unwrap();
        assert!(!collections.contains(&"test_workflow".to_string()));
    }
}
