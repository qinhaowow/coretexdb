//! CoreTexDB - A multimodal vector database for AI applications 

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
pub mod coretex_spatial_transaction;
pub mod coretex_sql;
pub mod coretex_compression;
pub mod coretex_security; 
#[cfg(feature = "python")]
pub mod coretex_python;
#[cfg(feature = "onnx")]
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
#[cfg(feature = "tantivy")]
pub mod coretex_tantivy;
pub mod coretex_graph;
pub mod coretex_hybrid;
pub mod coretex_rerank;
pub mod coretex_lakehouse;
pub mod coretex_document;
pub mod coretex_data;
pub mod coretex_domain_index;
pub mod coretex_search_pipeline;
pub mod coretex_observability_extra;
pub mod coretex_ha_extra;
pub mod coretex_bio;
pub mod coretex_types_extra;
pub mod coretex_grpo;

#[cfg(test)]
mod coretex_bm25_tests;
#[cfg(test)]
mod coretex_security_tests;
#[cfg(test)]
mod coretex_transaction_tests;
#[cfg(test)]
mod coretex_embedding_tests;
#[cfg(test)]
mod tests_integration;
#[cfg(test)]
mod wal_integration_tests;

#[cfg(feature = "python")]
pub use coretex_python::{PyCortexDB, PyAsyncCortexDB, PySearchResult, PyCollectionInfo, PyHealth, PyCoreTexError};
pub use coretex_incremental::{IncrementalIndex, IndexUpdate};
pub use coretex_cdc::{CdcEngine, CdcEvent, CdcConfig, CdcSource, CdcError, PostgresCdcSource, MysqlCdcSource, MongodbCdcSource, VectorSyncHandler, VectorSyncEvent, SchemaChangeType};
pub use coretex_transaction::{TransactionManager, TransactionId, Snapshot, WriteAheadLog, IsolationLevel, TransactionError, WalEntry, WalOperation, LockManager, LockMode, LockRequest, DeadlockInfo};
pub use coretex_edge::{EdgeDB, EdgeConfig, EdgeStats, EdgeSearchResult}; 

pub use coretex_core::{Vector, Document, CollectionSchema, IndexConfig, IndexType, CoreTexError, Result};
pub use coretex_storage::{StorageEngine, MemoryStorage, FileStorage};
#[cfg(feature = "rocksdb")]
pub use coretex_storage::PersistentStorage; 
pub use coretex_index::{VectorIndex, BruteForceIndex, IndexManager, SearchResult, HNSWIndex, IVFIndex, ScalarIndex}; 
pub use coretex_query::{QueryType, QueryParams, QueryResult as CoreTexQueryResult, DefaultQueryProcessor, QueryPlanner, QueryItem};
pub use coretex_query::cost_model::{IndexSelector as QueryIndexSelector, CostInput, CostEstimate, IndexKind as QueryIndexKind, JoinType, JoinPlan, JoinPushdownOptimizer, OptimizationStats};
pub use coretex_bm25::{BM25Index, BM25Result, HybridQueryEngine, HybridSearchResult, MetadataFilter, FilterCondition}; 
pub use coretex_api::rest::{start_server, ApiConfig};
pub use coretex_api::graphql::{AppSchema, build_schema}; 
pub use coretex_cli::run_cli; 
pub use coretex_utils::{
    ClusterManager, ClusterNode, NodeRole, NodeState, Shard,
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
pub use coretex_grpc::{CoretexService, start_grpc_server, start_grpc_server_with_config, GrpcConfig, GrpcMetrics, AuthInterceptor, RateLimitInterceptor, MetricsInterceptor, ComposedInterceptor};
pub mod grpc_client {
    pub use crate::coretex_grpc::server::client::{connect, AuthApply};
}
pub use coretex_gis::{GeoIndex, GeoPoint, GeoBoundingBox, GeoPolygon, GeoLineString, GeoQuery, GeoPoint3D, GeoLineString3D, GeoPolygon3D, GeoBoundingBox3D}; 
pub use coretex_timeseries::{TimeSeriesIndex, TimeSeries, TimeSeriesPoint, TimeSeriesStats, Aggregation, RollingWindow, ExponentialMovingAverage};
pub use coretex_export::{DataExporter, VectorExporter, BatchExporter, CollectionExporter, ExportResult, ExportFormat};
pub use coretex_ann::{ANNConfig, ANNAlgorithm, ANNParameters, HNSWParameters, IVFParameters, PQParameters, NSGParameters, SearchParameters, ANNTuner, IndexOptimizer, PerformanceRecord};
pub use coretex_distributed::{TwoPhaseCommit, DistributedTransaction, DistributedOperation, DistributedTransactionState, TransactionCoordinator, DistributedLockManager, DistributedLock, ParticipantState, ParticipantStatus, ParticipantRpc, LocalParticipantRpc, LockPeerRpc, LocalLockPeerRpc};
pub use coretex_auth::{AuthService, User, Role, Permission, JWTConfig, TokenClaims, AuthToken, UserInfo, RateLimiter};
pub use coretex_monitoring::{PrometheusMetrics, DatabaseMetrics, AlertManager, AlertRule, AlertCondition, AlertSeverity, Alert, GrafanaConfig, GrafanaClient, SlowQueryConfig, SlowQueryEntry, SlowQueryLogger};
pub use coretex_spatial_transaction::{RTreeIndex, RTreeEntry, RTreeNode, MBR, SplitStrategy, SpatialTransaction, SpatialTxState, SpatialOperation, TlsSpatialCoordinator, TlsChannel, TlsHandshakeResult};
pub use coretex_sql::{SQLExecutor, SQLStatement, SQLSelect, SQLInsert, SQLDelete, SQLResult, SQLValue, SQLLexer, SQLParser, SQLCreateIndex, SQLCondition, SQLToken, SQLUpdate, SelectColumn, AggregateFunction, VectorSearch};
pub use coretex_sql::optimizer::{SQLOptimizer, ExecutionPlan, SQLOperator, SQLOperatorKind, IndexKind, VectorPushdownOperator, FilterOperator, FilterOp, FilterValue, ProjectionOperator, LimitOperator, DistanceOp};
pub use coretex_compression::{VectorCompressor, CompressedVector, CompressionAlgorithm, CompressionStats, RunLengthEncoding, DeltaCoding, QuantizationCompressor};
pub use coretex_security::{TlsConfig, TlsServer, TlsClient, EncryptionService, EncryptedData, EncryptionKey, KeyManager, AuditLogger, AuditEvent, AuditLevel, AuditAction, ACLEngine, ACLPolicy, Subject, SubjectType, Resource, ResourceType, Action, Effect, ACLValidator, VaultKMS, KMSConfig, KMSProvider, ExternalKey, KeyRotationManager, InputValidator, RateLimitValidator, NetworkIsolation, NetworkPolicy, IpRange, PolicyAction, IPRangeManager}; 
pub use coretex_simd::{simd_utils, SimdCapabilities};
pub use coretex_websocket::{WebSocketServer, WebSocketClient, WebSocketConfig, WebSocketMessage, WebSocketStats, HeartbeatInfo, ReconnectInfo, AckInfo, HeartbeatManager, WsRateLimiter, ConnectionState, AuthRequest, AuthOkResponse, SearchRequest as WsSearchRequest, SearchResponse as WsSearchResponse, SearchResult as WsSearchResult, VectorEntry as WsVectorEntry, InsertRequest as WsInsertRequest, InsertResponse as WsInsertResponse, DeleteRequest as WsDeleteRequest, DeleteResponse as WsDeleteResponse, SubscribeRequest as WsSubscribeRequest, UnsubscribeRequest as WsUnsubscribeRequest, DataChangeEvent as WsDataChangeEvent, ErrorResponse as WsErrorResponse};
pub use coretex_api::graphql::{
    start_graphql_server,
    QueryRoot, MutationRoot, SubscriptionRoot,
    CollectionSchema as GqlCollectionSchema, SearchResultItem, VectorItem,
    InsertResult, DeleteResult, HealthInfo, DataChangeEvent as GqlDataChangeEvent,
    SearchInput, BatchSearchInput, CreateCollectionInput, VectorInput as GqlVectorInput,
    MetadataFilterInput, CompositeFilterInput, DistanceMetricEnum,
};
#[cfg(feature = "tantivy")]
pub use coretex_tantivy::{TantivySearcher, TantivyIndexConfig, TantivyDocumentEntry, TantivySearchResult, TantivyError};
pub use coretex_graph::{GraphDatabase, GraphNode, GraphEdge, GraphPath, GraphError};
pub use coretex_hybrid::{
    MultiModalDocument, VectorData, TextData, ScalarValue, TimeSeriesData, GeoLocation,
    HybridQuery, VectorQuery, TextQuery, ScalarFilter, QueryWeights, DistanceMetric,
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
pub use coretex_data::{DataManager, VectorRecord, BulkResult, UnifiedStorageAdapter, AdapterError, ConsistencyLevel, AdapterStats};
pub use coretex_failover::{FailoverManager, FailoverConfig, FailoverEvent, NodeHealth, NodeStatus, ClusterStats, ConnectionPool, RaftRpc, HttpRaftRpc, VoteRequest, VoteResponse, HeartbeatRequest, HeartbeatResponse, LogEntry, LogCommand, AppendEntriesRequest, AppendEntriesResponse, RaftLog, LogReplicator};
pub use coretex_domain_index::{DomainIndex, DomainDocument, DomainSearchResult, DomainIndexManager, NewsWeatherIndex, GeoLocationIndex, FinancialIndex, KnowledgeIndex};
pub use coretex_search_pipeline::{TextTokenizer, StopWords, Stemmer, RRFFusion, Candidate, RerankScorer, BM25RerankScorer, LengthPenaltyScorer, RerankPipeline, Modality, EmbeddingModel, RoutingStrategy, RoutingWeights, CrossModalResult, CrossModalRetriever};
pub use coretex_grpo::{GRPOConfig, PolicyNetwork, GRPOExperience, GRPOStats, GRPOOptimizer, GRPOUpdateResult, GRPOSearchOptimizer, SearchAction};
pub use coretex_bio::{KmerIndexer, SequenceChunker, SequenceChunk, SequenceChunkWithMeta, BinaryVector, IntegerVector, SpacetimeIndex, SpacetimePoint, UserDefinedFunction, UdfType, UdfParameter, UdfParamType, UdfRegistry};
pub use coretex_types_extra::{DE9IM, SpatialRelation, Topology3D, WindowType, WindowFunction, WindowResult, TimeSeriesWindow, DocumentChunk, RagResult, RagRetriever, ECommerceIndex, Product, Order, InventoryItem, MedicalIndex, Patient, Diagnosis, Drug, LogisticsIndex, Package, Route, Carrier};
pub use coretex_observability_extra::{AlertChannel, AlertNotification, WebhookChannel, SlackChannel, EmailChannel, EmailMessage, PagerDutyChannel, AlertDispatcher, DispatchResult, SpanContext, ContextPropagator, TraceHeaderFormat, PITRManager, TimelineEntry, BackupRecord, PITRReport};
pub use coretex_ha_extra::{RaftSnapshot, InstallSnapshotRequest, InstallSnapshotResponse, SnapshotStore, ExtendedRaftRpc, HttpExtendedRaftRpc, RaftSnapshotManager, TwoPCState, ParticipantStateFull, TwoPCTransaction, TwoPCCoordinator, TwoPCRpc, MockTwoPCRpc, CheckpointRecord, CrashRecoveryManager, RecoveryReport};

pub struct CoreTexDB {
    pub data_manager: DataManager,
    pub config: DbConfig,
    pub wal: Option<Arc<WriteAheadLog>>,
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
    pub wal_enabled: bool,
    pub wal_max_segment_size: u64,
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
            // FileStorage is already a crash-safe append-only log, so the
            // separate WAL would only be a second, redundant journal.
            wal_enabled: false,
            wal_max_segment_size: 64 * 1024 * 1024, // 64 MB
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseMetadata {
    pub version: String,
    pub created_at: u64,
    pub last_modified: u64,
    /// Collection names, kept so older metadata files and human readers still
    /// work. `schemas` is the authoritative copy.
    pub collections: Vec<String>,
    /// Full collection definitions, restored into memory at startup.
    #[serde(default)]
    pub schemas: Vec<CollectionSchema>,
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
            schemas: vec![],
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
            wal_enabled: false,
            wal_max_segment_size: 64 * 1024 * 1024,
        }
    }

    /// Config for a purely in-memory database: no directories, no files.
    pub fn memory_only() -> Self {
        Self {
            memory_only: true,
            create_dirs_on_init: false,
            wal_enabled: false,
            ..Self::default()
        }
    }
}

impl CoreTexDB {
    /// A volatile, in-memory database. Nothing is written to disk; intended for
    /// tests and ephemeral workloads. Use [`CoreTexDB::with_config`] with a
    /// non-`memory_only` config for durable storage.
    pub fn new() -> Self {
        Self::with_config(DbConfig::memory_only())
    }

    /// Build a database from `config`. When `config.memory_only` is false the
    /// data lives in a durable [`FileStorage`] log under `config.data_dir`.
    ///
    /// The storage engine is opened by [`CoreTexDB::init`], not here, so that
    /// its failure can be reported as an error instead of a panic.
    pub fn with_config(config: DbConfig) -> Self {
        let storage: Box<dyn StorageEngine> = if config.memory_only {
            Box::new(MemoryStorage::new())
        } else {
            Box::new(FileStorage::new(FileStorage::store_path(&config.data_dir)))
        };
        let storage = Arc::new(RwLock::new(storage));
        let index_manager = Arc::new(IndexManager::new());
        let data_manager = DataManager::new(storage, index_manager);

        Self {
            data_manager,
            config,
            wal: None,
        }
    }

    pub async fn init(&self) -> Result<()> {
        if self.config.create_dirs_on_init && !self.config.memory_only {
            self.create_directories().await?;
        }

        if !self.config.memory_only {
            self.init_metadata().await?;

            // Open the durable log before reading anything back out of it.
            self.data_manager.storage_ref().write().await.init().await?;

            // Rebuild in-memory state from the manifest plus the vector log.
            let metadata = self.load_metadata().await?;
            if !metadata.schemas.is_empty() {
                let vectors = self.data_manager.restore_from_storage(&metadata.schemas).await?;
                tracing::info!(
                    "restored {} collection(s) and {} vector(s) from {}",
                    metadata.schemas.len(),
                    vectors,
                    self.config.data_dir
                );
            }
        }

        // Initialize WAL if enabled
        if self.config.wal_enabled && !self.config.memory_only {
            use crate::coretex_utils::wal::WriteAheadLog;
            
            let wal = Arc::new(
                WriteAheadLog::new(&self.config.wal_dir)
                    .with_max_segment_size(self.config.wal_max_segment_size)
            );
            wal.init().await
                .map_err(CoreTexError::Io)?;

            // Wire WAL into DataManager (OnceLock ensures this happens exactly once)
            self.data_manager.set_wal(Arc::clone(&wal))
                .map_err(|_| CoreTexError::Internal("WAL already set".into()))?;

            // Recover from WAL (replay any unapplied entries)
            let recovery_result = self.data_manager.recover_from_wal().await?;
            
            if !recovery_result.is_clean() {
                eprintln!(
                    "WAL recovery: {} total, {} replayed, {} skipped, {} corrupted",
                    recovery_result.total_entries,
                    recovery_result.replayed,
                    recovery_result.skipped,
                    recovery_result.corrupted,
                );
            }
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
                    .map_err(CoreTexError::Io)?;
            }
        }
        
        let collections_dir = PathBuf::from(&self.config.data_dir).join("collections");
        if !collections_dir.exists() {
            fs::create_dir_all(&collections_dir)
                .map_err(CoreTexError::Io)?;
        }
        
        Ok(())
    }
    
    pub async fn init_metadata(&self) -> Result<()> {
        let metadata_path = PathBuf::from(&self.config.data_dir).join("metadata.json");
        
        if metadata_path.exists() {
            let content = fs::read_to_string(&metadata_path)
                .map_err(CoreTexError::Io)?;
            
            let _metadata: DatabaseMetadata = serde_json::from_str(&content)
                .map_err(|e| CoreTexError::ValidationError(format!("Invalid metadata format: {}", e)))?;
        } else {
            let metadata = DatabaseMetadata::default();
            let content = serde_json::to_string_pretty(&metadata)
                .map_err(CoreTexError::Serialization)?;
            fs::write(&metadata_path, content)
                .map_err(CoreTexError::Io)?;
        }
        
        Ok(())
    }
    
    pub async fn load_metadata(&self) -> Result<DatabaseMetadata> {
        let metadata_path = PathBuf::from(&self.config.data_dir).join("metadata.json");
        
        if !metadata_path.exists() {
            return Ok(DatabaseMetadata::default());
        }
        
        let content = fs::read_to_string(&metadata_path)
            .map_err(CoreTexError::Io)?;
        
        let metadata: DatabaseMetadata = serde_json::from_str(&content)
            .map_err(|e| CoreTexError::ValidationError(format!("Invalid metadata format: {}", e)))?;
        
        Ok(metadata)
    }
    
    /// Write `metadata` to `metadata.json` atomically: a reader sees either the
    /// previous manifest or the complete new one, never a half-written file.
    pub async fn save_metadata(&self, metadata: &DatabaseMetadata) -> Result<()> {
        let metadata_path = PathBuf::from(&self.config.data_dir).join("metadata.json");
        let temp_path = metadata_path.with_extension("json.tmp");
        let content = serde_json::to_string_pretty(metadata)
            .map_err(CoreTexError::Serialization)?;

        fs::write(&temp_path, content).map_err(CoreTexError::Io)?;
        fs::rename(&temp_path, &metadata_path).map_err(CoreTexError::Io)?;
        Ok(())
    }

    /// Persist the current set of collections to the manifest. A no-op for
    /// in-memory databases.
    async fn persist_manifest(&self) -> Result<()> {
        if self.config.memory_only {
            return Ok(());
        }

        let mut metadata = self.load_metadata().await.unwrap_or_default();
        metadata.schemas = self.data_manager.schemas().await;
        metadata.collections = metadata.schemas.iter().map(|s| s.name.clone()).collect();
        metadata.last_modified = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        self.save_metadata(&metadata).await
    }

    pub async fn create_collection(&self, name: &str, dimension: usize, metric: &str) -> Result<()> {
        self.data_manager.create_collection(name, dimension, metric).await?;
        self.persist_manifest().await
    }

    /// Create a collection with an explicit index type (`brute_force`, `hnsw`,
    /// `ivf`, `scalar`). Unrecognised values fall back to the exact index.
    pub async fn create_collection_with_index(
        &self,
        name: &str,
        dimension: usize,
        metric: &str,
        index_type: &str,
    ) -> Result<()> {
        self.data_manager
            .create_collection_with_index(name, dimension, metric, index_type)
            .await?;
        self.persist_manifest().await
    }

    pub async fn delete_collection(&self, name: &str) -> Result<()> {
        self.data_manager.delete_collection(name).await?;
        self.persist_manifest().await
    }

    pub async fn rename_collection(&self, old_name: &str, new_name: &str) -> Result<()> {
        self.data_manager.rename_collection(old_name, new_name).await?;
        self.persist_manifest().await
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
