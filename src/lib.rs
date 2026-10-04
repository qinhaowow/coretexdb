//! CoreTexDB - A multimodal vector database for AI applications 

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use serde::{Deserialize, Serialize};

pub const DB_VERSION: &str = env!("CARGO_PKG_VERSION");

/// C API: version string for `include/coretexdb.h` consumers.
/// Pointer remains valid for the lifetime of the process (static).
#[no_mangle]
pub extern "C" fn coretexdb_version() -> *const std::os::raw::c_char {
    static VERSION_C: std::sync::OnceLock<Vec<std::os::raw::c_char>> = std::sync::OnceLock::new();
    let buf = VERSION_C.get_or_init(|| {
        let mut v: Vec<std::os::raw::c_char> =
            DB_VERSION.bytes().map(|b| b as std::os::raw::c_char).collect();
        v.push(0);
        v
    });
    buf.as_ptr()
}

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
pub mod coretex_crypto;
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
pub mod coretex_ffi;
pub mod coretex_replication;
pub mod coretex_cluster;
pub mod coretex_pubsub;

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
pub use coretex_replication::{ReplicationSnapshot, EntriesBatch, ReplicationStatus, ReplicationTransport, HttpTransport, InProcessTransport, ReplicaSync, SyncOutcome};
pub use coretex_cluster::{slot_of, SLOT_COUNT, CollectionChunk, NodeInfo, ClusterNodeHealth, NodeRouting, ClusterInfo, ClusterRouter, ClusterTransport, LocalNodeTransport, ClusterMigrator, MigrationOutcome};
pub use coretex_pubsub::{EventBus, EventReceiver};

pub use coretex_core::{Vector, Document, CollectionSchema, IndexConfig, IndexType, CoreTexError, Result};
pub use coretex_storage::{StorageEngine, MemoryStorage, FileStorage};
#[cfg(feature = "rocksdb")]
pub use coretex_storage::PersistentStorage; 
pub use coretex_index::{VectorIndex, BruteForceIndex, IndexManager, SearchResult, HNSWIndex, IVFIndex, ScalarIndex}; 
pub use coretex_query::{QueryType, QueryParams, QueryResult as CoreTexQueryResult, DefaultQueryProcessor, QueryPlanner, QueryItem};
pub use coretex_query::cost_model::{IndexSelector as QueryIndexSelector, CostInput, CostEstimate, IndexKind as QueryIndexKind, JoinType, JoinPlan, JoinPushdownOptimizer, OptimizationStats};
pub use coretex_bm25::{BM25Index, BM25Result, HybridQueryEngine, HybridSearchResult, MetadataFilter, FilterCondition}; 
pub use coretex_api::rest::{start_server, start_server_with_db, ApiConfig};
pub use coretex_api::graphql::{AppSchema, build_schema}; 
pub use coretex_cli::{run_cli, run_cli_with_args}; 
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
pub use coretex_grpc::{CoretexService, start_grpc_server, start_grpc_server_with_config, start_grpc_server_shared, GrpcConfig, GrpcMetrics, AuthInterceptor, RateLimitInterceptor, AuthLayer, AuthServiceWrapper, MetricsLayer, MetricsService, ComposedInterceptor};
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
pub use coretex_compression::{VectorCompressor, CompressedVector, CompressionAlgorithm, CompressionStats, CompressionFactory, CompressedStorage, RunLengthEncoding, DeltaCoding, QuantizationCompressor};
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
pub use coretex_grpo::{GRPOConfig, PolicyNetwork, GRPOExperience, GRPOStats, GRPOOptimizer, GRPOUpdateResult, GRPOSearchOptimizer, SearchAction, AdaptiveSearchController};
pub use coretex_bio::{KmerIndexer, SequenceChunker, SequenceChunk, SequenceChunkWithMeta, BinaryVector, IntegerVector, SpacetimeIndex, SpacetimePoint, UserDefinedFunction, UdfType, UdfParameter, UdfParamType, UdfRegistry};
pub use coretex_types_extra::{DE9IM, SpatialRelation, Topology3D, WindowType, WindowFunction, WindowResult, TimeSeriesWindow, DocumentChunk, RagResult, RagRetriever, ECommerceIndex, Product, Order, InventoryItem, MedicalIndex, Patient, Diagnosis, Drug, LogisticsIndex, Package, Route, Carrier};
pub use coretex_observability_extra::{AlertChannel, AlertNotification, WebhookChannel, SlackChannel, EmailChannel, EmailMessage, PagerDutyChannel, AlertDispatcher, DispatchResult, SpanContext, ContextPropagator, TraceHeaderFormat, PITRManager, TimelineEntry, BackupRecord, PITRReport};
pub use coretex_ha_extra::{RaftSnapshot, InstallSnapshotRequest, InstallSnapshotResponse, SnapshotStore, ExtendedRaftRpc, HttpExtendedRaftRpc, RaftSnapshotManager, TwoPCState, ParticipantStateFull, TwoPCTransaction, TwoPCCoordinator, TwoPCRpc, MockTwoPCRpc, CheckpointRecord, CrashRecoveryManager, RecoveryReport};

pub struct CoreTexDB {
    pub data_manager: DataManager,
    pub config: DbConfig,
    pub wal: Option<Arc<WriteAheadLog>>,
    /// Per-collection file persistence nested under FileStorage's `store/` dir.
    pub persistence: Option<Arc<coretex_persistence::PersistenceManager>>,
    /// Derived BM25 text indexes for [`Self::hybrid_search`], keyed by
    /// `(collection, text_field)` and validated against
    /// [`DataManager::data_version`] so no write can leave a stale index
    /// behind. Guards are never held across an `.await`.
    bm25_cache: std::sync::RwLock<std::collections::HashMap<(String, String), CachedBm25>>,
}

/// Parameters for [`CoreTexDB::hybrid_search`].
///
/// Provide `vector`, `text`, or both. With both, an id returned by both
/// retrievers outranks one returned by either alone (reciprocal rank fusion).
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct HybridSearchRequest {
    /// Query vector for the ANN side; `None` skips that side.
    pub vector: Option<Vec<f32>>,
    /// Text query for the BM25 side; `None` (or blank) skips that side.
    pub text: Option<String>,
    /// How many hits to return.
    #[serde(default)]
    pub k: usize,
    /// Metadata filter, applied to **both** sides before fusing.
    pub filter: Option<serde_json::Value>,
    /// Metadata field holding the document text (default `"text"`).
    pub text_field: Option<String>,
}

impl HybridSearchRequest {
    /// Request `k` hits; then add a `vector` and/or `text`.
    pub fn new(k: usize) -> Self {
        Self {
            k,
            ..Default::default()
        }
    }

    pub fn with_vector(mut self, vector: Vec<f32>) -> Self {
        self.vector = Some(vector);
        self
    }

    pub fn with_text(mut self, text: impl Into<String>) -> Self {
        self.text = Some(text.into());
        self
    }

    pub fn with_filter(mut self, filter: serde_json::Value) -> Self {
        self.filter = Some(filter);
        self
    }

    /// Metadata field to read the document text from (default `"text"`).
    pub fn with_text_field(mut self, field: impl Into<String>) -> Self {
        self.text_field = Some(field.into());
        self
    }
}

/// One hit from [`CoreTexDB::hybrid_search`].
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct HybridSearchHit {
    /// Vector id.
    pub id: String,
    /// Reciprocal-rank-fused score; higher is better.
    pub score: f32,
    /// Which retrievers returned this id: `"vector"`, `"text"`, or both.
    pub sources: Vec<String>,
}

/// A BM25 index materialised from the map state at `version`.
struct CachedBm25 {
    version: u64,
    index: Arc<BM25Index>,
}

/// BM25 term-saturation / length-normalisation parameters (standard defaults).
const BM25_K1: f32 = 1.2;
const BM25_B: f32 = 0.75;
/// RRF constant: a result at `rank` contributes `1 / (RRF_K + rank)`.
const RRF_K: u32 = 60;

/// On-disk layout under an install root (`--data-dir`):
///
/// ```text
/// {base}/
/// ├── bin/ include/
/// └── data/
///     ├── coretex/
///     │   ├── collections/<name>/{vectors,metadata}
///     │   ├── indexes/{vector,scalar}
///     │   ├── metadata/metadata.json
///     │   └── store/store-NNNNNN.log
///     ├── wal/
///     ├── backup/{full,incremental}/
///     ├── logs/
///     └── temp/
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbConfig {
    /// Install root (`--data-dir`).
    #[serde(default)]
    pub base_dir: String,
    /// Main database data: `{base}/data/coretex`.
    pub data_dir: String,
    pub bin_dir: String,
    /// `{base}/data/logs`.
    pub log_dir: String,
    /// `{base}/data/wal`.
    pub wal_dir: String,
    /// `{base}/data/backup` (with `full/` and `incremental/` children).
    pub backup_dir: String,
    pub include_dir: String,
    /// `{base}/data/temp`.
    #[serde(default)]
    pub temp_dir: String,
    pub memory_only: bool,
    pub max_vectors_per_collection: usize,
    pub create_dirs_on_init: bool,
    pub wal_enabled: bool,
    pub wal_max_segment_size: u64,
    /// fsync every storage write (power-loss durability). Default true.
    #[serde(default = "default_sync_writes")]
    pub sync_writes: bool,
}

fn default_sync_writes() -> bool {
    true
}

impl DbConfig {
    fn from_base(base_dir: &str) -> Self {
        Self {
            base_dir: base_dir.to_string(),
            data_dir: format!("{}/data/coretex", base_dir),
            bin_dir: format!("{}/bin", base_dir),
            log_dir: format!("{}/data/logs", base_dir),
            wal_dir: format!("{}/data/wal", base_dir),
            backup_dir: format!("{}/data/backup", base_dir),
            include_dir: format!("{}/include", base_dir),
            temp_dir: format!("{}/data/temp", base_dir),
            memory_only: false,
            max_vectors_per_collection: 1000000,
            create_dirs_on_init: true,
            // FileStorage is already a crash-safe append-only log, so the
            // separate WAL would only be a second, redundant journal.
            wal_enabled: false,
            wal_max_segment_size: 64 * 1024 * 1024, // 64 MB
            sync_writes: true,
        }
    }

    pub fn collections_dir(&self) -> std::path::PathBuf {
        std::path::PathBuf::from(&self.data_dir).join("collections")
    }

    pub fn metadata_dir(&self) -> std::path::PathBuf {
        std::path::PathBuf::from(&self.data_dir).join("metadata")
    }

    pub fn metadata_path(&self) -> std::path::PathBuf {
        self.metadata_dir().join("metadata.json")
    }

    pub fn indexes_dir(&self) -> std::path::PathBuf {
        std::path::PathBuf::from(&self.data_dir).join("indexes")
    }

    pub fn backup_full_dir(&self) -> std::path::PathBuf {
        std::path::PathBuf::from(&self.backup_dir).join("full")
    }

    pub fn backup_incremental_dir(&self) -> std::path::PathBuf {
        std::path::PathBuf::from(&self.backup_dir).join("incremental")
    }
}

impl Default for DbConfig {
    fn default() -> Self {
        Self::from_base("./coretex_data")
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
        Self::from_base(base_dir)
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
            Box::new(
                FileStorage::new(FileStorage::store_path(&config.data_dir))
                    // Durable by default: survive power loss, not just process
                    // crashes. Opt out via `DbConfig::sync_writes = false`.
                    .with_fsync(config.sync_writes),
            )
        };
        let storage = Arc::new(RwLock::new(storage));
        let index_manager = Arc::new(IndexManager::new());
        let data_manager = if config.memory_only {
            DataManager::new(storage, index_manager)
        } else {
            // Enable index persistence: on init a checksum-matching index file
            // is loaded instead of rebuilding HNSW/IVF/PQ from storage.
            DataManager::new(storage, index_manager)
                .with_indexes_dir(config.indexes_dir().join("vector"))
        };

        let persistence = if config.memory_only {
            None
        } else {
            let pconfig = coretex_persistence::PersistenceConfig {
                backend: coretex_persistence::StorageBackend::FileSystem,
                data_dir: config.data_dir.clone(),
                rocksdb_config: None,
                s3_config: None,
                replication_factor: 1,
                sync_write: false,
                wal_enabled: false,
                compression_enabled: false,
            };
            Some(Arc::new(coretex_persistence::PersistenceManager::new(pconfig)))
        };

        Self {
            data_manager,
            config,
            wal: None,
            persistence,
            bm25_cache: std::sync::RwLock::new(std::collections::HashMap::new()),
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

            // Initialize the nested PersistenceManager (data/coretex/collections/).
            if let Some(ref p) = self.persistence {
                p.initialize().await.map_err(|e| {
                    CoreTexError::Internal(format!("persistence init failed: {}", e))
                })?;
            }

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
        // Runtime creates only data-side dirs under the install root.
        // bin/ and include/ belong to the install layout and are NOT created here.
        let dirs = vec![
            &self.config.data_dir,
            &self.config.log_dir,
            &self.config.wal_dir,
            &self.config.backup_dir,
            &self.config.temp_dir,
        ];

        for dir in dirs {
            let path = PathBuf::from(dir);
            if !path.exists() {
                fs::create_dir_all(&path)
                    .map_err(CoreTexError::Io)?;
            }
        }

        let extra = [
            self.config.collections_dir(),
            self.config.metadata_dir(),
            self.config.indexes_dir().join("vector"),
            self.config.indexes_dir().join("scalar"),
            self.config.backup_full_dir(),
            self.config.backup_incremental_dir(),
            PathBuf::from(&self.config.backup_dir).join("snapshots"),
            PathBuf::from(&self.config.data_dir).join("store"),
            // Reserved for audit logging. Created so the layout matches the
            // documented install tree, but nothing writes here: `AuditLogger`
            // lives in `coretex_security`, which has no production call site.
            // See README §0 (未接线 → 审计日志).
            PathBuf::from(&self.config.log_dir).join("audit"),
            PathBuf::from(&self.config.base_dir).join("data").join("versions"),
        ];
        for path in extra {
            if !path.exists() {
                fs::create_dir_all(&path).map_err(CoreTexError::Io)?;
            }
        }

        Ok(())
    }

    /// Atomic create: write to a sibling temp file, fsync, rename over target.
    /// Does not overwrite an existing target (caller must check `!exists`).
    fn write_file_atomic(path: &std::path::Path, contents: &str) -> std::io::Result<()> {
        use std::io::Write as _;
        let parent = path
            .parent()
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "no parent"))?;
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("file");
        let tmp = parent.join(format!(".{}.{}.tmp", name, std::process::id()));
        {
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(contents.as_bytes())?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp, path)?;
        Ok(())
    }

    pub async fn init_metadata(&self) -> Result<()> {
        let metadata_path = self.config.metadata_path();
        if let Some(parent) = metadata_path.parent() {
            if !parent.exists() {
                fs::create_dir_all(parent).map_err(CoreTexError::Io)?;
            }
        }

        if metadata_path.exists() {
            let content = fs::read_to_string(&metadata_path)
                .map_err(CoreTexError::Io)?;

            let _metadata: DatabaseMetadata = serde_json::from_str(&content)
                .map_err(|e| CoreTexError::ValidationError(format!("Invalid metadata format: {}", e)))?;
        } else {
            let metadata = DatabaseMetadata::default();
            let content = serde_json::to_string_pretty(&metadata)
                .map_err(CoreTexError::Serialization)?;
            Self::write_file_atomic(&metadata_path, &content).map_err(CoreTexError::Io)?;
        }

        // Spec: metadata/ must always contain config.toml and auth.json.
        // Create atomically; never overwrite an existing file.
        let meta_dir = self.config.metadata_dir();
        let config_toml = meta_dir.join("config.toml");
        if !config_toml.exists() {
            Self::write_file_atomic(&config_toml, "").map_err(CoreTexError::Io)?;
        }
        let auth_json = meta_dir.join("auth.json");
        if !auth_json.exists() {
            Self::write_file_atomic(&auth_json, "{\"users\":{}}").map_err(CoreTexError::Io)?;
        }

        Ok(())
    }
    
    pub async fn load_metadata(&self) -> Result<DatabaseMetadata> {
        let metadata_path = self.config.metadata_path();

        if !metadata_path.exists() {
            return Ok(DatabaseMetadata::default());
        }
        
        let content = fs::read_to_string(&metadata_path)
            .map_err(CoreTexError::Io)?;
        
        let metadata: DatabaseMetadata = serde_json::from_str(&content)
            .map_err(|e| CoreTexError::ValidationError(format!("Invalid metadata format: {}", e)))?;
        
        Ok(metadata)
    }
    
    /// Write the manifest atomically under `data/coretex/metadata/`: a reader
    /// sees either the previous manifest or the complete new one, never a
    /// half-written file. The `.tmp` sibling lives next to the target so the
    /// rename stays on one filesystem.
    pub async fn save_metadata(&self, metadata: &DatabaseMetadata) -> Result<()> {
        let metadata_path = self.config.metadata_path();
        if let Some(parent) = metadata_path.parent() {
            if !parent.exists() {
                fs::create_dir_all(parent).map_err(CoreTexError::Io)?;
            }
        }
        let temp_path = metadata_path.with_extension("json.tmp");
        let content = serde_json::to_string_pretty(metadata)
            .map_err(CoreTexError::Serialization)?;

        // fsync the temp file before rename so a crash cannot publish a
        // half-written manifest; then fsync the directory so the rename
        // itself is durable.
        {
            use std::io::Write as _;
            let mut f = std::fs::File::create(&temp_path).map_err(CoreTexError::Io)?;
            f.write_all(content.as_bytes()).map_err(CoreTexError::Io)?;
            f.sync_all().map_err(CoreTexError::Io)?;
        }
        fs::rename(&temp_path, &metadata_path).map_err(CoreTexError::Io)?;
        if let Some(parent) = metadata_path.parent() {
            if let Ok(dir) = std::fs::File::open(parent) {
                let _ = dir.sync_all();
            }
        }
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
        // Create the per-collection directory under data/coretex/collections/.
        if self.persistence.is_some() {
            let base = self.config.collections_dir().join(name);
            let _ = std::fs::create_dir_all(base.join("vectors"));
            let _ = std::fs::create_dir_all(base.join("metadata"));
        }
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
        // Create the per-collection directory under data/coretex/collections/.
        if self.persistence.is_some() {
            let base = self.config.collections_dir().join(name);
            let _ = std::fs::create_dir_all(base.join("vectors"));
            let _ = std::fs::create_dir_all(base.join("metadata"));
        }
        self.persist_manifest().await
    }

    pub async fn delete_collection(&self, name: &str) -> Result<()> {
        self.data_manager.delete_collection(name).await?;
        // Remove the per-collection directory under data/coretex/collections/.
        let base = self.config.collections_dir().join(name);
        if base.exists() {
            let _ = std::fs::remove_dir_all(&base);
        }
        self.persist_manifest().await
    }

    pub async fn rename_collection(&self, old_name: &str, new_name: &str) -> Result<()> {
        self.data_manager.rename_collection(old_name, new_name).await?;
        // Move the per-collection directory under data/coretex/collections/.
        let old_path = self.config.collections_dir().join(old_name);
        let new_path = self.config.collections_dir().join(new_name);
        if old_path.exists() {
            let _ = std::fs::rename(&old_path, &new_path);
        }
        self.persist_manifest().await
    }

    pub async fn list_collections(&self) -> Result<Vec<String>> {
        self.data_manager.list_collections().await
    }

    pub async fn get_collection(&self, name: &str) -> Result<CollectionSchema> {
        self.data_manager.get_collection(name).await
    }

    pub async fn insert_vectors(&self, collection: &str, vectors: Vec<(String, Vec<f32>, serde_json::Value)>) -> Result<Vec<String>> {
        let ids = self.data_manager.insert_vectors(collection, vectors.clone()).await?;
        // Mirror writes into PersistenceManager (data/coretex/collections/<name>/).
        if let Some(ref p) = self.persistence {
            for (id, vec, meta) in &vectors {
                let _ = p.save_vector(collection, id, vec, Some(meta)).await;
            }
        }
        Ok(ids)
    }

    pub async fn get_vector(&self, collection: &str, id: &str) -> Result<Option<(Vec<f32>, serde_json::Value)>> {
        self.data_manager.get_vector(collection, id).await.map(|opt| {
            opt.map(|r| (r.vector, r.metadata))
        })
    }

    pub async fn delete_vectors(&self, collection: &str, ids: &[String]) -> Result<usize> {
        let n = self.data_manager.delete_vectors(collection, ids).await?;
        // Mirror deletes into PersistenceManager.
        if let Some(ref p) = self.persistence {
            for id in ids {
                let _ = p.delete_vector(collection, id).await;
            }
        }
        Ok(n)
    }

    /// Delete every vector whose metadata matches `filter`; returns the ids removed.
    pub async fn delete_vectors_where(
        &self,
        collection: &str,
        filter: &serde_json::Value,
    ) -> Result<Vec<String>> {
        let ids = self.data_manager.delete_vectors_where(collection, filter).await?;
        if let Some(ref p) = self.persistence {
            for id in &ids {
                let _ = p.delete_vector(collection, id).await;
            }
        }
        Ok(ids)
    }

    /// Remove every vector in `collection`; returns how many were removed.
    pub async fn clear_collection(&self, collection: &str) -> Result<usize> {
        let n = self.data_manager.clear_collection(collection).await?;
        // Drop the whole per-collection directory and recreate it empty.
        let base = self.config.collections_dir().join(collection);
        if base.exists() {
            let _ = std::fs::remove_dir_all(&base);
        }
        let _ = std::fs::create_dir_all(base.join("vectors"));
        let _ = std::fs::create_dir_all(base.join("metadata"));
        Ok(n)
    }

    /// Every vector in `collection`, ordered by id, as `(id, vector, metadata)`.
    pub async fn list_vectors(
        &self,
        collection: &str,
    ) -> Result<Vec<(String, Vec<f32>, serde_json::Value)>> {
        let records = self.data_manager.get_all_vectors(collection).await?;
        Ok(records
            .into_iter()
            .map(|(id, record)| (id, record.vector, record.metadata))
            .collect())
    }

    pub async fn search(&self, collection: &str, query: Vec<f32>, k: usize, filter: Option<serde_json::Value>) -> Result<Vec<SearchResult>> {
        self.data_manager.search(collection, query, k, filter).await
    }

    /// Hybrid search: fuse ANN neighbours with BM25 text matches using
    /// reciprocal rank fusion ([`RRF_K`]).
    ///
    /// Either side may be omitted. The BM25 index is materialised from the
    /// collection's `metadata[text_field]` string fields and cached against
    /// [`DataManager::data_version`], so it is rebuilt after any write but
    /// never served stale. `filter` is enforced on both sides — the vector
    /// side inside [`Self::search`], text hits before fusing — so every
    /// returned id satisfies it.
    ///
    /// For reranked ordering see [`Self::hybrid_search_reranked`].
    pub async fn hybrid_search(
        &self,
        collection: &str,
        request: HybridSearchRequest,
    ) -> Result<Vec<HybridSearchHit>> {
        self.hybrid_search_impl(collection, request, false).await
    }

    /// [`Self::hybrid_search`] followed by a two-stage rerank of the fused
    /// candidates: the coarse stage min-max normalises the RRF scores as a
    /// single group, the fine stage scores each candidate's *real*
    /// `metadata[text_field]` text against the query terms, so lexical
    /// overlap can overturn close RRF ties. Ids, `sources`, `k` and the
    /// `filter` contract are unchanged; hit scores become rerank final
    /// scores (higher is better) instead of RRF `1 / (RRF_K + rank)`.
    ///
    /// Without a text query there is nothing to match against, so the RRF
    /// order and scores pass through untouched.
    pub async fn hybrid_search_reranked(
        &self,
        collection: &str,
        request: HybridSearchRequest,
    ) -> Result<Vec<HybridSearchHit>> {
        self.hybrid_search_impl(collection, request, true).await
    }

    async fn hybrid_search_impl(
        &self,
        collection: &str,
        request: HybridSearchRequest,
        rerank: bool,
    ) -> Result<Vec<HybridSearchHit>> {
        // Fail early and identically for both sides: an unknown collection
        // is an error, not a silently empty result.
        self.data_manager.get_collection(collection).await?;
        if request.k == 0 {
            return Ok(Vec::new());
        }

        // Fuse from a pool deeper than k so the two sides can overlap.
        let pool = request.k.saturating_mul(4).max(16);
        let mut ranked: Vec<MultiModalResult> = Vec::new();

        if let Some(vector) = request.vector.clone() {
            let hits = self
                .search(collection, vector, pool, request.filter.clone())
                .await?;
            ranked.extend(hits.into_iter().enumerate().map(|(rank, hit)| {
                MultiModalResult {
                    id: hit.id,
                    // RRF ranks by position; the raw side scores are carried
                    // only for callers that pick a weighted fusion instead.
                    score: 0.0,
                    rank,
                    source: "vector".to_string(),
                    weight: 1.0,
                    metadata: None,
                }
            }));
        }

        let text_query = request
            .text
            .as_deref()
            .map(str::trim)
            .filter(|t| !t.is_empty());
        if let Some(text) = text_query {
            let field = request.text_field.as_deref().unwrap_or("text");
            let index = self.bm25_index_for(collection, field).await?;
            let text_hits = index
                .search(text, pool)
                .await
                .map_err(|e| CoreTexError::Internal(format!("bm25 search failed: {}", e)))?;

            // BM25 scores every document; ones with none of the query terms
            // score exactly 0 but still occupy top-k slots. They are not
            // hits — keep them and they'd give pure vector results phantom
            // "text" sources and make an empty query look non-empty.
            let text_hits: Vec<_> = text_hits.into_iter().filter(|h| h.score > 0.0).collect();

            // Filter first, then assign ranks: the rank RRF sees must be the
            // position a caller would count, not a pre-filter gap.
            let mut kept: Vec<(String, f32)> = Vec::new();
            if let Some(filter) = &request.filter {
                let ids: Vec<String> = text_hits.iter().map(|h| h.id.clone()).collect();
                let records = self
                    .data_manager
                    .get_vectors_by_ids(collection, &ids)
                    .await?;
                let by_id: std::collections::HashMap<&str, &_> =
                    records.iter().map(|(id, r)| (id.as_str(), r)).collect();
                for hit in text_hits {
                    let passes = by_id
                        .get(hit.id.as_str())
                        .map(|record| DataManager::matches_filter(&record.metadata, filter))
                        .unwrap_or(false);
                    if passes {
                        kept.push((hit.id, hit.score));
                    }
                }
            } else {
                // No filter: the cached index is rebuilt on every write, so
                // it cannot contain deleted ids.
                kept.extend(text_hits.into_iter().map(|h| (h.id, h.score)));
            }

            ranked.extend(
                kept.into_iter().enumerate().map(|(rank, (id, score))| {
                    MultiModalResult {
                        id,
                        score,
                        rank,
                        source: "text".to_string(),
                        weight: 1.0,
                        metadata: None,
                    }
                }),
            );
        }

        // Works for a single side too: a lone rank still orders by rank.
        let engine = ScoreFusionEngine::new(ScoreFusion::RRF { k: RRF_K });
        let fused = engine.fuse(&ranked);

        // Rerank only pays off with a text query (the fine stage measures
        // term overlap); without one it would be a monotonic rescore of
        // the same order, so pass the RRF result through untouched.
        if rerank {
            if let Some(text) = text_query {
                return self
                    .rerank_fused(
                        collection,
                        text,
                        fused,
                        request.k,
                        request.text_field.as_deref(),
                    )
                    .await;
            }
        }

        Ok(fused
            .into_iter()
            .take(request.k)
            .map(|fused| HybridSearchHit {
                id: fused.id,
                score: fused.score,
                sources: fused.sources,
            })
            .collect())
    }

    /// Two-stage rerank of fused hybrid candidates: the coarse stage
    /// min-max normalises the RRF scores as one group, the fine stage
    /// scores each candidate's *real* `metadata[text_field]` text against
    /// the query, so lexical overlap can overturn close RRF ties. Ids and
    /// their `sources` survive; ties break by id for a deterministic
    /// order; candidates the coarse stage drops (top `coarse_top_k` keep)
    /// keep their RRF score.
    async fn rerank_fused(
        &self,
        collection: &str,
        text: &str,
        fused: Vec<FusedResult>,
        k: usize,
        text_field: Option<&str>,
    ) -> Result<Vec<HybridSearchHit>> {
        if fused.is_empty() {
            return Ok(Vec::new());
        }

        // Real document texts for every candidate; a record that vanished
        // between fusion and fetch simply falls back to a synthetic body
        // (the fine stage still ranks it, it just cannot win on overlap).
        let ids: Vec<String> = fused.iter().map(|f| f.id.clone()).collect();
        let records = self
            .data_manager
            .get_vectors_by_ids(collection, &ids)
            .await?;
        let field = text_field.unwrap_or("text");
        let documents: std::collections::HashMap<String, RerankDocument> = records
            .iter()
            .map(|(id, record)| {
                let body = record
                    .metadata
                    .get(field)
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string();
                (
                    id.clone(),
                    RerankDocument {
                        id: id.clone(),
                        text: body,
                        vector: None,
                    },
                )
            })
            .collect();

        // One source group, so the coarse stage normalises across the
        // whole candidate set instead of per original side.
        let raw: Vec<MultiModalResult> = fused
            .iter()
            .enumerate()
            .map(|(rank, f)| MultiModalResult {
                id: f.id.clone(),
                score: f.score,
                rank,
                source: "hybrid".to_string(),
                weight: 1.0,
                metadata: None,
            })
            .collect();

        // A fresh pipeline per call: CoarseRanker carries min/max state
        // across rank() calls, which would leak normalisation ranges
        // between queries.
        let query = HybridQuery::new().with_text(text).with_top_k(fused.len());
        let mut pipeline = TwoStageSearchPipeline::new().with_fine_config(FineRankerConfig {
            rerank_top_k: fused.len(),
            ..FineRankerConfig::default()
        });
        let reranked = pipeline.search_with_documents(&query, raw, documents);

        // Overlay rerank scores onto the fused entries (ids the coarse
        // stage dropped keep their RRF score), keeping original sources;
        // order by score desc with id as the tie-break.
        let mut score_by_id: std::collections::HashMap<&str, f32> = reranked
            .iter()
            .map(|r| (r.id.as_str(), r.score))
            .collect();
        let mut hits: Vec<HybridSearchHit> = fused
            .into_iter()
            .map(|f| HybridSearchHit {
                score: score_by_id.remove(f.id.as_str()).unwrap_or(f.score),
                id: f.id,
                sources: f.sources,
            })
            .collect();
        hits.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.id.cmp(&b.id))
        });
        hits.truncate(k);
        Ok(hits)
    }

    /// The BM25 index for `(collection, text_field)`, materialised on first
    /// use and cached until [`DataManager::data_version`] moves.
    ///
    /// Snapshot discipline: the version is read *before* the map. A write
    /// landing in between either bumps the version (this entry, stamped with
    /// the old one, is then discarded on the next call) or is not yet visible
    /// to the read. It can never be hidden *and* stamped current, because
    /// `write_data` bumps under the very write lock this read excludes.
    async fn bm25_index_for(&self, collection: &str, text_field: &str) -> Result<Arc<BM25Index>> {
        let version = self.data_manager.data_version();
        let key = (collection.to_string(), text_field.to_string());

        {
            let cache = self.bm25_cache.read().expect("bm25 cache poisoned");
            if let Some(cached) = cache.get(&key) {
                if cached.version == version {
                    return Ok(cached.index.clone());
                }
            }
        }

        // Build without the cache lock held: tokenising is the slow part and
        // must not block lookups for other collections.
        let texts = self.data_manager.text_records(collection, text_field).await;
        let docs: Vec<coretex_bm25::Document> = texts
            .into_iter()
            .map(|(id, text)| coretex_bm25::Document::new(id, text))
            .collect();
        let index = Arc::new(BM25Index::new(BM25_K1, BM25_B));
        index
            .add_documents(docs)
            .await
            .map_err(|e| CoreTexError::Internal(format!("bm25 build failed: {}", e)))?;

        let mut cache = self.bm25_cache.write().expect("bm25 cache poisoned");
        // A concurrent builder may have finished first; keep a single copy.
        if let Some(cached) = cache.get(&key) {
            if cached.version == version {
                return Ok(cached.index.clone());
            }
        }
        cache.insert(key, CachedBm25 {
            version,
            index: index.clone(),
        });
        Ok(index)
    }

    pub async fn get_vectors_count(&self, collection: &str) -> Result<usize> {
        self.data_manager.get_vectors_count(collection).await
    }

    /// Set a time-to-live (seconds) on one vector. It is removed by
    /// [`Self::purge_expired`] once the TTL elapses.
    pub async fn set_vector_ttl(&self, collection: &str, id: &str, ttl_secs: u64) -> Result<()> {
        self.data_manager.set_ttl(collection, id, ttl_secs).await
    }

    /// Remove a previously set TTL from one vector.
    pub async fn remove_vector_ttl(&self, collection: &str, id: &str) -> Result<()> {
        self.data_manager.remove_ttl(collection, id).await
    }

    /// Drop every vector whose TTL has expired, from storage, memory and the
    /// index. Returns how many were removed.
    pub async fn purge_expired(&self) -> Result<usize> {
        self.data_manager.purge_expired().await
    }

    /// Persist the ANN indexes (hnsw/ivf/pq) to `<data>/indexes/vector/` so the
    /// next `init()` can load them instead of rebuilding from storage.
    /// Returns the number of index files written.
    pub async fn save_indexes(&self) -> Result<usize> {
        self.data_manager.save_indexes().await
    }

    /// Directory holding persisted per-collection index files.
    pub fn index_dir(&self) -> std::path::PathBuf {
        self.config.indexes_dir().join("vector")
    }

    pub async fn update_vector(
        &self,
        collection: &str,
        id: &str,
        vector: Vec<f32>,
        metadata: Option<serde_json::Value>,
    ) -> Result<bool> {
        let ok = self.data_manager.update_vector(collection, id, vector.clone(), metadata.clone()).await?;
        if ok {
            if let Some(ref p) = self.persistence {
                let _ = p.save_vector(collection, id, &vector, metadata.as_ref()).await;
            }
        }
        Ok(ok)
    }

    pub async fn upsert_vectors(
        &self,
        collection: &str,
        vectors: Vec<(String, Vec<f32>, serde_json::Value)>,
    ) -> Result<(Vec<String>, Vec<String>)> {
        let result = self.data_manager.upsert_vectors(collection, vectors.clone()).await?;
        if let Some(ref p) = self.persistence {
            for (id, vec, meta) in &vectors {
                let _ = p.save_vector(collection, id, vec, Some(meta)).await;
            }
        }
        Ok(result)
    }

    pub async fn bulk_insert(
        &self,
        collection: &str,
        vectors: Vec<(String, Vec<f32>, serde_json::Value)>,
    ) -> Result<Vec<String>> {
        let ids = self.data_manager.bulk_insert(collection, vectors.clone()).await?;
        if let Some(ref p) = self.persistence {
            for (id, vec, meta) in &vectors {
                let _ = p.save_vector(collection, id, vec, Some(meta)).await;
            }
        }
        Ok(ids)
    }

    pub async fn bulk_update(
        &self,
        collection: &str,
        vectors: Vec<(String, Vec<f32>, serde_json::Value)>,
    ) -> Result<Vec<String>> {
        let ids = self.data_manager.bulk_update(collection, vectors.clone()).await?;
        if let Some(ref p) = self.persistence {
            for (id, vec, meta) in &vectors {
                let _ = p.save_vector(collection, id, vec, Some(meta)).await;
            }
        }
        Ok(ids)
    }

    pub async fn bulk_delete(
        &self,
        collection: &str,
        ids: Vec<String>,
    ) -> Result<Vec<String>> {
        let deleted = self.data_manager.bulk_delete(collection, ids.clone()).await?;
        if let Some(ref p) = self.persistence {
            for id in &deleted {
                let _ = p.delete_vector(collection, id).await;
            }
        }
        Ok(deleted)
    }

    pub async fn bulk_upsert(
        &self,
        collection: &str,
        vectors: Vec<(String, Vec<f32>, serde_json::Value)>,
    ) -> Result<BulkResult> {
        let result = self.data_manager.bulk_upsert(collection, vectors.clone()).await?;
        if let Some(ref p) = self.persistence {
            for (id, vec, meta) in &vectors {
                let _ = p.save_vector(collection, id, vec, Some(meta)).await;
            }
        }
        Ok(result)
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

#[cfg(test)]
mod benchmarks {
    use super::*;
    use std::time::Instant;

    fn bench_config() -> DbConfig {
        let dir = std::env::temp_dir().join(format!("coretex_bench_{}", rand_id()));
        DbConfig::new(dir.to_str().unwrap())
    }

    fn rand_id() -> u64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64
    }

    #[tokio::test]
    async fn bench_insert_throughput() {
        let config = bench_config();
        let db = CoreTexDB::with_config(config.clone());
        db.init().await.unwrap();

        let dim = 128;
        db.create_collection("bench_insert", dim, "cosine").await.unwrap();
        let batch_sizes = [100, 500, 1000];
        let mut results = Vec::new();
        let mut offset = 0;

        for &n in &batch_sizes {
            let vectors: Vec<(String, Vec<f32>, serde_json::Value)> = (0..n)
                .map(|i| {
                    let v: Vec<f32> = (0..dim).map(|j| ((offset + i) * dim + j) as f32).collect();
                    (format!("v{}", offset + i), v, serde_json::json!({}))
                })
                .collect();
            offset += n;

            let start = Instant::now();
            db.insert_vectors("bench_insert", vectors).await.unwrap();
            let elapsed = start.elapsed();
            let throughput = n as f64 / elapsed.as_secs_f64();
            results.push((n, elapsed, throughput));
        }

        println!("\n=== Insert Throughput Benchmark ===");
        println!("{:<10} {:>12} {:>15}", "Batch", "Time", "Vectors/sec");
        println!("{}", "-".repeat(40));
        for (n, elapsed, throughput) in &results {
            println!("{:<10} {:>11.2?} {:>13.0}/s", n, elapsed, throughput);
        }

        let count = db.get_vectors_count("bench_insert").await.unwrap();
        assert_eq!(count, 1600);

        let _ = std::fs::remove_dir_all(&config.data_dir);
    }

    #[tokio::test]
    async fn bench_search_latency() {
        let config = bench_config();
        let db = CoreTexDB::with_config(config.clone());
        db.init().await.unwrap();

        let dim = 128;
        let n = 1000;
        db.create_collection("bench_search", dim, "cosine").await.unwrap();

        // Insert test data
        let vectors: Vec<(String, Vec<f32>, serde_json::Value)> = (0..n)
            .map(|i| {
                let v: Vec<f32> = (0..dim).map(|j| ((i * dim + j) as f32).sin()).collect();
                (format!("v{}", i), v, serde_json::json!({}))
            })
            .collect();
        db.insert_vectors("bench_search", vectors).await.unwrap();

        // Benchmark search
        let query: Vec<f32> = (0..dim).map(|j| (42 * dim + j) as f32).collect();
        let k_values = [1, 5, 10, 50];
        let iterations = 100;
        let mut results = Vec::new();

        for &k in &k_values {
            let start = Instant::now();
            for _ in 0..iterations {
                let _ = db.search("bench_search", query.clone(), k, None).await.unwrap();
            }
            let elapsed = start.elapsed();
            let avg_us = elapsed.as_micros() as f64 / iterations as f64;
            results.push((k, avg_us));
        }

        println!("\n=== Search Latency Benchmark (1000 vectors, dim=128) ===");
        println!("{:<10} {:>15}", "K", "Avg Latency");
        println!("{}", "-".repeat(28));
        for (k, avg_us) in &results {
            println!("{:<10} {:>12.1}μs", k, avg_us);
        }

        // Cleanup
        let _ = std::fs::remove_dir_all(&config.data_dir);
    }

    #[tokio::test]
    async fn bench_concurrent_insert() {
        use std::sync::Arc;

        let config = bench_config();
        let db = Arc::new(CoreTexDB::with_config(config.clone()));
        db.init().await.unwrap();

        let dim = 64;
        let total = 2000;
        let num_tasks = 4;
        let per_task = total / num_tasks;
        db.create_collection("bench_concurrent", dim, "cosine").await.unwrap();

        let start = Instant::now();
        let mut handles = Vec::new();

        for t in 0..num_tasks {
            let db = db.clone();
            handles.push(tokio::spawn(async move {
                let vectors: Vec<(String, Vec<f32>, serde_json::Value)> = (0..per_task)
                    .map(|i| {
                        let idx = t * per_task + i;
                        let v: Vec<f32> = (0..dim).map(|j| ((idx * dim + j) as f32).sin()).collect();
                        (format!("v{}", idx), v, serde_json::json!({}))
                    })
                    .collect();
                db.insert_vectors("bench_concurrent", vectors).await.unwrap();
            }));
        }

        for h in handles {
            h.await.unwrap();
        }
        let elapsed = start.elapsed();
        let throughput = total as f64 / elapsed.as_secs_f64();

        println!("\n=== Concurrent Insert Benchmark ===");
        println!("Tasks: {}, Total vectors: {}, Dim: {}", num_tasks, total, dim);
        println!("Elapsed: {:.2?}", elapsed);
        println!("Throughput: {:.0} vectors/sec", throughput);

        let count = db.get_vectors_count("bench_concurrent").await.unwrap();
        assert_eq!(count, total);

        // Cleanup
        let _ = std::fs::remove_dir_all(&config.data_dir);
    }
}
