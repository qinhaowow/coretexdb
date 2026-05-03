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
pub use coretex_transaction::{TransactionManager, TransactionId, Snapshot, WriteAheadLog};
pub use coretex_edge::{EdgeDB, EdgeConfig, EdgeStats, EdgeSearchResult}; 

pub use coretex_core::{Vector, Document, CollectionSchema, IndexConfig, IndexType, CoreTexError, Result};
pub use coretex_storage::{StorageEngine, MemoryStorage};
#[cfg(feature = "rocksdb")]
pub use coretex_storage::PersistentStorage; 
pub use coretex_index::{VectorIndex, BruteForceIndex, IndexManager, SearchResult, HNSWIndex, IVFIndex, ScalarIndex}; 
pub use coretex_query::{QueryType, QueryParams, QueryResult as CoreTexQueryResult, DefaultQueryProcessor, QueryPlanner, QueryItem}; 
pub use coretex_bm25::{BM25Index, BM25Result, HybridQueryEngine, HybridSearchResult, MetadataFilter, FilterCondition}; 
pub use coretex_api::rest::{start_server, ApiConfig};
pub use coretex_api::graphql::{GraphQLExecutor, GraphQLServer, GraphQLRequest, GraphQLResponse}; 
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
pub use coretex_monitoring::{PrometheusMetrics, DatabaseMetrics, AlertManager, AlertRule, AlertCondition, AlertSeverity, Alert, GrafanaConfig, GrafanaClient};
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

pub struct CoreTexDB {
    pub storage: Arc<RwLock<Box<dyn StorageEngine>>>,
    pub index_manager: Arc<IndexManager>,
    pub collections: Arc<RwLock<HashMap<String, CollectionSchema>>>,
    pub data: Arc<RwLock<HashMap<String, HashMap<String, (Vec<f32>, serde_json::Value)>>>>,
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
        
        Self {
            storage: Arc::new(RwLock::new(storage)),
            index_manager: Arc::new(IndexManager::new()),
            collections: Arc::new(RwLock::new(HashMap::new())),
            data: Arc::new(RwLock::new(HashMap::new())),
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
            { Box::new(MemoryStorage::new()) }
        };
        
        Self {
            storage: Arc::new(RwLock::new(storage)),
            index_manager: Arc::new(IndexManager::new()),
            collections: Arc::new(RwLock::new(HashMap::new())),
            data: Arc::new(RwLock::new(HashMap::new())),
            config,
        }
    }

    pub async fn init(&self) -> Result<()> {
        if self.config.create_dirs_on_init && !self.config.memory_only {
            self.create_directories().await?;
        }
        
        let mut storage = self.storage.write().await;
        storage.init().await.map_err(|e| CoreTexError::StorageError(e.to_string()))?;
        
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
        let mut collections = self.collections.write().await;
        
        if collections.contains_key(name) {
            return Err(CoreTexError::ValidationError(format!("Collection '{}' already exists", name)));
        }

        let schema = CollectionSchema {
            name: name.to_string(),
            dimension,
            distance_metric: match metric {
                "euclidean" => coretex_core::DistanceMetric::Euclidean,
                "dotproduct" => coretex_core::DistanceMetric::DotProduct,
                "manhattan" => coretex_core::DistanceMetric::Manhattan,
                _ => coretex_core::DistanceMetric::Cosine,
            },
            indexes: vec![],
            metadata_schema: None,
        };

        collections.insert(name.to_string(), schema.clone());

        let mut data = self.data.write().await;
        data.insert(name.to_string(), HashMap::new());

        let index_name = format!("{}_hnsw", name);
        self.index_manager.create_index(&index_name, "hnsw", metric).await
            .map_err(|e| CoreTexError::IndexError(e.to_string()))?;

        Ok(())
    }

    pub async fn delete_collection(&self, name: &str) -> Result<()> {
        let mut collections = self.collections.write().await;
        
        if !collections.contains_key(name) {
            return Err(CoreTexError::CollectionNotFound(name.to_string()));
        }

        collections.remove(name);

        let mut data = self.data.write().await;
        data.remove(name);

        let index_name = format!("{}_hnsw", name);
        self.index_manager.delete_index(&index_name).await
            .map_err(|e| CoreTexError::IndexError(e.to_string()))?;

        Ok(())
    }

    pub async fn list_collections(&self) -> Result<Vec<String>> {
        let collections = self.collections.read().await;
        Ok(collections.keys().cloned().collect())
    }

    pub async fn get_collection(&self, name: &str) -> Result<CollectionSchema> {
        let collections = self.collections.read().await;
        
        collections.get(name)
            .cloned()
            .ok_or(CoreTexError::CollectionNotFound(name.to_string()))
    }

    pub async fn insert_vectors(&self, collection: &str, vectors: Vec<(String, Vec<f32>, serde_json::Value)>) -> Result<Vec<String>> {
        let collections = self.collections.read().await;
        let schema = collections.get(collection)
            .ok_or(CoreTexError::CollectionNotFound(collection.to_string()))?;
        
        for (_, vec, _) in &vectors {
            if vec.len() != schema.dimension {
                return Err(CoreTexError::DimensionMismatch { 
                    expected: schema.dimension, 
                    actual: vec.len() 
                });
            }
        }
        drop(collections);

        let mut data = self.data.write().await;
        let collection_data = data.get_mut(collection)
            .ok_or(CoreTexError::CollectionNotFound(collection.to_string()))?;

        let index_name = format!("{}_hnsw", collection);
        if let Ok(Some(index)) = self.index_manager.get_index(&index_name).await {
            for (id, vector, _) in &vectors {
                let _ = index.add(id, vector).await;
            }
        }

        let mut ids = Vec::new();
        for (id, vector, metadata) in vectors {
            collection_data.insert(id.clone(), (vector.clone(), metadata.clone()));
            ids.push(id.clone());
            
            let storage = self.storage.read().await;
            let storage_key = format!("{}:{}", collection, id);
            let _ = storage.store(&storage_key, &vector, &metadata).await;
        }

        Ok(ids)
    }

    pub async fn get_vector(&self, collection: &str, id: &str) -> Result<Option<(Vec<f32>, serde_json::Value)>> {
        let data = self.data.read().await;
        let collection_data = data.get(collection)
            .ok_or(CoreTexError::CollectionNotFound(collection.to_string()))?;
        
        Ok(collection_data.get(id).cloned())
    }

    pub async fn delete_vectors(&self, collection: &str, ids: &[String]) -> Result<usize> {
        let mut data = self.data.write().await;
        let collection_data = data.get_mut(collection)
            .ok_or(CoreTexError::CollectionNotFound(collection.to_string()))?;

        let index_name = format!("{}_hnsw", collection);
        if let Ok(Some(index)) = self.index_manager.get_index(&index_name).await {
            for id in ids {
                let _ = index.remove(id).await;
            }
        }

        let mut deleted = 0;
        for id in ids {
            if collection_data.remove(id).is_some() {
                deleted += 1;
                
                let storage = self.storage.read().await;
                let storage_key = format!("{}:{}", collection, id);
                let _ = storage.delete(&storage_key).await;
            }
        }

        Ok(deleted)
    }

    pub async fn search(&self, collection: &str, query: Vec<f32>, k: usize, filter: Option<serde_json::Value>) -> Result<Vec<SearchResult>> {
        let collections = self.collections.read().await;
        let _schema = collections.get(collection)
            .ok_or(CoreTexError::CollectionNotFound(collection.to_string()))?;
        drop(collections);

        let index_name = format!("{}_hnsw", collection);
        
        if let Ok(Some(index)) = self.index_manager.get_index(&index_name).await {
            let results = index.search(&query, k * 2).await
                .map_err(|e| CoreTexError::IndexError(e.to_string()))?;
            
            if let Some(filter_obj) = filter {
                let data = self.data.read().await;
                let collection_data = data.get(collection);
                
                let filtered: Vec<SearchResult> = results.into_iter()
                    .filter(|r| {
                        if let Some(cd) = collection_data {
                            if let Some((_, metadata)) = cd.get(&r.id) {
                                return Self::matches_filter(metadata, &filter_obj);
                            }
                        }
                        true
                    })
                    .take(k)
                    .collect();
                
                return Ok(filtered);
            }
            
            return Ok(results.into_iter().take(k).collect());
        }

        let data = self.data.read().await;
        let collection_data = data.get(collection)
            .ok_or(CoreTexError::CollectionNotFound(collection.to_string()))?;

        let mut results: Vec<SearchResult> = collection_data
            .iter()
            .map(|(id, (vec, _))| {
                let distance = Self::cosine_distance(&query, vec);
                SearchResult {
                    id: id.clone(),
                    distance,
                }
            })
            .collect();

        Self::sort_search_results(&mut results);
        Ok(results.into_iter().take(k).collect())
    }

    fn sort_search_results(results: &mut Vec<SearchResult>) {
        results.sort_by(|a, b| {
            a.distance.partial_cmp(&b.distance).unwrap_or(std::cmp::Ordering::Equal)
        });
    }

    pub async fn get_vectors_count(&self, collection: &str) -> Result<usize> {
        let data = self.data.read().await;
        let collection_data = data.get(collection)
            .ok_or(CoreTexError::CollectionNotFound(collection.to_string()))?;
        
        Ok(collection_data.len())
    }

    pub async fn update_vector(
        &self,
        collection: &str,
        id: &str,
        vector: Vec<f32>,
        metadata: Option<serde_json::Value>,
    ) -> Result<bool> {
        let collections = self.collections.read().await;
        let schema = collections.get(collection)
            .ok_or(CoreTexError::CollectionNotFound(collection.to_string()))?;

        if vector.len() != schema.dimension {
            return Err(CoreTexError::DimensionMismatch {
                expected: schema.dimension,
                actual: vector.len()
            });
        }
        drop(collections);

        let mut data = self.data.write().await;
        let collection_data = data.get_mut(collection)
            .ok_or(CoreTexError::CollectionNotFound(collection.to_string()))?;

        if !collection_data.contains_key(id) {
            return Ok(false);
        }

        let meta = metadata.unwrap_or(serde_json::json!({}));
        collection_data.insert(id.to_string(), (vector.clone(), meta));

        let index_name = format!("{}_hnsw", collection);
        if let Ok(Some(index)) = self.index_manager.get_index(&index_name).await {
            let _ = index.add(id, &vector).await;
        }

        Ok(true)
    }

    pub async fn upsert_vectors(
        &self,
        collection: &str,
        vectors: Vec<(String, Vec<f32>, serde_json::Value)>,
    ) -> Result<(Vec<String>, Vec<String>)> {
        let collections = self.collections.read().await;
        let schema = collections.get(collection)
            .ok_or(CoreTexError::CollectionNotFound(collection.to_string()))?;

        for (_, vector, _) in &vectors {
            if vector.len() != schema.dimension {
                return Err(CoreTexError::DimensionMismatch {
                    expected: schema.dimension,
                    actual: vector.len()
                });
            }
        }
        drop(collections);

        let mut inserted = Vec::new();
        let mut updated = Vec::new();

        let mut data = self.data.write().await;
        let collection_data = data.get_mut(collection)
            .ok_or(CoreTexError::CollectionNotFound(collection.to_string()))?;

        for (id, vector, metadata) in vectors {
            if collection_data.contains_key(&id) {
                collection_data.insert(id.clone(), (vector, metadata));
                updated.push(id);
            } else {
                collection_data.insert(id.clone(), (vector, metadata));
                inserted.push(id);
            }
        }

        Ok((inserted, updated))
    }

    pub async fn bulk_insert(
        &self,
        collection: &str,
        vectors: Vec<(String, Vec<f32>, serde_json::Value)>,
    ) -> Result<Vec<String>> {
        let collections = self.collections.read().await;
        let schema = collections.get(collection)
            .ok_or(CoreTexError::CollectionNotFound(collection.to_string()))?;

        for (_, vector, _) in &vectors {
            if vector.len() != schema.dimension {
                return Err(CoreTexError::DimensionMismatch {
                    expected: schema.dimension,
                    actual: vector.len()
                });
            }
        }
        drop(collections);

        let mut data = self.data.write().await;
        let collection_data = data.get_mut(collection)
            .ok_or(CoreTexError::CollectionNotFound(collection.to_string()))?;

        let mut ids = Vec::new();
        for (id, vector, metadata) in vectors {
            collection_data.insert(id.clone(), (vector, metadata));
            ids.push(id.clone());
        }

        Ok(ids)
    }

    pub async fn bulk_update(
        &self,
        collection: &str,
        vectors: Vec<(String, Vec<f32>, serde_json::Value)>,
    ) -> Result<Vec<String>> {
        let collections = self.collections.read().await;
        let schema = collections.get(collection)
            .ok_or(CoreTexError::CollectionNotFound(collection.to_string()))?;

        for (_, vector, _) in &vectors {
            if vector.len() != schema.dimension {
                return Err(CoreTexError::DimensionMismatch {
                    expected: schema.dimension,
                    actual: vector.len()
                });
            }
        }
        drop(collections);

        let mut data = self.data.write().await;
        let collection_data = data.get_mut(collection)
            .ok_or(CoreTexError::CollectionNotFound(collection.to_string()))?;

        let mut updated_ids = Vec::new();
        for (id, vector, metadata) in vectors {
            if collection_data.contains_key(&id) {
                collection_data.insert(id.clone(), (vector, metadata));
                updated_ids.push(id);
            }
        }

        Ok(updated_ids)
    }

    pub async fn bulk_delete(
        &self,
        collection: &str,
        ids: Vec<String>,
    ) -> Result<Vec<String>> {
        let mut deleted_ids = Vec::new();

        let mut data = self.data.write().await;
        let collection_data = data.get_mut(collection)
            .ok_or(CoreTexError::CollectionNotFound(collection.to_string()))?;

        for id in &ids {
            if collection_data.remove(id).is_some() {
                deleted_ids.push(id.clone());
            }
        }

        Ok(deleted_ids)
    }

    pub async fn bulk_upsert(
        &self,
        collection: &str,
        vectors: Vec<(String, Vec<f32>, serde_json::Value)>,
    ) -> Result<BulkResult> {
        let collections = self.collections.read().await;
        let schema = collections.get(collection)
            .ok_or(CoreTexError::CollectionNotFound(collection.to_string()))?;

        for (_, vector, _) in &vectors {
            if vector.len() != schema.dimension {
                return Err(CoreTexError::DimensionMismatch {
                    expected: schema.dimension,
                    actual: vector.len(),
                });
            }
        }
        drop(collections);

        let mut inserted = Vec::new();
        let mut updated = Vec::new();

        let mut data = self.data.write().await;
        let collection_data = data.get_mut(collection)
            .ok_or(CoreTexError::CollectionNotFound(collection.to_string()))?;

        for (id, vector, metadata) in vectors {
            if collection_data.contains_key(&id) {
                collection_data.insert(id.clone(), (vector, metadata));
                updated.push(id);
            } else {
                collection_data.insert(id.clone(), (vector, metadata));
                inserted.push(id);
            }
        }

        Ok(BulkResult {
            inserted,
            updated,
        })
    }

    fn matches_filter(metadata: &serde_json::Value, filter: &serde_json::Value) -> bool {
        if let (Some(metadata_obj), Some(filter_obj)) = (
            metadata.as_object(),
            filter.as_object()
        ) {
            for (key, value) in filter_obj {
                if let Some(meta_val) = metadata_obj.get(key) {
                    if meta_val != value {
                        return false;
                    }
                } else {
                    return false;
                }
            }
        }
        true
    }

    fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
        if a.len() != b.len() || a.is_empty() {
            return f32::MAX;
        }

        let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
        let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
        let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();

        if norm_a == 0.0 || norm_b == 0.0 {
            return 1.0;
        }

        1.0 - (dot / (norm_a * norm_b))
    }
}

impl Default for CoreTexDB {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BulkResult {
    pub inserted: Vec<String>,
    pub updated: Vec<String>,
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
