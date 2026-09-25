use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use tokio::sync::RwLock;
use serde::{Deserialize, Serialize};

use crate::coretex_core::{CollectionSchema, CoreTexError, IndexConfig, IndexType, Result, DistanceMetric};
use crate::coretex_storage::StorageEngine;
use crate::coretex_index::{IndexManager, SearchResult};
use crate::coretex_transaction::{TransactionManager, TransactionId, IsolationLevel, TransactionError};
use crate::coretex_lakehouse::VectorLakehouse;
use crate::coretex_utils::wal::{WriteAheadLog, WalEntryType};

pub mod storage_adapter;
pub use storage_adapter::{UnifiedStorageAdapter, AdapterError, ConsistencyLevel, AdapterStats};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorRecord {
    pub vector: Vec<f32>,
    pub metadata: serde_json::Value,
}

/// Canonical lowercase name of a [`DistanceMetric`]. Matches the strings
/// accepted by the CLI and the REST API, and by the index constructors.
pub(crate) fn metric_name(metric: &DistanceMetric) -> &'static str {
    match metric {
        DistanceMetric::Cosine => "cosine",
        DistanceMetric::Euclidean => "euclidean",
        DistanceMetric::DotProduct => "dotproduct",
        DistanceMetric::Manhattan => "manhattan",
    }
}

fn parse_metric(name: &str) -> DistanceMetric {
    match name {
        "euclidean" => DistanceMetric::Euclidean,
        "dotproduct" => DistanceMetric::DotProduct,
        "manhattan" => DistanceMetric::Manhattan,
        _ => DistanceMetric::Cosine,
    }
}

/// The single index name for a collection.
fn index_name_for(collection: &str) -> String {
    format!("{}_index", collection)
}

/// Stable, filesystem-safe index file name for a collection. A short hash of
/// the original name is appended so distinct collections can never collide
/// (e.g. `a/b` and `a_b` both sanitise to `a_b`).
fn index_file_name(collection: &str) -> String {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    collection.hash(&mut hasher);
    let safe: String = collection
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .take(64)
        .collect();
    format!("{}-{:016x}.json", safe, hasher.finish())
}

/// The default index is exact, so out-of-the-box results are always correct.
/// The approximate indexes (`hnsw`, `ivf`) must be requested explicitly, and
/// are documented as approximate.
///
/// This is the single source of truth: the CLI, the REST API and the library
/// all resolve their default from here, so none of them can drift into
/// silently defaulting to an approximate index.
pub(crate) const DEFAULT_INDEX_TYPE: &str = "brute_force";

/// Map a user-facing index name onto the index type and the name
/// [`IndexManager::create_index`] expects.
///
/// Anything unrecognised becomes the exact index. Falling back to an
/// approximate index would silently trade away result quality, which is the
/// worse failure for a database.
fn parse_index_type(raw: &str) -> (IndexType, &'static str) {
    match raw {
        "hnsw" => (IndexType::HNSW, "hnsw"),
        "ivf" => (IndexType::IVF, "ivf"),
        "scalar" => (IndexType::Scalar, "scalar"),
        _ => (IndexType::BruteForce, "brute_force"),
    }
}

/// Reverse of [`parse_index_type`], for persisting the choice in the manifest.
pub(crate) fn index_type_name(index_type: &IndexType) -> &'static str {
    match index_type {
        IndexType::BruteForce => "brute_force",
        IndexType::HNSW => "hnsw",
        IndexType::IVF => "ivf",
        IndexType::Scalar => "scalar",
    }
}

#[derive(Clone)]
pub struct DataManager {
    collections: Arc<RwLock<HashMap<String, CollectionSchema>>>,
    data: Arc<RwLock<HashMap<String, HashMap<String, VectorRecord>>>>,
    index_manager: Arc<IndexManager>,
    storage: Arc<RwLock<Box<dyn StorageEngine>>>,
    unified_adapter: Option<Arc<UnifiedStorageAdapter>>,
    transaction_manager: Arc<TransactionManager>,
    lakehouse: Option<Arc<VectorLakehouse>>,
    wal: OnceLock<Arc<WriteAheadLog>>,
    /// Directory holding per-collection index files (`<data>/indexes/vector`).
    /// `None` disables index persistence (tests, memory-only configs).
    indexes_dir: Option<std::path::PathBuf>,
}

impl DataManager {
    pub fn new(
        storage: Arc<RwLock<Box<dyn StorageEngine>>>,
        index_manager: Arc<IndexManager>,
    ) -> Self {
        Self {
            collections: Arc::new(RwLock::new(HashMap::new())),
            data: Arc::new(RwLock::new(HashMap::new())),
            index_manager,
            storage,
            unified_adapter: None,
            transaction_manager: Arc::new(TransactionManager::new()),
            lakehouse: None,
            wal: OnceLock::new(),
            indexes_dir: None,
        }
    }

    pub fn with_transaction_manager(
        storage: Arc<RwLock<Box<dyn StorageEngine>>>,
        index_manager: Arc<IndexManager>,
        transaction_manager: Arc<TransactionManager>,
    ) -> Self {
        Self {
            collections: Arc::new(RwLock::new(HashMap::new())),
            data: Arc::new(RwLock::new(HashMap::new())),
            index_manager,
            storage,
            unified_adapter: None,
            transaction_manager,
            lakehouse: None,
            wal: OnceLock::new(),
            indexes_dir: None,
        }
    }

    pub fn with_collections(
        storage: Arc<RwLock<Box<dyn StorageEngine>>>,
        index_manager: Arc<IndexManager>,
        collections: HashMap<String, CollectionSchema>,
        data: HashMap<String, HashMap<String, VectorRecord>>,
    ) -> Self {
        Self {
            collections: Arc::new(RwLock::new(collections)),
            data: Arc::new(RwLock::new(data)),
            index_manager,
            storage,
            unified_adapter: None,
            transaction_manager: Arc::new(TransactionManager::new()),
            lakehouse: None,
            wal: OnceLock::new(),
            indexes_dir: None,
        }
    }

    /// 完整配置：注入统一存储适配器和 Lakehouse
    pub fn with_adapters(
        storage: Arc<RwLock<Box<dyn StorageEngine>>>,
        index_manager: Arc<IndexManager>,
        transaction_manager: Arc<TransactionManager>,
        unified_adapter: Option<Arc<UnifiedStorageAdapter>>,
        lakehouse: Option<Arc<VectorLakehouse>>,
    ) -> Self {
        Self {
            collections: Arc::new(RwLock::new(HashMap::new())),
            data: Arc::new(RwLock::new(HashMap::new())),
            index_manager,
            storage,
            unified_adapter,
            transaction_manager,
            lakehouse,
            wal: OnceLock::new(),
            indexes_dir: None,
        }
    }

    /// 注入 Lakehouse（运行时挂载冷热分层）
    pub async fn attach_lakehouse(&mut self, lakehouse: Arc<VectorLakehouse>) {
        self.lakehouse = Some(lakehouse);
    }

    /// 注入统一存储适配器
    pub async fn attach_unified_adapter(&mut self, adapter: Arc<UnifiedStorageAdapter>) {
        self.unified_adapter = Some(adapter);
    }

    /// Inject a WAL for durability. All subsequent writes will be logged
    /// to the WAL before being applied to the storage engine.
    pub fn with_wal(self, wal: Arc<WriteAheadLog>) -> Self {
        let _ = self.wal.set(wal);
        self
    }

    /// Set where persisted index files are read/written. Enables load-on-init
    /// and `save_indexes`, letting an expensive ANN index survive a restart
    /// instead of being rebuilt from storage every time.
    pub fn with_indexes_dir(mut self, dir: std::path::PathBuf) -> Self {
        self.indexes_dir = Some(dir);
        self
    }

    /// Set the WAL after construction (for late binding in CoreTexDB::init).
    /// Uses OnceLock — can only be called once.
    pub fn set_wal(&self, wal: Arc<WriteAheadLog>) -> Result<()> {
        self.wal.set(wal).map_err(|_| CoreTexError::Other("WAL already set".to_string()))
    }

    /// Check if WAL is enabled.
    pub fn has_wal(&self) -> bool {
        self.wal.get().is_some()
    }

    /// Log an operation to the WAL if configured. Returns the WAL sequence
    /// number, or 0 if WAL is not enabled.
    async fn wal_log(
        &self,
        entry_type: WalEntryType,
        collection: &str,
        key: &str,
        vector: &[f32],
        metadata: &serde_json::Value,
    ) -> Result<u64> {
        if let Some(wal) = self.wal.get() {
            let data = serde_json::json!({
                "vector": vector,
                "metadata": metadata,
            });
            wal.log_operation(entry_type, collection, key, data)
                .await
                .map_err(CoreTexError::Io)
        } else {
            Ok(0)
        }
    }

    /// Recover database state by replaying the WAL into the storage engine.
    /// Must be called after initialization and before accepting queries.
    pub async fn recover_from_wal(&self) -> Result<crate::coretex_utils::wal::ReplayResult> {
        let wal = match self.wal.get() {
            Some(w) => w.clone(),
            None => {
                return Ok(crate::coretex_utils::wal::ReplayResult {
                    total_entries: 0,
                    replayed: 0,
                    skipped: 0,
                    corrupted: 0,
                });
            }
        };

        use crate::coretex_utils::wal::RecoveryManager;
        let recovery = RecoveryManager::new(wal);

        // Collect entries and replay
        let entries = recovery
            .recover_storage_entries()
            .await
            .map_err(CoreTexError::Io)?;

        let mut replayed = 0u64;
        let mut skipped = 0u64;

        // Collapse the WAL to the *final* operation per `collection:id`. The
        // WAL is append-only, so replaying every entry in order and applying it
        // one by one made the outcome depend on coincidences (e.g. Insert then
        // Delete then a failed Delete left the row in memory). Last-write-wins
        // is the correct semantics for a replay.
        let mut order: Vec<String> = Vec::new();
        let mut final_ops: std::collections::HashMap<
            String,
            (WalEntryType, String, String, Vec<f32>, serde_json::Value),
        > = std::collections::HashMap::new();
        for (entry_type, collection, key, vector, metadata) in &entries {
            if !matches!(
                entry_type,
                WalEntryType::Insert | WalEntryType::Update | WalEntryType::Delete
            ) {
                continue;
            }
            let storage_key = format!("{}:{}", collection, key);
            if final_ops
                .insert(
                    storage_key.clone(),
                    (
                        *entry_type,
                        collection.clone(),
                        key.clone(),
                        vector.clone(),
                        metadata.clone(),
                    ),
                )
                .is_none()
            {
                order.push(storage_key);
            }
        }

        for storage_key in &order {
            let (entry_type, collection, key, vector, metadata) =
                match final_ops.get(storage_key) {
                    Some(v) => v,
                    None => continue,
                };
            match entry_type {
                WalEntryType::Insert | WalEntryType::Update => {
                    {
                        let storage = self.storage.read().await;
                        if let Err(e) = storage.store(storage_key, vector, metadata).await {
                            log::warn!("WAL replay store failed for {}: {}", storage_key, e);
                            skipped += 1;
                            continue;
                        }
                    }

                    // Apply to the in-memory map and index so the replayed row
                    // is visible without a restart, and so the next
                    // `restore_from_storage` does not overwrite it. If the
                    // manifest was lost, rebuild the collection from the WAL
                    // instead of dropping the row.
                    if !self.collection_exists(collection).await {
                        let _ = self
                            .create_collection_with_index(
                                collection,
                                vector.len(),
                                "cosine",
                                DEFAULT_INDEX_TYPE,
                            )
                            .await;
                    }
                    let index_name = index_name_for(collection);
                    if let Ok(Some(index)) = self.index_manager.get_index(&index_name).await {
                        let _ = index.add(key, vector).await;
                    }
                    let mut data = self.data.write().await;
                    if let Some(collection_data) = data.get_mut(collection.as_str()) {
                        collection_data.insert(
                            key.clone(),
                            VectorRecord {
                                vector: vector.clone(),
                                metadata: metadata.clone(),
                            },
                        );
                    }
                    replayed += 1;
                }
                WalEntryType::Delete => {
                    {
                        let storage = self.storage.read().await;
                        if let Err(e) = storage.delete(storage_key).await {
                            // A delete of something absent is not a failure.
                            log::warn!("WAL replay delete failed for {}: {}", storage_key, e);
                            skipped += 1;
                            continue;
                        }
                    }
                    // Always drop from memory/index after a successful delete,
                    // so a prior Insert of the same key in this replay pass
                    // cannot survive.
                    let index_name = index_name_for(collection);
                    if let Ok(Some(index)) = self.index_manager.get_index(&index_name).await {
                        let _ = index.remove(key).await;
                    }
                    let mut data = self.data.write().await;
                    if let Some(collection_data) = data.get_mut(collection.as_str()) {
                        collection_data.remove(key);
                    }
                    replayed += 1;
                }
                _ => {}
            }
        }

        Ok(crate::coretex_utils::wal::ReplayResult {
            total_entries: entries.len() as u64,
            replayed,
            skipped,
            corrupted: 0,
        })
    }

    pub fn unified_adapter_ref(&self) -> Option<&Arc<UnifiedStorageAdapter>> {
        self.unified_adapter.as_ref()
    }

    pub fn lakehouse_ref(&self) -> Option<&Arc<VectorLakehouse>> {
        self.lakehouse.as_ref()
    }

    pub fn transaction_manager_ref(&self) -> &Arc<TransactionManager> {
        &self.transaction_manager
    }

    pub fn collections_ref(&self) -> &Arc<RwLock<HashMap<String, CollectionSchema>>> {
        &self.collections
    }

    pub fn data_ref(&self) -> &Arc<RwLock<HashMap<String, HashMap<String, VectorRecord>>>> {
        &self.data
    }

    pub fn index_manager_ref(&self) -> &Arc<IndexManager> {
        &self.index_manager
    }

    pub fn storage_ref(&self) -> &Arc<RwLock<Box<dyn StorageEngine>>> {
        &self.storage
    }

    /// Snapshot of every collection schema, ordered by name for stable output.
    pub async fn schemas(&self) -> Vec<CollectionSchema> {
        let collections = self.collections.read().await;
        let mut schemas: Vec<CollectionSchema> = collections.values().cloned().collect();
        schemas.sort_by(|a, b| a.name.cmp(&b.name));
        schemas
    }

    /// Rebuild in-memory state from the durable log.
    ///
    /// Each schema in `schemas` is recreated along with its index, then every
    /// persisted vector is streamed back into both the in-memory map and the
    /// index. Returns the number of vectors restored.
    ///
    /// Vector keys are matched against the known collection names rather than
    /// split on `:`, so a collection name or vector id containing a colon stays
    /// unambiguous. Keys belonging to no known collection are orphans of a
    /// deleted collection and are skipped.
    pub async fn restore_from_storage(&self, schemas: &[CollectionSchema]) -> Result<usize> {
        for schema in schemas {
            if self.collection_exists(&schema.name).await {
                continue;
            }
            self.create_collection_with_index(
                &schema.name,
                schema.dimension,
                metric_name(&schema.distance_metric),
                schema
                    .indexes
                    .first()
                    .map(|i| index_type_name(&i.index_type))
                    .unwrap_or(DEFAULT_INDEX_TYPE),
            )
            .await?;
        }

        let keys = {
            let storage = self.storage.read().await;
            storage.list().await?
        };

        // Phase 1: rebuild the in-memory map from the durable log. The index is
        // deliberately NOT touched here; phase 2 decides load-vs-rebuild.
        let mut restored = 0usize;
        for key in keys {
            let Some(schema) = schemas
                .iter()
                .filter(|s| {
                    key.len() > s.name.len()
                        && key.starts_with(&s.name)
                        && key.as_bytes()[s.name.len()] == b':'
                })
                .max_by_key(|s| s.name.len())
            else {
                continue;
            };
            let collection = schema.name.as_str();
            let id = &key[collection.len() + 1..];

            let record = {
                let storage = self.storage.read().await;
                storage.retrieve(&key).await?
            };
            let Some((vector, metadata)) = record else {
                continue;
            };

            let mut data = self.data.write().await;
            if let Some(collection_data) = data.get_mut(collection) {
                collection_data.insert(id.to_string(), VectorRecord { vector, metadata });
                restored += 1;
            }
        }

        // Phase 2: for each collection, install a persisted index whose checksum
        // matches the data just loaded; otherwise rebuild it from the map. An
        // index file that is stale (data changed since it was written) fails the
        // checksum and is transparently ignored.
        for schema in schemas {
            let collection = schema.name.as_str();
            let pairs: Vec<(String, Vec<f32>)> = {
                let data = self.data.read().await;
                match data.get(collection) {
                    Some(collection_data) => collection_data
                        .iter()
                        .map(|(id, record)| (id.clone(), record.vector.clone()))
                        .collect(),
                    None => continue,
                }
            };

            let index_type = schema
                .indexes
                .first()
                .map(|i| index_type_name(&i.index_type))
                .unwrap_or(DEFAULT_INDEX_TYPE);
            let metric = metric_name(&schema.distance_metric);
            let index_name = index_name_for(collection);

            if let Some(ref dir) = self.indexes_dir {
                let path = dir.join(index_file_name(collection));
                let checksum = crate::coretex_index::vectors_checksum(&pairs);
                if self
                    .index_manager
                    .load_index(&index_name, index_type, metric, &path, &checksum)
                    .await?
                {
                    continue;
                }
            }

            if let Ok(Some(index)) = self.index_manager.get_index(&index_name).await {
                for (id, vector) in &pairs {
                    let _ = index.add(id, vector).await;
                }
            }
        }

        Ok(restored)
    }

    /// Persist every collection's index into `indexes_dir`. Returns how many
    /// index files were written; index types without on-disk support
    /// (`brute_force`, `scalar`) are skipped.
    pub async fn save_indexes(&self) -> Result<usize> {
        let Some(ref dir) = self.indexes_dir else {
            return Ok(0);
        };

        let mut saved = 0usize;
        for collection in self.get_collection_names().await {
            let pairs: Vec<(String, Vec<f32>)> = {
                let data = self.data.read().await;
                match data.get(&collection) {
                    Some(collection_data) => collection_data
                        .iter()
                        .map(|(id, record)| (id.clone(), record.vector.clone()))
                        .collect(),
                    None => continue,
                }
            };

            let checksum = crate::coretex_index::vectors_checksum(&pairs);
            let index_name = index_name_for(&collection);
            let path = dir.join(index_file_name(&collection));
            if self
                .index_manager
                .persist_index(&index_name, &path, &checksum)
                .await?
            {
                saved += 1;
            }
        }
        Ok(saved)
    }

    /// Create a collection backed by the default index, which is exact.
    pub async fn create_collection(&self, name: &str, dimension: usize, metric: &str) -> Result<()> {
        self.create_collection_with_index(name, dimension, metric, DEFAULT_INDEX_TYPE)
            .await
    }

    /// Create a collection together with its index.
    ///
    /// `index_type` accepts `brute_force`/`brute` (exact), `hnsw`, `ivf`, or
    /// `scalar`. Anything unrecognised falls back to the exact index, so a typo
    /// can never silently trade accuracy for speed. The choice is recorded in
    /// the schema, so restarting the database rebuilds the same index.
    pub async fn create_collection_with_index(
        &self,
        name: &str,
        dimension: usize,
        metric: &str,
        index_type: &str,
    ) -> Result<()> {
        let (kind, engine_name) = parse_index_type(index_type);
        let distance_metric = parse_metric(metric);
        let index_name = index_name_for(name);

        {
            let mut collections = self.collections.write().await;
            if collections.contains_key(name) {
                return Err(CoreTexError::ValidationError(format!(
                    "Collection '{}' already exists",
                    name
                )));
            }

            collections.insert(
                name.to_string(),
                CollectionSchema {
                    name: name.to_string(),
                    dimension,
                    distance_metric: distance_metric.clone(),
                    indexes: vec![IndexConfig {
                        name: index_name.clone(),
                        index_type: kind,
                        parameters: HashMap::new(),
                    }],
                    metadata_schema: None,
                },
            );
        }

        {
            let mut data = self.data.write().await;
            data.insert(name.to_string(), HashMap::new());
        }

        self.index_manager
            .create_index(&index_name, engine_name, metric_name(&distance_metric))
            .await
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
        drop(data);

        let index_name = index_name_for(name);
        self.index_manager.delete_index(&index_name).await
            .map_err(|e| CoreTexError::IndexError(e.to_string()))?;

        // Drop the persisted vectors as well. Leaving them behind would
        // resurrect them if a collection of the same name were recreated.
        let prefix = format!("{}:", name);
        let storage = self.storage.read().await;
        let keys: Vec<String> = storage
            .list()
            .await?
            .into_iter()
            .filter(|key| key.starts_with(&prefix))
            .collect();
        for key in keys {
            storage.delete(&key).await?;
        }

        Ok(())
    }

    pub async fn list_collections(&self) -> Result<Vec<String>> {
        let collections = self.collections.read().await;
        Ok(collections.keys().cloned().collect())
    }

    /// 重命名 collection：复制 schema + 数据 + 索引，然后删除旧 collection
    pub async fn rename_collection(&self, old_name: &str, new_name: &str) -> Result<()> {
        if old_name == new_name {
            return Ok(());
        }

        // 检查新名称是否已存在
        {
            let collections = self.collections.read().await;
            if collections.contains_key(new_name) {
                return Err(CoreTexError::ValidationError(format!(
                    "Collection '{}' already exists", new_name
                )));
            }
        }

        // 获取旧 schema
        let schema = {
            let collections = self.collections.read().await;
            collections.get(old_name)
                .cloned()
                .ok_or_else(|| CoreTexError::CollectionNotFound(old_name.to_string()))?
        };

        // 移出旧数据
        let old_data = {
            let mut data = self.data.write().await;
            data.remove(old_name)
                .unwrap_or_default()
        };

        // 删除旧索引
        let old_index_name = index_name_for(old_name);
        let _ = self.index_manager.delete_index(&old_index_name).await;

        // 创建新索引
        let new_index_name = index_name_for(new_name);
        self.index_manager.create_index(&new_index_name, "hnsw", metric_name(&schema.distance_metric)).await
            .map_err(|e| CoreTexError::IndexError(e.to_string()))?;

        // 将数据写入新索引
        if let Ok(Some(index)) = self.index_manager.get_index(&new_index_name).await {
            for (id, record) in &old_data {
                let _ = index.add(id, &record.vector).await;
            }
        }

        // 插入新 collection schema + 数据
        {
            let mut collections = self.collections.write().await;
            collections.insert(new_name.to_string(), CollectionSchema {
                name: new_name.to_string(),
                ..schema
            });
        }
        {
            let mut data = self.data.write().await;
            data.insert(new_name.to_string(), old_data);
        }

        // 删除旧 collection 条目
        {
            let mut collections = self.collections.write().await;
            collections.remove(old_name);
        }

        // 迁移持久化存储中的 key
        {
            let storage = self.storage.read().await;
            let keys_to_migrate: Vec<String> = {
                let data = self.data.read().await;
                if let Some(new_data) = data.get(new_name) {
                    new_data.keys()
                        .map(|id| format!("{}:{}", old_name, id))
                        .collect()
                } else {
                    vec![]
                }
            };
            for old_key in keys_to_migrate {
                let new_key = old_key.replacen(old_name, new_name, 1);
                if let Ok(Some((vector, metadata))) = storage.retrieve(&old_key).await {
                    let _ = storage.store(&new_key, &vector, &metadata).await;
                    let _ = storage.delete(&old_key).await;
                }
            }
        }

        Ok(())
    }

    pub async fn get_collection(&self, name: &str) -> Result<CollectionSchema> {
        let collections = self.collections.read().await;
        collections.get(name)
            .cloned()
            .ok_or(CoreTexError::CollectionNotFound(name.to_string()))
    }

    pub async fn collection_exists(&self, name: &str) -> bool {
        self.collections.read().await.contains_key(name)
    }

    pub async fn get_collection_dimension(&self, name: &str) -> Result<usize> {
        let collections = self.collections.read().await;
        collections.get(name)
            .map(|s| s.dimension)
            .ok_or(CoreTexError::CollectionNotFound(name.to_string()))
    }

    pub async fn insert_vectors(
        &self,
        collection: &str,
        vectors: Vec<(String, Vec<f32>, serde_json::Value)>,
    ) -> Result<Vec<String>> {
        let dimension = self.get_collection_dimension(collection).await?;

        for (_, vec, _) in &vectors {
            if vec.len() != dimension {
                return Err(CoreTexError::DimensionMismatch {
                    expected: dimension,
                    actual: vec.len(),
                });
            }
        }

        let mut data = self.data.write().await;
        let collection_data = data.get_mut(collection)
            .ok_or(CoreTexError::CollectionNotFound(collection.to_string()))?;

        let index_name = index_name_for(collection);

        let mut ids = Vec::new();
        for (id, vector, metadata) in vectors {
            // Durable order: WAL → storage → memory → index. Any durable
            // failure aborts the batch instead of pretending success.
            self.wal_log(
                WalEntryType::Insert,
                collection,
                &id,
                &vector,
                &metadata,
            ).await?;

            let storage_key = format!("{}:{}", collection, id);
            {
                let storage = self.storage.read().await;
                storage.store(&storage_key, &vector, &metadata).await?;
            }

            let record = VectorRecord {
                vector: vector.clone(),
                metadata: metadata.clone(),
            };
            collection_data.insert(id.clone(), record);
            ids.push(id.clone());

            if let Ok(Some(index)) = self.index_manager.get_index(&index_name).await {
                let _ = index.add(&id, &vector).await;
            }
        }

        Ok(ids)
    }

    pub async fn get_vector(
        &self,
        collection: &str,
        id: &str,
    ) -> Result<Option<VectorRecord>> {
        let data = self.data.read().await;
        let collection_data = data.get(collection)
            .ok_or(CoreTexError::CollectionNotFound(collection.to_string()))?;
        Ok(collection_data.get(id).cloned())
    }

    pub async fn delete_vectors(&self, collection: &str, ids: &[String]) -> Result<usize> {
        let mut data = self.data.write().await;
        let collection_data = data.get_mut(collection)
            .ok_or(CoreTexError::CollectionNotFound(collection.to_string()))?;

        let mut deleted = 0;
        for id in ids {
            if !collection_data.contains_key(id) {
                continue;
            }

            // Durable order: WAL → storage → memory → index.
            self.wal_log(
                WalEntryType::Delete,
                collection,
                id,
                &[],
                &serde_json::json!({}),
            ).await?;

            let storage_key = format!("{}:{}", collection, id);
            {
                let storage = self.storage.read().await;
                storage.delete(&storage_key).await?;
            }

            collection_data.remove(id);

            let index_name = index_name_for(collection);
            if let Ok(Some(index)) = self.index_manager.get_index(&index_name).await {
                let _ = index.remove(id).await;
            }

            deleted += 1;
        }

        Ok(deleted)
    }

    pub async fn search(
        &self,
        collection: &str,
        query: Vec<f32>,
        k: usize,
        filter: Option<serde_json::Value>,
    ) -> Result<Vec<SearchResult>> {
        let schema = self.get_collection(collection).await?;

        // A filtered query is answered by an exact scan. Asking the index for
        // only its top `k` candidates and then filtering would return fewer
        // than `k` matches even when more exist, because a selective filter can
        // reject every candidate the index returned.
        if let Some(filter) = filter {
            return self
                .search_scan(collection, &query, k, Some(&filter), &schema.distance_metric)
                .await;
        }

        let index_name = index_name_for(collection);
        if let Ok(Some(index)) = self.index_manager.get_index(&index_name).await {
            let results = index.search(&query, k).await
                .map_err(|e| CoreTexError::IndexError(e.to_string()))?;
            return Ok(results.into_iter().take(k).collect());
        }

        // No index for this collection: fall back to an exact scan.
        self.search_scan(collection, &query, k, None, &schema.distance_metric)
            .await
    }

    /// Exact k-NN scan honouring `metric` and, when given, `filter`.
    ///
    /// The filter is applied before ranking, so `k` results come back whenever
    /// `k` matching vectors exist.
    async fn search_scan(
        &self,
        collection: &str,
        query: &[f32],
        k: usize,
        filter: Option<&serde_json::Value>,
        metric: &DistanceMetric,
    ) -> Result<Vec<SearchResult>> {
        let data = self.data.read().await;
        let collection_data = data.get(collection)
            .ok_or(CoreTexError::CollectionNotFound(collection.to_string()))?;

        let mut results: Vec<SearchResult> = collection_data
            .iter()
            .filter(|(_, record)| {
                filter
                    .map(|f| Self::matches_filter(&record.metadata, f))
                    .unwrap_or(true)
            })
            .map(|(id, record)| SearchResult {
                id: id.clone(),
                distance: Self::distance(metric, query, &record.vector),
            })
            .collect();

        results.sort_by(|a, b| {
            a.distance.partial_cmp(&b.distance).unwrap_or(std::cmp::Ordering::Equal)
        });
        results.truncate(k);
        Ok(results)
    }

    /// Distance between two vectors under `metric`. Lower is always better, so
    /// every metric yields a consistent "closest first" ordering.
    ///
    /// Delegates to [`crate::coretex_index::metric_distance`] so the exact scan
    /// and the indexes can never disagree about what a metric means.
    fn distance(metric: &DistanceMetric, a: &[f32], b: &[f32]) -> f32 {
        crate::coretex_index::metric_distance(metric_name(metric), a, b)
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
        let dimension = self.get_collection_dimension(collection).await?;

        if vector.len() != dimension {
            return Err(CoreTexError::DimensionMismatch {
                expected: dimension,
                actual: vector.len(),
            });
        }

        let mut data = self.data.write().await;
        let collection_data = data.get_mut(collection)
            .ok_or(CoreTexError::CollectionNotFound(collection.to_string()))?;

        if !collection_data.contains_key(id) {
            return Ok(false);
        }

        let meta = metadata.unwrap_or(serde_json::json!({}));

        // Durable order: WAL → storage → memory → index.
        self.wal_log(
            WalEntryType::Update,
            collection,
            id,
            &vector,
            &meta,
        ).await?;

        // Without this the new vector only ever lived in memory and in a WAL
        // that is disabled by default, so an update was silently lost on
        // restart while a plain insert survived.
        let storage_key = format!("{}:{}", collection, id);
        {
            let storage = self.storage.read().await;
            storage.store(&storage_key, &vector, &meta).await?;
        }

        collection_data.insert(id.to_string(), VectorRecord {
            vector: vector.clone(),
            metadata: meta.clone(),
        });

        let index_name = index_name_for(collection);
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
        let dimension = self.get_collection_dimension(collection).await?;

        for (_, vector, _) in &vectors {
            if vector.len() != dimension {
                return Err(CoreTexError::DimensionMismatch {
                    expected: dimension,
                    actual: vector.len(),
                });
            }
        }

        let result = self.bulk_upsert(collection, vectors).await?;
        Ok((result.inserted, result.updated))
    }

    /// Insert-or-replace a batch of vectors.
    ///
    /// Delegates to [`Self::insert_vectors`] so a bulk write reaches the index,
    /// the WAL and `FileStorage` exactly like a single insert. It used to write
    /// straight into the in-memory map, which left search stale and lost the
    /// batch on restart.
    pub async fn bulk_insert(
        &self,
        collection: &str,
        vectors: Vec<(String, Vec<f32>, serde_json::Value)>,
    ) -> Result<Vec<String>> {
        self.insert_vectors(collection, vectors).await
    }

    /// Update the vectors that already exist, skipping the rest.
    ///
    /// Delegates to [`Self::update_vector`] so the index, WAL and `FileStorage`
    /// stay in step with the in-memory map.
    pub async fn bulk_update(
        &self,
        collection: &str,
        vectors: Vec<(String, Vec<f32>, serde_json::Value)>,
    ) -> Result<Vec<String>> {
        let mut updated_ids = Vec::new();
        for (id, vector, metadata) in vectors {
            if self
                .update_vector(collection, &id, vector, Some(metadata))
                .await?
            {
                updated_ids.push(id);
            }
        }

        Ok(updated_ids)
    }

    /// Delete the ids that are present, reporting exactly which ones went away.
    ///
    /// Delegates to [`Self::delete_vectors`]. Resolving the present ids first is
    /// what lets this keep returning a list rather than a bare count.
    pub async fn bulk_delete(
        &self,
        collection: &str,
        ids: Vec<String>,
    ) -> Result<Vec<String>> {
        let mut deleted_ids = Vec::new();
        let mut seen = std::collections::HashSet::new();

        {
            let data = self.data.read().await;
            let collection_data = data
                .get(collection)
                .ok_or(CoreTexError::CollectionNotFound(collection.to_string()))?;
            for id in &ids {
                if collection_data.contains_key(id) && seen.insert(id.clone()) {
                    deleted_ids.push(id.clone());
                }
            }
        }

        if deleted_ids.is_empty() {
            return Ok(deleted_ids);
        }

        self.delete_vectors(collection, &deleted_ids).await?;
        Ok(deleted_ids)
    }

    pub async fn bulk_upsert(
        &self,
        collection: &str,
        vectors: Vec<(String, Vec<f32>, serde_json::Value)>,
    ) -> Result<BulkResult> {
        let dimension = self.get_collection_dimension(collection).await?;

        for (_, vector, _) in &vectors {
            if vector.len() != dimension {
                return Err(CoreTexError::DimensionMismatch {
                    expected: dimension,
                    actual: vector.len(),
                });
            }
        }

        // Partition first so the caller still learns which ids were new, then
        // route each half through the single-vector paths. This is what makes an
        // upsert durable and visible to search; the old body wrote only to the
        // in-memory map.
        let mut fresh: Vec<(String, Vec<f32>, serde_json::Value)> = Vec::new();
        let mut existing: Vec<(String, Vec<f32>, serde_json::Value)> = Vec::new();
        {
            let data = self.data.read().await;
            let collection_data = data
                .get(collection)
                .ok_or(CoreTexError::CollectionNotFound(collection.to_string()))?;
            let mut claimed: std::collections::HashSet<String> = std::collections::HashSet::new();
            for entry in vectors {
                // A repeated id inside the batch behaves like the old code: the
                // first occurrence inserts, the rest update.
                if collection_data.contains_key(&entry.0) || !claimed.insert(entry.0.clone()) {
                    existing.push(entry);
                } else {
                    fresh.push(entry);
                }
            }
        }

        let inserted = self.insert_vectors(collection, fresh).await?;

        let mut updated = Vec::new();
        for (id, vector, metadata) in existing {
            if self
                .update_vector(collection, &id, vector, Some(metadata))
                .await?
            {
                updated.push(id);
            }
        }

        Ok(BulkResult { inserted, updated })
    }

    pub async fn get_all_vectors(
        &self,
        collection: &str,
    ) -> Result<Vec<(String, VectorRecord)>> {
        let data = self.data.read().await;
        let collection_data = data.get(collection)
            .ok_or(CoreTexError::CollectionNotFound(collection.to_string()))?;
        let mut result: Vec<_> = collection_data.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        result.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(result)
    }

    pub async fn get_vectors_by_ids(
        &self,
        collection: &str,
        ids: &[String],
    ) -> Result<Vec<(String, VectorRecord)>> {
        let data = self.data.read().await;
        let collection_data = data.get(collection)
            .ok_or(CoreTexError::CollectionNotFound(collection.to_string()))?;
        let mut result = Vec::new();
        for id in ids {
            if let Some(record) = collection_data.get(id) {
                result.push((id.clone(), record.clone()));
            }
        }
        Ok(result)
    }

    /// Delete every vector whose metadata matches `filter`, returning the ids
    /// that were really removed.
    ///
    /// Filtered deletion must travel the same durable path as an explicit
    /// delete ([`Self::delete_vectors`]) so every id gets a WAL entry and a
    /// storage tombstone. Clearing the in-memory map instead would look like a
    /// delete while leaving the log full of live records — a restart replays
    /// them and the "deleted" vectors come back.
    pub async fn delete_vectors_where(
        &self,
        collection: &str,
        filter: &serde_json::Value,
    ) -> Result<Vec<String>> {
        let mut ids: Vec<String> = {
            let data = self.data.read().await;
            let collection_data = data
                .get(collection)
                .ok_or(CoreTexError::CollectionNotFound(collection.to_string()))?;
            collection_data
                .iter()
                .filter(|(_, record)| Self::matches_filter(&record.metadata, filter))
                .map(|(id, _)| id.clone())
                .collect()
        };
        ids.sort();

        if ids.is_empty() {
            return Ok(Vec::new());
        }

        self.delete_vectors(collection, &ids).await?;

        // Report only what is actually gone, so the reported ids can never
        // disagree with the store.
        let data = self.data.read().await;
        let gone = match data.get(collection) {
            Some(collection_data) => ids
                .into_iter()
                .filter(|id| !collection_data.contains_key(id))
                .collect(),
            None => Vec::new(),
        };
        Ok(gone)
    }

    /// Remove every vector in `collection`, returning how many were removed.
    ///
    /// Delegates to [`Self::delete_vectors_where`] with the match-everything
    /// filter, so a clear is exactly as durable as a delete. It used to clear
    /// the in-memory map and the index while writing nothing to storage, which
    /// made every cleared vector reappear after a restart.
    pub async fn clear_collection(&self, collection: &str) -> Result<usize> {
        // An empty filter object matches every record.
        let removed = self
            .delete_vectors_where(collection, &serde_json::json!({}))
            .await?;
        Ok(removed.len())
    }

    pub async fn get_total_vector_count(&self) -> usize {
        let data = self.data.read().await;
        data.values().map(|c| c.len()).sum()
    }

    pub async fn get_collection_names(&self) -> Vec<String> {
        self.collections.read().await.keys().cloned().collect()
    }

    pub async fn set_ttl(&self, collection: &str, id: &str, ttl_secs: u64) -> Result<()> {
        let storage = self.storage.read().await;
        let storage_key = format!("{}:{}", collection, id);
        storage.set_ttl(&storage_key, ttl_secs).await
            .map_err(|e| CoreTexError::StorageError(e.to_string()))
    }

    pub async fn remove_ttl(&self, collection: &str, id: &str) -> Result<()> {
        let storage = self.storage.read().await;
        let storage_key = format!("{}:{}", collection, id);
        storage.remove_ttl(&storage_key).await
            .map_err(|e| CoreTexError::StorageError(e.to_string()))
    }

    pub async fn purge_expired(&self) -> Result<usize> {
        // Ask storage which keys have elapsed *before* purging — afterwards the
        // TTL bookkeeping is gone. `storage.list()` cannot be used for this: it
        // already hides expired keys, so a before/after diff detects nothing and
        // the in-memory map and index kept ghost vectors.
        let expired = {
            let storage = self.storage.read().await;
            storage.expired_keys().await?
        };

        let purged = {
            let storage = self.storage.read().await;
            storage.purge_expired().await
                .map_err(|e| CoreTexError::StorageError(e.to_string()))?
        };

        if !expired.is_empty() {
            // Resolve `collection:id` against the *known* collection names
            // rather than splitting on the first ':' — ids may legitimately
            // contain ':' (e.g. "ns:user:1"), which would pick the wrong
            // collection. Longest prefix wins.
            let known: Vec<String> = self.collections.read().await.keys().cloned().collect();

            let mut data = self.data.write().await;
            for storage_key in &expired {
                let Some((collection, id)) = Self::split_storage_key(storage_key, &known) else {
                    log::warn!("purge_expired: cannot resolve storage key {}", storage_key);
                    continue;
                };
                let index_name = index_name_for(collection);
                if let Ok(Some(index)) = self.index_manager.get_index(&index_name).await {
                    let _ = index.remove(id).await;
                }
                if let Some(collection_data) = data.get_mut(collection) {
                    collection_data.remove(id);
                }
            }
        }

        Ok(purged)
    }

    /// Split a storage key (`collection:id`) into its parts using the known
    /// collection names, preferring the longest matching prefix so ids that
    /// contain ':' are attributed to the right collection.
    fn split_storage_key<'a>(key: &'a str, known: &'a [String]) -> Option<(&'a str, &'a str)> {
        let mut best: Option<(&str, &str)> = None;
        for name in known {
            let prefix = format!("{}:", name);
            if let Some(id) = key.strip_prefix(prefix.as_str()) {
                match best {
                    Some((prev, _)) if prev.len() >= name.len() => {}
                    _ => best = Some((name.as_str(), id)),
                }
            }
        }
        best
    }

    pub async fn get_shard_for_key(&self, key: &str, total_shards: usize) -> usize {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % total_shards
    }

    pub async fn begin_transaction(&self, isolation_level: IsolationLevel) -> std::result::Result<TransactionId, TransactionError> {
        self.transaction_manager.begin_transaction(isolation_level).await
    }

    pub async fn commit_transaction(&self, txn_id: TransactionId) -> std::result::Result<(), TransactionError> {
        self.transaction_manager.commit(txn_id).await
    }

    pub async fn abort_transaction(&self, txn_id: TransactionId) -> std::result::Result<(), TransactionError> {
        self.transaction_manager.abort(txn_id).await
    }

    pub async fn insert_vectors_tx(
        &self,
        collection: &str,
        vectors: Vec<(String, Vec<f32>, serde_json::Value)>,
        txn_id: TransactionId,
    ) -> Result<Vec<String>> {
        let dimension = self.get_collection_dimension(collection).await?;

        for (_, vec, _) in &vectors {
            if vec.len() != dimension {
                return Err(CoreTexError::DimensionMismatch {
                    expected: dimension,
                    actual: vec.len(),
                });
            }
        }

        let mut data = self.data.write().await;
        let collection_data = data.get_mut(collection)
            .ok_or(CoreTexError::CollectionNotFound(collection.to_string()))?;

        let mut ids = Vec::new();
        for (id, vector, metadata) in vectors {
            let record = VectorRecord {
                vector: vector.clone(),
                metadata: metadata.clone(),
            };
            collection_data.insert(id.clone(), record);
            ids.push(id.clone());

            let storage = self.storage.read().await;
            let storage_key = format!("{}:{}", collection, id);
            let _ = storage.store(&storage_key, &vector, &metadata).await;
        }

        let mut wal = self.transaction_manager_ref().wal.write().await;
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        for id in &ids {
            let _ = wal.append(crate::coretex_transaction::WalEntry {
                transaction_id: txn_id,
                timestamp,
                operation: crate::coretex_transaction::WalOperation::Insert {
                    key: format!("{}:{}", collection, id),
                    value: bincode::serialize(&VectorRecord {
                        vector: vec![],
                        metadata: serde_json::json!({}),
                    }).unwrap_or_default(),
                },
                lsn: 0,
            }).map_err(|e| CoreTexError::TransactionError(e.to_string()))?;
        }
        drop(wal);

        Ok(ids)
    }

    pub async fn delete_vectors_tx(
        &self,
        collection: &str,
        ids: &[String],
        txn_id: TransactionId,
    ) -> Result<usize> {
        let mut data = self.data.write().await;
        let collection_data = data.get_mut(collection)
            .ok_or(CoreTexError::CollectionNotFound(collection.to_string()))?;

        let mut deleted = 0;
        for id in ids {
            if collection_data.remove(id).is_some() {
                deleted += 1;
                let storage = self.storage.read().await;
                let storage_key = format!("{}:{}", collection, id);
                let _ = storage.delete(&storage_key).await;
            }
        }

        let mut wal = self.transaction_manager_ref().wal.write().await;
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        for id in ids {
            let lsn = wal.entries.len() as u64;
            let _ = wal.append(crate::coretex_transaction::WalEntry {
                transaction_id: txn_id,
                timestamp,
                operation: crate::coretex_transaction::WalOperation::Delete {
                    key: format!("{}:{}", collection, id),
                    value: vec![],
                },
                lsn,
            });
        }
        drop(wal);

        Ok(deleted)
    }

    pub async fn update_vector_tx(
        &self,
        collection: &str,
        id: &str,
        vector: Vec<f32>,
        metadata: Option<serde_json::Value>,
        txn_id: TransactionId,
    ) -> Result<bool> {
        let dimension = self.get_collection_dimension(collection).await?;

        if vector.len() != dimension {
            return Err(CoreTexError::DimensionMismatch {
                expected: dimension,
                actual: vector.len(),
            });
        }

        let mut data = self.data.write().await;
        let collection_data = data.get_mut(collection)
            .ok_or(CoreTexError::CollectionNotFound(collection.to_string()))?;

        if !collection_data.contains_key(id) {
            return Ok(false);
        }

        let meta = metadata.unwrap_or(serde_json::json!({}));
        collection_data.insert(id.to_string(), VectorRecord {
            vector: vector.clone(),
            metadata: meta.clone(),
        });

        let mut wal = self.transaction_manager_ref().wal.write().await;
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let lsn = wal.entries.len() as u64;
        let _ = wal.append(crate::coretex_transaction::WalEntry {
            transaction_id: txn_id,
            timestamp,
            operation: crate::coretex_transaction::WalOperation::Update {
                key: format!("{}:{}", collection, id),
                old_value: vec![],
                new_value: bincode::serialize(&VectorRecord {
                    vector,
                    metadata: meta,
                }).unwrap_or_default(),
            },
            lsn,
        }).map_err(|e| CoreTexError::TransactionError(e.to_string()))?;
        drop(wal);

        Ok(true)
    }

    pub async fn upsert_vectors_tx(
        &self,
        collection: &str,
        vectors: Vec<(String, Vec<f32>, serde_json::Value)>,
        txn_id: TransactionId,
    ) -> Result<(Vec<String>, Vec<String>)> {
        let dimension = self.get_collection_dimension(collection).await?;

        for (_, vector, _) in &vectors {
            if vector.len() != dimension {
                return Err(CoreTexError::DimensionMismatch {
                    expected: dimension,
                    actual: vector.len(),
                });
            }
        }

        let mut inserted = Vec::new();
        let mut updated = Vec::new();

        let mut data = self.data.write().await;
        let collection_data = data.get_mut(collection)
            .ok_or(CoreTexError::CollectionNotFound(collection.to_string()))?;

        for (id, vector, metadata) in vectors {
            let record = VectorRecord {
                vector,
                metadata,
            };
            if collection_data.contains_key(&id) {
                collection_data.insert(id.clone(), record);
                updated.push(id);
            } else {
                collection_data.insert(id.clone(), record);
                inserted.push(id);
            }
        }

        let mut wal = self.transaction_manager_ref().wal.write().await;
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        for id in &inserted {
            let lsn = wal.entries.len() as u64;
            let _ = wal.append(crate::coretex_transaction::WalEntry {
                transaction_id: txn_id,
                timestamp,
                operation: crate::coretex_transaction::WalOperation::Insert {
                    key: format!("{}:{}", collection, id),
                    value: vec![],
                },
                lsn,
            });
        }
        for id in &updated {
            let lsn = wal.entries.len() as u64;
            let _ = wal.append(crate::coretex_transaction::WalEntry {
                transaction_id: txn_id,
                timestamp,
                operation: crate::coretex_transaction::WalOperation::Update {
                    key: format!("{}:{}", collection, id),
                    old_value: vec![],
                    new_value: vec![],
                },
                lsn,
            });
        }
        drop(wal);

        Ok((inserted, updated))
    }

    fn matches_filter(metadata: &serde_json::Value, filter: &serde_json::Value) -> bool {
        match filter {
            serde_json::Value::Object(obj) => {
                if obj.is_empty() {
                    return true;
                }

                if let Some(and_val) = obj.get("$and") {
                    if let Some(conditions) = and_val.as_array() {
                        return conditions.iter().all(|c| Self::matches_filter(metadata, c));
                    }
                    return true;
                }

                if let Some(or_val) = obj.get("$or") {
                    if let Some(conditions) = or_val.as_array() {
                        return conditions.iter().any(|c| Self::matches_filter(metadata, c));
                    }
                    return true;
                }

                if let Some(not_val) = obj.get("$not") {
                    return !Self::matches_filter(metadata, not_val);
                }

                for (key, value) in obj {
                    if key.starts_with('$') {
                        continue;
                    }

                    let meta_val = match metadata.get(key) {
                        Some(v) => v,
                        None => {
                            if let Some(exists_val) = value.as_object().and_then(|o| o.get("$exists")) {
                                if let Some(exists) = exists_val.as_bool() {
                                    if !exists {
                                        continue;
                                    }
                                }
                            }
                            return false;
                        }
                    };

                    if let Some(filter_obj) = value.as_object() {
                        if filter_obj.contains_key("$gt") || filter_obj.contains_key("$gte")
                            || filter_obj.contains_key("$lt") || filter_obj.contains_key("$lte")
                            || filter_obj.contains_key("$ne") || filter_obj.contains_key("$in")
                            || filter_obj.contains_key("$exists") || filter_obj.contains_key("$regex")
                        {
                            if !Self::apply_filter_conditions(meta_val, filter_obj) {
                                return false;
                            }
                            continue;
                        }
                    }

                    if meta_val != value {
                        return false;
                    }
                }
                true
            }
            serde_json::Value::Array(arr) => {
                arr.iter().any(|v| Self::matches_filter(metadata, v))
            }
            _ => true,
        }
    }

    fn apply_filter_conditions(meta_val: &serde_json::Value, conditions: &serde_json::Map<String, serde_json::Value>) -> bool {
        for (op, cond_val) in conditions {
            match op.as_str() {
                "$gt" => {
                    if let (Some(a), Some(b)) = (meta_val.as_f64(), cond_val.as_f64()) {
                        if !(a > b) { return false; }
                    } else { return false; }
                }
                "$gte" => {
                    if let (Some(a), Some(b)) = (meta_val.as_f64(), cond_val.as_f64()) {
                        if !(a >= b) { return false; }
                    } else { return false; }
                }
                "$lt" => {
                    if let (Some(a), Some(b)) = (meta_val.as_f64(), cond_val.as_f64()) {
                        if !(a < b) { return false; }
                    } else { return false; }
                }
                "$lte" => {
                    if let (Some(a), Some(b)) = (meta_val.as_f64(), cond_val.as_f64()) {
                        if !(a <= b) { return false; }
                    } else { return false; }
                }
                "$ne" => {
                    if meta_val == cond_val { return false; }
                }
                "$in" => {
                    if let Some(arr) = cond_val.as_array() {
                        if !arr.iter().any(|v| meta_val == v) { return false; }
                    } else { return false; }
                }
                "$exists" => {
                    if let Some(exists) = cond_val.as_bool() {
                        if !exists { return false; }
                    }
                }
                "$regex" => {
                    if let Some(pattern) = cond_val.as_str() {
                        if let Ok(re) = regex::Regex::new(pattern) {
                            if let Some(s) = meta_val.as_str() {
                                if !re.is_match(s) { return false; }
                            } else { return false; }
                        } else { return false; }
                    } else { return false; }
                }
                _ => {
                    if meta_val != cond_val { return false; }
                }
            }
        }
        true
    }
}

// =================== 事务感知写入（解决事务孤岛）====================

impl DataManager {
    /// 事务感知的向量插入：开始事务 → 写索引 → 写数据 → 写存储 → 写WAL → 提交
    /// 一旦任何一步失败，自动 abort 事务
    pub async fn tx_aware_insert(
        &self,
        collection: &str,
        vectors: Vec<(String, Vec<f32>, serde_json::Value)>,
    ) -> Result<Vec<String>> {
        // 1. 开启事务
        let txn_id = self.transaction_manager
            .begin_transaction(IsolationLevel::ReadCommitted)
            .await
            .map_err(|e| CoreTexError::TransactionError(e.to_string()))?;

        // 2. 校验维度
        let dimension = self.get_collection_dimension(collection).await?;
        for (_, vec, _) in &vectors {
            if vec.len() != dimension {
                let _ = self.transaction_manager.abort(txn_id).await;
                return Err(CoreTexError::DimensionMismatch {
                    expected: dimension,
                    actual: vec.len(),
                });
            }
        }

        // 3. 写数据 + 索引
        let mut data = self.data.write().await;
        let collection_data = data.get_mut(collection)
            .ok_or_else(|| {
                let _ = tokio::runtime::Handle::try_current();
                CoreTexError::CollectionNotFound(collection.to_string())
            })?;
        let index_name = index_name_for(collection);
        if let Ok(Some(index)) = self.index_manager.get_index(&index_name).await {
            for (id, vector, _) in &vectors {
                let _ = index.add(id, vector).await;
            }
        }

        let mut ids = Vec::new();
        for (id, vector, metadata) in vectors {
            collection_data.insert(id.clone(), VectorRecord {
                vector: vector.clone(),
                metadata: metadata.clone(),
            });
            ids.push(id.clone());

            // 优先用统一适配器，否则回退到原始 storage
            if let Some(adapter) = &self.unified_adapter {
                let key = format!("{}:{}", collection, id);
                if let Err(e) = adapter.upsert(&key, &vector, &metadata).await {
                    // 回滚：清理已写入
                    let _ = self.transaction_manager.abort(txn_id).await;
                    return Err(CoreTexError::StorageError(e.to_string()));
                }
            } else {
                let storage = self.storage.read().await;
                let storage_key = format!("{}:{}", collection, id);
                if let Err(e) = storage.store(&storage_key, &vector, &metadata).await {
                    let _ = self.transaction_manager.abort(txn_id).await;
                    return Err(CoreTexError::StorageError(e.to_string()));
                }
            }
        }
        drop(data);

        // 4. 写 WAL
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        for id in &ids {
            let key = format!("{}:{}", collection, id);
            self.transaction_manager.append_wal(crate::coretex_transaction::WalEntry {
                transaction_id: txn_id,
                timestamp,
                operation: crate::coretex_transaction::WalOperation::Insert {
                    key,
                    value: vec![],
                },
                lsn: 0,
            }).await
            .map_err(|e| CoreTexError::TransactionError(e.to_string()))?;
        }

        // 5. 提交事务
        self.transaction_manager.commit(txn_id).await
            .map_err(|e| CoreTexError::TransactionError(e.to_string()))?;

        // 6. 触发 Lakehouse 迁移评估（如果挂载了）
        if let Some(lh) = &self.lakehouse {
            // 后台异步触发，不阻塞写入
            let lh_clone = lh.clone();
            tokio::spawn(async move {
                let _ = lh_clone.migrate_data().await;
            });
        }

        Ok(ids)
    }

    /// 事务感知的删除
    pub async fn tx_aware_delete(
        &self,
        collection: &str,
        ids: &[String],
    ) -> Result<usize> {
        let txn_id = self.transaction_manager
            .begin_transaction(IsolationLevel::ReadCommitted)
            .await
            .map_err(|e| CoreTexError::TransactionError(e.to_string()))?;

        let mut data = self.data.write().await;
        let collection_data = data.get_mut(collection)
            .ok_or_else(|| {
                // Note: abort is fire-and-forget here; we log the error if it fails
                let txn_mgr = self.transaction_manager.clone();
                let txn = txn_id;
                tokio::spawn(async move { let _ = txn_mgr.abort(txn).await; });
                CoreTexError::CollectionNotFound(collection.to_string())
            })?;

        // 从索引删除
        let index_name = index_name_for(collection);
        if let Ok(Some(index)) = self.index_manager.get_index(&index_name).await {
            for id in ids {
                let _ = index.remove(id).await;
            }
        }

        let mut deleted = 0;
        for id in ids {
            if collection_data.remove(id).is_some() {
                deleted += 1;
                if let Some(adapter) = &self.unified_adapter {
                    let key = format!("{}:{}", collection, id);
                    let _ = adapter.delete(&key).await;
                } else {
                    let storage = self.storage.read().await;
                    let storage_key = format!("{}:{}", collection, id);
                    let _ = storage.delete(&storage_key).await;
                }
            }
        }
        drop(data);

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        for id in ids {
            self.transaction_manager.append_wal(crate::coretex_transaction::WalEntry {
                transaction_id: txn_id,
                timestamp,
                operation: crate::coretex_transaction::WalOperation::Delete {
                    key: format!("{}:{}", collection, id),
                    value: vec![],
                },
                lsn: 0,
            }).await
            .map_err(|e| CoreTexError::TransactionError(e.to_string()))?;
        }

        self.transaction_manager.commit(txn_id).await
            .map_err(|e| CoreTexError::TransactionError(e.to_string()))?;

        Ok(deleted)
    }

    /// 手动触发 Lakehouse 迁移
    pub async fn migrate_to_lakehouse(&self) -> Result<crate::coretex_lakehouse::MigrationReport> {
        let lh = self.lakehouse.as_ref()
            .ok_or_else(|| CoreTexError::Other("Lakehouse not attached".to_string()))?;
        lh.migrate_data().await
            .map_err(CoreTexError::Other)
    }

    /// 获取 Lakehouse 统计
    pub async fn lakehouse_stats(&self) -> Result<crate::coretex_lakehouse::LakehouseStats> {
        let lh = self.lakehouse.as_ref()
            .ok_or_else(|| CoreTexError::Other("Lakehouse not attached".to_string()))?;
        Ok(lh.get_stats().await)
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
    use crate::coretex_storage::MemoryStorage;
    use crate::coretex_index::IndexManager;

    fn create_test_data_manager() -> DataManager {
        let storage: Box<dyn StorageEngine> = Box::new(MemoryStorage::new());
        let storage = Arc::new(RwLock::new(storage));
        let index_manager = Arc::new(IndexManager::new());
        DataManager::new(storage, index_manager)
    }

    #[tokio::test]
    async fn test_create_and_list_collection() {
        let dm = create_test_data_manager();

        dm.create_collection("test", 128, "cosine").await.unwrap();

        let collections = dm.list_collections().await.unwrap();
        assert!(collections.contains(&"test".to_string()));
    }

    /// Ids may themselves contain ':' — resolving a storage key by splitting on
    /// the first ':' would attribute the row to the wrong collection.
    #[test]
    fn test_split_storage_key_handles_colons_in_id() {
        let known = vec!["demo".to_string(), "demo:ns".to_string()];

        // Longest matching prefix wins.
        assert_eq!(
            DataManager::split_storage_key("demo:ns:user:1", &known),
            Some(("demo:ns", "user:1"))
        );
        assert_eq!(
            DataManager::split_storage_key("demo:plain", &known),
            Some(("demo", "plain"))
        );
        assert_eq!(DataManager::split_storage_key("other:x", &known), None);
    }

    #[tokio::test]
    async fn test_insert_and_search() {
        let dm = create_test_data_manager();

        dm.create_collection("test", 4, "cosine").await.unwrap();

        let vectors = vec![
            ("vec1".to_string(), vec![1.0, 0.0, 0.0, 0.0], serde_json::json!({"text": "hello"})),
            ("vec2".to_string(), vec![0.0, 1.0, 0.0, 0.0], serde_json::json!({"text": "world"})),
            ("vec3".to_string(), vec![0.9, 0.1, 0.0, 0.0], serde_json::json!({"text": "hi"})),
        ];

        dm.insert_vectors("test", vectors).await.unwrap();

        let results = dm.search("test", vec![1.0, 0.0, 0.0, 0.0], 2, None).await.unwrap();

        assert!(!results.is_empty());
        assert_eq!(results[0].id, "vec1");
    }

    #[tokio::test]
    async fn test_delete_collection() {
        let dm = create_test_data_manager();

        dm.create_collection("test", 128, "cosine").await.unwrap();
        dm.delete_collection("test").await.unwrap();

        let collections = dm.list_collections().await.unwrap();
        assert!(!collections.contains(&"test".to_string()));
    }

    #[tokio::test]
    async fn test_get_vector() {
        let dm = create_test_data_manager();

        dm.create_collection("test", 4, "cosine").await.unwrap();

        let vectors = vec![
            ("v1".to_string(), vec![1.0, 0.0, 0.0, 0.0], serde_json::json!({"label": "a"})),
        ];

        dm.insert_vectors("test", vectors).await.unwrap();

        let record = dm.get_vector("test", "v1").await.unwrap();
        assert!(record.is_some());
        assert_eq!(record.unwrap().vector, vec![1.0, 0.0, 0.0, 0.0]);
    }

    #[tokio::test]
    async fn test_update_vector() {
        let dm = create_test_data_manager();

        dm.create_collection("test", 4, "cosine").await.unwrap();

        let vectors = vec![
            ("v1".to_string(), vec![1.0, 0.0, 0.0, 0.0], serde_json::json!({"label": "a"})),
        ];

        dm.insert_vectors("test", vectors).await.unwrap();

        let updated = dm.update_vector("test", "v1", vec![0.0, 1.0, 0.0, 0.0], None).await.unwrap();
        assert!(updated);

        let record = dm.get_vector("test", "v1").await.unwrap().unwrap();
        assert_eq!(record.vector, vec![0.0, 1.0, 0.0, 0.0]);
    }

    #[tokio::test]
    async fn test_upsert_vectors() {
        let dm = create_test_data_manager();

        dm.create_collection("test", 4, "cosine").await.unwrap();

        let vectors = vec![
            ("v1".to_string(), vec![1.0, 0.0, 0.0, 0.0], serde_json::json!({"label": "a"})),
        ];

        dm.insert_vectors("test", vectors).await.unwrap();

        let upsert = vec![
            ("v1".to_string(), vec![0.0, 1.0, 0.0, 0.0], serde_json::json!({"label": "b"})),
            ("v2".to_string(), vec![0.0, 0.0, 1.0, 0.0], serde_json::json!({"label": "c"})),
        ];

        let (inserted, updated) = dm.upsert_vectors("test", upsert).await.unwrap();
        assert_eq!(inserted, vec!["v2"]);
        assert_eq!(updated, vec!["v1"]);
    }

    #[tokio::test]
    async fn test_bulk_operations() {
        let dm = create_test_data_manager();

        dm.create_collection("test", 4, "cosine").await.unwrap();

        let vectors = vec![
            ("v1".to_string(), vec![1.0, 0.0, 0.0, 0.0], serde_json::json!({})),
            ("v2".to_string(), vec![0.0, 1.0, 0.0, 0.0], serde_json::json!({})),
            ("v3".to_string(), vec![0.0, 0.0, 1.0, 0.0], serde_json::json!({})),
        ];

        dm.bulk_insert("test", vectors).await.unwrap();

        assert_eq!(dm.get_vectors_count("test").await.unwrap(), 3);

        let deleted = dm.bulk_delete("test", vec!["v1".to_string()]).await.unwrap();
        assert_eq!(deleted, vec!["v1"]);

        assert_eq!(dm.get_vectors_count("test").await.unwrap(), 2);
    }

    #[tokio::test]
    async fn test_get_all_vectors() {
        let dm = create_test_data_manager();

        dm.create_collection("test", 4, "cosine").await.unwrap();

        let vectors = vec![
            ("b".to_string(), vec![0.0, 1.0, 0.0, 0.0], serde_json::json!({})),
            ("a".to_string(), vec![1.0, 0.0, 0.0, 0.0], serde_json::json!({})),
        ];

        dm.insert_vectors("test", vectors).await.unwrap();

        let all = dm.get_all_vectors("test").await.unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].0, "a");
        assert_eq!(all[1].0, "b");
    }

    #[tokio::test]
    async fn test_clear_collection() {
        let dm = create_test_data_manager();

        dm.create_collection("test", 4, "cosine").await.unwrap();

        let vectors = vec![
            ("v1".to_string(), vec![1.0, 0.0, 0.0, 0.0], serde_json::json!({})),
            ("v2".to_string(), vec![0.0, 1.0, 0.0, 0.0], serde_json::json!({})),
        ];

        dm.insert_vectors("test", vectors).await.unwrap();
        dm.clear_collection("test").await.unwrap();

        assert_eq!(dm.get_vectors_count("test").await.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_total_vector_count() {
        let dm = create_test_data_manager();

        dm.create_collection("c1", 4, "cosine").await.unwrap();
        dm.create_collection("c2", 4, "cosine").await.unwrap();

        dm.insert_vectors("c1", vec![
            ("v1".to_string(), vec![1.0, 0.0, 0.0, 0.0], serde_json::json!({})),
        ]).await.unwrap();

        dm.insert_vectors("c2", vec![
            ("v2".to_string(), vec![0.0, 1.0, 0.0, 0.0], serde_json::json!({})),
            ("v3".to_string(), vec![0.0, 0.0, 1.0, 0.0], serde_json::json!({})),
        ]).await.unwrap();

        assert_eq!(dm.get_total_vector_count().await, 3);
    }

    #[tokio::test]
    async fn test_collection_exists() {
        let dm = create_test_data_manager();

        dm.create_collection("test", 4, "cosine").await.unwrap();
        assert!(dm.collection_exists("test").await);
        assert!(!dm.collection_exists("nonexistent").await);
    }

    #[tokio::test]
    async fn test_duplicate_collection() {
        let dm = create_test_data_manager();

        dm.create_collection("test", 4, "cosine").await.unwrap();
        let result = dm.create_collection("test", 4, "cosine").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_dimension_mismatch() {
        let dm = create_test_data_manager();

        dm.create_collection("test", 4, "cosine").await.unwrap();

        let vectors = vec![
            ("v1".to_string(), vec![1.0, 0.0, 0.0], serde_json::json!({})),
        ];

        let result = dm.insert_vectors("test", vectors).await;
        assert!(result.is_err());
    }
}