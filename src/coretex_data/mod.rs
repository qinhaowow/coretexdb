use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use serde::{Deserialize, Serialize};

use crate::coretex_core::{CollectionSchema, CoreTexError, Result, DistanceMetric};
use crate::coretex_storage::StorageEngine;
use crate::coretex_index::{IndexManager, SearchResult};
use crate::coretex_transaction::{TransactionManager, TransactionId, IsolationLevel, TransactionError};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorRecord {
    pub vector: Vec<f32>,
    pub metadata: serde_json::Value,
}

pub struct DataManager {
    collections: Arc<RwLock<HashMap<String, CollectionSchema>>>,
    data: Arc<RwLock<HashMap<String, HashMap<String, VectorRecord>>>>,
    index_manager: Arc<IndexManager>,
    storage: Arc<RwLock<Box<dyn StorageEngine>>>,
    transaction_manager: Arc<TransactionManager>,
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
            transaction_manager: Arc::new(TransactionManager::new()),
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
            transaction_manager,
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
            transaction_manager: Arc::new(TransactionManager::new()),
        }
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

    pub async fn create_collection(&self, name: &str, dimension: usize, metric: &str) -> Result<()> {
        let mut collections = self.collections.write().await;

        if collections.contains_key(name) {
            return Err(CoreTexError::ValidationError(format!("Collection '{}' already exists", name)));
        }

        let schema = CollectionSchema {
            name: name.to_string(),
            dimension,
            distance_metric: match metric {
                "euclidean" => DistanceMetric::Euclidean,
                "dotproduct" => DistanceMetric::DotProduct,
                "manhattan" => DistanceMetric::Manhattan,
                _ => DistanceMetric::Cosine,
            },
            indexes: vec![],
            metadata_schema: None,
        };

        collections.insert(name.to_string(), schema);

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

        let index_name = format!("{}_hnsw", collection);
        if let Ok(Some(index)) = self.index_manager.get_index(&index_name).await {
            for (id, vector, _) in &vectors {
                let _ = index.add(id, vector).await;
            }
        }

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

    pub async fn search(
        &self,
        collection: &str,
        query: Vec<f32>,
        k: usize,
        filter: Option<serde_json::Value>,
    ) -> Result<Vec<SearchResult>> {
        let _schema = self.get_collection(collection).await?;

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
                            if let Some(record) = cd.get(&r.id) {
                                return Self::matches_filter(&record.metadata, &filter_obj);
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
            .map(|(id, record)| {
                let distance = Self::cosine_distance(&query, &record.vector);
                SearchResult {
                    id: id.clone(),
                    distance,
                }
            })
            .collect();

        results.sort_by(|a, b| {
            a.distance.partial_cmp(&b.distance).unwrap_or(std::cmp::Ordering::Equal)
        });
        Ok(results.into_iter().take(k).collect())
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
        collection_data.insert(id.to_string(), VectorRecord {
            vector: vector.clone(),
            metadata: meta,
        });

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

        Ok((inserted, updated))
    }

    pub async fn bulk_insert(
        &self,
        collection: &str,
        vectors: Vec<(String, Vec<f32>, serde_json::Value)>,
    ) -> Result<Vec<String>> {
        let dimension = self.get_collection_dimension(collection).await?;

        for (_, vector, _) in &vectors {
            if vector.len() != dimension {
                return Err(CoreTexError::DimensionMismatch {
                    expected: dimension,
                    actual: vector.len(),
                });
            }
        }

        let mut data = self.data.write().await;
        let collection_data = data.get_mut(collection)
            .ok_or(CoreTexError::CollectionNotFound(collection.to_string()))?;

        let mut ids = Vec::new();
        for (id, vector, metadata) in vectors {
            collection_data.insert(id.clone(), VectorRecord { vector, metadata });
            ids.push(id.clone());
        }

        Ok(ids)
    }

    pub async fn bulk_update(
        &self,
        collection: &str,
        vectors: Vec<(String, Vec<f32>, serde_json::Value)>,
    ) -> Result<Vec<String>> {
        let dimension = self.get_collection_dimension(collection).await?;

        for (_, vector, _) in &vectors {
            if vector.len() != dimension {
                return Err(CoreTexError::DimensionMismatch {
                    expected: dimension,
                    actual: vector.len(),
                });
            }
        }

        let mut data = self.data.write().await;
        let collection_data = data.get_mut(collection)
            .ok_or(CoreTexError::CollectionNotFound(collection.to_string()))?;

        let mut updated_ids = Vec::new();
        for (id, vector, metadata) in vectors {
            if collection_data.contains_key(&id) {
                collection_data.insert(id.clone(), VectorRecord { vector, metadata });
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
            let record = VectorRecord { vector, metadata };
            if collection_data.contains_key(&id) {
                collection_data.insert(id.clone(), record);
                updated.push(id);
            } else {
                collection_data.insert(id.clone(), record);
                inserted.push(id);
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

    pub async fn clear_collection(&self, collection: &str) -> Result<()> {
        let mut data = self.data.write().await;
        let collection_data = data.get_mut(collection)
            .ok_or(CoreTexError::CollectionNotFound(collection.to_string()))?;
        collection_data.clear();

        let index_name = format!("{}_hnsw", collection);
        if let Ok(Some(index)) = self.index_manager.get_index(&index_name).await {
            let _ = index.clear().await;
        }

        Ok(())
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
        let storage = self.storage.read().await;
        let purged = storage.purge_expired().await
            .map_err(|e| CoreTexError::StorageError(e.to_string()))?;

        let mut data = self.data.write().await;
        for collection_data in data.values_mut() {
            collection_data.retain(|_, _| true);
        }

        Ok(purged)
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
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        for id in &ids {
            let lsn = wal.entries.len() as u64;
            wal.append(crate::coretex_transaction::WalEntry {
                transaction_id: txn_id,
                timestamp,
                operation: crate::coretex_transaction::WalOperation::Insert {
                    key: format!("{}:{}", collection, id),
                    value: bincode::serialize(&VectorRecord {
                        vector: vec![],
                        metadata: serde_json::json!({}),
                    }).unwrap_or_default(),
                },
                lsn,
            });
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
            wal.append(crate::coretex_transaction::WalEntry {
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
        wal.append(crate::coretex_transaction::WalEntry {
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
        });
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
            wal.append(crate::coretex_transaction::WalEntry {
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
            wal.append(crate::coretex_transaction::WalEntry {
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