//! In-memory storage engine for CortexDB
//! 
//! This module implements an in-memory storage engine that stores all data
//! in RAM, providing fast read/write performance but no persistence.

use crate::cortex_core::{Vector, Document, CollectionSchema, Result, CortexError, StorageError};
use super::engine::{StorageEngine, StorageStats, StorageBatch, StorageOperation, StorageEngineFactory, StorageConfig};
use serde::{Serialize, Deserialize};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::Instant;

/// In-memory storage engine implementation
#[derive(Debug)]
pub struct MemoryStorage {
    /// Collections mapping: collection name -> (schema, documents)
    collections: Arc<RwLock<HashMap<String, (CollectionSchema, HashMap<String, Document>)>>>,
    /// Configuration
    config: StorageConfig,
    /// Startup time
    startup_time: Instant,
}

impl MemoryStorage {
    /// Create a new in-memory storage engine
    pub fn new(config: StorageConfig) -> Self {
        Self {
            collections: Arc::new(RwLock::new(HashMap::new())),
            config,
            startup_time: Instant::now(),
        }
    }
}

impl StorageEngine for MemoryStorage {
    fn init(&mut self) -> Result<()> {
        // No initialization needed for in-memory storage
        Ok(())
    }

    fn create_collection(&mut self, name: &str, schema: &CollectionSchema) -> Result<()> {
        let mut collections = self.collections.write().unwrap();
        
        if collections.contains_key(name) {
            return Err(CortexError::Storage(StorageError::CollectionAlreadyExists(name.to_string())));
        }
        
        collections.insert(name.to_string(), (schema.clone(), HashMap::new()));
        Ok(())
    }

    fn drop_collection(&mut self, name: &str) -> Result<()> {
        let mut collections = self.collections.write().unwrap();
        
        if !collections.contains_key(name) {
            return Err(CortexError::Storage(StorageError::CollectionNotFound(name.to_string())));
        }
        
        collections.remove(name);
        Ok(())
    }

    fn list_collections(&self) -> Result<Vec<String>> {
        let collections = self.collections.read().unwrap();
        Ok(collections.keys().cloned().collect())
    }

    fn collection_exists(&self, name: &str) -> Result<bool> {
        let collections = self.collections.read().unwrap();
        Ok(collections.contains_key(name))
    }

    fn insert_document(&mut self, collection: &str, document: &Document) -> Result<String> {
        let mut collections = self.collections.write().unwrap();
        
        let (_, documents) = collections.get_mut(collection)
            .ok_or_else(|| CortexError::Storage(StorageError::CollectionNotFound(collection.to_string())))?;
        
        let document_id = document.id.clone();
        documents.insert(document_id.clone(), document.clone());
        
        Ok(document_id)
    }

    fn insert_documents(&mut self, collection: &str, documents: &[Document]) -> Result<Vec<String>> {
        let mut collections = self.collections.write().unwrap();
        
        let (_, collection_docs) = collections.get_mut(collection)
            .ok_or_else(|| CortexError::Storage(StorageError::CollectionNotFound(collection.to_string())))?;
        
        let mut inserted_ids = Vec::with_capacity(documents.len());
        
        for document in documents {
            let document_id = document.id.clone();
            collection_docs.insert(document_id.clone(), document.clone());
            inserted_ids.push(document_id);
        }
        
        Ok(inserted_ids)
    }

    fn get_document(&self, collection: &str, document_id: &str) -> Result<Option<Document>> {
        let collections = self.collections.read().unwrap();
        
        let (_, documents) = collections.get(collection)
            .ok_or_else(|| CortexError::Storage(StorageError::CollectionNotFound(collection.to_string())))?;
        
        Ok(documents.get(document_id).cloned())
    }

    fn get_documents(&self, collection: &str, document_ids: &[String]) -> Result<Vec<Option<Document>>> {
        let collections = self.collections.read().unwrap();
        
        let (_, documents) = collections.get(collection)
            .ok_or_else(|| CortexError::Storage(StorageError::CollectionNotFound(collection.to_string())))?;
        
        let mut results = Vec::with_capacity(document_ids.len());
        
        for doc_id in document_ids {
            results.push(documents.get(doc_id).cloned());
        }
        
        Ok(results)
    }

    fn update_document(&mut self, collection: &str, document_id: &str, updates: &HashMap<String, serde_json::Value>) -> Result<bool> {
        let mut collections = self.collections.write().unwrap();
        
        let (_, documents) = collections.get_mut(collection)
            .ok_or_else(|| CortexError::Storage(StorageError::CollectionNotFound(collection.to_string())))?;
        
        if let Some(document) = documents.get_mut(document_id) {
            // Update document fields
            for (key, value) in updates {
                // In a real implementation, we would validate the updates against the schema
                // For now, we'll just update the fields
                if let Ok(mut doc_map) = serde_json::from_value::<serde_json::Map<String, serde_json::Value>>(document.data.clone()) {
                    doc_map.insert(key.clone(), value.clone());
                    document.data = serde_json::to_value(doc_map).unwrap();
                }
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn delete_document(&mut self, collection: &str, document_id: &str) -> Result<bool> {
        let mut collections = self.collections.write().unwrap();
        
        let (_, documents) = collections.get_mut(collection)
            .ok_or_else(|| CortexError::Storage(StorageError::CollectionNotFound(collection.to_string())))?;
        
        Ok(documents.remove(document_id).is_some())
    }

    fn delete_documents(&mut self, collection: &str, document_ids: &[String]) -> Result<usize> {
        let mut collections = self.collections.write().unwrap();
        
        let (_, documents) = collections.get_mut(collection)
            .ok_or_else(|| CortexError::Storage(StorageError::CollectionNotFound(collection.to_string())))?;
        
        let mut deleted_count = 0;
        
        for doc_id in document_ids {
            if documents.remove(doc_id).is_some() {
                deleted_count += 1;
            }
        }
        
        Ok(deleted_count)
    }

    fn count_documents(&self, collection: &str) -> Result<usize> {
        let collections = self.collections.read().unwrap();
        
        let (_, documents) = collections.get(collection)
            .ok_or_else(|| CortexError::Storage(StorageError::CollectionNotFound(collection.to_string())))?;
        
        Ok(documents.len())
    }

    fn clear_collection(&mut self, collection: &str) -> Result<()> {
        let mut collections = self.collections.write().unwrap();
        
        let (schema, _) = collections.get(collection)
            .ok_or_else(|| CortexError::Storage(StorageError::CollectionNotFound(collection.to_string())))?;
        
        // Replace with empty document map
        collections.insert(collection.to_string(), (schema.clone(), HashMap::new()));
        Ok(())
    }

    fn get_collection_schema(&self, collection: &str) -> Result<Option<CollectionSchema>> {
        let collections = self.collections.read().unwrap();
        
        if let Some((schema, _)) = collections.get(collection) {
            Ok(Some(schema.clone()))
        } else {
            Ok(None)
        }
    }

    fn update_collection_schema(&mut self, collection: &str, schema: &CollectionSchema) -> Result<()> {
        let mut collections = self.collections.write().unwrap();
        
        if !collections.contains_key(collection) {
            return Err(CortexError::Storage(StorageError::CollectionNotFound(collection.to_string())));
        }
        
        let (_, documents) = collections.remove(collection).unwrap();
        collections.insert(collection.to_string(), (schema.clone(), documents));
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        // No flush needed for in-memory storage
        Ok(())
    }

    fn close(&mut self) -> Result<()> {
        // No cleanup needed for in-memory storage
        Ok(())
    }

    fn get_stats(&self) -> Result<StorageStats> {
        let collections = self.collections.read().unwrap();
        
        let mut total_documents = 0;
        for (_, (_, documents)) in &*collections {
            total_documents += documents.len();
        }
        
        Ok(StorageStats {
            total_size: 0, // In-memory size is not tracked
            collection_count: collections.len(),
            document_count: total_documents as u64,
            pending_operations: 0,
            engine_type: "memory".to_string(),
            read_only: false,
            custom: HashMap::new(),
        })
    }
}

/// Memory storage engine factory
#[derive(Debug)]
pub struct MemoryStorageFactory;

impl StorageEngineFactory for MemoryStorageFactory {
    fn create(&self, config: &StorageConfig) -> Result<Box<dyn StorageEngine>> {
        Ok(Box::new(MemoryStorage::new(config.clone())))
    }

    fn engine_type(&self) -> &str {
        "memory"
    }
}

/// Register the memory storage engine
pub fn register_memory_storage() {
    use super::engine::register_storage_engine;
    register_storage_engine(Box::new(MemoryStorageFactory));
}