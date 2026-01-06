//! Persistent storage engine for CortexDB
//! 
//! This module implements a persistent storage engine that uses RocksDB
//! for data persistence, providing durability and persistence across restarts.

use crate::cortex_core::{Vector, Document, CollectionSchema, Result, CortexError, StorageError};
use super::engine::{StorageEngine, StorageStats, StorageBatch, StorageOperation, StorageEngineFactory, StorageConfig};
use serde::{Serialize, Deserialize};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::Instant;
use rocksdb::{DB, Options, WriteOptions, ReadOptions};

/// Persistent storage engine implementation using RocksDB
#[derive(Debug)]
pub struct PersistentStorage {
    /// RocksDB instance
    db: Arc<RwLock<DB>>,
    /// Configuration
    config: StorageConfig,
    /// Storage directory
    directory: PathBuf,
    /// Startup time
    startup_time: Instant,
    /// Collections cache
    collections: Arc<RwLock<HashMap<String, CollectionSchema>>>,
}

impl PersistentStorage {
    /// Create a new persistent storage engine
    pub fn new(config: StorageConfig) -> Result<Self> {
        let directory = Path::new(&config.directory).to_path_buf();
        
        // Create directory if it doesn't exist
        std::fs::create_dir_all(&directory)?;
        
        // Configure RocksDB
        let mut options = Options::default();
        options.create_if_missing(true);
        options.set_max_open_files(config.max_open_files as i32);
        options.set_write_buffer_size(config.write_buffer_size);
        options.set_compression_type(if config.enable_compression {
            rocksdb::DBCompressionType::Lz4
        } else {
            rocksdb::DBCompressionType::NoCompression
        });
        
        // Open RocksDB
        let db = DB::open(&options, &directory)
            .map_err(|e| CortexError::Storage(StorageError::ReadError(e.to_string())))?;
        
        let storage = Self {
            db: Arc::new(RwLock::new(db)),
            config,
            directory,
            startup_time: Instant::now(),
            collections: Arc::new(RwLock::new(HashMap::new())),
        };
        
        // Load collections from storage
        storage.load_collections()?;
        
        Ok(storage)
    }
    
    /// Load collections from storage
    fn load_collections(&self) -> Result<()> {
        let db = self.db.read().unwrap();
        let mut collections = self.collections.write().unwrap();
        
        // Scan for collection schemas
        let mut iter = db.raw_iterator();
        iter.seek(b"collection:");
        
        while iter.valid() {
            let key = iter.key().unwrap();
            let key_str = String::from_utf8_lossy(key);
            
            if key_str.starts_with("collection:") {
                let collection_name = key_str.trim_start_matches("collection:");
                let value = iter.value().unwrap();
                
                if let Ok(schema) = serde_json::from_slice::<CollectionSchema>(value) {
                    collections.insert(collection_name.to_string(), schema);
                }
            } else {
                break;
            }
            
            iter.next();
        }
        
        Ok(())
    }
    
    /// Get the RocksDB key for a collection schema
    fn collection_key(collection: &str) -> Vec<u8> {
        format!("collection:{}", collection).as_bytes().to_vec()
    }
    
    /// Get the RocksDB key for a document
    fn document_key(collection: &str, document_id: &str) -> Vec<u8> {
        format!("doc:{}:{}", collection, document_id).as_bytes().to_vec()
    }
    
    /// Get the RocksDB prefix for documents in a collection
    fn document_prefix(collection: &str) -> Vec<u8> {
        format!("doc:{}:", collection).as_bytes().to_vec()
    }
}

impl StorageEngine for PersistentStorage {
    fn init(&mut self) -> Result<()> {
        // No additional initialization needed
        Ok(())
    }

    fn create_collection(&mut self, name: &str, schema: &CollectionSchema) -> Result<()> {
        let mut collections = self.collections.write().unwrap();
        
        if collections.contains_key(name) {
            return Err(CortexError::Storage(StorageError::CollectionAlreadyExists(name.to_string())));
        }
        
        // Store collection schema in RocksDB
        let db = self.db.write().unwrap();
        let key = Self::collection_key(name);
        let value = serde_json::to_vec(schema)
            .map_err(|e| CortexError::Serde(e))?;
        
        db.put(&key, &value)
            .map_err(|e| CortexError::Storage(StorageError::WriteError(e.to_string())))?;
        
        // Add to collections cache
        collections.insert(name.to_string(), schema.clone());
        Ok(())
    }

    fn drop_collection(&mut self, name: &str) -> Result<()> {
        let mut collections = self.collections.write().unwrap();
        
        if !collections.contains_key(name) {
            return Err(CortexError::Storage(StorageError::CollectionNotFound(name.to_string())));
        }
        
        // Delete all documents in the collection
        let db = self.db.write().unwrap();
        let prefix = Self::document_prefix(name);
        
        // Delete documents with the prefix
        let mut iter = db.raw_iterator();
        iter.seek(&prefix);
        
        while iter.valid() {
            let key = iter.key().unwrap();
            if key.starts_with(&prefix) {
                db.delete(key)
                    .map_err(|e| CortexError::Storage(StorageError::WriteError(e.to_string())))?;
                iter.next();
            } else {
                break;
            }
        }
        
        // Delete collection schema
        let key = Self::collection_key(name);
        db.delete(&key)
            .map_err(|e| CortexError::Storage(StorageError::WriteError(e.to_string())))?;
        
        // Remove from collections cache
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
        let collections = self.collections.read().unwrap();
        
        if !collections.contains_key(collection) {
            return Err(CortexError::Storage(StorageError::CollectionNotFound(collection.to_string())));
        }
        
        // Store document in RocksDB
        let db = self.db.write().unwrap();
        let key = Self::document_key(collection, &document.id);
        let value = serde_json::to_vec(document)
            .map_err(|e| CortexError::Serde(e))?;
        
        db.put(&key, &value)
            .map_err(|e| CortexError::Storage(StorageError::WriteError(e.to_string())))?;
        
        Ok(document.id.clone())
    }

    fn insert_documents(&mut self, collection: &str, documents: &[Document]) -> Result<Vec<String>> {
        let collections = self.collections.read().unwrap();
        
        if !collections.contains_key(collection) {
            return Err(CortexError::Storage(StorageError::CollectionNotFound(collection.to_string())));
        }
        
        // Store documents in RocksDB
        let db = self.db.write().unwrap();
        let mut write_options = WriteOptions::default();
        write_options.set_sync(false);
        
        let mut batch = rocksdb::WriteBatch::default();
        let mut inserted_ids = Vec::with_capacity(documents.len());
        
        for document in documents {
            let key = Self::document_key(collection, &document.id);
            let value = serde_json::to_vec(document)
                .map_err(|e| CortexError::Serde(e))?;
            
            batch.put(&key, &value);
            inserted_ids.push(document.id.clone());
        }
        
        db.write_opt(batch, &write_options)
            .map_err(|e| CortexError::Storage(StorageError::WriteError(e.to_string())))?;
        
        Ok(inserted_ids)
    }

    fn get_document(&self, collection: &str, document_id: &str) -> Result<Option<Document>> {
        let collections = self.collections.read().unwrap();
        
        if !collections.contains_key(collection) {
            return Err(CortexError::Storage(StorageError::CollectionNotFound(collection.to_string())));
        }
        
        // Get document from RocksDB
        let db = self.db.read().unwrap();
        let key = Self::document_key(collection, document_id);
        
        match db.get(&key) {
            Ok(Some(value)) => {
                let document = serde_json::from_slice::<Document>(&value)
                    .map_err(|e| CortexError::Serde(e))?;
                Ok(Some(document))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(CortexError::Storage(StorageError::ReadError(e.to_string()))),
        }
    }

    fn get_documents(&self, collection: &str, document_ids: &[String]) -> Result<Vec<Option<Document>>> {
        let collections = self.collections.read().unwrap();
        
        if !collections.contains_key(collection) {
            return Err(CortexError::Storage(StorageError::CollectionNotFound(collection.to_string())));
        }
        
        let db = self.db.read().unwrap();
        let mut results = Vec::with_capacity(document_ids.len());
        
        for doc_id in document_ids {
            let key = Self::document_key(collection, doc_id);
            
            match db.get(&key) {
                Ok(Some(value)) => {
                    if let Ok(document) = serde_json::from_slice::<Document>(&value) {
                        results.push(Some(document));
                    } else {
                        results.push(None);
                    }
                }
                Ok(None) => results.push(None),
                Err(_) => results.push(None),
            }
        }
        
        Ok(results)
    }

    fn update_document(&mut self, collection: &str, document_id: &str, updates: &HashMap<String, serde_json::Value>) -> Result<bool> {
        let collections = self.collections.read().unwrap();
        
        if !collections.contains_key(collection) {
            return Err(CortexError::Storage(StorageError::CollectionNotFound(collection.to_string())));
        }
        
        // Get current document
        let db = self.db.write().unwrap();
        let key = Self::document_key(collection, document_id);
        
        match db.get(&key) {
            Ok(Some(value)) => {
                let mut document = serde_json::from_slice::<Document>(&value)
                    .map_err(|e| CortexError::Serde(e))?;
                
                // Update document fields
                if let Ok(mut doc_map) = serde_json::from_value::<serde_json::Map<String, serde_json::Value>>(document.data.clone()) {
                    for (key, value) in updates {
                        doc_map.insert(key.clone(), value.clone());
                    }
                    document.data = serde_json::to_value(doc_map).unwrap();
                }
                
                // Write updated document back
                let updated_value = serde_json::to_vec(&document)
                    .map_err(|e| CortexError::Serde(e))?;
                
                db.put(&key, &updated_value)
                    .map_err(|e| CortexError::Storage(StorageError::WriteError(e.to_string())))?;
                
                Ok(true)
            }
            Ok(None) => Ok(false),
            Err(e) => Err(CortexError::Storage(StorageError::ReadError(e.to_string()))),
        }
    }

    fn delete_document(&mut self, collection: &str, document_id: &str) -> Result<bool> {
        let collections = self.collections.read().unwrap();
        
        if !collections.contains_key(collection) {
            return Err(CortexError::Storage(StorageError::CollectionNotFound(collection.to_string())));
        }
        
        let db = self.db.write().unwrap();
        let key = Self::document_key(collection, document_id);
        
        match db.delete(&key) {
            Ok(()) => Ok(true),
            Err(e) => Err(CortexError::Storage(StorageError::WriteError(e.to_string()))),
        }
    }

    fn delete_documents(&mut self, collection: &str, document_ids: &[String]) -> Result<usize> {
        let collections = self.collections.read().unwrap();
        
        if !collections.contains_key(collection) {
            return Err(CortexError::Storage(StorageError::CollectionNotFound(collection.to_string())));
        }
        
        let db = self.db.write().unwrap();
        let mut write_options = WriteOptions::default();
        write_options.set_sync(false);
        
        let mut batch = rocksdb::WriteBatch::default();
        
        for doc_id in document_ids {
            let key = Self::document_key(collection, doc_id);
            batch.delete(&key);
        }
        
        db.write_opt(batch, &write_options)
            .map_err(|e| CortexError::Storage(StorageError::WriteError(e.to_string())))?;
        
        Ok(document_ids.len())
    }

    fn count_documents(&self, collection: &str) -> Result<usize> {
        let collections = self.collections.read().unwrap();
        
        if !collections.contains_key(collection) {
            return Err(CortexError::Storage(StorageError::CollectionNotFound(collection.to_string())));
        }
        
        let db = self.db.read().unwrap();
        let prefix = Self::document_prefix(collection);
        
        let mut count = 0;
        let mut iter = db.raw_iterator();
        iter.seek(&prefix);
        
        while iter.valid() {
            let key = iter.key().unwrap();
            if key.starts_with(&prefix) {
                count += 1;
                iter.next();
            } else {
                break;
            }
        }
        
        Ok(count)
    }

    fn clear_collection(&mut self, collection: &str) -> Result<()> {
        let collections = self.collections.read().unwrap();
        
        if !collections.contains_key(collection) {
            return Err(CortexError::Storage(StorageError::CollectionNotFound(collection.to_string())));
        }
        
        // Delete all documents in the collection
        let db = self.db.write().unwrap();
        let prefix = Self::document_prefix(collection);
        
        let mut iter = db.raw_iterator();
        iter.seek(&prefix);
        
        while iter.valid() {
            let key = iter.key().unwrap();
            if key.starts_with(&prefix) {
                db.delete(key)
                    .map_err(|e| CortexError::Storage(StorageError::WriteError(e.to_string())))?;
                iter.next();
            } else {
                break;
            }
        }
        
        Ok(())
    }

    fn get_collection_schema(&self, collection: &str) -> Result<Option<CollectionSchema>> {
        let collections = self.collections.read().unwrap();
        
        if let Some(schema) = collections.get(collection) {
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
        
        // Update schema in RocksDB
        let db = self.db.write().unwrap();
        let key = Self::collection_key(collection);
        let value = serde_json::to_vec(schema)
            .map_err(|e| CortexError::Serde(e))?;
        
        db.put(&key, &value)
            .map_err(|e| CortexError::Storage(StorageError::WriteError(e.to_string())))?;
        
        // Update in collections cache
        collections.insert(collection.to_string(), schema.clone());
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        let db = self.db.write().unwrap();
        db.flush()
            .map_err(|e| CortexError::Storage(StorageError::WriteError(e.to_string())))?;
        Ok(())
    }

    fn close(&mut self) -> Result<()> {
        // RocksDB will be closed automatically when dropped
        Ok(())
    }

    fn get_stats(&self) -> Result<StorageStats> {
        let collections = self.collections.read().unwrap();
        
        // Calculate total documents
        let mut total_documents = 0;
        for collection in collections.keys() {
            total_documents += self.count_documents(collection)?;
        }
        
        // Get directory size
        let total_size = self.calculate_directory_size(&self.directory)
            .unwrap_or(0);
        
        Ok(StorageStats {
            total_size,
            collection_count: collections.len(),
            document_count: total_documents as u64,
            pending_operations: 0,
            engine_type: "persistent".to_string(),
            read_only: false,
            custom: HashMap::new(),
        })
    }
}

impl PersistentStorage {
    /// Calculate directory size
    fn calculate_directory_size(&self, path: &Path) -> Result<u64> {
        let mut total_size = 0;
        
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let metadata = entry.metadata()?;
            
            if metadata.is_file() {
                total_size += metadata.len();
            } else if metadata.is_dir() {
                total_size += self.calculate_directory_size(&entry.path())?;
            }
        }
        
        Ok(total_size)
    }
}

/// Persistent storage engine factory
#[derive(Debug)]
pub struct PersistentStorageFactory;

impl StorageEngineFactory for PersistentStorageFactory {
    fn create(&self, config: &StorageConfig) -> Result<Box<dyn StorageEngine>> {
        PersistentStorage::new(config.clone())
            .map(|storage| Box::new(storage) as Box<dyn StorageEngine>)
    }

    fn engine_type(&self) -> &str {
        "persistent"
    }
}

/// Register the persistent storage engine
pub fn register_persistent_storage() {
    use super::engine::register_storage_engine;
    register_storage_engine(Box::new(PersistentStorageFactory));
}