
//! Storage engine abstraction for CortexDB
//! 
//! This module defines the abstract StorageEngine trait that all storage
//! implementations must implement, providing a unified interface for
//! data persistence operations.

use crate::cortex_core::{Vector, Document, CollectionSchema, Result};
use serde::{Serialize, Deserialize};
use std::collections::HashMap;

/// Storage engine abstraction trait
pub trait StorageEngine: Send + Sync {
    /// Initialize the storage engine
    fn init(&mut self) -> Result<()>;
    
    /// Create a new collection
    fn create_collection(&mut self, name: &str, schema: &CollectionSchema) -> Result<()>;
    
    /// Drop a collection
    fn drop_collection(&mut self, name: &str) -> Result<()>;
    
    /// List all collections
    fn list_collections(&self) -> Result<Vec<String>>;
    
    /// Check if a collection exists
    fn collection_exists(&self, name: &str) -> Result<bool>;
    
    /// Insert a document into a collection
    fn insert_document(&mut self, collection: &str, document: &Document) -> Result<String>;
    
    /// Insert multiple documents into a collection
    fn insert_documents(&mut self, collection: &str, documents: &[Document]) -> Result<Vec<String>>;
    
    /// Get a document by ID
    fn get_document(&self, collection: &str, document_id: &str) -> Result<Option<Document>>;
    
    /// Get multiple documents by IDs
    fn get_documents(&self, collection: &str, document_ids: &[String]) -> Result<Vec<Option<Document>>>;
    
    /// Update a document
    fn update_document(&mut self, collection: &str, document_id: &str, updates: &HashMap<String, serde_json::Value>) -> Result<bool>;
    
    /// Delete a document
    fn delete_document(&mut self, collection: &str, document_id: &str) -> Result<bool>;
    
    /// Delete multiple documents
    fn delete_documents(&mut self, collection: &str, document_ids: &[String]) -> Result<usize>;
    
    /// Count documents in a collection
    fn count_documents(&self, collection: &str) -> Result<usize>;
    
    /// Clear all documents from a collection
    fn clear_collection(&mut self, collection: &str) -> Result<()>;
    
    /// Get collection schema
    fn get_collection_schema(&self, collection: &str) -> Result<Option<CollectionSchema>>;
    
    /// Update collection schema
    fn update_collection_schema(&mut self, collection: &str, schema: &CollectionSchema) -> Result<()>;
    
    /// Flush any pending writes to storage
    fn flush(&mut self) -> Result<()>;
    
    /// Close the storage engine
    fn close(&mut self) -> Result<()>;
    
    /// Get storage statistics
    fn get_stats(&self) -> Result<StorageStats>;
}

/// Storage statistics structure
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StorageStats {
    /// Total size in bytes
    pub total_size: u64,
    /// Number of collections
    pub collection_count: usize,
    /// Total number of documents
    pub document_count: u64,
    /// Number of pending operations
    pub pending_operations: usize,
    /// Storage engine type
    pub engine_type: String,
    /// Is the storage read-only
    pub read_only: bool,
    /// Additional custom stats
    pub custom: HashMap<String, serde_json::Value>,
}

/// Storage batch operation for atomic writes
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StorageBatch {
    /// Operations to perform
    pub operations: Vec<StorageOperation>,
    /// Whether the batch should be atomic
    pub atomic: bool,
}

/// Storage operation types
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum StorageOperation {
    /// Insert document operation
    Insert {
        collection: String,
        document: Document,
    },
    /// Update document operation
    Update {
        collection: String,
        document_id: String,
        updates: HashMap<String, serde_json::Value>,
    },
    /// Delete document operation
    Delete {
        collection: String,
        document_id: String,
    },
    /// Create collection operation
    CreateCollection {
        name: String,
        schema: CollectionSchema,
    },
    /// Drop collection operation
    DropCollection {
        name: String,
    },
}

/// Storage transaction interface
pub trait StorageTransaction: Send + Sync {
    /// Commit the transaction
    fn commit(self) -> Result<()>;
    
    /// Rollback the transaction
    fn rollback(self) -> Result<()>;
    
    /// Add an operation to the transaction
    fn add_operation(&mut self, operation: StorageOperation) -> Result<()>;
    
    /// Add multiple operations to the transaction
    fn add_operations(&mut self, operations: &[StorageOperation]) -> Result<()>;
}

/// Storage engine factory trait
pub trait StorageEngineFactory: Send + Sync {
    /// Create a new storage engine instance
    fn create(&self, config: &StorageConfig) -> Result<Box<dyn StorageEngine>>;
    
    /// Get the storage engine type name
    fn engine_type(&self) -> &str;
}

/// Storage configuration structure
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StorageConfig {
    /// Storage engine type
    pub engine: String,
    /// Storage directory
    pub directory: String,
    /// Maximum memory usage in bytes
    pub max_memory: usize,
    /// Enable compression
    pub enable_compression: bool,
    /// Compression level (1-9)
    pub compression_level: u8,
    /// Enable encryption
    pub enable_encryption: bool,
    /// Encryption key (base64 encoded)
    pub encryption_key: Option<String>,
    /// Write buffer size in bytes
    pub write_buffer_size: usize,
    /// Maximum open files
    pub max_open_files: usize,
    /// Additional custom configuration
    pub custom: HashMap<String, serde_json::Value>,
}

/// Default storage configuration
impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            engine: "memory".to_string(),
            directory: "./data".to_string(),
            max_memory: 1024 * 1024 * 1024, // 1GB
            enable_compression: false,
            compression_level: 6,
            enable_encryption: false,
            encryption_key: None,
            write_buffer_size: 64 * 1024 * 1024, // 64MB
            max_open_files: 1000,
            custom: HashMap::new(),
        }
    }
}

/// Storage engine registry
pub struct StorageEngineRegistry {
    factories: HashMap<String, Box<dyn StorageEngineFactory>>,
}

impl StorageEngineRegistry {
    /// Create a new storage engine registry
    pub fn new() -> Self {
        Self {
            factories: HashMap::new(),
        }
    }
    
    /// Register a storage engine factory
    pub fn register(&mut self, factory: Box<dyn StorageEngineFactory>) {
        self.factories.insert(factory.engine_type().to_string(), factory);
    }
    
    /// Create a storage engine instance from config
    pub fn create(&self, config: &StorageConfig) -> Result<Box<dyn StorageEngine>> {
        let factory = self.factories.get(&config.engine)
            .ok_or_else(|| crate::cortex_core::CortexError::Storage(
                crate::cortex_core::StorageError::InvalidConfig(format!("Unknown storage engine: {}", config.engine))
            ))?;
        
        factory.create(config)
    }
    
    /// List available storage engines
    pub fn list_engines(&self) -> Vec<String> {
        self.factories.keys().cloned().collect()
    }
}

/// Global storage engine registry instance
lazy_static::lazy_static! {
    pub static ref STORAGE_ENGINE_REGISTRY: std::sync::Mutex<StorageEngineRegistry> = {
        let mut registry = StorageEngineRegistry::new();
        // Register built-in engines here
        registry
    };
}

/// Register a storage engine factory
pub fn register_storage_engine(factory: Box<dyn StorageEngineFactory>) {
    let mut registry = STORAGE_ENGINE_REGISTRY.lock().unwrap();
    registry.register(factory);
}

/// Create a storage engine from config
pub fn create_storage_engine(config: &StorageConfig) -> Result<Box<dyn StorageEngine>> {
    let registry = STORAGE_ENGINE_REGISTRY.lock().unwrap();
    registry.create(config)
}