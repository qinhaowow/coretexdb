//! Storage engine for CortexDB

use async_trait::async_trait;
use std::error::Error;
use std::path::Path;
use rocksdb::{DB, Options};
use bincode;

/// Storage engine trait
#[async_trait]
pub trait StorageEngine: Send + Sync {
    /// Initialize the storage engine
    async fn init(&mut self) -> Result<(), Box<dyn Error>>;
    
    /// Store a vector with metadata
    async fn store(&self, id: &str, vector: &[f32], metadata: &serde_json::Value) -> Result<(), Box<dyn Error>>;
    
    /// Retrieve a vector by ID
    async fn retrieve(&self, id: &str) -> Result<Option<(Vec<f32>, serde_json::Value)>, Box<dyn Error>>;
    
    /// Delete a vector by ID
    async fn delete(&self, id: &str) -> Result<bool, Box<dyn Error>>;
    
    /// List all vectors
    async fn list(&self) -> Result<Vec<String>, Box<dyn Error>>;
    
    /// Count the number of vectors
    async fn count(&self) -> Result<usize, Box<dyn Error>>;
}

/// In-memory storage implementation
pub struct MemoryStorage {
    data: std::sync::Arc<tokio::sync::RwLock<std::collections::HashMap<String, (Vec<f32>, serde_json::Value)>>>,
}

impl MemoryStorage {
    /// Create a new in-memory storage engine
    pub fn new() -> Self {
        Self {
            data: std::sync::Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
        }
    }
}

#[async_trait]
impl StorageEngine for MemoryStorage {
    async fn init(&mut self) -> Result<(), Box<dyn Error>> {
        Ok(())
    }
    
    async fn store(&self, id: &str, vector: &[f32], metadata: &serde_json::Value) -> Result<(), Box<dyn Error>> {
        let mut data = self.data.write().await;
        data.insert(id.to_string(), (vector.to_vec(), metadata.clone()));
        Ok(())
    }
    
    async fn retrieve(&self, id: &str) -> Result<Option<(Vec<f32>, serde_json::Value)>, Box<dyn Error>> {
        let data = self.data.read().await;
        Ok(data.get(id).cloned())
    }
    
    async fn delete(&self, id: &str) -> Result<bool, Box<dyn Error>> {
        let mut data = self.data.write().await;
        Ok(data.remove(id).is_some())
    }
    
    async fn list(&self) -> Result<Vec<String>, Box<dyn Error>> {
        let data = self.data.read().await;
        Ok(data.keys().cloned().collect())
    }
    
    async fn count(&self) -> Result<usize, Box<dyn Error>> {
        let data = self.data.read().await;
        Ok(data.len())
    }
}

/// Persistent storage implementation (uses RocksDB)
pub struct PersistentStorage {
    db_path: String,
    db: Option<DB>,
}

impl PersistentStorage {
    /// Create a new persistent storage engine
    pub fn new(db_path: &str) -> Self {
        Self {
            db_path: db_path.to_string(),
            db: None,
        }
    }
}

#[async_trait]
impl StorageEngine for PersistentStorage {
    async fn init(&mut self) -> Result<(), Box<dyn Error>> {
        let path = Path::new(&self.db_path);
        
        // Create directory if it doesn't exist
        if !path.exists() {
            std::fs::create_dir_all(path)?;
        }
        
        // Initialize RocksDB
        let mut options = Options::default();
        options.create_if_missing(true);
        options.set_compression_type(rocksdb::DBCompressionType::Snappy);
        
        let db = DB::open(&options, path)?;
        self.db = Some(db);
        
        Ok(())
    }
    
    async fn store(&self, id: &str, vector: &[f32], metadata: &serde_json::Value) -> Result<(), Box<dyn Error>> {
        let db = self.db.as_ref().ok_or("RocksDB not initialized")?;
        
        // Serialize vector and metadata
        let data = (vector, metadata);
        let serialized = bincode::serialize(&data)?;
        
        // Store in RocksDB
        db.put(id, serialized)?;
        
        Ok(())
    }
    
    async fn retrieve(&self, id: &str) -> Result<Option<(Vec<f32>, serde_json::Value)>, Box<dyn Error>> {
        let db = self.db.as_ref().ok_or("RocksDB not initialized")?;
        
        // Retrieve from RocksDB
        match db.get(id)? {
            Some(serialized) => {
                // Deserialize
                let data: (Vec<f32>, serde_json::Value) = bincode::deserialize(&serialized)?;
                Ok(Some(data))
            }
            None => Ok(None),
        }
    }
    
    async fn delete(&self, id: &str) -> Result<bool, Box<dyn Error>> {
        let db = self.db.as_ref().ok_or("RocksDB not initialized")?;
        
        // Check if key exists
        let exists = db.get(id)?.is_some();
        
        // Delete from RocksDB
        if exists {
            db.delete(id)?;
        }
        
        Ok(exists)
    }
    
    async fn list(&self) -> Result<Vec<String>, Box<dyn Error>> {
        let db = self.db.as_ref().ok_or("RocksDB not initialized")?;
        
        let mut keys = Vec::new();
        let iterator = db.iterator(rocksdb::IteratorMode::Start);
        
        for (key, _) in iterator {
            if let Ok(key_str) = String::from_utf8(key.to_vec()) {
                keys.push(key_str);
            }
        }
        
        Ok(keys)
    }
    
    async fn count(&self) -> Result<usize, Box<dyn Error>> {
        let db = self.db.as_ref().ok_or("RocksDB not initialized")?;
        
        let mut count = 0;
        let iterator = db.iterator(rocksdb::IteratorMode::Start);
        
        for _ in iterator {
            count += 1;
        }
        
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    include!("tests.rs");
}