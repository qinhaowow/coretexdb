//! Storage engine for CortexDB

use async_trait::async_trait;
use std::error::Error;
use std::path::Path;
#[cfg(feature = "rocksdb")]
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
#[cfg(feature = "rocksdb")]
pub struct PersistentStorage {
    db_path: String,
    db: Option<DB>,
}

#[cfg(feature = "rocksdb")]
impl PersistentStorage {
    /// Create a new persistent storage engine
    pub fn new(db_path: &str) -> Self {
        Self {
            db_path: db_path.to_string(),
            db: None,
        }
    }
}

#[cfg(feature = "rocksdb")]
#[async_trait]
impl StorageEngine for PersistentStorage {
    async fn init(&mut self) -> Result<(), Box<dyn Error>> {
        let path = Path::new(&self.db_path);

        if !path.exists() {
            std::fs::create_dir_all(path)?;
        }

        let mut options = Options::default();
        options.create_if_missing(true);
        options.set_compression_type(rocksdb::DBCompressionType::Snappy);

        let db = DB::open(&options, path)?;
        self.db = Some(db);

        Ok(())
    }

    async fn store(&self, id: &str, vector: &[f32], metadata: &serde_json::Value) -> Result<(), Box<dyn Error>> {
        let db = self.db.as_ref().ok_or("RocksDB not initialized")?;

        let mut data = bincode::serialize(vector)?;
        let meta_json = serde_json::to_string(metadata)?;
        let meta_bytes = meta_json.as_bytes();

        let mut entry = Vec::new();
        entry.extend_from_slice(&(data.len() as u32).to_le_bytes());
        entry.append(&mut data);
        entry.extend_from_slice(meta_bytes);

        db.put(id.as_bytes(), &entry)?;

        Ok(())
    }

    async fn retrieve(&self, id: &str) -> Result<Option<(Vec<f32>, serde_json::Value)>, Box<dyn Error>> {
        let db = self.db.as_ref().ok_or("RocksDB not initialized")?;

        if let Some(entry) = db.get(id.as_bytes())? {
            let vec_len = u32::from_le_bytes(entry[..4].try_into()?) as usize;
            let vector: Vec<f32> = bincode::deserialize(&entry[4..4 + vec_len])?;
            let metadata: serde_json::Value = serde_json::from_slice(&entry[4 + vec_len..])?;

            Ok(Some((vector, metadata)))
        } else {
            Ok(None)
        }
    }

    async fn delete(&self, id: &str) -> Result<bool, Box<dyn Error>> {
        let db = self.db.as_ref().ok_or("RocksDB not initialized")?;

        let exists = db.get(id.as_bytes())?.is_some();
        if exists {
            db.delete(id.as_bytes())?;
        }

        Ok(exists)
    }

    async fn list(&self) -> Result<Vec<String>, Box<dyn Error>> {
        let db = self.db.as_ref().ok_or("RocksDB not initialized")?;

        let mut keys = Vec::new();
        let iter = db.iterator(rocksdb::IteratorMode::Start);
        for item in iter {
            let (key, _) = item?;
            keys.push(String::from_utf8(key.to_vec())?);
        }

        Ok(keys)
    }

    async fn count(&self) -> Result<usize, Box<dyn Error>> {
        let db = self.db.as_ref().ok_or("RocksDB not initialized")?;

        let mut count = 0;
        let iter = db.iterator(rocksdb::IteratorMode::Start);
        for _ in iter {
            count += 1;
        }

        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    include!("tests.rs");
}
