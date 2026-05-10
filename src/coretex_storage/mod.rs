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

    /// Set TTL (time-to-live in seconds) for a key. After TTL expires, the entry will be auto-deleted.
    async fn set_ttl(&self, id: &str, ttl_secs: u64) -> Result<(), Box<dyn Error>>;

    /// Remove TTL from a key
    async fn remove_ttl(&self, id: &str) -> Result<(), Box<dyn Error>>;

    /// Get remaining TTL for a key (None if no TTL set)
    async fn get_ttl(&self, id: &str) -> Result<Option<u64>, Box<dyn Error>>;

    /// Purge all expired entries. Returns the number of purged entries.
    async fn purge_expired(&self) -> Result<usize, Box<dyn Error>>;
}

/// In-memory storage implementation
pub struct MemoryStorage {
    data: std::sync::Arc<tokio::sync::RwLock<std::collections::HashMap<String, (Vec<f32>, serde_json::Value)>>>,
    ttl_map: std::sync::Arc<tokio::sync::RwLock<std::collections::HashMap<String, std::time::Instant>>>,
}

impl MemoryStorage {
    /// Create a new in-memory storage engine
    pub fn new() -> Self {
        Self {
            data: std::sync::Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
            ttl_map: std::sync::Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
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

    async fn set_ttl(&self, id: &str, ttl_secs: u64) -> Result<(), Box<dyn Error>> {
        let mut ttl_map = self.ttl_map.write().await;
        ttl_map.insert(id.to_string(), std::time::Instant::now() + std::time::Duration::from_secs(ttl_secs));
        Ok(())
    }

    async fn remove_ttl(&self, id: &str) -> Result<(), Box<dyn Error>> {
        let mut ttl_map = self.ttl_map.write().await;
        ttl_map.remove(id);
        Ok(())
    }

    async fn get_ttl(&self, id: &str) -> Result<Option<u64>, Box<dyn Error>> {
        let ttl_map = self.ttl_map.read().await;
        if let Some(expiry) = ttl_map.get(id) {
            let now = std::time::Instant::now();
            if *expiry > now {
                let remaining = expiry.duration_since(now).as_secs();
                Ok(Some(remaining))
            } else {
                Ok(Some(0))
            }
        } else {
            Ok(None)
        }
    }

    async fn purge_expired(&self) -> Result<usize, Box<dyn Error>> {
        let mut ttl_map = self.ttl_map.write().await;
        let mut data = self.data.write().await;
        let now = std::time::Instant::now();
        let mut purged = 0;

        let expired_keys: Vec<String> = ttl_map.iter()
            .filter(|(_, expiry)| *expiry <= &now)
            .map(|(k, _)| k.clone())
            .collect();

        for key in &expired_keys {
            ttl_map.remove(key);
            data.remove(key);
            purged += 1;
        }

        Ok(purged)
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

    async fn set_ttl(&self, id: &str, ttl_secs: u64) -> Result<(), Box<dyn Error>> {
        let db = self.db.as_ref().ok_or("RocksDB not initialized")?;
        let ttl_key = format!("__ttl__:{}", id);
        let ttl_data = ttl_secs.to_le_bytes();
        db.put(ttl_key.as_bytes(), &ttl_data)?;
        Ok(())
    }

    async fn remove_ttl(&self, id: &str) -> Result<(), Box<dyn Error>> {
        let db = self.db.as_ref().ok_or("RocksDB not initialized")?;
        let ttl_key = format!("__ttl__:{}", id);
        db.delete(ttl_key.as_bytes())?;
        Ok(())
    }

    async fn get_ttl(&self, id: &str) -> Result<Option<u64>, Box<dyn Error>> {
        let db = self.db.as_ref().ok_or("RocksDB not initialized")?;
        let ttl_key = format!("__ttl__:{}", id);
        if let Some(data) = db.get(ttl_key.as_bytes())? {
            let ttl_secs = u64::from_le_bytes(data[..8].try_into()?);
            Ok(Some(ttl_secs))
        } else {
            Ok(None)
        }
    }

    async fn purge_expired(&self) -> Result<usize, Box<dyn Error>> {
        let db = self.db.as_ref().ok_or("RocksDB not initialized")?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let mut purged = 0;

        let iter = db.iterator(rocksdb::IteratorMode::Start);
        let mut to_delete = Vec::new();

        for item in iter {
            let (key, _) = item?;
            let key_str = String::from_utf8(key.to_vec())?;
            if key_str.starts_with("__ttl__:") {
                let data_key = &key_str[7..];
                let ttl_secs = u64::from_le_bytes(key.to_vec()[8..16].try_into()?);
                if now >= ttl_secs {
                    to_delete.push(data_key.to_string());
                    to_delete.push(key_str);
                }
            }
        }

        for key in &to_delete {
            db.delete(key.as_bytes())?;
            purged += 1;
        }

        Ok(purged / 2)
    }
}

#[cfg(test)]
mod tests {
    include!("tests.rs");
}
