//! Storage engine for CortexDB

use async_trait::async_trait;
use crate::coretex_core::{CoreTexError, Result};
#[cfg(feature = "rocksdb")]
use rocksdb::{DB, Options};
use bincode;

/// Storage engine trait
#[async_trait]
pub trait StorageEngine: Send + Sync {
    /// Initialize the storage engine
    async fn init(&mut self) -> Result<()>;

    /// Store a vector with metadata
    async fn store(&self, id: &str, vector: &[f32], metadata: &serde_json::Value) -> Result<()>;

    /// Retrieve a vector by ID
    async fn retrieve(&self, id: &str) -> Result<Option<(Vec<f32>, serde_json::Value)>>;

    /// Delete a vector by ID
    async fn delete(&self, id: &str) -> Result<bool>;

    /// List all vectors
    async fn list(&self) -> Result<Vec<String>>;

    /// Count the number of vectors
    async fn count(&self) -> Result<usize>;

    /// Set TTL (time-to-live in seconds) for a key. After TTL expires, the entry will be auto-deleted.
    async fn set_ttl(&self, id: &str, ttl_secs: u64) -> Result<()>;

    /// Remove TTL from a key
    async fn remove_ttl(&self, id: &str) -> Result<()>;

    /// Get remaining TTL for a key (None if no TTL set)
    async fn get_ttl(&self, id: &str) -> Result<Option<u64>>;

    /// Purge all expired entries. Returns the number of purged entries.
    async fn purge_expired(&self) -> Result<usize>;
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
    async fn init(&mut self) -> Result<()> {
        Ok(())
    }

    async fn store(&self, id: &str, vector: &[f32], metadata: &serde_json::Value) -> Result<()> {
        let mut data = self.data.write().await;
        data.insert(id.to_string(), (vector.to_vec(), metadata.clone()));
        Ok(())
    }

    async fn retrieve(&self, id: &str) -> Result<Option<(Vec<f32>, serde_json::Value)>> {
        let data = self.data.read().await;
        Ok(data.get(id).cloned())
    }

    async fn delete(&self, id: &str) -> Result<bool> {
        let mut data = self.data.write().await;
        Ok(data.remove(id).is_some())
    }

    async fn list(&self) -> Result<Vec<String>> {
        let data = self.data.read().await;
        Ok(data.keys().cloned().collect())
    }

    async fn count(&self) -> Result<usize> {
        let data = self.data.read().await;
        Ok(data.len())
    }

    async fn set_ttl(&self, id: &str, ttl_secs: u64) -> Result<()> {
        let mut ttl_map = self.ttl_map.write().await;
        ttl_map.insert(id.to_string(), std::time::Instant::now() + std::time::Duration::from_secs(ttl_secs));
        Ok(())
    }

    async fn remove_ttl(&self, id: &str) -> Result<()> {
        let mut ttl_map = self.ttl_map.write().await;
        ttl_map.remove(id);
        Ok(())
    }

    async fn get_ttl(&self, id: &str) -> Result<Option<u64>> {
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

    async fn purge_expired(&self) -> Result<usize> {
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
    async fn init(&mut self) -> Result<()> {
        use std::path::Path;
        let path = Path::new(&self.db_path);

        if !path.exists() {
            std::fs::create_dir_all(path)?;
        }

        let mut options = Options::default();
        options.create_if_missing(true);
        options.set_compression_type(rocksdb::DBCompressionType::Snappy);

        let db = DB::open(&options, path)
            .map_err(|e| CoreTexError::StorageError(format!("Failed to open RocksDB: {}", e)))?;
        self.db = Some(db);

        Ok(())
    }

    async fn store(&self, id: &str, vector: &[f32], metadata: &serde_json::Value) -> Result<()> {
        let db = self.db.as_ref().ok_or(CoreTexError::StorageNotInitialized)?;

        let data = bincode::serialize(vector)?;
        let meta_json = serde_json::to_string(metadata)?;
        let meta_bytes = meta_json.as_bytes();

        let mut entry = Vec::new();
        entry.extend_from_slice(&data);
        entry.extend_from_slice(meta_bytes);

        db.put(id.as_bytes(), &entry)
            .map_err(|e| CoreTexError::StorageError(format!("RocksDB put failed: {}", e)))?;

        Ok(())
    }

    async fn retrieve(&self, id: &str) -> Result<Option<(Vec<f32>, serde_json::Value)>> {
        let db = self.db.as_ref().ok_or(CoreTexError::StorageNotInitialized)?;

        if let Some(entry) = db.get(id.as_bytes())
            .map_err(|e| CoreTexError::StorageError(format!("RocksDB get failed: {}", e)))? 
        {
            let vector: Vec<f32> = bincode::deserialize(&entry)?;
            let vec_byte_len = vector.len() * 4;
            let metadata: serde_json::Value = serde_json::from_slice(&entry[vec_byte_len..])?;

            Ok(Some((vector, metadata)))
        } else {
            Ok(None)
        }
    }

    async fn delete(&self, id: &str) -> Result<bool> {
        let db = self.db.as_ref().ok_or(CoreTexError::StorageNotInitialized)?;

        let exists = db.get(id.as_bytes())
            .map_err(|e| CoreTexError::StorageError(e.to_string()))?
            .is_some();
        if exists {
            db.delete(id.as_bytes())
                .map_err(|e| CoreTexError::StorageError(e.to_string()))?;
        }

        Ok(exists)
    }

    async fn list(&self) -> Result<Vec<String>> {
        let db = self.db.as_ref().ok_or(CoreTexError::StorageNotInitialized)?;

        let mut keys = Vec::new();
        let iter = db.iterator(rocksdb::IteratorMode::Start);
        for item in iter {
            let (key, _) = item.map_err(|e| CoreTexError::StorageError(e.to_string()))?;
            keys.push(String::from_utf8(key.to_vec())?);
        }

        Ok(keys)
    }

    async fn count(&self) -> Result<usize> {
        let db = self.db.as_ref().ok_or(CoreTexError::StorageNotInitialized)?;

        let mut count = 0;
        let iter = db.iterator(rocksdb::IteratorMode::Start);
        for _ in iter {
            count += 1;
        }

        Ok(count)
    }

    async fn set_ttl(&self, id: &str, ttl_secs: u64) -> Result<()> {
        let db = self.db.as_ref().ok_or(CoreTexError::StorageNotInitialized)?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| CoreTexError::Internal(e.to_string()))?
            .as_secs();
        let expiry = now.saturating_add(ttl_secs);
        let ttl_key = format!("__ttl__:{}", id);
        db.put(ttl_key.as_bytes(), &expiry.to_le_bytes())
            .map_err(|e| CoreTexError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn remove_ttl(&self, id: &str) -> Result<()> {
        let db = self.db.as_ref().ok_or(CoreTexError::StorageNotInitialized)?;
        let ttl_key = format!("__ttl__:{}", id);
        db.delete(ttl_key.as_bytes())
            .map_err(|e| CoreTexError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn get_ttl(&self, id: &str) -> Result<Option<u64>> {
        let db = self.db.as_ref().ok_or(CoreTexError::StorageNotInitialized)?;
        let ttl_key = format!("__ttl__:{}", id);
        if let Some(data) = db.get(ttl_key.as_bytes())
            .map_err(|e| CoreTexError::StorageError(e.to_string()))? 
        {
            if data.len() < 8 {
                return Ok(None);
            }
            let expiry = u64::from_le_bytes(data[..8].try_into()?);
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|e| CoreTexError::Internal(e.to_string()))?
                .as_secs();
            if expiry > now {
                Ok(Some(expiry - now))
            } else {
                Ok(Some(0))
            }
        } else {
            Ok(None)
        }
    }

    async fn purge_expired(&self) -> Result<usize> {
        let db = self.db.as_ref().ok_or(CoreTexError::StorageNotInitialized)?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| CoreTexError::Internal(e.to_string()))?
            .as_secs();
        let mut purged = 0;

        let iter = db.iterator(rocksdb::IteratorMode::Start);
        let mut to_delete = Vec::new();

        for item in iter {
            let (key, value) = item.map_err(|e| CoreTexError::StorageError(e.to_string()))?;
            let key_str = String::from_utf8(key.to_vec())?;
            if key_str.starts_with("__ttl__:") {
                let data_key = key_str[7..].to_string();
                if value.len() >= 8 {
                    let expiry = u64::from_le_bytes(value[..8].try_into()?);
                    if now >= expiry {
                        to_delete.push(data_key);
                        to_delete.push(key_str);
                    }
                }
            }
        }

        for key in &to_delete {
            db.delete(key.as_bytes())
                .map_err(|e| CoreTexError::StorageError(e.to_string()))?;
            purged += 1;
        }

        Ok(purged / 2)
    }
}

#[cfg(test)]
mod tests {
    include!("tests.rs");
}
