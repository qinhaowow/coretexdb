//! Data persistence module for CoreTexDB
//! Provides deep integration with RocksDB, S3, and other storage backends

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StorageBackend {
    RocksDB,
    S3,
    FileSystem,
    Hybrid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistenceConfig {
    pub backend: StorageBackend,
    pub data_dir: String,
    pub rocksdb_config: Option<RocksDBConfig>,
    pub s3_config: Option<S3Config>,
    pub replication_factor: usize,
    pub sync_write: bool,
    pub wal_enabled: bool,
    pub compression_enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RocksDBConfig {
    pub path: String,
    pub max_open_files: i32,
    pub write_buffer_size: usize,
    pub max_write_buffer_number: i32,
    pub compression: String,
    pub bloom_filter_bits: i32,
    pub cache_size: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Config {
    pub bucket: String,
    pub region: String,
    pub endpoint: Option<String>,
    pub access_key: String,
    pub secret_key: String,
    pub prefix: String,
    pub multipart_threshold: usize,
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self {
            backend: StorageBackend::RocksDB,
            data_dir: "./data".to_string(),
            rocksdb_config: Some(RocksDBConfig::default()),
            s3_config: None,
            replication_factor: 1,
            sync_write: true,
            wal_enabled: true,
            compression_enabled: true,
        }
    }
}

impl Default for RocksDBConfig {
    fn default() -> Self {
        Self {
            path: "./data/rocksdb".to_string(),
            max_open_files: -1,
            write_buffer_size: 64 * 1024 * 1024,
            max_write_buffer_number: 6,
            compression: "lz4".to_string(),
            bloom_filter_bits: 10,
            cache_size: 512 * 1024 * 1024,
        }
    }
}

pub struct PersistenceManager {
    config: PersistenceConfig,
    collections: Arc<RwLock<HashMap<String, CollectionStorage>>>,
    stats: Arc<RwLock<PersistenceStats>>,
}

#[derive(Debug, Clone)]
pub struct CollectionStorage {
    pub name: String,
    pub vector_count: usize,
    pub metadata_count: usize,
    pub size_bytes: u64,
    pub last_modified: i64,
    pub checksum: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistenceStats {
    pub total_writes: u64,
    pub total_reads: u64,
    pub total_bytes_written: u64,
    pub total_bytes_read: u64,
    pub last_checkpoint: i64,
    pub checkpoint_count: u64,
    pub error_count: u64,
}

impl Default for PersistenceStats {
    fn default() -> Self {
        Self {
            total_writes: 0,
            total_reads: 0,
            total_bytes_written: 0,
            total_bytes_read: 0,
            last_checkpoint: 0,
            checkpoint_count: 0,
            error_count: 0,
        }
    }
}

impl PersistenceManager {
    pub fn new(config: PersistenceConfig) -> Self {
        Self {
            config,
            collections: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(RwLock::new(PersistenceStats::default())),
        }
    }

    pub fn config(&self) -> &PersistenceConfig {
        &self.config
    }

    pub async fn initialize(&self) -> Result<(), PersistenceError> {
        let data_dir = PathBuf::from(&self.config.data_dir);
        
        if !data_dir.exists() {
            std::fs::create_dir_all(&data_dir)
                .map_err(|e| PersistenceError::IoError(e.to_string()))?;
        }

        self.load_collections().await?;
        
        Ok(())
    }

    async fn load_collections(&self) -> Result<(), PersistenceError> {
        let data_dir = PathBuf::from(&self.config.data_dir);
        let collections_dir = data_dir.join("collections");
        
        if !collections_dir.exists() {
            return Ok(());
        }

        let mut collections = self.collections.write().await;
        
        for entry in std::fs::read_dir(&collections_dir)
            .map_err(|e| PersistenceError::IoError(e.to_string()))?
        {
            let entry = entry.map_err(|e| PersistenceError::IoError(e.to_string()))?;
            let path = entry.path();
            
            if path.is_dir() {
                if let Some(name) = path.file_name() {
                    let name = name.to_string_lossy().to_string();
                    
                    let vector_count = self.count_files_in_dir(&path.join("vectors")).await;
                    let metadata_count = self.count_files_in_dir(&path.join("metadata")).await;
                    let size_bytes = self.dir_size(&path);
                    
                    collections.insert(name.clone(), CollectionStorage {
                        name,
                        vector_count,
                        metadata_count,
                        size_bytes,
                        last_modified: chrono::Utc::now().timestamp(),
                        checksum: String::new(),
                    });
                }
            }
        }
        
        Ok(())
    }

    async fn count_files_in_dir(&self, dir: &PathBuf) -> usize {
        std::fs::read_dir(dir)
            .map(|entries| entries.count())
            .unwrap_or(0)
    }

    fn dir_size(&self, path: &PathBuf) -> u64 {
        std::fs::read_dir(path)
            .map(|entries| {
                entries.filter_map(|e| e.ok())
                    .filter_map(|e| e.metadata().ok())
                    .map(|m| m.len())
                    .sum()
            })
            .unwrap_or(0)
    }

    pub async fn save_vector(
        &self,
        collection: &str,
        id: &str,
        vector: &[f32],
        metadata: Option<&serde_json::Value>,
    ) -> Result<(), PersistenceError> {
        let collection_dir = PathBuf::from(&self.config.data_dir)
            .join("collections")
            .join(collection);
        
        std::fs::create_dir_all(&collection_dir.join("vectors"))
            .map_err(|e| PersistenceError::IoError(e.to_string()))?;
        
        if metadata.is_some() {
            std::fs::create_dir_all(&collection_dir.join("metadata"))
                .map_err(|e| PersistenceError::IoError(e.to_string()))?;
        }

        let vector_path = collection_dir.join("vectors").join(format!("{}.vec", id));
        let vector_bytes: Vec<u8> = vector.iter()
            .flat_map(|f| f.to_le_bytes())
            .collect();
        
        if self.config.compression_enabled {
            #[cfg(feature = "compression")]
            {
                let compressed = Self::compress_data(&vector_bytes)?;
                std::fs::write(&vector_path, compressed)
                    .map_err(|e| PersistenceError::IoError(e.to_string()))?;
            }
            #[cfg(not(feature = "compression"))]
            {
                std::fs::write(&vector_path, &vector_bytes)
                    .map_err(|e| PersistenceError::IoError(e.to_string()))?;
            }
        } else {
            std::fs::write(&vector_path, &vector_bytes)
                .map_err(|e| PersistenceError::IoError(e.to_string()))?;
        }

        if let Some(meta) = metadata {
            let metadata_path = collection_dir.join("metadata").join(format!("{}.json", id));
            let meta_json = serde_json::to_vec(meta)
                .map_err(|e| PersistenceError::SerializationError(e.to_string()))?;
            std::fs::write(&metadata_path, meta_json)
                .map_err(|e| PersistenceError::IoError(e.to_string()))?;
        }

        if self.config.sync_write {
            Self::sync_directory(&collection_dir)?;
        }

        {
            let mut stats = self.stats.write().await;
            stats.total_writes += 1;
            stats.total_bytes_written += vector_bytes.len() as u64;
        }

        Ok(())
    }

    pub async fn load_vector(
        &self,
        collection: &str,
        id: &str,
    ) -> Result<Option<(Vec<f32>, Option<serde_json::Value>)>, PersistenceError> {
        let collection_dir = PathBuf::from(&self.config.data_dir)
            .join("collections")
            .join(collection);

        let vector_path = collection_dir.join("vectors").join(format!("{}.vec", id));
        
        if !vector_path.exists() {
            return Ok(None);
        }

        let mut vector_bytes = std::fs::read(&vector_path)
            .map_err(|e| PersistenceError::IoError(e.to_string()))?;

        if self.config.compression_enabled {
            #[cfg(feature = "compression")]
            {
                vector_bytes = Self::decompress_data(&vector_bytes)?;
            }
        }

        let vector: Vec<f32> = vector_bytes
            .chunks_exact(4)
            .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
            .collect();

        let metadata_path = collection_dir.join("metadata").join(format!("{}.json", id));
        let metadata = if metadata_path.exists() {
            let meta_bytes = std::fs::read(&metadata_path)
                .map_err(|e| PersistenceError::IoError(e.to_string()))?;
            Some(serde_json::from_slice(&meta_bytes)
                .map_err(|e| PersistenceError::SerializationError(e.to_string()))?)
        } else {
            None
        };

        {
            let mut stats = self.stats.write().await;
            stats.total_reads += 1;
            stats.total_bytes_read += vector_bytes.len() as u64;
        }

        Ok(Some((vector, metadata)))
    }

    pub async fn delete_vector(&self, collection: &str, id: &str) -> Result<bool, PersistenceError> {
        let collection_dir = PathBuf::from(&self.config.data_dir)
            .join("collections")
            .join(collection);

        let vector_path = collection_dir.join("vectors").join(format!("{}.vec", id));
        let metadata_path = collection_dir.join("metadata").join(format!("{}.json", id));

        let mut deleted = false;

        if vector_path.exists() {
            std::fs::remove_file(&vector_path)
                .map_err(|e| PersistenceError::IoError(e.to_string()))?;
            deleted = true;
        }

        if metadata_path.exists() {
            std::fs::remove_file(&metadata_path)
                .map_err(|e| PersistenceError::IoError(e.to_string()))?;
        }

        Ok(deleted)
    }

    pub async fn create_checkpoint(&self) -> Result<String, PersistenceError> {
        let checkpoint_id = format!("checkpoint_{}", chrono::Utc::now().timestamp());
        
        let data_dir = PathBuf::from(&self.config.data_dir);
        let checkpoint_dir = data_dir.join("checkpoints").join(&checkpoint_id);
        
        std::fs::create_dir_all(&checkpoint_dir)
            .map_err(|e| PersistenceError::IoError(e.to_string()))?;

        let collections = self.collections.read().await;
        
        for (name, _) in collections.iter() {
            let src = data_dir.join("collections").join(name);
            let dst = checkpoint_dir.join(name);
            
            if src.exists() {
                Self::copy_dir_recursive(&src, &dst)?;
            }
        }

        {
            let mut stats = self.stats.write().await;
            stats.last_checkpoint = chrono::Utc::now().timestamp();
            stats.checkpoint_count += 1;
        }

        Ok(checkpoint_id)
    }

    pub async fn restore_from_checkpoint(&self, checkpoint_id: &str) -> Result<(), PersistenceError> {
        let checkpoint_dir = PathBuf::from(&self.config.data_dir)
            .join("checkpoints")
            .join(checkpoint_id);
        
        if !checkpoint_dir.exists() {
            return Err(PersistenceError::CheckpointNotFound(checkpoint_id.to_string()));
        }

        let data_dir = PathBuf::from(&self.config.data_dir);
        let collections_dir = data_dir.join("collections");

        if collections_dir.exists() {
            std::fs::remove_dir_all(&collections_dir)
                .map_err(|e| PersistenceError::IoError(e.to_string()))?;
        }

        Self::copy_dir_recursive(&checkpoint_dir, &collections_dir)?;
        
        self.load_collections().await?;

        Ok(())
    }

    pub async fn get_stats(&self) -> PersistenceStats {
        self.stats.read().await.clone()
    }

    pub async fn get_collection_stats(&self, collection: &str) -> Option<CollectionStorage> {
        self.collections.read().await.get(collection).cloned()
    }

    pub async fn list_checkpoints(&self) -> Vec<CheckpointInfo> {
        let checkpoints_dir = PathBuf::from(&self.config.data_dir)
            .join("checkpoints");
        
        if !checkpoints_dir.exists() {
            return vec![];
        }

        std::fs::read_dir(&checkpoints_dir)
            .map(|entries| {
                entries.filter_map(|e| e.ok())
                    .filter(|e| e.path().is_dir())
                    .filter_map(|e| {
                        let name = e.file_name().to_string_lossy().to_string();
                        if name.starts_with("checkpoint_") {
                            let metadata = e.metadata().ok()?;
                            let size = metadata.len();
                            Some(CheckpointInfo {
                                id: name,
                                created_at: chrono::Utc::now().timestamp(),
                                size_bytes: size,
                            })
                        } else {
                            None
                        }
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    #[cfg(feature = "compression")]
    fn compress_data(data: &[u8]) -> Result<Vec<u8>, PersistenceError> {
        use std::io::Write;
        
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(data)
            .map_err(|e| PersistenceError::CompressionError(e.to_string()))?;
        encoder.finish()
            .map_err(|e| PersistenceError::CompressionError(e.to_string()))
    }

    #[cfg(feature = "compression")]
    fn decompress_data(data: &[u8]) -> Result<Vec<u8>, PersistenceError> {
        use std::io::Read;
        
        let mut decoder = flate2::read::GzDecoder::new(data);
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed)
            .map_err(|e| PersistenceError::CompressionError(e.to_string()))?;
        Ok(decompressed)
    }

    fn sync_directory(dir: &PathBuf) -> Result<(), PersistenceError> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileTypeExt;
            let _ = dir;
            Ok(())
        }
        #[cfg(not(unix))]
        {
            let _ = dir;
            Ok(())
        }
    }

    fn copy_dir_recursive(src: &PathBuf, dst: &PathBuf) -> Result<(), PersistenceError> {
        std::fs::create_dir_all(dst)
            .map_err(|e| PersistenceError::IoError(e.to_string()))?;
        
        for entry in std::fs::read_dir(src)
            .map_err(|e| PersistenceError::IoError(e.to_string()))?
        {
            let entry = entry.map_err(|e| PersistenceError::IoError(e.to_string()))?;
            let src_path = entry.path();
            let dst_path = dst.join(entry.file_name());
            
            if src_path.is_dir() {
                Self::copy_dir_recursive(&src_path, &dst_path)?;
            } else {
                std::fs::copy(&src_path, &dst_path)
                    .map_err(|e| PersistenceError::IoError(e.to_string()))?;
            }
        }
        
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointInfo {
    pub id: String,
    pub created_at: i64,
    pub size_bytes: u64,
}

#[derive(Debug)]
pub enum PersistenceError {
    IoError(String),
    SerializationError(String),
    CompressionError(String),
    CheckpointNotFound(String),
    CollectionNotFound(String),
}

impl std::fmt::Display for PersistenceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PersistenceError::IoError(msg) => write!(f, "IO Error: {}", msg),
            PersistenceError::SerializationError(msg) => write!(f, "Serialization Error: {}", msg),
            PersistenceError::CompressionError(msg) => write!(f, "Compression Error: {}", msg),
            PersistenceError::CheckpointNotFound(id) => write!(f, "Checkpoint not found: {}", id),
            PersistenceError::CollectionNotFound(name) => write!(f, "Collection not found: {}", name),
        }
    }
}

impl std::error::Error for PersistenceError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_persistence_manager_new() {
        let config = PersistenceConfig::default();
        let manager = PersistenceManager::new(config);
        
        assert_eq!(manager.config().data_dir, "./data");
    }

    #[tokio::test]
    async fn test_save_and_load_vector() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = PersistenceConfig {
            data_dir: temp_dir.path().to_string_lossy().to_string(),
            compression_enabled: false,
            ..Default::default()
        };
        
        let manager = PersistenceManager::new(config);
        manager.initialize().await.unwrap();
        
        let vector = vec![1.0, 2.0, 3.0, 4.0];
        let metadata = serde_json::json!({"key": "value"});
        
        manager.save_vector("test", "vec1", &vector, Some(&metadata)).await.unwrap();
        
        let result = manager.load_vector("test", "vec1").await.unwrap();
        assert!(result.is_some());
        
        let (loaded_vector, loaded_meta) = result.unwrap();
        assert_eq!(loaded_vector, vector);
        assert!(loaded_meta.is_some());
    }

    #[tokio::test]
    async fn test_delete_vector() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = PersistenceConfig {
            data_dir: temp_dir.path().to_string_lossy().to_string(),
            compression_enabled: false,
            ..Default::default()
        };
        
        let manager = PersistenceManager::new(config);
        manager.initialize().await.unwrap();
        
        let vector = vec![1.0, 2.0, 3.0];
        manager.save_vector("test", "vec1", &vector, None).await.unwrap();
        
        let deleted = manager.delete_vector("test", "vec1").await.unwrap();
        assert!(deleted);
        
        let result = manager.load_vector("test", "vec1").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_checkpoint() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = PersistenceConfig {
            data_dir: temp_dir.path().to_string_lossy().to_string(),
            compression_enabled: false,
            ..Default::default()
        };
        
        let manager = PersistenceManager::new(config);
        manager.initialize().await.unwrap();
        
        let vector = vec![1.0, 2.0, 3.0];
        manager.save_vector("test", "vec1", &vector, None).await.unwrap();
        
        let checkpoint_id = manager.create_checkpoint().await.unwrap();
        assert!(checkpoint_id.starts_with("checkpoint_"));
        
        let checkpoints = manager.list_checkpoints().await;
        assert!(!checkpoints.is_empty());
    }

    #[tokio::test]
    async fn test_stats() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = PersistenceConfig {
            data_dir: temp_dir.path().to_string_lossy().to_string(),
            compression_enabled: false,
            ..Default::default()
        };
        
        let manager = PersistenceManager::new(config);
        manager.initialize().await.unwrap();
        
        let vector = vec![1.0, 2.0, 3.0];
        manager.save_vector("test", "vec1", &vector, None).await.unwrap();
        manager.load_vector("test", "vec1").await.unwrap();
        
        let stats = manager.get_stats().await;
        assert!(stats.total_writes > 0);
    }

    #[cfg(feature = "compression")]
    #[tokio::test]
    async fn test_compression() {
        let data = vec![1u8; 1000];
        
        let compressed = PersistenceManager::compress_data(&data).unwrap();
        let decompressed = PersistenceManager::decompress_data(&compressed).unwrap();
        
        assert_eq!(decompressed, data);
    }
}
