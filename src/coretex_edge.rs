//! Edge Deployment for CortexDB
//! Embedded mode for resource-constrained devices

use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct EdgeDB {
    data_dir: String,
    in_memory: bool,
    collections: Arc<RwLock<std::collections::HashMap<String, EdgeCollection>>>,
    config: EdgeConfig,
}

#[derive(Debug, Clone)]
pub struct EdgeConfig {
    pub max_memory_mb: usize,
    pub max_disk_gb: usize,
    pub cache_size_mb: usize,
    pub enable_compression: bool,
    pub enable_encryption: bool,
}

impl Default for EdgeConfig {
    fn default() -> Self {
        Self {
            max_memory_mb: 256,
            max_disk_gb: 1,
            cache_size_mb: 64,
            enable_compression: false,
            enable_encryption: false,
        }
    }
}

impl EdgeConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_max_memory(mut self, mb: usize) -> Self {
        self.max_memory_mb = mb;
        self
    }

    pub fn with_max_disk(mut self, gb: usize) -> Self {
        self.max_disk_gb = gb;
        self
    }

    pub fn with_cache_size(mut self, mb: usize) -> Self {
        self.cache_size_mb = mb;
        self
    }

    pub fn with_compression(mut self, enabled: bool) -> Self {
        self.enable_compression = enabled;
        self
    }

    pub fn with_encryption(mut self, enabled: bool) -> Self {
        self.enable_encryption = enabled;
        self
    }
}

#[derive(Debug)]
pub struct EdgeCollection {
    pub name: String,
    pub dimension: usize,
    pub vectors: std::collections::HashMap<String, Vec<f32>>,
    pub metadata: std::collections::HashMap<String, serde_json::Value>,
}

impl EdgeDB {
    pub fn new() -> Self {
        Self {
            data_dir: "./data".to_string(),
            in_memory: true,
            collections: Arc::new(RwLock::new(std::collections::HashMap::new())),
            config: EdgeConfig::default(),
        }
    }

    pub fn with_config(config: EdgeConfig) -> Self {
        Self {
            data_dir: "./data".to_string(),
            in_memory: false,
            collections: Arc::new(RwLock::new(std::collections::HashMap::new())),
            config,
        }
    }

    pub fn in_memory() -> Self {
        Self::new()
    }

    pub fn with_data_dir(mut self, dir: &str) -> Self {
        self.data_dir = dir.to_string();
        self.in_memory = false;
        self
    }

    pub async fn init(&self) -> Result<(), EdgeError> {
        if !self.in_memory {
            let path = Path::new(&self.data_dir);
            if !path.exists() {
                std::fs::create_dir_all(path)
                    .map_err(|e| EdgeError::IoError(e.to_string()))?;
            }
        }
        Ok(())
    }

    pub async fn create_collection(&self, name: &str, dimension: usize) -> Result<(), EdgeError> {
        let mut collections = self.collections.write().await;
        
        if collections.contains_key(name) {
            return Err(EdgeError::CollectionExists(name.to_string()));
        }

        collections.insert(name.to_string(), EdgeCollection {
            name: name.to_string(),
            dimension,
            vectors: std::collections::HashMap::new(),
            metadata: std::collections::HashMap::new(),
        });

        Ok(())
    }

    pub async fn delete_collection(&self, name: &str) -> Result<(), EdgeError> {
        let mut collections = self.collections.write().await;
        
        if collections.remove(name).is_none() {
            return Err(EdgeError::CollectionNotFound(name.to_string()));
        }

        Ok(())
    }

    pub async fn list_collections(&self) -> Vec<String> {
        let collections = self.collections.read().await;
        collections.keys().cloned().collect()
    }

    pub async fn insert(&self, collection: &str, id: &str, vector: Vec<f32>, metadata: Option<serde_json::Value>) -> Result<(), EdgeError> {
        let mut collections = self.collections.write().await;
        
        let coll = collections.get_mut(collection)
            .ok_or(EdgeError::CollectionNotFound(collection.to_string()))?;

        if vector.len() != coll.dimension {
            return Err(EdgeError::InvalidDimension(format!(
                "Expected {}, got {}",
                coll.dimension,
                vector.len()
            )));
        }

        coll.vectors.insert(id.to_string(), vector);
        
        if let Some(meta) = metadata {
            coll.metadata.insert(id.to_string(), meta);
        }

        Ok(())
    }

    pub async fn search(&self, collection: &str, query: &[f32], k: usize) -> Result<Vec<EdgeSearchResult>, EdgeError> {
        let collections = self.collections.read().await;
        
        let coll = collections.get(collection)
            .ok_or(EdgeError::CollectionNotFound(collection.to_string()))?;

        let mut results: Vec<EdgeSearchResult> = coll.vectors
            .iter()
            .map(|(id, vector)| {
                let distance = cosine_distance(query, vector);
                EdgeSearchResult {
                    id: id.clone(),
                    distance,
                }
            })
            .collect();

        results.sort_by(|a, b| a.distance.partial_cmp(&b.distance).unwrap());
        results.truncate(k);

        Ok(results)
    }

    pub async fn get(&self, collection: &str, id: &str) -> Result<Option<(Vec<f32>, serde_json::Value)>, EdgeError> {
        let collections = self.collections.read().await;
        
        let coll = collections.get(collection)
            .ok_or(EdgeError::CollectionNotFound(collection.to_string()))?;

        if let Some(vector) = coll.vectors.get(id) {
            let metadata = coll.metadata.get(id).cloned().unwrap_or(serde_json::json!({}));
            Ok(Some((vector.clone(), metadata)))
        } else {
            Ok(None)
        }
    }

    pub async fn delete(&self, collection: &str, id: &str) -> Result<bool, EdgeError> {
        let mut collections = self.collections.write().await;
        
        let coll = collections.get_mut(collection)
            .ok_or(EdgeError::CollectionNotFound(collection.to_string()))?;

        let removed = coll.vectors.remove(id).is_some();
        coll.metadata.remove(id);

        Ok(removed)
    }

    pub async fn get_stats(&self) -> EdgeStats {
        let collections = self.collections.read().await;
        
        let mut total_vectors = 0;
        let mut total_memory_bytes = 0;

        for coll in collections.values() {
            total_vectors += coll.vectors.len();
            total_memory_bytes += coll.vectors.len() * coll.dimension * 4;
        }

        EdgeStats {
            collection_count: collections.len(),
            total_vectors,
            memory_usage_bytes: total_memory_bytes,
            disk_usage_bytes: 0,
        }
    }

    pub async fn flush(&self) -> Result<(), EdgeError> {
        if self.in_memory {
            return Ok(());
        }

        let data_dir = std::path::Path::new(&self.data_dir);
        if !data_dir.exists() {
            std::fs::create_dir_all(data_dir)
                .map_err(|e| EdgeError::IoError(format!("Failed to create data dir: {}", e)))?;
        }

        let collections = self.collections.read().await;

        for (name, coll) in collections.iter() {
            let coll_dir = data_dir.join(name);
            let vectors_dir = coll_dir.join("vectors");
            let metadata_dir = coll_dir.join("metadata");

            std::fs::create_dir_all(&vectors_dir)
                .map_err(|e| EdgeError::IoError(format!("Failed to create vectors dir: {}", e)))?;
            std::fs::create_dir_all(&metadata_dir)
                .map_err(|e| EdgeError::IoError(format!("Failed to create metadata dir: {}", e)))?;

            // Persist each vector as a binary file (f32 little-endian)
            for (id, vector) in coll.vectors.iter() {
                let vector_path = vectors_dir.join(format!("{}.vec", id));
                let bytes: Vec<u8> = vector.iter().flat_map(|f| f.to_le_bytes()).collect();
                std::fs::write(&vector_path, &bytes)
                    .map_err(|e| EdgeError::IoError(format!("Failed to write vector {}: {}", id, e)))?;
            }

            // Persist each metadata entry as JSON
            for (id, meta) in coll.metadata.iter() {
                let meta_path = metadata_dir.join(format!("{}.json", id));
                let json = serde_json::to_vec(meta)
                    .map_err(|e| EdgeError::IoError(format!("Failed to serialize metadata: {}", e)))?;
                std::fs::write(&meta_path, json)
                    .map_err(|e| EdgeError::IoError(format!("Failed to write metadata {}: {}", id, e)))?;
            }

            // Write collection manifest (name + dimension)
            let manifest = serde_json::json!({
                "name": coll.name,
                "dimension": coll.dimension,
                "vector_count": coll.vectors.len(),
            });
            let manifest_path = coll_dir.join("manifest.json");
            let manifest_bytes = serde_json::to_vec_pretty(&manifest)
                .map_err(|e| EdgeError::IoError(format!("Failed to serialize manifest: {}", e)))?;
            std::fs::write(&manifest_path, manifest_bytes)
                .map_err(|e| EdgeError::IoError(format!("Failed to write manifest: {}", e)))?;
        }

        // fsync the data directory to ensure durability
        Self::sync_dir(data_dir)?;

        Ok(())
    }

    pub async fn close(&self) -> Result<(), EdgeError> {
        self.flush().await
    }

    /// Fsync a directory to guarantee durability of writes.
    fn sync_dir(dir: &std::path::Path) -> Result<(), EdgeError> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            let file = std::fs::OpenOptions::new()
                .read(true)
                .open(dir)
                .map_err(|e| EdgeError::IoError(format!("Failed to open dir for sync: {}", e)))?;
            file.sync_all()
                .map_err(|e| EdgeError::IoError(format!("fsync directory failed: {}", e)))?;
        }
        #[cfg(not(unix))]
        {
            // On Windows, sync each file in the directory
            for entry in std::fs::read_dir(dir)
                .map_err(|e| EdgeError::IoError(format!("Failed to read dir for sync: {}", e)))?
            {
                let entry = entry.map_err(|e| EdgeError::IoError(e.to_string()))?;
                let path = entry.path();
                if path.is_file() {
                    let f = std::fs::File::open(&path)
                        .map_err(|e| EdgeError::IoError(format!("Failed to open file for sync: {}", e)))?;
                    f.sync_all()
                        .map_err(|e| EdgeError::IoError(format!("fsync file failed: {}", e)))?;
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct EdgeSearchResult {
    pub id: String,
    pub distance: f32,
}

#[derive(Debug, Clone)]
pub struct EdgeStats {
    pub collection_count: usize,
    pub total_vectors: usize,
    pub memory_usage_bytes: usize,
    pub disk_usage_bytes: usize,
}

#[derive(Debug)]
pub enum EdgeError {
    CollectionNotFound(String),
    CollectionExists(String),
    InvalidDimension(String),
    IoError(String),
    OutOfMemory,
}

impl std::fmt::Display for EdgeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EdgeError::CollectionNotFound(name) => {
                write!(f, "Collection not found: {}", name)
            },
            EdgeError::CollectionExists(name) => {
                write!(f, "Collection already exists: {}", name)
            },
            EdgeError::InvalidDimension(msg) => {
                write!(f, "Invalid dimension: {}", msg)
            },
            EdgeError::IoError(msg) => {
                write!(f, "IO error: {}", msg)
            },
            EdgeError::OutOfMemory => {
                write!(f, "Out of memory")
            },
        }
    }
}

impl std::error::Error for EdgeError {}

fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm_a == 0.0 || norm_b == 0.0 {
        return 1.0;
    }
    1.0 - (dot / (norm_a * norm_b))
}

#[cfg(feature = "embedded")]
pub mod embedded {
    use super::*;
    
    pub type CortexDb = EdgeDB;
    
    pub fn new_embedded() -> CortexDb {
        EdgeDB::in_memory()
    }
    
    pub fn new_embedded_with_config(config: EdgeConfig) -> CortexDb {
        EdgeDB::with_config(config)
    }
}

#[cfg(feature = "wasm")]
pub mod wasm {
    use super::*;
use crate::coretex_core::Result;
    
    pub struct WasmDB {
        collections: std::collections::HashMap<String, EdgeCollection>,
    }
    
    impl WasmDB {
        pub fn new() -> Self {
            Self {
                collections: std::collections::HashMap::new(),
            }
        }
        
        pub fn create_collection(&mut self, name: &str, dimension: usize) -> Result<(), EdgeError> {
            if self.collections.contains_key(name) {
                return Err(EdgeError::CollectionExists(name.to_string()));
            }
            
            self.collections.insert(name.to_string(), EdgeCollection {
                name: name.to_string(),
                dimension,
                vectors: std::collections::HashMap::new(),
                metadata: std::collections::HashMap::new(),
            });
            
            Ok(())
        }
        
        pub fn insert(&mut self, collection: &str, id: &str, vector: Vec<f32>) -> Result<(), EdgeError> {
            let coll = self.collections.get_mut(collection)
                .ok_or(EdgeError::CollectionNotFound(collection.to_string()))?;
            
            coll.vectors.insert(id.to_string(), vector);
            Ok(())
        }
        
        pub fn search(&self, collection: &str, query: &[f32], k: usize) -> Result<Vec<EdgeSearchResult>, EdgeError> {
            let coll = self.collections.get(collection)
                .ok_or(EdgeError::CollectionNotFound(collection.to_string()))?;
            
            let mut results: Vec<EdgeSearchResult> = coll.vectors
                .iter()
                .map(|(id, vector)| {
                    let distance = cosine_distance(query, vector);
                    EdgeSearchResult {
                        id: id.clone(),
                        distance,
                    }
                })
                .collect();
            
            results.sort_by(|a, b| a.distance.partial_cmp(&b.distance).unwrap());
            results.truncate(k);
            
            Ok(results)
        }
    }
}
