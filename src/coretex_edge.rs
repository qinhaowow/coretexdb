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
        Ok(())
    }

    pub async fn close(&self) -> Result<(), EdgeError> {
        self.flush().await
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
