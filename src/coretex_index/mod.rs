//! Vector indexing for CortexDB

use async_trait::async_trait;
use std::error::Error;

/// Result of a vector search
#[derive(Debug)]
pub struct SearchResult {
    /// ID of the matched vector
    pub id: String,
    
    /// Distance from the query vector
    pub distance: f32,
}

/// Vector index trait
#[async_trait]
pub trait VectorIndex: Send + Sync {
    /// Add a vector to the index
    async fn add(&self, id: &str, vector: &[f32]) -> Result<(), Box<dyn Error>>;
    
    /// Remove a vector from the index
    async fn remove(&self, id: &str) -> Result<bool, Box<dyn Error>>;
    
    /// Search for similar vectors
    async fn search(&self, query: &[f32], k: usize) -> Result<Vec<SearchResult>, Box<dyn Error>>;
    
    /// Build the index (if needed)
    async fn build(&self) -> Result<(), Box<dyn Error>>;
    
    /// Clear the index
    async fn clear(&self) -> Result<(), Box<dyn Error>>;
}

/// Brute-force index implementation
pub struct BruteForceIndex {
    vectors: std::sync::Arc<tokio::sync::RwLock<std::collections::HashMap<String, Vec<f32>>>>,
    metric: String,
}

/// HNSW (Hierarchical Navigable Small World) index implementation
pub struct HNSWIndex {
    vectors: std::sync::Arc<tokio::sync::RwLock<std::collections::HashMap<String, Vec<f32>>>>,
    metric: String,
    // HNSW-specific parameters
    m: usize, // Maximum number of connections per node
    ef_construction: usize, // Size of the dynamic candidate list during construction
    ef_search: usize, // Size of the dynamic candidate list during search
    max_level: usize, // Maximum level of the graph
}

/// IVF (Inverted File) index implementation
pub struct IVFIndex {
    vectors: std::sync::Arc<tokio::sync::RwLock<std::collections::HashMap<String, Vec<f32>>>>,
    metric: String,
    // IVF-specific parameters
    nlist: usize, // Number of clusters
    nprobe: usize, // Number of clusters to probe during search
    centroids: std::sync::Arc<tokio::sync::RwLock<Vec<Vec<f32>>>>, // Cluster centroids
    vector_to_cluster: std::sync::Arc<tokio::sync::RwLock<std::collections::HashMap<String, usize>>>, // Mapping from vector ID to cluster ID
}

/// Scalar index implementation for numerical values
pub struct ScalarIndex {
    scalars: std::sync::Arc<tokio::sync::RwLock<std::collections::HashMap<String, f32>>>,
    sorted_scalars: std::sync::Arc<tokio::sync::RwLock<Vec<(f32, String)>>>, // Sorted list of (value, ID) pairs
}

impl BruteForceIndex {
    /// Create a new brute-force index with the specified distance metric
    pub fn new(metric: &str) -> Self {
        Self {
            vectors: std::sync::Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
            metric: metric.to_string(),
        }
    }
    
    /// Calculate distance between two vectors
    fn calculate_distance(&self, a: &[f32], b: &[f32]) -> f32 {
        match self.metric.as_str() {
            "cosine" => {
                // Cosine similarity (higher is better, so we return 1 - similarity for distance)
                let dot_product: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
                let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
                let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
                
                if norm_a == 0.0 || norm_b == 0.0 {
                    return 1.0;
                }
                
                1.0 - (dot_product / (norm_a * norm_b))
            },
            "euclidean" => {
                // Euclidean distance
                a.iter().zip(b.iter())
                    .map(|(x, y)| (x - y).powi(2))
                    .sum::<f32>()
                    .sqrt()
            },
            _ => {
                // Default to cosine
                let dot_product: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
                let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
                let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
                
                if norm_a == 0.0 || norm_b == 0.0 {
                    return 1.0;
                }
                
                1.0 - (dot_product / (norm_a * norm_b))
            }
        }
    }
}

impl HNSWIndex {
    /// Create a new HNSW index with the specified parameters
    pub fn new(metric: &str) -> Self {
        Self {
            vectors: std::sync::Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
            metric: metric.to_string(),
            m: 16, // Default maximum number of connections per node
            ef_construction: 200, // Default size of candidate list during construction
            ef_search: 50, // Default size of candidate list during search
            max_level: 16, // Default maximum level
        }
    }
    
    /// Calculate distance between two vectors
    fn calculate_distance(&self, a: &[f32], b: &[f32]) -> f32 {
        match self.metric.as_str() {
            "cosine" => {
                // Cosine similarity (higher is better, so we return 1 - similarity for distance)
                let dot_product: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
                let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
                let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
                
                if norm_a == 0.0 || norm_b == 0.0 {
                    return 1.0;
                }
                
                1.0 - (dot_product / (norm_a * norm_b))
            },
            "euclidean" => {
                // Euclidean distance
                a.iter().zip(b.iter())
                    .map(|(x, y)| (x - y).powi(2))
                    .sum::<f32>()
                    .sqrt()
            },
            _ => {
                // Default to cosine
                let dot_product: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
                let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
                let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
                
                if norm_a == 0.0 || norm_b == 0.0 {
                    return 1.0;
                }
                
                1.0 - (dot_product / (norm_a * norm_b))
            }
        }
    }
    
    /// Select the top k nearest neighbors from a list of candidates
    fn select_neighbors(&self, candidates: &[(String, Vec<f32>)], query: &[f32], k: usize) -> Vec<SearchResult> {
        let mut results: Vec<SearchResult> = candidates
            .iter()
            .map(|(id, vec)| {
                let distance = self.calculate_distance(query, vec);
                SearchResult {
                    id: id.clone(),
                    distance,
                }
            })
            .collect();
        
        // Sort by distance (ascending)
        results.sort_by(|a, b| a.distance.partial_cmp(&b.distance).unwrap());
        
        // Take top k results
        results.into_iter().take(k).collect()
    }
}

impl IVFIndex {
    /// Create a new IVF index with the specified parameters
    pub fn new(metric: &str) -> Self {
        Self {
            vectors: std::sync::Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
            metric: metric.to_string(),
            nlist: 100, // Default number of clusters
            nprobe: 10, // Default number of clusters to probe
            centroids: std::sync::Arc::new(tokio::sync::RwLock::new(Vec::new())),
            vector_to_cluster: std::sync::Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
        }
    }
    
    /// Calculate distance between two vectors
    fn calculate_distance(&self, a: &[f32], b: &[f32]) -> f32 {
        match self.metric.as_str() {
            "cosine" => {
                // Cosine similarity (higher is better, so we return 1 - similarity for distance)
                let dot_product: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
                let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
                let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
                
                if norm_a == 0.0 || norm_b == 0.0 {
                    return 1.0;
                }
                
                1.0 - (dot_product / (norm_a * norm_b))
            },
            "euclidean" => {
                // Euclidean distance
                a.iter().zip(b.iter())
                    .map(|(x, y)| (x - y).powi(2))
                    .sum::<f32>()
                    .sqrt()
            },
            _ => {
                // Default to cosine
                let dot_product: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
                let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
                let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
                
                if norm_a == 0.0 || norm_b == 0.0 {
                    return 1.0;
                }
                
                1.0 - (dot_product / (norm_a * norm_b))
            }
        }
    }
    
    /// Assign a vector to the nearest centroid
    fn assign_to_cluster(&self, vector: &[f32]) -> usize {
        let centroids = self.centroids.read().unwrap();
        if centroids.is_empty() {
            return 0;
        }
        
        let mut min_distance = f32::MAX;
        let mut closest_cluster = 0;
        
        for (i, centroid) in centroids.iter().enumerate() {
            let distance = self.calculate_distance(vector, centroid);
            if distance < min_distance {
                min_distance = distance;
                closest_cluster = i;
            }
        }
        
        closest_cluster
    }
}

impl ScalarIndex {
    /// Create a new scalar index
    pub fn new() -> Self {
        Self {
            scalars: std::sync::Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
            sorted_scalars: std::sync::Arc::new(tokio::sync::RwLock::new(Vec::new())),
        }
    }
    
    /// Update the sorted list of scalars
    fn update_sorted(&self) {
        let scalars = self.scalars.read().unwrap();
        let mut sorted = scalars.iter()
            .map(|(id, value)| (*value, id.clone()))
            .collect::<Vec<_>>();
        
        sorted.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        
        let mut sorted_scalars = self.sorted_scalars.write().unwrap();
        *sorted_scalars = sorted;
    }
}

#[async_trait]
impl VectorIndex for BruteForceIndex {
    async fn add(&self, id: &str, vector: &[f32]) -> Result<(), Box<dyn Error>> {
        let mut vectors = self.vectors.write().await;
        vectors.insert(id.to_string(), vector.to_vec());
        Ok(())
    }
    
    async fn remove(&self, id: &str) -> Result<bool, Box<dyn Error>> {
        let mut vectors = self.vectors.write().await;
        Ok(vectors.remove(id).is_some())
    }
    
    async fn search(&self, query: &[f32], k: usize) -> Result<Vec<SearchResult>, Box<dyn Error>> {
        let vectors = self.vectors.read().await;
        
        let mut results: Vec<SearchResult> = vectors
            .iter()
            .map(|(id, vec)| {
                let distance = self.calculate_distance(query, vec);
                SearchResult {
                    id: id.clone(),
                    distance,
                }
            })
            .collect();
        
        // Sort by distance (ascending)
        results.sort_by(|a, b| a.distance.partial_cmp(&b.distance).unwrap());
        
        // Take top k results
        Ok(results.into_iter().take(k).collect())
    }
    
    async fn build(&self) -> Result<(), Box<dyn Error>> {
        // Brute-force index doesn't need building
        Ok(())
    }
    
    async fn clear(&self) -> Result<(), Box<dyn Error>> {
        let mut vectors = self.vectors.write().await;
        vectors.clear();
        Ok(())
    }
}

#[async_trait]
impl VectorIndex for HNSWIndex {
    async fn add(&self, id: &str, vector: &[f32]) -> Result<(), Box<dyn Error>> {
        let mut vectors = self.vectors.write().await;
        vectors.insert(id.to_string(), vector.to_vec());
        Ok(())
    }
    
    async fn remove(&self, id: &str) -> Result<bool, Box<dyn Error>> {
        let mut vectors = self.vectors.write().await;
        Ok(vectors.remove(id).is_some())
    }
    
    async fn search(&self, query: &[f32], k: usize) -> Result<Vec<SearchResult>, Box<dyn Error>> {
        let vectors = self.vectors.read().await;
        
        // For simplicity, we'll use a brute-force approach here
        // In a real HNSW implementation, we would use the graph structure for efficient search
        let mut results: Vec<SearchResult> = vectors
            .iter()
            .map(|(id, vec)| {
                let distance = self.calculate_distance(query, vec);
                SearchResult {
                    id: id.clone(),
                    distance,
                }
            })
            .collect();
        
        // Sort by distance (ascending)
        results.sort_by(|a, b| a.distance.partial_cmp(&b.distance).unwrap());
        
        // Take top k results
        Ok(results.into_iter().take(k).collect())
    }
    
    async fn build(&self) -> Result<(), Box<dyn Error>> {
        // In a real HNSW implementation, we would build the graph structure here
        // For now, we'll just return Ok(())
        Ok(())
    }
    
    async fn clear(&self) -> Result<(), Box<dyn Error>> {
        let mut vectors = self.vectors.write().await;
        vectors.clear();
        Ok(())
    }
}

#[async_trait]
impl VectorIndex for IVFIndex {
    async fn add(&self, id: &str, vector: &[f32]) -> Result<(), Box<dyn Error>> {
        let mut vectors = self.vectors.write().await;
        vectors.insert(id.to_string(), vector.to_vec());
        
        // Assign to cluster
        let cluster_id = self.assign_to_cluster(vector);
        let mut vector_to_cluster = self.vector_to_cluster.write().await;
        vector_to_cluster.insert(id.to_string(), cluster_id);
        
        Ok(())
    }
    
    async fn remove(&self, id: &str) -> Result<bool, Box<dyn Error>> {
        let mut vectors = self.vectors.write().await;
        let removed = vectors.remove(id).is_some();
        
        if removed {
            let mut vector_to_cluster = self.vector_to_cluster.write().await;
            vector_to_cluster.remove(id);
        }
        
        Ok(removed)
    }
    
    async fn search(&self, query: &[f32], k: usize) -> Result<Vec<SearchResult>, Box<dyn Error>> {
        let vectors = self.vectors.read().await;
        let vector_to_cluster = self.vector_to_cluster.read().await;
        
        // For simplicity, we'll use a brute-force approach here
        // In a real IVF implementation, we would first find the nearest clusters
        // and then only search within those clusters
        let mut results: Vec<SearchResult> = vectors
            .iter()
            .map(|(id, vec)| {
                let distance = self.calculate_distance(query, vec);
                SearchResult {
                    id: id.clone(),
                    distance,
                }
            })
            .collect();
        
        // Sort by distance (ascending)
        results.sort_by(|a, b| a.distance.partial_cmp(&b.distance).unwrap());
        
        // Take top k results
        Ok(results.into_iter().take(k).collect())
    }
    
    async fn build(&self) -> Result<(), Box<dyn Error>> {
        // In a real IVF implementation, we would run k-means clustering here
        // to compute the centroids
        Ok(())
    }
    
    async fn clear(&self) -> Result<(), Box<dyn Error>> {
        let mut vectors = self.vectors.write().await;
        vectors.clear();
        
        let mut vector_to_cluster = self.vector_to_cluster.write().await;
        vector_to_cluster.clear();
        
        Ok(())
    }
}

#[async_trait]
impl VectorIndex for ScalarIndex {
    async fn add(&self, id: &str, vector: &[f32]) -> Result<(), Box<dyn Error>> {
        // For scalar index, we'll use the first element of the vector as the scalar value
        if vector.is_empty() {
            return Err("Vector must not be empty for scalar index".into());
        }
        
        let scalar = vector[0];
        let mut scalars = self.scalars.write().await;
        scalars.insert(id.to_string(), scalar);
        
        // Update sorted list
        self.update_sorted();
        
        Ok(())
    }
    
    async fn remove(&self, id: &str) -> Result<bool, Box<dyn Error>> {
        let mut scalars = self.scalars.write().await;
        let removed = scalars.remove(id).is_some();
        
        if removed {
            // Update sorted list
            self.update_sorted();
        }
        
        Ok(removed)
    }
    
    async fn search(&self, query: &[f32], k: usize) -> Result<Vec<SearchResult>, Box<dyn Error>> {
        // For scalar index, we'll use the first element of the query vector as the target value
        if query.is_empty() {
            return Err("Query vector must not be empty for scalar index".into());
        }
        
        let target = query[0];
        let sorted_scalars = self.sorted_scalars.read().await;
        
        // Find the nearest neighbors using binary search
        let mut results: Vec<SearchResult> = sorted_scalars
            .iter()
            .map(|(value, id)| {
                let distance = (value - target).abs();
                SearchResult {
                    id: id.clone(),
                    distance,
                }
            })
            .collect();
        
        // Sort by distance (ascending)
        results.sort_by(|a, b| a.distance.partial_cmp(&b.distance).unwrap());
        
        // Take top k results
        Ok(results.into_iter().take(k).collect())
    }
    
    async fn build(&self) -> Result<(), Box<dyn Error>> {
        // Update sorted list
        self.update_sorted();
        Ok(())
    }
    
    async fn clear(&self) -> Result<(), Box<dyn Error>> {
        let mut scalars = self.scalars.write().await;
        scalars.clear();
        
        let mut sorted_scalars = self.sorted_scalars.write().await;
        sorted_scalars.clear();
        
        Ok(())
    }
}

/// Index manager for handling multiple indexes
pub struct IndexManager {
    indexes: std::sync::Arc<tokio::sync::RwLock<std::collections::HashMap<String, Box<dyn VectorIndex>>>>,
}

impl IndexManager {
    /// Create a new index manager
    pub fn new() -> Self {
        Self {
            indexes: std::sync::Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
        }
    }
    
    /// Create a new index
    pub async fn create_index(&self, name: &str, index_type: &str, metric: &str) -> Result<(), Box<dyn Error>> {
        let mut indexes = self.indexes.write().await;
        
        let index: Box<dyn VectorIndex> = match index_type {
            "brute_force" => Box::new(BruteForceIndex::new(metric)),
            "hnsw" => Box::new(HNSWIndex::new(metric)),
            "ivf" => Box::new(IVFIndex::new(metric)),
            "scalar" => Box::new(ScalarIndex::new()),
            _ => Box::new(BruteForceIndex::new(metric)),
        };
        
        indexes.insert(name.to_string(), index);
        Ok(())
    }
    
    /// Get an index by name
    pub async fn get_index(&self, name: &str) -> Result<Option<Box<dyn VectorIndex + 'static>>, Box<dyn Error>> {
        let indexes = self.indexes.read().await;
        match indexes.get(name) {
            Some(index) => Ok(Some(std::boxed::Box::clone(index))),
            None => Ok(None),
        }
    }
    
    /// Delete an index
    pub async fn delete_index(&self, name: &str) -> Result<bool, Box<dyn Error>> {
        let mut indexes = self.indexes.write().await;
        Ok(indexes.remove(name).is_some())
    }
}

pub use {IVFIndex, ScalarIndex};

#[cfg(test)]
mod tests {
    include!("tests.rs");
}