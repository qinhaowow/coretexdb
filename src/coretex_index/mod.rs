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
        
        Self::sort_results(&mut results);
        results.into_iter().take(k).collect()
    }

    fn sort_results(results: &mut Vec<SearchResult>) {
        results.sort_by(|a, b| {
            a.distance.partial_cmp(&b.distance).unwrap_or(std::cmp::Ordering::Equal)
        });
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
        
        sorted.sort_by(|a, b| {
            a.0.partial_cmp(&b.0).unwrap_or_else(|| {
                if a.0.is_nan() && b.0.is_nan() {
                    std::cmp::Ordering::Equal
                } else if a.0.is_nan() {
                    std::cmp::Ordering::Greater
                } else if b.0.is_nan() {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Equal
                }
            })
        });
        
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
        results.sort_by(|a, b| {
            a.distance.partial_cmp(&b.distance).unwrap_or_else(|| {
                if a.distance.is_nan() && b.distance.is_nan() {
                    std::cmp::Ordering::Equal
                } else if a.distance.is_nan() {
                    std::cmp::Ordering::Greater
                } else if b.distance.is_nan() {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Equal
                }
            })
        });
        
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

    async fn search(&self, query: &[f32], k: usize) -> Result<Vec<SearchResult>, Box<dyn Error + Send + Sync>> {
        let vectors = self.vectors.read().await;
        let vector_to_cluster = self.vector_to_cluster.read().await;
        
        let query_vec = query.to_vec();
        let calculate_distance = self.calculate_distance;
        
        let results = tokio::task::spawn_blocking(move || {
            let mut search_results: Vec<SearchResult> = vectors
                .iter()
                .map(|(id, vec)| {
                    let distance = calculate_distance(&query_vec, vec);
                    SearchResult {
                        id: id.clone(),
                        distance,
                    }
                })
                .collect();
            
            search_results.sort_by(|a, b| {
                a.distance.partial_cmp(&b.distance).unwrap_or_else(|| {
                    if a.distance.is_nan() && b.distance.is_nan() {
                        std::cmp::Ordering::Equal
                    } else if a.distance.is_nan() {
                        std::cmp::Ordering::Greater
                    } else if b.distance.is_nan() {
                        std::cmp::Ordering::Less
                    } else {
                        std::cmp::Ordering::Equal
                    }
                })
            });
            
            search_results.into_iter().take(k).collect::<Vec<_>>()
        }).await?;
        
        Ok(results)
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

pub struct PQIndex {
    vectors: std::sync::Arc<tokio::sync::RwLock<std::collections::HashMap<String, Vec<u8>>>>,
    original_vectors: std::sync::Arc<tokio::sync::RwLock<std::collections::HashMap<String, Vec<f32>>>>,
    metric: String,
    dimension: usize,
    n_subquantizers: usize,
    n_bits: usize,
    codebooks: std::sync::Arc<tokio::sync::RwLock<Vec<Vec<Vec<f32>>>>>,
}

impl PQIndex {
    pub fn new(metric: &str, dimension: usize, n_subquantizers: usize, n_bits: usize) -> Self {
        Self {
            vectors: std::sync::Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
            original_vectors: std::sync::Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
            metric: metric.to_string(),
            dimension,
            n_subquantizers,
            n_bits,
            codebooks: std::sync::Arc::new(tokio::sync::RwLock::new(Vec::new())),
        }
    }

    pub async fn train(&self, training_vectors: &[Vec<f32>]) -> Result<(), String> {
        if training_vectors.is_empty() {
            return Err("No training vectors provided".to_string());
        }

        let sub_dim = self.dimension / self.n_subquantizers;
        if sub_dim == 0 {
            return Err("Too many subquantizers for the vector dimension".to_string());
        }

        let mut codebooks = Vec::new();

        for i in 0..self.n_subquantizers {
            let start = i * sub_dim;
            let end = if i == self.n_subquantizers - 1 {
                self.dimension
            } else {
                start + sub_dim
            };

            let mut sub_vectors: Vec<Vec<f32>> = training_vectors
                .iter()
                .map(|v| v[start..end].to_vec())
                .collect();

            let n_centroids = 1 << self.n_bits;
            let codebook = Self::kmeans(&mut sub_vectors, n_centroids);
            codebooks.push(codebook);
        }

        let mut cb = self.codebooks.write().await;
        *cb = codebooks;

        Ok(())
    }

    fn kmeans(data: &mut Vec<Vec<f32>>, k: usize) -> Vec<Vec<f32>> {
        if data.is_empty() || k == 0 {
            return Vec::new();
        }

        let dim = data[0].len();
        let k = k.min(data.len());

        let mut centroids: Vec<Vec<f32>> = data
            .iter()
            .step_by(data.len() / k.max(1))
            .take(k)
            .cloned()
            .collect();

        while centroids.len() < k {
            centroids.push(vec![0.0; dim]);
        }

        for _ in 0..20 {
            let mut clusters: Vec<Vec<Vec<f32>>> = vec![Vec::new(); k];

            for vec in data.iter() {
                let mut min_dist = f32::MAX;
                let mut best_centroid = 0;

                for (i, centroid) in centroids.iter().enumerate() {
                    let dist: f32 = vec.iter()
                        .zip(centroid.iter())
                        .map(|(a, b)| (a - b).powi(2))
                        .sum::<f32>()
                        .sqrt();

                    if dist < min_dist {
                        min_dist = dist;
                        best_centroid = i;
                    }
                }

                clusters[best_centroid].push(vec.clone());
            }

            for (i, cluster) in clusters.iter().enumerate() {
                if !cluster.is_empty() {
                    let dim = cluster[0].len();
                    let mut new_centroid = vec![0.0; dim];
                    for vec in cluster {
                        for (j, val) in vec.iter().enumerate() {
                            new_centroid[j] += val;
                        }
                    }
                    for val in new_centroid.iter_mut() {
                        *val /= cluster.len() as f32;
                    }
                    centroids[i] = new_centroid;
                }
            }
        }

        centroids
    }

    pub async fn add(&self, id: String, vector: Vec<f32>) -> Result<(), String> {
        if vector.len() != self.dimension {
            return Err(format!("Vector dimension {} does not match index dimension {}", vector.len(), self.dimension));
        }

        let codebook = self.codebooks.read().await;
        if codebook.is_empty() {
            return Err("Index not trained. Call train() first.".to_string());
        }

        let code = self.encode_vector(&vector, &codebook);

        let mut vectors = self.vectors.write().await;
        vectors.insert(id, code);

        let mut original = self.original_vectors.write().await;
        original.insert(id, vector);

        Ok(())
    }

    fn encode_vector(&self, vector: &[f32], codebook: &[Vec<Vec<f32>>]) -> Vec<u8> {
        let sub_dim = self.dimension / self.n_subquantizers;
        let mut code = Vec::with_capacity(self.n_subquantizers);

        for (i, sub_codebook) in codebook.iter().enumerate() {
            let start = i * sub_dim;
            let end = if i == self.n_subquantizers - 1 {
                self.dimension
            } else {
                start + sub_dim
            };

            let sub_vector = &vector[start..end];

            let mut min_dist = f32::MAX;
            let mut best_idx = 0u8;

            for (j, centroid) in sub_codebook.iter().enumerate() {
                let dist: f32 = sub_vector
                    .iter()
                    .zip(centroid.iter())
                    .map(|(a, b)| (a - b).powi(2))
                    .sum::<f32>()
                    .sqrt();

                if dist < min_dist {
                    min_dist = dist;
                    best_idx = j as u8;
                }
            }

            code.push(best_idx);
        }

        code
    }

    pub async fn search(&self, query: &[f32], k: usize) -> Result<Vec<super::SearchResult>, String> {
        let codebook = self.codebooks.read().await;
        if codebook.is_empty() {
            return Err("Index not trained. Call train() first.".to_string());
        }

        let query_code = self.encode_vector(query, &codebook);
        let original = self.original_vectors.read().await;

        let mut results: Vec<super::SearchResult> = original
            .iter()
            .map(|(id, orig)| {
                let dist = self.calculate_distance(query, orig);
                super::SearchResult {
                    id: id.clone(),
                    distance: dist,
                }
            })
            .collect();

        results.sort_by(|a, b| a.distance.partial_cmp(&b.distance).unwrap());
        results.truncate(k);

        Ok(results)
    }

    fn calculate_distance(&self, a: &[f32], b: &[f32]) -> f32 {
        match self.metric.as_str() {
            "cosine" => {
                let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
                let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
                let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
                if norm_a == 0.0 || norm_b == 0.0 {
                    return 1.0;
                }
                1.0 - (dot / (norm_a * norm_b))
            },
            "euclidean" => {
                a.iter().zip(b.iter())
                    .map(|(x, y)| (x - y).powi(2))
                    .sum::<f32>()
                    .sqrt()
            },
            _ => {
                let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
                let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
                let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
                if norm_a == 0.0 || norm_b == 0.0 {
                    return 1.0;
                }
                1.0 - (dot / (norm_a * norm_b))
            }
        }
    }

    pub fn compression_ratio(&self) -> f32 {
        let original_size = self.dimension * 4;
        let compressed_size = self.n_subquantizers;
        original_size as f32 / compressed_size as f32
    }
}

pub use {IVFIndex, ScalarIndex, PQIndex};

#[cfg(test)]
mod tests {
    include!("tests.rs");
}