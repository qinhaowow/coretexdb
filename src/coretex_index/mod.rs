//! Vector indexing for CortexDB

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::error::Error;

/// Result of a vector search
#[derive(Debug, Clone, PartialEq)]
pub struct SearchResult {
    /// ID of the matched vector
    pub id: String,

    /// Distance from the query vector
    pub distance: f32,
}

impl Eq for SearchResult {}

impl PartialOrd for SearchResult {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.distance.partial_cmp(&other.distance)
    }
}

impl Ord for SearchResult {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.distance.partial_cmp(&other.distance).unwrap_or(std::cmp::Ordering::Equal)
    }
}

/// Vector index trait
#[async_trait]
pub trait VectorIndex: Send + Sync {
    /// Add a vector to the index
    async fn add(&self, id: &str, vector: &[f32]) -> Result<(), Box<dyn Error + Send + Sync>>;
    
    /// Remove a vector from the index
    async fn remove(&self, id: &str) -> Result<bool, Box<dyn Error + Send + Sync>>;
    
    /// Search for similar vectors
    async fn search(&self, query: &[f32], k: usize) -> Result<Vec<SearchResult>, Box<dyn Error + Send + Sync>>;
    
    /// Build the index (if needed)
    async fn build(&self) -> Result<(), Box<dyn Error + Send + Sync>>;
    
    /// Clear the index
    async fn clear(&self) -> Result<(), Box<dyn Error + Send + Sync>>;
    
    /// Clone the index into a box
    fn clone_box(&self) -> Box<dyn VectorIndex>;
}

/// Brute-force index implementation
#[derive(Clone)]
pub struct BruteForceIndex {
    vectors: std::sync::Arc<tokio::sync::RwLock<std::collections::HashMap<String, Vec<f32>>>>,
    metric: String,
}

/// HNSW (Hierarchical Navigable Small World) index implementation
#[derive(Clone)]
pub struct HNSWIndex {
    vectors: std::sync::Arc<tokio::sync::RwLock<std::collections::HashMap<String, Vec<f32>>>>,
    metric: String,
    m: usize,
    ef_construction: usize,
    ef_search: usize,
    max_level: usize,
    entry_point: std::sync::Arc<tokio::sync::RwLock<Option<String>>>,
    graph: std::sync::Arc<tokio::sync::RwLock<std::collections::HashMap<String, Vec<Vec<String>>>>>,
}

/// IVF (Inverted File) index implementation
#[derive(Clone)]
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
#[derive(Clone)]
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
            m: 16,
            ef_construction: 200,
            ef_search: 50,
            max_level: 16,
            entry_point: std::sync::Arc::new(tokio::sync::RwLock::new(None)),
            graph: std::sync::Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
        }
    }

    pub fn with_params(metric: &str, m: usize, ef_construction: usize, ef_search: usize) -> Self {
        Self {
            vectors: std::sync::Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
            metric: metric.to_string(),
            m,
            ef_construction,
            ef_search,
            max_level: 16,
            entry_point: std::sync::Arc::new(tokio::sync::RwLock::new(None)),
            graph: std::sync::Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
        }
    }

    pub async fn save_to_file(&self, path: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        use std::io::Write;

        let vectors = self.vectors.read().await;
        let graph = self.graph.read().await;
        let entry_point = self.entry_point.read().await.clone();

        let serializable = HNSWIndexData {
            metric: self.metric.clone(),
            m: self.m,
            ef_construction: self.ef_construction,
            ef_search: self.ef_search,
            max_level: self.max_level,
            entry_point,
            vectors: vectors.clone(),
            graph: graph.clone(),
        };

        let json = serde_json::to_string(&serializable)?;
        let mut file = std::fs::File::create(path)?;
        file.write_all(json.as_bytes())?;
        Ok(())
    }

    pub async fn load_from_file(path: &str) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let content = std::fs::read_to_string(path)?;
        let data: HNSWIndexData = serde_json::from_str(&content)?;

        let index = Self {
            vectors: std::sync::Arc::new(tokio::sync::RwLock::new(data.vectors)),
            metric: data.metric,
            m: data.m,
            ef_construction: data.ef_construction,
            ef_search: data.ef_search,
            max_level: data.max_level,
            entry_point: std::sync::Arc::new(tokio::sync::RwLock::new(data.entry_point)),
            graph: std::sync::Arc::new(tokio::sync::RwLock::new(data.graph)),
        };

        Ok(index)
    }

    fn calculate_distance(&self, a: &[f32], b: &[f32]) -> f32 {
        match self.metric.as_str() {
            "cosine" => {
                let dot_product: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
                let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
                let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
                if norm_a == 0.0 || norm_b == 0.0 {
                    return 1.0;
                }
                1.0 - (dot_product / (norm_a * norm_b))
            }
            "euclidean" => {
                a.iter().zip(b.iter())
                    .map(|(x, y)| (x - y).powi(2))
                    .sum::<f32>()
                    .sqrt()
            }
            _ => {
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

    fn random_level() -> usize {
        let mut rng = rand::thread_rng();
        let p: f64 = rand::Rng::gen(&mut rng);
        (-p.ln() * 16.0) as usize
    }

    fn search_layer(
        &self,
        entry_id: &str,
        query: &[f32],
        ef: usize,
        layer: usize,
        vectors: &std::collections::HashMap<String, Vec<f32>>,
    ) -> (std::collections::BinaryHeap<std::cmp::Reverse<SearchResult>>, std::collections::HashSet<String>) {
        use std::collections::{BinaryHeap, HashSet};

        let mut visited = HashSet::new();
        let mut candidates = BinaryHeap::new();
        let mut results = BinaryHeap::new();

        visited.insert(entry_id.to_string());
        let dist = self.calculate_distance(query, vectors.get(entry_id).unwrap());
        candidates.push(std::cmp::Reverse(SearchResult { id: entry_id.to_string(), distance: dist }));
        results.push(std::cmp::Reverse(SearchResult { id: entry_id.to_string(), distance: dist }));

        while let Some(std::cmp::Reverse(current)) = candidates.pop() {
            let furthest = results.peek().map(|r| r.0.distance).unwrap_or(f32::MAX);
            if current.distance > furthest {
                break;
            }
            for neighbor_id in self.get_neighbors(&current.id, layer) {
                if visited.insert(neighbor_id.clone()) {
                    let dist = self.calculate_distance(query, vectors.get(&neighbor_id).unwrap());
                    if dist < furthest || results.len() < ef {
                        candidates.push(std::cmp::Reverse(SearchResult { id: neighbor_id.clone(), distance: dist }));
                        results.push(std::cmp::Reverse(SearchResult { id: neighbor_id.clone(), distance: dist }));
                        if results.len() > ef {
                            results.pop();
                        }
                    }
                }
            }
        }

        (results, visited)
    }

    fn get_neighbors(&self, id: &str, layer: usize) -> Vec<String> {
        let graph = self.graph.blocking_read();
        graph.get(id)
            .and_then(|levels| levels.get(layer))
            .cloned()
            .unwrap_or_default()
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

    pub fn with_params(metric: &str, nlist: usize, nprobe: usize) -> Self {
        Self {
            vectors: std::sync::Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
            metric: metric.to_string(),
            nlist,
            nprobe,
            centroids: std::sync::Arc::new(tokio::sync::RwLock::new(Vec::new())),
            vector_to_cluster: std::sync::Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
        }
    }

    pub async fn save_to_file(&self, path: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        use std::io::Write;

        let vectors = self.vectors.read().await;
        let centroids = self.centroids.read().await;
        let vector_to_cluster = self.vector_to_cluster.read().await;

        let data = IVFIndexData {
            metric: self.metric.clone(),
            nlist: self.nlist,
            nprobe: self.nprobe,
            centroids: centroids.clone(),
            vector_to_cluster: vector_to_cluster.clone(),
            vectors: vectors.clone(),
        };

        let json = serde_json::to_string(&data)?;
        let mut file = std::fs::File::create(path)?;
        file.write_all(json.as_bytes())?;
        Ok(())
    }

    pub async fn load_from_file(path: &str) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let content = std::fs::read_to_string(path)?;
        let data: IVFIndexData = serde_json::from_str(&content)?;

        let index = Self {
            vectors: std::sync::Arc::new(tokio::sync::RwLock::new(data.vectors)),
            metric: data.metric,
            nlist: data.nlist,
            nprobe: data.nprobe,
            centroids: std::sync::Arc::new(tokio::sync::RwLock::new(data.centroids)),
            vector_to_cluster: std::sync::Arc::new(tokio::sync::RwLock::new(data.vector_to_cluster)),
        };

        Ok(index)
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
        let centroids = self.centroids.blocking_read();
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
        let scalars = self.scalars.blocking_read();
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
        
        let mut sorted_scalars = self.sorted_scalars.blocking_write();
        *sorted_scalars = sorted;
    }
}

#[async_trait]
impl VectorIndex for BruteForceIndex {
    async fn add(&self, id: &str, vector: &[f32]) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut vectors = self.vectors.write().await;
        vectors.insert(id.to_string(), vector.to_vec());
        Ok(())
    }
    
    async fn remove(&self, id: &str) -> Result<bool, Box<dyn Error + Send + Sync>> {
        let mut vectors = self.vectors.write().await;
        Ok(vectors.remove(id).is_some())
    }
    
    async fn search(&self, query: &[f32], k: usize) -> Result<Vec<SearchResult>, Box<dyn Error + Send + Sync>> {
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
    
    async fn build(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Brute-force index doesn't need building
        Ok(())
    }
    
    async fn clear(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut vectors = self.vectors.write().await;
        vectors.clear();
        Ok(())
    }
    
    fn clone_box(&self) -> Box<dyn VectorIndex> {
        Box::new(self.clone())
    }
}

#[async_trait]
impl VectorIndex for HNSWIndex {
    async fn add(&self, id: &str, vector: &[f32]) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut vectors = self.vectors.write().await;
        vectors.insert(id.to_string(), vector.to_vec());
        drop(vectors);

        let level = Self::random_level();
        let mut graph = self.graph.write().await;
        let entry_point = self.entry_point.read().await.clone();

        let mut node_levels = vec![Vec::new(); level + 1];
        if let Some(ref ep) = entry_point {
            let vectors_read = self.vectors.read().await;
            let top_level = graph.get(ep).map(|l| l.len().saturating_sub(1)).unwrap_or(0);

            for l in (0..=level.min(top_level)).rev() {
                let (results, _) = self.search_layer(ep, vector, self.ef_construction, l, &vectors_read);
                let neighbors: Vec<String> = results.into_iter()
                    .take(self.m)
                    .map(|r| r.0.id)
                    .collect();
                if l <= level {
                    node_levels[l] = neighbors.clone();
                }
                for neighbor_id in &neighbors {
                    let neighbor_levels = graph.entry(neighbor_id.clone()).or_insert_with(|| {
                        let mut v = Vec::new();
                        v.push(Vec::new());
                        v
                    });
                    if l < neighbor_levels.len() {
                        neighbor_levels[l].push(id.to_string());
                        if neighbor_levels[l].len() > self.m {
                            neighbor_levels[l].truncate(self.m);
                        }
                    }
                }
            }
        }

        graph.insert(id.to_string(), node_levels);
        drop(graph);

        if level > self.entry_point.read().await.as_ref().map(|_| 0).unwrap_or(0) {
            let mut ep = self.entry_point.write().await;
            *ep = Some(id.to_string());
        } else if entry_point.is_none() {
            let mut ep = self.entry_point.write().await;
            *ep = Some(id.to_string());
        }

        Ok(())
    }

    async fn remove(&self, id: &str) -> Result<bool, Box<dyn Error + Send + Sync>> {
        let mut vectors = self.vectors.write().await;
        vectors.remove(id);
        let mut graph = self.graph.write().await;
        graph.remove(id);
        for (_, levels) in graph.iter_mut() {
            for layer in levels.iter_mut() {
                layer.retain(|n| n != id);
            }
        }
        let mut ep = self.entry_point.write().await;
        if ep.as_ref().map(|e| e == id).unwrap_or(false) {
            *ep = graph.keys().next().cloned();
        }
        Ok(true)
    }

    async fn search(&self, query: &[f32], k: usize) -> Result<Vec<SearchResult>, Box<dyn Error + Send + Sync>> {
        let vectors = self.vectors.read().await;
        if vectors.is_empty() {
            return Ok(Vec::new());
        }

        let entry_point = self.entry_point.read().await.clone();
        let ep = match entry_point {
            Some(ep) => ep,
            None => return Ok(Vec::new()),
        };

        let graph = self.graph.read().await;
        let top_level = graph.get(&ep).map(|l| l.len().saturating_sub(1)).unwrap_or(0);

        let mut current_entry = ep.clone();
        for l in (1..=top_level).rev() {
            let (results, _) = self.search_layer(&current_entry, query, 1, l, &vectors);
            if let Some(closest) = results.into_iter().min_by(|a, b| a.0.distance.partial_cmp(&b.0.distance).unwrap()) {
                current_entry = closest.0.id;
            }
        }

        let (results, _) = self.search_layer(&current_entry, query, self.ef_search.max(k), 0, &vectors);
        let mut final_results: Vec<SearchResult> = results.into_iter().map(|r| r.0).collect();
        final_results.sort_by(|a, b| a.distance.partial_cmp(&b.distance).unwrap());
        final_results.truncate(k);

        Ok(final_results)
    }

    async fn build(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let vectors = self.vectors.read().await;
        let ids: Vec<String> = vectors.keys().cloned().collect();
        drop(vectors);

        for id in &ids {
            let vector = {
                let v = self.vectors.read().await;
                v.get(id).cloned()
            };
            if let Some(vec) = vector {
                let level = Self::random_level();
                let mut graph = self.graph.write().await;
                let entry_point = self.entry_point.read().await.clone();

                let mut node_levels = vec![Vec::new(); level + 1];
                if let Some(ref ep) = entry_point {
                    let vectors_read = self.vectors.read().await;
                    let top_level = graph.get(ep).map(|l| l.len().saturating_sub(1)).unwrap_or(0);

                    for l in (0..=level.min(top_level)).rev() {
                        let (results, _) = self.search_layer(ep, &vec, self.ef_construction, l, &vectors_read);
                        let neighbors: Vec<String> = results.into_iter()
                            .take(self.m)
                            .map(|r| r.0.id)
                            .collect();
                        if l <= level {
                            node_levels[l] = neighbors.clone();
                        }
                        for neighbor_id in &neighbors {
                            let neighbor_levels = graph.entry(neighbor_id.clone()).or_insert_with(|| {
                                let mut v = Vec::new();
                                v.push(Vec::new());
                                v
                            });
                            if l < neighbor_levels.len() {
                                neighbor_levels[l].push(id.to_string());
                                if neighbor_levels[l].len() > self.m {
                                    neighbor_levels[l].truncate(self.m);
                                }
                            }
                        }
                    }
                }

                graph.insert(id.clone(), node_levels);

                if level > self.entry_point.read().await.as_ref().map(|_| 0).unwrap_or(0) {
                    let mut ep = self.entry_point.write().await;
                    *ep = Some(id.clone());
                } else if entry_point.is_none() {
                    let mut ep = self.entry_point.write().await;
                    *ep = Some(id.clone());
                }
            }
        }

        Ok(())
    }

    async fn clear(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut vectors = self.vectors.write().await;
        vectors.clear();
        let mut graph = self.graph.write().await;
        graph.clear();
        let mut ep = self.entry_point.write().await;
        *ep = None;
        Ok(())
    }

    fn clone_box(&self) -> Box<dyn VectorIndex> {
        Box::new(self.clone())
    }
}

#[async_trait]
impl VectorIndex for IVFIndex {
    async fn add(&self, id: &str, vector: &[f32]) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut vectors = self.vectors.write().await;
        vectors.insert(id.to_string(), vector.to_vec());

        let cluster_id = self.assign_to_cluster(vector);
        let mut vector_to_cluster = self.vector_to_cluster.write().await;
        vector_to_cluster.insert(id.to_string(), cluster_id);

        Ok(())
    }

    async fn remove(&self, id: &str) -> Result<bool, Box<dyn Error + Send + Sync>> {
        let mut vectors = self.vectors.write().await;
        let removed = vectors.remove(id).is_some();

        if removed {
            let mut vector_to_cluster = self.vector_to_cluster.write().await;
            vector_to_cluster.remove(id);
        }

        Ok(removed)
    }

    async fn search(&self, query: &[f32], k: usize) -> Result<Vec<SearchResult>, Box<dyn Error + Send + Sync>> {
        let vectors = self.vectors.read().await;
        let vector_to_cluster = self.vector_to_cluster.read().await;
        let centroids = self.centroids.read().await;

        if centroids.is_empty() {
            return Ok(Vec::new());
        }

        let mut cluster_distances: Vec<(usize, f32)> = centroids
            .iter()
            .enumerate()
            .map(|(i, centroid)| {
                let dist = self.calculate_distance(query, centroid);
                (i, dist)
            })
            .collect();

        cluster_distances.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

        let nprobe = self.nprobe.min(cluster_distances.len());
        let probed_clusters: std::collections::HashSet<usize> = cluster_distances
            .into_iter()
            .take(nprobe)
            .map(|(i, _)| i)
            .collect();

        let mut results: Vec<SearchResult> = vectors
            .iter()
            .filter(|(id, _)| {
                vector_to_cluster
                    .get(*id)
                    .map(|c| probed_clusters.contains(c))
                    .unwrap_or(false)
            })
            .map(|(id, vec)| {
                let distance = self.calculate_distance(query, vec);
                SearchResult {
                    id: id.clone(),
                    distance,
                }
            })
            .collect();

        results.sort_by(|a, b| a.distance.partial_cmp(&b.distance).unwrap());
        results.truncate(k);

        Ok(results)
    }

    async fn build(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let vectors = self.vectors.read().await;
        let all_vectors: Vec<Vec<f32>> = vectors.values().cloned().collect();
        let ids: Vec<String> = vectors.keys().cloned().collect();
        drop(vectors);

        if all_vectors.is_empty() || all_vectors.len() < self.nlist {
            return Ok(());
        }

        let dim = all_vectors[0].len();
        let nlist = self.nlist.min(all_vectors.len());

        let mut centroids: Vec<Vec<f32>> = all_vectors
            .iter()
            .step_by(all_vectors.len() / nlist)
            .take(nlist)
            .cloned()
            .collect();

        while centroids.len() < nlist {
            centroids.push(vec![0.0; dim]);
        }

        let mut assignments: Vec<usize> = vec![0; all_vectors.len()];

        for _ in 0..20 {
            let mut new_centroids = vec![vec![0.0; dim]; nlist];
            let mut counts = vec![0usize; nlist];

            for (i, vec) in all_vectors.iter().enumerate() {
                let mut min_dist = f32::MAX;
                let mut best = 0;
                for (j, centroid) in centroids.iter().enumerate() {
                    let dist: f32 = vec
                        .iter()
                        .zip(centroid.iter())
                        .map(|(a, b)| (a - b).powi(2))
                        .sum::<f32>()
                        .sqrt();
                    if dist < min_dist {
                        min_dist = dist;
                        best = j;
                    }
                }
                assignments[i] = best;
                counts[best] += 1;
                for (d, val) in vec.iter().enumerate() {
                    new_centroids[best][d] += val;
                }
            }

            for j in 0..nlist {
                if counts[j] > 0 {
                    for d in 0..dim {
                        new_centroids[j][d] /= counts[j] as f32;
                    }
                } else {
                    new_centroids[j] = centroids[j].clone();
                }
            }

            centroids = new_centroids;
        }

        let mut centroids_lock = self.centroids.write().await;
        *centroids_lock = centroids;

        let mut vector_to_cluster = self.vector_to_cluster.write().await;
        for (i, id) in ids.iter().enumerate() {
            vector_to_cluster.insert(id.clone(), assignments[i]);
        }

        Ok(())
    }

    async fn clear(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut vectors = self.vectors.write().await;
        vectors.clear();

        let mut vector_to_cluster = self.vector_to_cluster.write().await;
        vector_to_cluster.clear();

        let mut centroids = self.centroids.write().await;
        centroids.clear();

        Ok(())
    }

    fn clone_box(&self) -> Box<dyn VectorIndex> {
        Box::new(self.clone())
    }
}

#[async_trait]
impl VectorIndex for ScalarIndex {
    async fn add(&self, id: &str, vector: &[f32]) -> Result<(), Box<dyn Error + Send + Sync>> {
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
    
    async fn remove(&self, id: &str) -> Result<bool, Box<dyn Error + Send + Sync>> {
        let mut scalars = self.scalars.write().await;
        let removed = scalars.remove(id).is_some();
        
        if removed {
            // Update sorted list
            self.update_sorted();
        }
        
        Ok(removed)
    }
    
    async fn search(&self, query: &[f32], k: usize) -> Result<Vec<SearchResult>, Box<dyn Error + Send + Sync>> {
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
    
    async fn build(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Update sorted list
        self.update_sorted();
        Ok(())
    }
    
    async fn clear(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut scalars = self.scalars.write().await;
        scalars.clear();
        
        let mut sorted_scalars = self.sorted_scalars.write().await;
        sorted_scalars.clear();
        
        Ok(())
    }
    
    fn clone_box(&self) -> Box<dyn VectorIndex> {
        Box::new(self.clone())
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
    pub async fn create_index(&self, name: &str, index_type: &str, metric: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
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
    pub async fn get_index(&self, name: &str) -> Result<Option<Box<dyn VectorIndex + 'static>>, Box<dyn Error + Send + Sync>> {
        let indexes = self.indexes.read().await;
        match indexes.get(name) {
            Some(index) => Ok(Some(index.clone_box())),
            None => Ok(None),
        }
    }
    
    /// Delete an index
    pub async fn delete_index(&self, name: &str) -> Result<bool, Box<dyn Error + Send + Sync>> {
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct HNSWIndexData {
    metric: String,
    m: usize,
    ef_construction: usize,
    ef_search: usize,
    max_level: usize,
    entry_point: Option<String>,
    vectors: std::collections::HashMap<String, Vec<f32>>,
    graph: std::collections::HashMap<String, Vec<Vec<String>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IVFIndexData {
    metric: String,
    nlist: usize,
    nprobe: usize,
    centroids: Vec<Vec<f32>>,
    vector_to_cluster: std::collections::HashMap<String, usize>,
    vectors: std::collections::HashMap<String, Vec<f32>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PQIndexData {
    metric: String,
    dimension: usize,
    n_subquantizers: usize,
    n_bits: usize,
    codebooks: Vec<Vec<Vec<f32>>>,
    vectors: std::collections::HashMap<String, Vec<u8>>,
    original_vectors: std::collections::HashMap<String, Vec<f32>>,
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

    pub async fn save_to_file(&self, path: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        use std::io::Write;

        let vectors = self.vectors.read().await;
        let original_vectors = self.original_vectors.read().await;
        let codebooks = self.codebooks.read().await;

        let data = PQIndexData {
            metric: self.metric.clone(),
            dimension: self.dimension,
            n_subquantizers: self.n_subquantizers,
            n_bits: self.n_bits,
            codebooks: codebooks.clone(),
            vectors: vectors.clone(),
            original_vectors: original_vectors.clone(),
        };

        let json = serde_json::to_string(&data)?;
        let mut file = std::fs::File::create(path)?;
        file.write_all(json.as_bytes())?;
        Ok(())
    }

    pub async fn load_from_file(path: &str) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let content = std::fs::read_to_string(path)?;
        let data: PQIndexData = serde_json::from_str(&content)?;

        let index = Self {
            vectors: std::sync::Arc::new(tokio::sync::RwLock::new(data.vectors)),
            original_vectors: std::sync::Arc::new(tokio::sync::RwLock::new(data.original_vectors)),
            metric: data.metric,
            dimension: data.dimension,
            n_subquantizers: data.n_subquantizers,
            n_bits: data.n_bits,
            codebooks: std::sync::Arc::new(tokio::sync::RwLock::new(data.codebooks)),
        };

        Ok(index)
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
        vectors.insert(id.clone(), code);

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

#[cfg(test)]
mod tests {
    include!("tests.rs");
}