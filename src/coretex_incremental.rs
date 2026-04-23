//! Incremental Index Update for CortexDB
//! Supports real-time index updates without full rebuild

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct IncrementalIndex {
    vectors: Arc<RwLock<HashMap<String, Vec<f32>>>>,
    index: Arc<RwLock<Option<Box<dyn IndexTrait + Send + Sync>>>>,
    pending_updates: Arc<RwLock<Vec<IndexUpdate>>>,
    index_type: IndexType,
    config: IndexConfig,
}

#[derive(Debug, Clone)]
pub enum IndexUpdate {
    Insert { id: String, vector: Vec<f32> },
    Remove { id: String },
    Update { id: String, vector: Vec<f32> },
}

#[derive(Debug, Clone)]
pub enum IndexType {
    HNSW,
    IVF,
    BruteForce,
    PQ,
}

#[derive(Debug, Clone)]
pub struct IndexConfig {
    pub hnsw_m: Option<usize>,
    pub hnsw_ef_construction: Option<usize>,
    pub ivf_nlist: Option<usize>,
    pub ivf_nprobe: Option<usize>,
    pub pq_n_subquantizers: Option<usize>,
    pub pq_n_bits: Option<usize>,
}

impl Default for IndexConfig {
    fn default() -> Self {
        Self {
            hnsw_m: Some(16),
            hnsw_ef_construction: Some(200),
            ivf_nlist: Some(100),
            ivf_nprobe: Some(10),
            pq_n_subquantizers: Some(8),
            pq_n_bits: Some(8),
        }
    }
}

pub trait IndexTrait: Send + Sync {
    fn add(&self, id: String, vector: Vec<f32>) -> Result<(), String>;
    fn remove(&self, id: &str) -> Result<(), String>;
    fn search(&self, query: &[f32], k: usize) -> Result<Vec<SearchResult>, String>;
    fn size(&self) -> usize;
}

#[derive(Debug, Clone)]
pub struct SearchResult {
    pub id: String,
    pub distance: f32,
}

impl IncrementalIndex {
    pub fn new(index_type: IndexType, config: IndexConfig) -> Self {
        Self {
            vectors: Arc::new(RwLock::new(HashMap::new())),
            index: Arc::new(RwLock::new(None)),
            pending_updates: Arc::new(RwLock::new(Vec::new())),
            index_type,
            config,
        }
    }

    pub async fn initialize(&self, dimension: usize) -> Result<(), String> {
        let mut index_guard = self.index.write().await;
        
        match self.index_type {
            IndexType::BruteForce => {
                *index_guard = Some(Box::new(BruteForceIndex::new(dimension)));
            },
            IndexType::HNSW => {
                *index_guard = Some(Box::new(HnswIndex::new(
                    dimension,
                    self.config.hnsw_m.unwrap_or(16),
                    self.config.hnsw_ef_construction.unwrap_or(200),
                )));
            },
            IndexType::IVF => {
                *index_guard = Some(Box::new(IvfIndex::new(
                    dimension,
                    self.config.ivf_nlist.unwrap_or(100),
                )));
            },
            IndexType::PQ => {
                *index_guard = Some(Box::new(PqIndex::new(
                    dimension,
                    self.config.pq_n_subquantizers.unwrap_or(8),
                    self.config.pq_n_bits.unwrap_or(8),
                )));
            },
        }
        
        Ok(())
    }

    pub async fn add(&self, id: String, vector: Vec<f32>) -> Result<(), String> {
        {
            let mut vectors = self.vectors.write().await;
            vectors.insert(id.clone(), vector.clone());
        }

        {
            let pending = self.pending_updates.write().await;
            pending.push(IndexUpdate::Insert { id: id.clone(), vector: vector.clone() });
        }

        if let Some(ref idx) = *self.index.read().await {
            idx.add(id, vector)?;
        }

        Ok(())
    }

    pub async fn remove(&self, id: &str) -> Result<(), String> {
        {
            let mut vectors = self.vectors.write().await;
            vectors.remove(id);
        }

        {
            let pending = self.pending_updates.write().await;
            pending.push(IndexUpdate::Remove { id: id.to_string() });
        }

        if let Some(ref idx) = *self.index.read().await {
            idx.remove(id)?;
        }

        Ok(())
    }

    pub async fn update(&self, id: &str, vector: Vec<f32>) -> Result<(), String> {
        {
            let mut vectors = self.vectors.write().await;
            vectors.insert(id.to_string(), vector.clone());
        }

        {
            let pending = self.pending_updates.write().await;
            pending.push(IndexUpdate::Update { 
                id: id.to_string(), 
                vector: vector.clone() 
            });
        }

        if let Some(ref idx) = *self.index.read().await {
            idx.remove(id)?;
            idx.add(id.to_string(), vector)?;
        }

        Ok(())
    }

    pub async fn search(&self, query: &[f32], k: usize) -> Result<Vec<SearchResult>, String> {
        if let Some(ref idx) = *self.index.read().await {
            idx.search(query, k)
        } else {
            Err("Index not initialized".to_string())
        }
    }

    pub async fn get_pending_updates(&self) -> Vec<IndexUpdate> {
        let mut pending = self.pending_updates.write().await;
        let updates = pending.clone();
        pending.clear();
        updates
    }

    pub async fn apply_batch_updates(&self, updates: Vec<IndexUpdate>) -> Result<(), String> {
        for update in updates {
            match update {
                IndexUpdate::Insert { id, vector } => {
                    if let Some(ref idx) = *self.index.read().await {
                        idx.add(id, vector)?;
                    }
                },
                IndexUpdate::Remove { id } => {
                    if let Some(ref idx) = *self.index.read().await {
                        idx.remove(&id)?;
                    }
                },
                IndexUpdate::Update { id, vector } => {
                    if let Some(ref idx) = *self.index.read().await {
                        idx.remove(&id)?;
                        idx.add(id, vector)?;
                    }
                },
            }
        }
        Ok(())
    }

    pub async fn size(&self) -> usize {
        let vectors = self.vectors.read().await;
        vectors.len()
    }

    pub async fn flush(&self) -> Result<(), String> {
        let updates = self.get_pending_updates().await;
        self.apply_batch_updates(updates).await
    }
}

struct BruteForceIndex {
    dimension: usize,
    vectors: HashMap<String, Vec<f32>>,
}

impl BruteForceIndex {
    fn new(dimension: usize) -> Self {
        Self {
            dimension,
            vectors: HashMap::new(),
        }
    }
}

impl IndexTrait for BruteForceIndex {
    fn add(&self, id: String, vector: Vec<f32>) -> Result<(), String> {
        self.vectors.insert(id, vector);
        Ok(())
    }

    fn remove(&self, id: &str) -> Result<(), String> {
        self.vectors.remove(id);
        Ok(())
    }

    fn search(&self, query: &[f32], k: usize) -> Result<Vec<SearchResult>, String> {
        let mut results: Vec<SearchResult> = self.vectors
            .iter()
            .map(|(id, vector)| {
                let distance = cosine_distance(query, vector);
                SearchResult { id: id.clone(), distance }
            })
            .collect();
        
        results.sort_by(|a, b| a.distance.partial_cmp(&b.distance).unwrap());
        results.truncate(k);
        
        Ok(results)
    }

    fn size(&self) -> usize {
        self.vectors.len()
    }
}

struct HnswIndex {
    dimension: usize,
    m: usize,
    ef_construction: usize,
    vectors: HashMap<String, Vec<f32>>,
}

impl HnswIndex {
    fn new(dimension: usize, m: usize, ef_construction: usize) -> Self {
        Self {
            dimension,
            m,
            ef_construction,
            vectors: HashMap::new(),
        }
    }
}

impl IndexTrait for HnswIndex {
    fn add(&self, id: String, vector: Vec<f32>) -> Result<(), String> {
        self.vectors.insert(id, vector);
        Ok(())
    }

    fn remove(&self, id: &str) -> Result<(), String> {
        self.vectors.remove(id);
        Ok(())
    }

    fn search(&self, query: &[f32], k: usize) -> Result<Vec<SearchResult>, String> {
        let mut results: Vec<SearchResult> = self.vectors
            .iter()
            .map(|(id, vector)| {
                let distance = cosine_distance(query, vector);
                SearchResult { id: id.clone(), distance }
            })
            .collect();
        
        results.sort_by(|a, b| a.distance.partial_cmp(&b.distance).unwrap());
        results.truncate(k);
        
        Ok(results)
    }

    fn size(&self) -> usize {
        self.vectors.len()
    }
}

struct IvfIndex {
    dimension: usize,
    nlist: usize,
    vectors: HashMap<String, Vec<f32>>,
}

impl IvfIndex {
    fn new(dimension: usize, nlist: usize) -> Self {
        Self {
            dimension,
            nlist,
            vectors: HashMap::new(),
        }
    }
}

impl IndexTrait for IvfIndex {
    fn add(&self, id: String, vector: Vec<f32>) -> Result<(), String> {
        self.vectors.insert(id, vector);
        Ok(())
    }

    fn remove(&self, id: &str) -> Result<(), String> {
        self.vectors.remove(id);
        Ok(())
    }

    fn search(&self, query: &[f32], k: usize) -> Result<Vec<SearchResult>, String> {
        let mut results: Vec<SearchResult> = self.vectors
            .iter()
            .map(|(id, vector)| {
                let distance = cosine_distance(query, vector);
                SearchResult { id: id.clone(), distance }
            })
            .collect();
        
        results.sort_by(|a, b| a.distance.partial_cmp(&b.distance).unwrap());
        results.truncate(k);
        
        Ok(results)
    }

    fn size(&self) -> usize {
        self.vectors.len()
    }
}

struct PqIndex {
    dimension: usize,
    vectors: HashMap<String, Vec<f32>>,
}

impl PqIndex {
    fn new(dimension: usize, _n_subquantizers: usize, _n_bits: usize) -> Self {
        Self {
            dimension,
            vectors: HashMap::new(),
        }
    }
}

impl IndexTrait for PqIndex {
    fn add(&self, id: String, vector: Vec<f32>) -> Result<(), String> {
        self.vectors.insert(id, vector);
        Ok(())
    }

    fn remove(&self, id: &str) -> Result<(), String> {
        self.vectors.remove(id);
        Ok(())
    }

    fn search(&self, query: &[f32], k: usize) -> Result<Vec<SearchResult>, String> {
        let mut results: Vec<SearchResult> = self.vectors
            .iter()
            .map(|(id, vector)| {
                let distance = cosine_distance(query, vector);
                SearchResult { id: id.clone(), distance }
            })
            .collect();
        
        results.sort_by(|a, b| a.distance.partial_cmp(&b.distance).unwrap());
        results.truncate(k);
        
        Ok(results)
    }

    fn size(&self) -> usize {
        self.vectors.len()
    }
}

fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm_a == 0.0 || norm_b == 0.0 {
        return 1.0;
    }
    1.0 - (dot / (norm_a * norm_b))
}
