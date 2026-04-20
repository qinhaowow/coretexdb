//! Utility functions for CortexDB

use std::error::Error;

pub mod wal;
pub mod transaction;
pub mod cluster;
pub mod backup;
pub mod monitoring;
pub mod cache;

pub use wal::WriteAheadLog;
pub use transaction::{TransactionManager, LockManager, Transaction, TransactionOperation, TransactionState};
pub use cluster::{ClusterManager, ClusterNode, NodeRole, NodeState, Shard};
pub use backup::BackupManager;
pub use monitoring::{MonitoringService, Metrics};
pub use cache::{LRUCache, TimedLRUCache, AsyncLRUCache, MultiLevelCache, CacheStats, MultiLevelCacheStats};

/// Calculate cosine similarity between two vectors
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() {
        return 0.0;
    }
    
    let dot_product: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    
    if norm_a == 0.0 || norm_b == 0.0 {
        return 0.0;
    }
    
    dot_product / (norm_a * norm_b)
}

/// Calculate Euclidean distance between two vectors
pub fn euclidean_distance(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() {
        return f32::INFINITY;
    }
    
    a.iter().zip(b.iter())
        .map(|(x, y)| (x - y).powi(2))
        .sum::<f32>()
        .sqrt()
}

/// Normalize a vector to unit length
pub fn normalize_vector(vector: &[f32]) -> Vec<f32> {
    let norm: f32 = vector.iter().map(|x| x * x).sum::<f32>().sqrt();
    
    if norm == 0.0 {
        return vector.to_vec();
    }
    
    vector.iter().map(|x| x / norm).collect()
}

/// Parse a vector from a comma-separated string
pub fn parse_vector(vector_str: &str) -> Result<Vec<f32>, Box<dyn Error>> {
    let parts: Vec<&str> = vector_str.split(',').collect();
    let mut vector = Vec::with_capacity(parts.len());
    
    for part in parts {
        let value = part.trim().parse::<f32>()?;
        vector.push(value);
    }
    
    Ok(vector)
}

/// Generate a random vector of the specified dimension
pub fn random_vector(dimension: usize) -> Vec<f32> {
    use rand::Rng;
    
    let mut rng = rand::thread_rng();
    (0..dimension).map(|_| rng.gen::<f32>()).collect()
}
