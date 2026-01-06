//! Core type definitions for CortexDB 

use serde::{Deserialize, Serialize}; 
use std::collections::HashMap; 

/// Vector representation 
#[derive(Debug, Clone, Serialize, Deserialize)] 
pub struct Vector { 
    pub data: Vec<f32>, 
    pub dim: usize, 
} 

impl Vector { 
    pub fn new(data: Vec<f32>) -> Self { 
        let dim = data.len(); 
        Self { data, dim } 
    } 
    
    pub fn zeros(dim: usize) -> Self { 
        Self { 
            data: vec![0.0; dim], 
            dim, 
        } 
    } 
    
    pub fn cosine_similarity(&self, other: &Self) -> f32 { 
        if self.dim != other.dim { 
            return 0.0; 
        } 
        
        let dot_product: f32 = self.data.iter() 
            .zip(&other.data) 
            .map(|(a, b)| a * b) 
            .sum(); 
        
        let norm_a: f32 = self.data.iter().map(|x| x * x).sum::<f32>().sqrt(); 
        let norm_b: f32 = other.data.iter().map(|x| x * x).sum::<f32>().sqrt(); 
        
        if norm_a == 0.0 || norm_b == 0.0 { 
            return 0.0; 
        } 
        
        dot_product / (norm_a * norm_b) 
    } 
} 

/// Document with vector and metadata 
#[derive(Debug, Clone, Serialize, Deserialize)] 
pub struct Document { 
    pub id: String, 
    pub vector: Vector, 
    pub metadata: HashMap<String, serde_json::Value>, 
    pub content: Option<String>, 
    pub created_at: chrono::DateTime<chrono::Utc>, 
    pub updated_at: chrono::DateTime<chrono::Utc>, 
} 

impl Document { 
    pub fn new(id: String, vector: Vector) -> Self { 
        let now = chrono::Utc::now(); 
        Self { 
            id, 
            vector, 
            metadata: HashMap::new(), 
            content: None, 
            created_at: now, 
            updated_at: now, 
        } 
    } 
    
    pub fn with_metadata(mut self, metadata: HashMap<String, serde_json::Value>) -> Self { 
        self.metadata = metadata; 
        self 
    } 
    
    pub fn with_content(mut self, content: String) -> Self { 
        self.content = Some(content); 
        self 
    } 
} 

/// Query result 
#[derive(Debug, Clone, Serialize, Deserialize)] 
pub struct QueryResult { 
    pub document: Document, 
    pub score: f32, 
    pub distance: f32, 
} 

/// Collection schema 
#[derive(Debug, Clone, Serialize, Deserialize)] 
pub struct CollectionSchema { 
    pub name: String, 
    pub dimension: usize, 
    pub distance_metric: DistanceMetric, 
    pub indexes: Vec<IndexConfig>, 
    pub metadata_schema: Option<serde_json::Value>, 
} 

/// Distance metric for vector similarity 
#[derive(Debug, Clone, Serialize, Deserialize)] 
pub enum DistanceMetric { 
    Cosine, 
    Euclidean, 
    DotProduct, 
    Manhattan, 
} 

/// Index configuration 
#[derive(Debug, Clone, Serialize, Deserialize)] 
pub struct IndexConfig { 
    pub name: String, 
    pub index_type: IndexType, 
    pub parameters: HashMap<String, serde_json::Value>, 
} 

/// Index type 
#[derive(Debug, Clone, Serialize, Deserialize)] 
pub enum IndexType { 
    BruteForce, 
    HNSW, 
    IVF, 
    Scalar, 
} 

/// Error type for CortexDB 
#[derive(Debug, thiserror::Error)] 
pub enum CortexError { 
    #[error("IO error: {0}")] 
    Io(#[from] std::io::Error), 
    
    #[error("Serialization error: {0}")] 
    Serialization(#[from] serde_json::Error), 
    
    #[error("Vector dimension mismatch: expected {expected}, got {actual}")] 
    DimensionMismatch { expected: usize, actual: usize }, 
    
    #[error("Collection not found: {0}")] 
    CollectionNotFound(String), 
    
    #[error("Document not found: {0}")] 
    DocumentNotFound(String), 
    
    #[error("Index error: {0}")] 
    IndexError(String), 
    
    #[error("Storage error: {0}")] 
    StorageError(String), 
    
    #[error("Validation error: {0}")] 
    ValidationError(String), 
    
    #[error("Configuration error: {0}")] 
    ConfigError(String), 
} 

pub type Result<T> = std::result::Result<T, CortexError>; 