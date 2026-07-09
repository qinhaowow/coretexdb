//! Core type definitions for CoreTexDB 

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

/// Error type for CoreTexDB 
#[derive(Debug, thiserror::Error)] 
pub enum CoreTexError { 
    // === External error wrappers ===
    #[error("IO error: {0}")] 
    Io(#[from] std::io::Error), 
    
    #[error("Serialization error: {0}")] 
    Serialization(#[from] serde_json::Error), 

    #[error("Bincode error: {0}")]
    Bincode(#[from] Box<bincode::ErrorKind>),

    #[error("UTF-8 conversion error: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),

    #[error("Slice conversion error: {0}")]
    SliceConversion(String),

    #[error("Parse error: {0}")]
    Parse(String),

    // === Data errors ===
    #[error("Collection not found: {0}")] 
    CollectionNotFound(String), 

    #[error("Collection already exists: {0}")]
    CollectionAlreadyExists(String),
    
    #[error("Document not found: {0}")] 
    DocumentNotFound(String), 

    #[error("Vector dimension mismatch: expected {expected}, got {actual}")] 
    DimensionMismatch { expected: usize, actual: usize }, 

    #[error("Invalid dimension: {0}")]
    InvalidDimension(String),

    // === Index errors ===
    #[error("Index error: {0}")] 
    IndexError(String), 

    #[error("Index not found: {0}")]
    IndexNotFound(String),

    // === Storage errors ===
    #[error("Storage error: {0}")] 
    StorageError(String), 

    #[error("Storage not initialized")]
    StorageNotInitialized,

    // === Transaction errors ===
    #[error("Transaction not found: {0}")]
    TransactionNotFound(String),

    #[error("Invalid transaction state: {0}")]
    InvalidTransactionState(String),

    #[error("Write conflict on key: {0}")]
    WriteConflict(String),

    #[error("Snapshot not found: {0}")]
    SnapshotNotFound(String),

    // === CDC errors ===
    #[error("CDC connection error: {0}")]
    CdcConnectionError(String),

    #[error("CDC query error: {0}")]
    CdcQueryError(String),

    #[error("CDC position error: {0}")]
    CdcPositionError(String),

    #[error("CDC transform error: {0}")]
    CdcTransformError(String),

    // === Backup errors ===
    #[error("Backup not found: {0}")]
    BackupNotFound(String),

    #[error("Backup incomplete: {0}")]
    BackupIncomplete(String),

    // === Persistence errors ===
    #[error("Compression error: {0}")]
    CompressionError(String),

    #[error("Checkpoint not found: {0}")]
    CheckpointNotFound(String),

    // === Graph errors ===
    #[error("Node not found: {0}")]
    NodeNotFound(String),

    #[error("Edge not found: {0}")]
    EdgeNotFound(String),

    #[error("Node already exists: {0}")]
    NodeAlreadyExists(String),

    #[error("Edge already exists: {0}")]
    EdgeAlreadyExists(String),

    #[error("Invalid graph operation: {0}")]
    InvalidGraphOperation(String),

    // === General errors ===
    #[error("Validation error: {0}")] 
    ValidationError(String), 
    
    #[error("Configuration error: {0}")]
    ConfigError(String),

    #[error("Not implemented: {0}")]
    NotImplemented(String),

    #[error("Operation timed out: {0}")]
    Timeout(String),

    #[error("Connection error: {0}")]
    ConnectionError(String),

    #[error("Out of memory: {0}")]
    OutOfMemory(String),

    #[error("Internal error: {0}")]
    Internal(String),
}

// Additional From impls that can't use #[from] due to orphan rules or boxing
impl From<bincode::Error> for CoreTexError {
    fn from(e: bincode::Error) -> Self {
        CoreTexError::Bincode(Box::new(*e))
    }
}

impl From<std::array::TryFromSliceError> for CoreTexError {
    fn from(e: std::array::TryFromSliceError) -> Self {
        CoreTexError::SliceConversion(e.to_string())
    }
}

impl From<Box<dyn std::error::Error + Send + Sync>> for CoreTexError {
    fn from(e: Box<dyn std::error::Error + Send + Sync>) -> Self {
        CoreTexError::Internal(e.to_string())
    }
}

impl From<Box<dyn std::error::Error>> for CoreTexError {
    fn from(e: Box<dyn std::error::Error>) -> Self {
        CoreTexError::Internal(e.to_string())
    }
}

/// Standard result type for CoreTexDB
pub type Result<T> = std::result::Result<T, CoreTexError>; 