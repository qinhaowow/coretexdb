//! Vector index abstraction for CortexDB.
//!
//! This module defines the VectorIndex trait and related types for vector indexing
//! in CortexDB, providing a common interface for different vector index implementations.

use crate::cortex_core::error::{CortexError, IndexError};
use crate::cortex_core::types::{Document, Embedding, Vector};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::sync::Arc;

/// Distance metric for vector similarity
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DistanceMetric {
    /// Euclidean distance (L2)
    Euclidean,
    /// Cosine similarity
    Cosine,
    /// Manhattan distance (L1)
    Manhattan,
}

/// Vector index configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorIndexConfig {
    /// Distance metric to use
    pub distance_metric: DistanceMetric,
    /// Dimension of vectors
    pub dimension: usize,
    /// Number of elements to search during approximate search
    pub ef_construction: Option<usize>,
    /// Number of elements to search during query
    pub ef_search: Option<usize>,
    /// Number of layers (for hierarchical indexes)
    pub num_layers: Option<usize>,
    /// Other index-specific parameters
    pub parameters: Option<serde_json::Value>,
}

/// Search result for vector queries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResult {
    /// Document ID
    pub document_id: String,
    /// Similarity score (lower is better for distance metrics)
    pub score: f32,
    /// Optional document (if included in search)
    pub document: Option<Document>,
}

/// Vector index trait defining the common interface for all vector index implementations
pub trait VectorIndex: Send + Sync + Debug {
    /// Get the index configuration
    fn config(&self) -> &VectorIndexConfig;

    /// Add a vector to the index
    ///
    /// # Arguments
    /// * `document_id` - Unique identifier for the document
    /// * `embedding` - Vector embedding to index
    /// * `document` - Optional document to store with the vector
    ///
    /// # Returns
    /// * `Ok(())` if the vector was added successfully
    /// * `Err(CortexError)` if there was an error adding the vector
    fn add(
        &mut self,
        document_id: &str,
        embedding: &Embedding,
        document: Option<Document>,
    ) -> Result<(), CortexError>;

    /// Add multiple vectors to the index in bulk
    ///
    /// # Arguments
    /// * `items` - Iterator of (document_id, embedding, document) tuples
    ///
    /// # Returns
    /// * `Ok(usize)` - Number of vectors added successfully
    /// * `Err(CortexError)` if there was an error adding vectors
    fn add_batch(
        &mut self,
        items: impl Iterator<Item = (String, Embedding, Option<Document>)>,
    ) -> Result<usize, CortexError>;

    /// Search for similar vectors in the index
    ///
    /// # Arguments
    /// * `query` - Query vector
    /// * `limit` - Maximum number of results to return
    /// * `include_documents` - Whether to include documents in results
    ///
    /// # Returns
    /// * `Ok(Vec<SearchResult>)` - List of search results sorted by similarity
    /// * `Err(CortexError)` if there was an error during search
    fn search(
        &self,
        query: &Embedding,
        limit: usize,
        include_documents: bool,
    ) -> Result<Vec<SearchResult>, CortexError>;

    /// Search for similar vectors with a distance threshold
    ///
    /// # Arguments
    /// * `query` - Query vector
    /// * `limit` - Maximum number of results to return
    /// * `threshold` - Distance threshold (results with score > threshold are filtered out)
    /// * `include_documents` - Whether to include documents in results
    ///
    /// # Returns
    /// * `Ok(Vec<SearchResult>)` - List of search results sorted by similarity
    /// * `Err(CortexError)` if there was an error during search
    fn search_with_threshold(
        &self,
        query: &Embedding,
        limit: usize,
        threshold: f32,
        include_documents: bool,
    ) -> Result<Vec<SearchResult>, CortexError>;

    /// Remove a vector from the index
    ///
    /// # Arguments
    /// * `document_id` - Unique identifier of the document to remove
    ///
    /// # Returns
    /// * `Ok(bool)` - Whether the document was found and removed
    /// * `Err(CortexError)` if there was an error removing the vector
    fn remove(&mut self, document_id: &str) -> Result<bool, CortexError>;

    /// Get the number of vectors in the index
    ///
    /// # Returns
    /// * `usize` - Number of vectors in the index
    fn size(&self) -> usize;

    /// Clear all vectors from the index
    ///
    /// # Returns
    /// * `Ok(())` if the index was cleared successfully
    /// * `Err(CortexError)` if there was an error clearing the index
    fn clear(&mut self) -> Result<(), CortexError>;

    /// Optimize the index for better performance
    ///
    /// # Returns
    /// * `Ok(())` if the index was optimized successfully
    /// * `Err(CortexError)` if there was an error optimizing the index
    fn optimize(&mut self) -> Result<(), CortexError>;

    /// Get index statistics
    ///
    /// # Returns
    /// * `Ok(serde_json::Value)` - Index statistics as JSON
    /// * `Err(CortexError)` if there was an error getting statistics
    fn statistics(&self) -> Result<serde_json::Value, CortexError>;
}

/// Compute distance between two vectors using the specified metric
///
/// # Arguments
/// * `a` - First vector
/// * `b` - Second vector
/// * `metric` - Distance metric to use
///
/// # Returns
/// * `f32` - Computed distance/similarity score
/// * `Err(CortexError)` if the vectors have different dimensions
pub fn compute_distance(a: &Embedding, b: &Embedding, metric: DistanceMetric) -> Result<f32, CortexError> {
    if a.dimension() != b.dimension() {
        return Err(CortexError::Index(IndexError::InvalidVectorDimension(
            format!(
                "Vectors have different dimensions: {} vs {}",
                a.dimension(),
                b.dimension()
            ),
        )));
    }

    match metric {
        DistanceMetric::Euclidean => compute_euclidean(a, b),
        DistanceMetric::Cosine => compute_cosine(a, b),
        DistanceMetric::Manhattan => compute_manhattan(a, b),
    }
}

/// Compute Euclidean distance between two vectors
fn compute_euclidean(a: &Embedding, b: &Embedding) -> Result<f32, CortexError> {
    let mut sum = 0.0;
    for (ai, bi) in a.values().iter().zip(b.values().iter()) {
        let diff = ai - bi;
        sum += diff * diff;
    }
    Ok(sum.sqrt())
}

/// Compute Cosine similarity between two vectors
fn compute_cosine(a: &Embedding, b: &Embedding) -> Result<f32, CortexError> {
    let mut dot_product = 0.0;
    let mut norm_a = 0.0;
    let mut norm_b = 0.0;

    for (ai, bi) in a.values().iter().zip(b.values().iter()) {
        dot_product += ai * bi;
        norm_a += ai * ai;
        norm_b += bi * bi;
    }

    if norm_a == 0.0 || norm_b == 0.0 {
        return Ok(0.0);
    }

    let similarity = dot_product / (norm_a.sqrt() * norm_b.sqrt());
    // Return 1 - similarity to make it a distance metric (lower is better)
    Ok(1.0 - similarity)
}

/// Compute Manhattan distance between two vectors
fn compute_manhattan(a: &Embedding, b: &Embedding) -> Result<f32, CortexError> {
    let mut sum = 0.0;
    for (ai, bi) in a.values().iter().zip(b.values().iter()) {
        sum += (ai - bi).abs();
    }
    Ok(sum)
}

/// Vector index builder trait for creating vector index instances
pub trait VectorIndexBuilder {
    /// Build a new vector index instance
    ///
    /// # Returns
    /// * `Ok(Arc<dyn VectorIndex>)` - New vector index instance
    /// * `Err(CortexError)` if there was an error building the index
    fn build(&self) -> Result<Arc<dyn VectorIndex>, CortexError>;

    /// Get the index type name
    fn index_type(&self) -> &str;
}

/// Test utilities for vector indexing
#[cfg(test)]
mod tests {
    use super::*;
    use crate::cortex_core::types::Float32Vector;

    #[test]
    fn test_euclidean_distance() {
        let a = Float32Vector::from(vec![1.0, 2.0, 3.0]);
        let b = Float32Vector::from(vec![4.0, 5.0, 6.0]);
        
        let distance = compute_distance(&a, &b, DistanceMetric::Euclidean).unwrap();
        assert!((distance - 5.196152).abs() < 1e-6);
    }

    #[test]
    fn test_cosine_similarity() {
        let a = Float32Vector::from(vec![1.0, 0.0, 0.0]);
        let b = Float32Vector::from(vec![1.0, 0.0, 0.0]);
        
        let distance = compute_distance(&a, &b, DistanceMetric::Cosine).unwrap();
        assert!((distance - 0.0).abs() < 1e-6);
        
        let c = Float32Vector::from(vec![0.0, 1.0, 0.0]);
        let distance = compute_distance(&a, &c, DistanceMetric::Cosine).unwrap();
        assert!((distance - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_manhattan_distance() {
        let a = Float32Vector::from(vec![1.0, 2.0, 3.0]);
        let b = Float32Vector::from(vec![4.0, 5.0, 6.0]);
        
        let distance = compute_distance(&a, &b, DistanceMetric::Manhattan).unwrap();
        assert!((distance - 9.0).abs() < 1e-6);
    }

    #[test]
    fn test_different_dimensions() {
        let a = Float32Vector::from(vec![1.0, 2.0, 3.0]);
        let b = Float32Vector::from(vec![4.0, 5.0]);
        
        let result = compute_distance(&a, &b, DistanceMetric::Euclidean);
        assert!(result.is_err());
    }
}
