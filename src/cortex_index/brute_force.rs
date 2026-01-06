//! Brute force vector index implementation for CortexDB.
//!
//! This module provides a brute force vector index implementation that computes
//! distance between the query vector and all indexed vectors, returning the most
//! similar results. While simple, this approach is accurate and works well for
//! small to medium-sized datasets.

use crate::cortex_core::error::{CortexError, IndexError};
use crate::cortex_core::types::{Document, Embedding};
use crate::cortex_index::vector::{
    compute_distance, DistanceMetric, SearchResult, VectorIndex, VectorIndexBuilder,
    VectorIndexConfig,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, RwLock};

/// Brute force index configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BruteForceIndexConfig {
    /// Base vector index configuration
    pub base_config: VectorIndexConfig,
}

/// Brute force index builder
#[derive(Debug, Clone)]
pub struct BruteForceIndexBuilder {
    /// Index configuration
    config: BruteForceIndexConfig,
}

impl BruteForceIndexBuilder {
    /// Create a new brute force index builder
    pub fn new(config: BruteForceIndexConfig) -> Self {
        Self { config }
    }
}

impl VectorIndexBuilder for BruteForceIndexBuilder {
    fn build(&self) -> Result<Arc<dyn VectorIndex>, CortexError> {
        Ok(Arc::new(BruteForceIndex::new(
            self.config.base_config.clone(),
        )?))
    }

    fn index_type(&self) -> &str {
        "brute_force"
    }
}

/// Brute force vector index implementation
pub struct BruteForceIndex {
    /// Index configuration
    config: VectorIndexConfig,
    /// Stored vectors (document_id -> embedding)
    vectors: RwLock<HashMap<String, Embedding>>,
    /// Stored documents (document_id -> document)
    documents: RwLock<HashMap<String, Document>>,
}

impl Debug for BruteForceIndex {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BruteForceIndex")
            .field("config", &self.config)
            .field("vector_count", &self.vectors.read().unwrap().len())
            .field("document_count", &self.documents.read().unwrap().len())
            .finish()
    }
}

impl BruteForceIndex {
    /// Create a new brute force index
    pub fn new(config: VectorIndexConfig) -> Result<Self, CortexError> {
        if config.dimension == 0 {
            return Err(CortexError::Index(IndexError::InvalidVectorDimension(
                "Vector dimension must be greater than 0".to_string(),
            )));
        }

        Ok(Self {
            config,
            vectors: RwLock::new(HashMap::new()),
            documents: RwLock::new(HashMap::new()),
        })
    }
}

impl VectorIndex for BruteForceIndex {
    fn config(&self) -> &VectorIndexConfig {
        &self.config
    }

    fn add(
        &mut self,
        document_id: &str,
        embedding: &Embedding,
        document: Option<Document>,
    ) -> Result<(), CortexError> {
        if embedding.dimension() != self.config.dimension {
            return Err(CortexError::Index(IndexError::InvalidVectorDimension(
                format!(
                    "Vector dimension mismatch: expected {}, got {}",
                    self.config.dimension,
                    embedding.dimension()
                ),
            )));
        }

        let mut vectors = self.vectors.write().map_err(|e| {
            CortexError::Index(IndexError::InternalError(format!(
                "Failed to acquire write lock: {}",
                e
            )))
        })?;

        vectors.insert(document_id.to_string(), embedding.clone());

        if let Some(doc) = document {
            let mut documents = self.documents.write().map_err(|e| {
                CortexError::Index(IndexError::InternalError(format!(
                    "Failed to acquire write lock: {}",
                    e
                )))
            })?;
            documents.insert(document_id.to_string(), doc);
        }

        Ok(())
    }

    fn add_batch(
        &mut self,
        items: impl Iterator<Item = (String, Embedding, Option<Document>)>,
    ) -> Result<usize, CortexError> {
        let mut vectors = self.vectors.write().map_err(|e| {
            CortexError::Index(IndexError::InternalError(format!(
                "Failed to acquire write lock: {}",
                e
            )))
        })?;

        let mut documents = self.documents.write().map_err(|e| {
            CortexError::Index(IndexError::InternalError(format!(
                "Failed to acquire write lock: {}",
                e
            )))
        })?;

        let mut count = 0;

        for (document_id, embedding, document) in items {
            if embedding.dimension() != self.config.dimension {
                return Err(CortexError::Index(IndexError::InvalidVectorDimension(
                    format!(
                        "Vector dimension mismatch: expected {}, got {}",
                        self.config.dimension,
                        embedding.dimension()
                    ),
                )));
            }

            vectors.insert(document_id.clone(), embedding);

            if let Some(doc) = document {
                documents.insert(document_id, doc);
            }

            count += 1;
        }

        Ok(count)
    }

    fn search(
        &self,
        query: &Embedding,
        limit: usize,
        include_documents: bool,
    ) -> Result<Vec<SearchResult>, CortexError> {
        if query.dimension() != self.config.dimension {
            return Err(CortexError::Index(IndexError::InvalidVectorDimension(
                format!(
                    "Query vector dimension mismatch: expected {}, got {}",
                    self.config.dimension,
                    query.dimension()
                ),
            )));
        }

        let vectors = self.vectors.read().map_err(|e| {
            CortexError::Index(IndexError::InternalError(format!(
                "Failed to acquire read lock: {}",
                e
            )))
        })?;

        let documents = if include_documents {
            Some(self.documents.read().map_err(|e| {
                CortexError::Index(IndexError::InternalError(format!(
                    "Failed to acquire read lock: {}",
                    e
                )))
            })?)
        } else {
            None
        };

        let mut results = Vec::with_capacity(vectors.len());

        for (document_id, embedding) in vectors.iter() {
            let distance = compute_distance(query, embedding, self.config.distance_metric)?;
            results.push(SearchResult {
                document_id: document_id.clone(),
                score: distance,
                document: documents
                    .as_ref()
                    .and_then(|docs| docs.get(document_id).cloned()),
            });
        }

        // Sort by score (lower is better)
        results.sort_by(|a, b| a.score.partial_cmp(&b.score).unwrap());

        // Take top N results
        let limited_results = results.into_iter().take(limit).collect();

        Ok(limited_results)
    }

    fn search_with_threshold(
        &self,
        query: &Embedding,
        limit: usize,
        threshold: f32,
        include_documents: bool,
    ) -> Result<Vec<SearchResult>, CortexError> {
        let results = self.search(query, limit, include_documents)?;

        // Filter results by threshold
        let filtered_results = results
            .into_iter()
            .filter(|result| result.score <= threshold)
            .collect();

        Ok(filtered_results)
    }

    fn remove(&mut self, document_id: &str) -> Result<bool, CortexError> {
        let mut vectors = self.vectors.write().map_err(|e| {
            CortexError::Index(IndexError::InternalError(format!(
                "Failed to acquire write lock: {}",
                e
            )))
        })?;

        let mut documents = self.documents.write().map_err(|e| {
            CortexError::Index(IndexError::InternalError(format!(
                "Failed to acquire write lock: {}",
                e
            )))
        })?;

        let vector_removed = vectors.remove(document_id).is_some();
        let document_removed = documents.remove(document_id).is_some();

        Ok(vector_removed || document_removed)
    }

    fn size(&self) -> usize {
        self.vectors.read().unwrap().len()
    }

    fn clear(&mut self) -> Result<(), CortexError> {
        let mut vectors = self.vectors.write().map_err(|e| {
            CortexError::Index(IndexError::InternalError(format!(
                "Failed to acquire write lock: {}",
                e
            )))
        })?;

        let mut documents = self.documents.write().map_err(|e| {
            CortexError::Index(IndexError::InternalError(format!(
                "Failed to acquire write lock: {}",
                e
            )))
        })?;

        vectors.clear();
        documents.clear();

        Ok(())
    }

    fn optimize(&mut self) -> Result<(), CortexError> {
        // Brute force index doesn't require optimization
        Ok(())
    }

    fn statistics(&self) -> Result<serde_json::Value, CortexError> {
        let vectors = self.vectors.read().map_err(|e| {
            CortexError::Index(IndexError::InternalError(format!(
                "Failed to acquire read lock: {}",
                e
            )))
        })?;

        let documents = self.documents.read().map_err(|e| {
            CortexError::Index(IndexError::InternalError(format!(
                "Failed to acquire read lock: {}",
                e
            )))
        })?;

        let stats = serde_json::json!({
            "index_type": "brute_force",
            "vector_count": vectors.len(),
            "document_count": documents.len(),
            "dimension": self.config.dimension,
            "distance_metric": format!("{:?}", self.config.distance_metric),
        });

        Ok(stats)
    }
}

/// Test utilities for brute force index
#[cfg(test)]
mod tests {
    use super::*;
    use crate::cortex_core::types::Float32Vector;

    #[test]
    fn test_brute_force_index_basic() {
        let config = VectorIndexConfig {
            distance_metric: DistanceMetric::Euclidean,
            dimension: 3,
            ef_construction: None,
            ef_search: None,
            num_layers: None,
            parameters: None,
        };

        let mut index = BruteForceIndex::new(config).unwrap();

        // Add vectors
        let embedding1 = Float32Vector::from(vec![1.0, 2.0, 3.0]);
        let embedding2 = Float32Vector::from(vec![4.0, 5.0, 6.0]);
        let embedding3 = Float32Vector::from(vec![7.0, 8.0, 9.0]);

        index.add("doc1", &embedding1, None).unwrap();
        index.add("doc2", &embedding2, None).unwrap();
        index.add("doc3", &embedding3, None).unwrap();

        assert_eq!(index.size(), 3);

        // Search for similar vectors
        let query = Float32Vector::from(vec![1.1, 2.1, 3.1]);
        let results = index.search(&query, 2, false).unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].document_id, "doc1");
        assert_eq!(results[1].document_id, "doc2");

        // Remove a vector
        let removed = index.remove("doc2").unwrap();
        assert!(removed);
        assert_eq!(index.size(), 2);

        // Clear index
        index.clear().unwrap();
        assert_eq!(index.size(), 0);
    }

    #[test]
    fn test_brute_force_index_with_documents() {
        let config = VectorIndexConfig {
            distance_metric: DistanceMetric::Cosine,
            dimension: 2,
            ef_construction: None,
            ef_search: None,
            num_layers: None,
            parameters: None,
        };

        let mut index = BruteForceIndex::new(config).unwrap();

        // Add vectors with documents
        let embedding1 = Float32Vector::from(vec![1.0, 0.0]);
        let document1 = Document::new("doc1", serde_json::json!({ "text": "Hello" }));

        let embedding2 = Float32Vector::from(vec![0.0, 1.0]);
        let document2 = Document::new("doc2", serde_json::json!({ "text": "World" }));

        index.add("doc1", &embedding1, Some(document1)).unwrap();
        index.add("doc2", &embedding2, Some(document2)).unwrap();

        // Search with documents included
        let query = Float32Vector::from(vec![1.0, 0.0]);
        let results = index.search(&query, 2, true).unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].document_id, "doc1");
        assert!(results[0].document.is_some());
        assert_eq!(results[0].document.as_ref().unwrap().id(), "doc1");
    }

    #[test]
    fn test_brute_force_index_batch_add() {
        let config = VectorIndexConfig {
            distance_metric: DistanceMetric::Manhattan,
            dimension: 2,
            ef_construction: None,
            ef_search: None,
            num_layers: None,
            parameters: None,
        };

        let mut index = BruteForceIndex::new(config).unwrap();

        // Batch add vectors
        let items = vec![
            (
                "doc1".to_string(),
                Float32Vector::from(vec![1.0, 2.0]),
                None,
            ),
            (
                "doc2".to_string(),
                Float32Vector::from(vec![3.0, 4.0]),
                None,
            ),
            (
                "doc3".to_string(),
                Float32Vector::from(vec![5.0, 6.0]),
                None,
            ),
        ];

        let count = index.add_batch(items.into_iter()).unwrap();
        assert_eq!(count, 3);
        assert_eq!(index.size(), 3);
    }

    #[test]
    fn test_brute_force_index_threshold() {
        let config = VectorIndexConfig {
            distance_metric: DistanceMetric::Euclidean,
            dimension: 2,
            ef_construction: None,
            ef_search: None,
            num_layers: None,
            parameters: None,
        };

        let mut index = BruteForceIndex::new(config).unwrap();

        // Add vectors
        let embedding1 = Float32Vector::from(vec![1.0, 1.0]);
        let embedding2 = Float32Vector::from(vec![2.0, 2.0]);
        let embedding3 = Float32Vector::from(vec![10.0, 10.0]);

        index.add("doc1", &embedding1, None).unwrap();
        index.add("doc2", &embedding2, None).unwrap();
        index.add("doc3", &embedding3, None).unwrap();

        // Search with threshold
        let query = Float32Vector::from(vec![1.0, 1.0]);
        let results = index.search_with_threshold(&query, 3, 2.0, false).unwrap();

        // Should only return doc1 and doc2 (distance <= 2.0)
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].document_id, "doc1");
        assert_eq!(results[1].document_id, "doc2");
    }

    #[test]
    fn test_brute_force_index_statistics() {
        let config = VectorIndexConfig {
            distance_metric: DistanceMetric::Euclidean,
            dimension: 3,
            ef_construction: None,
            ef_search: None,
            num_layers: None,
            parameters: None,
        };

        let index = BruteForceIndex::new(config).unwrap();

        let stats = index.statistics().unwrap();
        assert_eq!(stats["vector_count"], 0);
        assert_eq!(stats["dimension"], 3);
    }
}
