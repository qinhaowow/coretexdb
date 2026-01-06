//! Index manager for CortexDB.
//!
//! This module provides index management functionality for CortexDB, including:
//! - Index creation and deletion
//! - Vector index management
//! - Scalar index management
//! - Index lifecycle management
//! - Index statistics and monitoring

use crate::cortex_core::error::{CortexError, IndexError};
use crate::cortex_core::types::Document;
use crate::cortex_index::brute_force::{BruteForceIndex, BruteForceIndexBuilder, BruteForceIndexConfig};
use crate::cortex_index::scalar::{create_scalar_index, ScalarIndex, ScalarIndexConfig};
use crate::cortex_index::vector::{
    DistanceMetric, VectorIndex, VectorIndexConfig, VectorIndexBuilder,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{Arc, RwLock};

/// Index type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexType {
    /// Vector index for similarity search
    Vector,
    /// Scalar index for exact and range queries
    Scalar,
}

/// Index metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexMetadata {
    /// Index name
    pub name: String,
    /// Index type
    pub index_type: IndexType,
    /// Creation time (timestamp)
    pub created_at: u64,
    /// Last modified time (timestamp)
    pub modified_at: u64,
    /// Index configuration as JSON
    pub configuration: serde_json::Value,
}

/// Index manager
pub struct IndexManager {
    /// Vector indexes
    vector_indexes: RwLock<HashMap<String, Arc<dyn VectorIndex>>>,
    /// Scalar indexes
    scalar_indexes: RwLock<HashMap<String, Arc<dyn ScalarIndex>>>,
    /// Index metadata
    metadata: RwLock<HashMap<String, IndexMetadata>>,
}

impl Debug for IndexManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let vector_indexes = self.vector_indexes.read().unwrap();
        let scalar_indexes = self.scalar_indexes.read().unwrap();
        let metadata = self.metadata.read().unwrap();

        f.debug_struct("IndexManager")
            .field("vector_index_count", &vector_indexes.len())
            .field("scalar_index_count", &scalar_indexes.len())
            .field("total_index_count", &metadata.len())
            .finish()
    }
}

impl IndexManager {
    /// Create a new index manager
    pub fn new() -> Self {
        Self {
            vector_indexes: RwLock::new(HashMap::new()),
            scalar_indexes: RwLock::new(HashMap::new()),
            metadata: RwLock::new(HashMap::new()),
        }
    }

    /// Create a vector index
    pub fn create_vector_index(
        &self,
        name: &str,
        config: VectorIndexConfig,
    ) -> Result<Arc<dyn VectorIndex>, CortexError> {
        // Check if index already exists
        let metadata = self.metadata.read().unwrap();
        if metadata.contains_key(name) {
            return Err(CortexError::Index(IndexError::IndexAlreadyExists(
                format!("Index '{}' already exists", name),
            )));
        }
        drop(metadata);

        // Create brute force index builder
        let brute_force_config = BruteForceIndexConfig {
            base_config: config.clone(),
        };
        let builder = BruteForceIndexBuilder::new(brute_force_config);

        // Build the index
        let index = builder.build()?;

        // Add to manager
        let mut vector_indexes = self.vector_indexes.write().unwrap();
        let mut metadata = self.metadata.write().unwrap();

        vector_indexes.insert(name.to_string(), index.clone());

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        let index_metadata = IndexMetadata {
            name: name.to_string(),
            index_type: IndexType::Vector,
            created_at: now,
            modified_at: now,
            configuration: serde_json::to_value(config).map_err(|e| {
                CortexError::Index(IndexError::InternalError(format!(
                    "Failed to serialize configuration: {}",
                    e
                )))
            })?,
        };

        metadata.insert(name.to_string(), index_metadata);

        Ok(index)
    }

    /// Create a scalar index
    pub fn create_scalar_index(
        &self,
        name: &str,
        config: ScalarIndexConfig,
    ) -> Result<Arc<dyn ScalarIndex>, CortexError> {
        // Check if index already exists
        let metadata = self.metadata.read().unwrap();
        if metadata.contains_key(name) {
            return Err(CortexError::Index(IndexError::IndexAlreadyExists(
                format!("Index '{}' already exists", name),
            )));
        }
        drop(metadata);

        // Create scalar index
        let index = create_scalar_index(config.clone());

        // Add to manager
        let mut scalar_indexes = self.scalar_indexes.write().unwrap();
        let mut metadata = self.metadata.write().unwrap();

        scalar_indexes.insert(name.to_string(), index.clone());

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        let index_metadata = IndexMetadata {
            name: name.to_string(),
            index_type: IndexType::Scalar,
            created_at: now,
            modified_at: now,
            configuration: serde_json::to_value(config).map_err(|e| {
                CortexError::Index(IndexError::InternalError(format!(
                    "Failed to serialize configuration: {}",
                    e
                )))
            })?,
        };

        metadata.insert(name.to_string(), index_metadata);

        Ok(index)
    }

    /// Get a vector index
    pub fn get_vector_index(&self, name: &str) -> Result<Arc<dyn VectorIndex>, CortexError> {
        let vector_indexes = self.vector_indexes.read().unwrap();
        vector_indexes
            .get(name)
            .cloned()
            .ok_or_else(|| {
                CortexError::Index(IndexError::IndexNotFound(format!(
                    "Vector index '{}' not found",
                    name
                )))
            })
    }

    /// Get a scalar index
    pub fn get_scalar_index(&self, name: &str) -> Result<Arc<dyn ScalarIndex>, CortexError> {
        let scalar_indexes = self.scalar_indexes.read().unwrap();
        scalar_indexes
            .get(name)
            .cloned()
            .ok_or_else(|| {
                CortexError::Index(IndexError::IndexNotFound(format!(
                    "Scalar index '{}' not found",
                    name
                )))
            })
    }

    /// Get index metadata
    pub fn get_index_metadata(&self, name: &str) -> Result<IndexMetadata, CortexError> {
        let metadata = self.metadata.read().unwrap();
        metadata
            .get(name)
            .cloned()
            .ok_or_else(|| {
                CortexError::Index(IndexError::IndexNotFound(format!(
                    "Index '{}' not found",
                    name
                )))
            })
    }

    /// List all indexes
    pub fn list_indexes(&self) -> Vec<IndexMetadata> {
        let metadata = self.metadata.read().unwrap();
        metadata.values().cloned().collect()
    }

    /// Delete an index
    pub fn delete_index(&self, name: &str) -> Result<bool, CortexError> {
        let metadata = self.metadata.read().unwrap();
        let index_type = metadata.get(name).map(|m| m.index_type);
        drop(metadata);

        let index_type = match index_type {
            Some(typ) => typ,
            None => return Ok(false),
        };

        // Remove from appropriate index map
        match index_type {
            IndexType::Vector => {
                let mut vector_indexes = self.vector_indexes.write().unwrap();
                vector_indexes.remove(name);
            }
            IndexType::Scalar => {
                let mut scalar_indexes = self.scalar_indexes.write().unwrap();
                scalar_indexes.remove(name);
            }
        }

        // Remove from metadata
        let mut metadata = self.metadata.write().unwrap();
        metadata.remove(name);

        Ok(true)
    }

    /// Update an index (rebuild with new configuration)
    pub fn update_index(
        &self,
        name: &str,
        configuration: serde_json::Value,
    ) -> Result<(), CortexError> {
        let metadata = self.metadata.read().unwrap();
        let index_type = metadata
            .get(name)
            .ok_or_else(|| {
                CortexError::Index(IndexError::IndexNotFound(format!(
                    "Index '{}' not found",
                    name
                )))
            })?
            .index_type;
        drop(metadata);

        match index_type {
            IndexType::Vector => {
                let config: VectorIndexConfig = serde_json::from_value(configuration).map_err(|e| {
                    CortexError::Index(IndexError::InvalidConfiguration(format!(
                        "Invalid vector index configuration: {}",
                        e
                    )))
                })?;

                // Delete existing index
                self.delete_index(name)?;

                // Create new index with updated configuration
                self.create_vector_index(name, config)?;
            }
            IndexType::Scalar => {
                let config: ScalarIndexConfig = serde_json::from_value(configuration).map_err(|e| {
                    CortexError::Index(IndexError::InvalidConfiguration(format!(
                        "Invalid scalar index configuration: {}",
                        e
                    )))
                })?;

                // Delete existing index
                self.delete_index(name)?;

                // Create new index with updated configuration
                self.create_scalar_index(name, config)?;
            }
        }

        Ok(())
    }

    /// Get index statistics
    pub fn get_index_statistics(&self, name: &str) -> Result<serde_json::Value, CortexError> {
        let metadata = self.metadata.read().unwrap();
        let index_type = metadata
            .get(name)
            .ok_or_else(|| {
                CortexError::Index(IndexError::IndexNotFound(format!(
                    "Index '{}' not found",
                    name
                )))
            })?
            .index_type;
        drop(metadata);

        match index_type {
            IndexType::Vector => {
                let index = self.get_vector_index(name)?;
                index.statistics()
            }
            IndexType::Scalar => {
                let index = self.get_scalar_index(name)?;
                index.statistics()
            }
        }
    }

    /// Add a document to all relevant indexes
    pub fn add_document_to_indexes(
        &self,
        document_id: &str,
        document: &Document,
    ) -> Result<(), CortexError> {
        // Add to scalar indexes
        let scalar_indexes = self.scalar_indexes.read().unwrap();
        for (name, index) in &scalar_indexes {
            // Clone index to avoid holding lock during operation
            let index = index.clone();
            drop(scalar_indexes);

            // Create mutable reference (safe because we're working with Arc)
            let mut index = Arc::get_mut(&mut Arc::clone(&index))
                .ok_or_else(|| {
                    CortexError::Index(IndexError::InternalError(
                        "Failed to get mutable reference to scalar index".to_string(),
                    ))
                })?;

            index.add(document_id, document)?;

            // Re-acquire lock for next iteration
            let scalar_indexes = self.scalar_indexes.read().unwrap();
        }

        // Vector indexes typically don't add documents directly
        // They are usually populated through separate embedding insertion

        Ok(())
    }

    /// Remove a document from all indexes
    pub fn remove_document_from_indexes(
        &self,
        document_id: &str,
    ) -> Result<(), CortexError> {
        // Remove from scalar indexes
        let scalar_indexes = self.scalar_indexes.read().unwrap();
        for (name, index) in &scalar_indexes {
            // Clone index to avoid holding lock during operation
            let index = index.clone();
            drop(scalar_indexes);

            // Create mutable reference (safe because we're working with Arc)
            let mut index = Arc::get_mut(&mut Arc::clone(&index))
                .ok_or_else(|| {
                    CortexError::Index(IndexError::InternalError(
                        "Failed to get mutable reference to scalar index".to_string(),
                    ))
                })?;

            index.remove(document_id)?;

            // Re-acquire lock for next iteration
            let scalar_indexes = self.scalar_indexes.read().unwrap();
        }

        // Vector indexes typically don't remove documents directly
        // They are usually managed through separate embedding operations

        Ok(())
    }

    /// Clear all indexes
    pub fn clear_all_indexes(&self) -> Result<(), CortexError> {
        // Clear vector indexes
        let mut vector_indexes = self.vector_indexes.write().unwrap();
        vector_indexes.clear();

        // Clear scalar indexes
        let mut scalar_indexes = self.scalar_indexes.write().unwrap();
        scalar_indexes.clear();

        // Clear metadata
        let mut metadata = self.metadata.write().unwrap();
        metadata.clear();

        Ok(())
    }
}

/// Test utilities for index manager
#[cfg(test)]
mod tests {
    use super::*;
    use crate::cortex_core::types::{Document, Float32Vector};

    #[test]
    fn test_index_manager_vector_index() {
        let manager = IndexManager::new();

        // Create vector index
        let config = VectorIndexConfig {
            distance_metric: DistanceMetric::Euclidean,
            dimension: 3,
            ef_construction: None,
            ef_search: None,
            num_layers: None,
            parameters: None,
        };

        let index = manager.create_vector_index("vector_idx", config).unwrap();
        assert!(!index.is_null());

        // List indexes
        let indexes = manager.list_indexes();
        assert_eq!(indexes.len(), 1);
        assert_eq!(indexes[0].name, "vector_idx");
        assert_eq!(indexes[0].index_type, IndexType::Vector);

        // Get index metadata
        let metadata = manager.get_index_metadata("vector_idx").unwrap();
        assert_eq!(metadata.name, "vector_idx");

        // Delete index
        let deleted = manager.delete_index("vector_idx").unwrap();
        assert!(deleted);

        // List indexes after deletion
        let indexes = manager.list_indexes();
        assert_eq!(indexes.len(), 0);
    }

    #[test]
    fn test_index_manager_scalar_index() {
        use crate::cortex_index::scalar::ScalarIndexType;

        let manager = IndexManager::new();

        // Create scalar index
        let config = ScalarIndexConfig {
            index_type: ScalarIndexType::Hash,
            field_name: "id".to_string(),
            required: true,
            unique: true,
        };

        let index = manager.create_scalar_index("scalar_idx", config).unwrap();
        assert!(!index.is_null());

        // List indexes
        let indexes = manager.list_indexes();
        assert_eq!(indexes.len(), 1);
        assert_eq!(indexes[0].name, "scalar_idx");
        assert_eq!(indexes[0].index_type, IndexType::Scalar);

        // Add document to index
        let document = Document::new("doc1", serde_json::json!({ "id": 1, "name": "Alice" }));
        manager.add_document_to_indexes("doc1", &document).unwrap();

        // Delete index
        let deleted = manager.delete_index("scalar_idx").unwrap();
        assert!(deleted);
    }

    #[test]
    fn test_index_manager_document_operations() {
        use crate::cortex_index::scalar::ScalarIndexType;

        let manager = IndexManager::new();

        // Create scalar index
        let config = ScalarIndexConfig {
            index_type: ScalarIndexType::Hash,
            field_name: "id".to_string(),
            required: true,
            unique: true,
        };

        manager.create_scalar_index("scalar_idx", config).unwrap();

        // Add document
        let document = Document::new("doc1", serde_json::json!({ "id": 1, "name": "Alice" }));
        manager.add_document_to_indexes("doc1", &document).unwrap();

        // Remove document
        manager.remove_document_from_indexes("doc1").unwrap();

        // Clear all indexes
        manager.clear_all_indexes().unwrap();
        let indexes = manager.list_indexes();
        assert_eq!(indexes.len(), 0);
    }

    #[test]
    fn test_index_manager_error_handling() {
        let manager = IndexManager::new();

        // Create vector index
        let config = VectorIndexConfig {
            distance_metric: DistanceMetric::Euclidean,
            dimension: 3,
            ef_construction: None,
            ef_search: None,
            num_layers: None,
            parameters: None,
        };

        manager.create_vector_index("test_idx", config.clone()).unwrap();

        // Try to create duplicate index
        let result = manager.create_vector_index("test_idx", config);
        assert!(result.is_err());

        // Try to get non-existent index
        let result = manager.get_index_metadata("non_existent");
        assert!(result.is_err());
    }
}
