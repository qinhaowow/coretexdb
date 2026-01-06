//! Scalar index implementation for CortexDB.
//!
//! This module provides scalar index functionality for CortexDB, including:
//! - Scalar index abstraction
//! - Hash index implementation for fast lookups
//! - B-tree index implementation for range queries
//! - Support for different scalar data types
//! - Index operations and maintenance

use crate::cortex_core::error::{CortexError, IndexError};
use crate::cortex_core::types::Document;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::sync::{Arc, RwLock};

/// Scalar index type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ScalarIndexType {
    /// Hash index for exact match lookups
    Hash,
    /// B-tree index for range queries
    BTree,
}

/// Scalar index configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScalarIndexConfig {
    /// Index type
    pub index_type: ScalarIndexType,
    /// Field name to index
    pub field_name: String,
    /// Whether the field is required
    pub required: bool,
    /// Whether the field has unique values
    pub unique: bool,
}

/// Scalar index trait defining the common interface for all scalar index implementations
pub trait ScalarIndex: Send + Sync + Debug {
    /// Get the index configuration
    fn config(&self) -> &ScalarIndexConfig;

    /// Add a document to the index
    ///
    /// # Arguments
    /// * `document_id` - Unique identifier for the document
    /// * `document` - Document to index
    ///
    /// # Returns
    /// * `Ok(())` if the document was added successfully
    /// * `Err(CortexError)` if there was an error adding the document
    fn add(&mut self, document_id: &str, document: &Document) -> Result<(), CortexError>;

    /// Remove a document from the index
    ///
    /// # Arguments
    /// * `document_id` - Unique identifier of the document to remove
    ///
    /// # Returns
    /// * `Ok(bool)` - Whether the document was found and removed
    /// * `Err(CortexError)` if there was an error removing the document
    fn remove(&mut self, document_id: &str) -> Result<bool, CortexError>;

    /// Find documents by exact value
    ///
    /// # Arguments
    /// * `value` - Value to search for
    ///
    /// # Returns
    /// * `Ok(Vec<String>)` - List of document IDs matching the value
    /// * `Err(CortexError)` if there was an error during search
    fn find_exact(&self, value: &serde_json::Value) -> Result<Vec<String>, CortexError>;

    /// Find documents by range
    ///
    /// # Arguments
    /// * `min` - Minimum value (inclusive)
    /// * `max` - Maximum value (inclusive)
    ///
    /// # Returns
    /// * `Ok(Vec<String>)` - List of document IDs matching the range
    /// * `Err(CortexError)` if there was an error during search
    fn find_range(
        &self,
        min: Option<&serde_json::Value>,
        max: Option<&serde_json::Value>,
    ) -> Result<Vec<String>, CortexError>;

    /// Get the number of entries in the index
    ///
    /// # Returns
    /// * `usize` - Number of entries in the index
    fn size(&self) -> usize;

    /// Clear all entries from the index
    ///
    /// # Returns
    /// * `Ok(())` if the index was cleared successfully
    /// * `Err(CortexError)` if there was an error clearing the index
    fn clear(&mut self) -> Result<(), CortexError>;

    /// Get index statistics
    ///
    /// # Returns
    /// * `Ok(serde_json::Value)` - Index statistics as JSON
    /// * `Err(CortexError)` if there was an error getting statistics
    fn statistics(&self) -> Result<serde_json::Value, CortexError>;
}

/// Hash index implementation for exact match lookups
pub struct HashIndex {
    /// Index configuration
    config: ScalarIndexConfig,
    /// Value to document IDs mapping
    value_to_docs: RwLock<HashMap<serde_json::Value, HashSet<String>>>,
    /// Document ID to value mapping
    doc_to_value: RwLock<HashMap<String, serde_json::Value>>,
}

impl Debug for HashIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HashIndex")
            .field("config", &self.config)
            .field("size", &self.size())
            .finish()
    }
}

impl HashIndex {
    /// Create a new hash index
    pub fn new(config: ScalarIndexConfig) -> Self {
        Self {
            config,
            value_to_docs: RwLock::new(HashMap::new()),
            doc_to_value: RwLock::new(HashMap::new()),
        }
    }
}

impl ScalarIndex for HashIndex {
    fn config(&self) -> &ScalarIndexConfig {
        &self.config
    }

    fn add(&mut self, document_id: &str, document: &Document) -> Result<(), CortexError> {
        let value = match document.fields().get(&self.config.field_name) {
            Some(value) if !value.is_null() => value.clone(),
            _ => {
                if self.config.required {
                    return Err(CortexError::Index(IndexError::MissingRequiredField(
                        format!(
                            "Required field '{}' is missing or null for document {}",
                            self.config.field_name, document_id
                        ),
                    )));
                }
                return Ok(());
            }
        };

        let mut value_to_docs = self.value_to_docs.write().map_err(|e| {
            CortexError::Index(IndexError::InternalError(format!(
                "Failed to acquire write lock: {}",
                e
            )))
        })?;

        let mut doc_to_value = self.doc_to_value.write().map_err(|e| {
            CortexError::Index(IndexError::InternalError(format!(
                "Failed to acquire write lock: {}",
                e
            )))
        })?;

        // Check for uniqueness if required
        if self.config.unique {
            if value_to_docs.contains_key(&value) {
                return Err(CortexError::Index(IndexError::DuplicateKey(
                    format!(
                        "Unique constraint violated for field '{}': value {:?} already exists",
                        self.config.field_name, value
                    ),
                )));
            }
        }

        // Remove old value if document already exists
        if let Some(old_value) = doc_to_value.remove(document_id) {
            if let Some(docs) = value_to_docs.get_mut(&old_value) {
                docs.remove(document_id);
                if docs.is_empty() {
                    value_to_docs.remove(&old_value);
                }
            }
        }

        // Add new value
        value_to_docs
            .entry(value.clone())
            .or_insert_with(HashSet::new)
            .insert(document_id.to_string());
        doc_to_value.insert(document_id.to_string(), value);

        Ok(())
    }

    fn remove(&mut self, document_id: &str) -> Result<bool, CortexError> {
        let mut doc_to_value = self.doc_to_value.write().map_err(|e| {
            CortexError::Index(IndexError::InternalError(format!(
                "Failed to acquire write lock: {}",
                e
            )))
        })?;

        let old_value = match doc_to_value.remove(document_id) {
            Some(value) => value,
            None => return Ok(false),
        };

        let mut value_to_docs = self.value_to_docs.write().map_err(|e| {
            CortexError::Index(IndexError::InternalError(format!(
                "Failed to acquire write lock: {}",
                e
            )))
        })?;

        if let Some(docs) = value_to_docs.get_mut(&old_value) {
            docs.remove(document_id);
            if docs.is_empty() {
                value_to_docs.remove(&old_value);
            }
        }

        Ok(true)
    }

    fn find_exact(&self, value: &serde_json::Value) -> Result<Vec<String>, CortexError> {
        let value_to_docs = self.value_to_docs.read().map_err(|e| {
            CortexError::Index(IndexError::InternalError(format!(
                "Failed to acquire read lock: {}",
                e
            )))
        })?;

        match value_to_docs.get(value) {
            Some(docs) => Ok(docs.iter().cloned().collect()),
            None => Ok(Vec::new()),
        }
    }

    fn find_range(
        &self,
        _min: Option<&serde_json::Value>,
        _max: Option<&serde_json::Value>,
    ) -> Result<Vec<String>, CortexError> {
        Err(CortexError::Index(IndexError::UnsupportedOperation(
            "Range queries are not supported by hash index".to_string(),
        )))
    }

    fn size(&self) -> usize {
        self.doc_to_value.read().unwrap().len()
    }

    fn clear(&mut self) -> Result<(), CortexError> {
        let mut value_to_docs = self.value_to_docs.write().map_err(|e| {
            CortexError::Index(IndexError::InternalError(format!(
                "Failed to acquire write lock: {}",
                e
            )))
        })?;

        let mut doc_to_value = self.doc_to_value.write().map_err(|e| {
            CortexError::Index(IndexError::InternalError(format!(
                "Failed to acquire write lock: {}",
                e
            )))
        })?;

        value_to_docs.clear();
        doc_to_value.clear();

        Ok(())
    }

    fn statistics(&self) -> Result<serde_json::Value, CortexError> {
        let value_to_docs = self.value_to_docs.read().map_err(|e| {
            CortexError::Index(IndexError::InternalError(format!(
                "Failed to acquire read lock: {}",
                e
            )))
        })?;

        let doc_to_value = self.doc_to_value.read().map_err(|e| {
            CortexError::Index(IndexError::InternalError(format!(
                "Failed to acquire read lock: {}",
                e
            )))
        })?;

        let stats = serde_json::json!({
            "index_type": "hash",
            "field_name": self.config.field_name,
            "entry_count": doc_to_value.len(),
            "unique_values": value_to_docs.len(),
            "unique_constraint": self.config.unique,
            "required": self.config.required,
        });

        Ok(stats)
    }
}

/// B-tree index implementation for range queries
///
/// Note: This is a simplified B-tree implementation for demonstration purposes.
/// In a production environment, you would likely use a more optimized implementation.
pub struct BTreeIndex {
    /// Index configuration
    config: ScalarIndexConfig,
    /// Sorted values (for range queries)
    sorted_values: RwLock<Vec<(serde_json::Value, String)>>,
    /// Document ID to value mapping
    doc_to_value: RwLock<HashMap<String, serde_json::Value>>,
}

impl Debug for BTreeIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BTreeIndex")
            .field("config", &self.config)
            .field("size", &self.size())
            .finish()
    }
}

impl BTreeIndex {
    /// Create a new B-tree index
    pub fn new(config: ScalarIndexConfig) -> Self {
        Self {
            config,
            sorted_values: RwLock::new(Vec::new()),
            doc_to_value: RwLock::new(HashMap::new()),
        }
    }

    /// Compare two JSON values for sorting
    fn compare_values(a: &serde_json::Value, b: &serde_json::Value) -> std::cmp::Ordering {
        use serde_json::Value;

        match (a, b) {
            (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
            (Value::Null, _) => std::cmp::Ordering::Less,
            (_, Value::Null) => std::cmp::Ordering::Greater,
            (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
            (Value::Number(a), Value::Number(b)) => {
                // Compare as f64 for simplicity
                let a_f64 = a.as_f64().unwrap_or(0.0);
                let b_f64 = b.as_f64().unwrap_or(0.0);
                a_f64.partial_cmp(&b_f64).unwrap_or(std::cmp::Ordering::Equal)
            }
            (Value::String(a), Value::String(b)) => a.cmp(b),
            (Value::Array(a), Value::Array(b)) => a.cmp(b),
            (Value::Object(a), Value::Object(b)) => a.cmp(b),
            // Different types - compare type names
            (a, b) => format!("{:?}", a).cmp(&format!("{:?}", b)),
        }
    }

    /// Rebuild the sorted values vector
    fn rebuild_sorted(&mut self) -> Result<(), CortexError> {
        let doc_to_value = self.doc_to_value.read().map_err(|e| {
            CortexError::Index(IndexError::InternalError(format!(
                "Failed to acquire read lock: {}",
                e
            )))
        })?;

        let mut sorted = doc_to_value
            .iter()
            .map(|(id, value)| (value.clone(), id.clone()))
            .collect::<Vec<_>>();

        sorted.sort_by(|(a_val, _), (b_val, _)| Self::compare_values(a_val, b_val));

        let mut sorted_values = self.sorted_values.write().map_err(|e| {
            CortexError::Index(IndexError::InternalError(format!(
                "Failed to acquire write lock: {}",
                e
            )))
        })?;

        *sorted_values = sorted;

        Ok(())
    }
}

impl ScalarIndex for BTreeIndex {
    fn config(&self) -> &ScalarIndexConfig {
        &self.config
    }

    fn add(&mut self, document_id: &str, document: &Document) -> Result<(), CortexError> {
        let value = match document.fields().get(&self.config.field_name) {
            Some(value) if !value.is_null() => value.clone(),
            _ => {
                if self.config.required {
                    return Err(CortexError::Index(IndexError::MissingRequiredField(
                        format!(
                            "Required field '{}' is missing or null for document {}",
                            self.config.field_name, document_id
                        ),
                    )));
                }
                return Ok(());
            }
        };

        let mut doc_to_value = self.doc_to_value.write().map_err(|e| {
            CortexError::Index(IndexError::InternalError(format!(
                "Failed to acquire write lock: {}",
                e
            )))
        })?;

        // Check for uniqueness if required
        if self.config.unique {
            for (existing_value, _) in &*self.sorted_values.read().unwrap() {
                if existing_value == &value {
                    return Err(CortexError::Index(IndexError::DuplicateKey(
                        format!(
                            "Unique constraint violated for field '{}': value {:?} already exists",
                            self.config.field_name, value
                        ),
                    )));
                }
            }
        }

        // Remove old value if document already exists
        doc_to_value.insert(document_id.to_string(), value);

        // Rebuild sorted values
        self.rebuild_sorted()?;

        Ok(())
    }

    fn remove(&mut self, document_id: &str) -> Result<bool, CortexError> {
        let mut doc_to_value = self.doc_to_value.write().map_err(|e| {
            CortexError::Index(IndexError::InternalError(format!(
                "Failed to acquire write lock: {}",
                e
            )))
        })?;

        let removed = doc_to_value.remove(document_id).is_some();

        if removed {
            self.rebuild_sorted()?;
        }

        Ok(removed)
    }

    fn find_exact(&self, value: &serde_json::Value) -> Result<Vec<String>, CortexError> {
        let sorted_values = self.sorted_values.read().map_err(|e| {
            CortexError::Index(IndexError::InternalError(format!(
                "Failed to acquire read lock: {}",
                e
            )))
        })?;

        // Binary search for the value
        let mut left = 0;
        let mut right = sorted_values.len();

        while left < right {
            let mid = (left + right) / 2;
            let cmp = Self::compare_values(&sorted_values[mid].0, value);
            
            match cmp {
                std::cmp::Ordering::Less => left = mid + 1,
                std::cmp::Ordering::Greater => right = mid,
                std::cmp::Ordering::Equal => {
                    // Found the value, collect all matching entries
                    let mut results = Vec::new();
                    // Check backward
                    let mut i = mid;
                    while i > 0 && Self::compare_values(&sorted_values[i-1].0, value) == std::cmp::Ordering::Equal {
                        i -= 1;
                    }
                    // Check forward
                    while i < sorted_values.len() && Self::compare_values(&sorted_values[i].0, value) == std::cmp::Ordering::Equal {
                        results.push(sorted_values[i].1.clone());
                        i += 1;
                    }
                    return Ok(results);
                }
            }
        }

        Ok(Vec::new())
    }

    fn find_range(
        &self,
        min: Option<&serde_json::Value>,
        max: Option<&serde_json::Value>,
    ) -> Result<Vec<String>, CortexError> {
        let sorted_values = self.sorted_values.read().map_err(|e| {
            CortexError::Index(IndexError::InternalError(format!(
                "Failed to acquire read lock: {}",
                e
            )))
        })?;

        let mut results = Vec::new();

        for (value, doc_id) in &sorted_values {
            let mut include = true;

            // Check minimum bound
            if let Some(min_val) = min {
                if Self::compare_values(value, min_val) == std::cmp::Ordering::Less {
                    include = false;
                }
            }

            // Check maximum bound
            if let Some(max_val) = max {
                if Self::compare_values(value, max_val) == std::cmp::Ordering::Greater {
                    include = false;
                }
            }

            if include {
                results.push(doc_id.clone());
            }
        }

        Ok(results)
    }

    fn size(&self) -> usize {
        self.doc_to_value.read().unwrap().len()
    }

    fn clear(&mut self) -> Result<(), CortexError> {
        let mut sorted_values = self.sorted_values.write().map_err(|e| {
            CortexError::Index(IndexError::InternalError(format!(
                "Failed to acquire write lock: {}",
                e
            )))
        })?;

        let mut doc_to_value = self.doc_to_value.write().map_err(|e| {
            CortexError::Index(IndexError::InternalError(format!(
                "Failed to acquire write lock: {}",
                e
            )))
        })?;

        sorted_values.clear();
        doc_to_value.clear();

        Ok(())
    }

    fn statistics(&self) -> Result<serde_json::Value, CortexError> {
        let doc_to_value = self.doc_to_value.read().map_err(|e| {
            CortexError::Index(IndexError::InternalError(format!(
                "Failed to acquire read lock: {}",
                e
            )))
        })?;

        let stats = serde_json::json!({
            "index_type": "b_tree",
            "field_name": self.config.field_name,
            "entry_count": doc_to_value.len(),
            "unique_constraint": self.config.unique,
            "required": self.config.required,
        });

        Ok(stats)
    }
}

/// Create a new scalar index based on configuration
pub fn create_scalar_index(config: ScalarIndexConfig) -> Arc<dyn ScalarIndex> {
    match config.index_type {
        ScalarIndexType::Hash => Arc::new(HashIndex::new(config)),
        ScalarIndexType::BTree => Arc::new(BTreeIndex::new(config)),
    }
}

/// Test utilities for scalar index
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_hash_index_basic() {
        let config = ScalarIndexConfig {
            index_type: ScalarIndexType::Hash,
            field_name: "id".to_string(),
            required: true,
            unique: true,
        };

        let mut index = HashIndex::new(config);

        // Add documents
        let doc1 = Document::new("doc1", json!({ "id": 1, "name": "Alice" }));
        let doc2 = Document::new("doc2", json!({ "id": 2, "name": "Bob" }));
        let doc3 = Document::new("doc3", json!({ "id": 3, "name": "Charlie" }));

        index.add("doc1", &doc1).unwrap();
        index.add("doc2", &doc2).unwrap();
        index.add("doc3", &doc3).unwrap();

        assert_eq!(index.size(), 3);

        // Find by exact value
        let results = index.find_exact(&json!(2)).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], "doc2");

        // Remove a document
        let removed = index.remove("doc2").unwrap();
        assert!(removed);
        assert_eq!(index.size(), 2);

        // Find non-existent value
        let results = index.find_exact(&json!(2)).unwrap();
        assert_eq!(results.len(), 0);

        // Clear index
        index.clear().unwrap();
        assert_eq!(index.size(), 0);
    }

    #[test]
    fn test_hash_index_unique_constraint() {
        let config = ScalarIndexConfig {
            index_type: ScalarIndexType::Hash,
            field_name: "email".to_string(),
            required: true,
            unique: true,
        };

        let mut index = HashIndex::new(config);

        // Add document
        let doc1 = Document::new("doc1", json!({ "email": "alice@example.com" }));
        index.add("doc1", &doc1).unwrap();

        // Try to add duplicate
        let doc2 = Document::new("doc2", json!({ "email": "alice@example.com" }));
        let result = index.add("doc2", &doc2);
        assert!(result.is_err());
    }

    #[test]
    fn test_btree_index_range_query() {
        let config = ScalarIndexConfig {
            index_type: ScalarIndexType::BTree,
            field_name: "age".to_string(),
            required: true,
            unique: false,
        };

        let mut index = BTreeIndex::new(config);

        // Add documents
        let doc1 = Document::new("doc1", json!({ "age": 20 }));
        let doc2 = Document::new("doc2", json!({ "age": 25 }));
        let doc3 = Document::new("doc3", json!({ "age": 30 }));
        let doc4 = Document::new("doc4", json!({ "age": 35 }));
        let doc5 = Document::new("doc5", json!({ "age": 40 }));

        index.add("doc1", &doc1).unwrap();
        index.add("doc2", &doc2).unwrap();
        index.add("doc3", &doc3).unwrap();
        index.add("doc4", &doc4).unwrap();
        index.add("doc5", &doc5).unwrap();

        assert_eq!(index.size(), 5);

        // Range query
        let results = index.find_range(Some(&json!(25)), Some(&json!(35))).unwrap();
        assert_eq!(results.len(), 3);
        assert!(results.contains(&"doc2".to_string()));
        assert!(results.contains(&"doc3".to_string()));
        assert!(results.contains(&"doc4".to_string()));

        // Exact query
        let results = index.find_exact(&json!(30)).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], "doc3");
    }

    #[test]
    fn test_btree_index_string_sorting() {
        let config = ScalarIndexConfig {
            index_type: ScalarIndexType::BTree,
            field_name: "name".to_string(),
            required: true,
            unique: false,
        };

        let mut index = BTreeIndex::new(config);

        // Add documents
        let doc1 = Document::new("doc1", json!({ "name": "Charlie" }));
        let doc2 = Document::new("doc2", json!({ "name": "Alice" }));
        let doc3 = Document::new("doc3", json!({ "name": "Bob" }));

        index.add("doc1", &doc1).unwrap();
        index.add("doc2", &doc2).unwrap();
        index.add("doc3", &doc3).unwrap();

        assert_eq!(index.size(), 3);

        // Range query (should return sorted)
        let results = index.find_range(Some(&json!("A")), Some(&json!("Z"))).unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0], "doc2"); // Alice
        assert_eq!(results[1], "doc3"); // Bob
        assert_eq!(results[2], "doc1"); // Charlie
    }
}
