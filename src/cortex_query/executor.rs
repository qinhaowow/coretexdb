//! Query executor for CortexDB.
//!
//! This module provides query execution functionality for CortexDB, including:
//! - Execution of vector similarity search queries
//! - Execution of scalar filter queries
//! - Execution of compound queries
//! - Query result processing and pagination
//! - Query execution statistics and metrics

use crate::cortex_core::error::{CortexError, QueryError, StorageError};
use crate::cortex_core::types::{Document, Embedding};
use crate::cortex_index::{
    IndexManager, ScalarIndex, VectorIndex, VectorIndexConfig,
};
use crate::cortex_query::builder::{
    FilterOperator, LogicalOperator, Query, QueryCondition, QueryResult,
};
use crate::cortex_storage::engine::StorageEngine;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

/// Query execution statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryStats {
    /// Total execution time in milliseconds
    pub execution_time_ms: f64,
    /// Number of documents scanned
    pub documents_scanned: usize,
    /// Number of documents filtered
    pub documents_filtered: usize,
    /// Number of results returned
    pub results_returned: usize,
    /// Whether the query used an index
    pub used_index: bool,
    /// Index name used (if any)
    pub index_used: Option<String>,
}

/// Query executor
pub struct QueryExecutor {
    /// Storage engine
    storage: Arc<dyn StorageEngine>,
    /// Index manager
    index_manager: Arc<IndexManager>,
}

impl QueryExecutor {
    /// Create a new query executor
    pub fn new(storage: Arc<dyn StorageEngine>, index_manager: Arc<IndexManager>) -> Self {
        Self {
            storage,
            index_manager,
        }
    }

    /// Execute a query
    pub async fn execute(&self, query: &Query) -> Result<(Vec<QueryResult>, QueryStats), CortexError> {
        let start_time = Instant::now();

        let results = match query.query_type {
            crate::cortex_query::builder::QueryType::VectorSearch => {
                self.execute_vector_search(query).await?
            }
            crate::cortex_query::builder::QueryType::ScalarFilter => {
                self.execute_scalar_filter(query).await?
            }
            crate::cortex_query::builder::QueryType::Compound => {
                self.execute_compound(query).await?
            }
        };

        // Process results (pagination, sorting, etc.)
        let processed_results = self.process_results(results, query).await?;

        // Calculate statistics
        let execution_time = start_time.elapsed().as_secs_f64() * 1000.0;
        let stats = QueryStats {
            execution_time_ms: execution_time,
            documents_scanned: 0, // TODO: Track actual scanned documents
            documents_filtered: 0, // TODO: Track actual filtered documents
            results_returned: processed_results.len(),
            used_index: true, // TODO: Track actual index usage
            index_used: None, // TODO: Track actual index used
        };

        Ok((processed_results, stats))
    }

    /// Execute a vector search query
    async fn execute_vector_search(
        &self,
        query: &Query,
    ) -> Result<Vec<QueryResult>, CortexError> {
        if let QueryCondition::VectorSearch(condition) = &query.condition {
            // Get vector index for the collection
            let index_name = format!("{}_vector", query.collection);
            let vector_index = match self.index_manager.get_vector_index(&index_name) {
                Ok(index) => index,
                Err(_) => {
                    // Create a temporary index if none exists
                    let config = VectorIndexConfig {
                        distance_metric: condition.query.distance_metric(),
                        dimension: condition.query.dimension(),
                        ef_construction: None,
                        ef_search: None,
                        num_layers: None,
                        parameters: None,
                    };
                    self.index_manager.create_vector_index(&index_name, config)?
                }
            };

            // Perform vector search
            let search_results = vector_index.search(
                &condition.query,
                query.options.limit + query.options.offset,
                query.options.include_documents,
            )?;

            // Convert to QueryResult
            let mut results = Vec::with_capacity(search_results.len());
            for (rank, result) in search_results.iter().enumerate() {
                results.push(QueryResult {
                    document_id: result.document_id.clone(),
                    document: result.document.clone(),
                    score: Some(result.score),
                    rank: rank + 1,
                });
            }

            // Apply threshold if specified
            if let Some(threshold) = condition.threshold {
                results.retain(|result| {
                    if let Some(score) = result.score {
                        score <= threshold
                    } else {
                        true
                    }
                });
            }

            Ok(results)
        } else {
            Err(CortexError::Query(QueryError::InvalidQuery(
                "Expected vector search condition".to_string(),
            )))
        }
    }

    /// Execute a scalar filter query
    async fn execute_scalar_filter(
        &self,
        query: &Query,
    ) -> Result<Vec<QueryResult>, CortexError> {
        if let QueryCondition::Filter(condition) = &query.condition {
            // Try to use a scalar index if available
            let index_name = format!("{}_scalar_{}", query.collection, condition.field);
            let scalar_index = self.index_manager.get_scalar_index(&index_name);

            match scalar_index {
                Ok(index) => {
                    // Use index for faster filtering
                    self.execute_indexed_scalar_filter(index, condition, query).await
                }
                Err(_) => {
                    // Fall back to full collection scan
                    self.execute_full_scan_scalar_filter(condition, query).await
                }
            }
        } else {
            Err(CortexError::Query(QueryError::InvalidQuery(
                "Expected scalar filter condition".to_string(),
            )))
        }
    }

    /// Execute a scalar filter using an index
    async fn execute_indexed_scalar_filter(
        &self,
        index: Arc<dyn ScalarIndex>,
        condition: &crate::cortex_query::builder::FilterCondition,
        query: &Query,
    ) -> Result<Vec<QueryResult>, CortexError> {
        let document_ids = match condition.operator {
            FilterOperator::Eq => index.find_exact(&condition.value)?,
            FilterOperator::Ne => {
                // For NOT EQUAL, we need to get all documents and exclude matches
                let all_docs = self.storage.list_documents(&query.collection).await?;
                let matching_docs = index.find_exact(&condition.value)?;
                let matching_set: HashSet<_> = matching_docs.into_iter().collect();
                all_docs
                    .into_iter()
                    .filter(|id| !matching_set.contains(id))
                    .collect()
            }
            FilterOperator::Gt | FilterOperator::Gte | FilterOperator::Lt | FilterOperator::Lte => {
                // For range operators, use range query if supported
                let min = match condition.operator {
                    FilterOperator::Gt | FilterOperator::Gte => Some(&condition.value),
                    _ => None,
                };
                let max = match condition.operator {
                    FilterOperator::Lt | FilterOperator::Lte => Some(&condition.value),
                    _ => None,
                };
                index.find_range(min, max)?
            }
            _ => {
                // For other operators, fall back to full scan
                return self.execute_full_scan_scalar_filter(condition, query).await;
            }
        };

        // Get documents and create results
        self.create_results_from_document_ids(&document_ids, query).await
    }

    /// Execute a scalar filter with full collection scan
    async fn execute_full_scan_scalar_filter(
        &self,
        condition: &crate::cortex_query::builder::FilterCondition,
        query: &Query,
    ) -> Result<Vec<QueryResult>, CortexError> {
        let document_ids = self.storage.list_documents(&query.collection).await?;
        let mut matching_docs = Vec::new();

        for doc_id in document_ids {
            let document = self.storage.get_document(&query.collection, &doc_id).await?;
            if self.matches_filter(&document, condition)? {
                matching_docs.push(doc_id);
            }
        }

        self.create_results_from_document_ids(&matching_docs, query).await
    }

    /// Execute a compound query
    async fn execute_compound(
        &self,
        query: &Query,
    ) -> Result<Vec<QueryResult>, CortexError> {
        if let QueryCondition::Compound { operator, conditions } = &query.condition {
            match operator {
                LogicalOperator::And => {
                    self.execute_and_query(conditions, query).await
                }
                LogicalOperator::Or => {
                    self.execute_or_query(conditions, query).await
                }
                LogicalOperator::Not => {
                    self.execute_not_query(conditions, query).await
                }
            }
        } else {
            Err(CortexError::Query(QueryError::InvalidQuery(
                "Expected compound condition".to_string(),
            )))
        }
    }

    /// Execute an AND compound query
    async fn execute_and_query(
        &self,
        conditions: &[QueryCondition],
        query: &Query,
    ) -> Result<Vec<QueryResult>, CortexError> {
        if conditions.is_empty() {
            return Ok(Vec::new());
        }

        // Execute first condition to get initial results
        let first_query = Query {
            condition: conditions[0].clone(),
            ..query.clone()
        };
        let (mut results, _) = self.execute(&first_query).await?;

        // Intersect with results from other conditions
        for condition in &conditions[1..] {
            let sub_query = Query {
                condition: condition.clone(),
                ..query.clone()
            };
            let (sub_results, _) = self.execute(&sub_query).await?;

            // Get document IDs from sub-results
            let sub_doc_ids: HashSet<_> = sub_results
                .into_iter()
                .map(|r| r.document_id)
                .collect();

            // Keep only results that are in both sets
            results.retain(|r| sub_doc_ids.contains(&r.document_id));
        }

        Ok(results)
    }

    /// Execute an OR compound query
    async fn execute_or_query(
        &self,
        conditions: &[QueryCondition],
        query: &Query,
    ) -> Result<Vec<QueryResult>, CortexError> {
        let mut all_results = HashMap::new();

        for condition in conditions {
            let sub_query = Query {
                condition: condition.clone(),
                ..query.clone()
            };
            let (sub_results, _) = self.execute(&sub_query).await?;

            // Add results to the set (using document ID as key to avoid duplicates)
            for result in sub_results {
                all_results.insert(result.document_id.clone(), result);
            }
        }

        // Convert back to vector
        Ok(all_results.into_values().collect())
    }

    /// Execute a NOT compound query
    async fn execute_not_query(
        &self,
        conditions: &[QueryCondition],
        query: &Query,
    ) -> Result<Vec<QueryResult>, CortexError> {
        if conditions.len() != 1 {
            return Err(CortexError::Query(QueryError::InvalidQuery(
                "NOT query must have exactly one condition".to_string(),
            )));
        }

        // Get all documents in the collection
        let all_doc_ids = self.storage.list_documents(&query.collection).await?;
        let all_doc_set: HashSet<_> = all_doc_ids.into_iter().collect();

        // Execute the NOT condition
        let sub_query = Query {
            condition: conditions[0].clone(),
            ..query.clone()
        };
        let (sub_results, _) = self.execute(&sub_query).await?;

        // Get document IDs from sub-results
        let sub_doc_ids: HashSet<_> = sub_results
            .into_iter()
            .map(|r| r.document_id)
            .collect();

        // Keep only documents that are NOT in the sub-results
        let not_doc_ids: Vec<_> = all_doc_set
            .into_iter()
            .filter(|id| !sub_doc_ids.contains(id))
            .collect();

        self.create_results_from_document_ids(&not_doc_ids, query).await
    }

    /// Create query results from document IDs
    async fn create_results_from_document_ids(
        &self,
        document_ids: &[String],
        query: &Query,
    ) -> Result<Vec<QueryResult>, CortexError> {
        let mut results = Vec::with_capacity(document_ids.len());

        for (rank, doc_id) in document_ids.iter().enumerate() {
            let document = if query.options.include_documents {
                Some(self.storage.get_document(&query.collection, doc_id).await?)
            } else {
                None
            };

            results.push(QueryResult {
                document_id: doc_id.clone(),
                document,
                score: None, // Scalar filters don't have scores
                rank: rank + 1,
            });
        }

        Ok(results)
    }

    /// Process results (pagination, sorting, etc.)
    async fn process_results(
        &self,
        mut results: Vec<QueryResult>,
        query: &Query,
    ) -> Result<Vec<QueryResult>, CortexError> {
        // Sort results if needed
        if let Some(sort_field) = &query.options.sort_field {
            results.sort_by(|a, b| {
                let a_val = a.document.as_ref().and_then(|doc| doc.fields().get(sort_field));
                let b_val = b.document.as_ref().and_then(|doc| doc.fields().get(sort_field));

                self.compare_values(a_val, b_val, query.options.sort_ascending)
            });
        } else if query.query_type == crate::cortex_query::builder::QueryType::VectorSearch {
            // For vector search, sort by score if not already sorted
            results.sort_by(|a, b| {
                let a_score = a.score.unwrap_or(f32::MAX);
                let b_score = b.score.unwrap_or(f32::MAX);
                a_score.partial_cmp(&b_score).unwrap_or(std::cmp::Ordering::Equal)
            });
        }

        // Apply pagination
        let offset = query.options.offset;
        let limit = query.options.limit;
        let end = offset + limit;
        let paginated_results = if end > results.len() {
            results.into_iter().skip(offset).collect()
        } else {
            results.into_iter().skip(offset).take(limit).collect()
        };

        // Re-rank results after pagination
        let final_results = paginated_results
            .into_iter()
            .enumerate()
            .map(|(idx, mut result)| {
                result.rank = idx + 1;
                result
            })
            .collect();

        Ok(final_results)
    }

    /// Compare two JSON values for sorting
    fn compare_values(
        &self,
        a: Option<&serde_json::Value>,
        b: Option<&serde_json::Value>,
        ascending: bool,
    ) -> std::cmp::Ordering {
        use serde_json::Value;

        let cmp = match (a, b) {
            (None, None) => std::cmp::Ordering::Equal,
            (None, Some(_)) => std::cmp::Ordering::Less,
            (Some(_), None) => std::cmp::Ordering::Greater,
            (Some(a_val), Some(b_val)) => match (a_val, b_val) {
                (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
                (Value::Null, _) => std::cmp::Ordering::Less,
                (_, Value::Null) => std::cmp::Ordering::Greater,
                (Value::Bool(a_bool), Value::Bool(b_bool)) => a_bool.cmp(b_bool),
                (Value::Number(a_num), Value::Number(b_num)) => {
                    let a_f64 = a_num.as_f64().unwrap_or(0.0);
                    let b_f64 = b_num.as_f64().unwrap_or(0.0);
                    a_f64.partial_cmp(&b_f64).unwrap_or(std::cmp::Ordering::Equal)
                }
                (Value::String(a_str), Value::String(b_str)) => a_str.cmp(b_str),
                (Value::Array(a_arr), Value::Array(b_arr)) => a_arr.cmp(b_arr),
                (Value::Object(a_obj), Value::Object(b_obj)) => a_obj.cmp(b_obj),
                (a_val, b_val) => format!("{:?}", a_val).cmp(&format!("{:?}", b_val)),
            },
        };

        if ascending {
            cmp
        } else {
            cmp.reverse()
        }
    }

    /// Check if a document matches a filter condition
    fn matches_filter(
        &self,
        document: &Document,
        condition: &crate::cortex_query::builder::FilterCondition,
    ) -> Result<bool, CortexError> {
        let field_value = match document.fields().get(&condition.field) {
            Some(value) if !value.is_null() => value,
            _ => return Ok(false),
        };

        match condition.operator {
            FilterOperator::Eq => Ok(field_value == &condition.value),
            FilterOperator::Ne => Ok(field_value != &condition.value),
            FilterOperator::Gt => self.compare_values_gt(field_value, &condition.value),
            FilterOperator::Gte => self.compare_values_gte(field_value, &condition.value),
            FilterOperator::Lt => self.compare_values_lt(field_value, &condition.value),
            FilterOperator::Lte => self.compare_values_lte(field_value, &condition.value),
            FilterOperator::Contains => self.compare_values_contains(field_value, &condition.value),
            FilterOperator::NotContains => {
                let contains = self.compare_values_contains(field_value, &condition.value)?;
                Ok(!contains)
            }
            FilterOperator::In => self.compare_values_in(field_value, &condition.value),
            FilterOperator::NotIn => {
                let in_list = self.compare_values_in(field_value, &condition.value)?;
                Ok(!in_list)
            }
        }
    }

    /// Compare values for greater than
    fn compare_values_gt(
        &self,
        a: &serde_json::Value,
        b: &serde_json::Value,
    ) -> Result<bool, CortexError> {
        use serde_json::Value;

        match (a, b) {
            (Value::Number(a_num), Value::Number(b_num)) => {
                let a_f64 = a_num.as_f64().unwrap_or(0.0);
                let b_f64 = b_num.as_f64().unwrap_or(0.0);
                Ok(a_f64 > b_f64)
            }
            (Value::String(a_str), Value::String(b_str)) => Ok(a_str > b_str),
            _ => Err(CortexError::Query(QueryError::InvalidQuery(
                "Cannot compare values for GT operation".to_string(),
            ))),
        }
    }

    /// Compare values for greater than or equal to
    fn compare_values_gte(
        &self,
        a: &serde_json::Value,
        b: &serde_json::Value,
    ) -> Result<bool, CortexError> {
        use serde_json::Value;

        match (a, b) {
            (Value::Number(a_num), Value::Number(b_num)) => {
                let a_f64 = a_num.as_f64().unwrap_or(0.0);
                let b_f64 = b_num.as_f64().unwrap_or(0.0);
                Ok(a_f64 >= b_f64)
            }
            (Value::String(a_str), Value::String(b_str)) => Ok(a_str >= b_str),
            _ => Err(CortexError::Query(QueryError::InvalidQuery(
                "Cannot compare values for GTE operation".to_string(),
            ))),
        }
    }

    /// Compare values for less than
    fn compare_values_lt(
        &self,
        a: &serde_json::Value,
        b: &serde_json::Value,
    ) -> Result<bool, CortexError> {
        use serde_json::Value;

        match (a, b) {
            (Value::Number(a_num), Value::Number(b_num)) => {
                let a_f64 = a_num.as_f64().unwrap_or(0.0);
                let b_f64 = b_num.as_f64().unwrap_or(0.0);
                Ok(a_f64 < b_f64)
            }
            (Value::String(a_str), Value::String(b_str)) => Ok(a_str < b_str),
            _ => Err(CortexError::Query(QueryError::InvalidQuery(
                "Cannot compare values for LT operation".to_string(),
            ))),
        }
    }

    /// Compare values for less than or equal to
    fn compare_values_lte(
        &self,
        a: &serde_json::Value,
        b: &serde_json::Value,
    ) -> Result<bool, CortexError> {
        use serde_json::Value;

        match (a, b) {
            (Value::Number(a_num), Value::Number(b_num)) => {
                let a_f64 = a_num.as_f64().unwrap_or(0.0);
                let b_f64 = b_num.as_f64().unwrap_or(0.0);
                Ok(a_f64 <= b_f64)
            }
            (Value::String(a_str), Value::String(b_str)) => Ok(a_str <= b_str),
            _ => Err(CortexError::Query(QueryError::InvalidQuery(
                "Cannot compare values for LTE operation".to_string(),
            ))),
        }
    }

    /// Check if a value contains another value
    fn compare_values_contains(
        &self,
        a: &serde_json::Value,
        b: &serde_json::Value,
    ) -> Result<bool, CortexError> {
        use serde_json::Value;

        match (a, b) {
            (Value::String(a_str), Value::String(b_str)) => Ok(a_str.contains(b_str)),
            (Value::Array(a_arr), _) => Ok(a_arr.contains(b)),
            _ => Err(CortexError::Query(QueryError::InvalidQuery(
                "Cannot check contains operation on these value types".to_string(),
            ))),
        }
    }

    /// Check if a value is in a list
    fn compare_values_in(
        &self,
        a: &serde_json::Value,
        b: &serde_json::Value,
    ) -> Result<bool, CortexError> {
        if let serde_json::Value::Array(b_arr) = b {
            Ok(b_arr.contains(a))
        } else {
            Err(CortexError::Query(QueryError::InvalidQuery(
                "IN operator requires a list value".to_string(),
            )))
        }
    }
}

/// Test utilities for query executor
#[cfg(test)]
mod tests {
    use super::*;
    use crate::cortex_core::types::{Document, Float32Vector};
    use crate::cortex_index::IndexManager;
    use crate::cortex_storage::memory::MemoryStorage;

    #[test]
    fn test_query_executor_creation() {
        let storage = Arc::new(MemoryStorage::new());
        let index_manager = Arc::new(IndexManager::new());

        let executor = QueryExecutor::new(storage, index_manager);
        assert!(!executor.is_null());
    }

    #[test]
    fn test_matches_filter() {
        let storage = Arc::new(MemoryStorage::new());
        let index_manager = Arc::new(IndexManager::new());
        let executor = QueryExecutor::new(storage, index_manager);

        let document = Document::new(
            "doc1",
            serde_json::json!({
                "id": 1,
                "name": "Alice",
                "age": 30,
                "tags": ["developer", "rust"]
            }),
        );

        // Test Eq
        let condition = crate::cortex_query::builder::FilterCondition {
            field: "age".to_string(),
            operator: FilterOperator::Eq,
            value: serde_json::json!(30),
        };
        assert!(executor.matches_filter(&document, &condition).unwrap());

        // Test Ne
        let condition = crate::cortex_query::builder::FilterCondition {
            field: "age".to_string(),
            operator: FilterOperator::Ne,
            value: serde_json::json!(25),
        };
        assert!(executor.matches_filter(&document, &condition).unwrap());

        // Test Gt
        let condition = crate::cortex_query::builder::FilterCondition {
            field: "age".to_string(),
            operator: FilterOperator::Gt,
            value: serde_json::json!(25),
        };
        assert!(executor.matches_filter(&document, &condition).unwrap());

        // Test Contains
        let condition = crate::cortex_query::builder::FilterCondition {
            field: "tags".to_string(),
            operator: FilterOperator::Contains,
            value: serde_json::json!("rust"),
        };
        assert!(executor.matches_filter(&document, &condition).unwrap());

        // Test In
        let condition = crate::cortex_query::builder::FilterCondition {
            field: "age".to_string(),
            operator: FilterOperator::In,
            value: serde_json::json!([25, 30, 35]),
        };
        assert!(executor.matches_filter(&document, &condition).unwrap());
    }
}
