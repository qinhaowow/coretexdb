//! Query builder for CortexDB.
//!
//! This module provides a fluent API for constructing queries in CortexDB, including:
//! - Vector similarity search queries
//! - Scalar filter queries
//! - Compound queries with multiple conditions
//! - Query pagination and sorting
//! - Query configuration and options

use crate::cortex_core::error::{CortexError, QueryError};
use crate::cortex_core::types::{Document, Embedding};
use serde::{Deserialize, Serialize};

/// Query type
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum QueryType {
    /// Vector similarity search
    VectorSearch,
    /// Scalar filter query
    ScalarFilter,
    /// Compound query with multiple conditions
    Compound,
}

/// Filter operator for scalar queries
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FilterOperator {
    /// Equal to
    Eq,
    /// Not equal to
    Ne,
    /// Greater than
    Gt,
    /// Greater than or equal to
    Gte,
    /// Less than
    Lt,
    /// Less than or equal to
    Lte,
    /// Contains (for strings and arrays)
    Contains,
    /// Not contains (for strings and arrays)
    NotContains,
    /// In (for lists)
    In,
    /// Not in (for lists)
    NotIn,
}

/// Logical operator for compound queries
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogicalOperator {
    /// Logical AND
    And,
    /// Logical OR
    Or,
    /// Logical NOT
    Not,
}

/// Scalar filter condition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterCondition {
    /// Field name
    pub field: String,
    /// Operator
    pub operator: FilterOperator,
    /// Value
    pub value: serde_json::Value,
}

/// Vector search condition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorSearchCondition {
    /// Query vector
    pub query: Embedding,
    /// Distance threshold (optional)
    pub threshold: Option<f32>,
    /// Vector field name (if multiple vector fields exist)
    pub vector_field: Option<String>,
}

/// Query condition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QueryCondition {
    /// Scalar filter condition
    Filter(FilterCondition),
    /// Vector search condition
    VectorSearch(VectorSearchCondition),
    /// Compound condition with logical operator
    Compound {
        /// Logical operator
        operator: LogicalOperator,
        /// Subconditions
        conditions: Vec<QueryCondition>,
    },
}

/// Query options
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryOptions {
    /// Maximum number of results to return
    pub limit: usize,
    /// Number of results to skip (for pagination)
    pub offset: usize,
    /// Whether to include documents in results
    pub include_documents: bool,
    /// Whether to include scores in results
    pub include_scores: bool,
    /// Sort field (optional)
    pub sort_field: Option<String>,
    /// Sort order (true for ascending, false for descending)
    pub sort_ascending: bool,
}

/// Query structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Query {
    /// Query type
    pub query_type: QueryType,
    /// Query condition
    pub condition: QueryCondition,
    /// Query options
    pub options: QueryOptions,
    /// Collection name
    pub collection: String,
}

/// Query result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    /// Document ID
    pub document_id: String,
    /// Document (if included)
    pub document: Option<Document>,
    /// Similarity score (for vector searches)
    pub score: Option<f32>,
    /// Rank position
    pub rank: usize,
}

/// Query builder
pub struct QueryBuilder {
    /// Query being built
    query: Query,
}

impl QueryBuilder {
    /// Create a new query builder for the specified collection
    pub fn new(collection: &str) -> Self {
        Self {
            query: Query {
                query_type: QueryType::ScalarFilter,
                condition: QueryCondition::Filter(FilterCondition {
                    field: "".to_string(),
                    operator: FilterOperator::Eq,
                    value: serde_json::Value::Null,
                }),
                options: QueryOptions {
                    limit: 10,
                    offset: 0,
                    include_documents: true,
                    include_scores: true,
                    sort_field: None,
                    sort_ascending: true,
                },
                collection: collection.to_string(),
            },
        }
    }

    /// Set the query limit
    pub fn limit(mut self, limit: usize) -> Self {
        self.query.options.limit = limit;
        self
    }

    /// Set the query offset
    pub fn offset(mut self, offset: usize) -> Self {
        self.query.options.offset = offset;
        self
    }

    /// Include documents in results
    pub fn include_documents(mut self, include: bool) -> Self {
        self.query.options.include_documents = include;
        self
    }

    /// Include scores in results
    pub fn include_scores(mut self, include: bool) -> Self {
        self.query.options.include_scores = include;
        self
    }

    /// Set sort field and order
    pub fn sort(mut self, field: &str, ascending: bool) -> Self {
        self.query.options.sort_field = Some(field.to_string());
        self.query.options.sort_ascending = ascending;
        self
    }

    /// Build a vector search query
    pub fn vector_search(mut self, query: Embedding, threshold: Option<f32>) -> Self {
        self.query.query_type = QueryType::VectorSearch;
        self.query.condition = QueryCondition::VectorSearch(VectorSearchCondition {
            query,
            threshold,
            vector_field: None,
        });
        self
    }

    /// Build a vector search query with a specific vector field
    pub fn vector_search_with_field(
        mut self,
        query: Embedding,
        vector_field: &str,
        threshold: Option<f32>,
    ) -> Self {
        self.query.query_type = QueryType::VectorSearch;
        self.query.condition = QueryCondition::VectorSearch(VectorSearchCondition {
            query,
            threshold,
            vector_field: Some(vector_field.to_string()),
        });
        self
    }

    /// Build a scalar filter query with equality
    pub fn filter_eq(self, field: &str, value: serde_json::Value) -> Self {
        self.filter(field, FilterOperator::Eq, value)
    }

    /// Build a scalar filter query with not equality
    pub fn filter_ne(self, field: &str, value: serde_json::Value) -> Self {
        self.filter(field, FilterOperator::Ne, value)
    }

    /// Build a scalar filter query with greater than
    pub fn filter_gt(self, field: &str, value: serde_json::Value) -> Self {
        self.filter(field, FilterOperator::Gt, value)
    }

    /// Build a scalar filter query with greater than or equal to
    pub fn filter_gte(self, field: &str, value: serde_json::Value) -> Self {
        self.filter(field, FilterOperator::Gte, value)
    }

    /// Build a scalar filter query with less than
    pub fn filter_lt(self, field: &str, value: serde_json::Value) -> Self {
        self.filter(field, FilterOperator::Lt, value)
    }

    /// Build a scalar filter query with less than or equal to
    pub fn filter_lte(self, field: &str, value: serde_json::Value) -> Self {
        self.filter(field, FilterOperator::Lte, value)
    }

    /// Build a scalar filter query with contains
    pub fn filter_contains(self, field: &str, value: serde_json::Value) -> Self {
        self.filter(field, FilterOperator::Contains, value)
    }

    /// Build a scalar filter query with not contains
    pub fn filter_not_contains(self, field: &str, value: serde_json::Value) -> Self {
        self.filter(field, FilterOperator::NotContains, value)
    }

    /// Build a scalar filter query with in
    pub fn filter_in(self, field: &str, value: serde_json::Value) -> Self {
        self.filter(field, FilterOperator::In, value)
    }

    /// Build a scalar filter query with not in
    pub fn filter_not_in(self, field: &str, value: serde_json::Value) -> Self {
        self.filter(field, FilterOperator::NotIn, value)
    }

    /// Build a scalar filter query with custom operator
    pub fn filter(mut self, field: &str, operator: FilterOperator, value: serde_json::Value) -> Self {
        self.query.query_type = QueryType::ScalarFilter;
        self.query.condition = QueryCondition::Filter(FilterCondition {
            field: field.to_string(),
            operator,
            value,
        });
        self
    }

    /// Build a compound AND query
    pub fn and(mut self, conditions: Vec<QueryCondition>) -> Self {
        self.query.query_type = QueryType::Compound;
        self.query.condition = QueryCondition::Compound {
            operator: LogicalOperator::And,
            conditions,
        };
        self
    }

    /// Build a compound OR query
    pub fn or(mut self, conditions: Vec<QueryCondition>) -> Self {
        self.query.query_type = QueryType::Compound;
        self.query.condition = QueryCondition::Compound {
            operator: LogicalOperator::Or,
            conditions,
        };
        self
    }

    /// Build a compound NOT query
    pub fn not(mut self, condition: QueryCondition) -> Self {
        self.query.query_type = QueryType::Compound;
        self.query.condition = QueryCondition::Compound {
            operator: LogicalOperator::Not,
            conditions: vec![condition],
        };
        self
    }

    /// Build the final query
    pub fn build(self) -> Result<Query, CortexError> {
        // Validate query
        self.validate()?;
        Ok(self.query)
    }

    /// Validate the query
    fn validate(&self) -> Result<(), CortexError> {
        match &self.query.condition {
            QueryCondition::Filter(condition) => {
                if condition.field.is_empty() {
                    return Err(CortexError::Query(QueryError::InvalidQuery(
                        "Filter field cannot be empty".to_string(),
                    )));
                }
            }
            QueryCondition::VectorSearch(condition) => {
                // Vector is already validated when created
            }
            QueryCondition::Compound { conditions, .. } => {
                if conditions.is_empty() {
                    return Err(CortexError::Query(QueryError::InvalidQuery(
                        "Compound query cannot have empty conditions".to_string(),
                    )));
                }
                // Validate each subcondition
                for condition in conditions {
                    self.validate_condition(condition)?;
                }
            }
        }

        if self.query.collection.is_empty() {
            return Err(CortexError::Query(QueryError::InvalidQuery(
                "Collection name cannot be empty".to_string(),
            )));
        }

        if self.query.options.limit == 0 {
            return Err(CortexError::Query(QueryError::InvalidQuery(
                "Query limit must be greater than 0".to_string(),
            )));
        }

        Ok(())
    }

    /// Validate a query condition
    fn validate_condition(&self, condition: &QueryCondition) -> Result<(), CortexError> {
        match condition {
            QueryCondition::Filter(filter) => {
                if filter.field.is_empty() {
                    return Err(CortexError::Query(QueryError::InvalidQuery(
                        "Filter field cannot be empty".to_string(),
                    )));
                }
            }
            QueryCondition::VectorSearch(_) => {
                // Vector is already validated when created
            }
            QueryCondition::Compound { conditions, .. } => {
                if conditions.is_empty() {
                    return Err(CortexError::Query(QueryError::InvalidQuery(
                        "Compound query cannot have empty conditions".to_string(),
                    )));
                }
                // Validate each subcondition
                for subcondition in conditions {
                    self.validate_condition(subcondition)?;
                }
            }
        }
        Ok(())
    }
}

/// Test utilities for query builder
#[cfg(test)]
mod tests {
    use super::*;
    use crate::cortex_core::types::Float32Vector;

    #[test]
    fn test_vector_search_query() {
        let embedding = Float32Vector::from(vec![1.0, 2.0, 3.0]);

        let query = QueryBuilder::new("collection1")
            .vector_search(embedding, Some(0.5))
            .limit(10)
            .include_documents(true)
            .build()
            .unwrap();

        assert_eq!(query.collection, "collection1");
        assert_eq!(query.query_type, QueryType::VectorSearch);
        assert_eq!(query.options.limit, 10);
        assert!(query.options.include_documents);

        if let QueryCondition::VectorSearch(condition) = &query.condition {
            assert_eq!(condition.threshold, Some(0.5));
            assert_eq!(condition.vector_field, None);
        } else {
            panic!("Expected vector search condition");
        }
    }

    #[test]
    fn test_scalar_filter_query() {
        let query = QueryBuilder::new("collection1")
            .filter_eq("age", serde_json::json!(30))
            .limit(5)
            .offset(2)
            .build()
            .unwrap();

        assert_eq!(query.collection, "collection1");
        assert_eq!(query.query_type, QueryType::ScalarFilter);
        assert_eq!(query.options.limit, 5);
        assert_eq!(query.options.offset, 2);

        if let QueryCondition::Filter(condition) = &query.condition {
            assert_eq!(condition.field, "age");
            assert_eq!(condition.operator, FilterOperator::Eq);
            assert_eq!(condition.value, serde_json::json!(30));
        } else {
            panic!("Expected scalar filter condition");
        }
    }

    #[test]
    fn test_compound_query() {
        let age_condition = QueryCondition::Filter(FilterCondition {
            field: "age".to_string(),
            operator: FilterOperator::Gte,
            value: serde_json::json!(18),
        });

        let name_condition = QueryCondition::Filter(FilterCondition {
            field: "name".to_string(),
            operator: FilterOperator::Contains,
            value: serde_json::json!("John"),
        });

        let query = QueryBuilder::new("collection1")
            .and(vec![age_condition, name_condition])
            .limit(20)
            .build()
            .unwrap();

        assert_eq!(query.collection, "collection1");
        assert_eq!(query.query_type, QueryType::Compound);
        assert_eq!(query.options.limit, 20);

        if let QueryCondition::Compound { operator, conditions } = &query.condition {
            assert_eq!(*operator, LogicalOperator::And);
            assert_eq!(conditions.len(), 2);
        } else {
            panic!("Expected compound condition");
        }
    }

    #[test]
    fn test_query_validation() {
        // Test empty collection name
        let result = QueryBuilder::new("")
            .filter_eq("age", serde_json::json!(30))
            .build();
        assert!(result.is_err());

        // Test empty filter field
        let result = QueryBuilder::new("collection1")
            .filter("", FilterOperator::Eq, serde_json::json!(30))
            .build();
        assert!(result.is_err());

        // Test zero limit
        let result = QueryBuilder::new("collection1")
            .filter_eq("age", serde_json::json!(30))
            .limit(0)
            .build();
        assert!(result.is_err());

        // Test empty compound query
        let result = QueryBuilder::new("collection1")
            .and(vec![])
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_query_options() {
        let query = QueryBuilder::new("collection1")
            .filter_eq("age", serde_json::json!(30))
            .limit(15)
            .offset(5)
            .include_documents(false)
            .include_scores(true)
            .sort("name", true)
            .build()
            .unwrap();

        assert_eq!(query.options.limit, 15);
        assert_eq!(query.options.offset, 5);
        assert!(!query.options.include_documents);
        assert!(query.options.include_scores);
        assert_eq!(query.options.sort_field, Some("name".to_string()));
        assert!(query.options.sort_ascending);
    }
}
