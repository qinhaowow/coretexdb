//! Query planner for CortexDB.
//!
//! This module provides query planning functionality for CortexDB, including:
//! - Query parsing and analysis
//! - Execution plan generation
//! - Plan optimization strategies
//! - Cost estimation for different execution paths
//! - Plan caching and reuse

use crate::cortex_core::error::{CortexError, QueryError};
use crate::cortex_query::builder::{Query, QueryCondition, QueryType};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// Execution plan type
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PlanType {
    /// Full collection scan
    FullScan,
    /// Index scan
    IndexScan,
    /// Vector search
    VectorSearch,
    /// Compound plan with multiple steps
    Compound,
}

/// Execution plan step
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanStep {
    /// Step type
    pub step_type: PlanType,
    /// Index name (if using an index)
    pub index_name: Option<String>,
    /// Estimated cost
    pub estimated_cost: f64,
    /// Estimated rows
    pub estimated_rows: usize,
    /// Child steps (for compound plans)
    pub children: Vec<PlanStep>,
    /// Additional step-specific metadata
    pub metadata: serde_json::Value,
}

/// Execution plan
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionPlan {
    /// Root plan step
    pub root: PlanStep,
    /// Total estimated cost
    pub total_cost: f64,
    /// Total estimated rows
    pub total_rows: usize,
    /// Plan generation time in milliseconds
    pub generation_time_ms: f64,
    /// Whether the plan uses an index
    pub uses_index: bool,
}

/// Query planner
pub struct QueryPlanner {
    /// Plan cache
    plan_cache: HashMap<String, ExecutionPlan>,
    /// Cache size limit
    cache_size_limit: usize,
}

impl QueryPlanner {
    /// Create a new query planner
    pub fn new(cache_size_limit: usize) -> Self {
        Self {
            plan_cache: HashMap::new(),
            cache_size_limit,
        }
    }

    /// Create a query planner with default settings
    pub fn with_defaults() -> Self {
        Self::new(1000) // Default cache size limit
    }

    /// Generate an execution plan for a query
    pub fn generate_plan(&mut self, query: &Query) -> Result<ExecutionPlan, CortexError> {
        let start_time = std::time::Instant::now();

        // Check if we have a cached plan for this query
        let query_key = self.generate_query_key(query);
        if let Some(cached_plan) = self.plan_cache.get(&query_key) {
            return Ok(cached_plan.clone());
        }

        // Generate plan based on query type
        let plan = match query.query_type {
            QueryType::VectorSearch => self.plan_vector_search(query)?,
            QueryType::ScalarFilter => self.plan_scalar_filter(query)?,
            QueryType::Compound => self.plan_compound(query)?,
        };

        // Calculate generation time
        let generation_time = start_time.elapsed().as_secs_f64() * 1000.0;

        let execution_plan = ExecutionPlan {
            root: plan,
            total_cost: self.calculate_total_cost(&plan),
            total_rows: self.calculate_total_rows(&plan),
            generation_time_ms: generation_time,
            uses_index: self.plan_uses_index(&plan),
        };

        // Cache the plan
        self.cache_plan(query_key, execution_plan.clone());

        Ok(execution_plan)
    }

    /// Plan a vector search query
    fn plan_vector_search(&self, query: &Query) -> Result<PlanStep, CortexError> {
        // For vector search, we'll typically use a vector index
        let index_name = format!("{}_vector", query.collection);

        let plan = PlanStep {
            step_type: PlanType::VectorSearch,
            index_name: Some(index_name),
            estimated_cost: 1.0, // Vector search is typically fast
            estimated_rows: query.options.limit,
            children: vec![],
            metadata: serde_json::json!({
                "query_type": "vector_search",
                "limit": query.options.limit,
                "offset": query.options.offset,
                "include_documents": query.options.include_documents
            }),
        };

        Ok(plan)
    }

    /// Plan a scalar filter query
    fn plan_scalar_filter(&self, query: &Query) -> Result<PlanStep, CortexError> {
        if let QueryCondition::Filter(condition) = &query.condition {
            // Check if there's an index for this field
            let index_name = format!("{}_scalar_{}", query.collection, condition.field);

            // For now, we'll assume an index exists and use it
            // In a real implementation, we'd check if the index actually exists
            let plan = PlanStep {
                step_type: PlanType::IndexScan,
                index_name: Some(index_name),
                estimated_cost: 1.0, // Index scan is typically fast
                estimated_rows: query.options.limit,
                children: vec![],
                metadata: serde_json::json!({
                    "query_type": "scalar_filter",
                    "field": condition.field,
                    "operator": format!("{:?}", condition.operator),
                    "limit": query.options.limit,
                    "offset": query.options.offset
                }),
            };

            Ok(plan)
        } else {
            Err(CortexError::Query(QueryError::InvalidQuery(
                "Expected scalar filter condition".to_string(),
            )))
        }
    }

    /// Plan a compound query
    fn plan_compound(&self, query: &Query) -> Result<PlanStep, CortexError> {
        if let QueryCondition::Compound { conditions, .. } = &query.condition {
            let mut child_plans = Vec::new();

            // Generate plans for each subcondition
            for condition in conditions {
                let sub_query = Query {
                    condition: condition.clone(),
                    ..query.clone()
                };
                let child_plan = self.generate_subplan(&sub_query)?;
                child_plans.push(child_plan);
            }

            // Create compound plan
            let plan = PlanStep {
                step_type: PlanType::Compound,
                index_name: None,
                estimated_cost: child_plans.iter().map(|p| p.estimated_cost).sum(),
                estimated_rows: query.options.limit,
                children: child_plans,
                metadata: serde_json::json!({
                    "query_type": "compound",
                    "limit": query.options.limit,
                    "offset": query.options.offset
                }),
            };

            Ok(plan)
        } else {
            Err(CortexError::Query(QueryError::InvalidQuery(
                "Expected compound condition".to_string(),
            )))
        }
    }

    /// Generate a subplan for a query condition
    fn generate_subplan(&self, query: &Query) -> Result<PlanStep, CortexError> {
        match query.condition {
            QueryCondition::Filter(_) => self.plan_scalar_filter(query),
            QueryCondition::VectorSearch(_) => self.plan_vector_search(query),
            QueryCondition::Compound { .. } => self.plan_compound(query),
        }
    }

    /// Calculate total cost of a plan
    fn calculate_total_cost(&self, plan: &PlanStep) -> f64 {
        plan.estimated_cost
            + plan.children.iter().map(|child| self.calculate_total_cost(child)).sum::<f64>()
    }

    /// Calculate total rows of a plan
    fn calculate_total_rows(&self, plan: &PlanStep) -> usize {
        plan.estimated_rows
    }

    /// Check if a plan uses an index
    fn plan_uses_index(&self, plan: &PlanStep) -> bool {
        if plan.index_name.is_some() {
            return true;
        }
        for child in &plan.children {
            if self.plan_uses_index(child) {
                return true;
            }
        }
        false
    }

    /// Generate a cache key for a query
    fn generate_query_key(&self, query: &Query) -> String {
        // In a real implementation, we'd generate a hash of the query
        // For simplicity, we'll use a JSON string
        serde_json::to_string(query).unwrap_or_default()
    }

    /// Cache a plan
    fn cache_plan(&mut self, key: String, plan: ExecutionPlan) {
        // Check if cache is full
        if self.plan_cache.len() >= self.cache_size_limit {
            // Remove oldest entry (simple LRU implementation)
            if let Some(oldest_key) = self.plan_cache.keys().next() {
                self.plan_cache.remove(oldest_key);
            }
        }
        self.plan_cache.insert(key, plan);
    }

    /// Clear the plan cache
    pub fn clear_cache(&mut self) {
        self.plan_cache.clear();
    }

    /// Get cache size
    pub fn cache_size(&self) -> usize {
        self.plan_cache.len()
    }
}

/// Test utilities for query planner
#[cfg(test)]
mod tests {
    use super::*;
    use crate::cortex_core::types::Float32Vector;
    use crate::cortex_query::builder::QueryBuilder;

    #[test]
    fn test_query_planner_creation() {
        let planner = QueryPlanner::with_defaults();
        assert_eq!(planner.cache_size(), 0);
    }

    #[test]
    fn test_vector_search_plan() {
        let mut planner = QueryPlanner::with_defaults();

        let query = QueryBuilder::new("collection1")
            .vector_search(Float32Vector::from(vec![1.0, 2.0, 3.0]), Some(0.5))
            .limit(10)
            .build()
            .unwrap();

        let plan = planner.generate_plan(&query).unwrap();
        assert_eq!(plan.root.step_type, PlanType::VectorSearch);
        assert!(plan.uses_index);
        assert!(plan.total_cost > 0.0);
    }

    #[test]
    fn test_scalar_filter_plan() {
        let mut planner = QueryPlanner::with_defaults();

        let query = QueryBuilder::new("collection1")
            .filter_eq("age", serde_json::json!(30))
            .limit(5)
            .build()
            .unwrap();

        let plan = planner.generate_plan(&query).unwrap();
        assert_eq!(plan.root.step_type, PlanType::IndexScan);
        assert!(plan.uses_index);
        assert!(plan.total_cost > 0.0);
    }

    #[test]
    fn test_plan_caching() {
        let mut planner = QueryPlanner::with_defaults();

        let query = QueryBuilder::new("collection1")
            .filter_eq("age", serde_json::json!(30))
            .limit(5)
            .build()
            .unwrap();

        // Generate first plan
        let plan1 = planner.generate_plan(&query).unwrap();
        assert_eq!(planner.cache_size(), 1);

        // Generate second plan (should be cached)
        let plan2 = planner.generate_plan(&query).unwrap();
        assert_eq!(planner.cache_size(), 1);

        // Plans should be the same
        assert_eq!(plan1.total_cost, plan2.total_cost);
        assert_eq!(plan1.total_rows, plan2.total_rows);

        // Clear cache
        planner.clear_cache();
        assert_eq!(planner.cache_size(), 0);
    }
}
