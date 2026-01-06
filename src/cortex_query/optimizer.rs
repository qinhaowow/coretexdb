//! Query optimizer for CortexDB.
//!
//! This module provides query optimization functionality for CortexDB, including:
//! - Execution plan optimization
//! - Cost-based optimization strategies
//! - Index selection and utilization
//! - Query rewrite rules
//! - Optimization statistics and metrics

use crate::cortex_core::error::{CortexError, QueryError};
use crate::cortex_query::planner::{ExecutionPlan, PlanStep, PlanType};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Optimization strategy
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OptimizationStrategy {
    /// Cost-based optimization
    CostBased,
    /// Rule-based optimization
    RuleBased,
    /// Hybrid optimization (both cost and rule based)
    Hybrid,
}

/// Optimization statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptimizationStats {
    /// Optimization time in milliseconds
    pub optimization_time_ms: f64,
    /// Number of optimization rules applied
    pub rules_applied: usize,
    /// Original plan cost
    pub original_cost: f64,
    /// Optimized plan cost
    pub optimized_cost: f64,
    /// Cost reduction percentage
    pub cost_reduction_percent: f64,
    /// Whether the plan was changed
    pub plan_changed: bool,
}

/// Query optimizer
pub struct QueryOptimizer {
    /// Optimization strategy
    strategy: OptimizationStrategy,
    /// Whether to use index statistics
    use_index_stats: bool,
}

impl QueryOptimizer {
    /// Create a new query optimizer
    pub fn new(strategy: OptimizationStrategy, use_index_stats: bool) -> Self {
        Self {
            strategy,
            use_index_stats,
        }
    }

    /// Create a query optimizer with default settings
    pub fn with_defaults() -> Self {
        Self::new(OptimizationStrategy::Hybrid, true)
    }

    /// Optimize an execution plan
    pub fn optimize_plan(&self, plan: ExecutionPlan) -> Result<(ExecutionPlan, OptimizationStats), CortexError> {
        let start_time = std::time::Instant::now();

        let original_cost = plan.total_cost;

        // Apply optimization based on strategy
        let optimized_plan = match self.strategy {
            OptimizationStrategy::CostBased => {
                self.optimize_cost_based(plan)?
            }
            OptimizationStrategy::RuleBased => {
                self.optimize_rule_based(plan)?
            }
            OptimizationStrategy::Hybrid => {
                let rule_optimized = self.optimize_rule_based(plan)?;
                self.optimize_cost_based(rule_optimized)?
            }
        };

        // Calculate optimization statistics
        let optimization_time = start_time.elapsed().as_secs_f64() * 1000.0;
        let optimized_cost = optimized_plan.total_cost;
        let cost_reduction = if original_cost > 0.0 {
            ((original_cost - optimized_cost) / original_cost) * 100.0
        } else {
            0.0
        };

        let stats = OptimizationStats {
            optimization_time_ms: optimization_time,
            rules_applied: 1, // Simplified for now
            original_cost,
            optimized_cost,
            cost_reduction_percent: cost_reduction,
            plan_changed: original_cost != optimized_cost,
        };

        Ok((optimized_plan, stats))
    }

    /// Optimize plan using cost-based strategies
    fn optimize_cost_based(&self, plan: ExecutionPlan) -> Result<ExecutionPlan, CortexError> {
        // For now, we'll just return the plan as is
        // In a real implementation, we'd:
        // 1. Calculate actual costs based on index statistics
        // 2. Compare different execution strategies
        // 3. Choose the lowest cost plan
        Ok(plan)
    }

    /// Optimize plan using rule-based strategies
    fn optimize_rule_based(&self, plan: ExecutionPlan) -> Result<ExecutionPlan, CortexError> {
        // Apply rule-based optimizations
        let optimized_root = self.optimize_plan_step(plan.root)?;

        let optimized_plan = ExecutionPlan {
            root: optimized_root,
            total_cost: self.calculate_total_cost(&optimized_root),
            total_rows: self.calculate_total_rows(&optimized_root),
            generation_time_ms: plan.generation_time_ms,
            uses_index: self.plan_uses_index(&optimized_root),
        };

        Ok(optimized_plan)
    }

    /// Optimize a single plan step
    fn optimize_plan_step(&self, step: PlanStep) -> Result<PlanStep, CortexError> {
        // Apply optimizations based on step type
        let optimized_step = match step.step_type {
            PlanType::FullScan => {
                // Try to find an index to use instead of full scan
                self.optimize_full_scan(step)?
            }
            PlanType::IndexScan => {
                // Optimize index usage
                self.optimize_index_scan(step)?
            }
            PlanType::VectorSearch => {
                // Optimize vector search parameters
                self.optimize_vector_search(step)?
            }
            PlanType::Compound => {
                // Optimize compound plan
                self.optimize_compound(step)?
            }
        };

        Ok(optimized_step)
    }

    /// Optimize full scan step
    fn optimize_full_scan(&self, step: PlanStep) -> Result<PlanStep, CortexError> {
        // In a real implementation, we'd:
        // 1. Check if there's an index that could be used
        // 2. Rewrite the plan to use the index if it's beneficial
        Ok(step)
    }

    /// Optimize index scan step
    fn optimize_index_scan(&self, step: PlanStep) -> Result<PlanStep, CortexError> {
        // In a real implementation, we'd:
        // 1. Check if the index is the best one for the query
        // 2. Consider covering indexes
        // 3. Optimize index access patterns
        Ok(step)
    }

    /// Optimize vector search step
    fn optimize_vector_search(&self, step: PlanStep) -> Result<PlanStep, CortexError> {
        // In a real implementation, we'd:
        // 1. Optimize vector search parameters (ef_search, etc.)
        // 2. Consider using scalar filters to reduce the search space
        // 3. Optimize batch sizes and parallelism
        Ok(step)
    }

    /// Optimize compound plan step
    fn optimize_compound(&self, step: PlanStep) -> Result<PlanStep, CortexError> {
        // Optimize child steps
        let mut optimized_children = Vec::new();
        for child in step.children {
            let optimized_child = self.optimize_plan_step(child)?;
            optimized_children.push(optimized_child);
        }

        // Create optimized compound step
        let optimized_step = PlanStep {
            children: optimized_children,
            ..step
        };

        Ok(optimized_step)
    }

    /// Calculate total cost of a plan step
    fn calculate_total_cost(&self, step: &PlanStep) -> f64 {
        step.estimated_cost
            + step.children.iter().map(|child| self.calculate_total_cost(child)).sum::<f64>()
    }

    /// Calculate total rows of a plan step
    fn calculate_total_rows(&self, step: &PlanStep) -> usize {
        step.estimated_rows
    }

    /// Check if a plan step uses an index
    fn plan_uses_index(&self, step: &PlanStep) -> bool {
        if step.index_name.is_some() {
            return true;
        }
        for child in &step.children {
            if self.plan_uses_index(child) {
                return true;
            }
        }
        false
    }

    /// Get optimization strategy
    pub fn strategy(&self) -> OptimizationStrategy {
        self.strategy
    }

    /// Set optimization strategy
    pub fn set_strategy(&mut self, strategy: OptimizationStrategy) {
        self.strategy = strategy;
    }

    /// Enable/disable index statistics usage
    pub fn set_use_index_stats(&mut self, use_index_stats: bool) {
        self.use_index_stats = use_index_stats;
    }
}

/// Test utilities for query optimizer
#[cfg(test)]
mod tests {
    use super::*;
    use crate::cortex_query::planner::{ExecutionPlan, PlanStep, PlanType};

    #[test]
    fn test_query_optimizer_creation() {
        let optimizer = QueryOptimizer::with_defaults();
        assert_eq!(optimizer.strategy(), OptimizationStrategy::Hybrid);
    }

    #[test]
    fn test_optimize_plan() {
        let optimizer = QueryOptimizer::with_defaults();

        // Create a simple execution plan
        let plan = ExecutionPlan {
            root: PlanStep {
                step_type: PlanType::IndexScan,
                index_name: Some("test_index".to_string()),
                estimated_cost: 1.0,
                estimated_rows: 10,
                children: vec![],
                metadata: serde_json::json!({}),
            },
            total_cost: 1.0,
            total_rows: 10,
            generation_time_ms: 0.1,
            uses_index: true,
        };

        let (optimized_plan, stats) = optimizer.optimize_plan(plan).unwrap();
        assert!(stats.optimization_time_ms > 0.0);
        assert!(stats.original_cost > 0.0);
        assert!(stats.optimized_cost > 0.0);
    }

    #[test]
    fn test_optimization_strategies() {
        let cost_based_optimizer = QueryOptimizer::new(OptimizationStrategy::CostBased, true);
        assert_eq!(cost_based_optimizer.strategy(), OptimizationStrategy::CostBased);

        let rule_based_optimizer = QueryOptimizer::new(OptimizationStrategy::RuleBased, true);
        assert_eq!(rule_based_optimizer.strategy(), OptimizationStrategy::RuleBased);

        let hybrid_optimizer = QueryOptimizer::new(OptimizationStrategy::Hybrid, true);
        assert_eq!(hybrid_optimizer.strategy(), OptimizationStrategy::Hybrid);
    }
}
