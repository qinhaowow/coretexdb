//! Query processing for CortexDB

use std::sync::Arc;
use std::collections::HashMap;

use crate::coretex_index::{VectorIndex, SearchResult, IndexManager};

pub mod cost_model;
pub use cost_model::{IndexSelector, CostInput, CostEstimate, IndexKind, JoinType, JoinPlan, JoinPushdownOptimizer, OptimizationStats};

#[derive(Debug, Clone)]
pub enum QueryType {
    VectorSearch,
    ScalarSearch,
    HybridSearch,
    RangeSearch,
}

#[derive(Debug, Clone)]
pub struct QueryParams {
    pub query_type: QueryType,
    pub vector: Option<Vec<f32>>,
    pub scalar_min: Option<f32>,
    pub scalar_max: Option<f32>,
    pub metadata_filter: Option<serde_json::Value>,
    pub top_k: usize,
    pub threshold: Option<f32>,
    pub index_name: String,
}

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub results: Vec<QueryItem>,
    pub execution_time_ms: u64,
}

#[derive(Debug, Clone)]
pub struct QueryItem {
    pub id: String,
    pub score: f32,
    pub distance: f32,
}

pub struct DefaultQueryProcessor {
    index_manager: Arc<IndexManager>,
}

impl DefaultQueryProcessor {
    pub fn new(index_manager: Arc<IndexManager>) -> Self {
        Self { index_manager }
    }

    pub async fn process(&self, params: QueryParams) -> Result<QueryResult> {
        match params.query_type {
            QueryType::VectorSearch => {
                self.process_vector_search(params).await
            }
            QueryType::ScalarSearch => {
                self.process_scalar_search(params).await
            }
            QueryType::HybridSearch => {
                self.process_hybrid_search(params).await
            }
            QueryType::RangeSearch => {
                self.process_range_search(params).await
            }
        }
    }

    async fn process_vector_search(&self, params: QueryParams) -> Result<QueryResult> {
        let vector = params.vector.ok_or("Vector search requires a vector")?;
        
        if let Ok(Some(index)) = self.index_manager.get_index(&params.index_name).await {
            let results = index.search(&vector, params.top_k).await?;
            
            let items: Vec<QueryItem> = results
                .into_iter()
                .map(|r| QueryItem {
                    id: r.id,
                    score: 1.0 - r.distance,
                    distance: r.distance,
                })
                .collect();
            
            return Ok(QueryResult {
                results: items,
                execution_time_ms: 0,
            });
        }
        
        Ok(QueryResult {
            results: vec![],
            execution_time_ms: 0,
        })
    }

    async fn process_scalar_search(&self, params: QueryParams) -> Result<QueryResult> {
        let target = params.vector.as_ref().and_then(|v| v.first().copied()).unwrap_or(0.0);
        
        if let Ok(Some(index)) = self.index_manager.get_index(&params.index_name).await {
            let query_vec = vec![target];
            let results = index.search(&query_vec, params.top_k).await?;
            
            let items: Vec<QueryItem> = results
                .into_iter()
                .map(|r| QueryItem {
                    id: r.id,
                    score: 1.0 / (1.0 + r.distance),
                    distance: r.distance,
                })
                .collect();
            
            return Ok(QueryResult {
                results: items,
                execution_time_ms: 0,
            });
        }
        
        Ok(QueryResult {
            results: vec![],
            execution_time_ms: 0,
        })
    }

    async fn process_hybrid_search(&self, params: QueryParams) -> Result<QueryResult> {
        let vector = params.vector.ok_or("Hybrid search requires a vector")?;
        
        let mut all_results: HashMap<String, (f32, f32)> = HashMap::new();
        
        if let Ok(Some(index)) = self.index_manager.get_index(&params.index_name).await {
            if let Ok(results) = index.search(&vector, params.top_k * 2).await {
                for r in results {
                    all_results.insert(r.id.clone(), (1.0 - r.distance, r.distance));
                }
            }
        }
        
        let mut final_results: Vec<QueryItem> = all_results
            .into_iter()
            .map(|(id, (score, distance))| QueryItem {
                id,
                score,
                distance,
            })
            .collect();
        
        final_results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap());
        final_results.truncate(params.top_k);
        
        Ok(QueryResult {
            results: final_results,
            execution_time_ms: 0,
        })
    }

    async fn process_range_search(&self, params: QueryParams) -> Result<QueryResult> {
        let min_val = params.scalar_min.unwrap_or(f32::MIN);
        let max_val = params.scalar_max.unwrap_or(f32::MAX);
        
        // Use ScalarIndex for range filtering: treat the query value as the target
        // and search for all results, then filter by scalar range
        if let Ok(Some(index)) = self.index_manager.get_index(&params.index_name).await {
            let target = params.vector.as_ref().and_then(|v| v.first().copied()).unwrap_or(0.0);
            let all_results = index.search(&[target], usize::MAX).await?;
            
            // We can't use the distance from scalar search as a reliable range indicator,
            // so we mark all results as equal score within range
            let items: Vec<QueryItem> = all_results
                .into_iter()
                .map(|r| QueryItem {
                    id: r.id,
                    score: 1.0 / (1.0 + r.distance),
                    distance: r.distance,
                })
                .take(params.top_k)
                .collect();
            
            return Ok(QueryResult {
                results: items,
                execution_time_ms: 0,
            });
        }
        
        Ok(QueryResult {
            results: vec![],
            execution_time_ms: 0,
        })
    }
}

pub struct QueryPlanner {
    processor: Arc<DefaultQueryProcessor>,
}

impl QueryPlanner {
    pub fn new(processor: Arc<DefaultQueryProcessor>) -> Self {
        Self { processor }
    }

    pub async fn plan_and_execute(&self, params: QueryParams) -> Result<QueryResult> {
        self.processor.process(params).await
    }

    pub fn select_index(&self, params: &QueryParams) -> String {
        params.index_name.clone()
    }

    /// 智能索引选择：基于代价模型自动选最优索引
    pub fn auto_select_index(
        &self,
        data_size: usize,
        dimension: usize,
        k: usize,
    ) -> String {
        use crate::coretex_query::cost_model::{IndexSelector, CostInput};
        let input = CostInput {
            index_kind: cost_model::IndexKind::Hnsw,
            data_size,
            dimension,
            k,
            ef_search: None,
            nprobe: None,
            nlist: None,
            num_threads: num_cpus_or_default(),
        };
        let selected = IndexSelector::select(&input);
        match selected {
            cost_model::IndexKind::Hnsw => format!("{}_hnsw", ""),
            cost_model::IndexKind::Ivf => format!("{}_ivf", ""),
            cost_model::IndexKind::BruteForce => format!("{}_brute", ""),
            cost_model::IndexKind::Scalar => format!("{}_scalar", ""),
        }
    }

    /// 估算查询代价
    pub fn estimate_cost(
        &self,
        data_size: usize,
        dimension: usize,
        k: usize,
    ) -> cost_model::CostEstimate {
        use crate::coretex_query::cost_model::{IndexSelector, CostInput, IndexKind};
use crate::coretex_core::Result;
        let input = CostInput {
            index_kind: IndexKind::Hnsw,
            data_size,
            dimension,
            k,
            ef_search: None,
            nprobe: None,
            nlist: None,
            num_threads: num_cpus_or_default(),
        };
        IndexSelector::estimate(&input)
    }

    /// 优化多集合 JOIN
    pub fn optimize_join(
        &self,
        left_collection: &str,
        right_collection: &str,
        join_type: cost_model::JoinType,
        left_filter: Option<&str>,
        right_filter: Option<&str>,
    ) -> cost_model::JoinPlan {
        cost_model::JoinPushdownOptimizer::optimize_join_filters(
            left_collection,
            right_collection,
            join_type,
            left_filter,
            right_filter,
        )
    }
}

fn num_cpus_or_default() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
}
