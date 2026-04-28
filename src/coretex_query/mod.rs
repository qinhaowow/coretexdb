//! Query processing for CortexDB

use std::error::Error;
use std::sync::Arc;
use std::collections::HashMap;

use crate::coretex_index::{VectorIndex, SearchResult, IndexManager};

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

    pub async fn process(&self, params: QueryParams) -> Result<QueryResult, Box<dyn Error + Send + Sync>> {
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

    async fn process_vector_search(&self, params: QueryParams) -> Result<QueryResult, Box<dyn Error + Send + Sync>> {
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

    async fn process_scalar_search(&self, params: QueryParams) -> Result<QueryResult, Box<dyn Error + Send + Sync>> {
        Ok(QueryResult {
            results: vec![],
            execution_time_ms: 0,
        })
    }

    async fn process_hybrid_search(&self, params: QueryParams) -> Result<QueryResult, Box<dyn Error + Send + Sync>> {
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

    async fn process_range_search(&self, params: QueryParams) -> Result<QueryResult, Box<dyn Error + Send + Sync>> {
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

    pub async fn plan_and_execute(&self, params: QueryParams) -> Result<QueryResult, Box<dyn Error + Send + Sync>> {
        self.processor.process(params).await
    }

    pub fn select_index(&self, params: &QueryParams) -> String {
        params.index_name.clone()
    }
}
