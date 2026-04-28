//! Multi-Stage Search Pipeline
//! Orchestrates coarse ranking and fine ranking stages

use crate::coretex_hybrid::{HybridQuery, MultiModalResult, FusedResult};
use crate::coretex_rerank::coarse_ranker::{CoarseRanker, CoarseResult, CoarseRankerConfig};
use crate::coretex_rerank::fine_ranker::{FineRanker, FineRankerConfig, RerankDocument};
use std::collections::HashMap;

pub struct TwoStageSearchPipeline {
    coarse_ranker: CoarseRanker,
    fine_ranker: FineRanker,
}

impl TwoStageSearchPipeline {
    pub fn new() -> Self {
        Self {
            coarse_ranker: CoarseRanker::with_default_config(),
            fine_ranker: FineRanker::new(FineRankerConfig::default()),
        }
    }

    pub fn with_coarse_config(mut self, config: CoarseRankerConfig) -> Self {
        self.coarse_ranker = CoarseRanker::new(config);
        self
    }

    pub fn with_fine_config(mut self, config: FineRankerConfig) -> Self {
        self.fine_ranker = FineRanker::new(config);
        self
    }

    pub fn search(&mut self, query: &HybridQuery, raw_results: Vec<MultiModalResult>) -> Vec<FusedResult> {
        let coarse_results: Vec<CoarseResult> = raw_results
            .iter()
            .map(|r| CoarseResult {
                id: r.id.clone(),
                source: r.source.clone(),
                raw_score: r.score,
                normalized_score: 0.0,
            })
            .collect();

        let coarse_ranked = self.coarse_ranker.rank(coarse_results);

        let mut documents = HashMap::new();
        for r in &raw_results {
            documents.insert(r.id.clone(), RerankDocument {
                id: r.id.clone(),
                text: format!("doc {}", r.id),
                vector: None,
            });
        }

        let fine_ranked = self.fine_ranker.rerank(
            query.text_query.as_ref().map(|t| t.query.as_str()).unwrap_or(""),
            &coarse_ranked,
            &documents,
        );

        fine_ranked
            .into_iter()
            .map(|r| FusedResult {
                id: r.id,
                score: r.final_score,
                sources: vec!["hybrid".to_string()],
            })
            .collect()
    }

    pub fn search_with_callback<F>(&mut self, query: &HybridQuery, mut fetch_docs: F) -> Vec<FusedResult>
    where
        F: FnMut(&str) -> Option<RerankDocument>,
    {
        let mock_results = vec![
            MultiModalResult {
                id: "doc1".to_string(),
                score: 0.9,
                rank: 1,
                source: "vector".to_string(),
                weight: 1.0,
                metadata: None,
            },
            MultiModalResult {
                id: "doc2".to_string(),
                score: 0.8,
                rank: 2,
                source: "text".to_string(),
                weight: 1.0,
                metadata: None,
            },
        ];

        self.search(query, mock_results)
    }
}

impl Default for TwoStageSearchPipeline {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_two_stage_pipeline() {
        let mut pipeline = TwoStageSearchPipeline::new();
        
        let query = HybridQuery::new()
            .with_text("test query")
            .with_top_k(10);

        let results = pipeline.search(&query, vec![]);
        
        assert!(results.is_empty());
    }
}
