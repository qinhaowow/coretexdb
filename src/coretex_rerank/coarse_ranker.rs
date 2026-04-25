//! Coarse Ranking Stage for Hybrid Search
//! Fast candidate selection using lightweight indexes

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoarseRankerConfig {
    pub use_vector: bool,
    pub use_text: bool,
    pub use_scalar: bool,
    pub coarse_top_k: usize,
    pub vector_top_k: usize,
    pub text_top_k: usize,
    pub scalar_top_k: usize,
}

impl Default for CoarseRankerConfig {
    fn default() -> Self {
        Self {
            use_vector: true,
            use_text: true,
            use_scalar: true,
            coarse_top_k: 500,
            vector_top_k: 200,
            text_top_k: 200,
            scalar_top_k: 200,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CoarseResult {
    pub id: String,
    pub source: String,
    pub raw_score: f32,
    pub normalized_score: f32,
}

pub struct CoarseRanker {
    config: CoarseRankerConfig,
    max_scores: HashMap<String, f32>,
    min_scores: HashMap<String, f32>,
}

impl CoarseRanker {
    pub fn new(config: CoarseRankerConfig) -> Self {
        Self {
            config,
            max_scores: HashMap::new(),
            min_scores: HashMap::new(),
        }
    }

    pub fn with_default_config() -> Self {
        Self::new(CoarseRankerConfig::default())
    }

    pub fn rank(&mut self, mut results: Vec<CoarseResult>) -> Vec<CoarseResult> {
        if results.is_empty() {
            return results;
        }

        for result in &results {
            let entry = self.max_scores.entry(result.source.clone()).or_insert(f32::MIN);
            *entry = result.raw_score.max(*entry);
            
            let entry = self.min_scores.entry(result.source.clone()).or_insert(f32::MAX);
            *entry = result.raw_score.min(*entry);
        }

        for result in &mut results {
            let max = self.max_scores.get(&result.source).copied().unwrap_or(1.0);
            let min = self.min_scores.get(&result.source).copied().unwrap_or(0.0);
            
            let range = max - min;
            result.normalized_score = if range.abs() > f32::EPSILON {
                (result.raw_score - min) / range
            } else {
                result.raw_score
            };
        }

        results.sort_by(|a, b| {
            b.normalized_score.partial_cmp(&a.normalized_score).unwrap()
        });

        results.truncate(self.config.coarse_top_k);
        results
    }

    pub fn merge_results(&self, vector_results: Vec<CoarseResult>, text_results: Vec<CoarseResult>) -> Vec<CoarseResult> {
        let mut all_results = Vec::new();
        
        if self.config.use_vector {
            let top_k = self.config.vector_top_k;
            all_results.extend(vector_results.into_iter().take(top_k));
        }
        
        if self.config.use_text {
            let top_k = self.config.text_top_k;
            all_results.extend(text_results.into_iter().take(top_k));
        }

        all_results.sort_by(|a, b| b.raw_score.partial_cmp(&a.raw_score).unwrap());
        
        let mut seen = std::collections::HashSet::new();
        all_results.into_iter()
            .filter(|r| seen.insert(r.id.clone()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_coarse_ranker() {
        let mut ranker = CoarseRanker::with_default_config();
        
        let results = vec![
            CoarseResult { id: "doc1".to_string(), source: "vector".to_string(), raw_score: 0.9, normalized_score: 0.0 },
            CoarseResult { id: "doc2".to_string(), source: "vector".to_string(), raw_score: 0.7, normalized_score: 0.0 },
            CoarseResult { id: "doc3".to_string(), source: "text".to_string(), raw_score: 0.5, normalized_score: 0.0 },
        ];

        let ranked = ranker.rank(results);
        
        assert!(ranked[0].normalized_score >= ranked[1].normalized_score);
    }
}
