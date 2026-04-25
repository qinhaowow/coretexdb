//! Fine Ranking Stage (Reranking) for Hybrid Search
//! Advanced reranking using learned models or sophisticated features

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FineRankerConfig {
    pub rerank_top_k: usize,
    pub use_cross_encoder: bool,
    pub use_feature_rerank: bool,
}

impl Default for FineRankerConfig {
    fn default() -> Self {
        Self {
            rerank_top_k: 50,
            use_cross_encoder: false,
            use_feature_rerank: true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct FineResult {
    pub id: String,
    pub coarse_score: f32,
    pub rerank_score: f32,
    pub final_score: f32,
    pub features: HashMap<String, f32>,
}

pub trait RerankModel: Send + Sync {
    fn rerank(&self, query: &str, documents: &[RerankDocument]) -> Vec<f32>;
}

#[derive(Debug, Clone)]
pub struct RerankDocument {
    pub id: String,
    pub text: String,
    pub vector: Option<Vec<f32>>,
}

pub struct FineRanker {
    config: FineRankerConfig,
    model: Option<Box<dyn RerankModel>>,
    feature_weights: FeatureWeights,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureWeights {
    pub cosine_similarity: f32,
    pub bm25_score: f32,
    pub recency: f32,
    pub popularity: f32,
    pub text_match: f32,
}

impl Default for FeatureWeights {
    fn default() -> Self {
        Self {
            cosine_similarity: 0.4,
            bm25_score: 0.3,
            recency: 0.1,
            popularity: 0.1,
            text_match: 0.1,
        }
    }
}

impl FineRanker {
    pub fn new(config: FineRankerConfig) -> Self {
        Self {
            config,
            model: None,
            feature_weights: FeatureWeights::default(),
        }
    }

    pub fn with_model<M: RerankModel + 'static>(mut self, model: M) -> Self {
        self.model = Some(Box::new(model));
        self
    }

    pub fn with_feature_weights(mut self, weights: FeatureWeights) -> Self {
        self.feature_weights = weights;
        self
    }

    pub fn rerank(&self, query: &str, candidates: &[CoarseResult], documents: &HashMap<String, RerankDocument>) -> Vec<FineResult> {
        let mut results: Vec<FineResult> = candidates
            .iter()
            .filter_map(|c| {
                documents.get(&c.id).map(|doc| {
                    let features = self.compute_features(query, doc, c);
                    FineResult {
                        id: c.id.clone(),
                        coarse_score: c.normalized_score,
                        rerank_score: 0.0,
                        final_score: 0.0,
                        features,
                    }
                })
            })
            .collect();

        if let Some(ref model) = self.model {
            let docs: Vec<RerankDocument> = results
                .iter()
                .filter_map(|r| documents.get(&r.id).cloned())
                .collect();
            
            let scores = model.rerank(query, &docs);
            
            for (result, score) in results.iter_mut().zip(scores) {
                result.rerank_score = score;
            }
        } else {
            for result in &mut results {
                result.rerank_score = self.compute_feature_score(&result.features);
            }
        }

        for result in &mut results {
            result.final_score = self.combine_scores(result.coarse_score, result.rerank_score);
        }

        results.sort_by(|a, b| b.final_score.partial_cmp(&a.final_score).unwrap());
        
        results.truncate(self.config.rerank_top_k);
        results
    }

    fn compute_features(&self, query: &str, document: &RerankDocument, coarse: &CoarseResult) -> HashMap<String, f32> {
        let mut features = HashMap::new();
        
        let query_terms: Vec<&str> = query.split_whitespace().collect();
        let doc_terms: Vec<&str> = document.text.split_whitespace().collect();
        
        let match_count = query_terms.iter()
            .filter(|t| doc_terms.contains(t))
            .count();
        let text_match = match_count as f32 / query_terms.len().max(1) as f32;
        
        features.insert("text_match".to_string(), text_match);
        features.insert("coarse_score".to_string(), coarse.normalized_score);
        
        features
    }

    fn compute_feature_score(&self, features: &HashMap<String, f32>) -> f32 {
        let text_match = features.get("text_match").copied().unwrap_or(0.0);
        let coarse = features.get("coarse_score").copied().unwrap_or(0.0);

        text_match * self.feature_weights.text_match + coarse * self.feature_weights.cosine_similarity
    }

    fn combine_scores(&self, coarse: f32, rerank: f32) -> f32 {
        if rerank > 0.0 {
            coarse * 0.3 + rerank * 0.7
        } else {
            coarse
        }
    }
}

pub struct CrossEncoderReranker;

impl RerankModel for CrossEncoderReranker {
    fn rerank(&self, query: &str, documents: &[RerankDocument]) -> Vec<f32> {
        documents
            .iter()
            .map(|doc| {
                let query_lower = query.to_lowercase();
                let doc_lower = doc.text.to_lowercase();
                
                let query_terms: Vec<&str> = query_lower.split_whitespace().collect();
                let doc_terms: Vec<&str> = doc_lower.split_whitespace().collect();
                
                let match_ratio = query_terms.iter()
                    .filter(|t| doc_terms.contains(t))
                    .count() as f32 / query_terms.len().max(1) as f32;
                
                match_ratio
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fine_ranker() {
        let ranker = FineRanker::new(FineRankerConfig::default());
        
        let candidates = vec![
            CoarseResult { id: "doc1".to_string(), source: "vector".to_string(), raw_score: 0.9, normalized_score: 0.9 },
            CoarseResult { id: "doc2".to_string(), source: "text".to_string(), raw_score: 0.7, normalized_score: 0.7 },
        ];
        
        let mut documents = HashMap::new();
        documents.insert("doc1".to_string(), RerankDocument {
            id: "doc1".to_string(),
            text: "machine learning algorithms".to_string(),
            vector: None,
        });
        documents.insert("doc2".to_string(), RerankDocument {
            id: "doc2".to_string(),
            text: "deep neural networks".to_string(),
            vector: None,
        });

        let results = ranker.rerank("machine learning", &candidates, &documents);
        
        assert!(!results.is_empty());
    }
}
