//! Fine Ranking Stage (Reranking) for Hybrid Search
//! Advanced reranking using learned models or sophisticated features

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::coretex_rerank::coarse_ranker::CoarseResult;

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

pub struct CrossEncoderReranker {
    _query_weight: f32,
    _doc_weight: f32,
    _bm25_k1: f32,
    _bm25_b: f32,
}

impl Default for CrossEncoderReranker {
    fn default() -> Self {
        Self {
            _query_weight: 0.6,
            _doc_weight: 0.4,
            _bm25_k1: 1.2,
            _bm25_b: 0.75,
        }
    }
}

impl CrossEncoderReranker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_params(query_weight: f32, doc_weight: f32, bm25_k1: f32, bm25_b: f32) -> Self {
        Self { _query_weight: query_weight, _doc_weight: doc_weight, _bm25_k1: bm25_k1, _bm25_b: bm25_b }
    }

    fn tokenize(text: &str) -> Vec<String> {
        text.to_lowercase()
            .split(|c: char| !c.is_alphanumeric())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect()
    }

    fn idf_score(term: &str, documents: &[&str]) -> f32 {
        let n = documents.len() as f32;
        let df = documents.iter()
            .filter(|doc| doc.to_lowercase().contains(term))
            .count() as f32;
        ((n - df + 0.5) / (df + 0.5) + 1.0).ln()
    }

    fn bm25_score(query: &str, document: &str, avg_doc_len: f32, documents: &[&str]) -> f32 {
        let query_terms = Self::tokenize(query);
        let doc_terms = Self::tokenize(document);
        let doc_len = doc_terms.len() as f32;

        let mut score = 0.0f32;
        for qt in &query_terms {
            let tf = doc_terms.iter().filter(|t| *t == qt).count() as f32;
            let idf = Self::idf_score(qt, documents);
            let numerator = tf * (1.2 + 1.0);
            let denominator = tf + 1.2 * (1.0 - 0.75 + 0.75 * doc_len / avg_doc_len);
            score += idf * numerator / denominator;
        }
        score
    }

    fn bm25_scores(query: &str, documents: &[&str]) -> Vec<f32> {
        let avg_doc_len: f32 = if documents.is_empty() {
            0.0
        } else {
            documents.iter().map(|d| d.split_whitespace().count() as f32).sum::<f32>() / documents.len() as f32
        };

        documents.iter()
            .map(|doc| Self::bm25_score(query, doc, avg_doc_len, documents))
            .collect()
    }

    fn cosine_similarity(query_tokens: &[String], doc_tokens: &[String]) -> f32 {
        let mut all_terms: Vec<String> = query_tokens.iter().chain(doc_tokens.iter()).cloned().collect();
        all_terms.sort();
        all_terms.dedup();

        let qv: Vec<f32> = all_terms.iter()
            .map(|t| query_tokens.iter().filter(|qt| *qt == t).count() as f32)
            .collect();
        let dv: Vec<f32> = all_terms.iter()
            .map(|t| doc_tokens.iter().filter(|dt| *dt == t).count() as f32)
            .collect();

        let dot: f32 = qv.iter().zip(dv.iter()).map(|(a, b)| a * b).sum();
        let qm: f32 = qv.iter().map(|a| a * a).sum::<f32>().sqrt();
        let dm: f32 = dv.iter().map(|a| a * a).sum::<f32>().sqrt();

        if qm == 0.0 || dm == 0.0 { 0.0 } else { dot / (qm * dm) }
    }

    fn compute_cross_score(query: &str, document: &str, all_docs: &[&str]) -> f32 {
        let query_tokens = Self::tokenize(query);
        let doc_tokens = Self::tokenize(document);

        let cosine = Self::cosine_similarity(&query_tokens, &doc_tokens);
        let bm25_scores = Self::bm25_scores(query, all_docs);
        let bm25_max = bm25_scores.iter().cloned().fold(0.0f32, f32::max);

        let bm25_norm = if bm25_max > 0.0 {
            Self::bm25_score(query, document, 
                all_docs.iter().map(|d| d.split_whitespace().count() as f32).sum::<f32>() / all_docs.len() as f32,
                all_docs) / bm25_max
        } else {
            0.0
        };

        let token_overlap = query_tokens.iter()
            .filter(|qt| doc_tokens.iter().any(|dt| dt == *qt))
            .count() as f32 / query_tokens.len().max(1) as f32;

        0.35 * cosine + 0.30 * bm25_norm + 0.35 * token_overlap
    }
}

impl RerankModel for CrossEncoderReranker {
    fn rerank(&self, query: &str, documents: &[RerankDocument]) -> Vec<f32> {
        let all_docs: Vec<&str> = documents.iter().map(|d| d.text.as_str()).collect();

        documents.iter()
            .map(|doc| Self::compute_cross_score(query, &doc.text, &all_docs))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cross_encoder_basic() {
        let reranker = CrossEncoderReranker::new();
        let docs = vec![
            RerankDocument { id: "1".into(), text: "machine learning algorithms".into(), vector: None },
            RerankDocument { id: "2".into(), text: "deep neural networks".into(), vector: None },
            RerankDocument { id: "3".into(), text: "natural language processing".into(), vector: None },
        ];
        let scores = reranker.rerank("machine learning", &docs);
        assert_eq!(scores.len(), 3);
        assert!(scores[0] >= scores[1]);
    }

    #[test]
    fn test_cross_encoder_params() {
        let reranker = CrossEncoderReranker::with_params(0.7, 0.3, 1.5, 0.8);
        assert_eq!(reranker._query_weight, 0.7);
        assert_eq!(reranker._bm25_k1, 1.5);
    }

    #[test]
    fn test_fine_ranker_with_cross_encoder() {
        let ranker = FineRanker::new(FineRankerConfig {
            use_cross_encoder: true,
            ..Default::default()
        }).with_model(CrossEncoderReranker::new());

        let candidates = vec![
            CoarseResult { id: "d1".into(), source: "vec".into(), raw_score: 0.9, normalized_score: 0.9 },
            CoarseResult { id: "d2".into(), source: "txt".into(), raw_score: 0.7, normalized_score: 0.7 },
        ];

        let mut docs = std::collections::HashMap::new();
        docs.insert("d1".into(), RerankDocument { id: "d1".into(), text: "machine learning algorithms for classification".into(), vector: None });
        docs.insert("d2".into(), RerankDocument { id: "d2".into(), text: "deep neural networks architecture".into(), vector: None });

        let results = ranker.rerank("machine learning", &candidates, &docs);
        assert!(!results.is_empty());
        assert!(results[0].rerank_score > 0.0);
    }

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
