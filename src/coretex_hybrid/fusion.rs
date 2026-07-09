//! Score Fusion for Hybrid Search
//! Implements various score fusion algorithms for combining results from different retrievers

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ScoreFusion {
    RRF { k: u32 },
    WeightedSum,
    WeightedSumNormalized,
    LearningToRank,
    CombMNZ,
}

impl Default for ScoreFusion {
    fn default() -> Self {
        Self::WeightedSum
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LTRWeights {
    pub vector_score_weight: f32,
    pub text_score_weight: f32,
    pub rank_reciprocal_weight: f32,
    pub source_count_weight: f32,
    pub score_variance_weight: f32,
}

impl Default for LTRWeights {
    fn default() -> Self {
        Self {
            vector_score_weight: 0.35,
            text_score_weight: 0.35,
            rank_reciprocal_weight: 0.15,
            source_count_weight: 0.10,
            score_variance_weight: 0.05,
        }
    }
}

pub struct ScoreFusionEngine {
    method: ScoreFusion,
    ltr_weights: LTRWeights,
}

impl ScoreFusionEngine {
    pub fn new(method: ScoreFusion) -> Self {
        Self {
            method,
            ltr_weights: LTRWeights::default(),
        }
    }

    pub fn with_ltr_weights(mut self, weights: LTRWeights) -> Self {
        self.ltr_weights = weights;
        self
    }

    pub fn fuse(&self, results: &[MultiModalResult]) -> Vec<FusedResult> {
        match &self.method {
            ScoreFusion::RRF { k } => self.rrf_fusion(results, *k),
            ScoreFusion::WeightedSum => self.weighted_sum_fusion(results),
            ScoreFusion::WeightedSumNormalized => self.normalized_weighted_sum(results),
            ScoreFusion::CombMNZ => self.comb_mnz_fusion(results),
            ScoreFusion::LearningToRank => self.learning_to_rank_fusion(results),
        }
    }

    fn rrf_fusion(&self, results: &[MultiModalResult], k: u32) -> Vec<FusedResult> {
        let mut score_map: HashMap<String, f32> = HashMap::new();

        for result in results {
            let rank = result.rank as f32;
            let rrf_score = 1.0 / (k as f32 + rank);
            *score_map.entry(result.id.clone()).or_insert(0.0) += rrf_score;
        }

        let mut fused: Vec<FusedResult> = score_map
            .into_iter()
            .map(|(id, score)| FusedResult {
                id,
                score,
                sources: results.iter().map(|r| r.source.clone()).collect(),
            })
            .collect();

        fused.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap());
        fused
    }

    fn weighted_sum_fusion(&self, results: &[MultiModalResult]) -> Vec<FusedResult> {
        let mut score_map: HashMap<String, (f32, Vec<String>)> = HashMap::new();

        for result in results {
            let entry = score_map.entry(result.id.clone()).or_insert((0.0, Vec::new()));
            entry.0 += result.score * result.weight;
            if !entry.1.contains(&result.source) {
                entry.1.push(result.source.clone());
            }
        }

        let mut fused: Vec<FusedResult> = score_map
            .into_iter()
            .map(|(id, (score, sources))| FusedResult {
                id,
                score,
                sources,
            })
            .collect();

        fused.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap());
        fused
    }

    fn normalized_weighted_sum(&self, results: &[MultiModalResult]) -> Vec<FusedResult> {
        if results.is_empty() {
            return Vec::new();
        }

        let max_score = results.iter().map(|r| r.score).fold(f32::MIN, f32::max);
        let min_score = results.iter().map(|r| r.score).fold(f32::MAX, f32::min);

        let range = if (max_score - min_score).abs() > f32::EPSILON {
            max_score - min_score
        } else {
            1.0
        };

        let mut score_map: HashMap<String, (f32, Vec<String>)> = HashMap::new();

        for result in results {
            let normalized = (result.score - min_score) / range;
            let entry = score_map.entry(result.id.clone()).or_insert((0.0, Vec::new()));
            entry.0 += normalized * result.weight;
            if !entry.1.contains(&result.source) {
                entry.1.push(result.source.clone());
            }
        }

        let mut fused: Vec<FusedResult> = score_map
            .into_iter()
            .map(|(id, (score, sources))| FusedResult {
                id,
                score,
                sources,
            })
            .collect();

        fused.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap());
        fused
    }

    fn comb_mnz_fusion(&self, results: &[MultiModalResult]) -> Vec<FusedResult> {
        let mut score_map: HashMap<String, (f32, usize, Vec<String>)> = HashMap::new();

        for result in results {
            let entry = score_map.entry(result.id.clone()).or_insert((0.0, 0, Vec::new()));
            entry.0 += result.score * result.weight;
            entry.1 += 1;
            if !entry.2.contains(&result.source) {
                entry.2.push(result.source.clone());
            }
        }

        let mut fused: Vec<FusedResult> = score_map
            .into_iter()
            .map(|(id, (score, count, sources))| FusedResult {
                id,
                score: score * count as f32,
                sources,
            })
            .collect();

        fused.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap());
        fused
    }

    fn learning_to_rank_fusion(&self, results: &[MultiModalResult]) -> Vec<FusedResult> {
        let mut doc_features: HashMap<String, LTRFeatures> = HashMap::new();

        for result in results {
            let features = doc_features.entry(result.id.clone()).or_insert_with(|| {
                let mut sources = Vec::new();
                sources.push(result.source.clone());
                LTRFeatures {
                    vector_score: if result.source == "vector" { result.score } else { 0.0 },
                    text_score: if result.source == "text" || result.source == "bm25" { result.score } else { 0.0 },
                    min_rank: result.rank,
                    source_count: 0,
                    scores: Vec::new(),
                    sources,
                }
            });

            if result.source == "vector" {
                features.vector_score = features.vector_score.max(result.score);
            } else {
                features.text_score = features.text_score.max(result.score);
            }
            features.min_rank = features.min_rank.min(result.rank);
            features.source_count += 1;
            features.scores.push(result.score);
            if !features.sources.contains(&result.source) {
                features.sources.push(result.source.clone());
            }
        }

        let global_max_score = results.iter().map(|r| r.score).fold(f32::MIN, f32::max);
        let global_min_score = results.iter().map(|r| r.score).fold(f32::MAX, f32::min);
        let score_range = if (global_max_score - global_min_score).abs() > f32::EPSILON {
            global_max_score - global_min_score
        } else {
            1.0
        };

        let mut fused: Vec<FusedResult> = doc_features
            .into_iter()
            .map(|(id, features)| {
                let normalized_vector = features.vector_score / global_max_score.max(1.0);
                let normalized_text = features.text_score / global_max_score.max(1.0);
                let rank_reciprocal = 1.0 / (features.min_rank as f32 + 1.0);
                let source_count_norm = features.source_count as f32 / 5.0;
                let mean = features.scores.iter().sum::<f32>() / features.scores.len() as f32;
                let variance = features.scores.iter().map(|s| (s - mean).powi(2)).sum::<f32>() / features.scores.len() as f32;
                let variance_norm = (variance / score_range).min(1.0);

                let ltr_score =
                    normalized_vector * self.ltr_weights.vector_score_weight
                    + normalized_text * self.ltr_weights.text_score_weight
                    + rank_reciprocal * self.ltr_weights.rank_reciprocal_weight
                    + source_count_norm * self.ltr_weights.source_count_weight
                    + variance_norm * self.ltr_weights.score_variance_weight;

                FusedResult {
                    id,
                    score: ltr_score,
                    sources: features.sources,
                }
            })
            .collect();

        fused.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap());
        fused
    }
}

struct LTRFeatures {
    vector_score: f32,
    text_score: f32,
    min_rank: usize,
    source_count: usize,
    scores: Vec<f32>,
    sources: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultiModalResult {
    pub id: String,
    pub score: f32,
    pub rank: usize,
    pub source: String,
    pub weight: f32,
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FusedResult {
    pub id: String,
    pub score: f32,
    pub sources: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rrf_fusion() {
        let engine = ScoreFusionEngine::new(ScoreFusion::RRF { k: 60 });
        
        let results = vec![
            MultiModalResult {
                id: "doc1".to_string(),
                score: 1.0,
                rank: 1,
                source: "vector".to_string(),
                weight: 1.0,
                metadata: None,
            },
            MultiModalResult {
                id: "doc2".to_string(),
                score: 0.9,
                rank: 2,
                source: "text".to_string(),
                weight: 1.0,
                metadata: None,
            },
            MultiModalResult {
                id: "doc1".to_string(),
                score: 0.8,
                rank: 1,
                source: "text".to_string(),
                weight: 1.0,
                metadata: None,
            },
        ];

        let fused = engine.fuse(&results);
        assert!(!fused.is_empty());
        assert_eq!(fused[0].id, "doc1");
    }

    #[test]
    fn test_weighted_sum() {
        let engine = ScoreFusionEngine::new(ScoreFusion::WeightedSum);
        
        let results = vec![
            MultiModalResult {
                id: "doc1".to_string(),
                score: 1.0,
                rank: 1,
                source: "vector".to_string(),
                weight: 0.6,
                metadata: None,
            },
            MultiModalResult {
                id: "doc1".to_string(),
                score: 0.8,
                rank: 1,
                source: "text".to_string(),
                weight: 0.4,
                metadata: None,
            },
        ];

        let fused = engine.fuse(&results);
        assert_eq!(fused.len(), 1);
        assert!((fused[0].score - 0.92).abs() < 0.01);
    }
}
