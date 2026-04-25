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

pub struct ScoreFusionEngine {
    method: ScoreFusion,
}

impl ScoreFusionEngine {
    pub fn new(method: ScoreFusion) -> Self {
        Self { method }
    }

    pub fn fuse(&self, results: &[MultiModalResult]) -> Vec<FusedResult> {
        match &self.method {
            ScoreFusion::RRF { k } => self.rrf_fusion(results, *k),
            ScoreFusion::WeightedSum => self.weighted_sum_fusion(results),
            ScoreFusion::WeightedSumNormalized => self.normalized_weighted_sum(results),
            ScoreFusion::CombMNZ => self.comb_mnz_fusion(results),
            ScoreFusion::LearningToRank => unimplemented!("Learning to rank requires trained model"),
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
