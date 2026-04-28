//! Hybrid Retriever for CoreTexDB
//! Coordinates multiple retrievers for unified multi-modal search

use crate::coretex_hybrid::document::MultiModalDocument;
use crate::coretex_hybrid::query::{HybridQuery, DistanceMetric, FilterOperator};
use crate::coretex_hybrid::fusion::{ScoreFusionEngine, MultiModalResult, ScoreFusion, FusedResult};
use crate::coretex_index::SearchResult;
use crate::coretex_bm25::BM25Index;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct HybridRetriever {
    vector_index: Arc<RwLock<Option<Box<dyn VectorRetriever>>>>,
    text_index: Arc<RwLock<Option<Box<dyn TextRetriever>>>>,
    scalar_storage: Arc<RwLock<HashMap<String, HashMap<String, serde_json::Value>>>>,
    fusion_engine: ScoreFusionEngine,
    coarse_top_k: usize,
}

pub trait VectorRetriever: Send + Sync {
    fn search(&self, vector: &[f32], k: usize, metric: DistanceMetric) -> Vec<SearchResult>;
}

pub trait TextRetriever: Send + Sync {
    fn search(&self, query: &str, k: usize) -> Vec<TextSearchResult>;
}

#[derive(Debug, Clone)]
pub struct TextSearchResult {
    pub id: String,
    pub score: f32,
}

impl HybridRetriever {
    pub fn new() -> Self {
        Self {
            vector_index: Arc::new(RwLock::new(None)),
            text_index: Arc::new(RwLock::new(None)),
            scalar_storage: Arc::new(RwLock::new(HashMap::new())),
            fusion_engine: ScoreFusionEngine::new(ScoreFusion::RRF { k: 60 }),
            coarse_top_k: 100,
        }
    }

    pub fn with_vector_index<T: VectorRetriever + 'static>(self, index: T) -> Self {
        let index: Box<dyn VectorRetriever> = Box::new(index);
        self.vector_index.blocking_write().replace(index);
        self
    }

    pub fn with_text_index<T: TextRetriever + 'static>(self, index: T) -> Self {
        let index: Box<dyn TextRetriever> = Box::new(index);
        self.text_index.blocking_write().replace(index);
        self
    }

    pub fn with_fusion_method(mut self, method: ScoreFusion) -> Self {
        self.fusion_engine = ScoreFusionEngine::new(method);
        self
    }

    pub fn with_coarse_top_k(mut self, k: usize) -> Self {
        self.coarse_top_k = k;
        self
    }

    pub async fn index_document(&self, doc: &MultiModalDocument) -> Result<(), String> {
        if let Some(ref vector) = doc.vector {
            let mut index = self.vector_index.write().await;
            if let Some(ref mut idx) = *index {
                idx.search(&vector.values, 1, DistanceMetric::Cosine);
            }
        }

        if let Some(ref text) = doc.text {
            let mut index = self.text_index.write().await;
            if let Some(ref mut idx) = *index {
                idx.search(&text.content, 1);
            }
        }

        let mut storage = self.scalar_storage.write().await;
        let doc_fields: HashMap<String, serde_json::Value> = doc.scalar_fields
            .iter()
            .map(|(k, v)| (k.clone(), serde_json::to_value(v).unwrap_or(serde_json::Value::Null)))
            .collect();
        storage.insert(doc.id.clone(), doc_fields);

        Ok(())
    }

    pub async fn search(&self, query: &HybridQuery) -> Vec<FusedResult> {
        let mut results = Vec::new();

        if let Some(ref vq) = query.vector_query {
            if let Some(index) = self.vector_index.read().await.as_ref() {
                let vector_results = index.search(&vq.vector, self.coarse_top_k, vq.metric);
                for (rank, result) in vector_results.into_iter().enumerate() {
                    results.push(MultiModalResult {
                        id: result.id,
                        score: 1.0 / (1.0 + result.distance),
                        rank,
                        source: "vector".to_string(),
                        weight: query.weights.vector_weight,
                        metadata: None,
                    });
                }
            }
        }

        if let Some(ref tq) = query.text_query {
            if let Some(index) = self.text_index.read().await.as_ref() {
                let text_results = index.search(&tq.query, self.coarse_top_k);
                for (rank, result) in text_results.into_iter().enumerate() {
                    results.push(MultiModalResult {
                        id: result.id,
                        score: result.score,
                        rank,
                        source: "text".to_string(),
                        weight: query.weights.text_weight,
                        metadata: None,
                    });
                }
            }
        }

        if !query.scalar_filters.is_empty() {
            results = self.apply_scalar_filters(results, &query.scalar_filters).await;
        }

        let fused = self.fusion_engine.fuse(&results);
        
        fused.into_iter()
            .take(query.top_k)
            .collect()
    }

    async fn apply_scalar_filters(
        &self,
        results: Vec<MultiModalResult>,
        filters: &[crate::coretex_hybrid::query::ScalarFilter],
    ) -> Vec<MultiModalResult> {
        let storage = self.scalar_storage.read().await;

        results.into_iter()
            .filter(|r| {
                if let Some(doc_fields) = storage.get(&r.id) {
                    filters.iter().all(|f| self.matches_filter(doc_fields, f))
                } else {
                    false
                }
            })
            .collect()
    }

    fn matches_filter(&self, doc_fields: &HashMap<String, serde_json::Value>, filter: &crate::coretex_hybrid::query::ScalarFilter) -> bool {
        let Some(doc_value) = doc_fields.get(&filter.field) else {
            return false;
        };

        match &filter.operator {
            FilterOperator::Eq => doc_value == &serde_json::to_value(&filter.value).unwrap_or(serde_json::Value::Null),
            FilterOperator::Ne => doc_value != &serde_json::to_value(&filter.value).unwrap_or(serde_json::Value::Null),
            _ => true,
        }
    }

    pub async fn get_document(&self, id: &str) -> Option<MultiModalDocument> {
        let storage = self.scalar_storage.read().await;
        storage.get(id).map(|fields| {
            let mut doc = MultiModalDocument::new(id.to_string());
            for (k, v) in fields {
                if let Ok(scalar) = serde_json::from_value(v.clone()) {
                    doc.scalar_fields.insert(k.clone(), scalar);
                }
            }
            doc
        })
    }
}

impl Default for HybridRetriever {
    fn default() -> Self {
        Self::new()
    }
}

pub struct BruteForceVectorAdapter {
    index: Arc<RwLock<Vec<(String, Vec<f32>)>>>,
    metric: String,
}

impl BruteForceVectorAdapter {
    pub fn new(metric: &str) -> Self {
        Self {
            index: Arc::new(RwLock::new(Vec::new())),
            metric: metric.to_string(),
        }
    }

    pub async fn add(&self, id: &str, vector: &[f32]) {
        let mut index = self.index.write().await;
        index.push((id.to_string(), vector.to_vec()));
    }
}

impl VectorRetriever for BruteForceVectorAdapter {
    fn search(&self, vector: &[f32], k: usize, _metric: DistanceMetric) -> Vec<SearchResult> {
        let index = self.index.blocking_read();
        let mut results: Vec<SearchResult> = index
            .iter()
            .map(|(id, vec)| {
                let distance = calculate_distance(vector, vec, &self.metric);
                SearchResult {
                    id: id.clone(),
                    distance,
                }
            })
            .collect();

        results.sort_by(|a, b| a.distance.partial_cmp(&b.distance).unwrap_or(std::cmp::Ordering::Equal));
        results.into_iter().take(k).collect()
    }
}

fn calculate_distance(a: &[f32], b: &[f32], metric: &str) -> f32 {
    match metric {
        "cosine" => {
            let dot_product: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
            let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
            let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
            if norm_a == 0.0 || norm_b == 0.0 {
                return 1.0;
            }
            1.0 - (dot_product / (norm_a * norm_b))
        }
        "euclidean" => {
            a.iter().zip(b.iter())
                .map(|(x, y)| (x - y).powi(2))
                .sum::<f32>()
                .sqrt()
        }
        _ => {
            let dot_product: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
            let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
            let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
            if norm_a == 0.0 || norm_b == 0.0 {
                return 1.0;
            }
            1.0 - (dot_product / (norm_a * norm_b))
        }
    }
}

pub struct BM25TextAdapter {
    index: BM25Index,
}

impl BM25TextAdapter {
    pub fn new(k1: f32, b: f32) -> Self {
        Self {
            index: BM25Index::new(k1, b),
        }
    }

    pub async fn add(&self, id: &str, text: &str) -> Result<(), String> {
        let doc = crate::coretex_bm25::Document::new(id.to_string(), text.to_string());
        self.index.add_document(doc).await
    }
}

impl TextRetriever for BM25TextAdapter {
    fn search(&self, query: &str, k: usize) -> Vec<TextSearchResult> {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            self.index.search(query, k)
                .await
                .unwrap_or_default()
                .into_iter()
                .map(|r| TextSearchResult { id: r.id, score: r.score })
                .collect()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_hybrid_retriever() {
        let retriever = HybridRetriever::new();
        
        let query = HybridQuery::new()
            .with_text("test query")
            .with_top_k(10);

        let results = retriever.search(&query).await;
        assert!(results.is_empty());
    }
}
