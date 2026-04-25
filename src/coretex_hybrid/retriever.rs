//! Hybrid Retriever for CoreTexDB
//! Coordinates multiple retrievers for unified multi-modal search

use crate::coretex_hybrid::document::MultiModalDocument;
use crate::coretex_hybrid::query::{HybridQuery, DistanceMetric, FilterOperator, ScalarFilterValue};
use crate::coretex_hybrid::fusion::{ScoreFusionEngine, MultiModalResult, ScoreFusion, FusedResult};
use crate::coretex_index::{SearchResult, BruteForceIndex, HNSWIndex};
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
    index: BruteForceIndex,
}

impl BruteForceVectorAdapter {
    pub fn new(dimension: usize) -> Self {
        let index = BruteForceIndex::new(dimension);
        Self { index }
    }

    pub fn add(&self, id: &str, vector: &[f32]) -> Result<(), String> {
        self.index.add(id, vector).map_err(|e| e.to_string())
    }
}

impl VectorRetriever for BruteForceVectorAdapter {
    fn search(&self, vector: &[f32], k: usize, _metric: DistanceMetric) -> Vec<SearchResult> {
        self.index.search(vector, k)
    }
}

pub struct BM25TextAdapter {
    index: BM25Index,
}

impl BM25TextAdapter {
    pub fn new() -> Self {
        Self {
            index: BM25Index::with_defaults(),
        }
    }

    pub fn add(&self, id: &str, text: &str) {
        let index = &self.index;
        tokio::runtime::Handle::current().block_on(async {
            index.add_document(id, text).await;
        });
    }
}

impl TextRetriever for BM25TextAdapter {
    fn search(&self, query: &str, k: usize) -> Vec<TextSearchResult> {
        let index = &self.index;
        tokio::runtime::Handle::current().block_on(async {
            index.search(query, k)
                .await
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
