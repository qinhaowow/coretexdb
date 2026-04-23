//! BM25 Full-Text Search for CortexDB

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct BM25Index {
    documents: Arc<RwLock<HashMap<String, Document>>>,
    idf: Arc<RwLock<HashMap<String, f32>>>,
    avgdl: Arc<RwLock<f32>>,
    k1: f32,
    b: f32,
}

#[derive(Debug, Clone)]
pub struct Document {
    pub id: String,
    pub text: String,
    pub tokens: Vec<String>,
    pub field_values: HashMap<String, String>,
}

impl Document {
    pub fn new(id: String, text: String) -> Self {
        let tokens = Self::tokenize(&text);
        Self {
            id,
            text,
            tokens,
            field_values: HashMap::new(),
        }
    }

    pub fn with_field(mut self, field: &str, value: String) -> Self {
        self.field_values.insert(field.to_string(), value);
        self
    }

    fn tokenize(text: &str) -> Vec<String> {
        text.to_lowercase()
            .split(|c: char| !c.is_alphanumeric() && c != '_')
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect()
    }
}

impl BM25Index {
    pub fn new(k1: f32, b: f32) -> Self {
        Self {
            documents: Arc::new(RwLock::new(HashMap::new())),
            idf: Arc::new(RwLock::new(HashMap::new())),
            avgdl: Arc::new(RwLock::new(0.0)),
            k1,
            b,
        }
    }

    pub async fn add_document(&self, doc: Document) -> Result<(), String> {
        let doc_id = doc.id.clone();
        let doc_len = doc.tokens.len() as f32;
        
        let mut docs = self.documents.write().await;
        docs.insert(doc_id.clone(), doc);

        let N = docs.len() as f32;
        let mut doc_freqs: HashMap<String, usize> = HashMap::new();

        for doc in docs.values() {
            let mut unique_terms: HashSet<String> = doc.tokens.iter().cloned().collect();
            for term in unique_terms {
                *doc_freqs.entry(term).or_insert(0) += 1;
            }
        }

        let mut idf = self.idf.write().await;
        idf.clear();
        
        for (term, df) in doc_freqs {
            let idf_score = ((N - df as f32 + 0.5) / (df as f32 + 0.5) + 1.0).ln();
            idf.insert(term, idf_score);
        }

        let total_len: f32 = docs.values().map(|d| d.tokens.len() as f32).sum();
        let mut avgdl = self.avgdl.write().await;
        *avgdl = if N > 0.0 { total_len / N } else { 0.0 };

        Ok(())
    }

    pub async fn add_documents(&self, docs: Vec<Document>) -> Result<(), String> {
        for doc in docs {
            self.add_document(doc).await?;
        }
        Ok(())
    }

    pub async fn search(&self, query: &str, top_k: usize) -> Result<Vec<BM25Result>, String> {
        let query_tokens = Document::tokenize(query);
        
        let docs = self.documents.read().await;
        let idf = self.idf.read().await;
        let avgdl = *self.avgdl.read().await;

        let mut scores: Vec<(String, f32)> = Vec::new();

        for (doc_id, doc) in docs.iter() {
            let score = self.calculate_score(&query_tokens, &doc.tokens, doc.tokens.len() as f32, avgdl, &idf);
            scores.push((doc_id.clone(), score));
        }

        Self::sort_scores(&mut scores);
        scores.truncate(top_k);

        Ok(scores
            .into_iter()
            .map(|(id, score)| BM25Result { id, score })
            .collect())
    }

    fn sort_scores(scores: &mut Vec<(String, f32)>) {
        scores.sort_by(|a, b| {
            b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal)
        });
    }

    fn calculate_score(
        &self,
        query_tokens: &[String],
        doc_tokens: &[String],
        doc_len: f32,
        avgdl: f32,
        idf: &HashMap<String, f32>,
    ) -> f32 {
        let mut score = 0.0;
        let doc_tf: HashMap<&String, usize> = doc_tokens.iter().fold(HashMap::new(), |mut acc, t| {
            *acc.entry(t).or_insert(0) += 1;
            acc
        });

        for term in query_tokens {
            let tf = doc_tf.get(term).unwrap_or(&0) as f32;
            let idf_score = idf.get(term).copied().unwrap_or(0.0);
            
            let numerator = tf * (self.k1 + 1.0);
            let denominator = tf + self.k1 * (1.0 - self.b + self.b * doc_len / avgdl);
            
            score += idf_score * (numerator / denominator);
        }

        score
    }

    pub async fn search_with_filter<F>(
        &self,
        query: &str,
        top_k: usize,
        filter: F,
    ) -> Result<Vec<BM25Result>, String>
    where
        F: Fn(&HashMap<String, String>) -> bool,
    {
        let query_tokens = Document::tokenize(query);
        
        let docs = self.documents.read().await;
        let idf = self.idf.read().await;
        let avgdl = *self.avgdl.read().await;

        let mut scores: Vec<(String, f32)> = Vec::new();

        for (doc_id, doc) in docs.iter() {
            if !filter(&doc.field_values) {
                continue;
            }
            
            let score = self.calculate_score(&query_tokens, &doc.tokens, doc.tokens.len() as f32, avgdl, &idf);
            scores.push((doc_id.clone(), score));
        }

        Self::sort_scores(&mut scores);
        scores.truncate(top_k);

        Ok(scores
            .into_iter()
            .map(|(id, score)| BM25Result { id, score })
            .collect())
    }

    pub async fn get_document(&self, id: &str) -> Option<Document> {
        let docs = self.documents.read().await;
        docs.get(id).cloned()
    }

    pub async fn document_count(&self) -> usize {
        let docs = self.documents.read().await;
        docs.len()
    }
}

#[derive(Debug, Clone)]
pub struct BM25Result {
    pub id: String,
    pub score: f32,
}

pub struct HybridQueryEngine {
    bm25: Arc<BM25Index>,
    vector_weight: f32,
    text_weight: f32,
}

impl HybridQueryEngine {
    pub fn new(bm25: Arc<BM25Index>, vector_weight: f32, text_weight: f32) -> Self {
        Self {
            bm25,
            vector_weight,
            text_weight,
        }
    }

    pub async fn search(
        &self,
        query: &str,
        vector_results: Option<Vec<VectorSearchResult>>,
        metadata_filter: Option<MetadataFilter>,
        top_k: usize,
    ) -> Result<Vec<HybridSearchResult>, String> {
        let mut combined_scores: HashMap<String, (f32, f32, f32)> = HashMap::new();

        if let Some(vr) = vector_results {
            for r in vr {
                let final_score = r.score * self.vector_weight;
                combined_scores.insert(r.id, (final_score, r.score, 0.0));
            }
        }

        let bm25_results = if let Some(ref filter) = metadata_filter {
            self.bm25
                .search_with_filter(query, top_k * 2, |fields| filter.matches(fields))
                .await?
        } else {
            self.bm25.search(query, top_k * 2).await?
        };

        for r in bm25_results {
            let final_score = r.score * self.text_weight;
            if let Some(existing) = combined_scores.get_mut(&r.id) {
                existing.0 += final_score;
                existing.2 = r.score;
            } else {
                combined_scores.insert(r.id, (final_score, 0.0, r.score));
            }
        }

        let mut results: Vec<HybridSearchResult> = combined_scores
            .into_iter()
            .map(|(id, (combined, vector_score, text_score))| HybridSearchResult {
                id,
                combined_score: combined,
                vector_score,
                text_score,
            })
            .collect();

        results.sort_by(|a, b| b.combined_score.partial_cmp(&a.combined_score).unwrap());
        results.truncate(top_k);

        Ok(results)
    }
}

#[derive(Debug, Clone)]
pub struct VectorSearchResult {
    pub id: String,
    pub score: f32,
    pub distance: f32,
}

#[derive(Debug, Clone)]
pub struct HybridSearchResult {
    pub id: String,
    pub combined_score: f32,
    pub vector_score: f32,
    pub text_score: f32,
}

#[derive(Debug, Clone)]
pub struct MetadataFilter {
    conditions: Vec<FilterCondition>,
}

impl MetadataFilter {
    pub fn new() -> Self {
        Self {
            conditions: Vec::new(),
        }
    }

    pub fn eq(mut self, field: &str, value: &str) -> Self {
        self.conditions.push(FilterCondition::Equal(field.to_string(), value.to_string()));
        self
    }

    pub fn gt(mut self, field: &str, value: f32) -> Self {
        self.conditions.push(FilterCondition::GreaterThan(field.to_string(), value));
        self
    }

    pub fn lt(mut self, field: &str, value: f32) -> Self {
        self.conditions.push(FilterCondition::LessThan(field.to_string(), value));
        self
    }

    pub fn in_values(mut self, field: &str, values: Vec<String>) -> Self {
        self.conditions.push(FilterCondition::In(field.to_string(), values));
        self
    }

    pub fn matches(&self, fields: &HashMap<String, String>) -> bool {
        for cond in &self.conditions {
            if !cond.matches(fields) {
                return false;
            }
        }
        true
    }
}

#[derive(Debug, Clone)]
pub enum FilterCondition {
    Equal(String, String),
    GreaterThan(String, f32),
    LessThan(String, f32),
    In(String, Vec<String>),
}

impl FilterCondition {
    fn matches(&self, fields: &HashMap<String, String>) -> bool {
        match self {
            FilterCondition::Equal(field, value) => {
                fields.get(field).map(|v| v == value).unwrap_or(false)
            }
            FilterCondition::GreaterThan(field, value) => {
                fields.get(field)
                    .and_then(|v| v.parse::<f32>().ok())
                    .map(|v| v > *value)
                    .unwrap_or(false)
            }
            FilterCondition::LessThan(field, value) => {
                fields.get(field)
                    .and_then(|v| v.parse::<f32>().ok())
                    .map(|v| v < *value)
                    .unwrap_or(false)
            }
            FilterCondition::In(field, values) => {
                fields.get(field)
                    .map(|v| values.contains(v))
                    .unwrap_or(false)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_bm25_basic() {
        let bm25 = BM25Index::new(1.5, 0.75);

        bm25.add_document(Document::new("1".to_string(), "hello world".to_string())).await.unwrap();
        bm25.add_document(Document::new("2".to_string(), "hello rust".to_string())).await.unwrap();
        bm25.add_document(Document::new("3".to_string(), "world of rust".to_string())).await.unwrap();

        let results = bm25.search("hello", 2).await.unwrap();
        
        assert!(!results.is_empty());
        assert!(results[0].score >= results[1].score);
    }

    #[tokio::test]
    async fn test_metadata_filter() {
        let filter = MetadataFilter::new()
            .eq("category", "tech")
            .gt("priority", 5.0);

        let mut fields = HashMap::new();
        fields.insert("category".to_string(), "tech".to_string());
        fields.insert("priority".to_string(), "10".to_string());
        
        assert!(filter.matches(&fields));
    }
}
