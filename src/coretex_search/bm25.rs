//! BM25 Full-Text Search for CoreTexDB

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct BM25Index {
    documents: Arc<RwLock<HashMap<String, DocumentIndex>>>,
    average_doc_length: Arc<RwLock<f32>>,
    k1: f32,
    b: f32,
}

#[derive(Debug, Clone)]
struct DocumentIndex {
    id: String,
    terms: HashMap<String, u32>,
    term_frequency: HashMap<String, f32>,
    doc_length: usize,
}

#[derive(Debug, Clone)]
pub struct BM25SearchResult {
    pub id: String,
    pub score: f32,
}

impl BM25Index {
    pub fn new(k1: f32, b: f32) -> Self {
        Self {
            documents: Arc::new(RwLock::new(HashMap::new())),
            average_doc_length: Arc::new(RwLock::new(0.0)),
            k1,
            b,
        }
    }

    pub fn with_defaults() -> Self {
        Self::new(1.5, 0.75)
    }

    pub async fn add_document(&self, id: &str, text: &str) {
        let terms = self.tokenize(text);
        let doc_length = terms.len();
        
        let mut term_freq: HashMap<String, u32> = HashMap::new();
        for term in &terms {
            *term_freq.entry(term.clone()).or_insert(0) += 1;
        }

        let term_tf: HashMap<String, f32> = term_freq
            .iter()
            .map(|(k, v)| (k.clone(), *v as f32))
            .collect();

        let doc = DocumentIndex {
            id: id.to_string(),
            terms: term_freq,
            term_frequency: term_tf,
            doc_length,
        };

        let mut docs = self.documents.write().await;
        docs.insert(id.to_string(), doc);

        let total_length: usize = docs.values().map(|d| d.doc_length).sum();
        let avg = total_length as f32 / docs.len().max(1) as f32;
        *self.average_doc_length.write().await = avg;
    }

    pub async fn search(&self, query: &str, top_k: usize) -> Vec<BM25SearchResult> {
        let query_terms = self.tokenize(query);
        if query_terms.is_empty() {
            return vec![];
        }

        let docs = self.documents.read().await;
        let avg_doc_length = *self.average_doc_length.read().await;
        let num_docs = docs.len() as f32;

        let mut idf: HashMap<String, f32> = HashMap::new();
        for term in &query_terms {
            let doc_count = docs.values()
                .filter(|d| d.terms.contains_key(term))
                .count() as f32;
            
            if doc_count > 0.0 {
                idf.insert(term.clone(), ((num_docs - doc_count + 0.5) / (doc_count + 0.5) + 1.0).ln());
            }
        }

        let mut scores: Vec<(String, f32)> = Vec::new();

        for (doc_id, doc) in docs.iter() {
            let mut score = 0.0f32;
            
            for term in &query_terms {
                if let Some(&tf) = doc.term_frequency.get(term) {
                    let idf_score = idf.get(term).unwrap_or(&0.0);
                    
                    let numerator = tf * (self.k1 + 1.0);
                    let denominator = tf + self.k1 * (1.0 - self.b + self.b * (doc.doc_length as f32 / avg_doc_length.max(1.0)));
                    
                    score += idf_score * (numerator / denominator.max(0.001));
                }
            }
            
            scores.push((doc_id.clone(), score));
        }

        scores.sort_by(|a, b| {
            b.1.partial_cmp(&a.1).unwrap_or_else(|| {
                if a.1.is_nan() && b.1.is_nan() {
                    std::cmp::Ordering::Equal
                } else if a.1.is_nan() {
                    std::cmp::Ordering::Greater
                } else if b.1.is_nan() {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Equal
                }
            })
        });

        scores.into_iter()
            .take(top_k)
            .map(|(id, score)| BM25SearchResult { id, score })
            .collect()
    }

    pub async fn get_document_count(&self) -> usize {
        self.documents.read().await.len()
    }

    pub async fn clear(&self) {
        self.documents.write().await.clear();
        *self.average_doc_length.write().await = 0.0;
    }

    fn tokenize(&self, text: &str) -> Vec<String> {
        text.to_lowercase()
            .split(|c: char| !c.is_alphanumeric() && c != '_')
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_bm25_search() {
        let index = BM25Index::with_defaults();
        
        index.add_document("doc1", "the quick brown fox").await;
        index.add_document("doc2", "the lazy dog").await;
        index.add_document("doc3", "quick brown fox jumps over lazy dog").await;
        
        let results = index.search("quick fox", 2).await;
        
        assert!(!results.is_empty());
        assert_eq!(results[0].id, "doc1");
    }

    #[tokio::test]
    async fn test_tokenize() {
        let index = BM25Index::with_defaults();
        let terms = index.tokenize("Hello, World! 123");
        
        assert!(terms.contains(&"hello".to_string()));
        assert!(terms.contains(&"world".to_string()));
    }
}
