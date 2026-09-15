//! Tests for BM25 full-text search

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use crate::BM25Index;
    use crate::MetadataFilter;
    use crate::HybridQueryEngine;
    use crate::coretex_bm25::Document;
    use crate::coretex_bm25::VectorSearchResult;

    #[tokio::test]
    async fn test_bm25_basic_search() {
        let bm25 = BM25Index::new(1.5, 0.75);
        
        bm25.add_document(Document::new(
            "doc1".to_string(),
            "rust programming language".to_string()
        )).await.unwrap();
        
        bm25.add_document(Document::new(
            "doc2".to_string(),
            "python programming tutorials".to_string()
        )).await.unwrap();
        
        bm25.add_document(Document::new(
            "doc3".to_string(),
            "rust async programming".to_string()
        )).await.unwrap();
        
        let results = bm25.search("rust", 10).await.unwrap();
        
        assert!(!results.is_empty());
        assert_eq!(results[0].id, "doc1");
    }

    #[tokio::test]
    async fn test_bm25_empty_index() {
        let bm25 = BM25Index::new(1.5, 0.75);
        
        let results = bm25.search("test", 10).await.unwrap();
        
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_bm25_document_count() {
        let bm25 = BM25Index::new(1.5, 0.75);
        
        assert_eq!(bm25.document_count().await, 0);
        
        bm25.add_document(Document::new(
            "doc1".to_string(),
            "test content".to_string()
        )).await.unwrap();
        
        assert_eq!(bm25.document_count().await, 1);
    }

    #[tokio::test]
    async fn test_bm25_get_document() {
        let bm25 = BM25Index::new(1.5, 0.75);
        
        bm25.add_document(Document::new(
            "doc1".to_string(),
            "test content".to_string()
        )).await.unwrap();
        
        let doc = bm25.get_document("doc1").await;
        assert!(doc.is_some());
        assert_eq!(doc.unwrap().text, "test content");
    }

    #[tokio::test]
    async fn test_bm25_search_no_match() {
        let bm25 = BM25Index::new(1.5, 0.75);
        
        bm25.add_document(Document::new(
            "doc1".to_string(),
            "rust programming".to_string()
        )).await.unwrap();
        
        let results = bm25.search("python", 10).await.unwrap();
        
        assert!(results.is_empty() || results[0].score == 0.0);
    }

    #[tokio::test]
    async fn test_bm25_add_documents_batch() {
        let bm25 = BM25Index::new(1.5, 0.75);
        
        let docs = vec![
            Document::new("doc1".to_string(), "first document".to_string()),
            Document::new("doc2".to_string(), "second document".to_string()),
            Document::new("doc3".to_string(), "third document".to_string()),
        ];
        
        bm25.add_documents(docs).await.unwrap();
        
        assert_eq!(bm25.document_count().await, 3);
    }

    #[tokio::test]
    async fn test_bm25_search_with_filter() {
        let bm25 = BM25Index::new(1.5, 0.75);
        
        let mut doc1 = Document::new("doc1".to_string(), "rust programming".to_string());
        doc1 = doc1.with_field("category", "programming".to_string());
        
        let mut doc2 = Document::new("doc2".to_string(), "python programming".to_string());
        doc2 = doc2.with_field("category", "programming".to_string());
        
        let mut doc3 = Document::new("doc3".to_string(), "machine learning".to_string());
        doc3 = doc3.with_field("category", "ai".to_string());
        
        bm25.add_document(doc1).await.unwrap();
        bm25.add_document(doc2).await.unwrap();
        bm25.add_document(doc3).await.unwrap();
        
        let results = bm25.search_with_filter("programming", 10, |fields| {
            fields.get("category") == Some(&"programming".to_string())
        }).await.unwrap();
        
        assert_eq!(results.len(), 2);
    }

    #[tokio::test]
    async fn test_document_tokenize() {
        let doc = Document::new("test".to_string(), "Hello, World! This is a TEST.".to_string());
        
        assert!(doc.tokens.contains(&"hello".to_string()));
        assert!(doc.tokens.contains(&"world".to_string()));
        assert!(doc.tokens.contains(&"this".to_string()));
        assert!(doc.tokens.contains(&"is".to_string()));
        assert!(doc.tokens.contains(&"a".to_string()));
        assert!(doc.tokens.contains(&"test".to_string()));
    }

    #[tokio::test]
    async fn test_hybrid_query_engine() {
        let bm25 = Arc::new(BM25Index::new(1.5, 0.75));
        
        bm25.add_document(Document::new(
            "doc1".to_string(),
            "rust programming language".to_string()
        )).await.unwrap();
        
        let engine = HybridQueryEngine::new(bm25, 0.5, 0.5);
        
        let vector_results = vec![
            VectorSearchResult {
                id: "doc1".to_string(),
                score: 0.9,
                distance: 0.1,
            },
            VectorSearchResult {
                id: "doc2".to_string(),
                score: 0.7,
                distance: 0.3,
            },
        ];
        
        let results = engine.search(
            "rust",
            Some(vector_results),
            None,
            10
        ).await.unwrap();
        
        assert!(!results.is_empty());
        assert!(results[0].combined_score > 0.0);
    }

    #[tokio::test]
    async fn test_metadata_filter_equal() {
        let filter = MetadataFilter::new().eq("status", "published");
        
        let mut fields = std::collections::HashMap::new();
        fields.insert("status".to_string(), "published".to_string());
        
        let result = filter.matches(&fields);
        assert!(result);
    }

    #[tokio::test]
    async fn test_metadata_filter_greater_than() {
        let filter = MetadataFilter::new().gt("score", 50.0);
        
        let mut fields = std::collections::HashMap::new();
        fields.insert("score".to_string(), "75".to_string());
        
        let result = filter.matches(&fields);
        assert!(result);
    }

    #[tokio::test]
    async fn test_metadata_filter_less_than() {
        let filter = MetadataFilter::new().lt("price", 100.0);
        
        let mut fields = std::collections::HashMap::new();
        fields.insert("price".to_string(), "50".to_string());
        
        let result = filter.matches(&fields);
        assert!(result);
    }
}
