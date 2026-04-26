//! Tantivy Full-Text Search Integration for CoreTexDB
//! Provides production-grade full-text search capabilities

use std::path::Path;
use std::sync::Arc;
use tantivy::{
    collector::TopDocs,
    query::QueryParser,
    schema::*,
    Index, IndexReader, IndexWriter, ReloadPolicy, TantivyDocument,
};
use tokio::sync::RwLock;

pub struct TantivySearcher {
    index: Index,
    reader: IndexReader,
    writer: Arc<RwLock<IndexWriter>>,
    schema: Schema,
    id_field: Field,
    text_field: Field,
    score_field: Field,
}

pub struct TantivyDocumentResult {
    pub id: String,
    pub text: String,
    pub score: f32,
}

impl TantivySearcher {
    pub fn new(index_path: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let mut schema_builder = Schema::builder();

        let id_field = schema_builder.add_text_field("id", STRING | STORED);
        let text_field = schema_builder.add_text_field("text", TEXT | STORED);
        let score_field = schema_builder.add_f64_field("score", STORED);

        let schema = schema_builder.build();

        let index = if Path::new(index_path).exists() {
            Index::open_in_dir(index_path)?
        } else {
            std::fs::create_dir_all(index_path)?;
            Index::create_in_dir(index_path, schema.clone())?
        };

        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommitWithDelay)
            .try_into()?;

        let writer = index.writer(50_000_000)?;

        Ok(Self {
            index,
            reader,
            writer: Arc::new(RwLock::new(writer)),
            schema,
            id_field,
            text_field,
            score_field,
        })
    }

    pub fn in_memory() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let mut schema_builder = Schema::builder();

        let id_field = schema_builder.add_text_field("id", STRING | STORED);
        let text_field = schema_builder.add_text_field("text", TEXT | STORED);
        let score_field = schema_builder.add_f64_field("score", STORED);

        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema.clone());

        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommitWithDelay)
            .try_into()?;

        let writer = index.writer(50_000_000)?;

        Ok(Self {
            index,
            reader,
            writer: Arc::new(RwLock::new(writer)),
            schema,
            id_field,
            text_field,
            score_field,
        })
    }

    pub async fn add_document(
        &self,
        id: &str,
        text: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut doc = TantivyDocument::default();
        doc.add_text(self.id_field, id);
        doc.add_text(self.text_field, text);
        doc.add_f64(self.score_field, 0.0);

        let mut writer = self.writer.write().await;
        writer.add_document(doc)?;
        writer.commit()?;

        Ok(())
    }

    pub async fn add_documents(
        &self,
        documents: Vec<(String, String)>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut writer = self.writer.write().await;

        for (id, text) in documents {
            let mut doc = TantivyDocument::default();
            doc.add_text(self.id_field, &id);
            doc.add_text(self.text_field, &text);
            doc.add_f64(self.score_field, 0.0);
            writer.add_document(doc)?;
        }

        writer.commit()?;
        Ok(())
    }

    pub async fn search(
        &self,
        query_str: &str,
        top_k: usize,
    ) -> Result<Vec<TantivyDocumentResult>, Box<dyn std::error::Error + Send + Sync>> {
        let searcher = self.reader.searcher();
        let query_parser = QueryParser::for_index(&self.index, vec![self.text_field]);

        let query = query_parser.parse_query(query_str)?;
        let top_docs = searcher.search(&query, &TopDocs::with_limit(top_k))?;

        let mut results = Vec::new();

        for (score, doc_address) in top_docs {
            let doc: TantivyDocument = searcher.doc(doc_address)?;

            let id = doc
                .get_first(self.id_field)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            let text = doc
                .get_first(self.text_field)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            results.push(TantivyDocumentResult {
                id,
                text,
                score,
            });
        }

        Ok(results)
    }

    pub async fn delete_document(&self, id: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut writer = self.writer.write().await;
        let term = tantivy::Term::from_field_text(self.id_field, id);
        writer.delete_term(term);
        writer.commit()?;
        Ok(())
    }

    pub async fn clear(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut writer = self.writer.write().await;
        writer.delete_all_documents()?;
        writer.commit()?;
        Ok(())
    }

    pub async fn get_doc_count(&self) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
        let searcher = self.reader.searcher();
        Ok(searcher.num_docs() as usize)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_tantivy_in_memory() {
        let searcher = TantivySearcher::in_memory().unwrap();

        searcher
            .add_document("1", "The quick brown fox jumps")
            .await
            .unwrap();
        searcher
            .add_document("2", "The lazy dog sleeps")
            .await
            .unwrap();
        searcher
            .add_document("3", "A quick brown puppy")
            .await
            .unwrap();

        let results = searcher.search("quick", 2).await.unwrap();

        assert!(!results.is_empty());
        assert_eq!(results[0].id, "1");
    }

    #[tokio::test]
    async fn test_tantivy_batch_add() {
        let searcher = TantivySearcher::in_memory().unwrap();

        let docs = vec![
            ("doc1".to_string(), "hello world".to_string()),
            ("doc2".to_string(), "rust programming".to_string()),
            ("doc3".to_string(), "vector database".to_string()),
        ];

        searcher.add_documents(docs).await.unwrap();

        let count = searcher.get_doc_count().await.unwrap();
        assert_eq!(count, 3);
    }

    #[tokio::test]
    async fn test_tantivy_delete() {
        let searcher = TantivySearcher::in_memory().unwrap();

        searcher.add_document("1", "test content").await.unwrap();
        searcher.add_document("2", "another document").await.unwrap();

        searcher.delete_document("1").await.unwrap();

        let count = searcher.get_doc_count().await.unwrap();
        assert_eq!(count, 1);
    }
}
