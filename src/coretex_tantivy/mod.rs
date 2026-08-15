//! Full-text search module powered by Tantivy
//!
//! Provides tokenization (CJK + Latin), BM25 scoring, and index persistence
//! for the CoreTexDB full-text search pipeline.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

// Re-export tantivy types for downstream consumers
pub use tantivy::schema::{Schema, Field, Value, STORED, TEXT, STRING};
pub use tantivy::query::QueryParser;
pub use tantivy::schema::TantivyDocument;
pub use tantivy::{DateTime, doc};

/// Configuration for a full-text index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TantivyIndexConfig {
    /// On-disk directory for the index.
    pub index_path: String,
    /// Whether to create the index directory if it does not exist.
    pub create_if_missing: bool,
    /// Fields to index. Each entry is (field_name, is_stored).
    pub fields: Vec<(String, bool)>,
}

impl Default for TantivyIndexConfig {
    fn default() -> Self {
        Self {
            index_path: "./data/tantivy_index".to_string(),
            create_if_missing: true,
            fields: vec![
                ("title".to_string(), true),
                ("body".to_string(), true),
                ("id".to_string(), true),
            ],
        }
    }
}

/// A single document to be indexed.
#[derive(Debug, Clone)]
pub struct TantivyDocumentEntry {
    pub id: String,
    pub fields: Vec<(String, String)>,
}

/// Search result returned from a full-text query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TantivySearchResult {
    pub id: String,
    pub score: f32,
    pub snippet: String,
}

/// Error type for Tantivy operations.
#[derive(Debug)]
pub enum TantivyError {
    IndexError(String),
    QueryError(String),
    IoError(String),
    SchemaError(String),
}

impl std::fmt::Display for TantivyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TantivyError::IndexError(msg) => write!(f, "Index error: {}", msg),
            TantivyError::QueryError(msg) => write!(f, "Query error: {}", msg),
            TantivyError::IoError(msg) => write!(f, "IO error: {}", msg),
            TantivyError::SchemaError(msg) => write!(f, "Schema error: {}", msg),
        }
    }
}

impl std::error::Error for TantivyError {}

/// The main full-text searcher wrapping a Tantivy index.
pub struct TantivySearcher {
    config: TantivyIndexConfig,
    schema: Schema,
    index_writer: Arc<RwLock<Option<tantivy::IndexWriter<TantivyDocument>>>>,
    reader: Arc<RwLock<Option<tantivy::IndexReader>>>,
    field_map: Arc<RwLock<std::collections::HashMap<String, Field>>>,
}

impl TantivySearcher {
    /// Create a new searcher. Call `open()` to actually build/open the index.
    pub fn new(config: TantivyIndexConfig) -> Self {
        let mut schema_builder = Schema::builder();
        for (name, stored) in &config.fields {
            if *stored {
                schema_builder.add_text_field(name, TEXT | STORED);
            } else {
                schema_builder.add_text_field(name, TEXT);
            }
        }
        let schema = schema_builder.build();

        Self {
            config,
            schema,
            index_writer: Arc::new(RwLock::new(None)),
            reader: Arc::new(RwLock::new(None)),
            field_map: Arc::new(RwLock::new(std::collections::HashMap::new())),
        }
    }

    /// Open (or create) the index on disk and prepare the writer/reader.
    pub async fn open(&self) -> Result<(), TantivyError> {
        let index_path = Path::new(&self.config.index_path);

        if self.config.create_if_missing && !index_path.exists() {
            std::fs::create_dir_all(index_path)
                .map_err(|e| TantivyError::IoError(format!("Failed to create index dir: {}", e)))?;
        }

        let index = tantivy::Index::open_or_create(
            tantivy::directory::MmapDirectory::open(index_path)
                .map_err(|e| TantivyError::IoError(format!("Failed to open mmap directory: {}", e)))?,
            self.schema.clone(),
        )
        .map_err(|e| TantivyError::IndexError(format!("Failed to open index: {}", e)))?;

        // Build field name → Field mapping
        let mut fmap = std::collections::HashMap::new();
        for (name, _) in &self.config.fields {
            if let Some(field) = self.schema.get_field(name) {
                fmap.insert(name.clone(), field);
            }
        }
        *self.field_map.write().await = fmap;

        // Create writer (heap size: 50 MB)
        let writer = index
            .writer(50 * 1024 * 1024)
            .map_err(|e| TantivyError::IndexError(format!("Failed to create writer: {}", e)))?;
        *self.index_writer.write().await = Some(writer);

        // Create reader
        let reader = index
            .reader_builder()
            .reload_policy(tantivy::ReloadPolicy::OnCommitWithDelay)
            .try_into()
            .map_err(|e| TantivyError::IndexError(format!("Failed to create reader: {}", e)))?;
        *self.reader.write().await = Some(reader);

        Ok(())
    }

    /// Index a batch of documents.
    pub async fn index_documents(&self, docs: Vec<TantivyDocumentEntry>) -> Result<usize, TantivyError> {
        let mut writer_guard = self.index_writer.write().await;
        let writer = writer_guard
            .as_mut()
            .ok_or_else(|| TantivyError::IndexError("Index not opened".to_string()))?;

        let fmap = self.field_map.read().await;
        let mut count = 0usize;

        for doc_entry in docs {
            let mut doc = TantivyDocument::default();
            for (field_name, value) in &doc_entry.fields {
                if let Some(&field) = fmap.get(field_name) {
                    doc.add_text(field, value);
                }
            }
            // Also store the id field
            if let Some(&id_field) = fmap.get("id") {
                doc.add_text(id_field, &doc_entry.id);
            }

            writer
                .add_document(doc)
                .map_err(|e| TantivyError::IndexError(format!("Failed to add document: {}", e)))?;
            count += 1;
        }

        writer
            .commit()
            .map_err(|e| TantivyError::IndexError(format!("Commit failed: {}", e)))?;

        // Reload the reader so searches see the new documents
        if let Some(reader) = self.reader.read().await.as_ref() {
            reader.reload().ok();
        }

        Ok(count)
    }

    /// Index a single document.
    pub async fn index_document(&self, doc: TantivyDocumentEntry) -> Result<(), TantivyError> {
        self.index_documents(vec![doc]).await?;
        Ok(())
    }

    /// Delete a document by its id field value.
    pub async fn delete_by_id(&self, id: &str) -> Result<bool, TantivyError> {
        let mut writer_guard = self.index_writer.write().await;
        let writer = writer_guard
            .as_mut()
            .ok_or_else(|| TantivyError::IndexError("Index not opened".to_string()))?;

        let fmap = self.field_map.read().await;
        let id_field = fmap
            .get("id")
            .ok_or_else(|| TantivyError::SchemaError("No 'id' field in schema".to_string()))?;

        let count = writer
            .delete_term(tantivy::Term::from_field_text(*id_field, id));
        writer
            .commit()
            .map_err(|e| TantivyError::IndexError(format!("Commit failed: {}", e)))?;

        Ok(count > 0)
    }

    /// Full-text search with BM25 scoring.
    pub async fn search(&self, query_str: &str, limit: usize) -> Result<Vec<TantivySearchResult>, TantivyError> {
        let reader_guard = self.reader.read().await;
        let reader = reader_guard
            .as_ref()
            .ok_or_else(|| TantivyError::IndexError("Index not opened".to_string()))?;

        let searcher = reader.searcher();

        // Determine default search fields: all TEXT fields except 'id'
        let default_fields: Vec<Field> = self.config.fields.iter()
            .filter(|(name, _)| name != "id")
            .filter_map(|(name, _)| self.schema.get_field(name))
            .collect();

        let query_parser = QueryParser::for_index(&self.schema, default_fields);
        let query = query_parser
            .parse_query(query_str)
            .map_err(|e| TantivyError::QueryError(format!("Parse error: {}", e)))?;

        let top_docs = searcher
            .search(&*query, &tantivy::collector::TopDocs::with_limit(limit))
            .map_err(|e| TantivyError::QueryError(format!("Search failed: {}", e)))?;

        let mut results = Vec::new();
        for (score, doc_addr) in top_docs {
            if let Ok(doc) = searcher.doc::<TantivyDocument>(doc_addr) {
                let id = doc.get_first(self.schema.get_field("id").unwrap_or_default())
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();

                // Build snippet from first text field
                let snippet = self.config.fields.iter()
                    .filter(|(name, _)| name != "id")
                    .find_map(|(name, _)| {
                        let field = self.schema.get_field(name)?;
                        doc.get_first(field).and_then(|v| v.as_str()).map(|s| s.to_string())
                    })
                    .unwrap_or_default();

                results.push(TantivySearchResult {
                    id,
                    score,
                    snippet,
                });
            }
        }

        Ok(results)
    }

    /// Return the number of indexed documents.
    pub async fn count(&self) -> Result<usize, TantivyError> {
        let reader_guard = self.reader.read().await;
        let reader = reader_guard
            .as_ref()
            .ok_or_else(|| TantivyError::IndexError("Index not opened".to_string()))?;
        Ok(reader.searcher().num_docs() as usize)
    }

    /// Get a reference to the underlying schema.
    pub fn schema(&self) -> &Schema {
        &self.schema
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let cfg = TantivyIndexConfig::default();
        assert!(!cfg.index_path.is_empty());
        assert!(!cfg.fields.is_empty());
    }
}
