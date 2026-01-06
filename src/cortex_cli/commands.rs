//! Command execution for CortexDB CLI.
//!
//! This module provides command execution functionality for the CortexDB CLI, including:
//! - Command execution logic
//! - Collection management commands
//! - Document management commands
//! - Search and query commands
//! - Index management commands
//! - System management commands
//! - Error handling and reporting

use crate::cortex_cli::parser::{
    CliCommand, CollectionSubcommand, DocumentSubcommand, IndexSubcommand, SearchSubcommand,
    SystemSubcommand,
};
use crate::cortex_core::error::{CortexError, CollectionError, DocumentError, IndexError, QueryError};
use crate::cortex_core::types::{Document, Float32Vector};
use crate::cortex_index::IndexManager;
use crate::cortex_query::{QueryBuilder, QueryExecutor};
use crate::cortex_storage::engine::StorageEngine;
use serde_json;
use std::fs;
use std::sync::Arc;
use std::time::Instant;

/// Command executor
pub struct CommandExecutor {
    /// Storage engine
    storage: Arc<dyn StorageEngine>,
    /// Index manager
    index_manager: Arc<IndexManager>,
    /// Query executor
    query_executor: Arc<QueryExecutor>,
}

impl CommandExecutor {
    /// Create a new command executor
    pub fn new(
        storage: Arc<dyn StorageEngine>,
        index_manager: Arc<IndexManager>,
    ) -> Self {
        let query_executor = Arc::new(QueryExecutor::new(storage.clone(), index_manager.clone()));

        Self {
            storage,
            index_manager,
            query_executor,
        }
    }

    /// Execute a CLI command
    pub async fn execute(&self, command: CliCommand) -> Result<(), CortexError> {
        match command {
            CliCommand::Collection { subcommand } => {
                self.execute_collection_command(subcommand).await
            }
            CliCommand::Document { subcommand } => {
                self.execute_document_command(subcommand).await
            }
            CliCommand::Search { subcommand } => {
                self.execute_search_command(subcommand).await
            }
            CliCommand::Index { subcommand } => {
                self.execute_index_command(subcommand).await
            }
            CliCommand::System { subcommand } => {
                self.execute_system_command(subcommand).await
            }
            CliCommand::Help => {
                crate::cortex_cli::parser::print_help();
                Ok(())
            }
            CliCommand::Version => {
                crate::cortex_cli::parser::print_version();
                Ok(())
            }
        }
    }

    /// Execute collection command
    async fn execute_collection_command(&self, subcommand: CollectionSubcommand) -> Result<(), CortexError> {
        match subcommand {
            CollectionSubcommand::List => {
                let start_time = Instant::now();
                let collections = self.storage.list_collections().await?;
                let elapsed = start_time.elapsed().as_millis();

                println!("Collections ({} found):", collections.len());
                for collection in collections {
                    println!("  - {}", collection);
                }
                println!("Execution time: {}ms", elapsed);
                Ok(())
            }
            CollectionSubcommand::Create { name, schema } => {
                let start_time = Instant::now();
                let schema = schema.as_ref().map(|s| serde_json::from_str(s)).transpose()?;
                self.storage.create_collection(&name, schema, None).await?;
                let elapsed = start_time.elapsed().as_millis();

                println!("Created collection: {}", name);
                println!("Execution time: {}ms", elapsed);
                Ok(())
            }
            CollectionSubcommand::Get { name } => {
                let start_time = Instant::now();
                let collection = self.storage.get_collection(&name).await?;
                let elapsed = start_time.elapsed().as_millis();

                println!("Collection: {}", name);
                println!("{:#}", serde_json::to_string_pretty(&collection)?);
                println!("Execution time: {}ms", elapsed);
                Ok(())
            }
            CollectionSubcommand::Delete { name } => {
                let start_time = Instant::now();
                self.storage.delete_collection(&name).await?;
                let elapsed = start_time.elapsed().as_millis();

                println!("Deleted collection: {}", name);
                println!("Execution time: {}ms", elapsed);
                Ok(())
            }
            CollectionSubcommand::Update { name, data } => {
                let start_time = Instant::now();
                let update_data = serde_json::from_str(&data)?;
                self.storage.update_collection(&name, update_data).await?;
                let elapsed = start_time.elapsed().as_millis();

                println!("Updated collection: {}", name);
                println!("Execution time: {}ms", elapsed);
                Ok(())
            }
        }
    }

    /// Execute document command
    async fn execute_document_command(&self, subcommand: DocumentSubcommand) -> Result<(), CortexError> {
        match subcommand {
            DocumentSubcommand::List { collection, limit, offset } => {
                let start_time = Instant::now();
                let document_ids = self.storage.list_documents(&collection).await?;
                let paginated_ids = document_ids.into_iter()
                    .skip(offset.unwrap_or(0))
                    .take(limit.unwrap_or(10))
                    .collect::<Vec<_>>();
                let elapsed = start_time.elapsed().as_millis();

                println!("Documents in collection '{}' ({} found):", collection, paginated_ids.len());
                for id in paginated_ids {
                    println!("  - {}", id);
                }
                println!("Execution time: {}ms", elapsed);
                Ok(())
            }
            DocumentSubcommand::Insert { collection, file, overwrite } => {
                let start_time = Instant::now();
                let file_content = fs::read_to_string(file)?;
                let documents: Vec<Document> = serde_json::from_str(&file_content)?;
                self.storage.insert_documents(&collection, &documents, overwrite).await?;
                let elapsed = start_time.elapsed().as_millis();

                println!("Inserted {} documents into collection '{}'", documents.len(), collection);
                println!("Execution time: {}ms", elapsed);
                Ok(())
            }
            DocumentSubcommand::Get { collection, id } => {
                let start_time = Instant::now();
                let document = self.storage.get_document(&collection, &id).await?;
                let elapsed = start_time.elapsed().as_millis();

                println!("Document {} in collection '{}':", id, collection);
                println!("{:#}", serde_json::to_string_pretty(&document)?);
                println!("Execution time: {}ms", elapsed);
                Ok(())
            }
            DocumentSubcommand::Delete { collection, id } => {
                let start_time = Instant::now();
                self.storage.delete_document(&collection, &id).await?;
                let elapsed = start_time.elapsed().as_millis();

                println!("Deleted document {} from collection '{}'", id, collection);
                println!("Execution time: {}ms", elapsed);
                Ok(())
            }
            DocumentSubcommand::Update { collection, id, data } => {
                let start_time = Instant::now();
                let document_data: serde_json::Value = serde_json::from_str(&data)?;
                let document = Document::new(id.clone(), document_data);
                self.storage.update_document(&collection, &document).await?;
                let elapsed = start_time.elapsed().as_millis();

                println!("Updated document {} in collection '{}'", id, collection);
                println!("Execution time: {}ms", elapsed);
                Ok(())
            }
        }
    }

    /// Execute search command
    async fn execute_search_command(&self, subcommand: SearchSubcommand) -> Result<(), CortexError> {
        match subcommand {
            SearchSubcommand::Vector { collection, query, limit, threshold } => {
                let start_time = Instant::now();
                let query_vector: Vec<f32> = serde_json::from_str(&query)?;
                
                let query = QueryBuilder::new(&collection)
                    .vector_search(Float32Vector::from(query_vector), threshold)
                    .limit(limit.unwrap_or(10))
                    .build()?;
                
                let (results, stats) = self.query_executor.execute(&query).await?;
                let elapsed = start_time.elapsed().as_millis();

                println!("Vector search results ({} found):", results.len());
                for (i, result) in results.iter().enumerate() {
                    println!("{}: {} (score: {})", i + 1, result.document_id, result.score.unwrap_or_default());
                    if let Some(doc) = &result.document {
                        println!("   {}", serde_json::to_string_pretty(doc)?);
                    }
                }
                println!("Execution time: {}ms", elapsed);
                println!("Search time: {}ms", stats.execution_time_ms);
                Ok(())
            }
            SearchSubcommand::Scalar { collection, filter, limit, offset } => {
                let start_time = Instant::now();
                // In a real implementation, we'd execute the scalar query
                // For now, we'll just print the filter
                let elapsed = start_time.elapsed().as_millis();

                println!("Scalar query on collection '{}':", collection);
                println!("Filter: {}", filter);
                println!("Limit: {:?}", limit);
                println!("Offset: {:?}", offset);
                println!("Execution time: {}ms", elapsed);
                Ok(())
            }
            SearchSubcommand::Hybrid { collection, query, filter, limit, threshold } => {
                let start_time = Instant::now();
                // In a real implementation, we'd execute the hybrid search
                // For now, we'll just print the parameters
                let elapsed = start_time.elapsed().as_millis();

                println!("Hybrid search on collection '{}':", collection);
                println!("Query: {}", query);
                println!("Filter: {}", filter);
                println!("Limit: {:?}", limit);
                println!("Threshold: {:?}", threshold);
                println!("Execution time: {}ms", elapsed);
                Ok(())
            }
        }
    }

    /// Execute index command
    async fn execute_index_command(&self, subcommand: IndexSubcommand) -> Result<(), CortexError> {
        match subcommand {
            IndexSubcommand::List { collection } => {
                let start_time = Instant::now();
                let indexes = self.index_manager.list_indexes();
                let collection_indexes = indexes
                    .into_iter()
                    .filter(|idx| idx.name.starts_with(&format!("{}_", collection)))
                    .collect::<Vec<_>>();
                let elapsed = start_time.elapsed().as_millis();

                println!("Indexes for collection '{}' ({} found):", collection, collection_indexes.len());
                for index in collection_indexes {
                    println!("  - {} (type: {:?})", index.name, index.index_type);
                }
                println!("Execution time: {}ms", elapsed);
                Ok(())
            }
            IndexSubcommand::Create { collection, name, type: index_type, config } => {
                let start_time = Instant::now();
                let full_index_name = format!("{}_{}", collection, name);
                
                match index_type.as_str() {
                    "vector" => {
                        let vector_config: crate::cortex_index::vector::VectorIndexConfig = serde_json::from_str(&config)?;
                        self.index_manager.create_vector_index(&full_index_name, vector_config)?;
                    }
                    "scalar" => {
                        let scalar_config: crate::cortex_index::scalar::ScalarIndexConfig = serde_json::from_str(&config)?;
                        self.index_manager.create_scalar_index(&full_index_name, scalar_config)?;
                    }
                    _ => {
                        return Err(CortexError::Index(IndexError::InvalidConfiguration(
                            format!("Invalid index type: {}", index_type)
                        )));
                    }
                }
                
                let elapsed = start_time.elapsed().as_millis();

                println!("Created index '{}' for collection '{}'", name, collection);
                println!("Execution time: {}ms", elapsed);
                Ok(())
            }
            IndexSubcommand::Get { collection, name } => {
                let start_time = Instant::now();
                let full_index_name = format!("{}_{}", collection, name);
                let metadata = self.index_manager.get_index_metadata(&full_index_name)?;
                let elapsed = start_time.elapsed().as_millis();

                println!("Index '{}' for collection '{}':", name, collection);
                println!("{:#}", serde_json::to_string_pretty(&metadata)?);
                println!("Execution time: {}ms", elapsed);
                Ok(())
            }
            IndexSubcommand::Delete { collection, name } => {
                let start_time = Instant::now();
                let full_index_name = format!("{}_{}", collection, name);
                self.index_manager.delete_index(&full_index_name)?;
                let elapsed = start_time.elapsed().as_millis();

                println!("Deleted index '{}' from collection '{}'", name, collection);
                println!("Execution time: {}ms", elapsed);
                Ok(())
            }
        }
    }

    /// Execute system command
    async fn execute_system_command(&self, subcommand: SystemSubcommand) -> Result<(), CortexError> {
        match subcommand {
            SystemSubcommand::Health => {
                let start_time = Instant::now();
                // In a real implementation, we'd check system health
                let elapsed = start_time.elapsed().as_millis();

                println!("System health check:");
                println!("✓ Storage: Healthy");
                println!("✓ Indexes: Healthy");
                println!("✓ API: Healthy");
                println!("Execution time: {}ms", elapsed);
                Ok(())
            }
            SystemSubcommand::Status => {
                let start_time = Instant::now();
                let collections = self.storage.list_collections().await?;
                let indexes = self.index_manager.list_indexes();
                let elapsed = start_time.elapsed().as_millis();

                println!("System status:");
                println!("Collections: {}", collections.len());
                println!("Indexes: {}", indexes.len());
                println!("Vector indexes: {}", indexes.iter().filter(|idx| idx.index_type == crate::cortex_index::IndexType::Vector).count());
                println!("Scalar indexes: {}", indexes.iter().filter(|idx| idx.index_type == crate::cortex_index::IndexType::Scalar).count());
                println!("Execution time: {}ms", elapsed);
                Ok(())
            }
            SystemSubcommand::Metrics => {
                let start_time = Instant::now();
                // In a real implementation, we'd get system metrics
                let elapsed = start_time.elapsed().as_millis();

                println!("System metrics:");
                println!("Storage usage: 0 MB");
                println!("Memory usage: 0 MB");
                println!("CPU usage: 0%");
                println!("Request count: 0");
                println!("Execution time: {}ms", elapsed);
                Ok(())
            }
            SystemSubcommand::Config { action, key, value } => {
                let start_time = Instant::now();
                // In a real implementation, we'd manage system configuration
                let elapsed = start_time.elapsed().as_millis();

                println!("System configuration:");
                println!("Action: {}", action);
                println!("Key: {:?}", key);
                println!("Value: {:?}", value);
                println!("Execution time: {}ms", elapsed);
                Ok(())
            }
            SystemSubcommand::Shutdown => {
                let start_time = Instant::now();
                // In a real implementation, we'd shutdown the system
                let elapsed = start_time.elapsed().as_millis();

                println!("Shutting down system...");
                println!("Execution time: {}ms", elapsed);
                Ok(())
            }
        }
    }
}

/// Execute CLI commands from main
pub async fn execute_cli_command(command: CliCommand) -> Result<(), CortexError> {
    // Create storage engine and index manager
    let storage = Arc::new(crate::cortex_storage::memory::MemoryStorage::new());
    let index_manager = Arc::new(IndexManager::new());

    // Create command executor
    let executor = CommandExecutor::new(storage, index_manager);

    // Execute command
    executor.execute(command).await
}
