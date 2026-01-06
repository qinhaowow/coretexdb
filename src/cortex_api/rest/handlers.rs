//! Request handlers for CortexDB REST API.
//!
//! This module provides the request handling functions for the CortexDB REST API, including:
//! - Collection management handlers
//! - Document management handlers
//! - Search handlers
//! - Index management handlers
//! - System handlers
//! - Request validation and error handling

use crate::cortex_api::rest::server::{ApiErrorResponse, ApiServerState, ApiSuccessResponse};
use crate::cortex_core::error::{CortexError, ApiError, CollectionError, DocumentError, IndexError, QueryError, StorageError};
use crate::cortex_core::schema::{CollectionSchema, SchemaManager};
use crate::cortex_core::types::{Document, Embedding, Float32Vector};
use crate::cortex_index::{IndexManager, IndexType, ScalarIndexConfig, VectorIndexConfig};
use crate::cortex_query::{QueryBuilder, QueryExecutor};
use crate::cortex_storage::engine::StorageEngine;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Instant;

// ============================================================================
// Common request/response models
// ============================================================================

/// Pagination parameters
#[derive(Debug, Deserialize)]
pub struct PaginationParams {
    /// Page number
    pub page: Option<usize>,
    /// Items per page
    pub limit: Option<usize>,
}

/// Collection creation request
#[derive(Debug, Deserialize)]
pub struct CreateCollectionRequest {
    /// Collection name
    pub name: String,
    /// Collection schema
    pub schema: Option<CollectionSchema>,
    /// Collection description
    pub description: Option<String>,
    /// Collection settings
    pub settings: Option<serde_json::Value>,
}

/// Document insertion request
#[derive(Debug, Deserialize)]
pub struct InsertDocumentsRequest {
    /// Documents to insert
    pub documents: Vec<Document>,
    /// Whether to overwrite existing documents
    pub overwrite: Option<bool>,
}

/// Batch document update request
#[derive(Debug, Deserialize)]
pub struct BatchUpdateDocumentsRequest {
    /// Documents to update
    pub documents: Vec<Document>,
    /// Update mode (insert, update, upsert)
    pub mode: Option<String>,
}

/// Vector search request
#[derive(Debug, Deserialize)]
pub struct VectorSearchRequest {
    /// Query vector
    pub query: Vec<f32>,
    /// Limit of results
    pub limit: Option<usize>,
    /// Distance threshold
    pub threshold: Option<f32>,
    /// Include documents in results
    pub include_documents: Option<bool>,
    /// Additional search parameters
    pub params: Option<serde_json::Value>,
}

/// Scalar query request
#[derive(Debug, Deserialize)]
pub struct ScalarQueryRequest {
    /// Filter conditions
    pub filter: serde_json::Value,
    /// Limit of results
    pub limit: Option<usize>,
    /// Offset of results
    pub offset: Option<usize>,
    /// Sort field
    pub sort: Option<String>,
    /// Sort order
    pub sort_order: Option<String>,
    /// Include documents in results
    pub include_documents: Option<bool>,
}

/// Index creation request
#[derive(Debug, Deserialize)]
pub struct CreateIndexRequest {
    /// Index name
    pub name: String,
    /// Index type
    pub type: String,
    /// Index configuration
    pub config: serde_json::Value,
}

// ============================================================================
// Collection handlers
// ============================================================================

/// Collection handlers module
pub mod collection_handlers {
    use super::*;

    /// List all collections
    pub async fn list_collections(
        State(state): State<Arc<ApiServerState>>,
    ) -> impl axum::response::IntoResponse {
        let start_time = Instant::now();

        match state.storage.list_collections().await {
            Ok(collections) => {
                let elapsed = start_time.elapsed().as_millis() as f64;
                let response = ApiSuccessResponse::new(
                    collections,
                    Some(serde_json::json!({
                        "count": collections.len(),
                        "time_ms": elapsed,
                    })),
                );
                response.into_response()
            }
            Err(error) => {
                let api_error = ApiErrorResponse::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    error.to_string(),
                    "INTERNAL_ERROR".to_string(),
                    None,
                );
                api_error.into_response()
            }
        }
    }

    /// Create a new collection
    pub async fn create_collection(
        State(state): State<Arc<ApiServerState>>,
        Json(request): Json<CreateCollectionRequest>,
    ) -> impl axum::response::IntoResponse {
        let start_time = Instant::now();

        match state.storage.create_collection(&request.name, request.schema, request.settings).await {
            Ok(collection) => {
                let elapsed = start_time.elapsed().as_millis() as f64;
                let response = ApiSuccessResponse::new(
                    collection,
                    Some(serde_json::json!({
                        "time_ms": elapsed,
                    })),
                );
                response.into_response()
            }
            Err(error) => {
                let status = match error {
                    CortexError::Collection(CollectionError::CollectionAlreadyExists(_)) => StatusCode::CONFLICT,
                    CortexError::Collection(CollectionError::InvalidCollectionName(_)) => StatusCode::BAD_REQUEST,
                    _ => StatusCode::INTERNAL_SERVER_ERROR,
                };
                let api_error = ApiErrorResponse::new(
                    status,
                    error.to_string(),
                    "COLLECTION_ERROR".to_string(),
                    None,
                );
                api_error.into_response()
            }
        }
    }

    /// Get collection details
    pub async fn get_collection(
        State(state): State<Arc<ApiServerState>>,
        Path(params): Path<HashMap<String, String>>,
    ) -> impl axum::response::IntoResponse {
        let start_time = Instant::now();
        let name = params.get("name").unwrap();

        match state.storage.get_collection(name).await {
            Ok(collection) => {
                let elapsed = start_time.elapsed().as_millis() as f64;
                let response = ApiSuccessResponse::new(
                    collection,
                    Some(serde_json::json!({
                        "time_ms": elapsed,
                    })),
                );
                response.into_response()
            }
            Err(error) => {
                let status = match error {
                    CortexError::Collection(CollectionError::CollectionNotFound(_)) => StatusCode::NOT_FOUND,
                    _ => StatusCode::INTERNAL_SERVER_ERROR,
                };
                let api_error = ApiErrorResponse::new(
                    status,
                    error.to_string(),
                    "COLLECTION_ERROR".to_string(),
                    None,
                );
                api_error.into_response()
            }
        }
    }

    /// Delete a collection
    pub async fn delete_collection(
        State(state): State<Arc<ApiServerState>>,
        Path(params): Path<HashMap<String, String>>,
    ) -> impl axum::response::IntoResponse {
        let start_time = Instant::now();
        let name = params.get("name").unwrap();

        match state.storage.delete_collection(name).await {
            Ok(success) => {
                let elapsed = start_time.elapsed().as_millis() as f64;
                let response = ApiSuccessResponse::new(
                    serde_json::json!({ "deleted": success }),
                    Some(serde_json::json!({
                        "time_ms": elapsed,
                    })),
                );
                response.into_response()
            }
            Err(error) => {
                let status = match error {
                    CortexError::Collection(CollectionError::CollectionNotFound(_)) => StatusCode::NOT_FOUND,
                    _ => StatusCode::INTERNAL_SERVER_ERROR,
                };
                let api_error = ApiErrorResponse::new(
                    status,
                    error.to_string(),
                    "COLLECTION_ERROR".to_string(),
                    None,
                );
                api_error.into_response()
            }
        }
    }

    /// Update a collection
    pub async fn update_collection(
        State(state): State<Arc<ApiServerState>>,
        Path(params): Path<HashMap<String, String>>,
        Json(request): Json<serde_json::Value>,
    ) -> impl axum::response::IntoResponse {
        let start_time = Instant::now();
        let name = params.get("name").unwrap();

        match state.storage.update_collection(name, request).await {
            Ok(collection) => {
                let elapsed = start_time.elapsed().as_millis() as f64;
                let response = ApiSuccessResponse::new(
                    collection,
                    Some(serde_json::json!({
                        "time_ms": elapsed,
                    })),
                );
                response.into_response()
            }
            Err(error) => {
                let status = match error {
                    CortexError::Collection(CollectionError::CollectionNotFound(_)) => StatusCode::NOT_FOUND,
                    _ => StatusCode::INTERNAL_SERVER_ERROR,
                };
                let api_error = ApiErrorResponse::new(
                    status,
                    error.to_string(),
                    "COLLECTION_ERROR".to_string(),
                    None,
                );
                api_error.into_response()
            }
        }
    }
}

// ============================================================================
// Document handlers
// ============================================================================

/// Document handlers module
pub mod document_handlers {
    use super::*;

    /// List documents in a collection
    pub async fn list_documents(
        State(state): State<Arc<ApiServerState>>,
        Path(params): Path<HashMap<String, String>>,
        Query(pagination): Query<PaginationParams>,
    ) -> impl axum::response::IntoResponse {
        let start_time = Instant::now();
        let name = params.get("name").unwrap();
        let limit = pagination.limit.unwrap_or(10);
        let offset = pagination.page.map(|p| (p - 1) * limit).unwrap_or(0);

        match state.storage.list_documents(name).await {
            Ok(document_ids) => {
                let paginated_ids = document_ids.into_iter().skip(offset).take(limit).collect::<Vec<_>>();
                let elapsed = start_time.elapsed().as_millis() as f64;
                let response = ApiSuccessResponse::new(
                    paginated_ids,
                    Some(serde_json::json!({
                        "count": paginated_ids.len(),
                        "limit": limit,
                        "offset": offset,
                        "time_ms": elapsed,
                    })),
                );
                response.into_response()
            }
            Err(error) => {
                let status = match error {
                    CortexError::Collection(CollectionError::CollectionNotFound(_)) => StatusCode::NOT_FOUND,
                    _ => StatusCode::INTERNAL_SERVER_ERROR,
                };
                let api_error = ApiErrorResponse::new(
                    status,
                    error.to_string(),
                    "DOCUMENT_ERROR".to_string(),
                    None,
                );
                api_error.into_response()
            }
        }
    }

    /// Insert documents into a collection
    pub async fn insert_documents(
        State(state): State<Arc<ApiServerState>>,
        Path(params): Path<HashMap<String, String>>,
        Json(request): Json<InsertDocumentsRequest>,
    ) -> impl axum::response::IntoResponse {
        let start_time = Instant::now();
        let name = params.get("name").unwrap();
        let overwrite = request.overwrite.unwrap_or(false);

        match state.storage.insert_documents(name, &request.documents, overwrite).await {
            Ok(inserted) => {
                let elapsed = start_time.elapsed().as_millis() as f64;
                let response = ApiSuccessResponse::new(
                    serde_json::json!({ "inserted": inserted }),
                    Some(serde_json::json!({
                        "count": inserted,
                        "time_ms": elapsed,
                    })),
                );
                response.into_response()
            }
            Err(error) => {
                let status = match error {
                    CortexError::Collection(CollectionError::CollectionNotFound(_)) => StatusCode::NOT_FOUND,
                    CortexError::Document(DocumentError::InvalidDocument(_)) => StatusCode::BAD_REQUEST,
                    _ => StatusCode::INTERNAL_SERVER_ERROR,
                };
                let api_error = ApiErrorResponse::new(
                    status,
                    error.to_string(),
                    "DOCUMENT_ERROR".to_string(),
                    None,
                );
                api_error.into_response()
            }
        }
    }

    /// Get a document by ID
    pub async fn get_document(
        State(state): State<Arc<ApiServerState>>,
        Path(params): Path<HashMap<String, String>>,
    ) -> impl axum::response::IntoResponse {
        let start_time = Instant::now();
        let name = params.get("name").unwrap();
        let id = params.get("id").unwrap();

        match state.storage.get_document(name, id).await {
            Ok(document) => {
                let elapsed = start_time.elapsed().as_millis() as f64;
                let response = ApiSuccessResponse::new(
                    document,
                    Some(serde_json::json!({
                        "time_ms": elapsed,
                    })),
                );
                response.into_response()
            }
            Err(error) => {
                let status = match error {
                    CortexError::Collection(CollectionError::CollectionNotFound(_)) => StatusCode::NOT_FOUND,
                    CortexError::Document(DocumentError::DocumentNotFound(_)) => StatusCode::NOT_FOUND,
                    _ => StatusCode::INTERNAL_SERVER_ERROR,
                };
                let api_error = ApiErrorResponse::new(
                    status,
                    error.to_string(),
                    "DOCUMENT_ERROR".to_string(),
                    None,
                );
                api_error.into_response()
            }
        }
    }

    /// Update a document
    pub async fn update_document(
        State(state): State<Arc<ApiServerState>>,
        Path(params): Path<HashMap<String, String>>,
        Json(document): Json<Document>,
    ) -> impl axum::response::IntoResponse {
        let start_time = Instant::now();
        let name = params.get("name").unwrap();
        let id = params.get("id").unwrap();

        // Ensure the document ID matches the path parameter
        if document.id() != id {
            let api_error = ApiErrorResponse::new(
                StatusCode::BAD_REQUEST,
                "Document ID in path must match document ID in body".to_string(),
                "INVALID_REQUEST".to_string(),
                None,
            );
            return api_error.into_response();
        }

        match state.storage.update_document(name, &document).await {
            Ok(updated) => {
                let elapsed = start_time.elapsed().as_millis() as f64;
                let response = ApiSuccessResponse::new(
                    serde_json::json!({ "updated": updated }),
                    Some(serde_json::json!({
                        "time_ms": elapsed,
                    })),
                );
                response.into_response()
            }
            Err(error) => {
                let status = match error {
                    CortexError::Collection(CollectionError::CollectionNotFound(_)) => StatusCode::NOT_FOUND,
                    CortexError::Document(DocumentError::DocumentNotFound(_)) => StatusCode::NOT_FOUND,
                    _ => StatusCode::INTERNAL_SERVER_ERROR,
                };
                let api_error = ApiErrorResponse::new(
                    status,
                    error.to_string(),
                    "DOCUMENT_ERROR".to_string(),
                    None,
                );
                api_error.into_response()
            }
        }
    }

    /// Delete a document
    pub async fn delete_document(
        State(state): State<Arc<ApiServerState>>,
        Path(params): Path<HashMap<String, String>>,
    ) -> impl axum::response::IntoResponse {
        let start_time = Instant::now();
        let name = params.get("name").unwrap();
        let id = params.get("id").unwrap();

        match state.storage.delete_document(name, id).await {
            Ok(deleted) => {
                let elapsed = start_time.elapsed().as_millis() as f64;
                let response = ApiSuccessResponse::new(
                    serde_json::json!({ "deleted": deleted }),
                    Some(serde_json::json!({
                        "time_ms": elapsed,
                    })),
                );
                response.into_response()
            }
            Err(error) => {
                let status = match error {
                    CortexError::Collection(CollectionError::CollectionNotFound(_)) => StatusCode::NOT_FOUND,
                    _ => StatusCode::INTERNAL_SERVER_ERROR,
                };
                let api_error = ApiErrorResponse::new(
                    status,
                    error.to_string(),
                    "DOCUMENT_ERROR".to_string(),
                    None,
                );
                api_error.into_response()
            }
        }
    }

    /// Batch update documents
    pub async fn batch_update_documents(
        State(state): State<Arc<ApiServerState>>,
        Path(params): Path<HashMap<String, String>>,
        Json(request): Json<BatchUpdateDocumentsRequest>,
    ) -> impl axum::response::IntoResponse {
        let start_time = Instant::now();
        let name = params.get("name").unwrap();
        let mode = request.mode.unwrap_or_else(|| "upsert".to_string());

        let result = match mode.as_str() {
            "insert" => state.storage.insert_documents(name, &request.documents, false).await,
            "update" => {
                let mut updated = 0;
                for doc in &request.documents {
                    if state.storage.update_document(name, doc).await.is_ok() {
                        updated += 1;
                    }
                }
                Ok(updated)
            }
            "upsert" => state.storage.insert_documents(name, &request.documents, true).await,
            _ => Err(CortexError::Api(ApiError::InvalidRequest(
                "Invalid update mode. Must be one of: insert, update, upsert".to_string(),
            ))),
        };

        match result {
            Ok(updated) => {
                let elapsed = start_time.elapsed().as_millis() as f64;
                let response = ApiSuccessResponse::new(
                    serde_json::json!({ "updated": updated }),
                    Some(serde_json::json!({
                        "count": updated,
                        "mode": mode,
                        "time_ms": elapsed,
                    })),
                );
                response.into_response()
            }
            Err(error) => {
                let status = match error {
                    CortexError::Collection(CollectionError::CollectionNotFound(_)) => StatusCode::NOT_FOUND,
                    _ => StatusCode::INTERNAL_SERVER_ERROR,
                };
                let api_error = ApiErrorResponse::new(
                    status,
                    error.to_string(),
                    "DOCUMENT_ERROR".to_string(),
                    None,
                );
                api_error.into_response()
            }
        }
    }
}

// ============================================================================
// Search handlers
// ============================================================================

/// Search handlers module
pub mod search_handlers {
    use super::*;

    /// Perform vector search
    pub async fn vector_search(
        State(state): State<Arc<ApiServerState>>,
        Path(params): Path<HashMap<String, String>>,
        Json(request): Json<VectorSearchRequest>,
    ) -> impl axum::response::IntoResponse {
        let start_time = Instant::now();
        let name = params.get("name").unwrap();
        let limit = request.limit.unwrap_or(10);
        let threshold = request.threshold;
        let include_documents = request.include_documents.unwrap_or(true);

        // Create query
        let query = QueryBuilder::new(name)
            .vector_search(Float32Vector::from(request.query), threshold)
            .limit(limit)
            .include_documents(include_documents)
            .build();

        match query {
            Ok(query) => {
                // Create query executor
                let executor = QueryExecutor::new(state.storage.clone(), state.index_manager.clone());
                
                // Execute query
                match executor.execute(&query).await {
                    Ok((results, stats)) => {
                        let elapsed = start_time.elapsed().as_millis() as f64;
                        let response = ApiSuccessResponse::new(
                            results,
                            Some(serde_json::json!({
                                "count": results.len(),
                                "execution_time_ms": stats.execution_time_ms,
                                "time_ms": elapsed,
                                "stats": stats,
                            })),
                        );
                        response.into_response()
                    }
                    Err(error) => {
                        let api_error = ApiErrorResponse::new(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            error.to_string(),
                            "SEARCH_ERROR".to_string(),
                            None,
                        );
                        api_error.into_response()
                    }
                }
            }
            Err(error) => {
                let api_error = ApiErrorResponse::new(
                    StatusCode::BAD_REQUEST,
                    error.to_string(),
                    "INVALID_QUERY".to_string(),
                    None,
                );
                api_error.into_response()
            }
        }
    }

    /// Perform scalar query
    pub async fn scalar_query(
        State(state): State<Arc<ApiServerState>>,
        Path(params): Path<HashMap<String, String>>,
        Json(request): Json<ScalarQueryRequest>,
    ) -> impl axum::response::IntoResponse {
        let start_time = Instant::now();
        let name = params.get("name").unwrap();
        let limit = request.limit.unwrap_or(10);
        let offset = request.offset.unwrap_or(0);
        let include_documents = request.include_documents.unwrap_or(true);

        // For now, we'll return a placeholder response
        // In a real implementation, we'd parse the filter and execute the query
        let response = ApiSuccessResponse::new(
            vec![],
            Some(serde_json::json!({
                "count": 0,
                "limit": limit,
                "offset": offset,
                "time_ms": start_time.elapsed().as_millis() as f64,
            })),
        );
        response.into_response()
    }

    /// Perform hybrid search (vector + scalar)
    pub async fn hybrid_search(
        State(state): State<Arc<ApiServerState>>,
        Path(params): Path<HashMap<String, String>>,
        Json(request): Json<serde_json::Value>,
    ) -> impl axum::response::IntoResponse {
        let start_time = Instant::now();
        let name = params.get("name").unwrap();

        // For now, we'll return a placeholder response
        // In a real implementation, we'd execute a hybrid search
        let response = ApiSuccessResponse::new(
            vec![],
            Some(serde_json::json!({
                "count": 0,
                "time_ms": start_time.elapsed().as_millis() as f64,
            })),
        );
        response.into_response()
    }
}

// ============================================================================
// Index handlers
// ============================================================================

/// Index handlers module
pub mod index_handlers {
    use super::*;
    use crate::cortex_index::scalar::ScalarIndexType;

    /// List indexes for a collection
    pub async fn list_indexes(
        State(state): State<Arc<ApiServerState>>,
        Path(params): Path<HashMap<String, String>>,
    ) -> impl axum::response::IntoResponse {
        let start_time = Instant::now();
        let name = params.get("name").unwrap();

        // List indexes for the collection
        let indexes = state.index_manager.list_indexes();
        let collection_indexes = indexes
            .into_iter()
            .filter(|idx| idx.name.starts_with(&format!("{}_", name)))
            .collect::<Vec<_>>();

        let elapsed = start_time.elapsed().as_millis() as f64;
        let response = ApiSuccessResponse::new(
            collection_indexes,
            Some(serde_json::json!({
                "count": collection_indexes.len(),
                "time_ms": elapsed,
            })),
        );
        response.into_response()
    }

    /// Create an index
    pub async fn create_index(
        State(state): State<Arc<ApiServerState>>,
        Path(params): Path<HashMap<String, String>>,
        Json(request): Json<CreateIndexRequest>,
    ) -> impl axum::response::IntoResponse {
        let start_time = Instant::now();
        let collection_name = params.get("name").unwrap();
        let index_name = format!("{}_{}", collection_name, request.name);

        match request.type.as_str() {
            "vector" => {
                // Create vector index
                let config: VectorIndexConfig = serde_json::from_value(request.config)
                    .map_err(|e| CortexError::Index(IndexError::InvalidConfiguration(e.to_string())))?;
                
                match state.index_manager.create_vector_index(&index_name, config) {
                    Ok(index) => {
                        let elapsed = start_time.elapsed().as_millis() as f64;
                        let response = ApiSuccessResponse::new(
                            serde_json::json!({ "created": true, "name": index_name }),
                            Some(serde_json::json!({
                                "time_ms": elapsed,
                            })),
                        );
                        response.into_response()
                    }
                    Err(error) => {
                        let api_error = ApiErrorResponse::new(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            error.to_string(),
                            "INDEX_ERROR".to_string(),
                            None,
                        );
                        api_error.into_response()
                    }
                }
            }
            "scalar" => {
                // Create scalar index
                let config: ScalarIndexConfig = serde_json::from_value(request.config)
                    .map_err(|e| CortexError::Index(IndexError::InvalidConfiguration(e.to_string())))?;
                
                match state.index_manager.create_scalar_index(&index_name, config) {
                    Ok(index) => {
                        let elapsed = start_time.elapsed().as_millis() as f64;
                        let response = ApiSuccessResponse::new(
                            serde_json::json!({ "created": true, "name": index_name }),
                            Some(serde_json::json!({
                                "time_ms": elapsed,
                            })),
                        );
                        response.into_response()
                    }
                    Err(error) => {
                        let api_error = ApiErrorResponse::new(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            error.to_string(),
                            "INDEX_ERROR".to_string(),
                            None,
                        );
                        api_error.into_response()
                    }
                }
            }
            _ => {
                let api_error = ApiErrorResponse::new(
                    StatusCode::BAD_REQUEST,
                    "Invalid index type. Must be 'vector' or 'scalar'".to_string(),
                    "INVALID_INDEX_TYPE".to_string(),
                    None,
                );
                api_error.into_response()
            }
        }
    }

    /// Get index details
    pub async fn get_index(
        State(state): State<Arc<ApiServerState>>,
        Path(params): Path<HashMap<String, String>>,
    ) -> impl axum::response::IntoResponse {
        let start_time = Instant::now();
        let collection_name = params.get("name").unwrap();
        let index_name = params.get("name").unwrap();
        let full_index_name = format!("{}_{}", collection_name, index_name);

        match state.index_manager.get_index_metadata(&full_index_name) {
            Ok(metadata) => {
                let elapsed = start_time.elapsed().as_millis() as f64;
                let response = ApiSuccessResponse::new(
                    metadata,
                    Some(serde_json::json!({
                        "time_ms": elapsed,
                    })),
                );
                response.into_response()
            }
            Err(error) => {
                let api_error = ApiErrorResponse::new(
                    StatusCode::NOT_FOUND,
                    error.to_string(),
                    "INDEX_NOT_FOUND".to_string(),
                    None,
                );
                api_error.into_response()
            }
        }
    }

    /// Delete an index
    pub async fn delete_index(
        State(state): State<Arc<ApiServerState>>,
        Path(params): Path<HashMap<String, String>>,
    ) -> impl axum::response::IntoResponse {
        let start_time = Instant::now();
        let collection_name = params.get("name").unwrap();
        let index_name = params.get("name").unwrap();
        let full_index_name = format!("{}_{}", collection_name, index_name);

        match state.index_manager.delete_index(&full_index_name) {
            Ok(deleted) => {
                let elapsed = start_time.elapsed().as_millis() as f64;
                let response = ApiSuccessResponse::new(
                    serde_json::json!({ "deleted": deleted }),
                    Some(serde_json::json!({
                        "time_ms": elapsed,
                    })),
                );
                response.into_response()
            }
            Err(error) => {
                let api_error = ApiErrorResponse::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    error.to_string(),
                    "INDEX_ERROR".to_string(),
                    None,
                );
                api_error.into_response()
            }
        }
    }

    /// Update an index
    pub async fn update_index(
        State(state): State<Arc<ApiServerState>>,
        Path(params): Path<HashMap<String, String>>,
        Json(request): Json<serde_json::Value>,
    ) -> impl axum::response::IntoResponse {
        let start_time = Instant::now();
        let collection_name = params.get("name").unwrap();
        let index_name = params.get("name").unwrap();
        let full_index_name = format!("{}_{}", collection_name, index_name);

        match state.index_manager.update_index(&full_index_name, request) {
            Ok(_) => {
                let elapsed = start_time.elapsed().as_millis() as f64;
                let response = ApiSuccessResponse::new(
                    serde_json::json!({ "updated": true }),
                    Some(serde_json::json!({
                        "time_ms": elapsed,
                    })),
                );
                response.into_response()
            }
            Err(error) => {
                let api_error = ApiErrorResponse::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    error.to_string(),
                    "INDEX_ERROR".to_string(),
                    None,
                );
                api_error.into_response()
            }
        }
    }
}

// ============================================================================
// System handlers
// ============================================================================

/// System handlers module
pub mod system_handlers {
    use super::*;

    /// Health check endpoint
    pub async fn health_check() -> impl axum::response::IntoResponse {
        let response = ApiSuccessResponse::new(
            serde_json::json!({ "status": "healthy" }),
            Some(serde_json::json!({
                "timestamp": chrono::Utc::now().to_rfc3339(),
            })),
        );
        response.into_response()
    }

    /// Ready check endpoint
    pub async fn ready_check(
        State(state): State<Arc<ApiServerState>>,
    ) -> impl axum::response::IntoResponse {
        // Check if storage is ready
        let storage_ready = true; // In a real implementation, we'd check the storage status
        
        if storage_ready {
            let response = ApiSuccessResponse::new(
                serde_json::json!({ "status": "ready" }),
                Some(serde_json::json!({
                    "timestamp": chrono::Utc::now().to_rfc3339(),
                })),
            );
            response.into_response()
        } else {
            let api_error = ApiErrorResponse::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "Service not ready".to_string(),
                "SERVICE_UNAVAILABLE".to_string(),
                None,
            );
            api_error.into_response()
        }
    }

    /// Metrics endpoint
    pub async fn metrics() -> impl axum::response::IntoResponse {
        // In a real implementation, we'd expose Prometheus metrics
        let response = ApiSuccessResponse::new(
            serde_json::json!({ "metrics": {} }),
            Some(serde_json::json!({
                "timestamp": chrono::Utc::now().to_rfc3339(),
            })),
        );
        response.into_response()
    }

    /// System stats endpoint
    pub async fn system_stats(
        State(state): State<Arc<ApiServerState>>,
    ) -> impl axum::response::IntoResponse {
        let start_time = Instant::now();

        // Get storage stats
        let storage_stats = serde_json::json!({
            "type": "memory", // In a real implementation, we'd get the actual storage type
            "collections": state.storage.list_collections().await.unwrap_or_default().len(),
        });

        // Get index stats
        let indexes = state.index_manager.list_indexes();
        let index_stats = serde_json::json!({
            "count": indexes.len(),
            "vector_indexes": indexes.iter().filter(|idx| idx.index_type == IndexType::Vector).count(),
            "scalar_indexes": indexes.iter().filter(|idx| idx.index_type == IndexType::Scalar).count(),
        });

        let elapsed = start_time.elapsed().as_millis() as f64;
        let response = ApiSuccessResponse::new(
            serde_json::json!({
                "storage": storage_stats,
                "indexes": index_stats,
                "system": serde_json::json!({
                    "rust_version": env!("CARGO_PKG_VERSION"),
                    "timestamp": chrono::Utc::now().to_rfc3339(),
                }),
            }),
            Some(serde_json::json!({
                "time_ms": elapsed,
            })),
        );
        response.into_response()
    }

    /// Get system configuration
    pub async fn get_config(
        State(state): State<Arc<ApiServerState>>,
    ) -> impl axum::response::IntoResponse {
        let response = ApiSuccessResponse::new(
            state.config.clone(),
            None,
        );
        response.into_response()
    }

    /// Update system configuration
    pub async fn update_config(
        State(state): State<Arc<ApiServerState>>,
        Json(request): Json<serde_json::Value>,
    ) -> impl axum::response::IntoResponse {
        // In a real implementation, we'd update the configuration
        let response = ApiSuccessResponse::new(
            serde_json::json!({ "updated": true }),
            None,
        );
        response.into_response()
    }

    /// Shutdown the server
    pub async fn shutdown() -> impl axum::response::IntoResponse {
        // In a real implementation, we'd trigger a graceful shutdown
        let response = ApiSuccessResponse::new(
            serde_json::json!({ "shutdown": true }),
            None,
        );
        response.into_response()
    }
}

// ============================================================================
// Middleware handlers
// ============================================================================

/// Middleware handlers module
pub mod middleware_handlers {
    use super::*;

    // These are placeholder functions - the actual middleware is implemented in middleware.rs
}
