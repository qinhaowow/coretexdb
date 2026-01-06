//! Route definitions for CortexDB REST API.
//!
//! This module provides the route setup and configuration for the CortexDB REST API, including:
//! - Collection management routes
//! - Document management routes
//! - Vector search routes
//! - Index management routes
//! - Health check routes
//! - Middleware application

use crate::cortex_api::rest::handlers::{
    collection_handlers, document_handlers, index_handlers, search_handlers, system_handlers,
};
use crate::cortex_api::rest::middleware::{
    auth_middleware, cors_middleware, logging_middleware, metrics_middleware,
};
use crate::cortex_api::rest::server::ApiServerState;
use axum::middleware;
use axum::Router;
use std::sync::Arc;

/// Setup API routes
pub fn setup_routes(state: Arc<ApiServerState>) -> Router {
    // Create base router with state
    let app = Router::new()
        .with_state(state)
        // Apply global middleware
        .layer(middleware::from_fn(logging_middleware))
        .layer(middleware::from_fn(metrics_middleware))
        .layer(cors_middleware());

    // Health check routes
    let health_routes = Router::new()
        .route("/health", axum::routing::get(system_handlers::health_check))
        .route("/ready", axum::routing::get(system_handlers::ready_check))
        .route("/metrics", axum::routing::get(system_handlers::metrics));

    // Collection routes
    let collection_routes = Router::new()
        .route("/collections", axum::routing::get(collection_handlers::list_collections))
        .route("/collections", axum::routing::post(collection_handlers::create_collection))
        .route("/collections/:name", axum::routing::get(collection_handlers::get_collection))
        .route("/collections/:name", axum::routing::delete(collection_handlers::delete_collection))
        .route("/collections/:name", axum::routing::put(collection_handlers::update_collection));

    // Document routes
    let document_routes = Router::new()
        .route("/collections/:name/documents", axum::routing::get(document_handlers::list_documents))
        .route("/collections/:name/documents", axum::routing::post(document_handlers::insert_documents))
        .route("/collections/:name/documents/:id", axum::routing::get(document_handlers::get_document))
        .route("/collections/:name/documents/:id", axum::routing::put(document_handlers::update_document))
        .route("/collections/:name/documents/:id", axum::routing::delete(document_handlers::delete_document))
        .route("/collections/:name/documents/batch", axum::routing::post(document_handlers::batch_update_documents));

    // Search routes
    let search_routes = Router::new()
        .route("/collections/:name/search", axum::routing::post(search_handlers::vector_search))
        .route("/collections/:name/query", axum::routing::post(search_handlers::scalar_query))
        .route("/collections/:name/hybrid", axum::routing::post(search_handlers::hybrid_search));

    // Index routes
    let index_routes = Router::new()
        .route("/collections/:name/indexes", axum::routing::get(index_handlers::list_indexes))
        .route("/collections/:name/indexes", axum::routing::post(index_handlers::create_index))
        .route("/collections/:name/indexes/:name", axum::routing::get(index_handlers::get_index))
        .route("/collections/:name/indexes/:name", axum::routing::delete(index_handlers::delete_index))
        .route("/collections/:name/indexes/:name", axum::routing::put(index_handlers::update_index));

    // System routes
    let system_routes = Router::new()
        .route("/system/stats", axum::routing::get(system_handlers::system_stats))
        .route("/system/config", axum::routing::get(system_handlers::get_config))
        .route("/system/config", axum::routing::put(system_handlers::update_config))
        .route("/system/shutdown", axum::routing::post(system_handlers::shutdown));

    // Assemble all routes
    app
        .merge(health_routes)
        .merge(collection_routes)
        .merge(document_routes)
        .merge(search_routes)
        .merge(index_routes)
        .merge(system_routes)
}

/// Test utilities for routes
#[cfg(test)]
mod tests {
    use super::*;
    use crate::cortex_core::config::ConfigManager;
    use crate::cortex_index::IndexManager;
    use crate::cortex_storage::memory::MemoryStorage;

    #[test]
    fn test_setup_routes() {
        let storage = Arc::new(MemoryStorage::new());
        let index_manager = Arc::new(IndexManager::new());
        let config = ConfigManager::load_default().unwrap();

        let state = Arc::new(ApiServerState {
            storage,
            index_manager,
            config: Arc::new(config),
        });

        let router = setup_routes(state);
        // Just verify the router is created without errors
        assert!(!router.is_null());
    }
}
