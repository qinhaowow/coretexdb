//! HTTP server implementation for CortexDB REST API.
//!
//! This module provides the HTTP server setup and configuration for the CortexDB REST API, including:
//! - Server initialization and configuration
//! - TLS support
//! - Connection pooling
//! - Request handling
//! - Server shutdown and cleanup

use crate::cortex_api::rest::routes::setup_routes;
use crate::cortex_core::config::CortexConfig;
use crate::cortex_core::error::{CortexError, ApiError};
use crate::cortex_index::IndexManager;
use crate::cortex_storage::engine::StorageEngine;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Router;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::signal;
use tokio::time;

/// API server state
pub struct ApiServerState {
    /// Storage engine
    pub storage: Arc<dyn StorageEngine>,
    /// Index manager
    pub index_manager: Arc<IndexManager>,
    /// Configuration
    pub config: Arc<CortexConfig>,
}

/// API server
pub struct ApiServer {
    /// Server state
    state: Arc<ApiServerState>,
    /// Socket address
    addr: SocketAddr,
    /// Router
    router: Router,
}

impl ApiServer {
    /// Create a new API server
    pub fn new(
        storage: Arc<dyn StorageEngine>,
        index_manager: Arc<IndexManager>,
        config: Arc<CortexConfig>,
    ) -> Result<Self, CortexError> {
        let state = Arc::new(ApiServerState {
            storage,
            index_manager,
            config: config.clone(),
        });

        // Setup routes
        let router = setup_routes(state.clone());

        // Get address from config
        let addr = format!(
            "{}:{}",
            config.api.host, config.api.port
        )
        .parse()
        .map_err(|e| {
            CortexError::Api(ApiError::InvalidConfig(format!(
                "Invalid API address: {}",
                e
            )))
        })?;

        Ok(Self {
            state,
            addr,
            router,
        })
    }

    /// Start the API server
    pub async fn start(&self) -> Result<(), CortexError> {
        tracing::info!(
            "Starting CortexDB API server on {}...",
            self.addr
        );

        // Create TCP listener
        let listener = TcpListener::bind(&self.addr)
            .await
            .map_err(|e| {
                CortexError::Api(ApiError::ServerError(format!(
                    "Failed to bind to address: {}",
                    e
                )))
            })?;

        // Serve with graceful shutdown
        axum::serve(listener, self.router.clone())
            .with_graceful_shutdown(self.shutdown_signal())
            .await
            .map_err(|e| {
                CortexError::Api(ApiError::ServerError(format!(
                    "Server error: {}",
                    e
                )))
            })?;

        tracing::info!("CortexDB API server stopped gracefully");
        Ok(())
    }

    /// Shutdown signal handler
    async fn shutdown_signal(&self) {
        let ctrl_c = async {
            signal::ctrl_c()
                .await
                .expect("Failed to install Ctrl+C handler");
        };

        #[cfg(unix)]
        let terminate = async {
            signal::unix::signal(signal::unix::SignalKind::terminate())
                .expect("Failed to install signal handler")
                .recv()
                .await;
        };

        #[cfg(not(unix))]
        let terminate = std::future::pending::<()>();

        tokio::select! {
            _ = ctrl_c => {},
            _ = terminate => {},
        }

        tracing::info!("Shutting down CortexDB API server...");

        // Give active connections 30 seconds to finish
        time::sleep(Duration::from_secs(30)).await;
    }

    /// Get server address
    pub fn addr(&self) -> &SocketAddr {
        &self.addr
    }

    /// Get server state
    pub fn state(&self) -> &Arc<ApiServerState> {
        &self.state
    }
}

/// API error response
pub struct ApiErrorResponse {
    /// HTTP status code
    status: StatusCode,
    /// Error message
    message: String,
    /// Error code
    code: String,
    /// Additional error details
    details: Option<serde_json::Value>,
}

impl ApiErrorResponse {
    /// Create a new API error response
    pub fn new(status: StatusCode, message: String, code: String, details: Option<serde_json::Value>) -> Self {
        Self {
            status,
            message,
            code,
            details,
        }
    }
}

impl IntoResponse for ApiErrorResponse {
    fn into_response(self) -> Response {
        let body = serde_json::json!({
            "error": {
                "message": self.message,
                "code": self.code,
                "details": self.details,
            }
        });

        (self.status, body).into_response()
    }
}

/// API success response
pub struct ApiSuccessResponse<T> {
    /// Response data
    data: T,
    /// Optional metadata
    metadata: Option<serde_json::Value>,
}

impl<T> ApiSuccessResponse<T>
where
    T: serde::Serialize,
{
    /// Create a new API success response
    pub fn new(data: T, metadata: Option<serde_json::Value>) -> Self {
        Self { data, metadata }
    }
}

impl<T> IntoResponse for ApiSuccessResponse<T>
where
    T: serde::Serialize,
{
    fn into_response(self) -> Response {
        let body = serde_json::json!({
            "data": self.data,
            "metadata": self.metadata,
        });

        (StatusCode::OK, body).into_response()
    }
}

/// Test utilities for API server
#[cfg(test)]
mod tests {
    use super::*;
    use crate::cortex_core::config::ConfigManager;
    use crate::cortex_index::IndexManager;
    use crate::cortex_storage::memory::MemoryStorage;

    #[test]
    fn test_api_server_creation() {
        let storage = Arc::new(MemoryStorage::new());
        let index_manager = Arc::new(IndexManager::new());
        let config = ConfigManager::load_default().unwrap();

        let server = ApiServer::new(storage, index_manager, Arc::new(config));
        assert!(server.is_ok());
    }

    #[test]
    fn test_api_error_response() {
        let error = ApiErrorResponse::new(
            StatusCode::BAD_REQUEST,
            "Invalid request".to_string(),
            "INVALID_REQUEST".to_string(),
            None,
        );

        let response = error.into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn test_api_success_response() {
        let data = serde_json::json!({ "message": "Success" });
        let success = ApiSuccessResponse::new(data, None);

        let response = success.into_response();
        assert_eq!(response.status(), StatusCode::OK);
    }
}
