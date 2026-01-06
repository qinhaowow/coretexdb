//! Error types and error handling for CortexDB
//! 
//! This module defines the core error types used throughout the system,
//! including database errors, storage errors, index errors, and API errors.

use thiserror::Error;
use std::fmt::{Display, Formatter};

/// Result type alias for CortexDB operations
pub type Result<T> = std::result::Result<T, CortexError>;

/// Core error type for CortexDB
#[derive(Debug, Error)]
pub enum CortexError {
    /// Storage engine errors
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),
    
    /// Index system errors
    #[error("Index error: {0}")]
    Index(#[from] IndexError),
    
    /// Query processing errors
    #[error("Query error: {0}")]
    Query(#[from] QueryError),
    
    /// Schema validation errors
    #[error("Schema error: {0}")]
    Schema(#[from] SchemaError),
    
    /// API and client errors
    #[error("API error: {0}")]
    Api(#[from] ApiError),
    
    /// Authentication and authorization errors
    #[error("Security error: {0}")]
    Security(#[from] SecurityError),
    
    /// Distributed system errors
    #[error("Distributed error: {0}")]
    Distributed(#[from] DistributedError),
    
    /// Backup and restore errors
    #[error("Backup error: {0}")]
    Backup(#[from] BackupError),
    
    /// Configuration errors
    #[error("Configuration error: {0}")]
    Config(#[from] ConfigError),
    
    /// Internal system errors
    #[error("Internal error: {0}")]
    Internal(String),
    
    /// I/O errors
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    
    /// Serialization/deserialization errors
    #[error("Serialization error: {0}")]
    Serde(#[from] serde_json::Error),
    
    /// Network errors
    #[error("Network error: {0}")]
    Network(String),
    
    /// Timeout errors
    #[error("Timeout error: {0}")]
    Timeout(String),
    
    /// Resource exhaustion errors
    #[error("Resource error: {0}")]
    Resource(String),
}

/// Storage engine error types
#[derive(Debug, Error)]
pub enum StorageError {
    /// Collection not found
    #[error("Collection not found: {0}")]
    CollectionNotFound(String),
    
    /// Document not found
    #[error("Document not found: {0}")]
    DocumentNotFound(String),
    
    /// Failed to read from storage
    #[error("Failed to read: {0}")]
    ReadError(String),
    
    /// Failed to write to storage
    #[error("Failed to write: {0}")]
    WriteError(String),
    
    /// Storage is full or out of space
    #[error("Storage full: {0}")]
    StorageFull(String),
    
    /// Storage corruption
    #[error("Storage corruption: {0}")]
    Corruption(String),
    
    /// Invalid storage path
    #[error("Invalid storage path: {0}")]
    InvalidPath(String),
}

/// Index system error types
#[derive(Debug, Error)]
pub enum IndexError {
    /// Index not found
    #[error("Index not found: {0}")]
    IndexNotFound(String),
    
    /// Failed to create index
    #[error("Failed to create index: {0}")]
    CreateError(String),
    
    /// Failed to update index
    #[error("Failed to update index: {0}")]
    UpdateError(String),
    
    /// Failed to delete index
    #[error("Failed to delete index: {0}")]
    DeleteError(String),
    
    /// Invalid index configuration
    #[error("Invalid index config: {0}")]
    InvalidConfig(String),
    
    /// Index is full
    #[error("Index full: {0}")]
    IndexFull(String),
}

/// Query processing error types
#[derive(Debug, Error)]
pub enum QueryError {
    /// Invalid query parameters
    #[error("Invalid query params: {0}")]
    InvalidParams(String),
    
    /// Query execution failed
    #[error("Execution failed: {0}")]
    ExecutionError(String),
    
    /// Query timeout
    #[error("Query timeout: {0}")]
    Timeout(String),
    
    /// Query canceled
    #[error("Query canceled: {0}")]
    Canceled(String),
    
    /// Insufficient permissions for query
    #[error("Permission denied: {0}")]
    PermissionDenied(String),
}

/// Schema validation error types
#[derive(Debug, Error)]
pub enum SchemaError {
    /// Collection already exists
    #[error("Collection already exists: {0}")]
    CollectionAlreadyExists(String),
    
    /// Collection not found
    #[error("Collection not found: {0}")]
    CollectionNotFound(String),
    
    /// Missing required field
    #[error("Missing required field: {0}")]
    MissingRequiredField(String),
    
    /// Invalid field type
    #[error("Invalid field type: {0}")]
    InvalidFieldType(String),
    
    /// Field already exists
    #[error("Field already exists: {0}")]
    FieldAlreadyExists(String),
    
    /// Vector field not found
    #[error("Vector field not found: {0}")]
    VectorFieldNotFound(String),
    
    /// Invalid vector field type
    #[error("Invalid vector field type: {0}")]
    InvalidVectorFieldType(String),
}

/// API and client error types
#[derive(Debug, Error)]
pub enum ApiError {
    /// Invalid API request
    #[error("Invalid request: {0}")]
    InvalidRequest(String),
    
    /// Invalid API response
    #[error("Invalid response: {0}")]
    InvalidResponse(String),
    
    /// API endpoint not found
    #[error("Endpoint not found: {0}")]
    EndpointNotFound(String),
    
    /// API rate limit exceeded
    #[error("Rate limit exceeded: {0}")]
    RateLimitExceeded(String),
    
    /// API version error
    #[error("Version error: {0}")]
    VersionError(String),
}

/// Authentication and authorization error types
#[derive(Debug, Error)]
pub enum SecurityError {
    /// Invalid credentials
    #[error("Invalid credentials: {0}")]
    InvalidCredentials(String),
    
    /// Authentication failed
    #[error("Authentication failed: {0}")]
    AuthenticationFailed(String),
    
    /// Authorization failed
    #[error("Authorization failed: {0}")]
    AuthorizationFailed(String),
    
    /// Invalid token
    #[error("Invalid token: {0}")]
    InvalidToken(String),
    
    /// Expired token
    #[error("Expired token: {0}")]
    ExpiredToken(String),
    
    /// Insufficient permissions
    #[error("Insufficient permissions: {0}")]
    InsufficientPermissions(String),
}

/// Distributed system error types
#[derive(Debug, Error)]
pub enum DistributedError {
    /// Node not found
    #[error("Node not found: {0}")]
    NodeNotFound(String),
    
    /// Cluster connection error
    #[error("Connection error: {0}")]
    ConnectionError(String),
    
    /// Leader election error
    #[error("Leader election error: {0}")]
    LeaderElectionError(String),
    
    /// Shard error
    #[error("Shard error: {0}")]
    ShardError(String),
    
    /// Cluster state error
    #[error("Cluster state error: {0}")]
    ClusterStateError(String),
}

/// Backup and restore error types
#[derive(Debug, Error)]
pub enum BackupError {
    /// Backup failed
    #[error("Backup failed: {0}")]
    BackupFailed(String),
    
    /// Restore failed
    #[error("Restore failed: {0}")]
    RestoreFailed(String),
    
    /// Backup not found
    #[error("Backup not found: {0}")]
    BackupNotFound(String),
    
    /// Invalid backup format
    #[error("Invalid backup format: {0}")]
    InvalidBackupFormat(String),
    
    /// Replication error
    #[error("Replication error: {0}")]
    ReplicationError(String),
}

/// Configuration error types
#[derive(Debug, Error)]
pub enum ConfigError {
    /// Invalid configuration
    #[error("Invalid config: {0}")]
    InvalidConfig(String),
    
    /// Missing configuration
    #[error("Missing config: {0}")]
    MissingConfig(String),
    
    /// Configuration file not found
    #[error("Config file not found: {0}")]
    ConfigFileNotFound(String),
    
    /// Configuration parse error
    #[error("Parse error: {0}")]
    ParseError(String),
}

/// Error codes for API responses
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    // 1xx: Informational
    Continue = 100,
    SwitchingProtocols = 101,
    
    // 2xx: Success
    Ok = 200,
    Created = 201,
    Accepted = 202,
    NoContent = 204,
    
    // 4xx: Client errors
    BadRequest = 400,
    Unauthorized = 401,
    Forbidden = 403,
    NotFound = 404,
    MethodNotAllowed = 405,
    RequestTimeout = 408,
    Conflict = 409,
    PayloadTooLarge = 413,
    UnsupportedMediaType = 415,
    TooManyRequests = 429,
    
    // 5xx: Server errors
    InternalServerError = 500,
    NotImplemented = 501,
    BadGateway = 502,
    ServiceUnavailable = 503,
    GatewayTimeout = 504,
    InsufficientStorage = 507,
}

impl ErrorCode {
    /// Get the HTTP status code for this error
    pub fn status_code(&self) -> u16 {
        *self as u16
    }
    
    /// Get the error code for a given HTTP status code
    pub fn from_status_code(code: u16) -> Option<Self> {
        match code {
            100 => Some(ErrorCode::Continue),
            101 => Some(ErrorCode::SwitchingProtocols),
            200 => Some(ErrorCode::Ok),
            201 => Some(ErrorCode::Created),
            202 => Some(ErrorCode::Accepted),
            204 => Some(ErrorCode::NoContent),
            400 => Some(ErrorCode::BadRequest),
            401 => Some(ErrorCode::Unauthorized),
            403 => Some(ErrorCode::Forbidden),
            404 => Some(ErrorCode::NotFound),
            405 => Some(ErrorCode::MethodNotAllowed),
            408 => Some(ErrorCode::RequestTimeout),
            409 => Some(ErrorCode::Conflict),
            413 => Some(ErrorCode::PayloadTooLarge),
            415 => Some(ErrorCode::UnsupportedMediaType),
            429 => Some(ErrorCode::TooManyRequests),
            500 => Some(ErrorCode::InternalServerError),
            501 => Some(ErrorCode::NotImplemented),
            502 => Some(ErrorCode::BadGateway),
            503 => Some(ErrorCode::ServiceUnavailable),
            504 => Some(ErrorCode::GatewayTimeout),
            507 => Some(ErrorCode::InsufficientStorage),
            _ => None,
        }
    }
}

/// Error response structure for API
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct ErrorResponse {
    pub error: String,
    pub code: u16,
    pub message: String,
    pub details: Option<serde_json::Value>,
    pub timestamp: u64,
}

impl ErrorResponse {
    /// Create a new error response
    pub fn new(error: &str, code: ErrorCode, message: &str, details: Option<serde_json::Value>) -> Self {
        Self {
            error: error.to_string(),
            code: code.status_code(),
            message: message.to_string(),
            details,
            timestamp: chrono::Utc::now().timestamp() as u64,
        }
    }
}

/// Convert CortexError to ErrorCode
impl From<CortexError> for ErrorCode {
    fn from(error: CortexError) -> Self {
        match error {
            CortexError::Storage(StorageError::CollectionNotFound(_)) |
            CortexError::Storage(StorageError::DocumentNotFound(_)) |
            CortexError::Index(IndexError::IndexNotFound(_)) => ErrorCode::NotFound,
            
            CortexError::Schema(SchemaError::InvalidFieldType(_)) |
            CortexError::Schema(SchemaError::MissingRequiredField(_)) |
            CortexError::Query(QueryError::InvalidParams(_)) |
            CortexError::Api(ApiError::InvalidRequest(_)) => ErrorCode::BadRequest,
            
            CortexError::Security(SecurityError::InvalidCredentials(_)) |
            CortexError::Security(SecurityError::InvalidToken(_)) |
            CortexError::Security(SecurityError::ExpiredToken(_)) => ErrorCode::Unauthorized,
            
            CortexError::Security(SecurityError::AuthorizationFailed(_)) |
            CortexError::Security(SecurityError::InsufficientPermissions(_)) |
            CortexError::Query(QueryError::PermissionDenied(_)) => ErrorCode::Forbidden,
            
            CortexError::Query(QueryError::Timeout(_)) |
            CortexError::Distributed(DistributedError::ConnectionError(_)) => ErrorCode::RequestTimeout,
            
            CortexError::Api(ApiError::RateLimitExceeded(_)) => ErrorCode::TooManyRequests,
            
            CortexError::Storage(StorageError::StorageFull(_)) |
            CortexError::Backup(BackupError::BackupFailed(_)) => ErrorCode::InsufficientStorage,
            
            CortexError::Schema(SchemaError::CollectionAlreadyExists(_)) |
            CortexError::Distributed(DistributedError::ClusterStateError(_)) => ErrorCode::Conflict,
            
            CortexError::Api(ApiError::EndpointNotFound(_)) => ErrorCode::NotFound,
            
            CortexError::Api(ApiError::MethodNotAllowed(_)) => ErrorCode::MethodNotAllowed,
            
            _ => ErrorCode::InternalServerError,
        }
    }
}

/// Error handling utilities
pub mod utils {
    use super::*;
    
    /// Create an internal error
    pub fn internal_error(message: &str) -> CortexError {
        CortexError::Internal(message.to_string())
    }
    
    /// Create a storage error
    pub fn storage_error(message: &str) -> CortexError {
        CortexError::Storage(StorageError::ReadError(message.to_string()))
    }
    
    /// Create a query error
    pub fn query_error(message: &str) -> CortexError {
        CortexError::Query(QueryError::ExecutionError(message.to_string()))
    }
    
    /// Create a schema error
    pub fn schema_error(message: &str) -> CortexError {
        CortexError::Schema(SchemaError::InvalidFieldType(message.to_string()))
    }
    
    /// Create a security error
    pub fn security_error(message: &str) -> CortexError {
        CortexError::Security(SecurityError::AuthenticationFailed(message.to_string()))
    }
    
    /// Create a distributed error
    pub fn distributed_error(message: &str) -> CortexError {
        CortexError::Distributed(DistributedError::ConnectionError(message.to_string()))
    }
    
    /// Create a backup error
    pub fn backup_error(message: &str) -> CortexError {
        CortexError::Backup(BackupError::BackupFailed(message.to_string()))
    }
    
    /// Create a config error
    pub fn config_error(message: &str) -> CortexError {
        CortexError::Config(ConfigError::InvalidConfig(message.to_string()))
    }
}