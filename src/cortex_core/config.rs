//! Configuration management for CortexDB
//! 
//! This module handles loading, parsing, and validating configuration files,
//! as well as providing access to configuration values throughout the system.

use serde::{Serialize, Deserialize};
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Server configuration
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ServerConfig {
    /// Host address to bind to
    pub host: String,
    /// Port to listen on
    pub port: u16,
    /// Number of worker threads
    pub workers: usize,
    /// Maximum request body size in bytes
    pub max_body_size: usize,
    /// Enable CORS
    pub enable_cors: bool,
    /// CORS allowed origins
    pub cors_origins: Vec<String>,
    /// Enable TLS
    pub enable_tls: bool,
    /// TLS certificate path
    pub tls_cert: Option<String>,
    /// TLS private key path
    pub tls_key: Option<String>,
}

/// Storage configuration
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StorageConfig {
    /// Storage engine type (memory, persistent, etc.)
    pub engine: String,
    /// Storage directory for persistent storage
    pub directory: String,
    /// Maximum memory usage in bytes
    pub max_memory: usize,
    /// Enable compression
    pub enable_compression: bool,
    /// Compression level (1-9)
    pub compression_level: u8,
    /// Enable encryption
    pub enable_encryption: bool,
    /// Encryption key (base64 encoded)
    pub encryption_key: Option<String>,
    /// Write buffer size in bytes
    pub write_buffer_size: usize,
    /// Maximum open files
    pub max_open_files: usize,
}

/// Index configuration
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct IndexConfig {
    /// Default index type (brute_force, hnsw, etc.)
    pub default_type: String,
    /// Default distance metric (euclidean, cosine, dot)
    pub default_metric: String,
    /// Default HNSW M parameter
    pub hnsw_m: usize,
    /// Default HNSW ef_construction parameter
    pub hnsw_ef_construction: usize,
    /// Default HNSW ef_search parameter
    pub hnsw_ef_search: usize,
    /// Default IVF nlist parameter
    pub ivf_nlist: usize,
    /// Default IVF nprobe parameter
    pub ivf_nprobe: usize,
}

/// Query configuration
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct QueryConfig {
    /// Default limit for search results
    pub default_limit: usize,
    /// Default timeout in milliseconds
    pub default_timeout: u64,
    /// Maximum concurrent queries
    pub max_concurrent_queries: usize,
    /// Enable query caching
    pub enable_caching: bool,
    /// Query cache size
    pub cache_size: usize,
    /// Cache TTL in seconds
    pub cache_ttl: u64,
    /// Enable parallel execution
    pub enable_parallel: bool,
    /// Maximum parallelism
    pub max_parallelism: usize,
}

/// Distributed configuration
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DistributedConfig {
    /// Enable distributed mode
    pub enabled: bool,
    /// Cluster node ID
    pub node_id: String,
    /// Cluster seed nodes
    pub seed_nodes: Vec<String>,
    /// Enable leader election
    pub enable_leader_election: bool,
    /// Heartbeat interval in milliseconds
    pub heartbeat_interval: u64,
    /// Election timeout in milliseconds
    pub election_timeout: u64,
    /// Enable sharding
    pub enable_sharding: bool,
    /// Number of shards
    pub shard_count: usize,
    /// Sharding strategy (hash, range, etc.)
    pub sharding_strategy: String,
    /// Enable replication
    pub enable_replication: bool,
    /// Replication factor
    pub replication_factor: usize,
}

/// Monitoring configuration
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MonitoringConfig {
    /// Enable monitoring
    pub enabled: bool,
    /// Metrics collection interval in seconds
    pub metrics_interval: u64,
    /// Enable Prometheus endpoint
    pub enable_prometheus: bool,
    /// Prometheus endpoint path
    pub prometheus_path: String,
    /// Enable health checks
    pub enable_health_checks: bool,
    /// Health check interval in seconds
    pub health_check_interval: u64,
    /// Enable tracing
    pub enable_tracing: bool,
    /// Tracing endpoint
    pub tracing_endpoint: Option<String>,
}

/// Backup configuration
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BackupConfig {
    /// Enable scheduled backups
    pub enabled: bool,
    /// Backup directory
    pub directory: String,
    /// Backup interval in hours
    pub interval: u64,
    /// Retention days
    pub retention_days: u32,
    /// Enable compression
    pub enable_compression: bool,
    /// Enable encryption
    pub enable_encryption: bool,
    /// Encryption key (base64 encoded)
    pub encryption_key: Option<String>,
    /// Enable remote backup
    pub enable_remote: bool,
    /// Remote backup endpoint
    pub remote_endpoint: Option<String>,
    /// Remote backup credentials
    pub remote_credentials: Option<String>,
}

/// Security configuration
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SecurityConfig {
    /// Enable authentication
    pub enable_auth: bool,
    /// JWT secret key
    pub jwt_secret: String,
    /// JWT expiration in hours
    pub jwt_expiration: u64,
    /// Password hashing algorithm (argon2, bcrypt)
    pub password_algorithm: String,
    /// Argon2 memory cost
    pub argon2_memory: u32,
    /// Argon2 time cost
    pub argon2_time: u32,
    /// Argon2 parallelism
    pub argon2_parallelism: u32,
    /// Enable rate limiting
    pub enable_rate_limiting: bool,
    /// Rate limit requests per second
    pub rate_limit: u64,
    /// Enable IP whitelist
    pub enable_ip_whitelist: bool,
    /// IP whitelist
    pub ip_whitelist: Vec<String>,
}

/// Full configuration structure
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CortexConfig {
    /// Server configuration
    pub server: ServerConfig,
    /// Storage configuration
    pub storage: StorageConfig,
    /// Index configuration
    pub index: IndexConfig,
    /// Query configuration
    pub query: QueryConfig,
    /// Distributed configuration
    pub distributed: DistributedConfig,
    /// Monitoring configuration
    pub monitoring: MonitoringConfig,
    /// Backup configuration
    pub backup: BackupConfig,
    /// Security configuration
    pub security: SecurityConfig,
    /// Environment (development, production, testing)
    pub environment: String,
    /// Log level (trace, debug, info, warn, error)
    pub log_level: String,
    /// Additional custom configuration
    pub custom: HashMap<String, serde_json::Value>,
}

/// Configuration manager for handling config operations
#[derive(Debug)]
pub struct ConfigManager {
    config: Arc<RwLock<CortexConfig>>,
    config_path: Option<String>,
}

impl ConfigManager {
    /// Create a new config manager with default values
    pub fn new() -> Self {
        Self {
            config: Arc::new(RwLock::new(Self::default_config())),
            config_path: None,
        }
    }

    /// Create a new config manager from file
    pub fn from_file(path: &str) -> Result<Self, ConfigError> {
        let config = Self::load_from_file(path)?;
        Ok(Self {
            config: Arc::new(RwLock::new(config)),
            config_path: Some(path.to_string()),
        })
    }

    /// Load configuration from file
    pub fn load_from_file(path: &str) -> Result<CortexConfig, ConfigError> {
        let path = Path::new(path);
        if !path.exists() {
            return Err(ConfigError::ConfigFileNotFound(path.display().to_string()));
        }

        let mut file = File::open(path)
            .map_err(|e| ConfigError::ReadError(e.to_string()))?;

        let mut content = String::new();
        file.read_to_string(&mut content)
            .map_err(|e| ConfigError::ReadError(e.to_string()))?;

        let config: CortexConfig = toml::from_str(&content)
            .map_err(|e| ConfigError::ParseError(e.to_string()))?;

        Ok(config)
    }

    /// Save configuration to file
    pub async fn save_to_file(&self, path: &str) -> Result<(), ConfigError> {
        let config = self.config.read().await;
        let content = toml::to_string_pretty(&config)
            .map_err(|e| ConfigError::WriteError(e.to_string()))?;

        std::fs::write(path, content)
            .map_err(|e| ConfigError::WriteError(e.to_string()))?;

        Ok(())
    }

    /// Get current configuration
    pub async fn get_config(&self) -> CortexConfig {
        self.config.read().await.clone()
    }

    /// Update configuration
    pub async fn update_config(&self, new_config: CortexConfig) {
        let mut config = self.config.write().await;
        *config = new_config;
    }

    /// Get server configuration
    pub async fn get_server_config(&self) -> ServerConfig {
        self.config.read().await.server.clone()
    }

    /// Get storage configuration
    pub async fn get_storage_config(&self) -> StorageConfig {
        self.config.read().await.storage.clone()
    }

    /// Get index configuration
    pub async fn get_index_config(&self) -> IndexConfig {
        self.config.read().await.index.clone()
    }

    /// Get query configuration
    pub async fn get_query_config(&self) -> QueryConfig {
        self.config.read().await.query.clone()
    }

    /// Get distributed configuration
    pub async fn get_distributed_config(&self) -> DistributedConfig {
        self.config.read().await.distributed.clone()
    }

    /// Get monitoring configuration
    pub async fn get_monitoring_config(&self) -> MonitoringConfig {
        self.config.read().await.monitoring.clone()
    }

    /// Get backup configuration
    pub async fn get_backup_config(&self) -> BackupConfig {
        self.config.read().await.backup.clone()
    }

    /// Get security configuration
    pub async fn get_security_config(&self) -> SecurityConfig {
        self.config.read().await.security.clone()
    }

    /// Get environment
    pub async fn get_environment(&self) -> String {
        self.config.read().await.environment.clone()
    }

    /// Get log level
    pub async fn get_log_level(&self) -> String {
        self.config.read().await.log_level.clone()
    }

    /// Get custom configuration value
    pub async fn get_custom_value(&self, key: &str) -> Option<serde_json::Value> {
        self.config.read().await.custom.get(key).cloned()
    }

    /// Set custom configuration value
    pub async fn set_custom_value(&self, key: &str, value: serde_json::Value) {
        let mut config = self.config.write().await;
        config.custom.insert(key.to_string(), value);
    }

    /// Reload configuration from file
    pub async fn reload(&self) -> Result<(), ConfigError> {
        if let Some(path) = &self.config_path {
            let new_config = Self::load_from_file(path)?;
            let mut config = self.config.write().await;
            *config = new_config;
            Ok(())
        } else {
            Err(ConfigError::MissingConfig("No config file path set".to_string()))
        }
    }

    /// Get default configuration
    pub fn default_config() -> CortexConfig {
        CortexConfig {
            server: ServerConfig {
                host: "0.0.0.0".to_string(),
                port: 8080,
                workers: num_cpus::get(),
                max_body_size: 10 * 1024 * 1024, // 10MB
                enable_cors: true,
                cors_origins: vec!["*".to_string()],
                enable_tls: false,
                tls_cert: None,
                tls_key: None,
            },
            storage: StorageConfig {
                engine: "persistent".to_string(),
                directory: "./data".to_string(),
                max_memory: 1024 * 1024 * 1024, // 1GB
                enable_compression: true,
                compression_level: 6,
                enable_encryption: false,
                encryption_key: None,
                write_buffer_size: 64 * 1024 * 1024, // 64MB
                max_open_files: 1000,
            },
            index: IndexConfig {
                default_type: "brute_force".to_string(),
                default_metric: "cosine".to_string(),
                hnsw_m: 16,
                hnsw_ef_construction: 100,
                hnsw_ef_search: 10,
                ivf_nlist: 100,
                ivf_nprobe: 10,
            },
            query: QueryConfig {
                default_limit: 10,
                default_timeout: 30000, // 30 seconds
                max_concurrent_queries: 100,
                enable_caching: true,
                cache_size: 10000,
                cache_ttl: 3600, // 1 hour
                enable_parallel: true,
                max_parallelism: num_cpus::get(),
            },
            distributed: DistributedConfig {
                enabled: false,
                node_id: format!("node-{}", uuid::Uuid::new_v4()),
                seed_nodes: vec![],
                enable_leader_election: true,
                heartbeat_interval: 1000, // 1 second
                election_timeout: 5000, // 5 seconds
                enable_sharding: false,
                shard_count: 1,
                sharding_strategy: "hash".to_string(),
                enable_replication: false,
                replication_factor: 1,
            },
            monitoring: MonitoringConfig {
                enabled: true,
                metrics_interval: 10, // 10 seconds
                enable_prometheus: true,
                prometheus_path: "/metrics".to_string(),
                enable_health_checks: true,
                health_check_interval: 30, // 30 seconds
                enable_tracing: false,
                tracing_endpoint: None,
            },
            backup: BackupConfig {
                enabled: false,
                directory: "./backups".to_string(),
                interval: 24, // 24 hours
                retention_days: 7,
                enable_compression: true,
                enable_encryption: false,
                encryption_key: None,
                enable_remote: false,
                remote_endpoint: None,
                remote_credentials: None,
            },
            security: SecurityConfig {
                enable_auth: false,
                jwt_secret: "your-secret-key-here".to_string(),
                jwt_expiration: 24, // 24 hours
                password_algorithm: "argon2".to_string(),
                argon2_memory: 65536, // 64MB
                argon2_time: 3,
                argon2_parallelism: 4,
                enable_rate_limiting: false,
                rate_limit: 100, // 100 requests per second
                enable_ip_whitelist: false,
                ip_whitelist: vec![],
            },
            environment: "development".to_string(),
            log_level: "info".to_string(),
            custom: HashMap::new(),
        }
    }
}

/// Configuration error types
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// Configuration file not found
    #[error("Config file not found: {0}")]
    ConfigFileNotFound(String),
    
    /// Missing configuration value
    #[error("Missing config: {0}")]
    MissingConfig(String),
    
    /// Invalid configuration value
    #[error("Invalid config: {0}")]
    InvalidConfig(String),
    
    /// Configuration parse error
    #[error("Parse error: {0}")]
    ParseError(String),
    
    /// Configuration read error
    #[error("Read error: {0}")]
    ReadError(String),
    
    /// Configuration write error
    #[error("Write error: {0}")]
    WriteError(String),
}

/// Default implementation for CortexConfig
impl Default for CortexConfig {
    fn default() -> Self {
        ConfigManager::default_config()
    }
}