//! Vector Lakehouse Module for CoreTexDB
//! Provides hot/warm/cold data tiering for cost-effective storage

pub mod tier;
pub mod storage;
pub mod s3_http;
pub mod policy;
pub mod lakehouse;

pub use tier::{StorageTier, TierConfig, DocumentMeta};
pub use storage::{StorageBackend, LocalConfig, S3Config, MinIOConfig, AzureConfig, StorageBackendTrait};
pub use s3_http::HttpS3Storage;
pub use policy::{TieringPolicy, LRUTieringPolicy, TTLTieringPolicy, HybridTieringPolicy, SizeBasedPolicy};
pub use lakehouse::{VectorLakehouse, MigrationReport, LakehouseStats};
