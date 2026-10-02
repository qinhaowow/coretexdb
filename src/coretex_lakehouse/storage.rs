//! Storage Backend for Vector Lakehouse
//! Supports local, S3, and MinIO storage backends

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StorageBackend {
    Local(LocalConfig),
    S3(S3Config),
    MinIO(MinIOConfig),
    AzureBlob(AzureConfig),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalConfig {
    pub base_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Config {
    pub bucket: String,
    pub region: String,
    pub access_key: String,
    pub secret_key: String,
    pub endpoint: Option<String>,
    pub use_ssl: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MinIOConfig {
    pub bucket: String,
    pub endpoint: String,
    pub access_key: String,
    pub secret_key: String,
    pub use_ssl: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AzureConfig {
    pub container: String,
    pub account_name: String,
    pub account_key: String,
}

pub trait StorageBackendTrait: Send + Sync {
    fn write(&self, key: &str, data: &[u8]) -> Result<(), String>;
    fn read(&self, key: &str) -> Result<Vec<u8>, String>;
    fn delete(&self, key: &str) -> Result<(), String>;
    fn exists(&self, key: &str) -> bool;
    fn list(&self, prefix: &str) -> Result<Vec<String>, String>;
}

pub struct LocalStorage {
    base_path: PathBuf,
}

impl LocalStorage {
    pub fn new(base_path: &str) -> Self {
        Self {
            base_path: PathBuf::from(base_path),
        }
    }

    fn full_path(&self, key: &str) -> PathBuf {
        self.base_path.join(key)
    }
}

impl StorageBackendTrait for LocalStorage {
    fn write(&self, key: &str, data: &[u8]) -> Result<(), String> {
        let path = self.full_path(key);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
        }
        std::fs::write(&path, data).map_err(|e| e.to_string())
    }

    fn read(&self, key: &str) -> Result<Vec<u8>, String> {
        std::fs::read(self.full_path(key)).map_err(|e| e.to_string())
    }

    fn delete(&self, key: &str) -> Result<(), String> {
        std::fs::remove_file(self.full_path(key)).map_err(|e| e.to_string())
    }

    fn exists(&self, key: &str) -> bool {
        self.full_path(key).exists()
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>, String> {
        let mut results = Vec::new();
        let prefix_path = self.full_path(prefix);
        
        if let Ok(entries) = std::fs::read_dir(prefix_path.parent().unwrap_or(&self.base_path)) {
            for entry in entries.flatten() {
                if let Ok(path) = entry.path().strip_prefix(&self.base_path) {
                    if let Some(s) = path.to_str() {
                        if s.starts_with(prefix) {
                            results.push(s.to_string());
                        }
                    }
                }
            }
        }
        
        Ok(results)
    }
}

// An earlier draft of the S3 backend called the `aws-sdk-s3` / `aws-config`
// crates. Neither was ever added to `Cargo.toml`, so the `s3` feature stayed
// empty (`s3 = []`) and this module could not compile: `--features s3` — and
// therefore `--features full`, which is what CI builds and tests with — failed
// with "use of undeclared crate or module aws_sdk_s3" before a single test ran.
//
// `s3_http::HttpS3Storage` is the replacement: it speaks AWS Signature V4 over
// plain HTTP with `reqwest`, needs no vendor SDK, and covers AWS S3, MinIO and
// any S3-compatible endpoint. It is compiled unconditionally. Use it via
// `HttpS3Storage::new(..)` / `::new_minio(..)`.
//
// Restoring an SDK-backed backend means adding the dependencies to `Cargo.toml`
// first, then giving that feature a compile+test gate so it cannot rot again.

impl StorageBackend {
    pub fn create_local(path: &str) -> Self {
        Self::Local(LocalConfig {
            base_path: path.to_string(),
        })
    }

    pub fn create_s3(bucket: &str, region: &str, access_key: &str, secret_key: &str) -> Self {
        Self::S3(S3Config {
            bucket: bucket.to_string(),
            region: region.to_string(),
            access_key: access_key.to_string(),
            secret_key: secret_key.to_string(),
            endpoint: None,
            use_ssl: true,
        })
    }

    pub fn create_minio(bucket: &str, endpoint: &str, access_key: &str, secret_key: &str) -> Self {
        Self::MinIO(MinIOConfig {
            bucket: bucket.to_string(),
            endpoint: endpoint.to_string(),
            access_key: access_key.to_string(),
            secret_key: secret_key.to_string(),
            use_ssl: false,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;
use crate::coretex_core::Result;

    #[test]
    fn test_local_storage() {
        let temp_dir = TempDir::new().unwrap();
        let storage = LocalStorage::new(temp_dir.path().to_str().unwrap());
        
        storage.write("test/key.txt", b"hello").unwrap();
        assert!(storage.exists("test/key.txt"));
        
        let data = storage.read("test/key.txt").unwrap();
        assert_eq!(data, b"hello");
        
        storage.delete("test/key.txt").unwrap();
        assert!(!storage.exists("test/key.txt"));
    }
}
