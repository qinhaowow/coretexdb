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

#[cfg(feature = "s3")]
mod s3_backend {
    use super::*;
    
    pub struct S3Storage {
        client: aws_sdk_s3::Client,
        bucket: String,
    }
    
    impl S3Storage {
        pub fn new(config: &S3Config) -> Self {
            let sdk_config = aws_config::from_env()
                .region(aws_sdk_s3::config::Region::new(&config.region))
                .load()
                .await;
            
            Self {
                client: aws_sdk_s3::Client::new(&sdk_config),
                bucket: config.bucket.clone(),
            }
        }
    }
    
    impl StorageBackendTrait for S3Storage {
        fn write(&self, key: &str, data: &[u8]) -> Result<(), String> {
            tokio::runtime::Handle::current()
                .block_on(async {
                    self.client.put_object()
                        .bucket(&self.bucket)
                        .key(key)
                        .body(data.into())
                        .send()
                        .await
                        .map_err(|e| e.to_string())
                })
        }
        
        fn read(&self, key: &str) -> Result<Vec<u8>, String> {
            tokio::runtime::Handle::current()
                .block_on(async {
                    let response = self.client.get_object()
                        .bucket(&self.bucket)
                        .key(key)
                        .send()
                        .await
                        .map_err(|e| e.to_string())?;
                    
                    let bytes = response.body.collect().await.map_err(|e| e.to_string())?;
                    Ok(bytes.to_vec())
                })
        }
        
        fn delete(&self, key: &str) -> Result<(), String> {
            tokio::runtime::Handle::current()
                .block_on(async {
                    self.client.delete_object()
                        .bucket(&self.bucket)
                        .key(key)
                        .send()
                        .await
                        .map_err(|e| e.to_string())
                })
        }
        
        fn exists(&self, key: &str) -> bool {
            tokio::runtime::Handle::current()
                .block_on(async {
                    self.client.head_object()
                        .bucket(&self.bucket)
                        .key(key)
                        .send()
                        .await
                        .is_ok()
                })
        }
        
        fn list(&self, prefix: &str) -> Result<Vec<String>, String> {
            tokio::runtime::Handle::current()
                .block_on(async {
                    let response = self.client.list_objects_v2()
                        .bucket(&self.bucket)
                        .prefix(prefix)
                        .send()
                        .await
                        .map_err(|e| e.to_string())?;
                    
                    Ok(response.contents()
                        .map(|objects| {
                            objects.iter()
                                .filter_map(|o| o.key().map(|k| k.to_string()))
                                .collect()
                        })
                        .unwrap_or_default())
                })
        }
    }
}

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
