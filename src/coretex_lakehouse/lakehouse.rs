//! Vector Lakehouse Manager
//! Orchestrates data tiering and management across storage layers

use crate::coretex_lakehouse::tier::{StorageTier, DocumentMeta, TierConfig};
use crate::coretex_lakehouse::policy::{TieringPolicy, HybridTieringPolicy};
use crate::coretex_lakehouse::storage::{StorageBackendTrait, LocalStorage};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct VectorLakehouse {
    hot_storage: Arc<dyn StorageBackendTrait>,
    warm_storage: Arc<dyn StorageBackendTrait>,
    cold_storage: Arc<dyn StorageBackendTrait>,
    metadata: Arc<RwLock<HashMap<String, DocumentMeta>>>,
    policy: Box<dyn TieringPolicy>,
    config: TierConfig,
}

impl VectorLakehouse {
    pub fn new(data_dir: &str) -> Result<Self, String> {
        let hot_path = Path::new(data_dir).join("hot");
        let warm_path = Path::new(data_dir).join("warm");
        let cold_path = Path::new(data_dir).join("cold");

        std::fs::create_dir_all(&hot_path).map_err(|e| e.to_string())?;
        std::fs::create_dir_all(&warm_path).map_err(|e| e.to_string())?;
        std::fs::create_dir_all(&cold_path).map_err(|e| e.to_string())?;

        let hot_storage: Arc<dyn StorageBackendTrait> = Arc::new(LocalStorage::new(hot_path.to_str().unwrap()));
        let warm_storage: Arc<dyn StorageBackendTrait> = Arc::new(LocalStorage::new(warm_path.to_str().unwrap()));
        let cold_storage: Arc<dyn StorageBackendTrait> = Arc::new(LocalStorage::new(cold_path.to_str().unwrap()));

        let config = TierConfig::default();
        let policy = Box::new(HybridTieringPolicy::new(config.clone()));

        Ok(Self {
            hot_storage,
            warm_storage,
            cold_storage,
            metadata: Arc::new(RwLock::new(HashMap::new())),
            policy,
            config,
        })
    }

    pub fn with_policy(mut self, policy: Box<dyn TieringPolicy>) -> Self {
        self.policy = policy;
        self
    }

    pub async fn write(&self, collection: &str, id: &str, data: &[u8], vector_dim: Option<usize>) -> Result<(), String> {
        let key = format!("{}/{}", collection, id);
        
        let tier = {
            let meta = self.metadata.read().await;
            if let Some(existing) = meta.get(&key) {
                self.policy.determine_tier(existing)
            } else {
                StorageTier::Hot
            }
        };

        let storage = self.get_storage_for_tier(tier);
        storage.write(&key, data)?;

        let mut meta = DocumentMeta::new(id.to_string(), collection.to_string());
        meta.size_bytes = data.len() as u64;
        meta.vector_dimension = vector_dim;
        meta.tier = tier;

        self.metadata.write().await.insert(key, meta);

        Ok(())
    }

    pub async fn read(&self, collection: &str, id: &str) -> Result<Vec<u8>, String> {
        let key = format!("{}/{}", collection, id);
        
        let tier = {
            let meta = self.metadata.read().await;
            meta.get(&key).map(|m| m.tier).unwrap_or(StorageTier::Hot)
        };

        if let Ok(data) = self.get_storage_for_tier(tier).read(&key) {
            let mut meta_map = self.metadata.write().await;
            if let Some(meta) = meta_map.get_mut(&key) {
                meta.record_access();
            }
            return Ok(data);
        }

        for storage in [&self.hot_storage, &self.warm_storage, &self.cold_storage] {
            if let Ok(data) = storage.read(&key) {
                let mut meta_map = self.metadata.write().await;
                if let Some(meta) = meta_map.get_mut(&key) {
                    meta.tier = tier;
                    meta.record_access();
                }
                return Ok(data);
            }
        }

        Err(format!("Document not found: {}", key))
    }

    pub async fn delete(&self, collection: &str, id: &str) -> Result<(), String> {
        let key = format!("{}/{}", collection, id);
        
        let tier = {
            let meta = self.metadata.read().await;
            meta.get(&key).map(|m| m.tier).unwrap_or(StorageTier::Hot)
        };

        self.get_storage_for_tier(tier).delete(&key)?;
        self.metadata.write().await.remove(&key);

        Ok(())
    }

    pub async fn migrate_data(&self) -> Result<MigrationReport, String> {
        let mut report = MigrationReport::default();
        
        let mut meta_map = self.metadata.write().await;
        
        for (key, meta) in meta_map.iter_mut() {
            let new_tier = self.policy.determine_tier(meta);
            
            if new_tier != meta.tier {
                let old_storage = self.get_storage_for_tier(meta.tier);
                let new_storage = self.get_storage_for_tier(new_tier);
                
                if let Ok(data) = old_storage.read(key) {
                    if new_storage.write(key, &data).is_ok() {
                        old_storage.delete(key).ok();
                        meta.tier = new_tier;
                        match new_tier {
                            StorageTier::Hot => report.hot_count += 1,
                            StorageTier::Warm => report.warm_count += 1,
                            StorageTier::Cold => report.cold_count += 1,
                        }
                        report.migrated_count += 1;
                    }
                }
            }
        }
        
        Ok(report)
    }

    pub async fn get_stats(&self) -> LakehouseStats {
        let meta_map = self.metadata.read().await;
        
        let mut hot_size = 0u64;
        let mut warm_size = 0u64;
        let mut cold_size = 0u64;
        let mut hot_count = 0u64;
        let mut warm_count = 0u64;
        let mut cold_count = 0u64;
        
        for meta in meta_map.values() {
            match meta.tier {
                StorageTier::Hot => {
                    hot_size += meta.size_bytes;
                    hot_count += 1;
                }
                StorageTier::Warm => {
                    warm_size += meta.size_bytes;
                    warm_count += 1;
                }
                StorageTier::Cold => {
                    cold_size += meta.size_bytes;
                    cold_count += 1;
                }
            }
        }
        
        LakehouseStats {
            hot_count,
            warm_count,
            cold_count,
            hot_size_bytes: hot_size,
            warm_size_bytes: warm_size,
            cold_size_bytes: cold_size,
            total_count: hot_count + warm_count + cold_count,
        }
    }

    fn get_storage_for_tier(&self, tier: StorageTier) -> Arc<dyn StorageBackendTrait> {
        match tier {
            StorageTier::Hot => self.hot_storage.clone(),
            StorageTier::Warm => self.warm_storage.clone(),
            StorageTier::Cold => self.cold_storage.clone(),
        }
    }
}

#[derive(Debug, Default)]
pub struct MigrationReport {
    pub migrated_count: u64,
    pub hot_count: u64,
    pub warm_count: u64,
    pub cold_count: u64,
}

#[derive(Debug, Default)]
pub struct LakehouseStats {
    pub hot_count: u64,
    pub warm_count: u64,
    pub cold_count: u64,
    pub hot_size_bytes: u64,
    pub warm_size_bytes: u64,
    pub cold_size_bytes: u64,
    pub total_count: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_lakehouse() {
        let temp_dir = TempDir::new().unwrap();
        let lakehouse = VectorLakehouse::new(temp_dir.path().to_str().unwrap()).unwrap();
        
        let data = b"test data";
        lakehouse.write("test", "doc1", data, Some(128)).await.unwrap();
        
        let retrieved = lakehouse.read("test", "doc1").await.unwrap();
        assert_eq!(retrieved, data);
        
        let stats = lakehouse.get_stats().await;
        assert_eq!(stats.total_count, 1);
    }
}
