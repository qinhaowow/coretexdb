//! Backup and recovery module for CoreTexDB
//! Provides comprehensive backup, restore, and disaster recovery capabilities

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;
use serde::{Deserialize, Serialize};
use tokio::fs;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupConfig {
    pub backup_dir: String,
    pub retention_days: i32,
    pub incremental_enabled: bool,
    pub compression_enabled: bool,
    pub encryption_enabled: bool,
    pub schedule: BackupSchedule,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BackupSchedule {
    Manual,
    Hourly,
    Daily,
    Weekly,
}

impl Default for BackupConfig {
    fn default() -> Self {
        Self {
            backup_dir: "./backups".to_string(),
            retention_days: 30,
            incremental_enabled: true,
            compression_enabled: true,
            encryption_enabled: false,
            schedule: BackupSchedule::Daily,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupMetadata {
    pub id: String,
    pub name: String,
    pub backup_type: BackupType,
    pub created_at: i64,
    pub size_bytes: u64,
    pub collection_count: usize,
    pub vector_count: u64,
    pub checksum: String,
    pub parent_backup_id: Option<String>,
    pub status: BackupStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BackupType {
    Full,
    Incremental,
    Snapshot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BackupStatus {
    InProgress,
    Completed,
    Failed,
}

pub struct BackupManager {
    config: BackupConfig,
    backups: Arc<RwLock<HashMap<String, BackupMetadata>>>,
    data_dir: String,
}

impl BackupManager {
    pub fn new(config: BackupConfig, data_dir: &str) -> Self {
        Self {
            config,
            backups: Arc::new(RwLock::new(HashMap::new())),
            data_dir: data_dir.to_string(),
        }
    }

    pub fn config(&self) -> &BackupConfig {
        &self.config
    }

    pub async fn initialize(&self) -> Result<(), BackupError> {
        let backup_dir = PathBuf::from(&self.config.backup_dir);
        
        if !backup_dir.exists() {
            fs::create_dir_all(&backup_dir)
                .await
                .map_err(|e| BackupError::IoError(e.to_string()))?;
        }

        self.load_backup_metadata().await?;

        Ok(())
    }

    async fn load_backup_metadata(&self) -> Result<(), BackupError> {
        let metadata_file = PathBuf::from(&self.config.backup_dir).join("backups.json");
        
        if metadata_file.exists() {
            let content = fs::read_to_string(&metadata_file)
                .await
                .map_err(|e| BackupError::IoError(e.to_string()))?;
            
            let metadata: Vec<BackupMetadata> = serde_json::from_str(&content)
                .map_err(|e| BackupError::SerializationError(e.to_string()))?;
            
            let mut backups = self.backups.write().await;
            for backup in metadata {
                backups.insert(backup.id.clone(), backup);
            }
        }

        Ok(())
    }

    async fn save_backup_metadata(&self) -> Result<(), BackupError> {
        let backups = self.backups.read().await;
        let metadata: Vec<&BackupMetadata> = backups.values().collect();
        
        let content = serde_json::to_string_pretty(&metadata)
            .map_err(|e| BackupError::SerializationError(e.to_string()))?;
        
        let metadata_file = PathBuf::from(&self.config.backup_dir).join("backups.json");
        fs::write(&metadata_file, content)
            .await
            .map_err(|e| BackupError::IoError(e.to_string()))?;

        Ok(())
    }

    pub async fn create_backup(&self, name: &str, backup_type: BackupType) -> Result<String, BackupError> {
        let backup_id = format!("backup_{}_{}", 
            name.replace(" ", "_"),
            chrono::Utc::now().timestamp()
        );

        let backup_dir = PathBuf::from(&self.config.backup_dir).join(&backup_id);
        
        fs::create_dir_all(&backup_dir)
            .await
            .map_err(|e| BackupError::IoError(e.to_string()))?;

        let metadata = BackupMetadata {
            id: backup_id.clone(),
            name: name.to_string(),
            backup_type,
            created_at: chrono::Utc::now().timestamp(),
            size_bytes: 0,
            collection_count: 0,
            vector_count: 0,
            checksum: String::new(),
            parent_backup_id: None,
            status: BackupStatus::InProgress,
        };

        {
            let mut backups = self.backups.write().await;
            backups.insert(backup_id.clone(), metadata);
        }

        let collections_dir = PathBuf::from(&self.data_dir).join("collections");
        
        let mut total_size: u64 = 0;
        let mut collection_count = 0;
        
        if collections_dir.exists() {
            let mut entries = fs::read_dir(&collections_dir)
                .await
                .map_err(|e| BackupError::IoError(e.to_string()))?;
            
            while let Some(entry) = entries.next_entry().await
                .map_err(|e| BackupError::IoError(e.to_string()))?
            {
                let path = entry.path();
                if path.is_dir() {
                    let collection_name = path.file_name()
                        .unwrap_or_default()
                        .to_string_lossy()
                        .to_string();
                    
                    let dest = backup_dir.join(&collection_name);
                    Self::copy_dir(&path, &dest).await?;
                    
                    let size = Self::dir_size(&dest).await;
                    total_size += size;
                    collection_count += 1;
                }
            }
        }

        let config_file = PathBuf::from(&self.data_dir).join("config");
        if config_file.exists() {
            let dest = backup_dir.join("config");
            fs::copy(&config_file, &dest)
                .await
                .map_err(|e| BackupError::IoError(e.to_string()))?;
        }

        let index_dir = PathBuf::from(&self.data_dir).join("index");
        if index_dir.exists() {
            let dest = backup_dir.join("index");
            Self::copy_dir(&index_dir, &dest).await?;
        }

        let checksum = self.calculate_checksum(&backup_dir).await?;

        {
            let mut backups = self.backups.write().await;
            if let Some(backup) = backups.get_mut(&backup_id) {
                backup.size_bytes = total_size;
                backup.collection_count = collection_count;
                backup.checksum = checksum;
                backup.status = BackupStatus::Completed;
            }
        }

        self.save_backup_metadata().await?;
        self.cleanup_old_backups().await?;

        Ok(backup_id)
    }

    pub async fn restore_backup(&self, backup_id: &str) -> Result<RestoreReport, BackupError> {
        let backup_dir = PathBuf::from(&self.config.backup_dir).join(backup_id);
        
        if !backup_dir.exists() {
            return Err(BackupError::BackupNotFound(backup_id.to_string()));
        }

        let backups = self.backups.read().await;
        let backup = backups.get(backup_id)
            .ok_or_else(|| BackupError::BackupNotFound(backup_id.to_string()))?;
        
        if backup.status != BackupStatus::Completed {
            return Err(BackupError::BackupIncomplete(backup_id.to_string()));
        }

        drop(backups);

        let data_dir = PathBuf::from(&self.data_dir);
        let collections_dir = data_dir.join("collections");
        
        if collections_dir.exists() {
            fs::remove_dir_all(&collections_dir)
                .await
                .map_err(|e| BackupError::IoError(e.to_string()))?;
        }

        let backup_collections = backup_dir.join("collections");
        if backup_collections.exists() {
            Self::copy_dir(&backup_collections, &collections_dir).await?;
        }

        let backup_config = backup_dir.join("config");
        if backup_config.exists() {
            fs::copy(&backup_config, &data_dir.join("config"))
                .await
                .map_err(|e| BackupError::IoError(e.to_string()))?;
        }

        let backup_index = backup_dir.join("index");
        if backup_index.exists() {
            let dest_index = data_dir.join("index");
            fs::remove_dir_all(&dest_index)
                .await
                .map_err(|e| BackupError::IoError(e.to_string()))?;
            Self::copy_dir(&backup_index, &dest_index).await?;
        }

        let collection_count = Self::count_collections(&collections_dir).await?;
        let vector_count = Self::count_vectors(&collections_dir).await?;

        Ok(RestoreReport {
            backup_id: backup_id.to_string(),
            restored_at: chrono::Utc::now().timestamp(),
            collection_count,
            vector_count,
            success: true,
        })
    }

    pub async fn list_backups(&self) -> Vec<BackupMetadata> {
        let backups = self.backups.read().await;
        let mut list: Vec<BackupMetadata> = backups.values().cloned().collect();
        list.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        list
    }

    pub async fn get_backup(&self, backup_id: &str) -> Option<BackupMetadata> {
        self.backups.read().await.get(backup_id).cloned()
    }

    pub async fn delete_backup(&self, backup_id: &str) -> Result<bool, BackupError> {
        let backup_dir = PathBuf::from(&self.config.backup_dir).join(backup_id);
        
        if backup_dir.exists() {
            fs::remove_dir_all(&backup_dir)
                .await
                .map_err(|e| BackupError::IoError(e.to_string()))?;
        }

        {
            let mut backups = self.backups.write().await;
            backups.remove(backup_id);
        }

        self.save_backup_metadata().await?;

        Ok(true)
    }

    pub async fn verify_backup(&self, backup_id: &str) -> Result<bool, BackupError> {
        let backups = self.backups.read().await;
        let backup = backups.get(backup_id)
            .ok_or_else(|| BackupError::BackupNotFound(backup_id.to_string()))?;
        
        let backup_dir = PathBuf::from(&self.config.backup_dir).join(backup_id);
        
        let current_checksum = self.calculate_checksum(&backup_dir).await?;
        
        Ok(current_checksum == backup.checksum)
    }

    async fn cleanup_old_backups(&self) -> Result<(), BackupError> {
        let cutoff = chrono::Utc::now().timestamp() - (self.config.retention_days as i64 * 86400);
        
        let mut backups = self.backups.write().await;
        let to_delete: Vec<String> = backups.values()
            .filter(|b| b.created_at < cutoff)
            .map(|b| b.id.clone())
            .collect();
        
        for backup_id in to_delete {
            let backup_dir = PathBuf::from(&self.config.backup_dir).join(&backup_id);
            if backup_dir.exists() {
                fs::remove_dir_all(&backup_dir)
                    .await
                    .map_err(|e| BackupError::IoError(e.to_string()))?;
            }
            backups.remove(&backup_id);
        }

        drop(backups);
        self.save_backup_metadata().await?;

        Ok(())
    }

    async fn copy_dir(src: &PathBuf, dst: &PathBuf) -> Result<(), BackupError> {
        fs::create_dir_all(dst)
            .await
            .map_err(|e| BackupError::IoError(e.to_string()))?;

        let mut entries = fs::read_dir(src)
            .await
            .map_err(|e| BackupError::IoError(e.to_string()))?;

        while let Some(entry) = entries.next_entry().await
            .map_err(|e| BackupError::IoError(e.to_string()))?
        {
            let src_path = entry.path();
            let dst_path = dst.join(entry.file_name());
            
            if src_path.is_dir() {
                Box::pin(Self::copy_dir(&src_path, &dst_path)).await?;
            } else {
                fs::copy(&src_path, &dst_path)
                    .await
                    .map_err(|e| BackupError::IoError(e.to_string()))?;
            }
        }

        Ok(())
    }

    async fn dir_size(path: &PathBuf) -> u64 {
        let mut total: u64 = 0;
        
        if let Ok(entries) = fs::read_dir(path).await {
            let mut entries = entries;
            while let Ok(Some(entry)) = entries.next_entry().await {
                if let Ok(meta) = entry.metadata().await {
                    if meta.is_file() {
                        total += meta.len();
                    } else if meta.is_dir() {
                        total += Box::pin(Self::dir_size(&entry.path())).await;
                    }
                }
            }
        }
        
        total
    }

    async fn count_collections(path: &PathBuf) -> Result<usize, BackupError> {
        let mut count = 0;
        
        if path.exists() {
            let mut entries = fs::read_dir(path)
                .await
                .map_err(|e| BackupError::IoError(e.to_string()))?;
            
            while let Some(entry) = entries.next_entry().await
                .map_err(|e| BackupError::IoError(e.to_string()))?
            {
                if entry.path().is_dir() {
                    count += 1;
                }
            }
        }
        
        Ok(count)
    }

    async fn count_vectors(path: &PathBuf) -> Result<u64, BackupError> {
        let mut count: u64 = 0;
        
        if path.exists() {
            let mut entries = fs::read_dir(path)
                .await
                .map_err(|e| BackupError::IoError(e.to_string()))?;
            
            while let Some(entry) = entries.next_entry().await
                .map_err(|e| BackupError::IoError(e.to_string()))?
            {
                let entry_path = entry.path();
                
                if entry_path.is_dir() {
                    let vectors_dir = entry_path.join("vectors");
                    if vectors_dir.exists() {
                        let mut vector_entries = fs::read_dir(&vectors_dir)
                            .await
                            .map_err(|e| BackupError::IoError(e.to_string()))?;
                        
                        while let Some(_) = vector_entries.next_entry().await
                            .map_err(|e| BackupError::IoError(e.to_string()))?
                        {
                            count += 1;
                        }
                    }
                }
            }
        }
        
        Ok(count)
    }

    async fn calculate_checksum(&self, path: &PathBuf) -> Result<String, BackupError> {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        
        if path.exists() {
            let mut stack = vec![path.clone()];
            
            while let Some(current) = stack.pop() {
                if current.is_dir() {
                    let mut entries = fs::read_dir(&current)
                        .await
                        .map_err(|e| BackupError::IoError(e.to_string()))?;
                    
                    while let Some(entry) = entries.next_entry().await
                        .map_err(|e| BackupError::IoError(e.to_string()))?
                    {
                        stack.push(entry.path());
                    }
                } else {
                    let content = fs::read(&current)
                        .await
                        .map_err(|e| BackupError::IoError(e.to_string()))?;
                    content.hash(&mut hasher);
                }
            }
        }
        
        Ok(format!("{:x}", hasher.finish()))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestoreReport {
    pub backup_id: String,
    pub restored_at: i64,
    pub collection_count: usize,
    pub vector_count: u64,
    pub success: bool,
}

#[derive(Debug)]
pub enum BackupError {
    IoError(String),
    SerializationError(String),
    BackupNotFound(String),
    BackupIncomplete(String),
}

impl std::fmt::Display for BackupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BackupError::IoError(msg) => write!(f, "IO Error: {}", msg),
            BackupError::SerializationError(msg) => write!(f, "Serialization Error: {}", msg),
            BackupError::BackupNotFound(id) => write!(f, "Backup not found: {}", id),
            BackupError::BackupIncomplete(id) => write!(f, "Backup incomplete: {}", id),
        }
    }
}

impl std::error::Error for BackupError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_backup_manager_new() {
        let config = BackupConfig::default();
        let manager = BackupManager::new(config, "./data");
        
        assert_eq!(manager.config().backup_dir, "./backups");
    }

    #[tokio::test]
    async fn test_create_backup() {
        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir = temp_dir.path().join("data");
        let backup_dir = temp_dir.path().join("backups");
        
        fs::create_dir_all(&data_dir).await.unwrap();
        fs::create_dir_all(&data_dir.join("collections/test")).await.unwrap();
        
        let config = BackupConfig {
            backup_dir: backup_dir.to_string_lossy().to_string(),
            ..Default::default()
        };
        
        let manager = BackupManager::new(config, &data_dir.to_string_lossy());
        manager.initialize().await.unwrap();
        
        let backup_id = manager.create_backup("test_backup", BackupType::Full).await.unwrap();
        
        let backups = manager.list_backups().await;
        assert!(!backups.is_empty());
    }

    #[tokio::test]
    async fn test_list_backups() {
        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir = temp_dir.path().join("data");
        let backup_dir = temp_dir.path().join("backups");
        
        fs::create_dir_all(&data_dir).await.unwrap();
        
        let config = BackupConfig {
            backup_dir: backup_dir.to_string_lossy().to_string(),
            ..Default::default()
        };
        
        let manager = BackupManager::new(config, &data_dir.to_string_lossy());
        manager.initialize().await.unwrap();
        
        let backups = manager.list_backups().await;
        assert!(backups.is_empty());
    }

    #[tokio::test]
    async fn test_delete_backup() {
        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir = temp_dir.path().join("data");
        let backup_dir = temp_dir.path().join("backups");
        
        fs::create_dir_all(&data_dir).await.unwrap();
        
        let config = BackupConfig {
            backup_dir: backup_dir.to_string_lossy().to_string(),
            ..Default::default()
        };
        
        let manager = BackupManager::new(config, &data_dir.to_string_lossy());
        manager.initialize().await.unwrap();
        
        let backup_id = manager.create_backup("test", BackupType::Full).await.unwrap();
        
        let deleted = manager.delete_backup(&backup_id).await.unwrap();
        assert!(deleted);
    }
}
