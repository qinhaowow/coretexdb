//! Backup and restore utilities for CortexDB

use std::path::Path;
use std::sync::Arc;
use tokio::fs;
use tokio::sync::RwLock;
use tokio::io::AsyncWriteExt;
use std::collections::HashMap;

use crate::CortexDB;

pub struct BackupManager {
    backup_dir: String,
}

impl BackupManager {
    pub fn new(backup_dir: &str) -> Self {
        Self {
            backup_dir: backup_dir.to_string(),
        }
    }

    pub async fn create_backup(&self, db: &CortexDB, name: &str) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let timestamp = chrono::Utc::now().format("%Y%m%d_%H%M%S").to_string();
        let backup_name = format!("{}_{}", name, timestamp);
        let backup_path = Path::new(&self.backup_dir).join(&backup_name);
        
        fs::create_dir_all(&backup_path).await?;
        
        let collections = db.list_collections().await?;
        
        let mut metadata = HashMap::new();
        metadata.insert("collections".to_string(), serde_json::json!(collections));
        metadata.insert("timestamp".to_string(), serde_json::json!(timestamp));
        
        let metadata_path = backup_path.join("metadata.json");
        let metadata_content = serde_json::to_string_pretty(&metadata)?;
        fs::write(&metadata_path, metadata_content).await?;
        
        for collection_name in &collections {
            let collection_path = backup_path.join(format!("{}.json", collection_name));
            let mut vectors_data = Vec::new();
            
            let count = db.get_vectors_count(collection_name).await.unwrap_or(0);
            
            for i in 0..count {
                if let Ok(Some((vector, metadata))) = db.get_vector(collection_name, &format!("vec_{}", i)).await {
                    vectors_data.push(serde_json::json!({
                        "id": format!("vec_{}", i),
                        "vector": vector,
                        "metadata": metadata,
                    }));
                }
            }
            
            let collection_content = serde_json::to_string_pretty(&vectors_data)?;
            fs::write(&collection_path, collection_content).await?;
        }
        
        Ok(backup_name)
    }

    pub async fn restore_backup(&self, db: &CortexDB, backup_name: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let backup_path = Path::new(&self.backup_dir).join(backup_name);
        
        if !backup_path.exists() {
            return Err("Backup not found".into());
        }
        
        let metadata_path = backup_path.join("metadata.json");
        let metadata_content = fs::read_to_string(&metadata_path).await?;
        let metadata: serde_json::Value = serde_json::from_str(&metadata_content)?;
        
        let collections = metadata["collections"].as_array()
            .ok_or("Invalid metadata format")?;
        
        for collection in collections {
            let collection_name = collection.as_str().ok_or("Invalid collection name")?;
            
            let collection_path = backup_path.join(format!("{}.json", collection_name));
            
            if collection_path.exists() {
                let content = fs::read_to_string(&collection_path).await?;
                let vectors: Vec<serde_json::Value> = serde_json::from_str(&content)?;
                
                let _ = db.create_collection(collection_name, 384, "cosine").await;
                
                for vector_data in vectors {
                    let id = vector_data["id"].as_str().unwrap_or("unknown");
                    let vector = vector_data["vector"].as_array()
                        .unwrap_or(&vec![])
                        .iter()
                        .filter_map(|v| v.as_f64())
                        .map(|v| v as f32)
                        .collect::<Vec<_>>();
                    let metadata = vector_data["metadata"].clone();
                    
                    let _ = db.insert_vectors(collection_name, vec![(id.to_string(), vector, metadata)]).await;
                }
            }
        }
        
        Ok(())
    }

    pub async fn list_backups(&self) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
        let mut backups = Vec::new();
        
        let mut entries = fs::read_dir(&self.backup_dir).await?;
        
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.is_dir() {
                if let Some(name) = path.file_name() {
                    backups.push(name.to_string_lossy().to_string());
                }
            }
        }
        
        Ok(backups)
    }

    pub async fn delete_backup(&self, backup_name: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let backup_path = Path::new(&self.backup_dir).join(backup_name);
        
        if backup_path.exists() {
            fs::remove_dir_all(&backup_path).await?;
        }
        
        Ok(())
    }
}
