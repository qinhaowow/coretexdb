//! Write-Ahead Log (WAL) for CortexDB

use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncWriteExt, AsyncReadExt, BufReader};
use std::path::PathBuf;
use std::collections::VecDeque;
use serde::{Serialize, Deserialize};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalEntryType {
    Insert,
    Update,
    Delete,
    CreateCollection,
    DeleteCollection,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalEntry {
    pub id: u64,
    pub timestamp: u64,
    pub entry_type: WalEntryType,
    pub collection: String,
    pub data: serde_json::Value,
}

pub struct WriteAheadLog {
    log_dir: PathBuf,
    current_file: PathBuf,
    entry_counter: Arc<RwLock<u64>>,
    max_file_size: u64,
    current_size: Arc<RwLock<u64>>,
}

impl WriteAheadLog {
    pub fn new(log_dir: &str) -> Self {
        let log_path = PathBuf::from(log_dir);
        let current_file = log_path.join("wal.log");
        
        Self {
            log_dir: log_path,
            current_file,
            entry_counter: Arc::new(RwLock::new(0)),
            max_file_size: 64 * 1024 * 1024,
            current_size: Arc::new(RwLock::new(0)),
        }
    }

    pub async fn init(&self) -> std::io::Result<()> {
        tokio::fs::create_dir_all(&self.log_dir).await?;
        
        if !self.current_file.exists() {
            let file = File::create(&self.current_file).await?;
            file.sync_all().await?;
        }
        
        Ok(())
    }

    pub async fn append(&self, entry: &WalEntry) -> std::io::Result<()> {
        let serialized = serde_json::to_vec(entry)?;
        let entry_size = serialized.len() as u64;
        
        let mut current_size = self.current_size.write().await;
        if *current_size + entry_size > self.max_file_size {
            drop(current_size);
            self.rotate().await?;
            current_size = self.current_size.write().await;
        }
        
        let mut file = OpenOptions::new()
            .append(true)
            .open(&self.current_file)
            .await?;
        
        file.write_all(&serialized).await?;
        file.write_all(b"\n").await?;
        file.sync_all().await?;
        
        *current_size += entry_size + 1;
        
        Ok(())
    }

    async fn rotate(&self) -> std::io::Result<()> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        let new_file = self.log_dir.join(format!("wal_{}.log", timestamp));
        
        let old_file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&new_file)
            .await?;
        
        old_file.sync_all().await?;
        
        self.current_file = new_file;
        *self.current_size.write().await = 0;
        
        Ok(())
    }

    pub async fn read_entries(&self) -> std::io::Result<Vec<WalEntry>> {
        let file = File::open(&self.current_file).await?;
        let reader = BufReader::new(file);
        let mut lines = reader.lines();
        let mut entries = Vec::new();
        
        while let Ok(Some(line)) = lines.next_line().await {
            if let Ok(entry) = serde_json::from_str::<WalEntry>(&line) {
                entries.push(entry);
            }
        }
        
        Ok(entries)
    }

    pub async fn replay(&self, handler: &impl Fn(WalEntry)) -> std::io::Result<()> {
        let entries = self.read_entries().await?;
        
        for entry in entries {
            handler(entry);
        }
        
        Ok(())
    }

    pub async fn create_entry(
        &self,
        entry_type: WalEntryType,
        collection: &str,
        data: serde_json::Value,
    ) -> std::io::Result<WalEntry> {
        let mut counter = self.entry_counter.write().await;
        *counter += 1;
        let id = *counter;
        
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        let entry = WalEntry {
            id,
            timestamp,
            entry_type,
            collection: collection.to_string(),
            data,
        };
        
        self.append(&entry).await?;
        
        Ok(entry)
    }
}
