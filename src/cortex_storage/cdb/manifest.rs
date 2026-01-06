
//! CDB file manifest management
//! 
//! This module handles the manifest file for CDB databases, which tracks
//! file metadata, collection information, and other database-level metadata.

use crate::cortex_core::{CollectionSchema, Result, CortexError, StorageError};
use serde::{Serialize, Deserialize};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;
use std::time::SystemTime;

/// CDB manifest structure
#[derive(Debug, Serialize, Deserialize)]
pub struct CdbManifest {
    /// Manifest version
    pub version: u32,
    /// Database name
    pub database_name: String,
    /// Creation timestamp (UNIX seconds)
    pub created_at: u64,
    /// Last modified timestamp (UNIX seconds)
    pub modified_at: u64,
    /// Number of collections
    pub collection_count: u32,
    /// Collections information
    pub collections: Vec<CollectionInfo>,
    /// File information
    pub files: Vec<FileInfo>,
    /// Database statistics
    pub stats: DatabaseStats,
    /// Custom metadata
    pub metadata: std::collections::HashMap<String, String>,
}

/// Collection information in manifest
#[derive(Debug, Serialize, Deserialize)]
pub struct CollectionInfo {
    /// Collection name
    pub name: String,
    /// Number of documents
    pub document_count: u64,
    /// Collection size in bytes
    pub size_bytes: u64,
    /// Creation timestamp (UNIX seconds)
    pub created_at: u64,
    /// Last modified timestamp (UNIX seconds)
    pub modified_at: u64,
    /// Schema version
    pub schema_version: u32,
    /// Compression enabled
    pub compressed: bool,
    /// Encryption enabled
    pub encrypted: bool,
}

/// File information in manifest
#[derive(Debug, Serialize, Deserialize)]
pub struct FileInfo {
    /// File name
    pub name: String,
    /// File path
    pub path: String,
    /// File size in bytes
    pub size_bytes: u64,
    /// Last modified timestamp (UNIX seconds)
    pub modified_at: u64,
    /// Checksum (SHA-256)
    pub checksum: String,
    /// File type (data, index, journal, etc.)
    pub file_type: String,
    /// Status (active, archived, temporary)
    pub status: String,
}

/// Database statistics in manifest
#[derive(Debug, Serialize, Deserialize)]
pub struct DatabaseStats {
    /// Total size in bytes
    pub total_size: u64,
    /// Total number of documents
    pub total_documents: u64,
    /// Number of active connections
    pub active_connections: u32,
    /// Last compacted timestamp (UNIX seconds)
    pub last_compacted: Option<u64>,
    /// Last backed up timestamp (UNIX seconds)
    pub last_backed_up: Option<u64>,
}

/// Manifest manager for CDB databases
#[derive(Debug)]
pub struct ManifestManager {
    /// Manifest path
    path: String,
    /// Current manifest
    manifest: CdbManifest,
}

impl ManifestManager {
    /// Create a new manifest manager
    pub fn new(path: &str, database_name: &str) -> Result<Self> {
        let manifest = CdbManifest {
            version: 1,
            database_name: database_name.to_string(),
            created_at: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            modified_at: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            collection_count: 0,
            collections: Vec::new(),
            files: Vec::new(),
            stats: DatabaseStats {
                total_size: 0,
                total_documents: 0,
                active_connections: 0,
                last_compacted: None,
                last_backed_up: None,
            },
            metadata: std::collections::HashMap::new(),
        };
        
        let manager = Self {
            path: path.to_string(),
            manifest,
        };
        
        manager.save()?;
        Ok(manager)
    }
    
    /// Load an existing manifest
    pub fn load(path: &str) -> Result<Self> {
        if !Path::new(path).exists() {
            return Err(CortexError::Storage(StorageError::ReadError(format!("Manifest file not found: {}", path)));
        }
        
        let mut file = File::open(path)
            .map_err(|e| CortexError::Storage(StorageError::ReadError(e.to_string())))?;
        
        let mut content = String::new();
        file.read_to_string(&mut content)
            .map_err(|e| CortexError::Storage(StorageError::ReadError(e.to_string())))?;
        
        let manifest: CdbManifest = serde_json::from_str(&content)
            .map_err(|e| CortexError::Storage(StorageError::ReadError(e.to_string())))?;
        
        Ok(Self {
            path: path.to_string(),
            manifest,
        })
    }
    
    /// Save the manifest to file
    pub fn save(&self) -> Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)
            .map_err(|e| CortexError::Storage(StorageError::WriteError(e.to_string())))?;
        
        let content = serde_json::to_string_pretty(&self.manifest)
            .map_err(|e| CortexError::Storage(StorageError::WriteError(e.to_string())))?;
        
        file.write_all(content.as_bytes())
            .map_err(|e| CortexError::Storage(StorageError::WriteError(e.to_string())))?;
        
        Ok(())
    }
    
    /// Add a collection to the manifest
    pub fn add_collection(&mut self, name: &str) -> Result<()> {
        // Check if collection already exists
        if self.manifest.collections.iter().any(|c| c.name == name) {
            return Err(CortexError::Storage(StorageError::CollectionAlreadyExists(name.to_string())));
        }
        
        let collection = CollectionInfo {
            name: name.to_string(),
            document_count: 0,
            size_bytes: 0,
            created_at: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            modified_at: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            schema_version: 1,
            compressed: false,
            encrypted: false,
        };
        
        self.manifest.collections.push(collection);
        self.manifest.collection_count += 1;
        self.manifest.modified_at = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        self.save()?;
        Ok(())
    }
    
    /// Remove a collection from the manifest
    pub fn remove_collection(&mut self, name: &str) -> Result<()> {
        let collection_index = self.manifest.collections.iter().position(|c| c.name == name)
            .ok_or_else(|| CortexError::Storage(StorageError::CollectionNotFound(name.to_string())))?;
        
        self.manifest.collections.remove(collection_index);
        self.manifest.collection_count -= 1;
        self.manifest.modified_at = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        self.save()?;
        Ok(())
    }
    
    /// Add a file to the manifest
    pub fn add_file(&mut self, name: &str, path: &str, file_type: &str) -> Result<()> {
        let file_info = FileInfo {
            name: name.to_string(),
            path: path.to_string(),
            size_bytes: Path::new(path).metadata()
                .map(|m| m.len())
                .unwrap_or(0),
            modified_at: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            checksum: "".to_string(), // In a real implementation, we would calculate the checksum
            file_type: file_type.to_string(),
            status: "active".to_string(),
        };
        
        self.manifest.files.push(file_info);
        self.manifest.modified_at = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        self.update_stats()?;
        self.save()?;
        Ok(())
    }
    
    /// Remove a file from the manifest
    pub fn remove_file(&mut self, name: &str) -> Result<()> {
        let file_index = self.manifest.files.iter().position(|f| f.name == name)
            .ok_or_else(|| CortexError::Storage(StorageError::ReadError(format!("File not found in manifest: {}", name)))?;
        
        self.manifest.files.remove(file_index);
        self.manifest.modified_at = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        self.update_stats()?;
        self.save()?;
        Ok(())
    }
    
    /// Update collection statistics
    pub fn update_collection_stats(&mut self, name: &str, document_count: u64, size_bytes: u64) -> Result<()> {
        let collection = self.manifest.collections.iter_mut()
            .find(|c| c.name == name)
            .ok_or_else(|| CortexError::Storage(StorageError::CollectionNotFound(name.to_string())))?;
        
        collection.document_count = document_count;
        collection.size_bytes = size_bytes;
        collection.modified_at = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        self.manifest.modified_at = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        self.update_stats()?;
        self.save()?;
        Ok(())
    }
    
    /// Update database statistics
    pub fn update_stats(&mut self) -> Result<()> {
        // Calculate total documents
        let total_documents: u64 = self.manifest.collections.iter()
            .map(|c| c.document_count)
            .sum();
        
        // Calculate total size
        let total_size: u64 = self.manifest.files.iter()
            .map(|f| f.size_bytes)
            .sum();
        
        self.manifest.stats = DatabaseStats {
            total_size,
            total_documents,
            active_connections: 0,
            last_compacted: None,
            last_backed_up: None,
        };
        
        Ok(())
    }
    
    /// Get the manifest
    pub fn get_manifest(&self) -> &CdbManifest {
        &self.manifest
    }
    
    /// Get collection information
    pub fn get_collection(&self, name: &str) -> Option<&CollectionInfo> {
        self.manifest.collections.iter().find(|c| c.name == name)
    }
    
    /// Get file information
    pub fn get_file(&self, name: &str) -> Option<&FileInfo> {
        self.manifest.files.iter().find(|f| f.name == name)
    }
    
    /// Set custom metadata
    pub fn set_metadata(&mut self, key: &str, value: &str) -> Result<()> {
        self.manifest.metadata.insert(key.to_string(), value.to_string());
        self.manifest.modified_at = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        self.save()?;
        Ok(())
    }
    
    /// Get custom metadata
    pub fn get_metadata(&self, key: &str) -> Option<&String> {
        self.manifest.metadata.get(key)
    }
}

/// Default manifest version
pub const MANIFEST_VERSION: u32 = 1;

/// Create a default manifest
pub fn create_default_manifest(database_name: &str) -> CdbManifest {
    CdbManifest {
        version: MANIFEST_VERSION,
        database_name: database_name.to_string(),
        created_at: SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
        modified_at: SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
        collection_count: 0,
        collections: Vec::new(),
        files: Vec::new(),
        stats: DatabaseStats {
            total_size: 0,
            total_documents: 0,
            active_connections: 0,
            last_compacted: None,
            last_backed_up: None,
        },
        metadata: std::collections::HashMap::new(),
    }
}