//! CDB file format definition
//! 
//! This module defines the binary file format for CortexDB's custom .cdb files,
//! including the file header, block structure, and data layout.

use crate::cortex_core::{Vector, Document, CollectionSchema, Result, CortexError, StorageError};
use serde::{Serialize, Deserialize};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write, Seek, SeekFrom};
use std::path::Path;
use std::time::SystemTime;

/// CDB file magic number
pub const CDB_MAGIC: [u8; 8] = *b"CortexDB";

/// CDB file version
pub const CDB_VERSION: u32 = 1;

/// CDB file header structure
#[derive(Debug, Serialize, Deserialize)]
pub struct CdbFileHeader {
    /// Magic number: "CortexDB"
    pub magic: [u8; 8],
    /// File format version
    pub version: u32,
    /// Creation timestamp (UNIX seconds)
    pub created_at: u64,
    /// Last modified timestamp (UNIX seconds)
    pub modified_at: u64,
    /// Number of collections
    pub collection_count: u32,
    /// Offset to collection index
    pub collection_index_offset: u64,
    /// Offset to data section
    pub data_section_offset: u64,
    /// Total file size
    pub file_size: u64,
    /// Flags (compression, encryption, etc.)
    pub flags: u32,
    /// Checksum of the header
    pub header_checksum: u32,
}

/// CDB file flags
pub mod flags {
    /// Enable compression
    pub const COMPRESSION: u32 = 1 << 0;
    /// Enable encryption
    pub const ENCRYPTION: u32 = 1 << 1;
    /// Read-only mode
    pub const READ_ONLY: u32 = 1 << 2;
    /// Transactional mode
    pub const TRANSACTIONAL: u32 = 1 << 3;
    /// Journaling enabled
    pub const JOURNALING: u32 = 1 << 4;
}

/// Collection index entry
#[derive(Debug, Serialize, Deserialize)]
pub struct CollectionIndexEntry {
    /// Collection name
    pub name: String,
    /// Offset to collection schema
    pub schema_offset: u64,
    /// Offset to collection data
    pub data_offset: u64,
    /// Number of documents
    pub document_count: u64,
    /// Collection size in bytes
    pub size_bytes: u64,
    /// Flags
    pub flags: u32,
}

/// Document entry in collection
#[derive(Debug, Serialize, Deserialize)]
pub struct DocumentEntry {
    /// Document ID
    pub document_id: String,
    /// Offset to document data
    pub offset: u64,
    /// Size of document data
    pub size: u32,
    /// Compression flag
    pub compressed: bool,
    /// Encryption flag
    pub encrypted: bool,
}

/// CDB file structure
#[derive(Debug)]
pub struct CdbFile {
    /// File handle
    file: File,
    /// File header
    header: CdbFileHeader,
    /// Collection index
    collection_index: Vec<CollectionIndexEntry>,
    /// Path to the file
    path: String,
}

impl CdbFile {
    /// Create a new CDB file
    pub fn create(path: &str) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .truncate(true)
            .open(path)
            .map_err(|e| CortexError::Storage(StorageError::WriteError(e.to_string())))?;
        
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        let header = CdbFileHeader {
            magic: CDB_MAGIC,
            version: CDB_VERSION,
            created_at: now,
            modified_at: now,
            collection_count: 0,
            collection_index_offset: std::mem::size_of::<CdbFileHeader>() as u64,
            data_section_offset: std::mem::size_of::<CdbFileHeader>() as u64,
            file_size: std::mem::size_of::<CdbFileHeader>() as u64,
            flags: 0,
            header_checksum: 0,
        };
        
        let mut cdb_file = Self {
            file,
            header,
            collection_index: Vec::new(),
            path: path.to_string(),
        };
        
        // Write header
        cdb_file.write_header()?;
        
        Ok(cdb_file)
    }
    
    /// Open an existing CDB file
    pub fn open(path: &str) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|e| CortexError::Storage(StorageError::ReadError(e.to_string())))?;
        
        let mut cdb_file = Self {
            file,
            header: CdbFileHeader::default(),
            collection_index: Vec::new(),
            path: path.to_string(),
        };
        
        // Read header
        cdb_file.read_header()?;
        
        // Read collection index
        cdb_file.read_collection_index()?;
        
        Ok(cdb_file)
    }
    
    /// Read file header
    fn read_header(&mut self) -> Result<()> {
        self.file.seek(SeekFrom::Start(0))
            .map_err(|e| CortexError::Storage(StorageError::ReadError(e.to_string())))?;
        
        let mut header_bytes = vec![0; std::mem::size_of::<CdbFileHeader>()];
        self.file.read_exact(&mut header_bytes)
            .map_err(|e| CortexError::Storage(StorageError::ReadError(e.to_string())))?;
        
        let header: CdbFileHeader = bincode::deserialize(&header_bytes)
            .map_err(|e| CortexError::Storage(StorageError::ReadError(e.to_string())))?;
        
        // Verify magic number
        if header.magic != CDB_MAGIC {
            return Err(CortexError::Storage(StorageError::ReadError("Invalid CDB file: wrong magic number".to_string())));
        }
        
        // Verify version
        if header.version != CDB_VERSION {
            return Err(CortexError::Storage(StorageError::ReadError(format!("Unsupported CDB version: {}", header.version))));
        }
        
        self.header = header;
        Ok(())
    }
    
    /// Write file header
    fn write_header(&mut self) -> Result<()> {
        self.file.seek(SeekFrom::Start(0))
            .map_err(|e| CortexError::Storage(StorageError::WriteError(e.to_string())))?;
        
        // Update modified timestamp
        self.header.modified_at = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        // Calculate checksum
        let header_bytes = bincode::serialize(&self.header)
            .map_err(|e| CortexError::Storage(StorageError::WriteError(e.to_string())))?;
        
        self.file.write_all(&header_bytes)
            .map_err(|e| CortexError::Storage(StorageError::WriteError(e.to_string())))?;
        
        Ok(())
    }
    
    /// Read collection index
    fn read_collection_index(&mut self) -> Result<()> {
        self.file.seek(SeekFrom::Start(self.header.collection_index_offset))
            .map_err(|e| CortexError::Storage(StorageError::ReadError(e.to_string())))?;
        
        let mut index_bytes = vec![0; (self.header.data_section_offset - self.header.collection_index_offset) as usize];
        self.file.read_exact(&mut index_bytes)
            .map_err(|e| CortexError::Storage(StorageError::ReadError(e.to_string())))?;
        
        self.collection_index = bincode::deserialize(&index_bytes)
            .map_err(|e| CortexError::Storage(StorageError::ReadError(e.to_string())))?;
        
        Ok(())
    }
    
    /// Write collection index
    fn write_collection_index(&mut self) -> Result<()> {
        self.file.seek(SeekFrom::Start(self.header.collection_index_offset))
            .map_err(|e| CortexError::Storage(StorageError::WriteError(e.to_string())))?;
        
        let index_bytes = bincode::serialize(&self.collection_index)
            .map_err(|e| CortexError::Storage(StorageError::WriteError(e.to_string())))?;
        
        self.file.write_all(&index_bytes)
            .map_err(|e| CortexError::Storage(StorageError::WriteError(e.to_string())))?;
        
        // Update header
        self.header.collection_count = self.collection_index.len() as u32;
        self.header.data_section_offset = self.header.collection_index_offset + index_bytes.len() as u64;
        
        self.write_header()?;
        Ok(())
    }
    
    /// Add a collection to the file
    pub fn add_collection(&mut self, name: &str, schema: &CollectionSchema) -> Result<()> {
        // Check if collection already exists
        if self.collection_index.iter().any(|entry| entry.name == name) {
            return Err(CortexError::Storage(StorageError::CollectionAlreadyExists(name.to_string())));
        }
        
        // Write collection schema
        let schema_offset = self.file.seek(SeekFrom::End(0))
            .map_err(|e| CortexError::Storage(StorageError::WriteError(e.to_string())))?;
        
        let schema_bytes = bincode::serialize(schema)
            .map_err(|e| CortexError::Storage(StorageError::WriteError(e.to_string())))?;
        
        self.file.write_all(&schema_bytes)
            .map_err(|e| CortexError::Storage(StorageError::WriteError(e.to_string())))?;
        
        // Add to collection index
        let entry = CollectionIndexEntry {
            name: name.to_string(),
            schema_offset,
            data_offset: self.file.stream_position()
                .map_err(|e| CortexError::Storage(StorageError::WriteError(e.to_string())))?,
            document_count: 0,
            size_bytes: 0,
            flags: 0,
        };
        
        self.collection_index.push(entry);
        self.write_collection_index()?;
        
        Ok(())
    }
    
    /// Add a document to a collection
    pub fn add_document(&mut self, collection_name: &str, document: &Document) -> Result<()> {
        // Find collection
        let collection_index = self.collection_index.iter().position(|entry| entry.name == collection_name)
            .ok_or_else(|| CortexError::Storage(StorageError::CollectionNotFound(collection_name.to_string())))?;
        
        let collection = &mut self.collection_index[collection_index];
        
        // Write document
        let document_offset = self.file.seek(SeekFrom::End(0))
            .map_err(|e| CortexError::Storage(StorageError::WriteError(e.to_string())))?;
        
        let document_bytes = bincode::serialize(document)
            .map_err(|e| CortexError::Storage(StorageError::WriteError(e.to_string())))?;
        
        self.file.write_all(&document_bytes)
            .map_err(|e| CortexError::Storage(StorageError::WriteError(e.to_string())))?;
        
        // Update collection info
        collection.document_count += 1;
        collection.size_bytes += document_bytes.len() as u64;
        
        self.write_collection_index()?;
        
        Ok(())
    }
    
    /// Get a document from a collection
    pub fn get_document(&mut self, collection_name: &str, document_id: &str) -> Result<Option<Document>> {
        // Find collection
        let collection = self.collection_index.iter()
            .find(|entry| entry.name == collection_name)
            .ok_or_else(|| CortexError::Storage(StorageError::CollectionNotFound(collection_name.to_string())))?;
        
        // In a real implementation, we would have a document index
        // For now, we'll just return None as a placeholder
        Ok(None)
    }
    
    /// List collections in the file
    pub fn list_collections(&self) -> Result<Vec<String>> {
        Ok(self.collection_index.iter().map(|entry| entry.name.clone()).collect())
    }
    
    /// Close the CDB file
    pub fn close(self) -> Result<()> {
        // The file will be closed automatically when dropped
        Ok(())
    }
}

/// Default implementation for CdbFileHeader
impl Default for CdbFileHeader {
    fn default() -> Self {
        Self {
            magic: CDB_MAGIC,
            version: CDB_VERSION,
            created_at: 0,
            modified_at: 0,
            collection_count: 0,
            collection_index_offset: std::mem::size_of::<CdbFileHeader>() as u64,
            data_section_offset: std::mem::size_of::<CdbFileHeader>() as u64,
            file_size: std::mem::size_of::<CdbFileHeader>() as u64,
            flags: 0,
            header_checksum: 0,
        }
    }
}

/// CDB file utilities
pub mod utils {
    use super::*;
    
    /// Check if a file is a valid CDB file
    pub fn is_cdb_file(path: &str) -> bool {
        let path = Path::new(path);
        if !path.exists() || !path.is_file() {
            return false;
        }
        
        let mut file = match File::open(path) {
            Ok(file) => file,
            Err(_) => return false,
        };
        
        let mut magic = [0; 8];
        if file.read_exact(&mut magic).is_err() {
            return false;
        }
        
        magic == CDB_MAGIC
    }
    
    /// Get CDB file info
    pub fn get_file_info(path: &str) -> Result<CdbFileHeader> {
        let mut file = File::open(path)
            .map_err(|e| CortexError::Storage(StorageError::ReadError(e.to_string())))?;
        
        let mut header_bytes = vec![0; std::mem::size_of::<CdbFileHeader>()];
        file.read_exact(&mut header_bytes)
            .map_err(|e| CortexError::Storage(StorageError::ReadError(e.to_string())))?;
        
        let header: CdbFileHeader = bincode::deserialize(&header_bytes)
            .map_err(|e| CortexError::Storage(StorageError::ReadError(e.to_string())))?;
        
        Ok(header)
    }
}