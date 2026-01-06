//! CDB file compression support
//! 
//! This module provides compression functionality for CDB files, supporting
//! multiple compression algorithms and levels.

use crate::cortex_core::{Result, CortexError, StorageError};
use serde::{Serialize, Deserialize};
use std::io::{Read, Write};
use flate2::{Compression, Compress, Decompress, FlushCompress, FlushDecompress};

/// Compression algorithm types
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
pub enum CompressionAlgorithm {
    /// No compression
    None,
    /// LZ4 compression (fast)
    Lz4,
    /// Snappy compression (balanced)
    Snappy,
    /// Gzip compression (high compression)
    Gzip,
    /// Zstd compression (modern, good balance)
    Zstd,
}

/// Compression level
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
pub enum CompressionLevel {
    /// No compression
    None,
    /// Fast compression (low compression ratio)
    Fast,
    /// Balanced compression
    Medium,
    /// High compression (slow)
    High,
    /// Maximum compression (very slow)
    Maximum,
}

/// Compression statistics
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CompressionStats {
    /// Original size in bytes
    pub original_size: u64,
    /// Compressed size in bytes
    pub compressed_size: u64,
    /// Compression ratio
    pub compression_ratio: f64,
    /// Compression time in milliseconds
    pub compression_time_ms: f64,
    /// Decompression time in milliseconds
    pub decompression_time_ms: f64,
    /// Algorithm used
    pub algorithm: CompressionAlgorithm,
    /// Compression level used
    pub level: CompressionLevel,
}

/// Compressor trait
pub trait Compressor {
    /// Compress data
    fn compress(&mut self, input: &[u8]) -> Result<Vec<u8>>;
    
    /// Compress data with specified level
    fn compress_with_level(&mut self, input: &[u8], level: CompressionLevel) -> Result<Vec<u8>>;
    
    /// Get compression statistics
    fn get_stats(&self) -> CompressionStats;
}

/// Decompressor trait
pub trait Decompressor {
    /// Decompress data
    fn decompress(&mut self, input: &[u8]) -> Result<Vec<u8>>;
    
    /// Get decompression statistics
    fn get_stats(&self) -> CompressionStats;
}

/// Gzip compressor implementation
#[derive(Debug)]
pub struct GzipCompressor {
    stats: CompressionStats,
}

impl GzipCompressor {
    /// Create a new Gzip compressor
    pub fn new() -> Self {
        Self {
            stats: CompressionStats {
                original_size: 0,
                compressed_size: 0,
                compression_ratio: 0.0,
                compression_time_ms: 0.0,
                decompression_time_ms: 0.0,
                algorithm: CompressionAlgorithm::Gzip,
                level: CompressionLevel::Medium,
            },
        }
    }
}

impl Compressor for GzipCompressor {
    fn compress(&mut self, input: &[u8]) -> Result<Vec<u8>> {
        self.compress_with_level(input, CompressionLevel::Medium)
    }
    
    fn compress_with_level(&mut self, input: &[u8], level: CompressionLevel) -> Result<Vec<u8>> {
        let start_time = std::time::Instant::now();
        
        let compression_level = match level {
            CompressionLevel::None => Compression::none(),
            CompressionLevel::Fast => Compression::fast(),
            CompressionLevel::Medium => Compression::default(),
            CompressionLevel::High => Compression::best(),
            CompressionLevel::Maximum => Compression::best(),
        };
        
        let mut compressor = Compress::new(compression_level, false);
        let mut output = Vec::with_capacity(input.len() / 2);
        let mut input_bytes = input;
        
        loop {
            let (status, input_used, output_used) = compressor.compress(input_bytes, &mut output, FlushCompress::None);
            input_bytes = &input_bytes[input_used..];
            
            if input_bytes.is_empty() {
                compressor.compress(input_bytes, &mut output, FlushCompress::Finish).unwrap();
                break;
            }
        }
        
        let elapsed = start_time.elapsed().as_secs_f64() * 1000.0;
        
        // Update stats
        self.stats.original_size = input.len() as u64;
        self.stats.compressed_size = output.len() as u64;
        self.stats.compression_ratio = if input.len() > 0 {
            output.len() as f64 / input.len() as f64
        } else {
            1.0
        };
        self.stats.compression_time_ms = elapsed;
        self.stats.level = level;
        
        Ok(output)
    }
    
    fn get_stats(&self) -> CompressionStats {
        self.stats.clone()
    }
}

/// Gzip decompressor implementation
#[derive(Debug)]
pub struct GzipDecompressor {
    stats: CompressionStats,
}

impl GzipDecompressor {
    /// Create a new Gzip decompressor
    pub fn new() -> Self {
        Self {
            stats: CompressionStats {
                original_size: 0,
                compressed_size: 0,
                compression_ratio: 0.0,
                compression_time_ms: 0.0,
                decompression_time_ms: 0.0,
                algorithm: CompressionAlgorithm::Gzip,
                level: CompressionLevel::Medium,
            },
        }
    }
}

impl Decompressor for GzipDecompressor {
    fn decompress(&mut self, input: &[u8]) -> Result<Vec<u8>> {
        let start_time = std::time::Instant::now();
        
        let mut decompressor = Decompress::new(false);
        let mut output = Vec::with_capacity(input.len() * 2);
        let mut input_bytes = input;
        
        loop {
            let (status, input_used, output_used) = decompressor.decompress(input_bytes, &mut output, FlushDecompress::None);
            input_bytes = &input_bytes[input_used..];
            
            if input_bytes.is_empty() {
                decompressor.decompress(input_bytes, &mut output, FlushDecompress::Finish).unwrap();
                break;
            }
        }
        
        let elapsed = start_time.elapsed().as_secs_f64() * 1000.0;
        
        // Update stats
        self.stats.compressed_size = input.len() as u64;
        self.stats.original_size = output.len() as u64;
        self.stats.compression_ratio = if output.len() > 0 {
            input.len() as f64 / output.len() as f64
        } else {
            1.0
        };
        self.stats.decompression_time_ms = elapsed;
        
        Ok(output)
    }
    
    fn get_stats(&self) -> CompressionStats {
        self.stats.clone()
    }
}

/// Compression utilities
pub struct CompressionUtils;

impl CompressionUtils {
    /// Compress data with specified algorithm and level
    pub fn compress(data: &[u8], algorithm: CompressionAlgorithm, level: CompressionLevel) -> Result<Vec<u8>> {
        match algorithm {
            CompressionAlgorithm::None => Ok(data.to_vec()),
            CompressionAlgorithm::Gzip => {
                let mut compressor = GzipCompressor::new();
                compressor.compress_with_level(data, level)
            }
            // TODO: Implement other compression algorithms
            _ => Err(CortexError::Storage(StorageError::WriteError(format!("Compression algorithm not implemented: {:?}", algorithm))),
        }
    }
    
    /// Decompress data with specified algorithm
    pub fn decompress(data: &[u8], algorithm: CompressionAlgorithm) -> Result<Vec<u8>> {
        match algorithm {
            CompressionAlgorithm::None => Ok(data.to_vec()),
            CompressionAlgorithm::Gzip => {
                let mut decompressor = GzipDecompressor::new();
                decompressor.decompress(data)
            }
            // TODO: Implement other decompression algorithms
            _ => Err(CortexError::Storage(StorageError::ReadError(format!("Decompression algorithm not implemented: {:?}", algorithm))),
        }
    }
    
    /// Get compression level for flate2
    pub fn get_flate2_level(level: CompressionLevel) -> Compression {
        match level {
            CompressionLevel::None => Compression::none(),
            CompressionLevel::Fast => Compression::fast(),
            CompressionLevel::Medium => Compression::default(),
            CompressionLevel::High => Compression::best(),
            CompressionLevel::Maximum => Compression::best(),
        }
    }
    
    /// Calculate compression ratio
    pub fn calculate_compression_ratio(original_size: u64, compressed_size: u64) -> f64 {
        if original_size == 0 {
            1.0
        } else {
            compressed_size as f64 / original_size as f64
        }
    }
    
    /// Calculate compression savings percentage
    pub fn calculate_savings_percentage(original_size: u64, compressed_size: u64) -> f64 {
        if original_size == 0 {
            0.0
        } else {
            (1.0 - (compressed_size as f64 / original_size as f64)) * 100.0
        }
    }
}

/// Compression header for compressed data
#[derive(Debug, Serialize, Deserialize)]
pub struct CompressionHeader {
    /// Magic number for compression header
    pub magic: [u8; 4],
    /// Compression algorithm
    pub algorithm: CompressionAlgorithm,
    /// Compression level
    pub level: CompressionLevel,
    /// Original size in bytes
    pub original_size: u64,
    /// Compressed size in bytes
    pub compressed_size: u64,
    /// Checksum of compressed data
    pub checksum: u32,
}

/// Compression header magic number
pub const COMPRESSION_MAGIC: [u8; 4] = *b"CDBC";

/// Create a compression header
pub fn create_compression_header(
    algorithm: CompressionAlgorithm,
    level: CompressionLevel,
    original_size: u64,
    compressed_size: u64,
) -> CompressionHeader {
    CompressionHeader {
        magic: COMPRESSION_MAGIC,
        algorithm,
        level,
        original_size,
        compressed_size,
        checksum: 0, // TODO: Implement checksum calculation
    }
}

/// Check if data is compressed (has compression header)
pub fn is_compressed(data: &[u8]) -> bool {
    data.len() >= 4 && &data[0..4] == &COMPRESSION_MAGIC
}