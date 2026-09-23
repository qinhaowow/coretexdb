//! Data Compression module for CoreTexDB
//! Provides compression for vector data storage with storage layer integration

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

pub trait CompressionAlgorithm: Send + Sync {
    fn name(&self) -> &str;
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>, String>;
    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>, String>;
    fn compression_ratio(&self, original: &[u8], compressed: &[u8]) -> f64;
}

pub struct NoCompression;

impl CompressionAlgorithm for NoCompression {
    fn name(&self) -> &str { "none" }
    
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>, String> {
        Ok(data.to_vec())
    }
    
    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>, String> {
        Ok(data.to_vec())
    }
    
    fn compression_ratio(&self, original: &[u8], compressed: &[u8]) -> f64 {
        if original.is_empty() { return 1.0; }
        original.len() as f64 / compressed.len() as f64
    }
}

pub struct VectorCompressor {
    algorithm: Box<dyn CompressionAlgorithm>,
    compressed_data: Arc<RwLock<HashMap<String, CompressedVector>>>,
    original_size: Arc<RwLock<usize>>,
    compressed_size: Arc<RwLock<usize>>,
}

#[derive(Debug, Clone)]
pub struct CompressedVector {
    pub id: String,
    pub data: Vec<u8>,
    pub original_size: usize,
    pub compression_ratio: f64,
}

impl VectorCompressor {
    pub fn new(algorithm: Box<dyn CompressionAlgorithm>) -> Self {
        Self {
            algorithm,
            compressed_data: Arc::new(RwLock::new(HashMap::new())),
            original_size: Arc::new(RwLock::new(0)),
            compressed_size: Arc::new(RwLock::new(0)),
        }
    }

    pub fn with_lz4() -> Self {
        Self::new(Box::new(LZ4Compression))
    }

    pub fn with_zstd() -> Self {
        Self::new(Box::new(ZstdCompression))
    }

    pub fn with_snappy() -> Self {
        Self::new(Box::new(SnappyCompression))
    }

    pub fn with_deflate() -> Self {
        Self::new(Box::new(DeflateCompression))
    }

    pub fn algorithm(&self) -> &dyn CompressionAlgorithm {
        self.algorithm.as_ref()
    }

    pub async fn compress_vector(&self, id: &str, vector: &[f32]) -> Result<CompressedVector, String> {
        let bytes: Vec<u8> = vector.iter()
            .flat_map(|f| f.to_le_bytes())
            .collect();
        
        let original_size = bytes.len();
        
        let compressed = self.algorithm.compress(&bytes)?;
        let compressed_len = compressed.len();
        
        let compression_ratio = if original_size > 0 {
            original_size as f64 / compressed_len as f64
        } else {
            1.0
        };
        
        let cv = CompressedVector {
            id: id.to_string(),
            data: compressed,
            original_size,
            compression_ratio,
        };
        
        {
            let mut data = self.compressed_data.write().await;
            data.insert(id.to_string(), cv.clone());
        }
        
        {
            let mut orig = self.original_size.write().await;
            *orig += original_size;
        }
        {
            let mut comp = self.compressed_size.write().await;
            *comp += compressed_len;
        }
        
        Ok(cv)
    }

    pub async fn decompress_vector(&self, id: &str) -> Result<Vec<f32>, String> {
        let data = {
            let compressed_data = self.compressed_data.read().await;
            compressed_data.get(id).cloned()
        };
        
        match data {
            Some(cv) => {
                let decompressed = self.algorithm.decompress(&cv.data)?;
                
                let floats: Vec<f32> = decompressed
                    .chunks_exact(4)
                    .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
                    .collect();
                
                Ok(floats)
            }
            None => Err(format!("Vector '{}' not found", id)),
        }
    }

    pub async fn get_compression_stats(&self) -> CompressionStats {
        let original = *self.original_size.read().await;
        let compressed = *self.compressed_size.read().await;
        
        CompressionStats {
            algorithm: self.algorithm.name().to_string(),
            original_size: original,
            compressed_size: compressed,
            compression_ratio: if original > 0 { original as f64 / compressed as f64 } else { 1.0 },
            vector_count: self.compressed_data.read().await.len(),
        }
    }

    pub async fn remove(&self, id: &str) -> bool {
        let mut data = self.compressed_data.write().await;
        data.remove(id).is_some()
    }

    pub async fn clear(&self) {
        let mut data = self.compressed_data.write().await;
        data.clear();
        *self.original_size.write().await = 0;
        *self.compressed_size.write().await = 0;
    }
}

#[derive(Debug, Clone)]
pub struct CompressionStats {
    pub algorithm: String,
    pub original_size: usize,
    pub compressed_size: usize,
    pub compression_ratio: f64,
    pub vector_count: usize,
}

// ═══════════════════════════════════════════════════════════════
// 压缩与存储层集成
// ═══════════════════════════════════════════════════════════════

use crate::coretex_lakehouse::StorageBackendTrait;

/// Compressed storage wrapper that integrates compression with any storage backend
pub struct CompressedStorage {
    backend: Box<dyn StorageBackendTrait>,
    compressor: VectorCompressor,
}

impl CompressedStorage {
    pub fn new(backend: Box<dyn StorageBackendTrait>, compressor: VectorCompressor) -> Self {
        Self { backend, compressor }
    }

    /// Write a vector with compression
    pub async fn write_vector(&self, key: &str, vector: &[f32]) -> Result<(), String> {
        // Serialize vector to bytes
        let bytes: Vec<u8> = vector.iter()
            .flat_map(|f| f.to_le_bytes())
            .collect();
        
        // Compress
        let compressed = self.compressor.algorithm().compress(&bytes)?;
        
        // Store compression metadata + compressed data
        let mut payload = Vec::new();
        // Write original size (4 bytes) so we know how to decompress
        payload.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        // Write compression algorithm name length + name
        let algo_name = self.compressor.algorithm().name().as_bytes();
        payload.push(algo_name.len() as u8);
        payload.extend_from_slice(algo_name);
        // Write compressed data
        payload.extend_from_slice(&compressed);
        
        self.backend.write(key, &payload)
    }

    /// Read a vector with decompression
    pub async fn read_vector(&self, key: &str) -> Result<Vec<f32>, String> {
        let payload = self.backend.read(key)?;
        
        if payload.len() < 5 {
            return Err("Invalid compressed payload".to_string());
        }
        
        // Read original size
        let original_size = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]) as usize;
        
        // Read algorithm name
        let algo_len = payload[4] as usize;
        if payload.len() < 5 + algo_len {
            return Err("Invalid algorithm name".to_string());
        }
        let _algo_name = std::str::from_utf8(&payload[5..5 + algo_len])
            .map_err(|e| format!("Invalid algorithm name: {}", e))?;
        
        // Read compressed data
        let compressed_data = &payload[5 + algo_len..];
        
        // Decompress
        let decompressed = self.compressor.algorithm().decompress(compressed_data)?;
        
        // Convert bytes to f32 vector
        let floats: Vec<f32> = decompressed
            .chunks_exact(4)
            .take(original_size / 4)
            .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
            .collect();
        
        Ok(floats)
    }

    /// Delete a compressed vector
    pub fn delete_vector(&self, key: &str) -> Result<(), String> {
        self.backend.delete(key)
    }

    /// Check if a compressed vector exists
    pub fn exists(&self, key: &str) -> bool {
        self.backend.exists(key)
    }

    /// List all compressed vectors with a given prefix
    pub fn list_vectors(&self, prefix: &str) -> Result<Vec<String>, String> {
        self.backend.list(prefix)
    }

    /// Get compression statistics
    pub async fn stats(&self) -> CompressionStats {
        self.compressor.get_compression_stats().await
    }
}

/// Compression factory for creating compressors with different algorithms
pub struct CompressionFactory;

impl CompressionFactory {
    pub fn create(algorithm: &str) -> Box<dyn CompressionAlgorithm> {
        match algorithm.to_lowercase().as_str() {
            "lz4" => Box::new(LZ4Compression),
            "zstd" | "zstandard" => Box::new(ZstdCompression),
            "snappy" => Box::new(SnappyCompression),
            "deflate" | "gzip" => Box::new(DeflateCompression),
            "rle" => Box::new(RleCompression),
            "delta" => Box::new(DeltaCompression),
            _ => Box::new(NoCompression),
        }
    }

    pub fn create_compressor(algorithm: &str) -> VectorCompressor {
        VectorCompressor::new(Self::create(algorithm))
    }

    pub fn create_compressed_storage(
        backend: Box<dyn StorageBackendTrait>,
        algorithm: &str,
    ) -> CompressedStorage {
        CompressedStorage::new(backend, Self::create_compressor(algorithm))
    }
}

struct RleCompression;

impl CompressionAlgorithm for RleCompression {
    fn name(&self) -> &str { "rle" }
    
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>, String> {
        Ok(RunLengthEncoding::compress(data))
    }
    
    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>, String> {
        Ok(RunLengthEncoding::decompress(data))
    }
    
    fn compression_ratio(&self, original: &[u8], compressed: &[u8]) -> f64 {
        if original.is_empty() { return 1.0; }
        original.len() as f64 / compressed.len().max(1) as f64
    }
}

struct DeltaCompression;

impl CompressionAlgorithm for DeltaCompression {
    fn name(&self) -> &str { "delta" }
    
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>, String> {
        // Treat bytes as f32 array and apply delta encoding
        if data.len() % 4 != 0 {
            return Err("Data length must be multiple of 4 for delta encoding".to_string());
        }
            let floats: Vec<f32> = data
                .chunks_exact(4)
                .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
                .collect();
            Ok(DeltaCoding::encode(&floats))
    }
    
    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>, String> {
        let floats = DeltaCoding::decode(data);
        Ok(floats.iter().flat_map(|f| f.to_le_bytes()).collect())
    }
    
    fn compression_ratio(&self, original: &[u8], compressed: &[u8]) -> f64 {
        if original.is_empty() { return 1.0; }
        original.len() as f64 / compressed.len().max(1) as f64
    }
}

struct DeflateCompression;

impl CompressionAlgorithm for DeflateCompression {
    fn name(&self) -> &str { "deflate" }
    
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>, String> {
        #[cfg(feature = "compression")]
        {
            use flate2::write::GzEncoder;
            use flate2::Compression;
            use std::io::Write;
            let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
            encoder.write_all(data).map_err(|e| e.to_string())?;
            encoder.finish().map_err(|e| e.to_string())
        }
        #[cfg(not(feature = "compression"))]
        {
            // Fallback: use RLE
            let rle = RunLengthEncoding::compress(data);
            if rle.len() < data.len() {
                let mut result = vec![1u8];
                result.extend_from_slice(&(rle.len() as u32).to_le_bytes());
                result.extend_from_slice(&rle);
                return Ok(result);
            }
            let mut result = vec![0u8];
            result.extend_from_slice(&(data.len() as u32).to_le_bytes());
            result.extend_from_slice(data);
            Ok(result)
        }
    }
    
    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>, String> {
        #[cfg(feature = "compression")]
        {
            use flate2::read::GzDecoder;
            use std::io::Read;
            let mut decoder = GzDecoder::new(data);
            let mut decompressed = Vec::new();
            decoder.read_to_end(&mut decompressed).map_err(|e| e.to_string())?;
            Ok(decompressed)
        }
        #[cfg(not(feature = "compression"))]
        {
            if data.is_empty() {
                return Err("Empty compressed data".to_string());
            }
            let mode = data[0];
            if data.len() < 5 {
                return Err("Invalid compressed data header".to_string());
            }
            let len = u32::from_le_bytes([data[1], data[2], data[3], data[4]]) as usize;
            if data.len() < 5 + len {
                return Err("Compressed data truncated".to_string());
            }
            let payload = &data[5..5 + len];
            if mode == 1 {
                Ok(RunLengthEncoding::decompress(payload))
            } else {
                Ok(payload.to_vec())
            }
        }
    }
    
    fn compression_ratio(&self, original: &[u8], compressed: &[u8]) -> f64 {
        if original.is_empty() { return 1.0; }
        original.len() as f64 / compressed.len().max(1) as f64
    }
}

struct LZ4Compression;

impl CompressionAlgorithm for LZ4Compression {
    fn name(&self) -> &str { "lz4" }
    
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>, String> {
        #[cfg(feature = "compression")]
        {
            Ok(lz4_flex::compress_prepend_size(data))
        }
        #[cfg(not(feature = "compression"))]
        {
            let rle = RunLengthEncoding::compress(data);
            if rle.len() < data.len() {
                let mut result = vec![1u8];
                result.extend_from_slice(&(rle.len() as u32).to_le_bytes());
                result.extend_from_slice(&rle);
                return Ok(result);
            }
            let mut result = vec![0u8];
            result.extend_from_slice(&(data.len() as u32).to_le_bytes());
            result.extend_from_slice(data);
            Ok(result)
        }
    }
    
    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>, String> {
        #[cfg(feature = "compression")]
        {
            lz4_flex::decompress_size_prepended(data).map_err(|e| e.to_string())
        }
        #[cfg(not(feature = "compression"))]
        {
            if data.is_empty() {
                return Err("Empty compressed data".to_string());
            }
            let mode = data[0];
            if data.len() < 5 {
                return Err("Invalid compressed data header".to_string());
            }
            let len = u32::from_le_bytes([data[1], data[2], data[3], data[4]]) as usize;
            if data.len() < 5 + len {
                return Err("Compressed data truncated".to_string());
            }
            let payload = &data[5..5 + len];
            if mode == 1 {
                Ok(RunLengthEncoding::decompress(payload))
            } else {
                Ok(payload.to_vec())
            }
        }
    }
    
    fn compression_ratio(&self, original: &[u8], compressed: &[u8]) -> f64 {
        if original.is_empty() { return 1.0; }
        original.len() as f64 / compressed.len().max(1) as f64
    }
}

struct ZstdCompression;

impl CompressionAlgorithm for ZstdCompression {
    fn name(&self) -> &str { "zstd" }
    
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>, String> {
        #[cfg(feature = "compression")]
        {
            zstd::encode_all(data, 3).map_err(|e| e.to_string())
        }
        #[cfg(not(feature = "compression"))]
        {
            let rle = RunLengthEncoding::compress(data);
            if rle.len() < data.len() {
                let mut result = vec![1u8];
                result.extend_from_slice(&(rle.len() as u32).to_le_bytes());
                result.extend_from_slice(&rle);
                return Ok(result);
            }
            let mut result = vec![0u8];
            result.extend_from_slice(&(data.len() as u32).to_le_bytes());
            result.extend_from_slice(data);
            Ok(result)
        }
    }
    
    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>, String> {
        #[cfg(feature = "compression")]
        {
            zstd::decode_all(data).map_err(|e| e.to_string())
        }
        #[cfg(not(feature = "compression"))]
        {
            if data.is_empty() {
                return Err("Empty compressed data".to_string());
            }
            let mode = data[0];
            if data.len() < 5 {
                return Err("Invalid compressed data header".to_string());
            }
            let len = u32::from_le_bytes([data[1], data[2], data[3], data[4]]) as usize;
            if data.len() < 5 + len {
                return Err("Compressed data truncated".to_string());
            }
            let payload = &data[5..5 + len];
            if mode == 1 {
                Ok(RunLengthEncoding::decompress(payload))
            } else {
                Ok(payload.to_vec())
            }
        }
    }
    
    fn compression_ratio(&self, original: &[u8], compressed: &[u8]) -> f64 {
        if original.is_empty() { return 1.0; }
        original.len() as f64 / compressed.len().max(1) as f64
    }
}

struct SnappyCompression;

impl CompressionAlgorithm for SnappyCompression {
    fn name(&self) -> &str { "snappy" }
    
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>, String> {
        #[cfg(feature = "compression")]
        {
            use snap::write::FrameEncoder;
            use std::io::Write;
            let mut encoder = FrameEncoder::new(Vec::new());
            encoder.write_all(data).map_err(|e| e.to_string())?;
            encoder.into_inner().map_err(|e| e.to_string())
        }
        #[cfg(not(feature = "compression"))]
        {
            let rle = RunLengthEncoding::compress(data);
            if rle.len() < data.len() {
                let mut result = vec![1u8];
                result.extend_from_slice(&(rle.len() as u32).to_le_bytes());
                result.extend_from_slice(&rle);
                return Ok(result);
            }
            let mut result = vec![0u8];
            result.extend_from_slice(&(data.len() as u32).to_le_bytes());
            result.extend_from_slice(data);
            Ok(result)
        }
    }
    
    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>, String> {
        #[cfg(feature = "compression")]
        {
            use snap::read::FrameDecoder;
            use std::io::Read;
            let mut decoder = FrameDecoder::new(data);
            let mut decompressed = Vec::new();
            decoder.read_to_end(&mut decompressed).map_err(|e| e.to_string())?;
            Ok(decompressed)
        }
        #[cfg(not(feature = "compression"))]
        {
            if data.is_empty() {
                return Err("Empty compressed data".to_string());
            }
            let mode = data[0];
            if data.len() < 5 {
                return Err("Invalid compressed data header".to_string());
            }
            let len = u32::from_le_bytes([data[1], data[2], data[3], data[4]]) as usize;
            if data.len() < 5 + len {
                return Err("Compressed data truncated".to_string());
            }
            let payload = &data[5..5 + len];
            if mode == 1 {
                Ok(RunLengthEncoding::decompress(payload))
            } else {
                Ok(payload.to_vec())
            }
        }
    }
    
    fn compression_ratio(&self, original: &[u8], compressed: &[u8]) -> f64 {
        if original.is_empty() { return 1.0; }
        original.len() as f64 / compressed.len().max(1) as f64
    }
}

pub struct RunLengthEncoding;

impl RunLengthEncoding {
    pub fn compress(data: &[u8]) -> Vec<u8> {
        if data.is_empty() {
            return vec![];
        }
        
        let mut result = Vec::new();
        let mut count = 1u32;
        
        for i in 1..data.len() {
            if data[i] == data[i - 1] && count < 255 {
                count += 1;
            } else {
                result.push(data[i - 1]);
                result.push(count as u8);
                count = 1;
            }
        }
        
        result.push(data[data.len() - 1]);
        result.push(count as u8);
        
        result
    }
    
    pub fn decompress(data: &[u8]) -> Vec<u8> {
        if data.len() < 2 {
            return data.to_vec();
        }
        
        let mut result = Vec::new();
        
        for chunk in data.chunks(2) {
            if chunk.len() == 2 {
                let value = chunk[0];
                let count = chunk[1] as usize;
                result.extend(std::iter::repeat_n(value, count));
            }
        }
        
        result
    }
}

pub struct DeltaCoding;

impl DeltaCoding {
    pub fn encode(values: &[f32]) -> Vec<u8> {
        if values.is_empty() {
            return vec![];
        }
        
        let mut result = Vec::new();
        
        let first_bytes = values[0].to_le_bytes();
        result.extend_from_slice(&first_bytes);
        
        for i in 1..values.len() {
            let delta = values[i] - values[i - 1];
            let delta_bytes = delta.to_le_bytes();
            result.extend_from_slice(&delta_bytes);
        }
        
        result
    }
    
    pub fn decode(data: &[u8]) -> Vec<f32> {
        if data.len() < 4 || !data.len().is_multiple_of(4) {
            return vec![];
        }
        
        let mut result = Vec::new();
        
        let first = f32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        result.push(first);
        
        for i in (4..data.len()).step_by(4) {
            let delta = f32::from_le_bytes([data[i], data[i + 1], data[i + 2], data[i + 3]]);
            let value = result.last().unwrap() + delta;
            result.push(value);
        }
        
        result
    }
}

pub struct QuantizationCompressor {
    precision: u8,
}

impl QuantizationCompressor {
    pub fn new(precision: u8) -> Self {
        Self {
            precision: precision.clamp(1, 32),
        }
    }

    pub fn quantize(&self, vector: &[f32]) -> Vec<u8> {
        let bits = self.precision as usize;
        let max_val = (1u64 << bits) - 1;
        
        let min_val = vector.iter().cloned().fold(f32::INFINITY, f32::min);
        let max_val_f = vector.iter().cloned().fold(f32::NEG_INFINITY, f32::max);
        let range = max_val_f - min_val;
        
        if range == 0.0 {
            return vec![0u8; vector.len()];
        }
        
        vector
            .iter()
            .map(|v| {
                let normalized = (v - min_val) / range;
                let quantized = (normalized * max_val as f32) as u64;
                quantized.min(max_val) as u8
            })
            .collect()
    }

    pub fn dequantize(&self, data: &[u8], original_len: usize) -> Vec<f32> {
        let bits = self.precision as usize;
        let max_val = (1u64 << bits) - 1;
        
        let _min_val = f32::MAX;
        let _max_val_f = f32::MIN;
        
        let reconstructed: Vec<f32> = data
            .iter()
            .take(original_len)
            .map(|&q| {
                let normalized = q as f64 / max_val as f64;
                normalized as f32
            })
            .collect();
        
        reconstructed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_no_compression() {
        let compressor = VectorCompressor::new(Box::new(NoCompression));
        
        let vector = vec![1.0, 2.0, 3.0, 4.0];
        let result = compressor.compress_vector("test", &vector).await;
        
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_compression_stats() {
        let compressor = VectorCompressor::new(Box::new(NoCompression));
        
        let vector = vec![1.0, 2.0, 3.0, 4.0];
        compressor.compress_vector("test", &vector).await.unwrap();
        
        let stats = compressor.get_compression_stats().await;
        assert_eq!(stats.vector_count, 1);
    }

    #[test]
    fn test_rle_compression() {
        let data = vec![1, 1, 1, 2, 2, 3, 3, 3, 3];
        let compressed = RunLengthEncoding::compress(&data);
        let decompressed = RunLengthEncoding::decompress(&compressed);
        
        assert_eq!(data, decompressed);
    }

    #[test]
    fn test_delta_coding() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let encoded = DeltaCoding::encode(&values);
        let decoded = DeltaCoding::decode(&encoded);
        
        assert_eq!(values.len(), decoded.len());
    }

    #[test]
    fn test_quantization() {
        let compressor = QuantizationCompressor::new(8);
        
        let vector = vec![0.0, 0.25, 0.5, 0.75, 1.0];
        let quantized = compressor.quantize(&vector);
        
        assert_eq!(quantized.len(), vector.len());
    }

    #[tokio::test]
    async fn test_lz4_compress_decompress() {
        let compressor = VectorCompressor::with_lz4();
        let vector: Vec<f32> = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        compressor.compress_vector("lz4_test", &vector).await.unwrap();
        let decompressed = compressor.decompress_vector("lz4_test").await.unwrap();
        assert_eq!(decompressed, vector);
    }

    #[tokio::test]
    async fn test_zstd_compress_decompress() {
        let compressor = VectorCompressor::with_zstd();
        let vector: Vec<f32> = vec![10.5, 20.5, 30.5];
        compressor.compress_vector("zstd_test", &vector).await.unwrap();
        let decompressed = compressor.decompress_vector("zstd_test").await.unwrap();
        assert_eq!(decompressed, vector);
    }

    #[tokio::test]
    async fn test_snappy_compress_decompress() {
        let compressor = VectorCompressor::with_snappy();
        let vector: Vec<f32> = vec![1.1, 2.2, 3.3, 4.4];
        compressor.compress_vector("snappy_test", &vector).await.unwrap();
        let decompressed = compressor.decompress_vector("snappy_test").await.unwrap();
        assert_eq!(decompressed, vector);
    }

    #[tokio::test]
    async fn test_large_vector_roundtrip() {
        let compressor = VectorCompressor::with_zstd();
        let vector: Vec<f32> = (0..10000).map(|i| i as f32 * 0.001).collect();
        compressor.compress_vector("large", &vector).await.unwrap();
        let decompressed = compressor.decompress_vector("large").await.unwrap();
        assert_eq!(decompressed.len(), vector.len());
        for (a, b) in vector.iter().zip(decompressed.iter()) {
            assert!((a - b).abs() < 1e-6);
        }
    }

    #[tokio::test]
    async fn test_compression_factory() {
        let c = CompressionFactory::create_compressor("lz4");
        assert_eq!(c.algorithm.name(), "lz4");
        let c = CompressionFactory::create_compressor("zstd");
        assert_eq!(c.algorithm.name(), "zstd");
        let c = CompressionFactory::create_compressor("snappy");
        assert_eq!(c.algorithm.name(), "snappy");
        let c = CompressionFactory::create_compressor("deflate");
        assert_eq!(c.algorithm.name(), "deflate");
    }

    #[tokio::test]
    async fn test_compressed_vector_remove_and_clear() {
        let compressor = VectorCompressor::with_snappy();
        let vector: Vec<f32> = vec![1.0, 2.0, 3.0];
        compressor.compress_vector("v1", &vector).await.unwrap();
        compressor.compress_vector("v2", &vector).await.unwrap();
        assert_eq!(compressor.get_compression_stats().await.vector_count, 2);

        compressor.remove("v1").await;
        assert_eq!(compressor.get_compression_stats().await.vector_count, 1);

        compressor.clear().await;
        assert_eq!(compressor.get_compression_stats().await.vector_count, 0);
    }
}
