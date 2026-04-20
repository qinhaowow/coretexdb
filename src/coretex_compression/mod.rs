//! Data Compression module for CoreTexDB
//! Provides compression for vector data storage

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

    pub async fn compress_vector(&self, id: &str, vector: &[f32]) -> Result<CompressedVector, String> {
        let bytes: Vec<u8> = vector.iter()
            .flat_map(|f| f.to_le_bytes())
            .collect();
        
        let original_size = bytes.len();
        
        let compressed = self.algorithm.compress(&bytes)?;
        
        let compression_ratio = if original_size > 0 {
            original_size as f64 / compressed.len() as f64
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
            *comp += compressed.len();
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

struct LZ4Compression;

impl CompressionAlgorithm for LZ4Compression {
    fn name(&self) -> &str { "lz4" }
    
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>, String> {
        Err("LZ4 requires 'lz4' crate. Using simple compression.".to_string())
    }
    
    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>, String> {
        Err("LZ4 requires 'lz4' crate.".to_string())
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
        Err("Zstd requires 'zstd' crate. Using simple compression.".to_string())
    }
    
    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>, String> {
        Err("Zstd requires 'zstd' crate.".to_string())
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
        Err("Snappy requires 'snappy' crate. Using simple compression.".to_string())
    }
    
    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>, String> {
        Err("Snappy requires 'snappy' crate.".to_string())
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
                result.extend(std::iter::repeat(value).take(count));
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
        if data.len() < 4 || data.len() % 4 != 0 {
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
        
        let mut min_val = f32::MAX;
        let mut max_val_f = f32::MIN;
        
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
}
