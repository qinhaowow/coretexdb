//! Vector Dimension Extension for High-Dimensional Vectors
//! Supports 8K+ dimension vectors with memory optimization

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HighDimVector {
    pub data: Vec<f32>,
    pub dimension: usize,
    pub compression: Option<CompressionType>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum CompressionType {
    None,
    PQ { m: usize, nbits: usize },
    SQ { bits: u8 },
}

pub struct HighDimVectorStore {
    dimension: usize,
    vectors: HashMap<String, HighDimVector>,
    max_dimension: usize,
}

impl HighDimVectorStore {
    pub fn new(dimension: usize) -> Self {
        Self {
            dimension,
            vectors: HashMap::new(),
            max_dimension: 8192,
        }
    }

    pub fn with_max_dimension(mut self, max: usize) -> Self {
        self.max_dimension = max;
        self
    }

    pub fn insert(&mut self, id: String, data: Vec<f32>) -> Result<(), String> {
        if data.len() > self.max_dimension {
            return Err(format!("Vector dimension {} exceeds maximum {}", data.len(), self.max_dimension));
        }

        if data.len() != self.dimension && self.vectors.is_empty() {
            self.dimension = data.len();
        }

        if data.len() != self.dimension {
            return Err(format!("Dimension mismatch: expected {}, got {}", self.dimension, data.len()));
        }

        let vector = HighDimVector {
            data,
            dimension: self.dimension,
            compression: None,
        };

        self.vectors.insert(id, vector);
        Ok(())
    }

    pub fn get(&self, id: &str) -> Option<&HighDimVector> {
        self.vectors.get(id)
    }

    pub fn search(&self, query: &[f32], k: usize) -> Vec<(String, f32)> {
        let mut results: Vec<(String, f32)> = self.vectors
            .iter()
            .map(|(id, vec)| {
                let distance = cosine_distance(query, &vec.data);
                (id.clone(), distance)
            })
            .collect();

        results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        results.truncate(k);
        results
    }

    pub fn count(&self) -> usize {
        self.vectors.len()
    }

    pub fn dimension(&self) -> usize {
        self.dimension
    }

    pub fn memory_usage(&self) -> usize {
        self.vectors.values()
            .map(|v| v.data.len() * std::mem::size_of::<f32>())
            .sum()
    }
}

fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() {
        return f32::MAX;
    }

    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();

    if norm_a == 0.0 || norm_b == 0.0 {
        return 1.0;
    }

    1.0 - (dot / (norm_a * norm_b))
}

pub struct PQCompressor {
    m: usize,
    nbits: usize,
    codebooks: HashMap<usize, Vec<Vec<f32>>>,
}

impl PQCompressor {
    pub fn new(m: usize, nbits: usize) -> Self {
        Self {
            m,
            nbits,
            codebooks: HashMap::new(),
        }
    }

    pub fn train(&mut self, vectors: &[Vec<f32>], dimension: usize) -> Result<(), String> {
        if vectors.is_empty() {
            return Err("No vectors to train".to_string());
        }

        let sub_dim = dimension / self.m;
        if sub_dim == 0 {
            return Err("Vector dimension too small for PQ".to_string());
        }

        for m in 0..self.m {
            let mut centroids = Vec::new();
            let start = m * sub_dim;
            let end = (start + sub_dim).min(dimension);
            
            for _ in 0..(1 << self.nbits) {
                let idx = (m * 100 + centroids.len()) % vectors.len();
                centroids.push(vectors[idx][start..end].to_vec());
            }
            
            self.codebooks.insert(m, centroids);
        }

        Ok(())
    }

    pub fn compress(&self, vector: &[f32]) -> Vec<u8> {
        let sub_dim = vector.len() / self.m;
        let mut encoded = Vec::with_capacity(self.m);

        for m in 0..self.m {
            let start = m * sub_dim;
            let end = (start + sub_dim).min(vector.len());
            let sub_vec = &vector[start..end];

            let mut min_dist = f32::MAX;
            let mut best_code = 0u8;

            if let Some(codebook) = self.codebooks.get(&m) {
                for (code, centroid) in codebook.iter().enumerate() {
                    let dist = euclidean_distance(sub_vec, centroid);
                    if dist < min_dist {
                        min_dist = dist;
                        best_code = code as u8;
                    }
                }
            }

            encoded.push(best_code);
        }

        encoded
    }

    pub fn decompress(&self, encoded: &[u8], dimension: usize) -> Vec<f32> {
        let sub_dim = dimension / self.m;
        let mut result = vec![0.0; dimension];

        for (m, &code) in encoded.iter().enumerate() {
            if let Some(codebook) = self.codebooks.get(&m) {
                let centroid = &codebook[code as usize];
                let start = m * sub_dim;
                for (i, &val) in centroid.iter().enumerate() {
                    result[start + i] = val;
                }
            }
        }

        result
    }
}

fn euclidean_distance(a: &[f32], b: &[f32]) -> f32 {
    a.iter()
        .zip(b.iter())
        .map(|(x, y)| (x - y) * (x - y))
        .sum::<f32>()
        .sqrt()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_high_dim_vector() {
        let mut store = HighDimVectorStore::new(128).with_max_dimension(8192);
        
        let vector: Vec<f32> = (0..128).map(|i| i as f32 * 0.01).collect();
        store.insert("doc1".to_string(), vector).unwrap();
        
        assert_eq!(store.count(), 1);
        assert_eq!(store.dimension(), 128);
    }

    #[test]
    fn test_pq_compression() {
        let mut compressor = PQCompressor::new(8, 8);
        
        let vectors: Vec<Vec<f32>> = (0..100)
            .map(|i| (0..128).map(|j| ((i + j) % 256) as f32 * 0.01).collect())
            .collect();
        
        compressor.train(&vectors, 128).unwrap();
        
        let test_vec: Vec<f32> = (0..128).map(|i| i as f32 * 0.01).collect();
        let encoded = compressor.compress(&test_vec);
        
        assert_eq!(encoded.len(), 8);
        
        let decoded = compressor.decompress(&encoded, 128);
        assert_eq!(decoded.len(), 128);
    }
}
