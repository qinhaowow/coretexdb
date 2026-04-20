//! Video embedding service

use std::error::Error;

#[derive(Debug, Clone)]
pub struct VideoEmbeddingService {
    model_name: String,
    dimension: usize,
    device: String,
    frame_sample_rate: u32,
}

impl VideoEmbeddingService {
    pub fn new(model_name: &str, dimension: usize, device: &str, frame_sample_rate: u32) -> Self {
        Self {
            model_name: model_name.to_string(),
            dimension,
            device: device.to_string(),
            frame_sample_rate,
        }
    }

    pub fn with_defaults() -> Self {
        Self::new(
            "videoclip",
            768,
            "cpu",
            1,
        )
    }

    pub fn embed_video(&self, frames: &[Vec<u8>]) -> Result<Vec<f32>, Box<dyn Error + Send + Sync>> {
        let mut embedding = vec![0.0; self.dimension];
        
        if frames.is_empty() {
            return Ok(embedding);
        }

        let frame_dim = self.dimension / frames.len().max(1);
        
        for (frame_idx, frame) in frames.iter().enumerate() {
            let hash = self.simple_hash(frame);
            let start = frame_idx * frame_dim;
            let end = (start + frame_dim).min(self.dimension);
            
            for i in start..end {
                if i < self.dimension {
                    embedding[i] = ((hash + i) % 1000) as f32 / 1000.0;
                }
            }
        }
        
        self.normalize(&mut embedding);
        
        Ok(embedding)
    }

    pub fn embed_video_path(&self, path: &str) -> Result<Vec<f32>, Box<dyn Error + Send + Sync>> {
        let data = std::fs::read(path)?;
        
        let chunk_size = 1000;
        let frames: Vec<Vec<u8>> = data
            .chunks(chunk_size)
            .map(|chunk| chunk.to_vec())
            .collect();
        
        self.embed_video(&frames)
    }

    pub fn embed_video_frames(&self, frame_paths: &[String]) -> Result<Vec<f32>, Box<dyn Error + Send + Sync>> {
        let mut frames = Vec::new();
        
        for path in frame_paths {
            let data = std::fs::read(path)?;
            frames.push(data);
        }
        
        self.embed_video(&frames)
    }

    pub fn get_dimension(&self) -> usize {
        self.dimension
    }

    fn simple_hash(&self, data: &[u8]) -> usize {
        let mut hash: usize = 5381;
        for byte in data.iter().take(1000) {
            hash = hash.wrapping_mul(33).wrapping_add(*byte as usize);
        }
        hash
    }

    fn normalize(&self, vector: &mut Vec<f32>) {
        let norm: f32 = vector.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm > 0.0 {
            for v in vector.iter_mut() {
                *v /= norm;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_embed_video() {
        let service = VideoEmbeddingService::with_defaults();
        let frames = vec![
            vec![0u8; 100],
            vec![1u8; 100],
            vec![2u8; 100],
        ];
        
        let embedding = service.embed_video(&frames).unwrap();
        
        assert_eq!(embedding.len(), 768);
        
        let norm: f32 = embedding.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!(norm > 0.0);
    }

    #[test]
    fn test_embed_video_empty() {
        let service = VideoEmbeddingService::with_defaults();
        let frames: Vec<Vec<u8>> = vec![];
        
        let embedding = service.embed_video(&frames).unwrap();
        
        assert_eq!(embedding.len(), 768);
    }
}
