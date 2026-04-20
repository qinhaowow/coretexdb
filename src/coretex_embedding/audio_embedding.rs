//! Audio embedding service

use std::error::Error;

#[derive(Debug, Clone)]
pub struct AudioEmbeddingService {
    model_name: String,
    dimension: usize,
    device: String,
    sample_rate: u32,
}

impl AudioEmbeddingService {
    pub fn new(model_name: &str, dimension: usize, device: &str, sample_rate: u32) -> Self {
        Self {
            model_name: model_name.to_string(),
            dimension,
            device: device.to_string(),
            sample_rate,
        }
    }

    pub fn with_defaults() -> Self {
        Self::new(
            "wavlm-base",
            768,
            "cpu",
            16000,
        )
    }

    pub fn embed_audio(&self, audio_data: &[f32]) -> Result<Vec<f32>, Box<dyn Error + Send + Sync>> {
        let mut embedding = vec![0.0; self.dimension];
        
        if audio_data.is_empty() {
            return Ok(embedding);
        }

        let chunk_size = audio_data.len() / self.dimension.max(1);
        for i in 0..self.dimension {
            let start = i * chunk_size;
            let end = (start + chunk_size).min(audio_data.len());
            if start < audio_data.len() {
                let sum: f32 = audio_data[start..end].iter().sum();
                embedding[i] = sum / (chunk_size as f32).max(1.0);
            }
        }
        
        self.normalize(&mut embedding);
        
        Ok(embedding)
    }

    pub fn embed_audio_bytes(&self, audio_bytes: &[u8]) -> Result<Vec<f32>, Box<dyn Error + Send + Sync>> {
        let audio_data: Vec<f32> = audio_bytes
            .chunks(2)
            .filter_map(|chunk| {
                if chunk.len() == 2 {
                    let sample = i16::from_le_bytes([chunk[0], chunk[1]]);
                    Some(sample as f32 / 32768.0)
                } else {
                    None
                }
            })
            .collect();
        
        self.embed_audio(&audio_data)
    }

    pub fn embed_audio_path(&self, path: &str) -> Result<Vec<f32>, Box<dyn Error + Send + Sync>> {
        let audio_bytes = std::fs::read(path)?;
        self.embed_audio_bytes(&audio_bytes)
    }

    pub fn embed_batch(&self, audios: &[Vec<f32>]) -> Result<Vec<Vec<f32>>, Box<dyn Error + Send + Sync>> {
        audios.iter()
            .map(|audio| self.embed_audio(audio))
            .collect()
    }

    pub fn get_dimension(&self) -> usize {
        self.dimension
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
    fn test_embed_audio() {
        let service = AudioEmbeddingService::with_defaults();
        let audio_data = vec![0.5f32; 16000];
        
        let embedding = service.embed_audio(&audio_data).unwrap();
        
        assert_eq!(embedding.len(), 768);
        
        let norm: f32 = embedding.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!(norm > 0.0);
    }

    #[test]
    fn test_embed_audio_bytes() {
        let service = AudioEmbeddingService::with_defaults();
        let audio_bytes = vec![0u8; 100];
        
        let embedding = service.embed_audio_bytes(&audio_bytes).unwrap();
        
        assert_eq!(embedding.len(), 768);
    }
}
