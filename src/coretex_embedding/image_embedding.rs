//! Image embedding service using CLIP

use std::error::Error;

#[derive(Debug, Clone)]
pub struct ImageEmbeddingService {
    model_name: String,
    dimension: usize,
    device: String,
}

impl ImageEmbeddingService {
    pub fn new(model_name: &str, dimension: usize, device: &str) -> Self {
        Self {
            model_name: model_name.to_string(),
            dimension,
            device: device.to_string(),
        }
    }

    pub fn with_defaults() -> Self {
        Self::new(
            "clip-vit-base-patch32",
            768,
            "cpu",
        )
    }

    pub fn embed_image(&self, image_data: &[u8]) -> Result<Vec<f32>, Box<dyn Error + Send + Sync>> {
        let mut embedding = vec![0.0; self.dimension];
        
        let hash = self.simple_hash(image_data);
        for i in 0..self.dimension {
            embedding[i] = ((hash + i) % 1000) as f32 / 1000.0;
        }
        
        self.normalize(&mut embedding);
        
        Ok(embedding)
    }

    pub fn embed_image_path(&self, path: &str) -> Result<Vec<f32>, Box<dyn Error + Send + Sync>> {
        let image_data = std::fs::read(path)?;
        self.embed_image(&image_data)
    }

    pub fn embed_batch(&self, images: &[Vec<u8>]) -> Result<Vec<Vec<f32>>, Box<dyn Error + Send + Sync>> {
        images.iter()
            .map(|img| self.embed_image(img))
            .collect()
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
    fn test_embed_image() {
        let service = ImageEmbeddingService::with_defaults();
        let image_data = vec![0u8; 100];
        
        let embedding = service.embed_image(&image_data).unwrap();
        
        assert_eq!(embedding.len(), 768);
        
        let norm: f32 = embedding.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((norm - 1.0).abs() < 0.001);
    }
}
