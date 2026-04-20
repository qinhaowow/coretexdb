//! Text embedding service

use std::error::Error;

#[derive(Debug, Clone)]
pub struct TextEmbeddingService {
    model_name: String,
    dimension: usize,
    device: String,
}

impl TextEmbeddingService {
    pub fn new(model_name: &str, dimension: usize, device: &str) -> Self {
        Self {
            model_name: model_name.to_string(),
            dimension,
            device: device.to_string(),
        }
    }

    pub fn with_defaults() -> Self {
        Self::new(
            "sentence-transformers/all-MiniLM-L6-v2",
            384,
            "cpu",
        )
    }

    pub fn embed_text(&self, text: &str) -> Result<Vec<f32>, Box<dyn Error + Send + Sync>> {
        let mut embedding = vec![0.0; self.dimension];
        
        let words: Vec<&str> = text.split_whitespace().collect();
        if words.is_empty() {
            return Ok(embedding);
        }

        let hash = self.simple_hash(text);
        for i in 0..self.dimension {
            embedding[i] = ((hash + i) % 1000) as f32 / 1000.0;
        }
        
        self.normalize(&mut embedding);
        
        Ok(embedding)
    }

    pub fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, Box<dyn Error + Send + Sync>> {
        texts.iter()
            .map(|text| self.embed_text(text))
            .collect()
    }

    pub fn get_dimension(&self) -> usize {
        self.dimension
    }

    fn simple_hash(&self, text: &str) -> usize {
        let mut hash: usize = 5381;
        for c in text.chars() {
            hash = hash.wrapping_mul(33).wrapping_add(c as usize);
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
    fn test_embed_text() {
        let service = TextEmbeddingService::with_defaults();
        let embedding = service.embed_text("Hello world").unwrap();
        
        assert_eq!(embedding.len(), 384);
        
        let norm: f32 = embedding.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((norm - 1.0).abs() < 0.001);
    }

    #[test]
    fn test_embed_batch() {
        let service = TextEmbeddingService::with_defaults();
        let texts = vec![
            "Hello world".to_string(),
            "Test document".to_string(),
        ];
        
        let embeddings = service.embed_batch(&texts).unwrap();
        
        assert_eq!(embeddings.len(), 2);
        assert_eq!(embeddings[0].len(), 384);
    }
}
