//! Text embedding service with ONNX Runtime support

use crate::coretex_core::Result;

#[derive(Debug, Clone)]
pub struct TextEmbeddingService {
    _model_name: String,
    dimension: usize,
    _device: String,
}

/// ONNX-based embedding engine (used when `onnx` feature is enabled)
#[cfg(feature = "onnx")]
pub struct OnnxEmbeddingEngine {
    session: ort::Session,
    dimension: usize,
}

#[cfg(feature = "onnx")]
impl OnnxEmbeddingEngine {
    /// Load an ONNX model from file path
    pub fn new(model_path: &str, dimension: usize) -> Result<Self> {
        let session = ort::Session::builder()
            .map_err(|e| format!("Failed to create ONNX session: {}", e))?
            .commit_from_file(model_path)
            .map_err(|e| format!("Failed to load ONNX model: {}", e))?;

        Ok(Self {
            session,
            dimension,
        })
    }

    /// Run inference on a single text input
    /// Expected input: tokenized input_ids and attention_mask (batch=1, seq_len=128)
    pub fn embed(&self, input_ids: &[i64], attention_mask: &[i64]) -> Result<Vec<f32>> {
        let seq_len = input_ids.len();

        // Create input tensors
        let input_ids_array = ndarray::Array2::from_shape_vec((1, seq_len), input_ids.to_vec())
            .map_err(|e| format!("Failed to create input_ids tensor: {}", e))?;
        let attention_mask_array = ndarray::Array2::from_shape_vec((1, seq_len), attention_mask.to_vec())
            .map_err(|e| format!("Failed to create attention_mask tensor: {}", e))?;

        let inputs = ort::inputs![
            "input_ids" => input_ids_array,
            "attention_mask" => attention_mask_array,
        ].map_err(|e| format!("Failed to create ONNX inputs: {}", e))?;

        let outputs = self.session.run(inputs)
            .map_err(|e| format!("ONNX inference failed: {}", e))?;

        // Extract output (assuming last_hidden_state or pooled_output)
        if let Some(output) = outputs.first() {
            let tensor = output.try_extract_tensor::<f32>()
                .map_err(|e| format!("Failed to extract output tensor: {}", e))?;
            let slice = tensor.as_slice().unwrap_or(&[]);
            let mut result = slice.to_vec();
            result.truncate(self.dimension);
            if result.len() < self.dimension {
                result.resize(self.dimension, 0.0);
            }
            return Ok(result);
        }

        Err("No output from ONNX model".to_string())
    }

    /// Run batch inference
    pub fn embed_batch(&self, batch: &[(Vec<i64>, Vec<i64>)]) -> Result<Vec<Vec<f32>>> {
        batch.iter()
            .map(|(ids, mask)| self.embed(ids, mask))
            .collect()
    }

    pub fn dimension(&self) -> usize {
        self.dimension
    }
}

impl TextEmbeddingService {
    pub fn new(model_name: &str, dimension: usize, device: &str) -> Self {
        Self {
            _model_name: model_name.to_string(),
            dimension,
            _device: device.to_string(),
        }
    }

    pub fn with_defaults() -> Self {
        Self::new(
            "sentence-transformers/all-MiniLM-L6-v2",
            384,
            "cpu",
        )
    }

    /// Embed text using a simple hash-based fallback
    /// When ONNX is available, use OnnxEmbeddingEngine instead
    pub fn embed_text(&self, text: &str) -> Result<Vec<f32>> {
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

    pub fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>> {
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
use crate::coretex_core::Result;

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
