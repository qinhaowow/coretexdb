//! ONNX Runtime inference for CortexDB
//! Supports text embedding models for vector generation

use std::collections::HashMap;
use std::path::Path;
use thiserror::Error;
use ndarray::Array2;

#[derive(Error, Debug)]
pub enum OnnxError {
    #[error("Failed to load model: {0}")]
    ModelLoadError(String),
    #[error("Failed to run inference: {0}")]
    InferenceError(String),
    #[error("Invalid input: {0}")]
    InvalidInput(String),
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}

pub struct OnnxInference {
    session: ort::Session,
    input_name: String,
    output_name: String,
    normalize: bool,
}

impl OnnxInference {
    pub fn new(model_path: &str) -> Result<Self, OnnxError> {
        let session = ort::SessionBuilder::new()
            .map_err(|e| OnnxError::ModelLoadError(e.to_string()))?
            .with_model_from_file(model_path)
            .map_err(|e| OnnxError::ModelLoadError(e.to_string()))?;

        let input_name = session.inputs[0].name.to_string();
        let output_name = session.outputs[0].name.to_string();

        Ok(Self {
            session,
            input_name,
            output_name,
            normalize: true,
        })
    }

    pub fn from_bytes(model_bytes: &[u8]) -> Result<Self, OnnxError> {
        let session = ort::SessionBuilder::new()
            .map_err(|e| OnnxError::ModelLoadError(e.to_string()))?
            .with_model_from_memory(model_bytes)
            .map_err(|e| OnnxError::ModelLoadError(e.to_string()))?;

        let input_name = session.inputs[0].name.to_string();
        let output_name = session.outputs[0].name.to_string();

        Ok(Self {
            session,
            input_name,
            output_name,
            normalize: true,
        })
    }

    pub fn with_normalize(mut self, normalize: bool) -> Self {
        self.normalize = normalize;
        self
    }

    pub fn infer(&self, input_ids: &[i64], attention_mask: &[i64]) -> Result<Vec<f32>, OnnxError> {
        let batch_size = 1;
        let seq_length = input_ids.len();

        let input_ids_array = Array2::from_shape_vec(
            (batch_size, seq_length),
            input_ids.to_vec(),
        ).map_err(|e| OnnxError::InferenceError(e.to_string()))?;
        let input_ids_tensor = ort::Tensor::from_array(input_ids_array)
            .map_err(|e| OnnxError::InferenceError(e.to_string()))?;

        let attention_mask_array = Array2::from_shape_vec(
            (batch_size, seq_length),
            attention_mask.to_vec(),
        ).map_err(|e| OnnxError::InferenceError(e.to_string()))?;
        let attention_mask_tensor = ort::Tensor::from_array(attention_mask_array)
            .map_err(|e| OnnxError::InferenceError(e.to_string()))?;

        let outputs = self.session.run(
            vec![
                input_ids_tensor.into(),
                attention_mask_tensor.into(),
            ]
        ).map_err(|e| OnnxError::InferenceError(e.to_string()))?;

        let output_tensor = outputs[0].try_extract::<f32>().map_err(|e| OnnxError::InferenceError(e.to_string()))?;
        let mut embeddings: Vec<f32> = output_tensor.view_data().iter().cloned().collect();

        if self.normalize {
            Self::normalize_vector(&mut embeddings);
        }

        Ok(embeddings)
    }

    pub fn infer_text(&self, text: &str, tokenizer: &impl Tokenizer) -> Result<Vec<f32>, OnnxError> {
        let (input_ids, attention_mask) = tokenizer.encode(text);

        let result = self.infer(&input_ids, &attention_mask)?;

        Ok(result)
    }

    pub fn infer_batch(&self, texts: &[String], tokenizer: &impl Tokenizer) -> Result<Vec<Vec<f32>>, OnnxError> {
        let mut results = Vec::with_capacity(texts.len());

        for text in texts {
            let embedding = self.infer_text(text, tokenizer)?;
            results.push(embedding);
        }

        Ok(results)
    }

    fn normalize_vector(vec: &mut Vec<f32>) {
        let norm: f32 = vec.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm > 0.0 {
            for v in vec.iter_mut() {
                *v /= norm;
            }
        }
    }
}

pub trait Tokenizer: Send + Sync {
    fn encode(&self, text: &str) -> (Vec<i64>, Vec<i64>);
    fn decode(&self, ids: &[i64]) -> String;
}

pub struct BertTokenizer {
    vocab: HashMap<String, i64>,
    reversed_vocab: Vec<String>,
   unk_token_id: i64,
    sep_token_id: i64,
    pad_token_id: i64,
    cls_token_id: i64,
    mask_token_id: i64,
}

impl BertTokenizer {
    pub fn from_vocab_file(vocab_path: &str) -> Result<Self, OnnxError> {
        let content = std::fs::read_to_string(vocab_path)?;
        let mut vocab = HashMap::new();
        let mut reversed_vocab = Vec::new();

        for (idx, line) in content.lines().enumerate() {
            let token = line.trim().to_string();
            vocab.insert(token.clone(), idx as i64);
            reversed_vocab.push(token);
        }

        let unk_token_id = vocab.get("[UNK]").copied().unwrap_or(100);
        let sep_token_id = vocab.get("[SEP]").copied().unwrap_or(102);
        let pad_token_id = vocab.get("[PAD]").copied().unwrap_or(0);
        let cls_token_id = vocab.get("[CLS]").copied().unwrap_or(101);
        let mask_token_id = vocab.get("[MASK]").copied().unwrap_or(103);

        Ok(Self {
            vocab,
            reversed_vocab,
            unk_token_id,
            sep_token_id,
            pad_token_id,
            cls_token_id,
            mask_token_id,
        })
    }

    fn get_id(&self, token: &str) -> i64 {
        *self.vocab.get(token).unwrap_or(&self.unk_token_id)
    }

    fn tokenize(&self, text: &str) -> Vec<String> {
        let mut tokens = Vec::new();
        let text_lower = text.to_lowercase();

        let mut current_word = String::new();
        for ch in text_lower.chars() {
            if ch.is_alphanumeric() {
                current_word.push(ch);
            } else {
                if !current_word.is_empty() {
                    tokens.push(current_word.clone());
                    current_word.clear();
                }
                if !ch.is_whitespace() {
                    tokens.push(ch.to_string());
                }
            }
        }
        if !current_word.is_empty() {
            tokens.push(current_word);
        }

        let mut sub_tokens = Vec::new();
        for token in &tokens {
            if self.vocab.contains_key(token) {
                sub_tokens.push(token.clone());
            } else {
                let mut chars = token.chars();
                while let Some(c) = chars.next() {
                    let s = c.to_string();
                    if self.vocab.contains_key(&s) {
                        sub_tokens.push(s);
                    } else {
                        sub_tokens.push("[UNK]".to_string());
                    }
                }
            }
        }

        sub_tokens
    }
}

impl Tokenizer for BertTokenizer {
    fn encode(&self, text: &str) -> (Vec<i64>, Vec<i64>) {
        let tokens = self.tokenize(text);
        
        let mut input_ids = vec![self.cls_token_id];
        for token in &tokens {
            input_ids.push(self.get_id(token));
        }
        input_ids.push(self.sep_token_id);

        let seq_length = input_ids.len();
        let attention_mask = vec![1i64; seq_length];

        (input_ids, attention_mask)
    }

    fn decode(&self, ids: &[i64]) -> String {
        let mut tokens = Vec::new();
        for id in ids {
            if *id == self.pad_token_id || *id == self.cls_token_id || *id == self.sep_token_id {
                continue;
            }
            if let Some(token) = self.reversed_vocab.get(*id as usize) {
                tokens.push(token.clone());
            }
        }
        tokens.join(" ")
    }
}

pub struct SentenceTransformer {
    inference: OnnxInference,
    tokenizer: BertTokenizer,
}

impl SentenceTransformer {
    pub fn new(model_path: &str, vocab_path: &str) -> Result<Self, OnnxError> {
        let inference = OnnxInference::new(model_path)?;
        let tokenizer = BertTokenizer::from_vocab_file(vocab_path)?;

        Ok(Self {
            inference,
            tokenizer,
        })
    }

    pub fn encode(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, OnnxError> {
        self.inference.infer_batch(texts, &self.tokenizer)
    }

    pub fn encode_single(&self, text: &str) -> Result<Vec<f32>, OnnxError> {
        self.inference.infer_text(text, &self.tokenizer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokenizer_basic() {
        let vocab = r##"[PAD]
[UNK]
[CLS]
[SEP]
[MASK]
the
a
is
are
"##;
        
        let temp_dir = std::env::temp_dir();
        let vocab_path = temp_dir.join("vocab.txt");
        std::fs::write(&vocab_path, vocab).unwrap();

        let tokenizer = BertTokenizer::from_vocab_file(vocab_path.to_str().unwrap()).unwrap();
        
        let (input_ids, attention_mask) = tokenizer.encode("the a is");
        
        assert!(!input_ids.is_empty());
        assert_eq!(input_ids.len(), attention_mask.len());

        std::fs::remove_file(vocab_path).ok();
    }
}
