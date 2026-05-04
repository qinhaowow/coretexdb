//! Document Parser Interface and Implementations
//! Supports parsing PDF, images, audio, and other document types

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParsedDocument {
    pub text: String,
    pub images: Vec<ImageData>,
    pub tables: Vec<TableData>,
    pub metadata: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImageData {
    pub data: Vec<u8>,
    pub width: u32,
    pub height: u32,
    pub format: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableData {
    pub headers: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

pub trait DocumentParser: Send + Sync {
    fn parse(&self, data: &[u8]) -> Result<ParsedDocument, String>;
    fn supported_types(&self) -> Vec<&str>;
}

pub struct PdfParser;

impl PdfParser {
    pub fn new() -> Self {
        Self
    }
}

impl DocumentParser for PdfParser {
    fn parse(&self, data: &[u8]) -> Result<ParsedDocument, String> {
        let text = extract_text_from_pdf(data)?;
        
        Ok(ParsedDocument {
            text,
            images: Vec::new(),
            tables: Vec::new(),
            metadata: HashMap::new(),
        })
    }

    fn supported_types(&self) -> Vec<&str> {
        vec!["pdf"]
    }
}

fn extract_text_from_pdf(data: &[u8]) -> Result<String, String> {
    let content = std::str::from_utf8(data)
        .map_err(|_| "Invalid UTF-8 in PDF data".to_string())?;
    
    let mut text = String::new();
    let mut in_text = false;
    let mut in_stream = false;
    
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("BT") {
            in_text = true;
            continue;
        }
        if trimmed.starts_with("ET") {
            in_text = false;
            continue;
        }
        if trimmed.starts_with("stream") {
            in_stream = true;
            continue;
        }
        if trimmed.starts_with("endstream") {
            in_stream = false;
            continue;
        }
        
        if in_text {
            let mut chars = Vec::new();
            let mut in_paren = false;
            for ch in trimmed.chars() {
                if ch == '(' {
                    in_paren = true;
                } else if ch == ')' {
                    in_paren = false;
                    if !chars.is_empty() {
                        let s: String = chars.iter().collect();
                        text.push_str(&s);
                        text.push(' ');
                        chars.clear();
                    }
                } else if in_paren {
                    chars.push(ch);
                }
            }
        }
    }
    
    if text.is_empty() {
        text.push_str(&format!("PDF document ({} bytes)", data.len()));
    }
    
    Ok(text)
}

pub struct ImageParser;

impl ImageParser {
    pub fn new() -> Self {
        Self
    }

    pub fn extract_metadata(&self, data: &[u8]) -> Result<ImageData, String> {
        Ok(ImageData {
            data: data.to_vec(),
            width: 0,
            height: 0,
            format: "unknown".to_string(),
        })
    }
}

impl DocumentParser for ImageParser {
    fn parse(&self, data: &[u8]) -> Result<ParsedDocument, String> {
        let mut metadata = HashMap::new();
        metadata.insert("type".to_string(), serde_json::Value::String("image".to_string()));
        metadata.insert("size".to_string(), serde_json::Value::Number(data.len().into()));
        
        Ok(ParsedDocument {
            text: String::new(),
            images: vec![self.extract_metadata(data)?],
            tables: Vec::new(),
            metadata,
        })
    }

    fn supported_types(&self) -> Vec<&str> {
        vec!["jpg", "jpeg", "png", "gif", "bmp", "webp"]
    }
}

pub struct AudioParser;

impl AudioParser {
    pub fn new() -> Self {
        Self
    }
}

impl DocumentParser for AudioParser {
    fn parse(&self, data: &[u8]) -> Result<ParsedDocument, String> {
        let mut metadata = HashMap::new();
        metadata.insert("type".to_string(), serde_json::Value::String("audio".to_string()));
        metadata.insert("size".to_string(), serde_json::Value::Number(data.len().into()));
        
        let mut text = String::new();
        let mut format = "unknown".to_string();
        
        if data.len() >= 12 && &data[0..4] == b"RIFF" && &data[8..12] == b"WAVE" {
            format = "wav".to_string();
            let channels = if data.len() >= 22 { u16::from_le_bytes([data[22], data[23]]) } else { 0 };
            let sample_rate = if data.len() >= 24 { u32::from_le_bytes([data[24], data[25], data[26], data[27]]) } else { 0 };
            let bits_per_sample = if data.len() >= 34 { u16::from_le_bytes([data[34], data[35]]) } else { 0 };
            let duration_secs = if sample_rate > 0 && channels > 0 && bits_per_sample > 0 {
                let bytes_per_sec = sample_rate as u64 * channels as u64 * (bits_per_sample as u64 / 8);
                if bytes_per_sec > 0 { data.len() as u64 / bytes_per_sec } else { 0 }
            } else { 0 };
            
            metadata.insert("format".to_string(), serde_json::Value::String("WAV".to_string()));
            metadata.insert("channels".to_string(), serde_json::Value::Number(channels.into()));
            metadata.insert("sample_rate".to_string(), serde_json::Value::Number(sample_rate.into()));
            metadata.insert("bits_per_sample".to_string(), serde_json::Value::Number(bits_per_sample.into()));
            metadata.insert("duration_secs".to_string(), serde_json::Value::Number(duration_secs.into()));
            
            text = format!("WAV audio: {} channels, {} Hz, {} bits, ~{} seconds",
                          channels, sample_rate, bits_per_sample, duration_secs);
        } else if data.len() >= 3 && &data[0..3] == b"ID3" {
            format = "mp3".to_string();
            let id3_size = if data.len() >= 10 {
                ((data[6] as u32) << 21) | ((data[7] as u32) << 14) |
                ((data[8] as u32) << 7) | (data[9] as u32)
            } else { 0 };
            
            metadata.insert("format".to_string(), serde_json::Value::String("MP3".to_string()));
            metadata.insert("id3_tag_size".to_string(), serde_json::Value::Number(id3_size.into()));
            
            text = format!("MP3 audio with ID3 tag ({} bytes)", id3_size);
        } else if data.len() >= 4 && &data[0..4] == b"fLaC" {
            format = "flac".to_string();
            metadata.insert("format".to_string(), serde_json::Value::String("FLAC".to_string()));
            text = format!("FLAC audio ({} bytes)", data.len());
        } else {
            metadata.insert("format".to_string(), serde_json::Value::String(format!("unknown")));
            text = format!("Audio file ({} bytes)", data.len());
        }
        
        metadata.insert("detected_format".to_string(), serde_json::Value::String(format));
        
        Ok(ParsedDocument {
            text,
            images: Vec::new(),
            tables: Vec::new(),
            metadata,
        })
    }

    fn supported_types(&self) -> Vec<&str> {
        vec!["mp3", "wav", "flac", "aac", "ogg"]
    }
}

pub struct DocumentParserRegistry {
    parsers: Vec<Box<dyn DocumentParser>>,
}

impl DocumentParserRegistry {
    pub fn new() -> Self {
        let mut registry = Self { parsers: Vec::new() };
        registry.register(Box::new(PdfParser::new()));
        registry.register(Box::new(ImageParser::new()));
        registry.register(Box::new(AudioParser::new()));
        registry
    }

    pub fn register(&mut self, parser: Box<dyn DocumentParser>) {
        self.parsers.push(parser);
    }

    pub fn parse(&self, data: &[u8], extension: &str) -> Result<ParsedDocument, String> {
        for parser in &self.parsers {
            if parser.supported_types().contains(&extension.to_lowercase().as_str()) {
                return parser.parse(data);
            }
        }
        Err(format!("No parser available for file type: {}", extension))
    }

    pub fn get_supported_extensions(&self) -> Vec<String> {
        let mut extensions = Vec::new();
        for parser in &self.parsers {
            for ext in parser.supported_types() {
                extensions.push(ext.to_string());
            }
        }
        extensions
    }
}

impl Default for DocumentParserRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pdf_parser() {
        let parser = PdfParser::new();
        let data = b"fake pdf data";
        
        let result = parser.parse(data);
        assert!(result.is_ok());
    }

    #[test]
    fn test_registry() {
        let registry = DocumentParserRegistry::new();
        let extensions = registry.get_supported_extensions();
        
        assert!(extensions.contains(&"pdf".to_string()));
        assert!(extensions.contains(&"jpg".to_string()));
    }
}
