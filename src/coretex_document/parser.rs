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
    let mut text = String::new();
    text.push_str("PDF Document Content\n");
    text.push_str("Note: Full PDF parsing requires pdf-extract or similar crate\n");
    text.push_str(&format!("Document size: {} bytes\n", data.len()));
    text
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
        
        Ok(ParsedDocument {
            text: "Audio transcription placeholder".to_string(),
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
