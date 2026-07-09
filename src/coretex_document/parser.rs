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
    let mut stream_is_flate = false;
    let mut stream_buf: Vec<u8> = Vec::new();

    for line in content.lines() {
        let trimmed = line.trim();

        // 检测 stream 前面的字典是否声明了 /FlateDecode
        if !in_stream && trimmed.contains("/FlateDecode") {
            stream_is_flate = true;
        }

        if trimmed.starts_with("BT") && !in_stream {
            in_text = true;
            continue;
        }
        if trimmed.starts_with("ET") && !in_stream {
            in_text = false;
            continue;
        }

        // stream 体开始
        if trimmed.starts_with("stream") {
            in_stream = true;
            stream_buf.clear();
            // 处理 "stream" 同行后面可能跟着二进制数据的情况
            let rest = trimmed["stream".len()..].trim_start_matches('\r').trim_start_matches('\n');
            if !rest.is_empty() {
                stream_buf.extend_from_slice(rest.as_bytes());
            }
            continue;
        }
        if trimmed.starts_with("endstream") {
            in_stream = false;
            if stream_is_flate && !stream_buf.is_empty() {
                // 使用 flate2 解压 zlib/deflate 流
                use std::io::Read;
                let mut decoder = flate2::read::ZlibDecoder::new(&stream_buf[..]);
                let mut decompressed = String::new();
                if decoder.read_to_string(&mut decompressed).is_ok() {
                    // 从解压后的内容对象中提取文本
                    extract_text_from_content_stream(&decompressed, &mut text);
                } else {
                    // 尝试纯 deflate（无 zlib header）
                    let mut decoder = flate2::read::DeflateDecoder::new(&stream_buf[..]);
                    let mut decompressed = String::new();
                    if decoder.read_to_string(&mut decompressed).is_ok() {
                        extract_text_from_content_stream(&decompressed, &mut text);
                    }
                }
            }
            stream_is_flate = false;
            stream_buf.clear();
            continue;
        }

        if in_stream {
            // 累积 stream 二进制数据
            stream_buf.extend_from_slice(line.as_bytes());
            stream_buf.push(b'\n');
        } else if in_text {
            // 未压缩的 BT/ET 块内提取括号字符串
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

/// 从 PDF content stream（解压后）中提取文本操作符（Tj/TJ）中的字符串
fn extract_text_from_content_stream(content: &str, text: &mut String) {
    for token in content.split_whitespace() {
        // Tj 操作符：(... ) Tj
        if token.starts_with('(') {
            let s: String = token.trim_start_matches('(')
                .trim_end_matches(')')
                .chars()
                .collect();
            if !s.is_empty() {
                text.push_str(&s);
                text.push(' ');
            }
        }
    }
}

pub struct ImageParser;

impl ImageParser {
    pub fn new() -> Self {
        Self
    }

    pub fn extract_metadata(&self, data: &[u8]) -> Result<ImageData, String> {
        if data.len() < 8 {
            return Err("Image data too short".to_string());
        }

        // PNG: 8字节签名 + IHDR chunk
        if data.len() >= 24 && data[0..8] == [0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A] {
            // IHDR 在偏移 16 处，宽高各 4 字节大端
            let width = u32::from_be_bytes([data[16], data[17], data[18], data[19]]);
            let height = u32::from_be_bytes([data[20], data[21], data[22], data[23]]);
            return Ok(ImageData {
                data: data.to_vec(),
                width,
                height,
                format: "png".to_string(),
            });
        }

        // JPEG: FF D8 开头，扫描 SOF0/SOF2 标记获取尺寸
        if data.len() >= 4 && data[0] == 0xFF && data[1] == 0xD8 {
            let (w, h) = Self::parse_jpeg_dimensions(data)
                .unwrap_or((0, 0));
            return Ok(ImageData {
                data: data.to_vec(),
                width: w,
                height: h,
                format: "jpeg".to_string(),
            });
        }

        // GIF: "GIF87a" 或 "GIF89a"，宽高在偏移 6-9 小端
        if data.len() >= 10 && (data[0..6] == *b"GIF87a" || data[0..6] == *b"GIF89a") {
            let width = u16::from_le_bytes([data[6], data[7]]) as u32;
            let height = u16::from_le_bytes([data[8], data[9]]) as u32;
            return Ok(ImageData {
                data: data.to_vec(),
                width,
                height,
                format: "gif".to_string(),
            });
        }

        // BMP: "BM" 开头，宽高在偏移 18/22 小端
        if data.len() >= 26 && data[0] == b'B' && data[1] == b'M' {
            let width = u32::from_le_bytes([data[18], data[19], data[20], data[21]]);
            let height = u32::from_le_bytes([data[22], data[23], data[24], data[25]]);
            return Ok(ImageData {
                data: data.to_vec(),
                width,
                height,
                format: "bmp".to_string(),
            });
        }

        // WebP: "RIFF" + "WEBP"
        if data.len() >= 30 && &data[0..4] == b"RIFF" && &data[8..12] == b"WEBP" {
            let (w, h) = if &data[12..16] == b"VP8 " {
                // VP8 (lossy)
                let w = u16::from_le_bytes([data[26], data[27]]) as u32 & 0x3FFF;
                let h = u16::from_le_bytes([data[28], data[29]]) as u32 & 0x3FFF;
                (w, h)
            } else if &data[12..16] == b"VP8L" {
                // VP8L (lossless)
                let b0 = data[21] as u32;
                let b1 = data[22] as u32;
                let b2 = data[23] as u32;
                let b3 = data[24] as u32;
                let w = 1 + ((b1 & 0x3F) << 8 | b0);
                let h = 1 + ((b3 & 0x0F) << 10 | b2 << 2 | (b1 >> 6));
                (w, h)
            } else {
                (0, 0)
            };
            return Ok(ImageData {
                data: data.to_vec(),
                width: w,
                height: h,
                format: "webp".to_string(),
            });
        }

        Ok(ImageData {
            data: data.to_vec(),
            width: 0,
            height: 0,
            format: "unknown".to_string(),
        })
    }

    /// 扫描 JPEG SOF 标记获取宽高
    fn parse_jpeg_dimensions(data: &[u8]) -> Option<(u32, u32)> {
        let mut i = 2; // 跳过 SOI (FF D8)
        while i + 8 < data.len() {
            if data[i] != 0xFF {
                i += 1;
                continue;
            }
            let marker = data[i + 1];
            // SOF0=0xC0, SOF2=0xC2 等
            if (0xC0..=0xCF).contains(&marker) && marker != 0xC4 && marker != 0xC8 && marker != 0xCC {
                let height = u16::from_be_bytes([data[i + 5], data[i + 6]]) as u32;
                let width = u16::from_be_bytes([data[i + 7], data[i + 8]]) as u32;
                return Some((width, height));
            }
            // 跳过当前 marker 段
            if i + 3 < data.len() {
                let seg_len = u16::from_be_bytes([data[i + 2], data[i + 3]]) as usize;
                i += 2 + seg_len;
            } else {
                break;
            }
        }
        None
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
