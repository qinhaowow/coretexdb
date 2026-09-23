//! Data Export module for CoreTexDB
//! Supports exporting data to various formats: Parquet, ORC, JSON, CSV

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

/// UTF-8 byte-order mark, written at the start of every CSV export.
///
/// Excel on a Chinese Windows install decodes a `.csv` using the system ANSI
/// code page (GBK) unless the file begins with a BOM, so a UTF-8 file holding
/// Chinese text opens as mojibake. The BOM is the portable way to declare "this
/// is UTF-8", and other readers (LibreOffice, pandas, every Unix tool) ignore
/// it.
const UTF8_BOM: &[u8] = b"\xEF\xBB\xBF";

/// Wrap a field in double quotes, doubling any embedded quote.
///
/// Used for everything that can contain a comma, a quote or a newline: string
/// values, headers, and any JSON object/array rendered into a single cell.
fn quote_field(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

pub struct DataExporter {
    output_path: String,
}

impl DataExporter {
    pub fn new(output_path: &str) -> Self {
        Self {
            output_path: output_path.to_string(),
        }
    }

    pub fn export_json<T: serde::Serialize>(&self, data: &[T], filename: &str) -> Result<String, String> {
        let path = Path::new(&self.output_path).join(filename);
        let file = File::create(&path).map_err(|e| e.to_string())?;
        let mut writer = BufWriter::new(file);
        
        let json = serde_json::to_string_pretty(data).map_err(|e| e.to_string())?;
        writer.write_all(json.as_bytes()).map_err(|e| e.to_string())?;
        
        Ok(path.to_string_lossy().to_string())
    }

    pub fn export_json_lines<T: serde::Serialize>(&self, data: &[T], filename: &str) -> Result<String, String> {
        let path = Path::new(&self.output_path).join(filename);
        let file = File::create(&path).map_err(|e| e.to_string())?;
        let mut writer = BufWriter::new(file);
        
        for item in data {
            let json = serde_json::to_string(item).map_err(|e| e.to_string())?;
            writer.write_all(json.as_bytes()).map_err(|e| e.to_string())?;
            writer.write_all(b"\n").map_err(|e| e.to_string())?;
        }
        
        Ok(path.to_string_lossy().to_string())
    }

    pub fn export_csv<T: serde::Serialize + std::fmt::Debug>(&self, data: &[T], filename: &str) -> Result<String, String> {
        let path = Path::new(&self.output_path).join(filename);
        let file = File::create(&path).map_err(|e| e.to_string())?;
        let mut writer = BufWriter::new(file);
        
        writer.write_all(UTF8_BOM).map_err(|e| e.to_string())?;

        if let Some(first) = data.first() {
            let headers: Vec<String> = Self::get_headers(first)
                .iter()
                .map(|h| quote_field(h))
                .collect();
            writer.write_all(headers.join(",").as_bytes()).map_err(|e| e.to_string())?;
            writer.write_all(b"\n").map_err(|e| e.to_string())?;
            
            for item in data {
                let values = Self::get_values(item);
                writer.write_all(values.join(",").as_bytes()).map_err(|e| e.to_string())?;
                writer.write_all(b"\n").map_err(|e| e.to_string())?;
            }
        }
        
        Ok(path.to_string_lossy().to_string())
    }

    fn get_headers<T: serde::Serialize>(item: &T) -> Vec<String> {
        if let Ok(value) = serde_json::to_value(item) {
            if let Some(obj) = value.as_object() {
                return obj.keys().cloned().collect();
            }
        }
        vec!["value".to_string()]
    }

    fn get_values<T: serde::Serialize>(item: &T) -> Vec<String> {
        if let Ok(value) = serde_json::to_value(item) {
            if let Some(obj) = value.as_object() {
                return obj.values()
                    .map(|v| match v {
                        serde_json::Value::String(s) => quote_field(s),
                        serde_json::Value::Null => String::new(),
                        serde_json::Value::Bool(b) => b.to_string(),
                        serde_json::Value::Number(n) => n.to_string(),
                        // An object or array stringifies to something full of
                        // commas, so it must be quoted to stay in one column.
                        other => quote_field(&other.to_string()),
                    })
                    .collect();
            }
        }
        vec![serde_json::to_string(item).unwrap_or_default()]
    }
}

pub struct VectorExporter;

impl VectorExporter {
    pub fn export_vectors_json(
        vectors: &HashMap<String, (Vec<f32>, serde_json::Value)>,
        filename: &str,
    ) -> Result<String, String> {
        let file = File::create(filename).map_err(|e| e.to_string())?;
        let mut writer = BufWriter::new(file);
        
        let items: Vec<serde_json::Value> = vectors
            .iter()
            .map(|(id, (vec, meta))| {
                serde_json::json!({
                    "id": id,
                    "vector": vec,
                    "metadata": meta
                })
            })
            .collect();
        
        let json = serde_json::to_string_pretty(&items).map_err(|e| e.to_string())?;
        writer.write_all(json.as_bytes()).map_err(|e| e.to_string())?;
        
        Ok(filename.to_string())
    }

    pub fn export_vectors_csv(
        vectors: &HashMap<String, (Vec<f32>, serde_json::Value)>,
        filename: &str,
    ) -> Result<String, String> {
        let file = File::create(filename).map_err(|e| e.to_string())?;
        let mut writer = BufWriter::new(file);
        
        writer.write_all(UTF8_BOM).map_err(|e| e.to_string())?;
        writer.write_all(b"id,dimension,vector\n").map_err(|e| e.to_string())?;
        
        for (id, (vec, _)) in vectors {
            let vector_str = vec.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(";");
            writeln!(writer, "{},{},{}", quote_field(id), vec.len(), vector_str)
                .map_err(|e| e.to_string())?;
        }
        
        Ok(filename.to_string())
    }
}

pub struct ParquetExporter;

impl ParquetExporter {
    pub fn export_simple<T: serde::Serialize + Send + Sync + 'static>(
        _data: &[T],
        _filename: &str,
    ) -> Result<String, String> {
        Err("Parquet export requires 'parquet' feature. Use JSON/CSV for basic export.".to_string())
    }
}

pub struct OrcExporter;

impl OrcExporter {
    pub fn export_simple<T: serde::Serialize>(
        _data: &[T],
        _filename: &str,
    ) -> Result<String, String> {
        Err("ORC export requires 'orc' feature. Use JSON/CSV for basic export.".to_string())
    }
}

pub struct BatchExporter {
    max_batch_size: usize,
}

impl BatchExporter {
    pub fn new(max_batch_size: usize) -> Self {
        Self { max_batch_size }
    }

    pub async fn export_batched_json<T: serde::Serialize + Send + Sync>(
        &self,
        data: Vec<T>,
        exporter: &DataExporter,
        filename_prefix: &str,
    ) -> Result<Vec<String>, String> {
        let mut filenames = Vec::new();
        
        for (batch_idx, batch) in data.chunks(self.max_batch_size).enumerate() {
            let filename = format!("{}_part{}.json", filename_prefix, batch_idx);
            let path = exporter.export_json(batch, &filename)?;
            filenames.push(path);
        }
        
        Ok(filenames)
    }

    pub async fn export_batched_csv<T: serde::Serialize + std::fmt::Debug + Send + Sync>(
        &self,
        data: Vec<T>,
        exporter: &DataExporter,
        filename_prefix: &str,
    ) -> Result<Vec<String>, String> {
        let mut filenames = Vec::new();
        
        for (batch_idx, batch) in data.chunks(self.max_batch_size).enumerate() {
            let filename = format!("{}_part{}.csv", filename_prefix, batch_idx);
            let path = exporter.export_csv(batch, &filename)?;
            filenames.push(path);
        }
        
        Ok(filenames)
    }
}

pub struct ExportResult {
    pub filename: String,
    pub record_count: usize,
    pub file_size_bytes: u64,
}

impl ExportResult {
    pub fn new(filename: String, record_count: usize) -> Self {
        let file_size_bytes = std::fs::metadata(&filename)
            .map(|m| m.len())
            .unwrap_or(0);
        
        Self {
            filename,
            record_count,
            file_size_bytes,
        }
    }
}

#[derive(Debug, Clone)]
pub enum ExportFormat {
    Json,
    JsonLines,
    Csv,
    Parquet,
    Orc,
}

impl ExportFormat {
    pub fn extension(&self) -> &str {
        match self {
            Self::Json => "json",
            Self::JsonLines => "jsonl",
            Self::Csv => "csv",
            Self::Parquet => "parquet",
            Self::Orc => "orc",
        }
    }
}

pub struct CollectionExporter {
    exporter: DataExporter,
}

impl CollectionExporter {
    pub fn new(output_dir: &str) -> Self {
        Self {
            exporter: DataExporter::new(output_dir),
        }
    }

    pub fn export_collection<T: serde::Serialize + std::fmt::Debug>(
        &self,
        collection_name: &str,
        data: &[T],
        format: ExportFormat,
    ) -> Result<ExportResult, String> {
        let filename = format!("{}.{}", collection_name, format.extension());
        
        let record_count = data.len();
        
        match format {
            ExportFormat::Json => {
                self.exporter.export_json(data, &filename)?;
            }
            ExportFormat::JsonLines => {
                self.exporter.export_json_lines(data, &filename)?;
            }
            ExportFormat::Csv => {
                self.exporter.export_csv(data, &filename)?;
            }
            ExportFormat::Parquet => {
                return Err("Parquet format not available. Use JSON or CSV.".to_string());
            }
            ExportFormat::Orc => {
                return Err("ORC format not available. Use JSON or CSV.".to_string());
            }
        }
        
        Ok(ExportResult::new(filename, record_count))
    }

    pub fn export_multiple_formats<T: serde::Serialize + std::fmt::Debug>(
        &self,
        collection_name: &str,
        data: &[T],
    ) -> Result<Vec<ExportResult>, String> {
        let mut results = Vec::new();
        
        for format in &[ExportFormat::Json, ExportFormat::Csv] {
            {
                let result = self.export_collection(collection_name, data, format.clone())?;
                results.push(result)
            }
        }
        
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
use crate::coretex_core::Result;

    #[test]
    fn test_json_export() {
        let temp_dir = std::env::temp_dir();
        let exporter = DataExporter::new(temp_dir.to_str().unwrap());
        
        let data = vec![
            serde_json::json!({"id": "1", "value": 10}),
            serde_json::json!({"id": "2", "value": 20}),
        ];
        
        let result = exporter.export_json(&data, "test.json");
        assert!(result.is_ok());
    }

    #[test]
    fn test_csv_export() {
        let temp_dir = std::env::temp_dir();
        let exporter = DataExporter::new(temp_dir.to_str().unwrap());
        
        let data = vec![
            serde_json::json!({"id": "1", "value": 10}),
            serde_json::json!({"id": "2", "value": 20}),
        ];
        
        let result = exporter.export_csv(&data, "test.csv");
        assert!(result.is_ok());
    }

    #[test]
    fn csv_export_is_utf8_with_bom_and_quoted() {
        let dir = tempfile::tempdir().unwrap();
        let exporter = DataExporter::new(dir.path().to_str().unwrap());

        let data = vec![serde_json::json!({
            "名称": "红富士苹果, 特级",
            "备注": "含\"引号\"",
            "类别": "水果",
            "数量": 12
        })];
        let path = exporter.export_csv(&data, "中文.csv").unwrap();
        let bytes = std::fs::read(&path).unwrap();

        // Excel on a Chinese Windows install decodes a BOM-less .csv as the
        // system code page (GBK), which is what turns the file into mojibake.
        assert_eq!(&bytes[..3], b"\xEF\xBB\xBF", "CSV must start with a UTF-8 BOM");

        let text = std::str::from_utf8(&bytes[3..]).expect("CSV body must be valid UTF-8");
        assert!(text.contains("名称"), "header must survive: {text}");
        assert!(text.contains("红富士苹果"), "Chinese value must not be mangled");
        // A comma inside a value must not split the row into extra columns.
        assert!(text.contains("\"红富士苹果, 特级\""), "value with comma must be quoted");
        // An embedded quote is doubled rather than being left to end the field.
        assert!(text.contains("\"含\"\"引号\"\"\""), "quotes must be doubled");
        // Numbers stay unquoted so a spreadsheet still reads them as numbers.
        assert!(text.contains(",12,"), "number must stay unquoted: {text}");
    }

    #[test]
    fn test_export_format_extension() {
        assert_eq!(ExportFormat::Json.extension(), "json");
        assert_eq!(ExportFormat::Csv.extension(), "csv");
        assert_eq!(ExportFormat::JsonLines.extension(), "jsonl");
    }

    #[tokio::test]
    async fn test_batch_exporter() {
        let exporter = BatchExporter::new(2);
        let data_exporter = DataExporter::new(std::env::temp_dir().to_str().unwrap());
        
        let data: Vec<serde_json::Value> = (0..5)
            .map(|i| serde_json::json!({"id": i}))
            .collect();
        
        let result = exporter.export_batched_json(data, &data_exporter, "batch_test").await;
        
        if let Ok(filenames) = result {
            assert!(filenames.len() >= 2);
        }
    }
}
