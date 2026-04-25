//! Document Processing Module for CoreTexDB
//! Handles parsing and processing of unstructured documents

pub mod parser;
pub mod vector;

pub use parser::{DocumentParser, ParsedDocument, ImageData, TableData, PdfParser, ImageParser, AudioParser, DocumentParserRegistry};
pub use vector::{HighDimVector, CompressionType, HighDimVectorStore, PQCompressor};
