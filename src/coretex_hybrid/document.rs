//! Multi-modal Document Model for CoreTexDB
//! Provides unified document structure supporting vector, text, scalar, time-series, and graph data

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use chrono::{DateTime, Utc};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultiModalDocument {
    pub id: String,
    pub vector: Option<VectorData>,
    pub text: Option<TextData>,
    pub scalar_fields: HashMap<String, ScalarValue>,
    pub time_series: Option<TimeSeriesData>,
    pub geo_location: Option<GeoLocation>,
    pub metadata: HashMap<String, serde_json::Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorData {
    pub values: Vec<f32>,
    pub dimension: usize,
    pub model: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TextData {
    pub content: String,
    pub language: Option<String>,
    pub embeddings: Option<Vec<f32>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ScalarValue {
    Integer(i64),
    Float(f64),
    Boolean(bool),
    String(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeSeriesData {
    pub points: Vec<TimeSeriesPoint>,
    pub sampling_rate: Option<f64>,
    pub unit: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeSeriesPoint {
    pub timestamp: DateTime<Utc>,
    pub value: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeoLocation {
    pub latitude: f64,
    pub longitude: f64,
    pub altitude: Option<f64>,
    pub crs: Option<String>,
}

impl MultiModalDocument {
    pub fn new(id: String) -> Self {
        let now = Utc::now();
        Self {
            id,
            vector: None,
            text: None,
            scalar_fields: HashMap::new(),
            time_series: None,
            geo_location: None,
            metadata: HashMap::new(),
            created_at: now,
            updated_at: now,
        }
    }

    pub fn with_vector(mut self, vector: Vec<f32>) -> Self {
        let dimension = vector.len();
        self.vector = Some(VectorData {
            values: vector,
            dimension,
            model: None,
        });
        self
    }

    pub fn with_text(mut self, content: String) -> Self {
        self.text = Some(TextData {
            content,
            language: None,
            embeddings: None,
        });
        self
    }

    pub fn with_scalar(mut self, key: impl Into<String>, value: ScalarValue) -> Self {
        self.scalar_fields.insert(key.into(), value);
        self
    }

    pub fn with_time_series(mut self, points: Vec<TimeSeriesPoint>) -> Self {
        self.time_series = Some(TimeSeriesData {
            points,
            sampling_rate: None,
            unit: None,
        });
        self
    }

    pub fn with_geo(mut self, lat: f64, lon: f64) -> Self {
        self.geo_location = Some(GeoLocation {
            latitude: lat,
            longitude: lon,
            altitude: None,
            crs: None,
        });
        self
    }

    pub fn get_vector(&self) -> Option<&[f32]> {
        self.vector.as_ref().map(|v| v.values.as_slice())
    }

    pub fn get_text(&self) -> Option<&str> {
        self.text.as_ref().map(|t| t.content.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_document_creation() {
        let doc = MultiModalDocument::new("doc1".to_string())
            .with_vector(vec![0.1, 0.2, 0.3])
            .with_text("Hello world".to_string())
            .with_scalar("age", ScalarValue::Integer(25))
            .with_scalar("score", ScalarValue::Float(95.5))
            .with_geo(40.7128, -74.0060);

        assert_eq!(doc.id, "doc1");
        assert!(doc.vector.is_some());
        assert_eq!(doc.vector.as_ref().unwrap().dimension, 3);
        assert!(doc.text.is_some());
        assert_eq!(doc.scalar_fields.len(), 2);
        assert!(doc.geo_location.is_some());
    }
}
