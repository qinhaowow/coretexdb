//! Hybrid Query Interface for CoreTexDB
//! Provides unified query structure for multi-modal search

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HybridQuery {
    pub vector_query: Option<VectorQuery>,
    pub text_query: Option<TextQuery>,
    pub scalar_filters: Vec<ScalarFilter>,
    pub geo_filter: Option<GeoFilter>,
    pub time_range: Option<TimeRange>,
    pub top_k: usize,
    pub weights: QueryWeights,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorQuery {
    pub vector: Vec<f32>,
    pub metric: DistanceMetric,
    pub filter: Option<HashMap<String, serde_json::Value>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TextQuery {
    pub query: String,
    pub fields: Vec<String>,
    pub boost: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScalarFilter {
    pub field: String,
    pub operator: FilterOperator,
    pub value: ScalarFilterValue,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op")]
pub enum FilterOperator {
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
    In,
    Between { and: ScalarFilterValue },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ScalarFilterValue {
    Integer(i64),
    Float(f64),
    Boolean(bool),
    String(String),
    StringList(Vec<String>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeoFilter {
    pub field: String,
    pub geo_relation: GeoRelation,
    pub value: GeoValue,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GeoRelation {
    Within,
    Intersects,
    Contains,
    Disjoint,
    Near,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeoValue {
    pub geo_type: GeoType,
    pub coordinates: GeoCoordinates,
    pub distance: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GeoType {
    Point,
    Circle { radius: f64 },
    BoundingBox { min_lat: f64, max_lat: f64, min_lon: f64, max_lon: f64 },
    Polygon,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeoCoordinates {
    pub latitude: f64,
    pub longitude: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeRange {
    pub field: String,
    pub start: Option<chrono::DateTime<chrono::Utc>>,
    pub end: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryWeights {
    pub vector_weight: f32,
    pub text_weight: f32,
    pub scalar_weight: f32,
    pub geo_weight: f32,
    pub time_weight: f32,
}

impl Default for QueryWeights {
    fn default() -> Self {
        Self {
            vector_weight: 1.0,
            text_weight: 1.0,
            scalar_weight: 1.0,
            geo_weight: 1.0,
            time_weight: 1.0,
        }
    }
}

impl Default for HybridQuery {
    fn default() -> Self {
        Self {
            vector_query: None,
            text_query: None,
            scalar_filters: Vec::new(),
            geo_filter: None,
            time_range: None,
            top_k: 10,
            weights: QueryWeights::default(),
        }
    }
}

impl HybridQuery {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_vector(mut self, vector: Vec<f32>, metric: DistanceMetric) -> Self {
        self.vector_query = Some(VectorQuery {
            vector,
            metric,
            filter: None,
        });
        self
    }

    pub fn with_text(mut self, query: impl Into<String>) -> Self {
        self.text_query = Some(TextQuery {
            query: query.into(),
            fields: vec!["content".to_string()],
            boost: 1.0,
        });
        self
    }

    pub fn with_filter(mut self, field: impl Into<String>, operator: FilterOperator, value: ScalarFilterValue) -> Self {
        self.scalar_filters.push(ScalarFilter {
            field: field.into(),
            operator,
            value,
        });
        self
    }

    pub fn with_top_k(mut self, k: usize) -> Self {
        self.top_k = k;
        self
    }

    pub fn with_weights(mut self, weights: QueryWeights) -> Self {
        self.weights = weights;
        self
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum DistanceMetric {
    Cosine,
    Euclidean,
    DotProduct,
    Manhattan,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_creation() {
        let query = HybridQuery::new()
            .with_vector(vec![0.1, 0.2, 0.3], DistanceMetric::Cosine)
            .with_text("machine learning")
            .with_filter("age", FilterOperator::Gte, ScalarFilterValue::Integer(18))
            .with_top_k(20);

        assert!(query.vector_query.is_some());
        assert!(query.text_query.is_some());
        assert_eq!(query.scalar_filters.len(), 1);
        assert_eq!(query.top_k, 20);
    }
}
