//! Multi-modal Hybrid Search Engine for CoreTexDB
//! Provides unified search across vector, text, scalar, and other data types

pub mod document;
pub mod query;
pub mod fusion;
pub mod retriever;

pub use document::{MultiModalDocument, VectorData, TextData, ScalarValue, TimeSeriesData, GeoLocation};
pub use query::{HybridQuery, VectorQuery, TextQuery, ScalarFilter, FilterOperator, GeoFilter, QueryWeights, DistanceMetric};
pub use fusion::{ScoreFusion, ScoreFusionEngine, MultiModalResult, FusedResult};
pub use retriever::{HybridRetriever, VectorRetriever, TextRetriever, TextSearchResult, BruteForceVectorAdapter, BM25TextAdapter};
