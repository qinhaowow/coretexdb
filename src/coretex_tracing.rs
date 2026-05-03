//! OpenTelemetry tracing integration for CoreTexDB
//! Provides distributed tracing for queries, transactions, and network operations

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceConfig {
    pub service_name: String,
    pub exporter_type: TraceExporter,
    pub endpoint: String,
    pub sample_rate: f64,
    pub enable_logging: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TraceExporter {
    Jaeger,
    Zipkin,
    Console,
    None,
}

impl Default for TraceConfig {
    fn default() -> Self {
        Self {
            service_name: "coretexdb".to_string(),
            exporter_type: TraceExporter::Console,
            endpoint: "http://localhost:14268/api/traces".to_string(),
            sample_rate: 1.0,
            enable_logging: true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Span {
    pub trace_id: String,
    pub span_id: String,
    pub parent_id: Option<String>,
    pub name: String,
    pub start_time: Instant,
    pub end_time: Option<Instant>,
    pub attributes: HashMap<String, String>,
    pub status: SpanStatus,
    pub span_kind: SpanKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SpanKind {
    Server,
    Client,
    Producer,
    Consumer,
    Internal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SpanStatus {
    Unset,
    Ok,
    Error,
}

pub struct Tracer {
    config: TraceConfig,
    active_spans: Arc<RwLock<HashMap<String, Span>>>,
    completed_spans: Arc<RwLock<Vec<Span>>>,
    trace_id_counter: Arc<RwLock<u64>>,
}

impl Tracer {
    pub fn new(config: TraceConfig) -> Self {
        Self {
            config,
            active_spans: Arc::new(RwLock::new(HashMap::new())),
            completed_spans: Arc::new(RwLock::new(Vec::new())),
            trace_id_counter: Arc::new(RwLock::new(0)),
        }
    }

    pub fn config(&self) -> &TraceConfig {
        &self.config
    }

    pub async fn start_span(&self, name: &str, parent_id: Option<&str>) -> SpanBuilder {
        let trace_id = self.generate_trace_id().await;
        let span_id = self.generate_span_id();
        
        let span = Span {
            trace_id: trace_id.clone(),
            span_id: span_id.clone(),
            parent_id: parent_id.map(String::from),
            name: name.to_string(),
            start_time: Instant::now(),
            end_time: None,
            attributes: HashMap::new(),
            status: SpanStatus::Unset,
            span_kind: SpanKind::Internal,
        };
        
        let mut spans = self.active_spans.write().await;
        spans.insert(span_id.clone(), span);
        
        SpanBuilder {
            tracer: self.clone(),
            span_id,
            trace_id,
        }
    }

    async fn generate_trace_id(&self) -> String {
        let mut counter = self.trace_id_counter.write().await;
        *counter += 1;
        format!("{:016x}-{:016x}", *counter, rand::random::<u64>())
    }

    fn generate_span_id(&self) -> String {
        format!("{:016x}", rand::random::<u64>())
    }

    pub async fn end_span(&self, span_id: &str) {
        let mut spans = self.active_spans.write().await;
        
        if let Some(span) = spans.remove(span_id) {
            let mut completed = span;
            completed.end_time = Some(Instant::now());
            
            let mut completed_spans = self.completed_spans.write().await;
            completed_spans.push(completed);
            
            if self.config.enable_logging {
                self.export_span(&completed_spans[completed_spans.len() - 1]).await;
            }
        }
    }

    pub async fn add_attribute(&self, span_id: &str, key: &str, value: &str) {
        let mut spans = self.active_spans.write().await;
        
        if let Some(span) = spans.get_mut(span_id) {
            span.attributes.insert(key.to_string(), value.to_string());
        }
    }

    pub async fn set_status(&self, span_id: &str, status: SpanStatus) {
        let mut spans = self.active_spans.write().await;
        
        if let Some(span) = spans.get_mut(span_id) {
            span.status = status;
        }
    }

    async fn export_span(&self, span: &Span) {
        let duration = span.end_time
            .map(|t| t.duration_since(span.start_time))
            .unwrap_or(Duration::from_secs(0));
        
        eprintln!(
            "[TRACE] {} - {} ({:?}) - attrs: {:?}",
            span.name,
            span.trace_id,
            duration,
            span.attributes
        );
    }

    pub async fn get_completed_spans(&self) -> Vec<Span> {
        let spans = self.completed_spans.read().await;
        spans.clone()
    }

    pub async fn clear_completed_spans(&self) {
        let mut spans = self.completed_spans.write().await;
        spans.clear();
    }

    pub async fn get_span(&self, span_id: &str) -> Option<Span> {
        let spans = self.active_spans.read().await;
        spans.get(span_id).cloned()
    }
}

impl Clone for Tracer {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            active_spans: self.active_spans.clone(),
            completed_spans: self.completed_spans.clone(),
            trace_id_counter: self.trace_id_counter.clone(),
        }
    }
}

pub struct SpanBuilder {
    tracer: Tracer,
    span_id: String,
    trace_id: String,
}

impl SpanBuilder {
    pub async fn with_attribute(mut self, key: &str, value: &str) -> Self {
        self.tracer.add_attribute(&self.span_id, key, value).await;
        self
    }

    pub async fn with_kind(mut self, kind: SpanKind) -> Self {
        {
            let mut spans = self.tracer.active_spans.write().await;
            if let Some(span) = spans.get_mut(&self.span_id) {
                span.span_kind = kind;
            }
        }
        self
    }

    pub async fn start(self) -> String {
        self.span_id
    }

    pub async fn end(self) {
        self.tracer.end_span(&self.span_id).await;
    }
}

pub struct QueryTracer;

impl QueryTracer {
    pub async fn trace_query(
        tracer: &Tracer,
        collection: &str,
        query_type: &str,
    ) -> SpanBuilder {
        let builder = tracer
            .start_span(
                &format!("query:{}:{}", collection, query_type),
                None,
            )
            .await;
        
        builder
            .with_attribute("db.system", "coretexdb")
            .await
            .with_attribute("db.name", collection)
            .await
            .with_attribute("db.operation", query_type)
            .await
    }

    pub async fn trace_transaction(
        tracer: &Tracer,
        txn_id: &str,
    ) -> SpanBuilder {
        tracer
            .start_span(&format!("transaction:{}", txn_id), None)
            .await
            .with_attribute("txn.id", txn_id)
            .await
    }

    pub async fn trace_index_build(
        tracer: &Tracer,
        index_type: &str,
        collection: &str,
    ) -> SpanBuilder {
        tracer
            .start_span(&format!("index_build:{}", index_type), None)
            .await
            .with_attribute("index.type", index_type)
            .await
            .with_attribute("db.name", collection)
            .await
    }

    pub async fn trace_network(
        tracer: &Tracer,
        peer: &str,
        operation: &str,
    ) -> SpanBuilder {
        tracer
            .start_span(operation, None)
            .await
            .with_attribute("network.peer.address", peer)
            .await
            .with_attribute("network.operation", operation)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_trace_config_default() {
        let config = TraceConfig::default();
        
        assert_eq!(config.service_name, "coretexdb");
        assert_eq!(config.exporter_type, TraceExporter::Console);
    }

    #[tokio::test]
    async fn test_tracer_new() {
        let tracer = Tracer::new(TraceConfig::default());
        
        let config = tracer.config();
        assert_eq!(config.service_name, "coretexdb");
    }

    #[tokio::test]
    async fn test_start_and_end_span() {
        let tracer = Tracer::new(TraceConfig::default());
        
        let builder = tracer.start_span("test_span", None).await;
        let span_id = builder.span_id.clone();
        
        let span = tracer.get_span(&span_id).await;
        assert!(span.is_some());
        
        tracer.end_span(&span_id).await;
        
        let span = tracer.get_span(&span_id).await;
        assert!(span.is_none());
        
        let completed = tracer.get_completed_spans().await;
        assert_eq!(completed.len(), 1);
    }

    #[tokio::test]
    async fn test_span_builder_attributes() {
        let tracer = Tracer::new(TraceConfig::default());
        
        tracer
            .start_span("test", None)
            .await
            .with_attribute("key", "value")
            .await
            .with_attribute("another", "value2")
            .await
            .end()
            .await;
        
        let completed = tracer.get_completed_spans().await;
        assert_eq!(completed[0].attributes.get("key"), Some(&"value".to_string()));
    }

    #[tokio::test]
    async fn test_span_status() {
        let tracer = Tracer::new(TraceConfig::default());
        
        let builder = tracer.start_span("test", None).await;
        let span_id = builder.span_id.clone();
        
        tracer.set_status(&span_id, SpanStatus::Error).await;
        
        tracer.end_span(&span_id).await;
        
        let completed = tracer.get_completed_spans().await;
        assert_eq!(completed[0].status, SpanStatus::Error);
    }

    #[tokio::test]
    async fn test_parent_span() {
        let tracer = Tracer::new(TraceConfig::default());
        
        let parent = tracer.start_span("parent", None).await;
        let parent_id = parent.span_id.clone();
        parent.end().await;
        
        let child = tracer.start_span("child", Some(&parent_id)).await;
        let child_id = child.span_id.clone();
        child.end().await;
        
        let completed = tracer.get_completed_spans().await;
        let child_span = completed.iter().find(|s| s.span_id == child_id);
        
        assert!(child_span.is_some());
        assert_eq!(child_span.unwrap().parent_id, Some(parent_id));
    }

    #[tokio::test]
    async fn test_clear_completed_spans() {
        let tracer = Tracer::new(TraceConfig::default());
        
        tracer.start_span("span1", None).await.end().await;
        tracer.start_span("span2", None).await.end().await;
        
        let completed = tracer.get_completed_spans().await;
        assert_eq!(completed.len(), 2);
        
        tracer.clear_completed_spans().await;
        
        let completed = tracer.get_completed_spans().await;
        assert!(completed.is_empty());
    }

    #[tokio::test]
    async fn test_query_tracer() {
        let tracer = Tracer::new(TraceConfig::default());
        
        let _span = QueryTracer::trace_query(&tracer, "test_collection", "search")
            .await
            .end()
            .await;
        
        let completed = tracer.get_completed_spans().await;
        assert_eq!(completed.len(), 1);
        assert!(completed[0].name.contains("query"));
    }

    #[tokio::test]
    async fn test_span_kinds() {
        let tracer = Tracer::new(TraceConfig::default());
        
        tracer
            .start_span("server_span", None)
            .await
            .with_kind(SpanKind::Server)
            .await
            .end()
            .await;
        
        let completed = tracer.get_completed_spans().await;
        assert_eq!(completed[0].span_kind, SpanKind::Server);
    }
}
