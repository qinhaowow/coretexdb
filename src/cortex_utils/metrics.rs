//! Metrics utilities for CortexDB.
//!
//! This module provides metrics and monitoring functionality for CortexDB, including:
//! - Prometheus metrics integration
//! - Counter metrics
//! - Gauge metrics
//! - Histogram metrics
//! - Summary metrics
//! - Metrics exporter

use prometheus::{
    Counter, CounterVec, Gauge, GaugeVec, Histogram, HistogramVec, IntCounter, IntCounterVec,
    IntGauge, IntGaugeVec, Opts, Registry,
};
use std::sync::Arc;
use std::time::Instant;

/// Metrics registry
pub struct MetricsRegistry {
    /// Prometheus registry
    registry: Registry,
    /// Request counter
    request_counter: IntCounterVec,
    /// Request latency histogram
    request_latency: HistogramVec,
    /// Error counter
    error_counter: IntCounterVec,
    /// Collection count gauge
    collection_count: IntGauge,
    /// Document count gauge
    document_count: IntGaugeVec,
    /// Index count gauge
    index_count: IntGaugeVec,
    /// Memory usage gauge
    memory_usage: Gauge,
    /// Storage usage gauge
    storage_usage: Gauge,
}

impl MetricsRegistry {
    /// Create a new metrics registry
    pub fn new() -> Result<Self, anyhow::Error> {
        let registry = Registry::new();
        
        // Request counter
        let request_counter = IntCounterVec::new(
            Opts::new("cortexdb_requests_total", "Total number of requests"),
            &["endpoint", "method", "status"],
        )?;
        registry.register(Box::new(request_counter.clone()))?;
        
        // Request latency
        let request_latency = HistogramVec::new(
            Opts::new("cortexdb_request_duration_seconds", "Request duration in seconds"),
            &["endpoint", "method"],
        )?;
        registry.register(Box::new(request_latency.clone()))?;
        
        // Error counter
        let error_counter = IntCounterVec::new(
            Opts::new("cortexdb_errors_total", "Total number of errors"),
            &["error_type", "endpoint"],
        )?;
        registry.register(Box::new(error_counter.clone()))?;
        
        // Collection count
        let collection_count = IntGauge::new("cortexdb_collections_total", "Total number of collections")?;
        registry.register(Box::new(collection_count.clone()))?;
        
        // Document count
        let document_count = IntGaugeVec::new(
            Opts::new("cortexdb_documents_total", "Total number of documents"),
            &["collection"],
        )?;
        registry.register(Box::new(document_count.clone()))?;
        
        // Index count
        let index_count = IntGaugeVec::new(
            Opts::new("cortexdb_indexes_total", "Total number of indexes"),
            &["collection", "index_type"],
        )?;
        registry.register(Box::new(index_count.clone()))?;
        
        // Memory usage
        let memory_usage = Gauge::new("cortexdb_memory_usage_bytes", "Memory usage in bytes")?;
        registry.register(Box::new(memory_usage.clone()))?;
        
        // Storage usage
        let storage_usage = Gauge::new("cortexdb_storage_usage_bytes", "Storage usage in bytes")?;
        registry.register(Box::new(storage_usage.clone()))?;
        
        Ok(Self {
            registry,
            request_counter,
            request_latency,
            error_counter,
            collection_count,
            document_count,
            index_count,
            memory_usage,
            storage_usage,
        })
    }
    
    /// Get the Prometheus registry
    pub fn registry(&self) -> &Registry {
        &self.registry
    }
    
    /// Increment request counter
    pub fn increment_request_counter(&self, endpoint: &str, method: &str, status: &str) {
        self.request_counter
            .with_label_values(&[endpoint, method, status])
            .inc();
    }
    
    /// Record request latency
    pub fn record_request_latency(&self, endpoint: &str, method: &str, duration: f64) {
        self.request_latency
            .with_label_values(&[endpoint, method])
            .observe(duration);
    }
    
    /// Increment error counter
    pub fn increment_error_counter(&self, error_type: &str, endpoint: &str) {
        self.error_counter
            .with_label_values(&[error_type, endpoint])
            .inc();
    }
    
    /// Set collection count
    pub fn set_collection_count(&self, count: i64) {
        self.collection_count.set(count);
    }
    
    /// Set document count for a collection
    pub fn set_document_count(&self, collection: &str, count: i64) {
        self.document_count
            .with_label_values(&[collection])
            .set(count);
    }
    
    /// Set index count for a collection
    pub fn set_index_count(&self, collection: &str, index_type: &str, count: i64) {
        self.index_count
            .with_label_values(&[collection, index_type])
            .set(count);
    }
    
    /// Set memory usage
    pub fn set_memory_usage(&self, usage: f64) {
        self.memory_usage.set(usage);
    }
    
    /// Set storage usage
    pub fn set_storage_usage(&self, usage: f64) {
        self.storage_usage.set(usage);
    }
}

/// Metrics recorder for request latency
pub struct RequestMetricsGuard<'a> {
    /// Metrics registry
    registry: &'a MetricsRegistry,
    /// Endpoint
    endpoint: String,
    /// HTTP method
    method: String,
    /// Start time
    start_time: Instant,
}

impl<'a> RequestMetricsGuard<'a> {
    /// Create a new request metrics guard
    pub fn new(registry: &'a MetricsRegistry, endpoint: &str, method: &str) -> Self {
        Self {
            registry,
            endpoint: endpoint.to_string(),
            method: method.to_string(),
            start_time: Instant::now(),
        }
    }
    
    /// Record successful request
    pub fn record_success(self, status: &str) {
        let duration = self.start_time.elapsed().as_secs_f64();
        self.registry.increment_request_counter(&self.endpoint, &self.method, status);
        self.registry.record_request_latency(&self.endpoint, &self.method, duration);
    }
    
    /// Record failed request
    pub fn record_failure(self, status: &str, error_type: &str) {
        let duration = self.start_time.elapsed().as_secs_f64();
        self.registry.increment_request_counter(&self.endpoint, &self.method, status);
        self.registry.record_request_latency(&self.endpoint, &self.method, duration);
        self.registry.increment_error_counter(error_type, &self.endpoint);
    }
}

/// Metrics manager
pub struct MetricsManager {
    /// Metrics registry
    registry: Arc<MetricsRegistry>,
}

impl MetricsManager {
    /// Create a new metrics manager
    pub fn new(registry: Arc<MetricsRegistry>) -> Self {
        Self { registry }
    }
    
    /// Get the metrics registry
    pub fn registry(&self) -> &Arc<MetricsRegistry> {
        &self.registry
    }
    
    /// Create a request metrics guard
    pub fn start_request(&self, endpoint: &str, method: &str) -> RequestMetricsGuard {
        RequestMetricsGuard::new(&self.registry, endpoint, method)
    }
    
    /// Update system metrics
    pub fn update_system_metrics(&self) {
        // In a real implementation, we'd collect system metrics like memory usage
        // For now, we'll just set placeholder values
        self.registry.set_memory_usage(0.0);
        self.registry.set_storage_usage(0.0);
    }
}

/// Initialize metrics registry
pub fn init_metrics_registry() -> Result<Arc<MetricsRegistry>, anyhow::Error> {
    let registry = MetricsRegistry::new()?;
    Ok(Arc::new(registry))
}

/// Initialize metrics manager
pub fn init_metrics_manager() -> Result<MetricsManager, anyhow::Error> {
    let registry = init_metrics_registry()?;
    Ok(MetricsManager::new(registry))
}

/// Test utilities for metrics
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_registry_creation() {
        let result = MetricsRegistry::new();
        assert!(result.is_ok());
    }

    #[test]
    fn test_metrics_manager_creation() {
        let result = init_metrics_manager();
        assert!(result.is_ok());
    }

    #[test]
    fn test_request_metrics_guard() {
        let registry = MetricsRegistry::new().unwrap();
        let manager = MetricsManager::new(Arc::new(registry));
        
        let guard = manager.start_request("/api/collections", "GET");
        // Simulate request processing
        std::thread::sleep(std::time::Duration::from_millis(10));
        guard.record_success("200");
    }
}
