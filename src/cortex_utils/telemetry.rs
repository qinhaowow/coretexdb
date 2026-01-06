//! Telemetry utilities for CortexDB.
//!
//! This module provides telemetry and tracing functionality for CortexDB, including:
//! - Distributed tracing
//! - OpenTelemetry integration
//! - Span creation and management
//! - Trace context propagation
//! - Telemetry configuration

use opentelemetry::global;
use opentelemetry::sdk::export::trace::stdout;
use opentelemetry::sdk::trace::{BatchConfig, TracerProvider};
use opentelemetry::sdk::{trace, Resource};
use opentelemetry::{trace::Tracer, KeyValue};
use std::sync::Arc;
use std::time::Duration;
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::prelude::__tracing_subscriber_SubscriberExt;
use tracing_subscriber::Registry;

/// Telemetry configuration
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct TelemetryConfig {
    /// Enable telemetry
    pub enabled: bool,
    /// Exporter type (stdout, jaeger, zipkin, etc.)
    pub exporter: String,
    /// Service name
    pub service_name: String,
    /// Service version
    pub service_version: String,
    /// Sampling rate (0.0 to 1.0)
    pub sampling_rate: f64,
    /// Batch size for span export
    pub batch_size: usize,
    /// Batch timeout in milliseconds
    pub batch_timeout_ms: u64,
    /// Jaeger endpoint (if using jaeger exporter)
    pub jaeger_endpoint: Option<String>,
    /// Zipkin endpoint (if using zipkin exporter)
    pub zipkin_endpoint: Option<String>,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            exporter: "stdout".to_string(),
            service_name: "cortexdb".to_string(),
            service_version: "0.1.0".to_string(),
            sampling_rate: 1.0,
            batch_size: 1024,
            batch_timeout_ms: 1000,
            jaeger_endpoint: None,
            zipkin_endpoint: None,
        }
    }
}

/// Telemetry initialization
pub struct Telemetry {
    /// Telemetry configuration
    config: TelemetryConfig,
    /// Tracer provider
    tracer_provider: Option<Arc<TracerProvider>>,
}

impl Telemetry {
    /// Create a new telemetry instance
    pub fn new(config: TelemetryConfig) -> Self {
        Self {
            config,
            tracer_provider: None,
        }
    }

    /// Initialize telemetry
    pub fn init(&mut self) -> Result<(), anyhow::Error> {
        if !self.config.enabled {
            return Ok(());
        }

        let resource = Resource::new(vec![
            KeyValue::new("service.name", self.config.service_name.clone()),
            KeyValue::new("service.version", self.config.service_version.clone()),
            KeyValue::new("service.instance.id", crate::cortex_utils::id_generator::generate_short_id()),
        ]);

        let tracer_provider = match self.config.exporter.as_str() {
            "stdout" => {
                let exporter = stdout::Exporter::default();
                let provider = TracerProvider::builder()
                    .with_resource(resource)
                    .with_batch_exporter(exporter, BatchConfig::default())
                    .build();
                Arc::new(provider)
            }
            "jaeger" => {
                // In a real implementation, we'd configure Jaeger exporter
                let exporter = stdout::Exporter::default();
                let provider = TracerProvider::builder()
                    .with_resource(resource)
                    .with_batch_exporter(exporter, BatchConfig::default())
                    .build();
                Arc::new(provider)
            }
            "zipkin" => {
                // In a real implementation, we'd configure Zipkin exporter
                let exporter = stdout::Exporter::default();
                let provider = TracerProvider::builder()
                    .with_resource(resource)
                    .with_batch_exporter(exporter, BatchConfig::default())
                    .build();
                Arc::new(provider)
            }
            _ => {
                return Err(anyhow::anyhow!("Unsupported exporter: {}", self.config.exporter));
            }
        };

        self.tracer_provider = Some(tracer_provider.clone());

        // Set global tracer provider
        global::set_tracer_provider(tracer_provider);

        // Create OpenTelemetry layer
        let tracer = global::tracer("cortexdb");
        let telemetry = OpenTelemetryLayer::new(tracer);

        // Add telemetry layer to subscriber
        let subscriber = Registry::default().with(telemetry);
        tracing::subscriber::set_global_default(subscriber)?;

        Ok(())
    }

    /// Shutdown telemetry
    pub fn shutdown(&mut self) -> Result<(), anyhow::Error> {
        if let Some(provider) = &self.tracer_provider {
            provider.shutdown()?;
        }
        Ok(())
    }
}

/// Trace span utilities
pub mod trace {
    use super::*;
    use tracing::{info_span, span, Instrument, Level};

    /// Create a trace span
    pub fn span(name: &str) -> tracing::Span {
        span!(Level::INFO, name)
    }

    /// Create a trace span with attributes
    pub fn span_with_attributes(name: &str, attributes: &[(&str, &str)]) -> tracing::Span {
        let mut span = info_span!(name);
        for (key, value) in attributes {
            span = span.record(key, value);
        }
        span
    }

    /// Instrument a future with a span
    pub fn instrument<F>(fut: F, span: tracing::Span) -> impl futures::Future<Output = F::Output>
    where
        F: futures::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        fut.instrument(span)
    }
}

/// Initialize telemetry with default configuration
pub fn init_default_telemetry() -> Result<Telemetry, anyhow::Error> {
    let config = TelemetryConfig::default();
    let mut telemetry = Telemetry::new(config);
    telemetry.init()?;
    Ok(telemetry)
}

/// Initialize telemetry with custom configuration
pub fn init_telemetry(config: TelemetryConfig) -> Result<Telemetry, anyhow::Error> {
    let mut telemetry = Telemetry::new(config);
    telemetry.init()?;
    Ok(telemetry)
}

/// Test utilities for telemetry
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_telemetry_config_default() {
        let config = TelemetryConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.exporter, "stdout");
        assert_eq!(config.service_name, "cortexdb");
        assert_eq!(config.sampling_rate, 1.0);
    }

    #[test]
    fn test_telemetry_initialization() {
        let config = TelemetryConfig {
            enabled: true,
            ..Default::default()
        };
        
        let mut telemetry = Telemetry::new(config);
        let result = telemetry.init();
        assert!(result.is_ok());
        
        let shutdown_result = telemetry.shutdown();
        assert!(shutdown_result.is_ok());
    }
}
