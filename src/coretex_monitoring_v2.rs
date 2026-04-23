//! Monitoring and alerting module for CoreTexDB
//! Provides comprehensive metrics, monitoring, and alerting capabilities

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, RwLock};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonitoringConfig {
    pub enabled: bool,
    pub metrics_port: u16,
    pub scrape_interval_secs: u64,
    pub retention_period_hours: i64,
    pub alerts_enabled: bool,
    pub notification_channels: Vec<NotificationChannel>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationChannel {
    pub channel_type: ChannelType,
    pub config: HashMap<String, String>,
    pub enabled: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChannelType {
    Email,
    Slack,
    Webhook,
    PagerDuty,
}

impl Default for MonitoringConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            metrics_port: 9090,
            scrape_interval_secs: 15,
            retention_period_hours: 168,
            alerts_enabled: true,
            notification_channels: vec![],
        }
    }
}

#[derive(Debug, Clone)]
pub struct MetricsCollector {
    config: MonitoringConfig,
    counters: Arc<RwLock<HashMap<String, Counter>>>,
    gauges: Arc<RwLock<HashMap<String, Gauge>>>,
    histograms: Arc<RwLock<HashMap<String, Histogram>>>,
    timestamps: Arc<RwLock<HashMap<String, i64>>>,
    alert_sender: broadcast::Sender<Alert>,
}

#[derive(Debug, Clone)]
pub struct Counter {
    pub name: String,
    pub value: f64,
    pub labels: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct Gauge {
    pub name: String,
    pub value: f64,
    pub labels: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct Histogram {
    pub name: String,
    pub buckets: HashMap<u64, u64>,
    pub sum: f64,
    pub count: u64,
    pub labels: HashMap<String, String>,
}

impl MetricsCollector {
    pub fn new(config: MonitoringConfig) -> Self {
        let (alert_sender, _) = broadcast::channel(100);
        
        Self {
            config,
            counters: Arc::new(RwLock::new(HashMap::new())),
            gauges: Arc::new(RwLock::new(HashMap::new())),
            histograms: Arc::new(RwLock::new(HashMap::new())),
            timestamps: Arc::new(RwLock::new(HashMap::new())),
            alert_sender,
        }
    }

    pub fn config(&self) -> &MonitoringConfig {
        &self.config
    }

    pub fn counter(&self, name: &str) -> CounterBuilder {
        CounterBuilder {
            collector: self,
            name: name.to_string(),
            labels: HashMap::new(),
        }
    }

    pub fn gauge(&self, name: &str) -> GaugeBuilder {
        GaugeBuilder {
            collector: self,
            name: name.to_string(),
            labels: HashMap::new(),
        }
    }

    pub fn histogram(&self, name: &str) -> HistogramBuilder {
        HistogramBuilder {
            collector: self,
            name: name.to_string(),
            labels: HashMap::new(),
        }
    }

    pub async fn increment_counter(&self, name: &str, value: f64, labels: HashMap<String, String>) {
        let key = Self::make_key(name, &labels);
        
        let mut counters = self.counters.write().await;
        if let Some(counter) = counters.get_mut(&key) {
            counter.value += value;
        } else {
            counters.insert(key, Counter {
                name: name.to_string(),
                value,
                labels,
            });
        }
        
        self.timestamps.write().await.insert(name.to_string(), chrono::Utc::now().timestamp());
    }

    pub async fn set_gauge(&self, name: &str, value: f64, labels: HashMap<String, String>) {
        let key = Self::make_key(name, &labels);
        
        let mut gauges = self.gauges.write().await;
        gauges.insert(key, Gauge {
            name: name.to_string(),
            value,
            labels,
        });
        
        self.timestamps.write().await.insert(name.to_string(), chrono::Utc::now().timestamp());
    }

    pub async fn observe_histogram(&self, name: &str, value: f64, labels: HashMap<String, String>) {
        let key = Self::make_key(name, &labels);
        
        let mut histograms = self.histograms.write().await;
        
        if let Some(hist) = histograms.get_mut(&key) {
            hist.sum += value;
            hist.count += 1;
            
            let bucket = value as u64;
            for (bound, count) in hist.buckets.iter_mut() {
                if bucket <= *bound {
                    *count += 1;
                }
            }
        } else {
            let mut buckets = HashMap::new();
            for bound in [1, 5, 10, 50, 100, 500, 1000, 5000] {
                buckets.insert(bound, 0);
            }
            
            let bucket = value as u64;
            for (bound, count) in buckets.iter_mut() {
                if bucket <= *bound {
                    *count += 1;
                }
            }
            
            histograms.insert(key, Histogram {
                name: name.to_string(),
                buckets,
                sum: value,
                count: 1,
                labels,
            });
        }
        
        self.timestamps.write().await.insert(name.to_string(), chrono::Utc::now().timestamp());
    }

    pub async fn get_all_metrics(&self) -> Vec<Metric> {
        let mut metrics = Vec::new();
        
        let counters = self.counters.read().await;
        for counter in counters.values() {
            metrics.push(Metric::Counter(counter.clone()));
        }
        
        let gauges = self.gauges.read().await;
        for gauge in gauges.values() {
            metrics.push(Metric::Gauge(gauge.clone()));
        }
        
        let histograms = self.histograms.read().await;
        for hist in histograms.values() {
            metrics.push(Metric::Histogram(hist.clone()));
        }
        
        metrics
    }

    pub async fn get_metrics_text(&self) -> String {
        let mut output = String::new();
        
        let counters = self.counters.read().await;
        for counter in counters.values() {
            let labels = counter.labels.iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect::<Vec<_>>()
                .join(",");
            
            if labels.is_empty() {
                output.push_str(&format!("# TYPE {} counter\n", counter.name));
            } else {
                output.push_str(&format!("# TYPE {} counter\n", counter.name));
            }
            output.push_str(&format!("{}{{}} {}\n", counter.name, counter.value));
        }
        
        let gauges = self.gauges.read().await;
        for gauge in gauges.values() {
            output.push_str(&format!("# TYPE {} gauge\n", gauge.name));
            output.push_str(&format!("{} {{}} {}\n", gauge.name, gauge.value));
        }
        
        output
    }

    pub async fn reset_metrics(&self) {
        self.counters.write().await.clear();
        self.gauges.write().await.clear();
        self.histograms.write().await.clear();
        self.timestamps.write().await.clear();
    }

    fn make_key(name: &str, labels: &HashMap<String, String>) -> String {
        if labels.is_empty() {
            name.to_string()
        } else {
            let mut label_strs: Vec<String> = labels.iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect();
            label_strs.sort();
            format!("{}{{{}}}", name, label_strs.join(","))
        }
    }

    pub fn alert_receiver(&self) -> broadcast::Receiver<Alert> {
        self.alert_sender.subscribe()
    }

    pub async fn check_alerts(&self, rules: &[AlertRule]) {
        let gauges = self.gauges.read().await;
        
        for rule in rules {
            if let Some(gauge) = gauges.get(&rule.metric) {
                let should_alert = match rule.condition {
                    AlertCondition::Above => gauge.value > rule.threshold,
                    AlertCondition::Below => gauge.value < rule.threshold,
                    AlertCondition::Equals => (gauge.value - rule.threshold).abs() < 0.001,
                };
                
                if should_alert {
                    let alert = Alert {
                        id: format!("alert_{}_{}", rule.name, chrono::Utc::now().timestamp()),
                        name: rule.name.clone(),
                        severity: rule.severity,
                        message: rule.message.clone(),
                        metric: rule.metric.clone(),
                        value: gauge.value,
                        threshold: rule.threshold,
                        fired_at: chrono::Utc::now().timestamp(),
                    };
                    
                    let _ = self.alert_sender.send(alert);
                }
            }
        }
    }
}

pub struct CounterBuilder<'a> {
    collector: &'a MetricsCollector,
    name: String,
    labels: HashMap<String, String>,
}

impl<'a> CounterBuilder<'a> {
    pub fn label(mut self, key: &str, value: &str) -> Self {
        self.labels.insert(key.to_string(), value.to_string());
        self
    }

    pub async fn inc(&self, value: f64) {
        self.collector.increment_counter(&self.name, value, self.labels.clone()).await;
    }
}

pub struct GaugeBuilder<'a> {
    collector: &'a MetricsCollector,
    name: String,
    labels: HashMap<String, String>,
}

impl<'a> GaugeBuilder<'a> {
    pub fn label(mut self, key: &str, value: &str) -> Self {
        self.labels.insert(key.to_string(), value.to_string());
        self
    }

    pub async fn set(&self, value: f64) {
        self.collector.set_gauge(&self.name, value, self.labels.clone()).await;
    }

    pub async fn inc(&self) {
        self.collector.set_gauge(&self.name, 1.0, self.labels.clone()).await;
    }

    pub async fn dec(&self) {
        self.collector.set_gauge(&self.name, -1.0, self.labels.clone()).await;
    }
}

pub struct HistogramBuilder<'a> {
    collector: &'a MetricsCollector,
    name: String,
    labels: HashMap<String, String>,
}

impl<'a> HistogramBuilder<'a> {
    pub fn label(mut self, key: &str, value: &str) -> Self {
        self.labels.insert(key.to_string(), value.to_string());
        self
    }

    pub async fn observe(&self, value: f64) {
        self.collector.observe_histogram(&self.name, value, self.labels.clone()).await;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Metric {
    Counter(Counter),
    Gauge(Gauge),
    Histogram(Histogram),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertRule {
    pub name: String,
    pub metric: String,
    pub condition: AlertCondition,
    pub threshold: f64,
    pub severity: AlertSeverity,
    pub message: String,
    pub enabled: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AlertCondition {
    Above,
    Below,
    Equals,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AlertSeverity {
    Info,
    Warning,
    Error,
    Critical,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Alert {
    pub id: String,
    pub name: String,
    pub severity: AlertSeverity,
    pub message: String,
    pub metric: String,
    pub value: f64,
    pub threshold: f64,
    pub fired_at: i64,
}

pub struct DatabaseMetrics {
    pub queries_total: f64,
    pub queries_duration_ms: f64,
    pub vectors_stored: f64,
    pub vectors_searched: f64,
    pub index_build_time_ms: f64,
    pub memory_usage_bytes: f64,
    pub disk_usage_bytes: f64,
    pub active_connections: f64,
    pub cache_hit_ratio: f64,
    pub error_rate: f64,
}

impl Default for DatabaseMetrics {
    fn default() -> Self {
        Self {
            queries_total: 0.0,
            queries_duration_ms: 0.0,
            vectors_stored: 0.0,
            vectors_searched: 0.0,
            index_build_time_ms: 0.0,
            memory_usage_bytes: 0.0,
            disk_usage_bytes: 0.0,
            active_connections: 0.0,
            cache_hit_ratio: 0.0,
            error_rate: 0.0,
        }
    }
}

impl MetricsCollector {
    pub async fn record_database_metrics(&self, metrics: DatabaseMetrics) {
        let mut labels = HashMap::new();
        
        self.gauge("coretex_queries_total")
            .label("type", "total")
            .set(metrics.queries_total)
            .await;
        
        self.gauge("coretex_queries_duration_ms")
            .set(metrics.queries_duration_ms)
            .await;
        
        self.gauge("coretex_vectors_stored")
            .set(metrics.vectors_stored)
            .await;
        
        self.gauge("coretex_vectors_searched")
            .set(metrics.vectors_searched)
            .await;
        
        self.gauge("coretex_memory_usage_bytes")
            .set(metrics.memory_usage_bytes)
            .await;
        
        self.gauge("coretex_disk_usage_bytes")
            .set(metrics.disk_usage_bytes)
            .await;
        
        self.gauge("coretex_active_connections")
            .set(metrics.active_connections)
            .await;
        
        self.gauge("coretex_cache_hit_ratio")
            .set(metrics.cache_hit_ratio)
            .await;
        
        self.gauge("coretex_error_rate")
            .set(metrics.error_rate)
            .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_metrics_collector_new() {
        let collector = MetricsCollector::new(MonitoringConfig::default());
        
        assert!(collector.config().enabled);
    }

    #[tokio::test]
    async fn test_counter_increment() {
        let collector = MetricsCollector::new(MonitoringConfig::default());
        
        collector.counter("test_counter").inc(1.0).await;
        
        let metrics = collector.get_all_metrics().await;
        assert!(!metrics.is_empty());
    }

    #[tokio::test]
    async fn test_gauge_set() {
        let collector = MetricsCollector::new(MonitoringConfig::default());
        
        collector.gauge("test_gauge").set(42.0).await;
        
        let metrics = collector.get_all_metrics().await;
        assert!(!metrics.is_empty());
    }

    #[tokio::test]
    async fn test_histogram_observe() {
        let collector = MetricsCollector::new(MonitoringConfig::default());
        
        collector.histogram("test_histogram").observe(1.5).await;
        collector.histogram("test_histogram").observe(2.5).await;
        collector.histogram("test_histogram").observe(3.5).await;
        
        let metrics = collector.get_all_metrics().await;
        assert!(!metrics.is_empty());
    }

    #[tokio::test]
    async fn test_metrics_text_format() {
        let collector = MetricsCollector::new(MonitoringConfig::default());
        
        collector.counter("test_counter").inc(1.0).await;
        collector.gauge("test_gauge").set(42.0).await;
        
        let text = collector.get_metrics_text().await;
        assert!(text.contains("test_counter"));
        assert!(text.contains("test_gauge"));
    }

    #[tokio::test]
    async fn test_labels() {
        let collector = MetricsCollector::new(MonitoringConfig::default());
        
        collector.counter("test_counter")
            .label("method", "search")
            .label("collection", "test")
            .inc(1.0)
            .await;
        
        let metrics = collector.get_all_metrics().await;
        assert_eq!(metrics.len(), 1);
    }

    #[tokio::test]
    async fn test_reset_metrics() {
        let collector = MetricsCollector::new(MonitoringConfig::default());
        
        collector.counter("test_counter").inc(1.0).await;
        collector.gauge("test_gauge").set(42.0).await;
        
        collector.reset_metrics().await;
        
        let metrics = collector.get_all_metrics().await;
        assert!(metrics.is_empty());
    }

    #[tokio::test]
    async fn test_alert_check() {
        let collector = MetricsCollector::new(MonitoringConfig::default());
        
        collector.gauge("test_metric").set(80.0).await;
        
        let rules = vec![
            AlertRule {
                name: "high_error".to_string(),
                metric: "test_metric".to_string(),
                condition: AlertCondition::Above,
                threshold: 75.0,
                severity: AlertSeverity::Warning,
                message: "Test metric is too high".to_string(),
                enabled: true,
            }
        ];
        
        collector.check_alerts(&rules).await;
        
        let mut receiver = collector.alert_receiver();
        if let Ok(alert) = receiver.try_recv() {
            assert_eq!(alert.name, "high_error");
        }
    }

    #[tokio::test]
    async fn test_database_metrics() {
        let collector = MetricsCollector::new(MonitoringConfig::default());
        
        let metrics = DatabaseMetrics {
            queries_total: 1000.0,
            queries_duration_ms: 50.0,
            vectors_stored: 10000.0,
            vectors_searched: 5000.0,
            index_build_time_ms: 1000.0,
            memory_usage_bytes: 1024.0 * 1024.0 * 512.0,
            disk_usage_bytes: 1024.0 * 1024.0 * 1024.0 * 10.0,
            active_connections: 50.0,
            cache_hit_ratio: 0.95,
            error_rate: 0.01,
        };
        
        collector.record_database_metrics(metrics).await;
        
        let text = collector.get_metrics_text().await;
        assert!(text.contains("coretex_queries_total"));
        assert!(text.contains("coretex_memory_usage_bytes"));
    }
}
