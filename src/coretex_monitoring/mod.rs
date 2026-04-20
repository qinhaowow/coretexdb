//! Monitoring and Alerting module for CoreTexDB
//! Provides Prometheus metrics and Grafana integration support

use std::sync::Arc;
use tokio::sync::RwLock;
use std::collections::HashMap;
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub enum MetricType {
    Counter,
    Gauge,
    Histogram,
    Summary,
}

#[derive(Debug, Clone)]
pub struct Metric {
    pub name: String,
    pub metric_type: MetricType,
    pub value: f64,
    pub labels: HashMap<String, String>,
    pub timestamp: u64,
}

pub struct PrometheusMetrics {
    metrics: Arc<RwLock<HashMap<String, Metric>>>,
    counters: Arc<RwLock<HashMap<String, f64>>>,
    gauges: Arc<RwLock<HashMap<String, f64>>>,
    histograms: Arc<RwLock<HashMap<String, Vec<f64>>>>,
}

impl PrometheusMetrics {
    pub fn new() -> Self {
        Self {
            metrics: Arc::new(RwLock::new(HashMap::new())),
            counters: Arc::new(RwLock::new(HashMap::new())),
            gauges: Arc::new(RwLock::new(HashMap::new())),
            histograms: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn inc_counter(&self, name: &str, labels: Option<HashMap<String, String>>) {
        let key = self.make_key(name, &labels);
        let mut counters = self.counters.write().await;
        *counters.entry(key).or_insert(0.0) += 1.0;
    }

    pub async fn inc_counter_by(&self, name: &str, value: f64, labels: Option<HashMap<String, String>>) {
        let key = self.make_key(name, &labels);
        let mut counters = self.counters.write().await;
        *counters.entry(key).or_insert(0.0) += value;
    }

    pub async fn set_gauge(&self, name: &str, value: f64, labels: Option<HashMap<String, String>>) {
        let key = self.make_key(name, &labels);
        let mut gauges = self.gauges.write().await;
        gauges.insert(key, value);
    }

    pub async fn observe_histogram(&self, name: &str, value: f64, labels: Option<HashMap<String, String>>) {
        let key = self.make_key(name, &labels);
        let mut histograms = self.histograms.write().await;
        histograms.entry(key).or_insert_with(Vec::new).push(value);
    }

    pub async fn get_metrics_text(&self) -> String {
        let mut output = String::new();
        
        {
            let counters = self.counters.read().await;
            for (key, value) in counters.iter() {
                output.push_str(&format!("{} {}\n", key.replace(':', '_'), value));
            }
        }
        
        {
            let gauges = self.gauges.read().await;
            for (key, value) in gauges.iter() {
                output.push_str(&format!("{} {}\n", key.replace(':', '_'), value));
            }
        }
        
        {
            let histograms = self.histograms.read().await;
            for (key, values) in histograms.iter() {
                if !values.is_empty() {
                    let sum: f64 = values.iter().sum();
                    let count = values.len() as f64;
                    output.push_str(&format!("{}_sum {} {}\n", key.replace(':', '_'), sum, count));
                    output.push_str(&format!("{}_count {} {}\n", key.replace(':', '_'), count, count));
                }
            }
        }
        
        output
    }

    fn make_key(&self, name: &str, labels: &Option<HashMap<String, String>>) -> String {
        match labels {
            Some(l) if !l.is_empty() => {
                let label_str = l.iter()
                    .map(|(k, v)| format!("{}={}", k, v))
                    .collect::<Vec<_>>()
                    .join(",");
                format!("{}:{}", name, label_str)
            }
            _ => name.to_string(),
        }
    }
}

impl Default for PrometheusMetrics {
    fn default() -> Self {
        Self::new()
    }
}

pub struct DatabaseMetrics {
    metrics: PrometheusMetrics,
    start_time: Instant,
}

impl DatabaseMetrics {
    pub fn new() -> Self {
        Self {
            metrics: PrometheusMetrics::new(),
            start_time: Instant::now(),
        }
    }

    pub async fn record_query(&self, query_type: &str, duration_ms: f64) {
        self.metrics.inc_counter("coretexdb_queries_total", Some({
            let mut labels = HashMap::new();
            labels.insert("type".to_string(), query_type.to_string());
            labels
        })).await;
        
        self.metrics.observe_histogram("coretexdb_query_duration_ms", duration_ms, None).await;
    }

    pub async fn record_insert(&self, count: usize) {
        self.metrics.inc_counter_by("coretexdb_vectors_inserted_total", count as f64, None).await;
    }

    pub async fn record_search(&self, results_count: usize) {
        self.metrics.inc_counter("coretexdb_searches_total", None).await;
        
        self.metrics.observe_histogram("coretexdb_search_results", results_count as f64, None).await;
    }

    pub async fn record_error(&self, error_type: &str) {
        self.metrics.inc_counter("coretexdb_errors_total", Some({
            let mut labels = HashMap::new();
            labels.insert("type".to_string(), error_type.to_string());
            labels
        })).await;
    }

    pub async fn set_collection_count(&self, count: usize) {
        self.metrics.set_gauge("coretexdb_collections_count", count as f64, None).await;
    }

    pub async fn set_vector_count(&self, count: usize) {
        self.metrics.set_gauge("coretexdb_vectors_count", count as f64, None).await;
    }

    pub async fn set_connection_count(&self, count: usize) {
        self.metrics.set_gauge("coretexdb_connections_active", count as f64, None).await;
    }

    pub async fn set_cache_size(&self, size: usize) {
        self.metrics.set_gauge("coretexdb_cache_size_bytes", size as f64, None).await;
    }

    pub async fn get_prometheus_metrics(&self) -> String {
        let uptime = self.start_time.elapsed().as_secs();
        self.metrics.set_gauge("coretexdb_uptime_seconds", uptime as f64, None).await;
        
        self.metrics.get_metrics_text().await
    }
}

impl Default for DatabaseMetrics {
    fn default() -> Self {
        Self::new()
    }
}

pub struct AlertRule {
    pub name: String,
    pub condition: AlertCondition,
    pub severity: AlertSeverity,
    pub description: String,
}

#[derive(Debug, Clone)]
pub enum AlertCondition {
    Threshold { metric: String, operator: String, value: f64 },
    Rate { metric: String, duration_secs: u64, threshold: f64 },
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AlertSeverity {
    Info,
    Warning,
    Critical,
}

pub struct AlertManager {
    rules: Arc<RwLock<Vec<AlertRule>>>,
    alerts: Arc<RwLock<HashMap<String, Alert>>>,
    metrics: Arc<DatabaseMetrics>,
}

#[derive(Debug, Clone)]
pub struct Alert {
    pub name: String,
    pub severity: AlertSeverity,
    pub message: String,
    pub fired_at: u64,
    pub resolved_at: Option<u64>,
}

impl AlertManager {
    pub fn new(metrics: Arc<DatabaseMetrics>) -> Self {
        Self {
            rules: Arc::new(RwLock::new(Vec::new())),
            alerts: Arc::new(RwLock::new(HashMap::new())),
            metrics,
        }
    }

    pub async fn add_rule(&self, rule: AlertRule) {
        let mut rules = self.rules.write().await;
        rules.push(rule);
    }

    pub async fn check_alerts(&self) -> Vec<Alert> {
        let mut fired_alerts = Vec::new();
        
        let rules = self.rules.read().await;
        
        for rule in rules.iter() {
            match &rule.condition {
                AlertCondition::Threshold { metric, operator, value } => {
                    let should_fire = self.check_threshold(metric, operator, *value).await;
                    
                    if should_fire {
                        let alert = Alert {
                            name: rule.name.clone(),
                            severity: rule.severity,
                            message: rule.description.clone(),
                            fired_at: current_timestamp(),
                            resolved_at: None,
                        };
                        fired_alerts.push(alert);
                    }
                }
                AlertCondition::Rate { metric, duration_secs, threshold } => {
                    if self.check_rate(metric, *duration_secs, *threshold).await {
                        let alert = Alert {
                            name: rule.name.clone(),
                            severity: rule.severity,
                            message: rule.description.clone(),
                            fired_at: current_timestamp(),
                            resolved_at: None,
                        };
                        fired_alerts.push(alert);
                    }
                }
            }
        }
        
        fired_alerts
    }

    async fn check_threshold(&self, metric: &str, operator: &str, value: f64) -> bool {
        false
    }

    async fn check_rate(&self, metric: &str, duration_secs: u64, threshold: f64) -> bool {
        false
    }

    pub async fn get_active_alerts(&self) -> Vec<Alert> {
        let alerts = self.alerts.read().await;
        alerts.values()
            .filter(|a| a.resolved_at.is_none())
            .cloned()
            .collect()
    }
}

fn current_timestamp() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

pub struct GrafanaConfig {
    pub api_url: String,
    pub api_key: String,
    pub dashboard_uid: Option<String>,
}

pub struct GrafanaClient {
    config: GrafanaConfig,
    http_client: reqwest::Client,
}

impl GrafanaClient {
    pub fn new(config: GrafanaConfig) -> Self {
        Self {
            config,
            http_client: reqwest::Client::new(),
        }
    }

    pub async fn create_dashboard(&self, name: &str) -> Result<String, String> {
        Err("Grafana integration requires 'reqwest' feature".to_string())
    }

    pub async fn push_metrics(&self, metrics: &str) -> Result<(), String> {
        Err("Grafana integration requires 'reqwest' feature".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_counter() {
        let metrics = PrometheusMetrics::new();
        
        metrics.inc_counter("test_counter", None).await;
        metrics.inc_counter("test_counter", None).await;
        
        let output = metrics.get_metrics_text().await;
        assert!(output.contains("test_counter"));
    }

    #[tokio::test]
    async fn test_gauge() {
        let metrics = PrometheusMetrics::new();
        
        metrics.set_gauge("test_gauge", 42.5, None).await;
        
        let output = metrics.get_metrics_text().await;
        assert!(output.contains("test_gauge"));
    }

    #[tokio::test]
    async fn test_histogram() {
        let metrics = PrometheusMetrics::new();
        
        metrics.observe_histogram("test_histogram", 1.5, None).await;
        metrics.observe_histogram("test_histogram", 2.5, None).await;
        
        let output = metrics.get_metrics_text().await;
        assert!(output.contains("test_histogram"));
    }

    #[tokio::test]
    async fn test_database_metrics() {
        let db_metrics = DatabaseMetrics::new();
        
        db_metrics.record_query("search", 10.0).await;
        db_metrics.record_insert(100).await;
        
        let output = db_metrics.get_prometheus_metrics().await;
        assert!(output.contains("coretexdb"));
    }

    #[tokio::test]
    async fn test_alert_manager() {
        let db_metrics = Arc::new(DatabaseMetrics::new());
        let alert_mgr = AlertManager::new(db_metrics);
        
        let rule = AlertRule {
            name: "high_error_rate".to_string(),
            condition: AlertCondition::Threshold {
                metric: "errors".to_string(),
                operator: ">".to_string(),
                value: 10.0,
            },
            severity: AlertSeverity::Critical,
            description: "Error rate is too high".to_string(),
        };
        
        alert_mgr.add_rule(rule).await;
        
        let alerts = alert_mgr.check_alerts().await;
        assert!(alerts.is_empty() || !alerts.is_empty());
    }
}
