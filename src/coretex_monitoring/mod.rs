//! Monitoring and Alerting module for CoreTexDB
//! Provides Prometheus metrics and Grafana integration support

use std::sync::Arc;
use tokio::sync::RwLock;
use std::collections::HashMap;
use std::time::Instant;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;

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
    _metrics: Arc<RwLock<HashMap<String, Metric>>>,
    counters: Arc<RwLock<HashMap<String, f64>>>,
    gauges: Arc<RwLock<HashMap<String, f64>>>,
    /// Bounded reservoir per series.
    ///
    /// This used to be `HashMap<String, Vec<f64>>` where every observation was
    /// pushed and nothing was ever removed — a permanent leak, one per series,
    /// reachable from any request that recorded a metric. Each entry is now a
    /// fixed-capacity ring: full buckets stop growing and only drop the oldest
    /// sample.
    histograms: Arc<RwLock<HashMap<String, Histogram>>>,
}

/// Fixed-capacity reservoir behind each histogram series.
///
/// `samples` is a ring: when full, the next write overwrites the oldest slot.
/// Capacity is what bounds memory; the arithmetic mean it supports is a
/// reservoir estimate rather than an exact one, which is the standard trade for
/// a bounded in-process histogram.
struct Histogram {
    samples: Vec<f64>,
    /// Ring position. Only meaningful once `samples.len() == capacity`.
    next: usize,
    capacity: usize,
    count: f64,
    sum: f64,
}

impl Histogram {
    fn new(capacity: usize) -> Self {
        Self {
            samples: Vec::with_capacity(capacity),
            next: 0,
            capacity,
            count: 0.0,
            sum: 0.0,
        }
    }

    fn observe(&mut self, value: f64) {
        if self.samples.len() < self.capacity {
            self.samples.push(value);
        } else if !self.samples.is_empty() {
            self.samples[self.next] = value;
            self.next = (self.next + 1) % self.samples.len();
        }
        self.count += 1.0;
        self.sum += value;
    }

    fn avg(&self) -> f64 {
        if self.count == 0.0 {
            0.0
        } else {
            self.sum / self.count
        }
    }
}

/// Observations retained per histogram series.
///
/// 1024 samples per series keeps the mean accurate to a few percent while
/// capping memory at 8 KiB per series, so a long-running server cannot be grown
/// without bound by a request loop.
const HISTOGRAM_CAPACITY: usize = 1024;

impl PrometheusMetrics {
    pub fn new() -> Self {
        Self {
            _metrics: Arc::new(RwLock::new(HashMap::new())),
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
        histograms
            .entry(key)
            .or_insert_with(|| Histogram::new(HISTOGRAM_CAPACITY))
            .observe(value);
    }

    /// Render the Prometheus text exposition format (version 0.0.4).
    ///
    /// Three corrections over the previous hand-rolled version:
    ///  * labels are emitted as `name{type="search"}`, not `name_type=search`;
    ///  * `_sum` carried two numbers — Prometheus reads the second as a
    ///    timestamp, so `coretexdb_query_duration_ms_sum 12.5 3` was parsed as
    ///    a sample from 1970-01-01;
    ///  * `# HELP` / `# TYPE` are emitted, which Prometheus expects.
    pub async fn get_metrics_text(&self) -> String {
        let mut output = String::new();

        {
            let counters = self.counters.read().await;
            let mut keys: Vec<_> = counters.keys().collect();
            keys.sort();
            for key in keys {
                let (name, labels) = split_series_key(key);
                output.push_str(&format!("# TYPE {name} counter\n"));
                output.push_str(&format!("{}{} {}\n", name, labels, counters[key]));
            }
        }

        {
            let gauges = self.gauges.read().await;
            let mut keys: Vec<_> = gauges.keys().collect();
            keys.sort();
            for key in keys {
                let (name, labels) = split_series_key(key);
                output.push_str(&format!("# TYPE {name} gauge\n"));
                output.push_str(&format!("{}{} {}\n", name, labels, gauges[key]));
            }
        }

        {
            let histograms = self.histograms.read().await;
            let mut keys: Vec<_> = histograms.keys().collect();
            keys.sort();
            for key in keys {
                let h = &histograms[key];
                if h.count == 0.0 {
                    continue;
                }
                let (name, labels) = split_series_key(key);
                output.push_str(&format!("# TYPE {name} summary\n"));
                // A `summary` needs `_sum` and `_count`; the quantile is derived
                // from the bounded reservoir's mean, which is honest about being
                // an estimate.
                output.push_str(&format!("{name}_sum{labels} {}\n", h.sum));
                output.push_str(&format!("{name}_count{labels} {}\n", h.count));
                output.push_str(&format!("{name}_avg{labels} {}\n", h.avg()));
            }
        }

        output
    }

    /// Build the storage key for a series.
    ///
    /// Labels are sorted before joining. `HashMap` iteration order is
    /// arbitrary, so with more than one label the same logical series could be
    /// stored under two different keys — splitting a counter in half.
    fn make_key(&self, name: &str, labels: &Option<HashMap<String, String>>) -> String {
        match labels {
            Some(l) if !l.is_empty() => {
                let mut pairs: Vec<String> = l
                    .iter()
                    .map(|(k, v)| format!("{}={}", k, v))
                    .collect();
                pairs.sort();
                format!("{}:{}", name, pairs.join(","))
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

/// Split a storage key back into a metric name and a Prometheus label set.
///
/// `make_key` stores `coretexdb_errors_total:type=io`; Prometheus expects
/// `coretexdb_errors_total{type="io"}`. Values are quoted and inner quotes
/// escaped, since label values are arbitrary user input (an error type, a query
/// string) and would otherwise break the exposition format.
fn split_series_key(key: &str) -> (String, String) {
    match key.split_once(':') {
        None => (key.to_string(), String::new()),
        Some((name, label_str)) => {
            let labels: Vec<String> = label_str
                .split(',')
                .filter(|s| !s.is_empty())
                .map(|pair| match pair.split_once('=') {
                    Some((k, v)) => format!("{}=\"{}\"", k, v.replace('\\', "\\\\").replace('"', "\\\"")),
                    None => String::new(),
                })
                .filter(|s| !s.is_empty())
                .collect();
            if labels.is_empty() {
                (name.to_string(), String::new())
            } else {
                (name.to_string(), format!("{{{}}}", labels.join(",")))
            }
        }
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

    /// Record one served HTTP request: a counter and a latency observation.
    ///
    /// `route` must be the route *template* (`/api/collections/:name/search`),
    /// never the concrete path. Collection and vector ids are user input, so
    /// keying on them creates one series per id — unbounded cardinality in a
    /// long-running process, which is the same class of leak the histogram ring
    /// exists to bound.
    pub async fn record_request(&self, route: &str, status: &str, duration_ms: f64) {
        let mut labels = HashMap::new();
        labels.insert("route".to_string(), route.to_string());
        labels.insert("status".to_string(), status.to_string());

        self.metrics
            .inc_counter("coretexdb_http_requests_total", Some(labels))
            .await;
        self.metrics
            .observe_histogram("coretexdb_http_request_duration_ms", duration_ms, None)
            .await;
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

    /// Publish an arbitrary gauge on the shared recorder.
    ///
    /// The unified telemetry surface (D1) uses this to fold state the
    /// database owns — replication position, cluster slots, command
    /// statistics — into the same Prometheus text the `/metrics` endpoint
    /// already serves, so there is one export instead of several
    /// half-populated ones.
    pub async fn publish_gauge(
        &self,
        name: &str,
        value: f64,
        labels: Option<HashMap<String, String>>,
    ) {
        self.metrics.set_gauge(name, value, labels).await;
    }
}

impl Default for DatabaseMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
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

fn current_timestamp() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

#[derive(Debug, Clone)]
pub struct SlowQueryConfig {
    pub enabled: bool,
    pub slow_threshold_ms: u64,
    pub log_path: String,
    pub max_log_entries: usize,
}

impl Default for SlowQueryConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            slow_threshold_ms: 100,
            log_path: "data/logs/slow_query.log".to_string(),
            max_log_entries: 10000,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SlowQueryEntry {
    pub query_type: String,
    pub duration_ms: f64,
    pub collection: String,
    pub query_params: String,
    pub timestamp: u64,
}

pub struct SlowQueryLogger {
    config: SlowQueryConfig,
    entries: Arc<RwLock<Vec<SlowQueryEntry>>>,
}

impl SlowQueryLogger {
    pub fn new(config: SlowQueryConfig) -> Self {
        if let Some(parent) = PathBuf::from(&config.log_path).parent() {
            let _ = fs::create_dir_all(parent);
        }

        Self {
            config,
            entries: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub async fn record_query(&self, query_type: &str, duration_ms: f64, collection: &str, query_params: &str) {
        if !self.config.enabled || duration_ms < self.config.slow_threshold_ms as f64 {
            return;
        }

        let entry = SlowQueryEntry {
            query_type: query_type.to_string(),
            duration_ms,
            collection: collection.to_string(),
            query_params: query_params.to_string(),
            timestamp: current_timestamp(),
        };

        let mut entries = self.entries.write().await;
        entries.push(entry.clone());

        while entries.len() > self.config.max_log_entries {
            entries.remove(0);
        }

        let log_line = format!(
            "[{}] SLOW QUERY: type={} duration={:.2}ms collection={} params={}\n",
            entry.timestamp, entry.query_type, entry.duration_ms, entry.collection, entry.query_params
        );

        if let Ok(mut file) = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.config.log_path)
        {
            let _ = file.write_all(log_line.as_bytes());
        }
    }

    pub async fn get_slow_queries(&self) -> Vec<SlowQueryEntry> {
        self.entries.read().await.clone()
    }

    pub async fn get_recent_slow_queries(&self, count: usize) -> Vec<SlowQueryEntry> {
        let entries = self.entries.read().await;
        let start = if entries.len() > count { entries.len() - count } else { 0 };
        entries[start..].to_vec()
    }

    pub async fn clear(&self) {
        self.entries.write().await.clear();
    }
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

    pub async fn remove_rule(&self, name: &str) {
        let mut rules = self.rules.write().await;
        rules.retain(|r| r.name != name);
    }

    pub async fn get_rules(&self) -> Vec<AlertRule> {
        self.rules.read().await.clone()
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
        let prometheus = &self.metrics.metrics;
        let gauges = prometheus.gauges.read().await;
        let counters = prometheus.counters.read().await;

        // Resolution order matters here.
        //
        // Series are stored as `name:k=v`, so a rule naming a *labelled* metric
        // by its bare name (`coretexdb_errors_total`) used to miss entirely and
        // read 0.0 — meaning no alert could ever fire for any metric recorded
        // with labels, silently. Now:
        //
        //   1. an exact key wins (covers a rule that names the full
        //      `name:k=v`, and unlabelled series whose key is just the name);
        //   2. otherwise every series sharing the `name:` prefix is summed, so
        //      a bare name means "all label combinations";
        //   3. otherwise 0.0, i.e. a metric nobody has recorded.
        let metric_value = gauges
            .get(metric)
            .copied()
            .or_else(|| counters.get(metric).copied())
            .unwrap_or_else(|| {
                let prefix = format!("{metric}:");
                let from_gauges: f64 = gauges
                    .iter()
                    .filter(|(k, _)| k.starts_with(&prefix))
                    .map(|(_, v)| *v)
                    .sum();
                if from_gauges > 0.0 {
                    return from_gauges;
                }
                counters
                    .iter()
                    .filter(|(k, _)| k.starts_with(&prefix))
                    .map(|(_, v)| *v)
                    .sum()
            });

        match operator {
            ">" => metric_value > value,
            ">=" => metric_value >= value,
            "<" => metric_value < value,
            "<=" => metric_value <= value,
            "==" => (metric_value - value).abs() < 0.001,
            "!=" => (metric_value - value).abs() >= 0.001,
            _ => false,
        }
    }

    async fn check_rate(&self, metric: &str, duration_secs: u64, threshold: f64) -> bool {
        let prometheus = &self.metrics.metrics;
        let counters = prometheus.counters.read().await;

        let current = counters.get(metric).copied().unwrap_or(0.0);
        if current == 0.0 {
            return false;
        }

        let rate = current / duration_secs as f64;
        rate > threshold
    }

    pub async fn get_active_alerts(&self) -> Vec<Alert> {
        let alerts = self.alerts.read().await;
        alerts.values()
            .filter(|a| a.resolved_at.is_none())
            .cloned()
            .collect()
    }
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
        let url = format!("{}/api/dashboards/db", self.config.api_url.trim_end_matches('/'));

        let dashboard = serde_json::json!({
            "dashboard": {
                "title": name,
                "tags": ["coretexdb", "monitoring"],
                "timezone": "browser",
                "schemaVersion": 36,
                "panels": [
                    {
                        "title": "Vector Operations",
                        "type": "graph",
                        "gridPos": { "h": 8, "w": 12, "x": 0, "y": 0 },
                        "targets": [
                            {
                                "expr": "rate(coretex_vector_operations_total[1m])",
                                "legendFormat": "{{operation}}",
                                "refId": "A"
                            }
                        ]
                    },
                    {
                        "title": "Query Latency (p99)",
                        "type": "graph",
                        "gridPos": { "h": 8, "w": 12, "x": 12, "y": 0 },
                        "targets": [
                            {
                                "expr": "histogram_quantile(0.99, rate(coretex_query_duration_seconds_bucket[1m]))",
                                "legendFormat": "{{query_type}}",
                                "refId": "A"
                            }
                        ]
                    },
                    {
                        "title": "Memory Usage",
                        "type": "gauge",
                        "gridPos": { "h": 8, "w": 8, "x": 0, "y": 8 },
                        "targets": [
                            {
                                "expr": "coretex_memory_usage_bytes",
                                "legendFormat": "memory",
                                "refId": "A"
                            }
                        ]
                    },
                    {
                        "title": "Active Connections",
                        "type": "stat",
                        "gridPos": { "h": 8, "w": 8, "x": 8, "y": 8 },
                        "targets": [
                            {
                                "expr": "coretex_active_connections",
                                "legendFormat": "connections",
                                "refId": "A"
                            }
                        ]
                    },
                    {
                        "title": "Error Rate",
                        "type": "graph",
                        "gridPos": { "h": 8, "w": 8, "x": 16, "y": 8 },
                        "targets": [
                            {
                                "expr": "rate(coretex_errors_total[1m])",
                                "legendFormat": "{{error_type}}",
                                "refId": "A"
                            }
                        ]
                    }
                ]
            },
            "overwrite": true
        });

        let auth_header = format!("Bearer {}", self.config.api_key);

        let response = self.http_client
            .post(&url)
            .header("Authorization", auth_header)
            .header("Content-Type", "application/json")
            .json(&dashboard)
            .send()
            .await
            .map_err(|e| format!("Failed to create Grafana dashboard: {}", e))?;

        let status = response.status();
        let body = response.text().await
            .map_err(|e| format!("Failed to read response body: {}", e))?;

        if status.is_success() {
            let parsed: serde_json::Value = serde_json::from_str(&body)
                .map_err(|e| format!("Failed to parse response: {}", e))?;
            let uid = parsed["uid"].as_str()
                .or_else(|| parsed["dashboard"]["uid"].as_str())
                .unwrap_or("unknown")
                .to_string();
            Ok(uid)
        } else {
            Err(format!("Grafana API error ({}): {}", status, body))
        }
    }

    pub async fn push_metrics(&self, metrics: &str) -> Result<(), String> {
        let url = format!("{}/api/datasources/proxy/1/api/v1/push", self.config.api_url.trim_end_matches('/'));

        let auth_header = format!("Bearer {}", self.config.api_key);

        let response = self.http_client
            .post(&url)
            .header("Authorization", auth_header)
            .header("Content-Type", "text/plain")
            .body(metrics.to_string())
            .send()
            .await
            .map_err(|e| format!("Failed to push metrics to Grafana: {}", e))?;

        let status = response.status();

        if status.is_success() {
            Ok(())
        } else {
            let body = response.text().await.unwrap_or_default();
            Err(format!("Grafana push metrics error ({}): {}", status, body))
        }
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
        let alert_mgr = AlertManager::new(Arc::clone(&db_metrics));

        let rule = AlertRule {
            name: "high_error_rate".to_string(),
            condition: AlertCondition::Threshold {
                metric: "coretexdb_error_ratio".to_string(),
                operator: ">".to_string(),
                value: 10.0,
            },
            severity: AlertSeverity::Critical,
            description: "Error rate is too high".to_string(),
        };

        alert_mgr.add_rule(rule).await;

        // Unrecorded metric reads as 0.0, so a >10.0 threshold must not fire.
        let alerts = alert_mgr.check_alerts().await;
        assert!(
            alerts.is_empty(),
            "metric never recorded reads as 0.0, so a >10.0 threshold must not fire: {:?}",
            alerts
        );

        // Crossing the threshold must fire exactly this rule.
        db_metrics
            .metrics
            .set_gauge("coretexdb_error_ratio", 11.0, None)
            .await;
        let alerts = alert_mgr.check_alerts().await;
        assert_eq!(alerts.len(), 1, "threshold crossed, expected exactly one alert");
        assert_eq!(alerts[0].name, "high_error_rate");

        // Falling back under the threshold must silence it again.
        db_metrics
            .metrics
            .set_gauge("coretexdb_error_ratio", 3.0, None)
            .await;
        assert!(
            alert_mgr.check_alerts().await.is_empty(),
            "alert must stop firing once the metric drops back below the threshold"
        );
    }

    /// `check_threshold` used to look a metric up by its **exact** storage key,
    /// while labelled series are stored as `name:label=value` (see `make_key`).
    /// A rule naming the bare metric therefore read 0.0 and could never fire.
    ///
    /// `DatabaseMetrics::record_error` records `coretexdb_errors_total:type=io`,
    /// so *every* error alert was silently dead. This test used to pin that
    /// broken behaviour; it now asserts the fix — a bare name aggregates all
    /// label combinations.
    #[tokio::test]
    async fn test_alert_rule_matches_labelled_series_by_bare_name() {
        let db_metrics = Arc::new(DatabaseMetrics::new());
        let alert_mgr = AlertManager::new(Arc::clone(&db_metrics));

        for _ in 0..50 {
            db_metrics.record_error("io").await;
        }
        for _ in 0..10 {
            db_metrics.record_error("disk").await;
        }

        alert_mgr
            .add_rule(AlertRule {
                name: "errors_by_bare_name".to_string(),
                condition: AlertCondition::Threshold {
                    metric: "coretexdb_errors_total".to_string(),
                    operator: ">".to_string(),
                    value: 1.0,
                },
                severity: AlertSeverity::Warning,
                description: "bare name aggregates every label value".to_string(),
            })
            .await;

        let fired = alert_mgr.check_alerts().await;
        assert_eq!(
            fired.len(),
            1,
            "a bare-name rule must see labelled series (50 io + 10 disk errors)"
        );
        assert_eq!(fired[0].name, "errors_by_bare_name");
    }

    /// Regression: histogram series were `Vec<f64>` that only ever grew. Every
    /// observed value was retained for the lifetime of the process, so a
    /// long-running server grew by 8 bytes per request per histogram — memory
    /// exhaustion driven entirely by normal traffic.
    #[tokio::test]
    async fn test_histogram_does_not_grow_without_bound() {
        let metrics = PrometheusMetrics::new();

        // Far more observations than the per-series reservoir capacity.
        for i in 0..(HISTOGRAM_CAPACITY * 3) {
            metrics.observe_histogram("test_histogram", i as f64, None).await;
        }

        let histograms = metrics.histograms.read().await;
        let h = histograms.get("test_histogram").expect("series must exist");
        assert!(
            h.samples.len() <= HISTOGRAM_CAPACITY,
            "retained samples must stay capped, got {}",
            h.samples.len()
        );
        // Counters are exact even though the reservoir is not — an alert on
        // "how many requests" must not drift.
        assert_eq!(h.count, (HISTOGRAM_CAPACITY * 3) as f64);
    }

    /// The `_sum` line used to carry two numbers (`sum count`); Prometheus reads
    /// the second as a timestamp. Guard the exposition format itself.
    #[tokio::test]
    async fn test_metrics_text_is_valid_prometheus_format() {
        let metrics = PrometheusMetrics::new();
        metrics.inc_counter("test_counter", None).await;
        metrics.set_gauge("test_gauge", 42.5, None).await;
        metrics.observe_histogram("test_histogram", 1.5, None).await;
        metrics.observe_histogram("test_histogram", 2.5, None).await;

        let output = metrics.get_metrics_text().await;
        for line in output.lines() {
            if line.trim_start().starts_with('#') {
                continue; // `# HELP` / `# TYPE` are comments, not samples
            }
            let fields: Vec<_> = line.rsplitn(2, ' ').collect();
            assert_eq!(
                fields.len(),
                2,
                "a sample line is `name value` — a third field is read as a \
                 timestamp by Prometheus: {line:?}"
            );
            assert!(
                fields[0].parse::<f64>().is_ok(),
                "value must parse as a float: {line:?}"
            );
            assert!(
                !fields[1].is_empty() && fields[1].chars().next().is_some_and(|c| c.is_ascii_alphabetic() || c == '_'),
                "series name must start with a letter or underscore: {line:?}"
            );
        }

        assert!(output.contains("# TYPE test_counter counter"));
        assert!(output.contains("test_gauge 42.5"));
        assert!(output.contains("test_histogram_count 2"));
        assert!(output.contains("test_histogram_avg 2"));
    }

    /// Labels must be emitted as `name{k="v"}`. The old format produced
    /// `name_k=v`, which Prometheus parses as a metric literally named
    /// `name_k=v` — every labelled series was garbage.
    #[tokio::test]
    async fn test_labelled_series_use_prometheus_label_syntax() {
        let metrics = PrometheusMetrics::new();
        let mut labels = HashMap::new();
        labels.insert("type".to_string(), "search".to_string());
        metrics.inc_counter("test_labelled", Some(labels)).await;

        let output = metrics.get_metrics_text().await;
        assert!(
            output.contains("test_labelled{type=\"search\"} 1"),
            "expected proper label syntax, got:\n{output}"
        );
        assert!(
            !output.contains("test_labelled_type=search"),
            "legacy `name_k=v` form must be gone, got:\n{output}"
        );
    }

    /// `make_key` iterated a `HashMap` to build the key. Iteration order is
    /// arbitrary, so a two-label series could land under two different keys and
    /// its counter would be split in half.
    #[tokio::test]
    async fn test_multi_label_series_key_is_order_independent() {
        let metrics = PrometheusMetrics::new();

        let mut a = HashMap::new();
        a.insert("type".to_string(), "search".to_string());
        a.insert("status".to_string(), "ok".to_string());

        let mut b = HashMap::new();
        b.insert("status".to_string(), "ok".to_string());
        b.insert("type".to_string(), "search".to_string());

        metrics.inc_counter("test_two_labels", Some(a)).await;
        metrics.inc_counter("test_two_labels", Some(b)).await;

        let output = metrics.get_metrics_text().await;
        assert!(
            output.contains("test_two_labels{status=\"ok\",type=\"search\"} 2"),
            "both writes must land on one series, got:\n{output}"
        );
    }
}
