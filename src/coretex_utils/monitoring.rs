//! Monitoring and metrics for CortexDB

use std::sync::Arc;
use tokio::sync::RwLock;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub struct Metrics {
    pub requests_total: u64,
    pub requests_success: u64,
    pub requests_failed: u64,
    pub avg_latency_ms: f64,
    pub min_latency_ms: u64,
    pub max_latency_ms: u64,
    pub vectors_inserted: u64,
    pub vectors_deleted: u64,
    pub vectors_searched: u64,
    pub collections_count: u64,
    pub total_vectors: u64,
    pub index_size_bytes: u64,
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            requests_total: 0,
            requests_success: 0,
            requests_failed: 0,
            avg_latency_ms: 0.0,
            min_latency_ms: u64::MAX,
            max_latency_ms: 0,
            vectors_inserted: 0,
            vectors_deleted: 0,
            vectors_searched: 0,
            collections_count: 0,
            total_vectors: 0,
            index_size_bytes: 0,
        }
    }
}

pub struct MonitoringService {
    metrics: Arc<RwLock<Metrics>>,
    request_latencies: Arc<RwLock<Vec<u64>>>,
    start_time: u64,
}

impl MonitoringService {
    pub fn new() -> Self {
        let start_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        Self {
            metrics: Arc::new(RwLock::new(Metrics::default())),
            request_latencies: Arc::new(RwLock::new(Vec::new())),
            start_time,
        }
    }

    pub async fn record_request(&self, success: bool, latency_ms: u64) {
        let mut metrics = self.metrics.write().await;
        metrics.requests_total += 1;
        
        if success {
            metrics.requests_success += 1;
        } else {
            metrics.requests_failed += 1;
        }
        
        if latency_ms < metrics.min_latency_ms {
            metrics.min_latency_ms = latency_ms;
        }
        if latency_ms > metrics.max_latency_ms {
            metrics.max_latency_ms = latency_ms;
        }
        
        let mut latencies = self.request_latencies.write().await;
        latencies.push(latency_ms);
        
        if latencies.len() > 1000 {
            latencies.drain(0..500);
        }
        
        let sum: u64 = latencies.iter().sum();
        metrics.avg_latency_ms = sum as f64 / latencies.len() as f64;
    }

    pub async fn record_insert(&self, count: u64) {
        let mut metrics = self.metrics.write().await;
        metrics.vectors_inserted += count;
        metrics.total_vectors += count;
    }

    pub async fn record_delete(&self, count: u64) {
        let mut metrics = self.metrics.write().await;
        metrics.vectors_deleted += count;
        if metrics.total_vectors >= count {
            metrics.total_vectors -= count;
        } else {
            metrics.total_vectors = 0;
        }
    }

    pub async fn record_search(&self, count: u64) {
        let mut metrics = self.metrics.write().await;
        metrics.vectors_searched += count;
    }

    pub async fn update_collections_count(&self, count: u64) {
        let mut metrics = self.metrics.write().await;
        metrics.collections_count = count;
    }

    pub async fn update_index_size(&self, size_bytes: u64) {
        let mut metrics = self.metrics.write().await;
        metrics.index_size_bytes = size_bytes;
    }

    pub async fn get_metrics(&self) -> Metrics {
        self.metrics.read().await.clone()
    }

    pub async fn get_uptime(&self) -> u64 {
        let current = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        current - self.start_time
    }

    pub async fn reset(&self) {
        let mut metrics = self.metrics.write().await;
        *metrics = Metrics::default();
        
        let mut latencies = self.request_latencies.write().await;
        latencies.clear();
    }
}
