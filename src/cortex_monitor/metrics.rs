//! Metrics collection for CortexDB cluster

#[cfg(feature = "distributed")]
use std::sync::Arc;
#[cfg(feature = "distributed")]
use tokio::sync::RwLock;
#[cfg(feature = "distributed")]
use serde::{Serialize, Deserialize};
#[cfg(feature = "distributed")]
use std::time::Duration;

#[cfg(feature = "distributed")]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct NodeMetrics {
    pub node_id: String,
    pub cpu_usage: f64,
    pub memory_usage: f64,
    pub disk_usage: f64,
    pub network_in: u64,
    pub network_out: u64,
    pub query_count: u64,
    pub query_latency: f64,
    pub vector_count: u64,
    pub timestamp: u64,
}

#[cfg(feature = "distributed")]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ClusterMetrics {
    pub total_nodes: usize,
    pub healthy_nodes: usize,
    pub total_vectors: u64,
    pub total_queries: u64,
    pub average_query_latency: f64,
    pub timestamp: u64,
}

#[cfg(feature = "distributed")]
#[derive(Debug)]
pub struct MetricsCollector {
    node_metrics: Arc<RwLock<Vec<NodeMetrics>>>,
    cluster_metrics: Arc<RwLock<Option<ClusterMetrics>>>,
    collection_interval: Duration,
}

#[cfg(feature = "distributed")]
impl MetricsCollector {
    pub fn new() -> Self {
        Self {
            node_metrics: Arc::new(RwLock::new(Vec::new())),
            cluster_metrics: Arc::new(RwLock::new(None)),
            collection_interval: Duration::from_secs(10),
        }
    }

    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.collection_interval = interval;
        self
    }

    pub async fn collect_node_metrics(&self, node_id: &str) -> Result<NodeMetrics, Box<dyn std::error::Error>> {
        // Simulate collecting metrics (in a real implementation, we would use system APIs)
        let metrics = NodeMetrics {
            node_id: node_id.to_string(),
            cpu_usage: rand::Rng::gen_range(&mut rand::thread_rng(), 0.0..100.0),
            memory_usage: rand::Rng::gen_range(&mut rand::thread_rng(), 0.0..100.0),
            disk_usage: rand::Rng::gen_range(&mut rand::thread_rng(), 0.0..100.0),
            network_in: rand::Rng::gen_range(&mut rand::thread_rng(), 0..1000000),
            network_out: rand::Rng::gen_range(&mut rand::thread_rng(), 0..1000000),
            query_count: rand::Rng::gen_range(&mut rand::thread_rng(), 0..1000),
            query_latency: rand::Rng::gen_range(&mut rand::thread_rng(), 0.0..100.0),
            vector_count: rand::Rng::gen_range(&mut rand::thread_rng(), 0..1000000),
            timestamp: chrono::Utc::now().timestamp() as u64,
        };

        let mut node_metrics = self.node_metrics.write().await;
        node_metrics.push(metrics.clone());

        Ok(metrics)
    }

    pub async fn collect_cluster_metrics(&self, cluster_manager: &crate::cortex_distributed::ClusterManager) -> Result<ClusterMetrics, Box<dyn std::error::Error>> {
        let nodes = cluster_manager.get_nodes().await;
        let healthy_nodes = cluster_manager.get_healthy_nodes().await;

        let node_metrics = self.node_metrics.read().await;
        let total_queries: u64 = node_metrics.iter().map(|m| m.query_count).sum();
        let total_query_latency: f64 = node_metrics.iter().map(|m| m.query_latency).sum();
        let total_vectors: u64 = node_metrics.iter().map(|m| m.vector_count).sum();

        let metrics = ClusterMetrics {
            total_nodes: nodes.len(),
            healthy_nodes: healthy_nodes.len(),
            total_vectors,
            total_queries,
            average_query_latency: if !node_metrics.is_empty() {
                total_query_latency / node_metrics.len() as f64
            } else {
                0.0
            },
            timestamp: chrono::Utc::now().timestamp() as u64,
        };

        let mut cluster_metrics = self.cluster_metrics.write().await;
        *cluster_metrics = Some(metrics.clone());

        Ok(metrics)
    }

    pub async fn get_node_metrics(&self, node_id: &str) -> Result<Option<NodeMetrics>, Box<dyn std::error::Error>> {
        let node_metrics = self.node_metrics.read().await;
        Ok(node_metrics
            .iter()
            .filter(|m| m.node_id == node_id)
            .last()
            .cloned())
    }

    pub async fn get_cluster_metrics(&self) -> Result<Option<ClusterMetrics>, Box<dyn std::error::Error>> {
        let cluster_metrics = self.cluster_metrics.read().await;
        Ok(cluster_metrics.clone())
    }

    pub async fn start_collection(&self, cluster_manager: Arc<crate::cortex_distributed::ClusterManager>) {
        let cloned_self = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(cloned_self.collection_interval).await;
                
                // Collect metrics for each node
                let nodes = cluster_manager.get_nodes().await;
                for node in nodes {
                    if let Err(e) = cloned_self.collect_node_metrics(&node.id).await {
                        eprintln!("Error collecting metrics for node {}: {}", node.id, e);
                    }
                }
                
                // Collect cluster metrics
                if let Err(e) = cloned_self.collect_cluster_metrics(&cluster_manager).await {
                    eprintln!("Error collecting cluster metrics: {}", e);
                }
            }
        });
    }
}

#[cfg(feature = "distributed")]
impl Clone for MetricsCollector {
    fn clone(&self) -> Self {
        Self {
            node_metrics: self.node_metrics.clone(),
            cluster_metrics: self.cluster_metrics.clone(),
            collection_interval: self.collection_interval,
        }
    }
}
