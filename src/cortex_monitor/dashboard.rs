//! Monitoring dashboard for CortexDB cluster

#[cfg(feature = "distributed")]
use std::sync::Arc;
#[cfg(feature = "distributed")]
use tokio::sync::RwLock;
#[cfg(feature = "distributed")]
use serde::{Serialize, Deserialize};

#[cfg(feature = "distributed")]
use crate::cortex_monitor::{
    MetricsCollector,
    HealthChecker,
};

#[cfg(feature = "distributed")]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DashboardData {
    pub cluster_metrics: Option<crate::cortex_monitor::metrics::ClusterMetrics>,
    pub node_healths: Vec<crate::cortex_monitor::health::HealthCheckResult>,
    pub timestamp: u64,
}

#[cfg(feature = "distributed")]
#[derive(Debug)]
pub struct MonitoringDashboard {
    metrics_collector: Arc<MetricsCollector>,
    health_checker: Arc<HealthChecker>,
    dashboard_data: Arc<RwLock<Option<DashboardData>>>,
}

#[cfg(feature = "distributed")]
impl MonitoringDashboard {
    pub fn new() -> Self {
        Self {
            metrics_collector: Arc::new(MetricsCollector::new()),
            health_checker: Arc::new(HealthChecker::new()),
            dashboard_data: Arc::new(RwLock::new(None)),
        }
    }

    pub fn with_metrics_collector(mut self, metrics_collector: Arc<MetricsCollector>) -> Self {
        self.metrics_collector = metrics_collector;
        self
    }

    pub fn with_health_checker(mut self, health_checker: Arc<HealthChecker>) -> Self {
        self.health_checker = health_checker;
        self
    }

    pub async fn get_dashboard_data(&self, cluster_manager: &crate::cortex_distributed::ClusterManager) -> Result<DashboardData, Box<dyn std::error::Error>> {
        // Get cluster metrics
        let cluster_metrics = self.metrics_collector.get_cluster_metrics().await?;
        
        // Get node healths
        let nodes = cluster_manager.get_nodes().await;
        let mut node_healths = Vec::new();
        
        for node in nodes {
            if let Some(health) = self.health_checker.get_node_health(&node.id).await? {
                node_healths.push(health);
            }
        }
        
        let data = DashboardData {
            cluster_metrics,
            node_healths,
            timestamp: chrono::Utc::now().timestamp() as u64,
        };
        
        let mut dashboard_data = self.dashboard_data.write().await;
        *dashboard_data = Some(data.clone());
        
        Ok(data)
    }

    pub async fn start_refreshing(&self, cluster_manager: Arc<crate::cortex_distributed::ClusterManager>) {
        let cloned_self = self.clone();
        tokio::spawn(async move {
            loop {
                // Refresh dashboard data every 30 seconds
                tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                
                if let Err(e) = cloned_self.get_dashboard_data(&cluster_manager).await {
                    eprintln!("Error refreshing dashboard data: {}", e);
                }
            }
        });
    }

    pub async fn get_latest_data(&self) -> Result<Option<DashboardData>, Box<dyn std::error::Error>> {
        let dashboard_data = self.dashboard_data.read().await;
        Ok(dashboard_data.clone())
    }

    pub async fn export_dashboard_data(&self) -> Result<String, Box<dyn std::error::Error>> {
        let dashboard_data = self.dashboard_data.read().await;
        if let Some(data) = dashboard_data.as_ref() {
            let json = serde_json::to_string(data)?;
            Ok(json)
        } else {
            Ok("{}".to_string())
        }
    }
}

#[cfg(feature = "distributed")]
impl Clone for MonitoringDashboard {
    fn clone(&self) -> Self {
        Self {
            metrics_collector: self.metrics_collector.clone(),
            health_checker: self.health_checker.clone(),
            dashboard_data: self.dashboard_data.clone(),
        }
    }
}
