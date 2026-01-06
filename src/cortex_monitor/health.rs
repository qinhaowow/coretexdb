//! Health checking for CortexDB cluster

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
pub struct HealthCheckResult {
    pub node_id: String,
    pub status: String,
    pub checks: Vec<CheckResult>,
    pub timestamp: u64,
}

#[cfg(feature = "distributed")]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CheckResult {
    pub name: String,
    pub status: String,
    pub message: String,
    pub duration_ms: f64,
}

#[cfg(feature = "distributed")]
#[derive(Debug)]
pub struct HealthChecker {
    check_interval: Duration,
    health_results: Arc<RwLock<Vec<HealthCheckResult>>>,
}

#[cfg(feature = "distributed")]
impl HealthChecker {
    pub fn new() -> Self {
        Self {
            check_interval: Duration::from_secs(30),
            health_results: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.check_interval = interval;
        self
    }

    pub async fn check_node_health(&self, node_id: &str) -> Result<HealthCheckResult, Box<dyn std::error::Error>> {
        let start_time = std::time::Instant::now();
        
        let mut checks = Vec::new();
        
        // Check CPU usage
        let cpu_check = self.check_cpu_usage().await?;
        checks.push(cpu_check);
        
        // Check memory usage
        let memory_check = self.check_memory_usage().await?;
        checks.push(memory_check);
        
        // Check disk usage
        let disk_check = self.check_disk_usage().await?;
        checks.push(disk_check);
        
        // Check network connectivity
        let network_check = self.check_network_connectivity().await?;
        checks.push(network_check);
        
        // Check query processing
        let query_check = self.check_query_processing().await?;
        checks.push(query_check);
        
        // Determine overall status
        let status = if checks.iter().all(|c| c.status == "healthy") {
            "healthy"
        } else if checks.iter().any(|c| c.status == "critical") {
            "critical"
        } else {
            "degraded"
        };
        
        let duration_ms = start_time.elapsed().as_millis() as f64;
        
        let result = HealthCheckResult {
            node_id: node_id.to_string(),
            status,
            checks,
            timestamp: chrono::Utc::now().timestamp() as u64,
        };
        
        let mut health_results = self.health_results.write().await;
        health_results.push(result.clone());
        
        Ok(result)
    }

    async fn check_cpu_usage(&self) -> Result<CheckResult, Box<dyn std::error::Error>> {
        let start_time = std::time::Instant::now();
        
        // Simulate CPU check (in a real implementation, we would use system APIs)
        let cpu_usage = rand::Rng::gen_range(&mut rand::thread_rng(), 0.0..100.0);
        
        let (status, message) = if cpu_usage < 80.0 {
            ("healthy", format!("CPU usage: {:.2}%", cpu_usage))
        } else if cpu_usage < 95.0 {
            ("warning", format!("High CPU usage: {:.2}%", cpu_usage))
        } else {
            ("critical", format!("Critical CPU usage: {:.2}%", cpu_usage))
        };
        
        let duration_ms = start_time.elapsed().as_millis() as f64;
        
        Ok(CheckResult {
            name: "cpu_usage".to_string(),
            status,
            message,
            duration_ms,
        })
    }

    async fn check_memory_usage(&self) -> Result<CheckResult, Box<dyn std::error::Error>> {
        let start_time = std::time::Instant::now();
        
        // Simulate memory check (in a real implementation, we would use system APIs)
        let memory_usage = rand::Rng::gen_range(&mut rand::thread_rng(), 0.0..100.0);
        
        let (status, message) = if memory_usage < 80.0 {
            ("healthy", format!("Memory usage: {:.2}%", memory_usage))
        } else if memory_usage < 95.0 {
            ("warning", format!("High memory usage: {:.2}%", memory_usage))
        } else {
            ("critical", format!("Critical memory usage: {:.2}%", memory_usage))
        };
        
        let duration_ms = start_time.elapsed().as_millis() as f64;
        
        Ok(CheckResult {
            name: "memory_usage".to_string(),
            status,
            message,
            duration_ms,
        })
    }

    async fn check_disk_usage(&self) -> Result<CheckResult, Box<dyn std::error::Error>> {
        let start_time = std::time::Instant::now();
        
        // Simulate disk check (in a real implementation, we would use system APIs)
        let disk_usage = rand::Rng::gen_range(&mut rand::thread_rng(), 0.0..100.0);
        
        let (status, message) = if disk_usage < 80.0 {
            ("healthy", format!("Disk usage: {:.2}%", disk_usage))
        } else if disk_usage < 95.0 {
            ("warning", format!("High disk usage: {:.2}%", disk_usage))
        } else {
            ("critical", format!("Critical disk usage: {:.2}%", disk_usage))
        };
        
        let duration_ms = start_time.elapsed().as_millis() as f64;
        
        Ok(CheckResult {
            name: "disk_usage".to_string(),
            status,
            message,
            duration_ms,
        })
    }

    async fn check_network_connectivity(&self) -> Result<CheckResult, Box<dyn std::error::Error>> {
        let start_time = std::time::Instant::now();
        
        // Simulate network check (in a real implementation, we would ping other nodes)
        let network_healthy = rand::Rng::gen_bool(&mut rand::thread_rng(), 0.95);
        
        let (status, message) = if network_healthy {
            ("healthy", "Network connectivity is good".to_string())
        } else {
            ("critical", "Network connectivity issues detected".to_string())
        };
        
        let duration_ms = start_time.elapsed().as_millis() as f64;
        
        Ok(CheckResult {
            name: "network_connectivity".to_string(),
            status,
            message,
            duration_ms,
        })
    }

    async fn check_query_processing(&self) -> Result<CheckResult, Box<dyn std::error::Error>> {
        let start_time = std::time::Instant::now();
        
        // Simulate query processing check (in a real implementation, we would run a test query)
        let query_healthy = rand::Rng::gen_bool(&mut rand::thread_rng(), 0.98);
        
        let (status, message) = if query_healthy {
            ("healthy", "Query processing is working correctly".to_string())
        } else {
            ("critical", "Query processing issues detected".to_string())
        };
        
        let duration_ms = start_time.elapsed().as_millis() as f64;
        
        Ok(CheckResult {
            name: "query_processing".to_string(),
            status,
            message,
            duration_ms,
        })
    }

    pub async fn get_node_health(&self, node_id: &str) -> Result<Option<HealthCheckResult>, Box<dyn std::error::Error>> {
        let health_results = self.health_results.read().await;
        Ok(health_results
            .iter()
            .filter(|r| r.node_id == node_id)
            .last()
            .cloned())
    }

    pub async fn start_health_checks(&self, cluster_manager: Arc<crate::cortex_distributed::ClusterManager>) {
        let cloned_self = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(cloned_self.check_interval).await;
                
                // Check health for each node
                let nodes = cluster_manager.get_nodes().await;
                for node in nodes {
                    if let Err(e) = cloned_self.check_node_health(&node.id).await {
                        eprintln!("Error checking health for node {}: {}", node.id, e);
                    }
                }
            }
        });
    }
}

#[cfg(feature = "distributed")]
impl Clone for HealthChecker {
    fn clone(&self) -> Self {
        Self {
            check_interval: self.check_interval,
            health_results: self.health_results.clone(),
        }
    }
}
