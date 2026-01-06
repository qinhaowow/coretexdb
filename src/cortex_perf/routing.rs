//! Intelligent query routing for performance optimization

use std::sync::Arc;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct NodeLoad {
    pub node_id: String,
    pub cpu_usage: f64,
    pub memory_usage: f64,
    pub query_count: u64,
    pub queue_size: usize,
    pub last_updated: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RoutingConfig {
    pub load_balancing: bool,
    pub proximity_routing: bool,
    pub capacity_based_routing: bool,
}

#[derive(Debug)]
pub struct QueryRouter {
    node_loads: Arc<RwLock<Vec<NodeLoad>>>,
    config: RoutingConfig,
}

impl QueryRouter {
    pub fn new() -> Self {
        Self {
            node_loads: Arc::new(RwLock::new(Vec::new())),
            config: RoutingConfig {
                load_balancing: true,
                proximity_routing: true,
                capacity_based_routing: true,
            },
        }
    }

    pub fn with_config(mut self, config: RoutingConfig) -> Self {
        self.config = config;
        self
    }

    pub async fn update_node_load(&self, node_load: NodeLoad) -> Result<(), Box<dyn std::error::Error>> {
        let mut node_loads = self.node_loads.write().await;
        
        // Update existing node load or add new one
        if let Some(existing) = node_loads.iter_mut().find(|nl| nl.node_id == node_load.node_id) {
            *existing = node_load;
        } else {
            node_loads.push(node_load);
        }
        
        Ok(())
    }

    pub async fn route_query(&self, query_size: usize) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let node_loads = self.node_loads.read().await;
        
        if node_loads.is_empty() {
            return Err("No nodes available".into());
        }
        
        let mut nodes: Vec<(&NodeLoad, f64)> = Vec::new();
        
        // Calculate load score for each node
        for node in node_loads.iter() {
            let score = self.calculate_node_score(node, query_size);
            nodes.push((node, score));
        }
        
        // Sort nodes by score (lower score is better)
        nodes.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        
        // Select top nodes
        let selected_nodes: Vec<String> = nodes
            .iter()
            .take(3) // Select top 3 nodes for parallel querying
            .map(|(node, _)| node.node_id.clone())
            .collect();
        
        Ok(selected_nodes)
    }

    fn calculate_node_score(&self, node: &NodeLoad, query_size: usize) -> f64 {
        let mut score = 0.0;
        
        // CPU usage (0-100)
        score += node.cpu_usage * 0.3;
        
        // Memory usage (0-100)
        score += node.memory_usage * 0.2;
        
        // Query count (normalized)
        let normalized_query_count = (node.query_count as f64 / 100.0).min(1.0) * 100.0;
        score += normalized_query_count * 0.2;
        
        // Queue size (normalized)
        let normalized_queue_size = (node.queue_size as f64 / 100.0).min(1.0) * 100.0;
        score += normalized_queue_size * 0.2;
        
        // Query size impact
        let query_size_impact = (query_size as f64 / 1000.0).min(1.0) * 100.0;
        score += query_size_impact * 0.1;
        
        score
    }

    pub async fn get_node_loads(&self) -> Result<Vec<NodeLoad>, Box<dyn std::error::Error>> {
        let node_loads = self.node_loads.read().await;
        Ok(node_loads.clone())
    }

    pub async fn remove_node(&self, node_id: &str) -> Result<bool, Box<dyn std::error::Error>> {
        let mut node_loads = self.node_loads.write().await;
        let node_index = node_loads.iter().position(|nl| nl.node_id == node_id);
        
        if let Some(index) = node_index {
            node_loads.remove(index);
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

impl Clone for QueryRouter {
    fn clone(&self) -> Self {
        Self {
            node_loads: self.node_loads.clone(),
            config: self.config.clone(),
        }
    }
}
