//! Cluster management for distributed CortexDB

#[cfg(feature = "distributed")]
use std::sync::Arc;
#[cfg(feature = "distributed")]
use tokio::sync::RwLock;
#[cfg(feature = "distributed")]
use serde::{Serialize, Deserialize};
#[cfg(feature = "distributed")]
use std::time::Duration;

#[cfg(feature = "distributed")]
#[derive(Debug, Serialize, Deserialize)]
pub struct NodeInfo {
    pub id: String,
    pub address: String,
    pub port: u16,
    pub is_leader: bool,
    pub status: NodeStatus,
    pub last_heartbeat: u64,
}

#[cfg(feature = "distributed")]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum NodeStatus {
    Healthy,
    Unhealthy,
    Joining,
    Leaving,
}

#[cfg(feature = "distributed")]
#[derive(Debug)]
pub struct ClusterManager {
    nodes: Arc<RwLock<Vec<NodeInfo>>>,
    self_node: NodeInfo,
    leader_id: Arc<RwLock<Option<String>>>,
}

#[cfg(feature = "distributed")]
impl ClusterManager {
    pub fn new() -> Self {
        let self_id = uuid::Uuid::new_v4().to_string();
        let self_node = NodeInfo {
            id: self_id.clone(),
            address: "localhost".to_string(),
            port: 8080,
            is_leader: false,
            status: NodeStatus::Joining,
            last_heartbeat: chrono::Utc::now().timestamp() as u64,
        };

        Self {
            nodes: Arc::new(RwLock::new(vec![self_node.clone()])),
            self_node,
            leader_id: Arc::new(RwLock::new(None)),
        }
    }

    pub async fn add_node(&self, node: NodeInfo) -> Result<(), Box<dyn std::error::Error>> {
        let mut nodes = self.nodes.write().await;
        if !nodes.iter().any(|n| n.id == node.id) {
            nodes.push(node);
        }
        Ok(())
    }

    pub async fn remove_node(&self, node_id: &str) -> Result<(), Box<dyn std::error::Error>> {
        let mut nodes = self.nodes.write().await;
        nodes.retain(|n| n.id != node_id);
        
        // If the removed node was the leader, elect a new one
        let leader_id = self.leader_id.read().await;
        if leader_id.as_deref() == Some(node_id) {
            self.elect_leader().await?;
        }
        
        Ok(())
    }

    pub async fn get_nodes(&self) -> Vec<NodeInfo> {
        let nodes = self.nodes.read().await;
        nodes.clone()
    }

    pub async fn get_healthy_nodes(&self) -> Vec<NodeInfo> {
        let nodes = self.nodes.read().await;
        nodes.iter()
            .filter(|n| n.status == NodeStatus::Healthy)
            .cloned()
            .collect()
    }

    pub async fn update_node_status(&self, node_id: &str, status: NodeStatus) -> Result<(), Box<dyn std::error::Error>> {
        let mut nodes = self.nodes.write().await;
        if let Some(node) = nodes.iter_mut().find(|n| n.id == node_id) {
            node.status = status;
            node.last_heartbeat = chrono::Utc::now().timestamp() as u64;
        }
        Ok(())
    }

    pub async fn send_heartbeat(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut nodes = self.nodes.write().await;
        if let Some(self_node) = nodes.iter_mut().find(|n| n.id == self.self_node.id) {
            self_node.last_heartbeat = chrono::Utc::now().timestamp() as u64;
            self_node.status = NodeStatus::Healthy;
        }
        Ok(())
    }

    pub async fn elect_leader(&self) -> Result<(), Box<dyn std::error::Error>> {
        let nodes = self.nodes.read().await;
        let healthy_nodes: Vec<&NodeInfo> = nodes
            .iter()
            .filter(|n| n.status == NodeStatus::Healthy)
            .collect();

        if !healthy_nodes.is_empty() {
            // Simple leader election: choose the node with the smallest ID
            let leader = healthy_nodes
                .iter()
                .min_by(|a, b| a.id.cmp(&b.id))
                .unwrap();
            
            let mut leader_id = self.leader_id.write().await;
            *leader_id = Some(leader.id.clone());

            // Update leader status in nodes
            let mut nodes = self.nodes.write().await;
            for node in nodes.iter_mut() {
                node.is_leader = node.id == leader.id;
            }
        }

        Ok(())
    }

    pub async fn get_leader(&self) -> Option<NodeInfo> {
        let leader_id = self.leader_id.read().await;
        if let Some(id) = leader_id.as_ref() {
            let nodes = self.nodes.read().await;
            nodes.iter().find(|n| n.id == *id).cloned()
        } else {
            None
        }
    }

    pub async fn is_leader(&self) -> bool {
        let leader_id = self.leader_id.read().await;
        leader_id.as_deref() == Some(&self.self_node.id)
    }

    pub async fn start_heartbeat_monitor(&self) {
        let cloned_self = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;
                if let Err(e) = cloned_self.check_node_health().await {
                    eprintln!("Error checking node health: {}", e);
                }
            }
        });
    }

    async fn check_node_health(&self) -> Result<(), Box<dyn std::error::Error>> {
        let now = chrono::Utc::now().timestamp() as u64;
        let mut nodes = self.nodes.write().await;
        
        for node in nodes.iter_mut() {
            if node.id != self.self_node.id && (now - node.last_heartbeat) > 30 {
                node.status = NodeStatus::Unhealthy;
            }
        }
        
        // If leader is unhealthy, elect a new one
        let leader = self.get_leader().await;
        if let Some(leader_node) = leader {
            if leader_node.status == NodeStatus::Unhealthy {
                self.elect_leader().await?;
            }
        }
        
        Ok(())
    }
}

#[cfg(feature = "distributed")]
impl Clone for ClusterManager {
    fn clone(&self) -> Self {
        Self {
            nodes: self.nodes.clone(),
            self_node: self.self_node.clone(),
            leader_id: self.leader_id.clone(),
        }
    }
}
