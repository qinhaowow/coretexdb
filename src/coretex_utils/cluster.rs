//! Cluster support for CortexDB

use std::sync::Arc;
use tokio::sync::RwLock;
use std::collections::HashMap;
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NodeRole {
    Leader,
    Follower,
    Candidate,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NodeState {
    Active,
    Inactive,
   Joining,
    Leaving,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterNode {
    pub id: String,
    pub address: String,
    pub port: u16,
    pub role: NodeRole,
    pub state: NodeState,
    pub last_heartbeat: u64,
    pub shard_ids: Vec<u32>,
}

pub struct ClusterManager {
    nodes: Arc<RwLock<HashMap<String, ClusterNode>>>,
    current_node_id: String,
    shards: Arc<RwLock<HashMap<u32, Shard>>>,
    replication_factor: usize,
}

#[derive(Debug, Clone)]
pub struct Shard {
    pub id: u32,
    pub primary_node: String,
    pub replica_nodes: Vec<String>,
}

impl ClusterManager {
    pub fn new(node_id: &str, replication_factor: usize) -> Self {
        Self {
            nodes: Arc::new(RwLock::new(HashMap::new())),
            current_node_id: node_id.to_string(),
            shards: Arc::new(RwLock::new(HashMap::new())),
            replication_factor,
        }
    }

    pub async fn add_node(&self, node: ClusterNode) {
        let mut nodes = self.nodes.write().await;
        nodes.insert(node.id.clone(), node);
    }

    pub async fn remove_node(&self, node_id: &str) {
        let mut nodes = self.nodes.write().await;
        nodes.remove(node_id);
    }

    pub async fn get_nodes(&self) -> Vec<ClusterNode> {
        let nodes = self.nodes.read().await;
        nodes.values().cloned().collect()
    }

    pub async fn get_active_nodes(&self) -> Vec<ClusterNode> {
        let nodes = self.nodes.read().await;
        nodes.values()
            .filter(|n| matches!(n.state, NodeState::Active))
            .cloned()
            .collect()
    }

    pub async fn get_leader(&self) -> Option<ClusterNode> {
        let nodes = self.nodes.read().await;
        nodes.values()
            .find(|n| matches!(n.role, NodeRole::Leader))
            .cloned()
    }

    pub async fn create_shard(&self, shard_id: u32, primary_node: &str) {
        let mut shards = self.shards.write().await;
        
        let active_nodes = self.get_active_nodes().await;
        let mut replica_nodes = Vec::new();
        
        for node in active_nodes.iter() {
            if node.id != primary_node && replica_nodes.len() < self.replication_factor - 1 {
                replica_nodes.push(node.id.clone());
            }
        }
        
        shards.insert(shard_id, Shard {
            id: shard_id,
            primary_node: primary_node.to_string(),
            replica_nodes,
        });
    }

    pub async fn get_shard_nodes(&self, shard_id: u32) -> Option<(String, Vec<String>)> {
        let shards = self.shards.read().await;
        
        if let Some(shard) = shards.get(&shard_id) {
            Some((shard.primary_node.clone(), shard.replica_nodes.clone()))
        } else {
            None
        }
    }

    pub async fn rebalance_shards(&self) {
        let active_nodes = self.get_active_nodes().await;
        
        if active_nodes.is_empty() {
            return;
        }
        
        let num_shards = 16u32;
        let nodes_per_shard = self.replication_factor;
        
        for shard_id in 0..num_shards {
            let primary_idx = (shard_id as usize) % active_nodes.len();
            let primary = &active_nodes[primary_idx].id;
            
            let mut replicas = Vec::new();
            for i in 1..nodes_per_shard {
                let idx = (primary_idx + i) % active_nodes.len();
                replicas.push(active_nodes[idx].id.clone());
            }
            
            let mut shards = self.shards.write().await;
            shards.insert(shard_id, Shard {
                id: shard_id,
                primary_node: primary.clone(),
                replica_nodes: replicas,
            });
        }
    }

    pub async fn handle_heartbeat(&self, node_id: &str) {
        let mut nodes = self.nodes.write().await;
        
        if let Some(node) = nodes.get_mut(node_id) {
            node.last_heartbeat = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
        }
    }

    pub async fn detect_failed_nodes(&self, timeout_secs: u64) -> Vec<String> {
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        let nodes = self.nodes.read().await;
        
        nodes.values()
            .filter(|n| current_time - n.last_heartbeat > timeout_secs)
            .map(|n| n.id.clone())
            .collect()
    }
}
