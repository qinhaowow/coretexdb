//! Fault tolerance and failover mechanisms for CoreTexDB
//! Provides node health checking, leader election, and automatic failover

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, RwLock};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeHealth {
    pub node_id: String,
    pub status: NodeStatus,
    pub last_heartbeat: i64,
    pub load: f32,
    pub response_time_ms: u64,
    pub is_leader: bool,
    pub term: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum NodeStatus {
    Unknown,
    Healthy,
    Degraded,
    Unhealthy,
    Offline,
}

impl Default for NodeHealth {
    fn default() -> Self {
        Self {
            node_id: String::new(),
            status: NodeStatus::Unknown,
            last_heartbeat: 0,
            load: 0.0,
            response_time_ms: 0,
            is_leader: false,
            term: 0,
        }
    }
}

pub struct FailoverConfig {
    pub heartbeat_interval_ms: u64,
    pub election_timeout_ms: u64,
    pub max_retry_attempts: u32,
    pub retry_delay_ms: u64,
    pub health_check_interval_ms: u64,
    pub leader_timeout_ms: u64,
}

impl Default for FailoverConfig {
    fn default() -> Self {
        Self {
            heartbeat_interval_ms: 1000,
            election_timeout_ms: 5000,
            max_retry_attempts: 3,
            retry_delay_ms: 100,
            health_check_interval_ms: 2000,
            leader_timeout_ms: 10000,
        }
    }
}

pub struct FailoverManager {
    config: FailoverConfig,
    nodes: Arc<RwLock<HashMap<String, NodeHealth>>>,
    local_node_id: String,
    leader_id: Arc<RwLock<Option<String>>>,
    event_sender: broadcast::Sender<FailoverEvent>,
    current_term: Arc<RwLock<u64>>,
    voted_for: Arc<RwLock<Option<String>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FailoverEvent {
    LeaderElected { node_id: String, term: u64 },
    NodeFailed { node_id: String },
    NodeRecovered { node_id: String },
    LeaderChanged { old_leader: Option<String>, new_leader: String },
    HealthCheckFailed { node_id: String, reason: String },
}

impl FailoverManager {
    pub fn new(local_node_id: &str, config: FailoverConfig) -> Self {
        let (event_sender, _) = broadcast::channel(1000);
        
        Self {
            config,
            nodes: Arc::new(RwLock::new(HashMap::new())),
            local_node_id: local_node_id.to_string(),
            leader_id: Arc::new(RwLock::new(None)),
            event_sender,
            current_term: Arc::new(RwLock::new(0)),
            voted_for: Arc::new(RwLock::new(None)),
        }
    }

    pub async fn register_node(&self, node_id: &str, health: NodeHealth) {
        let mut nodes = self.nodes.write().await;
        nodes.insert(node_id.to_string(), health);
    }

    pub async fn update_heartbeat(&self, node_id: &str) {
        let mut nodes = self.nodes.write().await;
        if let Some(node) = nodes.get_mut(node_id) {
            node.last_heartbeat = chrono::Utc::now().timestamp();
            if node.status == NodeStatus::Offline {
                node.status = NodeStatus::Healthy;
                let _ = self.event_sender.send(FailoverEvent::NodeRecovered { 
                    node_id: node_id.to_string() 
                });
            }
        }
    }

    pub async fn check_node_health(&self, node_id: &str) -> NodeStatus {
        let nodes = self.nodes.read().await;
        
        if let Some(node) = nodes.get(node_id) {
            let now = chrono::Utc::now().timestamp_millis();
            let last_heartbeat = node.last_heartbeat * 1000;
            
            let elapsed = now - last_heartbeat;
            
            if elapsed > self.config.leader_timeout_ms as i64 {
                NodeStatus::Offline
            } else if elapsed > self.config.health_check_interval_ms as i64 * 2 {
                NodeStatus::Unhealthy
            } else if elapsed > self.config.health_check_interval_ms as i64 {
                NodeStatus::Degraded
            } else {
                NodeStatus::Healthy
            }
        } else {
            NodeStatus::Unknown
        }
    }

    pub async fn get_healthy_nodes(&self) -> Vec<String> {
        let nodes = self.nodes.read().await;
        
        nodes.iter()
            .filter(|(_, health)| {
                health.status == NodeStatus::Healthy || health.status == NodeStatus::Degraded
            })
            .map(|(id, _)| id.clone())
            .collect()
    }

    pub async fn request_vote(&self, candidate_id: &str) -> bool {
        let mut term = self.current_term.write().await;
        let mut voted_for = self.voted_for.write().await;
        
        if *term == 0 {
            *term += 1;
        }
        
        if voted_for.is_none() || voted_for.as_ref() == Some(&candidate_id.to_string()) {
            *voted_for = Some(candidate_id.to_string());
            return true;
        }
        
        false
    }

    pub async fn start_election(&self) -> Option<String> {
        let mut term = self.current_term.write().await;
        *term += 1;
        let current_term = *term;
        
        let nodes = self.nodes.read().await;
        let healthy_nodes: Vec<_> = nodes.iter()
            .filter(|(_, h)| h.status == NodeStatus::Healthy && h.node_id != self.local_node_id)
            .map(|(id, _)| id.clone())
            .collect();
        
        drop(nodes);
        
        let vote_count = 1;
        
        if vote_count > healthy_nodes.len() / 2 {
            let mut leader = self.leader_id.write().await;
            *leader = Some(self.local_node_id.clone());
            
            let _ = self.event_sender.send(FailoverEvent::LeaderElected {
                node_id: self.local_node_id.clone(),
                term: current_term,
            });
            
            Some(self.local_node_id.clone())
        } else {
            None
        }
    }

    pub async fn get_leader(&self) -> Option<String> {
        let leader = self.leader_id.read().await;
        leader.clone()
    }

    pub async fn set_leader(&self, leader_id: &str) {
        let mut leader = self.leader_id.write().await;
        let old_leader = leader.clone();
        *leader = Some(leader_id.to_string());
        
        if old_leader.as_ref() != Some(&leader_id.to_string()) {
            let _ = self.event_sender.send(FailoverEvent::LeaderChanged {
                old_leader,
                new_leader: leader_id.to_string(),
            });
        }
    }

    pub async fn promote_to_leader(&self) {
        let mut leader = self.leader_id.write().await;
        *leader = Some(self.local_node_id.clone());
        
        let mut term = self.current_term.write().await;
        *term += 1;
        
        let _ = self.event_sender.send(FailoverEvent::LeaderElected {
            node_id: self.local_node_id.clone(),
            term: *term,
        });
    }

    pub async fn handle_node_failure(&self, node_id: &str) {
        let mut nodes = self.nodes.write().await;
        if let Some(node) = nodes.get_mut(node_id) {
            node.status = NodeStatus::Offline;
        }
        
        let leader = self.leader_id.read().await;
        if leader.as_deref() == Some(node_id) {
            drop(leader);
            let _ = self.start_election().await;
        }
        
        let _ = self.event_sender.send(FailoverEvent::NodeFailed {
            node_id: node_id.to_string(),
        });
    }

    pub async fn get_cluster_stats(&self) -> ClusterStats {
        let nodes = self.nodes.read().await;
        
        let healthy = nodes.values().filter(|n| n.status == NodeStatus::Healthy).count();
        let degraded = nodes.values().filter(|n| n.status == NodeStatus::Degraded).count();
        let unhealthy = nodes.values().filter(|n| n.status == NodeStatus::Unhealthy).count();
        let offline = nodes.values().filter(|n| n.status == NodeStatus::Offline).count();
        
        let leader = self.leader_id.read().await;
        
        ClusterStats {
            total_nodes: nodes.len(),
            healthy_nodes: healthy,
            degraded_nodes: degraded,
            unhealthy_nodes: unhealthy,
            offline_nodes: offline,
            current_leader: leader.clone(),
            term: *self.current_term.read().await,
        }
    }

    pub fn event_receiver(&self) -> broadcast::Receiver<FailoverEvent> {
        self.event_sender.subscribe()
    }

    pub fn local_node_id(&self) -> &str {
        &self.local_node_id
    }

    pub async fn is_leader(&self) -> bool {
        let leader = self.leader_id.read().await;
        leader.as_deref() == Some(&self.local_node_id)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterStats {
    pub total_nodes: usize,
    pub healthy_nodes: usize,
    pub degraded_nodes: usize,
    pub unhealthy_nodes: usize,
    pub offline_nodes: usize,
    pub current_leader: Option<String>,
    pub term: u64,
}

pub struct ConnectionPool {
    nodes: Arc<RwLock<HashMap<String, ConnectionPoolEntry>>>,
    max_connections_per_node: usize,
}

struct ConnectionPoolEntry {
    active_connections: usize,
    available_connections: usize,
    last_used: Instant,
}

impl ConnectionPool {
    pub fn new(max_connections_per_node: usize) -> Self {
        Self {
            nodes: Arc::new(RwLock::new(HashMap::new())),
            max_connections_per_node,
        }
    }

    pub async fn add_node(&self, node_id: &str) {
        let mut nodes = self.nodes.write().await;
        nodes.insert(node_id.to_string(), ConnectionPoolEntry {
            active_connections: 0,
            available_connections: self.max_connections_per_node,
            last_used: Instant::now(),
        });
    }

    pub async fn acquire_connection(&self, node_id: &str) -> Result<(), String> {
        let mut nodes = self.nodes.write().await;
        
        if let Some(entry) = nodes.get_mut(node_id) {
            if entry.available_connections > 0 {
                entry.available_connections -= 1;
                entry.active_connections += 1;
                entry.last_used = Instant::now();
                Ok(())
            } else {
                Err("No available connections".to_string())
            }
        } else {
            Err("Node not found".to_string())
        }
    }

    pub async fn release_connection(&self, node_id: &str) {
        let mut nodes = self.nodes.write().await;
        
        if let Some(entry) = nodes.get_mut(node_id) {
            entry.available_connections += 1;
            entry.active_connections = entry.active_connections.saturating_sub(1);
        }
    }

    pub async fn remove_node(&self, node_id: &str) {
        let mut nodes = self.nodes.write().await;
        nodes.remove(node_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_failover_config_default() {
        let config = FailoverConfig::default();
        
        assert_eq!(config.heartbeat_interval_ms, 1000);
        assert_eq!(config.election_timeout_ms, 5000);
    }

    #[tokio::test]
    async fn test_failover_manager_new() {
        let manager = FailoverManager::new("node1", FailoverConfig::default());
        
        assert_eq!(manager.local_node_id(), "node1");
    }

    #[tokio::test]
    async fn test_register_node() {
        let manager = FailoverManager::new("node1", FailoverConfig::default());
        
        let health = NodeHealth {
            node_id: "node2".to_string(),
            status: NodeStatus::Healthy,
            last_heartbeat: chrono::Utc::now().timestamp(),
            load: 0.5,
            response_time_ms: 10,
            is_leader: false,
            term: 1,
        };
        
        manager.register_node("node2", health).await;
        
        let stats = manager.get_cluster_stats().await;
        assert_eq!(stats.total_nodes, 1);
    }

    #[tokio::test]
    async fn test_update_heartbeat() {
        let manager = FailoverManager::new("node1", FailoverConfig::default());
        
        let health = NodeHealth {
            node_id: "node2".to_string(),
            status: NodeStatus::Healthy,
            last_heartbeat: chrono::Utc::now().timestamp(),
            load: 0.5,
            response_time_ms: 10,
            is_leader: false,
            term: 1,
        };
        
        manager.register_node("node2", health).await;
        manager.update_heartbeat("node2").await;
        
        let status = manager.check_node_health("node2").await;
        assert_eq!(status, NodeStatus::Healthy);
    }

    #[tokio::test]
    async fn test_get_healthy_nodes() {
        let manager = FailoverManager::new("node1", FailoverConfig::default());
        
        manager.register_node("node2", NodeHealth {
            node_id: "node2".to_string(),
            status: NodeStatus::Healthy,
            last_heartbeat: chrono::Utc::now().timestamp(),
            load: 0.5,
            response_time_ms: 10,
            is_leader: false,
            term: 1,
        }).await;
        
        manager.register_node("node3", NodeHealth {
            node_id: "node3".to_string(),
            status: NodeStatus::Offline,
            last_heartbeat: chrono::Utc::now().timestamp(),
            load: 0.5,
            response_time_ms: 10,
            is_leader: false,
            term: 1,
        }).await;
        
        let healthy = manager.get_healthy_nodes().await;
        assert_eq!(healthy.len(), 1);
    }

    #[tokio::test]
    async fn test_leader_election() {
        let manager = FailoverManager::new("node1", FailoverConfig::default());
        
        let result = manager.start_election().await;
        assert!(result.is_some());
        
        let is_leader = manager.is_leader().await;
        assert!(is_leader);
    }

    #[tokio::test]
    async fn test_set_leader() {
        let manager = FailoverManager::new("node1", FailoverConfig::default());
        
        manager.set_leader("node2").await;
        
        let leader = manager.get_leader().await;
        assert_eq!(leader, Some("node2".to_string()));
    }

    #[tokio::test]
    async fn test_node_failure() {
        let manager = FailoverManager::new("node1", FailoverConfig::default());
        
        manager.register_node("node2", NodeHealth {
            node_id: "node2".to_string(),
            status: NodeStatus::Healthy,
            last_heartbeat: chrono::Utc::now().timestamp(),
            load: 0.5,
            response_time_ms: 10,
            is_leader: true,
            term: 1,
        }).await;
        
        manager.set_leader("node2").await;
        manager.handle_node_failure("node2").await;
        
        let leader = manager.get_leader().await;
        assert!(leader.is_some());
    }

    #[tokio::test]
    async fn test_connection_pool() {
        let pool = ConnectionPool::new(5);
        
        pool.add_node("node1").await;
        
        pool.acquire_connection("node1").await.unwrap();
        pool.acquire_connection("node1").await.unwrap();
        
        pool.release_connection("node1").await;
        
        let result = pool.acquire_connection("node1").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_connection_pool_no_available() {
        let pool = ConnectionPool::new(1);
        
        pool.add_node("node1").await;
        
        pool.acquire_connection("node1").await.unwrap();
        
        let result = pool.acquire_connection("node1").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_cluster_stats() {
        let manager = FailoverManager::new("node1", FailoverConfig::default());
        
        manager.register_node("node2", NodeHealth {
            node_id: "node2".to_string(),
            status: NodeStatus::Healthy,
            last_heartbeat: chrono::Utc::now().timestamp(),
            load: 0.5,
            response_time_ms: 10,
            is_leader: false,
            term: 1,
        }).await;
        
        let stats = manager.get_cluster_stats().await;
        
        assert_eq!(stats.total_nodes, 1);
        assert_eq!(stats.healthy_nodes, 1);
    }
}
