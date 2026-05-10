use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, RwLock, mpsc};
use tokio::time;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::coretex_failover::{VoteRequest, VoteResponse, HeartbeatRequest, HeartbeatResponse, RaftRpc, HttpRaftRpc};

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum SentinelNodeRole {
    Leader,
    Follower,
    Candidate,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum NodeStatus {
    Unknown,
    Online,
    SubjectiveOffline,
    ObjectiveOffline,
    Recovering,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SentinelNode {
    pub node_id: String,
    pub address: String,
    pub port: u16,
    pub role: SentinelNodeRole,
    pub status: NodeStatus,
    pub last_heartbeat: i64,
    pub term: u64,
    pub data_lag: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SentinelConfig {
    pub sentinel_id: String,
    pub quorum: usize,
    pub heartbeat_interval_ms: u64,
    pub election_timeout_ms: u64,
    pub failover_timeout_ms: u64,
    pub down_after_ms: u64,
    pub parallel_syncs: usize,
    pub monitor_address: String,
    pub monitor_port: u16,
}

impl Default for SentinelConfig {
    fn default() -> Self {
        Self {
            sentinel_id: format!("sentinel-{}", Uuid::new_v4().to_string().split('-').next().unwrap_or("1")),
            quorum: 2,
            heartbeat_interval_ms: 1000,
            election_timeout_ms: 5000,
            failover_timeout_ms: 30000,
            down_after_ms: 3000,
            parallel_syncs: 1,
            monitor_address: "127.0.0.1".to_string(),
            monitor_port: 6379,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SentinelEvent {
    NodeJoined { node_id: String, address: String },
    NodeLeft { node_id: String },
    NodeStatusChanged { node_id: String, status: NodeStatus },
    LeaderElected { leader_id: String, term: u64 },
    FailoverStarted { old_leader: String, term: u64 },
    FailoverCompleted { old_leader: String, new_leader: String, term: u64 },
    FailoverAborted { reason: String },
    ConfigurationChanged { changes: Vec<String> },
}

pub struct SentinelManager {
    config: SentinelConfig,
    nodes: Arc<RwLock<HashMap<String, SentinelNode>>>,
    local_node: Arc<RwLock<SentinelNode>>,
    current_term: Arc<RwLock<u64>>,
    voted_for: Arc<RwLock<Option<String>>>,
    event_sender: broadcast::Sender<SentinelEvent>,
    failover_in_progress: Arc<RwLock<bool>>,
    leader_last_seen: Arc<RwLock<Instant>>,
    rpc: Arc<dyn RaftRpc>,
}

impl SentinelManager {
    pub fn new(config: SentinelConfig) -> Self {
        let (event_sender, _) = broadcast::channel(1000);
        let rpc = Arc::new(HttpRaftRpc::new(config.election_timeout_ms));

        let local_node = SentinelNode {
            node_id: config.sentinel_id.clone(),
            address: config.monitor_address.clone(),
            port: config.monitor_port,
            role: SentinelNodeRole::Follower,
            status: NodeStatus::Online,
            last_heartbeat: chrono::Utc::now().timestamp(),
            term: 0,
            data_lag: 0,
        };

        Self {
            config,
            nodes: Arc::new(RwLock::new(HashMap::new())),
            local_node: Arc::new(RwLock::new(local_node)),
            current_term: Arc::new(RwLock::new(0)),
            voted_for: Arc::new(RwLock::new(None)),
            event_sender,
            failover_in_progress: Arc::new(RwLock::new(false)),
            leader_last_seen: Arc::new(RwLock::new(Instant::now())),
            rpc,
        }
    }

    pub fn with_rpc(config: SentinelConfig, rpc: Arc<dyn RaftRpc>) -> Self {
        let (event_sender, _) = broadcast::channel(1000);

        let local_node = SentinelNode {
            node_id: config.sentinel_id.clone(),
            address: config.monitor_address.clone(),
            port: config.monitor_port,
            role: SentinelNodeRole::Follower,
            status: NodeStatus::Online,
            last_heartbeat: chrono::Utc::now().timestamp(),
            term: 0,
            data_lag: 0,
        };

        Self {
            config,
            nodes: Arc::new(RwLock::new(HashMap::new())),
            local_node: Arc::new(RwLock::new(local_node)),
            current_term: Arc::new(RwLock::new(0)),
            voted_for: Arc::new(RwLock::new(None)),
            event_sender,
            failover_in_progress: Arc::new(RwLock::new(false)),
            leader_last_seen: Arc::new(RwLock::new(Instant::now())),
            rpc,
        }
    }

    pub async fn register_node(&self, node_id: &str, address: &str, port: u16) {
        let mut nodes = self.nodes.write().await;
        if !nodes.contains_key(node_id) {
            let node = SentinelNode {
                node_id: node_id.to_string(),
                address: address.to_string(),
                port,
                role: SentinelNodeRole::Follower,
                status: NodeStatus::Online,
                last_heartbeat: chrono::Utc::now().timestamp(),
                term: 0,
                data_lag: 0,
            };
            nodes.insert(node_id.to_string(), node);
            let _ = self.event_sender.send(SentinelEvent::NodeJoined {
                node_id: node_id.to_string(),
                address: address.to_string(),
            });
        }
    }

    pub async fn update_heartbeat(&self, node_id: &str) {
        let mut nodes = self.nodes.write().await;
        if let Some(node) = nodes.get_mut(node_id) {
            let was_offline = node.status == NodeStatus::ObjectiveOffline
                || node.status == NodeStatus::SubjectiveOffline;
            node.last_heartbeat = chrono::Utc::now().timestamp();
            node.status = NodeStatus::Online;
            if was_offline {
                let _ = self.event_sender.send(SentinelEvent::NodeStatusChanged {
                    node_id: node_id.to_string(),
                    status: NodeStatus::Online,
                });
            }
        }
    }

    pub async fn check_node_status(&self, node_id: &str) -> NodeStatus {
        let nodes = self.nodes.read().await;
        if let Some(node) = nodes.get(node_id) {
            let now = chrono::Utc::now().timestamp_millis();
            let last_hb = node.last_heartbeat * 1000;
            let elapsed = now - last_hb;

            if elapsed > self.config.down_after_ms as i64 * 2 {
                NodeStatus::ObjectiveOffline
            } else if elapsed > self.config.down_after_ms as i64 {
                NodeStatus::SubjectiveOffline
            } else {
                NodeStatus::Online
            }
        } else {
            NodeStatus::Unknown
        }
    }

    pub async fn get_online_nodes(&self) -> Vec<String> {
        let nodes = self.nodes.read().await;
        nodes.iter()
            .filter(|(_, n)| n.status == NodeStatus::Online)
            .map(|(id, _)| id.clone())
            .collect()
    }

    pub async fn start_heartbeat_monitor(&self) {
        let nodes = Arc::clone(&self.nodes);
        let config = self.config.clone();
        let local_id = self.config.sentinel_id.clone();
        let event_sender = self.event_sender.clone();

        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_millis(config.heartbeat_interval_ms));
            loop {
                interval.tick().await;
                let now = chrono::Utc::now().timestamp_millis();
                let mut changed_nodes = Vec::new();

                {
                    let mut all_nodes = nodes.write().await;
                    for node in all_nodes.values_mut() {
                        if node.node_id == local_id {
                            node.last_heartbeat = now / 1000;
                            continue;
                        }
                        let elapsed = now - (node.last_heartbeat * 1000);
                        let new_status = if elapsed > config.down_after_ms as i64 * 2 {
                            NodeStatus::ObjectiveOffline
                        } else if elapsed > config.down_after_ms as i64 {
                            NodeStatus::SubjectiveOffline
                        } else {
                            NodeStatus::Online
                        };

                        if node.status != new_status {
                            node.status = new_status;
                            changed_nodes.push(node.node_id.clone());
                        }
                    }
                }

                for node_id in changed_nodes {
                    let _ = event_sender.send(SentinelEvent::NodeStatusChanged {
                        node_id,
                        status: NodeStatus::ObjectiveOffline,
                    });
                }
            }
        });
    }

    pub async fn start_election(&self) -> Option<String> {
        let mut in_progress = self.failover_in_progress.write().await;
        if *in_progress {
            return None;
        }
        *in_progress = true;
        drop(in_progress);

        let mut term = self.current_term.write().await;
        *term += 1;
        let current_term = *term;
        drop(term);

        {
            let mut voted = self.voted_for.write().await;
            *voted = Some(self.config.sentinel_id.clone());
        }

        {
            let mut local_node = self.local_node.write().await;
            local_node.role = SentinelNodeRole::Candidate;
            local_node.term = current_term;
        }

        let nodes = self.nodes.read().await;
        let peer_addresses: Vec<(String, String)> = nodes.iter()
            .filter(|(id, _)| **id != self.config.sentinel_id)
            .map(|(id, n)| (id.clone(), format!("{}:{}", n.address, n.port)))
            .collect();
        let total_nodes = nodes.len() + 1;
        let votes_needed = total_nodes / 2 + 1;
        drop(nodes);

        let mut votes = 1;

        let vote_req = VoteRequest {
            candidate_id: self.config.sentinel_id.clone(),
            term: current_term,
            last_log_index: 0,
            last_log_term: 0,
        };

        let mut vote_futures = Vec::new();
        for (node_id, addr) in &peer_addresses {
            let rpc = self.rpc.clone();
            let req = vote_req.clone();
            let node_id = node_id.clone();
            let addr = addr.clone();

            vote_futures.push(tokio::spawn(async move {
                match rpc.request_vote(&addr, &req).await {
                    Ok(resp) => (node_id, resp.vote_granted, resp.term),
                    Err(e) => {
                        tracing::warn!("Sentinel vote request to {} failed: {}", node_id, e);
                        (node_id, false, 0)
                    }
                }
            }));
        }

        for future in vote_futures {
            if let Ok((node_id, granted, resp_term)) = future.await {
                let _ = self.event_sender.send(SentinelEvent::NodeStatusChanged {
                    node_id,
                    status: if granted { NodeStatus::Online } else { NodeStatus::SubjectiveOffline },
                });

                if resp_term > current_term {
                    let mut term = self.current_term.write().await;
                    *term = resp_term;
                    let mut in_progress = self.failover_in_progress.write().await;
                    *in_progress = false;
                    return None;
                }

                if granted {
                    votes += 1;
                }
            }
        }

        if votes >= votes_needed {
            self.become_leader(current_term).await;
            let mut in_progress = self.failover_in_progress.write().await;
            *in_progress = false;
            return Some(self.config.sentinel_id.clone());
        }

        let mut in_progress = self.failover_in_progress.write().await;
        *in_progress = false;
        None
    }

    async fn become_leader(&self, term: u64) {
        let mut local_node = self.local_node.write().await;
        local_node.role = SentinelNodeRole::Leader;
        local_node.term = term;
        drop(local_node);

        {
            let mut leader_seen = self.leader_last_seen.write().await;
            *leader_seen = Instant::now();
        }

        let _ = self.event_sender.send(SentinelEvent::LeaderElected {
            leader_id: self.config.sentinel_id.clone(),
            term,
        });
    }

    pub async fn detect_and_failover(&self) -> Result<(), String> {
        let is_leader = {
            let local = self.local_node.read().await;
            local.role == SentinelNodeRole::Leader
        };

        if !is_leader {
            return Err("Not the leader".to_string());
        }

        let mut in_progress = self.failover_in_progress.write().await;
        if *in_progress {
            return Err("Failover already in progress".to_string());
        }
        *in_progress = true;
        drop(in_progress);

        let nodes = self.nodes.read().await;
        let mut offline_leaders: Vec<String> = nodes.iter()
            .filter(|(_, n)| {
                n.status == NodeStatus::ObjectiveOffline
                    || n.status == NodeStatus::SubjectiveOffline
            })
            .map(|(id, _)| id.clone())
            .collect();
        drop(nodes);

        if offline_leaders.is_empty() {
            let mut in_progress = self.failover_in_progress.write().await;
            *in_progress = false;
            return Ok(());
        }

        let old_leader = offline_leaders.remove(0);
        let _ = self.event_sender.send(SentinelEvent::FailoverStarted {
            old_leader: old_leader.clone(),
            term: *self.current_term.read().await,
        });

        let online_nodes = self.get_online_nodes().await;
        if online_nodes.is_empty() {
            let _ = self.event_sender.send(SentinelEvent::FailoverAborted {
                reason: "No online nodes available for failover".to_string(),
            });
            let mut in_progress = self.failover_in_progress.write().await;
            *in_progress = false;
            return Err("No online nodes".to_string());
        }

        let new_leader = online_nodes[0].clone();

        {
            let mut nodes = self.nodes.write().await;
            if let Some(node) = nodes.get_mut(&new_leader) {
                node.role = SentinelNodeRole::Leader;
                node.status = NodeStatus::Online;
            }
            nodes.remove(&old_leader);
        }

        let term = *self.current_term.read().await;
        let _ = self.event_sender.send(SentinelEvent::FailoverCompleted {
            old_leader: old_leader.clone(),
            new_leader: new_leader.clone(),
            term,
        });

        let mut in_progress = self.failover_in_progress.write().await;
        *in_progress = false;

        Ok(())
    }

    pub async fn get_leader(&self) -> Option<String> {
        let local = self.local_node.read().await;
        if local.role == SentinelNodeRole::Leader {
            return Some(self.config.sentinel_id.clone());
        }

        let nodes = self.nodes.read().await;
        for (id, node) in nodes.iter() {
            if node.role == SentinelNodeRole::Leader {
                return Some(id.clone());
            }
        }
        None
    }

    pub async fn get_cluster_info(&self) -> SentinelClusterInfo {
        let nodes = self.nodes.read().await;
        let local = self.local_node.read().await;

        let total = nodes.len() + 1;
        let online = nodes.values().filter(|n| n.status == NodeStatus::Online).count()
            + if local.status == NodeStatus::Online { 1 } else { 0 };
        let offline = nodes.values().filter(|n| {
            n.status == NodeStatus::ObjectiveOffline || n.status == NodeStatus::SubjectiveOffline
        }).count();

        SentinelClusterInfo {
            sentinel_id: self.config.sentinel_id.clone(),
            local_role: local.role,
            total_nodes: total,
            online_nodes: online,
            offline_nodes: offline,
            quorum: self.config.quorum,
            current_term: *self.current_term.read().await,
            leader: self.get_leader().await,
            failover_in_progress: *self.failover_in_progress.read().await,
        }
    }

    pub fn event_receiver(&self) -> broadcast::Receiver<SentinelEvent> {
        self.event_sender.subscribe()
    }

    pub fn config(&self) -> &SentinelConfig {
        &self.config
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SentinelClusterInfo {
    pub sentinel_id: String,
    pub local_role: SentinelNodeRole,
    pub total_nodes: usize,
    pub online_nodes: usize,
    pub offline_nodes: usize,
    pub quorum: usize,
    pub current_term: u64,
    pub leader: Option<String>,
    pub failover_in_progress: bool,
}

pub struct SentinelClient {
    sentinel_manager: Arc<SentinelManager>,
    connection_pool: Arc<RwLock<HashMap<String, SentinelConnection>>>,
}

struct SentinelConnection {
    node_id: String,
    connected_at: Instant,
    last_used: Instant,
}

impl SentinelClient {
    pub fn new(sentinel_manager: Arc<SentinelManager>) -> Self {
        Self {
            sentinel_manager,
            connection_pool: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn get_master_address(&self) -> Option<(String, u16)> {
        let leader = self.sentinel_manager.get_leader().await?;
        let nodes = self.sentinel_manager.nodes.read().await;
        nodes.get(&leader).map(|n| (n.address.clone(), n.port))
    }

    pub async fn get_slave_addresses(&self) -> Vec<(String, u16)> {
        let nodes = self.sentinel_manager.nodes.read().await;
        nodes.iter()
            .filter(|(_, n)| n.role == SentinelNodeRole::Follower && n.status == NodeStatus::Online)
            .map(|(_, n)| (n.address.clone(), n.port))
            .collect()
    }

    pub async fn connect_to_node(&self, node_id: &str) -> Result<(), String> {
        let mut pool = self.connection_pool.write().await;
        pool.insert(node_id.to_string(), SentinelConnection {
            node_id: node_id.to_string(),
            connected_at: Instant::now(),
            last_used: Instant::now(),
        });
        Ok(())
    }

    pub async fn disconnect(&self, node_id: &str) {
        let mut pool = self.connection_pool.write().await;
        pool.remove(node_id);
    }

    pub async fn get_active_connections(&self) -> Vec<String> {
        let pool = self.connection_pool.read().await;
        pool.keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_sentinel_config_default() {
        let config = SentinelConfig::default();
        assert_eq!(config.quorum, 2);
        assert_eq!(config.heartbeat_interval_ms, 1000);
        assert_eq!(config.down_after_ms, 3000);
    }

    #[tokio::test]
    async fn test_sentinel_manager_new() {
        let config = SentinelConfig::default();
        let manager = SentinelManager::new(config);
        let info = manager.get_cluster_info().await;
        assert_eq!(info.total_nodes, 1);
        assert_eq!(info.local_role, SentinelNodeRole::Follower);
    }

    #[tokio::test]
    async fn test_register_node() {
        let manager = SentinelManager::new(SentinelConfig::default());
        manager.register_node("node2", "192.168.1.2", 6380).await;

        let info = manager.get_cluster_info().await;
        assert_eq!(info.total_nodes, 2);
    }

    #[tokio::test]
    async fn test_heartbeat_update() {
        let manager = SentinelManager::new(SentinelConfig::default());
        manager.register_node("node2", "192.168.1.2", 6380).await;

        let status = manager.check_node_status("node2").await;
        assert_eq!(status, NodeStatus::Online);

        manager.update_heartbeat("node2").await;
        let status = manager.check_node_status("node2").await;
        assert_eq!(status, NodeStatus::Online);
    }

    #[tokio::test]
    async fn test_election() {
        let manager = SentinelManager::new(SentinelConfig::default());
        let leader = manager.start_election().await;
        assert!(leader.is_some());
        assert_eq!(leader.unwrap(), manager.config().sentinel_id);
    }

    #[tokio::test]
    async fn test_failover_no_offline_nodes() {
        let manager = SentinelManager::new(SentinelConfig::default());
        let result = manager.detect_and_failover().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_sentinel_client() {
        let manager = Arc::new(SentinelManager::new(SentinelConfig::default()));
        let client = SentinelClient::new(manager.clone());

        let master = client.get_master_address().await;
        assert!(master.is_none());

        let slaves = client.get_slave_addresses().await;
        assert!(slaves.is_empty());
    }

    #[tokio::test]
    async fn test_event_broadcast() {
        let manager = SentinelManager::new(SentinelConfig::default());
        let mut rx = manager.event_receiver();

        manager.register_node("node2", "192.168.1.2", 6380).await;

        if let Ok(event) = rx.try_recv() {
            match event {
                SentinelEvent::NodeJoined { node_id, .. } => {
                    assert_eq!(node_id, "node2");
                }
                _ => panic!("Expected NodeJoined event"),
            }
        }
    }
}