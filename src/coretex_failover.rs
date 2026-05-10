//! Fault tolerance and failover mechanisms for CoreTexDB
//! Implements Raft-inspired leader election with actual network communication

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, RwLock, mpsc};
use tokio::time;
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

#[derive(Clone)]
pub struct FailoverConfig {
    pub heartbeat_interval_ms: u64,
    pub election_timeout_ms: u64,
    pub max_retry_attempts: u32,
    pub retry_delay_ms: u64,
    pub health_check_interval_ms: u64,
    pub leader_timeout_ms: u64,
    pub rpc_timeout_ms: u64,
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
            rpc_timeout_ms: 2000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoteRequest {
    pub candidate_id: String,
    pub term: u64,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoteResponse {
    pub voter_id: String,
    pub term: u64,
    pub vote_granted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatRequest {
    pub leader_id: String,
    pub term: u64,
    pub leader_commit: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatResponse {
    pub follower_id: String,
    pub term: u64,
    pub success: bool,
}

#[async_trait::async_trait]
pub trait RaftRpc: Send + Sync {
    async fn request_vote(&self, addr: &str, req: &VoteRequest) -> Result<VoteResponse, String>;
    async fn send_heartbeat(&self, addr: &str, req: &HeartbeatRequest) -> Result<HeartbeatResponse, String>;
}

pub struct HttpRaftRpc {
    client: reqwest::Client,
    timeout_ms: u64,
}

impl HttpRaftRpc {
    pub fn new(timeout_ms: u64) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(timeout_ms))
            .build()
            .unwrap_or_default();
        Self { client, timeout_ms }
    }
}

#[async_trait::async_trait]
impl RaftRpc for HttpRaftRpc {
    async fn request_vote(&self, addr: &str, req: &VoteRequest) -> Result<VoteResponse, String> {
        let url = format!("http://{}/raft/request_vote", addr);
        let resp = self.client
            .post(&url)
            .json(req)
            .send()
            .await
            .map_err(|e| format!("Vote RPC failed to {}: {}", addr, e))?;
        resp.json::<VoteResponse>()
            .await
            .map_err(|e| format!("Vote response parse failed from {}: {}", addr, e))
    }

    async fn send_heartbeat(&self, addr: &str, req: &HeartbeatRequest) -> Result<HeartbeatResponse, String> {
        let url = format!("http://{}/raft/heartbeat", addr);
        let resp = self.client
            .post(&url)
            .json(req)
            .send()
            .await
            .map_err(|e| format!("Heartbeat RPC failed to {}: {}", addr, e))?;
        resp.json::<HeartbeatResponse>()
            .await
            .map_err(|e| format!("Heartbeat response parse failed from {}: {}", addr, e))
    }
}

pub struct FailoverManager {
    config: FailoverConfig,
    nodes: Arc<RwLock<HashMap<String, NodeHealth>>>,
    node_addresses: Arc<RwLock<HashMap<String, String>>>,
    local_node_id: String,
    leader_id: Arc<RwLock<Option<String>>>,
    event_sender: broadcast::Sender<FailoverEvent>,
    current_term: Arc<RwLock<u64>>,
    voted_for: Arc<RwLock<Option<String>>>,
    rpc: Arc<dyn RaftRpc>,
    election_timer: Arc<RwLock<Option<Instant>>>,
    last_leader_heartbeat: Arc<RwLock<Instant>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FailoverEvent {
    LeaderElected { node_id: String, term: u64 },
    NodeFailed { node_id: String },
    NodeRecovered { node_id: String },
    LeaderChanged { old_leader: Option<String>, new_leader: String },
    HealthCheckFailed { node_id: String, reason: String },
    VoteReceived { voter_id: String, granted: bool, term: u64 },
}

impl FailoverManager {
    pub fn new(local_node_id: &str, config: FailoverConfig) -> Self {
        let (event_sender, _) = broadcast::channel(1000);
        let rpc = Arc::new(HttpRaftRpc::new(config.rpc_timeout_ms));

        Self {
            config,
            nodes: Arc::new(RwLock::new(HashMap::new())),
            node_addresses: Arc::new(RwLock::new(HashMap::new())),
            local_node_id: local_node_id.to_string(),
            leader_id: Arc::new(RwLock::new(None)),
            event_sender,
            current_term: Arc::new(RwLock::new(0)),
            voted_for: Arc::new(RwLock::new(None)),
            rpc,
            election_timer: Arc::new(RwLock::new(None)),
            last_leader_heartbeat: Arc::new(RwLock::new(Instant::now())),
        }
    }

    pub fn with_rpc(local_node_id: &str, config: FailoverConfig, rpc: Arc<dyn RaftRpc>) -> Self {
        let (event_sender, _) = broadcast::channel(1000);

        Self {
            config,
            nodes: Arc::new(RwLock::new(HashMap::new())),
            node_addresses: Arc::new(RwLock::new(HashMap::new())),
            local_node_id: local_node_id.to_string(),
            leader_id: Arc::new(RwLock::new(None)),
            event_sender,
            current_term: Arc::new(RwLock::new(0)),
            voted_for: Arc::new(RwLock::new(None)),
            rpc,
            election_timer: Arc::new(RwLock::new(None)),
            last_leader_heartbeat: Arc::new(RwLock::new(Instant::now())),
        }
    }

    pub async fn register_node(&self, node_id: &str, health: NodeHealth) {
        let mut nodes = self.nodes.write().await;
        nodes.insert(node_id.to_string(), health);
    }

    pub async fn register_node_address(&self, node_id: &str, address: &str) {
        let mut addresses = self.node_addresses.write().await;
        addresses.insert(node_id.to_string(), address.to_string());
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

    pub async fn request_vote_rpc(&self, candidate_id: &str, term: u64) -> bool {
        let mut current_term = self.current_term.write().await;
        let mut voted_for = self.voted_for.write().await;

        if term < *current_term {
            return false;
        }

        if term > *current_term {
            *current_term = term;
            *voted_for = None;
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
        drop(term);

        {
            let mut voted = self.voted_for.write().await;
            *voted = Some(self.local_node_id.clone());
        }

        let addresses = self.node_addresses.read().await;
        let peer_addresses: Vec<(String, String)> = addresses.iter()
            .filter(|(id, _)| *id != &self.local_node_id)
            .map(|(id, addr)| (id.clone(), addr.clone()))
            .collect();
        drop(addresses);

        let mut votes = 1;
        let total_nodes = peer_addresses.len() + 1;
        let majority = total_nodes / 2 + 1;

        let vote_req = VoteRequest {
            candidate_id: self.local_node_id.clone(),
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
                        tracing::warn!("Vote request to {} failed: {}", node_id, e);
                        (node_id, false, 0)
                    }
                }
            }));
        }

        for future in vote_futures {
            if let Ok((node_id, granted, resp_term)) = future.await {
                let _ = self.event_sender.send(FailoverEvent::VoteReceived {
                    voter_id: node_id,
                    granted,
                    term: resp_term,
                });

                if resp_term > current_term {
                    let mut term = self.current_term.write().await;
                    *term = resp_term;
                    let mut in_progress = self.election_timer.write().await;
                    *in_progress = None;
                    return None;
                }

                if granted {
                    votes += 1;
                }
            }
        }

        if votes >= majority {
            let mut leader = self.leader_id.write().await;
            *leader = Some(self.local_node_id.clone());

            let _ = self.event_sender.send(FailoverEvent::LeaderElected {
                node_id: self.local_node_id.clone(),
                term: current_term,
            });

            self.start_heartbeat_broadcast().await;

            Some(self.local_node_id.clone())
        } else {
            let mut in_progress = self.election_timer.write().await;
            *in_progress = None;
            None
        }
    }

    async fn start_heartbeat_broadcast(&self) {
        let local_id = self.local_node_id.clone();
        let rpc = self.rpc.clone();
        let node_addresses = self.node_addresses.clone();
        let leader_id = self.leader_id.clone();
        let current_term = self.current_term.clone();
        let config = self.config.clone();
        let event_sender = self.event_sender.clone();

        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_millis(config.heartbeat_interval_ms));
            loop {
                interval.tick().await;

                let is_still_leader = {
                    let leader = leader_id.read().await;
                    leader.as_deref() == Some(&local_id)
                };

                if !is_still_leader {
                    break;
                }

                let term = *current_term.read().await;
                let addresses = node_addresses.read().await;
                let peers: Vec<String> = addresses.iter()
                    .filter(|(id, _)| *id != &local_id)
                    .map(|(_, addr)| addr.clone())
                    .collect();
                drop(addresses);

                let hb_req = HeartbeatRequest {
                    leader_id: local_id.clone(),
                    term,
                    leader_commit: 0,
                };

                for addr in &peers {
                    let rpc = rpc.clone();
                    let req = hb_req.clone();
                    let addr = addr.clone();
                    let event_sender = event_sender.clone();
                    let local_id = local_id.clone();

                    tokio::spawn(async move {
                        match rpc.send_heartbeat(&addr, &req).await {
                            Ok(resp) => {
                                if resp.term > term {
                                    let _ = event_sender.send(FailoverEvent::LeaderChanged {
                                        old_leader: Some(local_id.clone()),
                                        new_leader: format!("unknown_term_{}", resp.term),
                                    });
                                }
                            }
                            Err(e) => {
                                tracing::warn!("Heartbeat to {} failed: {}", addr, e);
                            }
                        }
                    });
                }
            }
        });
    }

    pub async fn handle_heartbeat(&self, req: &HeartbeatRequest) -> HeartbeatResponse {
        let mut term = self.current_term.write().await;

        if req.term > *term {
            *term = req.term;
            let mut voted = self.voted_for.write().await;
            *voted = None;
        }

        if req.term >= *term {
            let mut leader = self.leader_id.write().await;
            *leader = Some(req.leader_id.clone());
            let mut last_hb = self.last_leader_heartbeat.write().await;
            *last_hb = Instant::now();
        }

        HeartbeatResponse {
            follower_id: self.local_node_id.clone(),
            term: *term,
            success: req.term >= *term,
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

    pub async fn promote_to_leader(&self) -> Option<String> {
        let result = self.start_election().await;
        if result.is_some() {
            self.start_heartbeat_broadcast().await;
        }
        result
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

    pub fn rpc_ref(&self) -> Arc<dyn RaftRpc> {
        self.rpc.clone()
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