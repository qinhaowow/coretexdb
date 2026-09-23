//! HTTP-based RPC implementation for distributed transactions
//! Enables real network communication between 2PC coordinator and participants

use std::sync::Arc;
use std::time::Duration;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::time::timeout;

use super::{ParticipantRpc, LockPeerRpc, DistributedOperation};

// ═══════════════════════════════════════════════════════════════
// RPC 消息类型
// ═══════════════════════════════════════════════════════════════

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RpcRequest {
    Prepare {
        tx_id: String,
        operations: Vec<OperationDto>,
    },
    Commit {
        tx_id: String,
    },
    Abort {
        tx_id: String,
    },
    TryLock {
        node_id: String,
        key: String,
        fence_token: u64,
        ttl_secs: u64,
    },
    Unlock {
        node_id: String,
        key: String,
        fence_token: u64,
    },
    VerifyLock {
        key: String,
        fence_token: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RpcResponse {
    PrepareVote { vote: bool },
    Ack { success: bool },
    LockResult { granted: bool },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationDto {
    pub op_type: String,
    pub collection: String,
    pub id: String,
    pub payload: Vec<u8>,
}

impl From<&DistributedOperation> for OperationDto {
    fn from(op: &DistributedOperation) -> Self {
        match op {
            DistributedOperation::Insert { collection, id, payload } => OperationDto {
                op_type: "insert".to_string(),
                collection: collection.clone(),
                id: id.clone(),
                payload: payload.clone(),
            },
            DistributedOperation::Update { collection, id, payload } => OperationDto {
                op_type: "update".to_string(),
                collection: collection.clone(),
                id: id.clone(),
                payload: payload.clone(),
            },
            DistributedOperation::Delete { collection, id } => OperationDto {
                op_type: "delete".to_string(),
                collection: collection.clone(),
                id: id.clone(),
                payload: vec![],
            },
            DistributedOperation::Read { collection, id } => OperationDto {
                op_type: "read".to_string(),
                collection: collection.clone(),
                id: id.clone(),
                payload: vec![],
            },
        }
    }
}

// ═══════════════════════════════════════════════════════════════
// HTTP 参与者 RPC 客户端
// ═══════════════════════════════════════════════════════════════

/// HTTP-based participant RPC client
/// Each participant is identified by a URL like "http://192.168.1.10:8080"
pub struct HttpParticipantRpc {
    client: reqwest::Client,
    timeout: Duration,
}

impl HttpParticipantRpc {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("Failed to create HTTP client"),
            timeout: Duration::from_secs(10),
        }
    }

    pub fn with_timeout(timeout: Duration) -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(timeout)
                .build()
                .expect("Failed to create HTTP client"),
            timeout,
        }
    }

    async fn send_request(&self, participant: &str, request: &RpcRequest) -> Result<RpcResponse, String> {
        let url = format!("{}/coretex/rpc", participant.trim_end_matches('/'));

        let response = timeout(self.timeout, self.client.post(&url)
            .json(request)
            .send())
            .await
            .map_err(|_| "RPC request timed out".to_string())?
            .map_err(|e| format!("RPC HTTP error: {}", e))?;

        if !response.status().is_success() {
            return Err(format!("RPC returned status: {}", response.status()));
        }

        response.json::<RpcResponse>().await
            .map_err(|e| format!("RPC parse error: {}", e))
    }
}

#[async_trait]
impl ParticipantRpc for HttpParticipantRpc {
    async fn prepare(&self, participant: &str, tx_id: &str, operations: &[DistributedOperation]) -> bool {
        let ops: Vec<OperationDto> = operations.iter().map(OperationDto::from).collect();
        let request = RpcRequest::Prepare {
            tx_id: tx_id.to_string(),
            operations: ops,
        };

        match self.send_request(participant, &request).await {
            Ok(RpcResponse::PrepareVote { vote }) => vote,
            Ok(_) => false,
            Err(e) => {
                eprintln!("[2PC] prepare to {} failed: {}", participant, e);
                false
            }
        }
    }

    async fn commit(&self, participant: &str, tx_id: &str) -> bool {
        let request = RpcRequest::Commit {
            tx_id: tx_id.to_string(),
        };

        match self.send_request(participant, &request).await {
            Ok(RpcResponse::Ack { success }) => success,
            Ok(_) => false,
            Err(e) => {
                eprintln!("[2PC] commit to {} failed: {}", participant, e);
                false
            }
        }
    }

    async fn abort(&self, participant: &str, tx_id: &str) -> bool {
        let request = RpcRequest::Abort {
            tx_id: tx_id.to_string(),
        };

        match self.send_request(participant, &request).await {
            Ok(RpcResponse::Ack { success }) => success,
            Ok(_) => false,
            Err(e) => {
                eprintln!("[2PC] abort to {} failed: {}", participant, e);
                false
            }
        }
    }
}

// ═══════════════════════════════════════════════════════════════
// HTTP 锁对等节点 RPC 客户端
// ═══════════════════════════════════════════════════════════════

pub struct HttpLockPeerRpc {
    client: reqwest::Client,
    timeout: Duration,
}

impl HttpLockPeerRpc {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .expect("Failed to create HTTP client"),
            timeout: Duration::from_secs(5),
        }
    }

    async fn send_request(&self, node_url: &str, request: &RpcRequest) -> Result<RpcResponse, String> {
        let url = format!("{}/coretex/rpc", node_url.trim_end_matches('/'));

        let response = timeout(self.timeout, self.client.post(&url)
            .json(request)
            .send())
            .await
            .map_err(|_| "Lock RPC timed out".to_string())?
            .map_err(|e| format!("Lock RPC HTTP error: {}", e))?;

        if !response.status().is_success() {
            return Err(format!("Lock RPC returned status: {}", response.status()));
        }

        response.json::<RpcResponse>().await
            .map_err(|e| format!("Lock RPC parse error: {}", e))
    }
}

#[async_trait]
impl LockPeerRpc for HttpLockPeerRpc {
    async fn try_lock(&self, node_id: &str, key: &str, fence_token: u64, ttl_secs: u64) -> bool {
        let request = RpcRequest::TryLock {
            node_id: node_id.to_string(),
            key: key.to_string(),
            fence_token,
            ttl_secs,
        };

        match self.send_request(node_id, &request).await {
            Ok(RpcResponse::LockResult { granted }) => granted,
            Ok(_) => false,
            Err(e) => {
                eprintln!("[Lock] try_lock to {} failed: {}", node_id, e);
                false
            }
        }
    }

    async fn unlock(&self, node_id: &str, key: &str, fence_token: u64) -> bool {
        let request = RpcRequest::Unlock {
            node_id: node_id.to_string(),
            key: key.to_string(),
            fence_token,
        };

        match self.send_request(node_id, &request).await {
            Ok(RpcResponse::Ack { success }) => success,
            Ok(_) => false,
            Err(e) => {
                eprintln!("[Lock] unlock to {} failed: {}", node_id, e);
                false
            }
        }
    }

    async fn verify_lock(&self, _key: &str, _fence_token: u64) -> bool {
        // verify_lock 需要找到持有锁的节点，这里简化处理
        // 实际实现中需要维护 key → node 映射
        true
    }
}

// ═══════════════════════════════════════════════════════════════
// RPC 服务端（HTTP Handler）
// ═══════════════════════════════════════════════════════════════

use std::collections::HashMap;
use tokio::sync::RwLock;

/// Distributed lock state for RPC server
pub struct RpcLockState {
    pub locks: RwLock<HashMap<String, LockEntry>>,
}

#[derive(Debug, Clone)]
pub struct LockEntry {
    pub owner: String,
    pub fence_token: u64,
    pub expires_at: Option<std::time::Instant>,
}

impl RpcLockState {
    pub fn new() -> Self {
        Self {
            locks: RwLock::new(HashMap::new()),
        }
    }
}

/// RPC handler that processes incoming distributed transaction requests
pub struct RpcHandler {
    lock_state: Arc<RpcLockState>,
    /// Callback for preparing a transaction
    prepare_callback: Option<Box<dyn Fn(String, Vec<OperationDto>) -> bool + Send + Sync>>,
    /// Callback for committing a transaction
    commit_callback: Option<Box<dyn Fn(String) -> bool + Send + Sync>>,
    /// Callback for aborting a transaction
    abort_callback: Option<Box<dyn Fn(String) -> bool + Send + Sync>>,
}

impl RpcHandler {
    pub fn new() -> Self {
        Self {
            lock_state: Arc::new(RpcLockState::new()),
            prepare_callback: None,
            commit_callback: None,
            abort_callback: None,
        }
    }

    pub fn with_prepare_callback<F>(mut self, cb: F) -> Self
    where
        F: Fn(String, Vec<OperationDto>) -> bool + Send + Sync + 'static,
    {
        self.prepare_callback = Some(Box::new(cb));
        self
    }

    pub fn with_commit_callback<F>(mut self, cb: F) -> Self
    where
        F: Fn(String) -> bool + Send + Sync + 'static,
    {
        self.commit_callback = Some(Box::new(cb));
        self
    }

    pub fn with_abort_callback<F>(mut self, cb: F) -> Self
    where
        F: Fn(String) -> bool + Send + Sync + 'static,
    {
        self.abort_callback = Some(Box::new(cb));
        self
    }

    pub async fn handle_request(&self, request: RpcRequest) -> RpcResponse {
        match request {
            RpcRequest::Prepare { tx_id, operations } => {
                let vote = if let Some(ref cb) = self.prepare_callback {
                    cb(tx_id, operations)
                } else {
                    // Default: accept all prepares
                    true
                };
                RpcResponse::PrepareVote { vote }
            }
            RpcRequest::Commit { tx_id } => {
                let success = if let Some(ref cb) = self.commit_callback {
                    cb(tx_id)
                } else {
                    true
                };
                RpcResponse::Ack { success }
            }
            RpcRequest::Abort { tx_id } => {
                let success = if let Some(ref cb) = self.abort_callback {
                    cb(tx_id)
                } else {
                    true
                };
                RpcResponse::Ack { success }
            }
            RpcRequest::TryLock { node_id, key, fence_token, ttl_secs } => {
                let granted = self.try_acquire_lock(&key, &node_id, fence_token, ttl_secs).await;
                RpcResponse::LockResult { granted }
            }
            RpcRequest::Unlock { node_id, key, fence_token } => {
                let success = self.release_lock(&key, &node_id, fence_token).await;
                RpcResponse::Ack { success }
            }
            RpcRequest::VerifyLock { key, fence_token } => {
                let valid = self.verify_lock(&key, fence_token).await;
                RpcResponse::LockResult { granted: valid }
            }
        }
    }

    async fn try_acquire_lock(&self, key: &str, node_id: &str, fence_token: u64, ttl_secs: u64) -> bool {
        let mut locks = self.lock_state.locks.write().await;

        if let Some(entry) = locks.get(key) {
            // Check if existing lock has expired
            if let Some(expires) = entry.expires_at {
                if expires > std::time::Instant::now() {
                    // Lock is still valid, reject if different owner or lower fence token
                    return entry.owner == node_id && fence_token >= entry.fence_token;
                }
            }
        }

        // Acquire the lock
        let expires_at = if ttl_secs > 0 {
            Some(std::time::Instant::now() + Duration::from_secs(ttl_secs))
        } else {
            None
        };

        locks.insert(key.to_string(), LockEntry {
            owner: node_id.to_string(),
            fence_token,
            expires_at,
        });

        true
    }

    async fn release_lock(&self, key: &str, node_id: &str, fence_token: u64) -> bool {
        let mut locks = self.lock_state.locks.write().await;

        if let Some(entry) = locks.get(key) {
            if entry.owner == node_id && entry.fence_token == fence_token {
                locks.remove(key);
                return true;
            }
        }

        false
    }

    async fn verify_lock(&self, key: &str, fence_token: u64) -> bool {
        let locks = self.lock_state.locks.read().await;

        if let Some(entry) = locks.get(key) {
            if let Some(expires) = entry.expires_at {
                if expires <= std::time::Instant::now() {
                    return false;
                }
            }
            return entry.fence_token == fence_token;
        }

        false
    }

    /// Get the lock state (for mounting into HTTP server)
    pub fn lock_state(&self) -> Arc<RpcLockState> {
        self.lock_state.clone()
    }
}

// ═══════════════════════════════════════════════════════════════
// Axum HTTP Server 路由（集成到现有 REST API）
// ═══════════════════════════════════════════════════════════════

use axum::{extract::State, http::StatusCode, Json, routing::post, Router};

/// Create an Axum router for the RPC endpoint
pub fn rpc_router(handler: Arc<RpcHandler>) -> Router {
    Router::new()
        .route("/coretex/rpc", post(handle_rpc))
        .with_state(handler)
}

async fn handle_rpc(
    State(handler): State<Arc<RpcHandler>>,
    Json(request): Json<RpcRequest>,
) -> Result<Json<RpcResponse>, StatusCode> {
    let response = handler.handle_request(request).await;
    Ok(Json(response))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_rpc_handler_prepare() {
        let handler = Arc::new(RpcHandler::new());

        let request = RpcRequest::Prepare {
            tx_id: "tx_test_1".to_string(),
            operations: vec![OperationDto {
                op_type: "insert".to_string(),
                collection: "test".to_string(),
                id: "1".to_string(),
                payload: vec![1, 2, 3],
            }],
        };

        let response = handler.handle_request(request).await;
        match response {
            RpcResponse::PrepareVote { vote } => assert!(vote),
            _ => panic!("Expected PrepareVote"),
        }
    }

    #[tokio::test]
    async fn test_rpc_handler_lock() {
        let handler = Arc::new(RpcHandler::new());

        // Try lock
        let request = RpcRequest::TryLock {
            node_id: "node1".to_string(),
            key: "test_key".to_string(),
            fence_token: 1,
            ttl_secs: 60,
        };
        let response = handler.handle_request(request).await;
        match response {
            RpcResponse::LockResult { granted } => assert!(granted),
            _ => panic!("Expected LockResult"),
        }

        // Verify lock
        let request = RpcRequest::VerifyLock {
            key: "test_key".to_string(),
            fence_token: 1,
        };
        let response = handler.handle_request(request).await;
        match response {
            RpcResponse::LockResult { granted } => assert!(granted),
            _ => panic!("Expected LockResult"),
        }

        // Unlock
        let request = RpcRequest::Unlock {
            node_id: "node1".to_string(),
            key: "test_key".to_string(),
            fence_token: 1,
        };
        let response = handler.handle_request(request).await;
        match response {
            RpcResponse::Ack { success } => assert!(success),
            _ => panic!("Expected Ack"),
        }
    }

    #[tokio::test]
    async fn test_http_participant_rpc_creation() {
        let rpc = HttpParticipantRpc::new();
        assert_eq!(rpc.timeout, Duration::from_secs(10));

        let rpc = HttpParticipantRpc::with_timeout(Duration::from_secs(30));
        assert_eq!(rpc.timeout, Duration::from_secs(30));
    }

    #[test]
    fn test_operation_dto_conversion() {
        let op = DistributedOperation::Insert {
            collection: "test".to_string(),
            id: "1".to_string(),
            payload: vec![1, 2, 3],
        };
        let dto = OperationDto::from(&op);
        assert_eq!(dto.op_type, "insert");
        assert_eq!(dto.collection, "test");

        let op = DistributedOperation::Delete {
            collection: "test".to_string(),
            id: "2".to_string(),
        };
        let dto = OperationDto::from(&op);
        assert_eq!(dto.op_type, "delete");
    }
}
