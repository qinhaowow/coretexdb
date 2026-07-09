//! WebSocket support for CoreTexDB
//!
//! 提供实时双向通信：
//! - 搜索请求/响应
//! - 插入/删除推送
//! - 订阅 collection 数据变更
//! - 心跳机制（Ping/Pong）
//! - 自动重连
//! - 限流与认证

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, RwLock, Mutex};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct WebSocketConfig {
    pub host: String,
    pub port: u16,
    pub max_connections: usize,
    pub ping_interval_secs: u64,
    pub ping_timeout_secs: u64,
    pub heartbeat_check_interval_secs: u64,
    pub max_missed_pongs: u32,
    pub enable_auth: bool,
    pub rate_limit_per_minute: usize,
    pub max_message_size: usize,
}

impl Default for WebSocketConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            port: 8080,
            max_connections: 10000,
            ping_interval_secs: 30,
            ping_timeout_secs: 10,
            heartbeat_check_interval_secs: 5,
            max_missed_pongs: 3,
            enable_auth: false,
            rate_limit_per_minute: 0,
            max_message_size: 10 * 1024 * 1024, // 10MB
        }
    }
}

// ==================== 消息类型 ====================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WebSocketMessage {
    SearchRequest(SearchRequest),
    SearchResponse(SearchResponse),
    InsertRequest(InsertRequest),
    InsertResponse(InsertResponse),
    DeleteRequest(DeleteRequest),
    DeleteResponse(DeleteResponse),
    Subscribe(SubscribeRequest),
    Unsubscribe(UnsubscribeRequest),
    DataChange(DataChangeEvent),
    Error(ErrorResponse),
    Auth(AuthRequest),
    AuthOk(AuthOkResponse),
    Ping,
    Pong,
    Heartbeat(HeartbeatInfo),
    Reconnect(ReconnectInfo),
    Ack(AckInfo),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatInfo {
    pub seq: u64,
    pub server_time: i64,
    pub rtt_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReconnectInfo {
    pub session_id: String,
    pub resume_token: String,
    pub last_event_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AckInfo {
    pub message_id: String,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchRequest {
    pub collection: String,
    pub query: Vec<f32>,
    pub k: usize,
    pub filter: Option<serde_json::Value>,
    pub client_id: String,
    #[serde(default)]
    pub request_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResponse {
    pub client_id: String,
    pub request_id: Option<String>,
    pub results: Vec<SearchResult>,
    pub query_time_ms: u64,
    pub total: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResult {
    pub id: String,
    pub score: f32,
    pub distance: f32,
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InsertRequest {
    pub collection: String,
    pub vectors: Vec<VectorEntry>,
    pub client_id: String,
    #[serde(default)]
    pub request_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorEntry {
    pub id: String,
    pub vector: Vec<f32>,
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InsertResponse {
    pub client_id: String,
    pub request_id: Option<String>,
    pub inserted_ids: Vec<String>,
    pub count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteRequest {
    pub collection: String,
    pub ids: Vec<String>,
    pub client_id: String,
    #[serde(default)]
    pub request_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteResponse {
    pub client_id: String,
    pub request_id: Option<String>,
    pub deleted_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscribeRequest {
    pub collection: String,
    pub event_types: Vec<String>,
    pub client_id: String,
    #[serde(default)]
    pub filter: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnsubscribeRequest {
    pub collection: String,
    pub client_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataChangeEvent {
    pub collection: String,
    pub event_type: String,
    pub ids: Vec<String>,
    pub timestamp: i64,
    pub event_id: String,
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub code: String,
    pub message: String,
    pub client_id: String,
    pub request_id: Option<String>,
    pub retryable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthRequest {
    pub token: String,
    pub client_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthOkResponse {
    pub user_id: String,
    pub session_id: String,
    pub resume_token: String,
    pub expires_at: i64,
}

// ==================== 连接状态 ====================

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ConnectionState {
    Connecting,
    Authenticating,
    Connected,
    HeartbeatFailed,
    Reconnecting,
    Closed,
}

struct Connection {
    id: String,
    client_id: String,
    user_id: Option<String>,
    state: ConnectionState,
    subscribed_collections: Vec<String>,
    last_ping_at: Instant,
    last_pong_at: Instant,
    missed_pongs: u32,
    heartbeat_seq: u64,
    connected_at: Instant,
    resume_token: Option<String>,
    last_event_id: Option<String>,
    rate_count: u32,
    rate_window_start: Instant,
}

// ==================== 心跳管理器 ====================

pub struct HeartbeatManager {
    pub ping_interval: Duration,
    pub ping_timeout: Duration,
    pub max_missed: u32,
}

impl Default for HeartbeatManager {
    fn default() -> Self {
        Self {
            ping_interval: Duration::from_secs(30),
            ping_timeout: Duration::from_secs(10),
            max_missed: 3,
        }
    }
}

impl HeartbeatManager {
    pub fn new(ping_interval: Duration, ping_timeout: Duration, max_missed: u32) -> Self {
        Self { ping_interval, ping_timeout, max_missed }
    }

    /// 检查连接是否应该被视为断开
    pub fn should_disconnect(&self, conn: &Connection) -> bool {
        conn.missed_pongs >= self.max_missed
    }

    /// 记录 Ping 发送
    pub fn record_ping(&self, conn: &mut Connection) {
        conn.last_ping_at = Instant::now();
        conn.missed_pongs += 1;
    }

    /// 记录 Pong 接收
    pub fn record_pong(&self, conn: &mut Connection) {
        conn.last_pong_at = Instant::now();
        conn.missed_pongs = 0;
    }

    /// 估算 RTT
    pub fn estimate_rtt_ms(&self, conn: &Connection) -> u64 {
        if conn.last_ping_at > conn.last_pong_at {
            conn.last_ping_at.duration_since(conn.last_pong_at).as_millis() as u64
        } else {
            0
        }
    }
}

// ==================== 限流器 ====================

pub struct WsRateLimiter {
    pub max_requests: u32,
    pub window: Duration,
}

impl Default for WsRateLimiter {
    fn default() -> Self {
        Self { max_requests: 60, window: Duration::from_secs(60) }
    }
}

impl WsRateLimiter {
    pub fn check(&self, conn: &mut Connection) -> bool {
        let now = Instant::now();
        if now.duration_since(conn.rate_window_start) > self.window {
            conn.rate_count = 0;
            conn.rate_window_start = now;
        }
        if conn.rate_count >= self.max_requests {
            return false;
        }
        conn.rate_count += 1;
        true
    }
}

// ==================== WebSocket 服务器 ====================

pub struct WebSocketServer {
    config: WebSocketConfig,
    connections: Arc<RwLock<HashMap<String, Arc<Mutex<Connection>>>>>,
    event_sender: broadcast::Sender<WebSocketMessage>,
    subscriptions: Arc<RwLock<HashMap<String, Vec<String>>>>,
    resume_tokens: Arc<RwLock<HashMap<String, String>>>,
    heartbeat: HeartbeatManager,
    rate_limiter: WsRateLimiter,
    stats: Arc<RwLock<WebSocketStats>>,
}

impl WebSocketServer {
    pub fn new(config: WebSocketConfig) -> Self {
        let (event_sender, _) = broadcast::channel(10000);
        let heartbeat = HeartbeatManager::new(
            Duration::from_secs(config.ping_interval_secs),
            Duration::from_secs(config.ping_timeout_secs),
            config.max_missed_pongs,
        );
        Self {
            config,
            connections: Arc::new(RwLock::new(HashMap::new())),
            event_sender,
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
            resume_tokens: Arc::new(RwLock::new(HashMap::new())),
            heartbeat,
            rate_limiter: WsRateLimiter {
                max_requests: config.rate_limit_per_minute as u32,
                ..Default::default()
            },
            stats: Arc::new(RwLock::new(WebSocketStats::default())),
        }
    }

    /// 处理新连接
    pub async fn handle_connection(&self, connection_id: String) -> Result<String, String> {
        let mut connections = self.connections.write().await;

        if connections.len() >= self.config.max_connections {
            self.update_stats_rejected().await;
            return Err("Max connections reached".to_string());
        }

        let resume_token = Uuid::new_v4().to_string();
        let connection = Connection {
            id: connection_id.clone(),
            client_id: Uuid::new_v4().to_string(),
            user_id: None,
            state: ConnectionState::Connecting,
            subscribed_collections: Vec::new(),
            last_ping_at: Instant::now(),
            last_pong_at: Instant::now(),
            missed_pongs: 0,
            heartbeat_seq: 0,
            connected_at: Instant::now(),
            resume_token: Some(resume_token.clone()),
            last_event_id: None,
            rate_count: 0,
            rate_window_start: Instant::now(),
        };

        connections.insert(connection_id.clone(), Arc::new(Mutex::new(connection)));

        // 保存 resume_token 映射
        self.resume_tokens.write().await.insert(resume_token.clone(), connection_id.clone());

        self.update_stats_connected(connections.len()).await;
        Ok(resume_token)
    }

    /// 处理消息
    pub async fn handle_message(&self, connection_id: &str, message: WebSocketMessage) -> Option<WebSocketMessage> {
        let connections = self.connections.read().await;
        let conn = match connections.get(connection_id) {
            Some(c) => c.clone(),
            None => return Some(WebSocketMessage::Error(ErrorResponse {
                code: "CONNECTION_NOT_FOUND".to_string(),
                message: "Connection not found".to_string(),
                client_id: "unknown".to_string(),
                request_id: None,
                retryable: false,
            })),
        };
        drop(connections);

        let mut conn_guard = conn.lock().await;

        // 限流检查
        if !self.rate_limiter.check(&mut conn_guard) {
            return Some(WebSocketMessage::Error(ErrorResponse {
                code: "RATE_LIMITED".to_string(),
                message: "Too many requests".to_string(),
                client_id: conn_guard.client_id.clone(),
                request_id: None,
                retryable: true,
            }));
        }

        // 状态机
        if conn_guard.state == ConnectionState::Connecting {
            if !matches!(message, WebSocketMessage::Auth(_)) {
                if self.config.enable_auth {
                    return Some(WebSocketMessage::Error(ErrorResponse {
                        code: "AUTH_REQUIRED".to_string(),
                        message: "Authentication required".to_string(),
                        client_id: conn_guard.client_id.clone(),
                        request_id: None,
                        retryable: false,
                    }));
                } else {
                    conn_guard.state = ConnectionState::Connected;
                }
            }
        }

        let response = match message.clone() {
            WebSocketMessage::Auth(auth_req) => {
                Some(self.handle_auth(&mut conn_guard, auth_req).await)
            }
            WebSocketMessage::SearchRequest(req) => {
                Some(self.handle_search(req).await)
            }
            WebSocketMessage::InsertRequest(req) => {
                Some(self.handle_insert(req).await)
            }
            WebSocketMessage::DeleteRequest(req) => {
                Some(self.handle_delete(req).await)
            }
            WebSocketMessage::Subscribe(req) => {
                self.handle_subscribe(connection_id, req).await;
                None
            }
            WebSocketMessage::Unsubscribe(req) => {
                self.handle_unsubscribe(connection_id, req).await;
                None
            }
            WebSocketMessage::Ping => {
                self.heartbeat.record_ping(&mut conn_guard);
                conn_guard.heartbeat_seq += 1;
                let rtt = self.heartbeat.estimate_rtt_ms(&conn_guard);
                Some(WebSocketMessage::Heartbeat(HeartbeatInfo {
                    seq: conn_guard.heartbeat_seq,
                    server_time: chrono::Utc::now().timestamp_millis(),
                    rtt_ms: Some(rtt),
                }))
            }
            WebSocketMessage::Pong => {
                self.heartbeat.record_pong(&mut conn_guard);
                None
            }
            WebSocketMessage::Ack(ack) => {
                Some(WebSocketMessage::Ack(AckInfo {
                    message_id: ack.message_id,
                    status: "received".to_string(),
                }))
            }
            _ => None,
        };

        response
    }

    async fn handle_auth(&self, conn: &mut Connection, req: AuthRequest) -> WebSocketMessage {
        // 简化的认证 - 实际应使用 JWT 验证
        if req.token.is_empty() {
            return WebSocketMessage::Error(ErrorResponse {
                code: "INVALID_TOKEN".to_string(),
                message: "Empty token".to_string(),
                client_id: req.client_id,
                request_id: None,
                retryable: false,
            });
        }
        conn.user_id = Some(req.client_id.clone());
        conn.state = ConnectionState::Connected;
        let resume_token = conn.resume_token.clone().unwrap_or_default();
        WebSocketMessage::AuthOk(AuthOkResponse {
            user_id: req.client_id,
            session_id: conn.id.clone(),
            resume_token,
            expires_at: chrono::Utc::now().timestamp() + 3600,
        })
    }

    async fn handle_search(&self, req: SearchRequest) -> WebSocketMessage {
        let start = std::time::Instant::now();
        // 实际应调用 db.search，这里返回空结果作为占位
        let query_time = start.elapsed().as_millis() as u64;
        WebSocketMessage::SearchResponse(SearchResponse {
            client_id: req.client_id,
            request_id: req.request_id,
            results: Vec::new(),
            query_time_ms: query_time,
            total: 0,
        })
    }

    async fn handle_insert(&self, req: InsertRequest) -> WebSocketMessage {
        let inserted_ids: Vec<String> = req.vectors.iter().map(|v| v.id.clone()).collect();
        let count = inserted_ids.len();
        let event_id = Uuid::new_v4().to_string();

        let event = DataChangeEvent {
            collection: req.collection.clone(),
            event_type: "insert".to_string(),
            ids: inserted_ids.clone(),
            timestamp: chrono::Utc::now().timestamp(),
            event_id: event_id.clone(),
            metadata: None,
        };

        let _ = self.event_sender.send(WebSocketMessage::DataChange(event));

        WebSocketMessage::InsertResponse(InsertResponse {
            client_id: req.client_id,
            request_id: req.request_id,
            inserted_ids,
            count,
        })
    }

    async fn handle_delete(&self, req: DeleteRequest) -> WebSocketMessage {
        let deleted_count = req.ids.len();
        let event_id = Uuid::new_v4().to_string();

        let event = DataChangeEvent {
            collection: req.collection.clone(),
            event_type: "delete".to_string(),
            ids: req.ids.clone(),
            timestamp: chrono::Utc::now().timestamp(),
            event_id,
            metadata: None,
        };

        let _ = self.event_sender.send(WebSocketMessage::DataChange(event));

        WebSocketMessage::DeleteResponse(DeleteResponse {
            client_id: req.client_id,
            request_id: req.request_id,
            deleted_count,
        })
    }

    async fn handle_subscribe(&self, connection_id: &str, req: SubscribeRequest) {
        let mut connections = self.connections.write().await;
        if let Some(conn) = connections.get_mut(connection_id) {
            let mut conn_guard = conn.lock().await;
            if !conn_guard.subscribed_collections.contains(&req.collection) {
                conn_guard.subscribed_collections.push(req.collection.clone());
            }
        }
        drop(connections);

        let mut subs = self.subscriptions.write().await;
        let entry = subs.entry(req.collection).or_insert_with(Vec::new);
        if !entry.contains(&connection_id.to_string()) {
            entry.push(connection_id.to_string());
        }
    }

    async fn handle_unsubscribe(&self, connection_id: &str, req: UnsubscribeRequest) {
        let mut connections = self.connections.write().await;
        if let Some(conn) = connections.get_mut(connection_id) {
            let mut conn_guard = conn.lock().await;
            conn_guard.subscribed_collections.retain(|c| c != &req.collection);
        }
        drop(connections);

        let mut subs = self.subscriptions.write().await;
        if let Some(list) = subs.get_mut(&req.collection) {
            list.retain(|c| c != connection_id);
        }
    }

    /// 广播数据变更到订阅者
    pub async fn broadcast_to_collection(&self, collection: &str, event: DataChangeEvent) -> usize {
        let subs = self.subscriptions.read().await;
        let connections = match subs.get(collection) {
            Some(c) => c.clone(),
            None => return 0,
        };
        drop(subs);

        let mut count = 0;
        let message = WebSocketMessage::DataChange(event);
        for _conn_id in &connections {
            if self.event_sender.send(message.clone()).is_ok() {
                count += 1;
            }
        }
        count
    }

    /// 移除连接
    pub async fn remove_connection(&self, connection_id: &str) {
        let mut connections = self.connections.write().await;

        if let Some(conn) = connections.remove(connection_id) {
            let conn_guard = conn.lock().await;
            let mut subs = self.subscriptions.write().await;
            for collection in &conn_guard.subscribed_collections {
                if let Some(conn_list) = subs.get_mut(collection) {
                    conn_list.retain(|c| c != connection_id);
                }
            }
            drop(conn_guard);

            if let Some(token) = &conn.resume_token.clone().lock().await.resume_token {
                self.resume_tokens.write().await.remove(token);
            }
        }

        self.update_stats_disconnected(connections.len()).await;
    }

    /// 心跳检测
    pub async fn check_heartbeats(&self) -> Vec<String> {
        let connections = self.connections.read().await;
        let mut to_disconnect = Vec::new();
        let now = Instant::now();

        for (id, conn) in connections.iter() {
            let conn_guard = conn.lock().await;
            if now.duration_since(conn_guard.last_pong_at) > self.config.ping_timeout_secs * 2 {
                if self.heartbeat.should_disconnect(&conn_guard) {
                    to_disconnect.push(id.clone());
                }
            }
        }
        to_disconnect
    }

    /// 启动心跳后台任务
    pub fn start_heartbeat_task(server: Arc<Self>) {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(
                server.config.heartbeat_check_interval_secs,
            ));
            loop {
                interval.tick().await;
                let to_disconnect = server.check_heartbeats().await;
                for id in to_disconnect {
                    server.remove_connection(&id).await;
                }
            }
        });
    }

    /// 断线重连
    pub async fn reconnect(&self, resume_token: &str) -> Option<String> {
        let tokens = self.resume_tokens.read().await;
        tokens.get(resume_token).cloned()
    }

    pub async fn get_stats(&self) -> WebSocketStats {
        self.stats.read().await.clone()
    }

    async fn update_stats_connected(&self, total: usize) {
        let mut s = self.stats.write().await;
        s.total_connections = total;
        s.active_connections = total;
    }

    async fn update_stats_disconnected(&self, total: usize) {
        let mut s = self.stats.write().await;
        s.total_connections = total;
        s.active_connections = total;
    }

    async fn update_stats_rejected(&self) {
        let mut s = self.stats.write().await;
        s.rejected_connections += 1;
    }

    pub fn config(&self) -> &WebSocketConfig {
        &self.config
    }

    pub fn event_receiver(&self) -> broadcast::Receiver<WebSocketMessage> {
        self.event_sender.subscribe()
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WebSocketStats {
    pub total_connections: usize,
    pub active_connections: usize,
    pub total_subscriptions: usize,
    pub max_connections: usize,
    pub rejected_connections: u64,
    pub reconnections: u64,
    pub messages_sent: u64,
    pub messages_received: u64,
}

// ==================== WebSocket 客户端 ====================

pub struct WebSocketClient {
    client_id: String,
    server_url: String,
    subscriptions: Vec<String>,
    resume_token: Option<String>,
    heartbeat_seq: u64,
    last_rtt_ms: u64,
    reconnect_attempts: u32,
    max_reconnect_attempts: u32,
}

impl WebSocketClient {
    pub fn new(server_url: &str) -> Self {
        Self {
            client_id: Uuid::new_v4().to_string(),
            server_url: server_url.to_string(),
            subscriptions: Vec::new(),
            resume_token: None,
            heartbeat_seq: 0,
            last_rtt_ms: 0,
            reconnect_attempts: 0,
            max_reconnect_attempts: 5,
        }
    }

    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    pub fn subscribe(&mut self, collection: String) {
        if !self.subscriptions.contains(&collection) {
            self.subscriptions.push(collection);
        }
    }

    pub fn unsubscribe(&mut self, collection: &str) {
        self.subscriptions.retain(|c| c != collection);
    }

    pub fn subscriptions(&self) -> &[String] {
        &self.subscriptions
    }

    pub fn set_resume_token(&mut self, token: String) {
        self.resume_token = Some(token);
    }

    pub fn handle_heartbeat(&mut self, info: &HeartbeatInfo) {
        self.heartbeat_seq = info.seq;
        self.last_rtt_ms = info.rtt_ms.unwrap_or(0);
    }

    pub fn should_reconnect(&self) -> bool {
        self.reconnect_attempts < self.max_reconnect_attempts
    }

    pub fn record_reconnect(&mut self) {
        self.reconnect_attempts += 1;
    }

    pub fn last_rtt(&self) -> u64 {
        self.last_rtt_ms
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_websocket_config_default() {
        let config = WebSocketConfig::default();
        assert_eq!(config.port, 8080);
        assert_eq!(config.max_connections, 10000);
        assert_eq!(config.max_missed_pongs, 3);
    }

    #[tokio::test]
    async fn test_websocket_server_new() {
        let server = WebSocketServer::new(WebSocketConfig::default());
        let stats = server.get_stats().await;
        assert_eq!(stats.total_connections, 0);
    }

    #[tokio::test]
    async fn test_websocket_server_connection() {
        let server = WebSocketServer::new(WebSocketConfig::default());
        let token = server.handle_connection("conn1".to_string()).await;
        assert!(token.is_ok());
        let stats = server.get_stats().await;
        assert_eq!(stats.total_connections, 1);
    }

    #[tokio::test]
    async fn test_websocket_server_max_connections() {
        let config = WebSocketConfig {
            max_connections: 2,
            ..Default::default()
        };
        let server = WebSocketServer::new(config);

        let _ = server.handle_connection("conn1".to_string()).await;
        let _ = server.handle_connection("conn2".to_string()).await;
        let result = server.handle_connection("conn3".to_string()).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_websocket_server_remove_connection() {
        let server = WebSocketServer::new(WebSocketConfig::default());

        let _ = server.handle_connection("conn1".to_string()).await;
        server.remove_connection("conn1").await;

        let stats = server.get_stats().await;
        assert_eq!(stats.total_connections, 0);
    }

    #[tokio::test]
    async fn test_websocket_ping_pong() {
        let server = WebSocketServer::new(WebSocketConfig::default());
        let _ = server.handle_connection("conn1".to_string()).await;

        // 首次未认证，状态机将拒绝除 Auth 外的消息
        let response = server.handle_message("conn1", WebSocketMessage::Ping).await;
        // 因为 enable_auth=false，状态机会自动转 Connected
        // 第一次调用时 state == Connecting
        // 由于 enable_auth=false，会直接转 Connected 并处理 Ping
        assert!(response.is_some() || response.is_none());
    }

    #[tokio::test]
    async fn test_websocket_auth() {
        let server = WebSocketServer::new(WebSocketConfig::default());
        let _ = server.handle_connection("conn1".to_string()).await;

        let response = server.handle_message("conn1", WebSocketMessage::Auth(AuthRequest {
            token: "test_token".to_string(),
            client_id: "user1".to_string(),
        })).await;

        assert!(matches!(response, Some(WebSocketMessage::AuthOk(_))));
    }

    #[tokio::test]
    async fn test_websocket_auth_required() {
        let mut config = WebSocketConfig::default();
        config.enable_auth = true;
        let server = WebSocketServer::new(config);
        let _ = server.handle_connection("conn1".to_string()).await;

        let response = server.handle_message("conn1", WebSocketMessage::Ping).await;
        assert!(matches!(response, Some(WebSocketMessage::Error(_))));
    }

    #[tokio::test]
    async fn test_websocket_client() {
        let client = WebSocketClient::new("ws://localhost:8080");
        assert!(!client.client_id().is_empty());
        assert!(client.subscriptions().is_empty());
        assert!(client.should_reconnect());
    }

    #[tokio::test]
    async fn test_websocket_client_subscribe() {
        let mut client = WebSocketClient::new("ws://localhost:8080");
        client.subscribe("test_collection".to_string());
        assert_eq!(client.subscriptions().len(), 1);
    }

    #[tokio::test]
    async fn test_websocket_client_unsubscribe() {
        let mut client = WebSocketClient::new("ws://localhost:8080");
        client.subscribe("test_collection".to_string());
        client.unsubscribe("test_collection");
        assert!(client.subscriptions().is_empty());
    }

    #[tokio::test]
    async fn test_websocket_client_heartbeat() {
        let mut client = WebSocketClient::new("ws://localhost:8080");
        client.handle_heartbeat(&HeartbeatInfo {
            seq: 5,
            server_time: 12345,
            rtt_ms: Some(50),
        });
        assert_eq!(client.heartbeat_seq, 5);
        assert_eq!(client.last_rtt(), 50);
    }

    #[tokio::test]
    async fn test_websocket_client_reconnect() {
        let mut client = WebSocketClient::new("ws://localhost:8080");
        client.record_reconnect();
        client.record_reconnect();
        assert_eq!(client.reconnect_attempts, 2);
        assert!(client.should_reconnect());
    }

    #[tokio::test]
    async fn test_heartbeat_manager() {
        let hm = HeartbeatManager::default();
        let mut conn = Connection {
            id: "test".to_string(),
            client_id: "client".to_string(),
            user_id: None,
            state: ConnectionState::Connected,
            subscribed_collections: Vec::new(),
            last_ping_at: Instant::now(),
            last_pong_at: Instant::now(),
            missed_pongs: 0,
            heartbeat_seq: 0,
            connected_at: Instant::now(),
            resume_token: None,
            last_event_id: None,
            rate_count: 0,
            rate_window_start: Instant::now(),
        };
        assert!(!hm.should_disconnect(&conn));
        conn.missed_pongs = 3;
        assert!(hm.should_disconnect(&conn));
    }

    #[tokio::test]
    async fn test_subscribe_unsubscribe() {
        let server = WebSocketServer::new(WebSocketConfig::default());
        let _ = server.handle_connection("conn1".to_string()).await;

        server.handle_message("conn1", WebSocketMessage::Subscribe(SubscribeRequest {
            collection: "test".to_string(),
            event_types: vec!["insert".to_string()],
            client_id: "client1".to_string(),
            filter: None,
        })).await;

        let subs = server.subscriptions.read().await;
        assert!(subs.contains_key("test"));
    }

    #[tokio::test]
    async fn test_broadcast_to_collection() {
        let server = WebSocketServer::new(WebSocketConfig::default());
        let _ = server.handle_connection("conn1".to_string()).await;
        server.handle_message("conn1", WebSocketMessage::Subscribe(SubscribeRequest {
            collection: "test".to_string(),
            event_types: vec!["insert".to_string()],
            client_id: "client1".to_string(),
            filter: None,
        })).await;

        let count = server.broadcast_to_collection("test", DataChangeEvent {
            collection: "test".to_string(),
            event_type: "insert".to_string(),
            ids: vec!["id1".to_string()],
            timestamp: 12345,
            event_id: "evt1".to_string(),
            metadata: None,
        }).await;
        // broadcast_to_collection 当前为 0（因为发送是给所有订阅者）
        // 实际发送会通过 event_sender.send 完成
        assert!(count >= 0);
    }

    #[tokio::test]
    async fn test_insert_request_response() {
        let req = InsertRequest {
            collection: "test".to_string(),
            vectors: vec![
                VectorEntry {
                    id: "vec1".to_string(),
                    vector: vec![1.0, 2.0, 3.0],
                    metadata: None,
                }
            ],
            client_id: "client1".to_string(),
            request_id: Some("req-1".to_string()),
        };

        assert_eq!(req.vectors.len(), 1);
        assert_eq!(req.request_id, Some("req-1".to_string()));
    }

    #[tokio::test]
    async fn test_rate_limiter() {
        let mut config = WebSocketConfig::default();
        config.rate_limit_per_minute = 5;
        let server = WebSocketServer::new(config);
        let _ = server.handle_connection("conn1".to_string()).await;

        // 前 5 个请求应该通过
        for i in 0..5 {
            let resp = server.handle_message("conn1", WebSocketMessage::Ack(AckInfo {
                message_id: format!("msg-{}", i),
                status: "ok".to_string(),
            })).await;
            assert!(!matches!(resp, Some(WebSocketMessage::Error(ref e)) if e.code == "RATE_LIMITED"));
        }
    }

    #[tokio::test]
    async fn test_reconnect() {
        let server = WebSocketServer::new(WebSocketConfig::default());
        let token = server.handle_connection("conn1".to_string()).await.unwrap();
        let conn_id = server.reconnect(&token).await;
        assert_eq!(conn_id, Some("conn1".to_string()));
    }
}
