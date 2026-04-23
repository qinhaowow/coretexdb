//! WebSocket support for CoreTexDB
//! Provides real-time bidirectional communication for push notifications and live queries

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct WebSocketConfig {
    pub host: String,
    pub port: u16,
    pub max_connections: usize,
    pub ping_interval_secs: u64,
    pub ping_timeout_secs: u64,
}

impl Default for WebSocketConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            port: 8080,
            max_connections: 10000,
            ping_interval_secs: 30,
            ping_timeout_secs: 10,
        }
    }
}

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
    Ping,
    Pong,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchRequest {
    pub collection: String,
    pub query: Vec<f32>,
    pub k: usize,
    pub filter: Option<serde_json::Value>,
    pub client_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResponse {
    pub client_id: String,
    pub results: Vec<SearchResult>,
    pub query_time_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResult {
    pub id: String,
    pub score: f32,
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InsertRequest {
    pub collection: String,
    pub vectors: Vec<VectorEntry>,
    pub client_id: String,
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
    pub inserted_ids: Vec<String>,
    pub count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteRequest {
    pub collection: String,
    pub ids: Vec<String>,
    pub client_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteResponse {
    pub client_id: String,
    pub deleted_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscribeRequest {
    pub collection: String,
    pub event_types: Vec<String>,
    pub client_id: String,
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub code: String,
    pub message: String,
    pub client_id: String,
}

pub struct WebSocketServer {
    config: WebSocketConfig,
    connections: Arc<RwLock<HashMap<String, Connection>>>,
    event_sender: broadcast::Sender<WebSocketMessage>,
    subscriptions: Arc<RwLock<HashMap<String, Vec<String>>>>,
}

struct Connection {
    id: String,
    client_id: String,
    subscribed_collections: Vec<String>,
}

impl WebSocketServer {
    pub fn new(config: WebSocketConfig) -> Self {
        let (event_sender, _) = broadcast::channel(10000);
        
        Self {
            config,
            connections: Arc::new(RwLock::new(HashMap::new())),
            event_sender,
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn handle_connection(&self, connection_id: String) -> Result<(), String> {
        let mut connections = self.connections.write().await;
        
        if connections.len() >= self.config.max_connections {
            return Err("Max connections reached".to_string());
        }

        let connection = Connection {
            id: connection_id.clone(),
            client_id: Uuid::new_v4().to_string(),
            subscribed_collections: Vec::new(),
        };

        connections.insert(connection_id, connection);
        
        Ok(())
    }

    pub async fn handle_message(&self, connection_id: &str, message: WebSocketMessage) -> Option<WebSocketMessage> {
        match message {
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
                Some(WebSocketMessage::Pong)
            }
            _ => None,
        }
    }

    async fn handle_search(&self, req: SearchRequest) -> WebSocketMessage {
        WebSocketMessage::SearchResponse(SearchResponse {
            client_id: req.client_id,
            results: Vec::new(),
            query_time_ms: 0,
        })
    }

    async fn handle_insert(&self, req: InsertRequest) -> WebSocketMessage {
        let inserted_ids: Vec<String> = req.vectors.iter().map(|v| v.id.clone()).collect();
        let count = inserted_ids.len();

        let event = DataChangeEvent {
            collection: req.collection.clone(),
            event_type: "insert".to_string(),
            ids: inserted_ids.clone(),
            timestamp: chrono::Utc::now().timestamp(),
        };

        let _ = self.event_sender.send(WebSocketMessage::DataChange(event));

        WebSocketMessage::InsertResponse(InsertResponse {
            client_id: req.client_id,
            inserted_ids,
            count,
        })
    }

    async fn handle_delete(&self, req: DeleteRequest) -> WebSocketMessage {
        let deleted_count = req.ids.len();

        let event = DataChangeEvent {
            collection: req.collection.clone(),
            event_type: "delete".to_string(),
            ids: req.ids.clone(),
            timestamp: chrono::Utc::now().timestamp(),
        };

        let _ = self.event_sender.send(WebSocketMessage::DataChange(event));

        WebSocketMessage::DeleteResponse(DeleteResponse {
            client_id: req.client_id,
            deleted_count,
        })
    }

    async fn handle_subscribe(&self, connection_id: &str, req: SubscribeRequest) {
        let mut connections = self.connections.write().await;
        if let Some(conn) = connections.get_mut(connection_id) {
            conn.subscribed_collections.push(req.collection.clone());
        }

        let mut subs = self.subscriptions.write().await;
        subs.entry(req.collection)
            .or_insert_with(Vec::new)
            .push(connection_id.to_string());
    }

    async fn handle_unsubscribe(&self, connection_id: &str, req: UnsubscribeRequest) {
        let mut connections = self.connections.write().await;
        if let Some(conn) = connections.get_mut(connection_id) {
            conn.subscribed_collections.retain(|c| c != &req.collection);
        }
    }

    pub async fn broadcast_to_collection(&self, collection: &str, event: DataChangeEvent) {
        let subs = self.subscriptions.read().await;
        if let Some(connections) = subs.get(collection) {
            let message = WebSocketMessage::DataChange(event);
            for conn_id in connections {
                if let Some(conn) = self.connections.read().await.get(conn_id) {
                    let _ = self.event_sender.send(message.clone());
                }
            }
        }
    }

    pub async fn remove_connection(&self, connection_id: &str) {
        let mut connections = self.connections.write().await;
        
        if let Some(conn) = connections.remove(connection_id) {
            let mut subs = self.subscriptions.write().await;
            for collection in conn.subscribed_collections {
                if let Some(conn_list) = subs.get_mut(&collection) {
                    conn_list.retain(|c| c != connection_id);
                }
            }
        }
    }

    pub async fn get_stats(&self) -> WebSocketStats {
        let connections = self.connections.read().await;
        let subs = self.subscriptions.read().await;

        WebSocketStats {
            total_connections: connections.len(),
            total_subscriptions: subs.len(),
            max_connections: self.config.max_connections,
        }
    }

    pub fn config(&self) -> &WebSocketConfig {
        &self.config
    }

    pub fn event_receiver(&self) -> broadcast::Receiver<WebSocketMessage> {
        self.event_sender.subscribe()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebSocketStats {
    pub total_connections: usize,
    pub total_subscriptions: usize,
    pub max_connections: usize,
}

pub struct WebSocketClient {
    client_id: String,
    server_url: String,
    subscriptions: Vec<String>,
}

impl WebSocketClient {
    pub fn new(server_url: &str) -> Self {
        Self {
            client_id: Uuid::new_v4().to_string(),
            server_url: server_url.to_string(),
            subscriptions: Vec::new(),
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_websocket_config_default() {
        let config = WebSocketConfig::default();
        
        assert_eq!(config.port, 8080);
        assert_eq!(config.max_connections, 10000);
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
        
        let result = server.handle_connection("conn1".to_string()).await;
        assert!(result.is_ok());
        
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
    async fn test_websocket_client() {
        let client = WebSocketClient::new("ws://localhost:8080");
        
        assert!(!client.client_id().is_empty());
        assert!(client.subscriptions().is_empty());
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
    async fn test_message_types() {
        let search_req = SearchRequest {
            collection: "test".to_string(),
            query: vec![1.0, 2.0, 3.0],
            k: 10,
            filter: None,
            client_id: "client1".to_string(),
        };
        
        let _ = WebSocketMessage::SearchRequest(search_req);
        let _ = WebSocketMessage::Ping;
        let _ = WebSocketMessage::Pong;
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
        };
        
        assert_eq!(req.vectors.len(), 1);
    }

    #[tokio::test]
    async fn test_data_change_event() {
        let event = DataChangeEvent {
            collection: "test".to_string(),
            event_type: "insert".to_string(),
            ids: vec!["id1".to_string(), "id2".to_string()],
            timestamp: 1234567890,
        };
        
        assert_eq!(event.ids.len(), 2);
    }
}
