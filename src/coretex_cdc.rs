//! CDC (Change Data Capture) for CortexDB
//! Real-time data synchronization from source databases

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock, broadcast};
use async_trait::async_trait;

pub struct CdcEngine {
    source_connectors: Arc<RwLock<HashMap<String, Box<dyn CdcSource + Send + Sync>>>>,
    event_sender: broadcast::Sender<CdcEvent>,
    config: CdcConfig,
}

#[derive(Debug, Clone)]
pub struct CdcConfig {
    pub poll_interval_ms: u64,
    pub batch_size: usize,
    pub enable_checkpoint: bool,
    pub retry_attempts: u32,
}

impl Default for CdcConfig {
    fn default() -> Self {
        Self {
            poll_interval_ms: 100,
            batch_size: 100,
            enable_checkpoint: true,
            retry_attempts: 3,
        }
    }
}

#[derive(Debug, Clone)]
pub enum CdcEvent {
    Insert { 
        table: String, 
        key: String, 
        data: HashMap<String, String>,
        timestamp: u64,
    },
    Update { 
        table: String, 
        key: String, 
        old_data: HashMap<String, String>,
        new_data: HashMap<String, String>,
        timestamp: u64,
    },
    Delete { 
        table: String, 
        key: String,
        data: HashMap<String, String>,
        timestamp: u64,
    },
    SchemaChange {
        table: String,
        change_type: SchemaChangeType,
        timestamp: u64,
    },
}

#[derive(Debug, Clone)]
pub enum SchemaChangeType {
    ColumnAdded { column: String, column_type: String },
    ColumnRemoved { column: String },
    ColumnTypeChanged { column: String, old_type: String, new_type: String },
}

#[async_trait]
pub trait CdcSource: Send + Sync {
    fn source_type(&self) -> &str;
    async fn connect(&mut self) -> Result<(), CdcError>;
    async fn disconnect(&mut self) -> Result<(), CdcError>;
    async fn get_changes(&mut self, last_position: Option<&str>) -> Result<Vec<CdcEvent>, CdcError>;
    fn get_position(&self) -> Option<String>;
}

#[derive(Debug)]
pub enum CdcError {
    ConnectionError(String),
    QueryError(String),
    PositionError(String),
    TransformError(String),
}

impl std::fmt::Display for CdcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CdcError::ConnectionError(msg) => write!(f, "Connection error: {}", msg),
            CdcError::QueryError(msg) => write!(f, "Query error: {}", msg),
            CdcError::PositionError(msg) => write!(f, "Position error: {}", msg),
            CdcError::TransformError(msg) => write!(f, "Transform error: {}", msg),
        }
    }
}

impl std::error::Error for CdcError {}

pub struct PostgresCdcSource {
    connection_string: String,
    slot_name: String,
    position: Option<String>,
}

impl PostgresCdcSource {
    pub fn new(connection_string: &str, slot_name: &str) -> Self {
        Self {
            connection_string: connection_string.to_string(),
            slot_name: slot_name.to_string(),
            position: None,
        }
    }
}

#[async_trait]
impl CdcSource for PostgresCdcSource {
    fn source_type(&self) -> &str {
        "postgres"
    }

    async fn connect(&mut self) -> Result<(), CdcError> {
        Ok(())
    }

    async fn disconnect(&mut self) -> Result<(), CdcError> {
        Ok(())
    }

    async fn get_changes(&mut self, last_position: Option<&str>) -> Result<Vec<CdcEvent>, CdcError> {
        self.position = last_position.map(|s| s.to_string());
        Ok(vec![])
    }

    fn get_position(&self) -> Option<String> {
        self.position.clone()
    }
}

pub struct MysqlCdcSource {
    connection_string: String,
    server_id: u32,
    position: Option<String>,
}

impl MysqlCdcSource {
    pub fn new(connection_string: &str, server_id: u32) -> Self {
        Self {
            connection_string: connection_string.to_string(),
            server_id,
            position: None,
        }
    }
}

#[async_trait]
impl CdcSource for MysqlCdcSource {
    fn source_type(&self) -> &str {
        "mysql"
    }

    async fn connect(&mut self) -> Result<(), CdcError> {
        Ok(())
    }

    async fn disconnect(&mut self) -> Result<(), CdcError> {
        Ok(())
    }

    async fn get_changes(&mut self, last_position: Option<&str>) -> Result<Vec<CdcEvent>, CdcError> {
        self.position = last_position.map(|s| s.to_string());
        Ok(vec![])
    }

    fn get_position(&self) -> Option<String> {
        self.position.clone()
    }
}

pub struct MongodbCdcSource {
    connection_string: String,
    collection: String,
    position: Option<String>,
}

impl MongodbCdcSource {
    pub fn new(connection_string: &str, collection: &str) -> Self {
        Self {
            connection_string: connection_string.to_string(),
            collection: collection.to_string(),
            position: None,
        }
    }
}

#[async_trait]
impl CdcSource for MongodbCdcSource {
    fn source_type(&self) -> &str {
        "mongodb"
    }

    async fn connect(&mut self) -> Result<(), CdcError> {
        Ok(())
    }

    async fn disconnect(&mut self) -> Result<(), CdcError> {
        Ok(())
    }

    async fn get_changes(&mut self, last_position: Option<&str>) -> Result<Vec<CdcEvent>, CdcError> {
        self.position = last_position.map(|s| s.to_string());
        Ok(vec![])
    }

    fn get_position(&self) -> Option<String> {
        self.position.clone()
    }
}

impl CdcEngine {
    pub fn new(config: CdcConfig) -> Self {
        let (sender, _) = broadcast::channel(1000);
        Self {
            source_connectors: Arc::new(RwLock::new(HashMap::new())),
            event_sender: sender,
            config,
        }
    }

    pub async fn register_source(&self, name: String, source: Box<dyn CdcSource + Send + Sync>) {
        let mut sources = self.source_connectors.write().await;
        sources.insert(name, source);
    }

    pub async fn unregister_source(&self, name: &str) {
        let mut sources = self.source_connectors.write().await;
        sources.remove(name);
    }

    pub async fn start_sync(&self, source_name: &str) -> Result<(), CdcError> {
        let sources = self.source_connectors.read().await;
        
        if let Some(source) = sources.get(source_name) {
            let _ = source.connect().await?;
        }
        
        Ok(())
    }

    pub fn subscribe(&self) -> broadcast::Receiver<CdcEvent> {
        self.event_sender.subscribe()
    }

    pub fn get_event_sender(&self) -> broadcast::Sender<CdcEvent> {
        self.event_sender.clone()
    }

    pub async fn get_all_sources(&self) -> Vec<String> {
        let sources = self.source_connectors.read().await;
        sources.keys().cloned().collect()
    }
}

pub struct VectorSyncHandler {
    cdc_receiver: broadcast::Receiver<CdcEvent>,
    target_collection: String,
    field_mapping: HashMap<String, String>,
}

impl VectorSyncHandler {
    pub fn new(
        cdc_receiver: broadcast::Receiver<CdcEvent>,
        target_collection: String,
        field_mapping: HashMap<String, String>,
    ) -> Self {
        Self {
            cdc_receiver,
            target_collection,
            field_mapping,
        }
    }

    pub async fn process_events(&mut self) -> Result<Vec<CdcEvent>, CdcError> {
        let mut events = Vec::new();
        
        while let Ok(event) = self.cdc_receiver.try_recv() {
            events.push(event);
        }
        
        Ok(events)
    }

    pub fn transform_to_vector_event(&self, event: &CdcEvent) -> Option<VectorSyncEvent> {
        match event {
            CdcEvent::Insert { table, key, data, timestamp } => {
                Some(VectorSyncEvent::Upsert {
                    id: key.clone(),
                    vector: self.extract_vector_fields(data),
                    metadata: data.clone(),
                    timestamp: *timestamp,
                })
            },
            CdcEvent::Update { table, key, new_data, timestamp, .. } => {
                Some(VectorSyncEvent::Upsert {
                    id: key.clone(),
                    vector: self.extract_vector_fields(new_data),
                    metadata: new_data.clone(),
                    timestamp: *timestamp,
                })
            },
            CdcEvent::Delete { table, key, timestamp } => {
                Some(VectorSyncEvent::Delete {
                    id: key.clone(),
                    timestamp: *timestamp,
                })
            },
            _ => None,
        }
    }

    fn extract_vector_fields(&self, data: &HashMap<String, String>) -> Vec<f32> {
        let mut vector = Vec::new();
        
        for (target_field, source_value) in self.field_mapping.iter() {
            if let Some(value) = data.get(source_field) {
                if let Ok(float_val) = source_value.parse::<f32>() {
                    vector.push(float_val);
                }
            }
        }
        
        vector
    }
}

#[derive(Debug, Clone)]
pub enum VectorSyncEvent {
    Upsert {
        id: String,
        vector: Vec<f32>,
        metadata: HashMap<String, String>,
        timestamp: u64,
    },
    Delete {
        id: String,
        timestamp: u64,
    },
}
