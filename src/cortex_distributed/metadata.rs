//! Metadata management for distributed CortexDB

#[cfg(feature = "distributed")]
use std::sync::Arc;
#[cfg(feature = "distributed")]
use tokio::sync::RwLock;
#[cfg(feature = "distributed")]
use serde::{Serialize, Deserialize};

#[cfg(feature = "distributed")]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CollectionMetadata {
    pub name: String,
    pub dimension: usize,
    pub metric: String,
    pub index_type: String,
    pub shard_count: usize,
    pub replica_count: usize,
    pub created_at: u64,
    pub updated_at: u64,
}

#[cfg(feature = "distributed")]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct NodeMetadata {
    pub id: String,
    pub address: String,
    pub port: u16,
    pub role: String,
    pub shards: Vec<usize>,
    pub load: f64,
    pub created_at: u64,
    pub last_heartbeat: u64,
}

#[cfg(feature = "distributed")]
#[derive(Debug)]
pub struct MetadataManager {
    collections: Arc<RwLock<Vec<CollectionMetadata>>>,
    nodes: Arc<RwLock<Vec<NodeMetadata>>>,
}

#[cfg(feature = "distributed")]
impl MetadataManager {
    pub fn new() -> Self {
        Self {
            collections: Arc::new(RwLock::new(Vec::new())),
            nodes: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub async fn add_collection(&self, metadata: CollectionMetadata) -> Result<(), Box<dyn std::error::Error>> {
        let mut collections = self.collections.write().await;
        if !collections.iter().any(|c| c.name == metadata.name) {
            collections.push(metadata);
        }
        Ok(())
    }

    pub async fn remove_collection(&self, name: &str) -> Result<(), Box<dyn std::error::Error>> {
        let mut collections = self.collections.write().await;
        collections.retain(|c| c.name != name);
        Ok(())
    }

    pub async fn get_collection(&self, name: &str) -> Result<Option<CollectionMetadata>, Box<dyn std::error::Error>> {
        let collections = self.collections.read().await;
        Ok(collections.iter().find(|c| c.name == name).cloned())
    }

    pub async fn get_all_collections(&self) -> Result<Vec<CollectionMetadata>, Box<dyn std::error::Error>> {
        let collections = self.collections.read().await;
        Ok(collections.clone())
    }

    pub async fn update_collection(&self, metadata: CollectionMetadata) -> Result<(), Box<dyn std::error::Error>> {
        let mut collections = self.collections.write().await;
        if let Some(index) = collections.iter().position(|c| c.name == metadata.name) {
            collections[index] = metadata;
        } else {
            collections.push(metadata);
        }
        Ok(())
    }

    pub async fn add_node(&self, metadata: NodeMetadata) -> Result<(), Box<dyn std::error::Error>> {
        let mut nodes = self.nodes.write().await;
        if !nodes.iter().any(|n| n.id == metadata.id) {
            nodes.push(metadata);
        }
        Ok(())
    }

    pub async fn remove_node(&self, id: &str) -> Result<(), Box<dyn std::error::Error>> {
        let mut nodes = self.nodes.write().await;
        nodes.retain(|n| n.id != id);
        Ok(())
    }

    pub async fn get_node(&self, id: &str) -> Result<Option<NodeMetadata>, Box<dyn std::error::Error>> {
        let nodes = self.nodes.read().await;
        Ok(nodes.iter().find(|n| n.id == id).cloned())
    }

    pub async fn get_all_nodes(&self) -> Result<Vec<NodeMetadata>, Box<dyn std::error::Error>> {
        let nodes = self.nodes.read().await;
        Ok(nodes.clone())
    }

    pub async fn update_node(&self, metadata: NodeMetadata) -> Result<(), Box<dyn std::error::Error>> {
        let mut nodes = self.nodes.write().await;
        if let Some(index) = nodes.iter().position(|n| n.id == metadata.id) {
            nodes[index] = metadata;
        } else {
            nodes.push(metadata);
        }
        Ok(())
    }

    pub async fn update_node_load(&self, id: &str, load: f64) -> Result<(), Box<dyn std::error::Error>> {
        let mut nodes = self.nodes.write().await;
        if let Some(node) = nodes.iter_mut().find(|n| n.id == id) {
            node.load = load;
            node.last_heartbeat = chrono::Utc::now().timestamp() as u64;
        }
        Ok(())
    }
}

#[cfg(feature = "distributed")]
impl Default for MetadataManager {
    fn default() -> Self {
        Self::new()
    }
}
