//! Sharding strategies for distributed CortexDB

#[cfg(feature = "distributed")]
use std::sync::Arc;
#[cfg(feature = "distributed")]
use tokio::sync::RwLock;
#[cfg(feature = "distributed")]
use crate::cortex_distributed::cluster::NodeInfo;

#[cfg(feature = "distributed")]
pub enum ShardingAlgorithm {
    Hash,
    Range,
    ConsistentHash,
}

#[cfg(feature = "distributed")]
#[derive(Debug)]
pub struct ShardingStrategy {
    algorithm: ShardingAlgorithm,
    node_mapping: Arc<RwLock<Vec<(u64, String)>>>, // (shard_id, node_id)
    num_shards: usize,
}

#[cfg(feature = "distributed")]
impl ShardingStrategy {
    pub fn new() -> Self {
        Self {
            algorithm: ShardingAlgorithm::ConsistentHash,
            node_mapping: Arc::new(RwLock::new(Vec::new())),
            num_shards: 100,
        }
    }

    pub fn with_algorithm(algorithm: ShardingAlgorithm) -> Self {
        Self {
            algorithm,
            node_mapping: Arc::new(RwLock::new(Vec::new())),
            num_shards: 100,
        }
    }

    pub fn with_num_shards(mut self, num_shards: usize) -> Self {
        self.num_shards = num_shards;
        self
    }

    pub async fn add_node(&self, node: &NodeInfo) -> Result<(), Box<dyn std::error::Error>> {
        let mut node_mapping = self.node_mapping.write().await;
        
        // For consistent hashing, add multiple virtual nodes
        let virtual_nodes = 10;
        for i in 0..virtual_nodes {
            let key = format!("{}:{}", node.id, i);
            let shard_id = self.hash_key(&key);
            node_mapping.push((shard_id, node.id.clone()));
        }
        
        // Sort the mapping by shard_id
        node_mapping.sort_by(|a, b| a.0.cmp(&b.0));
        
        Ok(())
    }

    pub async fn remove_node(&self, node_id: &str) -> Result<(), Box<dyn std::error::Error>> {
        let mut node_mapping = self.node_mapping.write().await;
        node_mapping.retain(|(_, id)| id != node_id);
        Ok(())
    }

    pub async fn get_node_for_key(&self, key: &str) -> Result<Option<String>, Box<dyn std::error::Error>> {
        let node_mapping = self.node_mapping.read().await;
        
        if node_mapping.is_empty() {
            return Ok(None);
        }
        
        let shard_id = self.hash_key(key);
        
        // Find the first node with shard_id >= the computed hash
        for (id, node_id) in node_mapping.iter() {
            if *id >= shard_id {
                return Ok(Some(node_id.clone()));
            }
        }
        
        // If no such node, wrap around to the first one
        Ok(node_mapping.first().map(|(_, node_id)| node_id.clone()))
    }

    pub async fn get_nodes_for_range(&self, start: &str, end: &str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let node_mapping = self.node_mapping.read().await;
        let mut nodes = Vec::new();
        
        let start_hash = self.hash_key(start);
        let end_hash = self.hash_key(end);
        
        for (id, node_id) in node_mapping.iter() {
            if (*id >= start_hash && *id <= end_hash) || 
               (start_hash > end_hash && (*id >= start_hash || *id <= end_hash)) {
                if !nodes.contains(node_id) {
                    nodes.push(node_id.clone());
                }
            }
        }
        
        Ok(nodes)
    }

    fn hash_key(&self, key: &str) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::Hasher;
        
        let mut hasher = DefaultHasher::new();
        hasher.write(key.as_bytes());
        hasher.finish()
    }

    pub async fn rebalance(&self, nodes: &[NodeInfo]) -> Result<(), Box<dyn std::error::Error>> {
        // Clear existing mapping
        let mut node_mapping = self.node_mapping.write().await;
        node_mapping.clear();
        
        // Add all nodes with their virtual nodes
        for node in nodes {
            self.add_node(node).await?;
        }
        
        Ok(())
    }
}

#[cfg(feature = "distributed")]
impl Default for ShardingStrategy {
    fn default() -> Self {
        Self::new()
    }
}
