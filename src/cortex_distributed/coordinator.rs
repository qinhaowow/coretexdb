//! Query coordinator for distributed CortexDB

#[cfg(feature = "distributed")]
use std::sync::Arc;
#[cfg(feature = "distributed")]
use tokio::sync::RwLock;
#[cfg(feature = "distributed")]
use crate::cortex_distributed::{
    cluster::ClusterManager,
    sharding::ShardingStrategy,
};
#[cfg(feature = "distributed")]
use crate::cortex_query::{QueryType, QueryParams, QueryResult as CortexQueryResult};

#[cfg(feature = "distributed")]
#[derive(Debug)]
pub struct QueryCoordinator {
    cluster_manager: Arc<ClusterManager>,
    sharding_strategy: Arc<ShardingStrategy>,
    local_query_processor: Arc<dyn crate::cortex_query::QueryProcessor>,
}

#[cfg(feature = "distributed")]
impl QueryCoordinator {
    pub fn new() -> Self {
        let cluster_manager = Arc::new(ClusterManager::new());
        let sharding_strategy = Arc::new(ShardingStrategy::new());
        
        // Create a dummy local query processor for now
        let local_query_processor = Arc::new(crate::cortex_query::DefaultQueryProcessor::new(
            Arc::new(crate::cortex_index::IndexManager::new())
        ));
        
        Self {
            cluster_manager,
            sharding_strategy,
            local_query_processor,
        }
    }

    pub fn with_cluster_manager(cluster_manager: Arc<ClusterManager>) -> Self {
        let sharding_strategy = Arc::new(ShardingStrategy::new());
        
        // Create a dummy local query processor for now
        let local_query_processor = Arc::new(crate::cortex_query::DefaultQueryProcessor::new(
            Arc::new(crate::cortex_index::IndexManager::new())
        ));
        
        Self {
            cluster_manager,
            sharding_strategy,
            local_query_processor,
        }
    }

    pub async fn process_query(&self, params: QueryParams) -> Result<CortexQueryResult, Box<dyn std::error::Error>> {
        // Determine which nodes to query based on the query type
        let nodes = match params.query_type {
            QueryType::VectorSearch => {
                // For vector search, we need to query all nodes
                self.cluster_manager.get_healthy_nodes().await
            },
            QueryType::ScalarRange => {
                // For scalar range query, use range sharding
                let start = params.scalar_min.map(|v| v.to_string()).unwrap_or("0".to_string());
                let end = params.scalar_max.map(|v| v.to_string()).unwrap_or("999999".to_string());
                let node_ids = self.sharding_strategy.get_nodes_for_range(&start, &end).await?;
                
                let all_nodes = self.cluster_manager.get_healthy_nodes().await;
                all_nodes.into_iter()
                    .filter(|n| node_ids.contains(&n.id))
                    .collect()
            },
            QueryType::MetadataFilter => {
                // For metadata filter, we need to query all nodes
                self.cluster_manager.get_healthy_nodes().await
            },
            QueryType::Hybrid => {
                // For hybrid query, we need to query all nodes
                self.cluster_manager.get_healthy_nodes().await;
                self.cluster_manager.get_healthy_nodes().await
            },
        };

        if nodes.is_empty() {
            // If no nodes available, process locally
            return self.local_query_processor.process(params).await;
        }

        // Send queries to all relevant nodes in parallel
        let mut tasks = Vec::new();
        for node in &nodes {
            let params_clone = params.clone();
            let node_clone = node.clone();
            
            tasks.push(tokio::spawn(async move {
                // In a real implementation, we would send the query to the remote node
                // For now, we'll just simulate a remote query by processing it locally
                // with a small delay to simulate network latency
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                
                // Create a dummy query processor for simulation
                let dummy_processor = crate::cortex_query::DefaultQueryProcessor::new(
                    Arc::new(crate::cortex_index::IndexManager::new())
                );
                
                dummy_processor.process(params_clone).await
            }));
        }

        // Collect results from all nodes
        let mut all_results = Vec::new();
        for task in tasks {
            if let Ok(result) = task.await {
                if let Ok(query_result) = result {
                    all_results.extend(query_result.results);
                }
            }
        }

        // Merge and sort results
        all_results.sort_by(|a, b| a.distance.partial_cmp(&b.distance).unwrap());
        
        // Take top k results
        let top_k = all_results.into_iter().take(params.top_k).collect();
        
        // Create merged result
        let merged_result = CortexQueryResult {
            query_id: uuid::Uuid::new_v4().to_string(),
            results: top_k,
            execution_time_ms: 0.0, // We should calculate the actual execution time
        };

        Ok(merged_result)
    }

    pub async fn add_vector(&self, id: &str, vector: &[f32], metadata: &serde_json::Value, index_name: &str) -> Result<(), Box<dyn std::error::Error>> {
        // Determine which node to store the vector on
        let node_id = self.sharding_strategy.get_node_for_key(id).await?;
        
        if let Some(node_id) = node_id {
            // Check if this node is the current node
            if node_id == self.cluster_manager.get_self_node_id().await? {
                // Store locally
                // In a real implementation, we would use the local storage engine
                Ok(())
            } else {
                // Send to remote node
                // In a real implementation, we would send the vector to the remote node
                Ok(())
            }
        } else {
            // If no node found, store locally
            Ok(())
        }
    }

    pub async fn remove_vector(&self, id: &str, index_name: &str) -> Result<bool, Box<dyn std::error::Error>> {
        // Determine which node the vector is stored on
        let node_id = self.sharding_strategy.get_node_for_key(id).await?;
        
        if let Some(node_id) = node_id {
            // Check if this node is the current node
            if node_id == self.cluster_manager.get_self_node_id().await? {
                // Remove locally
                // In a real implementation, we would use the local storage engine
                Ok(true)
            } else {
                // Send remove request to remote node
                // In a real implementation, we would send the request to the remote node
                Ok(true)
            }
        } else {
            // If no node found, return false
            Ok(false)
        }
    }
}

#[cfg(feature = "distributed")]
impl ClusterManager {
    pub async fn get_self_node_id(&self) -> Result<String, Box<dyn std::error::Error>> {
        Ok(self.self_node.id.clone())
    }
}
