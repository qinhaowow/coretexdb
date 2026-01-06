//! Test script for distributed mode

#[cfg(feature = "distributed")]
use cortexdb::{
    cortex_distributed::{
        ClusterManager,
        QueryCoordinator,
        ShardingStrategy,
        MetadataManager,
    },
    cortex_query::{QueryType, QueryParams},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "distributed")]
    {
        println!("Testing distributed mode...");
        
        // Test ClusterManager
        println!("1. Testing ClusterManager...");
        let cluster_manager = ClusterManager::new();
        
        // Start heartbeat monitor
        cluster_manager.start_heartbeat_monitor().await;
        
        // Send heartbeat
        cluster_manager.send_heartbeat().await?;
        println!("Heartbeat sent successfully");
        
        // Elect leader
        cluster_manager.elect_leader().await?;
        let leader = cluster_manager.get_leader().await;
        println!("Leader elected: {:?}", leader);
        
        // Test ShardingStrategy
        println!("2. Testing ShardingStrategy...");
        let sharding_strategy = ShardingStrategy::new();
        
        // Get node for key
        let node_id = sharding_strategy.get_node_for_key("test-key").await?;
        println!("Node for key 'test-key': {:?}", node_id);
        
        // Test MetadataManager
        println!("3. Testing MetadataManager...");
        let metadata_manager = MetadataManager::new();
        
        // Add collection metadata
        let collection_metadata = cortexdb::cortex_distributed::metadata::CollectionMetadata {
            name: "test-collection".to_string(),
            dimension: 128,
            metric: "cosine".to_string(),
            index_type: "hnsw".to_string(),
            shard_count: 4,
            replica_count: 2,
            created_at: chrono::Utc::now().timestamp() as u64,
            updated_at: chrono::Utc::now().timestamp() as u64,
        };
        metadata_manager.add_collection(collection_metadata).await?;
        println!("Collection metadata added successfully");
        
        // Get collection metadata
        let retrieved_collection = metadata_manager.get_collection("test-collection").await?;
        println!("Retrieved collection metadata: {:?}", retrieved_collection);
        
        // Test QueryCoordinator
        println!("4. Testing QueryCoordinator...");
        let query_coordinator = QueryCoordinator::with_cluster_manager(std::sync::Arc::new(cluster_manager));
        
        // Create test query
        let query_params = QueryParams {
            query_type: QueryType::VectorSearch,
            vector: Some(vec![1.0, 0.0, 0.0]),
            scalar_min: None,
            scalar_max: None,
            metadata_filter: None,
            top_k: 10,
            threshold: None,
            index_name: "test-index".to_string(),
        };
        
        // Process query
        let result = query_coordinator.process_query(query_params).await?;
        println!("Query processed successfully, results: {:?}", result);
        
        println!("Distributed mode tests completed!");
    }
    
    #[cfg(not(feature = "distributed"))]
    {
        println!("Distributed feature not enabled. Please run with --features distributed");
    }
    
    Ok(())
}
