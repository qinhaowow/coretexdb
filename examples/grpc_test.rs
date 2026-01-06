//! Test script for gRPC API

#[cfg(feature = "grpc")]
use tonic::transport::Channel;
#[cfg(feature = "grpc")]
use cortexdb::cortex_api::grpc::generated::cortexdb::{
    CortexDbServiceClient,
    SearchRequest,
    Vector,
    CreateIndexRequest,
    AddVectorRequest,
    Metadata,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "grpc")]
    {
        // Connect to gRPC server
        let channel = Channel::from_static("http://[::1]:50051")
            .connect()
            .await?;
        
        let mut client = CortexDbServiceClient::new(channel);
        
        println!("Testing gRPC API...");
        
        // Test health check
        println!("1. Testing health check...");
        let health_check_response = client.health_check(()).await?;
        println!("Health check response: {:?}", health_check_response.into_inner());
        
        // Test create index
        println!("2. Testing create index...");
        let create_index_request = CreateIndexRequest {
            name: "test-index".to_string(),
            r#type: "brute_force".to_string(),
            metric: "cosine".to_string(),
        };
        let create_index_response = client.create_index(create_index_request).await?;
        println!("Create index response: {:?}", create_index_response.into_inner());
        
        // Test add vector
        println!("3. Testing add vector...");
        let add_vector_request = AddVectorRequest {
            id: "vec1".to_string(),
            vector: Vector {
                values: vec![1.0, 0.0, 0.0],
            },
            metadata: Metadata {
                fields: vec![("category".to_string(), "test".to_string())]
                    .into_iter()
                    .collect(),
            },
            index: "test-index".to_string(),
        };
        let add_vector_response = client.add_vector(add_vector_request).await?;
        println!("Add vector response: {:?}", add_vector_response.into_inner());
        
        // Test search
        println!("4. Testing search...");
        let search_request = SearchRequest {
            query: Vector {
                values: vec![1.0, 0.0, 0.0],
            },
            k: 10,
            index: "test-index".to_string(),
            threshold: 0.0,
        };
        let search_response = client.search(search_request).await?;
        println!("Search response: {:?}", search_response.into_inner());
        
        println!("gRPC API tests completed!");
    }
    
    #[cfg(not(feature = "grpc"))]
    {
        println!("gRPC feature not enabled. Please run with --features grpc");
    }
    
    Ok(())
}
