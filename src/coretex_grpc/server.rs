//! gRPC server runner

use std::error::Error;
use std::net::SocketAddr;
use tonic::transport::Server;

use crate::coretex_grpc::coretex_service::coretex_service_server::CoretexServiceServer;
use crate::{CoreTexDB, CoretexService};

pub async fn start_grpc_server(
    db: CoreTexDB,
    addr: SocketAddr,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let service = CoretexService::new(db);
    
    println!("Starting gRPC server on {}", addr);
    println!("gRPC endpoints:");
    println!("  CreateCollection");
    println!("  DeleteCollection");
    println!("  ListCollections");
    println!("  InsertVectors");
    println!("  SearchVectors");
    println!("  GetVector");
    println!("  DeleteVectors");
    println!("  GetCollectionInfo");
    println!("  HealthCheck");
    
    Server::builder()
        .add_service(CoretexServiceServer::new(service))
        .serve(addr)
        .await?;
    
    Ok(())
}
