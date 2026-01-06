//! gRPC server for CortexDB

#[cfg(feature = "grpc")]
use tonic::{Request, Response, Status};
#[cfg(feature = "grpc")]
use super::generated::cortexdb::{
    CortexDbService,
    HealthCheckRequest,
    HealthCheckResponse,
    SearchRequest,
    SearchResponse,
    CreateIndexRequest,
    CreateIndexResponse,
    AddVectorRequest,
    AddVectorResponse,
    SearchResultItem,
};
#[cfg(feature = "grpc")]
use crate::{
    cortex_index::IndexManager,
    cortex_storage::{StorageEngine, MemoryStorage},
    cortex_query::{QueryType, QueryParams, QueryPlanner, DefaultQueryProcessor},
};
use std::sync::Arc;
use std::error::Error;

#[cfg(feature = "grpc")]
#[derive(Debug)]
pub struct CortexDBGrpcServer {
    index_manager: Arc<IndexManager>,
    storage: Arc<dyn StorageEngine>,
    query_planner: Arc<QueryPlanner>,
}

#[cfg(feature = "grpc")]
impl CortexDBGrpcServer {
    pub fn new(
        index_manager: Arc<IndexManager>,
        storage: Arc<dyn StorageEngine>,
        query_planner: Arc<QueryPlanner>,
    ) -> Self {
        Self {
            index_manager,
            storage,
            query_planner,
        }
    }
}

#[cfg(feature = "grpc")]
#[tonic::async_trait]
impl CortexDbService for CortexDBGrpcServer {
    async fn health_check(
        &self,
        _request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        let response = HealthCheckResponse {
            status: "ok".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
        };
        Ok(Response::new(response))
    }

    async fn search(
        &self,
        request: Request<SearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        let req = request.into_inner();
        let start_time = std::time::Instant::now();

        let query_params = QueryParams {
            query_type: QueryType::VectorSearch,
            vector: Some(req.query.values),
            scalar_min: None,
            scalar_max: None,
            metadata_filter: None,
            top_k: req.k as usize,
            threshold: if req.threshold > 0.0 { Some(req.threshold) } else { None },
            index_name: req.index,
        };

        let result = self.query_planner.plan_and_execute(query_params).await
            .map_err(|e| Status::internal(e.to_string()))?;

        let execution_time = start_time.elapsed().as_millis() as f64;

        let results = result.results.into_iter()
            .map(|item| SearchResultItem {
                id: item.id,
                distance: item.distance,
            })
            .collect();

        let response = SearchResponse {
            results,
            execution_time,
        };

        Ok(Response::new(response))
    }

    async fn create_index(
        &self,
        request: Request<CreateIndexRequest>,
    ) -> Result<Response<CreateIndexResponse>, Status> {
        let req = request.into_inner();

        let result = self.index_manager.create_index(&req.name, &req.r#type, &req.metric).await
            .map_err(|e| Status::internal(e.to_string()))?;

        let response = CreateIndexResponse {
            status: "ok".to_string(),
            index: req.name,
        };

        Ok(Response::new(response))
    }

    async fn add_vector(
        &self,
        request: Request<AddVectorRequest>,
    ) -> Result<Response<AddVectorResponse>, Status> {
        let req = request.into_inner();

        let metadata = serde_json::Value::Object(
            req.metadata.fields.into_iter()
                .map(|(k, v)| (k, serde_json::Value::String(v)))
                .collect()
        );

        let storage_result = self.storage.store(&req.id, &req.vector.values, &metadata).await
            .map_err(|e| Status::internal(e.to_string()))?;

        let index_result = self.index_manager.get_index(&req.index).await
            .map_err(|e| Status::internal(e.to_string()))?;

        if let Some(index) = index_result {
            index.add(&req.id, &req.vector.values).await
                .map_err(|e| Status::internal(e.to_string()))?;
        } else {
            return Err(Status::not_found(format!("Index {} not found", req.index)));
        }

        let response = AddVectorResponse {
            status: "ok".to_string(),
            id: req.id,
        };

        Ok(Response::new(response))
    }
}

#[cfg(feature = "grpc")]
pub async fn start_server(address: &str, port: u16) -> Result<(), Box<dyn Error>> {
    // Initialize storage
    let storage = Arc::new(MemoryStorage::new());
    let mut storage_mut = storage.as_ref() as &mut dyn StorageEngine;
    storage_mut.init().await?;
    
    // Initialize index manager
    let index_manager = Arc::new(IndexManager::new());
    
    // Initialize query processor and planner
    let query_processor = Arc::new(DefaultQueryProcessor::new(index_manager.clone()));
    let query_planner = Arc::new(QueryPlanner::new(query_processor));
    
    // Create gRPC server
    let server = CortexDBGrpcServer::new(index_manager, storage, query_planner);
    
    // Bind to address
    let addr = format!("{}:{}", address, port).parse()?;
    
    println!("Starting CortexDB gRPC server on {}", addr);
    
    // Start server
    tonic::transport::Server::builder()
        .add_service(crate::cortex_api::grpc::generated::cortexdb::CortexDbServiceServer::new(server))
        .serve(addr)
        .await?;
    
    Ok(())
}

#[cfg(not(feature = "grpc"))]
pub async fn start_server(_address: &str, _port: u16) -> Result<(), Box<dyn Error>> {
    Err("gRPC feature not enabled".into())
}
