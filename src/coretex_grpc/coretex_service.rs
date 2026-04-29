//! CoreTexDB gRPC Service

use tonic::{Request, Response, Status};
use std::sync::Arc;
use std::collections::HashMap;
use tokio::sync::RwLock;

use crate::CoreTexDB;

pub struct CoretexService {
    db: Arc<RwLock<CoreTexDB>>,
}

impl CoretexService {
    pub fn new(db: CoreTexDB) -> Self {
        Self {
            db: Arc::new(RwLock::new(db)),
        }
    }
}

tonic::include_proto!("coretex");

impl self::coretex_service_server::CoretexService for CoretexService {
    async fn create_collection(
        &self,
        request: Request<CreateCollectionRequest>,
    ) -> Result<Response<CollectionResponse>, Status> {
        let req = request.into_inner();
        
        let db = self.db.read().await;
        db.create_collection(&req.name, req.dimension as usize, &req.metric)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        
        Ok(Response::new(CollectionResponse {
            success: true,
            message: format!("Collection '{}' created", req.name),
        }))
    }

    async fn delete_collection(
        &self,
        request: Request<DeleteCollectionRequest>,
    ) -> Result<Response<CollectionResponse>, Status> {
        let req = request.into_inner();
        
        let db = self.db.read().await;
        db.delete_collection(&req.name)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        
        Ok(Response::new(CollectionResponse {
            success: true,
            message: format!("Collection '{}' deleted", req.name),
        }))
    }

    async fn list_collections(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<ListCollectionsResponse>, Status> {
        let db = self.db.read().await;
        let collections = db.list_collections()
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        
        Ok(Response::new(ListCollectionsResponse {
            collections,
        }))
    }

    async fn insert_vectors(
        &self,
        request: Request<InsertVectorsRequest>,
    ) -> Result<Response<InsertVectorsResponse>, Status> {
        let req = request.into_inner();
        
        let vectors: Vec<(String, Vec<f32>, serde_json::Value)> = req.vectors
            .into_iter()
            .map(|v| {
                let metadata = serde_json::json!(&v.metadata);
                (v.id, v.vector, metadata)
            })
            .collect();
        
        let db = self.db.read().await;
        let ids = db.insert_vectors(&req.collection, vectors)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        
        Ok(Response::new(InsertVectorsResponse {
            ids,
            count: ids.len() as u32,
        }))
    }

    async fn search_vectors(
        &self,
        request: Request<SearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        let req = request.into_inner();
        
        let db = self.db.read().await;
        let results = db.search(
            &req.collection,
            req.query_vector,
            req.k as usize,
            None,
        )
        .await
        .map_err(|e| Status::internal(e.to_string()))?;
        
        let results: Vec<SearchResult> = results
            .into_iter()
            .map(|r| SearchResult {
                id: r.id,
                score: 1.0 - r.distance,
                distance: r.distance,
            })
            .collect();
        
        Ok(Response::new(SearchResponse {
            results,
        }))
    }

    async fn get_vector(
        &self,
        request: Request<GetVectorRequest>,
    ) -> Result<Response<GetVectorResponse>, Status> {
        let req = request.into_inner();
        
        let db = self.db.read().await;
        let result = db.get_vector(&req.collection, &req.id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        
        match result {
            Some((vector, metadata)) => Ok(Response::new(GetVectorResponse {
                id: req.id,
                vector,
                metadata: serde_json::to_string(&metadata).unwrap_or_default(),
            })),
            None => Err(Status::not_found("Vector not found")),
        }
    }

    async fn delete_vectors(
        &self,
        request: Request<DeleteVectorsRequest>,
    ) -> Result<Response<DeleteVectorsResponse>, Status> {
        let req = request.into_inner();
        
        let db = self.db.read().await;
        let count = db.delete_vectors(&req.collection, &req.ids)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        
        Ok(Response::new(DeleteVectorsResponse {
            deleted_count: count as u32,
        }))
    }

    async fn get_collection_info(
        &self,
        request: Request<GetCollectionInfoRequest>,
    ) -> Result<Response<CollectionInfoResponse>, Status> {
        let req = request.into_inner();
        
        let db = self.db.read().await;
        let schema = db.get_collection(&req.name)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        
        let count = db.get_vectors_count(&req.name)
            .await
            .unwrap_or(0);
        
        Ok(Response::new(CollectionInfoResponse {
            name: schema.name,
            dimension: schema.dimension as u32,
            metric: format!("{:?}", schema.distance_metric),
            vector_count: count as u32,
        }))
    }

    async fn health_check(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<HealthResponse>, Status> {
        Ok(Response::new(HealthResponse {
            status: "healthy".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
        }))
    }
}
