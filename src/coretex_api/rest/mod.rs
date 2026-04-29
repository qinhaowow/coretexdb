//! REST API for CoreTexDB

use axum::{
    routing::{get, post, delete, put},
    Json, Router, Extension,
};
use tower_http::cors::{Any, CorsLayer};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::sync::Arc;
use std::net::SocketAddr;
use tokio::sync::RwLock;
use std::collections::HashMap;

use crate::{CoreTexDB, DbConfig, SearchResult};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ApiConfig {
    pub address: String,
    pub port: u16,
    pub enable_cors: bool,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            address: "0.0.0.0".to_string(),
            port: 5000,
            enable_cors: true,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateCollectionRequest {
    pub name: String,
    pub dimension: usize,
    pub distance_metric: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CollectionInfo {
    pub name: String,
    pub dimension: usize,
    pub distance_metric: String,
    pub vectors_count: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InsertVectorsRequest {
    pub vectors: Vec<VectorItem>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct VectorItem {
    pub id: String,
    pub vector: Vec<f32>,
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InsertVectorsResponse {
    pub status: String,
    pub ids: Vec<String>,
    pub count: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GetVectorResponse {
    pub id: String,
    pub vector: Vec<f32>,
    pub metadata: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DeleteVectorsRequest {
    pub ids: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DeleteVectorsResponse {
    pub status: String,
    pub deleted_count: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SearchRequest {
    pub vector: Vec<f32>,
    pub k: usize,
    pub filter: Option<serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SearchResponse {
    pub results: Vec<SearchResultItem>,
    pub execution_time_ms: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SearchResultItem {
    pub id: String,
    pub score: f32,
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BatchSearchRequest {
    pub queries: Vec<Vec<f32>>,
    pub k: usize,
    pub filter: Option<serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BatchSearchResponse {
    pub results: Vec<Vec<SearchResultItem>>,
    pub execution_time_ms: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateVectorsRequest {
    pub ids: Vec<String>,
    pub vectors: Option<Vec<Vec<f32>>>,
    pub metadata: Option<Vec<serde_json::Value>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateVectorsResponse {
    pub status: String,
    pub updated_count: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CollectionStats {
    pub name: String,
    pub vector_count: usize,
    pub dimension: usize,
    pub metric: String,
    pub index_type: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ApiResponse<T> {
    pub status: String,
    pub data: Option<T>,
    pub error: Option<String>,
}

impl<T> ApiResponse<T> {
    pub fn success(data: T) -> Self {
        Self {
            status: "ok".to_string(),
            data: Some(data),
            error: None,
        }
    }

    pub fn error(msg: &str) -> Self {
        Self {
            status: "error".to_string(),
            data: None,
            error: Some(msg.to_string()),
        }
    }
}

pub struct ApiState {
    pub db: Arc<RwLock<CoreTexDB>>,
}

pub async fn start_server(config: ApiConfig) -> Result<(), Box<dyn Error>> {
    let db = CoreTexDB::new();
    db.init().await.map_err(|e| format!("Failed to init DB: {}", e))?;

    let state = ApiState {
        db: Arc::new(RwLock::new(db)),
    };

    let mut app = Router::new()
        .route("/health", get(health_check))
        .route("/api/collections", get(list_collections))
        .route("/api/collections", post(create_collection))
        .route("/api/collections/:name", get(get_collection))
        .route("/api/collections/:name", delete(delete_collection))
        .route("/api/collections/:name/stats", get(get_collection_stats))
        .route("/api/collections/:name/vectors", post(insert_vectors))
        .route("/api/collections/:name/vectors", put(update_vectors))
        .route("/api/collections/:name/vectors/:id", get(get_vector))
        .route("/api/collections/:name/vectors", delete(delete_vectors))
        .route("/api/collections/:name/search", post(search))
        .route("/api/collections/:name/batch-search", post(batch_search))
        .route("/api/collections/:name/count", get(get_vectors_count))
        .layer(Extension(Arc::new(state)));

    if config.enable_cors {
        let cors = CorsLayer::new()
            .allow_origin(Any)
            .allow_methods(Any)
            .allow_headers(Any);
        app = app.layer(cors);
    }

    let addr = SocketAddr::new(
        config.address.parse().unwrap(),
        config.port,
    );

    println!("Starting CortexDB API server on http://{}", addr);
    println!("API endpoints:");
    println!("  GET  /health                              - Health check");
    println!("  GET  /api/collections                     - List collections");
    println!("  POST /api/collections                    - Create collection");
    println!("  GET  /api/collections/:name               - Get collection info");
    println!("  DELETE /api/collections/:name             - Delete collection");
    println!("  GET  /api/collections/:name/stats         - Get collection stats");
    println!("  POST /api/collections/:name/vectors       - Insert vectors");
    println!("  PUT  /api/collections/:name/vectors      - Update vectors");
    println!("  GET  /api/collections/:name/vectors/:id  - Get vector");
    println!("  DELETE /api/collections/:name/vectors     - Delete vectors");
    println!("  POST /api/collections/:name/search       - Search vectors");
    println!("  POST /api/collections/:name/batch-search - Batch search");
    println!("  GET  /api/collections/:name/count        - Get vectors count");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve::serve(listener, app).await?;

    Ok(())
}

async fn health_check() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
    })
}

async fn list_collections(
    Extension(state): Extension<Arc<ApiState>>,
) -> Json<ApiResponse<Vec<String>>> {
    let db = state.db.read().await;
    match db.list_collections().await {
        Ok(collections) => Json(ApiResponse::success(collections)),
        Err(e) => Json(ApiResponse::error(&e.to_string())),
    }
}

async fn create_collection(
    Extension(state): Extension<Arc<ApiState>>,
    Json(req): Json<CreateCollectionRequest>,
) -> Json<ApiResponse<CollectionInfo>> {
    let db = state.db.read().await;
    let metric = req.distance_metric.unwrap_or_else(|| "cosine".to_string());
    
    match db.create_collection(&req.name, req.dimension, &metric).await {
        Ok(_) => {
            let info = CollectionInfo {
                name: req.name.clone(),
                dimension: req.dimension,
                distance_metric: metric,
                vectors_count: 0,
            };
            Json(ApiResponse::success(info))
        }
        Err(e) => Json(ApiResponse::error(&e.to_string())),
    }
}

async fn get_collection(
    Extension(state): Extension<Arc<ApiState>>,
    axum::extract::Path(name): axum::extract::Path<String>,
) -> Json<ApiResponse<CollectionInfo>> {
    let db = state.db.read().await;
    
    match db.get_collection(&name).await {
        Ok(schema) => {
            let count = db.get_vectors_count(&name).await.unwrap_or(0);
            let info = CollectionInfo {
                name: schema.name,
                dimension: schema.dimension,
                distance_metric: format!("{:?}", schema.distance_metric),
                vectors_count: count,
            };
            Json(ApiResponse::success(info))
        }
        Err(e) => Json(ApiResponse::error(&e.to_string())),
    }
}

async fn delete_collection(
    Extension(state): Extension<Arc<ApiState>>,
    axum::extract::Path(name): axum::extract::Path<String>,
) -> Json<ApiResponse<String>> {
    let db = state.db.read().await;
    
    match db.delete_collection(&name).await {
        Ok(_) => Json(ApiResponse::success(format!("Collection '{}' deleted", name))),
        Err(e) => Json(ApiResponse::error(&e.to_string())),
    }
}

async fn insert_vectors(
    Extension(state): Extension<Arc<ApiState>>,
    axum::extract::Path(name): axum::extract::Path<String>,
    Json(req): Json<InsertVectorsRequest>,
) -> Json<ApiResponse<InsertVectorsResponse>> {
    let db = state.db.read().await;
    
    let vectors: Vec<(String, Vec<f32>, serde_json::Value)> = req.vectors
        .into_iter()
        .map(|v| (v.id, v.vector, v.metadata.unwrap_or(serde_json::json!({}))))
        .collect();
    
    let ids = vectors.iter().map(|(id, _, _)| id.clone()).collect();
    
    match db.insert_vectors(&name, vectors).await {
        Ok(inserted_ids) => Json(ApiResponse::success(InsertVectorsResponse {
            status: "ok".to_string(),
            ids: inserted_ids,
            count: ids.len(),
        })),
        Err(e) => Json(ApiResponse::error(&e.to_string())),
    }
}

async fn get_vector(
    Extension(state): Extension<Arc<ApiState>>,
    axum::extract::Path((name, id)): axum::extract::Path<(String, String)>,
) -> Json<ApiResponse<GetVectorResponse>> {
    let db = state.db.read().await;
    
    match db.get_vector(&name, &id).await {
        Ok(Some((vector, metadata))) => Json(ApiResponse::success(GetVectorResponse {
            id,
            vector,
            metadata,
        })),
        Ok(None) => Json(ApiResponse::error("Vector not found")),
        Err(e) => Json(ApiResponse::error(&e.to_string())),
    }
}

async fn delete_vectors(
    Extension(state): Extension<Arc<ApiState>>,
    axum::extract::Path(name): axum::extract::Path<String>,
    Json(req): Json<DeleteVectorsRequest>,
) -> Json<ApiResponse<DeleteVectorsResponse>> {
    let db = state.db.read().await;
    
    match db.delete_vectors(&name, &req.ids).await {
        Ok(count) => Json(ApiResponse::success(DeleteVectorsResponse {
            status: "ok".to_string(),
            deleted_count: count,
        })),
        Err(e) => Json(ApiResponse::error(&e.to_string())),
    }
}

async fn search(
    Extension(state): Extension<Arc<ApiState>>,
    axum::extract::Path(name): axum::extract::Path<String>,
    Json(req): Json<SearchRequest>,
) -> Json<ApiResponse<SearchResponse>> {
    let start = std::time::Instant::now();
    let db = state.db.read().await;
    
    match db.search(&name, req.vector, req.k, req.filter).await {
        Ok(results) => {
            let data = state.db.read().await;
            let data_lock = data.data.read().await;
            let collection_data = data_lock.get(&name);
            
            let search_results: Vec<SearchResultItem> = results
                .into_iter()
                .map(|r| {
                    let metadata = collection_data
                        .and_then(|cd| cd.get(&r.id))
                        .map(|(_, m)| m.clone());
                    
                    SearchResultItem {
                        id: r.id,
                        score: 1.0 - r.distance,
                        metadata,
                    }
                })
                .collect();
            
            let execution_time = start.elapsed().as_millis() as u64;
            
            Json(ApiResponse::success(SearchResponse {
                results: search_results,
                execution_time_ms: execution_time,
            }))
        }
        Err(e) => Json(ApiResponse::error(&e.to_string())),
    }
}

async fn get_vectors_count(
    Extension(state): Extension<Arc<ApiState>>,
    axum::extract::Path(name): axum::extract::Path<String>,
) -> Json<ApiResponse<usize>> {
    let db = state.db.read().await;
    
    match db.get_vectors_count(&name).await {
        Ok(count) => Json(ApiResponse::success(count)),
        Err(e) => Json(ApiResponse::error(&e.to_string())),
    }
}

async fn get_collection_stats(
    Extension(state): Extension<Arc<ApiState>>,
    axum::extract::Path(name): axum::extract::Path<String>,
) -> Json<ApiResponse<CollectionStats>> {
    let db = state.db.read().await;
    
    match db.get_collection(&name).await {
        Ok(schema) => {
            let count = db.get_vectors_count(&name).await.unwrap_or(0);
            let stats = CollectionStats {
                name: schema.name,
                dimension: schema.dimension,
                metric: format!("{:?}", schema.distance_metric),
                vector_count: count,
                index_type: "hnsw".to_string(),
            };
            Json(ApiResponse::success(stats))
        }
        Err(e) => Json(ApiResponse::error(&e.to_string())),
    }
}

async fn update_vectors(
    Extension(state): Extension<Arc<ApiState>>,
    axum::extract::Path(name): axum::extract::Path<String>,
    Json(req): Json<UpdateVectorsRequest>,
) -> Json<ApiResponse<UpdateVectorsResponse>> {
    let db = state.db.read().await;
    
    let mut updated_count = 0;
    
    if let Some(vectors) = req.vectors {
        for (i, id) in req.ids.iter().enumerate() {
            if i < vectors.len() {
                if let Ok(Some((_, metadata))) = db.get_vector(&name, id).await {
                    let new_vector = vectors[i].clone();
                    let new_metadata = req.metadata.as_ref().map(|m| m.get(i).cloned()).flatten().unwrap_or(metadata);
                    
                    let _ = db.delete_vectors(&name, &[id.clone()]).await;
                    let _ = db.insert_vectors(&name, vec![(id.clone(), new_vector, new_metadata)]).await;
                    updated_count += 1;
                }
            }
        }
    } else if let Some(metadata) = req.metadata {
        for (i, id) in req.ids.iter().enumerate() {
            if let Ok(Some((vector, _))) = db.get_vector(&name, id).await {
                let new_metadata = metadata.get(i).cloned().unwrap_or(serde_json::json!({}));
                let _ = db.delete_vectors(&name, &[id.clone()]).await;
                let _ = db.insert_vectors(&name, vec![(id.clone(), vector, new_metadata)]).await;
                updated_count += 1;
            }
        }
    }
    
    Json(ApiResponse::success(UpdateVectorsResponse {
        status: "ok".to_string(),
        updated_count,
    }))
}

async fn batch_search(
    Extension(state): Extension<Arc<ApiState>>,
    axum::extract::Path(name): axum::extract::Path<String>,
    Json(req): Json<BatchSearchRequest>,
) -> Json<ApiResponse<BatchSearchResponse>> {
    let start = std::time::Instant::now();
    let db = state.db.read().await;
    
    let mut all_results: Vec<Vec<SearchResultItem>> = Vec::new();
    
    for query in req.queries {
        match db.search(&name, query, req.k, req.filter.clone()).await {
            Ok(results) => {
                let data_lock = db.data.read().await;
                let collection_data = data_lock.get(&name);
                
                let search_results: Vec<SearchResultItem> = results
                    .into_iter()
                    .map(|r| {
                        let metadata = collection_data
                            .and_then(|cd| cd.get(&r.id))
                            .map(|(_, m)| m.clone());
                        
                        SearchResultItem {
                            id: r.id,
                            score: 1.0 - r.distance,
                            metadata,
                        }
                    })
                    .collect();
                
                all_results.push(search_results);
            }
            Err(_) => {
                all_results.push(Vec::new());
            }
        }
    }
    
    let execution_time = start.elapsed().as_millis() as u64;
    
    Json(ApiResponse::success(BatchSearchResponse {
        results: all_results,
        execution_time_ms: execution_time,
    }))
}
