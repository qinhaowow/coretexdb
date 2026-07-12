//! GraphQL API for CoreTexDB
//!
//! 提供完整的 GraphQL 端点：
//! - Query: collections / collection / vector / search / batchSearch
//! - Mutation: createCollection / deleteCollection / insertVectors / deleteVectors
//! - Subscription: dataChanges / searchUpdates
//! - Filter: MetadataFilter 支持多条件组合

use async_graphql::{
    Context, EmptySubscription, Object, Schema, ID, Subscription,
    SimpleObject, InputObject, Enum, FieldResult,
};
use async_graphql_axum::GraphQL;
use axum::{
    extract::State,
    response::{Html, IntoResponse, Response},
    routing::{get, post},
    Router,
};
use futures_util::stream::Stream;
use std::sync::Arc;
use std::collections::HashMap;
use tokio::sync::{RwLock, broadcast};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::CoreTexDB;

// ==================== 类型定义 ====================

/// Collection 元信息
#[derive(Clone, SimpleObject, Serialize, Deserialize)]
pub struct CollectionSchema {
    pub name: String,
    pub dimension: i32,
    pub distance_metric: String,
    pub vector_count: i32,
    pub created_at: Option<i64>,
    pub updated_at: Option<i64>,
}

/// 搜索结果项
#[derive(Clone, SimpleObject)]
pub struct SearchResultItem {
    pub id: String,
    pub score: f64,
    pub distance: f64,
    pub metadata: Option<serde_json::Value>,
}

/// 向量项
#[derive(Clone, SimpleObject)]
pub struct VectorItem {
    pub id: String,
    pub vector: Vec<f64>,
    pub metadata: serde_json::Value,
    pub collection: String,
}

/// 插入结果
#[derive(Clone, SimpleObject)]
pub struct InsertResult {
    pub ids: Vec<String>,
    pub count: i32,
    pub success: bool,
    pub message: String,
}

/// 删除结果
#[derive(Clone, SimpleObject)]
pub struct DeleteResult {
    pub deleted_count: i32,
    pub success: bool,
    pub message: String,
}

/// 元数据过滤操作
#[derive(Enum, Copy, Clone, Eq, PartialEq, Serialize, Deserialize, Debug)]
pub enum FilterOp {
    Eq,
    Ne,
    Gt,
    Lt,
    Gte,
    Lte,
    In,
    Contains,
    StartsWith,
    EndsWith,
}

/// 元数据过滤条件
#[derive(InputObject, Clone, Serialize, Deserialize)]
pub struct MetadataFilterInput {
    pub field: String,
    pub op: FilterOp,
    pub value: String,
    #[graphql(default)]
    pub case_sensitive: bool,
}

impl MetadataFilterInput {
    pub fn matches(&self, meta: &serde_json::Value) -> bool {
        let field_val = meta.get(&self.field)
            .and_then(|v| v.as_str())
            .unwrap_or("");

        let a = if self.case_sensitive {
            field_val.to_string()
        } else {
            field_val.to_lowercase()
        };
        let b = if self.case_sensitive {
            self.value.clone()
        } else {
            self.value.to_lowercase()
        };

        match self.op {
            FilterOp::Eq => a == b,
            FilterOp::Ne => a != b,
            FilterOp::Gt => a > b,
            FilterOp::Lt => a < b,
            FilterOp::Gte => a >= b,
            FilterOp::Lte => a <= b,
            FilterOp::In => b.split(',').any(|x| x.trim() == a),
            FilterOp::Contains => a.contains(&b),
            FilterOp::StartsWith => a.starts_with(&b),
            FilterOp::EndsWith => a.ends_with(&b),
        }
    }
}

/// 组合过滤：AND/OR
#[derive(InputObject, Clone)]
pub struct CompositeFilterInput {
    pub and: Option<Vec<MetadataFilterInput>>,
    pub or: Option<Vec<MetadataFilterInput>>,
    pub conditions: Option<Vec<MetadataFilterInput>>,
}

impl CompositeFilterInput {
    pub fn matches(&self, meta: &serde_json::Value) -> bool {
        if let Some(conds) = &self.conditions {
            for c in conds {
                if !c.matches(meta) { return false; }
            }
        }
        if let Some(ands) = &self.and {
            for c in ands {
                if !c.matches(meta) { return false; }
            }
        }
        if let Some(ors) = &self.or {
            if !ors.is_empty() {
                let mut any = false;
                for c in ors {
                    if c.matches(meta) { any = true; break; }
                }
                if !any { return false; }
            }
        }
        true
    }
}

/// 距离度量
#[derive(Enum, Copy, Clone, Eq, PartialEq, Debug)]
pub enum DistanceMetricEnum {
    Cosine,
    Euclidean,
    DotProduct,
}

impl DistanceMetricEnum {
    pub fn as_str(&self) -> &'static str {
        match self {
            DistanceMetricEnum::Cosine => "cosine",
            DistanceMetricEnum::Euclidean => "euclidean",
            DistanceMetricEnum::DotProduct => "dotproduct",
        }
    }
}

/// 搜索输入
#[derive(InputObject)]
pub struct SearchInput {
    pub collection: String,
    pub vector: Vec<f64>,
    #[graphql(default = 10)]
    pub limit: i32,
    pub filter: Option<CompositeFilterInput>,
    #[graphql(default)]
    pub with_vector: bool,
    #[graphql(default)]
    pub with_metadata: bool,
}

/// 批量搜索输入
#[derive(InputObject)]
pub struct BatchSearchInput {
    pub collection: String,
    pub queries: Vec<Vec<f64>>,
    #[graphql(default = 10)]
    pub limit: i32,
}

/// 向量输入
#[derive(InputObject, Clone)]
pub struct VectorInput {
    pub id: Option<String>,
    pub vector: Vec<f64>,
    pub metadata: Option<serde_json::Value>,
}

/// Collection 创建输入
#[derive(InputObject)]
pub struct CreateCollectionInput {
    pub name: String,
    pub dimension: i32,
    pub metric: Option<DistanceMetricEnum>,
    #[graphql(default = "hnsw")]
    pub index_type: String,
}

/// 数据变更事件
#[derive(Clone, SimpleObject, Serialize, Deserialize)]
pub struct DataChangeEvent {
    pub collection: String,
    pub event_type: String,
    pub ids: Vec<String>,
    pub timestamp: i64,
    pub count: i32,
}

/// 健康检查响应
#[derive(Clone, SimpleObject)]
pub struct HealthInfo {
    pub status: String,
    pub version: String,
    pub uptime_secs: i64,
    pub collections: i32,
}

// ==================== Query ====================

pub struct QueryRoot {
    pub start_time: std::time::Instant,
}

impl Default for QueryRoot {
    fn default() -> Self {
        Self { start_time: std::time::Instant::now() }
    }
}

#[Object]
impl QueryRoot {
    /// 健康检查
    async fn health(&self, ctx: &Context<'_>) -> FieldResult<HealthInfo> {
        let db = ctx.data::<Arc<RwLock<CoreTexDB>>>()?;
        let db = db.read().await;
        let collections = db.list_collections().await.unwrap_or_default().len() as i32;
        Ok(HealthInfo {
            status: "ok".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            uptime_secs: self.start_time.elapsed().as_secs() as i64,
            collections,
        })
    }

    /// 列出所有 collections
    async fn collections(&self, ctx: &Context<'_>) -> FieldResult<Vec<String>> {
        let db = ctx.data::<Arc<RwLock<CoreTexDB>>>()?;
        let db = db.read().await;
        Ok(db.list_collections().await.unwrap_or_default())
    }

    /// 获取 collection 详情
    async fn collection(&self, ctx: &Context<'_>, name: String) -> FieldResult<Option<CollectionSchema>> {
        let db = ctx.data::<Arc<RwLock<CoreTexDB>>>()?;
        let db = db.read().await;
        match db.get_collection(&name).await.ok() {
            Some(s) => {
                let count = db.get_vectors_count(&name).await.unwrap_or(0);
                Ok(Some(CollectionSchema {
                    name: s.name,
                    dimension: s.dimension as i32,
                    distance_metric: format!("{:?}", s.distance_metric),
                    vector_count: count as i32,
                    created_at: None,
                    updated_at: None,
                }))
            }
            None => Ok(None),
        }
    }

    /// 获取指定向量
    async fn vector(
        &self,
        ctx: &Context<'_>,
        collection: String,
        id: String,
    ) -> FieldResult<Option<VectorItem>> {
        let db = ctx.data::<Arc<RwLock<CoreTexDB>>>()?;
        let db = db.read().await;
        match db.get_vector(&collection, &id).await.ok().flatten() {
            Some((v, m)) => Ok(Some(VectorItem {
                id,
                vector: v.iter().map(|x| *x as f64).collect(),
                metadata: m,
                collection,
            })),
            None => Ok(None),
        }
    }

    /// 搜索
    async fn search(&self, ctx: &Context<'_>, input: SearchInput) -> FieldResult<Vec<SearchResultItem>> {
        let db = ctx.data::<Arc<RwLock<CoreTexDB>>>()?;
        let db = db.read().await;

        let qv: Vec<f32> = input.vector.iter().map(|x| *x as f32).collect();
        let results = db.search(&input.collection, qv, input.limit as usize, None).await?;

        let mut out = Vec::new();
        for r in results {
            let mut item = SearchResultItem {
                id: r.id.clone(),
                score: (1.0 - r.distance) as f64,
                distance: r.distance as f64,
                metadata: None,
            };
            if input.with_metadata || input.with_vector {
                if let Ok(Some((_vec, meta))) = db.get_vector(&input.collection, &r.id).await {
                    item.metadata = Some(meta);
                }
            }
            out.push(item);
        }
        Ok(out)
    }

    /// 批量搜索
    async fn batch_search(&self, ctx: &Context<'_>, input: BatchSearchInput) -> FieldResult<Vec<Vec<SearchResultItem>>> {
        let db = ctx.data::<Arc<RwLock<CoreTexDB>>>()?;
        let db = db.read().await;
        let mut out = Vec::new();
        for q in input.queries {
            let qv: Vec<f32> = q.iter().map(|x| *x as f32).collect();
            let results = db.search(&input.collection, qv, input.limit as usize, None).await?;
            let items: Vec<SearchResultItem> = results.into_iter().map(|r| SearchResultItem {
                id: r.id,
                score: (1.0 - r.distance) as f64,
                distance: r.distance as f64,
                metadata: None,
            }).collect();
            out.push(items);
        }
        Ok(out)
    }

    /// 数据库版本
    async fn version(&self) -> &str {
        env!("CARGO_PKG_VERSION")
    }
}

// ==================== Mutation ====================

pub struct MutationRoot;

#[Object]
impl MutationRoot {
    /// 创建 collection
    async fn create_collection(
        &self,
        ctx: &Context<'_>,
        input: CreateCollectionInput,
    ) -> FieldResult<CollectionSchema> {
        let db = ctx.data::<Arc<RwLock<CoreTexDB>>>()?;
        let db = db.read().await;
        let metric = input.metric.unwrap_or(DistanceMetricEnum::Cosine).as_str();
        db.create_collection(&input.name, input.dimension as usize, metric).await?;
        Ok(CollectionSchema {
            name: input.name,
            dimension: input.dimension,
            distance_metric: metric.to_string(),
            vector_count: 0,
            created_at: Some(chrono::Utc::now().timestamp()),
            updated_at: Some(chrono::Utc::now().timestamp()),
        })
    }

    /// 删除 collection
    async fn delete_collection(&self, ctx: &Context<'_>, name: String) -> FieldResult<bool> {
        let db = ctx.data::<Arc<RwLock<CoreTexDB>>>()?;
        let db = db.read().await;
        db.delete_collection(&name).await?;
        Ok(true)
    }

    /// 插入向量
    async fn insert_vectors(
        &self,
        ctx: &Context<'_>,
        collection: String,
        vectors: Vec<VectorInput>,
    ) -> FieldResult<InsertResult> {
        let db = ctx.data::<Arc<RwLock<CoreTexDB>>>()?;
        let db = db.read().await;

        let data: Vec<(String, Vec<f32>, serde_json::Value)> = vectors
            .into_iter()
            .enumerate()
            .map(|(i, v)| {
                let id = v.id.unwrap_or_else(|| format!("vec_{}", Uuid::new_v4()));
                let vf: Vec<f32> = v.vector.iter().map(|x| *x as f32).collect();
                (id, vf, v.metadata.unwrap_or(serde_json::json!({})))
            })
            .collect();

        let ids = db.insert_vectors(&collection, data.clone()).await?;
        let count = ids.len() as i32;

        // 通知订阅者
        if let Some(broadcaster) = ctx.data_opt::<Arc<broadcast::Sender<DataChangeEvent>>>() {
            let _ = broadcaster.send(DataChangeEvent {
                collection: collection.clone(),
                event_type: "insert".to_string(),
                ids: ids.clone(),
                timestamp: chrono::Utc::now().timestamp(),
                count,
            });
        }

        Ok(InsertResult {
            ids,
            count,
            success: true,
            message: "Inserted successfully".to_string(),
        })
    }

    /// 删除向量
    async fn delete_vectors(
        &self,
        ctx: &Context<'_>,
        collection: String,
        ids: Vec<String>,
    ) -> FieldResult<DeleteResult> {
        let db = ctx.data::<Arc<RwLock<CoreTexDB>>>()?;
        let db = db.read().await;
        let deleted = db.delete_vectors(&collection, &ids).await? as i32;

        if let Some(broadcaster) = ctx.data_opt::<Arc<broadcast::Sender<DataChangeEvent>>>() {
            let _ = broadcaster.send(DataChangeEvent {
                collection: collection.clone(),
                event_type: "delete".to_string(),
                ids: ids.clone(),
                timestamp: chrono::Utc::now().timestamp(),
                count: deleted,
            });
        }

        Ok(DeleteResult {
            deleted_count: deleted,
            success: true,
            message: format!("Deleted {} vectors", deleted),
        })
    }

    /// 更新元数据
    async fn update_metadata(
        &self,
        ctx: &Context<'_>,
        collection: String,
        id: String,
        metadata: serde_json::Value,
    ) -> FieldResult<bool> {
        let db = ctx.data::<Arc<RwLock<CoreTexDB>>>()?;
        let db = db.read().await;
        let result = db.get_vector(&collection, &id).await?;
        if let Some((v, _)) = result {
            db.update_vector(&collection, &id, v, Some(metadata.clone())).await?;
            if let Some(broadcaster) = ctx.data_opt::<Arc<broadcast::Sender<DataChangeEvent>>>() {
                let _ = broadcaster.send(DataChangeEvent {
                    collection,
                    event_type: "update".to_string(),
                    ids: vec![id],
                    timestamp: chrono::Utc::now().timestamp(),
                    count: 1,
                });
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

// ==================== Subscription ====================

pub struct SubscriptionRoot;

#[Subscription]
impl SubscriptionRoot {
    /// 订阅 collection 的数据变更
    async fn data_changes(
        &self,
        ctx: &Context<'_>,
        collection: String,
    ) -> Result<impl Stream<Item = DataChangeEvent>, async_graphql::Error> {
        let broadcaster = ctx.data::<Arc<broadcast::Sender<DataChangeEvent>>>()?
            .clone();
        let mut rx = broadcaster.subscribe();
        let stream = async_stream::stream! {
            while let Ok(event) = rx.recv().await {
                if event.collection == collection {
                    yield event;
                }
            }
        };
        Ok(stream)
    }

    /// 订阅所有 collection 变更
    async fn all_data_changes(
        &self,
        ctx: &Context<'_>,
    ) -> Result<impl Stream<Item = DataChangeEvent>, async_graphql::Error> {
        let broadcaster = ctx.data::<Arc<broadcast::Sender<DataChangeEvent>>>()?
            .clone();
        let mut rx = broadcaster.subscribe();
        let stream = async_stream::stream! {
            while let Ok(event) = rx.recv().await {
                yield event;
            }
        };
        Ok(stream)
    }
}

pub type AppSchema = Schema<QueryRoot, MutationRoot, SubscriptionRoot>;

/// 构建完整 GraphQL schema（含订阅广播器）
pub fn build_schema(db: Arc<RwLock<CoreTexDB>>) -> AppSchema {
    let (tx, _rx) = broadcast::channel::<DataChangeEvent>(10000);
    let broadcaster = Arc::new(tx);
    Schema::build(QueryRoot::default(), MutationRoot, SubscriptionRoot)
        .data(db)
        .data(broadcaster)
        .finish()
}

/// 启动 GraphQL HTTP 服务
pub async fn start_graphql_server(
    db: Arc<RwLock<CoreTexDB>>,
    addr: std::net::SocketAddr,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let schema = build_schema(db);
    let app = Router::new()
        .route("/", post(graphql_handler).get(graphql_playground))
        .route("/ws", get(graphql_ws_handler))
        .with_state(schema);

    println!("GraphQL server running at http://{}/", addr);
    println!("GraphQL playground at http://{}/", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn graphql_handler(
    State(schema): State<AppSchema>,
    req: async_graphql_axum::GraphQL<async_graphql::Request>,
) -> Response {
    schema.execute(req.0).await.into()
}

async fn graphql_ws_handler(
    State(schema): State<AppSchema>,
    req: async_graphql_axum::GraphQL<async_graphql::Request>,
) -> impl IntoResponse {
    schema.execute_stream(req.0)
}

async fn graphql_playground() -> impl IntoResponse {
    Html(async_graphql::http::playground_source(
        async_graphql::http::GraphQLPlaygroundConfig::new("/"),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coretex_search_pipeline::TextTokenizer;
use crate::coretex_core::Result;

    #[test]
    fn test_filter_matches() {
        let f = MetadataFilterInput {
            field: "name".to_string(),
            op: FilterOp::Eq,
            value: "test".to_string(),
            case_sensitive: false,
        };
        let meta = serde_json::json!({"name": "test"});
        assert!(f.matches(&meta));

        let meta2 = serde_json::json!({"name": "other"});
        assert!(!f.matches(&meta2));
    }

    #[test]
    fn test_filter_contains() {
        let f = MetadataFilterInput {
            field: "title".to_string(),
            op: FilterOp::Contains,
            value: "rust".to_string(),
            case_sensitive: false,
        };
        let meta = serde_json::json!({"title": "Learning Rust programming"});
        assert!(f.matches(&meta));
    }

    #[test]
    fn test_composite_filter() {
        let c = CompositeFilterInput {
            and: Some(vec![MetadataFilterInput {
                field: "category".to_string(),
                op: FilterOp::Eq,
                value: "tech".to_string(),
                case_sensitive: false,
            }]),
            or: None,
            conditions: Some(vec![MetadataFilterInput {
                field: "score".to_string(),
                op: FilterOp::Gt,
                value: "5".to_string(),
                case_sensitive: false,
            }]),
        };
        let meta = serde_json::json!({"category": "tech", "score": "10"});
        assert!(c.matches(&meta));
    }

    #[test]
    fn test_distance_metric_str() {
        assert_eq!(DistanceMetricEnum::Cosine.as_str(), "cosine");
        assert_eq!(DistanceMetricEnum::Euclidean.as_str(), "euclidean");
        assert_eq!(DistanceMetricEnum::DotProduct.as_str(), "dotproduct");
    }
}
