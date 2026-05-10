use async_graphql::{
    Context, EmptySubscription, Object, Schema, ID,
};
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::CoreTexDB;

pub struct QueryRoot;

#[Object]
impl QueryRoot {
    async fn collections(&self, ctx: &Context<'_>) -> Vec<String> {
        let db = ctx.data::<Arc<RwLock<CoreTexDB>>>().unwrap();
        let db = db.read().await;
        db.list_collections().await.unwrap_or_default()
    }

    async fn collection(&self, ctx: &Context<'_>, name: String) -> Option<CollectionSchema> {
        let db = ctx.data::<Arc<RwLock<CoreTexDB>>>().unwrap();
        let db = db.read().await;
        db.get_collection(&name).await.ok().map(|s| CollectionSchema {
            name: s.name,
            dimension: s.dimension,
            distance_metric: format!("{:?}", s.distance_metric),
            vector_count: db.get_vectors_count(&name).await.unwrap_or(0),
        })
    }

    async fn search(
        &self,
        ctx: &Context<'_>,
        collection: String,
        vector: Vec<f32>,
        limit: Option<i32>,
    ) -> Vec<SearchResultItem> {
        let db = ctx.data::<Arc<RwLock<CoreTexDB>>>().unwrap();
        let db = db.read().await;
        let k = limit.unwrap_or(10) as usize;
        let results = db.search(&collection, vector, k, None).await.unwrap_or_default();
        results
            .into_iter()
            .map(|r| SearchResultItem {
                id: r.id,
                distance: r.distance,
            })
            .collect()
    }

    async fn vector(
        &self,
        ctx: &Context<'_>,
        collection: String,
        id: String,
    ) -> Option<VectorItem> {
        let db = ctx.data::<Arc<RwLock<CoreTexDB>>>().unwrap();
        let db = db.read().await;
        db.get_vector(&collection, &id).await.ok().flatten().map(
            |(vector, metadata)| VectorItem {
                id,
                vector,
                metadata: metadata.to_string(),
            },
        )
    }
}

pub struct MutationRoot;

#[Object]
impl MutationRoot {
    async fn create_collection(
        &self,
        ctx: &Context<'_>,
        name: String,
        dimension: i32,
        metric: Option<String>,
    ) -> CollectionSchema {
        let db = ctx.data::<Arc<RwLock<CoreTexDB>>>().unwrap();
        let db = db.read().await;
        let metric = metric.unwrap_or_else(|| "cosine".to_string());
        db.create_collection(&name, dimension as usize, &metric)
            .await
            .unwrap();
        CollectionSchema {
            name,
            dimension: dimension as usize,
            distance_metric: metric,
            vector_count: 0,
        }
    }

    async fn delete_collection(&self, ctx: &Context<'_>, name: String) -> bool {
        let db = ctx.data::<Arc<RwLock<CoreTexDB>>>().unwrap();
        let db = db.read().await;
        db.delete_collection(&name).await.is_ok()
    }

    async fn insert_vectors(
        &self,
        ctx: &Context<'_>,
        collection: String,
        vectors: Vec<VectorInput>,
    ) -> InsertResult {
        let db = ctx.data::<Arc<RwLock<CoreTexDB>>>().unwrap();
        let db = db.read().await;
        let data: Vec<(String, Vec<f32>, serde_json::Value)> = vectors
            .into_iter()
            .map(|v| {
                let metadata: serde_json::Value =
                    serde_json::from_str(&v.metadata).unwrap_or(serde_json::json!({}));
                (v.id, v.vector, metadata)
            })
            .collect();
        let ids = db.insert_vectors(&collection, data).await.unwrap_or_default();
        InsertResult { ids }
    }
}

#[derive(Clone)]
pub struct CollectionSchema {
    pub name: String,
    pub dimension: usize,
    pub distance_metric: String,
    pub vector_count: usize,
}

#[Object]
impl CollectionSchema {
    async fn name(&self) -> &str {
        &self.name
    }

    async fn dimension(&self) -> i32 {
        self.dimension as i32
    }

    async fn distance_metric(&self) -> &str {
        &self.distance_metric
    }

    async fn vector_count(&self) -> i32 {
        self.vector_count as i32
    }
}

pub struct SearchResultItem {
    pub id: String,
    pub distance: f32,
}

#[Object]
impl SearchResultItem {
    async fn id(&self) -> &str {
        &self.id
    }

    async fn distance(&self) -> f64 {
        self.distance as f64
    }
}

pub struct VectorItem {
    pub id: String,
    pub vector: Vec<f32>,
    pub metadata: String,
}

#[Object]
impl VectorItem {
    async fn id(&self) -> &str {
        &self.id
    }

    async fn vector(&self) -> &[f32] {
        &self.vector
    }

    async fn metadata(&self) -> &str {
        &self.metadata
    }
}

#[derive(Clone)]
#[async_graphql::InputObject]
pub struct VectorInput {
    pub id: String,
    pub vector: Vec<f32>,
    pub metadata: String,
}

pub struct InsertResult {
    pub ids: Vec<String>,
}

#[Object]
impl InsertResult {
    async fn ids(&self) -> &[String] {
        &self.ids
    }
}

pub type AppSchema = Schema<QueryRoot, MutationRoot, EmptySubscription>;

pub fn build_schema(db: Arc<RwLock<CoreTexDB>>) -> AppSchema {
    Schema::build(QueryRoot, MutationRoot, EmptySubscription)
        .data(db)
        .finish()
}
