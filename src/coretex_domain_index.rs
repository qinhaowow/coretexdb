use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use crate::coretex_core::{CoreTexError, Result};
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::coretex_index::{VectorIndex, HNSWIndex, SearchResult};
use crate::coretex_gis::{GeoPoint, GeoBoundingBox, GeoPoint3D, GeoLineString3D, GeoPolygon3D, GeoBoundingBox3D};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DomainDocument {
    pub id: String,
    pub title: String,
    pub content: String,
    pub category: String,
    pub sub_category: String,
    pub tags: Vec<String>,
    pub vector: Vec<f32>,
    pub metadata: HashMap<String, String>,
    pub timestamp: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DomainSearchResult {
    pub id: String,
    pub title: String,
    pub content: String,
    pub category: String,
    pub score: f32,
    pub distance: f32,
    pub metadata: HashMap<String, String>,
}

#[async_trait]
pub trait DomainIndex: Send + Sync {
    async fn index_document(&self, doc: DomainDocument) -> Result<()>;
    async fn search(&self, query: &[f32], top_k: usize) -> Result<Vec<DomainSearchResult>>;
    async fn search_by_category(&self, query: &[f32], category: &str, top_k: usize) -> Result<Vec<DomainSearchResult>>;
    async fn remove(&self, id: &str) -> Result<bool>;
    async fn clear(&self) -> Result<()>;
    fn domain_name(&self) -> &str;
}

pub enum DomainIndexEnum {
    NewsWeather(NewsWeatherIndex),
    GeoLocation(GeoLocationIndex),
    Financial(FinancialIndex),
    Knowledge(KnowledgeIndex),
}

impl DomainIndexEnum {
    pub fn domain_name(&self) -> &str {
        match self {
            DomainIndexEnum::NewsWeather(idx) => idx.domain_name(),
            DomainIndexEnum::GeoLocation(idx) => idx.domain_name(),
            DomainIndexEnum::Financial(idx) => idx.domain_name(),
            DomainIndexEnum::Knowledge(idx) => idx.domain_name(),
        }
    }

    pub async fn index_document(&self, doc: DomainDocument) -> Result<()> {
        match self {
            DomainIndexEnum::NewsWeather(idx) => idx.index_document(doc).await,
            DomainIndexEnum::GeoLocation(idx) => idx.index_document(doc).await,
            DomainIndexEnum::Financial(idx) => idx.index_document(doc).await,
            DomainIndexEnum::Knowledge(idx) => idx.index_document(doc).await,
        }
    }

    pub async fn search(&self, query: &[f32], top_k: usize) -> Result<Vec<DomainSearchResult>> {
        match self {
            DomainIndexEnum::NewsWeather(idx) => idx.search(query, top_k).await,
            DomainIndexEnum::GeoLocation(idx) => idx.search(query, top_k).await,
            DomainIndexEnum::Financial(idx) => idx.search(query, top_k).await,
            DomainIndexEnum::Knowledge(idx) => idx.search(query, top_k).await,
        }
    }

    pub async fn search_by_category(&self, query: &[f32], category: &str, top_k: usize) -> Result<Vec<DomainSearchResult>> {
        match self {
            DomainIndexEnum::NewsWeather(idx) => idx.search_by_category(query, category, top_k).await,
            DomainIndexEnum::GeoLocation(idx) => idx.search_by_category(query, category, top_k).await,
            DomainIndexEnum::Financial(idx) => idx.search_by_category(query, category, top_k).await,
            DomainIndexEnum::Knowledge(idx) => idx.search_by_category(query, category, top_k).await,
        }
    }

    pub async fn remove(&self, id: &str) -> Result<bool> {
        match self {
            DomainIndexEnum::NewsWeather(idx) => idx.remove(id).await,
            DomainIndexEnum::GeoLocation(idx) => idx.remove(id).await,
            DomainIndexEnum::Financial(idx) => idx.remove(id).await,
            DomainIndexEnum::Knowledge(idx) => idx.remove(id).await,
        }
    }

    pub async fn clear(&self) -> Result<()> {
        match self {
            DomainIndexEnum::NewsWeather(idx) => idx.clear().await,
            DomainIndexEnum::GeoLocation(idx) => idx.clear().await,
            DomainIndexEnum::Financial(idx) => idx.clear().await,
            DomainIndexEnum::Knowledge(idx) => idx.clear().await,
        }
    }

    pub fn as_news_weather(&self) -> Option<&NewsWeatherIndex> {
        match self {
            DomainIndexEnum::NewsWeather(idx) => Some(idx),
            _ => None,
        }
    }

    pub fn as_geo_location(&self) -> Option<&GeoLocationIndex> {
        match self {
            DomainIndexEnum::GeoLocation(idx) => Some(idx),
            _ => None,
        }
    }

    pub fn as_financial(&self) -> Option<&FinancialIndex> {
        match self {
            DomainIndexEnum::Financial(idx) => Some(idx),
            _ => None,
        }
    }

    pub fn as_knowledge(&self) -> Option<&KnowledgeIndex> {
        match self {
            DomainIndexEnum::Knowledge(idx) => Some(idx),
            _ => None,
        }
    }
}

pub struct NewsWeatherIndex {
    domain: String,
    vector_index: Arc<RwLock<Box<dyn VectorIndex>>>,
    documents: Arc<RwLock<HashMap<String, DomainDocument>>>,
    category_index: Arc<RwLock<HashMap<String, Vec<String>>>>,
    tag_index: Arc<RwLock<HashMap<String, Vec<String>>>>,
}

impl NewsWeatherIndex {
    pub fn new() -> Self {
        Self {
            domain: "news_weather".to_string(),
            vector_index: Arc::new(RwLock::new(Box::new(HNSWIndex::new("cosine")))),
            documents: Arc::new(RwLock::new(HashMap::new())),
            category_index: Arc::new(RwLock::new(HashMap::new())),
            tag_index: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn search_by_tag(&self, tag: &str, top_k: usize) -> Vec<DomainSearchResult> {
        let tag_idx = self.tag_index.read().await;
        let docs = self.documents.read().await;

        if let Some(ids) = tag_idx.get(tag) {
            let mut results: Vec<DomainSearchResult> = ids.iter()
                .filter_map(|id| docs.get(id))
                .take(top_k)
                .map(|doc| DomainSearchResult {
                    id: doc.id.clone(),
                    title: doc.title.clone(),
                    content: doc.content.clone(),
                    category: doc.category.clone(),
                    score: 1.0,
                    distance: 0.0,
                    metadata: doc.metadata.clone(),
                })
                .collect();
            results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
            results
        } else {
            Vec::new()
        }
    }

    pub async fn search_by_date_range(&self, start_ts: i64, end_ts: i64, top_k: usize) -> Vec<DomainSearchResult> {
        let docs = self.documents.read().await;
        let mut results: Vec<DomainSearchResult> = docs.values()
            .filter(|doc| doc.timestamp >= start_ts && doc.timestamp <= end_ts)
            .take(top_k)
            .map(|doc| DomainSearchResult {
                id: doc.id.clone(),
                title: doc.title.clone(),
                content: doc.content.clone(),
                category: doc.category.clone(),
                score: 1.0,
                distance: 0.0,
                metadata: doc.metadata.clone(),
            })
            .collect();
        results.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
        results
    }
}

#[async_trait]
impl DomainIndex for NewsWeatherIndex {
    async fn index_document(&self, doc: DomainDocument) -> Result<()> {
        let mut docs = self.documents.write().await;
        docs.insert(doc.id.clone(), doc.clone());

        let mut cat_idx = self.category_index.write().await;
        cat_idx.entry(doc.category.clone()).or_default().push(doc.id.clone());

        let mut tag_idx = self.tag_index.write().await;
        for tag in &doc.tags {
            tag_idx.entry(tag.clone()).or_default().push(doc.id.clone());
        }

        let mut index = self.vector_index.write().await;
        index.add(&doc.id, &doc.vector).await
    }

    async fn search(&self, query: &[f32], top_k: usize) -> Result<Vec<DomainSearchResult>> {
        let index = self.vector_index.read().await;
        let results = index.search(query, top_k).await?;
        let docs = self.documents.read().await;

        Ok(results.into_iter().filter_map(|r| {
            docs.get(&r.id).map(|doc| DomainSearchResult {
                id: doc.id.clone(),
                title: doc.title.clone(),
                content: doc.content.clone(),
                category: doc.category.clone(),
                score: 1.0 - r.distance,
                distance: r.distance,
                metadata: doc.metadata.clone(),
            })
        }).collect())
    }

    async fn search_by_category(&self, query: &[f32], category: &str, top_k: usize) -> Result<Vec<DomainSearchResult>> {
        let index = self.vector_index.read().await;
        let all_results = index.search(query, top_k * 10).await?;
        let docs = self.documents.read().await;

        let mut results: Vec<DomainSearchResult> = all_results.into_iter()
            .filter_map(|r| {
                docs.get(&r.id).filter(|doc| doc.category == category).map(|doc| DomainSearchResult {
                    id: doc.id.clone(),
                    title: doc.title.clone(),
                    content: doc.content.clone(),
                    category: doc.category.clone(),
                    score: 1.0 - r.distance,
                    distance: r.distance,
                    metadata: doc.metadata.clone(),
                })
            })
            .take(top_k)
            .collect();

        results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
        Ok(results)
    }

    async fn remove(&self, id: &str) -> Result<bool> {
        let mut docs = self.documents.write().await;
        if docs.remove(id).is_some() {
            let mut index = self.vector_index.write().await;
            index.remove(id).await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn clear(&self) -> Result<()> {
        let mut index = self.vector_index.write().await;
        index.clear().await?;
        self.documents.write().await.clear();
        self.category_index.write().await.clear();
        self.tag_index.write().await.clear();
        Ok(())
    }

    fn domain_name(&self) -> &str {
        &self.domain
    }
}

pub struct GeoLocationIndex {
    domain: String,
    vector_index: Arc<RwLock<Box<dyn VectorIndex>>>,
    documents: Arc<RwLock<HashMap<String, DomainDocument>>>,
    spatial_index: Arc<RwLock<HashMap<String, GeoPoint>>>,
    rtree: Arc<RwLock<Vec<(GeoPoint, String)>>>,
}

impl GeoLocationIndex {
    pub fn new() -> Self {
        Self {
            domain: "geo_location".to_string(),
            vector_index: Arc::new(RwLock::new(Box::new(HNSWIndex::new("cosine")))),
            documents: Arc::new(RwLock::new(HashMap::new())),
            spatial_index: Arc::new(RwLock::new(HashMap::new())),
            rtree: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub async fn search_nearby(&self, lat: f64, lon: f64, radius_km: f64, top_k: usize) -> Vec<DomainSearchResult> {
        let point = GeoPoint::new(lat, lon);
        let (min_lat, max_lat, min_lon, max_lon) = point.bounding_box(radius_km);
        let docs = self.documents.read().await;
        let spatial = self.spatial_index.read().await;

        let mut results: Vec<(f64, DomainSearchResult)> = spatial.iter()
            .filter(|(_, gp)| {
                gp.latitude >= min_lat && gp.latitude <= max_lat &&
                gp.longitude >= min_lon && gp.longitude <= max_lon
            })
            .filter_map(|(id, gp)| {
                let dist = point.distance_to(gp);
                if dist <= radius_km {
                    docs.get(id).map(|doc| {
                        let score = 1.0 - (dist / radius_km) as f32;
                        (dist, DomainSearchResult {
                            id: doc.id.clone(),
                            title: doc.title.clone(),
                            content: doc.content.clone(),
                            category: doc.category.clone(),
                            score,
                            distance: dist as f32,
                            metadata: doc.metadata.clone(),
                        })
                    })
                } else {
                    None
                }
            })
            .collect();

        results.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        results.into_iter().take(top_k).map(|(_, r)| r).collect()
    }

    pub async fn search_in_bounds(&self, bounds: &GeoBoundingBox, top_k: usize) -> Vec<DomainSearchResult> {
        let docs = self.documents.read().await;
        let spatial = self.spatial_index.read().await;

        let mut results: Vec<DomainSearchResult> = spatial.iter()
            .filter(|(_, gp)| {
                gp.latitude >= bounds.min_lat && gp.latitude <= bounds.max_lat &&
                gp.longitude >= bounds.min_lon && gp.longitude <= bounds.max_lon
            })
            .filter_map(|(id, _)| {
                docs.get(id).map(|doc| DomainSearchResult {
                    id: doc.id.clone(),
                    title: doc.title.clone(),
                    content: doc.content.clone(),
                    category: doc.category.clone(),
                    score: 1.0,
                    distance: 0.0,
                    metadata: doc.metadata.clone(),
                })
            })
            .take(top_k)
            .collect();

        results
    }

    pub async fn search_nearby_3d(&self, center: &GeoPoint3D, radius: f64, top_k: usize) -> Vec<DomainSearchResult> {
        let (min_x, max_x, min_y, max_y, min_z, max_z) = center.bounding_box(radius);
        let docs = self.documents.read().await;

        let mut results: Vec<(f64, DomainSearchResult)> = docs.iter()
            .filter_map(|(id, doc)| {
                let x = doc.metadata.get("x").and_then(|v| v.parse::<f64>().ok())?;
                let y = doc.metadata.get("y").and_then(|v| v.parse::<f64>().ok())?;
                let z = doc.metadata.get("z").and_then(|v| v.parse::<f64>().ok())?;

                if x < min_x || x > max_x || y < min_y || y > max_y || z < min_z || z > max_z {
                    return None;
                }

                let p = GeoPoint3D::new(x, y, z);
                let dist = center.distance_to(&p);
                if dist <= radius {
                    let score = 1.0 - (dist / radius) as f32;
                    Some((dist, DomainSearchResult {
                        id: id.clone(),
                        title: doc.title.clone(),
                        content: doc.content.clone(),
                        category: doc.category.clone(),
                        score,
                        distance: dist as f32,
                        metadata: doc.metadata.clone(),
                    }))
                } else {
                    None
                }
            })
            .collect();

        results.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        results.into_iter().take(top_k).map(|(_, r)| r).collect()
    }

    pub async fn search_in_bounds_3d(&self, bounds: &GeoBoundingBox3D, top_k: usize) -> Vec<DomainSearchResult> {
        let docs = self.documents.read().await;

        let mut results: Vec<DomainSearchResult> = docs.iter()
            .filter_map(|(id, doc)| {
                let x = doc.metadata.get("x").and_then(|v| v.parse::<f64>().ok())?;
                let y = doc.metadata.get("y").and_then(|v| v.parse::<f64>().ok())?;
                let z = doc.metadata.get("z").and_then(|v| v.parse::<f64>().ok())?;

                let p = GeoPoint3D::new(x, y, z);
                if bounds.contains(&p) {
                    Some(DomainSearchResult {
                        id: id.clone(),
                        title: doc.title.clone(),
                        content: doc.content.clone(),
                        category: doc.category.clone(),
                        score: 1.0,
                        distance: 0.0,
                        metadata: doc.metadata.clone(),
                    })
                } else {
                    None
                }
            })
            .take(top_k)
            .collect();

        results
    }

    pub async fn distance_to_line_3d(&self, line: &GeoLineString3D, top_k: usize) -> Vec<DomainSearchResult> {
        let docs = self.documents.read().await;

        let mut results: Vec<(f64, DomainSearchResult)> = docs.iter()
            .filter_map(|(id, doc)| {
                let x = doc.metadata.get("x").and_then(|v| v.parse::<f64>().ok())?;
                let y = doc.metadata.get("y").and_then(|v| v.parse::<f64>().ok())?;
                let z = doc.metadata.get("z").and_then(|v| v.parse::<f64>().ok())?;

                let p = GeoPoint3D::new(x, y, z);
                let dist = line.distance_to_point(&p);
                let score = if dist > 0.0 { 1.0 / (1.0 + dist) as f32 } else { 1.0 };
                Some((dist, DomainSearchResult {
                    id: id.clone(),
                    title: doc.title.clone(),
                    content: doc.content.clone(),
                    category: doc.category.clone(),
                    score,
                    distance: dist as f32,
                    metadata: doc.metadata.clone(),
                }))
            })
            .collect();

        results.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        results.into_iter().take(top_k).map(|(_, r)| r).collect()
    }

    pub async fn distance_to_polygon_3d(&self, polygon: &GeoPolygon3D, top_k: usize) -> Vec<DomainSearchResult> {
        let docs = self.documents.read().await;

        let mut results: Vec<(f64, DomainSearchResult)> = docs.iter()
            .filter_map(|(id, doc)| {
                let x = doc.metadata.get("x").and_then(|v| v.parse::<f64>().ok())?;
                let y = doc.metadata.get("y").and_then(|v| v.parse::<f64>().ok())?;
                let z = doc.metadata.get("z").and_then(|v| v.parse::<f64>().ok())?;

                let p = GeoPoint3D::new(x, y, z);
                let dist = polygon.distance_to_point(&p);
                let score = if dist > 0.0 { 1.0 / (1.0 + dist) as f32 } else { 1.0 };
                Some((dist, DomainSearchResult {
                    id: id.clone(),
                    title: doc.title.clone(),
                    content: doc.content.clone(),
                    category: doc.category.clone(),
                    score,
                    distance: dist as f32,
                    metadata: doc.metadata.clone(),
                }))
            })
            .collect();

        results.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        results.into_iter().take(top_k).map(|(_, r)| r).collect()
    }
}

#[async_trait]
impl DomainIndex for GeoLocationIndex {
    async fn index_document(&self, doc: DomainDocument) -> Result<()> {
        let mut docs = self.documents.write().await;
        docs.insert(doc.id.clone(), doc.clone());

        if let (Some(lat_str), Some(lon_str)) = (doc.metadata.get("latitude"), doc.metadata.get("longitude")) {
            if let (Ok(lat), Ok(lon)) = (lat_str.parse::<f64>(), lon_str.parse::<f64>()) {
                let point = GeoPoint::new(lat, lon);
                self.spatial_index.write().await.insert(doc.id.clone(), point.clone());
                self.rtree.write().await.push((point, doc.id.clone()));
            }
        }

        let mut index = self.vector_index.write().await;
        index.add(&doc.id, &doc.vector).await
    }

    async fn search(&self, query: &[f32], top_k: usize) -> Result<Vec<DomainSearchResult>> {
        let index = self.vector_index.read().await;
        let results = index.search(query, top_k).await?;
        let docs = self.documents.read().await;

        Ok(results.into_iter().filter_map(|r| {
            docs.get(&r.id).map(|doc| DomainSearchResult {
                id: doc.id.clone(),
                title: doc.title.clone(),
                content: doc.content.clone(),
                category: doc.category.clone(),
                score: 1.0 - r.distance,
                distance: r.distance,
                metadata: doc.metadata.clone(),
            })
        }).collect())
    }

    async fn search_by_category(&self, query: &[f32], category: &str, top_k: usize) -> Result<Vec<DomainSearchResult>> {
        let index = self.vector_index.read().await;
        let all_results = index.search(query, top_k * 10).await?;
        let docs = self.documents.read().await;

        let mut results: Vec<DomainSearchResult> = all_results.into_iter()
            .filter_map(|r| {
                docs.get(&r.id).filter(|doc| doc.category == category).map(|doc| DomainSearchResult {
                    id: doc.id.clone(),
                    title: doc.title.clone(),
                    content: doc.content.clone(),
                    category: doc.category.clone(),
                    score: 1.0 - r.distance,
                    distance: r.distance,
                    metadata: doc.metadata.clone(),
                })
            })
            .take(top_k)
            .collect();

        results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
        Ok(results)
    }

    async fn remove(&self, id: &str) -> Result<bool> {
        let mut docs = self.documents.write().await;
        if docs.remove(id).is_some() {
            let mut index = self.vector_index.write().await;
            index.remove(id).await?;
            self.spatial_index.write().await.remove(id);
            self.rtree.write().await.retain(|(_, i)| i != id);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn clear(&self) -> Result<()> {
        let mut index = self.vector_index.write().await;
        index.clear().await?;
        self.documents.write().await.clear();
        self.spatial_index.write().await.clear();
        self.rtree.write().await.clear();
        Ok(())
    }

    fn domain_name(&self) -> &str {
        &self.domain
    }
}

pub struct FinancialIndex {
    domain: String,
    vector_index: Arc<RwLock<Box<dyn VectorIndex>>>,
    documents: Arc<RwLock<HashMap<String, DomainDocument>>>,
    symbol_index: Arc<RwLock<HashMap<String, Vec<String>>>>,
    date_index: Arc<RwLock<HashMap<String, Vec<String>>>>,
}

impl FinancialIndex {
    pub fn new() -> Self {
        Self {
            domain: "financial".to_string(),
            vector_index: Arc::new(RwLock::new(Box::new(HNSWIndex::new("cosine")))),
            documents: Arc::new(RwLock::new(HashMap::new())),
            symbol_index: Arc::new(RwLock::new(HashMap::new())),
            date_index: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn search_by_symbol(&self, symbol: &str, top_k: usize) -> Vec<DomainSearchResult> {
        let sym_idx = self.symbol_index.read().await;
        let docs = self.documents.read().await;

        sym_idx.get(symbol)
            .map(|ids| {
                let mut results: Vec<DomainSearchResult> = ids.iter()
                    .filter_map(|id| docs.get(id))
                    .take(top_k)
                    .map(|doc| DomainSearchResult {
                        id: doc.id.clone(),
                        title: doc.title.clone(),
                        content: doc.content.clone(),
                        category: doc.category.clone(),
                        score: 1.0,
                        distance: 0.0,
                        metadata: doc.metadata.clone(),
                    })
                    .collect();
                results.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
                results
            })
            .unwrap_or_default()
    }

    pub async fn search_by_date(&self, date: &str, top_k: usize) -> Vec<DomainSearchResult> {
        let date_idx = self.date_index.read().await;
        let docs = self.documents.read().await;

        date_idx.get(date)
            .map(|ids| {
                ids.iter()
                    .filter_map(|id| docs.get(id))
                    .take(top_k)
                    .map(|doc| DomainSearchResult {
                        id: doc.id.clone(),
                        title: doc.title.clone(),
                        content: doc.content.clone(),
                        category: doc.category.clone(),
                        score: 1.0,
                        distance: 0.0,
                        metadata: doc.metadata.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default()
    }
}

#[async_trait]
impl DomainIndex for FinancialIndex {
    async fn index_document(&self, doc: DomainDocument) -> Result<()> {
        let mut docs = self.documents.write().await;
        docs.insert(doc.id.clone(), doc.clone());

        if let Some(symbol) = doc.metadata.get("symbol") {
            self.symbol_index.write().await.entry(symbol.clone()).or_default().push(doc.id.clone());
        }
        if let Some(date) = doc.metadata.get("date") {
            self.date_index.write().await.entry(date.clone()).or_default().push(doc.id.clone());
        }

        let mut index = self.vector_index.write().await;
        index.add(&doc.id, &doc.vector).await
    }

    async fn search(&self, query: &[f32], top_k: usize) -> Result<Vec<DomainSearchResult>> {
        let index = self.vector_index.read().await;
        let results = index.search(query, top_k).await?;
        let docs = self.documents.read().await;

        Ok(results.into_iter().filter_map(|r| {
            docs.get(&r.id).map(|doc| DomainSearchResult {
                id: doc.id.clone(),
                title: doc.title.clone(),
                content: doc.content.clone(),
                category: doc.category.clone(),
                score: 1.0 - r.distance,
                distance: r.distance,
                metadata: doc.metadata.clone(),
            })
        }).collect())
    }

    async fn search_by_category(&self, query: &[f32], category: &str, top_k: usize) -> Result<Vec<DomainSearchResult>> {
        let index = self.vector_index.read().await;
        let all_results = index.search(query, top_k * 10).await?;
        let docs = self.documents.read().await;

        let mut results: Vec<DomainSearchResult> = all_results.into_iter()
            .filter_map(|r| {
                docs.get(&r.id).filter(|doc| doc.category == category).map(|doc| DomainSearchResult {
                    id: doc.id.clone(),
                    title: doc.title.clone(),
                    content: doc.content.clone(),
                    category: doc.category.clone(),
                    score: 1.0 - r.distance,
                    distance: r.distance,
                    metadata: doc.metadata.clone(),
                })
            })
            .take(top_k)
            .collect();

        results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
        Ok(results)
    }

    async fn remove(&self, id: &str) -> Result<bool> {
        let mut docs = self.documents.write().await;
        if let Some(doc) = docs.remove(id) {
            let mut index = self.vector_index.write().await;
            index.remove(id).await?;

            if let Some(symbol) = doc.metadata.get("symbol") {
                if let Some(ids) = self.symbol_index.write().await.get_mut(symbol) {
                    ids.retain(|i| i != id);
                }
            }
            if let Some(date) = doc.metadata.get("date") {
                if let Some(ids) = self.date_index.write().await.get_mut(date) {
                    ids.retain(|i| i != id);
                }
            }

            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn clear(&self) -> Result<()> {
        let mut index = self.vector_index.write().await;
        index.clear().await?;
        self.documents.write().await.clear();
        self.symbol_index.write().await.clear();
        self.date_index.write().await.clear();
        Ok(())
    }

    fn domain_name(&self) -> &str {
        &self.domain
    }
}

pub struct KnowledgeIndex {
    domain: String,
    vector_index: Arc<RwLock<Box<dyn VectorIndex>>>,
    documents: Arc<RwLock<HashMap<String, DomainDocument>>>,
    category_index: Arc<RwLock<HashMap<String, Vec<String>>>>,
    sub_category_index: Arc<RwLock<HashMap<String, Vec<String>>>>,
    tag_index: Arc<RwLock<HashMap<String, Vec<String>>>>,
}

impl KnowledgeIndex {
    pub fn new() -> Self {
        Self {
            domain: "knowledge".to_string(),
            vector_index: Arc::new(RwLock::new(Box::new(HNSWIndex::new("cosine")))),
            documents: Arc::new(RwLock::new(HashMap::new())),
            category_index: Arc::new(RwLock::new(HashMap::new())),
            sub_category_index: Arc::new(RwLock::new(HashMap::new())),
            tag_index: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn search_by_sub_category(&self, sub_category: &str, top_k: usize) -> Vec<DomainSearchResult> {
        let sub_idx = self.sub_category_index.read().await;
        let docs = self.documents.read().await;

        sub_idx.get(sub_category)
            .map(|ids| {
                ids.iter()
                    .filter_map(|id| docs.get(id))
                    .take(top_k)
                    .map(|doc| DomainSearchResult {
                        id: doc.id.clone(),
                        title: doc.title.clone(),
                        content: doc.content.clone(),
                        category: doc.category.clone(),
                        score: 1.0,
                        distance: 0.0,
                        metadata: doc.metadata.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    pub async fn search_by_tag(&self, tag: &str, top_k: usize) -> Vec<DomainSearchResult> {
        let tag_idx = self.tag_index.read().await;
        let docs = self.documents.read().await;

        tag_idx.get(tag)
            .map(|ids| {
                ids.iter()
                    .filter_map(|id| docs.get(id))
                    .take(top_k)
                    .map(|doc| DomainSearchResult {
                        id: doc.id.clone(),
                        title: doc.title.clone(),
                        content: doc.content.clone(),
                        category: doc.category.clone(),
                        score: 1.0,
                        distance: 0.0,
                        metadata: doc.metadata.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default()
    }
}

#[async_trait]
impl DomainIndex for KnowledgeIndex {
    async fn index_document(&self, doc: DomainDocument) -> Result<()> {
        let mut docs = self.documents.write().await;
        docs.insert(doc.id.clone(), doc.clone());

        let mut cat_idx = self.category_index.write().await;
        cat_idx.entry(doc.category.clone()).or_default().push(doc.id.clone());

        let mut sub_idx = self.sub_category_index.write().await;
        sub_idx.entry(doc.sub_category.clone()).or_default().push(doc.id.clone());

        let mut tag_idx = self.tag_index.write().await;
        for tag in &doc.tags {
            tag_idx.entry(tag.clone()).or_default().push(doc.id.clone());
        }

        let mut index = self.vector_index.write().await;
        index.add(&doc.id, &doc.vector).await
    }

    async fn search(&self, query: &[f32], top_k: usize) -> Result<Vec<DomainSearchResult>> {
        let index = self.vector_index.read().await;
        let results = index.search(query, top_k).await?;
        let docs = self.documents.read().await;

        Ok(results.into_iter().filter_map(|r| {
            docs.get(&r.id).map(|doc| DomainSearchResult {
                id: doc.id.clone(),
                title: doc.title.clone(),
                content: doc.content.clone(),
                category: doc.category.clone(),
                score: 1.0 - r.distance,
                distance: r.distance,
                metadata: doc.metadata.clone(),
            })
        }).collect())
    }

    async fn search_by_category(&self, query: &[f32], category: &str, top_k: usize) -> Result<Vec<DomainSearchResult>> {
        let cat_idx = self.category_index.read().await;
        let docs = self.documents.read().await;

        let candidate_ids: Vec<String> = cat_idx.get(category)
            .map(|ids| ids.clone())
            .unwrap_or_default();

        let index = self.vector_index.read().await;
        let all_results = index.search(query, top_k * 10).await?;

        let mut results: Vec<DomainSearchResult> = all_results.into_iter()
            .filter(|r| candidate_ids.contains(&r.id))
            .filter_map(|r| {
                docs.get(&r.id).map(|doc| DomainSearchResult {
                    id: doc.id.clone(),
                    title: doc.title.clone(),
                    content: doc.content.clone(),
                    category: doc.category.clone(),
                    score: 1.0 - r.distance,
                    distance: r.distance,
                    metadata: doc.metadata.clone(),
                })
            })
            .take(top_k)
            .collect();

        results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
        Ok(results)
    }

    async fn remove(&self, id: &str) -> Result<bool> {
        let mut docs = self.documents.write().await;
        if let Some(doc) = docs.remove(id) {
            let mut index = self.vector_index.write().await;
            index.remove(id).await?;

            if let Some(ids) = self.category_index.write().await.get_mut(&doc.category) {
                ids.retain(|i| i != id);
            }
            if let Some(ids) = self.sub_category_index.write().await.get_mut(&doc.sub_category) {
                ids.retain(|i| i != id);
            }
            for tag in &doc.tags {
                if let Some(ids) = self.tag_index.write().await.get_mut(tag) {
                    ids.retain(|i| i != id);
                }
            }

            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn clear(&self) -> Result<()> {
        let mut index = self.vector_index.write().await;
        index.clear().await?;
        self.documents.write().await.clear();
        self.category_index.write().await.clear();
        self.sub_category_index.write().await.clear();
        self.tag_index.write().await.clear();
        Ok(())
    }

    fn domain_name(&self) -> &str {
        &self.domain
    }
}

pub struct DomainIndexManager {
    indexes: Arc<RwLock<HashMap<String, DomainIndexEnum>>>,
}

impl DomainIndexManager {
    pub fn new() -> Self {
        Self {
            indexes: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn register_index(&self, name: &str, index: DomainIndexEnum) {
        self.indexes.write().await.insert(name.to_string(), index);
    }

    pub async fn get_index(&self, name: &str) -> Option<DomainIndexEnum> {
        self.indexes.read().await.get(name).cloned()
    }

    pub async fn list_indexes(&self) -> Vec<String> {
        self.indexes.read().await.keys().cloned().collect()
    }

    pub async fn remove_index(&self, name: &str) -> bool {
        self.indexes.write().await.remove(name).is_some()
    }

    pub async fn initialize_defaults(&self) {
        let mut indexes = self.indexes.write().await;
        indexes.insert("news_weather".to_string(), DomainIndexEnum::NewsWeather(NewsWeatherIndex::new()));
        indexes.insert("geo_location".to_string(), DomainIndexEnum::GeoLocation(GeoLocationIndex::new()));
        indexes.insert("financial".to_string(), DomainIndexEnum::Financial(FinancialIndex::new()));
        indexes.insert("knowledge".to_string(), DomainIndexEnum::Knowledge(KnowledgeIndex::new()));
    }
}
