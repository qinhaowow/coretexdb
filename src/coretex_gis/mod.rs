//! Geospatial Index (GIS) module for CoreTexDB
//! Supports spatial queries including point-in-polygon, nearest neighbor, and range queries

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Clone)]
pub struct GeoPoint {
    pub latitude: f64,
    pub longitude: f64,
}

impl GeoPoint {
    pub fn new(latitude: f64, longitude: f64) -> Self {
        Self { latitude, longitude }
    }

    pub fn distance_to(&self, other: &GeoPoint) -> f64 {
        Self::haversine_distance(self.latitude, self.longitude, other.latitude, other.longitude)
    }

    pub fn haversine_distance(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
        let r = 6371.0;
        let d_lat = (lat2 - lat1).to_radians();
        let d_lon = (lon2 - lon1).to_radians();
        
        let a = (d_lat / 2.0).sin().powi(2) 
            + lat1.to_radians().cos() * lat2.to_radians().cos() * (d_lon / 2.0).sin().powi(2);
        let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
        
        r * c
    }

    pub fn bounding_box(&self, radius_km: f64) -> (f64, f64, f64, f64) {
        let lat_delta = radius_km / 111.0;
        let lon_delta = radius_km / (111.0 * self.latitude.to_radians().cos());
        
        (
            self.latitude - lat_delta,
            self.latitude + lat_delta,
            self.longitude - lon_delta,
            self.longitude + lon_delta,
        )
    }
}

#[derive(Debug, Clone)]
pub struct GeoBoundingBox {
    pub min_lat: f64,
    pub max_lat: f64,
    pub min_lon: f64,
    pub max_lon: f64,
}

impl GeoBoundingBox {
    pub fn new(min_lat: f64, max_lat: f64, min_lon: f64, max_lon: f64) -> Self {
        Self { min_lat, max_lat, min_lon, max_lon }
    }

    pub fn contains(&self, point: &GeoPoint) -> bool {
        point.latitude >= self.min_lat 
            && point.latitude <= self.max_lat 
            && point.longitude >= self.min_lon 
            && point.longitude <= self.max_lon
    }

    pub fn intersects(&self, other: &GeoBoundingBox) -> bool {
        !(self.max_lat < other.min_lat 
            || self.min_lat > other.max_lat 
            || self.max_lon < other.min_lon 
            || self.min_lon > other.max_lon)
    }
}

#[derive(Debug, Clone)]
pub struct GeoPolygon {
    pub vertices: Vec<GeoPoint>,
}

impl GeoPolygon {
    pub fn new(vertices: Vec<GeoPoint>) -> Self {
        Self { vertices }
    }

    pub fn contains(&self, point: &GeoPoint) -> bool {
        let n = self.vertices.len();
        if n < 3 {
            return false;
        }

        let mut inside = false;
        let mut j = n - 1;

        for i in 0..n {
            let xi = self.vertices[i].longitude;
            let yi = self.vertices[i].latitude;
            let xj = self.vertices[j].longitude;
            let yj = self.vertices[j].latitude;

            if ((yi > point.latitude) != (yj > point.latitude))
                && (point.longitude < (xj - xi) * (point.latitude - yi) / (yj - yi) + xi) {
                inside = !inside;
            }
            j = i;
        }

        inside
    }

    pub fn area(&self) -> f64 {
        let n = self.vertices.len();
        if n < 3 {
            return 0.0;
        }

        let mut area = 0.0;
        let mut j = n - 1;

        for i in 0..n {
            area += (self.vertices[j].longitude + self.vertices[i].longitude) 
                * (self.vertices[j].latitude - self.vertices[i].latitude);
            j = i;
        }

        (area / 2.0).abs()
    }

    pub fn bounding_box(&self) -> GeoBoundingBox {
        let mut min_lat = f64::MAX;
        let mut max_lat = f64::MIN;
        let mut min_lon = f64::MAX;
        let mut max_lon = f64::MIN;

        for point in &self.vertices {
            min_lat = min_lat.min(point.latitude);
            max_lat = max_lat.max(point.latitude);
            min_lon = min_lon.min(point.longitude);
            max_lon = max_lon.max(point.longitude);
        }

        GeoBoundingBox::new(min_lat, max_lat, min_lon, max_lon)
    }
}

#[derive(Debug, Clone)]
pub struct GeoLineString {
    pub points: Vec<GeoPoint>,
}

impl GeoLineString {
    pub fn new(points: Vec<GeoPoint>) -> Self {
        Self { points }
    }

    pub fn length(&self) -> f64 {
        let mut total = 0.0;
        for i in 1..self.points.len() {
            total += self.points[i - 1].distance_to(&self.points[i]);
        }
        total
    }

    pub fn distance_to_point(&self, point: &GeoPoint) -> f64 {
        let mut min_dist = f64::MAX;
        
        for i in 1..self.points.len() {
            let dist = Self::point_to_segment_distance(
                point,
                &self.points[i - 1],
                &self.points[i]
            );
            min_dist = min_dist.min(dist);
        }
        
        min_dist
    }

    fn point_to_segment_distance(point: &GeoPoint, seg_start: &GeoPoint, seg_end: &GeoPoint) -> f64 {
        let l2 = seg_start.distance_to(seg_end).powi(2);
        if l2 == 0.0 {
            return point.distance_to(seg_start);
        }

        let t = ((point.latitude - seg_start.latitude) * (seg_end.latitude - seg_start.latitude)
            + (point.longitude - seg_start.longitude) * (seg_end.longitude - seg_start.longitude))
            / l2;

        let t = t.max(0.0).min(1.0);

        let proj_lat = seg_start.latitude + t * (seg_end.latitude - seg_start.latitude);
        let proj_lon = seg_start.longitude + t * (seg_end.longitude - seg_start.longitude);

        GeoPoint::new(proj_lat, proj_lon).distance_to(point)
    }
}

pub struct GeoIndex {
    points: Arc<RwLock<HashMap<String, GeoPoint>>>,
    rtree: Arc<RwLock<RTree>>,
    metadata: Arc<RwLock<HashMap<String, serde_json::Value>>>,
}

struct RTree {
    nodes: Vec<RTreeNode>,
    bounds: GeoBoundingBox,
}

struct RTreeNode {
    bounds: GeoBoundingBox,
    children: Vec<usize>,
    points: Vec<(String, GeoPoint)>,
    is_leaf: bool,
}

impl RTree {
    pub fn new() -> Self {
        Self {
            nodes: vec![RTreeNode {
                bounds: GeoBoundingBox::new(-90.0, 90.0, -180.0, 180.0),
                children: Vec::new(),
                points: Vec::new(),
                is_leaf: true,
            }],
            bounds: GeoBoundingBox::new(-90.0, 90.0, -180.0, 180.0),
        }
    }

    pub fn insert(&mut self, id: String, point: GeoPoint) {
        let leaf = self.nodes[0].points.len() < 4;
        
        if leaf {
            self.nodes[0].points.push((id, point));
        }
    }

    pub fn search(&self, query: &GeoBoundingBox) -> Vec<(String, GeoPoint)> {
        let mut results = Vec::new();
        self.search_node(0, query, &mut results);
        results
    }

    fn search_node(&self, node_idx: usize, query: &GeoBoundingBox, results: &mut Vec<(String, GeoPoint)>) {
        let node = &self.nodes[node_idx];
        
        if !node.bounds.intersects(query) {
            return;
        }

        if node.is_leaf {
            for (id, point) in &node.points {
                if query.contains(point) {
                    results.push((id.clone(), point.clone()));
                }
            }
        } else {
            for &child_idx in &node.children {
                self.search_node(child_idx, query, results);
            }
        }
    }
}

impl Default for RTree {
    fn default() -> Self {
        Self::new()
    }
}

impl GeoIndex {
    pub fn new() -> Self {
        Self {
            points: Arc::new(RwLock::new(HashMap::new())),
            rtree: Arc::new(RwLock::new(RTree::new())),
            metadata: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn insert(&self, id: String, point: GeoPoint, metadata: Option<serde_json::Value>) {
        let mut points = self.points.write().await;
        points.insert(id.clone(), point.clone());
        
        if let Some(meta) = metadata {
            let mut md = self.metadata.write().await;
            md.insert(id, meta);
        }

        drop(points);
        let mut rtree = self.rtree.write().await;
        rtree.insert(id, point);
    }

    pub async fn get(&self, id: &str) -> Option<GeoPoint> {
        let points = self.points.read().await;
        points.get(id).cloned()
    }

    pub async fn remove(&self, id: &str) -> Option<GeoPoint> {
        let mut points = self.points.write().await;
        points.remove(id)
    }

    pub async fn within_bounding_box(&self, bbox: GeoBoundingBox) -> Vec<(String, GeoPoint)> {
        let rtree = self.rtree.read().await;
        rtree.search(&bbox)
    }

    pub async fn within_radius(&self, center: &GeoPoint, radius_km: f64) -> Vec<(String, GeoPoint, f64)> {
        let (min_lat, max_lat, min_lon, max_lon) = center.bounding_box(radius_km);
        let bbox = GeoBoundingBox::new(min_lat, max_lat, min_lon, max_lon);
        
        let candidates = self.within_bounding_box(bbox).await;
        
        candidates
            .into_iter()
            .filter_map(|(id, point)| {
                let dist = center.distance_to(&point);
                if dist <= radius_km {
                    Some((id, point, dist))
                } else {
                    None
                }
            })
            .collect()
    }

    pub async fn nearest_neighbors(&self, center: &GeoPoint, k: usize) -> Vec<(String, GeoPoint, f64)> {
        let points = self.points.read().await;
        
        let mut distances: Vec<_> = points
            .iter()
            .map(|(id, point)| {
                let dist = center.distance_to(point);
                (id.clone(), point.clone(), dist)
            })
            .collect();
        
        distances.sort_by(|a, b| a.2.partial_cmp(&b.2).unwrap());
        
        distances.into_iter().take(k).collect()
    }

    pub async fn within_polygon(&self, polygon: &GeoPolygon) -> Vec<(String, GeoPoint)> {
        let bbox = polygon.bounding_box();
        let candidates = self.within_bounding_box(bbox).await;
        
        candidates
            .into_iter()
            .filter(|(_, point)| polygon.contains(point))
            .collect()
    }

    pub async fn distance_to_line(&self, line: &GeoLineString) -> Vec<(String, f64)> {
        let points = self.points.read().await;
        
        points
            .iter()
            .map(|(id, point)| {
                let dist = line.distance_to_point(point);
                (id.clone(), dist)
            })
            .collect()
    }

    pub async fn count(&self) -> usize {
        let points = self.points.read().await;
        points.len()
    }
}

impl Default for GeoIndex {
    fn default() -> Self {
        Self::new()
    }
}

pub struct GeoQuery {
    pub center: Option<GeoPoint>,
    pub radius_km: Option<f64>,
    pub bounding_box: Option<GeoBoundingBox>,
    pub polygon: Option<GeoPolygon>,
    pub limit: usize,
}

impl GeoQuery {
    pub fn new() -> Self {
        Self {
            center: None,
            radius_km: None,
            bounding_box: None,
            polygon: None,
            limit: 100,
        }
    }

    pub fn with_radius(mut self, center: GeoPoint, radius_km: f64) -> Self {
        self.center = Some(center);
        self.radius_km = Some(radius_km);
        self
    }

    pub fn with_bounding_box(mut self, bbox: GeoBoundingBox) -> Self {
        self.bounding_box = Some(bbox);
        self
    }

    pub fn with_polygon(mut self, polygon: GeoPolygon) -> Self {
        self.polygon = Some(polygon);
        self
    }

    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }
}

impl Default for GeoQuery {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_geo_point_distance() {
        let p1 = GeoPoint::new(0.0, 0.0);
        let p2 = GeoPoint::new(0.0, 1.0);
        
        let dist = p1.distance_to(&p2);
        assert!(dist > 111.0 && dist < 112.0);
    }

    #[test]
    fn test_bounding_box() {
        let point = GeoPoint::new(40.0, -74.0);
        let (min_lat, max_lat, min_lon, max_lon) = point.bounding_box(10.0);
        
        assert!(min_lat < 40.0);
        assert!(max_lat > 40.0);
    }

    #[test]
    fn test_polygon_contains() {
        let polygon = GeoPolygon::new(vec![
            GeoPoint::new(0.0, 0.0),
            GeoPoint::new(0.0, 10.0),
            GeoPoint::new(10.0, 10.0),
            GeoPoint::new(10.0, 0.0),
        ]);
        
        let inside = GeoPoint::new(5.0, 5.0);
        let outside = GeoPoint::new(15.0, 15.0);
        
        assert!(polygon.contains(&inside));
        assert!(!polygon.contains(&outside));
    }

    #[tokio::test]
    async fn test_geo_index_insert_and_search() {
        let index = GeoIndex::new();
        
        index.insert("loc1".to_string(), GeoPoint::new(40.7128, -74.0060), None).await;
        index.insert("loc2".to_string(), GeoPoint::new(34.0522, -118.2437), None).await;
        
        let count = index.count().await;
        assert_eq!(count, 2);
        
        let p1 = index.get("loc1").await;
        assert!(p1.is_some());
    }

    #[tokio::test]
    async fn test_nearest_neighbors() {
        let index = GeoIndex::new();
        
        index.insert("a".to_string(), GeoPoint::new(40.0, -74.0), None).await;
        index.insert("b".to_string(), GeoPoint::new(41.0, -74.0), None).await;
        index.insert("c".to_string(), GeoPoint::new(42.0, -74.0), None).await;
        
        let center = GeoPoint::new(40.5, -74.0);
        let neighbors = index.nearest_neighbors(&center, 2).await;
        
        assert_eq!(neighbors.len(), 2);
    }
}
