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
    root: usize,
    max_entries: usize,
    min_entries: usize,
}

struct RTreeNode {
    bounds: GeoBoundingBox,
    children: Vec<usize>,
    entries: Vec<(String, GeoPoint)>,
    parent: Option<usize>,
    is_leaf: bool,
}

impl RTreeNode {
    fn new_leaf() -> Self {
        Self {
            bounds: GeoBoundingBox::new(f64::MAX, f64::MIN, f64::MAX, f64::MIN),
            children: Vec::new(),
            entries: Vec::new(),
            parent: None,
            is_leaf: true,
        }
    }

    fn new_internal() -> Self {
        Self {
            bounds: GeoBoundingBox::new(f64::MAX, f64::MIN, f64::MAX, f64::MIN),
            children: Vec::new(),
            entries: Vec::new(),
            parent: None,
            is_leaf: false,
        }
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty() && self.children.is_empty()
    }

    fn entry_count(&self) -> usize {
        if self.is_leaf {
            self.entries.len()
        } else {
            self.children.len()
        }
    }

    fn update_bounds_for_point(&mut self, point: &GeoPoint) {
        self.bounds.min_lat = self.bounds.min_lat.min(point.latitude);
        self.bounds.max_lat = self.bounds.max_lat.max(point.latitude);
        self.bounds.min_lon = self.bounds.min_lon.min(point.longitude);
        self.bounds.max_lon = self.bounds.max_lon.max(point.longitude);
    }

    fn update_bounds_for_box(&mut self, bbox: &GeoBoundingBox) {
        self.bounds.min_lat = self.bounds.min_lat.min(bbox.min_lat);
        self.bounds.max_lat = self.bounds.max_lat.max(bbox.max_lat);
        self.bounds.min_lon = self.bounds.min_lon.min(bbox.min_lon);
        self.bounds.max_lon = self.bounds.max_lon.max(bbox.max_lon);
    }

    fn recompute_bounds(&mut self) {
        if self.is_leaf {
            self.bounds = GeoBoundingBox::new(f64::MAX, f64::MIN, f64::MAX, f64::MIN);
            for (_, p) in &self.entries {
                self.update_bounds_for_point(p);
            }
        } else {
            self.bounds = GeoBoundingBox::new(f64::MAX, f64::MIN, f64::MAX, f64::MIN);
            // bounds 由 adjust_tree 在插入子节点时更新
        }
    }
}

/// 计算 MBR 包含一个点后的面积增量
fn enlargement_for_point(bbox: &GeoBoundingBox, point: &GeoPoint) -> f64 {
    let new_min_lat = bbox.min_lat.min(point.latitude);
    let new_max_lat = bbox.max_lat.max(point.latitude);
    let new_min_lon = bbox.min_lon.min(point.longitude);
    let new_max_lon = bbox.max_lon.max(point.longitude);
    let old_area = (bbox.max_lat - bbox.min_lat) * (bbox.max_lon - bbox.min_lon);
    let new_area = (new_max_lat - new_min_lat) * (new_max_lon - new_min_lon);
    new_area - old_area
}

impl RTree {
    pub fn new() -> Self {
        Self {
            nodes: vec![RTreeNode::new_leaf()],
            root: 0,
            max_entries: 8,
            min_entries: 3,
        }
    }

    pub fn insert(&mut self, id: String, point: GeoPoint) {
        // 1. 选择最佳叶子节点
        let leaf_idx = self.choose_leaf(self.root, &point);

        // 2. 插入条目到叶子
        {
            let leaf = &mut self.nodes[leaf_idx];
            leaf.entries.push((id, point.clone()));
            leaf.update_bounds_for_point(&point);
        }

        // 3. 如果溢出，分裂
        let mut split_result = if self.nodes[leaf_idx].entries.len() > self.max_entries {
            Some(self.split_node(leaf_idx))
        } else {
            None
        };

        // 4. 向上调整树
        let mut current = leaf_idx;
        while let Some(parent_idx) = self.nodes[current].parent {
            // 更新父节点边界
            {
                let child_bounds = self.nodes[current].bounds.clone();
                let parent = &mut self.nodes[parent_idx];
                parent.update_bounds_for_box(&child_bounds);
            }

            // 如果当前节点分裂了，将新节点加入父节点的 children
            if let Some(sr) = split_result.take() {
                let new_idx = sr.new_node_idx;
                {
                    let new_bounds = self.nodes[new_idx].bounds.clone();
                    let parent = &mut self.nodes[parent_idx];
                    parent.children.push(new_idx);
                    parent.update_bounds_for_box(&new_bounds);
                }
                self.nodes[new_idx].parent = Some(parent_idx);

                // 检查父节点是否也溢出
                if self.nodes[parent_idx].children.len() > self.max_entries {
                    split_result = Some(self.split_node(parent_idx));
                }
            }
            current = parent_idx;
        }

        // 5. 如果根节点分裂，创建新根
        if let Some(sr) = split_result {
            let old_root = self.root;
            let new_node_idx = sr.new_node_idx;

            let mut new_root = RTreeNode::new_internal();
            let old_bounds = self.nodes[old_root].bounds.clone();
            let new_bounds = self.nodes[new_node_idx].bounds.clone();
            new_root.update_bounds_for_box(&old_bounds);
            new_root.update_bounds_for_box(&new_bounds);
            new_root.children.push(old_root);
            new_root.children.push(new_node_idx);

            let new_root_idx = self.nodes.len();
            self.nodes[old_root].parent = Some(new_root_idx);
            self.nodes[new_node_idx].parent = Some(new_root_idx);
            self.nodes.push(new_root);
            self.root = new_root_idx;
        }
    }

    /// 从根开始选择包含 point 的最佳叶子节点（最小面积增量）
    fn choose_leaf(&self, node_idx: usize, point: &GeoPoint) -> usize {
        let node = &self.nodes[node_idx];
        if node.is_leaf {
            return node_idx;
        }

        // 在子节点中选择面积增量最小的
        let mut best_child = node.children[0];
        let mut best_enlargement = f64::MAX;

        for &child_idx in &node.children {
            let child = &self.nodes[child_idx];
            let enlargement = enlargement_for_point(&child.bounds, point);
            if enlargement < best_enlargement
                || (enlargement == best_enlargement
                    && self.area(&child.bounds) < self.area(&self.nodes[best_child].bounds))
            {
                best_enlargement = enlargement;
                best_child = child_idx;
            }
        }

        self.choose_leaf(best_child, point)
    }

    fn area(&self, bbox: &GeoBoundingBox) -> f64 {
        (bbox.max_lat - bbox.min_lat).max(0.0) * (bbox.max_lon - bbox.min_lon).max(0.0)
    }

    /// 二次分裂算法：选择浪费最大的两个种子，然后分配剩余条目
    fn split_node(&mut self, node_idx: usize) -> SplitResult {
        let is_leaf = self.nodes[node_idx].is_leaf;

        // 提取所有条目
        let entries: Vec<(String, GeoPoint)> = if is_leaf {
            std::mem::take(&mut self.nodes[node_idx].entries)
        } else {
            // 内部节点分裂：提取子节点索引
            let children = std::mem::take(&mut self.nodes[node_idx].children);
            // 将子节点转为伪条目（用其 MBR 中心点作为代表）
            children.into_iter()
                .map(|child_idx| {
                    let b = &self.nodes[child_idx].bounds;
                    let center = GeoPoint::new(
                        (b.min_lat + b.max_lat) / 2.0,
                        (b.min_lon + b.max_lon) / 2.0,
                    );
                    (format!("__child__{}", child_idx), center)
                })
                .collect()
        };

        if entries.len() < 2 {
            return SplitResult { new_node_idx: node_idx };
        }

        // 1. 选种子：找两个组合 MBR 面积最大的（最大浪费）
        let mut max_waste = f64::MIN;
        let mut seed1 = 0;
        let mut seed2 = 1;

        for i in 0..entries.len() {
            for j in (i + 1)..entries.len() {
                let p1 = &entries[i].1;
                let p2 = &entries[j].1;
                let combined = GeoBoundingBox::new(
                    p1.latitude.min(p2.latitude),
                    p1.latitude.max(p2.latitude),
                    p1.longitude.min(p2.longitude),
                    p1.longitude.max(p2.longitude),
                );
                let waste = self.area(&combined);
                if waste > max_waste {
                    max_waste = waste;
                    seed1 = i;
                    seed2 = j;
                }
            }
        }

        // 2. 分配条目到两组
        let mut group1: Vec<usize> = vec![seed1];
        let mut group2: Vec<usize> = vec![seed2];

        let p_seed1 = entries[seed1].1.clone();
        let p_seed2 = entries[seed2].1.clone();

        let mut bbox1 = GeoBoundingBox::new(
            p_seed1.latitude, p_seed1.latitude,
            p_seed1.longitude, p_seed1.longitude,
        );
        let mut bbox2 = GeoBoundingBox::new(
            p_seed2.latitude, p_seed2.latitude,
            p_seed2.longitude, p_seed2.longitude,
        );

        for i in 0..entries.len() {
            if i == seed1 || i == seed2 {
                continue;
            }

            let point = &entries[i].1;
            let enlarg1 = enlargement_for_point(&bbox1, point);
            let enlarg2 = enlargement_for_point(&bbox2, point);

            // 平衡：如果一组远少于另一组，强制分配到少的组
            if group1.len() + entries.len() - group1.len() - group2.len() - 1 <= self.min_entries
                && group1.len() < self.min_entries
            {
                group1.push(i);
                bbox1.update_bounds_for_point_ref(point);
            } else if group2.len() < self.min_entries
                && group2.len() + entries.len() - group1.len() - group2.len() - 1 <= self.min_entries
            {
                group2.push(i);
                bbox2.update_bounds_for_point_ref(point);
            } else if enlarg1 < enlarg2 {
                group1.push(i);
                bbox1.update_bounds_for_point_ref(point);
            } else if enlarg2 < enlarg1 {
                group2.push(i);
                bbox2.update_bounds_for_point_ref(point);
            } else if self.area(&bbox1) < self.area(&bbox2) {
                group1.push(i);
                bbox1.update_bounds_for_point_ref(point);
            } else {
                group2.push(i);
                bbox2.update_bounds_for_point_ref(point);
            }
        }

        // 3. 重建节点
        // 原节点保留 group1
        let parent = self.nodes[node_idx].parent;
        self.nodes[node_idx] = RTreeNode::new_leaf();
        self.nodes[node_idx].parent = parent;
        self.nodes[node_idx].bounds = bbox1.clone();
        for &i in &group1 {
            self.nodes[node_idx].entries.push(entries[i].clone());
        }

        // 新节点存放 group2
        let new_node = RTreeNode::new_leaf();
        let new_node_idx = self.nodes.len();

        let mut new_node = new_node;
        new_node.bounds = bbox2.clone();
        for &i in &group2 {
            new_node.entries.push(entries[i].clone());
        }
        self.nodes.push(new_node);

        // 如果是内部节点分裂，需要恢复 children 关系
        if !is_leaf {
            // 将伪条目转回子节点索引
            let g1_children: Vec<usize> = self.nodes[node_idx].entries.drain(..)
                .filter_map(|(key, _)| key.strip_prefix("__child__").and_then(|s| s.parse::<usize>().ok()))
                .collect();
            let g2_children: Vec<usize> = self.nodes[new_node_idx].entries.drain(..)
                .filter_map(|(key, _)| key.strip_prefix("__child__").and_then(|s| s.parse::<usize>().ok()))
                .collect();

            self.nodes[node_idx].is_leaf = false;
            self.nodes[node_idx].children = g1_children.clone();
            self.nodes[new_node_idx].is_leaf = false;
            self.nodes[new_node_idx].children = g2_children.clone();

            // 更新子节点的 parent 指针
            for &child_idx in &g1_children {
                if child_idx < self.nodes.len() {
                    self.nodes[child_idx].parent = Some(node_idx);
                }
            }
            for &child_idx in &g2_children {
                if child_idx < self.nodes.len() {
                    self.nodes[child_idx].parent = Some(new_node_idx);
                }
            }
        }

        SplitResult { new_node_idx }
    }

    pub fn search(&self, query: &GeoBoundingBox) -> Vec<(String, GeoPoint)> {
        let mut results = Vec::new();
        self.search_node(self.root, query, &mut results);
        results
    }

    fn search_node(&self, node_idx: usize, query: &GeoBoundingBox, results: &mut Vec<(String, GeoPoint)>) {
        if node_idx >= self.nodes.len() {
            return;
        }
        let node = &self.nodes[node_idx];

        if !node.bounds.intersects(query) {
            return;
        }

        if node.is_leaf {
            for (id, point) in &node.entries {
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

    pub fn height(&self) -> usize {
        let mut h = 1;
        let mut node = &self.nodes[self.root];
        while !node.is_leaf && !node.children.is_empty() {
            h += 1;
            node = &self.nodes[node.children[0]];
        }
        h
    }

    pub fn count(&self) -> usize {
        let mut count = 0;
        self.count_node(self.root, &mut count);
        count
    }

    fn count_node(&self, node_idx: usize, count: &mut usize) {
        if node_idx >= self.nodes.len() {
            return;
        }
        let node = &self.nodes[node_idx];
        if node.is_leaf {
            *count += node.entries.len();
        } else {
            for &child_idx in &node.children {
                self.count_node(child_idx, count);
            }
        }
    }
}

struct SplitResult {
    new_node_idx: usize,
}

/// 为 GeoBoundingBox 添加辅助方法（通过 trait extension）
trait BBoxExt {
    fn update_bounds_for_point_ref(&mut self, point: &GeoPoint);
}

impl BBoxExt for GeoBoundingBox {
    fn update_bounds_for_point_ref(&mut self, point: &GeoPoint) {
        self.min_lat = self.min_lat.min(point.latitude);
        self.max_lat = self.max_lat.max(point.latitude);
        self.min_lon = self.min_lon.min(point.longitude);
        self.max_lon = self.max_lon.max(point.longitude);
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
            md.insert(id.clone(), meta);
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

#[derive(Debug, Clone)]
pub struct GeoPoint3D {
    pub x: f64,
    pub y: f64,
    pub z: f64,
}

impl GeoPoint3D {
    pub fn new(x: f64, y: f64, z: f64) -> Self {
        Self { x, y, z }
    }

    pub fn distance_to(&self, other: &GeoPoint3D) -> f64 {
        let dx = self.x - other.x;
        let dy = self.y - other.y;
        let dz = self.z - other.z;
        (dx * dx + dy * dy + dz * dz).sqrt()
    }

    pub fn distance_to_line(&self, start: &GeoPoint3D, end: &GeoPoint3D) -> f64 {
        let ab = GeoPoint3D::new(end.x - start.x, end.y - start.y, end.z - start.z);
        let ac = GeoPoint3D::new(self.x - start.x, self.y - start.y, self.z - start.z);

        let ab_len_sq = ab.x * ab.x + ab.y * ab.y + ab.z * ab.z;
        if ab_len_sq == 0.0 {
            return self.distance_to(start);
        }

        let t = (ac.x * ab.x + ac.y * ab.y + ac.z * ab.z) / ab_len_sq;
        let t = t.max(0.0).min(1.0);

        let proj = GeoPoint3D::new(
            start.x + t * ab.x,
            start.y + t * ab.y,
            start.z + t * ab.z,
        );

        self.distance_to(&proj)
    }

    pub fn bounding_box(&self, radius: f64) -> (f64, f64, f64, f64, f64, f64) {
        (
            self.x - radius, self.x + radius,
            self.y - radius, self.y + radius,
            self.z - radius, self.z + radius,
        )
    }
}

#[derive(Debug, Clone)]
pub struct GeoLineString3D {
    pub points: Vec<GeoPoint3D>,
}

impl GeoLineString3D {
    pub fn new(points: Vec<GeoPoint3D>) -> Self {
        Self { points }
    }

    pub fn length(&self) -> f64 {
        let mut total = 0.0;
        for i in 1..self.points.len() {
            total += self.points[i - 1].distance_to(&self.points[i]);
        }
        total
    }

    pub fn distance_to_point(&self, point: &GeoPoint3D) -> f64 {
        let mut min_dist = f64::MAX;
        for i in 1..self.points.len() {
            let dist = point.distance_to_line(&self.points[i - 1], &self.points[i]);
            min_dist = min_dist.min(dist);
        }
        min_dist
    }

    pub fn distance_to_line(&self, other: &GeoLineString3D) -> f64 {
        let mut min_dist = f64::MAX;
        for i in 1..self.points.len() {
            for j in 1..other.points.len() {
                let dist = Self::segment_to_segment_distance(
                    &self.points[i - 1], &self.points[i],
                    &other.points[j - 1], &other.points[j],
                );
                min_dist = min_dist.min(dist);
            }
        }
        min_dist
    }

    fn segment_to_segment_distance(
        a1: &GeoPoint3D, a2: &GeoPoint3D,
        b1: &GeoPoint3D, b2: &GeoPoint3D,
    ) -> f64 {
        let d1 = a2.x - a1.x; let d2 = a2.y - a1.y; let d3 = a2.z - a1.z;
        let e1 = b2.x - b1.x; let e2 = b2.y - b1.y; let e3 = b2.z - b1.z;
        let f1 = a1.x - b1.x; let f2 = a1.y - b1.y; let f3 = a1.z - b1.z;

        let a = d1*d1 + d2*d2 + d3*d3;
        let b = d1*e1 + d2*e2 + d3*e3;
        let c = e1*e1 + e2*e2 + e3*e3;
        let d = d1*f1 + d2*f2 + d3*f3;
        let e = e1*f1 + e2*f2 + e3*f3;
        let det = a*c - b*b;

        let mut s = 0.0;
        let mut t = 0.0;

        if det > 1e-12 {
            s = (b*e - c*d) / det;
            t = (a*e - b*d) / det;
            s = s.max(0.0).min(1.0);
            t = t.max(0.0).min(1.0);
        }

        let px = a1.x + s*d1;
        let py = a1.y + s*d2;
        let pz = a1.z + s*d3;
        let qx = b1.x + t*e1;
        let qy = b1.y + t*e2;
        let qz = b1.z + t*e3;

        let dx = px - qx;
        let dy = py - qy;
        let dz = pz - qz;
        (dx*dx + dy*dy + dz*dz).sqrt()
    }
}

#[derive(Debug, Clone)]
pub struct GeoPolygon3D {
    pub vertices: Vec<GeoPoint3D>,
}

impl GeoPolygon3D {
    pub fn new(vertices: Vec<GeoPoint3D>) -> Self {
        Self { vertices }
    }

    pub fn area(&self) -> f64 {
        let n = self.vertices.len();
        if n < 3 {
            return 0.0;
        }
        let mut cx = 0.0; let mut cy = 0.0; let mut cz = 0.0;
        for v in &self.vertices {
            cx += v.x; cy += v.y; cz += v.z;
        }
        cx /= n as f64; cy /= n as f64; cz /= n as f64;

        let mut nx = 0.0; let mut ny = 0.0; let mut nz = 0.0;
        let mut j = n - 1;
        for i in 0..n {
            let (x1, y1, z1) = (self.vertices[j].x - cx, self.vertices[j].y - cy, self.vertices[j].z - cz);
            let (x2, y2, z2) = (self.vertices[i].x - cx, self.vertices[i].y - cy, self.vertices[i].z - cz);
            nx += y1 * z2 - z1 * y2;
            ny += z1 * x2 - x1 * z2;
            nz += x1 * y2 - y1 * x2;
            j = i;
        }
        let norm = (nx*nx + ny*ny + nz*nz).sqrt();
        if norm == 0.0 { return 0.0; }
        (norm / 2.0).abs()
    }

    pub fn distance_to_point(&self, point: &GeoPoint3D) -> f64 {
        let n = self.vertices.len();
        if n < 3 {
            return self.vertices.iter().map(|v| v.distance_to(point)).fold(f64::MAX, f64::min);
        }
        let mut min_dist = f64::MAX;
        let mut j = n - 1;
        for i in 0..n {
            let dist = point.distance_to_line(&self.vertices[j], &self.vertices[i]);
            min_dist = min_dist.min(dist);
            j = i;
        }
        min_dist
    }

    pub fn distance_to_polygon(&self, other: &GeoPolygon3D) -> f64 {
        let mut min_dist = f64::MAX;
        for v in &self.vertices {
            let dist = other.distance_to_point(v);
            min_dist = min_dist.min(dist);
        }
        for v in &other.vertices {
            let dist = self.distance_to_point(v);
            min_dist = min_dist.min(dist);
        }
        min_dist
    }

    pub fn bounding_box(&self) -> (f64, f64, f64, f64, f64, f64) {
        let mut min_x = f64::MAX; let mut max_x = f64::MIN;
        let mut min_y = f64::MAX; let mut max_y = f64::MIN;
        let mut min_z = f64::MAX; let mut max_z = f64::MIN;
        for v in &self.vertices {
            min_x = min_x.min(v.x); max_x = max_x.max(v.x);
            min_y = min_y.min(v.y); max_y = max_y.max(v.y);
            min_z = min_z.min(v.z); max_z = max_z.max(v.z);
        }
        (min_x, max_x, min_y, max_y, min_z, max_z)
    }
}

#[derive(Debug, Clone)]
pub struct GeoBoundingBox3D {
    pub min_x: f64,
    pub max_x: f64,
    pub min_y: f64,
    pub max_y: f64,
    pub min_z: f64,
    pub max_z: f64,
}

impl GeoBoundingBox3D {
    pub fn new(min_x: f64, max_x: f64, min_y: f64, max_y: f64, min_z: f64, max_z: f64) -> Self {
        Self { min_x, max_x, min_y, max_y, min_z, max_z }
    }

    pub fn contains(&self, point: &GeoPoint3D) -> bool {
        point.x >= self.min_x && point.x <= self.max_x
            && point.y >= self.min_y && point.y <= self.max_y
            && point.z >= self.min_z && point.z <= self.max_z
    }

    pub fn intersects(&self, other: &GeoBoundingBox3D) -> bool {
        !(self.max_x < other.min_x || self.min_x > other.max_x
            || self.max_y < other.min_y || self.min_y > other.max_y
            || self.max_z < other.min_z || self.min_z > other.max_z)
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
