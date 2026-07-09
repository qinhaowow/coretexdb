//! 数据类型广度增强：
//! 1. 3D GIS 拓扑关系（DE-9IM）
//! 2. 时序窗口函数 + 复杂聚合
//! 3. 图算法：PageRank / ConnectedComponents / 最短路径（BFS/Dijkstra）
//! 4. 文档 RAG 跨模态链路
//! 5. 领域索引扩展（电商、医疗、物流）

use std::collections::{HashMap, HashSet, BinaryHeap};
use std::cmp::Reverse;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;

// =================== 3D GIS 拓扑关系 ===================

/// DE-9IM（Dimensionally Extended 9-Intersection Model）矩阵
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DE9IM {
    pub i_i: i8, // Interior / Interior
    pub i_b: i8, // Interior / Boundary
    pub i_e: i8, // Interior / Exterior
    pub b_i: i8,
    pub b_b: i8,
    pub b_e: i8,
    pub e_i: i8,
    pub e_b: i8,
    pub e_e: i8,
}

impl DE9IM {
    /// 判断两个几何体的拓扑关系
    pub fn relation(&self) -> SpatialRelation {
        // 简化判断
        if self.i_i >= 2 && self.b_b >= 1 && self.e_e >= 2 {
            SpatialRelation::Equals
        } else if self.i_b >= 1 && self.b_i >= 1 {
            SpatialRelation::Touches
        } else if self.i_i >= 1 {
            SpatialRelation::Overlaps
        } else if self.i_e >= 1 && self.b_e >= 1 {
            SpatialRelation::Contains
        } else if self.e_i >= 1 && self.e_b >= 1 {
            SpatialRelation::Within
        } else {
            SpatialRelation::Disjoint
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SpatialRelation {
    Disjoint,
    Touches,
    Overlaps,
    Contains,
    Within,
    Equals,
    Covers,
    CoveredBy,
}

/// 3D 点
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct GeoPoint3D {
    pub x: f64,
    pub y: f64,
    pub z: f64,
}

impl GeoPoint3D {
    pub fn new(x: f64, y: f64, z: f64) -> Self { Self { x, y, z } }

    /// 3D 欧氏距离
    pub fn distance_to(&self, other: &GeoPoint3D) -> f64 {
        let dx = self.x - other.x;
        let dy = self.y - other.y;
        let dz = self.z - other.z;
        (dx * dx + dy * dy + dz * dz).sqrt()
    }

    /// 3D 曼哈顿距离
    pub fn manhattan_to(&self, other: &GeoPoint3D) -> f64 {
        (self.x - other.x).abs() + (self.y - other.y).abs() + (self.z - other.z).abs()
    }

    /// 3D 球面距离（用于经纬度高程）
    pub fn haversine_3d(&self, other: &GeoPoint3D) -> f64 {
        const EARTH_RADIUS: f64 = 6_371_000.0; // 米
        let lat1 = self.y.to_radians();
        let lat2 = other.y.to_radians();
        let dlat = lat2 - lat1;
        let dlon = (other.x - self.x).to_radians();
        let dz = other.z - self.z;

        let a = (dlat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (dlon / 2.0).sin().powi(2);
        let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
        let surface_dist = EARTH_RADIUS * c;
        (surface_dist * surface_dist + dz * dz).sqrt()
    }
}

/// 3D 边界框
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeoBoundingBox3D {
    pub min: GeoPoint3D,
    pub max: GeoPoint3D,
}

impl GeoBoundingBox3D {
    pub fn new(min: GeoPoint3D, max: GeoPoint3D) -> Self { Self { min, max } }

    pub fn contains(&self, p: &GeoPoint3D) -> bool {
        p.x >= self.min.x && p.x <= self.max.x &&
        p.y >= self.min.y && p.y <= self.max.y &&
        p.z >= self.min.z && p.z <= self.max.z
    }

    pub fn intersects(&self, other: &GeoBoundingBox3D) -> bool {
        self.min.x <= other.max.x && self.max.x >= other.min.x &&
        self.min.y <= other.max.y && self.max.y >= other.min.y &&
        self.min.z <= other.max.z && self.max.z >= other.min.z
    }
}

/// 3D 拓扑关系计算器
pub struct Topology3D;

impl Topology3D {
    /// 计算两个 3D 边界框的 DE-9IM 关系
    pub fn relate_bbox(a: &GeoBoundingBox3D, b: &GeoBoundingBox3D) -> DE9IM {
        let i_i = if a.intersects(b) { 3 } else { -1 };
        let i_b = if a.min.x <= b.max.x && a.max.x >= b.min.x &&
                     a.min.y <= b.max.y && a.max.y >= b.min.y &&
                     a.min.z <= b.max.z && a.max.z >= b.min.z { 1 } else { 0 };
        let i_e = 2;
        let b_i = i_b;
        let b_b = if a.min == b.min && a.max == b.max { 1 } else { 0 };
        let b_e = 2;
        let e_i = 2;
        let e_b = 2;
        let e_e = 2;

        DE9IM { i_i, i_b, i_e, b_i, b_b, b_e, e_i, e_b, e_e }
    }

    /// 计算两个 3D 点的 DE-9IM
    pub fn relate_point(a: &GeoPoint3D, b: &GeoPoint3D) -> DE9IM {
        let equal = (a.x - b.x).abs() < 1e-9 && (a.y - b.y).abs() < 1e-9 && (a.z - b.z).abs() < 1e-9;
        if equal {
            DE9IM { i_i: 0, i_b: 0, i_e: 2, b_i: 0, b_b: 0, b_e: 2, e_i: 2, e_b: 2, e_e: 2 }
        } else {
            DE9IM { i_i: -1, i_b: -1, i_e: 2, b_i: -1, b_b: -1, b_e: 2, e_i: 2, e_b: 2, e_e: 2 }
        }
    }
}

// =================== 时序窗口函数 ===================

/// 时序窗口类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WindowType {
    /// 固定窗口（前 N 个点）
    Fixed(usize),
    /// 滑动窗口（按时间长度）
    Sliding { duration_secs: u64 },
    /// 滚动窗口（按时间桶）
    Tumbling { duration_secs: u64 },
    /// 会话窗口（基于空闲时间）
    Session { idle_secs: u64 },
}

/// 时序窗口函数
pub enum WindowFunction {
    Sum,
    Avg,
    Min,
    Max,
    Count,
    First,
    Last,
    StdDev,
    Percentile(f64),
}

impl WindowFunction {
    pub fn apply(&self, values: &[f64]) -> f64 {
        match self {
            WindowFunction::Sum => values.iter().sum(),
            WindowFunction::Avg => {
                if values.is_empty() { 0.0 } else { values.iter().sum::<f64>() / values.len() as f64 }
            }
            WindowFunction::Min => values.iter().cloned().fold(f64::INFINITY, f64::min),
            WindowFunction::Max => values.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
            WindowFunction::Count => values.len() as f64,
            WindowFunction::First => values.first().copied().unwrap_or(0.0),
            WindowFunction::Last => values.last().copied().unwrap_or(0.0),
            WindowFunction::StdDev => {
                if values.len() < 2 { return 0.0; }
                let mean: f64 = values.iter().sum::<f64>() / values.len() as f64;
                let var: f64 = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (values.len() - 1) as f64;
                var.sqrt()
            }
            WindowFunction::Percentile(p) => {
                if values.is_empty() { return 0.0; }
                let mut sorted = values.to_vec();
                sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
                let idx = (p * (sorted.len() - 1) as f64) as usize;
                sorted[idx.min(sorted.len() - 1)]
            }
        }
    }
}

/// 滑动窗口结果
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowResult {
    pub window_start: u64,
    pub window_end: u64,
    pub values: Vec<f64>,
    pub result: f64,
    pub function: String,
}

/// 时序窗口管理器
pub struct TimeSeriesWindow {
    points: Vec<crate::coretex_timeseries::TimeSeriesPoint>,
}

impl TimeSeriesWindow {
    pub fn new(points: Vec<crate::coretex_timeseries::TimeSeriesPoint>) -> Self {
        Self { points }
    }

    /// 应用滑动窗口函数
    pub fn apply_window(
        &self,
        window: WindowType,
        func: WindowFunction,
    ) -> Vec<WindowResult> {
        let mut results = Vec::new();
        if self.points.is_empty() {
            return results;
        }

        let sorted: Vec<_> = {
            let mut pts = self.points.clone();
            pts.sort_by_key(|p| p.timestamp);
            pts
        };

        match window {
            WindowType::Fixed(n) => {
                for i in 0..sorted.len() {
                    let end = (i + n).min(sorted.len());
                    let window_pts = &sorted[i..end];
                    if window_pts.len() < n && i + n > sorted.len() {
                        break;
                    }
                    let values: Vec<f64> = window_pts.iter().map(|p| p.value).collect();
                    let result = func.apply(&values);
                    results.push(WindowResult {
                        window_start: window_pts.first().unwrap().timestamp,
                        window_end: window_pts.last().unwrap().timestamp,
                        values,
                        result,
                        function: format!("{:?}", func),
                    });
                }
            }
            WindowType::Sliding { duration_secs } => {
                let mut i = 0;
                while i < sorted.len() {
                    let window_start = sorted[i].timestamp;
                    let window_end = window_start + duration_secs;
                    let window_pts: Vec<_> = sorted[i..]
                        .iter()
                        .take_while(|p| p.timestamp <= window_end)
                        .collect();
                    if window_pts.is_empty() { i += 1; continue; }
                    let values: Vec<f64> = window_pts.iter().map(|p| p.value).collect();
                    let result = func.apply(&values);
                    results.push(WindowResult {
                        window_start,
                        window_end,
                        values,
                        result,
                        function: format!("{:?}", func),
                    });
                    i += 1;
                }
            }
            WindowType::Tumbling { duration_secs } => {
                if let Some(first) = sorted.first() {
                    let mut bucket_start = first.timestamp;
                    while bucket_start <= sorted.last().unwrap().timestamp {
                        let bucket_end = bucket_start + duration_secs;
                        let bucket_pts: Vec<_> = sorted.iter()
                            .filter(|p| p.timestamp >= bucket_start && p.timestamp < bucket_end)
                            .collect();
                        if !bucket_pts.is_empty() {
                            let values: Vec<f64> = bucket_pts.iter().map(|p| p.value).collect();
                            let result = func.apply(&values);
                            results.push(WindowResult {
                                window_start: bucket_start,
                                window_end: bucket_end,
                                values,
                                result,
                                function: format!("{:?}", func),
                            });
                        }
                        bucket_start = bucket_end;
                    }
                }
            }
            WindowType::Session { idle_secs } => {
                let mut session_start = sorted[0].timestamp;
                let mut session_pts = vec![&sorted[0]];
                for w in sorted.windows(2) {
                    let gap = w[1].timestamp - w[0].timestamp;
                    if gap > idle_secs {
                        // 关闭当前 session
                        let values: Vec<f64> = session_pts.iter().map(|p| p.value).collect();
                        let result = func.apply(&values);
                        results.push(WindowResult {
                            window_start: session_start,
                            window_end: session_pts.last().unwrap().timestamp,
                            values,
                            result,
                            function: format!("{:?}", func),
                        });
                        session_start = w[1].timestamp;
                        session_pts = vec![&w[1]];
                    } else {
                        session_pts.push(&w[1]);
                    }
                }
                // 最后一个 session
                if !session_pts.is_empty() {
                    let values: Vec<f64> = session_pts.iter().map(|p| p.value).collect();
                    let result = func.apply(&values);
                    results.push(WindowResult {
                        window_start: session_start,
                        window_end: session_pts.last().unwrap().timestamp,
                        values,
                        result,
                        function: format!("{:?}", func),
                    });
                }
            }
        }
        results
    }
}

// =================== 图算法 ===================

/// 图节点
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GNode {
    pub id: String,
    pub label: String,
    pub properties: HashMap<String, String>,
}

/// 图边
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GEdge {
    pub from: String,
    pub to: String,
    pub label: String,
    pub weight: f64,
    pub properties: HashMap<String, String>,
}

/// 简单图存储
pub struct Graph {
    nodes: HashMap<String, GNode>,
    adj: HashMap<String, Vec<(String, f64)>>, // 邻接表
}

impl Default for Graph {
    fn default() -> Self { Self::new() }
}

impl Graph {
    pub fn new() -> Self {
        Self { nodes: HashMap::new(), adj: HashMap::new() }
    }

    pub fn add_node(&mut self, node: GNode) {
        self.adj.entry(node.id.clone()).or_default();
        self.nodes.insert(node.id.clone(), node);
    }

    pub fn add_edge(&mut self, edge: GEdge) {
        self.adj.entry(edge.from.clone()).or_default().push((edge.to.clone(), edge.weight));
        self.adj.entry(edge.to.clone()).or_default(); // 确保 to 节点存在
        if !self.nodes.contains_key(&edge.to) {
            // 自动创建 to 节点（无属性）
            self.nodes.insert(edge.to.clone(), GNode {
                id: edge.to.clone(),
                label: "auto".to_string(),
                properties: HashMap::new(),
            });
        }
    }

    /// BFS 最短路径（无权图）
    pub fn bfs_path(&self, start: &str, end: &str) -> Option<Vec<String>> {
        if start == end { return Some(vec![start.to_string()]); }
        let mut visited = HashSet::new();
        let mut queue = std::collections::VecDeque::new();
        let mut parent: HashMap<String, String> = HashMap::new();
        visited.insert(start.to_string());
        queue.push_back(start.to_string());

        while let Some(node) = queue.pop_front() {
            if let Some(neighbors) = self.adj.get(&node) {
                for (next, _) in neighbors {
                    if !visited.contains(next) {
                        visited.insert(next.clone());
                        parent.insert(next.clone(), node.clone());
                        if next == end {
                            // 重建路径
                            let mut path = vec![end.to_string()];
                            let mut cur = end.to_string();
                            while let Some(p) = parent.get(&cur) {
                                path.push(p.clone());
                                cur = p.clone();
                            }
                            path.reverse();
                            return Some(path);
                        }
                        queue.push_back(next.clone());
                    }
                }
            }
        }
        None
    }

    /// Dijkstra 最短路径（带权）
    pub fn dijkstra(&self, start: &str, end: &str) -> Option<(Vec<String>, f64)> {
        if start == end { return Some((vec![start.to_string()], 0.0)); }
        let mut dist: HashMap<String, f64> = HashMap::new();
        let mut parent: HashMap<String, String> = HashMap::new();
        let mut heap = BinaryHeap::new();

        dist.insert(start.to_string(), 0.0);
        heap.push(Reverse((0.0_f64, start.to_string())));

        while let Some(Reverse((d, node))) = heap.pop() {
            if node == end { break; }
            if d > *dist.get(&node).unwrap_or(&f64::INFINITY) { continue; }

            if let Some(neighbors) = self.adj.get(&node) {
                for (next, weight) in neighbors {
                    let new_dist = d + weight;
                    if new_dist < *dist.get(next).unwrap_or(&f64::INFINITY) {
                        dist.insert(next.clone(), new_dist);
                        parent.insert(next.clone(), node.clone());
                        heap.push(Reverse((new_dist, next.clone())));
                    }
                }
            }
        }

        if let Some(&total) = dist.get(end) {
            let mut path = vec![end.to_string()];
            let mut cur = end.to_string();
            while let Some(p) = parent.get(&cur) {
                path.push(p.clone());
                cur = p.clone();
            }
            path.reverse();
            Some((path, total))
        } else {
            None
        }
    }

    /// PageRank 算法
    pub fn pagerank(&self, damping: f64, iterations: usize) -> HashMap<String, f64> {
        let n = self.nodes.len() as f64;
        if n == 0.0 { return HashMap::new(); }

        let mut pr: HashMap<String, f64> = self.nodes.keys()
            .map(|k| (k.clone(), 1.0 / n))
            .collect();

        for _ in 0..iterations {
            let mut new_pr: HashMap<String, f64> = self.nodes.keys()
                .map(|k| (k.clone(), (1.0 - damping) / n))
                .collect();

            for (node, neighbors) in &self.adj {
                let out_degree = neighbors.len() as f64;
                if out_degree > 0.0 {
                    let contribution = pr.get(node).unwrap_or(&0.0) * damping / out_degree;
                    for (next, _) in neighbors {
                        *new_pr.entry(next.clone()).or_default() += contribution;
                    }
                }
            }
            pr = new_pr;
        }
        pr
    }

    /// 连通分量
    pub fn connected_components(&self) -> Vec<Vec<String>> {
        let mut visited: HashSet<String> = HashSet::new();
        let mut components: Vec<Vec<String>> = Vec::new();

        for start in self.nodes.keys() {
            if visited.contains(start) { continue; }
            let mut component = Vec::new();
            let mut stack = vec![start.clone()];
            while let Some(node) = stack.pop() {
                if visited.contains(&node) { continue; }
                visited.insert(node.clone());
                component.push(node.clone());
                if let Some(neighbors) = self.adj.get(&node) {
                    for (next, _) in neighbors {
                        if !visited.contains(next) {
                            stack.push(next.clone());
                        }
                    }
                }
            }
            components.push(component);
        }
        components
    }

    /// 三角形计数（社交网络分析）
    pub fn triangle_count(&self) -> HashMap<String, usize> {
        let mut counts: HashMap<String, usize> = self.nodes.keys()
            .map(|k| (k.clone(), 0))
            .collect();

        for a in self.nodes.keys() {
            if let Some(na) = self.adj.get(a) {
                for (b, _) in na {
                    if let Some(nb) = self.adj.get(b) {
                        for (c, _) in nb {
                            if c == a { continue; }
                            // 检查 a-c 是否相连
                            if let Some(nc) = self.adj.get(a) {
                                if nc.iter().any(|(n, _)| n == c) {
                                    *counts.entry(a.clone()).or_default() += 1;
                                }
                            }
                        }
                    }
                }
            }
        }
        counts
    }
}

// =================== 文档 RAG 跨模态链路 ===================

/// 文档分块
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentChunk {
    pub id: String,
    pub document_id: String,
    pub content: String,
    pub embedding: Option<Vec<f32>>,
    pub metadata: HashMap<String, String>,
    pub chunk_index: usize,
}

/// RAG 检索结果
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RagResult {
    pub chunk_id: String,
    pub content: String,
    pub score: f32,
    pub document_id: String,
    pub source_modality: String,
}

/// RAG 检索器：跨模态（文本+图像+表格）链路
pub struct RagRetriever {
    chunks: Arc<RwLock<HashMap<String, DocumentChunk>>>,
    /// 跨模态索引（通过 embedding 链接）
    cross_modal_index: Arc<RwLock<HashMap<String, Vec<String>>>>, // chunk_id -> related_chunk_ids
}

impl Default for RagRetriever {
    fn default() -> Self { Self::new() }
}

impl RagRetriever {
    pub fn new() -> Self {
        Self {
            chunks: Arc::new(RwLock::new(HashMap::new())),
            cross_modal_index: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// 添加文档分块
    pub async fn add_chunk(&self, chunk: DocumentChunk) {
        self.chunks.write().await.insert(chunk.id.clone(), chunk);
    }

    /// 创建跨模态链接（图->正文 / 表格->正文）
    pub async fn link_chunks(&self, source_id: &str, target_ids: Vec<String>) {
        let mut idx = self.cross_modal_index.write().await;
        idx.insert(source_id.to_string(), target_ids);
    }

    /// RAG 检索：先按向量相似度，再展开跨模态链接
    pub async fn retrieve(
        &self,
        query_embedding: &[f32],
        top_k: usize,
        expand_modal_links: bool,
    ) -> Vec<RagResult> {
        let chunks = self.chunks.read().await;
        let mut scored: Vec<(String, f32, String)> = Vec::new();

        for chunk in chunks.values() {
            if let Some(emb) = &chunk.embedding {
                if emb.len() == query_embedding.len() {
                    let score = cosine_sim(query_embedding, emb);
                    let modality = chunk.metadata.get("modality").cloned().unwrap_or_else(|| "text".to_string());
                    scored.push((chunk.id.clone(), score, modality));
                }
            }
        }

        scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        let top: Vec<(String, f32, String)> = scored.into_iter().take(top_k).collect();

        let mut results = Vec::new();
        let links = self.cross_modal_index.read().await;
        for (chunk_id, score, modality) in top {
            if let Some(chunk) = chunks.get(&chunk_id) {
                results.push(RagResult {
                    chunk_id: chunk_id.clone(),
                    content: chunk.content.clone(),
                    score,
                    document_id: chunk.document_id.clone(),
                    source_modality: modality.clone(),
                });

                if expand_modal_links {
                    if let Some(related) = links.get(&chunk_id) {
                        for rid in related {
                            if let Some(rchunk) = chunks.get(rid) {
                                let rmodality = rchunk.metadata.get("modality").cloned().unwrap_or_else(|| "text".to_string());
                                results.push(RagResult {
                                    chunk_id: rid.clone(),
                                    content: rchunk.content.clone(),
                                    score: score * 0.8, // 跨模态衰减
                                    document_id: rchunk.document_id.clone(),
                                    source_modality: rmodality,
                                });
                            }
                        }
                    }
                }
            }
        }
        results
    }
}

fn cosine_sim(a: &[f32], b: &[f32]) -> f32 {
    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let na: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let nb: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    if na == 0.0 || nb == 0.0 { return 0.0; }
    dot / (na * nb)
}

// =================== 领域索引扩展 ===================

/// 电商领域索引：商品/订单/库存
pub struct ECommerceIndex {
    pub products: Arc<RwLock<HashMap<String, Product>>>,
    pub orders: Arc<RwLock<HashMap<String, Order>>>,
    pub inventory: Arc<RwLock<HashMap<String, InventoryItem>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Product {
    pub id: String,
    pub name: String,
    pub category: String,
    pub price: f64,
    pub brand: String,
    pub embedding: Vec<f32>,
    pub tags: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    pub id: String,
    pub user_id: String,
    pub product_ids: Vec<String>,
    pub total: f64,
    pub status: String, // pending / paid / shipped / delivered / refunded
    pub created_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InventoryItem {
    pub sku: String,
    pub product_id: String,
    pub warehouse: String,
    pub stock: u32,
    pub reserved: u32,
}

impl ECommerceIndex {
    pub fn new() -> Self {
        Self {
            products: Arc::new(RwLock::new(HashMap::new())),
            orders: Arc::new(RwLock::new(HashMap::new())),
            inventory: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn add_product(&self, product: Product) {
        self.products.write().await.insert(product.id.clone(), product);
    }

    /// 相似商品推荐（基于 embedding）
    pub async fn recommend_similar(&self, product_id: &str, k: usize) -> Vec<(String, f32)> {
        let products = self.products.read().await;
        let target = match products.get(product_id) {
            Some(p) => p,
            None => return Vec::new(),
        };
        let target_emb = &target.embedding;
        let mut scored: Vec<(String, f32)> = products.iter()
            .filter(|(id, _)| *id != product_id)
            .filter_map(|(id, p)| {
                if p.embedding.len() == target_emb.len() {
                    Some((id.clone(), cosine_sim(&p.embedding, target_emb)))
                } else {
                    None
                }
            })
            .collect();
        scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        scored.truncate(k);
        scored
    }

    /// 按类目筛选 + 价格区间
    pub async fn filter_by_category(
        &self,
        category: &str,
        min_price: f64,
        max_price: f64,
    ) -> Vec<String> {
        let products = self.products.read().await;
        products.values()
            .filter(|p| p.category == category && p.price >= min_price && p.price <= max_price)
            .map(|p| p.id.clone())
            .collect()
    }

    /// 库存预订（事务性：检查并扣减）
    pub async fn reserve_stock(&self, sku: &str, quantity: u32) -> Result<bool, String> {
        let mut inventory = self.inventory.write().await;
        if let Some(item) = inventory.get_mut(sku) {
            let available = item.stock.saturating_sub(item.reserved);
            if available >= quantity {
                item.reserved += quantity;
                return Ok(true);
            } else {
                return Err(format!("Insufficient stock: available={}, requested={}", available, quantity));
            }
        }
        Err(format!("SKU {} not found", sku))
    }
}

impl Default for ECommerceIndex {
    fn default() -> Self { Self::new() }
}

/// 医疗领域索引：患者/诊断/药物
pub struct MedicalIndex {
    pub patients: Arc<RwLock<HashMap<String, Patient>>>,
    pub diagnoses: Arc<RwLock<HashMap<String, Diagnosis>>>,
    pub drugs: Arc<RwLock<HashMap<String, Drug>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Patient {
    pub id: String,
    pub name: String,
    pub age: u32,
    pub gender: String,
    pub medical_record_embedding: Vec<f32>,
    pub allergies: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Diagnosis {
    pub id: String,
    pub patient_id: String,
    pub code: String, // ICD-10
    pub description: String,
    pub diagnosed_at: u64,
    pub confidence: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Drug {
    pub id: String,
    pub name: String,
    pub generic_name: String,
    pub indications: Vec<String>,
    pub contraindications: Vec<String>,
    pub side_effects: Vec<String>,
}

impl MedicalIndex {
    pub fn new() -> Self {
        Self {
            patients: Arc::new(RwLock::new(HashMap::new())),
            diagnoses: Arc::new(RwLock::new(HashMap::new())),
            drugs: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// 检查药物过敏冲突
    pub async fn check_allergy_conflict(&self, patient_id: &str, drug_name: &str) -> Option<String> {
        let patients = self.patients.read().await;
        let patient = patients.get(patient_id)?;
        if patient.allergies.iter().any(|a| drug_name.to_lowercase().contains(&a.to_lowercase())) {
            return Some(format!("Allergy conflict: patient {} is allergic to {}", patient_id, drug_name));
        }
        None
    }

    /// 相似患者病历检索（基于 embedding）
    pub async fn find_similar_patients(&self, patient_id: &str, k: usize) -> Vec<(String, f32)> {
        let patients = self.patients.read().await;
        let target = match patients.get(patient_id) {
            Some(p) => p,
            None => return Vec::new(),
        };
        let target_emb = &target.medical_record_embedding;
        let mut scored: Vec<(String, f32)> = patients.iter()
            .filter(|(id, _)| *id != patient_id)
            .filter_map(|(id, p)| {
                if p.medical_record_embedding.len() == target_emb.len() {
                    Some((id.clone(), cosine_sim(&p.medical_record_embedding, target_emb)))
                } else {
                    None
                }
            })
            .collect();
        scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        scored.truncate(k);
        scored
    }
}

impl Default for MedicalIndex {
    fn default() -> Self { Self::new() }
}

/// 物流领域索引：包裹/路线/承运商
pub struct LogisticsIndex {
    pub packages: Arc<RwLock<HashMap<String, Package>>>,
    pub routes: Arc<RwLock<HashMap<String, Route>>>,
    pub carriers: Arc<RwLock<HashMap<String, Carrier>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Package {
    pub tracking_number: String,
    pub sender: String,
    pub receiver: String,
    pub current_location: GeoPoint3D,
    pub destination: GeoPoint3D,
    pub status: String, // in_transit / delivered / exception
    pub estimated_delivery: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Route {
    pub id: String,
    pub waypoints: Vec<GeoPoint3D>,
    pub total_distance_km: f64,
    pub estimated_duration_hours: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Carrier {
    pub id: String,
    pub name: String,
    pub rating: f32,
    pub service_zones: Vec<String>,
}

impl LogisticsIndex {
    pub fn new() -> Self {
        Self {
            packages: Arc::new(RwLock::new(HashMap::new())),
            routes: Arc::new(RwLock::new(HashMap::new())),
            carriers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// 包裹 3D 距离计算（当前位置到目的地）
    pub async fn distance_to_destination(&self, tracking: &str) -> Option<f64> {
        let packages = self.packages.read().await;
        let pkg = packages.get(tracking)?;
        Some(pkg.current_location.haversine_3d(&pkg.destination))
    }

    /// 路线总长度
    pub async fn route_distance(&self, route_id: &str) -> Option<f64> {
        let routes = self.routes.read().await;
        let route = routes.get(route_id)?;
        let mut total = 0.0;
        for w in route.waypoints.windows(2) {
            total += w[0].haversine_3d(&w[1]);
        }
        Some(total)
    }
}

impl Default for LogisticsIndex {
    fn default() -> Self { Self::new() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_3d_distance() {
        let p1 = GeoPoint3D::new(0.0, 0.0, 0.0);
        let p2 = GeoPoint3D::new(3.0, 4.0, 12.0);
        assert!((p1.distance_to(&p2) - 13.0).abs() < 1e-6);
    }

    #[test]
    fn test_bbox_intersects() {
        let a = GeoBoundingBox3D::new(GeoPoint3D::new(0.0, 0.0, 0.0), GeoPoint3D::new(5.0, 5.0, 5.0));
        let b = GeoBoundingBox3D::new(GeoPoint3D::new(3.0, 3.0, 3.0), GeoPoint3D::new(8.0, 8.0, 8.0));
        assert!(a.intersects(&b));
    }

    #[test]
    fn test_graph_bfs() {
        let mut g = Graph::new();
        g.add_node(GNode { id: "A".to_string(), label: "x".to_string(), properties: HashMap::new() });
        g.add_node(GNode { id: "B".to_string(), label: "x".to_string(), properties: HashMap::new() });
        g.add_node(GNode { id: "C".to_string(), label: "x".to_string(), properties: HashMap::new() });
        g.add_edge(GEdge { from: "A".to_string(), to: "B".to_string(), label: "ab".to_string(), weight: 1.0, properties: HashMap::new() });
        g.add_edge(GEdge { from: "B".to_string(), to: "C".to_string(), label: "bc".to_string(), weight: 1.0, properties: HashMap::new() });
        let path = g.bfs_path("A", "C").unwrap();
        assert_eq!(path, vec!["A".to_string(), "B".to_string(), "C".to_string()]);
    }

    #[test]
    fn test_graph_dijkstra() {
        let mut g = Graph::new();
        g.add_node(GNode { id: "A".to_string(), label: "x".to_string(), properties: HashMap::new() });
        g.add_node(GNode { id: "B".to_string(), label: "x".to_string(), properties: HashMap::new() });
        g.add_node(GNode { id: "C".to_string(), label: "x".to_string(), properties: HashMap::new() });
        g.add_edge(GEdge { from: "A".to_string(), to: "B".to_string(), label: "ab".to_string(), weight: 1.0, properties: HashMap::new() });
        g.add_edge(GEdge { from: "B".to_string(), to: "C".to_string(), label: "bc".to_string(), weight: 2.0, properties: HashMap::new() });
        let (path, dist) = g.dijkstra("A", "C").unwrap();
        assert_eq!(path, vec!["A".to_string(), "B".to_string(), "C".to_string()]);
        assert_eq!(dist, 3.0);
    }

    #[test]
    fn test_graph_pagerank() {
        let mut g = Graph::new();
        for n in ["A", "B", "C", "D"] {
            g.add_node(GNode { id: n.to_string(), label: "x".to_string(), properties: HashMap::new() });
        }
        g.add_edge(GEdge { from: "A".to_string(), to: "B".to_string(), label: "".to_string(), weight: 1.0, properties: HashMap::new() });
        g.add_edge(GEdge { from: "B".to_string(), to: "C".to_string(), label: "".to_string(), weight: 1.0, properties: HashMap::new() });
        g.add_edge(GEdge { from: "C".to_string(), to: "D".to_string(), label: "".to_string(), weight: 1.0, properties: HashMap::new() });
        g.add_edge(GEdge { from: "D".to_string(), to: "A".to_string(), label: "".to_string(), weight: 1.0, properties: HashMap::new() });
        let pr = g.pagerank(0.85, 50);
        assert_eq!(pr.len(), 4);
        // 所有节点的 PR 应该接近 0.25
        for v in pr.values() {
            assert!((v - 0.25).abs() < 0.1);
        }
    }

    #[test]
    fn test_window_function() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        assert_eq!(WindowFunction::Sum.apply(&values), 15.0);
        assert_eq!(WindowFunction::Avg.apply(&values), 3.0);
        assert_eq!(WindowFunction::Min.apply(&values), 1.0);
        assert_eq!(WindowFunction::Max.apply(&values), 5.0);
        assert!((WindowFunction::StdDev.apply(&values) - 1.5811).abs() < 0.01);
    }

    #[tokio::test]
    async fn test_ecommerce_recommend() {
        let idx = ECommerceIndex::new();
        let p1 = Product { id: "1".to_string(), name: "iPhone".to_string(), category: "phone".to_string(), price: 999.0, brand: "Apple".to_string(), embedding: vec![0.1, 0.2, 0.3], tags: vec!["smartphone".to_string()] };
        let p2 = Product { id: "2".to_string(), name: "Galaxy".to_string(), category: "phone".to_string(), price: 899.0, brand: "Samsung".to_string(), embedding: vec![0.1, 0.2, 0.3], tags: vec!["smartphone".to_string()] };
        let p3 = Product { id: "3".to_string(), name: "Pencil".to_string(), category: "stationery".to_string(), price: 1.0, brand: "Generic".to_string(), embedding: vec![0.9, 0.8, 0.7], tags: vec![] };
        idx.add_product(p1).await;
        idx.add_product(p2).await;
        idx.add_product(p3).await;
        let recs = idx.recommend_similar("1", 2).await;
        assert_eq!(recs[0].0, "2"); // Galaxy embedding 与 iPhone 完全相同
    }

    #[tokio::test]
    async fn test_medical_allergy_check() {
        let idx = MedicalIndex::new();
        let p = Patient { id: "P1".to_string(), name: "Alice".to_string(), age: 30, gender: "F".to_string(), medical_record_embedding: vec![], allergies: vec!["penicillin".to_string()] };
        idx.patients.write().await.insert("P1".to_string(), p);
        let conflict = idx.check_allergy_conflict("P1", "Penicillin G").await;
        assert!(conflict.is_some());
    }

    #[tokio::test]
    async fn test_logistics_distance() {
        let idx = LogisticsIndex::new();
        let pkg = Package {
            tracking_number: "T1".to_string(),
            sender: "Beijing".to_string(),
            receiver: "Shanghai".to_string(),
            current_location: GeoPoint3D::new(116.4, 39.9, 50.0),
            destination: GeoPoint3D::new(121.5, 31.2, 10.0),
            status: "in_transit".to_string(),
            estimated_delivery: 0,
        };
        idx.packages.write().await.insert("T1".to_string(), pkg);
        let d = idx.distance_to_destination("T1").await.unwrap();
        // 北京到上海约 1067 公里
        assert!(d > 1_000_000.0 && d < 1_200_000.0);
    }
}
