//! Vector indexing for CortexDB

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};
use crate::coretex_core::{CoreTexError, Result};

/// Result of a vector search
#[derive(Debug, Clone, PartialEq)]
pub struct SearchResult {
    /// ID of the matched vector
    pub id: String,

    /// Distance from the query vector
    pub distance: f32,
}

impl Eq for SearchResult {}

impl PartialOrd for SearchResult {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.distance.partial_cmp(&other.distance)
    }
}

impl Ord for SearchResult {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.distance.partial_cmp(&other.distance).unwrap_or(std::cmp::Ordering::Equal)
    }
}

/// Vector index trait
#[async_trait]
pub trait VectorIndex: Send + Sync {
    /// Add a vector to the index
    async fn add(&self, id: &str, vector: &[f32]) -> Result<()>;
    
    /// Remove a vector from the index
    async fn remove(&self, id: &str) -> Result<bool>;
    
    /// Search for similar vectors
    async fn search(&self, query: &[f32], k: usize) -> Result<Vec<SearchResult>>;
    
    /// Build the index (if needed)
    async fn build(&self) -> Result<()>;
    
    /// Clear the index
    async fn clear(&self) -> Result<()>;
    
    /// Persist this index to `path`, tagging it with `checksum` so a stale
    /// index can be rejected instead of silently serving incomplete results.
    /// Index types without on-disk support leave the default (`Ok(false)`).
    async fn persist(&self, _path: &std::path::Path, _checksum: &str) -> Result<bool> {
        Ok(false)
    }

    /// Clone the index into a box
    fn clone_box(&self) -> Box<dyn VectorIndex>;
}

/// Brute-force index implementation
#[derive(Clone)]
pub struct BruteForceIndex {
    vectors: std::sync::Arc<tokio::sync::RwLock<std::collections::HashMap<String, Vec<f32>>>>,
    metric: String,
}

/// HNSW (Hierarchical Navigable Small World) index implementation
#[derive(Clone)]
pub struct HNSWIndex {
    vectors: std::sync::Arc<tokio::sync::RwLock<std::collections::HashMap<String, Vec<f32>>>>,
    metric: String,
    m: usize,
    ef_construction: usize,
    ef_search: usize,
    max_level: usize,
    entry_point: std::sync::Arc<tokio::sync::RwLock<Option<String>>>,
    graph: std::sync::Arc<tokio::sync::RwLock<std::collections::HashMap<String, Vec<Vec<String>>>>>,
}

/// IVF (Inverted File) index implementation
#[derive(Clone)]
pub struct IVFIndex {
    vectors: std::sync::Arc<tokio::sync::RwLock<std::collections::HashMap<String, Vec<f32>>>>,
    metric: String,
    // IVF-specific parameters
    nlist: usize, // Number of clusters
    nprobe: usize, // Number of clusters to probe during search
    centroids: std::sync::Arc<tokio::sync::RwLock<Vec<Vec<f32>>>>, // Cluster centroids
    vector_to_cluster: std::sync::Arc<tokio::sync::RwLock<std::collections::HashMap<String, usize>>>, // Mapping from vector ID to cluster ID
    /// Set by every write, cleared by `build`. Clustering is deferred to the
    /// next search so a batch of inserts costs one k-means pass, not one per
    /// insert.
    dirty: std::sync::Arc<AtomicBool>,
}

/// Scalar index implementation for numerical values
#[derive(Clone)]
pub struct ScalarIndex {
    scalars: std::sync::Arc<tokio::sync::RwLock<std::collections::HashMap<String, f32>>>,
    sorted_scalars: std::sync::Arc<tokio::sync::RwLock<Vec<(f32, String)>>>, // Sorted list of (value, ID) pairs
}

/// Distance between two vectors under `metric`, lower always meaning "more
/// similar", so every index can rank results the same way.
///
/// This is the single source of truth for distance. Each index used to carry
/// its own copy of this match, and they disagreed: only `cosine` and
/// `euclidean` were implemented, while `dotproduct` and `manhattan` fell
/// through a `_ =>` arm into cosine. Collections created with either of those
/// metrics were therefore ranked by the wrong metric, silently.
///
/// A length mismatch yields the worst possible distance rather than silently
/// comparing the overlapping prefix.
pub(crate) fn metric_distance(metric: &str, a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() {
        return f32::MAX;
    }

    match metric {
        "euclidean" => a
            .iter()
            .zip(b)
            .map(|(x, y)| (x - y) * (x - y))
            .sum::<f32>()
            .sqrt(),
        // Negated inner product: maximising similarity is the same as
        // minimising this, which keeps "lower is better" true.
        "dotproduct" => -a.iter().zip(b).map(|(x, y)| x * y).sum::<f32>(),
        "manhattan" => a.iter().zip(b).map(|(x, y)| (x - y).abs()).sum(),
        // Cosine, including the empty/unknown metric name.
        _ => {
            let dot: f32 = a.iter().zip(b).map(|(x, y)| x * y).sum();
            let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
            let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
            if norm_a == 0.0 || norm_b == 0.0 {
                return 1.0;
            }
            1.0 - dot / (norm_a * norm_b)
        }
    }
}

impl BruteForceIndex {
    /// Create a new brute-force index with the specified distance metric
    pub fn new(metric: &str) -> Self {
        Self {
            vectors: std::sync::Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
            metric: metric.to_string(),
        }
    }
    
    /// Calculate distance between two vectors
    /// Distance between two vectors under this index's metric.
    fn calculate_distance(&self, a: &[f32], b: &[f32]) -> f32 {
        metric_distance(&self.metric, a, b)
    }
}

impl HNSWIndex {
    /// Create a new HNSW index with the specified parameters
    pub fn new(metric: &str) -> Self {
        Self {
            vectors: std::sync::Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
            metric: metric.to_string(),
            m: 16,
            ef_construction: 200,
            ef_search: 50,
            max_level: 16,
            entry_point: std::sync::Arc::new(tokio::sync::RwLock::new(None)),
            graph: std::sync::Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
        }
    }

    pub fn with_params(metric: &str, m: usize, ef_construction: usize, ef_search: usize) -> Self {
        Self {
            vectors: std::sync::Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
            metric: metric.to_string(),
            m,
            ef_construction,
            ef_search,
            max_level: 16,
            entry_point: std::sync::Arc::new(tokio::sync::RwLock::new(None)),
            graph: std::sync::Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
        }
    }

    /// Distance between two vectors under this index's metric.
    fn calculate_distance(&self, a: &[f32], b: &[f32]) -> f32 {
        metric_distance(&self.metric, a, b)
    }

    /// Random level for a new node, drawn from the standard HNSW geometric
    /// distribution with `mL = 1 / ln(M)`.
    ///
    /// This used to be `-ln(p) * 16.0`, which is `ln(M)` out by a factor of
    /// `1/mL`: it produced levels averaging ~11 instead of ~1, so the upper
    /// levels held one node each and carried no useful navigation.
    fn random_level(&self) -> usize {
        let mut rng = rand::thread_rng();
        // `gen_range` excludes zero, keeping `ln` finite and the cast bounded.
        let p: f64 = rand::Rng::gen_range(&mut rng, f64::EPSILON..1.0);
        let ml = 1.0 / (self.m.max(2) as f64).ln();
        ((-p.ln()) * ml) as usize
    }

    fn search_layer(
        &self,
        entry_id: &str,
        query: &[f32],
        ef: usize,
        layer: usize,
        vectors: &std::collections::HashMap<String, Vec<f32>>,
        graph: &std::collections::HashMap<String, Vec<Vec<String>>>,
    ) -> (std::collections::BinaryHeap<std::cmp::Reverse<SearchResult>>, std::collections::HashSet<String>) {
        use std::collections::{BinaryHeap, HashSet};

        let mut visited = HashSet::new();
        let mut candidates = BinaryHeap::new();
        let mut results = BinaryHeap::new();

        visited.insert(entry_id.to_string());
        // Guard against a graph node that has no vector (e.g. a legacy orphan
        // backlink): `unwrap()` here used to panic and take down the whole
        // search. A missing entry point just yields no results.
        let Some(entry_vec) = vectors.get(entry_id) else {
            return (results, visited);
        };
        let dist = self.calculate_distance(query, entry_vec);
        candidates.push(std::cmp::Reverse(SearchResult { id: entry_id.to_string(), distance: dist }));
        results.push(std::cmp::Reverse(SearchResult { id: entry_id.to_string(), distance: dist }));

        while let Some(std::cmp::Reverse(current)) = candidates.pop() {
            let furthest = results.peek().map(|r| r.0.distance).unwrap_or(f32::MAX);
            if current.distance > furthest {
                break;
            }
            for neighbor_id in Self::get_neighbors_from(&current.id, layer, graph) {
                if visited.insert(neighbor_id.clone()) {
                    // Skip dangling neighbors instead of panicking.
                    let Some(neighbor_vec) = vectors.get(&neighbor_id) else {
                        continue;
                    };
                    let dist = self.calculate_distance(query, neighbor_vec);
                    if dist < furthest || results.len() < ef {
                        candidates.push(std::cmp::Reverse(SearchResult { id: neighbor_id.clone(), distance: dist }));
                        results.push(std::cmp::Reverse(SearchResult { id: neighbor_id.clone(), distance: dist }));
                        if results.len() > ef {
                            results.pop();
                        }
                    }
                }
            }
        }

        (results, visited)
    }

    fn get_neighbors_from(id: &str, layer: usize, graph: &std::collections::HashMap<String, Vec<Vec<String>>>) -> Vec<String> {
        graph.get(id)
            .and_then(|levels| levels.get(layer))
            .cloned()
            .unwrap_or_default()
    }
}

impl IVFIndex {
    /// Create a new IVF index with the specified parameters
    pub fn new(metric: &str) -> Self {
        Self {
            vectors: std::sync::Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
            metric: metric.to_string(),
            nlist: 100, // Default number of clusters
            nprobe: 10, // Default number of clusters to probe
            centroids: std::sync::Arc::new(tokio::sync::RwLock::new(Vec::new())),
            vector_to_cluster: std::sync::Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
            dirty: std::sync::Arc::new(AtomicBool::new(true)),
        }
    }

    pub fn with_params(metric: &str, nlist: usize, nprobe: usize) -> Self {
        Self {
            vectors: std::sync::Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
            metric: metric.to_string(),
            nlist,
            nprobe,
            centroids: std::sync::Arc::new(tokio::sync::RwLock::new(Vec::new())),
            vector_to_cluster: std::sync::Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
            dirty: std::sync::Arc::new(AtomicBool::new(true)),
        }
    }

    /// Calculate distance between two vectors
    /// Distance between two vectors under this index's metric.
    fn calculate_distance(&self, a: &[f32], b: &[f32]) -> f32 {
        metric_distance(&self.metric, a, b)
    }
    
    /// Assign a vector to the nearest centroid
    async fn assign_to_cluster(&self, vector: &[f32]) -> usize {
        let centroids = self.centroids.read().await;
        if centroids.is_empty() {
            return 0;
        }
        
        let mut min_distance = f32::MAX;
        let mut closest_cluster = 0;
        
        for (i, centroid) in centroids.iter().enumerate() {
            let distance = self.calculate_distance(vector, centroid);
            if distance < min_distance {
                min_distance = distance;
                closest_cluster = i;
            }
        }
        
        closest_cluster
    }

    /// True when the clustered state no longer reflects the stored vectors.
    ///
    /// Also true for a freshly loaded index that has vectors but no centroids,
    /// so a build can never be silently skipped.
    async fn is_stale(&self) -> bool {
        if self.dirty.load(Ordering::Acquire) {
            return true;
        }
        // No vectors means there is nothing to cluster: an empty index is not
        // stale, it is simply empty.
        if self.vectors.read().await.is_empty() {
            return false;
        }
        self.centroids.read().await.is_empty()
    }
}

impl ScalarIndex {
    /// Create a new scalar index
    pub fn new() -> Self {
        Self {
            scalars: std::sync::Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
            sorted_scalars: std::sync::Arc::new(tokio::sync::RwLock::new(Vec::new())),
        }
    }
    
    /// Build the sorted `(value, id)` snapshot for a scalar map.
    ///
    /// Pure and lock-free on purpose: `add()` calls this while already holding
    /// the `scalars` write guard, so re-acquiring that guard here would
    /// self-deadlock. The previous `blocking_read()` version additionally
    /// panicked whenever it ran inside a tokio runtime.
    fn sorted_snapshot(
        scalars: &std::collections::HashMap<String, f32>,
    ) -> Vec<(f32, String)> {
        let mut sorted = scalars.iter()
            .map(|(id, value)| (*value, id.clone()))
            .collect::<Vec<_>>();

        sorted.sort_by(|a, b| {
            a.0.partial_cmp(&b.0).unwrap_or_else(|| {
                if a.0.is_nan() && b.0.is_nan() {
                    std::cmp::Ordering::Equal
                } else if a.0.is_nan() {
                    std::cmp::Ordering::Greater
                } else if b.0.is_nan() {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Equal
                }
            })
        });

        sorted
    }

    /// Rebuild `sorted_scalars` from the current scalar map.
    ///
    /// Acquires the read guard, derives the snapshot, releases it, and only
    /// then publishes the result — no lock is held across the update.
    async fn refresh_sorted(&self) {
        let sorted = {
            let scalars = self.scalars.read().await;
            Self::sorted_snapshot(&scalars)
        };
        *self.sorted_scalars.write().await = sorted;
    }
}

#[async_trait]
impl VectorIndex for BruteForceIndex {
    async fn add(&self, id: &str, vector: &[f32]) -> Result<()> {
        let mut vectors = self.vectors.write().await;
        vectors.insert(id.to_string(), vector.to_vec());
        Ok(())
    }
    
    async fn remove(&self, id: &str) -> Result<bool> {
        let mut vectors = self.vectors.write().await;
        Ok(vectors.remove(id).is_some())
    }
    
    async fn search(&self, query: &[f32], k: usize) -> Result<Vec<SearchResult>> {
        let vectors = self.vectors.read().await;
        
        let mut results: Vec<SearchResult> = vectors
            .iter()
            .map(|(id, vec)| {
                let distance = self.calculate_distance(query, vec);
                SearchResult {
                    id: id.clone(),
                    distance,
                }
            })
            .collect();
        
        // Sort by distance (ascending)
        results.sort_by(|a, b| {
            a.distance.partial_cmp(&b.distance).unwrap_or_else(|| {
                if a.distance.is_nan() && b.distance.is_nan() {
                    std::cmp::Ordering::Equal
                } else if a.distance.is_nan() {
                    std::cmp::Ordering::Greater
                } else if b.distance.is_nan() {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Equal
                }
            })
        });
        
        // Take top k results
        Ok(results.into_iter().take(k).collect())
    }
    
    async fn build(&self) -> Result<()> {
        // Brute-force index doesn't need building
        Ok(())
    }
    
    async fn clear(&self) -> Result<()> {
        let mut vectors = self.vectors.write().await;
        vectors.clear();
        Ok(())
    }
    
    fn clone_box(&self) -> Box<dyn VectorIndex> {
        Box::new(self.clone())
    }
}

#[async_trait]
impl VectorIndex for HNSWIndex {
    async fn add(&self, id: &str, vector: &[f32]) -> Result<()> {
        {
            let mut vectors = self.vectors.write().await;
            vectors.insert(id.to_string(), vector.to_vec());
        }

        // Lock order: vectors → entry_point → graph (same as search/remove/
        // clear). Taking graph first and vectors later inverted the order and
        // deadlocked against concurrent searches.
        let level = self.random_level();
        let entry_point = self.entry_point.read().await.clone();
        let vectors_read = self.vectors.read().await;
        let mut graph = self.graph.write().await;

        // Highest level present *before* this insert, used by the entry-point
        // rule below.
        let top_most_level = graph
            .values()
            .map(|levels| levels.len().saturating_sub(1))
            .max()
            .unwrap_or(0);

        let mut node_levels = vec![Vec::new(); level + 1];
        if let Some(ref ep) = entry_point {
            let top_level = graph.get(ep).map(|l| l.len().saturating_sub(1)).unwrap_or(0);

            for l in (0..=level.min(top_level)).rev() {
                let (results, _) = self.search_layer(ep, vector, self.ef_construction, l, &vectors_read, &graph);
                // `results` is a heap, so iterating it yields an arbitrary
                // order; taking the first `m` wired the graph to random
                // neighbours. Rank by distance and keep the closest `m`.
                let mut ranked: Vec<SearchResult> = results.into_iter().map(|r| r.0).collect();
                ranked.sort_by(|a, b| {
                    a.distance.partial_cmp(&b.distance).unwrap_or(std::cmp::Ordering::Equal)
                });
                ranked.truncate(self.m);
                let neighbors: Vec<String> = ranked.into_iter().map(|r| r.id).collect();
                if l <= level {
                    node_levels[l] = neighbors.clone();
                }
                for neighbor_id in &neighbors {
                    let neighbor_levels = graph.entry(neighbor_id.clone()).or_insert_with(|| {
                        let mut v = Vec::new();
                        v.push(Vec::new());
                        v
                    });
                    if l < neighbor_levels.len() {
                        neighbor_levels[l].push(id.to_string());
                        if neighbor_levels[l].len() > self.m {
                            neighbor_levels[l].truncate(self.m);
                        }
                    }
                }
            }
        }

        graph.insert(id.to_string(), node_levels);
        let update_ep = entry_point.is_none() || level > top_most_level;
        drop(graph);
        drop(vectors_read);

        // The entry point must be the node with the highest level. This used to
        // compare against `map(|_| 0)`, making the condition `level > 0`, so any
        // node above level 0 took over the entry point and the hierarchy became
        // unnavigable from the top.
        if update_ep {
            *self.entry_point.write().await = Some(id.to_string());
        }

        Ok(())
    }

    async fn remove(&self, id: &str) -> Result<bool> {
        // Lock order: vectors → entry_point → graph (same as add/build/search).
        // The old version took vectors → graph → entry_point and deadlocked
        // (AB-BA) against `add`, which holds entry_point while acquiring graph.
        let mut vectors = self.vectors.write().await;
        let removed = vectors.remove(id).is_some();

        // Snapshot the current entry point while holding the same hierarchy
        // position as `search`, so removal and search agree on it.
        let ep_was_self = self
            .entry_point
            .read()
            .await
            .as_ref()
            .map(|e| e == id)
            .unwrap_or(false);

        let mut graph = self.graph.write().await;
        graph.remove(id);
        // Unlink every backlink that still points at `id`; leaving them behind
        // would make `search_layer` walk into an id that no longer has a vector.
        for levels in graph.values_mut() {
            for layer in levels.iter_mut() {
                layer.retain(|n| n != id);
            }
        }

        // If the removed node was the entry point, promote the surviving node
        // with the highest level — not an arbitrary `keys().next()`. The entry
        // point must stay the highest-level node or the hierarchy stops being
        // navigable from the top (the same invariant `add` maintains).
        if ep_was_self {
            let new_ep = graph
                .iter()
                .max_by_key(|(_, levels)| levels.len())
                .map(|(node, _)| node.clone());
            drop(graph);
            *self.entry_point.write().await = new_ep;
        } else {
            drop(graph);
        }

        Ok(removed)
    }

    async fn search(&self, query: &[f32], k: usize) -> Result<Vec<SearchResult>> {
        let vectors = self.vectors.read().await;
        if vectors.is_empty() {
            return Ok(Vec::new());
        }

        let entry_point = self.entry_point.read().await.clone();
        let ep = match entry_point {
            Some(ep) => ep,
            None => return Ok(Vec::new()),
        };

        let graph = self.graph.read().await;
        let top_level = graph.get(&ep).map(|l| l.len().saturating_sub(1)).unwrap_or(0);

        let mut current_entry = ep.clone();
        for l in (1..=top_level).rev() {
            let (results, _) = self.search_layer(&current_entry, query, 1, l, &vectors, &graph);
            if let Some(closest) = results.into_iter().min_by(|a, b| a.0.distance.partial_cmp(&b.0.distance).unwrap()) {
                current_entry = closest.0.id;
            }
        }

        let (results, _) = self.search_layer(&current_entry, query, self.ef_search.max(k), 0, &vectors, &graph);
        let mut final_results: Vec<SearchResult> = results.into_iter().map(|r| r.0).collect();
        final_results.sort_by(|a, b| a.distance.partial_cmp(&b.distance).unwrap());
        final_results.truncate(k);

        Ok(final_results)
    }

    async fn build(&self) -> Result<()> {
        let vectors = self.vectors.read().await;
        let ids: Vec<String> = vectors.keys().cloned().collect();
        drop(vectors);

        for id in &ids {
            let vector = {
                let v = self.vectors.read().await;
                v.get(id).cloned()
            };
            if let Some(vec) = vector {
                let level = self.random_level();
                // Lock order: vectors → entry_point → graph (see `add`).
                let entry_point = self.entry_point.read().await.clone();
                let vectors_read = self.vectors.read().await;
                let mut graph = self.graph.write().await;

                // Highest level present *before* this insert.
                let top_most_level = graph
                    .values()
                    .map(|levels| levels.len().saturating_sub(1))
                    .max()
                    .unwrap_or(0);

                let mut node_levels = vec![Vec::new(); level + 1];
                let update_ep = entry_point.is_none() || level > top_most_level;
                if let Some(ref ep) = entry_point {
                    let top_level = graph.get(ep).map(|l| l.len().saturating_sub(1)).unwrap_or(0);

                    for l in (0..=level.min(top_level)).rev() {
                        let (results, _) = self.search_layer(ep, &vec, self.ef_construction, l, &vectors_read, &graph);
                        // Keep the closest `m`, not the heap's arbitrary first `m`.
                        let mut ranked: Vec<SearchResult> = results.into_iter().map(|r| r.0).collect();
                        ranked.sort_by(|a, b| {
                            a.distance.partial_cmp(&b.distance).unwrap_or(std::cmp::Ordering::Equal)
                        });
                        ranked.truncate(self.m);
                        let neighbors: Vec<String> = ranked.into_iter().map(|r| r.id).collect();
                        if l <= level {
                            node_levels[l] = neighbors.clone();
                        }
                        for neighbor_id in &neighbors {
                            let neighbor_levels = graph.entry(neighbor_id.clone()).or_insert_with(|| {
                                let mut v = Vec::new();
                                v.push(Vec::new());
                                v
                            });
                            if l < neighbor_levels.len() {
                                neighbor_levels[l].push(id.to_string());
                                if neighbor_levels[l].len() > self.m {
                                    neighbor_levels[l].truncate(self.m);
                                }
                            }
                        }
                    }
                }

                graph.insert(id.clone(), node_levels);
                drop(graph);
                drop(vectors_read);

                // The entry point must be the node with the highest level.
                // This used to compare against `map(|_| 0)`, making the
                // condition `level > 0`, so any node above level 0 took over
                // the entry point and the hierarchy stopped being navigable
                // from the top.
                if update_ep {
                    *self.entry_point.write().await = Some(id.clone());
                }
            }
        }

        Ok(())
    }

    async fn clear(&self) -> Result<()> {
        // Lock order: vectors → entry_point → graph (same as add/build/search/
        // remove). Take the entry_point lock before graph: clearing entry_point
        // last would deadlock against `add`, which holds entry_point while
        // waiting for graph.
        let mut vectors = self.vectors.write().await;
        vectors.clear();
        {
            let mut ep = self.entry_point.write().await;
            *ep = None;
        }
        let mut graph = self.graph.write().await;
        graph.clear();
        Ok(())
    }

    async fn persist(&self, path: &std::path::Path, checksum: &str) -> Result<bool> {
        // Lock order: vectors → entry_point → graph (see `add`).
        let vectors = self.vectors.read().await;
        let entry_point = self.entry_point.read().await.clone();
        let graph = self.graph.read().await;

        let data = HNSWIndexData {
            metric: self.metric.clone(),
            m: self.m,
            ef_construction: self.ef_construction,
            ef_search: self.ef_search,
            max_level: self.max_level,
            entry_point,
            vectors: vectors.clone(),
            graph: graph.clone(),
        };
        let count = data.vectors.len();
        write_index_file(
            path,
            "hnsw",
            &self.metric,
            count,
            checksum,
            serde_json::to_value(&data)?,
        )?;
        Ok(true)
    }

    fn clone_box(&self) -> Box<dyn VectorIndex> {
        Box::new(self.clone())
    }
}

#[async_trait]
impl VectorIndex for IVFIndex {
    async fn add(&self, id: &str, vector: &[f32]) -> Result<()> {
        let mut vectors = self.vectors.write().await;
        vectors.insert(id.to_string(), vector.to_vec());

        let cluster_id = self.assign_to_cluster(vector).await;
        let mut vector_to_cluster = self.vector_to_cluster.write().await;
        vector_to_cluster.insert(id.to_string(), cluster_id);

        self.dirty.store(true, Ordering::Release);

        Ok(())
    }

    async fn remove(&self, id: &str) -> Result<bool> {
        let mut vectors = self.vectors.write().await;
        let removed = vectors.remove(id).is_some();

        if removed {
            let mut vector_to_cluster = self.vector_to_cluster.write().await;
            vector_to_cluster.remove(id);
            self.dirty.store(true, Ordering::Release);
        }

        Ok(removed)
    }

    async fn search(&self, query: &[f32], k: usize) -> Result<Vec<SearchResult>> {
        // Clustering is deferred: writes only mark the index stale, and the
        // single rebuild happens here instead of once per insert. Without this
        // a freshly populated IVF index had no centroids at all and answered
        // every query with an empty result set, silently.
        if self.is_stale().await {
            self.build().await?;
        }

        let vectors = self.vectors.read().await;

        // Genuinely empty is the only case where an empty answer is correct.
        if vectors.is_empty() {
            return Ok(Vec::new());
        }

        let vector_to_cluster = self.vector_to_cluster.read().await;
        let centroids = self.centroids.read().await;

        if centroids.is_empty() {
            return Err(CoreTexError::IndexError(
                "IVF index has no centroids; refusing to return an empty result set"
                    .to_string(),
            ));
        }

        let mut cluster_distances: Vec<(usize, f32)> = centroids
            .iter()
            .enumerate()
            .map(|(i, centroid)| {
                let dist = self.calculate_distance(query, centroid);
                (i, dist)
            })
            .collect();

        cluster_distances.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

        let nprobe = self.nprobe.min(cluster_distances.len());
        let probed_clusters: std::collections::HashSet<usize> = cluster_distances
            .into_iter()
            .take(nprobe)
            .map(|(i, _)| i)
            .collect();

        let mut results: Vec<SearchResult> = vectors
            .iter()
            .filter(|(id, _)| {
                vector_to_cluster
                    .get(*id)
                    .map(|c| probed_clusters.contains(c))
                    .unwrap_or(false)
            })
            .map(|(id, vec)| {
                let distance = self.calculate_distance(query, vec);
                SearchResult {
                    id: id.clone(),
                    distance,
                }
            })
            .collect();

        results.sort_by(|a, b| a.distance.partial_cmp(&b.distance).unwrap_or(std::cmp::Ordering::Equal));
        results.truncate(k);

        Ok(results)
    }

    async fn build(&self) -> Result<()> {
        let vectors = self.vectors.read().await;
        let all_vectors: Vec<Vec<f32>> = vectors.values().cloned().collect();
        let ids: Vec<String> = vectors.keys().cloned().collect();
        drop(vectors);

        // Nothing to cluster: drop any stale centroids rather than leave a
        // clustering that no longer describes the data.
        if all_vectors.is_empty() {
            self.centroids.write().await.clear();
            self.vector_to_cluster.write().await.clear();
            self.dirty.store(false, Ordering::Release);
            return Ok(());
        }

        let dim = all_vectors[0].len();

        // Clamp instead of bailing out. The old guard returned early whenever
        // `len < nlist` (default nlist = 100), so a small collection was never
        // clustered and search answered empty forever. Clamping also keeps
        // `step_by` below from being called with 0, which panics.
        let nlist = self.nlist.clamp(1, all_vectors.len());
        let step = (all_vectors.len() / nlist).max(1);

        // k-means clusters by squared L2 regardless of `metric`; the metric
        // only decides ranking, which goes through `metric_distance`. This is
        // how IVF is normally built, and it keeps dotproduct (where the
        // "distance" is a negated inner product) from collapsing every vector
        // into a single cluster.
        let mut centroids: Vec<Vec<f32>> = all_vectors
            .iter()
            .step_by(step)
            .take(nlist)
            .cloned()
            .collect();

        while centroids.len() < nlist {
            centroids.push(vec![0.0; dim]);
        }

        let mut assignments: Vec<usize> = vec![0; all_vectors.len()];

        for _ in 0..20 {
            let mut new_centroids = vec![vec![0.0; dim]; nlist];
            let mut counts = vec![0usize; nlist];

            for (i, vec) in all_vectors.iter().enumerate() {
                let mut min_dist = f32::MAX;
                let mut best = 0;
                for (j, centroid) in centroids.iter().enumerate() {
                    let dist: f32 = vec
                        .iter()
                        .zip(centroid.iter())
                        .map(|(a, b)| (a - b).powi(2))
                        .sum::<f32>()
                        .sqrt();
                    if dist < min_dist {
                        min_dist = dist;
                        best = j;
                    }
                }
                assignments[i] = best;
                counts[best] += 1;
                for (d, val) in vec.iter().enumerate() {
                    new_centroids[best][d] += val;
                }
            }

            for j in 0..nlist {
                if counts[j] > 0 {
                    for d in 0..dim {
                        new_centroids[j][d] /= counts[j] as f32;
                    }
                } else {
                    new_centroids[j] = centroids[j].clone();
                }
            }

            centroids = new_centroids;
        }

        let mut centroids_lock = self.centroids.write().await;
        *centroids_lock = centroids;

        let mut vector_to_cluster = self.vector_to_cluster.write().await;
        for (i, id) in ids.iter().enumerate() {
            vector_to_cluster.insert(id.clone(), assignments[i]);
        }

        self.dirty.store(false, Ordering::Release);

        Ok(())
    }

    async fn clear(&self) -> Result<()> {
        let mut vectors = self.vectors.write().await;
        vectors.clear();

        let mut vector_to_cluster = self.vector_to_cluster.write().await;
        vector_to_cluster.clear();

        let mut centroids = self.centroids.write().await;
        centroids.clear();

        self.dirty.store(true, Ordering::Release);

        Ok(())
    }

    async fn persist(&self, path: &std::path::Path, checksum: &str) -> Result<bool> {
        let vectors = self.vectors.read().await;
        let centroids = self.centroids.read().await;
        let vector_to_cluster = self.vector_to_cluster.read().await;

        let data = IVFIndexData {
            metric: self.metric.clone(),
            nlist: self.nlist,
            nprobe: self.nprobe,
            centroids: centroids.clone(),
            vector_to_cluster: vector_to_cluster.clone(),
            vectors: vectors.clone(),
        };
        let count = data.vectors.len();
        write_index_file(
            path,
            "ivf",
            &self.metric,
            count,
            checksum,
            serde_json::to_value(&data)?,
        )?;
        Ok(true)
    }

    fn clone_box(&self) -> Box<dyn VectorIndex> {
        Box::new(self.clone())
    }
}

#[async_trait]
impl VectorIndex for ScalarIndex {
    async fn add(&self, id: &str, vector: &[f32]) -> Result<()> {
        // For scalar index, we'll use the first element of the vector as the scalar value
        if vector.is_empty() {
            return Err("Vector must not be empty for scalar index".into());
        }
        
        let scalar = vector[0];

        // Derive the sorted snapshot from the map we already hold, then publish
        // it. Acquiring `scalars` a second time here would deadlock.
        let sorted = {
            let mut scalars = self.scalars.write().await;
            scalars.insert(id.to_string(), scalar);
            Self::sorted_snapshot(&scalars)
        };
        *self.sorted_scalars.write().await = sorted;

        Ok(())
    }
    
    async fn remove(&self, id: &str) -> Result<bool> {
        {
            let mut scalars = self.scalars.write().await;
            if scalars.remove(id).is_none() {
                return Ok(false);
            }
        }

        self.refresh_sorted().await;

        Ok(true)
    }
    
    async fn search(&self, query: &[f32], k: usize) -> Result<Vec<SearchResult>> {
        // For scalar index, we'll use the first element of the query vector as the target value
        if query.is_empty() {
            return Err("Query vector must not be empty for scalar index".into());
        }
        
        let target = query[0];
        let sorted_scalars = self.sorted_scalars.read().await;
        
        // Find the nearest neighbors using binary search
        let mut results: Vec<SearchResult> = sorted_scalars
            .iter()
            .map(|(value, id)| {
                let distance = (value - target).abs();
                SearchResult {
                    id: id.clone(),
                    distance,
                }
            })
            .collect();
        
        // Sort by distance (ascending)
        results.sort_by(|a, b| a.distance.partial_cmp(&b.distance).unwrap());
        
        // Take top k results
        Ok(results.into_iter().take(k).collect())
    }
    
    async fn build(&self) -> Result<()> {
        // Update sorted list
        self.refresh_sorted().await;
        Ok(())
    }
    
    async fn clear(&self) -> Result<()> {
        let mut scalars = self.scalars.write().await;
        scalars.clear();
        
        let mut sorted_scalars = self.sorted_scalars.write().await;
        sorted_scalars.clear();
        
        Ok(())
    }
    
    fn clone_box(&self) -> Box<dyn VectorIndex> {
        Box::new(self.clone())
    }
}

/// Index manager for handling multiple indexes
/// On-disk envelope for a persisted index.
///
/// `checksum` binds the index to the exact set of `(id, vector)` pairs it was
/// built from. It is recomputed from storage at load time; a mismatch means the
/// index is stale and must be rebuilt rather than loaded, otherwise search would
/// silently miss or mis-rank rows.
#[derive(Debug, Serialize, Deserialize)]
struct PersistedIndex {
    format: u32,
    index_type: String,
    metric: String,
    count: usize,
    checksum: String,
    data: serde_json::Value,
}

const INDEX_FILE_FORMAT: u32 = 1;

/// Atomically write a persisted index: temp file → fsync → rename → dir fsync.
/// A crash mid-write leaves either the old file or the new one, never a half.
fn write_index_file(
    path: &std::path::Path,
    index_type: &str,
    metric: &str,
    count: usize,
    checksum: &str,
    data: serde_json::Value,
) -> Result<()> {
    let envelope = PersistedIndex {
        format: INDEX_FILE_FORMAT,
        index_type: index_type.to_string(),
        metric: metric.to_string(),
        count,
        checksum: checksum.to_string(),
        data,
    };
    let json = serde_json::to_vec(&envelope)?;

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("tmp");
    {
        use std::io::Write;
        let mut file = std::fs::File::create(&tmp)?;
        file.write_all(&json)?;
        file.sync_all()?;
    }
    std::fs::rename(&tmp, path)?;
    if let Some(parent) = path.parent() {
        if let Ok(dir) = std::fs::File::open(parent) {
            let _ = dir.sync_all();
        }
    }
    Ok(())
}

/// Read and validate a persisted index. Returns `Ok(None)` when the file is
/// absent, malformed, of a different type/metric, or its checksum does not
/// match — every one of which means "rebuild from storage instead".
fn read_index_file(
    path: &std::path::Path,
    index_type: &str,
    metric: &str,
    checksum: &str,
) -> Result<Option<serde_json::Value>> {
    let content = match std::fs::read_to_string(path) {
        Ok(c) => c,
        Err(_) => return Ok(None),
    };
    let envelope: PersistedIndex = match serde_json::from_str(&content) {
        Ok(e) => e,
        Err(_) => return Ok(None),
    };
    if envelope.format != INDEX_FILE_FORMAT
        || envelope.index_type != index_type
        || envelope.metric != metric
        || envelope.checksum != checksum
    {
        return Ok(None);
    }
    Ok(Some(envelope.data))
}

/// Stable content hash of a collection's `(id, vector)` pairs. Order-insensitive
/// (ids are sorted) so it depends only on the data, not on iteration order.
pub fn vectors_checksum(pairs: &[(String, Vec<f32>)]) -> String {
    use sha2::{Digest, Sha256};

    let mut sorted: Vec<&(String, Vec<f32>)> = pairs.iter().collect();
    sorted.sort_by(|a, b| a.0.cmp(&b.0));

    let mut hasher = Sha256::new();
    for (id, vector) in sorted {
        hasher.update(id.as_bytes());
        hasher.update([0u8]);
        for value in vector {
            hasher.update(value.to_le_bytes());
        }
        hasher.update([0u8]);
    }
    format!("{:x}", hasher.finalize())
}

pub struct IndexManager {
    indexes: std::sync::Arc<tokio::sync::RwLock<std::collections::HashMap<String, Box<dyn VectorIndex>>>>,
}

impl IndexManager {
    /// Create a new index manager
    pub fn new() -> Self {
        Self {
            indexes: std::sync::Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
        }
    }
    
    /// Create a new index
    pub async fn create_index(&self, name: &str, index_type: &str, metric: &str) -> Result<()> {
        let mut indexes = self.indexes.write().await;
        
        let index: Box<dyn VectorIndex> = match index_type {
            "brute_force" => Box::new(BruteForceIndex::new(metric)),
            "hnsw" => Box::new(HNSWIndex::new(metric)),
            "ivf" => Box::new(IVFIndex::new(metric)),
            "scalar" => Box::new(ScalarIndex::new()),
            "pq" => Box::new(PQIndex::new(metric, 128, 16, 8)),
            _ => Box::new(BruteForceIndex::new(metric)),
        };
        
        indexes.insert(name.to_string(), index);
        Ok(())
    }
    
    /// Get an index by name
    pub async fn get_index(&self, name: &str) -> Result<Option<Box<dyn VectorIndex + 'static>>> {
        let indexes = self.indexes.read().await;
        match indexes.get(name) {
            Some(index) => Ok(Some(index.clone_box())),
            None => Ok(None),
        }
    }
    
    /// Delete an index
    pub async fn delete_index(&self, name: &str) -> Result<bool> {
        let mut indexes = self.indexes.write().await;
        Ok(indexes.remove(name).is_some())
    }

    /// Persist the index registered under `name` to `path`, tagged with the
    /// collection `checksum`. Returns `false` for index types that do not
    /// support on-disk persistence (`brute_force`, `scalar`) or when unknown.
    pub async fn persist_index(
        &self,
        name: &str,
        path: &std::path::Path,
        checksum: &str,
    ) -> Result<bool> {
        let index = {
            let indexes = self.indexes.read().await;
            indexes.get(name).map(|i| i.clone_box())
        };
        match index {
            Some(index) => index.persist(path, checksum).await,
            None => Ok(false),
        }
    }

    /// Install a persisted index for `name`, replacing the empty placeholder
    /// created by `create_index`. Returns `true` only when the file exists, its
    /// type/metric/checksum match, and it deserialises — otherwise the caller
    /// must rebuild from storage.
    pub async fn load_index(
        &self,
        name: &str,
        index_type: &str,
        metric: &str,
        path: &std::path::Path,
        checksum: &str,
    ) -> Result<bool> {
        let Some(data) = read_index_file(path, index_type, metric, checksum)? else {
            return Ok(false);
        };

        let index: Box<dyn VectorIndex> = match index_type {
            "hnsw" => match serde_json::from_value::<HNSWIndexData>(data) {
                Ok(d) => Box::new(HNSWIndex {
                    vectors: std::sync::Arc::new(tokio::sync::RwLock::new(d.vectors)),
                    metric: d.metric,
                    m: d.m,
                    ef_construction: d.ef_construction,
                    ef_search: d.ef_search,
                    max_level: d.max_level,
                    entry_point: std::sync::Arc::new(tokio::sync::RwLock::new(d.entry_point)),
                    graph: std::sync::Arc::new(tokio::sync::RwLock::new(d.graph)),
                }),
                Err(_) => return Ok(false),
            },
            "ivf" => match serde_json::from_value::<IVFIndexData>(data) {
                Ok(d) => {
                    // A saved index that already carries centroids needs no rebuild.
                    let dirty = d.centroids.is_empty();
                    Box::new(IVFIndex {
                        vectors: std::sync::Arc::new(tokio::sync::RwLock::new(d.vectors)),
                        metric: d.metric,
                        nlist: d.nlist,
                        nprobe: d.nprobe,
                        centroids: std::sync::Arc::new(tokio::sync::RwLock::new(d.centroids)),
                        vector_to_cluster: std::sync::Arc::new(
                            tokio::sync::RwLock::new(d.vector_to_cluster),
                        ),
                        dirty: std::sync::Arc::new(AtomicBool::new(dirty)),
                    })
                }
                Err(_) => return Ok(false),
            },
            "pq" => match serde_json::from_value::<PQIndexData>(data) {
                Ok(d) => Box::new(PQIndex {
                    vectors: std::sync::Arc::new(tokio::sync::RwLock::new(d.vectors)),
                    original_vectors: std::sync::Arc::new(
                        tokio::sync::RwLock::new(d.original_vectors),
                    ),
                    metric: d.metric,
                    dimension: d.dimension,
                    n_subquantizers: d.n_subquantizers,
                    n_bits: d.n_bits,
                    codebooks: std::sync::Arc::new(tokio::sync::RwLock::new(d.codebooks)),
                }),
                Err(_) => return Ok(false),
            },
            _ => return Ok(false),
        };

        self.indexes.write().await.insert(name.to_string(), index);
        Ok(true)
    }
}

pub struct PQIndex {
    vectors: std::sync::Arc<tokio::sync::RwLock<std::collections::HashMap<String, Vec<u8>>>>,
    original_vectors: std::sync::Arc<tokio::sync::RwLock<std::collections::HashMap<String, Vec<f32>>>>,
    metric: String,
    dimension: usize,
    n_subquantizers: usize,
    n_bits: usize,
    codebooks: std::sync::Arc<tokio::sync::RwLock<Vec<Vec<Vec<f32>>>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct HNSWIndexData {
    metric: String,
    m: usize,
    ef_construction: usize,
    ef_search: usize,
    max_level: usize,
    entry_point: Option<String>,
    vectors: std::collections::HashMap<String, Vec<f32>>,
    graph: std::collections::HashMap<String, Vec<Vec<String>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IVFIndexData {
    metric: String,
    nlist: usize,
    nprobe: usize,
    centroids: Vec<Vec<f32>>,
    vector_to_cluster: std::collections::HashMap<String, usize>,
    vectors: std::collections::HashMap<String, Vec<f32>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PQIndexData {
    metric: String,
    dimension: usize,
    n_subquantizers: usize,
    n_bits: usize,
    codebooks: Vec<Vec<Vec<f32>>>,
    vectors: std::collections::HashMap<String, Vec<u8>>,
    original_vectors: std::collections::HashMap<String, Vec<f32>>,
}

impl PQIndex {
    pub fn new(metric: &str, dimension: usize, n_subquantizers: usize, n_bits: usize) -> Self {
        Self {
            vectors: std::sync::Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
            original_vectors: std::sync::Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
            metric: metric.to_string(),
            dimension,
            n_subquantizers,
            n_bits,
            codebooks: std::sync::Arc::new(tokio::sync::RwLock::new(Vec::new())),
        }
    }

    pub async fn train(&self, training_vectors: &[Vec<f32>]) -> std::result::Result<(), String> {
        if training_vectors.is_empty() {
            return Err("No training vectors provided".to_string());
        }

        let sub_dim = self.dimension / self.n_subquantizers;
        if sub_dim == 0 {
            return Err("Too many subquantizers for the vector dimension".to_string());
        }

        let mut codebooks = Vec::new();

        for i in 0..self.n_subquantizers {
            let start = i * sub_dim;
            let end = if i == self.n_subquantizers - 1 {
                self.dimension
            } else {
                start + sub_dim
            };

            let mut sub_vectors: Vec<Vec<f32>> = training_vectors
                .iter()
                .map(|v| v[start..end].to_vec())
                .collect();

            let n_centroids = 1 << self.n_bits;
            let codebook = Self::kmeans(&mut sub_vectors, n_centroids);
            codebooks.push(codebook);
        }

        let mut cb = self.codebooks.write().await;
        *cb = codebooks;

        Ok(())
    }

    fn kmeans(data: &mut Vec<Vec<f32>>, k: usize) -> Vec<Vec<f32>> {
        if data.is_empty() || k == 0 {
            return Vec::new();
        }

        let dim = data[0].len();
        let k = k.min(data.len());

        let mut centroids: Vec<Vec<f32>> = data
            .iter()
            .step_by(data.len() / k.max(1))
            .take(k)
            .cloned()
            .collect();

        while centroids.len() < k {
            centroids.push(vec![0.0; dim]);
        }

        for _ in 0..20 {
            let mut clusters: Vec<Vec<Vec<f32>>> = vec![Vec::new(); k];

            for vec in data.iter() {
                let mut min_dist = f32::MAX;
                let mut best_centroid = 0;

                for (i, centroid) in centroids.iter().enumerate() {
                    let dist: f32 = vec.iter()
                        .zip(centroid.iter())
                        .map(|(a, b)| (a - b).powi(2))
                        .sum::<f32>()
                        .sqrt();

                    if dist < min_dist {
                        min_dist = dist;
                        best_centroid = i;
                    }
                }

                clusters[best_centroid].push(vec.clone());
            }

            for (i, cluster) in clusters.iter().enumerate() {
                if !cluster.is_empty() {
                    let dim = cluster[0].len();
                    let mut new_centroid = vec![0.0; dim];
                    for vec in cluster {
                        for (j, val) in vec.iter().enumerate() {
                            new_centroid[j] += val;
                        }
                    }
                    for val in new_centroid.iter_mut() {
                        *val /= cluster.len() as f32;
                    }
                    centroids[i] = new_centroid;
                }
            }
        }

        centroids
    }

    pub async fn add(&self, id: String, vector: Vec<f32>) -> std::result::Result<(), String> {
        if vector.len() != self.dimension {
            return Err(format!("Vector dimension {} does not match index dimension {}", vector.len(), self.dimension));
        }

        let codebook = self.codebooks.read().await;
        if codebook.is_empty() {
            return Err("Index not trained. Call train() first.".to_string());
        }

        let code = self.encode_vector(&vector, &codebook);

        let mut vectors = self.vectors.write().await;
        vectors.insert(id.clone(), code);

        let mut original = self.original_vectors.write().await;
        original.insert(id, vector);

        Ok(())
    }

    fn encode_vector(&self, vector: &[f32], codebook: &[Vec<Vec<f32>>]) -> Vec<u8> {
        let sub_dim = self.dimension / self.n_subquantizers;
        let mut code = Vec::with_capacity(self.n_subquantizers);

        for (i, sub_codebook) in codebook.iter().enumerate() {
            let start = i * sub_dim;
            let end = if i == self.n_subquantizers - 1 {
                self.dimension
            } else {
                start + sub_dim
            };

            let sub_vector = &vector[start..end];

            let mut min_dist = f32::MAX;
            let mut best_idx = 0u8;

            for (j, centroid) in sub_codebook.iter().enumerate() {
                let dist: f32 = sub_vector
                    .iter()
                    .zip(centroid.iter())
                    .map(|(a, b)| (a - b).powi(2))
                    .sum::<f32>()
                    .sqrt();

                if dist < min_dist {
                    min_dist = dist;
                    best_idx = j as u8;
                }
            }

            code.push(best_idx);
        }

        code
    }

    pub async fn search(&self, query: &[f32], k: usize) -> Result<Vec<SearchResult>> {
        let codebook = self.codebooks.read().await;
        if codebook.is_empty() {
            return Err(CoreTexError::IndexError("Index not trained. Call train() first.".to_string()));
        }

        let _query_code = self.encode_vector(query, &codebook);
        let original = self.original_vectors.read().await;

        let mut results: Vec<SearchResult> = original
            .iter()
            .map(|(id, orig)| {
                let dist = self.calculate_distance(query, orig);
                SearchResult {
                    id: id.clone(),
                    distance: dist,
                }
            })
            .collect();

        results.sort_by(|a, b| a.distance.partial_cmp(&b.distance).unwrap());
        results.truncate(k);

        Ok(results)
    }

    /// Distance between two vectors under this index's metric.
    fn calculate_distance(&self, a: &[f32], b: &[f32]) -> f32 {
        metric_distance(&self.metric, a, b)
    }

    pub fn compression_ratio(&self) -> f32 {
        let original_size = self.dimension * 4;
        let compressed_size = self.n_subquantizers;
        original_size as f32 / compressed_size as f32
    }
}

#[async_trait]
impl VectorIndex for PQIndex {
    async fn add(&self, id: &str, vector: &[f32]) -> Result<()> {
        let codebook = self.codebooks.read().await;
        if codebook.is_empty() {
            return Err(CoreTexError::IndexError("Index not trained. Call train() first.".to_string()));
        }

        let code = self.encode_vector(vector, &codebook);

        let mut vectors = self.vectors.write().await;
        vectors.insert(id.to_string(), code);

        let mut original = self.original_vectors.write().await;
        original.insert(id.to_string(), vector.to_vec());

        Ok(())
    }

    async fn remove(&self, id: &str) -> Result<bool> {
        let mut vectors = self.vectors.write().await;
        let removed_vectors = vectors.remove(id).is_some();

        let mut original = self.original_vectors.write().await;
        let removed_original = original.remove(id).is_some();

        Ok(removed_vectors || removed_original)
    }

    async fn search(&self, query: &[f32], k: usize) -> Result<Vec<SearchResult>> {
        let codebook = self.codebooks.read().await;
        if codebook.is_empty() {
            return Err(CoreTexError::IndexError("Index not trained. Call train() first.".to_string()));
        }

        let original = self.original_vectors.read().await;

        let mut results: Vec<SearchResult> = original
            .iter()
            .map(|(id, orig)| {
                let dist = self.calculate_distance(query, orig);
                SearchResult {
                    id: id.clone(),
                    distance: dist,
                }
            })
            .collect();

        results.sort_by(|a, b| a.distance.partial_cmp(&b.distance).unwrap());
        results.truncate(k);

        Ok(results)
    }

    async fn build(&self) -> Result<()> {
        Ok(())
    }

    async fn clear(&self) -> Result<()> {
        let mut vectors = self.vectors.write().await;
        vectors.clear();

        let mut original = self.original_vectors.write().await;
        original.clear();

        Ok(())
    }

    async fn persist(&self, path: &std::path::Path, checksum: &str) -> Result<bool> {
        let vectors = self.vectors.read().await;
        let original_vectors = self.original_vectors.read().await;
        let codebooks = self.codebooks.read().await;

        let data = PQIndexData {
            metric: self.metric.clone(),
            dimension: self.dimension,
            n_subquantizers: self.n_subquantizers,
            n_bits: self.n_bits,
            codebooks: codebooks.clone(),
            vectors: vectors.clone(),
            original_vectors: original_vectors.clone(),
        };
        let count = data.vectors.len();
        write_index_file(
            path,
            "pq",
            &self.metric,
            count,
            checksum,
            serde_json::to_value(&data)?,
        )?;
        Ok(true)
    }

    fn clone_box(&self) -> Box<dyn VectorIndex> {
        Box::new(Self {
            vectors: std::sync::Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
            original_vectors: std::sync::Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
            metric: self.metric.clone(),
            dimension: self.dimension,
            n_subquantizers: self.n_subquantizers,
            n_bits: self.n_bits,
            codebooks: std::sync::Arc::new(tokio::sync::RwLock::new(Vec::new())),
        })
    }
}

#[cfg(test)]
mod tests {
    include!("tests.rs");
}