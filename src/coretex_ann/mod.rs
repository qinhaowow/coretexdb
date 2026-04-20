//! ANN (Approximate Nearest Neighbor) Optimization module for CoreTexDB
//! Provides advanced indexing parameters and tuning for high-performance vector search

use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Clone)]
pub struct ANNConfig {
    pub algorithm: ANNAlgorithm,
    pub metric: String,
    pub parameters: ANNParameters,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ANNAlgorithm {
    HNSW,
    IVF,
    IVF_PQ,
    NSG,
    SWGraph,
}

impl Default for ANNConfig {
    fn default() -> Self {
        Self {
            algorithm: ANNAlgorithm::HNSW,
            metric: "cosine".to_string(),
            parameters: ANNParameters::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ANNParameters {
    pub hnsw: HNSWParameters,
    pub ivf: IVFParameters,
    pub pq: PQParameters,
    pub nsg: NSGParameters,
    pub search_params: SearchParameters,
}

impl Default for ANNParameters {
    fn default() -> Self {
        Self {
            hnsw: HNSWParameters::default(),
            ivf: IVFParameters::default(),
            pq: PQParameters::default(),
            nsg: NSGParameters::default(),
            search_params: SearchParameters::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct HNSWParameters {
    pub m: usize,
    pub ef_construction: usize,
    pub ef_search: usize,
    pub max_level: usize,
    pub level_mult: f64,
    pub build_seed: u64,
}

impl Default for HNSWParameters {
    fn default() -> Self {
        Self {
            m: 16,
            ef_construction: 200,
            ef_search: 50,
            max_level: 16,
            level_mult: 1.0 / (std::f64::consts::LN_2 * std::f64::consts::LN_2),
            build_seed: 42,
        }
    }
}

impl HNSWParameters {
    pub fn with_m(mut self, m: usize) -> Self {
        self.m = m.clamp(1, 128);
        self
    }

    pub fn with_ef_construction(mut self, ef: usize) -> Self {
        self.ef_construction = ef.clamp(1, 1000);
        self
    }

    pub fn with_ef_search(mut self, ef: usize) -> Self {
        self.ef_search = ef.clamp(1, 500);
        self
    }

    pub fn tune_for_recall(&mut self, target_recall: f64) {
        self.ef_search = ((target_recall * 200.0) as usize).clamp(1, 500);
        self.ef_construction = ((target_recall * 400.0) as usize).clamp(10, 1000);
    }

    pub fn tune_for_speed(&mut self) {
        self.ef_search = (self.ef_search / 2).max(1);
        self.m = (self.m / 2).max(1);
    }

    pub fn memory_estimate(&self, num_vectors: usize, dimension: usize) -> usize {
        let edges_per_vector = self.m * 2;
        let edge_memory = num_vectors * edges_per_vector * std::mem::size_of::<u32>();
        let vector_memory = num_vectors * dimension * std::mem::size_of::<f32>();
        let metadata_memory = num_vectors * std::mem::size_of::<u32>() * 4;
        edge_memory + vector_memory + metadata_memory
    }
}

#[derive(Debug, Clone)]
pub struct IVFParameters {
    pub nlist: usize,
    pub nprobe: usize,
    pub kmeans_iterations: usize,
    pub min_cluster_size: usize,
}

impl Default for IVFParameters {
    fn default() -> Self {
        Self {
            nlist: 100,
            nprobe: 10,
            kmeans_iterations: 20,
            min_cluster_size: 1,
        }
    }
}

impl IVFParameters {
    pub fn with_nlist(mut self, nlist: usize) -> Self {
        self.nlist = nlist.clamp(1, 65536);
        self
    }

    pub fn with_nprobe(mut self, nprobe: usize) -> Self {
        self.nprobe = nprobe.clamp(1, self.nlist);
        self
    }

    pub fn tune_recall_vs_speed(&mut self, recall_target: f64) {
        let ratio = (recall_target * 10.0) as usize;
        self.nprobe = (self.nlist * ratio / 10).clamp(1, self.nlist);
    }
}

#[derive(Debug, Clone)]
pub struct PQParameters {
    pub m: usize,
    pub nbits: usize,
    pub kmeans_centers: usize,
}

impl Default for PQParameters {
    fn default() -> Self {
        Self {
            m: 8,
            nbits: 8,
            kmeans_centers: 256,
        }
    }
}

impl PQParameters {
    pub fn compression_ratio(&self) -> f64 {
        let original_bits = 32.0 * self.m as f32;
        let compressed_bits = self.nbits as f32 * self.m as f32;
        original_bits as f64 / compressed_bits as f64
    }
}

#[derive(Debug, Clone)]
pub struct NSGParameters {
    pub rn_construction: usize,
    pub search_length: usize,
    pub out_degree: usize,
    pub in_degree: usize,
}

impl Default for NSGParameters {
    fn default() -> Self {
        Self {
            rn_construction: 50,
            search_length: 40,
            out_degree: 30,
            in_degree: 40,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SearchParameters {
    pub ef_search: usize,
    pub max_scan: usize,
    pub use_filters: bool,
}

impl Default for SearchParameters {
    fn default() -> Self {
        Self {
            ef_search: 50,
            max_scan: 0,
            use_filters: true,
        }
    }
}

impl SearchParameters {
    pub fn with_ef(mut self, ef: usize) -> Self {
        self.ef_search = ef.clamp(1, 1000);
        self
    }

    pub fn with_max_scan(mut self, max: usize) -> Self {
        self.max_scan = max;
        self
    }
}

pub struct ANNTuner {
    config: ANNConfig,
    performance_history: Arc<RwLock<Vec<PerformanceRecord>>>,
}

#[derive(Debug, Clone)]
pub struct PerformanceRecord {
    pub recall: f64,
    pub latency_ms: f64,
    pub throughput_qps: f64,
    pub memory_mb: usize,
}

impl ANNTuner {
    pub fn new(config: ANNConfig) -> Self {
        Self {
            config,
            performance_history: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub async fn record_performance(&self, record: PerformanceRecord) {
        let mut history = self.performance_history.write().await;
        history.push(record);
        
        if history.len() > 100 {
            history.remove(0);
        }
    }

    pub async fn get_best_params(&self) -> Option<ANNParameters> {
        let history = self.performance_history.read().await;
        
        history
            .iter()
            .max_by(|a, b| {
                let score_a = a.recall * 0.7 + (1.0 / (a.latency_ms + 1.0)) * 0.3;
                let score_b = b.recall * 0.7 + (1.0 / (b.latency_ms + 1.0)) * 0.3;
                score_a.partial_cmp(&score_b).unwrap()
            })
            .map(|_| self.config.parameters.clone())
    }

    pub async fn auto_tune(&mut self, target_recall: f64) {
        let mut params = &mut self.config.parameters;
        
        params.hnsw.tune_for_recall(target_recall);
        params.ivf.tune_recall_vs_speed(target_recall);
    }
}

pub struct IndexOptimizer {
    index_type: ANNAlgorithm,
}

impl IndexOptimizer {
    pub fn new(index_type: ANNAlgorithm) -> Self {
        Self { index_type }
    }

    pub fn optimize_for_recall(&self, params: &mut ANNParameters, target_recall: f64) {
        match self.index_type {
            ANNAlgorithm::HNSW => {
                params.hnsw.tune_for_recall(target_recall);
            }
            ANNAlgorithm::IVF | ANNAlgorithm::IVF_PQ => {
                params.ivf.tune_recall_vs_speed(target_recall);
            }
            _ => {}
        }
    }

    pub fn optimize_for_memory(&self, params: &mut ANNParameters, max_memory_mb: usize) {
        params.hnsw.m = (max_memory_mb / 100).max(1).min(64);
    }

    pub fn estimate_build_time(&self, num_vectors: usize, dimension: usize) -> f64 {
        match self.index_type {
            ANNAlgorithm::HNSW => {
                let n = num_vectors as f64;
                let d = dimension as f64;
                (n * d / 1_000_000.0) + (n.log2() * n / 10_000_000.0)
            }
            ANNAlgorithm::IVF => {
                let n = num_vectors as f64;
                let nlist = 100.0;
                n * nlist.log2() / 5_000_000.0
            }
            _ => num_vectors as f64 / 100_000.0,
        }
    }

    pub fn recommend_parameters(&self, num_vectors: usize, dimension: usize) -> ANNParameters {
        let mut params = ANNParameters::default();
        
        if num_vectors < 10_000 {
            params.hnsw.m = 8;
            params.hnsw.ef_construction = 100;
        } else if num_vectors < 1_000_000 {
            params.hnsw.m = 16;
            params.hnsw.ef_construction = 200;
        } else {
            params.hnsw.m = 32;
            params.hnsw.ef_construction = 400;
        }
        
        let suggested_nlist = (num_vectors as f64 / 39.7).sqrt() as usize;
        params.ivf.nlist = suggested_nlist.clamp(1, 65536);
        
        params
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hnsw_parameters() {
        let params = HNSWParameters::default();
        assert_eq!(params.m, 16);
        assert_eq!(params.ef_construction, 200);
    }

    #[test]
    fn test_hnsw_memory_estimate() {
        let params = HNSWParameters::default();
        let memory = params.memory_estimate(1000, 128);
        assert!(memory > 0);
    }

    #[test]
    fn test_pq_compression_ratio() {
        let params = PQParameters::default();
        let ratio = params.compression_ratio();
        assert!(ratio > 1.0);
    }

    #[tokio::test]
    async fn test_ann_tuner() {
        let config = ANNConfig::default();
        let tuner = ANNTuner::new(config);
        
        tuner.record_performance(PerformanceRecord {
            recall: 0.95,
            latency_ms: 10.0,
            throughput_qps: 1000.0,
            memory_mb: 100,
        }).await;
        
        tuner.record_performance(PerformanceRecord {
            recall: 0.99,
            latency_ms: 20.0,
            throughput_qps: 800.0,
            memory_mb: 150,
        }).await;
        
        let best = tuner.get_best_params().await;
        assert!(best.is_some());
    }

    #[test]
    fn test_index_optimizer_recommend() {
        let optimizer = IndexOptimizer::new(ANNAlgorithm::HNSW);
        let params = optimizer.recommend_parameters(10000, 128);
        
        assert!(params.hnsw.m > 0);
        assert!(params.ivf.nlist > 0);
    }
}
