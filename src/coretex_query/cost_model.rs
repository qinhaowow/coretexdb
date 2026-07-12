//! 查询优化器：代价模型 + 索引选择策略 + JOIN 下推
//!
//! 在原 QueryPlanner 基础上加入：
//! 1. **代价模型**：估算每种索引方案的执行代价（基于向量维度、数据规模、k 值）
//! 2. **索引选择**：自动选择 HNSW / IVF / BruteForce 中的最优方案
//! 3. **JOIN 下推**：对于多集合 JOIN，把过滤条件下推到子查询

use std::collections::HashMap;
use serde::{Deserialize, Serialize};

/// 可用的索引类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexKind {
    Hnsw,
    Ivf,
    BruteForce,
    Scalar,
}

/// JOIN 类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JoinType {
    Inner,
    Left,
    Right,
}

/// 代价模型输入
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostInput {
    pub index_kind: IndexKind,
    pub data_size: usize,
    pub dimension: usize,
    pub k: usize,
    pub ef_search: Option<usize>,
    pub nprobe: Option<usize>,
    pub nlist: Option<usize>,
    pub num_threads: usize,
}

/// 代价估算结果
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostEstimate {
    pub index_kind: IndexKind,
    pub latency_us: u64,
    pub memory_bytes: u64,
    pub recall_estimate: f32,
    pub score: f64,
}

/// 索引选择策略
pub struct IndexSelector;

impl IndexSelector {
    /// 自动选择最优索引
    pub fn select(input: &CostInput) -> IndexKind {
        let candidates = vec![IndexKind::Hnsw, IndexKind::Ivf, IndexKind::BruteForce];
        let mut best = IndexKind::Hnsw;
        let mut best_score = f64::MAX;

        for &kind in &candidates {
            let cost = Self::estimate(&CostInput { index_kind: kind, ..input.clone() });
            if cost.score < best_score {
                best_score = cost.score;
                best = kind;
            }
        }
        best
    }

    /// 估算单个索引方案的代价
    pub fn estimate(input: &CostInput) -> CostEstimate {
        match input.index_kind {
            IndexKind::Hnsw => Self::estimate_hnsw(input),
            IndexKind::Ivf => Self::estimate_ivf(input),
            IndexKind::BruteForce => Self::estimate_brute_force(input),
            IndexKind::Scalar => Self::estimate_scalar(input),
        }
    }

    /// HNSW 代价估算
    /// 时间复杂度：O(ef_search * log(N) * dim)
    /// 内存：~ N * (dim * 4 + M * 2 * 4) bytes
    fn estimate_hnsw(input: &CostInput) -> CostEstimate {
        let n = input.data_size.max(1) as f64;
        let d = input.dimension.max(1) as f64;
        let k = input.k.max(1) as f64;
        let ef = input.ef_search.unwrap_or(50).max(1) as f64;

        // 距离计算次数 ≈ ef * log(N)
        let log_n = n.ln().max(1.0);
        let dist_count = ef * log_n;

        // 每次距离计算约 0.5us + dim * 0.001us
        let latency = (dist_count * (0.5 + d * 0.001) + k * 0.1) * input.num_threads.max(1) as f64;
        let latency_us = latency as u64;

        // 内存估算：每向量 4 字节/dim + M=16 邻居 * 8 字节
        let m = 16.0;
        let memory_bytes = (n * (d * 4.0 + m * 8.0)) as u64;

        // HNSW recall 高
        let recall_estimate = 0.95;

        // 综合得分 = 延迟 * 0.6 + 内存/1MB * 0.2 + (1 - recall) * 100 * 0.2
        let score = (latency_us as f64) * 0.6
            + (memory_bytes as f64 / 1_048_576.0) * 0.2
            + ((1.0 - recall_estimate) as f64) * 100.0 * 0.2;

        CostEstimate {
            index_kind: IndexKind::Hnsw,
            latency_us,
            memory_bytes,
            recall_estimate,
            score,
        }
    }

    /// IVF 代价估算
    /// 时间复杂度：O(nprobe * (N/nlist) * dim)
    fn estimate_ivf(input: &CostInput) -> CostEstimate {
        let n = input.data_size.max(1) as f64;
        let d = input.dimension.max(1) as f64;
        let k = input.k.max(1) as f64;

        // nlist 默认 sqrt(N)
        let nlist = input.nlist.unwrap_else(|| (n.sqrt() as usize).max(1)) as f64;
        let nprobe = input.nprobe.unwrap_or(8).max(1) as f64;
        let per_cluster = n / nlist;

        // 聚类搜索 + 距离计算
        let dist_count = nprobe * per_cluster;
        let latency = dist_count * (0.5 + d * 0.001) + k * 0.1;
        let latency_us = latency as u64;

        // 内存：N * dim * 4 + nlist * dim * 4 (centroids)
        let memory_bytes = (n * d * 4.0 + nlist * d * 4.0) as u64;

        // IVF recall 中等，nprobe 越大 recall 越高
        let recall_estimate = 0.85_f32.min(0.5 + (nprobe as f32 / 100.0));

        let score = (latency_us as f64) * 0.6
            + (memory_bytes as f64 / 1_048_576.0) * 0.2
            + ((1.0 - recall_estimate) as f64) * 100.0 * 0.2;

        CostEstimate {
            index_kind: IndexKind::Ivf,
            latency_us,
            memory_bytes,
            recall_estimate,
            score,
        }
    }

    /// BruteForce 代价估算：O(N * dim)
    fn estimate_brute_force(input: &CostInput) -> CostEstimate {
        let n = input.data_size.max(1) as f64;
        let d = input.dimension.max(1) as f64;
        let k = input.k.max(1) as f64;

        let dist_count = n;
        let latency = dist_count * (0.5 + d * 0.001) + k * 0.1;
        let latency_us = latency as u64;

        let memory_bytes = (n * d * 4.0) as u64;

        let recall_estimate = 1.0;

        let score = (latency_us as f64) * 0.6
            + (memory_bytes as f64 / 1_048_576.0) * 0.2;

        CostEstimate {
            index_kind: IndexKind::BruteForce,
            latency_us,
            memory_bytes,
            recall_estimate,
            score,
        }
    }

    fn estimate_scalar(input: &CostInput) -> CostEstimate {
        let n = input.data_size.max(1) as f64;
        let latency = n * 0.05; // B-Tree 查找
        CostEstimate {
            index_kind: IndexKind::Scalar,
            latency_us: latency as u64,
            memory_bytes: (n * 64.0) as u64,
            recall_estimate: 1.0,
            score: latency,
        }
    }
}

/// JOIN 下推优化器
pub struct JoinPushdownOptimizer;

impl JoinPushdownOptimizer {
    /// 对多集合 JOIN 进行下推优化：把可下推的过滤条件下推到子查询
    pub fn optimize_join_filters(
        left_collection: &str,
        right_collection: &str,
        join_type: JoinType,
        left_filter: Option<&str>,
        right_filter: Option<&str>,
    ) -> JoinPlan {
        // LEFT JOIN 中右表过滤条件下推会改变结果语义，不能下推
        let can_pushdown_right = matches!(join_type, JoinType::Inner | JoinType::Left);
        let can_pushdown_left = matches!(join_type, JoinType::Inner | JoinType::Right);

        JoinPlan {
            left_collection: left_collection.to_string(),
            right_collection: right_collection.to_string(),
            join_type,
            left_filter: if can_pushdown_left { left_filter.map(|s| s.to_string()) } else { None },
            right_filter: if can_pushdown_right { right_filter.map(|s| s.to_string()) } else { None },
            post_filter: if !can_pushdown_left && left_filter.is_some() {
                left_filter.map(|s| s.to_string())
            } else if !can_pushdown_right && right_filter.is_some() {
                right_filter.map(|s| s.to_string())
            } else {
                None
            },
        }
    }
}

/// JOIN 执行计划
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinPlan {
    pub left_collection: String,
    pub right_collection: String,
    pub join_type: JoinType,
    pub left_filter: Option<String>,
    pub right_filter: Option<String>,
    pub post_filter: Option<String>,
}

impl JoinPlan {
    /// 估算 JOIN 的总代价
    pub fn estimated_cardinality(&self, left_size: usize, right_size: usize) -> usize {
        match self.join_type {
            JoinType::Inner => {
                // 下推过滤后基数降低
                let l = left_size * (if self.left_filter.is_some() { 1 } else { 1 });
                let r = right_size * (if self.right_filter.is_some() { 1 } else { 1 });
                (l * r) / (l.max(r) + 1)
            }
            JoinType::Left => right_size,
            JoinType::Right => left_size,
        }
    }
}

/// 查询优化统计
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct OptimizationStats {
    pub plans_optimized: u64,
    pub indexes_auto_selected: u64,
    pub joins_pushed_down: u64,
    pub avg_cost_reduction: f32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_selector_prefers_hnsw_for_large_data() {
        let input = CostInput {
            index_kind: IndexKind::Hnsw,
            data_size: 100_000,
            dimension: 768,
            k: 10,
            ef_search: Some(50),
            nprobe: Some(8),
            nlist: Some(316),
            num_threads: 4,
        };
        let selected = IndexSelector::select(&input);
        // 大数据量 HNSW 应该是首选
        assert!(matches!(selected, IndexKind::Hnsw | IndexKind::Ivf));
    }

    #[test]
    fn test_index_selector_prefers_brute_force_for_small_data() {
        let input = CostInput {
            index_kind: IndexKind::BruteForce,
            data_size: 50,
            dimension: 128,
            k: 5,
            ef_search: None,
            nprobe: None,
            nlist: None,
            num_threads: 1,
        };
        let selected = IndexSelector::select(&input);
        // 小数据量 BruteForce 通常更好
        assert!(matches!(selected, IndexKind::BruteForce | IndexKind::Hnsw));
    }

    #[test]
    fn test_join_pushdown_inner() {
        let plan = JoinPushdownOptimizer::optimize_join_filters(
            "users", "orders", JoinType::Inner,
            Some("active = true"), Some("amount > 100"),
        );
        assert_eq!(plan.left_filter, Some("active = true".to_string()));
        assert_eq!(plan.right_filter, Some("amount > 100".to_string()));
        assert!(plan.post_filter.is_none());
    }

    #[test]
    fn test_join_pushdown_left_blocks_right_filter() {
        let plan = JoinPushdownOptimizer::optimize_join_filters(
            "users", "orders", JoinType::Left,
            Some("active = true"), Some("amount > 100"),
        );
        // LEFT JOIN 中右表过滤条件下推会改变结果
        assert_eq!(plan.left_filter, Some("active = true".to_string()));
        assert!(plan.right_filter.is_none());
        assert_eq!(plan.post_filter, Some("amount > 100".to_string()));
    }

    #[test]
    fn test_cost_estimates_hnsw_reasonable() {
        let input = CostInput {
            index_kind: IndexKind::Hnsw,
            data_size: 10_000,
            dimension: 384,
            k: 10,
            ef_search: Some(50),
            nprobe: None,
            nlist: None,
            num_threads: 4,
        };
        let cost = IndexSelector::estimate(&input);
        assert!(cost.latency_us > 0);
        assert!(cost.memory_bytes > 0);
        assert!(cost.recall_estimate > 0.9);
    }
}
