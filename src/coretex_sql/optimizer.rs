//! SQL 优化器：HNSW/IVF 算子下推与向量化算子
//!
//! 在执行 SQL 查询时，把向量距离计算下推到索引层：
//! - `SELECT ... WHERE vector <-> '[1,2,3]' < 0.5` → 直接走 HNSW 索引，返回候选后再过滤
//! - `ORDER BY vector <-> '[1,2,3]' LIMIT 10` → 用 HNSW k-NN 代替全表扫描 + 排序

use std::collections::HashMap;
use serde::{Deserialize, Serialize};

/// 算子类型
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SQLOperatorKind {
    TableScan,
    IndexScan,
    VectorKNN,
    Filter,
    Projection,
    Limit,
    Sort,
    Join,
}

/// 向量距离函数
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DistanceOp {
    L2,
    Cosine,
    InnerProduct,
}

impl DistanceOp {
    pub fn symbol(&self) -> &'static str {
        match self {
            DistanceOp::L2 => "<->",
            DistanceOp::Cosine => "<=>",
            DistanceOp::InnerProduct => "<#>",
        }
    }
}

/// 向量下推算子：把向量距离计算下推到 HNSW/IVF 索引
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorPushdownOperator {
    pub index_name: String,
    pub index_kind: IndexKind,
    pub query_vector: Vec<f32>,
    pub distance_op: DistanceOp,
    pub k: usize,
    pub ef_search: Option<usize>,
    pub nprobe: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexKind {
    Hnsw,
    Ivf,
    BruteForce,
}

/// 后置过滤算子：在 HNSW 候选集合上做精确过滤
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterOperator {
    pub column: String,
    pub op: FilterOp,
    pub value: FilterValue,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FilterOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    In,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FilterValue {
    Number(f64),
    String(String),
    Bool(bool),
    NumberList(Vec<f64>),
    StringList(Vec<String>),
}

/// 投影算子
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectionOperator {
    pub columns: Vec<String>,
}

/// Limit 算子
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LimitOperator {
    pub limit: usize,
    pub offset: usize,
}

/// SQL 算子（执行计划节点）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SQLOperator {
    VectorPushdown(VectorPushdownOperator),
    Filter(FilterOperator),
    Projection(ProjectionOperator),
    Limit(LimitOperator),
    Noop,
}

impl SQLOperator {
    pub fn kind(&self) -> SQLOperatorKind {
        match self {
            SQLOperator::VectorPushdown(_) => SQLOperatorKind::VectorKNN,
            SQLOperator::Filter(_) => SQLOperatorKind::Filter,
            SQLOperator::Projection(_) => SQLOperatorKind::Projection,
            SQLOperator::Limit(_) => SQLOperatorKind::Limit,
            SQLOperator::Noop => SQLOperatorKind::TableScan,
        }
    }

    /// 估算算子代价（处理行数）
    pub fn estimated_cardinality(&self, input_card: usize) -> usize {
        match self {
            SQLOperator::VectorPushdown(op) => op.k,
            SQLOperator::Filter(_) => input_card / 2, // 简化的选择性
            SQLOperator::Projection(_) => input_card,
            SQLOperator::Limit(op) => op.limit.min(input_card),
            SQLOperator::Noop => input_card,
        }
    }
}

/// 执行计划：算子管道
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionPlan {
    pub operators: Vec<SQLOperator>,
    pub uses_vector_index: bool,
    pub estimated_rows: usize,
}

/// SQL 优化器
pub struct SQLOptimizer {
    /// HNSW 索引可用阈值：数据量大于此值时优先用 HNSW
    pub hnsw_threshold: usize,
    /// IVF 索引可用阈值
    pub ivf_threshold: usize,
}

impl Default for SQLOptimizer {
    fn default() -> Self {
        Self {
            hnsw_threshold: 1_000,
            ivf_threshold: 100_000,
        }
    }
}

impl SQLOptimizer {
    pub fn new() -> Self {
        Self::default()
    }

    /// 分析 SQL 条件，识别可下推的向量距离谓词
    pub fn extract_vector_predicates(
        &self,
        filters: &[FilterOperator],
    ) -> Option<(Vec<f32>, DistanceOp, usize, IndexKind)> {
        for filter in filters {
            // 检测距离谓词（value 是向量）
            if let FilterValue::NumberList(vec) = &filter.value {
                if !vec.is_empty() {
                    let dim = vec.len();
                    let query_vector: Vec<f32> = vec.iter().map(|&x| x as f32).collect();

                    // 决定距离函数（基于 column 名前缀或默认 L2）
                    let distance_op = if filter.column.starts_with("cos_") {
                        DistanceOp::Cosine
                    } else if filter.column.starts_with("ip_") {
                        DistanceOp::InnerProduct
                    } else {
                        DistanceOp::L2
                    };

                    // 决定索引类型
                    let k = 100; // 默认 over-fetch 100，filter 后再 LIMIT
                    let index_kind = if dim >= 256 {
                        IndexKind::Hnsw
                    } else {
                        IndexKind::Ivf
                    };

                    return Some((query_vector, distance_op, k, index_kind));
                }
            }
        }
        None
    }

    /// 优化 SQL 执行计划
    pub fn optimize(
        &self,
        filters: Vec<FilterOperator>,
        projections: Vec<String>,
        limit: Option<(usize, usize)>,
        data_size: usize,
    ) -> ExecutionPlan {
        let mut operators = Vec::new();
        let mut estimated_rows = data_size;
        let mut uses_vector_index = false;

        // 1. 检测向量谓词 → 下推到 HNSW/IVF
        if let Some((query_vector, dist_op, k, index_kind)) =
            self.extract_vector_predicates(&filters)
        {
            // 决策索引类型：基于数据规模
            let final_kind = if data_size >= self.ivf_threshold {
                IndexKind::Ivf
            } else if data_size >= self.hnsw_threshold {
                IndexKind::Hnsw
            } else {
                IndexKind::BruteForce
            };

            // 选择 HNSW 默认 ef_search = max(k * 2, 50)
            let ef_search = Some((k * 2).max(50));
            // IVF nprobe = sqrt(nlist)，约 8-32
            let nprobe = Some(8);

            operators.push(SQLOperator::VectorPushdown(VectorPushdownOperator {
                index_name: "auto_hnsw".to_string(),
                index_kind: final_kind,
                query_vector,
                distance_op: dist_op,
                k,
                ef_search: if final_kind == IndexKind::Hnsw { ef_search } else { None },
                nprobe: if final_kind == IndexKind::Ivf { nprobe } else { None },
            }));
            estimated_rows = k;
            uses_vector_index = true;
        }

        // 2. 非向量过滤条件下推
        for filter in &filters {
            // 跳过已经被向量算子消费的
            if let FilterValue::NumberList(vec) = &filter.value {
                if !vec.is_empty() {
                    continue;
                }
            }
            operators.push(SQLOperator::Filter(filter.clone()));
            estimated_rows = SQLOperator::Filter(filter.clone()).estimated_cardinality(estimated_rows);
        }

        // 3. 投影
        if !projections.is_empty() {
            operators.push(SQLOperator::Projection(ProjectionOperator {
                columns: projections,
            }));
        }

        // 4. Limit
        if let Some((l, off)) = limit {
            operators.push(SQLOperator::Limit(LimitOperator {
                limit: l,
                offset: off,
            }));
            estimated_rows = l;
        }

        ExecutionPlan {
            operators,
            uses_vector_index,
            estimated_rows,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_optimizer_pushes_down_vector_predicate() {
        let optimizer = SQLOptimizer::new();
        let filters = vec![FilterOperator {
            column: "embedding".to_string(),
            op: FilterOp::Lt,
            value: FilterValue::NumberList(vec![1.0, 2.0, 3.0]),
        }];

        let plan = optimizer.optimize(filters, vec!["id".to_string()], Some((10, 0)), 50_000);
        assert!(plan.uses_vector_index);
        assert!(plan.operators.iter().any(|op| matches!(op, SQLOperator::VectorPushdown(_))));
    }

    #[test]
    fn test_optimizer_uses_brute_force_for_small_data() {
        let optimizer = SQLOptimizer::new();
        let filters = vec![FilterOperator {
            column: "embedding".to_string(),
            op: FilterOp::Lt,
            value: FilterValue::NumberList(vec![1.0, 2.0, 3.0]),
        }];

        let plan = optimizer.optimize(filters, vec![], None, 100);
        // 小数据量：BruteForce
        if let Some(SQLOperator::VectorPushdown(op)) = plan.operators.first() {
            assert_eq!(op.index_kind, IndexKind::BruteForce);
        }
    }

    #[test]
    fn test_optimizer_selects_hnsw_for_high_dim() {
        let optimizer = SQLOptimizer::new();
        let mut vec = vec![0.0; 1024];
        for i in 0..vec.len() {
            vec[i] = i as f64;
        }
        let filters = vec![FilterOperator {
            column: "embedding".to_string(),
            op: FilterOp::Lt,
            value: FilterValue::NumberList(vec),
        }];

        let plan = optimizer.optimize(filters, vec![], Some((10, 0)), 50_000);
        if let Some(SQLOperator::VectorPushdown(op)) = plan.operators.first() {
            // 高维大数据量用 HNSW
            assert_eq!(op.index_kind, IndexKind::Hnsw);
        }
    }
}
