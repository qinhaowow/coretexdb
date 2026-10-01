//! B2 — rerank 接线：`hybrid_search_reranked` 的两阶段重排。
//!
//! 契约：与纯 RRF 版保持**同一命中集合、同一 per-id `sources`、同一
//! k/filter 语义**，分数有限且降序；无文本查询时逐位透传（没有可匹配
//! 的词，重排无意义）。词重叠翻转排序的语义由 pipeline 单测覆盖
//! （`src/coretex_rerank/pipeline.rs`），避免依赖 BM25 排名的脆弱集成断言。

use coretexdb::{CoreTexDB, DbConfig, HybridSearchRequest};

const DIM: usize = 4;
const QUERY: [f32; DIM] = [0.0, 1.0, 0.0, 0.0];

async fn open(path: &str) -> CoreTexDB {
    let db = CoreTexDB::with_config(DbConfig::new(path));
    db.init().await.expect("init");
    db
}

/// 向量 `[i, 1, 0, 0]`（欧氏最近邻即最小 `i`），每 3 篇含 `alpha`。
fn rows(n: usize) -> Vec<(String, Vec<f32>, serde_json::Value)> {
    (0..n)
        .map(|i| {
            let mut v = vec![0.0; DIM];
            v[0] = i as f32;
            v[1] = 1.0;
            let text = if i % 3 == 0 {
                format!("alpha release note {i}")
            } else {
                format!("ordinary note {i}")
            };
            (
                format!("v{i}"),
                v,
                serde_json::json!({ "text": text, "n": i as u32 }),
            )
        })
        .collect()
}

fn id_index(id: &str) -> usize {
    id.trim_start_matches('v').parse().expect("id is v<n>")
}

/// 双侧 + rerank：id 集合与 per-id `sources` 与纯 RRF 完全一致，
/// 分数有限且非增序。
#[tokio::test]
async fn rerank_preserves_ids_sources_and_scores_are_descending() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir.path().to_str().unwrap().to_string()).await;
    db.create_collection("c", DIM, "euclidean").await.unwrap();
    db.insert_vectors("c", rows(40)).await.unwrap();

    let req = || {
        HybridSearchRequest::new(5)
            .with_vector(QUERY.to_vec())
            .with_text("alpha")
    };
    let base = db.hybrid_search("c", req()).await.unwrap();
    let reranked = db.hybrid_search_reranked("c", req()).await.unwrap();

    assert_eq!(reranked.len(), 5, "k=5 时应满额返回");
    assert_eq!(base.len(), 5);

    for hit in &base {
        let twin = reranked
            .iter()
            .find(|h| h.id == hit.id)
            .unwrap_or_else(|| panic!("rerank 不得丢 id {}", hit.id));
        assert_eq!(twin.sources, hit.sources, "{} 来源应与 RRF 一致", hit.id);
    }
    for w in reranked.windows(2) {
        assert!(w[0].score.is_finite() && w[1].score.is_finite());
        assert!(
            w[0].score >= w[1].score,
            "rerank 分数必须非增序：{:?} !>= {:?}",
            w[0],
            w[1]
        );
    }
}

/// 无文本查询：rerank 必须**逐位透传**纯 RRF 结果（id、分数 bits、来源）。
#[tokio::test]
async fn rerank_without_text_is_exact_passthrough() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir.path().to_str().unwrap().to_string()).await;
    db.create_collection("c", DIM, "euclidean").await.unwrap();
    db.insert_vectors("c", rows(40)).await.unwrap();

    let req = || HybridSearchRequest::new(5).with_vector(QUERY.to_vec());
    let base = db.hybrid_search("c", req()).await.unwrap();
    let rr = db.hybrid_search_reranked("c", req()).await.unwrap();

    assert_eq!(rr.len(), base.len(), "透传不得改变条数");
    for (a, b) in base.iter().zip(rr.iter()) {
        assert_eq!(a.id, b.id, "透传不得改变顺序");
        assert_eq!(a.score.to_bits(), b.score.to_bits(), "透传分数必须逐位一致");
        assert_eq!(a.sources, b.sources);
    }
}

/// filter 在 rerank 路径同样生效；k=0 返回空；未知集合与普通入口同错。
#[tokio::test]
async fn rerank_respects_filter_k_zero_and_unknown_collection() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir.path().to_str().unwrap().to_string()).await;
    db.create_collection("c", DIM, "euclidean").await.unwrap();
    db.insert_vectors("c", rows(40)).await.unwrap();

    let req = || {
        HybridSearchRequest::new(10)
            .with_vector(QUERY.to_vec())
            .with_text("alpha")
            .with_filter(serde_json::json!({ "n": { "$lt": 10 } }))
    };
    let base = db.hybrid_search("c", req()).await.unwrap();
    let rr = db.hybrid_search_reranked("c", req()).await.unwrap();

    assert!(!rr.is_empty(), "n<10 内存在含 alpha 的文档");
    for h in &rr {
        assert!(id_index(&h.id) < 10, "{} 未通过 filter", h.id);
    }
    let mut base_ids: Vec<&str> = base.iter().map(|h| h.id.as_str()).collect();
    let mut rr_ids: Vec<&str> = rr.iter().map(|h| h.id.as_str()).collect();
    base_ids.sort_unstable();
    rr_ids.sort_unstable();
    assert_eq!(base_ids, rr_ids, "rerank 不得改变命中集合");

    // k=0 → 空（与纯 RRF 一致的早退）
    let empty = db
        .hybrid_search_reranked(
            "c",
            HybridSearchRequest::new(0).with_vector(QUERY.to_vec()).with_text("alpha"),
        )
        .await
        .unwrap();
    assert!(empty.is_empty());

    // 未知集合：两种入口报同一种错
    let err = db
        .hybrid_search_reranked(
            "missing",
            HybridSearchRequest::new(3).with_text("alpha"),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, coretexdb::CoreTexError::CollectionNotFound(_)),
        "应为 CollectionNotFound，实际: {err}"
    );
}
