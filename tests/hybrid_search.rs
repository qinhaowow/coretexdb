//! B2a — hybrid search: 向量路 + BM25 文本路经 RRF 融合。
//!
//! 覆盖：单侧语义、双命中胜出、filter 两侧生效、**写入后缓存失效**
//! （BM25 索引按 `data_version` 验证，写路径统一 bump）、自定义文本字段。

use coretexdb::{CoreTexDB, DbConfig, HybridSearchRequest};

const DIM: usize = 4;
const QUERY: [f32; DIM] = [0.0, 1.0, 0.0, 0.0];

async fn open(path: &str) -> CoreTexDB {
    let db = CoreTexDB::with_config(DbConfig::new(path));
    db.init().await.expect("init");
    db
}

/// 向量 `[i, 1, 0, 0]`：欧氏距离随 `i` 单调增大，最近邻即最小 `i`。
fn rows(
    n: usize,
    text_of: impl Fn(usize) -> String,
) -> Vec<(String, Vec<f32>, serde_json::Value)> {
    (0..n)
        .map(|i| {
            let mut v = vec![0.0; DIM];
            v[0] = i as f32;
            v[1] = 1.0;
            (
                format!("v{i}"),
                v,
                serde_json::json!({ "text": text_of(i), "n": i as u32 }),
            )
        })
        .collect()
}

fn id_index(id: &str) -> usize {
    id.trim_start_matches('v').parse().expect("id is v<n>")
}

/// 只给文本：命中的必须全是含查询词的文档，且来源标为 `"text"`。
#[tokio::test]
async fn text_only_returns_bm25_hits() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir.path().to_str().unwrap().to_string()).await;
    db.create_collection("c", DIM, "euclidean").await.unwrap();

    // 400 条，仅 i % 17 == 0 的文档含目标词（24 篇，足够填满 pool）
    db.insert_vectors(
        "c",
        rows(400, |i| {
            if i % 17 == 0 {
                "quantum entanglement explained".to_string()
            } else {
                format!("ordinary note number {i}")
            }
        }),
    )
    .await
    .unwrap();

    let hits = db
        .hybrid_search("c", HybridSearchRequest::new(3).with_text("quantum"))
        .await
        .unwrap();

    assert_eq!(hits.len(), 3, "含查询词的文档充足时应返回 k 条");
    for h in &hits {
        assert_eq!(id_index(&h.id) % 17, 0, "{} 不含 quantum", h.id);
        assert_eq!(h.sources, vec!["text"], "只给文本时来源只有 text");
    }
}

/// 只给向量：结果顺序必须与普通 `search` 完全一致（RRF 对单侧是单调的）。
#[tokio::test]
async fn vector_only_agrees_with_plain_search() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir.path().to_str().unwrap().to_string()).await;
    db.create_collection("c", DIM, "euclidean").await.unwrap();
    db.insert_vectors("c", rows(500, |_| format!("note {}", 0))).await.unwrap();

    let plain = db.search("c", QUERY.to_vec(), 5, None).await.unwrap();
    let hybrid = db
        .hybrid_search("c", HybridSearchRequest::new(5).with_vector(QUERY.to_vec()))
        .await
        .unwrap();

    let plain_ids: Vec<&str> = plain.iter().map(|h| h.id.as_str()).collect();
    let hybrid_ids: Vec<&str> = hybrid.iter().map(|h| h.id.as_str()).collect();
    assert_eq!(hybrid_ids, plain_ids, "单侧融合不应改变向量排序");
    for h in &hybrid {
        assert_eq!(h.sources, vec!["vector"]);
    }
}

/// 双路都命中的 id 必须排第一（RRF 的核心语义），来源两侧齐全。
#[tokio::test]
async fn a_double_hit_outranks_either_single_side() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir.path().to_str().unwrap().to_string()).await;
    db.create_collection("c", DIM, "euclidean").await.unwrap();

    // 文本命中：池内的 v3（向量前 16 近）与池外的 v500
    db.insert_vectors(
        "c",
        rows(600, |i| {
            if i == 3 || i == 500 {
                "alpha release notes".to_string()
            } else {
                format!("note {i}")
            }
        }),
    )
    .await
    .unwrap();

    let hits = db
        .hybrid_search(
            "c",
            HybridSearchRequest::new(3)
                .with_vector(QUERY.to_vec())
                .with_text("alpha"),
        )
        .await
        .unwrap();

    assert_eq!(hits.len(), 3);
    assert_eq!(hits[0].id, "v3", "双路命中者必须排第一");
    assert_eq!(
        hits[0].sources,
        vec!["vector", "text"],
        "来源必须按该 id 实际命中情况给出"
    );
    assert_eq!(hits[1].id, "v0", "纯向量第一（最近邻）应排第二");
    assert_eq!(hits[1].sources, vec!["vector"]);
    assert!(hits[0].score > hits[1].score);
    // 第三名在 v1 与 v500 之间（文本 rank 取决于两篇同分文档的内部顺序）。
    assert!(hits[2].id == "v1" || hits[2].id == "v500");
}

/// filter 必须同时作用于两侧：文本命中里超范围的 id 被剔除，
/// 向量侧也只回范围内邻居。
#[tokio::test]
async fn filter_applies_to_both_sides() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir.path().to_str().unwrap().to_string()).await;
    db.create_collection("c", DIM, "euclidean").await.unwrap();

    db.insert_vectors(
        "c",
        rows(600, |i| {
            if i == 3 || i == 500 {
                "alpha release notes".to_string()
            } else {
                format!("note {i}")
            }
        }),
    )
    .await
    .unwrap();

    let hits = db
        .hybrid_search(
            "c",
            HybridSearchRequest::new(3)
                .with_vector(QUERY.to_vec())
                .with_text("alpha")
                .with_filter(serde_json::json!({ "n": { "$lt": 10 } })),
        )
        .await
        .unwrap();

    assert_eq!(hits.len(), 3, "范围内候选充足时应返回 k 条");
    for h in &hits {
        assert!(
            id_index(&h.id) < 10,
            "{} 超出 filter 范围（文本侧未过滤）",
            h.id
        );
    }
    assert_eq!(hits[0].id, "v3");
    assert_eq!(hits[0].sources, vec!["vector", "text"]);
    // v500 在范围外，文本侧已剔除 —— 它不能挤掉范围内的 v1。
    assert_eq!(hits[1].id, "v0");
    assert_eq!(hits[2].id, "v1");
}

/// 核心回归：BM25 缓存必须随写入失效（插入能被搜到、删除后搜不到）。
#[tokio::test]
async fn bm25_cache_invalidates_on_insert_and_delete() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir.path().to_str().unwrap().to_string()).await;
    db.create_collection("c", DIM, "euclidean").await.unwrap();

    db.insert_vectors("c", rows(400, |i| format!("note {i}")))
        .await
        .unwrap();

    // 第一次查询建立缓存（无 alpha → 空）
    assert!(
        db.hybrid_search("c", HybridSearchRequest::new(5).with_text("alpha"))
            .await
            .unwrap()
            .is_empty()
    );

    // 插入含 alpha 的文档 → 缓存失效重建后必须能搜到
    db.insert_vectors(
        "c",
        vec![(
            "v999".to_string(),
            vec![999.0, 1.0, 0.0, 0.0],
            serde_json::json!({ "text": "alpha docs", "n": 999u32 }),
        )],
    )
    .await
    .unwrap();
    let hits = db
        .hybrid_search("c", HybridSearchRequest::new(5).with_text("alpha"))
        .await
        .unwrap();
    assert_eq!(hits.len(), 1, "新插入的文本必须可搜（缓存已失效）");
    assert_eq!(hits[0].id, "v999");

    // 删除后 → 缓存再次失效，结果必须回落为空
    db.delete_vectors("c", &["v999".to_string()]).await.unwrap();
    assert!(
        db.hybrid_search("c", HybridSearchRequest::new(5).with_text("alpha"))
            .await
            .unwrap()
            .is_empty(),
        "已删除文档不得再被文本路返回"
    );
}

/// 文本字段可配置：数据放在 `content` 时，默认字段查不到，指定后才行。
#[tokio::test]
async fn text_field_is_configurable() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir.path().to_str().unwrap().to_string()).await;
    db.create_collection("c", DIM, "euclidean").await.unwrap();

    let rows: Vec<(String, Vec<f32>, serde_json::Value)> = (0..300)
        .map(|i| {
            let mut v = vec![0.0; DIM];
            v[0] = i as f32;
            v[1] = 1.0;
            let content = if i % 20 == 0 {
                "alpha build".to_string()
            } else {
                format!("log {i}")
            };
            (
                format!("v{i}"),
                v,
                serde_json::json!({ "content": content, "n": i as u32 }),
            )
        })
        .collect();
    db.insert_vectors("c", rows).await.unwrap();

    // 默认读 `text` 字段 —— 不存在 → 空
    let default_field = db
        .hybrid_search("c", HybridSearchRequest::new(3).with_text("alpha"))
        .await
        .unwrap();
    assert!(default_field.is_empty(), "默认字段 text 不存在时应为空");

    let hits = db
        .hybrid_search(
            "c",
            HybridSearchRequest::new(3)
                .with_text("alpha")
                .with_text_field("content"),
        )
        .await
        .unwrap();
    assert_eq!(hits.len(), 3);
    for h in &hits {
        assert_eq!(id_index(&h.id) % 20, 0, "{} 不是 alpha build", h.id);
    }
}

/// 边界：两侧都空 / 空白文本 → 空结果；未知集合 → 错误。
#[tokio::test]
async fn empty_request_and_unknown_collection() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir.path().to_str().unwrap().to_string()).await;
    db.create_collection("c", DIM, "euclidean").await.unwrap();
    db.insert_vectors("c", rows(50, |_| "note".to_string()))
        .await
        .unwrap();

    let none = db
        .hybrid_search("c", HybridSearchRequest::new(5))
        .await
        .unwrap();
    assert!(none.is_empty(), "既无向量也无文本 → 空");

    let blank = db
        .hybrid_search("c", HybridSearchRequest::new(5).with_text("   "))
        .await
        .unwrap();
    assert!(blank.is_empty(), "空白文本应被跳过而不是参与融合");

    let k_zero = db
        .hybrid_search(
            "c",
            HybridSearchRequest::new(0).with_vector(QUERY.to_vec()),
        )
        .await
        .unwrap();
    assert!(k_zero.is_empty(), "k = 0 → 空");

    let unknown = db
        .hybrid_search("nope", HybridSearchRequest::new(5).with_text("alpha"))
        .await;
    assert!(unknown.is_err(), "未知集合必须报错，两侧统一");
}
