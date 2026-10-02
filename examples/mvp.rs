//! 最小可运行 MVP：一条命令跑完核心链路，每一步都断言结果。
//!
//! 这个示例不是"演示用法"，而是**MVP 的可执行定义** —— 如果它通过，说明
//! 「建集合 → 写入 → 搜索 → 过滤 → 混合检索 → 持久化 → 重启恢复 → TTL」
//! 这条主链路是可用的。它被 CI 的 `examples` job 真实运行，所以任何回归都会
//! 让 CI 变红。
//!
//! 刻意**没有**在这里演示的：WebSocket、GraphQL、Raft、集群、多模态、
//! Prometheus —— 那些模块存在但没有入口能调用（见 README §0）。与其让示例
//! 看起来什么都能做，不如只承诺真正能用的部分。
//!
//! ```bash
//! cargo run --example mvp
//! ```

// `DistanceMetric` is exported from both `coretex_core` and `coretex_hybrid`
// (a name collision in the crate root), so it is imported by module path here.
use coretexdb::coretex_core::DistanceMetric;
use coretexdb::{CoreTexDB, DbConfig, HybridSearchRequest, IndexType};
use serde_json::json;

/// 固定维度的小向量，避免示例依赖随机数导致结果不可复现。
fn vec_with_tag(tag: f32) -> Vec<f32> {
    vec![tag, 1.0 - tag, tag * 0.5]
}

async fn open(path: &str) -> CoreTexDB {
    let db = CoreTexDB::with_config(DbConfig::new(path));
    db.init().await.expect("init failed");
    db
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_str().unwrap().to_string();

    // ── 1. 建集合并指定索引类型 ────────────────────────────────────────
    println!("[1/8] 创建集合 docs（dim=3, index=hnsw）");
    {
        let db = open(&path).await;
        db.create_collection_with_index("docs", 3, "cosine", "hnsw").await?;

        let schema = db.get_collection("docs").await?;
        assert_eq!(schema.dimension, 3, "维度应与建集合时一致");
        assert_eq!(schema.name, "docs", "集合名应正确保存");
        assert_eq!(
            schema.distance_metric,
            DistanceMetric::Cosine,
            "距离度量应为 cosine"
        );
        // 建集合时指定的 hnsw 必须真的落到 schema 上，而不是被忽略。
        assert!(
            schema.indexes.iter().any(|ix| ix.index_type == IndexType::HNSW),
            "schema 中应记录 hnsw 索引，实际 indexes={:?}",
            schema.indexes
        );
        println!("      ✓ schema 与索引类型均已生效");
    }

    // ── 2. 写入 ───────────────────────────────────────────────────────
    println!("[2/8] 写入 6 条向量（带 metadata，用于后面的过滤）");
    let db = open(&path).await;
    let rows: Vec<(String, Vec<f32>, serde_json::Value)> = vec![
        ("a".into(), vec_with_tag(0.0), json!({"lang": "zh", "kind": "intro"})),
        ("b".into(), vec_with_tag(0.2), json!({"lang": "zh", "kind": "body"})),
        ("c".into(), vec_with_tag(0.4), json!({"lang": "en", "kind": "body"})),
        ("d".into(), vec_with_tag(0.6), json!({"lang": "en", "kind": "intro"})),
        ("e".into(), vec_with_tag(0.8), json!({"lang": "zh", "kind": "body"})),
        ("f".into(), vec_with_tag(1.0), json!({"lang": "en", "kind": "note"})),
    ];
    let ids = db.insert_vectors("docs", rows).await?;
    assert_eq!(ids.len(), 6, "应写入 6 条");
    assert_eq!(db.get_vectors_count("docs").await?, 6, "计数应为 6");
    println!("      ✓ 写入 {} 条，计数一致", ids.len());

    // ── 3. 向量搜索 ───────────────────────────────────────────────────
    println!("[3/8] 向量搜索：查最接近 tag=0.9 的 2 条");
    let hits = db.search("docs", vec_with_tag(0.9), 2, None).await?;
    assert_eq!(hits.len(), 2, "k=2 应返回 2 条");
    assert_eq!(hits[0].id, "f", "tag=0.9 最接近 tag=1.0 的 f");
    // 距离必须单调不降，否则排序有问题。
    assert!(
        hits[0].distance <= hits[1].distance,
        "结果应按距离升序：{} vs {}",
        hits[0].distance,
        hits[1].distance
    );
    println!("      ✓ 命中 {} (d={:.4})，顺序正确", hits[0].id, hits[0].distance);

    // ── 4. metadata 过滤 ──────────────────────────────────────────────
    println!("[4/8] 过滤搜索：只要 lang=en");
    let filtered = db
        .search("docs", vec_with_tag(0.5), 10, Some(json!({"lang": "en"})))
        .await?;
    assert_eq!(filtered.len(), 3, "lang=en 的只有 c/d/f 三条");
    for hit in &filtered {
        let (_, meta) = db.get_vector("docs", &hit.id).await?.expect("hit 应存在");
        assert_eq!(meta["lang"], "en", "过滤不应返回 lang!=en 的记录");
    }
    println!("      ✓ 返回 {} 条，全部满足 lang=en", filtered.len());

    // ── 5. 混合检索（向量 + BM25 → RRF）─────────────────────────────
    println!("[5/8] 混合检索：向量 + 文本，仅按 kind=body 的记录排序");
    // 先补一条带文本的记录，hybrid 的文本侧读 metadata["text"]。
    db.insert_vectors("docs", vec![(
        "g".to_string(),
        vec_with_tag(0.5),
        json!({"lang": "zh", "kind": "body", "text": "rust vector database"}),
    )])
    .await?;

    let request = HybridSearchRequest::new(3)
        .with_vector(vec_with_tag(0.5))
        .with_text("vector database")
        .with_filter(json!({"kind": "body"}));
    let hits = db.hybrid_search("docs", request).await?;
    assert!(!hits.is_empty(), "混合检索应至少返回一条");
    assert!(
        hits.len() <= 3,
        "k=3 不应返回超过 3 条，实际 {}",
        hits.len()
    );
    // filter 必须在融合前生效 —— 每条命中的 kind 都得是 body。
    for hit in &hits {
        let (_, meta) = db.get_vector("docs", &hit.id).await?.expect("hit 应存在");
        assert_eq!(meta["kind"], "body", "filter 未在融合前生效");
    }
    let text_sides = hits.iter().filter(|h| h.sources.iter().any(|s| s == "text")).count();
    println!(
        "      ✓ 返回 {} 条（其中 {} 条由文本侧命中），filter 生效",
        hits.len(),
        text_sides
    );

    // rerank 变体：ids/k/filter 契约必须与 hybrid 一致。
    let reranked = db
        .hybrid_search_reranked(
            "docs",
            HybridSearchRequest::new(3)
                .with_vector(vec_with_tag(0.5))
                .with_text("vector database")
                .with_filter(json!({"kind": "body"})),
        )
        .await?;
    let mut plain: Vec<&str> = hits.iter().map(|h| h.id.as_str()).collect();
    let mut rr: Vec<&str> = reranked.iter().map(|h| h.id.as_str()).collect();
    plain.sort();
    rr.sort();
    assert_eq!(plain, rr, "rerank 不应改变命中集合，只应改变顺序");
    println!("      ✓ rerank 命中集合与 hybrid 一致");

    // ── 6. TTL ────────────────────────────────────────────────────────
    println!("[6/8] TTL：给 a 设置 60s TTL，清除后应回到 6 条");
    db.set_vector_ttl("docs", "a", 60).await?;
    db.remove_vector_ttl("docs", "a").await?;
    assert_eq!(
        db.get_vectors_count("docs").await?,
        7,
        "TTL 移除后记录必须还在（此时共 7 条：a-f + g）"
    );
    println!("      ✓ TTL 设置/移除不影响记录本身");

    // ── 7. 索引持久化 ─────────────────────────────────────────────────
    println!("[7/8] 落盘索引");
    let written = db.save_indexes().await?;
    assert!(written > 0, "应至少写出一个索引文件");
    println!("      ✓ 写入 {} 个索引文件 → {}", written, db.index_dir().display());

    // ── 8. 重启恢复 ───────────────────────────────────────────────────
    println!("[8/8] 重新打开同一目录，验证数据完整恢复");
    drop(db);
    let db = open(&path).await;

    assert_eq!(
        db.get_vectors_count("docs").await?,
        7,
        "重启后应恢复全部 7 条"
    );
    assert_eq!(
        db.list_collections().await?,
        vec!["docs".to_string()],
        "重启后集合应恢复"
    );

    let hits = db.search("docs", vec_with_tag(0.9), 2, None).await?;
    assert_eq!(hits.len(), 2, "重启后搜索应仍能返回结果");
    assert_eq!(hits[0].id, "f", "重启后最近邻应保持一致");

    let recovered = db.get_vector("docs", "g").await?;
    assert!(recovered.is_some(), "重启后带文本的记录应恢复");
    assert_eq!(
        recovered.unwrap().1["text"],
        "rust vector database",
        "metadata 应完整恢复，字段不能丢"
    );
    println!("      ✓ 7 条数据 + schema + metadata 全部恢复，搜索结果一致");

    println!();
    println!("MVP 核心链路通过：");
    println!("  集合与索引类型 · 写入 · 向量搜索 · metadata 过滤");
    println!("  混合检索（含 filter 与 rerank）· TTL · 索引持久化 · 重启恢复");
    println!();
    println!("未包含（模块存在但无入口，见 README §0）：");
    println!("  WebSocket · GraphQL · Raft/集群 · 多模态 · Prometheus 指标");

    Ok(())
}
