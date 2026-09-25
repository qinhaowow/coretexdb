//! A3 — 过滤搜索：过滤必须在算距离之前生效，且**过滤永远不能让查询变短**。
//!
//! 宽泛过滤（候选 > `FILTERED_ANN_MIN_CANDIDATES`）会走 ANN 提案路径，
//! 本文件覆盖三条路径：精确扫描、ANN 提案、提案被拒绝后的回退。

use coretexdb::{CoreTexDB, DbConfig};

const DIM: usize = 4;
const QUERY: [f32; DIM] = [0.0, 1.0, 0.0, 0.0];

async fn open(path: &str) -> CoreTexDB {
    let db = CoreTexDB::with_config(DbConfig::new(path));
    db.init().await.expect("init");
    db
}

/// 向量 `[i, 1, 0, 0]`：欧氏距离随 `i` 单调增大，因此"最近的 k 条"可推导。
fn rows(n: usize, group: impl Fn(usize) -> &'static str) -> Vec<(String, Vec<f32>, serde_json::Value)> {
    (0..n)
        .map(|i| {
            let mut v = vec![0.0; DIM];
            v[0] = i as f32;
            v[1] = 1.0;
            (format!("v{i}"), v, serde_json::json!({ "group": group(i) }))
        })
        .collect()
}

fn id_index(id: &str) -> usize {
    id.trim_start_matches('v').parse().expect("id is v<n>")
}

/// 窄过滤：候选数远小于 256，走精确扫描路径，返回的必须是匹配项里最近的。
#[tokio::test]
async fn narrow_filter_ranks_only_the_matches() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir.path().to_str().unwrap().to_string()).await;
    db.create_collection_with_index("c", DIM, "euclidean", "hnsw")
        .await
        .unwrap();

    // 500 条里只有 4 条属于 group=a（i = 0, 125, 250, 375）
    db.insert_vectors("c", rows(500, |i| if i % 125 == 0 { "a" } else { "b" }))
        .await
        .unwrap();

    let hits = db
        .search(
            "c",
            QUERY.to_vec(),
            3,
            Some(serde_json::json!({ "group": "a" })),
        )
        .await
        .unwrap();

    let ids: Vec<&str> = hits.iter().map(|h| h.id.as_str()).collect();
    assert_eq!(ids, vec!["v0", "v125", "v250"], "过滤后应按距离升序返回匹配项");
    for h in &hits {
        assert!(h.distance.is_finite(), "距离不应是 NaN/inf");
    }
    for w in hits.windows(2) {
        assert!(w[0].distance <= w[1].distance, "结果必须按距离升序");
    }
}

/// 宽过滤：候选数 > 256，走 ANN 提案路径，结果语义与精确扫描一致。
#[tokio::test]
async fn wide_filter_stays_correct_through_the_index() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir.path().to_str().unwrap().to_string()).await;
    db.create_collection_with_index("c", DIM, "euclidean", "hnsw")
        .await
        .unwrap();

    // 900 条，偶数 i 属于 group=a → 450 个候选，超过 256 的阈值
    db.insert_vectors("c", rows(900, |i| if i % 2 == 0 { "a" } else { "b" }))
        .await
        .unwrap();

    let hits = db
        .search(
            "c",
            QUERY.to_vec(),
            5,
            Some(serde_json::json!({ "group": "a" })),
        )
        .await
        .unwrap();

    assert_eq!(hits.len(), 5, "候选充足时必须返回 k 条");
    for h in &hits {
        assert_eq!(id_index(&h.id) % 2, 0, "{} 不是 group=a", h.id);
    }
    assert_eq!(hits[0].id, "v0", "最近的匹配项必须排第一");
}

/// 关键回归：候选全在远处时，索引提出的最近邻会被过滤**全部拒绝**，
/// 此时必须回退精确扫描，否则查询会变短（返回 0 条）。
#[tokio::test]
async fn proposals_all_rejected_fall_back_to_exact_scan() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir.path().to_str().unwrap().to_string()).await;
    db.create_collection_with_index("c", DIM, "euclidean", "hnsw")
        .await
        .unwrap();

    let mut data = Vec::new();
    for i in 0..800 {
        // i < 500 → group=b 且贴近查询点；i >= 500 → group=a 且在远处
        let mut v = vec![0.0; DIM];
        v[0] = if i < 500 {
            i as f32
        } else {
            10_000.0 + i as f32
        };
        v[1] = 1.0;
        let group = if i < 500 { "b" } else { "a" };
        data.push((
            format!("v{i}"),
            v,
            serde_json::json!({ "group": group }),
        ));
    }
    db.insert_vectors("c", data).await.unwrap();

    let hits = db
        .search(
            "c",
            QUERY.to_vec(),
            10,
            Some(serde_json::json!({ "group": "a" })),
        )
        .await
        .unwrap();

    assert_eq!(
        hits.len(),
        10,
        "300 个候选却只返回 {} 条 —— 过滤把查询变短了，回退没生效",
        hits.len()
    );
    // group=a 的 300 条全部在 i >= 500，最近的是 i = 500
    for h in &hits {
        assert!(
            id_index(&h.id) >= 500,
            "{} 不属于 group=a",
            h.id
        );
    }
    assert_eq!(hits[0].id, "v500", "候选中最近的一条应排第一");
    // 距离应严格递增
    for w in hits.windows(2) {
        assert!(w[0].distance <= w[1].distance, "结果必须按距离升序");
    }
}

/// 无候选 → 空结果（不 panic、不返回无关数据）。
#[tokio::test]
async fn no_match_returns_empty() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir.path().to_str().unwrap().to_string()).await;
    db.create_collection_with_index("c", DIM, "euclidean", "hnsw")
        .await
        .unwrap();
    db.insert_vectors("c", rows(400, |_| "b")).await.unwrap();

    let hits = db
        .search(
            "c",
            QUERY.to_vec(),
            5,
            Some(serde_json::json!({ "group": "zzz" })),
        )
        .await
        .unwrap();
    assert!(hits.is_empty());
}

/// k 大于匹配数：返回全部匹配项，数量就是匹配数。
#[tokio::test]
async fn k_larger_than_matches_returns_all_matches() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir.path().to_str().unwrap().to_string()).await;
    db.create_collection_with_index("c", DIM, "euclidean", "hnsw")
        .await
        .unwrap();
    // 600 条里只有 3 条 group=a
    db.insert_vectors("c", rows(600, |i| if i % 200 == 0 { "a" } else { "b" }))
        .await
        .unwrap();

    let hits = db
        .search(
            "c",
            QUERY.to_vec(),
            10,
            Some(serde_json::json!({ "group": "a" })),
        )
        .await
        .unwrap();
    let ids: Vec<&str> = hits.iter().map(|h| h.id.as_str()).collect();
    assert_eq!(ids, vec!["v0", "v200", "v400"]);
}

/// 操作符过滤 + 宽候选：`$gte` 在 ANN 提案路径下同样生效。
#[tokio::test]
async fn operator_filter_works_on_the_wide_path() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir.path().to_str().unwrap().to_string()).await;
    db.create_collection_with_index("c", DIM, "euclidean", "hnsw")
        .await
        .unwrap();

    let rows: Vec<(String, Vec<f32>, serde_json::Value)> = (0..700)
        .map(|i| {
            let mut v = vec![0.0; DIM];
            v[0] = i as f32;
            v[1] = 1.0;
            (
                format!("v{i}"),
                v,
                serde_json::json!({ "n": i as u32 }),
            )
        })
        .collect();
    db.insert_vectors("c", rows).await.unwrap();

    // n >= 400 → 300 个候选（> 256）
    let hits = db
        .search(
            "c",
            QUERY.to_vec(),
            4,
            Some(serde_json::json!({ "n": { "$gte": 400 } })),
        )
        .await
        .unwrap();

    assert_eq!(hits.len(), 4);
    let idx: Vec<usize> = hits.iter().map(|h| id_index(&h.id)).collect();
    assert_eq!(idx, vec![400, 401, 402, 403], "$gte 过滤后按距离升序");
}
