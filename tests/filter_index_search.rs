//! B5 — 过滤预筛索引的端到端行为：候选集预筛不改变任何结果语义，
//! 缓存随写入失效，删除路径与查询路径共享同一预筛。
//!
//! 数据 8 条，向量 `[i, 1, 0, 0]`（euclidean，查询 `[0, 1, 0, 0]` → 距离 = i），
//! 因此每个 filter 的预期结果顺序（按距离升序）都可手算。

use coretexdb::{CoreTexDB, DbConfig};

const DIM: usize = 4;
const QUERY: [f32; DIM] = [0.0, 1.0, 0.0, 0.0];

async fn open(path: &str) -> CoreTexDB {
    let db = CoreTexDB::with_config(DbConfig::new(path));
    db.init().await.expect("init");
    db
}

/// 元数据形态覆盖：等值、数字（含 1 vs 1.0 边界）、缺失字段、嵌套对象、
/// 数组、含 `$gt` 字面量的对象、非对象 metadata。
fn rows() -> Vec<(String, Vec<f32>, serde_json::Value)> {
    let metas = [
        serde_json::json!({"group": "alpha", "score": 10, "tag": "x"}),
        serde_json::json!({"group": "beta", "score": 1.0, "tag": "y"}),
        serde_json::json!({"group": "alpha", "score": 50}),
        serde_json::json!({"group": "beta", "score": 100, "deep": {"obj": 1}}),
        serde_json::json!({"other": true}),
        serde_json::json!(["array", "metadata"]),
        serde_json::json!(42),
        serde_json::json!({"group": "alpha", "score": 10, "extra": {"$gt": 9}}),
    ];
    metas
        .into_iter()
        .enumerate()
        .map(|(i, meta)| {
            let mut v = vec![0.0; DIM];
            v[0] = i as f32;
            v[1] = 1.0;
            (format!("r{}", i + 1), v, meta)
        })
        .collect()
}

async fn seeded_db() -> (tempfile::TempDir, CoreTexDB) {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir.path().to_str().unwrap().to_string()).await;
    db.create_collection_with_index("c", DIM, "euclidean", "brute_force")
        .await
        .unwrap();
    db.insert_vectors("c", rows()).await.unwrap();
    (dir, db)
}

async fn search_ids(db: &CoreTexDB, filter: serde_json::Value) -> Vec<String> {
    let hits = db.search("c", QUERY.to_vec(), 10, Some(filter)).await.unwrap();
    hits.into_iter().map(|h| h.id).collect()
}

/// 等值预筛的典型场景：结果 = 匹配项按距离升序，与全表扫语义一致。
#[tokio::test]
async fn equality_filter_returns_the_exact_subset() {
    let (_dir, db) = seeded_db().await;
    assert_eq!(
        search_ids(&db, serde_json::json!({"group": "alpha"})).await,
        vec!["r1", "r3", "r8"]
    );
}

/// 算子组（范围 / $in / $ne / $exists / $regex）逐一对照手算真值。
#[tokio::test]
async fn operator_filters_agree_with_linear_semantics() {
    let (_dir, db) = seeded_db().await;

    // score > 5：r1(10)、r3(50)、r4(100)、r8(10)；r2 的 1.0 不过。
    assert_eq!(
        search_ids(&db, serde_json::json!({"score": {"$gt": 5}})).await,
        vec!["r1", "r3", "r4", "r8"]
    );
    // $in 精确并集：score ∈ {10, 50} → r1、r3、r8。
    assert_eq!(
        search_ids(&db, serde_json::json!({"score": {"$in": [10, 50]}})).await,
        vec!["r1", "r3", "r8"]
    );
    // $ne 只判有该字段的：group ≠ alpha → r2、r4。
    assert_eq!(
        search_ids(&db, serde_json::json!({"group": {"$ne": "alpha"}})).await,
        vec!["r2", "r4"]
    );
    // $exists: false → 缺失 tag 的全部记录。
    assert_eq!(
        search_ids(&db, serde_json::json!({"tag": {"$exists": false}})).await,
        vec!["r3", "r4", "r5", "r6", "r7", "r8"]
    );
    // $regex 只命中字符串：tag ~ ^x → r1。
    assert_eq!(
        search_ids(&db, serde_json::json!({"tag": {"$regex": "^x"}})).await,
        vec!["r1"]
    );
    // 嵌套对象等值：deep == {"obj": 1} → r4。
    assert_eq!(
        search_ids(&db, serde_json::json!({"deep": {"obj": 1}})).await,
        vec!["r4"]
    );
}

/// 写入必须让缓存失效：新记录能被查到，删除的不再出现。
#[tokio::test]
async fn insert_and_delete_invalidate_the_index_cache() {
    let (_dir, db) = seeded_db().await;

    assert_eq!(
        search_ids(&db, serde_json::json!({"group": "alpha"})).await,
        vec!["r1", "r3", "r8"]
    );

    // 插入一条匹配的（id r9，i=8 → 距离 8，最后出现）
    let mut v = vec![0.0; DIM];
    v[0] = 8.0;
    v[1] = 1.0;
    db.insert_vectors(
        "c",
        vec![(
            "r9".to_string(),
            v,
            serde_json::json!({"group": "alpha", "score": 3}),
        )],
    )
    .await
    .unwrap();
    assert_eq!(
        search_ids(&db, serde_json::json!({"group": "alpha"})).await,
        vec!["r1", "r3", "r8", "r9"],
        "插入后索引必须失效重建，新记录要能被查到"
    );

    // 删除刚插入的
    db.delete_vectors("c", &["r9".to_string()]).await.unwrap();
    assert_eq!(
        search_ids(&db, serde_json::json!({"group": "alpha"})).await,
        vec!["r1", "r3", "r8"],
        "删除后索引必须失效，幽灵候选不能出现"
    );
}

/// 回退路径（$not、顶层数组）与多键隐式 AND：语义仍与线性一致。
#[tokio::test]
async fn fallback_and_compound_filters_keep_linear_semantics() {
    let (_dir, db) = seeded_db().await;

    // $not：group ≠ alpha 的记录里……注意 $not 语义是"整体不匹配"：
    // r2、r4、r5、r6、r7 匹配 {"group": "alpha"} 的补集。
    assert_eq!(
        search_ids(&db, serde_json::json!({"$not": {"group": "alpha"}})).await,
        vec!["r2", "r4", "r5", "r6", "r7"]
    );
    // 顶层数组 = any：group=alpha 或 other=true → r1、r3、r5、r8。
    assert_eq!(
        search_ids(
            &db,
            serde_json::json!([{"group": "alpha"}, {"other": true}])
        )
        .await,
        vec!["r1", "r3", "r5", "r8"]
    );
    // 隐式多键 AND：group=alpha 且 score>=50 → 仅 r3（r1/r8 的 score=10 不过）。
    assert_eq!(
        search_ids(
            &db,
            serde_json::json!({"group": "alpha", "score": {"$gte": 50}})
        )
        .await,
        vec!["r3"]
    );
}

/// 等价写法必须给出同一结果——证明"精确"与"超集+精筛"殊途同归。
#[tokio::test]
async fn equivalent_spellings_agree() {
    let (_dir, db) = seeded_db().await;

    let plain = search_ids(&db, serde_json::json!({"group": "alpha"})).await;
    let via_in = search_ids(&db, serde_json::json!({"group": {"$in": ["alpha"]}})).await;
    let via_and = search_ids(&db, serde_json::json!({"$and": [{"group": "alpha"}]})).await;
    let via_or = search_ids(
        &db,
        serde_json::json!({"$or": [{"group": "alpha"}, {"group": "alpha"}]}),
    )
    .await;

    assert_eq!(plain, via_in, "$in 单元素应等于等值");
    assert_eq!(plain, via_and, "$and 单条件应等于等值");
    assert_eq!(plain, via_or, "$or 同条件应等于等值");
}

/// 删除路径与查询路径共享同一预筛：删掉的恰是匹配项，其余完好。
#[tokio::test]
async fn delete_where_narrows_to_the_same_candidates() {
    let (_dir, db) = seeded_db().await;

    let deleted = db
        .delete_vectors_where("c", &serde_json::json!({"group": "alpha"}))
        .await
        .unwrap();
    assert_eq!(deleted, vec!["r1", "r3", "r8"]);

    // 残留数据完好：score>5 的剩余项 = r4。
    assert_eq!(
        search_ids(&db, serde_json::json!({"score": {"$gt": 5}})).await,
        vec!["r4"]
    );
    // 被删的不再出现
    assert_eq!(
        search_ids(&db, serde_json::json!({"group": "alpha"})).await,
        Vec::<String>::new()
    );
    // 其余 5 条仍在
    assert_eq!(
        db.get_vectors_count("c").await.unwrap(),
        5,
        "删除必须只影响匹配项"
    );
}
