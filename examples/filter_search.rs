//! 带元数据过滤的搜索：过滤条件在**算距离之前**生效，
//! 因此只要存在 k 条满足条件的向量，就一定返回 k 条。
//!
//! ```bash
//! cargo run --example filter_search
//! ```

use coretexdb::{CoreTexDB, DbConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_str().unwrap().to_string();

    let db = CoreTexDB::with_config(DbConfig::new(&path));
    db.init().await?;

    db.create_collection("logs", 4, "euclidean").await?;

    let mut rows = Vec::new();
    for i in 0..40u32 {
        let service = if i % 2 == 0 { "api" } else { "worker" };
        let vector = vec![i as f32, 1.0, 0.0, 0.0];
        rows.push((
            format!("log-{i}"),
            vector,
            serde_json::json!({ "service": service, "level": (i % 4) + 1 }),
        ));
    }
    db.insert_vectors("logs", rows).await?;

    let query = vec![10.0, 1.0, 0.0, 0.0];

    // 等值过滤
    let api = db
        .search("logs", query.clone(), 3, Some(serde_json::json!({"service": "api"})))
        .await?;
    println!("service=api 的最近 3 条：");
    for hit in &api {
        println!("  {:<8} distance={:.4}", hit.id, hit.distance);
    }

    // 操作符过滤：$gte
    let hot = db
        .search("logs", query, 3, Some(serde_json::json!({"level": {"$gte": 4}})))
        .await?;
    println!("level >= 4 的最近 3 条：");
    for hit in &hot {
        println!("  {:<8} distance={:.4}", hit.id, hit.distance);
    }

    // 过滤后的结果里不允许出现不匹配的行
    let all_match = api.iter().all(|h| {
        h.id.strip_prefix("log-")
            .and_then(|n| n.parse::<u32>().ok())
            .map(|n| n % 2 == 0)
            .unwrap_or(false)
    });
    println!("过滤条件全部生效：{all_match}");

    Ok(())
}
