//! 快速上手：建库 → 建集合 → 写入 → 搜索。
//!
//! ```bash
//! cargo run --example quickstart
//! ```

use coretexdb::{CoreTexDB, DbConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 数据目录用临时目录，跑完自动清理，不会污染当前工作区。
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_str().unwrap().to_string();

    let db = CoreTexDB::with_config(DbConfig::new(&path));
    db.init().await?;

    // 建一个 3 维、cosine 距离的集合（默认 brute_force 精确索引）。
    db.create_collection("fruits", 3, "cosine").await?;

    db.insert_vectors(
        "fruits",
        vec![
            ("apple".into(), vec![1.0, 0.0, 0.0], serde_json::json!({"kind": "fruit"})),
            ("banana".into(), vec![0.9, 0.1, 0.0], serde_json::json!({"kind": "fruit"})),
            ("car".into(), vec![0.0, 0.0, 1.0], serde_json::json!({"kind": "vehicle"})),
        ],
    )
    .await?;

    // 第四个参数是元数据过滤条件；None 表示不过滤。
    let hits = db.search("fruits", vec![1.0, 0.0, 0.0], 2, None).await?;

    println!("最接近 [1, 0, 0] 的 2 条：");
    for hit in hits {
        println!("  {:<8} distance={:.4}", hit.id, hit.distance);
    }

    println!("集合内向量总数：{}", db.get_vectors_count("fruits").await?);
    Ok(())
}
