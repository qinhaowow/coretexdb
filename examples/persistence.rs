//! 持久化：写入数据与 ANN 索引 → 重新打开 → 结果仍然可查。
//!
//! `save_indexes()` 以「临时文件 → fsync → rename → 目录 fsync」的方式原子写入
//! 索引，并附带由存储内容计算出的校验和；重启时校验和匹配就直接**加载**索引，
//! 不匹配就从存储**重建**，因此陈旧索引永远不会被静默使用。
//!
//! ```bash
//! cargo run --example persistence
//! ```

use coretexdb::{CoreTexDB, DbConfig};

async fn open(path: &str) -> CoreTexDB {
    let db = CoreTexDB::with_config(DbConfig::new(path));
    db.init().await.expect("init");
    db
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_str().unwrap().to_string();

    // 第一段：写入数据，并把 hnsw 索引落盘。
    {
        let db = open(&path).await;
        db.create_collection_with_index("vectors", 8, "cosine", "hnsw").await?;

        let rows: Vec<(String, Vec<f32>, serde_json::Value)> = (0..64u32)
            .map(|i| {
                let mut v = vec![0.0; 8];
                v[0] = i as f32;
                v[1] = 1.0;
                (format!("v{i}"), v, serde_json::json!({}))
            })
            .collect();
        db.insert_vectors("vectors", rows).await?;

        let written = db.save_indexes().await?;
        println!("已写入索引文件：{written} 个");
        println!("索引目录：{}", db.index_dir().display());
    }

    // 第二段：重新打开。索引从磁盘加载，数据从存储恢复。
    let db = open(&path).await;
    println!("重启后向量总数：{}", db.get_vectors_count("vectors").await?);

    let mut query = vec![0.0; 8];
    query[0] = 7.0;
    query[1] = 1.0;

    let hits = db.search("vectors", query, 3, None).await?;
    println!("重启后搜索最近 3 条：");
    for hit in hits {
        println!("  {:<6} distance={:.4}", hit.id, hit.distance);
    }

    // 不 save_indexes() 也能用：重启时会从存储重建索引。
    println!("（注意：即使不调用 save_indexes()，重启时也会自动重建，只是多花一点时间）");

    Ok(())
}
