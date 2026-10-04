//! C5 — command statistics, slow queries and INFO.
//!
//! The counters have to be right before anything else matters: a statistics
//! feature that reports the wrong number is worse than none. Then the slow
//! log has to actually capture slow operations, INFO has to describe the
//! node honestly (including that a replica is read-only and that a cluster
//! has slots), and an unobserved database must keep working.

use std::sync::Arc;

use coretexdb::coretex_cluster::{ClusterInfo, ClusterRouter, NodeInfo};
use coretexdb::coretex_monitoring::SlowQueryConfig;
use coretexdb::{collect_info, CoreTexDB, DbConfig, OperationObserver};

const DIM: usize = 4;
const QUERY: [f32; DIM] = [0.0, 1.0, 0.0, 0.0];

async fn seeded() -> (tempfile::TempDir, CoreTexDB) {
    let dir = tempfile::tempdir().unwrap();
    let db = CoreTexDB::with_config(DbConfig::new(&dir.path().to_str().unwrap().to_string()));
    db.init().await.expect("init");
    db.create_collection_with_index("docs", DIM, "euclidean", "brute_force")
        .await
        .unwrap();
    db.insert_vectors(
        "docs",
        vec![
            ("a".into(), vec![0.0, 1.0, 0.0, 0.0], serde_json::json!({})),
            ("b".into(), vec![1.0, 1.0, 0.0, 0.0], serde_json::json!({})),
        ],
    )
    .await
    .unwrap();
    (dir, db)
}

fn observer() -> Arc<OperationObserver> {
    Arc::new(OperationObserver::new())
}

fn slow_observer(threshold_ms: u64) -> Arc<OperationObserver> {
    let mut config = SlowQueryConfig::default();
    config.enabled = true;
    config.slow_threshold_ms = threshold_ms;
    config.log_path = std::env::temp_dir()
        .join("coretex-slow-query-test.log")
        .to_string_lossy()
        .to_string();
    Arc::new(OperationObserver::new().with_slow_query_logging(config))
}

/// 1. 计数准确：每个入口点各自计数，总数与明细一致。
#[tokio::test]
async fn commands_are_counted_across_entry_points() {
    let (_dir, db) = seeded().await;
    let obs = observer();
    db.set_operation_observer(obs.clone()).unwrap();

    for _ in 0..3 {
        db.search("docs", QUERY.to_vec(), 2, None).await.unwrap();
    }
    db.get_vector("docs", "a").await.unwrap();
    db.delete_vectors("docs", &["b".to_string()]).await.unwrap();
    db.insert_vectors("docs", vec![("c".into(), vec![2.0, 1.0, 0.0, 0.0], serde_json::json!({}))])
        .await
        .unwrap();

    let stats = obs.snapshot();
    assert_eq!(stats.total_calls, 6);
    assert_eq!(stats.commands["search"].calls, 3);
    assert_eq!(stats.commands["get_vector"].calls, 1);
    assert_eq!(stats.commands["delete_vectors"].calls, 1);
    assert_eq!(stats.commands["insert_vectors"].calls, 1);
    assert!(stats.commands.values().all(|s| s.errors == 0));
    assert!(stats.commands["search"].mean_ms() >= 0.0);
}

/// 2. 失败与成功分开计数。
#[tokio::test]
async fn failed_operations_count_as_errors() {
    let (_dir, db) = seeded().await;
    let obs = observer();
    db.set_operation_observer(obs.clone()).unwrap();

    assert!(db.search("absent", QUERY.to_vec(), 2, None).await.is_err());
    db.search("docs", QUERY.to_vec(), 2, None).await.unwrap();

    let stats = obs.snapshot();
    assert_eq!(stats.commands["search"].errors, 1);
    assert_eq!(stats.commands["search"].calls, 1);
    assert_eq!(stats.total_calls, 2);
}

/// 3. 慢查询真的进日志，且带上可定位的信息。
#[tokio::test]
async fn slow_queries_are_recorded_with_context() {
    let (_dir, db) = seeded().await;
    let obs = slow_observer(0); // everything counts as slow
    db.set_operation_observer(obs.clone()).unwrap();

    db.search("docs", QUERY.to_vec(), 5, None).await.unwrap();

    let logger = obs.slow_queries().expect("logger attached");
    let entries = logger.get_slow_queries().await;
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].query_type, "search");
    assert_eq!(entries[0].collection, "docs");
    assert!(entries[0].query_params.contains("k=5"), "params: {}", entries[0].query_params);
    assert!(entries[0].duration_ms >= 0.0);

    // The INFO surface reports it.
    let info = collect_info(&db, Some(&obs), None).await;
    assert_eq!(info.slow_query_count, 1);
}

/// 4. INFO 报告节点真相：版本、键空间、统计。
#[tokio::test]
async fn info_reports_server_keyspace_and_stats() {
    let (_dir, db) = seeded().await;
    let obs = observer();
    db.set_operation_observer(obs.clone()).unwrap();
    db.search("docs", QUERY.to_vec(), 2, None).await.unwrap();

    let info = collect_info(&db, Some(&obs), None).await;
    assert_eq!(info.version, env!("CARGO_PKG_VERSION"));
    assert_eq!(info.mode, "standalone");
    assert!(!info.read_only);
    assert_eq!(info.total_vectors, 2);
    assert_eq!(info.collections.len(), 1);
    assert_eq!(info.collections[0].name, "docs");
    assert_eq!(info.collections[0].vectors, 2);
    assert_eq!(info.stats.total_calls, 1);

    // Without an observer every counter reads zero rather than failing.
    let bare = collect_info(&db, None, None).await;
    assert_eq!(bare.stats.total_calls, 0);
    assert_eq!(bare.total_vectors, 2);
}

/// 5. 文本渲染含各段；集群段按需出现。
#[tokio::test]
async fn info_text_render_and_cluster_section() {
    let (_dir, db) = seeded().await;

    let router = Arc::new(
        ClusterRouter::new(vec![
            NodeInfo::new("n1", "http://a"),
            NodeInfo::new("n2", "http://b"),
        ])
        .unwrap(),
    );
    router.assign_collection("docs", "n1").await.unwrap();

    let plain = collect_info(&db, None, None).await.to_text();
    assert!(plain.contains("# Server"));
    assert!(plain.contains("# Keyspace"));
    assert!(plain.contains("coretexdb_collection:docs:vectors=2"));
    assert!(!plain.contains("# Cluster"), "no cluster info, no cluster section");

    let clustered = collect_info(&db, None, Some(router.cluster_info().await)).await;
    let text = clustered.to_text();
    assert!(text.contains("# Cluster"));
    assert!(text.contains("cluster_node:n1:slots=1"));
    assert!(text.contains("cluster_unassigned_slots:16383"));
}

/// 6. 副本的 INFO 说实话：只读标志来自复制守卫，不是配置里的猜测。
#[tokio::test]
async fn info_marks_a_replica_read_only() {
    let (_dir, db) = seeded().await;
    let obs = observer();
    db.set_operation_observer(obs.clone()).unwrap();

    assert!(!collect_info(&db, Some(&obs), None).await.read_only);

    db.data_manager.set_read_only(true);
    let info = collect_info(&db, Some(&obs), None).await;
    assert!(info.read_only);
    assert!(info.to_text().contains("read_only:true"));
}