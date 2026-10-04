//! C2 — cluster routing, discovery and collection migration end to end.
//!
//! Three same-process nodes stand in for a cluster: routing answers MOVED
//! style questions, migration copies a collection and only then moves the
//! slot, a failing target leaves the route on the node that still has the
//! data, and an imported collection survives a restart of the target.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use coretexdb::{
    ClusterInfo, ClusterMigrator, ClusterNodeHealth, ClusterRouter, ClusterTransport, CoreTexDB,
    CollectionChunk, DbConfig, LocalNodeTransport, NodeInfo, ReplicationStatus,
};

const DIM: usize = 4;
const QUERY: [f32; DIM] = [0.0, 1.0, 0.0, 0.0];

async fn open_node(path: &str) -> Arc<CoreTexDB> {
    let mut config = DbConfig::new(path);
    config.wal_enabled = true;
    let db = CoreTexDB::with_config(config);
    db.init().await.expect("init");
    Arc::new(db)
}

/// Three nodes on disk plus their transports, keyed by node id.
async fn three_node_cluster() -> (Vec<tempfile::TempDir>, HashMap<String, Arc<CoreTexDB>>, Arc<ClusterRouter>, HashMap<String, Arc<dyn ClusterTransport>>) {
    let mut dirs = Vec::new();
    let mut dbs: HashMap<String, Arc<CoreTexDB>> = HashMap::new();
    let mut transports: HashMap<String, Arc<dyn ClusterTransport>> = HashMap::new();
    let mut nodes = Vec::new();

    for id in ["n1", "n2", "n3"] {
        let dir = tempfile::tempdir().unwrap();
        let db = open_node(&dir.path().to_str().unwrap().to_string()).await;
        nodes.push(NodeInfo::new(id, format!("http://127.0.0.1:700{}", &id[1..])));
        transports.insert(id.to_string(), Arc::new(LocalNodeTransport::new(db.clone())));
        dbs.insert(id.to_string(), db);
        dirs.push(dir);
    }

    let router = Arc::new(ClusterRouter::new(nodes).unwrap());
    (dirs, dbs, router, transports)
}

async fn seed(db: &CoreTexDB, collection: &str, rows: usize, metric: &str, index: &str) {
    db.create_collection_with_index(collection, DIM, metric, index)
        .await
        .unwrap();
    let vectors: Vec<(String, Vec<f32>, serde_json::Value)> = (0..rows)
        .map(|i| {
            (
                format!("r{i}"),
                vec![i as f32, 1.0, 0.0, 0.0],
                serde_json::json!({"i": i}),
            )
        })
        .collect();
    db.insert_vectors(collection, vectors).await.unwrap();
}

async fn search_ids(db: &CoreTexDB, collection: &str) -> Vec<String> {
    let hits = db.search(collection, QUERY.to_vec(), 32, None).await.unwrap();
    hits.into_iter().map(|h| h.id).collect()
}

/// A node that fails every call — stands in for a target that is down or
/// refusing writes.
struct BrokenTransport;

#[async_trait]
impl ClusterTransport for BrokenTransport {
    async fn status(&self) -> coretexdb::Result<ReplicationStatus> {
        Err(coretexdb::CoreTexError::Other("node unreachable".to_string()))
    }

    async fn export_collection(&self, _name: &str) -> coretexdb::Result<Option<CollectionChunk>> {
        Err(coretexdb::CoreTexError::Other("node unreachable".to_string()))
    }

    async fn import_collection(&self, _chunk: &CollectionChunk) -> coretexdb::Result<usize> {
        Err(coretexdb::CoreTexError::Other("node is read-only".to_string()))
    }
}

/// 1. 路由：分配后命中正确节点，未分配报 MOVED 语义（含 slot 号）。
#[tokio::test]
async fn routing_points_collections_at_their_owner() {
    let (_dirs, dbs, router, _transports) = three_node_cluster().await;

    // Nothing routed yet.
    let err = router.lookup("docs").await.unwrap_err().to_string();
    assert!(err.contains("no node owns collection 'docs'"), "got: {err}");

    router.assign_collection("docs", "n2").await.unwrap();
    assert_eq!(router.lookup("docs").await.unwrap().id, "n2");
    assert_eq!(router.collections_of("n2").await.unwrap(), vec!["docs"]);

    // The data lives where the route says it does.
    seed(&dbs["n2"], "docs", 3, "euclidean", "brute_force").await;
    let owner = router.lookup("docs").await.unwrap();
    assert_eq!(search_ids(&dbs[&owner.id], "docs").await.len(), 3);
    assert!(dbs["n1"].list_collections().await.unwrap().is_empty());
}

/// 2. 导出/导入保真：schema 逐字保留（维度/度量/索引类型），行可查。
#[tokio::test]
async fn export_and_import_preserve_the_schema_verbatim() {
    let (_dirs, dbs, _router, transports) = three_node_cluster().await;
    seed(&dbs["n1"], "docs", 4, "euclidean", "hnsw").await;

    let chunk = transports["n1"]
        .export_collection("docs")
        .await
        .unwrap()
        .expect("collection exists on the source");
    assert_eq!(chunk.len(), 4);
    assert_eq!(chunk.schema.dimension, DIM);
    assert!(chunk.lsn > 0, "export records the source log position");

    let imported = transports["n2"].import_collection(&chunk).await.unwrap();
    assert_eq!(imported, 4);

    // Schema arrived unchanged — a default metric or index would silently
    // change what queries mean on the target.
    let schema = dbs["n2"].data_manager.get_collection("docs").await.unwrap();
    assert_eq!(schema.dimension, DIM);
    assert_eq!(
        format!("{:?}", schema.distance_metric),
        format!("{:?}", dbs["n1"].data_manager.get_collection("docs").await.unwrap().distance_metric)
    );
    assert_eq!(schema.indexes.len(), 1);
    assert_eq!(
        format!("{:?}", schema.indexes[0].index_type),
        format!("{:?}", dbs["n1"].data_manager.get_collection("docs").await.unwrap().indexes[0].index_type)
    );

    // Rows are searchable on the target, in the same order.
    assert_eq!(search_ids(&dbs["n2"], "docs").await, vec!["r0", "r1", "r2", "r3"]);

    // A collection the source does not have exports as None, not an error.
    assert!(transports["n1"].export_collection("absent").await.unwrap().is_none());
}

/// 3. 导入幂等：重导覆盖而非叠加，旧行不残留。
#[tokio::test]
async fn reimporting_replaces_rather_than_accumulates() {
    let (_dirs, dbs, _router, transports) = three_node_cluster().await;
    seed(&dbs["n1"], "docs", 3, "euclidean", "brute_force").await;
    let full = transports["n1"].export_collection("docs").await.unwrap().unwrap();

    // A smaller, different chunk lands on the target first.
    let mut smaller = full.clone();
    smaller.records.retain(|id, _| id != "r2");
    smaller.schema.name = "docs".to_string();
    transports["n2"].import_collection(&smaller).await.unwrap();
    assert_eq!(dbs["n2"].data_manager.get_vectors_count("docs").await.unwrap(), 2);

    // Re-importing the full chunk restores r2 and does not duplicate rows.
    transports["n2"].import_collection(&full).await.unwrap();
    assert_eq!(dbs["n2"].data_manager.get_vectors_count("docs").await.unwrap(), 3);
    assert_eq!(search_ids(&dbs["n2"], "docs").await, vec!["r0", "r1", "r2"]);
}

/// 4. 迁移：先搬数据，后切路由；源保留副本（清理是显式后续动作）。
#[tokio::test]
async fn migration_moves_data_then_the_route() {
    let (_dirs, dbs, router, transports) = three_node_cluster().await;
    seed(&dbs["n1"], "docs", 5, "euclidean", "brute_force").await;
    router.assign_collection("docs", "n1").await.unwrap();

    let migrator = ClusterMigrator::new(router.clone(), transports);
    let outcome = migrator.migrate("docs", "n1", "n3").await.unwrap();

    assert_eq!(outcome.collection, "docs");
    assert_eq!(outcome.records, 5);
    assert_eq!(outcome.slot, coretexdb::slot_of("docs"));
    assert!(outcome.source_retained);

    // Route moved, target serves the data, source untouched.
    assert_eq!(router.lookup("docs").await.unwrap().id, "n3");
    assert_eq!(dbs["n3"].data_manager.get_vectors_count("docs").await.unwrap(), 5);
    assert_eq!(search_ids(&dbs["n3"], "docs").await, vec!["r0", "r1", "r2", "r3", "r4"]);
    assert_eq!(dbs["n1"].data_manager.get_vectors_count("docs").await.unwrap(), 5);
    assert_eq!(router.collections_of("n3").await.unwrap(), vec!["docs"]);
    assert!(router.collections_of("n1").await.unwrap().is_empty());
}

/// 5. 目标故障：导入失败则路由不动——客户端继续命中还有数据的那台。
#[tokio::test]
async fn failed_target_leaves_the_route_alone() {
    let (_dirs, dbs, router, mut transports) = three_node_cluster().await;
    seed(&dbs["n1"], "docs", 2, "euclidean", "brute_force").await;
    router.assign_collection("docs", "n1").await.unwrap();

    transports.insert("broken".to_string(), Arc::new(BrokenTransport));
    let migrator = ClusterMigrator::new(router.clone(), transports);

    let err = migrator.migrate("docs", "n1", "broken").await.unwrap_err();
    assert!(err.to_string().contains("read-only"), "got: {err}");

    // Route and data unchanged.
    assert_eq!(router.lookup("docs").await.unwrap().id, "n1");
    assert!(dbs["n2"].list_collections().await.unwrap().is_empty());

    // A collection the source does not have fails before touching anything.
    let err = migrator.migrate("absent", "n1", "n2").await.unwrap_err();
    assert!(err.to_string().contains("not found on node 'n1'"), "got: {err}");
    assert_eq!(router.lookup("docs").await.unwrap().id, "n1");

    // Migrating to the node that already owns it is refused.
    let err = migrator.migrate("docs", "n1", "n1").await.unwrap_err();
    assert!(err.to_string().contains("both 'n1'"), "got: {err}");
}

/// 6. 节点发现：探测报告存活与状态；没有 transport 的节点报 down 而非跳过。
#[tokio::test]
async fn probing_reports_health_and_status_per_node() {
    let (_dirs, dbs, router, mut transports) = three_node_cluster().await;
    seed(&dbs["n1"], "a", 3, "euclidean", "brute_force").await;
    seed(&dbs["n2"], "b", 7, "euclidean", "brute_force").await;

    // A fourth node exists in the directory but has no transport.
    let health_before: Vec<ClusterNodeHealth> = router.probe_all(&transports).await;
    assert_eq!(health_before.len(), 3);
    assert!(health_before.iter().all(|h| h.alive));
    let n1 = health_before.iter().find(|h| h.node.id == "n1").unwrap();
    assert_eq!(n1.status.as_ref().unwrap().collections, 1);
    assert_eq!(n1.status.as_ref().unwrap().records, 3);
    let n2 = health_before.iter().find(|h| h.node.id == "n2").unwrap();
    assert_eq!(n2.status.as_ref().unwrap().records, 7);

    transports.clear();
    let health_after = router.probe_all(&transports).await;
    assert_eq!(health_after.len(), 3);
    assert!(health_after.iter().all(|h| !h.alive));
    assert!(health_after
        .iter()
        .all(|h| h.error.as_ref().unwrap().contains("no transport")));
}

/// 7. 迁移落地的目标节点重启后仍在：import 持久化 manifest + storage。
#[tokio::test]
async fn migrated_collection_survives_target_restart() {
    let primary_dir = tempfile::tempdir().unwrap();
    let target_dir = tempfile::tempdir().unwrap();
    let source = open_node(&primary_dir.path().to_str().unwrap().to_string()).await;
    seed(&source, "docs", 4, "euclidean", "brute_force").await;

    let chunk = LocalNodeTransport::new(source.clone())
        .export_collection("docs")
        .await
        .unwrap()
        .unwrap();
    {
        let target = open_node(&target_dir.path().to_str().unwrap().to_string()).await;
        LocalNodeTransport::new(target.clone())
            .import_collection(&chunk)
            .await
            .unwrap();
        assert_eq!(target.data_manager.get_vectors_count("docs").await.unwrap(), 4);
    }

    // Restart: schemas come from the manifest the transport persisted, rows
    // from storage.
    let target2 = open_node(&target_dir.path().to_str().unwrap().to_string()).await;
    assert_eq!(target2.data_manager.get_vectors_count("docs").await.unwrap(), 4);
    assert_eq!(search_ids(&target2, "docs").await, vec!["r0", "r1", "r2", "r3"]);
}

/// 8. 集群概览：槽位分布按节点统计，未分配槽可见。
#[tokio::test]
async fn cluster_info_summarises_slots_per_node() {
    let (_dirs, _dbs, router, _transports) = three_node_cluster().await;
    router.assign_collection("alpha", "n1").await.unwrap();
    router.assign_collection("beta", "n1").await.unwrap();
    router.assign_collection("gamma", "n3").await.unwrap();
    router.assign_range("n2", 0, 99).await.unwrap();

    let info: ClusterInfo = router.cluster_info().await;
    let n1 = info.nodes.iter().find(|n| n.node.id == "n1").unwrap();
    assert_eq!(n1.slots, 2, "two collections, two distinct slots expected");
    assert_eq!(n1.collections, 2);
    let n2 = info.nodes.iter().find(|n| n.node.id == "n2").unwrap();
    assert_eq!(n2.slots, 100, "range assignment owns 100 slots regardless of names");
    assert_eq!(n2.collections, 0);
    let n3 = info.nodes.iter().find(|n| n.node.id == "n3").unwrap();
    assert_eq!(n3.slots, 1);
    assert_eq!(n3.collections, 1);
    assert_eq!(info.unassigned_slots, coretexdb::SLOT_COUNT - 103);
}