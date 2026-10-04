//! C1 — 主从复制（真实 WAL 数据面）端到端行为。
//!
//! 同进程双实例（InProcessTransport 走与 HTTP 端点相同的载荷形态）：
//! 全量快照、增量续传、只读副本守卫、schema/删除传播、幂等重放、
//! 状态文件跨重启续传、无日志截断回退、快照-尾部接缝无缺口。

use std::path::PathBuf;
use std::sync::Arc;

use coretexdb::{CoreTexDB, DbConfig, InProcessTransport, ReplicaSync, SyncOutcome};

const DIM: usize = 4;
const QUERY: [f32; DIM] = [0.0, 1.0, 0.0, 0.0];

async fn open(path: &str, wal: bool) -> CoreTexDB {
    let mut config = DbConfig::new(path);
    config.wal_enabled = wal;
    let db = CoreTexDB::with_config(config);
    db.init().await.expect("init");
    db
}

/// `[i, 1, 0, 0]` 行，查询 `QUERY` 下按 `i` 升序——顺序可手算。
fn rows(start: usize, count: usize) -> Vec<(String, Vec<f32>, serde_json::Value)> {
    (start..start + count)
        .map(|i| {
            (
                format!("r{i}"),
                vec![i as f32, 1.0, 0.0, 0.0],
                serde_json::json!({"i": i}),
            )
        })
        .collect()
}

async fn search_ids(db: &CoreTexDB, collection: &str) -> Vec<String> {
    let hits = db.search(collection, QUERY.to_vec(), 32, None).await.unwrap();
    hits.into_iter().map(|h| h.id).collect()
}

/// 主库（开 WAL）+ 副本（开 WAL）+ 直连传输的同步器。
struct Pair {
    _primary_dir: tempfile::TempDir,
    _replica_dir: tempfile::TempDir,
    primary: Arc<CoreTexDB>,
    replica: Arc<CoreTexDB>,
    state_path: PathBuf,
    sync: ReplicaSync,
}

impl Pair {
    async fn new() -> Self {
        let primary_dir = tempfile::tempdir().unwrap();
        let replica_dir = tempfile::tempdir().unwrap();
        let primary = Arc::new(
            open(&primary_dir.path().to_str().unwrap().to_string(), true).await,
        );
        let replica = Arc::new(
            open(&replica_dir.path().to_str().unwrap().to_string(), true).await,
        );
        let state_path = replica_dir.path().join("replica_state.json");
        let sync = ReplicaSync::new(
            replica.clone(),
            Arc::new(InProcessTransport::new(primary.clone())),
            state_path.clone(),
        );
        Self {
            _primary_dir: primary_dir,
            _replica_dir: replica_dir,
            primary,
            replica,
            state_path,
            sync,
        }
    }
}

/// 1. 完整周期：全量 → 增量 → 追平。
#[tokio::test]
async fn full_sync_then_incremental_then_up_to_date() {
    let p = Pair::new().await;
    p.primary
        .create_collection_with_index("c", DIM, "euclidean", "brute_force")
        .await
        .unwrap();
    p.primary.insert_vectors("c", rows(0, 3)).await.unwrap();

    let out = p.sync.sync_once().await.unwrap();
    assert!(matches!(out, SyncOutcome::FullSync { lsn } if lsn > 0));
    assert_eq!(
        search_ids(&p.replica, "c").await,
        search_ids(&p.primary, "c").await
    );

    p.primary.insert_vectors("c", rows(3, 2)).await.unwrap();
    let out = p.sync.sync_once().await.unwrap();
    assert!(matches!(out, SyncOutcome::Incremental { applied, .. } if applied >= 1));
    assert_eq!(p.replica.data_manager.get_vectors_count("c").await.unwrap(), 5);
    assert_eq!(
        search_ids(&p.replica, "c").await,
        search_ids(&p.primary, "c").await
    );

    let out = p.sync.sync_once().await.unwrap();
    assert!(matches!(out, SyncOutcome::UpToDate { .. }));
}

/// 2. 只读副本：写与建集合被拒（read-only 错误），读不受影响。
#[tokio::test]
async fn replica_refuses_writes_but_serves_reads() {
    let p = Pair::new().await;
    p.primary
        .create_collection_with_index("c", DIM, "euclidean", "brute_force")
        .await
        .unwrap();
    p.primary.insert_vectors("c", rows(0, 2)).await.unwrap();
    p.sync.sync_once().await.unwrap();

    let err = p.replica.insert_vectors("c", rows(9, 1)).await.unwrap_err();
    assert!(err.to_string().contains("read-only"), "got: {err}");

    let err = p.replica.create_collection("x", DIM, "euclidean").await.unwrap_err();
    assert!(err.to_string().contains("read-only"), "got: {err}");

    let err = p.replica.delete_vectors("c", &["r0".into()]).await.unwrap_err();
    assert!(err.to_string().contains("read-only"), "got: {err}");

    assert_eq!(
        search_ids(&p.replica, "c").await,
        search_ids(&p.primary, "c").await
    );
}

/// 3. schema 与删除传播：新建集合、删行、删集合都经增量抵达副本。
#[tokio::test]
async fn schema_creates_and_deletes_propagate() {
    let p = Pair::new().await;
    p.primary
        .create_collection_with_index("a", DIM, "euclidean", "brute_force")
        .await
        .unwrap();
    p.primary.insert_vectors("a", rows(0, 2)).await.unwrap();
    p.sync.sync_once().await.unwrap();

    // 窗口：主新建集合 + 插行 → 增量携带 CreateCollection。
    p.primary
        .create_collection_with_index("b", DIM, "euclidean", "brute_force")
        .await
        .unwrap();
    p.primary.insert_vectors("b", rows(0, 1)).await.unwrap();
    let out = p.sync.sync_once().await.unwrap();
    assert!(matches!(out, SyncOutcome::Incremental { .. }));
    let collections = p.replica.list_collections().await.unwrap();
    assert!(collections.contains(&"b".to_string()), "got: {collections:?}");
    assert_eq!(p.replica.data_manager.get_vectors_count("b").await.unwrap(), 1);

    // 删行 → 增量 Delete。
    p.primary.delete_vectors("a", &["r0".into()]).await.unwrap();
    p.sync.sync_once().await.unwrap();
    assert_eq!(p.replica.data_manager.get_vectors_count("a").await.unwrap(), 1);

    // 删集合 → 增量 DeleteCollection（schema 与数据一起消失）。
    p.primary.data_manager.delete_collection("a").await.unwrap();
    p.sync.sync_once().await.unwrap();
    let collections = p.replica.list_collections().await.unwrap();
    assert!(!collections.contains(&"a".to_string()), "got: {collections:?}");
}

/// 4. 幂等重放：同一批条目应用两次，状态不变。
#[tokio::test]
async fn replaying_the_same_tail_is_idempotent() {
    let p = Pair::new().await;
    p.primary
        .create_collection_with_index("c", DIM, "euclidean", "brute_force")
        .await
        .unwrap();
    p.primary.insert_vectors("c", rows(0, 3)).await.unwrap();

    let (tail, truncated) = p
        .primary
        .data_manager
        .read_replication_entries(0)
        .await
        .unwrap();
    assert!(!truncated);
    assert!(!tail.is_empty());

    p.replica.data_manager.apply_replicated_entries(&tail).await.unwrap();
    let count = p.replica.data_manager.get_vectors_count("c").await.unwrap();
    let ids = search_ids(&p.replica, "c").await;

    // 重放：CreateCollection 跳过，行覆盖写为同值。
    p.replica.data_manager.apply_replicated_entries(&tail).await.unwrap();
    assert_eq!(p.replica.data_manager.get_vectors_count("c").await.unwrap(), count);
    assert_eq!(search_ids(&p.replica, "c").await, ids);
}

/// 5. 状态文件跨 syncer 重建续传：位置恢复，随后只走增量。
#[tokio::test]
async fn state_file_survives_replica_restarts() {
    let p = Pair::new().await;
    p.primary
        .create_collection_with_index("c", DIM, "euclidean", "brute_force")
        .await
        .unwrap();
    p.primary.insert_vectors("c", rows(0, 2)).await.unwrap();
    p.sync.sync_once().await.unwrap();
    let lsn = p.sync.last_lsn();
    assert!(lsn > 0);

    drop(p.sync);

    let sync2 = ReplicaSync::new(
        p.replica.clone(),
        Arc::new(InProcessTransport::new(p.primary.clone())),
        p.state_path.clone(),
    );
    assert_eq!(sync2.last_lsn(), lsn);

    p.primary.insert_vectors("c", rows(2, 1)).await.unwrap();
    let out = sync2.sync_once().await.unwrap();
    assert!(matches!(out, SyncOutcome::Incremental { .. }));
    assert_eq!(p.replica.data_manager.get_vectors_count("c").await.unwrap(), 3);
}

/// 6. 无日志的主：非零位置无法连续回答 → truncated（副本必须全量回退）。
#[tokio::test]
async fn unlogged_position_reports_truncation() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir.path().to_str().unwrap().to_string(), false).await;
    db.create_collection_with_index("c", DIM, "euclidean", "brute_force")
        .await
        .unwrap();
    db.insert_vectors("c", rows(0, 2)).await.unwrap();

    let (entries, truncated) = db.data_manager.read_replication_entries(1).await.unwrap();
    assert!(entries.is_empty());
    assert!(truncated);

    // 位置 0 是 "全部历史" 的语义：空日志也给合法的空答案。
    let (_, truncated) = db.data_manager.read_replication_entries(0).await.unwrap();
    assert!(!truncated);
}

/// 7. 副本本地 WAL：重启后靠本地回放恢复已应用状态。
#[tokio::test]
async fn replica_restart_recovers_from_its_own_wal() {
    let primary_dir = tempfile::tempdir().unwrap();
    let replica_dir = tempfile::tempdir().unwrap();
    let primary = Arc::new(open(&primary_dir.path().to_str().unwrap().to_string(), true).await);

    {
        let replica = Arc::new(open(&replica_dir.path().to_str().unwrap().to_string(), true).await);
        let sync = ReplicaSync::new(
            replica.clone(),
            Arc::new(InProcessTransport::new(primary.clone())),
            replica_dir.path().join("replica_state.json"),
        );
        primary
            .create_collection_with_index("c", DIM, "euclidean", "brute_force")
            .await
            .unwrap();
        primary.insert_vectors("c", rows(0, 3)).await.unwrap();
        sync.sync_once().await.unwrap();
        assert_eq!(replica.data_manager.get_vectors_count("c").await.unwrap(), 3);
        // sync + replica 作用域结束：句柄全部释放。
    }

    let replica2 = open(&replica_dir.path().to_str().unwrap().to_string(), true).await;
    assert_eq!(replica2.data_manager.get_vectors_count("c").await.unwrap(), 3);
    assert_eq!(search_ids(&replica2, "c").await, vec!["r0", "r1", "r2"]);
}

/// 8. 快照-尾部接缝无缺口：快照窗口内主库继续写（含新建集合），
///    位置在数据之前读取，窗口内的写全部由增量补上。
#[tokio::test]
async fn snapshot_and_tail_stitch_without_a_gap() {
    let p = Pair::new().await;
    p.primary
        .create_collection_with_index("c", DIM, "euclidean", "brute_force")
        .await
        .unwrap();
    p.primary.insert_vectors("c", rows(0, 3)).await.unwrap();

    // 快照（含位置），随后主在"窗口"里继续写。
    let snapshot = p.primary.data_manager.replication_snapshot().await;
    let lsn = snapshot.lsn;
    p.primary.insert_vectors("c", rows(3, 2)).await.unwrap();
    p.primary
        .create_collection_with_index("late", DIM, "euclidean", "brute_force")
        .await
        .unwrap();
    p.primary.insert_vectors("late", rows(0, 1)).await.unwrap();

    let (tail, truncated) = p
        .primary
        .data_manager
        .read_replication_entries(lsn)
        .await
        .unwrap();
    assert!(!truncated);
    assert!(!tail.is_empty());

    p.replica.data_manager.apply_replication_snapshot(&snapshot).await.unwrap();
    p.replica.data_manager.apply_replicated_entries(&tail).await.unwrap();

    assert_eq!(p.replica.data_manager.get_vectors_count("c").await.unwrap(), 5);
    let collections = p.replica.list_collections().await.unwrap();
    assert!(collections.contains(&"late".to_string()), "got: {collections:?}");
    assert_eq!(p.replica.data_manager.get_vectors_count("late").await.unwrap(), 1);
    assert_eq!(
        search_ids(&p.replica, "c").await,
        search_ids(&p.primary, "c").await
    );
}
