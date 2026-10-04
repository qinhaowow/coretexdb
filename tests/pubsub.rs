//! C3 — Pub/Sub: the write path publishes, and the WebSocket surface finally
//! has something to push.
//!
//! Events are asserted by type and payload, failed writes must stay silent,
//! delete lists only the ids that actually existed, every subscriber sees
//! every event, a node without a bus behaves exactly as before, and the
//! WebSocket bridge delivers to subscribers of that collection only.

use std::sync::Arc;

use coretexdb::coretex_pubsub::DataChangeEvent;
use coretexdb::coretex_websocket::{WebSocketConfig, WebSocketMessage, WebSocketServer};
use coretexdb::{CoreTexDB, DbConfig, EventBus};

const DIM: usize = 4;

async fn open() -> (tempfile::TempDir, CoreTexDB) {
    let dir = tempfile::tempdir().unwrap();
    let db = CoreTexDB::with_config(DbConfig::new(&dir.path().to_str().unwrap().to_string()));
    db.init().await.expect("init");
    (dir, db)
}

/// A database with a bus attached, plus a subscriber.
async fn node_with_bus() -> (
    tempfile::TempDir,
    CoreTexDB,
    Arc<EventBus>,
    coretexdb::EventReceiver,
) {
    let (dir, db) = open().await;
    let bus = Arc::new(EventBus::new(64));
    db.data_manager.set_event_bus(bus.clone()).unwrap();
    let rx = bus.subscribe();
    (dir, db, bus, rx)
}

fn vec_of(i: f32) -> Vec<f32> {
    vec![i, 1.0, 0.0, 0.0]
}

async fn next_event(rx: &mut coretexdb::EventReceiver) -> DataChangeEvent {
    tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
        .await
        .expect("event within timeout")
        .expect("bus open")
}

fn assert_no_event(rx: &mut coretexdb::EventReceiver) {
    // The bus is synchronous under the hood: a short sleep is enough, and a
    // timeout here would only slow the suite down.
    std::thread::sleep(std::time::Duration::from_millis(50));
    match rx.try_recv() {
        Err(tokio::sync::broadcast::error::TryRecvError::Empty) => {}
        other => panic!("expected no event, got {other:?}"),
    }
}

/// 1. 每类成功写入都发出带类型的��件；payload 与实际变更一致。
#[tokio::test]
async fn writes_publish_typed_events() {
    let (_dir, db, _bus, mut rx) = node_with_bus().await;

    db.create_collection_with_index("docs", DIM, "euclidean", "brute_force")
        .await
        .unwrap();
    let event = next_event(&mut rx).await;
    assert_eq!(event.collection, "docs");
    assert_eq!(event.event_type, "create_collection");
    assert!(event.ids.is_empty());

    db.insert_vectors(
        "docs",
        vec![
            ("a".into(), vec_of(0.0), serde_json::json!({})),
            ("b".into(), vec_of(1.0), serde_json::json!({})),
        ],
    )
    .await
    .unwrap();
    let event = next_event(&mut rx).await;
    assert_eq!(event.event_type, "insert");
    assert_eq!(event.ids, vec!["a", "b"]);

    db.data_manager
        .update_vector("docs", "a", vec_of(2.0), Some(serde_json::json!({"v": 1})))
        .await
        .unwrap();
    let event = next_event(&mut rx).await;
    assert_eq!(event.event_type, "update");
    assert_eq!(event.ids, vec!["a"]);

    db.delete_vectors("docs", &["b".to_string()]).await.unwrap();
    let event = next_event(&mut rx).await;
    assert_eq!(event.event_type, "delete");
    assert_eq!(event.ids, vec!["b"]);

    // Clear is a delete underneath: subscribers learn about it as a delete of
    // the rows that were there, not as a mystery event type.
    db.data_manager.clear_collection("docs").await.unwrap();
    let event = next_event(&mut rx).await;
    assert_eq!(event.event_type, "delete");
    assert_eq!(event.ids, vec!["a"]);

    db.data_manager.rename_collection("docs", "papers").await.unwrap();
    let event = next_event(&mut rx).await;
    assert_eq!(event.collection, "papers");
    assert_eq!(event.event_type, "rename_collection");
    assert_eq!(
        event.metadata.as_ref().unwrap().get("from").unwrap(),
        &serde_json::json!("docs")
    );

    db.data_manager.delete_collection("papers").await.unwrap();
    let event = next_event(&mut rx).await;
    assert_eq!(event.event_type, "delete_collection");
    assert_eq!(event.collection, "papers");
}

/// 2. 失败的写不发事件：只有真正落地的变更才该被广播。
#[tokio::test]
async fn failed_writes_publish_nothing() {
    let (_dir, db, _bus, mut rx) = node_with_bus().await;
    db.create_collection_with_index("docs", DIM, "euclidean", "brute_force")
        .await
        .unwrap();
    let _ = next_event(&mut rx).await; // create_collection

    // Wrong dimension: rejected before any change.
    assert!(db
        .insert_vectors("docs", vec![("a".into(), vec![1.0, 2.0], serde_json::json!({}))])
        .await
        .is_err());
    // Missing collection.
    assert!(db.delete_vectors("absent", &["a".to_string()]).await.is_err());
    // Duplicate collection.
    assert!(db
        .create_collection_with_index("docs", DIM, "euclidean", "brute_force")
        .await
        .is_err());

    assert_no_event(&mut rx);
}

/// 3. delete 事件只列出真正删掉的 id。
#[tokio::test]
async fn delete_event_lists_only_ids_that_existed() {
    let (_dir, db, _bus, mut rx) = node_with_bus().await;
    db.create_collection_with_index("docs", DIM, "euclidean", "brute_force")
        .await
        .unwrap();
    db.insert_vectors(
        "docs",
        vec![
            ("a".into(), vec_of(0.0), serde_json::json!({})),
            ("b".into(), vec_of(1.0), serde_json::json!({})),
        ],
    )
    .await
    .unwrap();
    let _ = next_event(&mut rx).await; // create_collection
    let _ = next_event(&mut rx).await; // insert

    let deleted = db
        .delete_vectors("docs", &["ghost".to_string(), "a".to_string()])
        .await
        .unwrap();
    assert_eq!(deleted, 1);

    let event = next_event(&mut rx).await;
    assert_eq!(event.event_type, "delete");
    assert_eq!(event.ids, vec!["a"], "a ghost id must not be announced");
}

/// 4. 每个订阅者都收到全量事件流（过滤是订阅者自己的事）。
#[tokio::test]
async fn every_subscriber_receives() {
    let (_dir, db, bus, _rx) = node_with_bus().await;
    let mut first = bus.subscribe();
    let mut second = bus.subscribe();
    assert_eq!(bus.subscriber_count(), 3);

    db.create_collection_with_index("docs", DIM, "euclidean", "brute_force")
        .await
        .unwrap();
    db.insert_vectors("docs", vec![("a".into(), vec_of(0.0), serde_json::json!({}))])
        .await
        .unwrap();

    for rx in [&mut first, &mut second] {
        assert_eq!(next_event(rx).await.event_type, "create_collection");
        assert_eq!(next_event(rx).await.event_type, "insert");
    }
}

/// 5. 没挂总线的节点一切照旧：发布是可选增强，不是前提。
#[tokio::test]
async fn writes_work_without_a_bus() {
    let (_dir, db) = open().await;
    db.create_collection_with_index("docs", DIM, "euclidean", "brute_force")
        .await
        .unwrap();
    db.insert_vectors("docs", vec![("a".into(), vec_of(0.0), serde_json::json!({}))])
        .await
        .unwrap();
    db.delete_vectors("docs", &["a".to_string()]).await.unwrap();
    db.data_manager.delete_collection("docs").await.unwrap();
    assert!(db.data_manager.event_bus().is_none());
}

/// 6. WebSocket 桥：写路径的事件到达订阅了该集合的连接，其他集合不打扰。
#[tokio::test]
async fn websocket_bridge_delivers_to_subscribers() {
    let (_dir, db, bus, _rx) = node_with_bus().await;

    let server = Arc::new(WebSocketServer::new(WebSocketConfig::default()));
    server.subscribe_connection("conn-1", "docs").await;
    let _bridge = server.attach_event_bus(bus);
    let mut messages = server.event_receiver();

    // A collection nobody subscribed to: the bridge filters it out.
    db.create_collection_with_index("quiet", DIM, "euclidean", "brute_force")
        .await
        .unwrap();

    db.create_collection_with_index("docs", DIM, "euclidean", "brute_force")
        .await
        .unwrap();
    db.insert_vectors("docs", vec![("a".into(), vec_of(0.0), serde_json::json!({}))])
        .await
        .unwrap();

    // The bridge task needs a moment to pick the events off the bus.
    let mut seen = Vec::new();
    while seen.len() < 2 {
        let msg = tokio::time::timeout(std::time::Duration::from_secs(5), messages.recv())
            .await
            .expect("bridge message within timeout")
            .expect("server channel open");
        if let WebSocketMessage::DataChange(event) = msg {
            assert_eq!(event.collection, "docs", "unrelated collections must not be pushed");
            seen.push(event.event_type);
        }
    }
    assert_eq!(seen, vec!["create_collection", "insert"]);
}