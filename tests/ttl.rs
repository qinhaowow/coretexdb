//! End-to-end tests for vector TTL: expired rows are purged from storage,
//! memory and the index; a removed TTL keeps the row.

use coretexdb::{CoreTexDB, DbConfig};

async fn open(path: &str) -> CoreTexDB {
    let db = CoreTexDB::with_config(DbConfig::new(path));
    db.init().await.expect("init");
    db
}

async fn seed(db: &CoreTexDB) {
    db.create_collection("c", 3, "cosine").await.unwrap();
    db.insert_vectors(
        "c",
        vec![
            ("a".into(), vec![1.0, 0.0, 0.0], serde_json::json!({})),
            ("b".into(), vec![0.0, 1.0, 0.0], serde_json::json!({})),
        ],
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn expired_ttl_purges_vector_everywhere() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();

    {
        let db = open(&path).await;
        seed(&db).await;

        // A zero-second TTL is already expired.
        db.set_vector_ttl("c", "a", 0).await.unwrap();
        assert_eq!(db.purge_expired().await.unwrap(), 1, "exactly one expired");

        assert_eq!(db.get_vectors_count("c").await.unwrap(), 1);
        assert!(db.get_vector("c", "a").await.unwrap().is_none());
        assert!(db.get_vector("c", "b").await.unwrap().is_some());

        // The purge must reach the index too, not just the memory map.
        let hits = db.search("c", vec![1.0, 0.0, 0.0], 5, None).await.unwrap();
        assert!(
            hits.iter().all(|h| h.id != "a"),
            "purged vector still returned by search: {:?}",
            hits.iter().map(|h| &h.id).collect::<Vec<_>>()
        );
    }

    // Deletion is durable: a restart does not resurrect the row.
    let db = open(&path).await;
    assert_eq!(db.get_vectors_count("c").await.unwrap(), 1);
    assert!(db.get_vector("c", "a").await.unwrap().is_none());
}

#[tokio::test]
async fn removing_a_ttl_keeps_the_vector() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();

    let db = open(&path).await;
    seed(&db).await;

    db.set_vector_ttl("c", "a", 0).await.unwrap();
    db.remove_vector_ttl("c", "a").await.unwrap();

    assert_eq!(db.purge_expired().await.unwrap(), 0, "nothing should expire");
    assert!(db.get_vector("c", "a").await.unwrap().is_some());
    assert_eq!(db.get_vectors_count("c").await.unwrap(), 2);
}
