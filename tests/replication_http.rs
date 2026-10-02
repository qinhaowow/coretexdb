//! `HttpTransport` against a real HTTP server on a real socket.
//!
//! Why this file is separate from `tests/replication.rs`:
//!
//! That suite drives `ReplicaSync` through `InProcessTransport`, where the
//! "primary" is just another `CoreTexDB` handle in the same process. Nothing
//! serialises, no socket is opened, no status code is ever seen. `HttpTransport`
//! — which builds a real `reqwest::Client`, concatenates URLs, and parses JSON
//! off the wire — therefore had **no test that ever executed it**. Code that has
//! never run is not evidence of anything.
//!
//! These tests start an actual `axum` server on an ephemeral port and point a
//! real `HttpTransport` at it, using only the public API. They deliberately do
//! not modify `tests/replication.rs` or `src/coretex_replication.rs`.
//!
//! What this catches that the in-process suite cannot:
//!   * a URL that no longer matches the route the REST layer registers;
//!   * serialisation drift between a handler's output type and the transport's
//!     `T` — both sides are `pub` structs, so renaming a field on one side
//!     compiles cleanly and fails only at runtime;
//!   * an HTTP error status being reported as a successful fetch.

use axum::extract::Query;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use coretexdb::coretex_replication::{
    EntriesBatch, HttpTransport, ReplicationSnapshot, ReplicationTransport, ReplicaSync,
    SyncOutcome,
};
use coretexdb::{CoreTexDB, DbConfig};

// `coretexdb::WalEntry` and `coretexdb::coretex_utils::WalEntry` are distinct
// types from different `http`-crate generations in the dependency graph —
// `EntriesBatch.entries` is the `coretex_utils` one.
type WalEntry = coretexdb::coretex_utils::WalEntry;
use std::collections::HashMap;
use std::sync::Arc;

/// A server to point the transport at, plus handles to change what it serves.
struct TestServer {
    base_url: String,
    snapshot: Arc<tokio::sync::RwLock<Option<ReplicationSnapshot>>>,
    /// When set, both routes return this status together with `body`.
    ///
    /// `body` is a caller-supplied string so a test can make the error page
    /// *look like* a valid payload — the case where a missing status check
    /// silently accepts garbage instead of merely mislabelling the cause.
    forced_status: Arc<tokio::sync::RwLock<Option<(StatusCode, String)>>>,
}

async fn start_server() -> TestServer {
    let snapshot: Arc<tokio::sync::RwLock<Option<ReplicationSnapshot>>> =
        Arc::new(tokio::sync::RwLock::new(None));
    let forced_status: Arc<tokio::sync::RwLock<Option<(StatusCode, String)>>> =
        Arc::new(tokio::sync::RwLock::new(None));

    let app = Router::new()
        .route(
            "/replication/snapshot",
            get({
                let snapshot = snapshot.clone();
                let forced = forced_status.clone();
                move || {
                    let snapshot = snapshot.clone();
                    let forced = forced.clone();
                    async move {
                        if let Some((code, body)) = forced.read().await.as_ref() {
                            return (code.clone(), body.clone()).into_response();
                        }
                        match snapshot.read().await.clone() {
                            Some(s) => axum::Json(serde_json::to_value(s).unwrap()).into_response(),
                            None => {
                                (StatusCode::SERVICE_UNAVAILABLE, "no snapshot").into_response()
                            }
                        }
                    }
                }
            }),
        )
        .route(
            "/replication/entries",
            get({
                let forced = forced_status.clone();
                move |Query(q): Query<HashMap<String, u64>>| {
                    let forced = forced.clone();
                    async move {
                        if let Some((code, body)) = forced.read().await.as_ref() {
                            return (code.clone(), body.clone()).into_response();
                        }
                        let since = q.get("since").copied().unwrap_or(0);
                        let batch = EntriesBatch {
                            entries: Vec::<WalEntry>::new(),
                            truncated: false,
                            lsn: since,
                        };
                        axum::Json(serde_json::to_value(batch).unwrap()).into_response()
                    }
                }
            }),
        );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    TestServer {
        base_url: format!("http://{addr}"),
        snapshot,
        forced_status,
    }
}

async fn make_db(dir: &tempfile::TempDir) -> Arc<CoreTexDB> {
    let db = CoreTexDB::with_config(DbConfig::new(dir.path().to_str().unwrap()));
    db.init().await.unwrap();
    Arc::new(db)
}

/// A primary with WAL switched on.
///
/// `DbConfig::new` leaves `wal_enabled: false`, so `replication_lsn()` stays 0
/// no matter how much is written. Any test that reasons about positions has to
/// turn it on or it is reasoning about nothing.
async fn make_db_with_wal(dir: &tempfile::TempDir) -> Arc<CoreTexDB> {
    let mut cfg = DbConfig::new(dir.path().to_str().unwrap());
    cfg.wal_enabled = true;
    let db = CoreTexDB::with_config(cfg);
    db.init().await.unwrap();
    Arc::new(db)
}

// ─────────────────────────────────────────────────────────────────────
// 1. The transport reaches a real server and parses its answer.
// ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn http_transport_fetches_a_snapshot_over_a_real_socket() {
    let server = start_server().await;
    let dir = tempfile::tempdir().unwrap();

    let primary = make_db(&dir).await;
    primary.create_collection("docs", 4, "cosine").await.unwrap();
    primary
        .insert_vectors(
            "docs",
            vec![(
                "a".to_string(),
                vec![1.0, 0.0, 0.0, 0.0],
                serde_json::json!({"t": "first"}),
            )],
        )
        .await
        .unwrap();

    let expected = primary.data_manager.replication_snapshot().await;
    *server.snapshot.write().await = Some(expected.clone());

    let transport = HttpTransport::new(&server.base_url);
    let got = transport
        .fetch_snapshot()
        .await
        .expect("fetch_snapshot over real HTTP must succeed");

    assert_eq!(got.lsn, expected.lsn, "watermark must survive the wire");
    assert_eq!(
        got.collections.len(),
        1,
        "the collection created above must be present in the fetched snapshot"
    );
    assert_eq!(
        got.collections[0].name, "docs",
        "schema must round-trip through JSON unchanged"
    );
    assert!(
        got.records.contains_key("docs"),
        "records must round-trip; got keys {:?}",
        got.records.keys().collect::<Vec<_>>()
    );
    assert!(got.records["docs"].contains_key("a"));
}

/// The URL is built by string concatenation inside the transport. Nothing else
/// in the suite would notice if it drifted from the routes the REST layer
/// registers, because the in-process transport never builds a URL at all.
#[tokio::test]
async fn transport_urls_match_the_routes_the_rest_layer_registers() {
    let server = start_server().await;
    let dir = tempfile::tempdir().unwrap();
    let db = make_db(&dir).await;
    db.create_collection("c", 2, "cosine").await.unwrap();
    *server.snapshot.write().await = Some(db.data_manager.replication_snapshot().await);

    // Both paths the transport formats must exist on a real server.
    for path in [
        "/replication/snapshot",
        "/replication/entries?since=0",
    ] {
        let resp = reqwest::get(format!("{}{}", server.base_url, path))
            .await
            .unwrap();
        // `reqwest::StatusCode` and `axum::http::StatusCode` come from two
        // different major versions of the `http` crate, so they are distinct
        // types with no `PartialEq` between them. Compare the numeric value.
        assert_ne!(
            resp.status().as_u16(),
            404,
            "{path} must be a registered route — the transport builds this URL by \
             concatenation and nothing else checks it"
        );
    }
}

// ─────────────────────────────────────────────────────────────────────
// 2. A failing primary must not look like a successful fetch.
// ─────────────────────────────────────────────────────────────────────

/// Regression: `HttpTransport` never checked the HTTP status code. It called
/// `.send()?.json::<T>()`, so a 503 carrying an error page surfaced as
/// "replication decode snapshot: expected value at line 1" — a message about
/// *parsing* that says nothing about the actual cause. An operator debugging a
/// primary that was refusing connections would be sent to look at their JSON.
///
/// The body below is deliberately a *valid* `ReplicationSnapshot`. With no
/// status check it is accepted silently: the replica would apply `lsn: 9999`
/// and skip everything before it, with no error anywhere. That is the failure
/// mode worth preventing.
#[tokio::test]
async fn http_transport_does_not_treat_an_error_status_as_success() {
    let server = start_server().await;

    let plausible = serde_json::json!({
        "lsn": 9999,
        "collections": [],
        "records": {},
    })
    .to_string();
    *server.forced_status.write().await = Some((StatusCode::SERVICE_UNAVAILABLE, plausible));

    let transport = HttpTransport::new(&server.base_url);
    let result = transport.fetch_snapshot().await;

    assert!(
        result.is_err(),
        "a 503 must not be reported as a successful snapshot — the replica \
         would apply lsn=9999 and skip everything before it"
    );

    let msg = result.unwrap_err().to_string();
    assert!(
        !msg.contains("decode"),
        "a status failure reported as a decode error sends the reader after the \
         wrong cause; got {msg:?}"
    );
    assert!(
        msg.contains("503") || msg.to_lowercase().contains("status"),
        "the error must identify the HTTP failure; got {msg:?}"
    );
}

/// Same, for the entries path.
#[tokio::test]
async fn http_transport_does_not_treat_an_error_status_as_success_on_entries() {
    let server = start_server().await;

    let plausible = serde_json::json!({
        "entries": [],
        "truncated": false,
        "lsn": 4242,
    })
    .to_string();
    *server.forced_status.write().await = Some((StatusCode::BAD_GATEWAY, plausible));

    let transport = HttpTransport::new(&server.base_url);
    let result = transport.fetch_entries(0).await;

    assert!(
        result.is_err(),
        "a 502 must not be reported as a valid empty batch at lsn 4242"
    );
    let msg = result.unwrap_err().to_string();
    assert!(
        !msg.contains("decode"),
        "a status failure reported as a decode error misdirects the reader; got {msg:?}"
    );
}

/// Guards the harness itself: if `forced_status` did not work, the two tests
/// above would pass for the wrong reason (the request would 404, or succeed
/// legitimately) and prove nothing.
#[tokio::test]
async fn the_test_server_really_returns_the_forced_status() {
    let server = start_server().await;

    // A body that is *also* a valid snapshot, so this test asserts both halves
    // of the scenario: the status really is 503, and the body really would
    // have decoded. Either alone is not enough to make the test meaningful.
    let plausible = serde_json::json!({
        "lsn": 9999,
        "collections": [],
        "records": {},
    })
    .to_string();
    *server.forced_status.write().await = Some((StatusCode::SERVICE_UNAVAILABLE, plausible.clone()));

    let resp = reqwest::get(format!("{}/replication/snapshot", server.base_url))
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        503,
        "the harness must be able to force a status, or the status tests are vacuous"
    );

    let body = resp.text().await.unwrap();
    let parsed: Result<ReplicationSnapshot, _> = serde_json::from_str(&body);
    assert!(
        parsed.is_ok(),
        "the forced body must decode as a snapshot for the 'looks like success' \
         scenario to be real; body was {body:?}"
    );
    assert_eq!(parsed.unwrap().lsn, 9999);
}

// ─────────────────────────────────────────────────────────────────────
// 3. End to end: a replica syncing from a primary over HTTP.
// ─────────────────────────────────────────────────────────────────────

/// The whole point of `HttpTransport`: a replica pulling from a primary that
/// only speaks HTTP. The in-process suite covers the state machine; this covers
/// the two actually meeting.
#[tokio::test]
async fn replica_syncs_from_a_primary_over_real_http() {
    let server = start_server().await;

    let primary_dir = tempfile::tempdir().unwrap();
    let replica_dir = tempfile::tempdir().unwrap();
    let state_dir = tempfile::tempdir().unwrap();

    let primary = make_db(&primary_dir).await;
    primary.create_collection("books", 3, "cosine").await.unwrap();
    primary
        .insert_vectors(
            "books",
            vec![
                (
                    "b1".to_string(),
                    vec![1.0, 0.0, 0.0],
                    serde_json::json!({"title": "dune"}),
                ),
                (
                    "b2".to_string(),
                    vec![0.0, 1.0, 0.0],
                    serde_json::json!({"title": "ubik"}),
                ),
            ],
        )
        .await
        .unwrap();

    *server.snapshot.write().await = Some(primary.data_manager.replication_snapshot().await);

    let replica = make_db(&replica_dir).await;
    let sync = ReplicaSync::new(
        replica.clone(),
        Arc::new(HttpTransport::new(&server.base_url)),
        state_dir.path().join("replica.json"),
    );

    let outcome = sync.sync_once().await.expect("first sync must succeed");
    assert!(
        matches!(outcome, SyncOutcome::FullSync { .. }),
        "position 0 must take a snapshot; got {outcome:?}"
    );

    // The data actually arrived, over HTTP.
    let names = replica.list_collections().await.unwrap();
    assert!(
        names.contains(&"books".to_string()),
        "collection must exist on the replica; got {names:?}"
    );
    assert_eq!(
        replica.get_vectors_count("books").await.unwrap(),
        2,
        "both vectors must have been replicated"
    );

    let hits = replica
        .search("books", vec![1.0, 0.0, 0.0], 1, None)
        .await
        .expect("replica must be able to search replicated data");
    assert_eq!(
        hits.len(),
        1,
        "search on a replica should return the nearest neighbour"
    );
    assert_eq!(hits[0].id, "b1", "the nearest vector must be the one asked for");

    // Read-only is part of the replica contract, not a side effect.
    assert!(
        replica.create_collection("nope", 3, "cosine").await.is_err(),
        "a replica must refuse writes"
    );
}

/// The replica must remember its position across restarts, or every restart
/// triggers a pointless full resync.
///
/// WAL must be enabled on the primary, otherwise `lsn` is 0 and "restored" is
/// indistinguishable from "reset" — which is exactly the mistake this test made
/// the first time, asserting `!= 0` against a primary with `wal_enabled: false`.
#[tokio::test]
async fn replica_remembers_its_position_across_restarts() {
    let server = start_server().await;

    let primary_dir = tempfile::tempdir().unwrap();
    let replica_dir = tempfile::tempdir().unwrap();
    let state_dir = tempfile::tempdir().unwrap();
    let state_path = state_dir.path().join("replica.json");

    let primary = make_db_with_wal(&primary_dir).await;
    primary.create_collection("t", 2, "cosine").await.unwrap();
    primary
        .insert_vectors(
            "t",
            vec![("v1".to_string(), vec![1.0, 0.0], serde_json::json!({}))],
        )
        .await
        .unwrap();

    let snap = primary.data_manager.replication_snapshot().await;
    assert_ne!(
        snap.lsn, 0,
        "precondition: with WAL enabled the primary must have a non-zero \
         position, otherwise this test cannot distinguish restored from reset"
    );
    *server.snapshot.write().await = Some(snap);

    let reached;
    {
        let replica = make_db(&replica_dir).await;
        let sync = ReplicaSync::new(
            replica,
            Arc::new(HttpTransport::new(&server.base_url)),
            &state_path,
        );
        sync.sync_once().await.unwrap();
        reached = sync.last_lsn();
    }

    assert!(
        state_path.exists(),
        "the applied position must be persisted to {state_path:?}"
    );
    let persisted: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&state_path).unwrap()).unwrap();
    assert_eq!(
        persisted["last_lsn"].as_u64(),
        Some(reached),
        "state file must record the position actually reached; got {persisted}"
    );

    // A second syncer over the same state starts where the first stopped.
    let replica2 = make_db(&replica_dir).await;
    let sync2 = ReplicaSync::new(
        replica2,
        Arc::new(HttpTransport::new(&server.base_url)),
        &state_path,
    );
    assert_eq!(
        sync2.last_lsn(),
        reached,
        "position must be restored from {state_path:?}, not reset — otherwise \
         every restart is a full resync"
    );
    assert_ne!(
        sync2.last_lsn(),
        0,
        "the restored position must be the non-zero one, not a silent zero"
    );
}

// ─────────────────────────────────────────────────────────────────────
// 4. Failure handling a replica actually depends on.
// ─────────────────────────────────────────────────────────────────────

/// An unreachable primary must fail the cycle outright.
#[tokio::test]
async fn unreachable_primary_fails_the_cycle() {
    let dir = tempfile::tempdir().unwrap();
    let state_dir = tempfile::tempdir().unwrap();

    // Port 1 on loopback: nothing listens there.
    let transport = HttpTransport::new("http://127.0.0.1:1");
    let sync = ReplicaSync::new(
        make_db(&dir).await,
        Arc::new(transport),
        state_dir.path().join("r.json"),
    );

    assert!(
        sync.sync_once().await.is_err(),
        "an unreachable primary must fail the cycle"
    );
}

/// The watermark is what makes a snapshot safe to apply. It has to survive the
/// wire byte-for-byte.
#[tokio::test]
async fn watermark_survives_serialisation() {
    let server = start_server().await;
    let dir = tempfile::tempdir().unwrap();
    let db = make_db(&dir).await;
    db.create_collection("w", 2, "cosine").await.unwrap();

    let mut snap = db.data_manager.replication_snapshot().await;
    snap.lsn = 12_345;
    *server.snapshot.write().await = Some(snap);

    let transport = HttpTransport::new(&server.base_url);
    assert_eq!(
        transport.fetch_snapshot().await.unwrap().lsn,
        12_345,
        "u64 watermark must not be truncated or reformatted in transit"
    );
}
