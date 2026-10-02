//! Integration tests for the REST layer's routing and metrics endpoint.
//!
//! These drive the real router built by `build_app` — including the auth and
//! metrics middleware — via `tower::ServiceExt::oneshot`, rather than binding a
//! socket. Calling handlers directly would pass even when a route was never
//! registered or a layer never applied, which is exactly how a dead
//! `/metrics` endpoint could ship.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use coretexdb::coretex_api::rest::build_app;
use coretexdb::{ApiConfig, CoreTexDB, DbConfig};
use std::sync::Arc;
use tower::ServiceExt;

/// Build the real app against a throwaway data directory.
///
/// Goes through `build_app`, so the auth and metrics layers are applied exactly
/// as they are in production. Nothing is bound to a port.
async fn app_for_test() -> (axum::Router, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();

    let db = CoreTexDB::with_config(DbConfig::new(dir.path().to_str().unwrap()));
    db.init().await.unwrap();
    let db = Arc::new(tokio::sync::RwLock::new(db));

    let config = ApiConfig {
        address: "127.0.0.1".to_string(),
        port: 0,
        data_dir: dir.path().to_str().unwrap().to_string(),
        // Auth off: these tests are about routing and metrics, not
        // authentication (covered by the `coretex_auth` unit tests).
        enable_auth: false,
        rate_limit_per_minute: 0,
        enable_cors: false,
        cors_allowed_origins: vec![],
        // `..Default::default()` rather than spelling every field: adding one
        // to `ApiConfig` used to break this file, and `cargo test --lib` does
        // not compile integration-test targets — the break only surfaced in the
        // `full --all-targets` gate.
        ..Default::default()
    };

    let app = build_app(&config, db).await.expect("router must build");
    (app, dir)
}

/// Send one request through the full router.
///
/// `axum::Router`'s `Service` impl is infallible, so the `Result` from
/// `oneshot` never carries an error — routing failures surface as 404
/// responses, which is exactly what several of these tests assert on.
async fn send(app: &axum::Router, req: Request<Body>) -> axum::response::Response {
    app.clone()
        .oneshot(req)
        .await
        .expect("axum::Router is an infallible service")
}

async fn get(app: &axum::Router, uri: &str) -> axum::response::Response {
    send(
        app,
        Request::builder()
            .uri(uri)
            .body(Body::empty())
            .unwrap(),
    )
    .await
}

async fn scrape(app: &axum::Router) -> String {
    let bytes = axum::body::to_bytes(get(app, "/metrics").await.into_body(), 4 * 1024 * 1024)
        .await
        .unwrap();
    String::from_utf8(bytes.to_vec()).unwrap()
}

#[tokio::test]
async fn metrics_endpoint_is_registered_and_serves_prometheus_text() {
    let (app, _dir) = app_for_test().await;

    let resp = get(&app, "/metrics").await;

    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "/metrics must be registered — an unregistered route returns 404"
    );

    let content_type = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    assert!(
        content_type.starts_with("text/plain"),
        "Prometheus scrapers require a text content type, got {content_type:?}"
    );
    assert!(
        content_type.contains("version=0.0.4"),
        "the exposition version must be declared, got {content_type:?}"
    );
}

#[tokio::test]
async fn metrics_reflects_traffic_after_the_endpoint_is_scraped() {
    let (app, _dir) = app_for_test().await;

    // Generate some traffic first, then scrape.
    for _ in 0..3 {
        let _ = get(&app, "/health").await;
    }

    let text = scrape(&app).await;

    assert!(
        text.contains("coretexdb_http_requests_total"),
        "the request counter must be recorded, got:\n{text}"
    );
    assert!(
        text.contains("coretexdb_http_request_duration_ms_count"),
        "the latency histogram must be recorded, got:\n{text}"
    );
    assert!(
        text.contains("coretexdb_uptime_seconds"),
        "uptime gauge is set by get_prometheus_metrics, got:\n{text}"
    );

    // Every sample line must be `name value` — a third field is parsed by
    // Prometheus as a timestamp, silently discarding the sample.
    for line in text.lines() {
        if line.starts_with('#') {
            continue;
        }
        assert!(
            line.split_whitespace().count() == 2,
            "malformed exposition line (extra field read as timestamp): {line:?}"
        );
    }
}

#[tokio::test]
async fn metrics_series_use_route_templates_not_concrete_paths() {
    let (app, _dir) = app_for_test().await;

    // Three different collection names hitting the same route must collapse
    // onto one series. Keying on the concrete path would create a series per
    // name — unbounded cardinality driven by user input.
    for name in ["alpha", "beta", "gamma"] {
        let _ = get(&app, &format!("/api/collections/{name}/count")).await;
    }

    let text = scrape(&app).await;

    assert!(
        text.contains("route=\"/api/collections/:name/count\""),
        "expected the route template as a label, got:\n{text}"
    );
    for name in ["alpha", "beta", "gamma"] {
        assert!(
            !text.contains(&format!("route=\"/api/collections/{name}")),
            "collection name {name} leaked into a metric label — series cardinality \
             would grow with user input:\n{text}"
        );
    }
}

#[tokio::test]
async fn unmatched_paths_do_not_leak_into_metric_labels() {
    let (app, _dir) = app_for_test().await;

    // A 404 path is entirely attacker-controlled.
    for i in 0..10 {
        let _ = get(&app, &format!("/does-not-exist-{i}")).await;
    }

    let text = scrape(&app).await;

    assert!(
        text.contains("route=\"unmatched\""),
        "404s must collapse into one bucket, got:\n{text}"
    );
    assert!(
        !text.contains("does-not-exist-"),
        "an attacker-chosen path must never become a metric label:\n{text}"
    );
}

#[tokio::test]
async fn console_route_is_served() {
    let (app, _dir) = app_for_test().await;

    let resp = get(&app, "/console").await;

    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(resp.into_body(), 4 * 1024 * 1024)
        .await
        .unwrap();
    let html = String::from_utf8(bytes.to_vec()).unwrap();
    assert!(
        html.to_lowercase().contains("<html"),
        "expected the embedded console page, got {} bytes",
        html.len()
    );
}

#[tokio::test]
async fn removed_raft_endpoint_stays_404() {
    let (app, _dir) = app_for_test().await;

    // Guards the earlier decision to delete /raft/append_entries rather than
    // stub it. It used to return `success: true` unconditionally, telling a
    // leader its log had replicated when nothing was written.
    let resp = send(
        &app,
        Request::builder()
            .method("POST")
            .uri("/raft/append_entries")
            .body(Body::empty())
            .unwrap(),
    )
    .await;

    assert_eq!(
        resp.status(),
        StatusCode::NOT_FOUND,
        "/raft/append_entries must stay absent, not answer with a fake success"
    );
}

/// Build the app with authentication switched on.
async fn app_for_test_with_auth() -> (axum::Router, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();

    let db = CoreTexDB::with_config(DbConfig::new(dir.path().to_str().unwrap()));
    db.init().await.unwrap();
    let db = Arc::new(tokio::sync::RwLock::new(db));

    let config = ApiConfig {
        address: "127.0.0.1".to_string(),
        port: 0,
        data_dir: dir.path().to_str().unwrap().to_string(),
        enable_auth: true,
        rate_limit_per_minute: 0,
        enable_cors: false,
        cors_allowed_origins: vec![],
        ..Default::default()
    };

    let app = build_app(&config, db).await.expect("router must build");
    (app, dir)
}

/// Rejected requests must be counted, not filtered out before the counter sees
/// them.
///
/// This pins the layer ordering. In axum the most recently added
/// `Router::layer` is the outermost, so registering the metrics layer before
/// the auth layer put auth *outside* it — every 401 was invisible and
/// `/metrics` reported only the requests that had already been let through.
/// That is precisely backwards: a spike in 401s is the incident an operator
/// opens this endpoint to diagnose.
#[tokio::test]
async fn auth_rejections_are_counted_in_metrics() {
    let (app, _dir) = app_for_test_with_auth().await;

    // No token → rejected by auth_middleware before reaching any handler.
    for _ in 0..3 {
        let resp = get(&app, "/api/collections").await;
        assert_eq!(
            resp.status(),
            StatusCode::UNAUTHORIZED,
            "precondition: the request must be rejected by auth"
        );
    }

    // Scrape with a token so /metrics itself is not blocked by the same
    // middleware we are testing.
    let resp = send(
        &app,
        Request::builder()
            .uri("/metrics")
            .header("authorization", format!("Bearer {}", admin_token(&app).await))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);

    let bytes = axum::body::to_bytes(resp.into_body(), 4 * 1024 * 1024)
        .await
        .unwrap();
    let text = String::from_utf8(bytes.to_vec()).unwrap();

    assert!(
        text.contains("status=\"401\""),
        "401 responses must appear in the metrics — auth is filtering them out \
         before the counter:\n{text}"
    );
}

/// `/metrics` must require authentication when `--auth` is on.
///
/// The series expose collection and vector counts. Leaving this route in the
/// `auth_middleware` whitelist next to `/health` would hand that to any
/// unauthenticated scraper.
#[tokio::test]
async fn metrics_requires_auth_when_auth_is_enabled() {
    let (app, _dir) = app_for_test_with_auth().await;

    let resp = get(&app, "/metrics").await;
    assert_eq!(
        resp.status(),
        StatusCode::UNAUTHORIZED,
        "/metrics must not be in the auth whitelist"
    );
}

/// Register an admin then log in, returning a bearer token.
///
/// Goes only through the public HTTP API so the test does not depend on
/// `AuthService` internals. Note that `register` returns a *user id*, not a
/// token, and that these handlers answer HTTP 200 even on failure (the body
/// carries the error), so success is asserted on the payload — not the status.
async fn admin_token(app: &axum::Router) -> String {
    let resp = send(
        app,
        Request::builder()
            .method("POST")
            .uri("/api/auth/register")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"username":"metrics-admin","password":"s3cret-password"}"#,
            ))
            .unwrap(),
    )
    .await;
    let json: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(resp.into_body(), 1024 * 1024)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        json["status"], "ok",
        "first-run registration should create the initial administrator: {json}"
    );

    let resp = send(
        app,
        Request::builder()
            .method("POST")
            .uri("/api/auth/login")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"username":"metrics-admin","password":"s3cret-password"}"#,
            ))
            .unwrap(),
    )
    .await;
    let json: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(resp.into_body(), 1024 * 1024)
            .await
            .unwrap(),
    )
    .unwrap();
    // `login` wraps `LoginResponse` in the generic `ApiResponse`, so the token
    // lives under `data`, not at the top level.
    json["data"]["token"]
        .as_str()
        .unwrap_or_else(|| panic!("login must return a token: {json}"))
        .to_string()
}
