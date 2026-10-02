//! GraphQL mounted on the REST server: reachability and access control.
//!
//! The GraphQL module was written, re-exported from lib.rs, and then never
//! reached: `start_graphql_server` had no callers and the CLI had no flag for
//! it. It also carried a `build_schema` that constructed its *own* `AuthService`,
//! so the five auth mutations operated on a private, empty, throwaway user
//! store — REST users did not exist there, and users created there did nothing.
//!
//! Inert by accident is not a security property. These tests pin the state that
//! matters now that `/graphql` is mounted: it exists, it requires a bearer
//! token, and the auth mutations additionally require Admin.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use coretexdb::coretex_api::rest::build_app;
use coretexdb::{ApiConfig, CoreTexDB, DbConfig};
use std::sync::Arc;
use tower::ServiceExt;

struct Harness {
    app: axum::Router,
    _dir: tempfile::TempDir,
}

impl Harness {
    async fn post_gql(&self, query: &str, token: Option<&str>) -> (StatusCode, String) {
        let mut b = Request::builder()
            .method("POST")
            .uri("/graphql")
            .header("content-type", "application/json");
        if let Some(t) = token {
            b = b.header("authorization", format!("Bearer {t}"));
        }
        let body = serde_json::json!({ "query": query }).to_string();
        let resp = self
            .app
            .clone()
            .oneshot(b.body(Body::from(body)).unwrap())
            .await
            .expect("infallible");
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), 4 * 1024 * 1024)
            .await
            .unwrap();
        (status, String::from_utf8_lossy(&bytes).into_owned())
    }

    async fn login(&self, user: &str, pass: &str) -> String {
        let body = serde_json::json!({ "username": user, "password": pass }).to_string();
        let resp = self
            .app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/auth/login")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .expect("infallible");
        let bytes = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        v["data"]["token"].as_str().unwrap().to_string()
    }
}

async fn harness(enable_auth: bool) -> Harness {
    let dir = tempfile::tempdir().unwrap();
    let db = CoreTexDB::with_config(DbConfig::new(dir.path().to_str().unwrap()));
    db.init().await.unwrap();
    let db = Arc::new(tokio::sync::RwLock::new(db));

    let config = ApiConfig {
        address: "127.0.0.1".to_string(),
        port: 0,
        data_dir: dir.path().to_str().unwrap().to_string(),
        enable_auth,
        ..Default::default()
    };
    let app = build_app(&config, db).await.expect("router must build");
    Harness { app, _dir: dir }
}

// ─────────────────────────────────────────────────────────────────────
// 1. The endpoint exists at all.
// ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn graphql_is_mounted_and_answers() {
    let h = harness(false).await;
    let (status, body) = h.post_gql("{ health { status version } }", None).await;

    assert_eq!(
        status,
        StatusCode::OK,
        "/graphql must be registered; an unmounted router returns 404"
    );
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert!(
        v.get("errors").is_none(),
        "a trivial health query must not error: {body}"
    );
    assert_eq!(v["data"]["health"]["status"], "ok");
    assert!(
        !v["data"]["health"]["version"].as_str().unwrap().is_empty(),
        "version must be reported: {body}"
    );
}

// ─────────────────────────────────────────────────────────────────────
// 2. Authentication.
// ─────────────────────────────────────────────────────────────────────

/// The whole reason the endpoint is not in the auth whitelist: the mutation
/// root carries `deleteUser`, `assignRole` and `revokeToken`. Before this, the
/// handler executed whatever arrived with no token check at all.
#[tokio::test]
async fn graphql_requires_a_token_when_auth_is_enabled() {
    let h = harness(true).await;

    for (label, token) in [("no token", None), ("empty token", Some(""))] {
        let (status, body) = h.post_gql("{ health { status } }", token).await;
        assert_eq!(
            status,
            StatusCode::UNAUTHORIZED,
            "{label} must be rejected outright, not answered: {body}"
        );
        assert!(
            !body.contains("\"data\""),
            "{label} must not receive query results: {body}"
        );
    }
}

#[tokio::test]
async fn graphql_rejects_an_invalid_token() {
    let h = harness(true).await;
    let (status, _) = h.post_gql("{ health { status } }", Some("not-a-token")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn graphql_accepts_a_valid_token() {
    let h = harness(true).await;
    h.app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/auth/register")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"username":"gqladmin","password":"s3cret-password"}"#,
                ))
                .unwrap(),
        )
        .await;
    let token = h.login("gqladmin", "s3cret-password").await;

    let (status, body) = h.post_gql("{ health { status } }", Some(&token)).await;
    assert_eq!(status, StatusCode::OK, "valid token must be accepted: {body}");
    assert!(
        !body.contains("\"errors\""),
        "valid token must produce a clean answer: {body}"
    );
}

/// The subscription endpoint is a query too — `allDataChanges` streams every
/// collection's writes. It must not be the unauthenticated way in.
#[tokio::test]
async fn graphql_subscriptions_require_a_token_too() {
    let h = harness(true).await;

    let resp = h
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/graphql/ws")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"query":"subscription { allDataChanges { collection } }"}"#))
                .unwrap(),
        )
        .await
        .expect("infallible");
    assert_eq!(
        resp.status(),
        StatusCode::UNAUTHORIZED,
        "/graphql/ws must not be an unauthenticated way into the same schema"
    );
}

// ─────────────────────────────────────────────────────────────────────
// 3. The auth mutations see the real user store.
// ─────────────────────────────────────────────────────────────────────

/// The root defect. `build_schema` used to do `AuthService::new()`, so a user
/// registered over REST did not exist as far as GraphQL was concerned, and
/// `login` there could never succeed. Sharing the instance makes them agree.
#[tokio::test]
async fn graphql_login_sees_users_registered_over_rest() {
    let h = harness(true).await;

    h.app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/auth/register")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"username":"crossuser","password":"s3cret-password"}"#,
                ))
                .unwrap(),
        )
        .await;

    let token = h.login("crossuser", "s3cret-password").await;
    let (status, body) = h
        .post_gql(
            r#"mutation { login(username: "crossuser", password: "s3cret-password") { success userId } }"#,
            Some(&token),
        )
        .await;

    assert_eq!(status, StatusCode::OK);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(
        v["data"]["login"]["success"], true,
        "GraphQL login must see REST-registered users — it previously had its \
         own empty AuthService, so this always failed: {body}"
    );
    assert!(
        !v["data"]["login"]["userId"].as_str().unwrap().is_empty(),
        "a successful login must return a user id: {body}"
    );
}

// ─────────────────────────────────────────────────────────────────────
// 4. Admin-only mutations.
// ─────────────────────────────────────────────────────────────────────

/// `deleteUser`, `assignRole`, `revokeToken` and `createUser` can grant or
/// revoke access to the whole system. An ordinary authenticated user must not
/// reach them.
///
/// There is no way to obtain an admin over HTTP — `create_user` hard-codes the
/// `user` role and nothing HTTP-facing assigns another — so every case here is
/// driven with a valid token for a non-admin. That is exactly the shape of the
/// attack the guard exists for.
#[tokio::test]
async fn auth_mutations_are_refused_for_any_non_admin() {
    let h = harness(true).await;

    h.app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/auth/register")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"username":"attacker","password":"s3cret-password"}"#,
                ))
                .unwrap(),
        )
        .await;
    let token = h.login("attacker", "s3cret-password").await;

    let cases = [
        (
            "deleteUser",
            r#"mutation { deleteUser(userId: "user_victim") }"#.to_string(),
        ),
        (
            "assignRole",
            r#"mutation { assignRole(userId: "user_victim", role: "admin") { success } }"#
                .to_string(),
        ),
        ("revokeToken", format!(r#"mutation {{ revokeToken(token: "{token}") }}"#)),
        (
            "createUser",
            r#"mutation { createUser(username: "backdoor", password: "s3cret-password") { success } }"#
                .to_string(),
        ),
    ];

    for (name, query) in cases {
        let (status, body) = h.post_gql(&query, Some(&token)).await;
        assert_eq!(status, StatusCode::OK, "{name}: transport should be 200");
        assert!(
            body.contains("\"errors\""),
            "{name} must be refused for a non-admin — it can grant or revoke \
             access to the whole system. Got: {body}"
        );
        assert!(
            body.contains("Admin"),
            "{name} rejection must say why (Admin role); got {body}"
        );
    }

    // The refused createUser must not actually have created anything: logging
    // in as the account it tried to make must fail.
    let resp = h
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/auth/login")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"username":"backdoor","password":"s3cret-password"}"#,
                ))
                .unwrap(),
        )
        .await;
    let bytes = axum::body::to_bytes(resp.expect("infallible").into_body(), 1024 * 1024)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        v["status"], "error",
        "the refused createUser must not have created the account: {v}"
    );
}

/// With auth disabled the endpoint is open, matching a REST deployment started
/// without `--auth`. Consistent, not a special case — and the reason the
/// transport guard keys off the configuration rather than always refusing.
///
/// The Admin guard still applies: there is no caller identity to evaluate a
/// role against, so admin-only operations remain unreachable rather than
/// becoming universally permitted. That asymmetry is deliberate — an open
/// deployment should not silently promote every request to administrator.
#[tokio::test]
async fn graphql_is_open_when_auth_is_disabled() {
    let h = harness(false).await;

    // Reads work with no token.
    let (status, body) = h.post_gql("{ health { status } }", None).await;
    assert_eq!(status, StatusCode::OK);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["data"]["health"]["status"], "ok", "{body}");

    // And so do ordinary writes — consistent with REST started without --auth.
    let (_, body) = h
        .post_gql(
            r#"mutation { createCollection(input: { name: "opencol", dimension: 4 }) { name dimension } }"#,
            None,
        )
        .await;
    assert!(
        !body.contains("\"errors\""),
        "with auth off an ordinary mutation should work, consistently with \
         REST: {body}"
    );
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["data"]["createCollection"]["name"], "opencol");

    // But an admin-only mutation is still refused: with no token there is no
    // identity, and defaulting that to "administrator" would be the worst
    // possible answer.
    let (_, body) = h
        .post_gql(
            r#"mutation { createUser(username: "openuser", password: "s3cret-password") { success } }"#,
            None,
        )
        .await;
    assert!(
        body.contains("\"errors\""),
        "admin-only operations must stay refused without an identity: {body}"
    );
}

/// Guards the tests above: if the admin guard rejected *everyone*, the
/// per-mutation assertions would still pass for the wrong reason.
///
/// This documents the actual permission model, which the tests above
/// discovered by failing: `AuthService::create_user` hard-codes
/// `roles: ["user"]`, and neither REST nor GraphQL can assign a role — only the
/// CLI's `coretex admin user create --role admin` calls `assign_role`. So no
/// account created over HTTP is ever an administrator, and the admin-only
/// mutations are unreachable through HTTP by construction.
///
/// That is worth stating as an assertion rather than discovering later: the
/// guard is not being overly strict, the deployment model requires an operator
/// step to create the first admin.
#[tokio::test]
async fn no_account_created_over_http_is_ever_an_admin() {
    let h = harness(true).await;
    h.app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/auth/register")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"username":"firstuser","password":"s3cret-password"}"#,
                ))
                .unwrap(),
        )
        .await;
    let token = h.login("firstuser", "s3cret-password").await;

    let (_, body) = h
        .post_gql(
            r#"mutation { createUser(username: "second", password: "s3cret-password") { success } }"#,
            Some(&token),
        )
        .await;
    assert!(
        body.contains("Admin"),
        "a user created over HTTP is never an admin — create_user hard-codes \
         the 'user' role and no HTTP path can assign another. An operator must \
         run `coretex admin user create --role admin`. Got: {body}"
    );

    // And the same holds for REST's own second-user path, which is why a
    // server started with --auth cannot register more than one account without
    // an operator creating an admin first.
    let resp = h
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/auth/register")
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::from(
                    r#"{"username":"another","password":"s3cret-password"}"#,
                ))
                .unwrap(),
        )
        .await;
    let bytes = axum::body::to_bytes(resp.expect("infallible").into_body(), 1024 * 1024)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert!(
        v["status"] == "error",
        "REST registration of a second user must be refused for a non-admin: {v}"
    );
}
