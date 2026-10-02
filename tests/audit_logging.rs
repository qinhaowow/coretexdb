//! End-to-end audit trail verification.
//!
//! `coretex_security`'s own tests prove `AuditLogger` stores what it is given.
//! They cannot prove the REST layer *gives it* anything — that `auth_middleware`
//! really hands the caller identity down, that the six audited handlers really
//! record both outcomes, or that the JSONL file the install tree reserves is
//! actually written. Before this file, `AuditLogger` had no production call
//! site at all, and no test could have noticed: unit tests of an unwired
//! component pass happily.
//!
//! These drive the real router through `tower::ServiceExt::oneshot` and then
//! read the persisted log back off disk.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use coretexdb::coretex_api::rest::build_app;
use coretexdb::{ApiConfig, CoreTexDB, DbConfig};
use std::sync::Arc;
use tower::ServiceExt;

/// Everything one test needs: the router, the temp dir that owns the data
/// tree (and therefore the audit file), and the state handle.
struct Harness {
    app: axum::Router,
    dir: tempfile::TempDir,
}

impl Harness {
    fn audit_file(&self) -> std::path::PathBuf {
        // `DbConfig::log_dir` is `<base>/data/logs` — a *sibling* of
        // `<base>/data/coretex` (which is `data_dir`), not a child of it.
        self.dir.path().join("data").join("logs").join("audit").join("audit.jsonl")
    }

    async fn send(&self, req: Request<Body>) -> (StatusCode, String) {
        let resp = self.app.clone().oneshot(req).await.expect("infallible");
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), 4 * 1024 * 1024)
            .await
            .unwrap();
        (status, String::from_utf8_lossy(&bytes).into_owned())
    }

    async fn post_json(&self, uri: &str, body: &str, token: Option<&str>) -> (StatusCode, String) {
        let mut b = Request::builder()
            .method("POST")
            .uri(uri)
            .header("content-type", "application/json");
        if let Some(t) = token {
            b = b.header("authorization", format!("Bearer {t}"));
        }
        self.send(b.body(Body::from(body.to_string())).unwrap()).await
    }

    /// Read the persisted trail and decode it line by line.
    fn events(&self) -> Vec<serde_json::Value> {
        let path = self.audit_file();
        let content = std::fs::read_to_string(&path).unwrap_or_else(|e| {
            panic!("audit file {} must exist once anything is audited: {e}", path.display())
        });
        content
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| {
                serde_json::from_str(l)
                    .unwrap_or_else(|e| panic!("audit line is not valid JSON: {l:?} ({e})"))
            })
            .collect()
    }
}

async fn harness_with_auth() -> Harness {
    let dir = tempfile::tempdir().unwrap();
    let db = CoreTexDB::with_config(DbConfig::new(dir.path().to_str().unwrap()));
    db.init().await.unwrap();
    let db = Arc::new(tokio::sync::RwLock::new(db));

    let config = ApiConfig {
        address: "127.0.0.1".to_string(),
        port: 0,
        data_dir: dir.path().to_str().unwrap().to_string(),
        enable_auth: true,
        ..Default::default()
    };

    let app = build_app(&config, db).await.expect("router must build");
    Harness { app, dir }
}

/// Register the first admin and return a usable token.
async fn admin(h: &Harness) -> String {
    h.post_json(
        "/api/auth/register",
        r#"{"username":"auditor","password":"s3cret-password"}"#,
        None,
    )
    .await;
    let (_, body) = h
        .post_json(
            "/api/auth/login",
            r#"{"username":"auditor","password":"s3cret-password"}"#,
            None,
        )
        .await;
    serde_json::from_str::<serde_json::Value>(&body).unwrap()["data"]["token"]
        .as_str()
        .unwrap_or_else(|| panic!("login must return a token: {body}"))
        .to_string()
}

// ─────────────────────────────────────────────────────────────────────
// 1. The trail exists at all, and is valid JSONL.
// ─────────────────────────────────────────────────────────────────────

/// The install tree has always created `<log_dir>/audit`, with a comment
/// admitting nothing wrote there. This is the assertion that it is no longer
/// an empty directory.
#[tokio::test]
async fn an_audit_file_is_actually_written_to_the_reserved_directory() {
    let h = harness_with_auth().await;
    let _token = admin(&h).await;

    let path = h.audit_file();
    assert!(
        path.exists(),
        "the audit file must be created at {} — that directory is created by \
         init_metadata specifically for this",
        path.display()
    );

    let events = h.events();
    assert!(
        !events.is_empty(),
        "registering and logging in must both be audited"
    );

    // Every line independently parseable — the old format produced
    // `[{...}]` then `,{...}`, which nothing could parse.
    for (i, e) in events.iter().enumerate() {
        assert!(e.get("id").is_some(), "event {i} missing id: {e}");
        assert!(e.get("timestamp").is_some(), "event {i} missing timestamp: {e}");
        assert!(e.get("action").is_some(), "event {i} missing action: {e}");
    }

    let ids: std::collections::HashSet<&str> =
        events.iter().filter_map(|e| e["id"].as_str()).collect();
    assert_eq!(
        ids.len(),
        events.len(),
        "event ids must be unique — the same second must not reuse one"
    );
}

// ─────────────────────────────────────────────────────────────────────
// 2. Authentication events, both outcomes.
// ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn logins_are_audited_on_both_outcomes() {
    let h = harness_with_auth().await;
    let _token = admin(&h).await;

    // A wrong password must leave a record. A trail containing only successes
    // cannot answer "who was trying to get in", which is usually the first
    // question worth asking.
    h.post_json(
        "/api/auth/login",
        r#"{"username":"auditor","password":"wrong-password"}"#,
        None,
    )
    .await;

    let events = h.events();
    let failed_logins: Vec<_> = events
        .iter()
        .filter(|e| e["action"] == "Login" && e["success"] == false)
        .collect();

    assert_eq!(
        failed_logins.len(),
        1,
        "the rejected login must be recorded; got {} login events: {events:#?}",
        events
            .iter()
            .filter(|e| e["action"] == "Login")
            .count()
    );
    let e = failed_logins[0];
    assert!(
        e["error_message"].is_string(),
        "a failed login must record why: {e}"
    );
    assert_eq!(
        e["level"], "Warning",
        "a failed login is a Warning, not Info: {e}"
    );

    // And the successful one is there too, with the resolved identity.
    let ok_logins: Vec<_> = events
        .iter()
        .filter(|e| e["action"] == "Login" && e["success"] == true)
        .collect();
    assert!(
        !ok_logins.is_empty(),
        "successful logins must be recorded too: {events:#?}"
    );
    assert!(
        ok_logins.iter().any(|e| e["user_id"].is_string()),
        "a successful login must record who: {ok_logins:#?}"
    );
}

/// Rejected tokens are audited by the middleware, before any handler runs.
/// Repeated failures against one address are what credential stuffing looks
/// like, and by the time anyone looks, a ring buffer has rolled over.
#[tokio::test]
async fn rejected_tokens_are_audited_by_the_middleware() {
    let h = harness_with_auth().await;
    let token = admin(&h).await;
    let _ = token;

    for _ in 0..3 {
        let (status, _) = h
            .send(
                Request::builder()
                    .uri("/api/collections")
                    .header("authorization", "Bearer not-a-real-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }

    let rejected: Vec<_> = h
        .events()
        .into_iter()
        .filter(|e| {
            e["success"] == false
                && e["error_message"]
                    .as_str()
                    .is_some_and(|m| m.contains("Invalid token"))
        })
        .collect();

    assert_eq!(
        rejected.len(),
        3,
        "all three rejected tokens must be recorded; events: {:#?}",
        rejected
    );
}

// ─────────────────────────────────────────────────────────────────────
// 3. Destructive actions carry an actor.
// ─────────────────────────────────────────────────────────────────────

/// The point of threading `Caller` through the middleware: "collection
/// deleted" without an actor answers none of the questions an audit log exists
/// for. Before this, `auth_middleware` discarded the verified claims, so no
/// handler could have known who was calling.
#[tokio::test]
async fn collection_lifecycle_is_audited_with_the_caller() {
    let h = harness_with_auth().await;
    let token = admin(&h).await;

    h.post_json(
        "/api/collections",
        r#"{"name":"audited","dimension":4,"metric":"cosine"}"#,
        Some(&token),
    )
    .await;

    let (status, _) = h
        .send(
            Request::builder()
                .method("DELETE")
                .uri("/api/collections/audited")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    assert_eq!(status, StatusCode::OK);

    let events = h.events();
    let creates: Vec<_> = events
        .iter()
        .filter(|e| e["resource"] == "collection:audited" && e["action"] == "Create")
        .collect();
    let deletes: Vec<_> = events
        .iter()
        .filter(|e| e["resource"] == "collection:audited" && e["action"] == "Delete")
        .collect();

    assert_eq!(creates.len(), 1, "create must be audited: {events:#?}");
    assert_eq!(deletes.len(), 1, "delete must be audited: {events:#?}");

    // The actor is present on both, and it is a real user id.
    for e in creates.iter().chain(deletes.iter()) {
        let uid = e["user_id"]
            .as_str()
            .unwrap_or_else(|| panic!("audit event has no actor: {e}"));
        assert!(
            !uid.is_empty(),
            "actor must not be empty — this is the whole reason Caller exists: {e}"
        );
    }

    // Deleting data irreversibly is worth more than a routine create.
    assert_eq!(
        deletes[0]["level"], "Warning",
        "a destructive action should not be logged at Info: {:#?}",
        deletes[0]
    );
}

/// Failures are recorded too — a repeated stream of failed deletes is someone
/// probing for what exists.
#[tokio::test]
async fn failed_actions_are_audited_with_the_reason() {
    let h = harness_with_auth().await;
    let token = admin(&h).await;

    for _ in 0..2 {
        h.send(
            Request::builder()
                .method("DELETE")
                .uri("/api/collections/no-such-collection")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    }

    let failures: Vec<_> = h
        .events()
        .into_iter()
        .filter(|e| e["resource"] == "collection:no-such-collection" && e["success"] == false)
        .collect();

    assert_eq!(
        failures.len(),
        2,
        "failed deletes must be audited: {:#?}",
        failures
    );
    assert!(
        failures[0]["error_message"].is_string(),
        "a failure must record why: {:#?}",
        failures[0]
    );
}

/// Restore overwrites live data with a snapshot — the single most
/// consequential thing the API exposes besides a drop.
#[tokio::test]
async fn backup_and_restore_are_audited() {
    let h = harness_with_auth().await;
    let token = admin(&h).await;

    h.post_json("/api/admin/backup", r#"{}"#, Some(&token)).await;
    // A restore naming a backup that does not exist still must be recorded.
    h.post_json(
        "/api/admin/restore",
        r#"{"backup_name":"definitely-absent"}"#,
        Some(&token),
    )
    .await;

    let events = h.events();
    let backups: Vec<_> = events
        .iter()
        .filter(|e| e["resource"].as_str().is_some_and(|r| r.starts_with("backup:")))
        .collect();
    let restores: Vec<_> = events
        .iter()
        .filter(|e| e["resource"].as_str().is_some_and(|r| r.starts_with("restore:")))
        .collect();

    assert_eq!(backups.len(), 1, "backup must be audited: {events:#?}");
    assert_eq!(
        restores.len(),
        1,
        "a failed restore must be audited too: {events:#?}"
    );
    assert_eq!(
        restores[0]["success"], false,
        "restoring a missing backup should record the failure: {:#?}",
        restores[0]
    );
    assert_eq!(restores[0]["level"], "Warning");
}

// ─────────────────────────────────────────────────────────────────────
// 4. Configuration that must NOT produce a trail.
// ─────────────────────────────────────────────────────────────────────

/// With auth off there is no identity to attribute an action to, so a trail of
/// anonymous operations would imply accountability it does not have. The
/// logger must be absent rather than present-but-empty-and-misleading.
#[tokio::test]
async fn no_audit_trail_without_auth() {
    let dir = tempfile::tempdir().unwrap();
    let db = CoreTexDB::with_config(DbConfig::new(dir.path().to_str().unwrap()));
    db.init().await.unwrap();
    let db = Arc::new(tokio::sync::RwLock::new(db));

    let config = ApiConfig {
        address: "127.0.0.1".to_string(),
        port: 0,
        data_dir: dir.path().to_str().unwrap().to_string(),
        enable_auth: false,
        ..Default::default()
    };
    let app = build_app(&config, db).await.unwrap();

    for uri in ["/api/collections", "/health"] {
        let _ = app
            .clone()
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await;
    }

    let path = dir
        .path()
        .join("data")
        .join("logs")
        .join("audit")
        .join("audit.jsonl");
    assert!(
        !path.exists(),
        "with auth disabled the audit logger must not be constructed — a file \
         of anonymous events would suggest accountability that does not exist"
    );
}

/// Guard against the `Caller` plumbing being dropped: the client address has to
/// reach the trail, or it cannot answer "from where".
#[tokio::test]
async fn the_client_address_reaches_the_trail() {
    let h = harness_with_auth().await;
    let _token = admin(&h).await;

    h.send(
        Request::builder()
            .uri("/api/collections")
            .header("x-forwarded-for", "203.0.113.7, 10.0.0.1")
            .header("authorization", "Bearer not-a-real-token")
            .body(Body::empty())
            .unwrap(),
    )
    .await;

    let with_ip: Vec<_> = h
        .events()
        .into_iter()
        .filter(|e| e["ip_address"] == "203.0.113.7")
        .collect();
    assert!(
        !with_ip.is_empty(),
        "the first hop of x-forwarded-for must be recorded; events: {:#?}",
        h.events()
    );
}
