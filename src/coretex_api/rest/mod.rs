//! REST API for CoreTexDB

use axum::{
    extract::Request,
    http::{header, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post, delete, put},
    Json, Router, extract::State, extract::Extension,
};
use tower_http::cors::{AllowOrigin, Any, CorsLayer};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::net::SocketAddr;
use tokio::sync::RwLock;

use crate::{CoreTexDB, DbConfig};
use crate::coretex_auth::{AuthService, Permission, RateLimiter};
use crate::coretex_core::Result;
use crate::coretex_monitoring::DatabaseMetrics;
use crate::coretex_security::{AuditAction, AuditLevel, AuditLogger};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ApiConfig {
    pub address: String,
    pub port: u16,
    /// Directory holding the database. The server keeps its state here so it
    /// survives restarts.
    pub data_dir: String,
    /// 是否启用 CORS。生产环境应设为 false，仅当需要被浏览器跨域调用时启用。
    pub enable_cors: bool,
    /// CORS 允许的来源白名单。当 enable_cors=true 时生效，必须显式配置，
    /// 禁止使用通配符（避免任何来源跨域调用 API）。
    pub cors_allowed_origins: Vec<String>,
    pub enable_auth: bool,
    pub rate_limit_per_minute: usize,
    /// In-memory audit events retained for querying. The JSONL file on disk is
    /// the durable record; this only bounds what `/api/admin/audit` can return
    /// without re-reading the file.
    pub audit_max_events: usize,
}

impl Default for ApiConfig {
    fn default() -> Self {
        // 安全修复：默认关闭 CORS；默认开启认证（生产导向）。
        Self {
            address: "0.0.0.0".to_string(),
            port: 5000,
            data_dir: "./coretex_data".to_string(),
            enable_cors: false,
            cors_allowed_origins: Vec::new(),
            enable_auth: true,
            rate_limit_per_minute: 0,
            audit_max_events: 10_000,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateCollectionRequest {
    pub name: String,
    pub dimension: usize,
    pub distance_metric: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CollectionInfo {
    pub name: String,
    pub dimension: usize,
    pub distance_metric: String,
    pub vectors_count: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InsertVectorsRequest {
    pub vectors: Vec<VectorItem>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct VectorItem {
    pub id: String,
    pub vector: Vec<f32>,
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InsertVectorsResponse {
    pub status: String,
    pub ids: Vec<String>,
    pub count: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GetVectorResponse {
    pub id: String,
    pub vector: Vec<f32>,
    pub metadata: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DeleteVectorsRequest {
    pub ids: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DeleteVectorsResponse {
    pub status: String,
    pub deleted_count: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SearchRequest {
    pub vector: Vec<f32>,
    pub k: usize,
    pub filter: Option<serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SearchResponse {
    pub results: Vec<SearchResultItem>,
    pub execution_time_ms: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SearchResultItem {
    pub id: String,
    pub score: f32,
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BatchSearchRequest {
    pub queries: Vec<Vec<f32>>,
    pub k: usize,
    pub filter: Option<serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BatchSearchResponse {
    pub results: Vec<Vec<SearchResultItem>>,
    pub execution_time_ms: u64,
}

/// Body of `POST /api/collections/:name/hybrid-search`.
///
/// `vector` and `text` are both optional but at least one is required;
/// supplying both is what makes this a *hybrid* query.
#[derive(Debug, Serialize, Deserialize)]
pub struct HybridSearchRequest {
    #[serde(default)]
    pub vector: Option<Vec<f32>>,
    #[serde(default)]
    pub text: Option<String>,
    pub k: usize,
    #[serde(default)]
    pub filter: Option<serde_json::Value>,
    /// Metadata field holding the document text (default `"text"`).
    #[serde(default)]
    pub text_field: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HybridSearchResponse {
    pub results: Vec<HybridSearchResultItem>,
    pub execution_time_ms: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HybridSearchResultItem {
    pub id: String,
    /// Reciprocal-rank-fused score; higher is better.
    pub score: f32,
    /// Which retrievers returned this id: `"vector"`, `"text"`, or both.
    pub sources: Vec<String>,
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateVectorsRequest {
    pub ids: Vec<String>,
    pub vectors: Option<Vec<Vec<f32>>>,
    pub metadata: Option<Vec<serde_json::Value>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateVectorsResponse {
    pub status: String,
    pub updated_count: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CollectionStats {
    pub name: String,
    pub vector_count: usize,
    pub dimension: usize,
    pub metric: String,
    pub index_type: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RenameCollectionRequest {
    pub new_name: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ListVectorsQuery {
    pub offset: Option<usize>,
    pub limit: Option<usize>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct VectorListItem {
    pub id: String,
    pub vector: Vec<f32>,
    pub metadata: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ListVectorsResponse {
    pub vectors: Vec<VectorListItem>,
    pub total: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UpsertVectorsResponse {
    pub status: String,
    pub inserted_ids: Vec<String>,
    pub updated_ids: Vec<String>,
    pub inserted_count: usize,
    pub updated_count: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ClearCollectionResponse {
    pub status: String,
    pub deleted_count: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ApiResponse<T> {
    pub status: String,
    pub data: Option<T>,
    pub error: Option<String>,
}

impl<T> ApiResponse<T> {
    pub fn success(data: T) -> Self {
        Self {
            status: "ok".to_string(),
            data: Some(data),
            error: None,
        }
    }

    pub fn error(msg: &str) -> Self {
        Self {
            status: "error".to_string(),
            data: None,
            error: Some(msg.to_string()),
        }
    }
}

pub struct ApiState {
    pub db: Arc<RwLock<CoreTexDB>>,
    pub auth: Arc<AuthService>,
    pub rate_limiter: Option<Arc<RateLimiter>>,
    pub enable_auth: bool,
    /// Backs `GET /metrics`.
    ///
    /// `DatabaseMetrics` was previously unreachable from any server: it is
    /// `pub use`d from `lib.rs`, but nothing constructed one, so no code path
    /// ever recorded a metric. Sharing one instance here is what makes the
    /// endpoint return anything other than an empty body.
    pub metrics: Arc<DatabaseMetrics>,
    /// Audit trail.
    ///
    /// Previously unreachable in exactly the same way: `AuditLogger` lives in
    /// `coretex_security`, is re-exported from `lib.rs`, and had no production
    /// call site — `with_persistent_storage` was never invoked, so
    /// `persist_event` never ran and the `logs/audit` directory that
    /// `init_metadata` creates stayed empty. The install tree documented a
    /// feature that did not exist.
    pub audit: Option<Arc<AuditLogger>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LoginResponse {
    pub token: String,
    pub user_id: String,
    pub expires_in: i64,
}

pub async fn start_server(config: ApiConfig) -> Result<()> {
    let db = CoreTexDB::with_config(DbConfig::new(&config.data_dir));
    db.init().await.map_err(|e| format!("Failed to init DB: {}", e))?;
    start_server_with_db(config, Arc::new(RwLock::new(db))).await
}

/// Start the REST API against an already-initialized shared DB handle.
/// Lets `coretex server` run REST + gRPC on the same instance.
pub async fn start_server_with_db(
    config: ApiConfig,
    db: Arc<RwLock<CoreTexDB>>,
) -> Result<()> {
    let app = build_app(&config, db).await?;
    let addr = SocketAddr::new(config.address.parse().unwrap(), config.port);

    println!("Starting CoreTexDB API server on http://{}", addr);
    println!("Auth enabled: {}", config.enable_auth);
    // 0 = 不限。与 gRPC 侧同样只打印真正生效的限制。
    if config.rate_limit_per_minute > 0 {
        println!("Rate limit: {} req/min", config.rate_limit_per_minute);
    } else {
        println!("Rate limit: disabled");
    }
    println!("API endpoints:");
    println!("  GET  /console                              - Browser console");
    println!("  GET  /health                              - Health check");
    println!("  GET  /metrics                             - Prometheus metrics");
    println!("  POST /api/auth/login                      - Login");
    println!("  POST /api/auth/register                   - Register");
    println!("  GET  /api/collections                     - List collections");
    println!("  POST /api/collections                    - Create collection");
    println!("  GET  /api/collections/:name               - Get collection info");
    println!("  DELETE /api/collections/:name             - Delete collection");
    println!("  GET  /api/collections/:name/stats         - Get collection stats");
    println!("  POST /api/collections/:name/vectors       - Insert vectors");
    println!("  PUT  /api/collections/:name/vectors      - Update vectors");
    println!("  GET  /api/collections/:name/vectors/:id  - Get vector");
    println!("  DELETE /api/collections/:name/vectors     - Delete vectors");
    println!("  POST /api/collections/:name/search       - Search vectors");
    println!("  POST /api/collections/:name/batch-search - Batch search");
    println!("  GET  /api/collections/:name/count        - Get vectors count");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

/// Build the fully-layered router.
///
/// Public so an embedding application can mount the REST API inside a larger
/// axum app, and so tests can drive the real routing stack — including the auth
/// and metrics layers — with `tower::ServiceExt` instead of binding a socket.
/// A test that only calls a handler would not notice a route that was never
/// registered or a middleware that was never applied, which is precisely what
/// was wrong before.
pub async fn build_app(config: &ApiConfig, db: Arc<RwLock<CoreTexDB>>) -> Result<Router> {
    // Persist users under `<data_dir>/metadata/auth.json`.
    //
    // `AuthService::new()` keeps them in memory only, so every registered user
    // vanished on restart — a server started with `--auth` lost its
    // administrators on every deploy. `with_persistence` existed but was never
    // called from here.
    //
    // The directory comes from the DB's own config, not from
    // `ApiConfig::data_dir`: the latter is the install root the user passed to
    // `--data-dir`, whereas `DbConfig::data_dir` is already `<root>/data/coretex`,
    // which is where `init_metadata()` puts the placeholder auth.json. Using the
    // install root produced a second, competing auth.json one level up.
    //
    // Only pay for it when auth is on; otherwise every default deployment would
    // create a metadata dir it never uses.
    let auth = Arc::new(if config.enable_auth {
        AuthService::with_persistence(&db.read().await.config.data_dir)
    } else {
        AuthService::new()
    });
    let rate_limiter = if config.rate_limit_per_minute > 0 {
        Some(Arc::new(RateLimiter::new(config.rate_limit_per_minute, 60)))
    } else {
        None
    };

    // Shared by the `/metrics` endpoint and the request-counting middleware
    // below, so both observe the same series.
    let metrics = Arc::new(DatabaseMetrics::new());

    // Audit trail, appended to `<data_dir>/logs/audit/audit.jsonl` — the
    // directory `init_metadata` already creates for exactly this purpose.
    //
    // Enabled whenever auth is, because the events worth recording are
    // authentication and administrative ones. Running with auth off means
    // there is no identity to attribute an action to, so a trail of anonymous
    // operations would be misleading rather than useful.
    let audit = if config.enable_auth {
        // `config.log_dir` is a String, not a PathBuf. `init_metadata` creates
        // `<log_dir>/audit`, so writing there keeps the audit trail inside the
        // tree it already reserves for it.
        let dir = std::path::PathBuf::from(&db.read().await.config.log_dir).join("audit");
        let path = dir.join("audit.jsonl");
        Some(Arc::new(
            AuditLogger::new(config.audit_max_events)
                .with_persistent_storage(true)
                .with_storage_path(&path.to_string_lossy()),
        ))
    } else {
        None
    };

    let state = Arc::new(ApiState {
        db: db.clone(),
        auth: auth.clone(),
        rate_limiter: rate_limiter.clone(),
        enable_auth: config.enable_auth,
        metrics: metrics.clone(),
        audit: audit.clone(),
    });

    let mut app = Router::new()
        .route("/console", get(serve_console))
        .route("/health", get(health_check))
        .route("/metrics", get(metrics_endpoint))
        .route("/api/auth/login", post(login))
        .route("/api/auth/register", post(register))
        .route("/api/collections", get(list_collections))
        .route("/api/collections", post(create_collection))
        .route("/api/collections/:name", get(get_collection))
        .route("/api/collections/:name", delete(delete_collection))
        .route("/api/collections/:name/stats", get(get_collection_stats))
        .route("/api/collections/:name/vectors", post(insert_vectors))
        .route("/api/collections/:name/vectors", put(update_vectors))
        .route("/api/collections/:name/vectors", get(list_vectors))
        .route("/api/collections/:name/vectors/upsert", post(upsert_vectors))
        .route("/api/collections/:name/vectors/clear", delete(clear_collection))
        .route("/api/collections/:name/vectors/:id", get(get_vector))
        .route(
            "/api/collections/:name/vectors/:id/ttl",
            put(set_vector_ttl).delete(remove_vector_ttl),
        )
        .route("/api/collections/:name/vectors", delete(delete_vectors))
        .route("/api/collections/:name/rename", put(rename_collection))
        .route("/api/collections/:name/search", post(search))
        .route("/api/collections/:name/batch-search", post(batch_search))
        .route("/api/collections/:name/hybrid-search", post(hybrid_search))
        .route("/api/collections/:name/count", get(get_vectors_count))
        .route("/api/admin/purge-expired", post(purge_expired))
        .route("/api/admin/backup", post(create_backup))
        .route("/api/admin/restore", post(restore_backup))
        .route("/api/admin/backup/list", get(list_backups))
        // Replication data plane (auth skipped below): the replica's
        // HttpTransport pulls these without a user session.
        //
        // No /raft/* routes on purpose. `coretex_failover` ships a RaftLog and
        // an HttpRaftRpc client, but nothing ever constructs a FailoverManager
        // and ApiState holds no log, so a handler here could only echo back a
        // hardcoded `success: true` — which tells a leader its log was
        // replicated when nothing was written. The endpoint was removed rather
        // than stubbed; `HttpRaftRpc` requests /raft/request_vote and
        // /raft/heartbeat, which were never routed either, so leader election
        // and heartbeats 404 as well. See docs/roadmap.md C2.
        .route("/replication/status", get(replication_status))
        .route("/replication/snapshot", get(replication_snapshot))
        .route("/replication/entries", get(replication_entries));

    // GraphQL, mounted under the same server so it inherits this process's
    // `AuthService` — its five auth mutations used to build a private, empty
    // one, so users registered over REST did not exist there and accounts
    // created there did nothing.
    //
    // It is deliberately NOT in the auth whitelist: the mutation root carries
    // `deleteUser`, `assignRole` and `revokeToken`. The GraphQL router does its
    // own bearer check (mirroring `enable_auth`), and its auth mutations
    // additionally require the Admin role.
    {
        use crate::coretex_api::graphql;
        let schema = graphql::build_schema_with_auth(db.clone(), auth.clone());
        let gql: axum::Router<Arc<ApiState>> =
            graphql::graphql_router(schema, auth.clone(), config.enable_auth);
        app = app.nest("/graphql", gql);
    }

    let mut app = app.with_state(state.clone());

    // 启用认证中间件
    if config.enable_auth {
        let auth_clone = auth.clone();
        let rl_clone = rate_limiter.clone();
        app = app.layer(middleware::from_fn(move |req, next| {
            let auth = auth_clone.clone();
            let rl = rl_clone.clone();
            let audit = audit.clone();
            async move { auth_middleware(req, next, auth, rl, audit).await }
        }));
    } else if rate_limiter.is_some() {
        // 即使没启用认证也启用速率限制
        let rl_clone = rate_limiter.clone();
        app = app.layer(middleware::from_fn(move |req, next| {
            let rl = rl_clone.clone();
            async move { rate_limit_middleware(req, next, rl).await }
        }));
    }

    // Guarantee a `Caller` always exists, whatever the auth configuration.
    //
    // Sits outside `auth_middleware`: it seeds a `Caller` with the client
    // address and no identity, and the auth layer (inner) overwrites it with
    // the verified user id when a token checks out. Handlers can then take
    // `Extension<Caller>` and be certain the extractor resolves — with auth
    // disabled there is no auth layer at all to seed it.
    app = app.layer(middleware::from_fn(
        |mut req: Request, next: Next| async move {
            let ip = caller_ip_from_headers(req.headers());
            req.extensions_mut().insert(Caller { user_id: None, ip });
            Ok::<_, std::convert::Infallible>(next.run(req).await)
        },
    ));

    // Metrics instrumentation is applied LAST so that it is the OUTERMOST
    // layer — in axum the most recently added `Router::layer` wraps the others.
    //
    // Ordering matters and is easy to get backwards. An operator watching a
    // 401 spike is looking at exactly the incident this endpoint exists to
    // reveal, so rejections must be counted rather than filtered out before the
    // counter ever sees them. Applied inside auth instead, `/metrics` reports
    // only the requests that got through — a clean picture precisely when
    // something is wrong. `auth_rejections_are_counted_in_metrics` pins this:
    // it was confirmed to fail under the inverted ordering.
    {
        let metrics_clone = metrics.clone();
        app = app.layer(middleware::from_fn(move |req, next| {
            let metrics = metrics_clone.clone();
            async move { metrics_middleware(req, next, metrics).await }
        }));
    }

    let app = if config.enable_cors {
        // 安全修复：仅允许显式配置的 origin 白名单，禁止使用 Any 通配符。
        if config.cors_allowed_origins.is_empty() {
            // 没有任何白名单时不启用 CORS 层，避免无意中放行所有来源。
            app
        } else {
            let origins: Vec<_> = config
                .cors_allowed_origins
                .iter()
                .filter_map(|s| s.parse().ok())
                .collect();
            let cors = CorsLayer::new()
                .allow_origin(AllowOrigin::list(origins))
                .allow_methods(Any)
                .allow_headers(Any);
            app.layer(
                tower::ServiceBuilder::new()
                    .layer(cors)
            )
        }
    } else {
        app
    };

    Ok(app)
}

// =============== 认证中间件 ===============

/// `GET /console` — 浏览器控制台。
///
/// The page is embedded rather than read from disk: the install root's
/// `share/doc/console.html` is not necessarily next to the binary (and a
/// single-file deployment may not ship it at all), so a file read here would
/// make the route work only in some layouts. Same-origin serving also sidesteps
/// the cross-origin block that stops a `file://` page from calling the API.
///
/// It is a static, dependency-free client for the routes below — no database
/// access, no state, nothing to authenticate against beyond what the API
/// itself requires.
async fn serve_console() -> Response {
    const PAGE: &str = include_str!("../../../share/doc/console.html");
    (
        StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            "text/html; charset=utf-8",
        )],
        PAGE,
    )
        .into_response()
}

/// 从请求中提取客户端 IP，优先使用 X-Forwarded-For（首个有效 IP），
/// 再回退 X-Real-IP，最后回退到 Authorization 标识。
/// 安全修复：使用 IP 而非 Authorization header 作为限流键，
/// 防止攻击者通过更换 token 绕过单 IP 速率限制。
fn extract_client_identifier(req: &Request) -> String {
    if let Some(xff) = req.headers().get("x-forwarded-for") {
        if let Ok(s) = xff.to_str() {
            if let Some(first) = s.split(',').next() {
                let ip = first.trim();
                if !ip.is_empty() {
                    return format!("ip:{}", ip);
                }
            }
        }
    }
    if let Some(xri) = req.headers().get("x-real-ip") {
        if let Ok(s) = xri.to_str() {
            if !s.is_empty() {
                return format!("ip:{}", s);
            }
        }
    }
    // 最后回退到 Authorization 标识，但仅作临时兜底。
    req.headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .map(|s| format!("auth:{}", s))
        .unwrap_or_else(|| "unknown".to_string())
}

async fn auth_middleware(
    req: Request,
    next: Next,
    auth: Arc<AuthService>,
    rate_limiter: Option<Arc<RateLimiter>>,
    audit: Option<Arc<AuditLogger>>,
) -> std::result::Result<Response, StatusCode> {
    let path = req.uri().path().to_string();

    // 白名单：登录、注册、健康检查、复制数据面不需要认证
    // 注意：/api/auth/register 在 register handler 内部会强制要求 admin token
    // （或首次启动时无用户时放开），这里仍放行至 handler。
    //
    // /console 是静态页面，不触碰任何数据；放行它只是为了浏览器能打开。
    // 它发出的每个 API 请求仍各自经过这里的认证检查。
    if path == "/health" || path == "/api/auth/login" || path == "/api/auth/register"
        || path == "/console"
        || path.starts_with("/replication/") {
        return Ok(next.run(req).await);
    }

    // 速率限制（基于客户端 IP）
    if let Some(rl) = rate_limiter {
        let identifier = extract_client_identifier(&req);
        if let Err(e) = rl.check_rate_limit(&identifier).await {
            return Ok((StatusCode::TOO_MANY_REQUESTS, Json(serde_json::json!({
                "status": "error",
                "error": format!("Rate limit exceeded: {}", e)
            }))).into_response());
        }
    }

    // 验证 token
    let token = match req.headers().get(header::AUTHORIZATION) {
        Some(v) => v.to_str().unwrap_or("").trim_start_matches("Bearer ").to_string(),
        None => {
            return Ok((StatusCode::UNAUTHORIZED, Json(serde_json::json!({
                "status": "error",
                "error": "Missing Authorization header"
            }))).into_response());
        }
    };

    match auth.verify_token(&token).await {
        Ok(claims) => {
            // Hand the caller's identity to the handler.
            //
            // It used to be discarded (`Ok(_claims)`), so no handler could know
            // who was calling. That makes an audit trail impossible to write
            // correctly: "collection deleted" without an actor answers none of
            // the questions an audit log exists for.
            //
            // Only the user id goes in the extensions; the `Caller` extractor
            // fills in the client address from the headers itself.
            let mut req = req;
            req.extensions_mut().insert(Caller {
                user_id: Some(claims.sub.clone()),
                ip: None,
            });
            Ok(next.run(req).await)
        }
        Err(e) => {
            // A rejected token is itself worth recording — repeated failures
            // against one address are what a credential-stuffing attempt looks
            // like, and by the time anyone notices, the in-memory ring buffer
            // that would have shown it has long since rolled over.
            if let Some(audit) = &audit {
                let ip = caller_ip_from_headers(req.headers());
                audit
                    .log_request(
                        AuditLevel::Warning,
                        AuditAction::Login,
                        "auth",
                        None,
                        ip.as_deref(),
                        false,
                        Some(&format!("Invalid token: {}", e)),
                    )
                    .await;
            }
            Ok((StatusCode::UNAUTHORIZED, Json(serde_json::json!({
                "status": "error",
                "error": format!("Invalid token: {}", e)
            }))).into_response())
        }
    }
}

/// First hop of `X-Forwarded-For`, else `X-Real-IP`.
///
/// Only the first entry is taken: everything after it is appended by
/// intermediate proxies and is trivially forged by a client. Shared by the
/// middleware and the `Caller` extractor so both derive the same address.
fn caller_ip_from_headers(headers: &axum::http::HeaderMap) -> Option<String> {
    headers
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.split(',').next().map(str::trim))
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or_else(|| {
            headers
                .get("x-real-ip")
                .and_then(|v| v.to_str().ok())
                .map(str::to_string)
        })
}

/// Who is making this request, and from where.
///
/// Lives in the request extensions. `auth_middleware` overwrites the entry with
/// the verified identity; a fallback layer guarantees one always exists, so
/// handlers can take the plain built-in `Extension<Caller>` extractor and be
/// certain it resolves.
///
/// Both fields are optional and frequently are: `user_id` is unknown when auth
/// is disabled or the route is whitelisted, and `ip` is unknown when the
/// request arrived without either forwarding header.
///
/// A custom `FromRequestParts` extractor was tried first and rejected: `Request`
/// and `Json<T>` both consume the body and axum permits only one body-consuming
/// argument, so an audited handler cannot see the caller that way. Reading the
/// extensions through the built-in `Extension` extractor avoids that entirely.
#[derive(Clone, Debug, Default)]
pub struct Caller {
    pub user_id: Option<String>,
    pub ip: Option<String>,
}

async fn rate_limit_middleware(
    req: Request,
    next: Next,
    rate_limiter: Option<Arc<RateLimiter>>,
) -> std::result::Result<Response, StatusCode> {
    if let Some(rl) = rate_limiter {
        // 安全修复：使用客户端 IP 而非 Authorization header 作为限流键。
        let identifier = extract_client_identifier(&req);
        if let Err(e) = rl.check_rate_limit(&identifier).await {
            return Ok((StatusCode::TOO_MANY_REQUESTS, Json(serde_json::json!({
                "status": "error",
                "error": format!("Rate limit exceeded: {}", e)
            }))).into_response());
        }
    }
    Ok(next.run(req).await)
}

async fn login(
    State(state): State<Arc<ApiState>>,
    Extension(caller): Extension<Caller>,
    Json(body): Json<LoginRequest>,
) -> Json<ApiResponse<LoginResponse>> {
    match state.auth.authenticate(&body.username, &body.password).await {
        Ok(token) => {
            if let Some(audit) = &state.audit {
                // Both outcomes are recorded. A trail that only contains
                // successes cannot answer "who was trying to get in", which is
                // usually the first question worth asking.
                audit
                    .log_request(
                        AuditLevel::Info,
                        AuditAction::Login,
                        "auth",
                        Some(&token.user_id),
                        caller.ip.as_deref(),
                        true,
                        None,
                    )
                    .await;
            }
            Json(ApiResponse::success(LoginResponse {
                token: token.token,
                user_id: token.user_id,
                expires_in: 86400, // 24 小时
            }))
        }
        Err(e) => {
            if let Some(audit) = &state.audit {
                audit
                    .log_request(
                        AuditLevel::Warning,
                        AuditAction::Login,
                        "auth",
                        // The user id is unknown here — the credentials were
                        // rejected — so the attempt is recorded against the
                        // address and the error, which is the useful signal.
                        None,
                        caller.ip.as_deref(),
                        false,
                        Some(&e.to_string()),
                    )
                    .await;
            }
            Json(ApiResponse::error(&e))
        }
    }
}

async fn register(
    State(state): State<Arc<ApiState>>,
    Extension(caller): Extension<Caller>,
    req: Request,
) -> Json<ApiResponse<String>> {
    // 安全修复：注册端点要求 admin token 鉴权，或在系统无任何用户时
    // （首次启动）允许无鉴权注册第一个用户。
    //
    // The token is verified here rather than taken from `caller`, because this
    // route is in `auth_middleware`'s whitelist — the middleware never checks
    // it, so there is no `Caller` to read even when a valid admin token was
    // sent.
    let user_count = state.auth.list_users().await.len();
    if user_count > 0 {
        // 已存在用户：要求 Authorization 头包含有效 admin token。
        let auth_header = req
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        let token = auth_header.trim_start_matches("Bearer ").trim();
        if token.is_empty() {
            if let Some(audit) = &state.audit {
                audit
                    .log_request(
                        AuditLevel::Warning,
                        AuditAction::Admin,
                        "user",
                        None,
                        caller.ip.as_deref(),
                        false,
                        Some("Admin token required to register new users"),
                    )
                    .await;
            }
            return Json(ApiResponse::error(
                "Admin token required to register new users",
            ));
        }
        let claims = match state.auth.verify_token(token).await {
            Ok(c) => c,
            Err(e) => return Json(ApiResponse::error(&format!("Invalid token: {}", e))),
        };
        // 必须拥有 Admin 权限。
        let is_admin = state.auth.has_permission(&claims.sub, Permission::Admin).await;
        if !is_admin {
            return Json(ApiResponse::error(
                "Admin role required to register new users",
            ));
        }
    }

    // 解析 body。
    let (_, body) = req.into_parts();
    let bytes = match axum::body::to_bytes(body, 64 * 1024).await {
        Ok(b) => b,
        Err(e) => return Json(ApiResponse::error(&format!("Invalid body: {}", e))),
    };
    let parsed = serde_json::from_slice::<LoginRequest>(&bytes);
    let body = match parsed {
        Ok(r) => r,
        Err(e) => return Json(ApiResponse::error(&format!("Invalid JSON: {}", e))),
    };
    match state.auth.create_user(&body.username, &body.password, None).await {
        Ok(user_id) => {
            if let Some(audit) = &state.audit {
                audit
                    .log_request(
                        AuditLevel::Info,
                        AuditAction::Admin,
                        &format!("user:{user_id}"),
                        // On first run there is no caller yet — the first
                        // registration is necessarily unauthenticated.
                        caller.user_id.as_deref(),
                        caller.ip.as_deref(),
                        true,
                        None,
                    )
                    .await;
            }
            Json(ApiResponse::success(user_id))
        }
        Err(e) => {
            if let Some(audit) = &state.audit {
                audit
                    .log_request(
                        AuditLevel::Warning,
                        AuditAction::Admin,
                        "user",
                        caller.user_id.as_deref(),
                        caller.ip.as_deref(),
                        false,
                        Some(&e.to_string()),
                    )
                    .await;
            }
            Json(ApiResponse::error(&e))
        }
    }
}

/// `GET /replication/status` — where this node's log is, and whether it
/// refuses writes. Lets an operator see primary watermark vs. replica
/// position at a glance.
async fn replication_status(
    State(state): State<Arc<ApiState>>,
) -> Json<crate::ReplicationStatus> {
    let db = state.db.read().await;
    Json(crate::ReplicationStatus::collect(&db).await)
}

/// `GET /replication/snapshot` — full-sync payload: every collection and
/// record with the log position they are consistent with.
async fn replication_snapshot(
    State(state): State<Arc<ApiState>>,
) -> Json<crate::ReplicationSnapshot> {
    let db = state.db.read().await;
    Json(db.data_manager.replication_snapshot().await)
}

#[derive(Deserialize)]
struct ReplicationEntriesParams {
    /// Last position the replica applied; entries with a higher sequence
    /// come back. Missing = 0 (everything the log still holds).
    #[serde(default)]
    since: u64,
}

/// `GET /replication/entries?since=N` — incremental tail plus the
/// continuity flag. The batch's `lsn` is derived from the shipped entries
/// themselves, so it can never advertise a sequence that was not sent.
async fn replication_entries(
    State(state): State<Arc<ApiState>>,
    axum::extract::Query(params): axum::extract::Query<ReplicationEntriesParams>,
) -> Response {
    let db = state.db.read().await;
    match db.data_manager.read_replication_entries(params.since).await {
        Ok((entries, truncated)) => {
            let lsn = entries.last().map(|e| e.sequence).unwrap_or(params.since);
            Json(crate::EntriesBatch {
                entries,
                truncated,
                lsn,
            })
            .into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

async fn health_check() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
    })
}

/// `GET /metrics` — Prometheus text exposition (version 0.0.4).
///
/// Requires the same bearer token as the rest of the API when `--auth` is on:
/// the series carry collection and vector counts, which is operational detail
/// an unauthenticated scraper should not be able to read. It is deliberately
/// **not** in the `auth_middleware` whitelist next to `/health`.
async fn metrics_endpoint(
    State(state): State<Arc<ApiState>>,
) -> Response {
    (
        StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        state.metrics.get_prometheus_metrics().await,
    )
        .into_response()
}

/// Count and time every REST request.
///
/// A middleware rather than per-handler calls: there are ~30 routes, and
/// instrumenting them one at a time is exactly how a metric ends up missing
/// from whichever handler nobody remembered. This way a newly added route is
/// covered by construction.
///
/// The route *template* is used rather than the concrete path, so
/// `/api/collections/a/search` and `/api/collections/b/search` land on one
/// series instead of one series per collection name — which would reintroduce
/// the unbounded-cardinality problem the histogram fix just removed, since
/// collection names are user input.
async fn metrics_middleware(
    req: Request,
    next: Next,
    metrics: Arc<DatabaseMetrics>,
) -> Response {
    let route = req
        .extensions()
        .get::<axum::extract::MatchedPath>()
        .map(|p| p.as_str().to_string())
        // A 404 has no matched route; fall back to a single bucket rather than
        // the raw path, which would be attacker-controlled and unbounded.
        .unwrap_or_else(|| "unmatched".to_string());

    let started = std::time::Instant::now();
    let response = next.run(req).await;
    let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;

    let status = response.status().as_u16().to_string();
    metrics.record_request(&route, &status, elapsed_ms).await;

    response
}

async fn list_collections(
    State(state): State<Arc<ApiState>>,
) -> Json<ApiResponse<Vec<String>>> {
    let db = state.db.read().await;
    match db.list_collections().await {
        Ok(collections) => Json(ApiResponse::success(collections)),
        Err(e) => Json(ApiResponse::error(&e.to_string())),
    }
}

async fn create_collection(
    State(state): State<Arc<ApiState>>,
    Extension(caller): Extension<Caller>,
    Json(req): Json<CreateCollectionRequest>,
) -> Json<ApiResponse<CollectionInfo>> {
    let metric = req.distance_metric.unwrap_or_else(|| "cosine".to_string());

    let result = {
        let db = state.db.read().await;
        db.create_collection(&req.name, req.dimension, &metric).await
    };

    if let Some(audit) = &state.audit {
        let (level, ok, err) = match &result {
            Ok(_) => (AuditLevel::Info, true, None),
            Err(e) => (AuditLevel::Warning, false, Some(e.to_string())),
        };
        audit
            .log_request(
                level,
                AuditAction::Create,
                &format!("collection:{}", req.name),
                caller.user_id.as_deref(),
                caller.ip.as_deref(),
                ok,
                err.as_deref(),
            )
            .await;
    }

    match result {
        Ok(_) => {
            let info = CollectionInfo {
                name: req.name.clone(),
                dimension: req.dimension,
                distance_metric: metric,
                vectors_count: 0,
            };
            Json(ApiResponse::success(info))
        }
        Err(e) => Json(ApiResponse::error(&e.to_string())),
    }
}

async fn get_collection(
    State(state): State<Arc<ApiState>>,
    axum::extract::Path(name): axum::extract::Path<String>,
) -> Json<ApiResponse<CollectionInfo>> {
    let db = state.db.read().await;
    
    match db.get_collection(&name).await {
        Ok(schema) => {
            let count = db.get_vectors_count(&name).await.unwrap_or(0);
            let info = CollectionInfo {
                name: schema.name,
                dimension: schema.dimension,
                distance_metric: format!("{:?}", schema.distance_metric),
                vectors_count: count,
            };
            Json(ApiResponse::success(info))
        }
        Err(e) => Json(ApiResponse::error(&e.to_string())),
    }
}

async fn delete_collection(
    State(state): State<Arc<ApiState>>,
    Extension(caller): Extension<Caller>,
    axum::extract::Path(name): axum::extract::Path<String>,
) -> Json<ApiResponse<String>> {
    let result = {
        let db = state.db.read().await;
        db.delete_collection(&name).await
    };

    // Deleting a collection destroys its data irreversibly. This is the single
    // most consequential action the API exposes, so it is recorded whether it
    // succeeded or failed — a repeated stream of failed deletes is someone
    // probing for what exists.
    if let Some(audit) = &state.audit {
        let (level, ok, err) = match &result {
            Ok(_) => (AuditLevel::Warning, true, None),
            Err(e) => (AuditLevel::Warning, false, Some(e.to_string())),
        };
        audit
            .log_request(
                level,
                AuditAction::Delete,
                &format!("collection:{name}"),
                caller.user_id.as_deref(),
                caller.ip.as_deref(),
                ok,
                err.as_deref(),
            )
            .await;
    }

    match result {
        Ok(_) => Json(ApiResponse::success(format!("Collection '{}' deleted", name))),
        Err(e) => Json(ApiResponse::error(&e.to_string())),
    }
}

async fn insert_vectors(
    State(state): State<Arc<ApiState>>,
    axum::extract::Path(name): axum::extract::Path<String>,
    Json(req): Json<InsertVectorsRequest>,
) -> Json<ApiResponse<InsertVectorsResponse>> {
    let db = state.db.read().await;
    
    let vectors: Vec<(String, Vec<f32>, serde_json::Value)> = req.vectors
        .into_iter()
        .map(|v| (v.id, v.vector, v.metadata.unwrap_or(serde_json::json!({}))))
        .collect();
    
    let ids: Vec<String> = vectors.iter().map(|(id, _, _)| id.clone()).collect();
    
    match db.insert_vectors(&name, vectors).await {
        Ok(inserted_ids) => Json(ApiResponse::success(InsertVectorsResponse {
            status: "ok".to_string(),
            ids: inserted_ids,
            count: ids.len(),
        })),
        Err(e) => Json(ApiResponse::error(&e.to_string())),
    }
}

async fn get_vector(
    State(state): State<Arc<ApiState>>,
    axum::extract::Path((name, id)): axum::extract::Path<(String, String)>,
) -> Json<ApiResponse<GetVectorResponse>> {
    let db = state.db.read().await;
    
    match db.get_vector(&name, &id).await {
        Ok(Some((vector, metadata))) => Json(ApiResponse::success(GetVectorResponse {
            id,
            vector,
            metadata,
        })),
        Ok(None) => Json(ApiResponse::error("Vector not found")),
        Err(e) => Json(ApiResponse::error(&e.to_string())),
    }
}

async fn delete_vectors(
    State(state): State<Arc<ApiState>>,
    axum::extract::Path(name): axum::extract::Path<String>,
    Json(req): Json<DeleteVectorsRequest>,
) -> Json<ApiResponse<DeleteVectorsResponse>> {
    let db = state.db.read().await;
    
    match db.delete_vectors(&name, &req.ids).await {
        Ok(count) => Json(ApiResponse::success(DeleteVectorsResponse {
            status: "ok".to_string(),
            deleted_count: count,
        })),
        Err(e) => Json(ApiResponse::error(&e.to_string())),
    }
}

async fn search(
    State(state): State<Arc<ApiState>>,
    axum::extract::Path(name): axum::extract::Path<String>,
    Json(req): Json<SearchRequest>,
) -> Json<ApiResponse<SearchResponse>> {
    let start = std::time::Instant::now();
    let db = state.db.read().await;
    
    match db.search(&name, req.vector, req.k, req.filter).await {
        Ok(results) => {
            let ids: Vec<String> = results.iter().map(|r| r.id.clone()).collect();
            let vectors = db.get_vectors_by_ids(&name, &ids).await.unwrap_or_default();
            let vector_map: std::collections::HashMap<String, (Vec<f32>, serde_json::Value)> = vectors.into_iter().collect();
            
            let search_results: Vec<SearchResultItem> = results
                .into_iter()
                .map(|r| {
                    let metadata = vector_map.get(&r.id).map(|(_, m)| m.clone());
                    
                    SearchResultItem {
                        id: r.id,
                        score: 1.0 - r.distance,
                        metadata,
                    }
                })
                .collect();
            
            let execution_time = start.elapsed().as_millis() as u64;
            
            Json(ApiResponse::success(SearchResponse {
                results: search_results,
                execution_time_ms: execution_time,
            }))
        }
        Err(e) => Json(ApiResponse::error(&e.to_string())),
    }
}

/// Hybrid retrieval: fuse ANN neighbours with BM25 text matches.
///
/// `crate::HybridSearchRequest` and this module's DTO share a name; the
/// fully-qualified path below is the library type.
async fn hybrid_search(
    State(state): State<Arc<ApiState>>,
    axum::extract::Path(name): axum::extract::Path<String>,
    Json(req): Json<HybridSearchRequest>,
) -> Json<ApiResponse<HybridSearchResponse>> {
    let start = std::time::Instant::now();
    let db = state.db.read().await;

    let request = crate::HybridSearchRequest {
        vector: req.vector,
        text: req.text,
        k: req.k,
        filter: req.filter,
        text_field: req.text_field,
    };

    match db.hybrid_search(&name, request).await {
        Ok(hits) => {
            let ids: Vec<String> = hits.iter().map(|h| h.id.clone()).collect();
            let records = db.get_vectors_by_ids(&name, &ids).await.unwrap_or_default();
            let metadata_map: std::collections::HashMap<String, serde_json::Value> =
                records.into_iter().map(|(id, (_, m))| (id, m)).collect();

            let search_results: Vec<HybridSearchResultItem> = hits
                .into_iter()
                .map(|h| HybridSearchResultItem {
                    metadata: metadata_map.get(&h.id).cloned(),
                    id: h.id,
                    score: h.score,
                    sources: h.sources,
                })
                .collect();

            Json(ApiResponse::success(HybridSearchResponse {
                results: search_results,
                execution_time_ms: start.elapsed().as_millis() as u64,
            }))
        }
        Err(e) => Json(ApiResponse::error(&e.to_string())),
    }
}

async fn get_vectors_count(
    State(state): State<Arc<ApiState>>,
    axum::extract::Path(name): axum::extract::Path<String>,
) -> Json<ApiResponse<usize>> {
    let db = state.db.read().await;
    
    match db.get_vectors_count(&name).await {
        Ok(count) => Json(ApiResponse::success(count)),
        Err(e) => Json(ApiResponse::error(&e.to_string())),
    }
}

async fn get_collection_stats(
    State(state): State<Arc<ApiState>>,
    axum::extract::Path(name): axum::extract::Path<String>,
) -> Json<ApiResponse<CollectionStats>> {
    let db = state.db.read().await;
    
    match db.get_collection(&name).await {
        Ok(schema) => {
            let count = db.get_vectors_count(&name).await.unwrap_or(0);
            let stats = CollectionStats {
                name: schema.name,
                dimension: schema.dimension,
                metric: format!("{:?}", schema.distance_metric),
                vector_count: count,
                index_type: "hnsw".to_string(),
            };
            Json(ApiResponse::success(stats))
        }
        Err(e) => Json(ApiResponse::error(&e.to_string())),
    }
}

async fn update_vectors(
    State(state): State<Arc<ApiState>>,
    axum::extract::Path(name): axum::extract::Path<String>,
    Json(req): Json<UpdateVectorsRequest>,
) -> Json<ApiResponse<UpdateVectorsResponse>> {
    let db = state.db.read().await;
    
    let mut updated_count = 0;
    
    if let Some(vectors) = req.vectors {
        for (i, id) in req.ids.iter().enumerate() {
            if i < vectors.len() {
                if let Ok(Some((_, metadata))) = db.get_vector(&name, id).await {
                    let new_vector = vectors[i].clone();
                    let new_metadata = req.metadata.as_ref().and_then(|m| m.get(i).cloned()).unwrap_or(metadata);
                    
                    let _ = db.delete_vectors(&name, std::slice::from_ref(id)).await;
                    let _ = db.insert_vectors(&name, vec![(id.clone(), new_vector, new_metadata)]).await;
                    updated_count += 1;
                }
            }
        }
    } else if let Some(metadata) = req.metadata {
        for (i, id) in req.ids.iter().enumerate() {
            if let Ok(Some((vector, _))) = db.get_vector(&name, id).await {
                let new_metadata = metadata.get(i).cloned().unwrap_or(serde_json::json!({}));
                let _ = db.delete_vectors(&name, std::slice::from_ref(id)).await;
                let _ = db.insert_vectors(&name, vec![(id.clone(), vector, new_metadata)]).await;
                updated_count += 1;
            }
        }
    }
    
    Json(ApiResponse::success(UpdateVectorsResponse {
        status: "ok".to_string(),
        updated_count,
    }))
}

async fn batch_search(
    State(state): State<Arc<ApiState>>,
    axum::extract::Path(name): axum::extract::Path<String>,
    Json(req): Json<BatchSearchRequest>,
) -> Json<ApiResponse<BatchSearchResponse>> {
    let start = std::time::Instant::now();
    let db = state.db.read().await;
    
    let mut all_results: Vec<Vec<SearchResultItem>> = Vec::new();
    
    for query in req.queries {
        match db.search(&name, query, req.k, req.filter.clone()).await {
            Ok(results) => {
                let ids: Vec<String> = results.iter().map(|r| r.id.clone()).collect();
                let vectors = db.get_vectors_by_ids(&name, &ids).await.unwrap_or_default();
                let vector_map: std::collections::HashMap<String, (Vec<f32>, serde_json::Value)> = vectors.into_iter().collect();
                
                let search_results: Vec<SearchResultItem> = results
                    .into_iter()
                    .map(|r| {
                        let metadata = vector_map.get(&r.id).map(|(_, m)| m.clone());
                        
                        SearchResultItem {
                            id: r.id,
                            score: 1.0 - r.distance,
                            metadata,
                        }
                    })
                    .collect();
                
                all_results.push(search_results);
            }
            Err(_) => {
                all_results.push(Vec::new());
            }
        }
    }
    
    let execution_time = start.elapsed().as_millis() as u64;
    
    Json(ApiResponse::success(BatchSearchResponse {
        results: all_results,
        execution_time_ms: execution_time,
    }))
}

async fn list_vectors(
    State(state): State<Arc<ApiState>>,
    axum::extract::Path(name): axum::extract::Path<String>,
    axum::extract::Query(query): axum::extract::Query<ListVectorsQuery>,
) -> Json<ApiResponse<ListVectorsResponse>> {
    let db = state.db.read().await;

    match db.list_vectors(&name).await {
        Ok(all_vectors) => {
            let total = all_vectors.len();
            let offset = query.offset.unwrap_or(0);
            let limit = query.limit.unwrap_or(100).min(10000);

            let paginated: Vec<VectorListItem> = all_vectors
                .into_iter()
                .skip(offset)
                .take(limit)
                .map(|(id, vector, metadata)| VectorListItem { id, vector, metadata })
                .collect();

            Json(ApiResponse::success(ListVectorsResponse {
                vectors: paginated,
                total,
            }))
        }
        Err(e) => Json(ApiResponse::error(&e.to_string())),
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SetTtlRequest {
    pub seconds: u64,
}

async fn set_vector_ttl(
    State(state): State<Arc<ApiState>>,
    axum::extract::Path((name, id)): axum::extract::Path<(String, String)>,
    Json(req): Json<SetTtlRequest>,
) -> Json<ApiResponse<()>> {
    let db = state.db.read().await;
    match db.set_vector_ttl(&name, &id, req.seconds).await {
        Ok(()) => Json(ApiResponse::success(())),
        Err(e) => Json(ApiResponse::error(&e.to_string())),
    }
}

async fn remove_vector_ttl(
    State(state): State<Arc<ApiState>>,
    axum::extract::Path((name, id)): axum::extract::Path<(String, String)>,
) -> Json<ApiResponse<()>> {
    let db = state.db.read().await;
    match db.remove_vector_ttl(&name, &id).await {
        Ok(()) => Json(ApiResponse::success(())),
        Err(e) => Json(ApiResponse::error(&e.to_string())),
    }
}

async fn purge_expired(State(state): State<Arc<ApiState>>) -> Json<ApiResponse<usize>> {
    let db = state.db.read().await;
    match db.purge_expired().await {
        Ok(n) => Json(ApiResponse::success(n)),
        Err(e) => Json(ApiResponse::error(&e.to_string())),
    }
}

async fn upsert_vectors(
    State(state): State<Arc<ApiState>>,
    axum::extract::Path(name): axum::extract::Path<String>,
    Json(req): Json<InsertVectorsRequest>,
) -> Json<ApiResponse<UpsertVectorsResponse>> {
    let db = state.db.read().await;

    let vectors: Vec<(String, Vec<f32>, serde_json::Value)> = req.vectors
        .into_iter()
        .map(|v| (v.id, v.vector, v.metadata.unwrap_or(serde_json::json!({}))))
        .collect();

    match db.upsert_vectors(&name, vectors).await {
        Ok((inserted_ids, updated_ids)) => {
            let inserted_count = inserted_ids.len();
            let updated_count = updated_ids.len();
            Json(ApiResponse::success(UpsertVectorsResponse {
                status: "ok".to_string(),
                inserted_ids,
                updated_ids,
                inserted_count,
                updated_count,
            }))
        }
        Err(e) => Json(ApiResponse::error(&e.to_string())),
    }
}

async fn clear_collection(
    State(state): State<Arc<ApiState>>,
    axum::extract::Path(name): axum::extract::Path<String>,
) -> Json<ApiResponse<ClearCollectionResponse>> {
    let db = state.db.read().await;

    match db.clear_collection(&name).await {
        Ok(deleted_count) => Json(ApiResponse::success(ClearCollectionResponse {
            status: "ok".to_string(),
            deleted_count,
        })),
        Err(e) => Json(ApiResponse::error(&e.to_string())),
    }
}

async fn rename_collection(
    State(state): State<Arc<ApiState>>,
    axum::extract::Path(name): axum::extract::Path<String>,
    Json(req): Json<RenameCollectionRequest>,
) -> Json<ApiResponse<String>> {
    let db = state.db.read().await;

    match db.rename_collection(&name, &req.new_name).await {
        Ok(_) => Json(ApiResponse::success(format!("Collection '{}' renamed to '{}'", name, req.new_name))),
        Err(e) => Json(ApiResponse::error(&e.to_string())),
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BackupRequest {
    pub name: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BackupResponse {
    pub status: String,
    pub backup_name: String,
    pub file_count: usize,
    pub total_bytes: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RestoreRequest {
    pub backup_name: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RestoreResponse {
    pub status: String,
    pub files_restored: usize,
    pub message: String,
}

async fn create_backup(
    State(state): State<Arc<ApiState>>,
    Extension(caller): Extension<Caller>,
    Json(req): Json<BackupRequest>,
) -> Json<ApiResponse<BackupResponse>> {
    let db = state.db.read().await;
    let install_root = std::path::Path::new(&db.config.base_dir);
    let backup_name = req.name.unwrap_or_else(|| {
        format!("backup_{}", chrono::Utc::now().format("%Y%m%d_%H%M%S"))
    });
    let backup_dir = install_root
        .join("data")
        .join("backup")
        .join("full")
        .join(&backup_name);

    let result = crate::coretex_cli::data_backup::create(install_root, &backup_dir);

    if let Some(audit) = &state.audit {
        let (ok, err) = match &result {
            Ok(_) => (true, None),
            Err(e) => (false, Some(e.clone())),
        };
        audit
            .log_request(
                AuditLevel::Info,
                AuditAction::Admin,
                &format!("backup:{backup_name}"),
                caller.user_id.as_deref(),
                caller.ip.as_deref(),
                ok,
                err.as_deref(),
            )
            .await;
    }

    match result {
        Ok(manifest) => {
            let total_bytes: u64 = manifest.files.iter().map(|f| f.bytes).sum();
            Json(ApiResponse::success(BackupResponse {
                status: "ok".to_string(),
                backup_name,
                file_count: manifest.files.len(),
                total_bytes,
            }))
        }
        Err(e) => Json(ApiResponse::error(&e)),
    }
}

async fn restore_backup(
    State(state): State<Arc<ApiState>>,
    Extension(caller): Extension<Caller>,
    Json(req): Json<RestoreRequest>,
) -> Json<ApiResponse<RestoreResponse>> {
    let db = state.db.read().await;
    let install_root = std::path::Path::new(&db.config.base_dir);
    let backup_dir = install_root
        .join("data")
        .join("backup")
        .join("full")
        .join(&req.backup_name);

    let outcome = if !backup_dir.exists() {
        Err("Backup not found".to_string())
    } else {
        crate::coretex_cli::data_backup::restore(&backup_dir, install_root)
            .map(|(manifest, count)| (manifest.created_at, count))
            .map_err(|e| e)
    };

    // Restore overwrites live data with a snapshot. If one is ever triggered by
    // someone who should not have been able to, the only way to find out is a
    // record that it happened and who did it.
    if let Some(audit) = &state.audit {
        let (ok, err) = match &outcome {
            Ok(_) => (true, None),
            Err(e) => (false, Some(e.clone())),
        };
        audit
            .log_request(
                AuditLevel::Warning,
                AuditAction::Admin,
                &format!("restore:{}", req.backup_name),
                caller.user_id.as_deref(),
                caller.ip.as_deref(),
                ok,
                err.as_deref(),
            )
            .await;
    }

    match outcome {
        Ok((created_at, count)) => Json(ApiResponse::success(RestoreResponse {
            status: "ok".to_string(),
            files_restored: count,
            message: format!("Restored {} files from backup '{}'", count, created_at),
        })),
        Err(e) => Json(ApiResponse::error(&e)),
    }
}

async fn list_backups(
    State(state): State<Arc<ApiState>>,
) -> Json<ApiResponse<Vec<String>>> {
    let db = state.db.read().await;
    let backups_root = std::path::Path::new(&db.config.base_dir)
        .join("data")
        .join("backup");

    if !backups_root.exists() {
        return Json(ApiResponse::success(Vec::new()));
    }

    let mut backups = Vec::new();
    for kind in ["full", "incremental"] {
        let kind_dir = backups_root.join(kind);
        if let Ok(entries) = std::fs::read_dir(&kind_dir) {
            for entry in entries.filter_map(|e| e.ok()) {
                if entry.path().is_dir() {
                    if let Some(name) = entry.file_name().to_str() {
                        backups.push(format!("{}/{}", kind, name));
                    }
                }
            }
        }
    }
    backups.sort();
    backups.reverse();
    Json(ApiResponse::success(backups))
}
