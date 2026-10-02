//! REST API for CoreTexDB

use axum::{
    extract::Request,
    http::{header, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post, delete, put},
    Json, Router, extract::State,
};
use tower_http::cors::{AllowOrigin, Any, CorsLayer};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::net::SocketAddr;
use tokio::sync::RwLock;

use crate::{CoreTexDB, DbConfig};
use crate::coretex_auth::{AuthService, Permission, RateLimiter};
use crate::coretex_core::Result;

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

    let state = Arc::new(ApiState {
        db,
        auth: auth.clone(),
        rate_limiter: rate_limiter.clone(),
        enable_auth: config.enable_auth,
    });

    let mut app = Router::new()
        .route("/console", get(serve_console))
        .route("/health", get(health_check))
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
        .route("/replication/entries", get(replication_entries))
        .with_state(state.clone());

    // 启用认证中间件
    if config.enable_auth {
        let auth_clone = auth.clone();
        let rl_clone = rate_limiter.clone();
        app = app.layer(middleware::from_fn(move |req, next| {
            let auth = auth_clone.clone();
            let rl = rl_clone.clone();
            async move { auth_middleware(req, next, auth, rl).await }
        }));
    } else if rate_limiter.is_some() {
        // 即使没启用认证也启用速率限制
        let rl_clone = rate_limiter.clone();
        app = app.layer(middleware::from_fn(move |req, next| {
            let rl = rl_clone.clone();
            async move { rate_limit_middleware(req, next, rl).await }
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

    let addr = SocketAddr::new(
        config.address.parse().unwrap(),
        config.port,
    );

    println!("Starting CoreTexDB API server on http://{}", addr);
    println!("Auth enabled: {}", config.enable_auth);
    println!("Rate limit: {} req/min", config.rate_limit_per_minute);
    println!("API endpoints:");
    println!("  GET  /console                              - Browser console");
    println!("  GET  /health                              - Health check");
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
        Ok(_claims) => Ok(next.run(req).await),
        Err(e) => Ok((StatusCode::UNAUTHORIZED, Json(serde_json::json!({
            "status": "error",
            "error": format!("Invalid token: {}", e)
        }))).into_response()),
    }
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
    Json(req): Json<LoginRequest>,
) -> Json<ApiResponse<LoginResponse>> {
    match state.auth.authenticate(&req.username, &req.password).await {
        Ok(token) => Json(ApiResponse::success(LoginResponse {
            token: token.token,
            user_id: token.user_id,
            expires_in: 86400, // 24 小时
        })),
        Err(e) => Json(ApiResponse::error(&e)),
    }
}

async fn register(
    State(state): State<Arc<ApiState>>,
    req: Request,
) -> Json<ApiResponse<String>> {
    // 安全修复：注册端点要求 admin token 鉴权，或在系统无任何用户时
    // （首次启动）允许无鉴权注册第一个用户。
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
    let (parts, body) = req.into_parts();
    let bytes = match axum::body::to_bytes(body, 64 * 1024).await {
        Ok(b) => b,
        Err(e) => return Json(ApiResponse::error(&format!("Invalid body: {}", e))),
    };
    let parsed = serde_json::from_slice::<LoginRequest>(&bytes);
    let req = match parsed {
        Ok(r) => r,
        Err(e) => return Json(ApiResponse::error(&format!("Invalid JSON: {}", e))),
    };
    let _ = parts; // 抑制未使用警告
    match state.auth.create_user(&req.username, &req.password, None).await {
        Ok(user_id) => Json(ApiResponse::success(user_id)),
        Err(e) => Json(ApiResponse::error(&e)),
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
    Json(req): Json<CreateCollectionRequest>,
) -> Json<ApiResponse<CollectionInfo>> {
    let db = state.db.read().await;
    let metric = req.distance_metric.unwrap_or_else(|| "cosine".to_string());
    
    match db.create_collection(&req.name, req.dimension, &metric).await {
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
    axum::extract::Path(name): axum::extract::Path<String>,
) -> Json<ApiResponse<String>> {
    let db = state.db.read().await;
    
    match db.delete_collection(&name).await {
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

    match crate::coretex_cli::data_backup::create(install_root, &backup_dir) {
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
    Json(req): Json<RestoreRequest>,
) -> Json<ApiResponse<RestoreResponse>> {
    let db = state.db.read().await;
    let install_root = std::path::Path::new(&db.config.base_dir);
    let backup_dir = install_root
        .join("data")
        .join("backup")
        .join("full")
        .join(&req.backup_name);

    if !backup_dir.exists() {
        return Json(ApiResponse::error("Backup not found"));
    }

    match crate::coretex_cli::data_backup::restore(&backup_dir, install_root) {
        Ok((manifest, count)) => {
            Json(ApiResponse::success(RestoreResponse {
                status: "ok".to_string(),
                files_restored: count,
                message: format!("Restored {} files from backup '{}'", count, manifest.created_at),
            }))
        }
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
