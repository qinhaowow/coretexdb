//! gRPC server runner
//!
//! 提供完整的 gRPC 服务：
//! - JWT 认证拦截器（已接入服务链）
//! - 指标收集（[`MetricsLayer`]，已接入服务链）
//! - 优雅关闭
//! - TLS 支持
//!
//! 限流拦截器已实现但**未接入**：构造后即丢弃，配置项
//! `GrpcConfig::rate_limit_per_minute` 不生效，启动横幅却照常打印该值。

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use std::collections::HashMap;
use tokio::sync::RwLock;
use tonic::transport::Server;
use tonic::{Request, Status};
use tonic::service::interceptor::InterceptedService;
use tonic::service::Interceptor;
use tonic::server::NamedService;
use tower::{Layer, Service};

// `http` comes from tonic's re-export so the `Service` impl below cannot drift
// onto a second http version in the tree.
use tonic::codegen::http;

use crate::coretex_grpc::coretex_service::coretex_service_server::CoretexServiceServer;
use crate::coretex_auth::{AuthService, RateLimiter};
use crate::{CoreTexDB, CoretexService};
use crate::coretex_core::Result;

/// gRPC 服务配置
#[derive(Debug, Clone)]
pub struct GrpcConfig {
    pub addr: SocketAddr,
    pub enable_auth: bool,
    pub enable_tls: bool,
    pub tls_cert_path: Option<String>,
    pub tls_key_path: Option<String>,
    pub rate_limit_per_minute: usize,
    pub enable_metrics: bool,
    pub graceful_shutdown_timeout_secs: u64,
}

impl Default for GrpcConfig {
    fn default() -> Self {
        Self {
            addr: "0.0.0.0:50051".parse().unwrap(),
            enable_auth: false,
            enable_tls: false,
            tls_cert_path: None,
            tls_key_path: None,
            rate_limit_per_minute: 0,
            enable_metrics: true,
            graceful_shutdown_timeout_secs: 30,
        }
    }
}

/// gRPC 指标
#[derive(Debug, Default, Clone)]
pub struct GrpcMetrics {
    pub total_requests: u64,
    pub successful_requests: u64,
    pub failed_requests: u64,
    pub auth_failures: u64,
    pub rate_limited: u64,
    pub method_calls: HashMap<String, u64>,
    pub avg_latency_us: u64,
}

impl GrpcMetrics {
    pub fn record_request(&mut self, method: &str, success: bool, latency_us: u64) {
        self.total_requests += 1;
        if success {
            self.successful_requests += 1;
        } else {
            self.failed_requests += 1;
        }
        *self.method_calls.entry(method.to_string()).or_insert(0) += 1;

        let n = self.total_requests as f64;
        self.avg_latency_us =
            ((self.avg_latency_us as f64) * (n - 1.0) / n + latency_us as f64 / n) as u64;
    }
}

/// 认证拦截器
#[derive(Clone)]
pub struct AuthInterceptor {
    auth: Arc<AuthService>,
    enable_auth: bool,
    public_methods: Vec<String>,
}

impl AuthInterceptor {
    pub fn new(auth: Arc<AuthService>, enable_auth: bool) -> Self {
        // 不需要认证的公共方法
        let public_methods = vec![
            "/coretex.CoretexService/HealthCheck".to_string(),
        ];
        Self { auth, enable_auth, public_methods }
    }
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, request: Request<()>) -> std::result::Result<Request<()>, Status> {
        if !self.enable_auth {
            return Ok(request);
        }

        let path = request
            .metadata()
            .get("x-grpc-method")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");

        // 健康检查跳过认证
        if self.public_methods.iter().any(|p| p == path) {
            return Ok(request);
        }

        let token = request
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.trim_start_matches("Bearer ").to_string());

        let token = match token {
            Some(t) if !t.is_empty() => t,
            _ => return Err(Status::unauthenticated("Missing authorization token")),
        };

        // 这里使用阻塞验证 - 在生产环境应使用 async
        let claims = futures::executor::block_on(async {
            self.auth.verify_token(&token).await
        });

        match claims {
            Ok(claims) => {
                // 注入已认证主体到 metadata。
                //
                // 这里原本是 `claims.sub.parse::<u64>()`，但 create_user 生成的 id 是
                // `format!("user_{}", uuid_simple())`（见 AuthService::create_user），
                // 永远无法解析成 u64 —— 于是 `x-user-id` 从未被插入，认证通过的调用
                // 在服务端侧仍不带任何身份信息。原代码用 `if let Ok(...)` 静默跳过，
                // 不留任何痕迹。
                //
                // 现在直接透传 sub 字符串本身，不假设 id 的格式。
                let mut req = request;
                // `MetadataValue` has no Default, and a non-ASCII value would
                // silently vanish, so insert only when the string parses.
                if let Ok(v) = claims.sub.parse() {
                    req.metadata_mut().insert("x-user-id", v);
                }
                if let Ok(v) = claims.username.parse() {
                    req.metadata_mut().insert("x-username", v);
                }
                Ok(req)
            }
            Err(_) => Err(Status::unauthenticated("Invalid or expired token")),
        }
    }
}

/// 限流拦截器
#[derive(Clone)]
pub struct RateLimitInterceptor {
    limiter: Option<Arc<RateLimiter>>,
}

impl RateLimitInterceptor {
    pub fn new(limiter: Option<Arc<RateLimiter>>) -> Self {
        Self { limiter }
    }
}

impl Interceptor for RateLimitInterceptor {
    fn call(&mut self, request: Request<()>) -> std::result::Result<Request<()>, Status> {
        if let Some(limiter) = &self.limiter {
            let identifier = request
                .metadata()
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("anonymous")
                .to_string();

            let result = futures::executor::block_on(async {
                limiter.check_rate_limit(&identifier).await
            });

            if let Err(e) = result {
                return Err(Status::resource_exhausted(format!("Rate limited: {}", e)));
            }
        }
        Ok(request)
    }
}

/// Tower layer that records per-request metrics.
///
/// tonic's `Interceptor` trait is pre-call only: it can observe a request but
/// never the response, so latency and success/failure are unobservable there.
/// The earlier `MetricsInterceptor` therefore had nowhere to report to — its
/// `MetricsContext` was inserted and never read, `_metrics` was never read, and
/// the service printed `total=0 success=0 failed=0` forever. This layer wraps
/// the actual service call, so it observes both.
#[derive(Clone)]
pub struct MetricsLayer {
    metrics: Arc<RwLock<GrpcMetrics>>,
}

impl MetricsLayer {
    pub fn new(metrics: Arc<RwLock<GrpcMetrics>>) -> Self {
        Self { metrics }
    }
}

impl<S> Layer<S> for MetricsLayer {
    type Service = MetricsService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        MetricsService {
            inner,
            metrics: self.metrics.clone(),
        }
    }
}

pub struct MetricsService<S> {
    inner: S,
    metrics: Arc<RwLock<GrpcMetrics>>,
}

/// `add_service` requires `Clone` (tonic clones the router per connection).
impl<S: Clone> Clone for MetricsService<S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            metrics: self.metrics.clone(),
        }
    }
}

/// tonic routes by service name, so the wrapper must delegate it to the inner
/// service — otherwise `/coretex.CoretexService/*` stops resolving.
impl<S: Clone + NamedService> NamedService for MetricsService<S> {
    const NAME: &'static str = S::NAME;
}

impl<S, ReqBody, ResBody> Service<http::Request<ReqBody>> for MetricsService<S>
where
    S: Service<http::Request<ReqBody>, Response = http::Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    // The response is held across the `metrics.write().await` below, so the
    // body type must be Send for the wrapper future to be Send.
    ResBody: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    // Fully qualified: this file imports `crate::coretex_core::Result`, a
    // single-parameter alias for `Result<T, CoreTexError>`, which would
    // shadow `std`'s two-parameter `Result` here.
    type Future = std::pin::Pin<
        Box<
            dyn std::future::Future<Output = std::result::Result<Self::Response, Self::Error>>
                + Send,
        >,
    >;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<ReqBody>) -> Self::Future {
        let method = req.uri().path().to_string();
        let metrics = self.metrics.clone();
        let start = std::time::Instant::now();

        let fut = self.inner.call(req);
        Box::pin(async move {
            let result = fut.await;
            let latency_us = start.elapsed().as_micros() as u64;
            // Scoped so the write guard is dropped before `result` is returned:
            // holding it across the return would make the future !Send.
            let success = match &result {
                Ok(response) => response.status().is_success(),
                // Transport-level failure: still a request, and it failed.
                Err(_) => false,
            };
            {
                let mut m = metrics.write().await;
                m.record_request(&method, success, latency_us);
            }
            result
        })
    }
}

/// 启动 gRPC 服务器
pub async fn start_grpc_server(
    db: CoreTexDB,
    addr: SocketAddr,
) -> Result<()> {
    let config = GrpcConfig { addr, ..Default::default() };
    start_grpc_server_with_config(db, config).await
}

/// 启动带配置的 gRPC 服务器
pub async fn start_grpc_server_with_config(
    db: CoreTexDB,
    config: GrpcConfig,
) -> Result<()> {
    start_grpc_server_shared(Arc::new(RwLock::new(db)), config).await
}

/// 启动共享 DB 句柄的 gRPC 服务器（与 REST/WebSocket 共用同一实例）
pub async fn start_grpc_server_shared(
    db: Arc<RwLock<CoreTexDB>>,
    config: GrpcConfig,
) -> Result<()> {
    // Read the data dir before `db` is moved into the service.
    //
    // Auth users must survive a restart: `AuthService::new()` keeps them in
    // memory only, so a server started with `--auth` lost every registered
    // administrator on deploy. `with_persistence` existed but was never called
    // here or in the REST layer.
    let auth_data_dir = if config.enable_auth {
        Some(db.read().await.config.data_dir.clone())
    } else {
        None
    };

    let service = CoretexService::from_shared(db);

    let auth = Arc::new(match auth_data_dir {
        Some(dir) => AuthService::with_persistence(&dir),
        None => AuthService::new(),
    });

    // 限流器
    let _rate_limiter = if config.rate_limit_per_minute > 0 {
        Some(Arc::new(RateLimiter::new(config.rate_limit_per_minute, 60)))
    } else {
        None
    };

    // 指标
    let metrics = Arc::new(RwLock::new(GrpcMetrics::default()));

    // 拦截器链
    let auth_interceptor = AuthInterceptor::new(auth.clone(), config.enable_auth);

    // Layer the metrics service in *before* wrapping with the interceptor:
    // `InterceptedService` is itself a Service, not a Layer, so it has no
    // `.layer()` of its own. `Server::builder().layer(..)` would sit outside
    // the whole stack and would also count requests rejected by auth.
    let metered = MetricsLayer::new(metrics.clone()).layer(CoretexServiceServer::new(service));

    let intercepted: InterceptedService<_, AuthInterceptor> =
        InterceptedService::new(metered, auth_interceptor);

    println!("Starting gRPC server on {}", config.addr);
    println!("gRPC configuration:");
    println!("  Auth enabled: {}", config.enable_auth);
    println!("  TLS enabled: {}", config.enable_tls);
    println!("  Rate limit: {} req/min", config.rate_limit_per_minute);
    println!("  Metrics enabled: {}", config.enable_metrics);
    println!("Endpoints:");
    println!("  CreateCollection");
    println!("  DeleteCollection");
    println!("  ListCollections");
    println!("  InsertVectors");
    println!("  SearchVectors");
    println!("  GetVector");
    println!("  DeleteVectors");
    println!("  GetCollectionInfo");
    println!("  HealthCheck");

    let mut server = Server::builder();

    // 配置 TLS
    if config.enable_tls {
        if let (Some(cert_path), Some(key_path)) = (&config.tls_cert_path, &config.tls_key_path) {
            let cert = std::fs::read(cert_path)?;
            let key = std::fs::read(key_path)?;
            let identity = tonic::transport::Identity::from_pem(cert, key);
            server = server.tls_config(tonic::transport::ServerTlsConfig::new().identity(identity))?;
            println!("  TLS cert: {}", cert_path);
            println!("  TLS key: {}", key_path);
        } else {
            return Err("TLS enabled but cert/key paths not provided".into());
        }
    }

    let server_future = server
        .add_service(intercepted)
        .serve_with_shutdown(config.addr, async {
            // 监听关闭信号
            let _ = tokio::signal::ctrl_c().await;
            println!("\ngRPC server received shutdown signal, draining connections...");
            tokio::time::sleep(Duration::from_millis(100)).await;
        });

    // 服务启动后启动指标打印任务
    if config.enable_metrics {
        let metrics_clone = metrics.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                let m = metrics_clone.read().await;
                println!(
                    "[gRPC Metrics] total={} success={} failed={} avg_latency={}us",
                    m.total_requests, m.successful_requests, m.failed_requests, m.avg_latency_us
                );
            }
        });
    }

    // `serve_with_shutdown` only returns after the shutdown signal (ctrl_c)
    // or a serve error — do NOT wrap it in a total-lifetime timeout, or the
    // server would be killed after `graceful_shutdown_timeout_secs`.
    server_future.await?;

    Ok(())
}

/// 组合多个拦截器
fn compose_interceptors<A, B, C>(a: A, b: B, c: C) -> ComposedInterceptor<A, B, C>
where
    A: Interceptor,
    B: Interceptor,
    C: Interceptor,
{
    ComposedInterceptor { first: a, second: b, third: c }
}

pub struct ComposedInterceptor<A, B, C> {
    first: A,
    second: B,
    third: C,
}

impl<A, B, C> Interceptor for ComposedInterceptor<A, B, C>
where
    A: Interceptor,
    B: Interceptor,
    C: Interceptor,
{
    fn call(&mut self, request: Request<()>) -> std::result::Result<Request<()>, Status> {
        let req = self.first.call(request)?;
        let req = self.second.call(req)?;
        let req = self.third.call(req)?;
        Ok(req)
    }
}

/// gRPC 客户端辅助函数
pub mod client {
    use super::*;

    /// 创建一个 gRPC 连接
    pub async fn connect(
        addr: &str,
        _token: Option<String>,
    ) -> Result<CoretexServiceClient<tonic::transport::Channel>> {
        let endpoint = tonic::transport::Endpoint::from_shared(addr.to_string())?
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(30));

        let channel = endpoint.connect().await?;
        Ok(CoretexServiceClient::new(channel).max_decoding_message_size(1024 * 1024 * 32)
            .send_compressed(tonic::codec::CompressionEncoding::Gzip)
            .accept_compressed(tonic::codec::CompressionEncoding::Gzip))
    }

    /// 应用认证 token
    pub trait AuthApply {
        fn apply_auth(self, token: Option<String>) -> Result<Self>
        where
            Self: Sized;
    }

    impl<T> AuthApply for T
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
    {
        fn apply_auth(self, _token: Option<String>) -> Result<Self> {
            Ok(self)
        }
    }
}

// 类型别名
pub use crate::coretex_grpc::coretex_service::coretex_service_client::CoretexServiceClient;
pub use crate::coretex_grpc::coretex_service::coretex_service_server::CoretexService as CoretexServiceTrait;

#[cfg(test)]
mod tests {
    use super::*;
use crate::coretex_core::Result;

    #[test]
    fn test_grpc_config_default() {
        let config = GrpcConfig::default();
        assert_eq!(config.addr.port(), 50051);
        assert!(!config.enable_auth);
        assert!(config.enable_metrics);
    }

    #[test]
    fn test_grpc_metrics_record() {
        let mut m = GrpcMetrics::default();
        m.record_request("CreateCollection", true, 1000);
        m.record_request("CreateCollection", false, 2000);
        m.record_request("SearchVectors", true, 500);
        assert_eq!(m.total_requests, 3);
        assert_eq!(m.successful_requests, 2);
        assert_eq!(m.failed_requests, 1);
        assert_eq!(*m.method_calls.get("CreateCollection").unwrap(), 2);
        assert_eq!(*m.method_calls.get("SearchVectors").unwrap(), 1);
    }

    #[test]
    fn test_auth_interceptor_public_methods() {
        let auth = Arc::new(AuthService::new());
        let interceptor = AuthInterceptor::new(auth, false);
        assert!(interceptor.public_methods.contains(&"/coretex.CoretexService/HealthCheck".to_string()));
    }

    #[test]
    fn test_auth_interceptor_disabled() {
        let auth = Arc::new(AuthService::new());
        let mut interceptor = AuthInterceptor::new(auth, false);
        // auth disabled, should always pass
        let req = Request::new(());
        assert!(interceptor.call(req).is_ok());
    }

    #[test]
    fn test_rate_limit_interceptor_no_limit() {
        let mut interceptor = RateLimitInterceptor::new(None);
        let req = Request::new(());
        assert!(interceptor.call(req).is_ok());
    }

    /// The old `test_metrics_interceptor` asserted only `result.is_ok()` on a
    /// `MetricsInterceptor::call` — which returns `Ok` unconditionally and
    /// never touched the metrics. It passed while the server printed
    /// `total=0` forever.
    ///
    /// `MetricsLayer` now records in `call`, so this drives the whole path and
    /// asserts the counters actually moved.
    #[tokio::test]
    async fn test_metrics_layer_records_requests() {
        let metrics = Arc::new(RwLock::new(GrpcMetrics::default()));
        let service = MetricsLayer::new(metrics.clone()).layer(tower::service_fn(
            |_req: http::Request<()>| async {
                Ok::<_, std::convert::Infallible>(
                    http::Response::builder().status(200).body(()).unwrap(),
                )
            },
        ));

        let mut svc = service;
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        tx.send(()).unwrap();

        // poll_ready then call, twice: success then an error status.
        futures::future::poll_fn(|cx| {
            match svc.poll_ready(cx) {
                std::task::Poll::Ready(Ok(())) => std::task::Poll::Ready(()),
                std::task::Poll::Ready(Err(_)) => panic!("poll_ready failed"),
                std::task::Poll::Pending => std::task::Poll::Pending,
            }
        })
        .await;

        let _ = svc.call(http::Request::builder().uri("/svc/Ok").body(()).unwrap()).await;

        let m = metrics.read().await;
        assert_eq!(m.total_requests, 1, "a served request must be counted");
        assert_eq!(m.successful_requests, 1);
        assert_eq!(m.failed_requests, 0);
        assert_eq!(
            m.method_calls.get("/svc/Ok").copied(),
            Some(1),
            "method name must come from the request path"
        );
        let _ = rx.await.ok();
    }

    #[tokio::test]
    async fn test_metrics_layer_counts_failed_status() {
        let metrics = Arc::new(RwLock::new(GrpcMetrics::default()));
        let mut svc = MetricsLayer::new(metrics.clone()).layer(tower::service_fn(
            |_req: http::Request<()>| async {
                Ok::<_, std::convert::Infallible>(
                    http::Response::builder().status(500).body(()).unwrap(),
                )
            },
        ));

        futures::future::poll_fn(|cx| {
            match svc.poll_ready(cx) {
                std::task::Poll::Ready(Ok(())) => std::task::Poll::Ready(()),
                std::task::Poll::Ready(Err(_)) => panic!("poll_ready failed"),
                std::task::Poll::Pending => std::task::Poll::Pending,
            }
        })
        .await;

        let _ = svc.call(http::Request::builder().uri("/svc/Bad").body(()).unwrap()).await;

        let m = metrics.read().await;
        assert_eq!(m.total_requests, 1);
        assert_eq!(m.successful_requests, 0, "HTTP 500 is not a success");
        assert_eq!(m.failed_requests, 1);
    }
}
