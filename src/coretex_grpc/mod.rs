//! gRPC API server for CoreTexDB
//!
//! 提供 gRPC 服务：
//! - 完整 CRUD（CreateCollection/Insert/Search/Delete...）
//! - 认证拦截器
//! - 限流拦截器
//! - 指标拦截器
//! - TLS 支持
//! - 优雅关闭

pub mod coretex_service;
pub mod server;

pub use coretex_service::CoretexService;
pub use server::{
    start_grpc_server, start_grpc_server_with_config,
    GrpcConfig, GrpcMetrics,
    AuthInterceptor, RateLimitInterceptor, MetricsInterceptor,
    ComposedInterceptor,
};
