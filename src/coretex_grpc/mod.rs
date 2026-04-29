//! gRPC API server for CoreTexDB

pub mod coretex_service;
pub mod server;

pub use coretex_service::CoretexService;
pub use server::start_grpc_server;
