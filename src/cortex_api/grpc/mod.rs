//! gRPC API for CortexDB

#[cfg(feature = "grpc")]
pub mod generated;

#[cfg(feature = "grpc")]
pub mod server;

#[cfg(feature = "grpc")]
pub use server::start_server;
pub use generated::cortexdb::*;