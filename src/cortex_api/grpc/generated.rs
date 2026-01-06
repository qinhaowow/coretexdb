//! Generated gRPC code

#[cfg(feature = "grpc")]
include!(concat!(env!("OUT_DIR"), "/cortexdb.rs"));

#[cfg(not(feature = "grpc"))]
pub mod cortexdb {}
