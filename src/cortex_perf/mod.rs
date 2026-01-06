//! Performance optimization for CortexDB

pub mod cache;
pub mod routing;
pub mod parallel;
pub mod optimization;

pub use cache::QueryCache;
pub use routing::QueryRouter;
pub use parallel::ParallelQueryExecutor;
pub use optimization::QueryOptimizer;
