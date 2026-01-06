//! Distributed mode for CortexDB

#[cfg(feature = "distributed")]
pub mod cluster;
#[cfg(feature = "distributed")]
pub mod sharding;
#[cfg(feature = "distributed")]
pub mod coordinator;
#[cfg(feature = "distributed")]
pub mod metadata;

#[cfg(feature = "distributed")]
pub use cluster::ClusterManager;
#[cfg(feature = "distributed")]
pub use coordinator::QueryCoordinator;
#[cfg(feature = "distributed")]
pub use sharding::ShardingStrategy;

#[cfg(not(feature = "distributed"))]
pub struct ClusterManager {}
#[cfg(not(feature = "distributed"))]
pub struct QueryCoordinator {}
#[cfg(not(feature = "distributed"))]
pub struct ShardingStrategy {}

impl ClusterManager {
    #[cfg(feature = "distributed")]
    pub fn new() -> Self {
        cluster::ClusterManager::new()
    }

    #[cfg(not(feature = "distributed"))]
    pub fn new() -> Self {
        Self {}
    }
}

impl QueryCoordinator {
    #[cfg(feature = "distributed")]
    pub fn new() -> Self {
        coordinator::QueryCoordinator::new()
    }

    #[cfg(not(feature = "distributed"))]
    pub fn new() -> Self {
        Self {}
    }
}

impl ShardingStrategy {
    #[cfg(feature = "distributed")]
    pub fn new() -> Self {
        sharding::ShardingStrategy::new()
    }

    #[cfg(not(feature = "distributed"))]
    pub fn new() -> Self {
        Self {}
    }
}
