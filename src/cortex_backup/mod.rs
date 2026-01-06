//! Backup and restore functionality for CortexDB

#[cfg(feature = "distributed")]
pub mod backup;
#[cfg(feature = "distributed")]
pub mod restore;
#[cfg(feature = "distributed")]
pub mod replication;

#[cfg(feature = "distributed")]
pub use backup::BackupManager;
#[cfg(feature = "distributed")]
pub use restore::RestoreManager;
#[cfg(feature = "distributed")]
pub use replication::ReplicationManager;

#[cfg(not(feature = "distributed"))]
pub struct BackupManager {}
#[cfg(not(feature = "distributed"))]
pub struct RestoreManager {}
#[cfg(not(feature = "distributed"))]
pub struct ReplicationManager {}

impl BackupManager {
    #[cfg(feature = "distributed")]
    pub fn new() -> Self {
        backup::BackupManager::new()
    }

    #[cfg(not(feature = "distributed"))]
    pub fn new() -> Self {
        Self {}
    }
}

impl RestoreManager {
    #[cfg(feature = "distributed")]
    pub fn new() -> Self {
        restore::RestoreManager::new()
    }

    #[cfg(not(feature = "distributed"))]
    pub fn new() -> Self {
        Self {}
    }
}

impl ReplicationManager {
    #[cfg(feature = "distributed")]
    pub fn new() -> Self {
        replication::ReplicationManager::new()
    }

    #[cfg(not(feature = "distributed"))]
    pub fn new() -> Self {
        Self {}
    }
}
