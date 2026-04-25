//! Storage Tier Definitions for Vector Lakehouse
//! Defines hot, warm, and cold storage tiers

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StorageTier {
    Hot,
    Warm,
    Cold,
}

impl StorageTier {
    pub fn as_str(&self) -> &'static str {
        match self {
            StorageTier::Hot => "hot",
            StorageTier::Warm => "warm",
            StorageTier::Cold => "cold",
        }
    }

    pub fn priority(&self) -> u8 {
        match self {
            StorageTier::Hot => 3,
            StorageTier::Warm => 2,
            StorageTier::Cold => 1,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TierConfig {
    pub hot_threshold_access_count: usize,
    pub hot_threshold_days: u32,
    pub warm_threshold_days: u32,
    pub max_hot_size_gb: u64,
    pub max_warm_size_gb: u64,
}

impl Default for TierConfig {
    fn default() -> Self {
        Self {
            hot_threshold_access_count: 100,
            hot_threshold_days: 7,
            warm_threshold_days: 30,
            max_hot_size_gb: 100,
            max_warm_size_gb: 500,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentMeta {
    pub id: String,
    pub tier: StorageTier,
    pub size_bytes: u64,
    pub access_count: u32,
    pub last_accessed: chrono::DateTime<chrono::Utc>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    pub vector_dimension: Option<usize>,
    pub collection: String,
}

impl DocumentMeta {
    pub fn new(id: String, collection: String) -> Self {
        let now = chrono::Utc::now();
        Self {
            id,
            tier: StorageTier::Hot,
            size_bytes: 0,
            access_count: 0,
            last_accessed: now,
            created_at: now,
            updated_at: now,
            vector_dimension: None,
            collection,
        }
    }

    pub fn record_access(&mut self) {
        self.access_count += 1;
        self.last_accessed = chrono::Utc::now();
    }
}
