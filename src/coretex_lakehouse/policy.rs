//! Tiering Policies for Vector Lakehouse
//! Implements data migration strategies based on access patterns and time

use crate::coretex_lakehouse::tier::{StorageTier, DocumentMeta, TierConfig};
use chrono::Utc;

pub trait TieringPolicy: Send + Sync {
    fn determine_tier(&self, doc: &DocumentMeta) -> StorageTier;
}

pub struct LRUTieringPolicy {
    config: TierConfig,
}

impl LRUTieringPolicy {
    pub fn new(config: TierConfig) -> Self {
        Self { config }
    }
}

impl TieringPolicy for LRUTieringPolicy {
    fn determine_tier(&self, doc: &DocumentMeta) -> StorageTier {
        if doc.access_count >= self.config.hot_threshold_access_count as u32 {
            StorageTier::Hot
        } else if doc.access_count > 0 {
            StorageTier::Warm
        } else {
            StorageTier::Cold
        }
    }
}

pub struct TTLTieringPolicy {
    config: TierConfig,
}

impl TTLTieringPolicy {
    pub fn new(config: TierConfig) -> Self {
        Self { config }
    }
}

impl TieringPolicy for TTLTieringPolicy {
    fn determine_tier(&self, doc: &DocumentMeta) -> StorageTier {
        let age_days = (Utc::now() - doc.created_at).num_days() as u32;
        
        if age_days < self.config.hot_threshold_days {
            StorageTier::Hot
        } else if age_days < self.config.warm_threshold_days {
            StorageTier::Warm
        } else {
            StorageTier::Cold
        }
    }
}

pub struct HybridTieringPolicy {
    config: TierConfig,
    lru_weight: f32,
    ttl_weight: f32,
}

impl HybridTieringPolicy {
    pub fn new(config: TierConfig) -> Self {
        Self {
            config,
            lru_weight: 0.6,
            ttl_weight: 0.4,
        }
    }

    pub fn with_weights(mut self, lru: f32, ttl: f32) -> Self {
        self.lru_weight = lru;
        self.ttl_weight = ttl;
        self
    }
}

impl TieringPolicy for HybridTieringPolicy {
    fn determine_tier(&self, doc: &DocumentMeta) -> StorageTier {
        let lru_score = if doc.access_count >= self.config.hot_threshold_access_count as u32 {
            1.0
        } else if doc.access_count > 0 {
            0.5
        } else {
            0.0
        };

        let age_days = (Utc::now() - doc.created_at).num_days() as u32;
        let ttl_score = if age_days < self.config.hot_threshold_days {
            1.0
        } else if age_days < self.config.warm_threshold_days {
            0.5
        } else {
            0.0
        };

        let combined_score = lru_score * self.lru_weight + ttl_score * self.ttl_weight;

        if combined_score >= 0.75 {
            StorageTier::Hot
        } else if combined_score >= 0.25 {
            StorageTier::Warm
        } else {
            StorageTier::Cold
        }
    }
}

pub struct SizeBasedPolicy {
    config: TierConfig,
    current_hot_size: u64,
}

impl SizeBasedPolicy {
    pub fn new(config: TierConfig) -> Self {
        Self {
            config,
            current_hot_size: 0,
        }
    }

    pub fn update_size(&mut self, size: u64) {
        self.current_hot_size = size;
    }
}

impl TieringPolicy for SizeBasedPolicy {
    fn determine_tier(&self, doc: &DocumentMeta) -> StorageTier {
        let max_hot_bytes = self.config.max_hot_size_gb * 1024 * 1024 * 1024;
        
        if self.current_hot_size < max_hot_bytes {
            StorageTier::Hot
        } else {
            StorageTier::Cold
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lru_policy() {
        let config = TierConfig::default();
        let policy = LRUTieringPolicy::new(config);
        
        let mut hot_doc = DocumentMeta::new("doc1".to_string(), "test".to_string());
        hot_doc.access_count = 200;
        
        let mut warm_doc = DocumentMeta::new("doc2".to_string(), "test".to_string());
        warm_doc.access_count = 50;
        
        let mut cold_doc = DocumentMeta::new("doc3".to_string(), "test".to_string());
        cold_doc.access_count = 0;
        
        assert_eq!(policy.determine_tier(&hot_doc), StorageTier::Hot);
        assert_eq!(policy.determine_tier(&warm_doc), StorageTier::Warm);
        assert_eq!(policy.determine_tier(&cold_doc), StorageTier::Cold);
    }

    #[test]
    fn test_ttl_policy() {
        let config = TierConfig::default();
        let policy = TTLTieringPolicy::new(config);
        
        let mut recent_doc = DocumentMeta::new("doc1".to_string(), "test".to_string());
        
        let old_doc = DocumentMeta::new("doc2".to_string(), "test".to_string());
        
        assert_eq!(policy.determine_tier(&recent_doc), StorageTier::Hot);
    }
}
