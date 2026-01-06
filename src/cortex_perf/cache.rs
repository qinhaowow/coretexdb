//! Query caching for performance optimization

use std::sync::Arc;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};
use std::time::Duration;
use lru::LruCache;

#[derive(Debug, Serialize, Deserialize, Clone, Hash, PartialEq, Eq)]
pub struct CacheKey {
    pub query_vector: Vec<f32>,
    pub k: usize,
    pub index_name: String,
    pub threshold: Option<f32>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CacheValue {
    pub results: Vec<crate::cortex_index::SearchResult>,
    pub timestamp: u64,
    pub execution_time: f64,
}

#[derive(Debug)]
pub struct QueryCache {
    cache: Arc<RwLock<LruCache<CacheKey, CacheValue>>>,
    max_size: usize,
    ttl: Duration,
}

impl QueryCache {
    pub fn new() -> Self {
        Self {
            cache: Arc::new(RwLock::new(LruCache::new(1000))),
            max_size: 1000,
            ttl: Duration::from_minutes(10),
        }
    }

    pub fn with_max_size(mut self, max_size: usize) -> Self {
        self.max_size = max_size;
        self.cache = Arc::new(RwLock::new(LruCache::new(max_size)));
        self
    }

    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }

    pub async fn get(&self, key: &CacheKey) -> Result<Option<CacheValue>, Box<dyn std::error::Error>> {
        let mut cache = self.cache.write().await;
        
        // Check if key exists
        if let Some(value) = cache.get(key) {
            // Check if value is expired
            let now = chrono::Utc::now().timestamp() as u64;
            let value_timestamp = value.timestamp;
            let ttl_seconds = self.ttl.as_secs() as u64;
            
            if now - value_timestamp < ttl_seconds {
                return Ok(Some(value.clone()));
            } else {
                // Remove expired entry
                cache.remove(key);
            }
        }
        
        Ok(None)
    }

    pub async fn set(&self, key: CacheKey, value: CacheValue) -> Result<(), Box<dyn std::error::Error>> {
        let mut cache = self.cache.write().await;
        cache.put(key, value);
        Ok(())
    }

    pub async fn invalidate(&self, index_name: &str) -> Result<(), Box<dyn std::error::Error>> {
        let mut cache = self.cache.write().await;
        
        // Invalidate all entries for this index
        let keys_to_remove: Vec<CacheKey> = cache.iter()
            .filter(|(key, _)| key.index_name == index_name)
            .map(|(key, _)| key.clone())
            .collect();
        
        for key in keys_to_remove {
            cache.remove(&key);
        }
        
        Ok(())
    }

    pub async fn clear(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut cache = self.cache.write().await;
        cache.clear();
        Ok(())
    }

    pub async fn size(&self) -> Result<usize, Box<dyn std::error::Error>> {
        let cache = self.cache.read().await;
        Ok(cache.len())
    }

    pub async fn start_cleanup_task(&self) {
        let cloned_self = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_minutes(5)).await;
                if let Err(e) = cloned_self.cleanup_expired().await {
                    eprintln!("Error cleaning up expired cache entries: {}", e);
                }
            }
        });
    }

    async fn cleanup_expired(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut cache = self.cache.write().await;
        let now = chrono::Utc::now().timestamp() as u64;
        let ttl_seconds = self.ttl.as_secs() as u64;
        
        let keys_to_remove: Vec<CacheKey> = cache.iter()
            .filter(|(_, value)| now - value.timestamp >= ttl_seconds)
            .map(|(key, _)| key.clone())
            .collect();
        
        for key in keys_to_remove {
            cache.remove(&key);
        }
        
        Ok(())
    }
}

impl Clone for QueryCache {
    fn clone(&self) -> Self {
        Self {
            cache: self.cache.clone(),
            max_size: self.max_size,
            ttl: self.ttl,
        }
    }
}
