//! Cache utilities for CortexDB.
//!
//! This module provides caching functionality for CortexDB, including:
//! - In-memory cache implementation
//! - Cache expiration policies
//! - Cache statistics
//! - Cache key generation
//! - Cache invalidation strategies

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// Cache entry with expiration
#[derive(Debug, Clone)]
pub struct CacheEntry<V> {
    /// Cache value
    value: V,
    /// Expiration time
    expires_at: Option<Instant>,
    /// Creation time
    created_at: Instant,
    /// Last access time
    last_accessed: Instant,
    /// Access count
    access_count: usize,
}

impl<V> CacheEntry<V> {
    /// Create a new cache entry
    pub fn new(value: V, ttl: Option<Duration>) -> Self {
        let now = Instant::now();
        let expires_at = ttl.map(|d| now + d);
        
        Self {
            value,
            expires_at,
            created_at: now,
            last_accessed: now,
            access_count: 0,
        }
    }
    
    /// Get the cache value
    pub fn value(&self) -> &V {
        &self.value
    }
    
    /// Check if the entry is expired
    pub fn is_expired(&self) -> bool {
        if let Some(expires_at) = self.expires_at {
            Instant::now() > expires_at
        } else {
            false
        }
    }
    
    /// Update last accessed time and increment access count
    pub fn update_access(&mut self) {
        self.last_accessed = Instant::now();
        self.access_count += 1;
    }
}

/// Cache traits
pub trait Cache<K, V> {
    /// Get value from cache
    async fn get(&self, key: &K) -> Option<V>;
    
    /// Set value in cache
    async fn set(&self, key: K, value: V, ttl: Option<Duration>) -> Result<(), anyhow::Error>;
    
    /// Remove value from cache
    async fn remove(&self, key: &K) -> bool;
    
    /// Clear all cache entries
    async fn clear(&self) -> Result<(), anyhow::Error>;
    
    /// Get cache size
    async fn size(&self) -> usize;
    
    /// Check if key exists in cache
    async fn contains(&self, key: &K) -> bool;
}

/// In-memory cache implementation
#[derive(Debug, Clone)]
pub struct MemoryCache<K, V>
where
    K: Eq + std::hash::Hash + Clone + Send + Sync,
    V: Clone + Send + Sync,
{
    /// Cache storage
    storage: Arc<RwLock<HashMap<K, CacheEntry<V>>>>,
    /// Maximum cache size
    max_size: Option<usize>,
    /// Default TTL
    default_ttl: Option<Duration>,
}

impl<K, V> MemoryCache<K, V>
where
    K: Eq + std::hash::Hash + Clone + Send + Sync,
    V: Clone + Send + Sync,
{
    /// Create a new memory cache
    pub fn new(max_size: Option<usize>, default_ttl: Option<Duration>) -> Self {
        Self {
            storage: Arc::new(RwLock::new(HashMap::new())),
            max_size,
            default_ttl,
        }
    }
    
    /// Create a memory cache with default settings
    pub fn with_defaults() -> Self {
        Self::new(Some(1000), Some(Duration::from_secs(3600))) // 1 hour default TTL
    }
    
    /// Evict expired entries
    pub async fn evict_expired(&self) -> usize {
        let mut storage = self.storage.write().await;
        let mut removed = 0;
        
        storage.retain(|_, entry| {
            if entry.is_expired() {
                removed += 1;
                false
            } else {
                true
            }
        });
        
        removed
    }
    
    /// Evict least recently used entries to maintain max size
    pub async fn evict_lru(&self) -> usize {
        if let Some(max_size) = self.max_size {
            let mut storage = self.storage.write().await;
            
            if storage.len() > max_size {
                let mut entries: Vec<_> = storage.into_iter().collect();
                entries.sort_by(|(_, a), (_, b)| a.last_accessed.cmp(&b.last_accessed));
                
                let to_remove = entries.len() - max_size;
                let removed_entries = entries.into_iter().take(to_remove).collect::<Vec<_>>();
                
                let mut new_storage = HashMap::new();
                for (k, v) in entries.into_iter().skip(to_remove) {
                    new_storage.insert(k, v);
                }
                
                *storage = new_storage;
                return to_remove;
            }
        }
        
        0
    }
}

impl<K, V> Cache<K, V> for MemoryCache<K, V>
where
    K: Eq + std::hash::Hash + Clone + Send + Sync,
    V: Clone + Send + Sync,
{
    async fn get(&self, key: &K) -> Option<V> {
        // Evict expired entries first
        self.evict_expired().await;
        
        let mut storage = self.storage.write().await;
        
        if let Some(entry) = storage.get_mut(key) {
            if !entry.is_expired() {
                entry.update_access();
                Some(entry.value.clone())
            } else {
                storage.remove(key);
                None
            }
        } else {
            None
        }
    }
    
    async fn set(&self, key: K, value: V, ttl: Option<Duration>) -> Result<(), anyhow::Error> {
        let ttl = ttl.or(self.default_ttl);
        
        let mut storage = self.storage.write().await;
        storage.insert(key, CacheEntry::new(value, ttl));
        
        // Evict LRU if over max size
        self.evict_lru().await;
        
        Ok(())
    }
    
    async fn remove(&self, key: &K) -> bool {
        let mut storage = self.storage.write().await;
        storage.remove(key).is_some()
    }
    
    async fn clear(&self) -> Result<(), anyhow::Error> {
        let mut storage = self.storage.write().await;
        storage.clear();
        Ok(())
    }
    
    async fn size(&self) -> usize {
        // Evict expired entries first
        self.evict_expired().await;
        
        let storage = self.storage.read().await;
        storage.len()
    }
    
    async fn contains(&self, key: &K) -> bool {
        // Evict expired entries first
        self.evict_expired().await;
        
        let storage = self.storage.read().await;
        storage.contains_key(key)
    }
}

/// Cache statistics
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CacheStats {
    /// Total entries
    pub total_entries: usize,
    /// Expired entries
    pub expired_entries: usize,
    /// Hit count
    pub hit_count: u64,
    /// Miss count
    pub miss_count: u64,
    /// Hit rate
    pub hit_rate: f64,
    /// Eviction count
    pub eviction_count: u64,
    /// Average entry age
    pub avg_entry_age: f64,
    /// Average access count
    pub avg_access_count: f64,
}

/// Caching manager
pub struct CacheManager {
    /// Document cache
    document_cache: Arc<MemoryCache<String, crate::cortex_core::types::Document>>,
    /// Query result cache
    query_cache: Arc<MemoryCache<String, Vec<crate::cortex_query::builder::QueryResult>>>,
    /// Schema cache
    schema_cache: Arc<MemoryCache<String, crate::cortex_core::schema::CollectionSchema>>,
    /// Statistics
    stats: Arc<RwLock<CacheStats>>,
}

impl CacheManager {
    /// Create a new cache manager
    pub fn new() -> Self {
        Self {
            document_cache: Arc::new(MemoryCache::with_defaults()),
            query_cache: Arc::new(MemoryCache::with_defaults()),
            schema_cache: Arc::new(MemoryCache::with_defaults()),
            stats: Arc::new(RwLock::new(CacheStats {
                total_entries: 0,
                expired_entries: 0,
                hit_count: 0,
                miss_count: 0,
                hit_rate: 0.0,
                eviction_count: 0,
                avg_entry_age: 0.0,
                avg_access_count: 0.0,
            })),
        }
    }
    
    /// Get document from cache
    pub async fn get_document(&self, key: &str) -> Option<crate::cortex_core::types::Document> {
        let result = self.document_cache.get(key).await;
        self.update_stats(result.is_some()).await;
        result
    }
    
    /// Set document in cache
    pub async fn set_document(&self, key: String, document: crate::cortex_core::types::Document, ttl: Option<Duration>) -> Result<(), anyhow::Error> {
        self.document_cache.set(key, document, ttl).await
    }
    
    /// Get query result from cache
    pub async fn get_query_result(&self, key: &str) -> Option<Vec<crate::cortex_query::builder::QueryResult>> {
        let result = self.query_cache.get(key).await;
        self.update_stats(result.is_some()).await;
        result
    }
    
    /// Set query result in cache
    pub async fn set_query_result(&self, key: String, result: Vec<crate::cortex_query::builder::QueryResult>, ttl: Option<Duration>) -> Result<(), anyhow::Error> {
        self.query_cache.set(key, result, ttl).await
    }
    
    /// Get schema from cache
    pub async fn get_schema(&self, key: &str) -> Option<crate::cortex_core::schema::CollectionSchema> {
        let result = self.schema_cache.get(key).await;
        self.update_stats(result.is_some()).await;
        result
    }
    
    /// Set schema in cache
    pub async fn set_schema(&self, key: String, schema: crate::cortex_core::schema::CollectionSchema, ttl: Option<Duration>) -> Result<(), anyhow::Error> {
        self.schema_cache.set(key, schema, ttl).await
    }
    
    /// Clear all caches
    pub async fn clear_all(&self) -> Result<(), anyhow::Error> {
        self.document_cache.clear().await?;
        self.query_cache.clear().await?;
        self.schema_cache.clear().await?;
        Ok(())
    }
    
    /// Get cache statistics
    pub async fn get_stats(&self) -> CacheStats {
        let mut stats = self.stats.write().await;
        
        // Update total entries
        let total_entries = self.document_cache.size().await 
            + self.query_cache.size().await 
            + self.schema_cache.size().await;
        stats.total_entries = total_entries;
        
        stats.clone()
    }
    
    /// Update cache statistics
    async fn update_stats(&self, hit: bool) {
        let mut stats = self.stats.write().await;
        
        if hit {
            stats.hit_count += 1;
        } else {
            stats.miss_count += 1;
        }
        
        // Update hit rate
        let total = stats.hit_count + stats.miss_count;
        if total > 0 {
            stats.hit_rate = stats.hit_count as f64 / total as f64;
        }
    }
}

/// Generate cache key for document
pub fn generate_document_cache_key(collection: &str, document_id: &str) -> String {
    format!("doc:{}_{}", collection, document_id)
}

/// Generate cache key for query
pub fn generate_query_cache_key(collection: &str, query: &str) -> String {
    format!("query:{}_{}", collection, md5::compute(query))
}

/// Generate cache key for schema
pub fn generate_schema_cache_key(collection: &str) -> String {
    format!("schema:{}", collection)
}

/// Initialize cache manager
pub fn init_cache_manager() -> CacheManager {
    CacheManager::new()
}

/// Test utilities for caching
#[cfg(test)]
mod tests {
    use super::*;
    use crate::cortex_core::types::Document;

    #[test]
    fn test_cache_entry_creation() {
        let entry = CacheEntry::new("test value", Some(Duration::from_secs(1)));
        assert!(!entry.is_expired());
        assert_eq!(entry.value(), &"test value");
    }

    #[tokio::test]
    async fn test_memory_cache_basic() {
        let cache = MemoryCache::with_defaults();
        
        // Set value
        cache.set("key1", "value1", None).await.unwrap();
        
        // Get value
        let value = cache.get(&"key1").await;
        assert_eq!(value, Some("value1"));
        
        // Get non-existent key
        let value = cache.get(&"key2").await;
        assert_eq!(value, None);
        
        // Remove value
        let removed = cache.remove(&"key1").await;
        assert!(removed);
        
        // Get removed value
        let value = cache.get(&"key1").await;
        assert_eq!(value, None);
    }

    #[tokio::test]
    async fn test_memory_cache_expiration() {
        let cache = MemoryCache::new(Some(100), Some(Duration::from_millis(100)));
        
        // Set value
        cache.set("key1", "value1", None).await.unwrap();
        
        // Get value before expiration
        let value = cache.get(&"key1").await;
        assert_eq!(value, Some("value1"));
        
        // Wait for expiration
        tokio::time::sleep(Duration::from_millis(200)).await;
        
        // Get value after expiration
        let value = cache.get(&"key1").await;
        assert_eq!(value, None);
    }

    #[tokio::test]
    async fn test_cache_manager() {
        let manager = CacheManager::new();
        
        // Set document
        let document = Document::new("doc1", serde_json::json!({ "name": "test" }));
        let key = generate_document_cache_key("collection1", "doc1");
        manager.set_document(key.clone(), document, None).await.unwrap();
        
        // Get document
        let cached_doc = manager.get_document(&key).await;
        assert!(cached_doc.is_some());
        
        // Clear all caches
        manager.clear_all().await.unwrap();
        
        // Get document after clear
        let cached_doc = manager.get_document(&key).await;
        assert!(cached_doc.is_none());
    }
}
