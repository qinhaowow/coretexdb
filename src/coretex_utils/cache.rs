//! LRU Cache implementation for CoreTexDB

use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::sync::Arc;
use tokio::sync::RwLock;
use std::time::{Duration, Instant};

pub struct LRUCache<K, V> {
    capacity: usize,
    cache: HashMap<K, V>,
    access_order: VecDeque<K>,
    hits: u64,
    misses: u64,
}

impl<K: Hash + Clone + Eq, V> LRUCache<K, V> {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            cache: HashMap::new(),
            access_order: VecDeque::new(),
            hits: 0,
            misses: 0,
        }
    }

    pub fn get(&mut self, key: &K) -> Option<&V> {
        if let Some(value) = self.cache.get(key) {
            self.hits += 1;
            if let Some(pos) = self.access_order.iter().position(|k| k == key) {
                self.access_order.remove(pos);
                self.access_order.push_back(key.clone());
            }
            Some(value)
        } else {
            self.misses += 1;
            None
        }
    }

    pub fn put(&mut self, key: K, value: V) {
        if self.cache.contains_key(&key) {
            if let Some(pos) = self.access_order.iter().position(|k| k == &key) {
                self.access_order.remove(pos);
            }
        } else if self.cache.len() >= self.capacity {
            if let Some(lru_key) = self.access_order.pop_front() {
                self.cache.remove(&lru_key);
            }
        }

        self.cache.insert(key.clone(), value);
        self.access_order.push_back(key);
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        if let Some(value) = self.cache.remove(key) {
            if let Some(pos) = self.access_order.iter().position(|k| k == key) {
                self.access_order.remove(pos);
            }
            Some(value)
        } else {
            None
        }
    }

    pub fn clear(&mut self) {
        self.cache.clear();
        self.access_order.clear();
        self.hits = 0;
        self.misses = 0;
    }

    pub fn len(&self) -> usize {
        self.cache.len()
    }

    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }

    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }

    pub fn stats(&self) -> CacheStats {
        CacheStats {
            hits: self.hits,
            misses: self.misses,
            size: self.cache.len(),
            capacity: self.capacity,
            hit_rate: self.hit_rate(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub size: usize,
    pub capacity: usize,
    pub hit_rate: f64,
}

pub struct TimedLRUCache<K, V> {
    capacity: usize,
    ttl: Duration,
    cache: HashMap<K, (V, Instant)>,
    access_order: VecDeque<K>,
    hits: u64,
    misses: u64,
}

impl<K: Hash + Clone + Eq, V: Clone> TimedLRUCache<K, V> {
    pub fn new(capacity: usize, ttl: Duration) -> Self {
        Self {
            capacity,
            ttl,
            cache: HashMap::new(),
            access_order: VecDeque::new(),
            hits: 0,
            misses: 0,
        }
    }

    pub fn get(&mut self, key: &K) -> Option<V> {
        if let Some((value, instant)) = self.cache.get(key) {
            if instant.elapsed() > self.ttl {
                self.cache.remove(key);
                self.access_order.retain(|k| k != key);
                self.misses += 1;
                return None;
            }
            self.hits += 1;
            if let Some(pos) = self.access_order.iter().position(|k| k == key) {
                self.access_order.remove(pos);
            }
            self.access_order.push_back(key.clone());
            Some(value.clone())
        } else {
            self.misses += 1;
            None
        }
    }

    pub fn put(&mut self, key: K, value: V) {
        if self.cache.contains_key(&key) {
            if let Some(pos) = self.access_order.iter().position(|k| k == &key) {
                self.access_order.remove(pos);
            }
        } else if self.cache.len() >= self.capacity {
            if let Some(lru_key) = self.access_order.pop_front() {
                self.cache.remove(&lru_key);
            }
        }

        self.cache.insert(key.clone(), (value, Instant::now()));
        self.access_order.push_back(key);
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        if let Some((value, _)) = self.cache.remove(key) {
            self.access_order.retain(|k| k != key);
            Some(value)
        } else {
            None
        }
    }

    pub fn cleanup_expired(&mut self) {
        let now = Instant::now();
        let expired_keys: Vec<K> = self.cache
            .iter()
            .filter(|(_, (_, instant))| instant.elapsed() > self.ttl)
            .map(|(k, _)| k.clone())
            .collect();

        for key in expired_keys {
            self.cache.remove(&key);
            self.access_order.retain(|k| k != &key);
        }
    }

    pub fn len(&self) -> usize {
        self.cache.len()
    }

    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }

    pub fn stats(&self) -> CacheStats {
        CacheStats {
            hits: self.hits,
            misses: self.misses,
            size: self.cache.len(),
            capacity: self.capacity,
            hit_rate: self.hit_rate(),
        }
    }
}

pub struct AsyncLRUCache<K, V> {
    inner: Arc<RwLock<LRUCache<K, V>>>,
}

impl<K: Hash + Clone + Eq + Send + Sync, V: Clone + Send + Sync> AsyncLRUCache<K, V> {
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Arc::new(RwLock::new(LRUCache::new(capacity))),
        }
    }

    pub async fn get(&self, key: &K) -> Option<V> {
        let mut cache = self.inner.write().await;
        cache.get(key).cloned()
    }

    pub async fn put(&self, key: K, value: V) {
        let mut cache = self.inner.write().await;
        cache.put(key, value);
    }

    pub async fn remove(&self, key: &K) -> Option<V> {
        let mut cache = self.inner.write().await;
        cache.remove(key)
    }

    pub async fn clear(&self) {
        let mut cache = self.inner.write().await;
        cache.clear();
    }

    pub async fn len(&self) -> usize {
        let cache = self.inner.read().await;
        cache.len()
    }

    pub async fn stats(&self) -> CacheStats {
        let cache = self.inner.read().await;
        cache.stats()
    }

    pub async fn get_or_put<F: Future<Output = V>>(&self, key: K, f: F) -> V {
        if let Some(value) = self.get(&key).await {
            return value;
        }
        let value = f.await;
        self.put(key, value.clone()).await;
        value
    }
}

impl<K: Hash + Clone + Eq, V> Default for LRUCache<K, V> {
    fn default() -> Self {
        Self::new(1000)
    }
}

use std::future::Future;

pub struct MultiLevelCache<K, V> {
    l1: AsyncLRUCache<K, V>,
    l2: Arc<RwLock<Option<TimedLRUCache<K, V>>>>,
}

impl<K: Hash + Clone + Eq + Send + Sync + 'static, V: Clone + Send + Sync + 'static> MultiLevelCache<K, V> {
    pub fn new(l1_capacity: usize, l2_capacity: usize, ttl_secs: u64) -> Self {
        Self {
            l1: AsyncLRUCache::new(l1_capacity),
            l2: Arc::new(RwLock::new(Some(TimedLRUCache::new(l2_capacity, Duration::from_secs(ttl_secs))))),
        }
    }

    pub async fn get(&self, key: &K) -> Option<V> {
        if let Some(value) = self.l1.get(key).await {
            return Some(value);
        }

        let mut l2 = self.l2.write().await;
        if let Some(ref mut cache) = *l2 {
            if let Some(value) = cache.get(key) {
                self.l1.put(key.clone(), value.clone()).await;
                return Some(value);
            }
        }
        None
    }

    pub async fn put(&self, key: K, value: V) {
        self.l1.put(key.clone(), value.clone()).await;
        
        let mut l2 = self.l2.write().await;
        if let Some(ref mut cache) = *l2 {
            cache.put(key, value);
        }
    }

    pub async fn invalidate(&self, key: &K) {
        let _ = self.l1.remove(key).await;
        
        let mut l2 = self.l2.write().await;
        if let Some(ref mut cache) = *l2 {
            cache.remove(key);
        }
    }

    pub async fn stats(&self) -> MultiLevelCacheStats {
        MultiLevelCacheStats {
            l1: self.l1.stats().await,
            l2: self.l2.read().await.as_ref().map(|c| c.stats()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MultiLevelCacheStats {
    pub l1: CacheStats,
    pub l2: Option<CacheStats>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lru_cache_basic() {
        let mut cache = LRUCache::new(3);
        
        cache.put("a", 1);
        cache.put("b", 2);
        cache.put("c", 3);
        
        assert_eq!(cache.get(&"a"), Some(&1));
        assert_eq!(cache.get(&"b"), Some(&2));
        assert_eq!(cache.get(&"c"), Some(&3));
        
        cache.put("d", 4);
        
        assert!(cache.get(&"a").is_some());
        assert!(cache.get(&"d").is_some());
    }

    #[test]
    fn test_lru_eviction() {
        let mut cache = LRUCache::new(2);
        
        cache.put("a", 1);
        cache.put("b", 2);
        cache.get(&"a");
        cache.put("c", 3);
        
        assert!(cache.get(&"a").is_some());
        assert!(cache.get(&"b").is_none());
    }

    #[tokio::test]
    async fn test_async_lru_cache() {
        let cache = AsyncLRUCache::new(3);
        
        cache.put("a", 1).await;
        cache.put("b", 2).await;
        
        assert_eq!(cache.get(&"a").await, Some(1));
        assert_eq!(cache.get(&"c").await, None);
    }

    #[test]
    fn test_timed_cache() {
        let mut cache = TimedLRUCache::new(3, Duration::from_millis(100));
        
        cache.put("a", 1);
        assert_eq!(cache.get(&"a"), Some(1));
        
        std::thread::sleep(Duration::from_millis(150));
        
        assert_eq!(cache.get(&"a"), None);
    }
}
