//! Caching layer for S3 storage to improve read performance

use bytes::Bytes;
use lru::LruCache;
use proven_logger::{debug, trace};
use proven_storage::StorageNamespace;
use std::{
    collections::HashMap,
    num::{NonZero, NonZeroUsize},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::RwLock;

use crate::config::CacheConfig;

/// Type aliases to reduce type complexity
type CacheKey = (StorageNamespace, NonZero<u64>);
type LruCacheType = Arc<RwLock<LruCache<CacheKey, CacheEntry>>>;
type BloomFiltersMap = Arc<RwLock<HashMap<StorageNamespace, BloomFilter>>>;

/// Cache entry with TTL
#[derive(Clone, Debug)]
struct CacheEntry {
    /// The cached data
    data: Bytes,
    /// When this entry expires
    expires_at: Instant,
}

/// Bloom filter for existence checks
pub struct BloomFilter {
    /// Bit vector
    bits: Vec<u64>,
    /// Number of hash functions
    hash_count: usize,
    /// Size of bit vector
    size: usize,
}

impl BloomFilter {
    /// Create a new bloom filter
    pub fn new(expected_items: usize, false_positive_rate: f64) -> Self {
        // Calculate optimal size and hash count
        let size = Self::optimal_size(expected_items, false_positive_rate);
        let hash_count = Self::optimal_hash_count(expected_items, size);

        let words = size.div_ceil(64);

        Self {
            bits: vec![0; words],
            hash_count,
            size,
        }
    }

    /// Calculate optimal bloom filter size
    fn optimal_size(n: usize, p: f64) -> usize {
        let ln2 = std::f64::consts::LN_2;
        (-(n as f64) * p.ln() / (ln2 * ln2)).ceil() as usize
    }

    /// Calculate optimal number of hash functions
    fn optimal_hash_count(n: usize, m: usize) -> usize {
        let ln2 = std::f64::consts::LN_2;
        ((m as f64 / n as f64) * ln2).round() as usize
    }

    /// Add an item to the bloom filter
    pub fn add(&mut self, key: &[u8]) {
        for i in 0..self.hash_count {
            let hash = self.hash(key, i);
            let bit_pos = hash % self.size;
            let word_idx = bit_pos / 64;
            let bit_idx = bit_pos % 64;

            if word_idx < self.bits.len() {
                self.bits[word_idx] |= 1u64 << bit_idx;
            }
        }
    }

    /// Check if an item might exist
    pub fn might_contain(&self, key: &[u8]) -> bool {
        for i in 0..self.hash_count {
            let hash = self.hash(key, i);
            let bit_pos = hash % self.size;
            let word_idx = bit_pos / 64;
            let bit_idx = bit_pos % 64;

            if word_idx >= self.bits.len() {
                return false;
            }

            if (self.bits[word_idx] & (1u64 << bit_idx)) == 0 {
                return false;
            }
        }
        true
    }

    /// Hash function using murmur-like mixing
    fn hash(&self, key: &[u8], seed: usize) -> usize {
        let mut hash = seed as u64;

        for chunk in key.chunks(8) {
            let mut value = 0u64;
            for (i, &byte) in chunk.iter().enumerate() {
                value |= (byte as u64) << (i * 8);
            }

            hash ^= value;
            hash = hash.wrapping_mul(0xc6a4a7935bd1e995);
            hash ^= hash >> 47;
        }

        hash as usize
    }

    /// Clear the bloom filter
    pub fn clear(&mut self) {
        self.bits.fill(0);
    }
}

/// Cache for log entries
pub struct LogCache {
    /// Configuration
    config: CacheConfig,
    /// LRU cache: (namespace, index) -> entry
    cache: LruCacheType,
    /// Bloom filters for each namespace
    bloom_filters: BloomFiltersMap,
    /// Current cache size in bytes
    current_size: Arc<RwLock<usize>>,
    /// Cache statistics
    stats: Arc<RwLock<CacheStats>>,
}

/// Cache statistics
#[derive(Default, Debug, Clone)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub bloom_true_positives: u64,
    pub bloom_false_positives: u64,
}

impl LogCache {
    /// Create a new cache
    pub fn new(config: CacheConfig) -> Self {
        let capacity = NonZeroUsize::new(10_000).unwrap(); // Start with 10k entries

        Self {
            config,
            cache: Arc::new(RwLock::new(LruCache::new(capacity))),
            bloom_filters: Arc::new(RwLock::new(HashMap::new())),
            current_size: Arc::new(RwLock::new(0)),
            stats: Arc::new(RwLock::new(CacheStats::default())),
        }
    }

    /// Check if an entry might exist (using bloom filter)
    pub async fn might_exist(&self, namespace: &StorageNamespace, index: NonZero<u64>) -> bool {
        if !self.config.enable_bloom_filters {
            return true; // Conservative: assume it might exist
        }

        let key = format!("{}/{}", namespace.as_str(), index.get());
        let filters = self.bloom_filters.read().await;

        if let Some(filter) = filters.get(namespace) {
            filter.might_contain(key.as_bytes())
        } else {
            true // No filter yet, assume it might exist
        }
    }

    /// Add entry to bloom filter
    pub async fn add_to_bloom(&self, namespace: &StorageNamespace, index: NonZero<u64>) {
        if !self.config.enable_bloom_filters {
            return;
        }

        let key = format!("{}/{}", namespace.as_str(), index.get());
        let mut filters = self.bloom_filters.write().await;

        let filter = filters.entry(namespace.clone()).or_insert_with(|| {
            BloomFilter::new(
                self.config.bloom_filter_capacity,
                self.config.bloom_filter_fpr,
            )
        });

        filter.add(key.as_bytes());
    }

    /// Get an entry from cache
    pub async fn get(&self, namespace: &StorageNamespace, index: NonZero<u64>) -> Option<Bytes> {
        let mut cache = self.cache.write().await;
        let mut stats = self.stats.write().await;

        let key = (namespace.clone(), index);

        match cache.get(&key) {
            Some(entry) => {
                if entry.expires_at > Instant::now() {
                    stats.hits += 1;
                    trace!("Cache hit for {namespace}:{index}");
                    Some(entry.data.clone())
                } else {
                    // Expired, remove it
                    cache.pop(&key);
                    stats.misses += 1;
                    None
                }
            }
            None => {
                stats.misses += 1;
                trace!("Cache miss for {namespace}:{index}");
                None
            }
        }
    }

    /// Put an entry in cache
    pub async fn put(&self, namespace: &StorageNamespace, index: NonZero<u64>, data: Bytes) {
        let data_size = data.len();
        let mut size = self.current_size.write().await;

        // Check if we need to evict entries
        if *size + data_size > self.config.max_size_bytes {
            drop(size);
            self.evict_to_fit(data_size).await;
            size = self.current_size.write().await;
        }

        let mut cache = self.cache.write().await;
        let key = (namespace.clone(), index);

        let entry = CacheEntry {
            data,
            expires_at: Instant::now() + Duration::from_secs(self.config.ttl_seconds),
        };

        // Update size tracking
        if let Some((_, old_entry)) = cache.push(key, entry) {
            *size = size.saturating_sub(old_entry.data.len());
        }
        *size += data_size;

        drop(cache);
        drop(size);

        // Update bloom filter
        self.add_to_bloom(namespace, index).await;

        trace!("Cached entry {namespace}:{index} ({data_size} bytes)");
    }

    /// Put multiple entries in cache
    pub async fn put_many(&self, namespace: &StorageNamespace, entries: &[(NonZero<u64>, Bytes)]) {
        let total_size: usize = entries.iter().map(|(_, data)| data.len()).sum();

        // Check if we need to evict
        let current = *self.current_size.read().await;
        if current + total_size > self.config.max_size_bytes {
            self.evict_to_fit(total_size).await;
        }

        let mut cache = self.cache.write().await;
        let mut size = self.current_size.write().await;
        let ttl = Duration::from_secs(self.config.ttl_seconds);
        let expires_at = Instant::now() + ttl;

        for (index, data) in entries {
            let key = (namespace.clone(), *index);
            let data_size = data.len();

            let entry = CacheEntry {
                data: data.clone(),
                expires_at,
            };

            if let Some((_, old_entry)) = cache.push(key, entry) {
                *size = size.saturating_sub(old_entry.data.len());
            }
            *size += data_size;
        }

        drop(cache);
        drop(size);

        // Update bloom filters
        if self.config.enable_bloom_filters {
            let mut filters = self.bloom_filters.write().await;
            let filter = filters.entry(namespace.clone()).or_insert_with(|| {
                BloomFilter::new(
                    self.config.bloom_filter_capacity,
                    self.config.bloom_filter_fpr,
                )
            });

            for (index, _) in entries {
                let key = format!("{}/{}", namespace.as_str(), index);
                filter.add(key.as_bytes());
            }
        }

        debug!(
            "Cached {} entries for {} ({} bytes total)",
            entries.len(),
            namespace,
            total_size
        );
    }

    /// Evict entries to make room
    async fn evict_to_fit(&self, needed_size: usize) {
        let mut cache = self.cache.write().await;
        let mut size = self.current_size.write().await;
        let mut stats = self.stats.write().await;

        let target_size = self.config.max_size_bytes.saturating_sub(needed_size);
        let mut evicted = 0;

        while *size > target_size && !cache.is_empty() {
            if let Some((_, entry)) = cache.pop_lru() {
                *size = size.saturating_sub(entry.data.len());
                evicted += 1;
                stats.evictions += 1;
            } else {
                break;
            }
        }

        if evicted > 0 {
            debug!("Evicted {evicted} entries to make room");
        }
    }

    /// Clear the entire cache
    pub async fn clear(&self) {
        let mut cache = self.cache.write().await;
        cache.clear();

        let mut filters = self.bloom_filters.write().await;
        filters.clear();

        *self.current_size.write().await = 0;

        debug!("Cache cleared");
    }

    /// Invalidate a specific entry from the cache
    pub async fn invalidate(&self, namespace: &StorageNamespace, index: NonZero<u64>) {
        let mut cache = self.cache.write().await;
        let key = (namespace.clone(), index);

        // Remove from cache and update size
        if let Some(entry) = cache.pop(&key) {
            let mut current_size = self.current_size.write().await;
            *current_size = current_size.saturating_sub(entry.data.len());
        }

        // Note: We don't remove from bloom filter as it's probabilistic
        // and removing would require rebuilding the entire filter
    }

    /// Get cache statistics
    pub async fn stats(&self) -> CacheStats {
        self.stats.read().await.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_filter() {
        let mut filter = BloomFilter::new(1000, 0.01);

        // Add some items
        filter.add(b"test1");
        filter.add(b"test2");
        filter.add(b"test3");

        // Check they exist
        assert!(filter.might_contain(b"test1"));
        assert!(filter.might_contain(b"test2"));
        assert!(filter.might_contain(b"test3"));

        // Check false positive rate is reasonable
        let mut false_positives = 0;
        for i in 0..10000 {
            let key = format!("nonexistent{i}");
            if filter.might_contain(key.as_bytes()) {
                false_positives += 1;
            }
        }

        // Should be around 1% (100 out of 10000)
        assert!(false_positives < 200); // Allow some variance
    }

    #[tokio::test]
    async fn test_cache_basic() {
        let config = CacheConfig {
            max_size_bytes: 1000,
            ttl_seconds: 60,
            enable_bloom_filters: true,
            bloom_filter_capacity: 100,
            bloom_filter_fpr: 0.01,
        };

        let cache = LogCache::new(config);
        let namespace = StorageNamespace::new("test");

        // Add entry
        let data = Bytes::from("test data");
        cache
            .put(&namespace, NonZero::new(1).unwrap(), data.clone())
            .await;

        // Get entry
        let retrieved = cache.get(&namespace, NonZero::new(1).unwrap()).await;
        assert_eq!(retrieved, Some(data));

        // Check bloom filter
        assert!(
            cache
                .might_exist(&namespace, NonZero::new(1).unwrap())
                .await
        );
    }
}
