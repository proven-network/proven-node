//! Read cache for S3 storage adaptor

use bytes::Bytes;
use lru::LruCache;
use std::{
    num::NonZeroUsize,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;
use tracing::{debug, trace};

use super::config::CacheConfig;

/// Cache entry with metadata
#[derive(Clone, Debug)]
struct CacheEntry {
    data: Bytes,
    size: usize,
    inserted_at: Instant,
}

/// Bloom filter for existence checks
pub struct BloomFilter {
    bits: Vec<u64>,
    hash_count: u32,
    size: usize,
}

impl BloomFilter {
    fn new(expected_items: usize, fp_rate: f64) -> Self {
        // Calculate optimal bloom filter parameters
        let ln2 = std::f64::consts::LN_2;
        let bits_per_item = -1.44 * fp_rate.ln() / (ln2 * ln2);
        let size = (expected_items as f64 * bits_per_item).ceil() as usize;
        let hash_count = (bits_per_item * ln2).ceil() as u32;

        let bits_size = size.div_ceil(64);

        Self {
            bits: vec![0; bits_size],
            hash_count,
            size,
        }
    }

    fn hash(&self, key: &str, seed: u32) -> usize {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        seed.hash(&mut hasher);
        hasher.finish() as usize % self.size
    }

    fn insert(&mut self, key: &str) {
        for i in 0..self.hash_count {
            let bit_pos = self.hash(key, i);
            let word_idx = bit_pos / 64;
            let bit_idx = bit_pos % 64;
            self.bits[word_idx] |= 1u64 << bit_idx;
        }
    }

    fn contains(&self, key: &str) -> bool {
        for i in 0..self.hash_count {
            let bit_pos = self.hash(key, i);
            let word_idx = bit_pos / 64;
            let bit_idx = bit_pos % 64;
            if (self.bits[word_idx] & (1u64 << bit_idx)) == 0 {
                return false;
            }
        }
        true
    }

    #[allow(dead_code)]
    fn clear(&mut self) {
        self.bits.fill(0);
    }
}

/// Read cache with LRU eviction and TTL
pub struct ReadCache {
    config: CacheConfig,
    lru: Arc<Mutex<LruCache<String, CacheEntry>>>,
    bloom_filter: Arc<Mutex<Option<BloomFilter>>>,
    current_size: Arc<Mutex<usize>>,
}

impl ReadCache {
    /// Create a new read cache
    pub fn new(config: &CacheConfig) -> Self {
        let capacity = NonZeroUsize::new(10000).unwrap(); // Max entries
        let bloom_filter = if config.use_bloom_filters {
            Some(BloomFilter::new(
                config.bloom_filter_items,
                config.bloom_filter_fp_rate,
            ))
        } else {
            None
        };

        Self {
            config: config.clone(),
            lru: Arc::new(Mutex::new(LruCache::new(capacity))),
            bloom_filter: Arc::new(Mutex::new(bloom_filter)),
            current_size: Arc::new(Mutex::new(0)),
        }
    }

    /// Get a value from cache
    pub async fn get(&self, key: &str) -> Option<Bytes> {
        // Check bloom filter first for non-existence
        if self.config.use_bloom_filters {
            let bloom = self.bloom_filter.lock().await;
            if let Some(filter) = bloom.as_ref() {
                if !filter.contains(key) {
                    trace!("Bloom filter: key {} definitely not in cache", key);
                    return None;
                }
            }
        }

        let mut cache = self.lru.lock().await;

        if let Some(entry) = cache.get(key) {
            // Check TTL
            if entry.inserted_at.elapsed() < Duration::from_secs(self.config.ttl_seconds) {
                debug!("Cache hit for key: {}", key);
                return Some(entry.data.clone());
            } else {
                // Expired, remove it
                let entry_size = entry.size;
                cache.pop(key);
                drop(cache); // Release the lock before acquiring another
                let mut size = self.current_size.lock().await;
                *size = size.saturating_sub(entry_size);
            }
        }

        None
    }

    /// Put a value in cache
    pub async fn put(&mut self, key: String, data: Bytes) {
        let entry_size = data.len();

        // Don't cache if it's too large
        if entry_size > self.config.max_size_bytes / 4 {
            debug!("Skipping cache for large entry: {} bytes", entry_size);
            return;
        }

        let entry = CacheEntry {
            data,
            size: entry_size,
            inserted_at: Instant::now(),
        };

        let mut cache = self.lru.lock().await;
        let mut current_size = self.current_size.lock().await;

        // Evict entries if necessary
        while *current_size + entry_size > self.config.max_size_bytes && !cache.is_empty() {
            if let Some((_, evicted)) = cache.pop_lru() {
                *current_size = current_size.saturating_sub(evicted.size);
                debug!("Evicted entry from cache, freed {} bytes", evicted.size);
            }
        }

        // Add to cache
        if let Some(old_entry) = cache.put(key.clone(), entry) {
            *current_size = current_size.saturating_sub(old_entry.size);
        }
        *current_size += entry_size;

        // Update bloom filter
        if self.config.use_bloom_filters {
            let mut bloom = self.bloom_filter.lock().await;
            if let Some(filter) = bloom.as_mut() {
                filter.insert(&key);
            }
        }

        trace!("Cached entry: {}, size: {} bytes", key, entry_size);
    }

    /// Invalidate a cache entry
    pub async fn invalidate(&mut self, key: &str) {
        let mut cache = self.lru.lock().await;

        if let Some(entry) = cache.pop(key) {
            let mut size = self.current_size.lock().await;
            *size = size.saturating_sub(entry.size);
            debug!("Invalidated cache entry: {}", key);
        }
    }

    /// Clear the entire cache
    #[allow(dead_code)]
    pub async fn clear(&mut self) {
        let mut cache = self.lru.lock().await;
        cache.clear();

        let mut size = self.current_size.lock().await;
        *size = 0;

        if self.config.use_bloom_filters {
            let mut bloom = self.bloom_filter.lock().await;
            if let Some(filter) = bloom.as_mut() {
                filter.clear();
            }
        }

        debug!("Cleared cache");
    }

    /// Get cache statistics
    pub async fn stats(&self) -> CacheStats {
        let cache = self.lru.lock().await;
        let current_size = self.current_size.lock().await;

        CacheStats {
            entries: cache.len(),
            size_bytes: *current_size,
            max_size_bytes: self.config.max_size_bytes,
        }
    }

    /// Expire old entries and return count of expired entries
    pub fn expire_old_entries(&mut self) -> usize {
        // This is a synchronous method that should be made async in production
        // For now, we'll use blocking to demonstrate the concept
        let rt = tokio::runtime::Handle::current();
        rt.block_on(async {
            let mut cache = self.lru.lock().await;
            let mut current_size = self.current_size.lock().await;
            let mut expired_count = 0;

            // Collect keys to expire (can't modify while iterating)
            let mut to_expire = Vec::new();
            for (key, entry) in cache.iter() {
                if entry.inserted_at.elapsed() >= Duration::from_secs(self.config.ttl_seconds) {
                    to_expire.push((key.clone(), entry.size));
                }
            }

            // Remove expired entries
            for (key, size) in to_expire {
                cache.pop(&key);
                *current_size = current_size.saturating_sub(size);
                expired_count += 1;
            }

            expired_count
        })
    }

    /// Reset statistics
    pub async fn reset_stats(&mut self) {
        // For now, just clear hit/miss counters if we were tracking them
        // In production, you'd reset actual statistics
    }
}

/// Cache statistics
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct CacheStats {
    pub entries: usize,
    pub size_bytes: usize,
    pub max_size_bytes: usize,
}
