//! Host-side tiered storage implementation
//!
//! This module implements the actual storage backend with hot and cold tiers.

use async_trait::async_trait;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::fs;
use tokio::sync::RwLock;

use crate::{
    BlobId, StorageTier, TierHint,
    error::{Result, VsockFuseError},
    storage::{BlobData, BlobInfo, BlobStorage, StorageStats, TierStats},
};

/// Hot tier implementation using local filesystem
pub struct HotTier {
    /// Base path for hot tier storage
    base_path: PathBuf,
    /// Cache of blob locations
    blob_index: RwLock<HashMap<BlobId, PathBuf>>,
    /// Access tracking for LRU eviction
    access_tracker: RwLock<HashMap<BlobId, HotBlobMetadata>>,
    /// Maximum size for hot tier (bytes)
    max_size: u64,
}

#[derive(Clone)]
struct HotBlobMetadata {
    size: u64,
    last_accessed: SystemTime,
    created_at: SystemTime,
}

impl HotTier {
    /// Create a new hot tier
    pub async fn new(base_path: PathBuf) -> Result<Self> {
        // Ensure directory exists
        fs::create_dir_all(&base_path).await?;

        // Default to 10GB max size
        let max_size = 10 * 1024 * 1024 * 1024;

        Ok(Self {
            base_path,
            blob_index: RwLock::new(HashMap::new()),
            access_tracker: RwLock::new(HashMap::new()),
            max_size,
        })
    }

    /// Create with specific max size
    pub async fn with_max_size(base_path: PathBuf, max_size: u64) -> Result<Self> {
        fs::create_dir_all(&base_path).await?;

        Ok(Self {
            base_path,
            blob_index: RwLock::new(HashMap::new()),
            access_tracker: RwLock::new(HashMap::new()),
            max_size,
        })
    }

    /// Get the path for a blob
    fn blob_path(&self, id: &BlobId) -> PathBuf {
        // Simple hex encoding
        let hex: String = id.0.iter().map(|b| format!("{b:02x}")).collect();
        self.base_path.join(&hex[0..2]).join(&hex[2..4]).join(&hex)
    }

    /// Store a blob in the hot tier
    pub async fn store(&self, id: BlobId, data: Vec<u8>) -> Result<()> {
        let size = data.len() as u64;
        let now = SystemTime::now();

        // Check if we need to evict
        let current_usage = self.get_total_size().await?;
        if current_usage + size > self.max_size {
            let needed_space = (current_usage + size) - (self.max_size * 9 / 10); // Target 90% usage
            self.evict_lru(needed_space).await?;
        }

        let path = self.blob_path(&id);

        // Create parent directory
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }

        // Write data
        fs::write(&path, &data).await?;

        // Update indices
        self.blob_index.write().await.insert(id, path);
        self.access_tracker.write().await.insert(
            id,
            HotBlobMetadata {
                size,
                last_accessed: now,
                created_at: now,
            },
        );

        Ok(())
    }

    /// Get a blob from the hot tier
    pub async fn get(&self, id: &BlobId) -> Result<Vec<u8>> {
        let path = self.blob_path(id);
        let data = fs::read(&path).await?;

        // Update access time
        self.access_tracker
            .write()
            .await
            .entry(*id)
            .and_modify(|m| m.last_accessed = SystemTime::now());

        Ok(data)
    }

    /// List all blobs in the hot tier
    pub async fn list_blobs(&self) -> Result<Vec<BlobInfo>> {
        let tracker = self.access_tracker.read().await;
        let mut blobs = Vec::new();

        for (blob_id, metadata) in tracker.iter() {
            blobs.push(BlobInfo {
                blob_id: *blob_id,
                size: metadata.size,
                tier: StorageTier::Hot,
                last_accessed: metadata.last_accessed,
                created_at: metadata.created_at,
            });
        }

        Ok(blobs)
    }

    /// Delete a blob from the hot tier
    pub async fn delete(&self, id: &BlobId) -> Result<()> {
        let path = self.blob_path(id);
        fs::remove_file(&path).await?;
        self.blob_index.write().await.remove(id);
        self.access_tracker.write().await.remove(id);
        Ok(())
    }

    /// Check if a blob exists in the hot tier
    pub async fn exists(&self, id: &BlobId) -> bool {
        let path = self.blob_path(id);
        path.exists()
    }

    /// Get statistics for the hot tier
    pub async fn get_stats(&self) -> Result<TierStats> {
        let total_size = self.get_total_size().await?;
        Ok(TierStats {
            total_bytes: self.max_size,
            used_bytes: total_size,
            file_count: self.blob_index.read().await.len() as u64,
            read_ops_per_sec: 0.0,
            write_ops_per_sec: 0.0,
        })
    }

    /// Get total size of all blobs in hot tier
    async fn get_total_size(&self) -> Result<u64> {
        let tracker = self.access_tracker.read().await;
        let total_size = tracker.values().map(|metadata| metadata.size).sum();
        Ok(total_size)
    }

    /// Evict least recently used blobs to free up space
    async fn evict_lru(&self, needed_space: u64) -> Result<()> {
        let tracker = self.access_tracker.write().await;

        // Collect all blobs with their access times
        let mut blobs: Vec<(BlobId, HotBlobMetadata)> = tracker
            .iter()
            .map(|(id, metadata)| (*id, metadata.clone()))
            .collect();

        // Sort by last access time (oldest first)
        blobs.sort_by_key(|(_, metadata)| metadata.last_accessed);

        let mut freed_space = 0u64;
        let mut to_evict = Vec::new();

        // Select blobs to evict
        for (blob_id, metadata) in blobs {
            if freed_space >= needed_space {
                break;
            }
            to_evict.push(blob_id);
            freed_space += metadata.size;
        }

        // Release the write lock before doing I/O
        drop(tracker);

        // Delete the selected blobs
        for blob_id in to_evict {
            let path = self.blob_path(&blob_id);
            if let Err(e) = fs::remove_file(&path).await {
                tracing::warn!("Failed to evict blob {:?}: {}", blob_id, e);
            }

            // Remove from indices
            self.blob_index.write().await.remove(&blob_id);
            self.access_tracker.write().await.remove(&blob_id);

            tracing::info!("Evicted blob {:?} from hot tier", blob_id);
        }

        if freed_space < needed_space {
            tracing::warn!(
                "Could only free {} bytes out of {} requested",
                freed_space,
                needed_space
            );
        }

        Ok(())
    }
}

/// Cold tier implementation using S3
#[cfg(feature = "host")]
pub struct ColdTier {
    /// S3 bucket name
    bucket: String,
    /// S3 prefix
    prefix: String,
    /// S3 client
    #[cfg(feature = "aws-sdk-s3")]
    client: aws_sdk_s3::Client,
    /// Compression enabled
    compression_enabled: bool,
    /// Storage class to use
    storage_class: String,
    /// Cache of blob metadata
    metadata_cache: RwLock<HashMap<BlobId, ColdBlobMetadata>>,
}

#[derive(Clone)]
struct ColdBlobMetadata {
    size: u64,
    compressed_size: u64,
    created_at: std::time::SystemTime,
}

#[cfg(feature = "host")]
impl ColdTier {
    /// Create a new cold tier
    pub async fn new(bucket: String, prefix: String) -> Result<Self> {
        #[cfg(feature = "aws-sdk-s3")]
        {
            let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
                .load()
                .await;
            let client = aws_sdk_s3::Client::new(&config);

            Ok(Self {
                bucket,
                prefix,
                client,
                compression_enabled: true,
                storage_class: "ONEZONE_IA".to_string(),
                metadata_cache: RwLock::new(HashMap::new()),
            })
        }

        #[cfg(not(feature = "aws-sdk-s3"))]
        {
            Ok(Self {
                bucket,
                prefix,
                compression_enabled: true,
                storage_class: "ONEZONE_IA".to_string(),
                metadata_cache: RwLock::new(HashMap::new()),
            })
        }
    }

    /// Get S3 key for a blob
    fn get_s3_key(&self, id: &BlobId) -> String {
        let hex: String = id.0.iter().map(|b| format!("{b:02x}")).collect();
        format!("{}/{}/{}/{}", self.prefix, &hex[0..2], &hex[2..4], hex)
    }

    /// Store a blob in the cold tier
    pub async fn store(&self, id: BlobId, data: Vec<u8>) -> Result<()> {
        let original_size = data.len() as u64;

        // Compress if enabled
        let (body, compressed_size) = if self.compression_enabled {
            #[cfg(feature = "zstd")]
            {
                let compressed = zstd::encode_all(data.as_slice(), 3).map_err(|e| {
                    VsockFuseError::Storage(crate::error::StorageError::S3Error {
                        message: format!("Compression failed: {e}"),
                    })
                })?;
                let size = compressed.len() as u64;
                (compressed, size)
            }

            #[cfg(not(feature = "zstd"))]
            {
                (data, original_size)
            }
        } else {
            (data, original_size)
        };

        #[cfg(feature = "aws-sdk-s3")]
        {
            let key = self.get_s3_key(&id);

            self.client
                .put_object()
                .bucket(&self.bucket)
                .key(&key)
                .body(body.into())
                .storage_class(
                    self.storage_class
                        .parse()
                        .unwrap_or(aws_sdk_s3::types::StorageClass::OnezoneIa),
                )
                .metadata("original-size", original_size.to_string())
                .metadata("compressed", self.compression_enabled.to_string())
                .send()
                .await
                .map_err(|e| {
                    VsockFuseError::Storage(crate::error::StorageError::S3Error {
                        message: format!("S3 put failed: {e}"),
                    })
                })?;
        }

        // Update metadata cache
        self.metadata_cache.write().await.insert(
            id,
            ColdBlobMetadata {
                size: original_size,
                compressed_size,
                created_at: std::time::SystemTime::now(),
            },
        );

        Ok(())
    }

    /// Get a blob from the cold tier
    pub async fn get(&self, id: &BlobId) -> Result<Vec<u8>> {
        #[cfg(feature = "aws-sdk-s3")]
        {
            let key = self.get_s3_key(id);

            let response = self
                .client
                .get_object()
                .bucket(&self.bucket)
                .key(&key)
                .send()
                .await
                .map_err(|e| {
                    VsockFuseError::Storage(crate::error::StorageError::S3Error {
                        message: format!("S3 get failed: {e}"),
                    })
                })?;

            // Check if compressed (before consuming body)
            let compressed = response
                .metadata()
                .and_then(|m| m.get("compressed"))
                .map(|v| v == "true")
                .unwrap_or(false);

            let body = response.body.collect().await.map_err(|e| {
                VsockFuseError::Storage(crate::error::StorageError::S3Error {
                    message: format!("Failed to read S3 body: {e}"),
                })
            })?;

            let data = body.into_bytes().to_vec();

            if compressed {
                #[cfg(feature = "zstd")]
                {
                    zstd::decode_all(data.as_slice()).map_err(|e| {
                        VsockFuseError::Storage(crate::error::StorageError::S3Error {
                            message: format!("Decompression failed: {e}"),
                        })
                    })
                }

                #[cfg(not(feature = "zstd"))]
                {
                    Ok(data)
                }
            } else {
                Ok(data)
            }
        }

        #[cfg(not(feature = "aws-sdk-s3"))]
        {
            Err(VsockFuseError::NotImplemented {
                feature: "S3 support not compiled in".to_string(),
            })
        }
    }

    /// Delete a blob from the cold tier
    pub async fn delete(&self, id: &BlobId) -> Result<()> {
        #[cfg(feature = "aws-sdk-s3")]
        {
            let key = self.get_s3_key(id);

            self.client
                .delete_object()
                .bucket(&self.bucket)
                .key(&key)
                .send()
                .await
                .map_err(|e| {
                    VsockFuseError::Storage(crate::error::StorageError::S3Error {
                        message: format!("S3 delete failed: {e}"),
                    })
                })?;
        }

        // Remove from cache
        self.metadata_cache.write().await.remove(id);

        Ok(())
    }

    /// Check if a blob exists in cold tier
    pub async fn exists(&self, id: &BlobId) -> bool {
        #[cfg(feature = "aws-sdk-s3")]
        {
            let key = self.get_s3_key(id);

            self.client
                .head_object()
                .bucket(&self.bucket)
                .key(&key)
                .send()
                .await
                .is_ok()
        }

        #[cfg(not(feature = "aws-sdk-s3"))]
        {
            false
        }
    }

    /// List all blobs in the cold tier
    pub async fn list_blobs(&self) -> Result<Vec<BlobInfo>> {
        let cache = self.metadata_cache.read().await;
        let mut blobs = Vec::new();

        for (blob_id, metadata) in cache.iter() {
            blobs.push(BlobInfo {
                blob_id: *blob_id,
                size: metadata.size,
                tier: StorageTier::Cold,
                last_accessed: metadata.created_at, // Use created_at as approximation
                created_at: metadata.created_at,
            });
        }

        Ok(blobs)
    }

    /// Get statistics for the cold tier
    pub async fn get_stats(&self) -> Result<TierStats> {
        // Get from cache for now
        let cache = self.metadata_cache.read().await;
        let total_size: u64 = cache.values().map(|m| m.compressed_size).sum();
        let file_count = cache.len() as u64;

        Ok(TierStats {
            total_bytes: 100_000_000_000, // 100GB capacity for S3 One Zone-IA
            used_bytes: total_size,
            file_count,
            read_ops_per_sec: 0.0,
            write_ops_per_sec: 0.0,
        })
    }
}

/// Tiered storage implementation
pub struct TieredStorage {
    /// Hot tier
    hot_tier: HotTier,
    /// Cold tier
    cold_tier: ColdTier,
    /// Blob location index
    blob_locations: RwLock<HashMap<BlobId, StorageTier>>,
    /// Policy engine for tier selection
    policy_engine: Option<Arc<crate::host::policy::PolicyEngine>>,
}

impl TieredStorage {
    /// Create a new tiered storage system
    pub async fn new(hot_tier_path: PathBuf, s3_bucket: String, s3_prefix: String) -> Result<Self> {
        let hot_tier = HotTier::new(hot_tier_path).await?;
        let cold_tier = ColdTier::new(s3_bucket, s3_prefix).await?;

        Ok(Self {
            hot_tier,
            cold_tier,
            blob_locations: RwLock::new(HashMap::new()),
            policy_engine: None,
        })
    }

    /// Set the policy engine for tier selection
    pub fn set_policy_engine(&mut self, engine: Arc<crate::host::policy::PolicyEngine>) {
        self.policy_engine = Some(engine);
    }

    /// Flush any pending operations
    pub async fn flush(&self) -> Result<()> {
        // TODO: Implement flush logic
        Ok(())
    }
}

#[async_trait]
impl BlobStorage for TieredStorage {
    async fn store_blob(&self, id: BlobId, data: Vec<u8>, hint: TierHint) -> Result<()> {
        let data_size = data.len() as u64;

        // Determine target tier based on hint and policy
        let tier = match hint {
            TierHint::PreferHot => StorageTier::Hot,
            TierHint::PreferCold => StorageTier::Cold,
            TierHint::Auto => {
                // Use policy engine if available
                if let Some(ref engine) = self.policy_engine {
                    // Get hot tier usage
                    let hot_stats = self.hot_tier.get_stats().await?;
                    let hot_usage = hot_stats.used_bytes as f64 / hot_stats.total_bytes as f64;

                    // Apply policy rules
                    let policy = engine.policy();
                    if hot_usage > policy.hot_tier_threshold {
                        // Hot tier is too full
                        StorageTier::Cold
                    } else if data_size > policy.max_hot_size {
                        // File is too large for hot tier
                        StorageTier::Cold
                    } else if data_size < policy.min_cold_size {
                        // File is too small for cold tier
                        StorageTier::Hot
                    } else {
                        // Default to hot tier for new files
                        StorageTier::Hot
                    }
                } else {
                    // No policy engine, default to hot
                    StorageTier::Hot
                }
            }
        };

        // Store in appropriate tier
        match tier {
            StorageTier::Hot => {
                self.hot_tier.store(id, data).await?;
            }
            StorageTier::Cold => {
                self.cold_tier.store(id, data).await?;
            }
            StorageTier::Both => {
                // Store in hot tier first
                self.hot_tier.store(id, data.clone()).await?;
                // TODO: Schedule background migration to cold tier
            }
        }

        // Update location index
        self.blob_locations.write().await.insert(id, tier);

        Ok(())
    }

    async fn get_blob(&self, id: BlobId) -> Result<BlobData> {
        // Check location index
        let locations = self.blob_locations.read().await;
        let tier = locations.get(&id).copied();
        drop(locations);

        // Try hot tier first
        if (tier.is_none() || tier == Some(StorageTier::Hot) || tier == Some(StorageTier::Both))
            && self.hot_tier.exists(&id).await
        {
            let data = self.hot_tier.get(&id).await?;
            return Ok(BlobData {
                data,
                tier: StorageTier::Hot,
            });
        }

        // Try cold tier
        if tier == Some(StorageTier::Cold) || tier == Some(StorageTier::Both) {
            let data = self.cold_tier.get(&id).await?;
            return Ok(BlobData {
                data,
                tier: StorageTier::Cold,
            });
        }

        Err(VsockFuseError::BlobNotFound { id })
    }

    async fn delete_blob(&self, id: BlobId) -> Result<()> {
        // Get location
        let locations = self.blob_locations.read().await;
        let tier = locations.get(&id).copied();
        drop(locations);

        // Delete from appropriate tier(s)
        match tier {
            Some(StorageTier::Hot) => {
                self.hot_tier.delete(&id).await?;
            }
            Some(StorageTier::Cold) => {
                self.cold_tier.delete(&id).await?;
            }
            Some(StorageTier::Both) => {
                self.hot_tier.delete(&id).await?;
                self.cold_tier.delete(&id).await?;
            }
            None => {
                // Try hot tier anyway
                if self.hot_tier.exists(&id).await {
                    self.hot_tier.delete(&id).await?;
                }
            }
        }

        // Remove from index
        self.blob_locations.write().await.remove(&id);

        Ok(())
    }

    async fn list_blobs(&self, prefix: Option<&[u8]>) -> Result<Vec<BlobInfo>> {
        let mut all_blobs = Vec::new();

        // Get blobs from hot tier
        let hot_blobs = self.hot_tier.list_blobs().await?;
        all_blobs.extend(hot_blobs);

        // Get blobs from cold tier
        let cold_blobs = self.cold_tier.list_blobs().await?;
        all_blobs.extend(cold_blobs);

        // Apply prefix filtering if specified
        if let Some(prefix_bytes) = prefix {
            all_blobs.retain(|blob| {
                let blob_id_bytes = &blob.blob_id.0;
                blob_id_bytes.starts_with(prefix_bytes)
            });
        }

        // Sort by last accessed time (most recent first)
        all_blobs.sort_by_key(|b| std::cmp::Reverse(b.last_accessed));

        Ok(all_blobs)
    }

    async fn get_stats(&self) -> Result<StorageStats> {
        let hot_stats = self.hot_tier.get_stats().await?;
        let cold_stats = self.cold_tier.get_stats().await?;

        Ok(StorageStats {
            hot_tier: hot_stats,
            cold_tier: cold_stats,
            migration_queue_size: 0, // TODO: Implement migration queue
        })
    }
}
