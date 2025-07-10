//! S3 storage adaptor implementation

use crate::storage::{
    traits::{
        LogStorage, MaintenanceResult, SnapshotMetadata, SnapshotStorage, StorageCapabilities,
        StorageEngine, StorageHints, StorageMetrics as StorageMetricsTrait, StorageStats,
    },
    types::{StorageError, StorageKey, StorageNamespace, StorageResult, StorageValue, WriteBatch},
};
use async_trait::async_trait;
use aws_sdk_s3::{Client as S3Client, config::Region};
use bytes::Bytes;
use std::{ops::RangeBounds, sync::Arc, time::Instant};
use tokio::sync::RwLock;
use tracing::{debug, error, info};

use super::{
    batching::{BatchEntry, BatchManager},
    cache::ReadCache,
    config::S3StorageConfig,
    encryption::EncryptionManager,
    iterator::S3Iterator,
    keys::S3KeyMapper,
    metrics::StorageMetrics,
    wal_client::WalClient,
};

/// S3 storage adaptor with VSOCK WAL and encryption
pub struct S3StorageAdaptor {
    /// S3 client
    client: S3Client,

    /// Configuration
    config: S3StorageConfig,

    /// Batching system
    batch_manager: Arc<BatchManager>,

    /// Read cache
    read_cache: Arc<RwLock<ReadCache>>,

    /// WAL client
    wal_client: Arc<WalClient>,

    /// Encryption manager
    encryptor: Arc<EncryptionManager>,

    /// Key mapper for S3 object keys
    key_mapper: Arc<S3KeyMapper>,

    /// Metrics
    metrics: Arc<StorageMetrics>,
}

impl S3StorageAdaptor {
    /// Compress snapshot data
    fn compress_snapshot(&self, data: &[u8]) -> StorageResult<Vec<u8>> {
        // For now, just return the data as-is
        // In production, we would use flate2 or zstd for compression
        Ok(data.to_vec())
    }

    /// Decompress snapshot data
    fn decompress_snapshot(&self, data: &[u8]) -> StorageResult<Vec<u8>> {
        // For now, just return the data as-is
        // In production, we would use flate2 or zstd for decompression
        Ok(data.to_vec())
    }

    /// Upload a batch of objects to S3
    async fn upload_batch(&self, batch: &[(String, Vec<u8>)]) -> StorageResult<()> {
        // Upload objects sequentially for now
        // In production, we would use futures::stream for concurrent uploads
        for (key, data) in batch {
            self.client
                .put_object()
                .bucket(&self.config.s3.bucket)
                .key(key)
                .body(aws_sdk_s3::primitives::ByteStream::from(data.clone()))
                .send()
                .await
                .map_err(|e| StorageError::Backend(format!("Failed to upload {}: {}", key, e)))?;
            self.metrics.s3_writes.inc();
        }

        Ok(())
    }

    /// Create a new S3 storage adaptor
    pub async fn new(config: S3StorageConfig) -> StorageResult<Self> {
        // Initialize AWS SDK
        let aws_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(Region::new(config.s3.region.clone()))
            .load()
            .await;

        let s3_config = aws_sdk_s3::config::Builder::from(&aws_config)
            .force_path_style(true)
            .build();

        let client = S3Client::from_conf(s3_config);

        // Initialize components
        let encryptor = Arc::new(EncryptionManager::new().await?);
        let wal_client = Arc::new(WalClient::new(&config.wal, encryptor.clone()).await?);
        let key_mapper = Arc::new(S3KeyMapper::new(&config.s3.prefix));
        let metrics = Arc::new(StorageMetrics::new());

        let read_cache = Arc::new(RwLock::new(ReadCache::new(&config.cache)));

        let batch_manager = Arc::new(BatchManager::new(
            config.batch.clone(),
            client.clone(),
            config.s3.bucket.clone(),
            wal_client.clone(),
            encryptor.clone(),
            key_mapper.clone(),
            metrics.clone(),
        ));

        // Start batch manager
        batch_manager.start().await;

        // Recover any pending batches from WAL
        let pending_batches = wal_client.recover_pending().await?;
        if !pending_batches.is_empty() {
            debug!(
                "Recovered {} pending batches from WAL",
                pending_batches.len()
            );
            for batch in pending_batches {
                batch_manager.recover_batch(batch).await?;
            }
        }

        Ok(Self {
            client,
            config,
            batch_manager,
            read_cache,
            wal_client,
            encryptor,
            key_mapper,
            metrics,
        })
    }

    /// Verify S3 bucket is accessible
    async fn verify_bucket_access(&self) -> StorageResult<()> {
        match self
            .client
            .head_bucket()
            .bucket(&self.config.s3.bucket)
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => Err(StorageError::Backend(format!(
                "Cannot access S3 bucket '{}': {}",
                self.config.s3.bucket, e
            ))),
        }
    }

    /// Warm up cache with frequently accessed keys
    async fn warm_cache(&self) -> StorageResult<()> {
        // This is a placeholder - in production you might:
        // 1. Load most recently used keys from a metadata store
        // 2. Pre-fetch critical system metadata
        // 3. Load keys based on access patterns
        Ok(())
    }

    /// Persist cache statistics for next startup
    async fn persist_cache_stats(&self) -> StorageResult<()> {
        // This is a placeholder - in production you might:
        // 1. Save cache hit rates to S3
        // 2. Save frequently accessed key patterns
        // 3. Save cache configuration recommendations
        Ok(())
    }

    /// Compact small objects into larger ones
    async fn compact_small_objects(&self) -> StorageResult<CompactionResult> {
        // This is a placeholder - in production you would:
        // 1. Scan for small objects below a threshold
        // 2. Group them by access pattern
        // 3. Combine into larger objects
        // 4. Update key mappings
        Ok(CompactionResult {
            count: 0,
            bytes_saved: 0,
        })
    }

    /// Static helper for parallel S3 fetches
    async fn fetch_from_s3_static(
        client: &S3Client,
        bucket: &str,
        s3_key: &str,
        encryptor: &EncryptionManager,
        metrics: &StorageMetrics,
    ) -> StorageResult<Option<Bytes>> {
        let _timer = metrics.s3_read_duration.start_timer();

        match client.get_object().bucket(bucket).key(s3_key).send().await {
            Ok(response) => {
                let data = response
                    .body
                    .collect()
                    .await
                    .map_err(|e| {
                        StorageError::Io(std::io::Error::other(format!(
                            "Failed to read S3 body: {}",
                            e
                        )))
                    })?
                    .into_bytes();

                // Decrypt the data
                let decrypted = encryptor.decrypt_from_s3(&data).await?;

                metrics.s3_reads.inc();
                metrics.bytes_read.inc_by(decrypted.len() as u64);

                Ok(Some(decrypted))
            }
            Err(e) => {
                if let aws_sdk_s3::error::SdkError::ServiceError(service_err) = &e {
                    if matches!(
                        service_err.err(),
                        aws_sdk_s3::operation::get_object::GetObjectError::NoSuchKey(_)
                    ) {
                        return Ok(None);
                    }
                }

                metrics.s3_errors.inc();
                error!("S3 read error: {}", e);
                Err(StorageError::Io(std::io::Error::other(format!(
                    "S3 read failed: {}",
                    e
                ))))
            }
        }
    }

    /// Read directly from S3
    async fn read_from_s3(&self, s3_key: &str) -> StorageResult<Option<Bytes>> {
        Self::fetch_from_s3_static(
            &self.client,
            &self.config.s3.bucket,
            s3_key,
            &self.encryptor,
            &self.metrics,
        )
        .await
    }
}

#[async_trait]
impl StorageMetricsTrait for S3StorageAdaptor {
    async fn get_stats(&self) -> StorageStats {
        let _cache_stats = self.read_cache.read().await.stats().await;

        StorageStats {
            reads: self.metrics.s3_reads.get(),
            writes: self.metrics.s3_writes.get(),
            deletes: self.metrics.s3_deletes.get(),
            cache_hits: Some(self.metrics.cache_hits.get()),
            cache_misses: Some(self.metrics.cache_misses.get()),
            bytes_read: self.metrics.bytes_read.get(),
            bytes_written: self.metrics.bytes_written.get(),
            avg_read_latency_ms: Some(self.metrics.calculate_avg_read_latency()),
            avg_write_latency_ms: Some(self.metrics.calculate_avg_write_latency()),
            errors: self.metrics.s3_errors.get(),
        }
    }

    async fn reset_stats(&self) {
        self.metrics.reset_all();
        self.read_cache.write().await.reset_stats().await;
    }
}

/// Result of compaction operation
struct CompactionResult {
    count: u64,
    bytes_saved: u64,
}

#[async_trait]
impl StorageEngine for S3StorageAdaptor {
    type Iterator = S3Iterator;

    fn capabilities(&self) -> StorageCapabilities {
        StorageCapabilities {
            atomic_batches: false,       // S3 doesn't support true atomic batches
            efficient_range_scan: false, // S3 LIST is expensive
            snapshots: true,             // We support snapshots via multipart
            eventual_consistency: true,  // S3 is eventually consistent
            max_value_size: Some(5 * 1024 * 1024 * 1024), // 5GB S3 limit
            atomic_conditionals: false,  // S3 doesn't support CAS natively
            streaming: true,             // We support multipart upload/download
            caching: true,               // We have built-in LRU cache
        }
    }

    async fn initialize(&self) -> StorageResult<()> {
        info!("Initializing S3 storage adaptor");

        // Verify S3 bucket accessibility
        self.verify_bucket_access().await?;

        // Warm up cache with frequently accessed keys
        self.warm_cache().await?;

        // Start metrics collection if not already started
        self.metrics.start_collectors();

        info!("S3 storage adaptor initialized successfully");
        Ok(())
    }

    async fn shutdown(&self) -> StorageResult<()> {
        info!("Shutting down S3 storage adaptor");

        // Flush all pending batches
        self.batch_manager.shutdown().await?;

        // Get last WAL ID and ensure it's synced
        if let Some(last_wal_id) = self.batch_manager.get_last_wal_id().await {
            self.wal_client.sync(last_wal_id).await?;
        }

        // Save cache statistics for next startup
        self.persist_cache_stats().await?;

        info!("S3 storage adaptor shut down successfully");
        Ok(())
    }

    async fn maintenance(&self) -> StorageResult<MaintenanceResult> {
        let start = Instant::now();
        let mut result = MaintenanceResult::default();

        // Clean up old WAL entries
        let cutoff_id = self.batch_manager.get_last_confirmed_wal_id().await;
        if let Some(cutoff) = cutoff_id {
            let cleaned = self.wal_client.cleanup(cutoff).await?;
            result.entries_compacted += cleaned as u64;
        }

        // Compact small objects into larger ones
        let compacted = self.compact_small_objects().await?;
        result.bytes_reclaimed = compacted.bytes_saved;
        result.entries_compacted += compacted.count;

        // Clean expired cache entries
        let cache_cleaned = self.read_cache.write().await.expire_old_entries();

        result.duration_ms = start.elapsed().as_millis() as u64;
        result.details = format!(
            "Cleaned {} WAL entries, compacted {} objects, expired {} cache entries",
            result.entries_compacted, compacted.count, cache_cleaned
        );

        Ok(result)
    }

    async fn get_batch(
        &self,
        namespace: &StorageNamespace,
        keys: &[StorageKey],
    ) -> StorageResult<Vec<Option<StorageValue>>> {
        // Check cache first for all keys
        let mut results = vec![None; keys.len()];
        let mut uncached_indices = Vec::new();

        {
            let cache = self.read_cache.read().await;
            for (i, key) in keys.iter().enumerate() {
                let s3_key = self.key_mapper.to_s3_key(namespace, key);
                if let Some(value) = cache.get(&s3_key).await {
                    self.metrics.cache_hits.inc();
                    results[i] = Some(StorageValue::new(value));
                } else {
                    self.metrics.cache_misses.inc();
                    uncached_indices.push((i, key, s3_key));
                }
            }
        }

        // Check pending batches for uncached keys
        let mut still_uncached = Vec::new();
        for (i, key, s3_key) in uncached_indices {
            if let Some(value) = self.batch_manager.get_pending(&s3_key).await? {
                results[i] = Some(StorageValue::new(value));
            } else {
                still_uncached.push((i, key, s3_key));
            }
        }

        // Parallel fetch for remaining uncached keys
        if !still_uncached.is_empty() {
            let mut futures = Vec::new();
            for (i, _key, s3_key) in &still_uncached {
                let client = self.client.clone();
                let bucket = self.config.s3.bucket.clone();
                let encryptor = self.encryptor.clone();
                let metrics = self.metrics.clone();
                let s3_key_clone = s3_key.clone();
                let idx = *i;

                futures.push(async move {
                    let result = Self::fetch_from_s3_static(
                        &client,
                        &bucket,
                        &s3_key_clone,
                        &encryptor,
                        &metrics,
                    )
                    .await;
                    (idx, s3_key_clone, result)
                });
            }

            // Wait for all fetches
            let fetched = futures::future::join_all(futures).await;

            // Update results and cache
            let mut cache = self.read_cache.write().await;
            for (i, s3_key, result) in fetched {
                match result {
                    Ok(Some(data)) => {
                        cache.put(s3_key, data.clone()).await;
                        results[i] = Some(StorageValue::new(data));
                    }
                    Ok(None) => results[i] = None,
                    Err(e) => return Err(e),
                }
            }
        }

        Ok(results)
    }

    async fn get(
        &self,
        namespace: &StorageNamespace,
        key: &StorageKey,
    ) -> StorageResult<Option<StorageValue>> {
        let s3_key = self.key_mapper.to_s3_key(namespace, key);

        // Check cache first
        let cache = self.read_cache.read().await;
        if let Some(value) = cache.get(&s3_key).await {
            self.metrics.cache_hits.inc();
            return Ok(Some(StorageValue::new(value)));
        }
        drop(cache);

        self.metrics.cache_misses.inc();

        // Check pending batches
        if let Some(value) = self.batch_manager.get_pending(&s3_key).await? {
            return Ok(Some(StorageValue::new(value)));
        }

        // Read from S3
        if let Some(data) = self.read_from_s3(&s3_key).await? {
            // Update cache
            let mut cache = self.read_cache.write().await;
            cache.put(s3_key.clone(), data.clone()).await;
            Ok(Some(StorageValue::new(data)))
        } else {
            Ok(None)
        }
    }

    async fn put_with_hints(
        &self,
        namespace: &StorageNamespace,
        key: StorageKey,
        value: StorageValue,
        hints: StorageHints,
    ) -> StorageResult<()> {
        let s3_key = self.key_mapper.to_s3_key(namespace, &key);

        // Optimize based on access pattern
        match hints.access_pattern {
            crate::storage::traits::AccessPattern::WriteOnce => {
                // For write-once data, we could use a different storage class
                // but for now, just use normal processing
                self.put(namespace, key, value).await?
            }
            crate::storage::traits::AccessPattern::Temporary => {
                // Skip WAL for temporary data with low priority
                if hints.priority == crate::storage::traits::Priority::Low {
                    // Direct S3 write without WAL
                    let encrypted = self.encryptor.encrypt_for_s3(&value.0).await?;
                    self.client
                        .put_object()
                        .bucket(&self.config.s3.bucket)
                        .key(&s3_key)
                        .body(aws_sdk_s3::primitives::ByteStream::from(encrypted))
                        .send()
                        .await
                        .map_err(|e| StorageError::Backend(format!("S3 put failed: {}", e)))?;
                    self.metrics.s3_writes.inc();
                } else {
                    self.put(namespace, key, value).await?
                }
            }
            _ => self.put(namespace, key, value).await?,
        }

        // Handle eventual consistency hint
        if !hints.allow_eventual_consistency {
            // Force immediate S3 upload for consistency
            self.batch_manager.force_flush_key(&s3_key).await?;
        }

        Ok(())
    }

    async fn put(
        &self,
        namespace: &StorageNamespace,
        key: StorageKey,
        value: StorageValue,
    ) -> StorageResult<()> {
        let s3_key = self.key_mapper.to_s3_key(namespace, &key);

        // Write to WAL first for durability
        let wal_id = self.wal_client.append(&value.0).await?;

        // Add to batch
        self.batch_manager
            .add_entry(BatchEntry {
                s3_key: s3_key.clone(),
                data: value.0,
                wal_id,
            })
            .await?;

        // Invalidate cache
        let mut cache = self.read_cache.write().await;
        cache.invalidate(&s3_key).await;

        Ok(())
    }

    async fn delete(&self, namespace: &StorageNamespace, key: &StorageKey) -> StorageResult<()> {
        let s3_key = self.key_mapper.to_s3_key(namespace, key);

        // Add deletion marker to batch
        self.batch_manager.add_deletion(s3_key.clone()).await?;

        // Invalidate cache
        let mut cache = self.read_cache.write().await;
        cache.invalidate(&s3_key).await;

        Ok(())
    }

    async fn write_batch(&self, batch: WriteBatch) -> StorageResult<()> {
        use crate::storage::types::BatchOperation;

        let operations = batch.into_operations();
        if operations.is_empty() {
            return Ok(());
        }

        // Group operations by type for efficient processing
        let mut puts = Vec::new();
        let mut deletes = Vec::new();

        for op in operations {
            match op {
                BatchOperation::Put {
                    namespace,
                    key,
                    value,
                } => {
                    puts.push((namespace, key, value));
                }
                BatchOperation::Delete { namespace, key } => {
                    deletes.push((namespace, key));
                }
            }
        }

        // Process deletions using S3's batch delete API (up to 1000 objects per request)
        if !deletes.is_empty() {
            // Process in chunks of 1000 (S3's limit)
            for chunk in deletes.chunks(1000) {
                let mut delete_objects = aws_sdk_s3::types::Delete::builder();

                for (namespace, key) in chunk {
                    let s3_key = self.key_mapper.to_s3_key(namespace, key);
                    delete_objects = delete_objects.objects(
                        aws_sdk_s3::types::ObjectIdentifier::builder()
                            .key(s3_key)
                            .build()
                            .map_err(|e| {
                                StorageError::Backend(format!(
                                    "Failed to build delete object: {}",
                                    e
                                ))
                            })?,
                    );
                }

                let delete = delete_objects.build().map_err(|e| {
                    StorageError::Backend(format!("Failed to build delete request: {}", e))
                })?;

                self.client
                    .delete_objects()
                    .bucket(&self.config.s3.bucket)
                    .delete(delete)
                    .send()
                    .await
                    .map_err(|e| {
                        StorageError::Io(std::io::Error::other(format!(
                            "Batch delete failed: {}",
                            e
                        )))
                    })?;
            }
        }

        // Process puts - use batch manager for efficient batching
        if !puts.is_empty() {
            for (namespace, key, value) in puts {
                let s3_key = self.key_mapper.to_s3_key(&namespace, &key);

                // Create a BatchEntry and add it
                let entry = crate::storage::adaptors::s3::batching::BatchEntry {
                    s3_key,
                    data: value.0,
                    wal_id: crate::storage::adaptors::s3::wal_client::WalEntryId {
                        id: uuid::Uuid::new_v4(),
                        offset: 0,
                    },
                };

                self.batch_manager.add_entry(entry).await?;
            }

            // Ensure batch is processed
            self.batch_manager.force_flush().await?;
        }

        Ok(())
    }

    async fn iter(&self, namespace: &StorageNamespace) -> StorageResult<Self::Iterator> {
        let prefix = self.key_mapper.namespace_prefix(namespace);
        S3Iterator::new(
            self.client.clone(),
            self.config.s3.bucket.clone(),
            prefix,
            self.encryptor.clone(),
            self.key_mapper.clone(),
            namespace.clone(),
        )
        .await
    }

    async fn iter_range(
        &self,
        namespace: &StorageNamespace,
        range: impl RangeBounds<StorageKey> + Send,
    ) -> StorageResult<Self::Iterator> {
        let prefix = self.key_mapper.namespace_prefix(namespace);
        S3Iterator::new_with_range(
            self.client.clone(),
            self.config.s3.bucket.clone(),
            prefix,
            range,
            self.encryptor.clone(),
            self.key_mapper.clone(),
            namespace.clone(),
        )
        .await
    }

    async fn create_namespace(&self, _namespace: &StorageNamespace) -> StorageResult<()> {
        // S3 doesn't require explicit namespace creation
        Ok(())
    }

    async fn drop_namespace(&self, namespace: &StorageNamespace) -> StorageResult<()> {
        let prefix = self.key_mapper.namespace_prefix(namespace);

        // List and delete all objects with the namespace prefix
        let mut continuation_token = None;

        loop {
            let mut request = self
                .client
                .list_objects_v2()
                .bucket(&self.config.s3.bucket)
                .prefix(&prefix)
                .max_keys(1000);

            if let Some(token) = continuation_token {
                request = request.continuation_token(token);
            }

            let response = request.send().await.map_err(|e| {
                StorageError::Io(std::io::Error::other(format!(
                    "Failed to list objects: {}",
                    e
                )))
            })?;

            if let Some(contents) = response.contents {
                for object in contents {
                    if let Some(key) = object.key {
                        self.client
                            .delete_object()
                            .bucket(&self.config.s3.bucket)
                            .key(&key)
                            .send()
                            .await
                            .map_err(|e| {
                                StorageError::Io(std::io::Error::other(format!(
                                    "Failed to delete object: {}",
                                    e
                                )))
                            })?;
                    }
                }
            }

            if response.is_truncated == Some(false) {
                break;
            }

            continuation_token = response.next_continuation_token;
        }

        Ok(())
    }

    async fn list_namespaces(&self) -> StorageResult<Vec<StorageNamespace>> {
        // List all unique namespace prefixes
        // This is a simplified implementation - in production you might want to
        // maintain a metadata object that tracks all namespaces

        let mut namespaces = Vec::new();
        let mut seen_prefixes = std::collections::HashSet::new();
        let mut continuation_token = None;

        loop {
            let mut request = self
                .client
                .list_objects_v2()
                .bucket(&self.config.s3.bucket)
                .prefix(&self.config.s3.prefix)
                .delimiter("/")
                .max_keys(1000);

            if let Some(token) = continuation_token {
                request = request.continuation_token(token);
            }

            let response = request.send().await.map_err(|e| {
                StorageError::Io(std::io::Error::other(format!(
                    "Failed to list objects: {}",
                    e
                )))
            })?;

            if let Some(prefixes) = response.common_prefixes {
                for prefix in prefixes {
                    if let Some(p) = prefix.prefix {
                        // Extract namespace from prefix
                        if let Some(ns) = self.key_mapper.extract_namespace(&p) {
                            if seen_prefixes.insert(ns.clone()) {
                                namespaces.push(StorageNamespace::new(&ns));
                            }
                        }
                    }
                }
            }

            if response.is_truncated == Some(false) {
                break;
            }

            continuation_token = response.next_continuation_token;
        }

        Ok(namespaces)
    }

    async fn flush(&self) -> StorageResult<()> {
        self.batch_manager.force_flush().await
    }

    async fn namespace_size(&self, namespace: &StorageNamespace) -> StorageResult<u64> {
        let prefix = self.key_mapper.namespace_prefix(namespace);
        let mut total_size = 0u64;
        let mut continuation_token = None;

        loop {
            let mut request = self
                .client
                .list_objects_v2()
                .bucket(&self.config.s3.bucket)
                .prefix(&prefix)
                .max_keys(1000);

            if let Some(token) = continuation_token {
                request = request.continuation_token(token);
            }

            let response = request.send().await.map_err(|e| {
                StorageError::Io(std::io::Error::other(format!(
                    "Failed to list objects: {}",
                    e
                )))
            })?;

            if let Some(contents) = response.contents {
                for object in contents {
                    if let Some(size) = object.size {
                        total_size += size as u64;
                    }
                }
            }

            if response.is_truncated == Some(false) {
                break;
            }

            continuation_token = response.next_continuation_token;
        }

        Ok(total_size)
    }
}

// Implement LogStorage trait with enhanced methods
#[async_trait]
impl LogStorage for S3StorageAdaptor {
    async fn append_log_batch(
        &self,
        namespace: &StorageNamespace,
        entries: &[(u64, Bytes)],
    ) -> StorageResult<()> {
        // Group sequential entries for optimal batching
        let mut sequential_groups = Vec::new();
        let mut current_group = Vec::new();
        let mut last_index = None;

        for (index, data) in entries {
            if let Some(last) = last_index {
                if *index != last + 1 {
                    // Gap detected, start new group
                    if !current_group.is_empty() {
                        sequential_groups.push(current_group);
                        current_group = Vec::new();
                    }
                }
            }
            current_group.push((*index, data.clone()));
            last_index = Some(*index);
        }

        if !current_group.is_empty() {
            sequential_groups.push(current_group);
        }

        // Process each group as a batch
        for group in sequential_groups {
            // For now, just append each entry individually
            // In production, you'd serialize the group as a single object
            for (index, data) in group {
                self.append_log(namespace, index, data).await?;
            }
        }

        Ok(())
    }
}

// Implement SnapshotStorage for S3
#[async_trait]
impl SnapshotStorage for S3StorageAdaptor {
    async fn create_snapshot(
        &self,
        namespace: &StorageNamespace,
        snapshot_id: &str,
    ) -> StorageResult<Bytes> {
        info!(
            "Creating snapshot {} for namespace {}",
            snapshot_id,
            namespace.as_str()
        );

        // Collect all data from the namespace
        let prefix = self.key_mapper.namespace_prefix(namespace);
        let mut entry_count = 0u64;

        // List all keys in the namespace
        let mut continuation_token = None;
        let mut keys_and_values = Vec::new();

        loop {
            let mut request = self
                .client
                .list_objects_v2()
                .bucket(&self.config.s3.bucket)
                .prefix(&prefix)
                .max_keys(1000);

            if let Some(token) = continuation_token {
                request = request.continuation_token(token);
            }

            let response = request.send().await.map_err(|e| {
                StorageError::Io(std::io::Error::other(format!(
                    "Failed to list objects for snapshot: {}",
                    e
                )))
            })?;

            if let Some(contents) = response.contents {
                for object in contents {
                    if let Some(key) = object.key {
                        // Skip snapshot-related keys
                        if key.contains("/__snapshots__/") {
                            continue;
                        }

                        // Fetch the object
                        if let Some(data) = self.read_from_s3(&key).await? {
                            let relative_key = key.strip_prefix(&prefix).unwrap_or(&key);
                            keys_and_values.push((relative_key.to_string(), data));
                            entry_count += 1;
                        }
                    }
                }
            }

            if response.is_truncated == Some(false) {
                break;
            }

            continuation_token = response.next_continuation_token;
        }

        // Create snapshot data structure
        let snapshot_data = SnapshotData {
            namespace: namespace.as_str().to_string(),
            snapshot_id: snapshot_id.to_string(),
            created_at: chrono::Utc::now().timestamp_millis() as u64,
            entry_count,
            entries: keys_and_values,
        };

        // Serialize the snapshot
        let mut serialized = Vec::new();
        ciborium::into_writer(&snapshot_data, &mut serialized).map_err(|e| {
            StorageError::InvalidValue(format!("Failed to serialize snapshot: {}", e))
        })?;

        // Compress the snapshot
        let compressed = self.compress_snapshot(&serialized)?;

        // Store the snapshot in S3
        let snapshot_key = self.key_mapper.snapshot_key(namespace, snapshot_id);
        let encrypted = self.encryptor.encrypt_for_s3(&compressed).await?;

        self.client
            .put_object()
            .bucket(&self.config.s3.bucket)
            .key(&snapshot_key)
            .body(aws_sdk_s3::primitives::ByteStream::from(encrypted))
            .metadata("snapshot-id", snapshot_id)
            .metadata("namespace", namespace.as_str())
            .metadata("created-at", snapshot_data.created_at.to_string())
            .metadata("entry-count", entry_count.to_string())
            .metadata("compressed-size", compressed.len().to_string())
            .send()
            .await
            .map_err(|e| StorageError::Backend(format!("Failed to store snapshot: {}", e)))?;

        self.metrics.s3_writes.inc();

        info!(
            "Snapshot {} created successfully with {} entries",
            snapshot_id, entry_count
        );

        Ok(Bytes::from(compressed))
    }

    async fn restore_snapshot(
        &self,
        namespace: &StorageNamespace,
        snapshot_id: &str,
        data: Bytes,
    ) -> StorageResult<()> {
        info!(
            "Restoring snapshot {} to namespace {}",
            snapshot_id,
            namespace.as_str()
        );

        // Decompress the snapshot
        let decompressed = self.decompress_snapshot(data.as_ref())?;

        // Deserialize the snapshot
        let snapshot_data: SnapshotData =
            ciborium::from_reader(&decompressed[..]).map_err(|e| {
                StorageError::InvalidValue(format!("Failed to deserialize snapshot: {}", e))
            })?;

        // Verify snapshot is for the correct namespace
        if snapshot_data.namespace != namespace.as_str() {
            return Err(StorageError::InvalidValue(format!(
                "Snapshot namespace mismatch: expected {}, got {}",
                namespace.as_str(),
                snapshot_data.namespace
            )));
        }

        // Clear existing data in the namespace
        self.drop_namespace(namespace).await?;
        self.create_namespace(namespace).await?;

        // Restore all entries
        let mut batch_count = 0;
        let mut batch: Vec<(String, Vec<u8>)> = Vec::new();
        const BATCH_SIZE: usize = 100;

        for (key, value) in snapshot_data.entries {
            let full_key = format!("{}{}", self.key_mapper.namespace_prefix(namespace), key);
            let encrypted = self.encryptor.encrypt_for_s3(&value).await?;

            batch.push((full_key, encrypted.to_vec()));
            batch_count += 1;

            if batch.len() >= BATCH_SIZE {
                self.upload_batch(&batch).await?;
                batch.clear();
            }
        }

        // Upload remaining entries
        if !batch.is_empty() {
            self.upload_batch(&batch).await?;
        }

        info!(
            "Snapshot {} restored successfully with {} entries",
            snapshot_id, batch_count
        );

        Ok(())
    }

    async fn list_snapshots(&self, namespace: &StorageNamespace) -> StorageResult<Vec<String>> {
        let prefix = self.key_mapper.snapshot_prefix(namespace);
        let mut snapshot_ids = Vec::new();
        let mut continuation_token = None;

        loop {
            let mut request = self
                .client
                .list_objects_v2()
                .bucket(&self.config.s3.bucket)
                .prefix(&prefix)
                .max_keys(1000);

            if let Some(token) = continuation_token {
                request = request.continuation_token(token);
            }

            let response = request.send().await.map_err(|e| {
                StorageError::Io(std::io::Error::other(format!(
                    "Failed to list snapshots: {}",
                    e
                )))
            })?;

            if let Some(contents) = response.contents {
                for object in contents {
                    if let Some(key) = object.key {
                        // Extract snapshot ID from key
                        if let Some(snapshot_id) = self.key_mapper.extract_snapshot_id(&key) {
                            snapshot_ids.push(snapshot_id);
                        }
                    }
                }
            }

            if response.is_truncated == Some(false) {
                break;
            }

            continuation_token = response.next_continuation_token;
        }

        Ok(snapshot_ids)
    }

    async fn delete_snapshot(
        &self,
        namespace: &StorageNamespace,
        snapshot_id: &str,
    ) -> StorageResult<()> {
        let snapshot_key = self.key_mapper.snapshot_key(namespace, snapshot_id);

        self.client
            .delete_object()
            .bucket(&self.config.s3.bucket)
            .key(&snapshot_key)
            .send()
            .await
            .map_err(|e| {
                StorageError::Io(std::io::Error::other(format!(
                    "Failed to delete snapshot: {}",
                    e
                )))
            })?;

        self.metrics.s3_deletes.inc();

        Ok(())
    }

    async fn snapshot_metadata(
        &self,
        namespace: &StorageNamespace,
        snapshot_id: &str,
    ) -> StorageResult<SnapshotMetadata> {
        let snapshot_key = self.key_mapper.snapshot_key(namespace, snapshot_id);

        let response = self
            .client
            .head_object()
            .bucket(&self.config.s3.bucket)
            .key(&snapshot_key)
            .send()
            .await
            .map_err(|e| {
                StorageError::Io(std::io::Error::other(format!(
                    "Failed to get snapshot metadata: {}",
                    e
                )))
            })?;

        let size = response.content_length().unwrap_or(0) as u64;

        // Extract metadata from S3 object metadata
        let created_at = response
            .metadata()
            .and_then(|m| m.get("created-at"))
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or_else(|| chrono::Utc::now().timestamp_millis() as u64);

        Ok(SnapshotMetadata {
            id: snapshot_id.to_string(),
            size,
            created_at,
            namespace: namespace.clone(),
        })
    }
}

// Snapshot data structure
#[derive(Debug, Serialize, Deserialize)]
struct SnapshotData {
    namespace: String,
    snapshot_id: String,
    created_at: u64,
    entry_count: u64,
    entries: Vec<(String, Bytes)>,
}

use serde::{Deserialize, Serialize};
