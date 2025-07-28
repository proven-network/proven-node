//! Production-grade S3 log storage implementation with advanced features

pub mod batching;
pub mod cache;
pub mod config;
pub mod encryption;
pub mod metrics;
pub mod wal;

use async_trait::async_trait;
use aws_sdk_s3::{Client as S3Client, primitives::ByteStream};
use bytes::Bytes;
use dashmap::DashMap;
use lru::LruCache;
use proven_storage::{LogIndex, LogStorage, StorageError, StorageNamespace, StorageResult};
use std::{
    collections::HashMap,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};
use tokio::sync::{RwLock, broadcast, mpsc};
use tokio_stream::Stream;
use tracing::{error, info, warn};
use uuid::Uuid;

/// Type aliases to reduce type complexity
type LogBoundsCache = Arc<RwLock<HashMap<StorageNamespace, (LogIndex, LogIndex)>>>;
type PendingWritesMap = Arc<RwLock<HashMap<StorageNamespace, HashMap<LogIndex, Bytes>>>>;
type AppendCache = Arc<DashMap<StorageNamespace, Arc<Mutex<LruCache<LogIndex, Bytes>>>>>;

use crate::{
    batching::{BatchManager, LogBatch, UploadManager},
    cache::LogCache,
    config::S3StorageConfig,
    encryption::EncryptionHandler,
    metrics::*,
    wal::{WalClient, WalRecovery},
};

/// Production S3 log storage implementation
#[derive(Clone)]
pub struct S3Storage {
    /// S3 client
    client: Arc<S3Client>,
    /// Configuration
    config: Arc<S3StorageConfig>,
    /// Log bounds cache: namespace -> (first_index, last_index)
    log_bounds: LogBoundsCache,
    /// Batch manager for write optimization
    batch_manager: Arc<BatchManager>,
    /// Read cache
    cache: Arc<LogCache>,
    /// WAL client for durability
    wal_client: Option<Arc<WalClient>>,
    /// Encryption handler
    encryption: Option<Arc<EncryptionHandler>>,
    /// Pending writes for read-after-write consistency
    pending_writes: PendingWritesMap,
    /// Per-namespace broadcast channels for notifications when new entries are added
    namespace_notifiers: Arc<DashMap<StorageNamespace, broadcast::Sender<()>>>,
    /// Per-namespace LRU cache for recently appended entries
    append_cache: AppendCache,
    /// Cache configuration - entries per namespace
    cache_entries_per_namespace: usize,
}

impl S3Storage {
    /// Create a new S3 storage instance with full production features
    pub async fn new(client: S3Client, config: S3StorageConfig) -> StorageResult<Self> {
        // Initialize metrics
        S3StorageMetrics::init();

        // Create batch upload channel
        let (batch_tx, batch_rx) = mpsc::channel(100);

        // Create WAL client if configured
        let wal_client = match WalClient::new(config.wal.clone()).await {
            Ok(client) => {
                info!("WAL client connected successfully");
                Some(Arc::new(client))
            }
            Err(e) => {
                warn!("Failed to connect WAL client: {}. Running without WAL.", e);
                None
            }
        };

        // Create encryption handler if configured
        let encryption = match &config.encryption {
            Some(enc_config) => {
                match EncryptionHandler::new(enc_config.clone(), config.compression_threshold) {
                    Ok(handler) => {
                        info!("Encryption enabled");
                        Some(Arc::new(handler))
                    }
                    Err(e) => {
                        error!("Failed to initialize encryption: {}", e);
                        return Err(e);
                    }
                }
            }
            None => None,
        };

        let client = Arc::new(client);
        let config = Arc::new(config);

        // Create instance for upload closure
        let storage = Self {
            client: client.clone(),
            config: config.clone(),
            log_bounds: Arc::new(RwLock::new(HashMap::new())),
            batch_manager: Arc::new(BatchManager::new(config.batch.clone(), batch_tx)),
            cache: Arc::new(LogCache::new(config.cache.clone())),
            wal_client: wal_client.clone(),
            encryption: encryption.clone(),
            pending_writes: Arc::new(RwLock::new(HashMap::new())),
            namespace_notifiers: Arc::new(DashMap::new()),
            append_cache: Arc::new(DashMap::new()),
            cache_entries_per_namespace: config.append_cache.entries_per_namespace,
        };

        // Create upload manager with closure that captures storage
        let upload_storage = storage.clone();
        let _upload_manager =
            UploadManager::new(config.batch.upload_workers, batch_rx, move |batch| {
                let storage = upload_storage.clone();
                async move { storage.upload_batch(batch).await }
            });

        // Recover from WAL if available
        if let Some(wal) = &storage.wal_client {
            let recovery = WalRecovery::new(wal.clone());
            match recovery.recover().await {
                Ok(recovered) => {
                    if !recovered.is_empty() {
                        info!("Recovered {} namespaces from WAL", recovered.len());
                        for (namespace, entries) in recovered {
                            // Convert u64 entries to LogIndex with Arc
                            let converted_entries: Vec<(LogIndex, Arc<Bytes>)> = entries
                                .into_iter()
                                .filter_map(|(idx, data)| {
                                    LogIndex::new(idx).map(|nz| (nz, Arc::new(data)))
                                })
                                .collect();

                            // Re-submit recovered entries using put_at since we have specific indices
                            if let Err(e) = storage.put_at(&namespace, converted_entries).await {
                                error!("Failed to re-append recovered entries: {}", e);
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("WAL recovery failed: {}", e);
                }
            }
        }

        Ok(storage)
    }

    /// Construct an S3 key for a log entry
    fn make_key(&self, namespace: &StorageNamespace, index: LogIndex) -> String {
        let base = format!("{}/{:020}", namespace.as_str(), index.get());
        match &self.config.s3.prefix {
            Some(prefix) => format!("{prefix}/{base}"),
            None => base,
        }
    }

    /// Parse an index from an S3 key
    fn parse_index(&self, key: &str) -> Option<LogIndex> {
        let parts: Vec<&str> = key.split('/').collect();
        if let Some(index_str) = parts.last() {
            index_str.parse::<u64>().ok().and_then(LogIndex::new)
        } else {
            None
        }
    }

    /// Upload a batch to S3
    async fn upload_batch(&self, mut batch: LogBatch) -> Result<(), String> {
        let timer = start_timer("upload_batch", batch.namespace.as_str());

        // Generate batch ID
        let batch_id = Uuid::new_v4().to_string();

        // Write to WAL first if available
        if let Some(wal) = &self.wal_client {
            let wal_timer = record_wal_operation("append", true);

            // Prepare entries for WAL
            let wal_entries: Vec<(u64, Bytes)> = if let Some(encryption) = &self.encryption {
                // Encrypt for WAL
                let mut encrypted = Vec::new();
                for (idx, data) in &batch.entries {
                    match encryption.encrypt_for_wal(data).await {
                        Ok(enc_data) => encrypted.push((idx.get(), Bytes::from(enc_data))),
                        Err(e) => {
                            error!("WAL encryption failed: {}", e);
                            Self::complete_batch_with_error(
                                &mut batch,
                                format!("Encryption failed: {e}"),
                            );
                            return Err("Encryption failed".to_string());
                        }
                    }
                }
                encrypted
            } else {
                batch
                    .entries
                    .iter()
                    .map(|(idx, data)| (idx.get(), data.clone()))
                    .collect()
            };

            // Write to WAL
            if let Err(e) = wal
                .append_logs(&batch.namespace, &wal_entries, batch_id.clone())
                .await
            {
                error!("WAL write failed: {}", e);
                wal_timer.record();
                Self::complete_batch_with_error(&mut batch, format!("WAL write failed: {e}"));
                return Err("WAL write failed".to_string());
            }

            wal_timer.record();
        }

        // Add to pending writes for read-after-write consistency
        {
            let mut pending = self.pending_writes.write().await;
            let namespace_pending = pending
                .entry(batch.namespace.clone())
                .or_insert_with(HashMap::new);
            for (idx, data) in &batch.entries {
                namespace_pending.insert(*idx, data.clone());
            }
        }

        // Upload each entry to S3
        let mut upload_success = true;
        let mut _total_uploaded = 0usize;

        for (index, data) in &batch.entries {
            let key = self.make_key(&batch.namespace, *index);

            // Encrypt data if needed
            let upload_data = if let Some(encryption) = &self.encryption {
                match encryption.encrypt_for_s3(data).await {
                    Ok(encrypted) => {
                        record_encryption("encrypt", "s3");
                        encrypted
                    }
                    Err(e) => {
                        error!("S3 encryption failed: {}", e);
                        upload_success = false;
                        break;
                    }
                }
            } else {
                data.clone()
            };

            // Upload to S3
            let mut put_request = self
                .client
                .put_object()
                .bucket(&self.config.s3.bucket)
                .key(&key)
                .body(ByteStream::from(upload_data.clone()));

            // Use S3 One Zone storage class if configured
            if self.config.s3.use_one_zone {
                put_request = put_request.storage_class(aws_sdk_s3::types::StorageClass::OnezoneIa);
            }

            let result = put_request.send().await;

            match result {
                Ok(_) => {
                    _total_uploaded += upload_data.len();
                    record_s3_upload(batch.namespace.as_str(), upload_data.len(), true);

                    // Update cache
                    self.cache.put(&batch.namespace, *index, data.clone()).await;
                }
                Err(e) => {
                    error!("S3 upload failed for key {}: {}", key, e);
                    record_s3_upload(batch.namespace.as_str(), 0, false);
                    upload_success = false;
                    break;
                }
            }
        }

        // Update bounds if successful
        if upload_success {
            self.update_bounds_for_batch(&batch).await;

            // Confirm with WAL
            if let Some(wal) = &self.wal_client {
                if let Err(e) = wal.confirm_batch(batch_id).await {
                    warn!("Failed to confirm batch with WAL: {}", e);
                }

                // Update pending size
                let batch_size: usize = batch.entries.iter().map(|(_, d)| d.len() + 8).sum();
                wal.update_pending_size(batch_size).await;
            }

            // Add entries to append cache if enabled
            if self.config.append_cache.enabled {
                self.add_to_append_cache(&batch).await;
            }

            // Complete all pending operations
            Self::complete_batch_with_success(&mut batch);

            // Remove from pending writes
            {
                let mut pending = self.pending_writes.write().await;
                if let Some(namespace_pending) = pending.get_mut(&batch.namespace) {
                    for (idx, _) in &batch.entries {
                        namespace_pending.remove(idx);
                    }
                }
            }

            // Notify any waiting streams for this specific namespace
            if let Some(notifier) = self.namespace_notifiers.get(&batch.namespace) {
                let _ = notifier.send(());
            }

            // Record metrics
            record_batch(
                batch.namespace.as_str(),
                batch.total_size,
                batch.entries.len(),
            );
            record_operation("batch_upload", batch.namespace.as_str(), true);
        } else {
            Self::complete_batch_with_error(&mut batch, "S3 upload failed".to_string());
            record_operation("batch_upload", batch.namespace.as_str(), false);
        }

        timer.record();

        if upload_success {
            Ok(())
        } else {
            Err("Batch upload failed".to_string())
        }
    }

    /// Update bounds after successful batch upload
    async fn update_bounds_for_batch(&self, batch: &LogBatch) {
        let mut bounds = self.log_bounds.write().await;
        let existing_bounds = bounds.get(&batch.namespace).copied();
        let mut first_index: Option<LogIndex> = existing_bounds.map(|(first, _)| first);
        let mut last_index: Option<LogIndex> = existing_bounds.map(|(_, last)| last);

        for (index, _) in &batch.entries {
            match first_index {
                None => first_index = Some(*index),
                Some(first) if *index < first => first_index = Some(*index),
                _ => {}
            }

            match last_index {
                None => last_index = Some(*index),
                Some(last) if *index > last => last_index = Some(*index),
                _ => {}
            }
        }

        if let (Some(first), Some(last)) = (first_index, last_index) {
            bounds.insert(batch.namespace.clone(), (first, last));
        }
    }

    /// Complete batch with success
    fn complete_batch_with_success(batch: &mut LogBatch) {
        for completion in batch.completions.drain(..) {
            let _ = completion.send(Ok(()));
        }
    }

    /// Complete batch with error
    fn complete_batch_with_error(batch: &mut LogBatch, error: String) {
        for completion in batch.completions.drain(..) {
            let _ = completion.send(Err(error.clone()));
        }
    }

    /// Add batch entries to the append cache
    async fn add_to_append_cache(&self, batch: &LogBatch) {
        let cache = self
            .append_cache
            .entry(batch.namespace.clone())
            .or_insert_with(|| {
                Arc::new(Mutex::new(LruCache::new(
                    NonZeroUsize::new(self.cache_entries_per_namespace).unwrap(),
                )))
            })
            .clone();

        let mut cache_guard = cache.lock().unwrap();
        for (index, data) in &batch.entries {
            cache_guard.put(*index, data.clone());
        }
    }
}

#[async_trait]
impl LogStorage for S3Storage {
    async fn append(
        &self,
        namespace: &StorageNamespace,
        entries: Arc<Vec<Bytes>>,
    ) -> StorageResult<LogIndex> {
        if entries.is_empty() {
            return Err(StorageError::InvalidValue(
                "Cannot append empty entries".to_string(),
            ));
        }

        let timer = start_timer("append", namespace.as_str());

        // Get the next index based on current bounds
        let bounds = self.bounds(namespace).await?;
        let start_index = if let Some((_, last)) = bounds {
            LogIndex::new(last.get() + 1).unwrap()
        } else {
            LogIndex::new(1).unwrap()
        };

        // Build entries with sequential indices
        let mut indexed_entries = Vec::new();
        let mut last_index = start_index;
        for (i, data) in entries.iter().enumerate() {
            let index = LogIndex::new(start_index.get() + i as u64).unwrap();
            indexed_entries.push((index, data.clone()));
            last_index = index;
        }

        // Use batch manager for optimized writes
        self.batch_manager
            .add_entries(namespace.clone(), indexed_entries)
            .await
            .map_err(StorageError::Backend)?;

        record_operation("append", namespace.as_str(), true);
        timer.record();

        Ok(last_index)
    }

    async fn put_at(
        &self,
        namespace: &StorageNamespace,
        entries: Vec<(LogIndex, Arc<Bytes>)>,
    ) -> StorageResult<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let timer = start_timer("put_at", namespace.as_str());

        // Convert Arc<Bytes> to Bytes for batch manager
        let entries: Vec<(LogIndex, Bytes)> = entries
            .into_iter()
            .map(|(idx, data)| (idx, (*data).clone()))
            .collect();

        // Use batch manager for optimized writes
        self.batch_manager
            .add_entries(namespace.clone(), entries)
            .await
            .map_err(StorageError::Backend)?;

        record_operation("put_at", namespace.as_str(), true);
        timer.record();

        Ok(())
    }

    async fn read_range(
        &self,
        namespace: &StorageNamespace,
        start: LogIndex,
        end: LogIndex,
    ) -> StorageResult<Vec<(LogIndex, Bytes)>> {
        let timer = start_timer("read_range", namespace.as_str());
        let mut entries = Vec::new();
        let mut cache_hits = 0;
        let mut cache_misses = 0;
        let mut indices_to_fetch = Vec::new();

        // First pass: collect data from pending writes and cache
        let mut current = start.get();
        while current < end.get() {
            let index = LogIndex::new(current).unwrap();
            // Check pending writes first for read-after-write consistency
            let from_pending = {
                let pending = self.pending_writes.read().await;
                if let Some(namespace_pending) = pending.get(namespace) {
                    namespace_pending.get(&index).cloned()
                } else {
                    None
                }
            };

            if let Some(data) = from_pending {
                entries.push((index, data));
                continue;
            }

            // Check cache
            if let Some(data) = self.cache.get(namespace, index).await {
                entries.push((index, data));
                cache_hits += 1;
                continue;
            }

            cache_misses += 1;

            // Check bloom filter to avoid unnecessary S3 calls
            if self.cache.might_exist(namespace, index).await {
                indices_to_fetch.push(index);
            }
            current += 1;
        }

        // Batch fetch missing entries from S3
        if !indices_to_fetch.is_empty() {
            let s3_results = self.batch_read_from_s3(namespace, &indices_to_fetch).await;

            // Update cache and collect results
            for &index in &indices_to_fetch {
                if let Some(data) = s3_results.get(&index) {
                    // Update cache
                    self.cache.put(namespace, index, data.clone()).await;
                    entries.push((index, data.clone()));
                    record_s3_download(namespace.as_str(), data.len(), true);
                }
            }
        }

        // Sort entries by index since we collected them out of order
        entries.sort_by_key(|(idx, _)| *idx);

        // Record cache metrics
        if cache_hits > 0 {
            for _ in 0..cache_hits {
                record_cache_hit(namespace.as_str());
            }
        }
        if cache_misses > 0 {
            for _ in 0..cache_misses {
                record_cache_miss(namespace.as_str());
            }
        }

        record_operation("read_range", namespace.as_str(), true);
        timer.record();

        Ok(entries)
    }

    async fn truncate_after(
        &self,
        namespace: &StorageNamespace,
        index: LogIndex,
    ) -> StorageResult<()> {
        let mut bounds = self.log_bounds.write().await;

        // List all objects after the index
        let prefix = format!("{}/", namespace.as_str());
        let full_prefix = match &self.config.s3.prefix {
            Some(p) => format!("{p}/{prefix}"),
            None => prefix,
        };

        let mut continuation_token = None;
        let mut keys_to_delete = Vec::new();

        loop {
            let mut request = self
                .client
                .list_objects_v2()
                .bucket(&self.config.s3.bucket)
                .prefix(&full_prefix);

            if let Some(token) = continuation_token {
                request = request.continuation_token(token);
            }

            let response = request
                .send()
                .await
                .map_err(|e| StorageError::Backend(format!("S3 list failed: {e}")))?;

            if let Some(contents) = response.contents {
                for obj in contents {
                    if let Some(key) = obj.key
                        && let Some(obj_index) = self.parse_index(&key)
                        && obj_index > index
                    {
                        keys_to_delete.push(key);
                    }
                }
            }

            if response.is_truncated.unwrap_or(false) {
                continuation_token = response.next_continuation_token;
            } else {
                break;
            }
        }

        // Delete all keys after index
        for key in keys_to_delete {
            self.client
                .delete_object()
                .bucket(&self.config.s3.bucket)
                .key(&key)
                .send()
                .await
                .map_err(|e| StorageError::Backend(format!("S3 delete failed: {e}")))?;
        }

        // Update bounds cache
        if let Some((first, last)) = bounds.get_mut(namespace) {
            if index < *last {
                *last = index;
            }
            if *first > *last {
                bounds.remove(namespace);
            }
        }

        Ok(())
    }

    async fn compact_before(
        &self,
        namespace: &StorageNamespace,
        index: LogIndex,
    ) -> StorageResult<()> {
        let mut bounds = self.log_bounds.write().await;

        // List all objects before and including the index
        let prefix = format!("{}/", namespace.as_str());
        let full_prefix = match &self.config.s3.prefix {
            Some(p) => format!("{p}/{prefix}"),
            None => prefix,
        };

        let mut continuation_token = None;
        let mut keys_to_delete = Vec::new();

        loop {
            let mut request = self
                .client
                .list_objects_v2()
                .bucket(&self.config.s3.bucket)
                .prefix(&full_prefix);

            if let Some(token) = continuation_token {
                request = request.continuation_token(token);
            }

            let response = request
                .send()
                .await
                .map_err(|e| StorageError::Backend(format!("S3 list failed: {e}")))?;

            if let Some(contents) = response.contents {
                for obj in contents {
                    if let Some(key) = obj.key
                        && let Some(obj_index) = self.parse_index(&key)
                        && obj_index <= index
                    {
                        keys_to_delete.push(key);
                    }
                }
            }

            if response.is_truncated.unwrap_or(false) {
                continuation_token = response.next_continuation_token;
            } else {
                break;
            }
        }

        // Delete all keys up to and including index
        for key in keys_to_delete {
            self.client
                .delete_object()
                .bucket(&self.config.s3.bucket)
                .key(&key)
                .send()
                .await
                .map_err(|e| StorageError::Backend(format!("S3 delete failed: {e}")))?;
        }

        // Update bounds cache
        if let Some((first, last)) = bounds.get_mut(namespace) {
            if index >= *first {
                if let Some(next) = LogIndex::new(index.get() + 1) {
                    *first = next;
                } else {
                    // If index + 1 overflows, remove the bounds entirely
                    bounds.remove(namespace);
                    return Ok(());
                }
            }
            if *first > *last {
                bounds.remove(namespace);
            }
        }

        Ok(())
    }

    async fn bounds(
        &self,
        namespace: &StorageNamespace,
    ) -> StorageResult<Option<(LogIndex, LogIndex)>> {
        // First check cache
        {
            let bounds = self.log_bounds.read().await;
            if let Some(&cached) = bounds.get(namespace) {
                return Ok(Some(cached));
            }
        }

        // If not in cache, list objects to find bounds
        let prefix = format!("{}/", namespace.as_str());
        let full_prefix = match &self.config.s3.prefix {
            Some(p) => format!("{p}/{prefix}"),
            None => prefix,
        };

        let mut continuation_token = None;
        let mut min_index: Option<LogIndex> = None;
        let mut max_index: Option<LogIndex> = None;

        loop {
            let mut request = self
                .client
                .list_objects_v2()
                .bucket(&self.config.s3.bucket)
                .prefix(&full_prefix);

            if let Some(token) = continuation_token {
                request = request.continuation_token(token);
            }

            let response = request
                .send()
                .await
                .map_err(|e| StorageError::Backend(format!("S3 list failed: {e}")))?;

            if let Some(contents) = response.contents {
                for obj in contents {
                    if let Some(key) = obj.key
                        && let Some(index) = self.parse_index(&key)
                    {
                        match min_index {
                            None => min_index = Some(index),
                            Some(min) if index < min => min_index = Some(index),
                            _ => {}
                        }
                        match max_index {
                            None => max_index = Some(index),
                            Some(max) if index > max => max_index = Some(index),
                            _ => {}
                        }
                    }
                }
            }

            if response.is_truncated.unwrap_or(false) {
                continuation_token = response.next_continuation_token;
            } else {
                break;
            }
        }

        if let (Some(min), Some(max)) = (min_index, max_index) {
            // Update cache
            let mut bounds = self.log_bounds.write().await;
            bounds.insert(namespace.clone(), (min, max));
            Ok(Some((min, max)))
        } else {
            Ok(None)
        }
    }

    async fn get_metadata(
        &self,
        namespace: &StorageNamespace,
        key: &str,
    ) -> StorageResult<Option<Bytes>> {
        let timer = start_timer("get_metadata", namespace.as_str());

        // Construct metadata key
        let metadata_key = format!("{}/metadata/{}", namespace.as_str(), key);
        let full_key = match &self.config.s3.prefix {
            Some(prefix) => format!("{prefix}/{metadata_key}"),
            None => metadata_key,
        };

        // Try to get from S3
        match self
            .client
            .get_object()
            .bucket(&self.config.s3.bucket)
            .key(&full_key)
            .send()
            .await
        {
            Ok(output) => {
                let body = output.body.collect().await.map_err(|e| {
                    StorageError::Backend(format!("Failed to read metadata body: {e}"))
                })?;

                timer.record();
                record_operation("get_metadata", namespace.as_str(), true);
                Ok(Some(body.into_bytes()))
            }
            Err(sdk_err) => {
                use aws_sdk_s3::error::SdkError;
                use aws_sdk_s3::operation::get_object::GetObjectError;

                match sdk_err {
                    SdkError::ServiceError(err) => match err.err() {
                        GetObjectError::NoSuchKey(_) => {
                            timer.record();
                            Ok(None)
                        }
                        _ => {
                            record_operation("get_metadata", namespace.as_str(), false);
                            Err(StorageError::Backend(format!(
                                "S3 get metadata failed: {}",
                                err.err()
                            )))
                        }
                    },
                    _ => {
                        record_operation("get_metadata", namespace.as_str(), false);
                        Err(StorageError::Backend(format!(
                            "S3 get metadata failed: {sdk_err}"
                        )))
                    }
                }
            }
        }
    }

    async fn set_metadata(
        &self,
        namespace: &StorageNamespace,
        key: &str,
        value: Bytes,
    ) -> StorageResult<()> {
        let timer = start_timer("set_metadata", namespace.as_str());

        // If WAL is available, write metadata through WAL for durability and batching
        if let Some(wal) = &self.wal_client {
            let wal_timer = record_wal_operation("set_metadata", true);

            // WAL will handle batching of metadata writes
            wal.set_metadata(namespace, key, value.clone())
                .await
                .map_err(|e| StorageError::Backend(format!("WAL metadata write failed: {e}")))?;

            wal_timer.record();
            timer.record();
            record_operation("set_metadata", namespace.as_str(), true);
            return Ok(());
        }

        // Direct S3 write if no WAL
        let metadata_key = format!("{}/metadata/{}", namespace.as_str(), key);
        let full_key = match &self.config.s3.prefix {
            Some(prefix) => format!("{prefix}/{metadata_key}"),
            None => metadata_key,
        };

        // Apply encryption if configured
        let data_to_write = if let Some(encryption) = &self.encryption {
            encryption
                .encrypt_for_s3(&value)
                .await
                .map_err(|e| StorageError::Backend(format!("Metadata encryption failed: {e}")))?
        } else {
            value
        };

        self.client
            .put_object()
            .bucket(&self.config.s3.bucket)
            .key(&full_key)
            .body(ByteStream::from(data_to_write))
            .send()
            .await
            .map_err(|e| StorageError::Backend(format!("S3 put metadata failed: {e}")))?;

        timer.record();
        record_operation("set_metadata", namespace.as_str(), true);
        Ok(())
    }
}

// Implement LogStorageWithDelete for S3Storage
#[async_trait]
impl proven_storage::LogStorageWithDelete for S3Storage {
    async fn delete_entry(
        &self,
        namespace: &StorageNamespace,
        index: LogIndex,
    ) -> StorageResult<bool> {
        let timer = start_timer("delete_entry", namespace.as_str());

        // Check if the entry exists in pending writes first
        let in_pending = {
            let mut pending = self.pending_writes.write().await;
            if let Some(namespace_pending) = pending.get_mut(namespace) {
                namespace_pending.remove(&index).is_some()
            } else {
                false
            }
        };

        if in_pending {
            // Entry was in pending writes and removed
            // Invalidate cache
            self.cache.invalidate(namespace, index).await;

            record_operation("delete_entry", namespace.as_str(), true);
            timer.record();
            return Ok(true);
        }

        // Build the S3 key
        let key = self.make_key(namespace, index);

        // First check if the object exists with a HEAD request (more efficient than GET)
        let head_result = self
            .client
            .head_object()
            .bucket(&self.config.s3.bucket)
            .key(&key)
            .send()
            .await;

        let exists = match head_result {
            Ok(_) => true,
            Err(e) => {
                let service_error = e.into_service_error();
                if service_error.is_not_found() {
                    false
                } else {
                    return Err(StorageError::Backend(format!(
                        "S3 HEAD request failed: {service_error}"
                    )));
                }
            }
        };

        if !exists {
            record_operation("delete_entry", namespace.as_str(), true);
            timer.record();
            return Ok(false);
        }

        // Delete the object from S3
        self.client
            .delete_object()
            .bucket(&self.config.s3.bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| StorageError::Backend(format!("S3 delete failed: {e}")))?;

        // Invalidate cache entry
        self.cache.invalidate(namespace, index).await;

        // Record WAL delete operation if available
        if let Some(wal) = &self.wal_client {
            // Generate a unique operation ID for the delete
            let operation_id = Uuid::new_v4().to_string();

            // Log the delete operation to WAL for recovery purposes
            // We use a special marker to indicate deletion
            let delete_marker = Bytes::from(format!("__DELETED__{}", index.get()));
            if let Err(e) = wal
                .append_logs(
                    namespace,
                    &[(index.get(), delete_marker)],
                    operation_id.clone(),
                )
                .await
            {
                warn!("Failed to log delete operation to WAL: {}", e);
            } else {
                // Confirm the operation
                if let Err(e) = wal.confirm_batch(operation_id).await {
                    warn!("Failed to confirm delete operation with WAL: {}", e);
                }
            }
        }

        record_operation("delete_entry", namespace.as_str(), true);
        timer.record();
        Ok(true)
    }
}

// Implement LogStorageStreaming for S3Storage
#[async_trait]
impl proven_storage::LogStorageStreaming for S3Storage {
    async fn stream_range(
        &self,
        namespace: &StorageNamespace,
        start: LogIndex,
        end: Option<LogIndex>,
    ) -> StorageResult<Box<dyn Stream<Item = StorageResult<(LogIndex, Bytes)>> + Send + Unpin>>
    {
        // Clone what we need for the stream
        let client = self.client.clone();
        let bucket = self.config.s3.bucket.clone();
        let cache = self.cache.clone();
        let encryption = self.encryption.clone();
        let pending_writes = self.pending_writes.clone();
        let prefix = self.config.s3.prefix.clone();
        let namespace_str = namespace.as_str().to_string();
        let namespace_notifiers = self.namespace_notifiers.clone();
        let append_cache = self.append_cache.clone();
        let append_cache_enabled = self.config.append_cache.enabled;

        // Get or create a notifier for this specific namespace
        let notifier = namespace_notifiers
            .entry(namespace.clone())
            .or_insert_with(|| {
                let (tx, _) = broadcast::channel(16); // Smaller buffer since it's per-namespace
                tx
            })
            .clone();

        let mut notifier_rx = notifier.subscribe();

        // Create a batched streaming implementation
        let stream = async_stream::stream! {
            let mut index = start.get();
            let batch_size = 100; // Read ahead in batches
            let namespace_obj = StorageNamespace::new(&namespace_str);

            loop {
                // Check if we've reached the end bound
                if let Some(end_idx) = end
                    && index >= end_idx.get() {
                        break;
                    }

                // Determine batch range
                let batch_end = if let Some(end_idx) = end {
                    std::cmp::min(index + batch_size, end_idx.get())
                } else {
                    index + batch_size
                };

                // Collect indices that need fetching from S3
                let mut indices_to_fetch = Vec::new();
                let mut cached_entries = Vec::new();
                let mut found_any = false;

                // Check cache and pending writes for the batch
                for idx in index..batch_end {
                    // Convert to NonZero
                    let idx_nz = match LogIndex::new(idx) {
                        Some(nz) => nz,
                        None => continue,
                    };

                    // Check append cache first (if enabled)
                    if append_cache_enabled
                        && let Some(namespace_cache) = append_cache.get(&namespace_obj) {
                            let mut cache_guard = namespace_cache.lock().unwrap();
                            if let Some(data) = cache_guard.get(&idx_nz) {
                                // Cache hit - data is automatically marked as recently used
                                cached_entries.push((idx_nz, data.clone()));
                                continue;
                            }
                        }

                    // Check pending writes for read-after-write consistency
                    let from_pending = {
                        let pending = pending_writes.read().await;
                        if let Some(namespace_pending) = pending.get(&namespace_obj) {
                            namespace_pending.get(&idx_nz).cloned()
                        } else {
                            None
                        }
                    };

                    if let Some(data) = from_pending {
                        cached_entries.push((idx_nz, data));
                        continue;
                    }

                    // Check regular cache
                    if let Some(data) = cache.get(&namespace_obj, idx_nz).await {
                        cached_entries.push((idx_nz, data));
                        continue;
                    }

                    // Check bloom filter
                    if cache.might_exist(&namespace_obj, idx_nz).await {
                        indices_to_fetch.push(idx_nz);
                    }
                }

                // Yield cached entries first
                for (idx, data) in cached_entries {
                    found_any = true;
                    yield Ok((idx, data));
                }

                // Batch fetch from S3 if needed
                if !indices_to_fetch.is_empty() {
                    // Helper to make keys
                    let make_key = |idx: LogIndex| -> String {
                        let base_key = format!("{namespace_str}/{:020}", idx.get());
                        match &prefix {
                            Some(p) => format!("{p}/{base_key}"),
                            None => base_key,
                        }
                    };

                    // Create futures for parallel fetching
                    let futures: Vec<_> = indices_to_fetch
                        .iter()
                        .map(|&idx| {
                            let client = client.clone();
                            let bucket = bucket.clone();
                            let key = make_key(idx);
                            let encryption = encryption.clone();

                            async move {
                                match client
                                    .get_object()
                                    .bucket(&bucket)
                                    .key(&key)
                                    .send()
                                    .await
                                {
                                    Ok(response) => {
                                        match response.body.collect().await {
                                            Ok(data) => {
                                                let data_bytes = data.into_bytes();

                                                // Decrypt if needed
                                                let decrypted = if let Some(ref enc) = encryption {
                                                    match enc.decrypt_from_s3(&data_bytes).await {
                                                        Ok(dec) => dec,
                                                        Err(e) => {
                                                            tracing::warn!("Failed to decrypt index {}: {}", idx, e);
                                                            return None;
                                                        }
                                                    }
                                                } else {
                                                    Bytes::from(data_bytes.to_vec())
                                                };

                                                Some((idx, decrypted))
                                            }
                                            Err(e) => {
                                                tracing::debug!("Failed to read body for index {}: {}", idx, e);
                                                None
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        if let Some(service_error) = e.as_service_error() {
                                            match service_error {
                                                aws_sdk_s3::operation::get_object::GetObjectError::NoSuchKey(_) => {
                                                    // Object not found, this is ok
                                                }
                                                _ => {
                                                    tracing::warn!("S3 get failed for index {}: {}", idx, e);
                                                }
                                            }
                                        } else {
                                            tracing::warn!("S3 get failed for index {}: {}", idx, e);
                                        }
                                        None
                                    }
                                }
                            }
                        })
                        .collect();

                    // Execute batch with concurrency limit
                    use futures::stream::StreamExt;
                    let concurrent_limit = 10;

                    let mut fetch_stream = futures::stream::iter(futures)
                        .buffer_unordered(concurrent_limit);

                    let mut results = Vec::new();
                    while let Some(result) = fetch_stream.next().await {
                        if let Some((idx, data)) = result {
                            results.push((idx, data));
                        }
                    }

                    // Sort results by index and yield
                    results.sort_by_key(|(idx, _)| *idx);
                    for (idx, data) in results {
                        // Update cache
                        cache.put(&namespace_obj, idx, data.clone()).await;
                        found_any = true;
                        yield Ok((idx, data));
                    }
                }

                // Move to next batch
                index = batch_end;

                // If we have an explicit end bound and have reached or passed it, we're done
                if let Some(end_idx) = end && index >= end_idx.get() {
                    break;
                }

                // In follow mode (no end bound), wait for new entries if we didn't find any
                if !found_any && end.is_none() {
                    // Wait for notification of new entries
                    match notifier_rx.recv().await {
                        Ok(()) => {
                            // New entries in our namespace, continue reading
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            // We missed some notifications, but that's ok, just try reading again
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            // Channel closed, storage is shutting down
                            return;
                        }
                    }
                }
            }
        };

        Ok(Box::new(Box::pin(stream)))
    }
}

impl S3Storage {
    /// Batch read multiple entries from S3
    /// Returns a map of index -> data for successfully read entries
    async fn batch_read_from_s3(
        &self,
        namespace: &StorageNamespace,
        indices: &[LogIndex],
    ) -> HashMap<LogIndex, Bytes> {
        let mut results = HashMap::new();

        // For now, we still read individually but in parallel
        // Future optimization: store multiple entries per S3 object
        let futures: Vec<_> = indices
            .iter()
            .map(|&index| {
                let client = self.client.clone();
                let bucket = self.config.s3.bucket.clone();
                let key = self.make_key(namespace, index);
                let encryption = self.encryption.clone();

                async move {
                    match client
                        .get_object()
                        .bucket(&bucket)
                        .key(&key)
                        .send()
                        .await
                    {
                        Ok(response) => {
                            match response.body.collect().await {
                                Ok(data) => {
                                    let data_bytes = data.into_bytes();

                                    // Decrypt if needed
                                    let decrypted = if let Some(ref enc) = encryption {
                                        match enc.decrypt_from_s3(&data_bytes).await {
                                            Ok(dec) => dec,
                                            Err(e) => {
                                                tracing::warn!("Failed to decrypt index {}: {}", index, e);
                                                return None;
                                            }
                                        }
                                    } else {
                                        Bytes::from(data_bytes.to_vec())
                                    };

                                    Some((index, decrypted))
                                }
                                Err(e) => {
                                    tracing::debug!("Failed to read body for index {}: {}", index, e);
                                    None
                                }
                            }
                        }
                        Err(e) => {
                            if let Some(service_error) = e.as_service_error() {
                                match service_error {
                                    aws_sdk_s3::operation::get_object::GetObjectError::NoSuchKey(_) => {
                                        // Object not found, this is ok
                                    }
                                    _ => {
                                        tracing::warn!("S3 get failed for index {}: {}", index, e);
                                    }
                                }
                            } else {
                                tracing::warn!("S3 get failed for index {}: {}", index, e);
                            }
                            None
                        }
                    }
                }
            })
            .collect();

        // Execute all reads in parallel with a concurrency limit
        use futures::stream::StreamExt;
        let concurrent_limit = 10; // Adjust based on S3 rate limits

        let mut stream = futures::stream::iter(futures).buffer_unordered(concurrent_limit);

        while let Some(result) = stream.next().await {
            if let Some((index, data)) = result {
                results.insert(index, data);
            }
        }

        results
    }

    /// Create a simple S3 storage instance without advanced features
    pub async fn simple(
        client: S3Client,
        bucket: String,
        prefix: Option<String>,
    ) -> StorageResult<Self> {
        let config = config::S3StorageConfigBuilder::new(bucket)
            .prefix(prefix.unwrap_or_default())
            .build();

        Self::new(client, config).await
    }

    /// Create an S3 storage instance with batching enabled
    pub async fn with_batching(
        client: S3Client,
        bucket: String,
        prefix: Option<String>,
        max_size_mb: usize,
        max_time_ms: u64,
    ) -> StorageResult<Self> {
        let config = config::S3StorageConfigBuilder::new(bucket)
            .prefix(prefix.unwrap_or_default())
            .batching(max_size_mb, max_time_ms, 1000)
            .build();

        Self::new(client, config).await
    }

    /// Create a production-ready S3 storage instance with all features
    pub async fn production(
        client: S3Client,
        bucket: String,
        prefix: Option<String>,
        vsock_port: u32,
    ) -> StorageResult<Self> {
        // Generate encryption keys
        let s3_key = encryption::generate_key();
        let wal_key = encryption::generate_key();

        let config = config::S3StorageConfigBuilder::new(bucket)
            .prefix(prefix.unwrap_or_default())
            .batching(5, 100, 1000)
            .caching(50, 300)
            .encryption(s3_key, wal_key)
            .wal(vsock_port, 100)
            .build();

        Self::new(client, config).await
    }
}

impl std::fmt::Debug for S3Storage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Storage")
            .field("bucket", &self.config.s3.bucket)
            .field("prefix", &self.config.s3.prefix)
            .field("encryption_enabled", &self.config.encryption.is_some())
            .field("vsock_port", &self.config.wal.vsock_port)
            .field("cache_max_size_bytes", &self.config.cache.max_size_bytes)
            .finish()
    }
}

// Implement StorageAdaptor for S3Storage
impl proven_storage::StorageAdaptor for S3Storage {
    // Use default implementations
    // shutdown() uses default implementation since S3 doesn't need cleanup
    // delete_all() could be implemented to delete all objects in the bucket
    // stats() could be implemented to return S3 usage statistics
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_config::BehaviorVersion;

    #[tokio::test]
    #[ignore] // Requires AWS credentials and S3 bucket
    async fn test_s3_storage_simple() {
        let config = aws_config::defaults(BehaviorVersion::latest()).load().await;
        let client = S3Client::new(&config);

        let storage = S3Storage::simple(
            client,
            "test-bucket".to_string(),
            Some("test-prefix".to_string()),
        )
        .await
        .unwrap();

        let namespace = StorageNamespace::new("test");

        // Test append and read
        let entries = vec![
            (LogIndex::new(1).unwrap(), Arc::new(Bytes::from("data 1"))),
            (LogIndex::new(2).unwrap(), Arc::new(Bytes::from("data 2"))),
            (LogIndex::new(3).unwrap(), Arc::new(Bytes::from("data 3"))),
        ];
        storage.put_at(&namespace, entries).await.unwrap();

        let range = storage
            .read_range(
                &namespace,
                LogIndex::new(1).unwrap(),
                LogIndex::new(4).unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(range.len(), 3);
        assert_eq!(range[0], (LogIndex::new(1).unwrap(), Bytes::from("data 1")));
        assert_eq!(range[1], (LogIndex::new(2).unwrap(), Bytes::from("data 2")));
        assert_eq!(range[2], (LogIndex::new(3).unwrap(), Bytes::from("data 3")));

        // Test bounds
        let bounds = storage.bounds(&namespace).await.unwrap();
        assert_eq!(
            bounds,
            Some((LogIndex::new(1).unwrap(), LogIndex::new(3).unwrap()))
        );
    }

    #[tokio::test]
    #[ignore] // Requires AWS credentials and S3 bucket
    async fn test_s3_storage_with_batching() {
        let config = aws_config::defaults(BehaviorVersion::latest()).load().await;
        let client = S3Client::new(&config);

        let storage = S3Storage::with_batching(
            client,
            "test-bucket".to_string(),
            Some("test-prefix".to_string()),
            5,   // 5MB batches
            100, // 100ms timeout
        )
        .await
        .unwrap();

        let namespace = StorageNamespace::new("test");

        // Test multiple appends that will be batched
        for i in 1..=10 {
            let entries = Arc::new(vec![Bytes::from(format!("data {i}"))]);
            storage.append(&namespace, entries).await.unwrap();
        }

        // Wait for batch to flush
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // Verify all entries were written
        let range = storage
            .read_range(
                &namespace,
                LogIndex::new(1).unwrap(),
                LogIndex::new(11).unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(range.len(), 10);
    }
}
