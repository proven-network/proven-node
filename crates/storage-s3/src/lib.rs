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
use proven_storage::{LogStorage, StorageError, StorageNamespace, StorageResult};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{RwLock, mpsc};
use tracing::{error, info, warn};
use uuid::Uuid;

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
    log_bounds: Arc<RwLock<HashMap<StorageNamespace, (u64, u64)>>>,
    /// Batch manager for write optimization
    batch_manager: Arc<BatchManager>,
    /// Read cache
    cache: Arc<LogCache>,
    /// WAL client for durability
    wal_client: Option<Arc<WalClient>>,
    /// Encryption handler
    encryption: Option<Arc<EncryptionHandler>>,
    /// Pending writes for read-after-write consistency
    pending_writes: Arc<RwLock<HashMap<StorageNamespace, HashMap<u64, Bytes>>>>,
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
                            // Re-submit recovered entries
                            if let Err(e) = storage.append(&namespace, entries).await {
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
    fn make_key(&self, namespace: &StorageNamespace, index: u64) -> String {
        let base = format!("{}/{:020}", namespace.as_str(), index);
        match &self.config.s3.prefix {
            Some(prefix) => format!("{prefix}/{base}"),
            None => base,
        }
    }

    /// Parse an index from an S3 key
    fn parse_index(&self, key: &str) -> Option<u64> {
        let parts: Vec<&str> = key.split('/').collect();
        if let Some(index_str) = parts.last() {
            index_str.parse().ok()
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
                        Ok(enc_data) => encrypted.push((*idx, Bytes::from(enc_data))),
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
                batch.entries.clone()
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
        let (mut first_index, mut last_index) = bounds
            .get(&batch.namespace)
            .copied()
            .unwrap_or((u64::MAX, 0));

        for (index, _) in &batch.entries {
            if first_index == u64::MAX || *index < first_index {
                first_index = *index;
            }
            if *index > last_index {
                last_index = *index;
            }
        }

        bounds.insert(batch.namespace.clone(), (first_index, last_index));
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
}

#[async_trait]
impl LogStorage for S3Storage {
    async fn append(
        &self,
        namespace: &StorageNamespace,
        entries: Vec<(u64, Bytes)>,
    ) -> StorageResult<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let timer = start_timer("append", namespace.as_str());

        // Use batch manager for optimized writes
        self.batch_manager
            .add_entries(namespace.clone(), entries)
            .await
            .map_err(StorageError::Backend)?;

        record_operation("append", namespace.as_str(), true);
        timer.record();

        Ok(())
    }

    async fn read_range(
        &self,
        namespace: &StorageNamespace,
        start: u64,
        end: u64,
    ) -> StorageResult<Vec<(u64, Bytes)>> {
        let timer = start_timer("read_range", namespace.as_str());
        let mut entries = Vec::new();
        let mut cache_hits = 0;
        let mut cache_misses = 0;

        // Read each entry in the range
        for index in start..end {
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
            if !self.cache.might_exist(namespace, index).await {
                continue; // Skip if bloom filter says it doesn't exist
            }

            // Read from S3
            let key = self.make_key(namespace, index);

            match self
                .client
                .get_object()
                .bucket(&self.config.s3.bucket)
                .key(&key)
                .send()
                .await
            {
                Ok(response) => {
                    let data =
                        response.body.collect().await.map_err(|e| {
                            StorageError::Backend(format!("S3 read body failed: {e}"))
                        })?;

                    let data_bytes = data.into_bytes();
                    record_s3_download(namespace.as_str(), data_bytes.len(), true);

                    // Decrypt if needed
                    let decrypted = if let Some(encryption) = &self.encryption {
                        match encryption.decrypt_from_s3(&data_bytes).await {
                            Ok(dec) => {
                                record_encryption("decrypt", "s3");
                                dec
                            }
                            Err(e) => {
                                error!("Failed to decrypt data from S3: {}", e);
                                return Err(e);
                            }
                        }
                    } else {
                        data_bytes
                    };

                    // Update cache
                    self.cache.put(namespace, index, decrypted.clone()).await;

                    entries.push((index, decrypted));
                }
                Err(e) => {
                    // If key not found, skip it (sparse logs are allowed)
                    let service_error = e.into_service_error();
                    if !service_error.is_no_such_key() {
                        record_s3_download(namespace.as_str(), 0, false);
                        return Err(StorageError::Backend(format!(
                            "S3 get failed: {service_error}"
                        )));
                    }
                }
            }
        }

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

    async fn truncate_after(&self, namespace: &StorageNamespace, index: u64) -> StorageResult<()> {
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

    async fn compact_before(&self, namespace: &StorageNamespace, index: u64) -> StorageResult<()> {
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
                *first = index + 1;
            }
            if *first > *last {
                bounds.remove(namespace);
            }
        }

        Ok(())
    }

    async fn bounds(&self, namespace: &StorageNamespace) -> StorageResult<Option<(u64, u64)>> {
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
        let mut min_index = u64::MAX;
        let mut max_index = 0u64;
        let mut found_any = false;

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
                        found_any = true;
                        min_index = min_index.min(index);
                        max_index = max_index.max(index);
                    }
                }
            }

            if response.is_truncated.unwrap_or(false) {
                continuation_token = response.next_continuation_token;
            } else {
                break;
            }
        }

        if found_any {
            // Update cache
            let mut bounds = self.log_bounds.write().await;
            bounds.insert(namespace.clone(), (min_index, max_index));
            Ok(Some((min_index, max_index)))
        } else {
            Ok(None)
        }
    }
}

impl S3Storage {
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
            (1, Bytes::from("data 1")),
            (2, Bytes::from("data 2")),
            (3, Bytes::from("data 3")),
        ];
        storage.append(&namespace, entries).await.unwrap();

        let range = storage.read_range(&namespace, 1, 4).await.unwrap();
        assert_eq!(range.len(), 3);
        assert_eq!(range[0], (1, Bytes::from("data 1")));
        assert_eq!(range[1], (2, Bytes::from("data 2")));
        assert_eq!(range[2], (3, Bytes::from("data 3")));

        // Test bounds
        let bounds = storage.bounds(&namespace).await.unwrap();
        assert_eq!(bounds, Some((1, 3)));
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
        for i in 0..10 {
            let entries = vec![(i, Bytes::from(format!("data {i}")))];
            storage.append(&namespace, entries).await.unwrap();
        }

        // Wait for batch to flush
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // Verify all entries were written
        let range = storage.read_range(&namespace, 0, 10).await.unwrap();
        assert_eq!(range.len(), 10);
    }
}
