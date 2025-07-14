//! Batching system for efficient S3 uploads

use crate::storage_backends::types::{StorageError, StorageResult};
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::primitives::ByteStream;
use bytes::Bytes;
use crossbeam::queue::SegQueue;
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    sync::{Mutex, RwLock},
    time::interval,
};
use tracing::{debug, error};

use super::{
    config::BatchConfig,
    encryption::EncryptionManager,
    keys::S3KeyMapper,
    metrics::StorageMetrics,
    wal_client::{PendingBatch, WalClient, WalEntryId},
};

/// Entry in a batch
#[derive(Clone, Debug)]
pub struct BatchEntry {
    pub s3_key: String,
    pub data: Bytes,
    pub wal_id: WalEntryId,
}

/// A batch ready for upload
#[derive(Debug)]
struct ReadyBatch {
    entries: Vec<BatchEntry>,
    deletions: Vec<String>,
    size_bytes: usize,
    #[allow(dead_code)]
    created_at: Instant,
    wal_ids: Vec<WalEntryId>,
}

/// Current batch being assembled
#[derive(Debug)]
struct CurrentBatch {
    entries: Vec<BatchEntry>,
    deletions: Vec<String>,
    size_bytes: usize,
    created_at: Instant,
}

impl CurrentBatch {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
            deletions: Vec::new(),
            size_bytes: 0,
            created_at: Instant::now(),
        }
    }

    fn add_entry(&mut self, entry: BatchEntry) {
        self.size_bytes += entry.data.len();
        self.entries.push(entry);
    }

    fn add_deletion(&mut self, key: String) {
        self.deletions.push(key);
    }

    fn is_ready(&self, config: &BatchConfig) -> bool {
        self.size_bytes >= config.max_size_bytes
            || self.entries.len() >= config.max_entries
            || self.created_at.elapsed() >= Duration::from_millis(config.max_time_ms)
    }

    fn into_ready(self) -> ReadyBatch {
        let wal_ids = self.entries.iter().map(|e| e.wal_id.clone()).collect();
        ReadyBatch {
            entries: self.entries,
            deletions: self.deletions,
            size_bytes: self.size_bytes,
            created_at: self.created_at,
            wal_ids,
        }
    }
}

/// Batch manager for efficient S3 uploads
pub struct BatchManager {
    config: BatchConfig,
    s3_client: S3Client,
    bucket: String,
    current_batch: Arc<Mutex<CurrentBatch>>,
    upload_queue: Arc<SegQueue<ReadyBatch>>,
    pending_reads: Arc<RwLock<HashMap<String, Bytes>>>,
    wal_client: Arc<WalClient>,
    encryptor: Arc<EncryptionManager>,
    key_mapper: Arc<S3KeyMapper>,
    metrics: Arc<StorageMetrics>,
    shutdown: Arc<Mutex<bool>>,
}

impl BatchManager {
    /// Create a new batch manager
    pub fn new(
        config: BatchConfig,
        s3_client: S3Client,
        bucket: String,
        wal_client: Arc<WalClient>,
        encryptor: Arc<EncryptionManager>,
        key_mapper: Arc<S3KeyMapper>,
        metrics: Arc<StorageMetrics>,
    ) -> Self {
        Self {
            config,
            s3_client,
            bucket,
            current_batch: Arc::new(Mutex::new(CurrentBatch::new())),
            upload_queue: Arc::new(SegQueue::new()),
            pending_reads: Arc::new(RwLock::new(HashMap::new())),
            wal_client,
            encryptor,
            key_mapper,
            metrics,
            shutdown: Arc::new(Mutex::new(false)),
        }
    }

    /// Start the batch manager workers
    pub async fn start(&self) {
        // Start the batch timer
        let manager = self.clone();
        tokio::spawn(async move {
            manager.batch_timer_loop().await;
        });

        // Start upload workers
        for i in 0..self.config.upload_workers {
            let manager = self.clone();
            tokio::spawn(async move {
                manager.upload_worker_loop(i).await;
            });
        }
    }

    /// Add an entry to the current batch
    pub async fn add_entry(&self, entry: BatchEntry) -> StorageResult<()> {
        // Add to pending reads for immediate consistency
        {
            let mut pending = self.pending_reads.write().await;
            pending.insert(entry.s3_key.clone(), entry.data.clone());
        }

        let mut batch = self.current_batch.lock().await;
        batch.add_entry(entry);

        if batch.is_ready(&self.config) {
            let ready = std::mem::replace(&mut *batch, CurrentBatch::new()).into_ready();
            drop(batch);

            self.upload_queue.push(ready);
            self.metrics.batches_queued.inc();
        }

        Ok(())
    }

    /// Add a deletion to the current batch
    pub async fn add_deletion(&self, key: String) -> StorageResult<()> {
        // Remove from pending reads
        {
            let mut pending = self.pending_reads.write().await;
            pending.remove(&key);
        }

        let mut batch = self.current_batch.lock().await;
        batch.add_deletion(key);

        Ok(())
    }

    /// Get a value from pending batches
    pub async fn get_pending(&self, key: &str) -> StorageResult<Option<Bytes>> {
        let pending = self.pending_reads.read().await;
        Ok(pending.get(key).cloned())
    }

    /// Force flush the current batch
    pub async fn force_flush(&self) -> StorageResult<()> {
        let mut batch = self.current_batch.lock().await;

        if !batch.entries.is_empty() || !batch.deletions.is_empty() {
            let ready = std::mem::replace(&mut *batch, CurrentBatch::new()).into_ready();
            drop(batch);

            self.upload_queue.push(ready);
            self.metrics.batches_queued.inc();
        }

        // Wait for queue to drain
        let start = Instant::now();
        while !self.upload_queue.is_empty() {
            if start.elapsed() > Duration::from_secs(30) {
                return Err(StorageError::Backend("Batch flush timeout".to_string()));
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        Ok(())
    }

    /// Recover a batch from WAL
    pub async fn recover_batch(&self, batch: PendingBatch) -> StorageResult<()> {
        let mut entries = Vec::new();

        for wal_entry in batch.entries {
            // Decrypt the data
            let decrypted = self.encryptor.decrypt_from_wal(&wal_entry.data).await?;

            // Parse the original entry
            // In production, you'd store the S3 key with the WAL entry
            let s3_key = format!("recovered/{}", wal_entry.id.id);

            entries.push(BatchEntry {
                s3_key,
                data: decrypted,
                wal_id: wal_entry.id,
            });
        }

        let ready = ReadyBatch {
            wal_ids: entries.iter().map(|e| e.wal_id.clone()).collect(),
            size_bytes: entries.iter().map(|e| e.data.len()).sum(),
            entries,
            deletions: Vec::new(),
            created_at: Instant::now(),
        };

        self.upload_queue.push(ready);
        self.metrics.batches_recovered.inc();

        Ok(())
    }

    /// Batch timer loop
    async fn batch_timer_loop(&self) {
        let mut ticker = interval(Duration::from_millis(self.config.max_time_ms / 10));

        loop {
            ticker.tick().await;

            if *self.shutdown.lock().await {
                break;
            }

            let mut batch = self.current_batch.lock().await;
            if batch.created_at.elapsed() >= Duration::from_millis(self.config.max_time_ms)
                && !batch.entries.is_empty()
            {
                let ready = std::mem::replace(&mut *batch, CurrentBatch::new()).into_ready();
                drop(batch);

                self.upload_queue.push(ready);
                self.metrics.batches_queued.inc();
            }
        }
    }

    /// Upload worker loop
    async fn upload_worker_loop(&self, worker_id: usize) {
        debug!("Starting upload worker {}", worker_id);

        loop {
            if *self.shutdown.lock().await {
                break;
            }

            if let Some(batch) = self.upload_queue.pop() {
                let batch_size = batch.size_bytes;
                let entry_count = batch.entries.len();
                let deletion_count = batch.deletions.len();

                match self.upload_batch(batch).await {
                    Ok(_) => {
                        self.metrics.batches_uploaded.inc();
                        self.metrics.batch_upload_size.observe(batch_size as f64);
                        debug!(
                            "Worker {} uploaded batch: {} entries, {} deletions, {} bytes",
                            worker_id, entry_count, deletion_count, batch_size
                        );
                    }
                    Err(e) => {
                        self.metrics.batch_upload_errors.inc();
                        error!("Worker {} batch upload failed: {}", worker_id, e);
                    }
                }
            } else {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }

    /// Upload a batch to S3
    async fn upload_batch(&self, batch: ReadyBatch) -> StorageResult<()> {
        let _timer = self.metrics.batch_upload_duration.start_timer();

        // Process deletions first
        for key in &batch.deletions {
            self.s3_client
                .delete_object()
                .bucket(&self.bucket)
                .key(key)
                .send()
                .await
                .map_err(|e| {
                    StorageError::Io(std::io::Error::other(format!("S3 delete failed: {e}")))
                })?;
        }

        // Upload entries
        for entry in &batch.entries {
            // Encrypt for S3
            let encrypted = self.encryptor.encrypt_for_s3(&entry.data).await?;

            // Upload to S3
            self.s3_client
                .put_object()
                .bucket(&self.bucket)
                .key(&entry.s3_key)
                .body(ByteStream::from(encrypted))
                .storage_class(aws_sdk_s3::types::StorageClass::OnezoneIa)
                .send()
                .await
                .map_err(|e| {
                    StorageError::Io(std::io::Error::other(format!("S3 put failed: {e}")))
                })?;
        }

        // Mark WAL entries as uploaded
        if !batch.wal_ids.is_empty() {
            self.wal_client.mark_uploaded(&batch.wal_ids).await?;
        }

        // Remove from pending reads
        {
            let mut pending = self.pending_reads.write().await;
            for entry in &batch.entries {
                pending.remove(&entry.s3_key);
            }
        }

        Ok(())
    }

    /// Shutdown the batch manager
    pub async fn shutdown(&self) -> StorageResult<()> {
        *self.shutdown.lock().await = true;
        self.force_flush().await?;
        Ok(())
    }

    /// Check if the batch manager is running
    #[allow(dead_code)]
    pub async fn is_running(&self) -> bool {
        !*self.shutdown.lock().await
    }

    /// Get the last WAL ID from current batch
    pub async fn get_last_wal_id(&self) -> Option<WalEntryId> {
        let batch = self.current_batch.lock().await;
        batch.entries.last().map(|e| e.wal_id.clone())
    }

    /// Get the last confirmed WAL ID (from uploaded batches)
    pub async fn get_last_confirmed_wal_id(&self) -> Option<WalEntryId> {
        // In production, you'd track this properly
        // For now, return None
        None
    }

    /// Force flush a specific key
    pub async fn force_flush_key(&self, s3_key: &str) -> StorageResult<()> {
        let mut batch = self.current_batch.lock().await;

        // Check if key is in current batch
        let has_key = batch.entries.iter().any(|e| e.s3_key == s3_key);

        if has_key {
            let ready = std::mem::replace(&mut *batch, CurrentBatch::new()).into_ready();
            drop(batch);

            self.upload_queue.push(ready);
            self.metrics.batches_queued.inc();

            // Wait for this specific key to be uploaded
            let start = Instant::now();
            loop {
                let pending = self.pending_reads.read().await;
                if !pending.contains_key(s3_key) {
                    break;
                }
                drop(pending);

                if start.elapsed() > Duration::from_secs(5) {
                    return Err(StorageError::Backend("Key flush timeout".to_string()));
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }

        Ok(())
    }
}

// Clone implementation for Arc-based sharing
impl Clone for BatchManager {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            s3_client: self.s3_client.clone(),
            bucket: self.bucket.clone(),
            current_batch: self.current_batch.clone(),
            upload_queue: self.upload_queue.clone(),
            pending_reads: self.pending_reads.clone(),
            wal_client: self.wal_client.clone(),
            encryptor: self.encryptor.clone(),
            key_mapper: self.key_mapper.clone(),
            metrics: self.metrics.clone(),
            shutdown: self.shutdown.clone(),
        }
    }
}
