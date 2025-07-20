//! Batching system for S3 storage to optimize write performance

use bytes::Bytes;
use proven_storage::StorageNamespace;
use std::{
    collections::HashMap,
    num::NonZero,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, mpsc, oneshot};
use tracing::{debug, warn};

use crate::config::BatchConfig;

/// A batch of log entries ready to be uploaded
#[derive(Debug)]
pub struct LogBatch {
    /// Namespace for this batch
    pub namespace: StorageNamespace,
    /// Entries in the batch
    pub entries: Vec<(NonZero<u64>, Bytes)>,
    /// Total size of entries in bytes
    pub total_size: usize,
    /// Creation time of the batch
    pub created_at: Instant,
    /// Completion channels for waiting appends
    pub completions: Vec<oneshot::Sender<Result<(), String>>>,
}

/// Request to add entries to a batch
pub struct BatchRequest {
    pub namespace: StorageNamespace,
    pub entries: Vec<(NonZero<u64>, Bytes)>,
    pub completion: oneshot::Sender<Result<(), String>>,
}

/// Manages batching of log entries for efficient S3 uploads
pub struct BatchManager {
    /// Batching configuration
    _config: BatchConfig,
    /// Request sender
    tx: mpsc::Sender<BatchRequest>,
    /// Background task handle
    _handle: tokio::task::JoinHandle<()>,
}

impl BatchManager {
    /// Create a new batch manager
    pub fn new(config: BatchConfig, upload_tx: mpsc::Sender<LogBatch>) -> Self {
        let (tx, rx) = mpsc::channel(1000);

        let handle = tokio::spawn(batch_worker(config.clone(), rx, upload_tx));

        Self {
            _config: config,
            tx,
            _handle: handle,
        }
    }

    /// Add entries to be batched
    pub async fn add_entries(
        &self,
        namespace: StorageNamespace,
        entries: Vec<(NonZero<u64>, Bytes)>,
    ) -> Result<(), String> {
        let (completion_tx, completion_rx) = oneshot::channel();

        self.tx
            .send(BatchRequest {
                namespace,
                entries,
                completion: completion_tx,
            })
            .await
            .map_err(|_| "Batch manager shut down".to_string())?;

        completion_rx
            .await
            .map_err(|_| "Batch completion channel closed".to_string())?
    }
}

/// Worker that manages batching
async fn batch_worker(
    config: BatchConfig,
    mut rx: mpsc::Receiver<BatchRequest>,
    upload_tx: mpsc::Sender<LogBatch>,
) {
    let mut active_batches: HashMap<StorageNamespace, LogBatch> = HashMap::new();
    let mut flush_interval = tokio::time::interval(Duration::from_millis(config.max_time_ms));
    flush_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            // Handle new batch requests
            Some(request) = rx.recv() => {
                let namespace = request.namespace.clone();
                let mut should_flush = false;

                // Calculate size of new entries
                let new_size: usize = request.entries.iter()
                    .map(|(_, data)| data.len() + 8) // 8 bytes for index
                    .sum();
                let new_entries_len = request.entries.len();

                // Check if we need to flush existing batch
                if let Some(batch) = active_batches.get(&namespace)
                    && !batch.entries.is_empty() &&
                       (batch.total_size + new_size > config.max_size_bytes ||
                        batch.entries.len() + new_entries_len > config.max_entries) {
                        should_flush = true;
                    }

                // Flush if needed
                if should_flush
                    && let Some(batch) = active_batches.remove(&namespace) {
                        flush_batch(batch, &upload_tx).await;
                    }

                // Now add the request to a batch (either existing or new)
                let batch = active_batches.entry(namespace.clone()).or_insert_with(|| {
                    LogBatch {
                        namespace: namespace.clone(),
                        entries: Vec::new(),
                        total_size: 0,
                        created_at: Instant::now(),
                        completions: Vec::new(),
                    }
                });

                // Add entries to batch
                batch.entries.extend(request.entries);
                batch.total_size += new_size;
                batch.completions.push(request.completion);

                // Check if batch is now full and needs immediate flush
                if (batch.total_size >= config.max_size_bytes ||
                   batch.entries.len() >= config.max_entries)
                    && let Some(batch) = active_batches.remove(&namespace) {
                        flush_batch(batch, &upload_tx).await;
                    }
            }

            // Periodic flush based on time
            _ = flush_interval.tick() => {
                let now = Instant::now();
                let mut to_flush = Vec::new();

                // Find batches that have exceeded time limit
                for (namespace, batch) in &active_batches {
                    if now.duration_since(batch.created_at).as_millis() >= config.max_time_ms as u128 {
                        to_flush.push(namespace.clone());
                    }
                }

                // Flush old batches
                for namespace in to_flush {
                    if let Some(batch) = active_batches.remove(&namespace) {
                        flush_batch(batch, &upload_tx).await;
                    }
                }
            }

            // Shutdown
            else => {
                debug!("Batch worker shutting down, flushing remaining batches");

                // Flush all remaining batches
                for (_, batch) in active_batches.drain() {
                    flush_batch(batch, &upload_tx).await;
                }

                break;
            }
        }
    }
}

/// Flush a batch to the upload queue
async fn flush_batch(batch: LogBatch, upload_tx: &mpsc::Sender<LogBatch>) {
    debug!(
        namespace = ?batch.namespace,
        entries = batch.entries.len(),
        size = batch.total_size,
        "Flushing batch"
    );

    if let Err(e) = upload_tx.send(batch).await {
        warn!("Failed to send batch to upload queue: {}", e);
    }
}

/// Manages parallel uploads of batches to S3
pub struct UploadManager {
    /// Number of parallel upload workers
    _workers: usize,
    /// Handles to worker tasks
    _handles: Vec<tokio::task::JoinHandle<()>>,
}

impl UploadManager {
    /// Create a new upload manager
    pub fn new<F, Fut>(workers: usize, batch_rx: mpsc::Receiver<LogBatch>, upload_fn: F) -> Self
    where
        F: Fn(LogBatch) -> Fut + Send + Sync + 'static + Clone,
        Fut: std::future::Future<Output = Result<(), String>> + Send,
    {
        let batch_rx = Arc::new(Mutex::new(batch_rx));
        let mut handles = Vec::new();

        for worker_id in 0..workers {
            let rx = batch_rx.clone();
            let upload = upload_fn.clone();

            let handle = tokio::spawn(async move {
                upload_worker(worker_id, rx, upload).await;
            });

            handles.push(handle);
        }

        Self {
            _workers: workers,
            _handles: handles,
        }
    }
}

/// Worker that uploads batches
async fn upload_worker<F, Fut>(
    id: usize,
    batch_rx: Arc<Mutex<mpsc::Receiver<LogBatch>>>,
    upload_fn: F,
) where
    F: Fn(LogBatch) -> Fut,
    Fut: std::future::Future<Output = Result<(), String>>,
{
    debug!("Upload worker {} started", id);

    loop {
        // Get next batch
        let batch = {
            let mut rx = batch_rx.lock().await;
            rx.recv().await
        };

        let Some(batch) = batch else {
            debug!("Upload worker {} shutting down", id);
            break;
        };

        debug!(
            worker = id,
            namespace = ?batch.namespace,
            entries = batch.entries.len(),
            "Worker uploading batch"
        );

        // Upload the batch
        let _result = upload_fn(batch).await;

        // Note: Completions are handled by the upload function
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_batching_by_size() {
        let (upload_tx, mut upload_rx) = mpsc::channel(10);

        let config = BatchConfig {
            max_size_bytes: 1000,
            max_time_ms: 10000, // 10 seconds (won't trigger in test)
            max_entries: 1000,
            upload_workers: 1,
        };

        let manager = BatchManager::new(config, upload_tx);
        let namespace = StorageNamespace::new("test");

        // Add entries that will exceed size limit
        let large_data = vec![0u8; 600];
        let entries1 = vec![(NonZero::new(1).unwrap(), Bytes::from(large_data.clone()))];
        let entries2 = vec![(NonZero::new(2).unwrap(), Bytes::from(large_data))];

        let result1 = manager.add_entries(namespace.clone(), entries1);
        let _result2 = manager.add_entries(namespace.clone(), entries2);

        // First batch should complete when second is added
        tokio::time::timeout(Duration::from_secs(1), result1)
            .await
            .expect("First batch should complete")
            .expect("First batch should succeed");

        // Check that batch was sent
        let batch = upload_rx.try_recv().expect("Should have batch");
        assert_eq!(batch.entries.len(), 1);
        assert_eq!(batch.namespace, namespace);
    }
}
