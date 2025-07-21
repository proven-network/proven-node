//! VSOCK logger implementation

pub use crate::config::{VsockLoggerConfig, VsockLoggerConfigBuilder};
use crate::{
    error::Result,
    messages::{LogBatch, LogEntry},
};
use proven_logger::{Context, Level, Logger, Record};
use proven_vsock_rpc::RpcClient;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use tokio::sync::mpsc;
use tokio::time::interval;
use tracing::{debug, error, warn};

/// VSOCK-based logger that batches and sends logs to a remote collector
pub struct VsockLogger {
    /// Configuration
    config: VsockLoggerConfig,
    /// Channel sender for log entries
    sender: mpsc::Sender<LogEntry>,
    /// Sequence counter for batches
    sequence: Arc<AtomicU64>,
    /// Logger context
    context: Context,
    /// Handle to the background worker
    _worker_handle: tokio::task::JoinHandle<()>,
}

impl VsockLogger {
    /// Create a new VSOCK logger with the given configuration
    pub async fn new(config: VsockLoggerConfig) -> Result<Self> {
        let (sender, receiver) = mpsc::channel(config.channel_buffer_size);
        let sequence = Arc::new(AtomicU64::new(0));

        // Create RPC client
        let rpc_client = RpcClient::builder().vsock_addr(config.vsock_addr).build()?;

        // Spawn background worker
        let worker_config = config.clone();
        let worker_sequence = sequence.clone();
        let worker_handle = tokio::spawn(async move {
            if let Err(e) =
                Self::worker_loop(receiver, rpc_client, worker_config, worker_sequence).await
            {
                error!("VSOCK logger worker failed: {}", e);
            }
        });

        Ok(Self {
            config,
            sender,
            sequence,
            context: Context::new("vsock"),
            _worker_handle: worker_handle,
        })
    }

    /// Create a new VSOCK logger with context
    pub async fn with_context(config: VsockLoggerConfig, context: Context) -> Result<Self> {
        let mut logger = Self::new(config).await?;
        logger.context = context;
        Ok(logger)
    }

    /// Background worker that batches and sends logs
    async fn worker_loop(
        mut receiver: mpsc::Receiver<LogEntry>,
        client: RpcClient,
        config: VsockLoggerConfig,
        sequence: Arc<AtomicU64>,
    ) -> Result<()> {
        let mut batch = Vec::with_capacity(config.batch_size);
        let mut flush_interval = interval(config.flush_interval);
        flush_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                // Receive log entries
                Some(entry) = receiver.recv() => {
                    batch.push(entry);

                    // Send if batch is full
                    if batch.len() >= config.batch_size {
                        Self::send_batch(&client, &mut batch, &sequence).await;
                    }
                }

                // Flush on interval
                _ = flush_interval.tick() => {
                    if !batch.is_empty() {
                        Self::send_batch(&client, &mut batch, &sequence).await;
                    }
                }

                // Channel closed, flush remaining and exit
                else => {
                    if !batch.is_empty() {
                        Self::send_batch(&client, &mut batch, &sequence).await;
                    }
                    break;
                }
            }
        }

        Ok(())
    }

    /// Send a batch of logs
    async fn send_batch(client: &RpcClient, batch: &mut Vec<LogEntry>, sequence: &AtomicU64) {
        if batch.is_empty() {
            return;
        }

        let seq = sequence.fetch_add(1, Ordering::Relaxed);
        let log_batch = LogBatch {
            entries: std::mem::take(batch),
            sequence: seq,
            enclave_id: None, // TODO: Add enclave ID from config
        };

        let entries_count = log_batch.entries.len();

        // Use fire-and-forget for performance
        if let Err(e) = client.send_one_way(log_batch).await {
            // Log the error but don't block
            warn!(
                "Failed to send log batch {} ({} entries): {}",
                seq, entries_count, e
            );
        } else {
            debug!("Sent log batch {} with {} entries", seq, entries_count);
        }
    }
}

impl Logger for VsockLogger {
    fn log(&self, record: Record) {
        // Apply level filter
        if record.level < self.config.min_level {
            return;
        }

        // Add context if not present
        let record = if record.context.is_none() {
            record.with_context(&self.context)
        } else {
            record
        };

        // Convert to log entry
        let entry = LogEntry::from(record);

        // Try to send to channel
        match self.sender.try_send(entry.clone()) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                if !self.config.drop_on_full {
                    // If not dropping, try blocking send
                    // Note: This could block in async context, use carefully
                    if let Ok(handle) = tokio::runtime::Handle::try_current() {
                        let sender = self.sender.clone();
                        handle.spawn(async move {
                            let _ = sender.send(entry).await;
                        });
                    }
                }
                // Otherwise drop the log
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                // Channel closed, worker must have failed
            }
        }
    }

    fn flush(&self) {
        // VSOCK logger flushes automatically on interval
        // Could implement manual flush by sending a signal to worker
    }

    fn is_enabled(&self, level: Level) -> bool {
        level >= self.config.min_level && level.is_enabled_static()
    }

    fn with_context(&self, context: Context) -> Arc<dyn Logger> {
        Arc::new(Self {
            config: self.config.clone(),
            sender: self.sender.clone(),
            sequence: self.sequence.clone(),
            context: self.context.merge(&context),
            _worker_handle: tokio::spawn(async {}), // Dummy handle for clone
        })
    }
}

// Implement Drop to ensure graceful shutdown
impl Drop for VsockLogger {
    fn drop(&mut self) {
        // The channel will be dropped, causing the worker to exit and flush
    }
}
