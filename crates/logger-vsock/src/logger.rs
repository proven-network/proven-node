//! VSOCK tracing subscriber implementation

pub use crate::config::{VsockLoggerConfig, VsockLoggerConfigBuilder};
use crate::{
    error::Result,
    messages::{LogBatch, LogEntry},
};
use proven_vsock_rpc::RpcClient;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use tokio::sync::mpsc;
use tokio::time::interval;
use tracing::{Event, Level, Subscriber, debug, error, warn};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::{Layer, layer::Context};

/// VSOCK-based tracing subscriber that batches and sends logs to a remote collector
#[derive(Clone)]
pub struct VsockSubscriber {
    /// Configuration
    config: VsockLoggerConfig,
    /// Channel sender for log entries
    sender: mpsc::Sender<LogEntry>,
    /// Sequence counter for batches
    sequence: Arc<AtomicU64>,
}

impl VsockSubscriber {
    /// Create a new VSOCK subscriber with the given configuration
    pub async fn new(config: VsockLoggerConfig) -> Result<Self> {
        let (sender, receiver) = mpsc::channel(config.channel_buffer_size);
        let sequence = Arc::new(AtomicU64::new(0));

        // Create RPC client
        let rpc_client = RpcClient::builder().vsock_addr(config.vsock_addr).build()?;

        // Spawn background worker
        let worker_config = config.clone();
        let worker_sequence = sequence.clone();
        tokio::spawn(async move {
            if let Err(e) =
                Self::worker_loop(receiver, rpc_client, worker_config, worker_sequence).await
            {
                error!("VSOCK subscriber worker failed: {}", e);
            }
        });

        Ok(Self {
            config,
            sender,
            sequence,
        })
    }

    /// Background worker that batches and sends logs
    async fn worker_loop(
        mut receiver: mpsc::Receiver<LogEntry>,
        client: RpcClient,
        config: VsockLoggerConfig,
        sequence: Arc<AtomicU64>,
    ) -> Result<()> {
        let mut batch = Vec::with_capacity(config.batch_size);
        let mut interval = interval(config.batch_interval);

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

                // Periodic flush
                _ = interval.tick() => {
                    if !batch.is_empty() {
                        Self::send_batch(&client, &mut batch, &sequence).await;
                    }
                }

                // Channel closed
                else => {
                    // Send any remaining logs
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

impl<S> Layer<S> for VsockSubscriber
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        // Apply level filter
        let level = *event.metadata().level();
        if level < Level::from(self.config.min_level) {
            return;
        }

        // Get timestamp
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        // Extract message
        let mut message = String::new();
        let mut visitor = MessageVisitor(&mut message);
        event.record(&mut visitor);

        // Extract span context
        let (node_id, component, span_id) =
            if let Some(span) = ctx.current_span().id().and_then(|id| ctx.span(id)) {
                let mut node_id: Option<String> = None;
                let component = span.name().to_string();
                let span_id = Some(span.id().into_u64());

                // Walk up the span tree looking for node_id
                let mut current_span = Some(span);
                while let Some(span_ref) = current_span {
                    // Check span name for node pattern
                    if span_ref.name().starts_with("node:") {
                        node_id = Some(span_ref.name()[5..].to_string());
                        break;
                    }
                    current_span = span_ref.parent();
                }

                (node_id, Some(component), span_id)
            } else {
                (None, None, None)
            };

        // Get metadata
        let metadata = event.metadata();

        let entry = LogEntry {
            level: level.into(),
            target: metadata.target().to_string(),
            message,
            file: metadata.file().map(|s| s.to_string()),
            line: metadata.line(),
            node_id,
            component,
            span_id,
            timestamp,
        };

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
}

/// Visitor to extract the message from an event
struct MessageVisitor<'a>(&'a mut String);

impl<'a> tracing::field::Visit for MessageVisitor<'a> {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "message" {
            self.0.push_str(value);
        }
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            use std::fmt::Write;
            let _ = write!(self.0, "{:?}", value);
        }
    }
}
