//! Server-side log collector for receiving logs over VSOCK

use crate::{
    error::{Error, Result},
    messages::{LogBatch, LogBatchAck},
};
use proven_vsock_rpc::{HandlerResponse, MessagePattern, RpcHandler, RpcServer, ServerConfig};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, error, info};

/// Trait for processing received log batches
#[async_trait::async_trait]
pub trait LogProcessor: Send + Sync + 'static {
    /// Process a batch of logs
    async fn process_batch(&self, batch: LogBatch) -> Result<()>;
}

/// Simple log processor that writes to stdout
pub struct StdoutLogProcessor;

#[async_trait::async_trait]
impl LogProcessor for StdoutLogProcessor {
    async fn process_batch(&self, batch: LogBatch) -> Result<()> {
        for entry in batch.entries {
            println!(
                "[{}] {} [{}] {}{}",
                entry.timestamp,
                entry.level,
                entry.target,
                entry
                    .node_id
                    .as_ref()
                    .map(|id| format!(" <{id}> "))
                    .unwrap_or_default(),
                entry.message
            );
        }
        Ok(())
    }
}

/// Log processor that forwards to another logger
pub struct ForwardingLogProcessor {
    logger: Arc<dyn proven_logger::Logger>,
}

impl ForwardingLogProcessor {
    /// Create a new forwarding processor
    pub fn new(logger: Arc<dyn proven_logger::Logger>) -> Self {
        Self { logger }
    }
}

#[async_trait::async_trait]
impl LogProcessor for ForwardingLogProcessor {
    async fn process_batch(&self, batch: LogBatch) -> Result<()> {
        use proven_logger::Record;

        for entry in batch.entries {
            // Convert target to 'static str
            let target: &'static str = Box::leak(entry.target.into_boxed_str());

            let mut record = Record::new(entry.level, entry.message).with_target(target);

            if let (Some(file), Some(line)) = (entry.file, entry.line) {
                // Convert file to 'static str
                let file: &'static str = Box::leak(file.into_boxed_str());
                record = record.with_location(file, line);
            }

            // Handle context separately since it has a different lifetime
            if let Some(node_id) = entry.node_id {
                // For context, we need to use a different approach
                // Since the logger.log() method takes ownership of the record,
                // we can't use a local context. Instead, let's add the node_id
                // to the message or create an owned record.
                let msg = format!("[node:{}] {}", node_id, record.message);
                record.message = msg.into();
            }

            self.logger.log(record);
        }
        Ok(())
    }
}

/// Channel-based log processor for custom handling
pub struct ChannelLogProcessor {
    sender: mpsc::Sender<LogBatch>,
}

impl ChannelLogProcessor {
    /// Create a new channel processor
    pub fn new(buffer_size: usize) -> (Self, mpsc::Receiver<LogBatch>) {
        let (sender, receiver) = mpsc::channel(buffer_size);
        (Self { sender }, receiver)
    }
}

#[async_trait::async_trait]
impl LogProcessor for ChannelLogProcessor {
    async fn process_batch(&self, batch: LogBatch) -> Result<()> {
        self.sender
            .send(batch)
            .await
            .map_err(|_| Error::SendFailed("Channel closed".to_string()))
    }
}

/// VSOCK log collector server
pub struct VsockLogCollector {
    processor: Arc<dyn LogProcessor>,
    acknowledge: bool,
}

impl VsockLogCollector {
    /// Create a new log collector with the given processor
    pub fn new(processor: Arc<dyn LogProcessor>) -> Self {
        Self {
            processor,
            acknowledge: false,
        }
    }

    /// Enable acknowledgments (for reliability testing)
    pub fn with_acknowledgments(mut self) -> Self {
        self.acknowledge = true;
        self
    }
}

#[async_trait::async_trait]
impl RpcHandler for VsockLogCollector {
    async fn handle_message(
        &self,
        message_id: &str,
        message: bytes::Bytes,
        _pattern: MessagePattern,
    ) -> proven_vsock_rpc::Result<HandlerResponse> {
        match message_id {
            "log_batch" => {
                let batch: LogBatch = bincode::deserialize(&message).map_err(|e| {
                    proven_vsock_rpc::error::Error::Codec(
                        proven_vsock_rpc::error::CodecError::DeserializationFailed(e.to_string()),
                    )
                })?;

                let sequence = batch.sequence;
                let entries_count = batch.entries.len();

                debug!(
                    "Received log batch {} with {} entries",
                    sequence, entries_count
                );

                // Process the batch
                if let Err(e) = self.processor.process_batch(batch).await {
                    error!("Failed to process log batch {}: {}", sequence, e);
                }

                if self.acknowledge {
                    // Send acknowledgment if enabled
                    let ack = LogBatchAck {
                        sequence,
                        entries_received: entries_count,
                    };

                    let response =
                        bincode::serialize(&ack)
                            .map(bytes::Bytes::from)
                            .map_err(|e| {
                                proven_vsock_rpc::error::Error::Codec(
                                    proven_vsock_rpc::error::CodecError::SerializationFailed(
                                        e.to_string(),
                                    ),
                                )
                            })?;

                    Ok(HandlerResponse::Single(response))
                } else {
                    // Fire-and-forget mode
                    Ok(HandlerResponse::None)
                }
            }
            _ => {
                error!("Unknown message type: {}", message_id);
                Err(proven_vsock_rpc::error::Error::Handler(
                    proven_vsock_rpc::error::HandlerError::NotFound(format!(
                        "Unknown message type: {message_id}"
                    )),
                ))
            }
        }
    }
}

/// Builder for creating a VSOCK log server
pub struct VsockLogServerBuilder {
    #[cfg(target_os = "linux")]
    addr: tokio_vsock::VsockAddr,
    #[cfg(not(target_os = "linux"))]
    addr: std::net::SocketAddr,
    processor: Option<Arc<dyn LogProcessor>>,
    acknowledge: bool,
    server_config: ServerConfig,
}

impl VsockLogServerBuilder {
    /// Create a new builder
    pub fn new(
        #[cfg(target_os = "linux")] addr: tokio_vsock::VsockAddr,
        #[cfg(not(target_os = "linux"))] addr: std::net::SocketAddr,
    ) -> Self {
        Self {
            addr,
            processor: None,
            acknowledge: false,
            server_config: ServerConfig::default(),
        }
    }

    /// Set the log processor
    pub fn processor(mut self, processor: Arc<dyn LogProcessor>) -> Self {
        self.processor = Some(processor);
        self
    }

    /// Enable acknowledgments
    pub fn with_acknowledgments(mut self) -> Self {
        self.acknowledge = true;
        self
    }

    /// Set server configuration
    pub fn server_config(mut self, config: ServerConfig) -> Self {
        self.server_config = config;
        self
    }

    /// Build and start the server
    pub async fn build(self) -> Result<RpcServer<VsockLogCollector>> {
        let processor = self
            .processor
            .unwrap_or_else(|| Arc::new(StdoutLogProcessor));

        let mut collector = VsockLogCollector::new(processor);
        if self.acknowledge {
            collector = collector.with_acknowledgments();
        }

        let server = RpcServer::new(self.addr, collector, self.server_config);

        info!("VSOCK log collector server configured for {:?}", self.addr);

        Ok(server)
    }
}

/// Convenience function to start a simple stdout log collector
pub async fn run_stdout_collector(
    #[cfg(target_os = "linux")] addr: tokio_vsock::VsockAddr,
    #[cfg(not(target_os = "linux"))] addr: std::net::SocketAddr,
) -> Result<()> {
    let server = VsockLogServerBuilder::new(addr).build().await?;

    // Start serving in a separate task
    let serve_handle = tokio::spawn(async move {
        if let Err(e) = server.serve().await {
            error!("Server error: {}", e);
        }
    });

    // Wait for shutdown signal
    tokio::signal::ctrl_c()
        .await
        .map_err(|e| Error::Configuration(format!("Failed to listen for ctrl-c: {e}")))?;

    info!("Shutting down log collector...");
    serve_handle.abort();

    Ok(())
}
