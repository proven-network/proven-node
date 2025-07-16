//! RPC client implementation.

use crate::error::{Error, Result};
use crate::protocol::message::MessageEnvelope;
use crate::protocol::{Frame, FrameType, RequestOptions, RpcMessage, codec};
use crate::transport::connection::{ConnectionPool, PoolConfig, ResponseSender};
use dashmap::DashMap;
use futures::stream::Stream;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;
use tracing::{debug, instrument};
use uuid::Uuid;

/// Configuration for the RPC client.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// Connection pool configuration.
    pub pool_config: PoolConfig,
    /// Default request timeout.
    pub default_timeout: Duration,
    /// Whether to enable request compression.
    pub compress_requests: bool,
    /// Maximum concurrent requests.
    pub max_concurrent_requests: usize,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            pool_config: PoolConfig::default(),
            default_timeout: Duration::from_secs(30),
            compress_requests: false,
            max_concurrent_requests: 1000,
        }
    }
}

/// Builder for creating RPC clients.
pub struct ClientBuilder {
    #[cfg(target_os = "linux")]
    addr: Option<tokio_vsock::VsockAddr>,
    #[cfg(not(target_os = "linux"))]
    addr: Option<std::net::SocketAddr>,
    config: ClientConfig,
}

impl ClientBuilder {
    /// Create a new client builder.
    #[must_use]
    pub fn new() -> Self {
        Self {
            addr: None,
            config: ClientConfig::default(),
        }
    }

    /// Set the VSOCK address to connect to. (Linux only)
    #[must_use]
    pub const fn vsock_addr(
        mut self,
        #[cfg(target_os = "linux")] addr: tokio_vsock::VsockAddr,
        #[cfg(not(target_os = "linux"))] addr: std::net::SocketAddr,
    ) -> Self {
        self.addr = Some(addr);
        self
    }

    /// Set the connection pool size.
    #[must_use]
    pub const fn pool_size(mut self, size: usize) -> Self {
        self.config.pool_config.max_connections = size;
        self
    }

    /// Set the default request timeout.
    #[must_use]
    pub const fn default_timeout(mut self, timeout: Duration) -> Self {
        self.config.default_timeout = timeout;
        self
    }

    /// Enable request compression.
    #[must_use]
    pub const fn compress_requests(mut self) -> Self {
        self.config.compress_requests = true;
        self
    }

    /// Build the RPC client.
    ///
    /// # Errors
    ///
    /// Returns an error if the client fails to build.
    pub fn build(self) -> Result<RpcClient> {
        let addr = self.addr.ok_or_else(|| {
            Error::from(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "VSOCK address not specified",
            ))
        })?;

        let pending_requests = Arc::new(DashMap::new());
        let pool = ConnectionPool::new(
            addr,
            self.config.pool_config.clone(),
            Arc::clone(&pending_requests),
        );
        pool.start();

        Ok(RpcClient {
            pool,
            config: self.config,
            pending_requests,
        })
    }
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// RPC client for making requests.
pub struct RpcClient {
    pool: ConnectionPool,
    config: ClientConfig,
    pending_requests: Arc<DashMap<Uuid, ResponseSender>>,
}

impl RpcClient {
    /// Create a new client builder.
    #[must_use]
    pub fn builder() -> ClientBuilder {
        ClientBuilder::new()
    }

    /// Send a request and wait for response.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response is invalid.
    #[instrument(skip(self, message))]
    pub async fn request<M: RpcMessage>(&self, message: M) -> Result<M::Response> {
        self.request_with_options(message, RequestOptions::default())
            .await
    }

    /// Send a request with custom options.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response is invalid.
    #[instrument(skip(self, message))]
    pub async fn request_with_options<M: RpcMessage>(
        &self,
        message: M,
        options: RequestOptions,
    ) -> Result<M::Response> {
        let request_id = Uuid::new_v4();
        let message_id = message.message_id();

        debug!(
            "Sending request {} with message_id: {}",
            request_id, message_id
        );

        // Serialize message
        let payload = codec::encode(&message)?;

        // Create envelope
        let envelope = MessageEnvelope {
            id: request_id,
            message_id: message_id.to_string(),
            payload: payload.to_vec(),
            checksum: Some(crc32fast::hash(&payload)),
        };

        // Serialize envelope
        let envelope_bytes = codec::encode(&envelope)?;

        // Create frame
        let frame = Frame::new(FrameType::Request, envelope_bytes);

        // Setup response channel
        let (response_tx, response_rx) = oneshot::channel();

        // Register pending request
        self.pending_requests.insert(request_id, response_tx);

        // Get connection and send
        let conn = self.pool.get().await?;

        // Send with timeout
        let send_timeout = options.timeout.min(self.config.default_timeout);
        timeout(send_timeout, conn.send_frame(frame))
            .await
            .map_err(|_| Error::Timeout(send_timeout))??;

        // Wait for response
        let response = timeout(options.timeout, response_rx)
            .await
            .map_err(|_| {
                // Clean up pending request
                self.pending_requests.remove(&request_id);
                Error::Timeout(options.timeout)
            })?
            .map_err(|_| Error::ChannelClosed)??;

        // Check for errors
        if let Some(error) = response.error {
            return Err(Error::Handler(crate::error::HandlerError::Internal(
                format!("{}: {}", error.code, error.message),
            )));
        }

        // Deserialize response
        let response_data: M::Response = codec::decode(&response.payload)?;

        Ok(response_data)
    }

    /// Start a request/stream interaction.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response is invalid.
    #[instrument(skip(self, message))]
    pub async fn request_stream<M: RpcMessage>(
        &self,
        message: M,
    ) -> Result<impl Stream<Item = Result<M::Response>>> {
        let request_id = Uuid::new_v4();
        let message_id = message.message_id();

        debug!(
            "Starting request stream {} with message_id: {}",
            request_id, message_id
        );

        // Create channel for streaming responses
        let (_tx, rx) = mpsc::channel(32);

        // TODO: Implement actual streaming logic
        // For now, return empty stream

        Ok(tokio_stream::wrappers::ReceiverStream::new(rx))
    }

    /// Start a bidirectional stream.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response is invalid.
    #[instrument(skip(self))]
    pub async fn bidi_stream<M: RpcMessage>(
        &self,
    ) -> Result<(mpsc::Sender<M>, mpsc::Receiver<M::Response>)> {
        let stream_id = Uuid::new_v4();

        debug!("Starting bidirectional stream {}", stream_id);

        // Create channels
        let (request_tx, _request_rx) = mpsc::channel(32);
        let (response_tx, response_rx) = mpsc::channel(32);

        // TODO: Implement actual bidirectional streaming
        // For now, just return the channels

        drop(response_tx); // Prevent unused warning

        Ok((request_tx, response_rx))
    }

    /// Send a one-way message (fire and forget).
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response is invalid.
    #[instrument(skip(self, message))]
    pub async fn send_one_way<M: RpcMessage>(&self, message: M) -> Result<()> {
        let message_id = message.message_id();

        debug!("Sending one-way message with message_id: {}", message_id);

        // Serialize message
        let payload = codec::encode(&message)?;

        // Create envelope without expecting response
        let envelope = MessageEnvelope {
            id: Uuid::new_v4(),
            message_id: message_id.to_string(),
            payload: payload.to_vec(),
            checksum: Some(crc32fast::hash(&payload)),
        };

        // Serialize envelope
        let envelope_bytes = codec::encode(&envelope)?;

        // Create frame
        let frame = Frame::new(FrameType::Request, envelope_bytes);

        // Get connection and send
        let conn = self.pool.get().await?;
        conn.send_frame(frame).await?;

        Ok(())
    }

    /// Shutdown the client gracefully.
    ///
    /// # Errors
    ///
    /// Returns an error if the client fails to shutdown.
    pub fn shutdown(&self) -> Result<()> {
        debug!("Shutting down RPC client");

        // Cancel all pending requests
        // Note: We can't send on the response_tx as it's already been moved
        // Just clear the pending requests
        self.pending_requests.clear();

        // Shutdown connection pool
        self.pool.shutdown();

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_client_builder() {
        #[cfg(target_os = "linux")]
        let addr = tokio_vsock::VsockAddr::new(2, 5000);
        #[cfg(not(target_os = "linux"))]
        let addr = std::net::SocketAddr::from(([127, 0, 0, 1], 5000));

        let result = RpcClient::builder()
            .vsock_addr(addr)
            .pool_size(5)
            .default_timeout(Duration::from_secs(60))
            .compress_requests()
            .build();

        // Should succeed in building the client (connection happens lazily)
        assert!(result.is_ok());
    }
}
