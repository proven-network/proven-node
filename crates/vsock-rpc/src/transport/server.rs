//! RPC server implementation.

use crate::error::{Error, HandlerError, Result};
use crate::protocol::message::{ErrorInfo, MessageEnvelope, ResponseEnvelope};
use crate::protocol::{Frame, FrameCodec, FrameType, MessagePattern, codec, patterns::RetryPolicy};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;
use tokio_util::codec::Framed;
use tokio_vsock::{VsockAddr, VsockListener, VsockStream};
use tracing::{debug, error, info, instrument, warn};

/// Configuration for the RPC server.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Maximum concurrent connections.
    pub max_connections: usize,
    /// Request handling timeout.
    pub request_timeout: Duration,
    /// Maximum frame size.
    pub max_frame_size: usize,
    /// Enable request decompression.
    pub decompress_requests: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            max_connections: 100,
            request_timeout: Duration::from_secs(60),
            max_frame_size: 10 * 1024 * 1024, // 10MB
            decompress_requests: false,
        }
    }
}

/// Response from a handler.
pub enum HandlerResponse {
    /// Single response.
    Single(Bytes),
    /// Stream of responses.
    Stream(mpsc::Receiver<Result<Bytes>>),
    /// No response (for one-way messages).
    None,
}

/// Trait for handling RPC requests.
#[async_trait]
pub trait RpcHandler: Send + Sync + 'static {
    /// Handle an incoming message.
    async fn handle_message(
        &self,
        message: Bytes,
        pattern: MessagePattern,
    ) -> Result<HandlerResponse>;

    /// Called when a new connection is established.
    async fn on_connect(&self, _addr: VsockAddr) -> Result<()> {
        Ok(())
    }

    /// Called when a connection is closed.
    async fn on_disconnect(&self, _addr: VsockAddr) {
        // Default: do nothing
    }
}

/// RPC server that listens for incoming connections.
pub struct RpcServer<H: RpcHandler> {
    addr: VsockAddr,
    handler: Arc<H>,
    config: ServerConfig,
    shutdown_tx: Option<oneshot::Sender<()>>,
}

impl<H: RpcHandler> RpcServer<H> {
    /// Create a new RPC server.
    pub fn new(addr: VsockAddr, handler: H, config: ServerConfig) -> Self {
        Self {
            addr,
            handler: Arc::new(handler),
            config,
            shutdown_tx: None,
        }
    }

    /// Start serving requests.
    ///
    /// # Errors
    ///
    /// Returns an error if the server fails to bind or accept connections.
    #[instrument(skip(self))]
    pub async fn serve(mut self) -> Result<()> {
        let listener = VsockListener::bind(self.addr).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::AddrInUse,
                format!("Failed to bind to {:?}: {}", self.addr, e),
            )
        })?;

        info!("RPC server listening on {:?}", self.addr);

        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        self.shutdown_tx = Some(shutdown_tx);

        let semaphore = Arc::new(tokio::sync::Semaphore::new(self.config.max_connections));

        loop {
            tokio::select! {
                accept_result = listener.accept() => {
                    match accept_result {
                        Ok((stream, addr)) => {
                            let handler = Arc::clone(&self.handler);
                            let config = self.config.clone();
                            let permit = semaphore.clone().try_acquire_owned();

                            match permit {
                                Ok(permit) => {
                                    tokio::spawn(async move {
                                        if let Err(e) = Self::handle_connection(
                                            stream,
                                            addr,
                                            handler,
                                                config,
                                        ).await {
                                            error!("Connection error from {:?}: {}", addr, e);
                                        }
                                        drop(permit);
                                    });
                                }
                                Err(_) => {
                                    warn!("Max connections reached, rejecting connection from {:?}", addr);
                                    // Connection will be dropped
                                }
                            }
                        }
                        Err(e) => {
                            error!("Failed to accept connection: {}", e);
                        }
                    }
                }
                _ = &mut shutdown_rx => {
                    info!("Server shutdown requested");
                    break;
                }
            }
        }

        Ok(())
    }

    /// Handle a single connection.
    #[instrument(skip(stream, handler))]
    async fn handle_connection(
        stream: VsockStream,
        addr: VsockAddr,
        handler: Arc<H>,
        config: ServerConfig,
    ) -> Result<()> {
        debug!("New connection from {:?}", addr);

        // Notify handler
        handler.on_connect(addr).await?;

        let mut framed = Framed::new(
            stream,
            FrameCodec::new().with_max_frame_size(config.max_frame_size),
        );

        loop {
            match timeout(config.request_timeout, framed.next()).await {
                Ok(Some(Ok(frame))) => {
                    match frame.frame_type {
                        FrameType::Request => {
                            // Handle request inline
                            if let Err(e) = Self::handle_request(
                                frame,
                                Arc::clone(&handler),
                                config.clone(),
                                &mut framed,
                            )
                            .await
                            {
                                error!("Failed to handle request: {}", e);
                            }
                        }
                        FrameType::Heartbeat => {
                            // Echo back heartbeat
                            let pong = Frame::new(FrameType::Heartbeat, frame.payload);
                            if let Err(e) = framed.send(pong).await {
                                error!("Failed to send heartbeat response: {}", e);
                                break;
                            }
                        }
                        FrameType::Close => {
                            debug!("Client requested close");
                            break;
                        }
                        _ => {
                            warn!("Unexpected frame type: {:?}", frame.frame_type);
                        }
                    }
                }
                Ok(Some(Err(e))) => {
                    error!("Frame error: {}", e);
                    break;
                }
                Ok(None) => {
                    debug!("Connection closed by client");
                    break;
                }
                Err(_) => {
                    warn!("Request timeout");
                    break;
                }
            }
        }

        // Notify handler
        handler.on_disconnect(addr).await;

        Ok(())
    }

    /// Handle a single request.
    async fn handle_request(
        frame: Frame,
        handler: Arc<H>,
        _config: ServerConfig,
        sink: &mut Framed<VsockStream, FrameCodec>,
    ) -> Result<()> {
        // Deserialize envelope
        let envelope: MessageEnvelope = codec::decode(&frame.payload)?;

        // Verify checksum if present
        if let Some(expected) = envelope.checksum {
            let actual = crc32fast::hash(&envelope.payload);
            if expected != actual {
                return Self::send_error_response(
                    sink,
                    envelope.id,
                    "CHECKSUM_MISMATCH",
                    "Message checksum verification failed",
                )
                .await;
            }
        }

        // Handle the message
        let pattern = MessagePattern::RequestResponse {
            timeout: Duration::from_secs(30),
            retry_policy: RetryPolicy::default(),
        };

        match handler
            .handle_message(envelope.payload.into(), pattern)
            .await
        {
            Ok(HandlerResponse::Single(response)) => {
                // Send single response
                let response_envelope = ResponseEnvelope {
                    request_id: envelope.id,
                    message_id: format!("{}.response", envelope.message_id),
                    payload: response.to_vec(),
                    error: None,
                };

                let response_bytes = codec::encode(&response_envelope)?;
                let response_frame = Frame::new(FrameType::Response, response_bytes);

                sink.send(response_frame).await.map_err(Error::Io)?;
            }
            Ok(HandlerResponse::Stream(mut stream)) => {
                // Send stream of responses
                while let Some(result) = stream.recv().await {
                    match result {
                        Ok(response) => {
                            let response_envelope = ResponseEnvelope {
                                request_id: envelope.id,
                                message_id: format!("{}.stream", envelope.message_id),
                                payload: response.to_vec(),
                                error: None,
                            };

                            let response_bytes = codec::encode(&response_envelope)?;
                            let response_frame = Frame::new(FrameType::Stream, response_bytes);

                            if let Err(e) = sink.send(response_frame).await {
                                error!("Failed to send stream response: {}", e);
                                break;
                            }
                        }
                        Err(e) => {
                            error!("Stream error: {}", e);
                            break;
                        }
                    }
                }
            }
            Ok(HandlerResponse::None) => {
                // No response expected
                debug!("One-way message handled successfully");
            }
            Err(e) => {
                // Send error response
                let (code, message) = match &e {
                    Error::Handler(HandlerError::NotFound(_)) => ("NOT_FOUND", e.to_string()),
                    Error::Handler(HandlerError::Internal(_)) => ("INTERNAL_ERROR", e.to_string()),
                    _ => ("UNKNOWN_ERROR", e.to_string()),
                };

                return Self::send_error_response(sink, envelope.id, code, &message).await;
            }
        }

        Ok(())
    }

    /// Send an error response.
    async fn send_error_response(
        sink: &mut Framed<VsockStream, FrameCodec>,
        request_id: uuid::Uuid,
        code: &str,
        message: &str,
    ) -> Result<()> {
        let response_envelope = ResponseEnvelope {
            request_id,
            message_id: "error".to_string(),
            payload: vec![],
            error: Some(ErrorInfo {
                code: code.to_string(),
                message: message.to_string(),
                details: None,
            }),
        };

        let response_bytes = codec::encode(&response_envelope)?;
        let response_frame = Frame::new(FrameType::Error, response_bytes);

        sink.send(response_frame).await.map_err(Error::Io)?;

        Ok(())
    }

    /// Shutdown the server gracefully.
    ///
    /// # Errors
    ///
    /// Returns an error if shutdown fails.
    pub fn shutdown(self) -> Result<()> {
        if let Some(tx) = self.shutdown_tx {
            let _ = tx.send(());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestHandler;

    #[async_trait]
    impl RpcHandler for TestHandler {
        async fn handle_message(
            &self,
            _message: Bytes,
            _pattern: MessagePattern,
        ) -> Result<HandlerResponse> {
            Ok(HandlerResponse::Single(Bytes::from("test response")))
        }
    }

    #[tokio::test]
    async fn test_server_creation() {
        let addr = VsockAddr::new(2, 5001);
        let handler = TestHandler;
        let config = ServerConfig::default();

        let server = RpcServer::new(addr, handler, config);

        // Server should be created successfully
        assert_eq!(server.addr, addr);
    }
}
