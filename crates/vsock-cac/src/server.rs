//! Server implementation for CAC commands.

use crate::commands::{InitializeRequest, InitializeResponse, ShutdownRequest, ShutdownResponse};
use crate::error::Result;
use async_trait::async_trait;
use bytes::Bytes;
use proven_vsock_rpc::{
    HandlerResponse, MessagePattern, RpcHandler, RpcServer, ServerConfig, error::HandlerError,
};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, instrument};

/// Handler for CAC commands.
pub struct CacHandler<H> {
    inner: Arc<H>,
    state: Arc<RwLock<HandlerState>>,
}

/// State of the CAC handler.
#[derive(Debug, Default)]
struct HandlerState {
    initialized: bool,
    shutting_down: bool,
}

/// Trait that must be implemented to handle CAC commands.
#[async_trait]
pub trait CacCommandHandler: Send + Sync + 'static {
    /// Handle initialization request.
    async fn handle_initialize(&self, request: InitializeRequest) -> Result<InitializeResponse>;

    /// Handle shutdown request.
    async fn handle_shutdown(&self, request: ShutdownRequest) -> Result<ShutdownResponse>;
}

impl<H: CacCommandHandler> CacHandler<H> {
    /// Create a new CAC handler.
    #[must_use]
    pub fn new(inner: H) -> Self {
        Self {
            inner: Arc::new(inner),
            state: Arc::new(RwLock::new(HandlerState::default())),
        }
    }

    /// Check if the handler has been initialized.
    pub async fn is_initialized(&self) -> bool {
        self.state.read().await.initialized
    }

    /// Check if the handler is shutting down.
    pub async fn is_shutting_down(&self) -> bool {
        self.state.read().await.shutting_down
    }
}

#[async_trait]
impl<H: CacCommandHandler> RpcHandler for CacHandler<H> {
    #[instrument(skip(self, message))]
    async fn handle_message(
        &self,
        message_id: &str,
        message: Bytes,
        _pattern: MessagePattern,
    ) -> proven_vsock_rpc::Result<HandlerResponse> {
        debug!("Received CAC command: {}", message_id);

        // Route based on message_id
        match message_id {
            "cac.initialize" => {
                // Decode InitializeRequest
                let request = InitializeRequest::try_from(message).map_err(|e| {
                    proven_vsock_rpc::Error::Handler(HandlerError::Internal(format!(
                        "Failed to decode InitializeRequest: {e}"
                    )))
                })?;

                // Handle the request
                match self.inner.handle_initialize(request).await {
                    Ok(resp) => {
                        if resp.success {
                            let mut state = self.state.write().await;
                            state.initialized = true;
                            drop(state);
                            info!("Enclave initialized successfully");
                        }

                        // Encode response
                        let response_bytes: Bytes =
                            resp.try_into().map_err(|e: bincode::Error| {
                                proven_vsock_rpc::Error::Handler(HandlerError::Internal(format!(
                                    "Failed to encode response: {e}"
                                )))
                            })?;
                        Ok(HandlerResponse::Single(response_bytes))
                    }
                    Err(e) => {
                        error!("Initialize failed: {}", e);
                        let resp = InitializeResponse {
                            success: false,
                            error: Some(e.to_string()),
                        };

                        let response_bytes: Bytes =
                            resp.try_into().map_err(|e: bincode::Error| {
                                proven_vsock_rpc::Error::Handler(HandlerError::Internal(format!(
                                    "Failed to encode response: {e}"
                                )))
                            })?;
                        Ok(HandlerResponse::Single(response_bytes))
                    }
                }
            }
            "cac.shutdown" => {
                // Decode ShutdownRequest
                let request = ShutdownRequest::try_from(message).map_err(|e| {
                    proven_vsock_rpc::Error::Handler(HandlerError::Internal(format!(
                        "Failed to decode ShutdownRequest: {e}"
                    )))
                })?;

                {
                    let mut state = self.state.write().await;
                    state.shutting_down = true;
                }

                // Handle the request
                match self.inner.handle_shutdown(request).await {
                    Ok(resp) => {
                        info!("Shutdown initiated");

                        // Encode response
                        let response_bytes: Bytes =
                            resp.try_into().map_err(|e: bincode::Error| {
                                proven_vsock_rpc::Error::Handler(HandlerError::Internal(format!(
                                    "Failed to encode response: {e}"
                                )))
                            })?;
                        Ok(HandlerResponse::Single(response_bytes))
                    }
                    Err(e) => {
                        error!("Shutdown failed: {}", e);
                        let resp = ShutdownResponse {
                            success: false,
                            message: Some(e.to_string()),
                        };

                        let response_bytes: Bytes =
                            resp.try_into().map_err(|e: bincode::Error| {
                                proven_vsock_rpc::Error::Handler(HandlerError::Internal(format!(
                                    "Failed to encode response: {e}"
                                )))
                            })?;
                        Ok(HandlerResponse::Single(response_bytes))
                    }
                }
            }
            _ => Err(proven_vsock_rpc::Error::Handler(HandlerError::NotFound(
                format!("Unknown message_id: {message_id}"),
            ))),
        }
    }

    async fn on_connect(
        &self,
        #[cfg(target_os = "linux")] addr: tokio_vsock::VsockAddr,
        #[cfg(not(target_os = "linux"))] addr: std::net::SocketAddr,
    ) -> proven_vsock_rpc::Result<()> {
        info!("CAC client connected from {:?}", addr);
        Ok(())
    }

    async fn on_disconnect(
        &self,
        #[cfg(target_os = "linux")] addr: tokio_vsock::VsockAddr,
        #[cfg(not(target_os = "linux"))] addr: std::net::SocketAddr,
    ) {
        info!("CAC client disconnected from {:?}", addr);
    }
}

/// CAC server that listens for commands.
pub struct CacServer<H: CacCommandHandler> {
    inner: RpcServer<CacHandler<H>>,
}

impl<H: CacCommandHandler> CacServer<H> {
    /// Create a new CAC server.
    #[must_use]
    pub fn new(
        #[cfg(target_os = "linux")] addr: tokio_vsock::VsockAddr,
        #[cfg(not(target_os = "linux"))] addr: std::net::SocketAddr,
        handler: H,
    ) -> Self {
        let cac_handler = CacHandler::new(handler);
        let config = ServerConfig::default();
        let inner = RpcServer::new(addr, cac_handler, config);

        Self { inner }
    }

    /// Create a new CAC server with custom configuration.
    #[must_use]
    pub fn with_config(
        #[cfg(target_os = "linux")] addr: tokio_vsock::VsockAddr,
        #[cfg(not(target_os = "linux"))] addr: std::net::SocketAddr,
        handler: H,
        config: ServerConfig,
    ) -> Self {
        let cac_handler = CacHandler::new(handler);
        let inner = RpcServer::new(addr, cac_handler, config);

        Self { inner }
    }

    /// Start serving CAC commands.
    ///
    /// # Errors
    /// Returns an error if the server fails to start or encounters a fatal error.
    pub async fn serve(self) -> proven_vsock_rpc::Result<()> {
        info!("Starting CAC server");
        self.inner.serve().await
    }
}
