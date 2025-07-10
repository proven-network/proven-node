//! Server implementation for CAC commands.

use crate::commands::{
    CacCommand, CacResponse, InitializeRequest, InitializeResponse, ShutdownRequest,
    ShutdownResponse,
};
use crate::error::Result;
use async_trait::async_trait;
use bytes::Bytes;
use proven_vsock_rpc::{
    HandlerResponse, MessagePattern, RpcHandler, RpcMessage, RpcServer, ServerConfig, VsockAddr,
    protocol::codec,
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
        message: Bytes,
        _pattern: MessagePattern,
    ) -> proven_vsock_rpc::Result<HandlerResponse> {
        // Decode the command
        let command: CacCommand = codec::decode(&message)?;

        debug!("Received CAC command: {:?}", command.message_id());

        // Handle the command
        let response = match command {
            CacCommand::Initialize(request) => match self.inner.handle_initialize(*request).await {
                Ok(resp) => {
                    if resp.success {
                        let mut state = self.state.write().await;
                        state.initialized = true;
                        drop(state);
                        info!("Enclave initialized successfully");
                    }
                    CacResponse::Initialize(resp)
                }
                Err(e) => {
                    error!("Initialize failed: {}", e);
                    CacResponse::Initialize(InitializeResponse {
                        success: false,
                        error: Some(e.to_string()),
                    })
                }
            },
            CacCommand::Shutdown(request) => {
                {
                    let mut state = self.state.write().await;
                    state.shutting_down = true;
                }

                match self.inner.handle_shutdown(request).await {
                    Ok(resp) => {
                        info!("Shutdown initiated");
                        CacResponse::Shutdown(resp)
                    }
                    Err(e) => {
                        error!("Shutdown failed: {}", e);
                        CacResponse::Shutdown(ShutdownResponse {
                            success: false,
                            message: Some(e.to_string()),
                        })
                    }
                }
            }
        };

        // Encode the response
        let response_bytes = codec::encode(&response)?;

        Ok(HandlerResponse::Single(response_bytes))
    }

    async fn on_connect(&self, addr: VsockAddr) -> proven_vsock_rpc::Result<()> {
        info!("CAC client connected from {:?}", addr);
        Ok(())
    }

    async fn on_disconnect(&self, addr: VsockAddr) {
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
    pub fn new(addr: VsockAddr, handler: H) -> Self {
        let cac_handler = CacHandler::new(handler);
        let config = ServerConfig::default();
        let inner = RpcServer::new(addr, cac_handler, config);

        Self { inner }
    }

    /// Create a new CAC server with custom configuration.
    #[must_use]
    pub fn with_config(addr: VsockAddr, handler: H, config: ServerConfig) -> Self {
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
