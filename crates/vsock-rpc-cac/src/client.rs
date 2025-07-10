//! Client implementation for CAC commands.

use crate::commands::{
    CacCommand, CacResponse, InitializeRequest, InitializeResponse, ShutdownRequest,
    ShutdownResponse,
};
use crate::error::{Error, Result};
use proven_vsock_rpc::{RpcClient, VsockAddr};
use tracing::{debug, instrument};

/// Client for sending CAC commands to the enclave.
pub struct CacClient {
    inner: RpcClient,
}

impl CacClient {
    /// Create a new CAC client connected to the given VSOCK address.
    ///
    /// # Errors
    /// Returns an error if the RPC client cannot be created.
    pub fn new(addr: VsockAddr) -> Result<Self> {
        let inner = RpcClient::builder().vsock_addr(addr).build()?;

        Ok(Self { inner })
    }

    /// Create a new CAC client from an existing RPC client.
    #[must_use]
    pub const fn from_rpc_client(inner: RpcClient) -> Self {
        Self { inner }
    }

    /// Initialize the enclave with the given configuration.
    ///
    /// # Errors
    /// Returns an error if the command fails or the response is invalid.
    #[instrument(skip(self, request))]
    pub async fn initialize(&self, request: InitializeRequest) -> Result<InitializeResponse> {
        debug!("Sending initialize request");

        let command = CacCommand::Initialize(Box::new(request));
        let response = self.inner.request(command).await?;

        match response {
            CacResponse::Initialize(resp) => Ok(resp),
            CacResponse::Shutdown(_) => Err(Error::InvalidResponseType {
                expected: "Initialize".to_string(),
                actual: format!("{response:?}"),
            }),
        }
    }

    /// Shutdown the enclave.
    ///
    /// # Errors
    /// Returns an error if the command fails or the response is invalid.
    #[instrument(skip(self))]
    pub async fn shutdown(&self) -> Result<ShutdownResponse> {
        self.shutdown_with_options(ShutdownRequest::default()).await
    }

    /// Shutdown the enclave with specific options.
    ///
    /// # Errors
    /// Returns an error if the command fails or the response is invalid.
    #[instrument(skip(self, request))]
    pub async fn shutdown_with_options(
        &self,
        request: ShutdownRequest,
    ) -> Result<ShutdownResponse> {
        debug!("Sending shutdown request");

        let command = CacCommand::Shutdown(request);
        let response = self.inner.request(command).await?;

        match response {
            CacResponse::Shutdown(resp) => Ok(resp),
            CacResponse::Initialize(_) => Err(Error::InvalidResponseType {
                expected: "Shutdown".to_string(),
                actual: format!("{response:?}"),
            }),
        }
    }
}
