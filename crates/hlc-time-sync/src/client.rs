//! Client for requesting time from host via vsock.

use crate::error::{Error, Result};
use crate::messages::{TimeRequest, TimeResponse};
use proven_vsock_rpc::{ClientBuilder, RpcClient};
use std::time::Duration;

/// Client for getting time from host.
pub struct TimeClient {
    rpc_client: RpcClient,
}

impl TimeClient {
    /// Create a new time client.
    ///
    /// # Errors
    ///
    /// Returns an error if the RPC client fails to build.
    pub fn new(
        #[cfg(target_os = "linux")] vsock_addr: tokio_vsock::VsockAddr,
        #[cfg(not(target_os = "linux"))] addr: std::net::SocketAddr,
    ) -> Result<Self> {
        let rpc_client = ClientBuilder::new()
            .vsock_addr(
                #[cfg(target_os = "linux")]
                vsock_addr,
                #[cfg(not(target_os = "linux"))]
                addr,
            )
            .build()?;

        Ok(Self { rpc_client })
    }

    /// Get current time from host.
    ///
    /// # Errors
    ///
    /// Returns an error if the RPC request fails.
    pub async fn get_time(&self) -> Result<TimeResponse> {
        self.rpc_client
            .request(TimeRequest)
            .await
            .map_err(Into::into)
    }

    /// Get time with a specific timeout.
    ///
    /// # Errors
    ///
    /// Returns an error if the request times out or fails.
    pub async fn get_time_with_timeout(&self, timeout_ms: u64) -> Result<TimeResponse> {
        let timeout = Duration::from_millis(timeout_ms);

        tokio::time::timeout(timeout, self.get_time())
            .await
            .map_err(|_| Error::Internal("Request timed out".to_string()))?
    }
}
