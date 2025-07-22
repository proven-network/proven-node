//! Main entrypoint for enclave images. Bootstraps all other components before
//! handing off to core.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![allow(clippy::redundant_pub_crate)]
#![allow(clippy::result_large_err)]
#![allow(clippy::large_futures)]

mod bootstrap;
mod error;
mod fdlimit;
mod net;
mod node;
mod speedtest;

use bootstrap::Bootstrap;
use error::{Error, Result};
use node::EnclaveNode;
use proven_logger_vsock::client::{VsockLogger, VsockLoggerConfig};

use std::sync::Arc;

use async_trait::async_trait;
use proven_vsock_cac::{
    CacServer, InitializeRequest, InitializeResponse, ShutdownResponse, commands::ShutdownRequest,
    server::CacCommandHandler,
};
use tokio::sync::RwLock;
#[cfg(target_os = "linux")]
use tokio_vsock::{VMADDR_CID_ANY, VsockAddr};
use tracing::{error, info};
use tracing_panic::panic_hook;

/// Enclave command handler state
struct EnclaveHandler {
    state: Arc<RwLock<Option<EnclaveNode>>>,
}

impl EnclaveHandler {
    fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(None)),
        }
    }
}

#[async_trait]
impl CacCommandHandler for EnclaveHandler {
    async fn handle_initialize(
        &self,
        request: InitializeRequest,
    ) -> proven_vsock_cac::Result<InitializeResponse> {
        // Check if already initialized
        if self.state.read().await.is_some() {
            error!("Already initialized");
            return Ok(InitializeResponse {
                success: false,
                error: Some("Already initialized".to_string()),
            });
        }

        // Create bootstrap from request
        let Ok(bootstrap) = Bootstrap::new(request) else {
            return Ok(InitializeResponse {
                success: false,
                error: Some("Failed to create bootstrap".to_string()),
            });
        };

        // Initialize the enclave
        match bootstrap.initialize().await {
            Ok(enclave) => {
                info!("Enclave started successfully");
                *self.state.write().await = Some(enclave);
                Ok(InitializeResponse {
                    success: true,
                    error: None,
                })
            }
            Err(e) => {
                error!("Failed to start enclave: {:?}", e);
                Ok(InitializeResponse {
                    success: false,
                    error: Some(format!("Failed to start enclave: {e}")),
                })
            }
        }
    }

    async fn handle_shutdown(
        &self,
        _request: ShutdownRequest,
    ) -> proven_vsock_cac::Result<ShutdownResponse> {
        let state = self.state.write().await.take();
        if let Some(enclave) = state {
            enclave.shutdown().await;
            info!("Enclave shutdown successfully");
            Ok(ShutdownResponse {
                success: true,
                message: Some("Enclave shutdown successfully".to_string()),
            })
        } else {
            Ok(ShutdownResponse {
                success: false,
                message: Some("Enclave not initialized".to_string()),
            })
        }
    }
}

// TODO: Don't hardcode threads
#[tokio::main(worker_threads = 12)]
async fn main() -> Result<()> {
    // Configure logging
    std::panic::set_hook(Box::new(panic_hook));

    // Initialize VSOCK logger to send logs to the host
    // The host runs the server on VMADDR_CID_HOST (2) port 5555
    let logger_config = VsockLoggerConfig::builder()
        .vsock_addr(
            #[cfg(target_os = "linux")]
            VsockAddr::new(tokio_vsock::VMADDR_CID_HOST, 5555),
            #[cfg(not(target_os = "linux"))]
            std::net::SocketAddr::from(([127, 0, 0, 1], 5555)),
        )
        .batch_size(50)
        .flush_interval(std::time::Duration::from_millis(100))
        .build();

    let vsock_logger = Arc::new(VsockLogger::new(logger_config).await?);

    // Initialize proven-logger with compatibility bridges for tracing
    proven_logger::compat::init_with_bridges(vsock_logger)
        .map_err(|e| Error::LoggerInit(format!("Failed to initialize logger: {e}").into()))?;

    fdlimit::raise_fdlimit();

    // Create the CAC server with our handler
    let handler = EnclaveHandler::new();
    let server = CacServer::new(
        #[cfg(target_os = "linux")]
        VsockAddr::new(VMADDR_CID_ANY, 1024),
        #[cfg(not(target_os = "linux"))]
        std::net::SocketAddr::from(([127, 0, 0, 1], 1024)),
        handler,
    );

    // Start serving
    info!("Starting enclave CAC server on port 1024");
    if let Err(e) = server.serve().await {
        error!("CAC server error: {:?}", e);
    }

    Ok(())
}
