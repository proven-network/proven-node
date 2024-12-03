mod error;

use crate::VSOCK_LOG_PORT;
pub use error::{Error, Result};

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tokio_vsock::{VsockAddr, VsockStream};
use tracing::{error, info};

/// Serive for receiving logs from the enclave.
#[derive(Debug, Default)]
pub struct VsockTracingConsumer {
    enclave_cid: u32,
    shutdown_token: CancellationToken,
    task_tracker: TaskTracker,
}

impl VsockTracingConsumer {
    /// Create a new `TracingService`.
    #[must_use]
    pub fn new(enclave_cid: u32) -> Self {
        Self {
            enclave_cid,
            shutdown_token: CancellationToken::new(),
            task_tracker: TaskTracker::new(),
        }
    }

    /// Start the tracing service.
    ///
    /// # Errors
    ///
    /// This function will return an error if the tracing service is already started
    /// or if there is an issue setting the global default subscriber or connecting to the vsock endpoint.
    pub fn start(&self) -> Result<JoinHandle<()>> {
        let shutdown_token = self.shutdown_token.clone();

        let vsock_addr = VsockAddr::new(self.enclave_cid, VSOCK_LOG_PORT);
        info!("connecting to log producer at {}", vsock_addr);

        let handle = self.task_tracker.spawn(async move {
            match VsockStream::connect(vsock_addr).await {
                Ok(mut stream) => {
                    let mut stdout = tokio::io::stdout();

                    tokio::select! {
                        () = shutdown_token.cancelled() => {}
                        _ = tokio::io::copy(&mut stream, &mut stdout) => {}
                    }
                }
                Err(err) => {
                    error!("error connecting: {:?}", err);
                }
            }
        });

        self.task_tracker.close();

        Ok(handle)
    }

    /// Shutdown the tracing service.
    pub async fn shutdown(&self) {
        info!("tracing service shutting down...");

        self.shutdown_token.cancel();
        self.task_tracker.wait().await;

        info!("tracing service shutdown complete.");
    }
}
