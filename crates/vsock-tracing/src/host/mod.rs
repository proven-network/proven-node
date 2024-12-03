mod error;

pub use error::{Error, Result};

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tokio_vsock::{VsockAddr, VsockListener, VMADDR_CID_ANY};
use tracing::{error, info, Level};
use tracing_subscriber::FmtSubscriber;

/// Serive for receiving logs from the enclave.
#[derive(Debug, Default)]
pub struct TracingService {
    shutdown_token: CancellationToken,
    task_tracker: TaskTracker,
}

impl TracingService {
    /// Create a new `TracingService`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            shutdown_token: CancellationToken::new(),
            task_tracker: TaskTracker::new(),
        }
    }

    /// Start the tracing service.
    ///
    /// # Errors
    ///
    /// This function will return an error if the tracing service is already started
    /// or if there is an issue setting the global default subscriber or binding the vsock listener.
    pub fn start(&self, log_port: u32) -> Result<JoinHandle<()>> {
        if self.task_tracker.is_closed() {
            return Err(Error::AlreadyStarted);
        }

        tracing::subscriber::set_global_default(
            FmtSubscriber::builder()
                .with_max_level(Level::TRACE)
                .finish(),
        )?;

        let shutdown_token = self.shutdown_token.clone();
        let mut vsock = VsockListener::bind(VsockAddr::new(VMADDR_CID_ANY, log_port))
            .map_err(|e| Error::Io("failed to bind vsock listener", e))?;

        let handle = self.task_tracker.spawn(async move {
            match vsock.accept().await {
                Ok((mut stream, addr)) => {
                    info!("accepted log connection from {}", addr);

                    let mut stdout = tokio::io::stdout();

                    tokio::select! {
                        () = shutdown_token.cancelled() => {}
                        _ = tokio::io::copy(&mut stream, &mut stdout) => {}
                    }
                }
                Err(err) => {
                    error!("error accepting connection: {:?}", err);
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
