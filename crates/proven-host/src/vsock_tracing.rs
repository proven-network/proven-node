use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tokio_vsock::{VsockAddr, VsockListener, VMADDR_CID_ANY};
use tracing::{error, info, Level};
use tracing_subscriber::FmtSubscriber;

pub struct TracingService {
    shutdown_token: CancellationToken,
    task_tracker: TaskTracker,
}

impl TracingService {
    pub fn new() -> Self {
        Self {
            shutdown_token: CancellationToken::new(),
            task_tracker: TaskTracker::new(),
        }
    }

    pub fn start(&self, log_port: u32) -> Result<JoinHandle<()>, TracingServiceError> {
        if self.task_tracker.is_closed() {
            return Err(TracingServiceError::AlreadyStarted);
        }

        tracing::subscriber::set_global_default(
            FmtSubscriber::builder()
                .with_max_level(Level::TRACE)
                .finish(),
        )
        .unwrap();

        let shutdown_token = self.shutdown_token.clone();
        let handle = self.task_tracker.spawn(async move {
            let mut vsock = VsockListener::bind(VsockAddr::new(VMADDR_CID_ANY, log_port)).unwrap();
            match vsock.accept().await {
                Ok((mut stream, addr)) => {
                    info!("accepted log connection from {}", addr);

                    let mut stdout = tokio::io::stdout();

                    tokio::select! {
                        _ = shutdown_token.cancelled() => {}
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

    pub async fn shutdown(&self) {
        info!("tracing service shutting down...");

        self.shutdown_token.cancel();
        self.task_tracker.wait().await;

        info!("tracing service shutdown complete.");
    }
}

#[derive(Debug)]
pub enum TracingServiceError {
    AlreadyStarted,
}

impl std::fmt::Display for TracingServiceError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            TracingServiceError::AlreadyStarted => write!(f, "racing service already started"),
        }
    }
}

impl std::error::Error for TracingServiceError {}
