mod error;

use crate::VSOCK_LOG_PORT;
pub use error::{Error, Result};

use tokio::task::JoinHandle;
use tokio::{io::AsyncWriteExt, sync::broadcast};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tokio_vsock::{VsockAddr, VsockListener, VMADDR_CID_ANY};
use tracing::{error, info};
use tracing_subscriber::{filter, fmt, layer::SubscriberExt, reload};

/// Serive for receiving logs from the enclave.
#[derive(Debug, Default)]
pub struct VsockTracingProducer {
    shutdown_token: CancellationToken,
    task_tracker: TaskTracker,
}

impl VsockTracingProducer {
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
    pub fn start(&self) -> Result<JoinHandle<()>> {
        if self.task_tracker.is_closed() {
            return Err(Error::AlreadyStarted);
        }

        let (sender, _) = broadcast::channel::<Vec<u8>>(100);

        let (level_filter, reload_handle) = reload::Layer::new(filter::LevelFilter::TRACE);

        let writer_layer =
            tracing_subscriber::fmt::Layer::new().with_writer(VsockWriter::new(sender.clone()));

        let subscriber = tracing_subscriber::registry()
            .with(level_filter)
            .with(fmt::Layer::default())
            .with(writer_layer);

        tracing::subscriber::set_global_default(subscriber)?;

        let _ = reload_handle.modify(|filter| *filter = filter::LevelFilter::INFO);

        let shutdown_token = self.shutdown_token.clone();
        let mut listener = VsockListener::bind(VsockAddr::new(VMADDR_CID_ANY, VSOCK_LOG_PORT))
            .map_err(|e| Error::Io("failed to bind vsock listener", e))?;

        let handle = self.task_tracker.spawn(async move {
            loop {
                if shutdown_token.is_cancelled() {
                    break;
                }

                match listener.accept().await {
                    Ok((mut stream, addr)) => {
                        info!("new log consumer connected from {}", addr);
                        let mut rx = sender.subscribe();

                        let shutdown_token = shutdown_token.clone();
                        tokio::spawn(async move {
                            while let Ok(msg) = rx.recv().await {
                                if shutdown_token.is_cancelled()
                                    || stream.write_all(&msg).await.is_err()
                                {
                                    break;
                                }
                            }
                        });
                    }
                    Err(e) => {
                        error!("failed to accept connection: {}", e);
                    }
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

struct VsockWriter {
    sender: broadcast::Sender<Vec<u8>>,
}

impl VsockWriter {
    pub const fn new(sender: broadcast::Sender<Vec<u8>>) -> Self {
        Self { sender }
    }
}

impl<'a> fmt::MakeWriter<'a> for VsockWriter {
    type Writer = SenderWriter;

    fn make_writer(&'a self) -> Self::Writer {
        let sender: broadcast::Sender<Vec<u8>> = self.sender.clone();

        SenderWriter { sender }
    }
}

struct SenderWriter {
    sender: broadcast::Sender<Vec<u8>>,
}

impl std::io::Write for SenderWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self.sender.send(buf.to_vec()) {
            Ok(_) => Ok(buf.len()),
            Err(_) => Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "no active subscribers",
            )),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
