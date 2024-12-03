mod error;

use crate::VSOCK_LOG_PORT;
pub use error::{Error, Result};

use tokio::{io::AsyncWriteExt, sync::broadcast};
use tokio_vsock::{VsockAddr, VsockListener, VMADDR_CID_ANY};
use tracing::{error, info};
use tracing_subscriber::{filter, fmt, layer::SubscriberExt, reload};

/// Serive for receiving logs from the enclave.
#[derive(Debug, Default)]
pub struct VsockTracingProducer;

impl VsockTracingProducer {
    /// Start the tracing service.
    ///
    /// # Errors
    ///
    /// This function will return an error if the tracing service is already started
    /// or if there is an issue setting the global default subscriber or binding the vsock listener.
    pub fn start() -> Result<()> {
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

        let mut listener = VsockListener::bind(VsockAddr::new(VMADDR_CID_ANY, VSOCK_LOG_PORT))
            .map_err(|e| Error::Io("failed to bind vsock listener", e))?;

        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((mut stream, addr)) => {
                        info!("new log consumer connected from {}", addr);
                        let mut rx = sender.subscribe();

                        tokio::spawn(async move {
                            while let Ok(msg) = rx.recv().await {
                                if stream.write_all(&msg).await.is_err() {
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

        Ok(())
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
