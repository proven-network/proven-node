//! Provides a tracing subscriber to allow logs to be sent from enclave to host
//! for processing.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod error;

pub use crate::error::{Error, Result};

use tokio::sync::mpsc;
use tokio::{io::AsyncWriteExt, sync::mpsc::error::TrySendError};
use tokio_vsock::{VsockAddr, VsockStream};
use tracing_subscriber::{filter, fmt, layer::SubscriberExt, reload};

/// Sets up a tracing subscriber that sends logs to the host via vsock.
///
/// # Errors
///
/// This function will return an error if it fails to create the vsock writer or set the global default subscriber.
pub async fn configure_logging_to_vsock(vsock_addr: VsockAddr) -> Result<()> {
    let (level_filter, reload_handle) = reload::Layer::new(filter::LevelFilter::TRACE);

    let writer_layer = tracing_subscriber::fmt::Layer::new().with_writer(
        VsockWriter::new(vsock_addr)
            .await
            .map_err(|e| Error::IoError("writer error", e))?,
    );

    let subscriber = tracing_subscriber::registry()
        .with(level_filter)
        .with(fmt::Layer::default())
        .with(writer_layer);

    tracing::subscriber::set_global_default(subscriber)?;

    let _ = reload_handle.modify(|filter| *filter = filter::LevelFilter::INFO);

    Ok(())
}

struct VsockWriter {
    sender: mpsc::Sender<Vec<u8>>,
}

impl VsockWriter {
    async fn new(addr: VsockAddr) -> std::io::Result<Self> {
        let (sender, mut receiver) = mpsc::channel::<Vec<u8>>(100);

        let mut stream = VsockStream::connect(addr).await?;

        tokio::spawn(async move {
            while let Some(msg) = receiver.recv().await {
                // Ignore errors here, as we can't do anything about them.
                let _ = stream.write_all(msg.as_slice()).await;
            }
        });

        Ok(Self { sender })
    }
}

impl<'a> fmt::MakeWriter<'a> for VsockWriter {
    type Writer = SenderWriter;

    fn make_writer(&'a self) -> Self::Writer {
        let sender: mpsc::Sender<Vec<u8>> = self.sender.clone();

        SenderWriter { sender }
    }
}

struct SenderWriter {
    sender: mpsc::Sender<Vec<u8>>,
}

impl std::io::Write for SenderWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self.sender.try_send(buf.to_vec()) {
            Ok(()) => Ok(buf.len()),
            Err(TrySendError::Closed(_)) => Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "mpsc channel closed",
            )),
            Err(TrySendError::Full(_)) => Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "mpsc channel full",
            )),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
