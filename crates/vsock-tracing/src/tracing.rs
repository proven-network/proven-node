pub use crate::error::Result;

use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio_vsock::{VsockAddr, VsockStream};
use tracing_subscriber::{filter, fmt, layer::SubscriberExt, reload};

pub async fn configure_logging_to_vsock(vsock_addr: VsockAddr) -> Result<()> {
    let (level_filter, reload_handle) = reload::Layer::new(filter::LevelFilter::TRACE);

    let writer_layer =
        tracing_subscriber::fmt::Layer::new().with_writer(VsockWriter::new(vsock_addr).await?);

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
                if let Err(e) = stream.write_all(msg.as_slice()).await {
                    eprintln!("Failed to write to vsock: {}", e);
                }
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

pub struct SenderWriter {
    sender: mpsc::Sender<Vec<u8>>,
}

impl std::io::Write for SenderWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self.sender.try_send(buf.to_vec()) {
            Ok(_) => Ok(buf.len()),
            Err(_) => Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Failed to send message",
            )),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
