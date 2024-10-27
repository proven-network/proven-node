pub use crate::error::Result;
use tracing_subscriber::fmt;

pub async fn configure_logging_to_vsock(_vsock_addr: ()) -> Result<()> {
    Ok(())
}

pub struct VsockWriter;

impl VsockWriter {
    pub async fn new(_addr: ()) -> std::io::Result<Self> {
        Ok(Self)
    }
}

impl<'a> fmt::MakeWriter<'a> for VsockWriter {
    type Writer = SenderWriter;

    fn make_writer(&'a self) -> Self::Writer {
        SenderWriter
    }
}

pub struct SenderWriter;

impl std::io::Write for SenderWriter {
    fn write(&mut self, _buf: &[u8]) -> std::io::Result<usize> {
        Ok(_buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
