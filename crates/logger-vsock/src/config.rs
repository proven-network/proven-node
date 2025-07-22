//! Configuration for VSOCK logger

use crate::messages::LogLevel;
use std::time::Duration;

/// Configuration for the VSOCK logger client
#[derive(Debug, Clone)]
pub struct VsockLoggerConfig {
    /// VSOCK address to connect to
    #[cfg(target_os = "linux")]
    pub vsock_addr: tokio_vsock::VsockAddr,

    /// TCP address for non-Linux platforms (for testing)
    #[cfg(not(target_os = "linux"))]
    pub vsock_addr: std::net::SocketAddr,

    /// Maximum number of logs to batch before sending
    pub batch_size: usize,

    /// Maximum time to wait before flushing a batch
    pub batch_interval: Duration,

    /// Buffer size for the log channel
    pub channel_buffer_size: usize,

    /// Whether to drop logs if the channel is full
    pub drop_on_full: bool,

    /// Minimum log level to send
    pub min_level: LogLevel,
}

impl Default for VsockLoggerConfig {
    fn default() -> Self {
        Self {
            #[cfg(target_os = "linux")]
            vsock_addr: tokio_vsock::VsockAddr::new(
                tokio_vsock::VMADDR_CID_HOST, // Send to host
                5555,                         // Default logging port
            ),
            #[cfg(not(target_os = "linux"))]
            vsock_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 5555)),
            batch_size: 100,
            batch_interval: Duration::from_millis(100),
            channel_buffer_size: 10000,
            drop_on_full: true,
            min_level: LogLevel::Info,
        }
    }
}

impl VsockLoggerConfig {
    /// Create a new builder for configuring the logger
    pub fn builder() -> VsockLoggerConfigBuilder {
        VsockLoggerConfigBuilder::default()
    }
}

/// Builder for VsockLoggerConfig
#[derive(Debug, Default)]
pub struct VsockLoggerConfigBuilder {
    config: VsockLoggerConfig,
}

impl VsockLoggerConfigBuilder {
    /// Set the VSOCK address
    pub fn vsock_addr(
        mut self,
        #[cfg(target_os = "linux")] addr: tokio_vsock::VsockAddr,
        #[cfg(not(target_os = "linux"))] addr: std::net::SocketAddr,
    ) -> Self {
        self.config.vsock_addr = addr;
        self
    }

    /// Set the batch size
    pub fn batch_size(mut self, size: usize) -> Self {
        self.config.batch_size = size;
        self
    }

    /// Set the batch interval (was flush_interval)
    pub fn batch_interval(mut self, interval: Duration) -> Self {
        self.config.batch_interval = interval;
        self
    }

    /// Set the channel buffer size
    pub fn channel_buffer_size(mut self, size: usize) -> Self {
        self.config.channel_buffer_size = size;
        self
    }

    /// Set whether to drop logs on full channel
    pub fn drop_on_full(mut self, drop: bool) -> Self {
        self.config.drop_on_full = drop;
        self
    }

    /// Set the minimum log level
    pub fn min_level(mut self, level: LogLevel) -> Self {
        self.config.min_level = level;
        self
    }

    /// Build the configuration
    pub fn build(self) -> VsockLoggerConfig {
        self.config
    }
}
