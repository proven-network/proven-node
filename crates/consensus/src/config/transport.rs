//! Transport configuration types

use std::net::SocketAddr;

/// Configuration for different transport types
#[derive(Debug, Clone)]
pub enum TransportConfig {
    /// TCP transport configuration
    Tcp {
        /// Address to listen on
        listen_addr: SocketAddr,
    },
    /// WebSocket transport configuration
    WebSocket,
}

impl TransportConfig {
    /// Check if this transport supports HTTP integration
    pub fn supports_http(&self) -> bool {
        matches!(self, TransportConfig::WebSocket)
    }
}
