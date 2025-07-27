//! Minimal transport abstraction for network communication
//!
//! This crate provides a transport-agnostic interface that focuses purely on
//! moving bytes between nodes. Higher-level concerns like connection management,
//! verification, and message framing are handled by the network layer.

use async_trait::async_trait;
use bytes::Bytes;
use proven_topology::Node;
use std::fmt::Debug;
use thiserror::Error;

/// Transport errors
#[derive(Error, Debug)]
pub enum TransportError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Connection failed: {0}")]
    ConnectionFailed(String),

    #[error("Connection closed")]
    ConnectionClosed,

    #[error("Invalid address: {0}")]
    InvalidAddress(String),

    #[error("Transport-specific error: {0}")]
    Other(String),
}

/// A connection to a remote peer
#[async_trait]
pub trait Connection: Send + Sync + Debug {
    /// Send raw bytes to the peer
    async fn send(&self, data: Bytes) -> Result<(), TransportError>;

    /// Receive raw bytes from the peer
    async fn recv(&self) -> Result<Bytes, TransportError>;

    /// Close the connection
    async fn close(self: Box<Self>) -> Result<(), TransportError>;
}

/// A listener for incoming connections
#[async_trait]
pub trait Listener: Send + Sync + Debug {
    /// Accept an incoming connection
    async fn accept(&self) -> Result<Box<dyn Connection>, TransportError>;

    /// Stop listening
    async fn close(self: Box<Self>) -> Result<(), TransportError>;
}

/// Transport trait for creating connections and listeners
#[async_trait]
pub trait Transport: Send + Sync + Debug + 'static {
    /// Connect to a remote peer
    async fn connect(&self, node: &Node) -> Result<Box<dyn Connection>, TransportError>;

    /// Create a listener based on the transport's configuration
    async fn listen(&self) -> Result<Box<dyn Listener>, TransportError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = TransportError::ConnectionClosed;
        assert_eq!(err.to_string(), "Connection closed");

        let err = TransportError::ConnectionFailed("timeout".to_string());
        assert_eq!(err.to_string(), "Connection failed: timeout");
    }
}
