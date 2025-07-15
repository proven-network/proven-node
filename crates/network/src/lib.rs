//! Network layer for proven-node
//!
//! This crate provides a generic network communication layer that handles:
//! - Message routing and correlation
//! - Request/response patterns
//! - Message signing and verification (via COSE)
//! - Peer management
//!
//! The network layer is transport-agnostic and works with any implementation
//! of the `proven_transport::Transport` trait.

pub mod error;
pub mod handler;
pub mod manager;
pub mod message;
pub mod namespace;
pub mod peer;

// Re-export commonly used types
pub use error::{NetworkError, NetworkResult};
pub use handler::{HandlerRegistry, HandlerResult};
pub use manager::NetworkManager;
pub use message::{HandledMessage, NetworkEnvelope, NetworkMessage};
pub use peer::Peer;
