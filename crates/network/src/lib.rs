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

pub mod bootable;
pub mod error;
pub mod handler;
pub mod handler_builder;
pub mod manager;
pub mod message;
pub mod peer;

// Re-export commonly used types
pub use bootable::BootableNetworkManager;
pub use error::{NetworkError, NetworkResult};
pub use handler::{HandlerRegistry, HandlerResult};
pub use handler_builder::{HandlerBuilderExt, HandlerContext};
pub use manager::NetworkManager;
pub use message::{HandledMessage, NetworkEnvelope, NetworkMessage, SerializableMessage};
pub use peer::Peer;

// Re-export dependencies that are part of our public API
pub use proven_topology::{Node, NodeId, TopologyManager};
pub use proven_transport::Transport;
