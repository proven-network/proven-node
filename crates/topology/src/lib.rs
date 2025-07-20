//! Network topology management for Proven Network
//!
//! This crate provides:
//! - Node identity types (NodeId, Node)
//! - Topology management
//! - Topology adaptor interface (formerly Governance)
//! - Version and specialization types

pub mod adaptor;
pub mod error;
pub mod manager;
pub mod node;
pub mod node_id;
pub mod specialization;
mod subscriber;
pub mod version;

pub use adaptor::TopologyAdaptor;
pub use error::{TopologyAdaptorError, TopologyAdaptorErrorKind, TopologyError};
pub use manager::{TopologyManager, TopologyManagerConfig};
pub use node::Node;
pub use node_id::NodeId;
pub use specialization::NodeSpecialization;
pub use subscriber::TopologySubscription;
pub use version::Version;
