//! Network topology management for Proven Network
//!
//! This crate provides:
//! - Node identity types (NodeId, Node)
//! - Topology management
//! - Governance-based topology discovery

pub mod error;
pub mod manager;
pub mod node;
pub mod node_id;

pub use error::TopologyError;
pub use manager::TopologyManager;
pub use node::Node;
pub use node_id::NodeId;
