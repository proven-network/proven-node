//! Clean consensus implementation with pluggable transports
//!
//! This crate provides a simplified consensus system where:
//! - Consensus owns all business logic
//! - Transports handle pure networking
//! - No circular dependencies
//! - Single builder pattern for initialization
#![warn(missing_docs)]
#![warn(clippy::all)]

pub mod attestation;
pub mod config;
pub mod consensus;
pub mod cose;
pub mod error;
pub mod global;
pub mod network;
pub mod node;
pub mod node_id;
pub mod pubsub;
pub mod subscription;
pub mod topology;
pub mod transport;
pub mod verification;

// Re-export main types
pub use consensus::Consensus;
pub use error::{ConsensusError, ConsensusResult};
pub use global::{
    ConsensusStorage, GlobalTypeConfig, MemoryConsensusStorage, RocksConsensusStorage,
};
pub use node::Node;
pub use node_id::NodeId;
pub use openraft::Config as RaftConfig;
pub use transport::{HttpIntegratedTransport, NetworkTransport, TransportConfig};

// Re-export config types
pub use config::{ConsensusConfig, StorageConfig};

// Re-export subscription types
pub use subscription::SubscriptionInvoker;

// Re-export topology types
pub use topology::TopologyManager;

// Re-export verification types
pub use verification::{
    ConnectionState, ConnectionVerification, ConnectionVerifier, VerificationMessage,
};
