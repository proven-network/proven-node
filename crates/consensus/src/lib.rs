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
pub mod network;
pub mod state_machine;
pub mod storage;
pub mod subject;
pub mod subscription;
pub mod topology;
pub mod transport;
pub mod types;
pub mod verification;

// Re-export main types
pub use consensus::Consensus;
pub use error::{ConsensusError, ConsensusResult};
pub use openraft::Config as RaftConfig;
pub use storage::{ConsensusStorage, MemoryConsensusStorage, RocksConsensusStorage};
pub use transport::{HttpIntegratedTransport, NetworkTransport, TransportConfig};
pub use types::TypeConfig;

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
