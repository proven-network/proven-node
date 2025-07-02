//! Consensus-based messaging implementation with immediate consistency across topology nodes.
//!
//! This implementation provides strong consistency guarantees by:
//! - Using the governance system to discover network topology
//! - Implementing synchronous replication to all healthy nodes
//! - Providing automatic catch-up for nodes that reconnect
//! - Using a Raft consensus algorithm for ordering and leadership
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

/// Attestation verification for consensus peers.
pub mod attestation;

/// Clients send requests to services in the consensus network.
pub mod client;

/// Consumers are stateful views of consensus streams.
pub mod consumer;

/// Core consensus protocol and node management.
pub mod consensus;

/// Top-level consensus system manager.
pub mod consensus_manager;

/// COSE (CBOR Object Signing and Encryption) support for secure messaging.
pub mod cose;

/// Error types for the consensus messaging system.
pub mod error;

/// Peer-to-peer networking for consensus nodes.
pub mod network;

/// OpenRaft network implementation for consensus messaging.
pub mod raft_network;

/// OpenRaft state machine implementation for consensus messaging.
pub mod raft_state_machine;

/// Services are special consumers that respond to requests in the consensus network.
pub mod service;

/// Service responders handle responses in the consensus network.
pub mod service_responder;

/// `OpenRaft` storage implementations for consensus.
pub mod storage;

/// Consensus-based streams with immediate consistency.
pub mod stream;

/// Subjects in the consensus messaging system.
pub mod subject;

/// Subscription management in the consensus network.
pub mod subscription;

/// Subscription responders handle subscription responses.
pub mod subscription_responder;

/// Network topology management and peer discovery.
pub mod topology;

pub use error::{ConsensusError, ConsensusResult};

// Re-export key types for easier integration
pub use consensus::{ConsensusConfig, ConsensusManager, TypeConfig};
pub use consensus_manager::{Consensus, PersistenceMode, StreamConfig};
pub use network::ConsensusNetwork;
pub use raft_network::{ConsensusRaftNetwork, ConsensusRaftNetworkFactory, RaftMessage};
pub use raft_state_machine::{
    ConsensusSnapshotBuilder, ConsensusStateMachine, StateMachineSnapshot,
};
pub use storage::{create_messaging_storage, MessagingStorage};
pub use stream::{ConsensusStream, ConsensusStreamOptions, InitializedConsensusStream};
pub use topology::{PeerInfo, TopologyManager};
