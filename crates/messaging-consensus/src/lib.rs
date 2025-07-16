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

/// Clients send requests to services in the consensus network.
pub mod client;

/// Consumers are stateful views of consensus streams.
pub mod consumer;

/// Error types for the messaging-consensus system.
pub mod error;

/// Services are special consumers that respond to requests in the consensus network.
pub mod service;

/// Service responders handle responses in the consensus network.
pub mod service_responder;

/// Consensus-based streams with immediate consistency.
pub mod stream;

/// Subjects in the consensus messaging system.
pub mod subject;

/// Subscription management in the consensus network.
pub mod subscription;

/// Subscription responders handle subscription responses.
pub mod subscription_responder;

// Re-export consensus types for convenience
pub use proven_engine::{Consensus, ConsensusConfig, GlobalTypeConfig, TopologyManager};

// Re-export messaging-specific error types
pub use error::MessagingConsensusError;

// Re-export messaging-specific types
pub use client::ConsensusClient;
pub use consumer::{ConsensusConsumer, ConsensusConsumerError, ConsensusConsumerOptions};
pub use service::{ConsensusService, ConsensusServiceError, ConsensusServiceOptions};
pub use service_responder::{ConsensusServiceResponder, ConsensusUsedResponder};
pub use stream::{ConsensusStream, ConsensusStreamOptions, InitializedConsensusStream};
pub use subject::ConsensusSubject;
pub use subscription::{ConsensusSubscription, ConsensusSubscriptionOptions};
pub use subscription_responder::{
    ConsensusSubscriptionResponder, ConsensusUsedSubscriptionResponder,
};
