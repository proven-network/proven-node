//! Clean consensus implementation with pluggable transports
//!
//! This crate provides a simplified consensus system where:
//! - Consensus owns all business logic
//! - Transports handle pure networking
//! - No circular dependencies
//! - Single builder pattern for initialization
#![warn(missing_docs)]
#![warn(clippy::all)]
#![allow(clippy::result_large_err)]

/// Stream allocation and consensus group management
pub mod allocation;
// pub mod cluster_discovery;
pub mod config;
pub mod consensus;
pub mod error;
pub mod global;
/// Group allocation algorithm for managing consensus groups
pub mod group_allocator;
/// Local consensus management
pub mod local;
/// Stream migration protocol
pub mod migration;
/// Monitoring and metrics
pub mod monitoring;
pub mod network;
pub mod node;
pub mod node_id;
/// New categorized operations
pub mod operations;
/// Hierarchical consensus orchestrator
pub mod orchestrator;
pub mod pubsub;
/// Consensus routing
pub mod router;
/// Storage adaptor layer for different backends
pub mod storage;
pub mod subscription;
pub mod topology;
/// Read-only views for orchestrator decision-making
pub mod views;

// Re-export main types
pub use consensus::Consensus;
pub use error::{ConsensusResult, Error};
pub use global::{ConsensusStorage, GlobalTypeConfig};
pub use node::Node;
pub use node_id::NodeId;
pub use openraft::Config as RaftConfig;

// Re-export config types
pub use config::{
    ClusterJoinRetryConfig, ConsensusConfig, ConsensusConfigBuilder, HierarchicalConsensusConfig,
    StorageConfig, TransportConfig,
};

// Re-export subscription types
pub use subscription::SubscriptionInvoker;

// Re-export topology types
pub use topology::TopologyManager;
