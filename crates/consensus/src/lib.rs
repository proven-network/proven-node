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
// TODO: Remove this soon
#![allow(dead_code)]

// Core modules
/// Consensus client API
pub mod client;
/// Core consensus functionality
pub(crate) mod core;
/// Network transport and messaging
pub(crate) mod network;
/// Consensus operations
pub(crate) mod operations;
/// PubSub messaging
pub(crate) mod pubsub;
/// Storage backends
pub(crate) mod storage_backends;

// Infrastructure modules
/// Engine builder
pub mod builder;
/// Configuration types
pub mod config;
/// Error types
pub mod error;
/// Group ID type
pub mod group_id;

// Re-export main types
pub use client::ConsensusClient;
pub use core::engine::Engine;
pub use error::{ConsensusResult, Error};
pub use openraft::Config as RaftConfig;
pub use proven_topology::{Node, NodeId};

pub use builder::EngineBuilder;

// Re-export config types
pub use config::{
    AllocationConfig, ClusterJoinRetryConfig, EngineConfig, GlobalConsensusConfig, GroupsConfig,
    MigrationConfig, MonitoringConfig, StorageConfig, TransportConfig,
};
pub use group_id::ConsensusGroupId;
