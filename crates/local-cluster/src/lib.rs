//! Local cluster management for Proven nodes
//!
//! This crate provides programmatic APIs for managing multiple Proven nodes
//! with isolated runtimes, independent topology control, partitioned logging,
//! and multi-user RPC support for integration testing and debugging.

#![warn(missing_docs)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod cluster;
mod config;
mod logging;
mod node;
mod persistence;
mod rpc;

pub mod test_utils;

// Re-export main types
pub use cluster::{ClusterBuilder, LocalCluster};
pub use config::NodeConfig;
pub use logging::{ClusterLogSystem, LogEntry, LogFilter, LogLevel, LogWriter};
pub use node::{ManagedNode, NodeInfo, NodeOperation};
pub use proven_local::NodeStatus;
pub use rpc::{RpcClient, RpcError, UserIdentity};

// Re-export types from dependencies that users will need
pub use proven_topology::NodeSpecialization;
pub use proven_topology_mock::MockTopologyAdaptor;
