//! Configuration types for consensus messaging
//!
//! This module provides a centralized location for all configuration-related
//! types and builders for the consensus system.

pub mod cluster;
pub mod consensus;
pub mod hierarchical;
pub mod storage;
pub mod transport;

// Re-export main config types for backward compatibility
pub use cluster::ClusterJoinRetryConfig;
pub use consensus::{ConsensusConfig, ConsensusConfigBuilder};
pub use hierarchical::{
    AlertingConfig, AllocationConfig, GlobalConsensusConfig, GroupConfig,
    HierarchicalConfigBuilder, HierarchicalConsensusConfig, LocalConsensusConfig, MigrationConfig,
    MigrationRateLimit, MigrationRetryConfig, MonitoringConfig, PlacementConstraints,
    PrometheusConfig, RebalancingConfig, ResourceLimits,
};
pub use storage::StorageConfig;
pub use transport::TransportConfig;
