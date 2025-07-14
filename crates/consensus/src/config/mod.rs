//! Configuration types for consensus messaging
//!
//! This module provides a centralized location for all configuration-related
//! types and builders for the consensus system.

pub mod cluster;
pub mod consensus;
pub mod global;
pub mod group;
pub mod monitoring;
pub mod storage;
pub mod stream;
pub mod transport;

// Re-export main config types
pub use cluster::ClusterJoinRetryConfig;
pub use consensus::EngineConfig;
pub use global::{GlobalConsensusConfig, GlobalConsensusConfigBuilder};
pub use group::{
    AllocationConfig, AllocationStrategy, GroupConfig, GroupsConfig, MigrationConfig,
    MigrationRateLimit, MigrationRetryConfig, PlacementConstraints, RebalancingConfig,
    ResourceLimits,
};
pub use monitoring::{
    AlertingConfig, GroupMonitoringConfig, HealthCheckConfig, MonitoringConfig,
    MonitoringConfigBuilder, PrometheusConfig, StreamMonitoringConfig,
};
pub use storage::StorageConfig;
pub use stream::{CompressionType, RetentionPolicy, StorageType, StreamConfig};
pub use transport::TransportConfig;

/// Type alias for backward compatibility
pub type LocalConsensusConfig = GroupsConfig;
