//! Configuration for consensus groups and allocation
//!
//! This module contains configuration types for consensus group management,
//! stream allocation strategies, and group-level settings.

use crate::ConsensusGroupId;
use openraft::Config as RaftConfig;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

/// Configuration for consensus groups
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupsConfig {
    /// Base Raft configuration for groups (can be overridden per group)
    pub base_raft_config: RaftConfig,
    /// Initial number of groups to create
    pub initial_groups: u32,
    /// Minimum number of nodes per group
    pub min_nodes_per_group: usize,
    /// Maximum number of nodes per group
    pub max_nodes_per_group: usize,
    /// Maximum streams per group (soft limit for load balancing)
    pub max_streams_per_group: u32,
    /// Per-group configuration overrides
    pub group_overrides: HashMap<ConsensusGroupId, GroupConfig>,
}

impl Default for GroupsConfig {
    fn default() -> Self {
        Self {
            base_raft_config: RaftConfig::default(),
            initial_groups: 1,
            min_nodes_per_group: 1,
            max_nodes_per_group: 5,
            max_streams_per_group: 1000,
            group_overrides: HashMap::new(),
        }
    }
}

/// Configuration for a specific consensus group
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct GroupConfig {
    /// Optional Raft config override for this group
    pub raft_config_override: Option<RaftConfig>,
    /// Preferred node IDs for this group
    pub preferred_nodes: Vec<u64>,
    /// Special purpose tag (e.g., "high-throughput", "archival")
    pub purpose_tag: Option<String>,
    /// Resource limits for this group
    pub resource_limits: ResourceLimits,
}

/// Resource limits for a consensus group
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceLimits {
    /// Maximum storage size in bytes
    pub max_storage_bytes: u64,
    /// Maximum message rate (messages/second)
    pub max_message_rate: f64,
    /// Maximum memory usage in bytes
    pub max_memory_bytes: u64,
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            max_storage_bytes: 10 * 1024 * 1024 * 1024, // 10GB
            max_message_rate: 10000.0,
            max_memory_bytes: 1024 * 1024 * 1024, // 1GB
        }
    }
}

/// Configuration for stream allocation
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct AllocationConfig {
    /// Allocation strategy to use
    pub strategy: AllocationStrategy,
    /// Rebalancing configuration
    pub rebalancing: RebalancingConfig,
    /// Stream placement constraints
    pub placement_constraints: PlacementConstraints,
}

/// Allocation strategy for mapping streams to consensus groups
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AllocationStrategy {
    /// Consistent hashing based on stream name
    ConsistentHash {
        /// The number of virtual nodes to use for consistent hashing
        virtual_nodes: u32,
    },
    /// Round-robin allocation
    RoundRobin,
    /// Manual assignment
    Manual,
    /// Load-based allocation (assigns to least loaded group)
    LoadBased,
}

impl Default for AllocationStrategy {
    fn default() -> Self {
        Self::ConsistentHash { virtual_nodes: 150 }
    }
}

/// Configuration for automatic rebalancing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RebalancingConfig {
    /// Enable automatic rebalancing
    pub enabled: bool,
    /// Check interval for rebalancing needs
    pub check_interval: Duration,
    /// Maximum load imbalance ratio before triggering rebalance
    pub max_imbalance_ratio: f64,
    /// Minimum time between rebalancing operations
    pub cooldown_period: Duration,
    /// Maximum number of concurrent migrations
    pub max_concurrent_migrations: usize,
}

impl Default for RebalancingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            check_interval: Duration::from_secs(300), // 5 minutes
            max_imbalance_ratio: 1.5,
            cooldown_period: Duration::from_secs(3600), // 1 hour
            max_concurrent_migrations: 3,
        }
    }
}

/// Placement constraints for streams
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct PlacementConstraints {
    /// Streams that must be co-located in the same group
    pub affinity_groups: Vec<Vec<String>>,
    /// Streams that must not be in the same group
    pub anti_affinity_groups: Vec<Vec<String>>,
    /// Preferred groups for specific streams
    pub stream_preferences: HashMap<String, ConsensusGroupId>,
    /// Groups to avoid for specific streams
    pub stream_exclusions: HashMap<String, Vec<ConsensusGroupId>>,
}

/// Configuration for stream migration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationConfig {
    /// Maximum time allowed for a migration
    pub timeout: Duration,
    /// Batch size for transferring messages during migration
    pub batch_size: usize,
    /// Maximum bandwidth to use for migration (bytes/second)
    pub max_bandwidth: Option<u64>,
    /// Retry configuration for failed migrations
    pub retry_config: MigrationRetryConfig,
    /// Migration rate limiting
    pub rate_limit: MigrationRateLimit,
}

impl Default for MigrationConfig {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(300), // 5 minutes
            batch_size: 1000,
            max_bandwidth: None,
            retry_config: MigrationRetryConfig::default(),
            rate_limit: MigrationRateLimit::default(),
        }
    }
}

/// Retry configuration for migrations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationRetryConfig {
    /// Maximum number of retry attempts
    pub max_attempts: usize,
    /// Initial backoff delay
    pub initial_delay: Duration,
    /// Maximum backoff delay
    pub max_delay: Duration,
    /// Backoff multiplier
    pub backoff_multiplier: f64,
}

impl Default for MigrationRetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_delay: Duration::from_secs(5),
            max_delay: Duration::from_secs(60),
            backoff_multiplier: 2.0,
        }
    }
}

/// Rate limiting for migrations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationRateLimit {
    /// Maximum migrations per hour
    pub max_per_hour: usize,
    /// Maximum data transferred per hour (bytes)
    pub max_bytes_per_hour: u64,
    /// Minimum interval between migrations of the same stream
    pub min_stream_migration_interval: Duration,
}

impl Default for MigrationRateLimit {
    fn default() -> Self {
        Self {
            max_per_hour: 10,
            max_bytes_per_hour: 100 * 1024 * 1024 * 1024, // 100GB
            min_stream_migration_interval: Duration::from_secs(3600), // 1 hour
        }
    }
}
