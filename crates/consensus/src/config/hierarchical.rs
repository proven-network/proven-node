//! Configuration for hierarchical consensus system
//!
//! This module provides configuration options for the hierarchical consensus
//! architecture, including settings for global consensus, local consensus groups,
//! stream allocation, and migration policies.

use super::storage::StorageConfig;
use crate::allocation::{AllocationStrategy, ConsensusGroupId};
use openraft::Config as RaftConfig;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

/// Configuration for the hierarchical consensus system
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct HierarchicalConsensusConfig {
    /// Global consensus configuration
    pub global: GlobalConsensusConfig,
    /// Local consensus configuration
    pub local: LocalConsensusConfig,
    /// Stream allocation configuration
    pub allocation: AllocationConfig,
    /// Migration configuration
    pub migration: MigrationConfig,
    /// Monitoring configuration
    pub monitoring: MonitoringConfig,
}

/// Configuration for the global consensus layer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalConsensusConfig {
    /// Raft configuration for global consensus
    pub raft_config: RaftConfig,
    /// Maximum number of streams the system can handle
    pub max_streams: usize,
    /// Maximum number of local consensus groups
    pub max_groups: u32,
    /// Interval for health checks on local groups
    pub health_check_interval: Duration,
}

/// Configuration for local consensus groups
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalConsensusConfig {
    /// Base Raft configuration for local groups (can be overridden per group)
    pub base_raft_config: RaftConfig,
    /// Initial number of local groups to create
    pub initial_groups: u32,
    /// Minimum number of nodes per group
    pub min_nodes_per_group: usize,
    /// Maximum number of nodes per group
    pub max_nodes_per_group: usize,
    /// Maximum streams per group (soft limit for load balancing)
    pub max_streams_per_group: u32,
    /// Storage configuration for local groups
    pub storage_config: StorageConfig,
    /// Per-group configuration overrides
    pub group_overrides: HashMap<ConsensusGroupId, GroupConfig>,
}

/// Configuration for a specific consensus group
#[derive(Debug, Clone, Serialize, Deserialize)]
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

/// Configuration for stream allocation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AllocationConfig {
    /// Allocation strategy to use
    pub strategy: AllocationStrategy,
    /// Rebalancing configuration
    pub rebalancing: RebalancingConfig,
    /// Stream placement constraints
    pub placement_constraints: PlacementConstraints,
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

/// Configuration for monitoring and metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonitoringConfig {
    /// Enable monitoring
    pub enabled: bool,
    /// Metrics update interval
    pub update_interval: Duration,
    /// Prometheus endpoint configuration
    pub prometheus: PrometheusConfig,
    /// Alerting configuration
    pub alerting: AlertingConfig,
}

/// Prometheus configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrometheusConfig {
    /// Enable Prometheus metrics export
    pub enabled: bool,
    /// HTTP endpoint for metrics
    pub endpoint: String,
    /// Port for metrics server
    pub port: u16,
}

/// Alerting configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertingConfig {
    /// Enable alerting
    pub enabled: bool,
    /// Alert when load imbalance exceeds this ratio
    pub imbalance_threshold: f64,
    /// Alert when migration failure rate exceeds this percentage
    pub migration_failure_threshold: f64,
    /// Alert when a group is unhealthy for this duration
    pub unhealthy_duration_threshold: Duration,
    /// Webhook URL for sending alerts
    pub webhook_url: Option<String>,
}

impl Default for GlobalConsensusConfig {
    fn default() -> Self {
        Self {
            raft_config: RaftConfig::default(),
            max_streams: 100_000,
            max_groups: 100,
            health_check_interval: Duration::from_secs(30),
        }
    }
}

impl Default for LocalConsensusConfig {
    fn default() -> Self {
        // Optimize for local consensus (faster timeouts)
        let base_config = RaftConfig {
            heartbeat_interval: 100,
            election_timeout_min: 200,
            election_timeout_max: 400,
            ..Default::default()
        };

        Self {
            base_raft_config: base_config,
            initial_groups: 3,
            min_nodes_per_group: 3,
            max_nodes_per_group: 7,
            max_streams_per_group: 1000,
            storage_config: StorageConfig::Memory,
            group_overrides: HashMap::new(),
        }
    }
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            max_storage_bytes: 10 * 1024 * 1024 * 1024, // 10GB
            max_message_rate: 10_000.0,
            max_memory_bytes: 1024 * 1024 * 1024, // 1GB
        }
    }
}

impl Default for AllocationConfig {
    fn default() -> Self {
        Self {
            strategy: AllocationStrategy::ConsistentHash { virtual_nodes: 150 },
            rebalancing: RebalancingConfig::default(),
            placement_constraints: PlacementConstraints::default(),
        }
    }
}

impl Default for RebalancingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            check_interval: Duration::from_secs(300), // 5 minutes
            max_imbalance_ratio: 2.0,
            cooldown_period: Duration::from_secs(600), // 10 minutes
            max_concurrent_migrations: 5,
        }
    }
}

impl Default for MigrationConfig {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(300), // 5 minutes
            batch_size: 1000,
            max_bandwidth: Some(100 * 1024 * 1024), // 100 MB/s
            retry_config: MigrationRetryConfig::default(),
            rate_limit: MigrationRateLimit::default(),
        }
    }
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

impl Default for MigrationRateLimit {
    fn default() -> Self {
        Self {
            max_per_hour: 100,
            max_bytes_per_hour: 100 * 1024 * 1024 * 1024, // 100GB
            min_stream_migration_interval: Duration::from_secs(3600), // 1 hour
        }
    }
}

impl Default for MonitoringConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            update_interval: Duration::from_secs(60),
            prometheus: PrometheusConfig::default(),
            alerting: AlertingConfig::default(),
        }
    }
}

impl Default for PrometheusConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            endpoint: "/metrics".to_string(),
            port: 9090,
        }
    }
}

impl Default for AlertingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            imbalance_threshold: 3.0,
            migration_failure_threshold: 0.2, // 20%
            unhealthy_duration_threshold: Duration::from_secs(300), // 5 minutes
            webhook_url: None,
        }
    }
}

/// Builder for hierarchical consensus configuration
pub struct HierarchicalConfigBuilder {
    config: HierarchicalConsensusConfig,
}

impl HierarchicalConfigBuilder {
    /// Create a new builder with default configuration
    pub fn new() -> Self {
        Self {
            config: HierarchicalConsensusConfig::default(),
        }
    }

    /// Set global consensus configuration
    pub fn global(mut self, global: GlobalConsensusConfig) -> Self {
        self.config.global = global;
        self
    }

    /// Set local consensus configuration
    pub fn local(mut self, local: LocalConsensusConfig) -> Self {
        self.config.local = local;
        self
    }

    /// Set allocation configuration
    pub fn allocation(mut self, allocation: AllocationConfig) -> Self {
        self.config.allocation = allocation;
        self
    }

    /// Set migration configuration
    pub fn migration(mut self, migration: MigrationConfig) -> Self {
        self.config.migration = migration;
        self
    }

    /// Set monitoring configuration
    pub fn monitoring(mut self, monitoring: MonitoringConfig) -> Self {
        self.config.monitoring = monitoring;
        self
    }

    /// Build the configuration
    pub fn build(self) -> HierarchicalConsensusConfig {
        self.config
    }
}

impl Default for HierarchicalConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = HierarchicalConsensusConfig::default();

        assert_eq!(config.global.max_streams, 100_000);
        assert_eq!(config.local.initial_groups, 3);
        assert!(config.allocation.rebalancing.enabled);
        assert_eq!(config.migration.batch_size, 1000);
        assert!(config.monitoring.enabled);
    }

    #[test]
    fn test_hierarchical_config_defaults() {
        let config = HierarchicalConsensusConfig::default();

        // Test global defaults
        assert_eq!(config.global.max_streams, 100_000);
        assert_eq!(config.global.max_groups, 100);
        assert_eq!(config.global.health_check_interval, Duration::from_secs(30));

        // Test local defaults
        assert_eq!(config.local.initial_groups, 3);
        assert_eq!(config.local.min_nodes_per_group, 3);
        assert_eq!(config.local.max_nodes_per_group, 7);
        assert_eq!(config.local.max_streams_per_group, 1000);

        // Test allocation defaults
        match config.allocation.strategy {
            AllocationStrategy::ConsistentHash { virtual_nodes } => {
                assert_eq!(virtual_nodes, 150);
            }
            _ => panic!("Expected ConsistentHash strategy"),
        }
        assert!(config.allocation.rebalancing.enabled);
        assert_eq!(config.allocation.rebalancing.max_imbalance_ratio, 2.0);

        // Test migration defaults
        assert_eq!(config.migration.timeout, Duration::from_secs(300));
        assert_eq!(config.migration.batch_size, 1000);
        assert_eq!(config.migration.max_bandwidth, Some(100 * 1024 * 1024));

        // Test monitoring defaults
        assert!(config.monitoring.enabled);
        assert_eq!(config.monitoring.update_interval, Duration::from_secs(60));
        assert!(config.monitoring.prometheus.enabled);
        assert!(!config.monitoring.alerting.enabled);
    }

    #[test]
    fn test_config_builder() {
        let config = HierarchicalConfigBuilder::new()
            .global(GlobalConsensusConfig {
                max_streams: 50_000,
                max_groups: 50,
                ..Default::default()
            })
            .local(LocalConsensusConfig {
                initial_groups: 5,
                max_streams_per_group: 500,
                ..Default::default()
            })
            .allocation(AllocationConfig {
                strategy: AllocationStrategy::RoundRobin,
                rebalancing: RebalancingConfig {
                    enabled: false,
                    ..Default::default()
                },
                ..Default::default()
            })
            .monitoring(MonitoringConfig {
                enabled: true,
                update_interval: Duration::from_secs(30),
                ..Default::default()
            })
            .build();

        assert_eq!(config.global.max_streams, 50_000);
        assert_eq!(config.global.max_groups, 50);
        assert_eq!(config.local.initial_groups, 5);
        assert_eq!(config.local.max_streams_per_group, 500);
        assert!(!config.allocation.rebalancing.enabled);
        assert_eq!(config.monitoring.update_interval, Duration::from_secs(30));
    }

    #[test]
    fn test_serialization() {
        let config = HierarchicalConsensusConfig::default();

        // Serialize to JSON
        let json = serde_json::to_string_pretty(&config).unwrap();

        // Deserialize back
        let deserialized: HierarchicalConsensusConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.global.max_streams, config.global.max_streams);
        assert_eq!(
            deserialized.local.initial_groups,
            config.local.initial_groups
        );
    }

    #[test]
    fn test_json_serialization() {
        let config = HierarchicalConsensusConfig::default();

        // Serialize to JSON
        let json = serde_json::to_string_pretty(&config).unwrap();
        println!("Config JSON:\n{}", json);

        // Deserialize back
        let deserialized: HierarchicalConsensusConfig = serde_json::from_str(&json).unwrap();

        // Verify key fields
        assert_eq!(deserialized.global.max_streams, config.global.max_streams);
        assert_eq!(
            deserialized.local.initial_groups,
            config.local.initial_groups
        );
        assert_eq!(
            deserialized.migration.batch_size,
            config.migration.batch_size
        );
    }

    #[test]
    fn test_yaml_serialization() {
        let config = HierarchicalConsensusConfig::default();

        // Serialize to YAML
        let yaml = serde_yaml::to_string(&config).unwrap();
        println!("Config YAML:\n{}", yaml);

        // Deserialize back
        let deserialized: HierarchicalConsensusConfig = serde_yaml::from_str(&yaml).unwrap();

        // Verify key fields
        assert_eq!(deserialized.global.max_streams, config.global.max_streams);
        assert_eq!(
            deserialized.local.initial_groups,
            config.local.initial_groups
        );
    }

    #[test]
    fn test_placement_constraints() {
        let mut config = HierarchicalConsensusConfig::default();

        // Add affinity groups
        config
            .allocation
            .placement_constraints
            .affinity_groups
            .push(vec!["stream-1".to_string(), "stream-2".to_string()]);

        // Add anti-affinity groups
        config
            .allocation
            .placement_constraints
            .anti_affinity_groups
            .push(vec!["stream-a".to_string(), "stream-b".to_string()]);

        // Add stream preferences
        config
            .allocation
            .placement_constraints
            .stream_preferences
            .insert("important-stream".to_string(), ConsensusGroupId::new(0));

        // Verify
        assert_eq!(
            config
                .allocation
                .placement_constraints
                .affinity_groups
                .len(),
            1
        );
        assert_eq!(
            config
                .allocation
                .placement_constraints
                .anti_affinity_groups
                .len(),
            1
        );
        assert_eq!(
            config
                .allocation
                .placement_constraints
                .stream_preferences
                .len(),
            1
        );
    }

    #[test]
    fn test_group_overrides() {
        let mut config = HierarchicalConsensusConfig::default();

        // Add group-specific configuration
        config.local.group_overrides.insert(
            ConsensusGroupId::new(0),
            GroupConfig {
                raft_config_override: Some(RaftConfig {
                    heartbeat_interval: 50,
                    ..Default::default()
                }),
                preferred_nodes: vec![1, 2, 3],
                purpose_tag: Some("high-throughput".to_string()),
                resource_limits: ResourceLimits {
                    max_storage_bytes: 50 * 1024 * 1024 * 1024, // 50GB
                    max_message_rate: 50_000.0,
                    max_memory_bytes: 5 * 1024 * 1024 * 1024, // 5GB
                },
            },
        );

        // Verify
        let group_config = config
            .local
            .group_overrides
            .get(&ConsensusGroupId::new(0))
            .unwrap();
        assert_eq!(group_config.preferred_nodes, vec![1, 2, 3]);
        assert_eq!(
            group_config.purpose_tag.as_ref().unwrap(),
            "high-throughput"
        );
        assert_eq!(
            group_config.resource_limits.max_storage_bytes,
            50 * 1024 * 1024 * 1024
        );
    }

    #[test]
    fn test_alerting_config() {
        let mut config = HierarchicalConsensusConfig::default();

        config.monitoring.alerting = AlertingConfig {
            enabled: true,
            imbalance_threshold: 2.5,
            migration_failure_threshold: 0.15,
            unhealthy_duration_threshold: Duration::from_secs(180),
            webhook_url: Some("https://example.com/alerts".to_string()),
        };

        assert!(config.monitoring.alerting.enabled);
        assert_eq!(config.monitoring.alerting.imbalance_threshold, 2.5);
        assert_eq!(config.monitoring.alerting.migration_failure_threshold, 0.15);
        assert_eq!(
            config.monitoring.alerting.webhook_url.as_ref().unwrap(),
            "https://example.com/alerts"
        );
    }

    #[test]
    fn test_migration_rate_limits() {
        let config = HierarchicalConsensusConfig::default();

        assert_eq!(config.migration.rate_limit.max_per_hour, 100);
        assert_eq!(
            config.migration.rate_limit.max_bytes_per_hour,
            100 * 1024 * 1024 * 1024
        );
        assert_eq!(
            config.migration.rate_limit.min_stream_migration_interval,
            Duration::from_secs(3600)
        );
    }

    #[test]
    fn test_consensus_config_with_hierarchical() {
        // This test just verifies that hierarchical config can be used
        let hierarchical_config = HierarchicalConsensusConfig::default();

        // Verify the hierarchical config is properly initialized
        assert!(hierarchical_config.monitoring.enabled);
        assert_eq!(hierarchical_config.local.initial_groups, 3);
    }
}
