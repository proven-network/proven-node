//! Configuration for membership service

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Configuration for membership service
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MembershipConfig {
    /// Discovery configuration
    pub discovery: DiscoveryConfig,
    /// Health monitoring configuration
    pub health: HealthConfig,
    /// Formation configuration
    pub formation: FormationConfig,
}

/// Configuration for discovery
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryConfig {
    /// Discovery timeout
    pub discovery_timeout: Duration,
    /// Time between discovery rounds
    pub discovery_interval: Duration,
    /// Maximum discovery rounds before giving up
    pub max_discovery_rounds: u32,
    /// Timeout for individual node requests
    pub node_request_timeout: Duration,
    /// Minimum percentage of nodes that must respond (0.0 - 1.0)
    pub min_response_ratio: f64,
    /// Minimum number of nodes that must respond
    pub min_responding_nodes: usize,
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        Self {
            discovery_timeout: Duration::from_secs(30),
            discovery_interval: Duration::from_secs(2),
            max_discovery_rounds: 15,
            node_request_timeout: Duration::from_secs(5),
            min_response_ratio: 0.5,
            min_responding_nodes: 1,
        }
    }
}

/// Configuration for health monitoring
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthConfig {
    /// Interval between health checks
    pub health_check_interval: Duration,
    /// Timeout for health check requests
    pub health_check_timeout: Duration,
    /// Number of failures before marking node as unreachable
    pub unreachable_threshold: u32,
    /// Duration before marking unreachable node as offline
    pub offline_threshold: Duration,
    /// Enable automatic removal of offline nodes
    pub auto_remove_offline: bool,
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            health_check_interval: Duration::from_secs(5),
            health_check_timeout: Duration::from_secs(2),
            unreachable_threshold: 3,
            offline_threshold: Duration::from_secs(60),
            auto_remove_offline: true,
        }
    }
}

/// Configuration for cluster formation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FormationConfig {
    /// Timeout for cluster formation
    pub formation_timeout: Duration,
    /// Minimum nodes required to form a cluster
    pub min_cluster_size: usize,
    /// Whether to allow single-node clusters
    pub allow_single_node: bool,
    /// Retry delay for formation operations
    pub formation_retry_delay: Duration,
}

impl Default for FormationConfig {
    fn default() -> Self {
        Self {
            formation_timeout: Duration::from_secs(60),
            min_cluster_size: 1,
            allow_single_node: true,
            formation_retry_delay: Duration::from_secs(2),
        }
    }
}
