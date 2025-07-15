//! Engine configuration

use serde::{Deserialize, Serialize};
use std::time::Duration;

use crate::services::{
    cluster::ClusterConfig, event::EventConfig, lifecycle::LifecycleConfig,
    migration::MigrationConfig, monitoring::MonitoringConfig,
    network::NetworkConfig as NetworkServiceConfig, pubsub::service::PubSubConfig,
    routing::RoutingConfig,
};

/// Engine configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineConfig {
    /// Node name
    pub node_name: String,

    /// Service configurations
    pub services: ServiceConfig,

    /// Consensus configuration
    pub consensus: ConsensusConfig,

    /// Network configuration
    pub network: NetworkConfig,

    /// Storage configuration
    pub storage: StorageConfig,
}

/// Service configurations
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ServiceConfig {
    /// Network service config
    pub network: NetworkServiceConfig,

    /// Cluster service config
    pub cluster: ClusterConfig,

    /// Event service config
    pub event: EventConfig,

    /// Monitoring service config
    pub monitoring: MonitoringConfig,

    /// Routing service config
    pub routing: RoutingConfig,

    /// Migration service config
    pub migration: MigrationConfig,

    /// Lifecycle service config
    pub lifecycle: LifecycleConfig,

    /// PubSub service config
    pub pubsub: PubSubConfig,
}

/// Consensus configuration
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ConsensusConfig {
    /// Global consensus config
    pub global: GlobalConsensusConfig,

    /// Group consensus config
    pub group: GroupConsensusConfig,
}

/// Global consensus configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalConsensusConfig {
    /// Election timeout range
    pub election_timeout_min: Duration,
    pub election_timeout_max: Duration,

    /// Heartbeat interval
    pub heartbeat_interval: Duration,

    /// Snapshot interval
    pub snapshot_interval: u64,

    /// Max entries per append
    pub max_entries_per_append: u64,
}

/// Group consensus configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupConsensusConfig {
    /// Election timeout range
    pub election_timeout_min: Duration,
    pub election_timeout_max: Duration,

    /// Heartbeat interval
    pub heartbeat_interval: Duration,

    /// Snapshot interval
    pub snapshot_interval: u64,

    /// Max entries per append
    pub max_entries_per_append: u64,
}

/// Network configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkConfig {
    /// Listen address
    pub listen_addr: String,

    /// Public address (for other nodes to connect)
    pub public_addr: String,

    /// Connection timeout
    pub connection_timeout: Duration,

    /// Request timeout
    pub request_timeout: Duration,
}

/// Storage configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Storage path
    pub path: String,

    /// Max log size
    pub max_log_size: u64,

    /// Compaction interval
    pub compaction_interval: Duration,

    /// Cache size
    pub cache_size: usize,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            node_name: "consensus-node".to_string(),
            services: ServiceConfig::default(),
            consensus: ConsensusConfig::default(),
            network: NetworkConfig::default(),
            storage: StorageConfig::default(),
        }
    }
}

impl Default for GlobalConsensusConfig {
    fn default() -> Self {
        Self {
            election_timeout_min: Duration::from_millis(150),
            election_timeout_max: Duration::from_millis(300),
            heartbeat_interval: Duration::from_millis(50),
            snapshot_interval: 1000,
            max_entries_per_append: 64,
        }
    }
}

impl Default for GroupConsensusConfig {
    fn default() -> Self {
        Self {
            election_timeout_min: Duration::from_millis(150),
            election_timeout_max: Duration::from_millis(300),
            heartbeat_interval: Duration::from_millis(50),
            snapshot_interval: 1000,
            max_entries_per_append: 64,
        }
    }
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            listen_addr: "127.0.0.1:9000".to_string(),
            public_addr: "127.0.0.1:9000".to_string(),
            connection_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(30),
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            path: "./data".to_string(),
            max_log_size: 1024 * 1024 * 1024,               // 1GB
            compaction_interval: Duration::from_secs(3600), // 1 hour
            cache_size: 1000,
        }
    }
}
