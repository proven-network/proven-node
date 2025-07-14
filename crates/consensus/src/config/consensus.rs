//! Main consensus configuration types

use std::sync::Arc;
use std::time::Duration;

use ed25519_dalek::SigningKey;
use openraft::Config;
use proven_governance::Governance;

use super::cluster::ClusterJoinRetryConfig;
use super::global::GlobalConsensusConfig;
use super::group::{AllocationConfig, GroupsConfig, MigrationConfig};
use super::monitoring::MonitoringConfig;
use super::storage::StorageConfig;
use super::transport::TransportConfig;
use crate::core::stream::StreamStorageBackend;

/// Configuration for the consensus system
#[derive(Debug, Clone)]
pub struct EngineConfig<G>
where
    G: Governance,
{
    /// Governance instance
    pub governance: Arc<G>,
    /// Node signing key
    pub signing_key: SigningKey,
    /// Raft configuration
    pub raft_config: Config,
    /// Transport configuration (will be used to create transport)
    pub transport_config: TransportConfig,
    /// Storage configuration (will be used to create storage)
    pub storage_config: StorageConfig,
    /// Cluster discovery timeout
    pub cluster_discovery_timeout: Option<Duration>,
    /// Cluster join retry configuration
    pub cluster_join_retry_config: ClusterJoinRetryConfig,
    /// Global consensus configuration
    pub global_config: GlobalConsensusConfig,
    /// Groups configuration
    pub groups_config: GroupsConfig,
    /// Stream allocation configuration
    pub allocation_config: AllocationConfig,
    /// Migration configuration
    pub migration_config: MigrationConfig,
    /// Monitoring configuration
    pub monitoring_config: MonitoringConfig,
    /// Storage backend to use for stream file storage
    pub stream_storage_backend: StreamStorageBackend,
}
