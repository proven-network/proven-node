//! Engine builder for the consensus system
//!
//! This module provides a builder pattern for creating Engine and ConsensusClient instances
//! with all necessary dependencies and configuration.

use crate::client::ConsensusClient;
use crate::config::{
    AllocationConfig, ClusterJoinRetryConfig, EngineConfig, GlobalConsensusConfig, GroupsConfig,
    MigrationConfig, MonitoringConfig, StorageConfig, TransportConfig,
};
use crate::core::engine::Engine;
use crate::core::global::global_state::GlobalState;
use crate::core::stream::StreamStorageBackend;
use crate::error::{ConfigurationError, ConsensusResult, Error};

use std::sync::Arc;
use std::time::Duration;

use ed25519_dalek::SigningKey;
use openraft::Config;
use proven_governance::Governance;
use proven_network::NetworkManager;
use proven_topology::NodeId;
use proven_topology::TopologyManager;
use proven_transport::Transport;
use tracing::info;

/// Builder for creating Engine and ConsensusClient instances
///
/// This builder provides a fluent API for configuring and creating
/// the consensus engine with all necessary dependencies.
pub struct EngineBuilder<T, G>
where
    T: Transport,
    G: Governance,
{
    // External dependencies
    network_manager: Option<Arc<NetworkManager<T, G>>>,
    topology_manager: Option<Arc<TopologyManager<G>>>,

    // Core configuration
    governance: Option<Arc<G>>,
    signing_key: Option<SigningKey>,

    // Consensus configurations
    raft_config: Config,
    transport_config: Option<TransportConfig>,
    storage_config: StorageConfig,

    // Cluster configuration
    cluster_discovery_timeout: Option<Duration>,
    cluster_join_retry_config: ClusterJoinRetryConfig,

    // Component configurations
    global_config: GlobalConsensusConfig,
    groups_config: GroupsConfig,
    allocation_config: AllocationConfig,
    migration_config: MigrationConfig,
    monitoring_config: MonitoringConfig,
    stream_storage_backend: StreamStorageBackend,
}

impl<T, G> EngineBuilder<T, G>
where
    T: Transport,
    G: Governance,
{
    /// Create a new engine builder with default values
    pub fn new() -> Self {
        Self {
            network_manager: None,
            topology_manager: None,
            governance: None,
            signing_key: None,
            raft_config: Config::default(),
            transport_config: None,
            storage_config: StorageConfig::Memory,
            cluster_discovery_timeout: None,
            cluster_join_retry_config: ClusterJoinRetryConfig::default(),
            global_config: GlobalConsensusConfig::default(),
            groups_config: GroupsConfig::default(),
            allocation_config: AllocationConfig::default(),
            migration_config: MigrationConfig::default(),
            monitoring_config: MonitoringConfig::default(),
            stream_storage_backend: StreamStorageBackend::default(),
        }
    }

    // External dependencies

    /// Set the network manager
    pub fn network_manager(mut self, network_manager: Arc<NetworkManager<T, G>>) -> Self {
        self.network_manager = Some(network_manager);
        self
    }

    /// Set the topology manager
    pub fn topology_manager(mut self, topology_manager: Arc<TopologyManager<G>>) -> Self {
        self.topology_manager = Some(topology_manager);
        self
    }

    // Core configuration

    /// Set the governance instance
    pub fn governance(mut self, governance: Arc<G>) -> Self {
        self.governance = Some(governance);
        self
    }

    /// Set the node signing key
    pub fn signing_key(mut self, signing_key: SigningKey) -> Self {
        self.signing_key = Some(signing_key);
        self
    }

    // Consensus configurations

    /// Set the Raft configuration
    pub fn raft_config(mut self, raft_config: Config) -> Self {
        self.raft_config = raft_config;
        self
    }

    /// Set the transport configuration
    pub fn transport_config(mut self, transport_config: TransportConfig) -> Self {
        self.transport_config = Some(transport_config);
        self
    }

    /// Set the storage configuration
    pub fn storage_config(mut self, storage_config: StorageConfig) -> Self {
        self.storage_config = storage_config;
        self
    }

    // Cluster configuration

    /// Set the cluster discovery timeout
    pub fn cluster_discovery_timeout(mut self, timeout: Duration) -> Self {
        self.cluster_discovery_timeout = Some(timeout);
        self
    }

    /// Set the cluster join retry configuration
    pub fn cluster_join_retry_config(mut self, config: ClusterJoinRetryConfig) -> Self {
        self.cluster_join_retry_config = config;
        self
    }

    // Component configurations

    /// Set the global consensus configuration
    pub fn global_config(mut self, config: GlobalConsensusConfig) -> Self {
        self.global_config = config;
        self
    }

    /// Set the groups configuration
    pub fn groups_config(mut self, config: GroupsConfig) -> Self {
        self.groups_config = config;
        self
    }

    /// Set the allocation configuration
    pub fn allocation_config(mut self, config: AllocationConfig) -> Self {
        self.allocation_config = config;
        self
    }

    /// Set the migration configuration
    pub fn migration_config(mut self, config: MigrationConfig) -> Self {
        self.migration_config = config;
        self
    }

    /// Set the monitoring configuration
    pub fn monitoring_config(mut self, config: MonitoringConfig) -> Self {
        self.monitoring_config = config;
        self
    }

    /// Set the stream storage backend
    pub fn stream_storage_backend(mut self, backend: StreamStorageBackend) -> Self {
        self.stream_storage_backend = backend;
        self
    }

    /// Build the engine configuration from current settings
    fn build_config(self) -> Result<EngineConfig<G>, Error> {
        let governance = self.governance.ok_or_else(|| {
            Error::Configuration(ConfigurationError::MissingRequired {
                key: "governance".to_string(),
            })
        })?;
        let signing_key = self.signing_key.ok_or_else(|| {
            Error::Configuration(ConfigurationError::MissingRequired {
                key: "signing_key".to_string(),
            })
        })?;
        let transport_config = self.transport_config.ok_or_else(|| {
            Error::Configuration(ConfigurationError::MissingRequired {
                key: "transport_config".to_string(),
            })
        })?;

        Ok(EngineConfig {
            governance,
            signing_key,
            raft_config: self.raft_config,
            transport_config,
            storage_config: self.storage_config,
            cluster_discovery_timeout: self.cluster_discovery_timeout,
            cluster_join_retry_config: self.cluster_join_retry_config,
            global_config: self.global_config,
            groups_config: self.groups_config,
            allocation_config: self.allocation_config,
            migration_config: self.migration_config,
            monitoring_config: self.monitoring_config,
            stream_storage_backend: self.stream_storage_backend,
        })
    }

    /// Build the engine and client
    pub async fn build(self) -> ConsensusResult<(Arc<Engine<T, G>>, ConsensusClient<T, G>)> {
        // Get required external dependencies
        let network_manager = self.network_manager.clone().ok_or_else(|| {
            Error::Configuration(ConfigurationError::MissingRequired {
                key: "network_manager".to_string(),
            })
        })?;
        let topology_manager = self.topology_manager.clone().ok_or_else(|| {
            Error::Configuration(ConfigurationError::MissingRequired {
                key: "topology_manager".to_string(),
            })
        })?;

        // Build config from the rest of the fields
        let config = self.build_config()?;

        // Generate node ID from public key
        let node_id = NodeId::new(config.signing_key.verifying_key());

        info!("Creating engine for node {}", node_id);

        // Create global state store
        let global_state = Arc::new(GlobalState::new());

        // Create global storage factory based on configuration
        let storage_factory =
            crate::core::global::storage::create_global_storage_factory(&config.storage_config)?;
        let components = storage_factory
            .create_components(global_state.clone())
            .await?;

        // Create Raft network factory for global consensus
        let network_factory =
            crate::core::global::global_network_adaptor::GlobalNetworkFactory::new(
                network_manager.clone(),
            );

        // Create Engine
        let engine = Arc::new(
            Engine::new(
                node_id.clone(),
                config.raft_config.clone(),
                network_factory,
                components.log_storage,
                components.state_machine,
                global_state.clone(),
                topology_manager.clone(),
                network_manager.clone(),
                config.raft_config.clone(), // local_base_config
                &config.storage_config,
                config.stream_storage_backend.clone(),
                config.cluster_join_retry_config.clone(),
                config.monitoring_config.clone(),
            )
            .await
            .map_err(|e| Error::Raft(format!("Failed to create Engine: {e}")))?,
        );

        // Create the client
        let client = ConsensusClient::new(engine.clone(), node_id.clone());

        Ok((engine, client))
    }
}

impl<T, G> Default for EngineBuilder<T, G>
where
    T: Transport,
    G: Governance,
{
    fn default() -> Self {
        Self::new()
    }
}
