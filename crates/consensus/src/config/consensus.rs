//! Main consensus configuration types and builder

use std::sync::Arc;
use std::time::Duration;

use ed25519_dalek::SigningKey;
use openraft::Config;
use proven_attestation::Attestor;
use proven_governance::Governance;

use super::cluster::ClusterJoinRetryConfig;
use super::hierarchical::HierarchicalConsensusConfig;
use super::storage::StorageConfig;
use super::transport::TransportConfig;

/// Configuration for the consensus system
#[derive(Debug, Clone)]
pub struct ConsensusConfig<G, A>
where
    G: Governance + Send + Sync + 'static,
    A: Attestor + Send + Sync + 'static,
{
    /// Governance instance
    pub governance: Arc<G>,
    /// Attestor instance
    pub attestor: Arc<A>,
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
    /// Hierarchical consensus configuration
    pub hierarchical_config: HierarchicalConsensusConfig,
}

/// Builder for creating ConsensusConfig instances
pub struct ConsensusConfigBuilder<G, A>
where
    G: Governance + Send + Sync + 'static,
    A: Attestor + Send + Sync + 'static,
{
    governance: Option<Arc<G>>,
    attestor: Option<Arc<A>>,
    signing_key: Option<SigningKey>,
    raft_config: Config,
    transport_config: Option<TransportConfig>,
    storage_config: StorageConfig,
    cluster_discovery_timeout: Option<Duration>,
    cluster_join_retry_config: ClusterJoinRetryConfig,
    hierarchical_config: HierarchicalConsensusConfig,
}

impl<G, A> ConsensusConfigBuilder<G, A>
where
    G: Governance + Send + Sync + 'static,
    A: Attestor + Send + Sync + 'static,
{
    /// Create a new builder with default values
    pub fn new() -> Self {
        Self {
            governance: None,
            attestor: None,
            signing_key: None,
            raft_config: Config::default(),
            transport_config: None,
            storage_config: StorageConfig::Memory,
            cluster_discovery_timeout: None,
            cluster_join_retry_config: ClusterJoinRetryConfig::default(),
            hierarchical_config: HierarchicalConsensusConfig::default(),
        }
    }

    /// Set the governance instance
    pub fn governance(mut self, governance: Arc<G>) -> Self {
        self.governance = Some(governance);
        self
    }

    /// Set the attestor instance
    pub fn attestor(mut self, attestor: Arc<A>) -> Self {
        self.attestor = Some(attestor);
        self
    }

    /// Set the node signing key
    pub fn signing_key(mut self, signing_key: SigningKey) -> Self {
        self.signing_key = Some(signing_key);
        self
    }

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

    /// Set the hierarchical consensus configuration
    pub fn hierarchical_config(mut self, config: HierarchicalConsensusConfig) -> Self {
        self.hierarchical_config = config;
        self
    }

    /// Build the ConsensusConfig
    ///
    /// # Returns
    ///
    /// Returns `Ok(ConsensusConfig)` if all required fields are set,
    /// otherwise returns `Err` with a description of missing fields.
    pub fn build(self) -> Result<ConsensusConfig<G, A>, String> {
        let governance = self.governance.ok_or("Governance is required")?;
        let attestor = self.attestor.ok_or("Attestor is required")?;
        let signing_key = self.signing_key.ok_or("Signing key is required")?;
        let transport_config = self
            .transport_config
            .ok_or("Transport config is required")?;

        Ok(ConsensusConfig {
            governance,
            attestor,
            signing_key,
            raft_config: self.raft_config,
            transport_config,
            storage_config: self.storage_config,
            cluster_discovery_timeout: self.cluster_discovery_timeout,
            cluster_join_retry_config: self.cluster_join_retry_config,
            hierarchical_config: self.hierarchical_config,
        })
    }
}

impl<G, A> Default for ConsensusConfigBuilder<G, A>
where
    G: Governance + Send + Sync + 'static,
    A: Attestor + Send + Sync + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;
    use std::str::FromStr;

    // Use mock implementations from other crates
    type MockGovernance = proven_governance_mock::MockGovernance;
    type MockAttestor = proven_attestation_mock::MockAttestor;

    #[test]
    fn test_builder_pattern() {
        let governance = Arc::new(MockGovernance::new(
            vec![],
            vec![],
            "test".to_string(),
            vec![],
        ));
        let attestor = Arc::new(MockAttestor::default());
        let signing_key = SigningKey::generate(&mut rand::rngs::OsRng);
        let addr = SocketAddr::from_str("127.0.0.1:8080").unwrap();

        let config = ConsensusConfigBuilder::new()
            .governance(governance.clone())
            .attestor(attestor.clone())
            .signing_key(signing_key)
            .transport_config(TransportConfig::Tcp { listen_addr: addr })
            .storage_config(StorageConfig::Memory)
            .cluster_discovery_timeout(Duration::from_secs(30))
            .build();

        assert!(config.is_ok());
        let config = config.unwrap();
        assert!(Arc::ptr_eq(&config.governance, &governance));
        assert!(Arc::ptr_eq(&config.attestor, &attestor));
        assert!(matches!(
            config.transport_config,
            TransportConfig::Tcp { .. }
        ));
        assert!(matches!(config.storage_config, StorageConfig::Memory));
        assert_eq!(
            config.cluster_discovery_timeout,
            Some(Duration::from_secs(30))
        );
    }

    #[test]
    fn test_builder_missing_required_fields() {
        let builder = ConsensusConfigBuilder::<MockGovernance, MockAttestor>::new();
        let result = builder.build();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Governance is required"));
    }

    #[test]
    fn test_builder_with_hierarchical_config() {
        let governance = Arc::new(MockGovernance::new(
            vec![],
            vec![],
            "test".to_string(),
            vec![],
        ));
        let attestor = Arc::new(MockAttestor::default());
        let signing_key = SigningKey::generate(&mut rand::rngs::OsRng);
        let addr = SocketAddr::from_str("127.0.0.1:8080").unwrap();

        let hierarchical_config = HierarchicalConsensusConfig::default();

        let config = ConsensusConfigBuilder::new()
            .governance(governance)
            .attestor(attestor)
            .signing_key(signing_key)
            .transport_config(TransportConfig::Tcp { listen_addr: addr })
            .hierarchical_config(hierarchical_config.clone())
            .build();

        assert!(config.is_ok());
        let config = config.unwrap();
        assert_eq!(
            config.hierarchical_config.global.max_streams,
            hierarchical_config.global.max_streams
        );
    }
}
