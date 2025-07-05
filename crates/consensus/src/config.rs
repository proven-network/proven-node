//! Configuration types for consensus messaging

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use ed25519_dalek::SigningKey;
use openraft::Config;
use proven_attestation::Attestor;
use proven_governance::Governance;

/// Storage configuration options
#[derive(Debug, Clone)]
pub enum StorageConfig {
    /// In-memory storage
    Memory,
    /// RocksDB persistent storage
    RocksDB {
        /// Database path
        path: PathBuf,
    },
}

/// Configuration for cluster join retry behavior
#[derive(Debug, Clone)]
pub struct ClusterJoinRetryConfig {
    /// Maximum number of join attempts before giving up (default: 5)
    pub max_attempts: usize,
    /// Initial delay between retry attempts (default: 500ms)
    pub initial_delay: Duration,
    /// Maximum delay between retry attempts (default: 5s)
    pub max_delay: Duration,
    /// Timeout for each individual join request (default: 30s)
    pub request_timeout: Duration,
}

impl Default for ClusterJoinRetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            initial_delay: Duration::from_millis(500),
            max_delay: Duration::from_secs(5),
            request_timeout: Duration::from_secs(30),
        }
    }
}

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
    pub transport_config: crate::transport::TransportConfig,
    /// Storage configuration (will be used to create storage)
    pub storage_config: StorageConfig,
    /// Cluster discovery timeout
    pub cluster_discovery_timeout: Option<Duration>,
    /// Cluster join retry configuration
    pub cluster_join_retry_config: ClusterJoinRetryConfig,
}
