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
}
