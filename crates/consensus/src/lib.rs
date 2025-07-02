//! Core consensus implementation with Raft-based distributed consensus.
//!
//! This implementation provides strong consistency guarantees by:
//! - Using the governance system to discover network topology
//! - Implementing synchronous replication to all healthy nodes
//! - Providing automatic catch-up for nodes that reconnect
//! - Using a Raft consensus algorithm for ordering and leadership
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use ed25519_dalek::SigningKey;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{info, warn};

use proven_attestation::Attestor;
use proven_bootable::Bootable;
use proven_governance::Governance;

/// Attestation verification for consensus peers.
pub mod attestation;

/// Core consensus protocol and node management.
pub mod consensus_manager;

/// Subject-based messaging and routing.
pub mod consensus_subject;

/// COSE (CBOR Object Signing and Encryption) support for secure messaging.
pub mod cose;

/// Error types for the consensus system.
pub mod error;

/// Peer-to-peer networking for consensus nodes.
pub mod network;

/// OpenRaft network implementation for consensus.
pub mod raft_network;

/// OpenRaft state machine implementation for consensus.
pub mod raft_state_machine;

/// `OpenRaft` storage implementations for consensus.
pub mod storage;

/// Network topology management and peer discovery.
pub mod topology;

/// Top-level consensus system that manages all components.
///
/// This struct centralizes the management of all consensus-related components
/// including the consensus protocol, networking, topology management, and storage.
/// It implements `Bootable` to provide proper lifecycle management.
pub struct Consensus<G, A>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug,
{
    /// Local node identifier.
    node_id: String,

    /// Core consensus protocol manager.
    consensus_manager: Arc<consensus_manager::ConsensusManager<G, A>>,

    /// Network layer for peer communication.
    network: Arc<network::ConsensusNetwork<G, A>>,

    /// Storage layer for persistence.
    storage: Arc<storage::MessagingStorage>,

    /// Topology manager for peer discovery.
    topology: Arc<topology::TopologyManager<G>>,

    /// Consensus configuration.
    config: consensus_manager::ConsensusConfig,

    /// Governance system reference.
    governance: Arc<G>,

    /// Attestation system reference.
    attestor: Arc<A>,

    /// Network sender for outbound messages.
    network_tx: mpsc::UnboundedSender<(u64, network::ConsensusMessage)>,

    /// Shutdown signal sender.
    shutdown_tx: Arc<std::sync::Mutex<Option<mpsc::UnboundedSender<()>>>>,

    /// Background task handles for lifecycle management.
    shutdown_handles: Arc<std::sync::Mutex<Vec<JoinHandle<()>>>>,
}

impl<G, A> Consensus<G, A>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug,
{
    /// Creates a new consensus system.
    ///
    /// # Arguments
    ///
    /// * `node_id` - Unique identifier for this node
    /// * `listen_addr` - Network address to listen on
    /// * `governance` - Governance system for topology discovery
    /// * `attestor` - Attestation system for peer verification
    /// * `signing_key` - Cryptographic signing key for network authentication
    /// * `config` - Consensus configuration parameters
    ///
    /// # Errors
    ///
    /// Returns `ConsensusError` if initialization fails.
    pub async fn new(
        node_id: String,
        listen_addr: SocketAddr,
        governance: Arc<G>,
        attestor: Arc<A>,
        signing_key: SigningKey,
        config: consensus_manager::ConsensusConfig,
    ) -> Result<Self, error::ConsensusError> {
        // Create topology manager
        let local_public_key = hex::encode(signing_key.verifying_key().to_bytes());
        let topology = Arc::new(topology::TopologyManager::new(
            governance.clone(),
            node_id.clone(),
            local_public_key,
        ));

        // Initialize topology
        topology.refresh_topology().await?;

        // Create storage layer with configurable database path
        let db_path = config
            .storage_dir
            .clone()
            .unwrap_or_else(|| format!("/tmp/consensus-{node_id}"));

        let storage = Arc::new(storage::create_messaging_storage(&db_path).map_err(|e| {
            error::ConsensusError::InvalidConfiguration(format!("Failed to create storage: {e}"))
        })?);

        info!("Using storage directory: {}", db_path);

        // Create channels for network communication
        let (consensus_tx, _consensus_rx) =
            mpsc::unbounded_channel::<(u64, network::ConsensusMessage)>();
        let (network_tx, network_rx) =
            mpsc::unbounded_channel::<(u64, network::ConsensusMessage)>();

        // Parse node ID to u64 for network layer
        let node_id_u64 = node_id.parse::<u64>().map_err(|_| {
            error::ConsensusError::InvalidConfiguration("Invalid node ID format".to_string())
        })?;

        // Create network layer
        let network = Arc::new(network::ConsensusNetwork::new(
            node_id_u64,
            listen_addr,
            topology.clone(),
            governance.clone(),
            attestor.clone(),
            signing_key,
            consensus_tx,
            network_rx,
        ));

        // Create consensus manager
        let mut consensus_manager =
            consensus_manager::ConsensusManager::new(&node_id, config.clone());

        // Initialize Raft with our storage and network implementations
        consensus_manager
            .initialize_raft_with_consensus((*storage).clone(), network.clone(), network_tx.clone())
            .await
            .map_err(|e| {
                error::ConsensusError::InvalidConfiguration(format!(
                    "Failed to initialize Raft: {e}"
                ))
            })?;

        let consensus_manager = Arc::new(consensus_manager);

        info!(
            "Created consensus system for node {} on {}",
            node_id, listen_addr
        );

        Ok(Self {
            node_id,
            consensus_manager,
            network,
            storage,
            topology,
            config,
            governance,
            attestor,
            network_tx,
            shutdown_tx: Arc::new(std::sync::Mutex::new(None)),
            shutdown_handles: Arc::new(std::sync::Mutex::new(Vec::new())),
        })
    }

    /// Gets a reference to the consensus manager.
    #[must_use]
    pub const fn consensus_manager(&self) -> &Arc<consensus_manager::ConsensusManager<G, A>> {
        &self.consensus_manager
    }

    /// Gets a reference to the network layer.
    #[must_use]
    pub const fn network(&self) -> &Arc<network::ConsensusNetwork<G, A>> {
        &self.network
    }

    /// Gets a reference to the storage layer.
    #[must_use]
    pub const fn storage(&self) -> &Arc<storage::MessagingStorage> {
        &self.storage
    }

    /// Gets a reference to the topology manager.
    #[must_use]
    pub const fn topology(&self) -> &Arc<topology::TopologyManager<G>> {
        &self.topology
    }

    /// Gets a reference to the governance system.
    #[must_use]
    pub const fn governance(&self) -> &Arc<G> {
        &self.governance
    }

    /// Gets a reference to the attestation system.
    #[must_use]
    pub const fn attestor(&self) -> &Arc<A> {
        &self.attestor
    }

    /// Gets the node ID.
    #[must_use]
    #[allow(clippy::missing_const_for_fn)]
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Gets the consensus configuration.
    #[must_use]
    pub const fn config(&self) -> &consensus_manager::ConsensusConfig {
        &self.config
    }

    /// Gets a clone of the network sender for outbound messages.
    #[must_use]
    pub fn network_sender(&self) -> mpsc::UnboundedSender<(u64, network::ConsensusMessage)> {
        self.network_tx.clone()
    }

    /// Checks if the consensus system is running.
    ///
    /// # Panics
    ///
    /// Panics if the shutdown mutex is poisoned.
    #[must_use]
    pub fn is_running(&self) -> bool {
        self.shutdown_tx.lock().unwrap().is_some()
    }
}

impl<G, A> std::fmt::Debug for Consensus<G, A>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Consensus")
            .field("node_id", &self.node_id)
            .field("config", &self.config)
            .field("is_running", &self.is_running())
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl<G, A> Bootable for Consensus<G, A>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug,
{
    fn bootable_name(&self) -> &'static str {
        "consensus-system"
    }

    async fn start(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.is_running() {
            return Err("Consensus system is already running".into());
        }

        let (shutdown_tx, _shutdown_rx) = mpsc::unbounded_channel();
        *self.shutdown_tx.lock().unwrap() = Some(shutdown_tx);

        // Start topology manager
        self.topology.start().await?;

        // Start network layer
        self.network.start().await?;

        // Note: Raft is now properly initialized during construction
        info!("Raft consensus protocol is already initialized and ready");

        info!("Consensus system started for node {}", self.node_id);
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let value = self.shutdown_tx.lock().unwrap().take();
        if let Some(shutdown_tx) = value {
            let _ = shutdown_tx.send(());
        }

        // Shutdown topology manager
        if let Err(e) = self.topology.shutdown().await {
            warn!("Topology manager shutdown error: {}", e);
        }

        // Shutdown network layer
        if let Err(e) = self.network.shutdown().await {
            warn!("Network layer shutdown error: {}", e);
        }

        info!(
            "Consensus system shutdown complete for node {}",
            self.node_id
        );
        Ok(())
    }

    async fn wait(&self) {
        // Wait for all background tasks to complete
        let mut incomplete_tasks = 0;
        {
            let handles = self.shutdown_handles.lock().unwrap();
            for handle in handles.iter() {
                if !handle.is_finished() {
                    incomplete_tasks += 1;
                }
            }
        }

        if incomplete_tasks > 0 {
            info!(
                "Waiting for {} consensus background tasks to complete",
                incomplete_tasks
            );

            // Yield periodically while tasks are running
            loop {
                let all_finished = {
                    let handles = self.shutdown_handles.lock().unwrap();
                    handles.iter().all(tokio::task::JoinHandle::is_finished)
                };
                if all_finished {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
    }
}

/// Stream-specific configuration options.
#[derive(Clone, Debug)]
pub struct StreamConfig {
    /// Maximum number of messages to cache locally.
    pub cache_size: Option<usize>,

    /// Persistence mode for the stream.
    pub persistence_mode: PersistenceMode,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            cache_size: Some(1000),
            persistence_mode: PersistenceMode::Full,
        }
    }
}

/// Persistence modes for stream data.
#[derive(Clone, Debug)]
pub enum PersistenceMode {
    /// Store all messages persistently.
    Full,

    /// Store only recent messages (number specified).
    Recent(usize),

    /// No persistence - memory only.
    None,
}

pub use error::{ConsensusError, ConsensusResult};

// Re-export key types for easier integration
pub use consensus_manager::{ConsensusConfig, ConsensusManager, TypeConfig};
pub use network::ConsensusNetwork;
pub use raft_network::{ConsensusRaftNetwork, ConsensusRaftNetworkFactory, RaftMessage};
pub use raft_state_machine::{
    ConsensusSnapshotBuilder, ConsensusStateMachine, StateMachineSnapshot,
};
pub use storage::{create_messaging_storage, MessagingStorage};
pub use topology::{PeerInfo, TopologyManager};

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;
    use proven_attestation_mock::MockAttestor;
    use proven_governance::{TopologyNode, Version};
    use proven_governance_mock::MockGovernance;
    use rand::rngs::OsRng;
    use serial_test::serial;
    use std::collections::HashSet;
    use tracing_test::traced_test;

    // Helper to create a simple single-node governance for testing
    fn create_test_governance(
        _node_id: &str,
        port: u16,
        signing_key: &SigningKey,
    ) -> Arc<MockGovernance> {
        let attestor = MockAttestor::new();
        let actual_pcrs = attestor.pcrs_sync();

        let test_version = Version {
            ne_pcr0: actual_pcrs.pcr0,
            ne_pcr1: actual_pcrs.pcr1,
            ne_pcr2: actual_pcrs.pcr2,
        };

        let topology_node = TopologyNode {
            availability_zone: "test-az".to_string(),
            origin: format!("127.0.0.1:{port}"),
            public_key: hex::encode(signing_key.verifying_key().to_bytes()),
            region: "test-region".to_string(),
            specializations: HashSet::new(),
        };

        Arc::new(MockGovernance::new(
            vec![topology_node],
            vec![test_version],
            "http://localhost:3200".to_string(),
            vec![],
        ))
    }

    async fn create_test_consensus(
        node_id: &str,
        port: u16,
    ) -> Arc<Consensus<MockGovernance, MockAttestor>> {
        let signing_key = SigningKey::generate(&mut OsRng);
        let governance = create_test_governance(node_id, port, &signing_key);
        let attestor = Arc::new(MockAttestor::new());

        let temp_dir = tempfile::tempdir().expect("Failed to create temp directory");
        let config = consensus_manager::ConsensusConfig {
            storage_dir: Some(temp_dir.path().to_string_lossy().to_string()),
            ..consensus_manager::ConsensusConfig::default()
        };

        let consensus = Consensus::new(
            node_id.to_string(),
            format!("127.0.0.1:{port}").parse().unwrap(),
            governance,
            attestor,
            signing_key,
            config,
        )
        .await
        .unwrap();

        Arc::new(consensus)
    }

    fn next_port() -> u16 {
        proven_util::port_allocator::allocate_port()
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_consensus_creation() {
        let consensus = create_test_consensus("1", next_port()).await;

        assert_eq!(consensus.node_id(), "1");
        assert!(!consensus.is_running());

        // Verify we can access all components (just check they're not null)
        let _ = consensus.consensus_manager();
        let _ = consensus.network();
        let _ = consensus.storage();
        let _ = consensus.topology();
        let _ = consensus.governance();
        let _ = consensus.attestor();
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_consensus_lifecycle() {
        let consensus = create_test_consensus("1", next_port()).await;

        // Test start
        let start_result = consensus.start().await;
        assert!(
            start_result.is_ok(),
            "Consensus start should succeed: {start_result:?}"
        );
        assert!(
            consensus.is_running(),
            "Consensus should be running after start"
        );

        // Give it a moment to fully initialize
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Test shutdown
        let shutdown_result = consensus.shutdown().await;
        assert!(
            shutdown_result.is_ok(),
            "Consensus shutdown should succeed: {shutdown_result:?}"
        );
        assert!(
            !consensus.is_running(),
            "Consensus should not be running after shutdown"
        );

        // Test wait - this should complete without hanging
        consensus.wait().await;

        println!("✅ Single consensus lifecycle test completed successfully");
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_multiple_consensus_systems() {
        let mut systems = Vec::new();
        let mut signing_keys = Vec::new();
        let mut ports = Vec::new();

        // Pre-allocate ports and keys for all nodes
        for _i in 1..=3 {
            ports.push(next_port());
            signing_keys.push(SigningKey::generate(&mut OsRng));
        }

        // Create shared governance that knows about all nodes
        let attestor = MockAttestor::new();
        let actual_pcrs = attestor.pcrs_sync();
        let test_version = Version {
            ne_pcr0: actual_pcrs.pcr0,
            ne_pcr1: actual_pcrs.pcr1,
            ne_pcr2: actual_pcrs.pcr2,
        };

        let mut topology_nodes = Vec::new();
        for (port, signing_key) in ports.iter().zip(signing_keys.iter()) {
            let topology_node = TopologyNode {
                availability_zone: "test-az".to_string(),
                origin: format!("127.0.0.1:{port}"),
                public_key: hex::encode(signing_key.verifying_key().to_bytes()),
                region: "test-region".to_string(),
                specializations: HashSet::new(),
            };
            topology_nodes.push(topology_node);
        }

        let shared_governance = Arc::new(MockGovernance::new(
            topology_nodes,
            vec![test_version],
            "http://localhost:3200".to_string(),
            vec![],
        ));

        // Create multiple consensus systems with shared governance
        for i in 1..=3 {
            let port = ports[i - 1];
            let signing_key = signing_keys[i - 1].clone();
            let attestor = Arc::new(MockAttestor::new());

            let temp_dir = tempfile::tempdir().expect("Failed to create temp directory");
            let config = consensus_manager::ConsensusConfig {
                storage_dir: Some(temp_dir.path().to_string_lossy().to_string()),
                ..consensus_manager::ConsensusConfig::default()
            };

            let consensus = Consensus::new(
                i.to_string(),
                format!("127.0.0.1:{port}").parse().unwrap(),
                shared_governance.clone(),
                attestor,
                signing_key,
                config,
            )
            .await
            .unwrap();

            systems.push(Arc::new(consensus));
        }

        // Start all systems
        for (i, consensus) in systems.iter().enumerate() {
            let result = consensus.start().await;
            assert!(
                result.is_ok(),
                "Consensus {} start should succeed: {:?}",
                i + 1,
                result
            );
        }

        // Give them time to initialize and attempt connections
        tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

        // Shutdown all systems
        for (i, consensus) in systems.iter().enumerate() {
            let result = consensus.shutdown().await;
            assert!(
                result.is_ok(),
                "Consensus {} shutdown should succeed: {:?}",
                i + 1,
                result
            );
        }

        // Wait for all systems to fully shut down
        for consensus in &systems {
            consensus.wait().await;
        }

        println!("✅ Multiple consensus systems test completed successfully");
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_consensus_start_stop_restart() {
        let consensus = create_test_consensus("1", next_port()).await;

        // Start
        consensus.start().await.unwrap();
        assert!(consensus.is_running());

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Stop
        consensus.shutdown().await.unwrap();
        consensus.wait().await;
        assert!(!consensus.is_running());

        // Restart should succeed since we can restart the same instance
        let restart_result = consensus.start().await;
        assert!(
            restart_result.is_ok(),
            "Restart should succeed after shutdown"
        );

        println!("✅ Start-stop-restart test completed successfully");
    }
}
