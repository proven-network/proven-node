//! Top-level consensus system that manages all core components.

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

use crate::consensus::{ConsensusConfig, ConsensusManager};
use crate::error::ConsensusError;
use crate::network::{ConsensusMessage, ConsensusNetwork};
use crate::storage::{create_messaging_storage, MessagingStorage};
use crate::topology::TopologyManager;

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
    consensus_manager: Arc<ConsensusManager<G, A>>,

    /// Network layer for peer communication.
    network: Arc<ConsensusNetwork<G, A>>,

    /// Storage layer for persistence.
    storage: Arc<MessagingStorage>,

    /// Topology manager for peer discovery.
    topology: Arc<TopologyManager<G>>,

    /// Consensus configuration.
    config: ConsensusConfig,

    /// Governance system reference.
    governance: Arc<G>,

    /// Attestation system reference.
    attestor: Arc<A>,

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
        config: ConsensusConfig,
    ) -> Result<Self, ConsensusError> {
        // Create topology manager
        let topology = Arc::new(TopologyManager::new(governance.clone(), node_id.clone()));

        // Initialize topology
        topology.refresh_topology().await?;

        // Create storage layer with configurable database path
        let db_path = config
            .storage_dir
            .clone()
            .unwrap_or_else(|| format!("/tmp/consensus-{node_id}"));

        let storage = Arc::new(create_messaging_storage(&db_path).map_err(|e| {
            ConsensusError::InvalidConfiguration(format!("Failed to create storage: {e}"))
        })?);

        info!("Using storage directory: {}", db_path);

        // Create channels for network communication
        let (consensus_tx, _consensus_rx) = mpsc::unbounded_channel::<(u64, ConsensusMessage)>();
        let (_network_tx, network_rx) = mpsc::unbounded_channel::<(u64, ConsensusMessage)>();

        // Parse node ID to u64 for network layer
        let node_id_u64 = node_id.parse::<u64>().map_err(|_| {
            ConsensusError::InvalidConfiguration("Invalid node ID format".to_string())
        })?;

        // Create network layer
        let network = Arc::new(ConsensusNetwork::new(
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
        let mut consensus_manager = ConsensusManager::new(&node_id, config.clone());

        // Initialize Raft with our storage and network implementations
        consensus_manager
            .initialize_raft_with_consensus((*storage).clone(), network.clone())
            .await
            .map_err(|e| {
                ConsensusError::InvalidConfiguration(format!("Failed to initialize Raft: {e}"))
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
            shutdown_tx: Arc::new(std::sync::Mutex::new(None)),
            shutdown_handles: Arc::new(std::sync::Mutex::new(Vec::new())),
        })
    }

    /// Gets a reference to the consensus manager.
    #[must_use]
    pub const fn consensus_manager(&self) -> &Arc<ConsensusManager<G, A>> {
        &self.consensus_manager
    }

    /// Gets a reference to the network layer.
    #[must_use]
    pub const fn network(&self) -> &Arc<ConsensusNetwork<G, A>> {
        &self.network
    }

    /// Gets a reference to the storage layer.
    #[must_use]
    pub const fn storage(&self) -> &Arc<MessagingStorage> {
        &self.storage
    }

    /// Gets a reference to the topology manager.
    #[must_use]
    pub const fn topology(&self) -> &Arc<TopologyManager<G>> {
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
    pub const fn config(&self) -> &ConsensusConfig {
        &self.config
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
        let topology_clone = self.topology.clone();
        let handle1 = tokio::spawn(async move {
            if let Err(e) = topology_clone.start().await {
                warn!("Topology manager error: {}", e);
            }
        });

        // Start network layer
        let network_clone = self.network.clone();
        let handle2 = tokio::spawn(async move {
            if let Err(e) = network_clone.start().await {
                warn!("Network layer error: {}", e);
            }
        });

        // Note: Raft is now properly initialized during construction
        info!("Raft consensus protocol is already initialized and ready");

        self.shutdown_handles.lock().unwrap().push(handle1);
        self.shutdown_handles.lock().unwrap().push(handle2);

        info!("Consensus system started for node {}", self.node_id);
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let value = self.shutdown_tx.lock().unwrap().take();
        if let Some(shutdown_tx) = value {
            let _ = shutdown_tx.send(());
        }

        // Wait for all handles to complete with timeout
        let timeout_duration = std::time::Duration::from_secs(10);
        let handles = std::mem::take(&mut *self.shutdown_handles.lock().unwrap());
        for handle in handles {
            if let Err(e) = tokio::time::timeout(timeout_duration, handle).await {
                warn!("Consensus component shutdown timed out: {}", e);
            }
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
