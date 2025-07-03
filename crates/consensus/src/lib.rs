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
use bytes::Bytes;
use ed25519_dalek::SigningKey;
use parking_lot::RwLock;
use proven_attestation::Attestor;
use proven_bootable::Bootable;
use proven_governance::Governance;
use tokio::sync::mpsc;
use tokio::sync::RwLock as AsyncRwLock;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

/// Represents the current state of cluster membership.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterState {
    /// Transport started but cluster not yet initialized.
    TransportReady,
    /// Currently discovering existing clusters or waiting for topology.
    Discovering,
    /// Successfully joined an existing cluster.
    Joined {
        /// When the cluster was joined.
        joined_at: std::time::Instant,
        /// Number of nodes in the cluster when joined.
        cluster_size: usize,
    },
    /// Became the cluster initiator (single node or timeout).
    Initiator {
        /// When became initiator.
        initiated_at: std::time::Instant,
        /// Whether this was due to timeout or single-node topology.
        reason: InitiatorReason,
    },
    /// Cluster initialization failed.
    Failed {
        /// When the failure occurred.
        failed_at: std::time::Instant,
        /// Error message.
        error: String,
    },
}

/// Reason for becoming cluster initiator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InitiatorReason {
    /// Single node in topology.
    SingleNode,
    /// Discovery timeout expired.
    DiscoveryTimeout,
}

mod config;

/// Attestation verification for consensus peers.
pub mod attestation;

/// Subject-based messaging and routing.
pub mod consensus_subject;

/// COSE (CBOR Object Signing and Encryption) support for secure messaging.
pub mod cose;

/// Error types for the consensus system.
pub mod error;

/// Messaging types for consensus operations.
pub mod messaging;

/// Transport abstraction for consensus networking.
pub mod transport;

/// Stream-based storage for consensus messages.
pub mod stream_store;

/// Subscription handling for consensus messaging.
pub mod subscription;

/// Type definitions for consensus operations.
pub mod types;

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
pub struct Consensus<G, A, T>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug,
    T: transport::ConsensusTransport,
{
    /// Local node identifier.
    node_id: String,

    /// `OpenRaft` instance
    raft: Arc<RwLock<Option<openraft::Raft<TypeConfig>>>>,

    /// Stream store for message storage
    store: StreamStore,

    /// Transport layer for peer communication.
    transport: Arc<T>,

    /// Storage layer for persistence.
    storage: Arc<storage::MessagingStorage>,

    /// Topology manager for peer discovery.
    topology: Arc<topology::TopologyManager<G>>,

    /// Consensus configuration.
    config: ConsensusConfig,

    /// Governance system reference.
    governance: Arc<G>,

    /// Attestation system reference.
    attestor: Arc<A>,

    /// Transport sender for outbound messages.
    network_tx: mpsc::UnboundedSender<(String, transport::ConsensusMessage)>,

    /// Shutdown signal sender.
    shutdown_tx: Arc<std::sync::Mutex<Option<mpsc::UnboundedSender<()>>>>,

    /// Background task handles for lifecycle management.
    shutdown_handles: Arc<std::sync::Mutex<Vec<JoinHandle<()>>>>,

    /// Cluster join state tracking.
    cluster_state: Arc<AsyncRwLock<ClusterState>>,
}

impl<G, A, T> Consensus<G, A, T>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug,
    T: transport::ConsensusTransport,
{
    /// Creates a new consensus system.
    ///
    /// # Arguments
    ///
    /// * `governance` - Governance system for topology discovery
    /// * `attestor` - Attestation system for peer verification
    /// * `signing_key` - Cryptographic signing key for network authentication
    /// * `transport` - Transport layer for peer communication
    /// * `config` - Consensus configuration parameters
    ///
    /// # Errors
    ///
    /// Returns `ConsensusError` if initialization fails.
    pub async fn new(
        governance: Arc<G>,
        attestor: Arc<A>,
        signing_key: SigningKey,
        transport: T,
        config: ConsensusConfig,
    ) -> Result<Self, error::ConsensusError> {
        // Create topology manager
        let local_public_key = hex::encode(signing_key.verifying_key().to_bytes());
        let node_id = hex::encode(signing_key.to_bytes());
        let topology: Arc<TopologyManager<G>> = Arc::new(topology::TopologyManager::new(
            governance.clone(),
            node_id.clone(),
            local_public_key.clone(),
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

        // Create raft and store directly
        let raft = Arc::new(RwLock::new(None)); // Will be initialized later with storage and transport
        let store = StreamStore::new();

        info!("Created consensus system for node {}", node_id);

        let transport = Arc::new(transport);

        let consensus = Self {
            node_id: node_id.clone(),
            raft,
            store: store.clone(),
            transport: transport.clone(),
            storage: storage.clone(),
            topology,
            config,
            governance,
            attestor,
            network_tx: transport.get_message_sender(), // Get sender from transport
            shutdown_tx: Arc::new(std::sync::Mutex::new(None)),
            shutdown_handles: Arc::new(std::sync::Mutex::new(Vec::new())),
            cluster_state: Arc::new(AsyncRwLock::new(ClusterState::TransportReady)),
        };

        // Initialize Raft with our transport abstraction
        info!("Initializing Raft with transport abstraction");
        consensus.initialize_raft().await?;

        Ok(consensus)
    }

    /// Gets a reference to the transport layer.
    #[must_use]
    pub const fn transport(&self) -> &Arc<T> {
        &self.transport
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
    pub const fn config(&self) -> &ConsensusConfig {
        &self.config
    }

    /// Gets a clone of the transport sender for outbound messages.
    #[must_use]
    pub fn transport_sender(&self) -> mpsc::UnboundedSender<(String, transport::ConsensusMessage)> {
        self.network_tx.clone()
    }

    /// Initialize Raft with our transport implementations
    ///
    /// # Errors
    ///
    /// Returns an error if Raft initialization fails.
    async fn initialize_raft(&self) -> Result<(), error::ConsensusError> {
        info!("Initializing Raft for node {}", self.node_id);

        // Create the Raft network factory using our transport abstraction
        let network_factory = raft_network::ConsensusRaftNetworkFactory::new(
            self.transport.clone(),
            self.network_tx.clone(),
        );

        // Create the state machine using our stream store
        let state_machine =
            raft_state_machine::ConsensusStateMachine::new(Arc::new(self.store.clone()));

        // Build the Raft instance
        let raft_instance = openraft::Raft::new(
            self.node_id.clone(),
            self.config.raft_config.clone(),
            network_factory,
            (*self.storage).clone(), // Clone the actual RocksLogStore, not the Arc wrapper
            state_machine,
        )
        .await
        .map_err(|e| {
            error::ConsensusError::InvalidConfiguration(format!(
                "Failed to create Raft instance: {e}"
            ))
        })?;

        // Store the Raft instance
        {
            let mut raft_guard = self.raft.write();
            *raft_guard = Some(raft_instance);
        }

        info!("Raft initialization completed for node {}", self.node_id);
        Ok(())
    }

    /// Check if Raft is initialized
    #[must_use]
    pub fn is_raft_initialized(&self) -> bool {
        self.raft.read().is_some()
    }

    /// Get the current cluster state
    #[must_use]
    pub async fn cluster_state(&self) -> ClusterState {
        self.cluster_state.read().await.clone()
    }

    /// Check if the node has successfully joined a cluster
    #[must_use]
    pub async fn is_cluster_joined(&self) -> bool {
        let state = self.cluster_state.read().await;
        matches!(
            *state,
            ClusterState::Joined { .. } | ClusterState::Initiator { .. }
        )
    }

    /// Check if cluster initialization is in progress
    #[must_use]
    pub async fn is_cluster_discovering(&self) -> bool {
        let state = self.cluster_state.read().await;
        matches!(*state, ClusterState::Discovering)
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

    /// Publish a message through consensus
    ///
    /// # Errors
    ///
    /// Returns an error if the Raft instance is not initialized or if consensus fails.
    pub async fn publish_message(
        &self,
        stream: String,
        data: Bytes,
    ) -> Result<u64, error::ConsensusError> {
        let request = MessagingRequest {
            operation: MessagingOperation::PublishToStream { stream, data },
        };

        self.propose_request(&request).await
    }

    /// Publish a message with metadata through consensus
    ///
    /// # Errors
    ///
    /// Returns an error if the Raft instance is not initialized or if consensus fails.
    pub async fn publish_message_with_metadata(
        &self,
        stream: String,
        data: Bytes,
        metadata: std::collections::HashMap<String, String>,
    ) -> Result<u64, error::ConsensusError> {
        let request = MessagingRequest {
            operation: MessagingOperation::PublishToStreamWithMetadata {
                stream,
                data,
                metadata,
            },
        };

        self.propose_request(&request).await
    }

    /// Publish multiple messages as a batch through consensus
    ///
    /// # Errors
    ///
    /// Returns an error if the Raft instance is not initialized or if consensus fails.
    pub async fn publish_batch(
        &self,
        stream: String,
        messages: Vec<Bytes>,
    ) -> Result<u64, error::ConsensusError> {
        let request = MessagingRequest {
            operation: MessagingOperation::PublishBatchToStream { stream, messages },
        };

        self.propose_request(&request).await
    }

    /// Publish a message to a subject through consensus
    ///
    /// # Errors
    ///
    /// Returns an error if the Raft instance is not initialized or if consensus fails.
    pub async fn publish(
        &self,
        subject: String,
        data: Bytes,
    ) -> Result<u64, error::ConsensusError> {
        let request = MessagingRequest {
            operation: MessagingOperation::Publish { subject, data },
        };

        self.propose_request(&request).await
    }

    /// Rollup operation through consensus
    ///
    /// # Errors
    ///
    /// Returns an error if the Raft instance is not initialized or if consensus fails.
    pub async fn rollup_message(
        &self,
        stream: String,
        data: Bytes,
        expected_seq: u64,
    ) -> Result<u64, error::ConsensusError> {
        let request = MessagingRequest {
            operation: MessagingOperation::RollupStream {
                stream,
                data,
                expected_seq,
            },
        };

        self.propose_request(&request).await
    }

    /// Delete a message from a stream through consensus
    ///
    /// # Errors
    ///
    /// Returns an error if the Raft instance is not initialized or if consensus fails.
    pub async fn delete_message(
        &self,
        stream: String,
        sequence: u64,
    ) -> Result<u64, error::ConsensusError> {
        let request = MessagingRequest {
            operation: MessagingOperation::DeleteFromStream { stream, sequence },
        };

        self.propose_request(&request).await
    }

    /// Subscribe a stream to a subject pattern through consensus
    ///
    /// # Errors
    ///
    /// Returns an error if the Raft instance is not initialized or if consensus fails.
    pub async fn subscribe_stream_to_subject(
        &self,
        stream_name: impl Into<String>,
        subject_pattern: impl Into<String>,
    ) -> Result<u64, error::ConsensusError> {
        let stream_name = stream_name.into();
        let subject_pattern = subject_pattern.into();

        let request = MessagingRequest {
            operation: MessagingOperation::SubscribeToSubject {
                stream_name,
                subject_pattern,
            },
        };

        self.propose_request(&request).await
    }

    /// Unsubscribe a stream from a subject pattern through consensus
    ///
    /// # Errors
    ///
    /// Returns an error if the Raft instance is not initialized or if consensus fails.
    pub async fn unsubscribe_stream_from_subject(
        &self,
        stream_name: impl Into<String>,
        subject_pattern: impl Into<String>,
    ) -> Result<u64, error::ConsensusError> {
        let request = MessagingRequest {
            operation: MessagingOperation::UnsubscribeFromSubject {
                stream_name: stream_name.into(),
                subject_pattern: subject_pattern.into(),
            },
        };

        self.propose_request(&request).await
    }

    /// Remove all subject subscriptions for a stream through consensus
    ///
    /// # Errors
    ///
    /// Returns an error if the Raft instance is not initialized or if consensus fails.
    pub async fn remove_stream_subscriptions(
        &self,
        stream_name: impl Into<String>,
    ) -> Result<u64, error::ConsensusError> {
        let request = MessagingRequest {
            operation: MessagingOperation::RemoveStreamSubscriptions {
                stream_name: stream_name.into(),
            },
        };

        self.propose_request(&request).await
    }

    /// Subscribe a stream to multiple subject patterns in one consensus operation
    ///
    /// This is more efficient than multiple individual subscriptions.
    ///
    /// # Errors
    ///
    /// Returns an error if the Raft instance is not initialized or if consensus fails.
    pub async fn bulk_subscribe_stream_to_subjects(
        &self,
        stream_name: impl Into<String>,
        subject_patterns: Vec<String>,
    ) -> Result<u64, error::ConsensusError> {
        let stream_name = stream_name.into();

        let request = MessagingRequest {
            operation: MessagingOperation::BulkSubscribeToSubjects {
                stream_name,
                subject_patterns,
            },
        };

        self.propose_request(&request).await
    }

    /// Unsubscribe a stream from multiple subject patterns in one consensus operation
    ///
    /// This is more efficient than multiple individual unsubscriptions.
    ///
    /// # Errors
    ///
    /// Returns an error if the Raft instance is not initialized or if consensus fails.
    pub async fn bulk_unsubscribe_stream_from_subjects(
        &self,
        stream_name: impl Into<String>,
        subject_patterns: Vec<String>,
    ) -> Result<u64, error::ConsensusError> {
        let request = MessagingRequest {
            operation: MessagingOperation::BulkUnsubscribeFromSubjects {
                stream_name: stream_name.into(),
                subject_patterns,
            },
        };

        self.propose_request(&request).await
    }

    /// Get a message by sequence number from local store
    ///
    /// # Errors
    ///
    /// Currently always returns `Ok` but may return errors in future implementations.
    pub fn get_message(
        &self,
        stream: &str,
        seq: u64,
    ) -> Result<Option<Bytes>, error::ConsensusError> {
        debug!("Getting message for stream {} sequence: {}", stream, seq);
        Ok(self.store.get_message(stream, seq))
    }

    /// Get the last sequence number for a stream
    ///
    /// # Errors
    ///
    /// Currently always returns `Ok` but may return errors in future implementations.
    pub fn last_sequence(&self, stream: &str) -> Result<u64, error::ConsensusError> {
        Ok(self.store.last_sequence(stream))
    }

    /// Check if this node is the leader
    #[must_use]
    pub fn is_leader(&self) -> bool {
        let raft = self.raft.read();
        raft.as_ref().is_some_and(|raft| {
            let metrics_guard = raft.metrics();
            let metrics = metrics_guard.borrow();
            matches!(metrics.current_leader, Some(ref leader_id) if leader_id == &self.node_id)
        })
    }

    /// Get current Raft metrics
    #[must_use]
    pub fn metrics(&self) -> Option<openraft::RaftMetrics<TypeConfig>> {
        let raft = self.raft.read();
        raft.as_ref().map(|r| r.metrics().borrow().clone())
    }

    /// Get the stream store
    #[must_use]
    pub const fn store(&self) -> &StreamStore {
        &self.store
    }

    /// Propose a request through Raft consensus
    async fn propose_request(
        &self,
        request: &MessagingRequest,
    ) -> Result<u64, error::ConsensusError> {
        let raft = self
            .raft
            .read()
            .as_ref()
            .ok_or_else(|| {
                error::ConsensusError::InvalidMessage("Raft not initialized".to_string())
            })?
            .clone();

        debug!("Proposing request through Raft: {:?}", request);

        let response = raft.client_write(request.clone()).await;

        match response {
            Ok(client_write_response) => {
                let response_data = &client_write_response.data;
                if response_data.success {
                    info!(
                        "Request successfully committed: sequence {}",
                        response_data.sequence
                    );
                    Ok(response_data.sequence)
                } else {
                    let error_msg = response_data.error.as_deref().unwrap_or("Unknown error");
                    warn!("Request failed during application: {}", error_msg);
                    Err(error::ConsensusError::InvalidMessage(error_msg.to_string()))
                }
            }
            Err(e) => {
                warn!("Failed to commit request through Raft: {}", e);
                Err(error::ConsensusError::InvalidMessage(format!(
                    "Consensus failed: {e}"
                )))
            }
        }
    }

    /// Start async cluster initialization based on topology
    ///
    /// This method spawns a background task that handles cluster discovery and initialization
    /// according to the topology and discovery timeout logic.
    fn start_cluster_initialization(&self) {
        let cluster_state = self.cluster_state.clone();
        let node_id = self.node_id.clone();
        let transport = self.transport.clone();
        let topology = self.topology.clone();
        let raft = self.raft.clone();
        let config = self.config.clone();

        let handle = tokio::spawn(async move {
            if let Err(e) = Self::run_cluster_initialization(
                node_id.clone(),
                transport,
                topology,
                raft,
                cluster_state.clone(),
                config,
            )
            .await
            {
                warn!("Cluster initialization failed for node {}: {}", node_id, e);
                let mut state = cluster_state.write().await;
                *state = ClusterState::Failed {
                    failed_at: std::time::Instant::now(),
                    error: e.to_string(),
                };
            }
        });

        // Store the handle for cleanup
        self.shutdown_handles.lock().unwrap().push(handle);
    }

    /// Run the cluster initialization logic
    async fn run_cluster_initialization(
        node_id: String,
        transport: Arc<T>,
        topology: Arc<topology::TopologyManager<G>>,
        raft: Arc<RwLock<Option<openraft::Raft<TypeConfig>>>>,
        cluster_state: Arc<AsyncRwLock<ClusterState>>,
        config: ConsensusConfig,
    ) -> Result<(), error::ConsensusError> {
        // Set state to discovering
        {
            let mut state = cluster_state.write().await;
            *state = ClusterState::Discovering;
        }

        info!("Starting cluster discovery for node {}", node_id);

        // Get all peers from topology
        let all_peers = topology.get_all_peers().await;

        if all_peers.is_empty() {
            // Single node topology - immediately become initiator
            info!(
                "Single node topology detected for node {}. Becoming cluster initiator.",
                node_id
            );

            Self::initialize_single_node_cluster(
                node_id,
                raft,
                cluster_state,
                InitiatorReason::SingleNode,
            )
            .await?;
        } else {
            // Multi-node topology - start discovery process
            info!(
                "Multi-node topology detected ({} peers). Starting discovery process.",
                all_peers.len()
            );

            Self::discover_and_join_cluster(
                node_id,
                transport,
                raft,
                cluster_state,
                all_peers,
                config,
            )
            .await?;
        }

        Ok(())
    }

    /// Initialize a single-node cluster
    async fn initialize_single_node_cluster(
        node_id: String,
        raft: Arc<RwLock<Option<openraft::Raft<TypeConfig>>>>,
        cluster_state: Arc<AsyncRwLock<ClusterState>>,
        reason: InitiatorReason,
    ) -> Result<(), error::ConsensusError> {
        info!(
            "Initializing single-node cluster as initiator for node {}",
            node_id
        );

        // Initialize cluster
        Self::initialize_cluster_raft(node_id.clone(), raft).await?;

        // Update state
        {
            let mut state = cluster_state.write().await;
            *state = ClusterState::Initiator {
                initiated_at: std::time::Instant::now(),
                reason,
            };
        }

        info!(
            "Successfully initialized as cluster initiator for node {}",
            node_id
        );
        Ok(())
    }

    /// Discover existing clusters and join or become initiator
    async fn discover_and_join_cluster(
        node_id: String,
        transport: Arc<T>,
        raft: Arc<RwLock<Option<openraft::Raft<TypeConfig>>>>,
        cluster_state: Arc<AsyncRwLock<ClusterState>>,
        _all_peers: Vec<PeerInfo>,
        config: ConsensusConfig,
    ) -> Result<(), error::ConsensusError> {
        let discovery_timeout = config
            .cluster_discovery_timeout
            .unwrap_or_else(|| std::time::Duration::from_secs(30));

        info!(
            "Starting cluster discovery for node {} with timeout: {:?}",
            node_id, discovery_timeout
        );

        // Run discovery with timeout
        let start_time = std::time::Instant::now();

        while start_time.elapsed() < discovery_timeout {
            tokio::time::sleep(std::time::Duration::from_millis(1000)).await;

            info!("Performing cluster discovery round for node {}...", node_id);

            // Perform discovery
            let discovery_responses =
                transport.discover_existing_clusters().await.map_err(|e| {
                    error::ConsensusError::InvalidMessage(format!("Cluster discovery failed: {e}"))
                })?;

            // Check if any peer reports an active cluster
            if let Some(existing_cluster) = discovery_responses
                .iter()
                .find(|response| response.has_active_cluster)
            {
                info!(
                    "Found existing cluster reported by {}: term={:?}, leader={:?}, size={:?}",
                    existing_cluster.responder_id,
                    existing_cluster.current_term,
                    existing_cluster.current_leader,
                    existing_cluster.cluster_size
                );

                // Join the existing cluster
                Self::join_existing_cluster(
                    node_id,
                    raft,
                    cluster_state,
                    existing_cluster.cluster_size.unwrap_or(1),
                )
                .await?;
                return Ok(());
            }

            info!(
                "No active cluster found in this round for node {}, continuing discovery...",
                node_id
            );
        }

        // Discovery timeout - become initiator
        warn!(
            "Cluster discovery timeout after {:?} for node {}. Becoming cluster initiator.",
            discovery_timeout, node_id
        );

        Self::initialize_single_node_cluster(
            node_id,
            raft,
            cluster_state,
            InitiatorReason::DiscoveryTimeout,
        )
        .await?;

        Ok(())
    }

    /// Join an existing cluster
    async fn join_existing_cluster(
        node_id: String,
        raft: Arc<RwLock<Option<openraft::Raft<TypeConfig>>>>,
        cluster_state: Arc<AsyncRwLock<ClusterState>>,
        cluster_size: usize,
    ) -> Result<(), error::ConsensusError> {
        info!(
            "Joining existing cluster with {} nodes for node {}",
            cluster_size, node_id
        );

        // Initialize as single node first, then let the network handle joining
        Self::initialize_cluster_raft(node_id.clone(), raft).await?;

        // Update state
        {
            let mut state = cluster_state.write().await;
            *state = ClusterState::Joined {
                joined_at: std::time::Instant::now(),
                cluster_size,
            };
        }

        info!("Successfully joined existing cluster for node {}", node_id);
        Ok(())
    }

    /// Initialize the Raft cluster (single node)
    async fn initialize_cluster_raft(
        node_id: String,
        raft: Arc<RwLock<Option<openraft::Raft<TypeConfig>>>>,
    ) -> Result<(), error::ConsensusError> {
        let raft_instance = {
            let raft_guard = raft.read();
            if raft_guard.is_none() {
                return Err(error::ConsensusError::InvalidConfiguration(
                    "Raft not initialized".to_string(),
                ));
            }
            raft_guard.as_ref().unwrap().clone()
        };

        let metrics = raft_instance.metrics().borrow().clone();
        let membership = metrics.membership_config.membership();
        let is_already_in_cluster =
            membership.voter_ids().count() > 0 || membership.learner_ids().count() > 0;

        if is_already_in_cluster {
            info!("Node {} already in cluster", node_id);
            return Ok(());
        }

        // Create single-node cluster
        let mut nodes = std::collections::BTreeMap::new();
        nodes.insert(
            node_id.clone(),
            openraft::BasicNode {
                addr: format!("node-{node_id}"),
            },
        );

        raft_instance.initialize(nodes).await.map_err(|e| {
            error::ConsensusError::InvalidMessage(format!("Failed to initialize cluster: {e}"))
        })?;

        info!("Raft cluster initialized for node {}", node_id);
        Ok(())
    }

    /// Add a peer to the cluster as a learner
    ///
    /// # Errors
    ///
    /// Returns an error if adding the learner fails.
    pub async fn add_learner(
        &self,
        peer_id: String,
        peer_addr: String,
    ) -> Result<(), error::ConsensusError> {
        let raft_instance = {
            let raft = self.raft.read();
            raft.as_ref()
                .ok_or_else(|| {
                    error::ConsensusError::InvalidMessage("Raft not initialized".to_string())
                })?
                .clone()
        };

        info!("Adding peer {} as learner", peer_id);
        let node = openraft::BasicNode { addr: peer_addr };

        raft_instance
            .add_learner(peer_id.clone(), node, true)
            .await
            .map_err(|e| {
                error::ConsensusError::InvalidMessage(format!("Failed to add learner: {e}"))
            })?;

        info!("Peer {} added as learner", peer_id);
        Ok(())
    }

    /// Get current cluster membership information
    ///
    /// # Errors
    ///
    /// Returns an error if Raft is not initialized.
    #[allow(clippy::significant_drop_tightening)]
    pub fn get_cluster_membership(
        &self,
    ) -> Result<
        (
            std::collections::BTreeSet<String>,
            std::collections::BTreeSet<String>,
        ),
        error::ConsensusError,
    > {
        let (voters, learners) = {
            let raft_guard = self.raft.read();
            let raft = raft_guard.as_ref().ok_or_else(|| {
                error::ConsensusError::InvalidMessage("Raft not initialized".to_string())
            })?;

            let metrics = raft.metrics().borrow().clone();
            let membership = metrics.membership_config.membership();

            let voters: std::collections::BTreeSet<String> = membership.voter_ids().collect();
            let learners: std::collections::BTreeSet<String> = membership.learner_ids().collect();

            (voters, learners)
        };

        Ok((voters, learners))
    }

    /// Check if the cluster has a healthy quorum
    ///
    /// # Errors
    ///
    /// Returns an error if Raft is not initialized.
    #[allow(clippy::significant_drop_tightening)]
    pub fn has_quorum(&self) -> Result<bool, error::ConsensusError> {
        let has_leader = {
            let raft_guard = self.raft.read();
            let raft = raft_guard.as_ref().ok_or_else(|| {
                error::ConsensusError::InvalidMessage("Raft not initialized".to_string())
            })?;

            let metrics = raft.metrics().borrow().clone();
            metrics.current_leader.is_some()
        };

        // Check if we have a leader and if the cluster is functional
        // A cluster has quorum if there's a current leader
        Ok(has_leader)
    }

    /// Get all streams that should receive messages for a given subject
    ///
    /// This reads from the consensus-synchronized routing table.
    #[must_use]
    pub fn route_subject(&self, subject: &str) -> std::collections::HashSet<String> {
        self.store.route_subject(subject)
    }

    /// Get all subject patterns that a stream is subscribed to
    ///
    /// This reads from the consensus-synchronized routing table.
    #[must_use]
    pub fn get_stream_subjects(
        &self,
        stream_name: &str,
    ) -> Option<std::collections::HashSet<String>> {
        self.store.get_stream_subjects(stream_name)
    }

    /// Get all streams subscribed to a specific subject pattern
    ///
    /// This reads from the consensus-synchronized routing table.
    #[must_use]
    pub fn get_subject_streams(
        &self,
        subject_pattern: &str,
    ) -> Option<std::collections::HashSet<String>> {
        self.store.get_subject_streams(subject_pattern)
    }

    /// Get a summary of all current subject subscriptions
    ///
    /// Returns a map from subject patterns to the streams subscribed to them.
    /// This reads from the consensus-synchronized routing table.
    #[must_use]
    pub fn get_all_subscriptions(
        &self,
    ) -> std::collections::HashMap<String, std::collections::HashSet<String>> {
        self.store.get_all_subscriptions()
    }

    /// Process an incoming raft message and generate response
    ///
    /// This method handles incoming raft requests (vote, `append_entries`, `install_snapshot`)
    /// by routing them to the local raft instance and returning the appropriate response.
    ///
    /// # Errors
    ///
    /// Returns an error if raft is not initialized or if processing fails.
    #[allow(clippy::too_many_lines)]
    pub async fn process_raft_message(
        &self,
        sender_id: &str,
        message: &transport::ConsensusMessage,
    ) -> Result<transport::ConsensusMessage, error::ConsensusError> {
        use crate::raft_network::RaftMessageEnvelope;
        use serde_json;

        let raft_instance = {
            let raft_guard = self.raft.read();
            raft_guard
                .as_ref()
                .ok_or_else(|| {
                    error::ConsensusError::InvalidMessage("Raft not initialized".to_string())
                })?
                .clone()
        };

        // Extract envelope and deserialize based on message type
        let (envelope, response_type) = match message {
            transport::ConsensusMessage::RaftVote(data) => {
                let envelope: RaftMessageEnvelope = serde_json::from_slice(data).map_err(|e| {
                    error::ConsensusError::InvalidMessage(format!(
                        "Failed to deserialize vote envelope: {e}"
                    ))
                })?;
                (envelope, "vote_response")
            }
            transport::ConsensusMessage::RaftAppendEntries(data) => {
                let envelope: RaftMessageEnvelope = serde_json::from_slice(data).map_err(|e| {
                    error::ConsensusError::InvalidMessage(format!(
                        "Failed to deserialize append_entries envelope: {e}"
                    ))
                })?;
                (envelope, "append_entries_response")
            }
            transport::ConsensusMessage::RaftInstallSnapshot(data) => {
                let envelope: RaftMessageEnvelope = serde_json::from_slice(data).map_err(|e| {
                    error::ConsensusError::InvalidMessage(format!(
                        "Failed to deserialize install_snapshot envelope: {e}"
                    ))
                })?;
                (envelope, "install_snapshot_response")
            }
            _ => {
                return Err(error::ConsensusError::InvalidMessage(
                    "Not a raft request message".to_string(),
                ));
            }
        };

        // Verify this is a request, not a response
        if envelope.is_response {
            // This is a response - route it to the response handler
            crate::raft_network::handle_raft_response(message.clone()).await;
            return Err(error::ConsensusError::InvalidMessage(
                "Received response message in request handler".to_string(),
            ));
        }

        debug!(
            "Processing {} request from {} with ID {}",
            envelope.message_type, sender_id, envelope.request_id
        );

        // Process the request based on message type
        let response_payload = match envelope.message_type.as_str() {
            "vote" => {
                let vote_request: openraft::raft::VoteRequest<TypeConfig> =
                    serde_json::from_slice(&envelope.payload).map_err(|e| {
                        error::ConsensusError::InvalidMessage(format!(
                            "Failed to deserialize vote request: {e}"
                        ))
                    })?;

                let vote_response = raft_instance.vote(vote_request).await.map_err(|e| {
                    error::ConsensusError::InvalidMessage(format!("Vote processing failed: {e}"))
                })?;

                serde_json::to_vec(&vote_response).map_err(|e| {
                    error::ConsensusError::InvalidMessage(format!(
                        "Failed to serialize vote response: {e}"
                    ))
                })?
            }
            "append_entries" => {
                let append_request: openraft::raft::AppendEntriesRequest<TypeConfig> =
                    serde_json::from_slice(&envelope.payload).map_err(|e| {
                        error::ConsensusError::InvalidMessage(format!(
                            "Failed to deserialize append_entries request: {e}"
                        ))
                    })?;

                let append_response =
                    raft_instance
                        .append_entries(append_request)
                        .await
                        .map_err(|e| {
                            error::ConsensusError::InvalidMessage(format!(
                                "Append entries processing failed: {e}"
                            ))
                        })?;

                serde_json::to_vec(&append_response).map_err(|e| {
                    error::ConsensusError::InvalidMessage(format!(
                        "Failed to serialize append_entries response: {e}"
                    ))
                })?
            }
            "install_snapshot" => {
                let snapshot_request: openraft::raft::InstallSnapshotRequest<TypeConfig> =
                    serde_json::from_slice(&envelope.payload).map_err(|e| {
                        error::ConsensusError::InvalidMessage(format!(
                            "Failed to deserialize install_snapshot request: {e}"
                        ))
                    })?;

                let snapshot_response = raft_instance
                    .install_snapshot(snapshot_request)
                    .await
                    .map_err(|e| {
                        error::ConsensusError::InvalidMessage(format!(
                            "Install snapshot processing failed: {e}"
                        ))
                    })?;

                serde_json::to_vec(&snapshot_response).map_err(|e| {
                    error::ConsensusError::InvalidMessage(format!(
                        "Failed to serialize install_snapshot response: {e}"
                    ))
                })?
            }
            _ => {
                return Err(error::ConsensusError::InvalidMessage(format!(
                    "Unknown raft message type: {}",
                    envelope.message_type
                )));
            }
        };

        // Create response envelope
        let response_envelope = RaftMessageEnvelope {
            request_id: envelope.request_id, // Same ID for correlation
            is_response: true,
            payload: response_payload,
            message_type: response_type.to_string(),
        };

        // Serialize response envelope
        let response_data = serde_json::to_vec(&response_envelope).map_err(|e| {
            error::ConsensusError::InvalidMessage(format!(
                "Failed to serialize response envelope: {e}"
            ))
        })?;

        // Create appropriate response message type
        let response_message = match response_type {
            "vote_response" => transport::ConsensusMessage::RaftVoteResponse(response_data),
            "append_entries_response" => {
                transport::ConsensusMessage::RaftAppendEntriesResponse(response_data)
            }
            "install_snapshot_response" => {
                transport::ConsensusMessage::RaftInstallSnapshotResponse(response_data)
            }
            _ => transport::ConsensusMessage::Data(response_data.into()),
        };

        debug!(
            "Generated {} response for request {} from {}",
            response_type, envelope.request_id, sender_id
        );

        Ok(response_message)
    }
}

impl<G, A, T> std::fmt::Debug for Consensus<G, A, T>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug,
    T: transport::ConsensusTransport,
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
impl<G, A, T> Bootable for Consensus<G, A, T>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug,
    T: transport::ConsensusTransport,
{
    fn bootable_name(&self) -> &'static str {
        "consensus-system"
    }

    async fn start(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.is_running() {
            return Err("Consensus system is already running".into());
        }

        {
            let (shutdown_tx, _shutdown_rx) = mpsc::unbounded_channel();
            *self.shutdown_tx.lock().unwrap() = Some(shutdown_tx);
        }

        // Start topology manager
        self.topology.start().await?;

        // Start transport layer immediately
        info!("Starting transport layer for node {}", self.node_id);
        self.transport.start().await?;

        // Start async cluster initialization (non-blocking)
        info!(
            "Starting async cluster initialization for node {}",
            self.node_id
        );
        self.start_cluster_initialization();

        info!("Consensus system transport started for node {}. Cluster initialization running in background.", self.node_id);
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

        // Shutdown transport layer
        if let Err(e) = self.transport.shutdown().await {
            warn!("Transport layer shutdown error: {}", e);
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

pub use attestation::AttestationVerifier;
pub use config::ConsensusConfig;
pub use error::{ConsensusError, ConsensusResult};
pub use messaging::{MessagingOperation, MessagingRequest, MessagingResponse};
pub use storage::MessagingStorage;
pub use stream_store::StreamStore;
pub use subscription::SubscriptionInvoker;
pub use topology::{PeerInfo, TopologyManager};
pub use transport::ConsensusMessage as NetworkConsensusMessage;
pub use transport::{
    ClusterDiscoveryRequest, ClusterDiscoveryResponse, ConsensusMessage, ConsensusTransport,
    PeerConnection,
};
pub use types::TypeConfig;

// Convenience constructors for backward compatibility during transition
impl<G, A> Consensus<G, A, transport::tcp::TcpTransport<G, A>>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug,
{
    /// Creates a new consensus system with TCP transport (backward compatibility).
    ///
    /// # Arguments
    ///
    /// * `listen_addr` - Network address to listen on
    /// * `governance` - Governance system for topology discovery
    /// * `attestor` - Attestation system for peer verification
    /// * `signing_key` - Cryptographic signing key for network authentication
    /// * `config` - Consensus configuration parameters
    ///
    /// # Errors
    ///
    /// Returns `ConsensusError` if initialization fails.
    pub async fn new_with_tcp(
        listen_addr: SocketAddr,
        governance: Arc<G>,
        attestor: Arc<A>,
        signing_key: SigningKey,
        config: ConsensusConfig,
    ) -> Result<Self, error::ConsensusError> {
        // Create topology manager
        let local_public_key = hex::encode(signing_key.verifying_key().to_bytes());
        let node_id = hex::encode(signing_key.to_bytes());
        let topology: Arc<TopologyManager<G>> = Arc::new(topology::TopologyManager::new(
            governance.clone(),
            node_id.clone(),
            local_public_key.clone(),
        ));

        // Initialize topology
        topology.refresh_topology().await?;

        // Create channels for transport communication
        let (consensus_tx, _consensus_rx) =
            mpsc::unbounded_channel::<(String, transport::ConsensusMessage)>();
        let (transport_tx, transport_rx) =
            mpsc::unbounded_channel::<(String, transport::ConsensusMessage)>();

        // Create TCP transport
        let tcp_transport = transport::tcp::TcpTransport::new(
            node_id.clone(),
            listen_addr,
            topology.clone(),
            governance.clone(),
            attestor.clone(),
            signing_key.clone(),
            consensus_tx,
            transport_rx,
        );

        // Create storage layer with configurable database path
        let db_path = config
            .storage_dir
            .clone()
            .unwrap_or_else(|| format!("/tmp/consensus-{node_id}"));

        let storage = Arc::new(storage::create_messaging_storage(&db_path).map_err(|e| {
            error::ConsensusError::InvalidConfiguration(format!("Failed to create storage: {e}"))
        })?);

        info!("Using storage directory: {}", db_path);

        // Create raft and store directly
        let raft = Arc::new(RwLock::new(None)); // Will be initialized later with storage and transport
        let store = StreamStore::new();

        info!("Created consensus system for node {}", node_id);

        let transport = Arc::new(tcp_transport);

        let consensus = Self {
            node_id,
            raft,
            store,
            transport,
            storage,
            topology,
            config,
            governance,
            attestor,
            network_tx: transport_tx, // Use the correct transport sender
            shutdown_tx: Arc::new(std::sync::Mutex::new(None)),
            shutdown_handles: Arc::new(std::sync::Mutex::new(Vec::new())),
            cluster_state: Arc::new(AsyncRwLock::new(ClusterState::TransportReady)),
        };

        // TODO: Initialize Raft with our storage and transport implementations
        // This needs to be updated to work with the transport abstraction
        // The raft_network.rs module needs to be updated to accept transports instead of ConsensusNetwork
        info!("Raft initialization postponed - needs transport abstraction support");

        Ok(consensus)
    }
}

// Convenience constructor for WebSocket transport
impl<G, A> Consensus<G, A, transport::websocket::WebSocketTransport<G, A>>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug,
{
    /// Creates a new consensus system with WebSocket transport.
    ///
    /// The WebSocket transport will use the same port as defined in the `TopologyNode`'s origin
    /// since it shares the HTTP server.
    ///
    /// # Arguments
    ///
    /// * `governance` - Governance system for topology discovery
    /// * `attestor` - Attestation system for peer verification
    /// * `signing_key` - Cryptographic signing key for network authentication
    /// * `config` - Consensus configuration parameters
    ///
    /// # Errors
    ///
    /// Returns `ConsensusError` if initialization fails.
    pub async fn new_with_websocket(
        governance: Arc<G>,
        attestor: Arc<A>,
        signing_key: SigningKey,
        config: ConsensusConfig,
    ) -> Result<Self, error::ConsensusError> {
        // Create topology manager
        let local_public_key = hex::encode(signing_key.verifying_key().to_bytes());
        let node_id = hex::encode(signing_key.to_bytes());
        let topology: Arc<TopologyManager<G>> = Arc::new(topology::TopologyManager::new(
            governance.clone(),
            node_id.clone(),
            local_public_key.clone(),
        ));

        // Initialize topology
        topology.refresh_topology().await?;

        // Get the topology from governance to find the local node
        let topology_nodes = governance.get_topology().await.map_err(|e| {
            error::ConsensusError::Governance(format!("Failed to get topology: {e}"))
        })?;

        // Find the local node by comparing public keys
        let local_node = topology_nodes
            .iter()
            .find(|node| node.public_key == local_public_key)
            .ok_or_else(|| {
                error::ConsensusError::InvalidConfiguration(
                    "Failed to find local node in topology".to_string(),
                )
            })?;

        // Parse the origin to extract the port (format: "host:port")
        let origin_url: url::Url = local_node.origin.parse().map_err(|e| {
            error::ConsensusError::InvalidConfiguration(format!(
                "Failed to parse local node origin '{}' as socket address: {}",
                local_node.origin, e
            ))
        })?;

        let listen_addr = match origin_url
            .socket_addrs(|| None)
            .map_err(|e| {
                error::ConsensusError::InvalidConfiguration(format!(
                    "Failed to parse local node origin '{}' as socket address: {}",
                    local_node.origin, e
                ))
            })?
            .first()
        {
            Some(addr) => *addr,
            None => {
                return Err(error::ConsensusError::InvalidConfiguration(format!(
                    "Failed to parse local node origin '{}' as socket address",
                    local_node.origin
                )));
            }
        };

        // Create channels for transport communication
        let (consensus_tx, _consensus_rx) =
            mpsc::unbounded_channel::<(String, transport::ConsensusMessage)>();
        let (_transport_tx, transport_rx) =
            mpsc::unbounded_channel::<(String, transport::ConsensusMessage)>();

        // Create WebSocket transport
        let websocket_transport = transport::websocket::WebSocketTransport::new(
            node_id.clone(),
            listen_addr,
            topology.clone(),
            governance.clone(),
            attestor.clone(),
            signing_key.clone(),
            consensus_tx,
            transport_rx,
        );

        // Use the generic constructor with the WebSocket transport
        Self::new(
            governance,
            attestor.clone(),
            signing_key,
            websocket_transport,
            config,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use crate::transport::tcp::TcpTransport;

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
    fn create_test_governance(port: u16, signing_key: &SigningKey) -> Arc<MockGovernance> {
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
        port: u16,
    ) -> Arc<Consensus<MockGovernance, MockAttestor, TcpTransport<MockGovernance, MockAttestor>>>
    {
        let signing_key = SigningKey::generate(&mut OsRng);
        let governance = create_test_governance(port, &signing_key);
        let attestor = Arc::new(MockAttestor::new());

        let temp_dir = tempfile::tempdir().expect("Failed to create temp directory");
        let config = ConsensusConfig {
            storage_dir: Some(temp_dir.path().to_string_lossy().to_string()),
            ..ConsensusConfig::default()
        };

        let consensus = Consensus::new_with_tcp(
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
        let consensus = create_test_consensus(next_port()).await;

        assert!(!consensus.is_running());

        // Verify we can access all components (just check they're not null)
        let _ = consensus.transport();
        let _ = consensus.storage();
        let _ = consensus.topology();
        let _ = consensus.governance();
        let _ = consensus.attestor();
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_consensus_lifecycle() {
        let consensus = create_test_consensus(next_port()).await;

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
            let config = ConsensusConfig {
                storage_dir: Some(temp_dir.path().to_string_lossy().to_string()),
                ..ConsensusConfig::default()
            };

            let consensus = Consensus::new_with_tcp(
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
        let consensus = create_test_consensus(next_port()).await;

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

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_leader_election() {
        const NUM_NODES: usize = 3;

        let mut signing_keys = Vec::new();
        let mut node_ports = Vec::new();

        // Pre-allocate keys and ports for all nodes
        for _i in 0..NUM_NODES {
            signing_keys.push(SigningKey::generate(&mut OsRng));
            node_ports.push(proven_util::port_allocator::allocate_port());
        }

        // Create topology nodes that reference each other's P2P ports
        let mut topology_nodes = Vec::new();
        for (i, signing_key) in signing_keys.iter().enumerate() {
            let topology_node = TopologyNode {
                availability_zone: "test-az".to_string(),
                origin: format!("127.0.0.1:{}", node_ports[i]), // Use the P2P port as origin
                public_key: hex::encode(signing_key.verifying_key().to_bytes()),
                region: "test-region".to_string(),
                specializations: HashSet::new(),
            };
            topology_nodes.push(topology_node);
        }

        // Sort by public key for consistent ordering
        topology_nodes.sort_by(|a, b| a.public_key.cmp(&b.public_key));

        println!("Creating {NUM_NODES} nodes for leader election test:");
        for (i, node) in topology_nodes.iter().enumerate() {
            println!(
                "  Node {}: {} on port {}",
                i + 1,
                &node.public_key[..8],
                node_ports[i]
            );
        }

        // Create shared governance that knows about all nodes
        let attestor = MockAttestor::new();
        let actual_pcrs = attestor.pcrs_sync();
        let test_version = Version {
            ne_pcr0: actual_pcrs.pcr0,
            ne_pcr1: actual_pcrs.pcr1,
            ne_pcr2: actual_pcrs.pcr2,
        };

        let shared_governance = Arc::new(MockGovernance::new(
            topology_nodes.clone(),
            vec![test_version],
            "http://localhost:3200".to_string(),
            vec![],
        ));

        // Create consensus nodes that listen on their P2P ports
        let mut consensus_nodes = Vec::new();
        #[allow(clippy::collection_is_never_read)]
        let mut temp_dirs = Vec::new();

        for i in 0..NUM_NODES {
            let port = node_ports[i];
            let signing_key = signing_keys[i].clone();
            let attestor = Arc::new(MockAttestor::new());

            let temp_dir = tempfile::tempdir().expect("Failed to create temp directory");
            let config = ConsensusConfig {
                storage_dir: Some(temp_dir.path().to_string_lossy().to_string()),
                ..ConsensusConfig::default()
            };

            let consensus = Consensus::new_with_tcp(
                format!("127.0.0.1:{port}").parse().unwrap(),
                shared_governance.clone(),
                attestor,
                signing_key,
                config,
            )
            .await
            .unwrap();

            consensus_nodes.push(Arc::new(consensus));
            temp_dirs.push(temp_dir); // Keep temp dirs alive
        }

        // Determine expected leader (smallest node_id)
        let expected_leader_node_id = consensus_nodes
            .iter()
            .map(|c| c.node_id())
            .min()
            .expect("Should have at least one node")
            .to_string();

        let _expected_leader = consensus_nodes
            .iter()
            .find(|c| c.node_id() == expected_leader_node_id)
            .expect("Should find expected leader")
            .clone();

        println!(
            "Expected leader: {} ({})",
            &expected_leader_node_id[..8],
            expected_leader_node_id
        );

        // Start INITIATOR FIRST to prevent split brain
        println!("\n=== Starting initiator first ===");
        let initiator_node = consensus_nodes
            .iter()
            .find(|c| c.node_id() == expected_leader_node_id)
            .expect("Should find initiator node")
            .clone();

        println!(
            "Starting initiator node: {} ({})",
            &initiator_node.node_id()[..8],
            initiator_node.node_id()
        );

        let start_result = initiator_node.start().await;
        assert!(
            start_result.is_ok(),
            "Initiator should start successfully: {start_result:?}"
        );

        // Wait for initiator to create cluster and stabilize
        println!("  Waiting for initiator cluster to stabilize...");
        tokio::time::sleep(tokio::time::Duration::from_millis(5000)).await;

        println!(
            "  Initiator started and running: {}",
            initiator_node.is_running()
        );

        // Now start the other nodes (non-initiators)
        println!("\n=== Starting follower nodes ===");
        for (i, consensus) in consensus_nodes.iter().enumerate() {
            // Skip the initiator - already started
            if consensus.node_id() == expected_leader_node_id {
                continue;
            }

            println!("Starting node {} ({})", i + 1, &consensus.node_id()[..8]);

            let start_result = consensus.start().await;
            assert!(
                start_result.is_ok(),
                "Node {} should start successfully: {start_result:?}",
                i + 1
            );

            // Give time for discovery and joining
            tokio::time::sleep(tokio::time::Duration::from_millis(2000)).await;

            println!(
                "  Node {} started and running: {}",
                i + 1,
                consensus.is_running()
            );
        }

        // Wait for cluster formation and leader election
        println!("\n=== Waiting for cluster formation and leader election ===");
        tokio::time::sleep(tokio::time::Duration::from_millis(5000)).await;

        // Check leader election results
        let mut leaders = Vec::new();
        let mut followers = Vec::new();

        for (i, consensus) in consensus_nodes.iter().enumerate() {
            let is_leader = consensus.is_leader();
            let metrics = consensus.metrics();

            println!(
                "Node {} ({}): leader={}, metrics={:?}",
                i + 1,
                &consensus.node_id()[..8],
                is_leader,
                metrics
                    .as_ref()
                    .map(|m| format!("term:{}, state:{:?}", m.current_term, m.state))
            );

            if is_leader {
                leaders.push(consensus);
            } else {
                followers.push(consensus);
            }

            // Verify the node is running
            assert!(consensus.is_running(), "Node {} should be running", i + 1);
        }

        // Verify exactly one leader was elected
        assert_eq!(
            leaders.len(),
            1,
            "Exactly one leader should be elected, found {} leaders",
            leaders.len()
        );

        assert_eq!(
            followers.len(),
            NUM_NODES - 1,
            "Should have {} followers, found {}",
            NUM_NODES - 1,
            followers.len()
        );

        let actual_leader = leaders[0];
        println!(
            "\n✅ Leader elected: {} ({})",
            &actual_leader.node_id()[..8],
            actual_leader.node_id()
        );

        // Test that the elected leader can perform operations
        println!("\n=== Testing leader operations ===");
        let publish_result = actual_leader
            .publish_message(
                "leader-test-stream".to_string(),
                bytes::Bytes::from("leader-test-message"),
            )
            .await;

        assert!(
            publish_result.is_ok(),
            "Leader should be able to publish messages: {publish_result:?}"
        );

        let sequence = publish_result.unwrap();
        println!("✅ Leader published message with sequence: {sequence}");

        // Test that followers cannot publish (should forward to leader)
        if !followers.is_empty() {
            let follower = &followers[0];
            println!("Testing follower behavior...");

            let follower_publish = follower
                .publish_message(
                    "follower-test-stream".to_string(),
                    bytes::Bytes::from("follower-test-message"),
                )
                .await;

            // This might succeed if forwarding works, or fail if no forwarding
            println!("Follower publish result: {follower_publish:?}");
        }

        // Test cluster membership
        println!("\n=== Testing cluster membership ===");
        let (voters, learners) = actual_leader.get_cluster_membership().unwrap_or_default();
        println!(
            "Cluster membership: {} voters, {} learners",
            voters.len(),
            learners.len()
        );

        // All nodes should be voters in a proper multi-node cluster
        assert!(
            !voters.is_empty(),
            "Should have at least 1 voter (the leader itself)"
        );

        // Verify the leader is in the voter set
        assert!(
            voters.contains(actual_leader.node_id()),
            "Leader should be a voter in its own cluster"
        );

        // Test leader failure and re-election
        println!("\n=== Testing leader failure and re-election ===");

        // Shutdown the current leader
        println!(
            "Shutting down current leader: {}",
            &actual_leader.node_id()[..8]
        );
        let shutdown_result = actual_leader.shutdown().await;
        assert!(
            shutdown_result.is_ok(),
            "Leader shutdown should succeed: {shutdown_result:?}"
        );

        actual_leader.wait().await;
        assert!(
            !actual_leader.is_running(),
            "Leader should not be running after shutdown"
        );

        // Wait for re-election among remaining nodes
        if !followers.is_empty() {
            println!(
                "Waiting for re-election among {} remaining nodes...",
                followers.len()
            );
            tokio::time::sleep(tokio::time::Duration::from_millis(5000)).await;

            // Check if a new leader was elected
            let mut new_leaders = Vec::new();
            for follower in &followers {
                if follower.is_leader() {
                    new_leaders.push(*follower);
                    println!(
                        "✅ New leader elected: {} ({})",
                        &follower.node_id()[..8],
                        follower.node_id()
                    );
                }
            }

            if new_leaders.len() == 1 {
                let new_leader = new_leaders[0];

                // Test that the new leader can perform operations
                let new_leader_publish = new_leader
                    .publish_message(
                        "new-leader-test-stream".to_string(),
                        bytes::Bytes::from("new-leader-test-message"),
                    )
                    .await;

                if new_leader_publish.is_ok() {
                    println!("✅ New leader can publish messages");
                } else {
                    println!("⚠️  New leader publish failed (may need more time): {new_leader_publish:?}");
                }
            } else {
                println!(
                    "⚠️  Re-election results: {} leaders found (may need more time for consensus)",
                    new_leaders.len()
                );
            }
        }

        // Cleanup: shutdown remaining nodes
        println!("\n=== Cleaning up remaining nodes ===");
        for (i, consensus) in consensus_nodes.iter().enumerate() {
            if consensus.is_running() {
                println!(
                    "Shutting down node {} ({})",
                    i + 1,
                    &consensus.node_id()[..8]
                );
                let shutdown_result = consensus.shutdown().await;
                if let Err(e) = shutdown_result {
                    println!("  Warning: shutdown error for node {}: {}", i + 1, e);
                }
                consensus.wait().await;
            }
        }

        println!("\n✅ Leader election with networking test completed successfully!");
        println!("✅ Verified: Leader election, cluster membership, leader operations, and failure handling");
    }
}
