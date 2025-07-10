//! Main consensus implementation
//!
//! This is the central component that owns all business logic and coordinates
//! between transport, storage, COSE, attestation, and Raft.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use axum::Router;
use bytes::Bytes;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};
use uuid::Uuid;

use proven_attestation::Attestor;
use proven_bootable::Bootable;
use proven_governance::Governance;

use crate::NodeId;
// use crate::cluster_discovery::{ClusterDiscovery, ClusterDiscoveryConfig, DiscoveryResult};
use crate::config::ConsensusConfig;
use crate::error::{ConsensusResult, Error};
use crate::global::{
    GlobalRequest, GlobalResponse, GlobalTypeConfig, global_manager::GlobalManager,
    global_state::GlobalState,
};
use crate::local::LocalStreamOperation;
use crate::network::messages::{ApplicationMessage, ClusterDiscoveryResponse};
use crate::network::network_manager::NetworkManager;
use crate::network::{ClusterState, InitiatorReason};
use crate::operations::{GlobalOperation, RoutingOperation, StreamManagementOperation};
use crate::orchestrator::Orchestrator;
use crate::router::ConsensusOperation;

// Type aliases for complex handler types
type AppMessageHandlerType =
    Option<Arc<dyn Fn(NodeId, ApplicationMessage, Option<Uuid>) + Send + Sync>>;
type RaftMessageHandlerType =
    Option<Arc<dyn Fn(NodeId, crate::network::messages::RaftMessage, Option<Uuid>) + Send + Sync>>;

/// Main consensus system that coordinates application-level logic
/// Uses both NetworkManager (pure networking) and GlobalManager (global consensus)
pub struct Consensus<G, A>
where
    G: Governance + Send + Sync + 'static,
    A: Attestor + Send + Sync + 'static,
{
    /// Pure networking manager
    network_manager: Arc<NetworkManager<G, A>>,
    /// Global consensus manager
    global_manager: Arc<GlobalManager<G, A>>,
    /// Node identifier (public key)
    node_id: NodeId,
    /// Global state store
    global_state: Arc<GlobalState>,
    /// PubSub manager for messaging
    pubsub_manager: Arc<crate::pubsub::PubSubManager<G, A>>,
    /// Stream bridge for PubSub->Stream integration
    stream_bridge: Arc<crate::pubsub::StreamBridge<G, A>>,
    /// Hierarchical consensus orchestrator
    hierarchical_orchestrator: Arc<Orchestrator<G, A>>,
    /// Background task handles
    task_handles: Arc<std::sync::Mutex<Vec<JoinHandle<()>>>>,
}

impl<G, A> Consensus<G, A>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug + Clone,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug + Clone,
{
    /// Create a new consensus instance with unified configuration
    pub async fn new(config: ConsensusConfig<G, A>) -> ConsensusResult<Self> {
        // Generate node ID from public key
        let node_id = NodeId::new(config.signing_key.verifying_key());

        info!("Creating consensus instance for node {}", node_id);

        // Create global state store
        let global_state = Arc::new(GlobalState::new());

        // Clone config values we'll need later
        let hierarchical_config = config.hierarchical_config.clone();
        let transport_config_clone = config.transport_config.clone();
        let raft_config_clone = config.raft_config.clone();
        let storage_config_clone = config.storage_config.clone();
        let cluster_join_retry_config_clone = config.cluster_join_retry_config.clone();

        // Create global storage factory based on configuration
        let storage_factory =
            crate::global::storage::create_global_storage_factory(&config.storage_config)?;
        let storage = storage_factory.create_storage(global_state.clone()).await?;

        // Create topology manager first (shared between managers)
        // The topology manager needs to be shared between GlobalManager and NetworkManager because:
        // - GlobalManager uses it for cluster discovery, adding/removing nodes, and tracking cluster membership
        // - NetworkManager uses it to validate incoming connections and determine which nodes to accept messages from
        // This shared state ensures both managers have a consistent view of the cluster topology
        let topology = Arc::new(crate::topology::TopologyManager::new(
            config.governance.clone(),
            node_id.clone(),
        ));

        // Create pure network manager first (handles transport, COSE, serialization)
        let network_manager = NetworkManager::new(
            config.signing_key.clone(),
            config.governance.clone(),
            config.attestor.clone(),
            node_id.clone(),
            config.transport_config,
            topology.clone(),
        )
        .await
        .map_err(|e| Error::Raft(format!("Failed to create NetworkManager: {e}")))?;

        // Wrap network manager in Arc for sharing
        let network_manager = Arc::new(network_manager);

        // Create channels for Raft network adapter communication
        let (raft_adapter_request_tx, raft_adapter_request_rx) =
            tokio::sync::mpsc::unbounded_channel();
        let (raft_adapter_response_tx, raft_adapter_response_rx) =
            tokio::sync::mpsc::unbounded_channel();

        // Create Raft network factory
        let network_factory = crate::network::adaptor::NetworkFactory::new(
            raft_adapter_request_tx.clone(),
            raft_adapter_response_rx,
        );

        // Create global manager
        let raft_config = Arc::new(config.raft_config);

        // Create GlobalManager
        let global_manager = Arc::new(
            GlobalManager::new(
                node_id.clone(),
                raft_config.as_ref().clone(),
                network_factory,
                storage,
                global_state.clone(),
                Some(topology.clone()),
                network_manager.clone(),
                config.cluster_join_retry_config.clone(),
                raft_adapter_request_rx,
                raft_adapter_response_tx,
            )
            .await
            .map_err(|e| Error::Raft(format!("Failed to create GlobalManager: {e}")))?,
        );

        // Create PubSub manager
        let pubsub_manager = Arc::new(crate::pubsub::PubSubManager::new(
            node_id.clone(),
            network_manager.clone(),
            topology.clone(),
        ));

        // Create hierarchical orchestrator
        info!("Creating hierarchical consensus orchestrator");
        // Create a new config for the orchestrator to avoid borrowing issues
        let orchestrator_config = ConsensusConfig {
            signing_key: config.signing_key.clone(),
            governance: config.governance.clone(),
            attestor: config.attestor.clone(),
            transport_config: transport_config_clone,
            raft_config: raft_config_clone,
            storage_config: storage_config_clone,
            cluster_discovery_timeout: config.cluster_discovery_timeout,
            cluster_join_retry_config: cluster_join_retry_config_clone,
            hierarchical_config: hierarchical_config.clone(),
            stream_storage_backend: config.stream_storage_backend.clone(),
        };
        let orchestrator = Orchestrator::new(
            orchestrator_config,
            hierarchical_config,
            Default::default(), // Use default views config
            global_manager.clone(),
            global_state.clone(),
            node_id.clone(),
        )
        .await?;
        let hierarchical_orchestrator = Arc::new(orchestrator);

        // Create StreamBridge with hierarchical orchestrator
        let stream_bridge = Arc::new(crate::pubsub::StreamBridge::new(
            pubsub_manager.clone(),
            global_manager.clone(),
            global_state.clone(),
            hierarchical_orchestrator.clone(),
        ));

        // Create consensus instance
        let consensus = Self {
            node_id: node_id.clone(),
            global_state,
            network_manager: network_manager.clone(),
            global_manager: global_manager.clone(),
            pubsub_manager,
            stream_bridge,
            hierarchical_orchestrator,
            task_handles: Arc::new(std::sync::Mutex::new(Vec::new())),
        };

        Ok(consensus)
    }

    /// Start internal components
    async fn start_internal(&self) -> ConsensusResult<()> {
        info!("Starting internal components for node {}", self.node_id);

        // Set up message handlers to route from NetworkManager to GlobalManager and PubSubManager
        let global_manager_clone = self.global_manager.clone();
        let pubsub_manager_clone = self.pubsub_manager.clone();
        let app_message_handler: AppMessageHandlerType =
            Some(Arc::new(move |node_id, message, correlation_id| {
                // Check if this is a PubSub message
                if let crate::network::messages::ApplicationMessage::PubSub(pubsub_msg) = message {
                    // Route to PubSub manager
                    let pubsub_manager = pubsub_manager_clone.clone();
                    tokio::spawn(async move {
                        if let Err(e) = pubsub_manager
                            .handle_message(node_id, *pubsub_msg, correlation_id)
                            .await
                        {
                            error!("Failed to handle PubSub message: {}", e);
                        }
                    });
                } else {
                    // Route other application messages to global manager
                    let global_manager = global_manager_clone.clone();
                    tokio::spawn(async move {
                        info!(
                            "Routing application message from {} to GlobalManager: {:?}",
                            node_id, message
                        );
                        if let Err(e) = global_manager
                            .handle_network_message(
                                node_id,
                                crate::network::messages::Message::Application(Box::new(message)),
                                correlation_id,
                            )
                            .await
                        {
                            error!("Failed to handle application message: {}", e);
                        }
                    });
                }
            }));

        let global_manager_clone2 = self.global_manager.clone();
        let raft_message_handler: RaftMessageHandlerType =
            Some(Arc::new(move |node_id, message, correlation_id| {
                // Route Raft messages to global manager
                let global_manager = global_manager_clone2.clone();
                tokio::spawn(async move {
                    if let Err(e) = global_manager
                        .handle_network_message(
                            node_id,
                            crate::network::messages::Message::Raft(message),
                            correlation_id,
                        )
                        .await
                    {
                        error!("Failed to handle Raft message: {}", e);
                    }
                });
            }));

        // Start the network transport and message handling
        info!("Starting network manager...");
        self.network_manager
            .start_network(app_message_handler, raft_message_handler)
            .await?;
        info!("Network manager started");

        // GlobalManager message processing is handled via the handlers we set up above

        info!("All internal components started successfully");
        Ok(())
    }

    /// Get a reference to the network manager
    pub fn network_manager(&self) -> Arc<NetworkManager<G, A>> {
        self.network_manager.clone()
    }

    /// Get a reference to the global manager
    pub fn global_manager(&self) -> Arc<GlobalManager<G, A>> {
        self.global_manager.clone()
    }

    /// Get the current term
    pub fn current_term(&self) -> Option<u64> {
        self.global_manager.current_term()
    }

    /// Get the current leader
    pub async fn current_leader(&self) -> Option<String> {
        self.global_manager.current_leader().await
    }

    /// Get the cluster size
    pub fn cluster_size(&self) -> Option<usize> {
        self.global_manager.cluster_size()
    }

    /// Check if this node is the leader
    pub fn is_leader(&self) -> bool {
        // For now, use a blocking call. In production, this should be refactored to async
        futures::executor::block_on(self.global_manager.is_leader())
    }

    /// Check if this node has an active cluster
    pub async fn has_active_cluster(&self) -> bool {
        self.global_manager.has_active_cluster().await
    }

    /// Discover existing clusters (delegated to GlobalManager)
    pub async fn discover_existing_clusters(
        &self,
    ) -> ConsensusResult<Vec<ClusterDiscoveryResponse>> {
        // Delegate to GlobalManager
        self.global_manager.discover_existing_clusters().await
    }

    /// Check if this consensus instance supports HTTP integration
    pub fn supports_http_integration(&self) -> bool {
        self.network_manager.supports_http_integration()
    }

    /// Create HTTP router for WebSocket transport
    pub fn create_router(&self) -> ConsensusResult<Router> {
        if let Some(router) = self.network_manager.create_router() {
            Ok(router)
        } else {
            Err(Error::Configuration(
                crate::error::ConfigurationError::InvalidTransport {
                    reason: "HTTP integration not supported by current transport".to_string(),
                },
            ))
        }
    }

    /// Submit a messaging request for consensus
    pub async fn submit_request(&self, request: GlobalRequest) -> ConsensusResult<GlobalResponse> {
        // Route through hierarchical system
        let operation = ConsensusOperation::GlobalAdmin(request.operation);

        let response = self
            .hierarchical_orchestrator
            .router()
            .route_operation(operation, None)
            .await?;

        Ok(GlobalResponse {
            success: response.success,
            sequence: response.sequence.unwrap_or(0),
            error: response.error,
        })
    }

    /// Internal method to propose a request through Raft consensus
    async fn propose_request(&self, request: &GlobalRequest) -> ConsensusResult<u64> {
        self.global_manager.propose_request(request).await
    }

    /// Publish a message through consensus
    ///
    /// # Errors
    ///
    /// Returns an error if the Raft instance is not initialized or if consensus fails.
    pub async fn publish_message(&self, stream: String, data: Bytes) -> ConsensusResult<u64> {
        // Use hierarchical routing
        let operation = LocalStreamOperation::PublishToStream {
            stream: stream.clone(),
            data,
            metadata: None,
        };

        let response = self
            .hierarchical_orchestrator
            .router()
            .route_stream_operation(&stream, operation, None)
            .await?;

        if response.success {
            Ok(response.sequence.unwrap_or(0))
        } else {
            Err(Error::ConsensusFailed(
                response
                    .error
                    .unwrap_or_else(|| "Unknown error".to_string()),
            ))
        }
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
    ) -> ConsensusResult<u64> {
        // Use hierarchical routing
        let operation = LocalStreamOperation::PublishToStream {
            stream: stream.clone(),
            data,
            metadata: Some(metadata),
        };

        let response = self
            .hierarchical_orchestrator
            .router()
            .route_stream_operation(&stream, operation, None)
            .await?;

        if response.success {
            Ok(response.sequence.unwrap_or(0))
        } else {
            Err(Error::ConsensusFailed(
                response
                    .error
                    .unwrap_or_else(|| "Unknown error".to_string()),
            ))
        }
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
    ) -> ConsensusResult<u64> {
        // Use hierarchical routing
        let operation = LocalStreamOperation::PublishBatchToStream {
            stream: stream.clone(),
            messages,
        };

        let response = self
            .hierarchical_orchestrator
            .router()
            .route_stream_operation(&stream, operation, None)
            .await?;

        if response.success {
            Ok(response.sequence.unwrap_or(0))
        } else {
            Err(Error::ConsensusFailed(
                response
                    .error
                    .unwrap_or_else(|| "Unknown error".to_string()),
            ))
        }
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
    ) -> ConsensusResult<u64> {
        // Use hierarchical routing
        let operation = LocalStreamOperation::RollupStream {
            stream: stream.clone(),
            data,
            expected_seq,
        };

        let response = self
            .hierarchical_orchestrator
            .router()
            .route_stream_operation(&stream, operation, None)
            .await?;

        if response.success {
            Ok(response.sequence.unwrap_or(0))
        } else {
            Err(Error::ConsensusFailed(
                response
                    .error
                    .unwrap_or_else(|| "Unknown error".to_string()),
            ))
        }
    }

    /// Delete a message from a stream through consensus
    ///
    /// # Errors
    ///
    /// Returns an error if the Raft instance is not initialized or if consensus fails.
    pub async fn delete_message(&self, stream: String, sequence: u64) -> ConsensusResult<u64> {
        // Use hierarchical routing
        let operation = LocalStreamOperation::DeleteFromStream {
            stream: stream.clone(),
            sequence,
        };

        let response = self
            .hierarchical_orchestrator
            .router()
            .route_stream_operation(&stream, operation, None)
            .await?;

        if response.success {
            Ok(response.sequence.unwrap_or(0))
        } else {
            Err(Error::ConsensusFailed(
                response
                    .error
                    .unwrap_or_else(|| "Unknown error".to_string()),
            ))
        }
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
    ) -> ConsensusResult<u64> {
        let stream_name = stream_name.into();
        let subject_pattern = subject_pattern.into();

        let request = GlobalRequest {
            operation: GlobalOperation::Routing(RoutingOperation::Subscribe {
                stream_name,
                subject_pattern,
            }),
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
    ) -> ConsensusResult<u64> {
        let request = GlobalRequest {
            operation: GlobalOperation::Routing(RoutingOperation::Unsubscribe {
                stream_name: stream_name.into(),
                subject_pattern: subject_pattern.into(),
            }),
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
    ) -> ConsensusResult<u64> {
        let request = GlobalRequest {
            operation: GlobalOperation::Routing(RoutingOperation::RemoveAllSubscriptions {
                stream_name: stream_name.into(),
            }),
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
    ) -> ConsensusResult<u64> {
        let stream_name = stream_name.into();

        let request = GlobalRequest {
            operation: GlobalOperation::Routing(RoutingOperation::BulkSubscribe {
                stream_name,
                subject_patterns,
            }),
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
    ) -> ConsensusResult<u64> {
        let request = GlobalRequest {
            operation: GlobalOperation::Routing(RoutingOperation::BulkUnsubscribe {
                stream_name: stream_name.into(),
                subject_patterns,
            }),
        };

        self.propose_request(&request).await
    }

    /// Get a message by sequence number from local store
    ///
    /// # Errors
    ///
    /// Currently always returns `Ok` but may return errors in future implementations.
    pub async fn get_message(&self, stream: &str, seq: u64) -> ConsensusResult<Option<Bytes>> {
        Ok(self.global_state.get_message(stream, seq).await)
    }

    /// Get the last sequence number for a stream
    ///
    /// # Errors
    ///
    /// Currently always returns `Ok` but may return errors in future implementations.
    pub async fn last_sequence(&self, stream: &str) -> ConsensusResult<u64> {
        Ok(self.global_state.last_sequence(stream).await)
    }

    /// Get all streams that should receive messages for a given subject
    ///
    /// This reads from the consensus-synchronized routing table.
    pub async fn route_subject(&self, subject: &str) -> std::collections::HashSet<String> {
        self.global_state.route_subject(subject).await
    }

    /// Get all subject patterns that a stream is subscribed to
    ///
    /// This reads from the consensus-synchronized routing table.
    pub async fn get_stream_subjects(
        &self,
        stream_name: &str,
    ) -> Option<std::collections::HashSet<String>> {
        // TODO: Implement in StreamStore
        // For now, return None as this functionality needs to be added to StreamStore
        let _ = stream_name;
        None
    }

    /// Get all streams subscribed to a specific subject pattern
    ///
    /// This reads from the consensus-synchronized routing table.
    pub async fn get_subject_streams(
        &self,
        subject_pattern: &str,
    ) -> Option<std::collections::HashSet<String>> {
        // TODO: Implement in StreamStore
        // For now, return None as this functionality needs to be added to StreamStore
        let _ = subject_pattern;
        None
    }

    /// Get a summary of all current subject subscriptions
    ///
    /// Returns a map from subject patterns to the streams subscribed to them.
    /// This reads from the consensus-synchronized routing table.
    pub async fn get_all_subscriptions(
        &self,
    ) -> std::collections::HashMap<String, std::collections::HashSet<String>> {
        // TODO: Implement in StreamStore
        // For now, return empty map as this functionality needs to be added to StreamStore
        std::collections::HashMap::new()
    }

    /// Create a new stream with configuration
    ///
    /// # Errors
    ///
    /// Returns an error if the stream name is invalid or consensus fails.
    pub async fn create_stream(
        &self,
        stream_name: impl Into<String>,
        config: crate::global::StreamConfig,
    ) -> ConsensusResult<u64> {
        let stream_name = stream_name.into();

        // Get the first available consensus group from global state
        // In a real implementation, this would use a more sophisticated allocation strategy
        let group_id = {
            let global_state = self.global_state();
            let groups = global_state.get_all_groups().await;
            if groups.is_empty() {
                return Err(Error::InvalidOperation(
                    "No consensus groups available".to_string(),
                ));
            }
            // For now, just use the first group
            // TODO: Implement proper group selection strategy
            groups.into_iter().next().unwrap().id
        };

        // Use hierarchical system - create the stream with the selected group
        let operation = GlobalOperation::StreamManagement(StreamManagementOperation::Create {
            name: stream_name,
            config,
            group_id,
        });

        let response = self
            .hierarchical_orchestrator
            .router()
            .route_global_operation(operation)
            .await?;

        if response.success {
            Ok(response.sequence)
        } else {
            Err(Error::ConsensusFailed(
                response
                    .error
                    .unwrap_or_else(|| "Unknown error".to_string()),
            ))
        }
    }

    /// Update stream configuration
    ///
    /// # Errors
    ///
    /// Returns an error if the stream doesn't exist or consensus fails.
    pub async fn update_stream_config(
        &self,
        stream_name: impl Into<String>,
        config: crate::global::StreamConfig,
    ) -> ConsensusResult<u64> {
        let request = GlobalRequest {
            operation: GlobalOperation::StreamManagement(StreamManagementOperation::UpdateConfig {
                name: stream_name.into(),
                config,
            }),
        };

        self.propose_request(&request).await
    }

    /// Delete a stream
    ///
    /// # Errors
    ///
    /// Returns an error if the stream doesn't exist or consensus fails.
    pub async fn delete_stream(&self, stream_name: impl Into<String>) -> ConsensusResult<u64> {
        let request = GlobalRequest {
            operation: GlobalOperation::StreamManagement(StreamManagementOperation::Delete {
                name: stream_name.into(),
            }),
        };

        self.propose_request(&request).await
    }

    /// Get current Raft metrics
    pub async fn metrics(&self) -> Option<openraft::RaftMetrics<GlobalTypeConfig>> {
        self.global_manager.metrics().await.ok()
    }

    /// Get connected peers
    pub async fn get_connected_peers(&self) -> ConsensusResult<Vec<(NodeId, bool, SystemTime)>> {
        self.network_manager.get_connected_peers().await
    }

    /// Get current cluster state
    pub async fn cluster_state(&self) -> ClusterState {
        self.global_manager.cluster_state().await
    }

    /// Wait for the cluster to be ready (either Initiator or Joined state)
    pub async fn wait_for_leader(
        &self,
        timeout: Option<std::time::Duration>,
    ) -> ConsensusResult<()> {
        self.global_manager.wait_for_leader(timeout).await
    }

    /// Check if cluster discovery is in progress
    pub async fn is_cluster_discovering(&self) -> bool {
        matches!(self.cluster_state().await, ClusterState::Discovering)
    }

    /// Start the consensus system and handle cluster discovery
    pub async fn start(&self) -> ConsensusResult<()> {
        info!("Starting consensus system for node {}", self.node_id);

        // Initialize transport and then complete startup with discovery
        self.initialize_transport().await?;
        self.complete_startup().await
    }

    /// Initialize the transport and internal components without starting discovery
    /// This is useful for tests that need to set up HTTP servers before discovery
    pub async fn initialize_transport(&self) -> ConsensusResult<()> {
        // Start internal components first (network, handlers, etc.)
        self.start_internal().await?;

        // Start topology manager to refresh from governance

        info!("Starting topology manager...");
        self.global_manager.start_topology().await?;
        info!("Topology manager started");

        Ok(())
    }

    /// Complete the startup process by running discovery
    /// Should be called after initialize_transport()
    pub async fn complete_startup(&self) -> ConsensusResult<()> {
        // Get all peers from topology
        let all_peers = self.global_manager.topology().get_all_peers().await;

        info!("Topology refresh complete. Found {} peers", all_peers.len());
        for peer in &all_peers {
            info!("  - Peer: {} at {}", peer.node_id(), peer.origin());
        }

        if all_peers.is_empty() {
            // Single node topology - immediately become initiator
            info!("Single node topology detected, becoming cluster initiator");
            self.initialize_single_node_cluster(InitiatorReason::SingleNode)
                .await?;
        } else {
            // Multiple nodes - start discovery (connections will be established on-demand)
            info!(
                "Multiple nodes in topology ({}), starting discovery with on-demand connections",
                all_peers.len()
            );

            info!("Starting cluster discovery with on-demand connections");
            self.discover_and_join_cluster(all_peers).await?;
        }

        Ok(())
    }

    /// Discover existing clusters and join or become initiator
    async fn discover_and_join_cluster(&self, all_peers: Vec<crate::Node>) -> ConsensusResult<()> {
        // Set state to discovering
        self.global_manager
            .set_cluster_state(ClusterState::Discovering)
            .await;

        // Add a small delay to ensure all nodes have started their listeners
        // This helps avoid race conditions where some nodes start discovery
        // before others are ready to respond
        tokio::time::sleep(Duration::from_secs(2)).await;

        let discovery_timeout = Duration::from_secs(15); // Give more time for discovery

        info!(
            "Starting cluster discovery for node {} with timeout: {:?}",
            self.node_id, discovery_timeout
        );

        // Run discovery with timeout
        let start_time = std::time::Instant::now();

        while start_time.elapsed() < discovery_timeout {
            tokio::time::sleep(Duration::from_millis(1000)).await;

            info!("Performing cluster discovery round...");

            // Perform discovery using GlobalManager
            let responses = self.global_manager.discover_existing_clusters().await?;

            // Check if any peer reports an active cluster
            if let Some(existing_cluster) = responses
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

                // Join the existing cluster via Raft membership change
                self.join_existing_cluster_via_raft(existing_cluster)
                    .await?;
                return Ok(());
            }

            info!("No active cluster found in this round, continuing discovery...");
        }

        // Discovery timeout - decide what to do based on our role
        info!(
            "Cluster discovery timeout after {:?}. Deciding cluster formation strategy.",
            discovery_timeout
        );

        // Use deterministic node ordering to decide who becomes the multi-node cluster coordinator
        let mut all_node_ids: Vec<NodeId> = all_peers
            .iter()
            .map(|p| NodeId::new(p.public_key()))
            .collect();
        all_node_ids.push(self.node_id.clone());
        all_node_ids.sort(); // Deterministic ordering

        if let Some(coordinator_id) = all_node_ids.first() {
            if coordinator_id == &self.node_id {
                info!("Selected as cluster coordinator, initializing multi-node cluster");

                // We are the coordinator
                self.initialize_multi_node_cluster(InitiatorReason::ElectedInitiator, all_peers)
                    .await?;
            } else {
                info!("Not selected as coordinator, waiting for coordinator to initialize cluster");
                // Wait a bit more for the coordinator to set up the cluster
                tokio::time::sleep(Duration::from_secs(2)).await;

                // Try one more discovery round
                let responses = self.global_manager.discover_existing_clusters().await?;

                if let Some(existing_cluster) = responses
                    .iter()
                    .find(|response| response.has_active_cluster)
                {
                    info!("Found coordinator's cluster, attempting to join");
                    self.join_existing_cluster_via_raft(existing_cluster)
                        .await?;
                } else {
                    warn!("Coordinator cluster not found, falling back to single-node cluster");
                    self.initialize_single_node_cluster(InitiatorReason::DiscoveryTimeout)
                        .await?;
                }
            }
        } else {
            // Fallback
            self.initialize_single_node_cluster(InitiatorReason::DiscoveryTimeout)
                .await?;
        }

        Ok(())
    }

    /// Initialize as a single-node cluster
    async fn initialize_single_node_cluster(&self, reason: InitiatorReason) -> ConsensusResult<()> {
        self.global_manager
            .initialize_single_node_cluster(self.node_id.clone(), reason)
            .await
    }

    /// Initialize a multi-node cluster by starting as single-node and expecting others to join
    async fn initialize_multi_node_cluster(
        &self,
        reason: InitiatorReason,
        peers: Vec<crate::Node>,
    ) -> ConsensusResult<()> {
        self.global_manager
            .initialize_multi_node_cluster(self.node_id.clone(), reason, peers)
            .await
    }

    /// Join an existing cluster via Raft membership change (learner -> voter)
    async fn join_existing_cluster_via_raft(
        &self,
        existing_cluster: &ClusterDiscoveryResponse,
    ) -> ConsensusResult<()> {
        self.global_manager
            .join_existing_cluster_via_raft(self.node_id.clone(), existing_cluster)
            .await
    }

    /// Shutdown the consensus system
    pub async fn shutdown(&self) -> ConsensusResult<()> {
        info!("Shutting down consensus system");

        // Comprehensive shutdown of NetworkManager (includes Raft, transport, topology)
        if let Err(e) = self.global_manager.shutdown_all().await {
            warn!("NetworkManager shutdown error: {}", e);
        }

        // Wait for background tasks
        let handles = {
            let mut task_handles = self.task_handles.lock().unwrap();
            std::mem::take(&mut *task_handles)
        };

        for handle in handles {
            if !handle.is_finished() {
                handle.abort();
            }
        }

        info!("Consensus system shutdown complete");
        Ok(())
    }

    /// Get node ID
    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Get the global state
    pub fn global_state(&self) -> &Arc<GlobalState> {
        &self.global_state
    }

    /// Force sync of consensus groups with allocation manager
    /// This is useful for testing or when immediate sync is needed
    pub async fn sync_consensus_groups(&self) -> ConsensusResult<()> {
        self.hierarchical_orchestrator.sync_consensus_groups().await
    }

    /// Publish a message to a subject using PubSub (bypasses consensus)
    ///
    /// This method provides high performance by routing messages
    /// directly to interested peers without going through Raft consensus.
    ///
    /// # Errors
    ///
    /// Returns an error if the subject pattern is invalid or network errors occur.
    pub async fn pubsub_publish(&self, subject: &str, payload: Bytes) -> ConsensusResult<()> {
        self.pubsub_manager
            .publish(subject, payload)
            .await
            .map_err(|e| Error::InvalidMessage(e.to_string()))
    }

    /// Subscribe to a subject pattern using PubSub
    ///
    /// Returns a Subscription handle that will receive matching messages.
    /// The subscription is automatically cleaned up when dropped.
    ///
    /// # Errors
    ///
    /// Returns an error if the subject pattern is invalid.
    pub async fn pubsub_subscribe(
        &self,
        subject: &str,
    ) -> ConsensusResult<crate::pubsub::Subscription<G, A>> {
        self.pubsub_manager
            .subscribe(subject)
            .await
            .map_err(|e| Error::InvalidMessage(e.to_string()))
    }

    /// Make a request and wait for a response using PubSub
    ///
    /// # Errors
    ///
    /// Returns an error if no responders are available or timeout occurs.
    pub async fn pubsub_request(
        &self,
        subject: &str,
        payload: Bytes,
        timeout: Duration,
    ) -> ConsensusResult<Bytes> {
        self.pubsub_manager
            .request(subject, payload, timeout)
            .await
            .map_err(|e| Error::InvalidMessage(e.to_string()))
    }

    /// Subscribe a consensus stream to receive PubSub messages
    ///
    /// This bridges the PubSub system with consensus-based streams,
    /// allowing streams to receive messages published via PubSub.
    ///
    /// # Errors
    ///
    /// Returns an error if the stream or subject pattern is invalid.
    pub async fn subscribe_stream_to_pubsub(
        &self,
        stream_name: &str,
        subject_pattern: &str,
    ) -> ConsensusResult<()> {
        // First, subscribe the stream to the subject pattern via consensus
        self.subscribe_stream_to_subject(stream_name, subject_pattern)
            .await?;

        // Then, enable the PubSub bridge for this subscription
        self.stream_bridge
            .subscribe_stream_to_subject(stream_name, subject_pattern)
            .await
            .map_err(|e| Error::InvalidMessage(format!("Failed to bridge subscription: {}", e)))
    }
}

#[async_trait]
impl<G, A> Bootable for Consensus<G, A>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug + Clone,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug + Clone,
{
    fn bootable_name(&self) -> &'static str {
        "consensus-system"
    }

    async fn start(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!(
            "Starting consensus system via Bootable trait for node {}",
            self.node_id
        );

        self.start()
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
    }

    async fn shutdown(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!(
            "Shutting down consensus system via Bootable trait for node {}",
            self.node_id
        );

        self.shutdown()
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
    }

    async fn wait(&self) {
        // For now, just return immediately
        // In a full implementation, this would wait for background tasks to complete
        // but JoinHandle doesn't implement Clone, so we can't easily access them here
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
            .finish_non_exhaustive()
    }
}
