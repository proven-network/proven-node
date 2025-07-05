//! Raft networking interface
//!
//! This module bridges OpenRaft with our transport layer, handling the
//! serialization/deserialization of Raft messages and providing access
//! to real raft state for transports.

pub mod adaptor;
mod cluster_state;
pub mod messages;

pub use cluster_state::{ClusterState, InitiatorReason};

use crate::attestation::AttestationVerifier;
use crate::cose::CoseHandler;
use crate::error::{ConsensusError, NetworkError};
use crate::transport::{NetworkTransport, tcp::TcpTransport, websocket::WebSocketTransport};
use crate::types::{NodeId, TypeConfig};
use crate::verification::{ConnectionVerification, ConnectionVerifier};

use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::{Instant, SystemTime};

use bytes::Bytes;
use openraft::storage::{RaftLogStorage, RaftStateMachine};
use proven_attestation::Attestor;
use proven_governance::Governance;
use tokio::sync::{RwLock, mpsc, oneshot};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

// Re-exports
use adaptor::{NetworkFactory, RaftAdapterRequest, RaftAdapterResponse};
pub use messages::{
    ApplicationMessage, ClusterDiscoveryRequest, ClusterDiscoveryResponse, ClusterJoinRequest,
    ClusterJoinResponse, Message, RaftMessage,
};

/// Generic pending request tracking for any message expecting a response
#[derive(Debug)]
pub struct PendingRequest<T> {
    /// Response sender
    pub response_tx: oneshot::Sender<T>,
    /// Target node ID
    pub target_node_id: NodeId,
    /// Request timestamp for timeout tracking
    pub timestamp: Instant,
    /// Message type for debugging
    pub message_type: String,
}

/// Specialized pending request for cluster discovery
pub type PendingDiscoveryRequest = PendingRequest<ClusterDiscoveryResponse>;

/// Channel structure for message routing in NetworkManager
#[derive(Debug)]
struct NetworkChannels {
    // Incoming message distribution
    incoming_message_tx: mpsc::UnboundedSender<(NodeId, Bytes)>,
    incoming_message_rx: Option<mpsc::UnboundedReceiver<(NodeId, Bytes)>>,

    // Application message processing
    app_message_tx: mpsc::UnboundedSender<(NodeId, ApplicationMessage, Option<Uuid>)>,
    app_message_rx: Option<mpsc::UnboundedReceiver<(NodeId, ApplicationMessage, Option<Uuid>)>>,

    // Raft message processing
    raft_message_tx: mpsc::UnboundedSender<(NodeId, RaftMessage, Option<Uuid>)>,
    raft_message_rx: Option<mpsc::UnboundedReceiver<(NodeId, RaftMessage, Option<Uuid>)>>,

    // Outgoing message sending
    outgoing_message_tx: mpsc::UnboundedSender<(NodeId, Message, Option<Uuid>)>,
    outgoing_message_rx: Option<mpsc::UnboundedReceiver<(NodeId, Message, Option<Uuid>)>>,

    // Response correlation
    response_tx: mpsc::UnboundedSender<(Uuid, Box<dyn std::any::Any + Send + Sync>)>,
    response_rx: Option<mpsc::UnboundedReceiver<(Uuid, Box<dyn std::any::Any + Send + Sync>)>>,
}

impl NetworkChannels {
    fn new() -> Self {
        let (incoming_message_tx, incoming_message_rx) = mpsc::unbounded_channel();
        let (app_message_tx, app_message_rx) = mpsc::unbounded_channel();
        let (raft_message_tx, raft_message_rx) = mpsc::unbounded_channel();
        let (outgoing_message_tx, outgoing_message_rx) = mpsc::unbounded_channel();
        let (response_tx, response_rx) = mpsc::unbounded_channel();

        Self {
            incoming_message_tx,
            incoming_message_rx: Some(incoming_message_rx),
            app_message_tx,
            app_message_rx: Some(app_message_rx),
            raft_message_tx,
            raft_message_rx: Some(raft_message_rx),
            outgoing_message_tx,
            outgoing_message_rx: Some(outgoing_message_rx),
            response_tx,
            response_rx: Some(response_rx),
        }
    }
}

/// High-level network manager that handles all networking and cluster management
/// Owns and manages the Raft instance, topology, discovery, and cluster state
pub struct NetworkManager<G, A>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug + Clone,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug + Clone,
{
    /// Our local node ID
    local_node_id: NodeId,

    /// Transport for networking - owned by NetworkManager
    transport: Arc<dyn NetworkTransport>,

    /// Raft instance - owned and managed by NetworkManager
    raft_instance: Arc<openraft::Raft<TypeConfig>>,
    /// Topology manager for node discovery and management
    topology: Arc<crate::topology::TopologyManager<G>>,

    /// Network channels for message routing
    network_channels: NetworkChannels,

    /// Generic pending requests by correlation ID - can handle any response type
    pending_requests: Arc<RwLock<HashMap<Uuid, Box<dyn std::any::Any + Send + Sync>>>>,
    /// Discovery responses collection (still needed for discovery flow)
    discovery_responses: Arc<RwLock<HashMap<Uuid, ClusterDiscoveryResponse>>>,
    /// Current cluster state
    cluster_state: Arc<RwLock<crate::network::ClusterState>>,

    /// Phantom data to ensure that the attestor type is used
    _marker: PhantomData<A>,
}

impl<G, A> NetworkManager<G, A>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug + Clone,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug + Clone,
{
    /// Create a new NetworkManager with task-based message processing
    pub async fn new(
        signing_key: ed25519_dalek::SigningKey,
        governance: Arc<G>,
        attestor: Arc<A>,
        local_node_id: NodeId,
        transport_config: crate::transport::TransportConfig,
        storage: impl RaftLogStorage<TypeConfig>
        + RaftStateMachine<TypeConfig>
        + Clone
        + Send
        + Sync
        + 'static,
        raft_config: Arc<openraft::Config>,
    ) -> Result<Self, openraft::error::RaftError<TypeConfig>> {
        let local_public_key = local_node_id.clone();

        // Create COSE handler
        let cose_handler = Arc::new(CoseHandler::new(signing_key));

        // Create attestation verifier
        let attestation_verifier = Arc::new(AttestationVerifier::new(
            (*governance).clone(),
            (*attestor).clone(),
        ));

        // Create connection verifier
        let connection_verifier: Arc<dyn ConnectionVerification> = Arc::new(
            ConnectionVerifier::new(attestation_verifier, cose_handler.clone(), local_public_key),
        );

        // Create topology manager first so we can pass it to transports
        let topology = Arc::new(crate::topology::TopologyManager::new(
            governance.clone(),
            local_node_id.clone(),
        ));

        // Create transport based on configuration
        let transport: Arc<dyn NetworkTransport> =
            match transport_config {
                crate::transport::TransportConfig::Tcp { listen_addr } => Arc::new(
                    TcpTransport::new(listen_addr, connection_verifier, topology.clone()),
                ),
                crate::transport::TransportConfig::WebSocket => Arc::new(WebSocketTransport::new(
                    connection_verifier,
                    topology.clone(),
                )),
            };

        let (transport_tx, transport_rx) = mpsc::unbounded_channel::<(NodeId, Bytes)>();
        let (raft_adapter_request_tx, raft_adapter_request_rx) =
            mpsc::unbounded_channel::<(NodeId, Uuid, Box<RaftAdapterRequest>)>();
        let (raft_adapter_response_tx, raft_adapter_response_rx) =
            mpsc::unbounded_channel::<(NodeId, Uuid, Box<RaftAdapterResponse>)>();

        // Create Raft network factory
        let network_factory =
            NetworkFactory::new(raft_adapter_request_tx.clone(), raft_adapter_response_rx);

        // Create Raft instance
        let raft_instance = Arc::new(
            openraft::Raft::new(
                local_node_id.clone(),
                raft_config,
                network_factory,
                storage.clone(),
                storage,
            )
            .await?,
        );

        // Create network channels
        let mut network_channels = NetworkChannels::new();

        // Create shared state
        let pending_requests = Arc::new(RwLock::new(HashMap::new()));
        let discovery_responses = Arc::new(RwLock::new(HashMap::new()));
        let cluster_state = Arc::new(RwLock::new(ClusterState::TransportReady));

        // Start background tasks
        let mut task_handles = Vec::new();

        // Task 1: Message Router Task
        let router_task = tokio::spawn(Self::message_router_loop(
            network_channels.incoming_message_rx.take().unwrap(),
            network_channels.app_message_tx.clone(),
            network_channels.raft_message_tx.clone(),
            cose_handler.clone(),
        ));
        task_handles.push(router_task);

        // Task 2: Application Message Processor Task
        let app_processor_task = tokio::spawn(Self::application_message_processor_loop(
            network_channels.app_message_rx.take().unwrap(),
            network_channels.outgoing_message_tx.clone(),
            network_channels.response_tx.clone(),
            local_node_id.clone(),
            topology.clone(),
            raft_instance.clone(),
            discovery_responses.clone(),
        ));
        task_handles.push(app_processor_task);

        // Task 3: Raft Message Processor Task
        let raft_processor_task = tokio::spawn(Self::raft_message_processor_loop(
            network_channels.raft_message_rx.take().unwrap(),
            network_channels.outgoing_message_tx.clone(),
            raft_adapter_response_tx.clone(),
            raft_instance.clone(),
        ));
        task_handles.push(raft_processor_task);

        // Task 4: Outgoing Message Sender Task
        let sender_task = tokio::spawn(Self::outgoing_message_sender_loop(
            network_channels.outgoing_message_rx.take().unwrap(),
            transport_tx.clone(),
            cose_handler.clone(),
            topology.clone(),
        ));
        task_handles.push(sender_task);

        // Task 5: Response Correlation Task
        let correlation_task = tokio::spawn(Self::response_correlation_loop(
            network_channels.response_rx.take().unwrap(),
            pending_requests.clone(),
        ));
        task_handles.push(correlation_task);

        // Task 6: Transport Message Sender Task (original functionality)
        let transport_clone = transport.clone();
        let transport_sender_task = tokio::spawn(async move {
            let mut transport_rx = transport_rx;
            debug!("Transport sender task started");
            while let Some((target_node_id, data)) = transport_rx.recv().await {
                if let Err(e) = transport_clone.send_bytes(&target_node_id, data).await {
                    error!("Failed to send message to {}: {}", target_node_id, e);
                }
            }
            debug!("Transport sender task exited");
        });
        task_handles.push(transport_sender_task);

        // Task 7: RaftAdapter Request Processor Task
        let raft_adapter_task = tokio::spawn(Self::raft_adapter_processor_loop(
            raft_adapter_request_rx,
            network_channels.outgoing_message_tx.clone(),
        ));
        task_handles.push(raft_adapter_task);

        Ok(Self {
            local_node_id,
            transport,
            raft_instance,
            topology,
            network_channels,
            pending_requests,
            discovery_responses,
            cluster_state,
            _marker: PhantomData,
        })
    }

    /// Get the Raft instance
    pub fn raft_instance(&self) -> Arc<openraft::Raft<TypeConfig>> {
        self.raft_instance.clone()
    }

    /// Get the topology manager
    pub fn topology(&self) -> Arc<crate::topology::TopologyManager<G>> {
        self.topology.clone()
    }

    /// Check if this node is the current leader
    pub fn is_leader(&self) -> bool {
        let metrics_ref = self.raft_instance.metrics();
        let metrics = metrics_ref.borrow();

        metrics.current_leader.as_ref() == Some(&self.local_node_id)
    }

    /// Create a simple transport handler that forwards messages to the router
    pub fn create_simple_transport_handler(&self) -> crate::transport::MessageHandler {
        let incoming_tx = self.network_channels.incoming_message_tx.clone();

        Arc::new(move |sender_id: NodeId, data: Bytes| {
            let incoming_tx = incoming_tx.clone();
            tokio::spawn(async move {
                if let Err(e) = incoming_tx.send((sender_id.clone(), data)) {
                    error!(
                        "Failed to forward incoming message from {} to router: {}",
                        sender_id, e
                    );
                }
            });
        })
    }

    /// Submit a request to the Raft consensus engine
    pub async fn submit_request(
        &self,
        request: crate::types::MessagingRequest,
    ) -> Result<crate::types::MessagingResponse, crate::error::ConsensusError> {
        match self.raft_instance.client_write(request.clone()).await {
            Ok(response) => Ok(response.data),
            Err(e) => {
                error!("Failed to submit request to Raft: {}", e);
                Err(crate::error::ConsensusError::Raft(format!(
                    "Raft client write failed: {}",
                    e
                )))
            }
        }
    }

    /// Propose a request and get the sequence number
    pub async fn propose_request(
        &self,
        request: &crate::types::MessagingRequest,
    ) -> Result<u64, crate::error::ConsensusError> {
        let response = self.raft_instance.client_write(request.clone()).await;
        match response {
            Ok(client_write_response) => {
                let log_id = client_write_response.log_id;
                Ok(log_id.index)
            }
            Err(e) => {
                error!("Failed to propose request: {}", e);
                Err(crate::error::ConsensusError::Raft(format!(
                    "Raft proposal failed: {}",
                    e
                )))
            }
        }
    }

    /// Get Raft metrics
    pub fn metrics(&self) -> Option<openraft::RaftMetrics<crate::types::TypeConfig>> {
        Some(self.raft_instance.metrics().borrow().clone())
    }

    /// Check if there's an active cluster
    pub async fn has_active_cluster(&self) -> bool {
        self.raft_instance.current_leader().await.is_some()
    }

    /// Get current term
    pub fn current_term(&self) -> Option<u64> {
        Some(self.raft_instance.metrics().borrow().current_term)
    }

    /// Get current leader
    pub async fn current_leader(&self) -> Option<String> {
        self.raft_instance
            .current_leader()
            .await
            .map(|id| id.to_hex())
    }

    /// Get cluster size
    pub fn cluster_size(&self) -> Option<usize> {
        let metrics = self.raft_instance.metrics();
        let metrics_borrow = metrics.borrow();
        let membership = metrics_borrow.membership_config.membership();
        Some(membership.voter_ids().count() + membership.learner_ids().count())
    }

    /// Shutdown the Raft instance
    pub async fn shutdown_raft(&self) -> Result<(), crate::error::ConsensusError> {
        if let Err(e) = self.raft_instance.shutdown().await {
            error!("Error shutting down Raft: {}", e);
            Err(crate::error::ConsensusError::Raft(format!(
                "Raft shutdown failed: {}",
                e
            )))
        } else {
            Ok(())
        }
    }

    /// Initialize a single-node cluster
    pub async fn initialize_single_node_cluster(
        &self,
        node_id: NodeId,
        reason: InitiatorReason,
    ) -> Result<(), crate::error::ConsensusError> {
        info!("Initializing single-node cluster for node {}", node_id);

        // Set cluster state to initiator
        self.set_cluster_state(ClusterState::Initiator {
            initiated_at: std::time::Instant::now(),
            reason,
        })
        .await;

        // Initialize Raft with just this node
        // Get our own GovernanceNode from topology
        let own_node = self.topology.get_own_node().await.map_err(|e| {
            crate::error::ConsensusError::InvalidMessage(format!("Failed to get own node: {e}"))
        })?;

        let membership = std::collections::BTreeMap::from([(node_id.clone(), own_node)]);

        self.raft_instance
            .initialize(membership)
            .await
            .map_err(|e| {
                crate::error::ConsensusError::Raft(format!("Failed to initialize Raft: {e}"))
            })?;

        info!("Successfully initialized single-node cluster");
        Ok(())
    }

    /// Initialize a multi-node cluster (as leader)
    pub async fn initialize_multi_node_cluster(
        &self,
        node_id: NodeId,
        _peers: Vec<crate::types::Node>,
    ) -> Result<(), crate::error::ConsensusError> {
        info!(
            "Initializing multi-node cluster as leader for node {}",
            node_id
        );

        // Set cluster state to initiator
        self.set_cluster_state(ClusterState::Initiator {
            initiated_at: std::time::Instant::now(),
            reason: InitiatorReason::DiscoveryTimeout,
        })
        .await;

        // For now, start as single node - peers will join later
        // Get our own GovernanceNode from topology
        let own_node = self.topology.get_own_node().await.map_err(|e| {
            crate::error::ConsensusError::InvalidMessage(format!("Failed to get own node: {e}"))
        })?;

        let membership = std::collections::BTreeMap::from([(node_id.clone(), own_node)]);

        self.raft_instance
            .initialize(membership)
            .await
            .map_err(|e| {
                crate::error::ConsensusError::Raft(format!("Failed to initialize leader Raft: {e}"))
            })?;

        info!("Successfully initialized multi-node cluster as leader");
        Ok(())
    }

    /// Join an existing cluster via Raft
    pub async fn join_existing_cluster_via_raft(
        &self,
        node_id: NodeId,
        existing_cluster: &crate::network::ClusterDiscoveryResponse,
    ) -> Result<(), crate::error::ConsensusError> {
        // Ensure we have a leader to send the join request to
        let leader_id = existing_cluster.current_leader.as_ref().ok_or_else(|| {
            crate::error::ConsensusError::InvalidMessage(
                "Cannot join cluster: no leader reported in discovery response".to_string(),
            )
        })?;

        info!(
            "Joining existing cluster via Raft. Leader: {}, Term: {:?}",
            leader_id, existing_cluster.current_term
        );

        // Set cluster state to waiting to join
        self.set_cluster_state(crate::network::ClusterState::WaitingToJoin {
            requested_at: std::time::Instant::now(),
            leader_id: leader_id.clone(),
        })
        .await;

        // Get our own node information for the join request
        let requester_node = self.topology().get_own_node().await.map_err(|e| {
            crate::error::ConsensusError::InvalidMessage(format!("Failed to get own node: {e}"))
        })?;

        // Create join request with correlation ID tracking
        let join_request = crate::network::ClusterJoinRequest {
            requester_id: node_id.clone(),
            requester_node: requester_node.into(),
        };

        // Generate correlation ID and add pending request
        let correlation_id = Uuid::new_v4();
        let response_rx = self
            .add_pending_request::<ClusterJoinResponse>(
                correlation_id,
                leader_id.clone(),
                "cluster_join_request".to_string(),
            )
            .await;

        // Send join request with correlation ID using channel-based system
        let message = crate::network::Message::Application(Box::new(
            crate::network::ApplicationMessage::ClusterJoinRequest(join_request),
        ));

        // Send using channel-based system
        self.send_message_with_correlation(leader_id.clone(), message, correlation_id)
            .await?;

        info!(
            "Sent join request to {} with correlation ID {}",
            leader_id, correlation_id
        );

        // Wait for join response with timeout
        let join_timeout = std::time::Duration::from_secs(30);
        let join_response = match tokio::time::timeout(join_timeout, response_rx).await {
            Ok(Ok(response)) => response,
            Ok(Err(_)) => {
                self.set_cluster_state(ClusterState::Failed {
                    failed_at: std::time::Instant::now(),
                    error: "Join request channel was cancelled".to_string(),
                })
                .await;
                return Err(crate::error::ConsensusError::InvalidMessage(
                    "Join request cancelled".to_string(),
                ));
            }
            Err(_) => {
                self.set_cluster_state(ClusterState::Failed {
                    failed_at: std::time::Instant::now(),
                    error: "Join request timeout".to_string(),
                })
                .await;
                return Err(crate::error::ConsensusError::InvalidMessage(
                    "Join request timeout".to_string(),
                ));
            }
        };

        if join_response.success {
            info!(
                "Successfully joined cluster! Leader: {}, Size: {:?}, Term: {:?}",
                join_response.responder_id, join_response.cluster_size, join_response.current_term
            );

            self.set_cluster_state(ClusterState::Joined {
                joined_at: std::time::Instant::now(),
                cluster_size: join_response.cluster_size.unwrap_or(1),
            })
            .await;

            Ok(())
        } else {
            let error_msg = join_response
                .error_message
                .unwrap_or_else(|| "Unknown join error".to_string());
            warn!("Failed to join cluster: {}", error_msg);

            self.set_cluster_state(ClusterState::Failed {
                failed_at: std::time::Instant::now(),
                error: error_msg.clone(),
            })
            .await;

            Err(crate::error::ConsensusError::InvalidMessage(format!(
                "Join request failed: {}",
                error_msg
            )))
        }
    }

    /// Wait for the cluster to be ready (either Initiator or Joined state)
    pub async fn wait_for_leader(
        &self,
        timeout: Option<std::time::Duration>,
    ) -> Result<(), crate::error::ConsensusError> {
        let timeout = timeout.unwrap_or(std::time::Duration::from_secs(30));
        let start_time = std::time::Instant::now();

        loop {
            let state = self.cluster_state().await;

            match state {
                ClusterState::Initiator { .. } | ClusterState::Joined { .. } => {
                    debug!("Cluster is ready with state: {:?}", state);
                    return Ok(());
                }
                ClusterState::Failed { error, .. } => {
                    return Err(crate::error::ConsensusError::InvalidMessage(format!(
                        "Cluster failed to initialize: {}",
                        error
                    )));
                }
                _ => {
                    // Continue waiting
                    if start_time.elapsed() > timeout {
                        return Err(crate::error::ConsensusError::InvalidMessage(format!(
                            "Timeout waiting for cluster to be ready. Current state: {:?}",
                            state
                        )));
                    }

                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            }
        }
    }

    /// Get the current cluster state
    pub async fn cluster_state(&self) -> ClusterState {
        self.cluster_state.read().await.clone()
    }

    /// Set the cluster state
    pub async fn set_cluster_state(&self, state: ClusterState) {
        *self.cluster_state.write().await = state;
    }

    /// Start topology manager
    pub async fn start_topology(&self) -> Result<(), crate::error::ConsensusError> {
        self.topology.start().await
    }

    /// Start the network transport and message handling
    pub async fn start_network(&self) -> Result<(), crate::error::ConsensusError> {
        // Create the simple transport handler that forwards to the router
        let message_handler = self.create_simple_transport_handler();

        // Start the transport listener
        self.transport
            .start_listener(message_handler)
            .await
            .map_err(crate::error::ConsensusError::Network)?;

        info!(
            "NetworkManager started transport listener for node {}",
            self.local_node_id
        );
        Ok(())
    }

    /// Discover existing clusters using the channel-based system
    pub async fn discover_existing_clusters(
        &self,
    ) -> Result<Vec<ClusterDiscoveryResponse>, crate::error::ConsensusError> {
        // Clear previous responses
        self.discovery_responses.write().await.clear();

        // Get all peers from topology
        let all_peers = self.topology.get_all_peers().await;
        debug!("Discovering clusters among {} peers", all_peers.len());

        if all_peers.is_empty() {
            return Ok(Vec::new());
        }

        // Collect correlation IDs and response receivers for all pending requests
        let mut response_receivers = Vec::new();
        let mut correlation_ids = Vec::new();

        // Send discovery requests to all peers and track them
        for peer in &all_peers {
            let peer_node_id = NodeId::new(peer.public_key());

            // Create unique discovery request for each peer
            let discovery_request = ClusterDiscoveryRequest::new(self.local_node_id.clone());

            // Generate correlation ID and add pending request
            let correlation_id = Uuid::new_v4();
            let response_rx = self
                .add_pending_request::<ClusterDiscoveryResponse>(
                    correlation_id,
                    peer_node_id.clone(),
                    "cluster_discovery_request".to_string(),
                )
                .await;

            correlation_ids.push(correlation_id);
            response_receivers.push(response_rx);

            // Send using channel-based system
            let message = Message::Application(Box::new(ApplicationMessage::ClusterDiscovery(
                discovery_request,
            )));

            if let Err(e) = self
                .send_message_with_correlation(peer_node_id.clone(), message, correlation_id)
                .await
            {
                warn!(
                    "Failed to send discovery request to {}: {}",
                    peer_node_id, e
                );
            }
        }

        // Wait for responses with timeout
        let discovery_timeout = std::time::Duration::from_secs(5);
        let mut responses = Vec::new();
        let mut successful_requests = 0;
        let mut failed_requests = 0;

        // Use tokio::time::timeout to wait for each response
        for response_rx in response_receivers.into_iter() {
            match tokio::time::timeout(discovery_timeout, response_rx).await {
                Ok(Ok(response)) => {
                    responses.push(response);
                    successful_requests += 1;
                }
                Ok(Err(_)) => {
                    failed_requests += 1;
                }
                Err(_) => {
                    failed_requests += 1;
                }
            }
        }

        // Clean up any remaining pending requests
        for correlation_id in correlation_ids {
            let mut pending = self.pending_requests.write().await;
            if pending.remove(&correlation_id).is_some() {
                debug!("Cleaned up pending request {}", correlation_id);
            }
        }

        info!(
            "Discovery complete: {} successful, {} failed out of {} requests",
            successful_requests,
            failed_requests,
            all_peers.len()
        );
        Ok(responses)
    }

    /// Handle incoming cluster discovery request
    pub async fn handle_cluster_discovery_request(
        &self,
        request: ClusterDiscoveryRequest,
        sender_node_id: NodeId,
        correlation_id: Option<Uuid>,
    ) -> (Option<ApplicationMessage>, Option<Uuid>) {
        debug!(
            "Handling cluster discovery request from {} with request: {:?}",
            sender_node_id, request
        );

        // Get Raft metrics to determine cluster info
        let metrics = self.raft_instance.metrics().borrow().clone();
        let is_leader = metrics.current_leader.as_ref() == Some(&self.local_node_id);

        debug!(
            "Current Raft metrics: term={}, leader={:?}, membership={:?}",
            metrics.current_term, metrics.current_leader, metrics.membership_config
        );

        let membership = &metrics.membership_config;
        let voter_ids: Vec<_> = membership.voter_ids().collect();
        let has_active_cluster = !voter_ids.is_empty();

        debug!(
            "Node {} cluster status: has_active_cluster={}, voter_count={}, is_leader={}",
            self.local_node_id,
            has_active_cluster,
            voter_ids.len(),
            is_leader
        );

        // Always respond with current status, even if no active cluster
        let response = ClusterDiscoveryResponse {
            responder_id: self.local_node_id.clone(),
            has_active_cluster,
            current_term: if has_active_cluster {
                Some(metrics.current_term)
            } else {
                None
            },
            current_leader: metrics.current_leader.clone(),
            cluster_size: if has_active_cluster {
                Some(voter_ids.len())
            } else {
                None
            },
        };

        debug!("Generated cluster discovery response: {:?}", response);
        (
            Some(ApplicationMessage::ClusterDiscoveryResponse(response)),
            correlation_id,
        )
    }

    /// Handle incoming cluster discovery response with correlation ID from COSE metadata
    pub async fn handle_cluster_discovery_response(
        &self,
        response: ClusterDiscoveryResponse,
        correlation_id: Option<Uuid>,
    ) {
        // Complete the pending request using the generic system
        if let Some(corr_id) = correlation_id {
            // Use the generic pending request system to complete the request
            if let Err(_e) = self
                .complete_pending_request(corr_id, response.clone())
                .await
            {
                // Fallback: Store in the discovery responses map for legacy compatibility
                self.discovery_responses
                    .write()
                    .await
                    .insert(corr_id, response);
            }
        }
    }

    /// Send a message to a target node (channel-based)
    pub async fn send_message(
        &self,
        target_node_id: NodeId,
        message: Message,
    ) -> Result<(), ConsensusError> {
        self.network_channels
            .outgoing_message_tx
            .send((target_node_id, message, None))
            .map_err(|_| {
                ConsensusError::InvalidMessage("Outgoing message channel closed".to_string())
            })
    }

    /// Send a message with correlation ID (channel-based)
    pub async fn send_message_with_correlation(
        &self,
        target_node_id: NodeId,
        message: Message,
        correlation_id: Uuid,
    ) -> Result<(), ConsensusError> {
        self.network_channels
            .outgoing_message_tx
            .send((target_node_id, message, Some(correlation_id)))
            .map_err(|_| {
                ConsensusError::InvalidMessage("Outgoing message channel closed".to_string())
            })
    }

    /// Send a message with optional correlation ID (channel-based)
    pub async fn send_message_with_optional_correlation(
        &self,
        target_node_id: NodeId,
        message: Message,
        correlation_id: Option<Uuid>,
    ) -> Result<(), ConsensusError> {
        self.network_channels
            .outgoing_message_tx
            .send((target_node_id, message, correlation_id))
            .map_err(|_| {
                ConsensusError::InvalidMessage("Outgoing message channel closed".to_string())
            })
    }

    /// Get connected peers from transport
    pub async fn get_connected_peers(
        &self,
    ) -> Result<Vec<(NodeId, bool, SystemTime)>, ConsensusError> {
        self.transport
            .get_connected_peers()
            .await
            .map_err(ConsensusError::Network)
    }

    /// Shutdown the underlying transport
    pub async fn shutdown(&self) -> Result<(), NetworkError> {
        self.transport.shutdown().await
    }

    /// Comprehensive shutdown of all NetworkManager components
    pub async fn shutdown_all(&self) -> Result<(), crate::error::ConsensusError> {
        info!(
            "Shutting down NetworkManager for node {}",
            self.local_node_id
        );

        // 1. Shutdown Raft instance first
        if let Err(e) = self.shutdown_raft().await {
            warn!("Raft shutdown error: {}", e);
        }

        // 2. Shutdown topology manager
        if let Err(e) = self.topology.shutdown().await {
            warn!("Topology shutdown error: {}", e);
        }

        // 3. Shutdown transport layer
        if let Err(e) = self.transport.shutdown().await {
            warn!("Transport shutdown error: {}", e);
        }

        // 4. The internal message sender task will automatically stop when the channel is dropped
        // since we don't have access to the task handle (it's stored as _sender_task_handle)
        // The task will exit when transport_rx.recv() returns None due to all senders being dropped

        // 5. Clear discovery responses
        self.discovery_responses.write().await.clear();

        // 6. Set cluster state to indicate shutdown
        self.set_cluster_state(ClusterState::Failed {
            failed_at: std::time::Instant::now(),
            error: "System shutdown".to_string(),
        })
        .await;

        info!(
            "NetworkManager shutdown complete for node {}",
            self.local_node_id
        );
        Ok(())
    }

    /// Check if the underlying transport supports HTTP integration
    pub fn supports_http_integration(&self) -> bool {
        use crate::transport::websocket::WebSocketTransport;
        self.transport
            .as_any()
            .downcast_ref::<WebSocketTransport<G>>()
            .is_some()
    }

    /// Create a router for HTTP integration if supported
    pub fn create_router(&self) -> Option<axum::Router> {
        use crate::transport::{HttpIntegratedTransport, websocket::WebSocketTransport};
        if let Some(ws_transport) = self
            .transport
            .as_any()
            .downcast_ref::<WebSocketTransport<G>>()
        {
            ws_transport.create_router_integration().ok()
        } else {
            None
        }
    }

    /// Add a pending request expecting a response of type T
    async fn add_pending_request<T: Send + Sync + 'static>(
        &self,
        correlation_id: Uuid,
        target_node_id: NodeId,
        message_type: String,
    ) -> oneshot::Receiver<T> {
        let (tx, rx) = oneshot::channel();
        let pending_request = PendingRequest {
            response_tx: tx,
            target_node_id: target_node_id.clone(),
            timestamp: Instant::now(),
            message_type: message_type.clone(),
        };

        self.pending_requests
            .write()
            .await
            .insert(correlation_id, Box::new(pending_request));

        rx
    }

    /// Complete a pending request with a response
    async fn complete_pending_request<T: Send + Sync + 'static>(
        &self,
        correlation_id: Uuid,
        response: T,
    ) -> Result<(), ConsensusError> {
        let mut pending = self.pending_requests.write().await;

        if let Some(boxed_request) = pending.remove(&correlation_id) {
            // Downcast the boxed request to the expected type
            if let Ok(pending_request) = boxed_request.downcast::<PendingRequest<T>>() {
                // Send the response through the channel
                let _ = pending_request.response_tx.send(response);

                Ok(())
            } else {
                Err(ConsensusError::InvalidMessage(
                    "Response type mismatch".to_string(),
                ))
            }
        } else {
            Err(ConsensusError::InvalidMessage(
                "No pending request found".to_string(),
            ))
        }
    }

    /// Parses incoming bytes and routes to appropriate processors
    async fn message_router_loop(
        mut incoming_rx: mpsc::UnboundedReceiver<(NodeId, Bytes)>,
        app_tx: mpsc::UnboundedSender<(NodeId, ApplicationMessage, Option<Uuid>)>,
        raft_tx: mpsc::UnboundedSender<(NodeId, RaftMessage, Option<Uuid>)>,
        cose_handler: Arc<CoseHandler>,
    ) {
        debug!("Message router task started");
        while let Some((sender_id, data)) = incoming_rx.recv().await {
            // Parse and route message
            match Self::parse_incoming_message(&data, &sender_id, &cose_handler).await {
                Ok((message, metadata)) => match message {
                    Message::Application(app_msg) => {
                        if let Err(e) = app_tx.send((sender_id, *app_msg, metadata.correlation_id))
                        {
                            error!("Failed to route application message: {}", e);
                        }
                    }
                    Message::Raft(raft_msg) => {
                        if let Err(e) = raft_tx.send((sender_id, raft_msg, metadata.correlation_id))
                        {
                            error!("Failed to route raft message: {}", e);
                        }
                    }
                },
                Err(e) => {
                    error!("Failed to parse incoming message from {}: {}", sender_id, e);
                }
            }
        }
        debug!("Message router task exited");
    }

    /// Static function: Application Message Processor Task
    /// Handles cluster discovery, join requests, etc.
    async fn application_message_processor_loop(
        mut app_rx: mpsc::UnboundedReceiver<(NodeId, ApplicationMessage, Option<Uuid>)>,
        outgoing_tx: mpsc::UnboundedSender<(NodeId, Message, Option<Uuid>)>,
        response_tx: mpsc::UnboundedSender<(Uuid, Box<dyn std::any::Any + Send + Sync>)>,
        local_node_id: NodeId,
        _topology: Arc<crate::topology::TopologyManager<G>>,
        raft_instance: Arc<openraft::Raft<TypeConfig>>,
        discovery_responses: Arc<RwLock<HashMap<Uuid, ClusterDiscoveryResponse>>>,
    ) {
        debug!("Application message processor task started");
        while let Some((sender_id, app_message, correlation_id)) = app_rx.recv().await {
            match app_message {
                ApplicationMessage::ClusterDiscovery(_) => {
                    if let Some((response_msg, response_correlation_id)) =
                        Self::process_cluster_discovery_request(
                            correlation_id,
                            &local_node_id,
                            &raft_instance,
                        )
                        .await
                    {
                        let response = Message::Application(Box::new(response_msg));
                        if let Err(e) =
                            outgoing_tx.send((sender_id, response, response_correlation_id))
                        {
                            error!("Failed to send cluster discovery response: {}", e);
                        }
                    }
                }
                ApplicationMessage::ClusterDiscoveryResponse(response) => {
                    Self::process_cluster_discovery_response(
                        response,
                        correlation_id,
                        &discovery_responses,
                        &response_tx,
                    )
                    .await;
                }
                ApplicationMessage::ClusterJoinRequest(request) => {
                    Self::process_cluster_join_request(
                        request,
                        sender_id,
                        correlation_id,
                        &local_node_id,
                        &raft_instance,
                        &_topology,
                        &outgoing_tx,
                    )
                    .await;
                }
                ApplicationMessage::ClusterJoinResponse(response) => {
                    if let Some(corr_id) = correlation_id {
                        if let Err(e) = response_tx.send((corr_id, Box::new(response))) {
                            error!("Failed to correlate join response: {}", e);
                        }
                    }
                }
                other_msg => {
                    debug!("Received other application message: {:?}", other_msg);
                    // These would be handled by the consensus layer in a real implementation
                }
            }
        }
        debug!("Application message processor task exited");
    }

    /// Static function: Raft Message Processor Task
    /// Processes Raft requests and generates responses
    async fn raft_message_processor_loop(
        mut raft_rx: mpsc::UnboundedReceiver<(NodeId, RaftMessage, Option<Uuid>)>,
        outgoing_tx: mpsc::UnboundedSender<(NodeId, Message, Option<Uuid>)>,
        raft_adapter_response_tx: mpsc::UnboundedSender<(NodeId, Uuid, Box<RaftAdapterResponse>)>,
        raft_instance: Arc<openraft::Raft<TypeConfig>>,
    ) {
        debug!("Raft message processor task started");
        while let Some((sender_id, raft_message, correlation_id)) = raft_rx.recv().await {
            match raft_message {
                // Handle Raft request messages
                RaftMessage::Vote(_)
                | RaftMessage::AppendEntries(_)
                | RaftMessage::InstallSnapshot(_) => {
                    if let Some((response_message, response_correlation_id)) =
                        Self::process_raft_request(
                            raft_message,
                            sender_id.clone(),
                            correlation_id,
                            &raft_instance,
                        )
                        .await
                    {
                        let response = Message::Raft(response_message);
                        if let Err(e) =
                            outgoing_tx.send((sender_id, response, response_correlation_id))
                        {
                            error!("Failed to send raft response: {}", e);
                        }
                    }
                }
                // Handle Raft response messages
                RaftMessage::VoteResponse(payload) => {
                    Self::process_raft_response::<openraft::raft::VoteResponse<TypeConfig>>(
                        &payload,
                        correlation_id,
                        &sender_id,
                        &raft_adapter_response_tx,
                        "VoteResponse",
                        |resp| RaftAdapterResponse::Vote(Box::new(resp)),
                    );
                }
                RaftMessage::AppendEntriesResponse(payload) => {
                    Self::process_raft_response::<openraft::raft::AppendEntriesResponse<TypeConfig>>(
                        &payload,
                        correlation_id,
                        &sender_id,
                        &raft_adapter_response_tx,
                        "AppendEntriesResponse",
                        RaftAdapterResponse::AppendEntries,
                    );
                }
                RaftMessage::InstallSnapshotResponse(payload) => {
                    Self::process_raft_response::<
                        openraft::raft::InstallSnapshotResponse<TypeConfig>,
                    >(
                        &payload,
                        correlation_id,
                        &sender_id,
                        &raft_adapter_response_tx,
                        "InstallSnapshotResponse",
                        RaftAdapterResponse::InstallSnapshot,
                    );
                }
            }
        }
        debug!("Raft message processor task exited");
    }

    /// Static function: Outgoing Message Sender Task
    /// Signs and sends all outgoing messages
    async fn outgoing_message_sender_loop(
        mut outgoing_rx: mpsc::UnboundedReceiver<(NodeId, Message, Option<Uuid>)>,
        transport_tx: mpsc::UnboundedSender<(NodeId, Bytes)>,
        cose_handler: Arc<CoseHandler>,
        _topology: Arc<crate::topology::TopologyManager<G>>,
    ) {
        debug!("Outgoing message sender task started");
        while let Some((target_node_id, message, correlation_id)) = outgoing_rx.recv().await {
            match Self::sign_message_static(&message, correlation_id, &cose_handler).await {
                Ok(signed_data) => {
                    if let Err(e) = transport_tx.send((target_node_id.clone(), signed_data)) {
                        error!("Failed to send signed message to {}: {}", target_node_id, e);
                    }
                }
                Err(e) => {
                    error!("Failed to sign message for {}: {}", target_node_id, e);
                }
            }
        }
        debug!("Outgoing message sender task exited");
    }

    /// Static function: Response Correlation Task
    /// Matches responses to pending requests
    async fn response_correlation_loop(
        mut response_rx: mpsc::UnboundedReceiver<(Uuid, Box<dyn std::any::Any + Send + Sync>)>,
        pending_requests: Arc<RwLock<HashMap<Uuid, Box<dyn std::any::Any + Send + Sync>>>>,
    ) {
        debug!("Response correlation task started");
        while let Some((correlation_id, response)) = response_rx.recv().await {
            let mut pending = pending_requests.write().await;
            if let Some(pending_request) = pending.remove(&correlation_id) {
                // Try to complete the pending request based on type matching
                if Self::complete_pending_request_static(pending_request, response).is_err() {
                    warn!(
                        "Failed to complete pending request {} - type mismatch",
                        correlation_id
                    );
                }
            } else {
                warn!(
                    "No pending request found for correlation_id {}",
                    correlation_id
                );
            }
        }
        debug!("Response correlation task exited");
    }

    /// Helper function: Process Raft response and send to RaftAdapter
    fn process_raft_response<T>(
        payload: &[u8],
        correlation_id: Option<Uuid>,
        sender_id: &NodeId,
        raft_adapter_response_tx: &mpsc::UnboundedSender<(NodeId, Uuid, Box<RaftAdapterResponse>)>,
        response_type: &str,
        response_constructor: impl FnOnce(T) -> RaftAdapterResponse,
    ) where
        T: serde::de::DeserializeOwned,
    {
        if let Some(corr_id) = correlation_id {
            match ciborium::de::from_reader::<T, _>(payload) {
                Ok(response) => {
                    if let Err(e) = raft_adapter_response_tx.send((
                        sender_id.clone(),
                        corr_id,
                        Box::new(response_constructor(response)),
                    )) {
                        error!(
                            "Failed to send {} response to RaftAdapter: {}",
                            response_type, e
                        );
                    }
                }
                Err(e) => {
                    error!("Failed to deserialize {}: {}", response_type, e);
                }
            }
        } else {
            debug!(
                "Received {} response without correlation ID from {}",
                response_type, sender_id
            );
        }
    }

    /// Static function: RaftAdapter Request Processor Task
    /// Handles RaftAdapter requests and sends them through the network
    async fn raft_adapter_processor_loop(
        mut request_rx: mpsc::UnboundedReceiver<(NodeId, Uuid, Box<RaftAdapterRequest>)>,
        outgoing_tx: mpsc::UnboundedSender<(NodeId, Message, Option<Uuid>)>,
    ) {
        debug!("RaftAdapter processor task started");

        while let Some((target_node_id, correlation_id, request)) = request_rx.recv().await {
            // Convert RaftAdapter request to network message
            let message = match *request {
                RaftAdapterRequest::Vote(vote_req) => {
                    let mut vote_data = Vec::new();
                    if let Err(_e) = ciborium::ser::into_writer(&vote_req, &mut vote_data) {
                        continue;
                    }
                    Message::Raft(RaftMessage::Vote(vote_data))
                }
                RaftAdapterRequest::AppendEntries(append_req) => {
                    let mut append_data = Vec::new();
                    if let Err(_e) = ciborium::ser::into_writer(&append_req, &mut append_data) {
                        continue;
                    }
                    Message::Raft(RaftMessage::AppendEntries(append_data))
                }
                RaftAdapterRequest::InstallSnapshot(snapshot_req) => {
                    let mut snapshot_data = Vec::new();
                    if let Err(e) = ciborium::ser::into_writer(&snapshot_req, &mut snapshot_data) {
                        error!("Failed to serialize InstallSnapshot request: {}", e);
                        continue;
                    }
                    Message::Raft(RaftMessage::InstallSnapshot(snapshot_data))
                }
            };

            // Send message through outgoing channel with correlation ID
            if let Err(e) =
                outgoing_tx.send((target_node_id.clone(), message, Some(correlation_id)))
            {
                error!(
                    "Failed to send RaftAdapter message to {}: {}",
                    target_node_id, e
                );
            }
        }

        debug!("RaftAdapter processor task exited");
    }

    /// Static helper: Parse incoming message and extract metadata
    async fn parse_incoming_message(
        data: &[u8],
        sender_node_id: &NodeId,
        cose_handler: &CoseHandler,
    ) -> Result<(Message, crate::cose::CoseMetadata), ConsensusError> {
        // Deserialize and verify COSE message
        let cose_message = cose_handler.deserialize_cose_message(data)?;

        // Verify the message signature and extract metadata
        let (payload_bytes, metadata) = cose_handler
            .verify_signed_message(&cose_message, sender_node_id.clone())
            .await?;

        // Deserialize the actual message
        let message: Message = ciborium::de::from_reader(payload_bytes.as_ref()).map_err(|e| {
            ConsensusError::InvalidMessage(format!("Failed to deserialize message: {e}"))
        })?;

        Ok((message, metadata))
    }

    /// Static helper: Process cluster discovery request
    async fn process_cluster_discovery_request(
        correlation_id: Option<Uuid>,
        local_node_id: &NodeId,
        raft_instance: &Arc<openraft::Raft<TypeConfig>>,
    ) -> Option<(ApplicationMessage, Option<Uuid>)> {
        // Get Raft metrics to determine cluster info
        let metrics = raft_instance.metrics().borrow().clone();

        let membership = &metrics.membership_config;
        let voter_ids: Vec<_> = membership.voter_ids().collect();
        let has_active_cluster = !voter_ids.is_empty();

        // Always respond with current status, even if no active cluster
        let response = ClusterDiscoveryResponse {
            responder_id: local_node_id.clone(),
            has_active_cluster,
            current_term: if has_active_cluster {
                Some(metrics.current_term)
            } else {
                None
            },
            current_leader: metrics.current_leader.clone(),
            cluster_size: if has_active_cluster {
                Some(voter_ids.len())
            } else {
                None
            },
        };

        Some((
            ApplicationMessage::ClusterDiscoveryResponse(response),
            correlation_id,
        ))
    }

    /// Static helper: Process cluster discovery response
    async fn process_cluster_discovery_response(
        response: ClusterDiscoveryResponse,
        correlation_id: Option<Uuid>,
        discovery_responses: &Arc<RwLock<HashMap<Uuid, ClusterDiscoveryResponse>>>,
        response_tx: &mpsc::UnboundedSender<(Uuid, Box<dyn std::any::Any + Send + Sync>)>,
    ) {
        if let Some(corr_id) = correlation_id {
            // Try to complete pending request
            if let Err(_e) = response_tx.send((corr_id, Box::new(response.clone()))) {
                // Fallback: Store in the discovery responses map for legacy compatibility
                discovery_responses.write().await.insert(corr_id, response);
            }
        } else {
            warn!("Received discovery response without correlation ID - ignoring");
        }
    }

    /// Static helper: Process cluster join request
    async fn process_cluster_join_request(
        request: ClusterJoinRequest,
        sender_node_id: NodeId,
        correlation_id: Option<Uuid>,
        local_node_id: &NodeId,
        raft_instance: &Arc<openraft::Raft<TypeConfig>>,
        _topology: &Arc<crate::topology::TopologyManager<G>>,
        outgoing_tx: &mpsc::UnboundedSender<(NodeId, Message, Option<Uuid>)>,
    ) {
        // Only the leader can handle join requests
        let metrics = raft_instance.metrics().borrow().clone();
        let is_leader = metrics.current_leader.as_ref() == Some(local_node_id);

        if !is_leader {
            warn!(
                "Received join request but not leader. Current leader: {:?}",
                metrics.current_leader
            );
            // Return error response indicating we're not the leader
            let response = ClusterJoinResponse {
                responder_id: local_node_id.clone(),
                success: false,
                error_message: Some(format!(
                    "Not the leader. Current leader: {:?}",
                    metrics.current_leader
                )),
                cluster_size: None,
                current_term: None,
            };
            let response_message =
                Message::Application(Box::new(ApplicationMessage::ClusterJoinResponse(response)));
            if let Err(e) = outgoing_tx.send((sender_node_id, response_message, correlation_id)) {
                error!("Failed to send join error response: {}", e);
            }
            return;
        }

        // Check if node is already in the cluster
        let is_already_member = metrics
            .membership_config
            .membership()
            .voter_ids()
            .any(|id| id == request.requester_id)
            || metrics
                .membership_config
                .membership()
                .learner_ids()
                .any(|id| id == request.requester_id);

        if is_already_member {
            // Return success response since node is already a member
            let response = ClusterJoinResponse {
                responder_id: local_node_id.clone(),
                success: true,
                error_message: None,
                cluster_size: Some(
                    metrics.membership_config.membership().voter_ids().count()
                        + metrics.membership_config.membership().learner_ids().count(),
                ),
                current_term: Some(metrics.current_term),
            };
            let response_message =
                Message::Application(Box::new(ApplicationMessage::ClusterJoinResponse(response)));
            if let Err(e) = outgoing_tx.send((sender_node_id, response_message, correlation_id)) {
                error!("Failed to send join success response: {}", e);
            }
            return;
        }

        // Spawn a background task for async membership change handling
        let raft_clone = raft_instance.clone();
        let requester_id = request.requester_id.clone();
        let requester_node = request.requester_node.clone();
        let local_node_id_clone = local_node_id.clone();
        let outgoing_tx_clone = outgoing_tx.clone();

        tokio::spawn(async move {
            match Self::add_node_to_cluster_with_retry_static(
                &raft_clone,
                requester_id.clone(),
                requester_node,
            )
            .await
            {
                Ok(()) => {
                    info!("Successfully added {} to cluster", requester_id);
                    // Send success response
                    let metrics = raft_clone.metrics().borrow().clone();
                    let response = ClusterJoinResponse {
                        responder_id: local_node_id_clone,
                        success: true,
                        error_message: None,
                        cluster_size: Some(
                            metrics.membership_config.membership().voter_ids().count()
                                + metrics.membership_config.membership().learner_ids().count(),
                        ),
                        current_term: Some(metrics.current_term),
                    };
                    let response_message = Message::Application(Box::new(
                        ApplicationMessage::ClusterJoinResponse(response),
                    ));
                    if let Err(e) =
                        outgoing_tx_clone.send((sender_node_id, response_message, correlation_id))
                    {
                        error!("Failed to send successful join response: {}", e);
                    }
                }
                Err(e) => {
                    error!("Failed to add {} to cluster: {}", requester_id, e);
                    // Send error response
                    let response = ClusterJoinResponse {
                        responder_id: local_node_id_clone,
                        success: false,
                        error_message: Some(e),
                        cluster_size: None,
                        current_term: None,
                    };
                    let response_message = Message::Application(Box::new(
                        ApplicationMessage::ClusterJoinResponse(response),
                    ));
                    if let Err(e) =
                        outgoing_tx_clone.send((sender_node_id, response_message, correlation_id))
                    {
                        error!("Failed to send failed join response: {}", e);
                    }
                }
            }
        });
    }

    /// Static helper: Process Raft request
    async fn process_raft_request(
        raft_message: RaftMessage,
        sender_id: NodeId,
        correlation_id: Option<Uuid>,
        raft_instance: &Arc<openraft::Raft<TypeConfig>>,
    ) -> Option<(RaftMessage, Option<Uuid>)> {
        match raft_message {
            RaftMessage::Vote(vote_data) => {
                let vote_request: openraft::raft::VoteRequest<TypeConfig> =
                    match ciborium::de::from_reader(vote_data.as_slice()) {
                        Ok(req) => req,
                        Err(e) => {
                            error!("Failed to deserialize VoteRequest: {}", e);
                            return None;
                        }
                    };

                match raft_instance.vote(vote_request).await {
                    Ok(vote_response) => {
                        let mut response_payload = Vec::new();
                        if let Err(e) =
                            ciborium::ser::into_writer(&vote_response, &mut response_payload)
                        {
                            error!("Failed to serialize vote response: {}", e);
                            return None;
                        }
                        Some((RaftMessage::VoteResponse(response_payload), correlation_id))
                    }
                    Err(e) => {
                        error!("Vote request failed: {}", e);
                        None
                    }
                }
            }
            RaftMessage::AppendEntries(append_data) => {
                let append_request: openraft::raft::AppendEntriesRequest<TypeConfig> =
                    match ciborium::de::from_reader(append_data.as_slice()) {
                        Ok(req) => req,
                        Err(e) => {
                            error!("Failed to deserialize AppendEntriesRequest: {}", e);
                            return None;
                        }
                    };

                match raft_instance.append_entries(append_request).await {
                    Ok(append_response) => {
                        let mut response_payload = Vec::new();
                        if let Err(e) =
                            ciborium::ser::into_writer(&append_response, &mut response_payload)
                        {
                            error!("Failed to serialize append entries response: {}", e);
                            return None;
                        }
                        Some((
                            RaftMessage::AppendEntriesResponse(response_payload),
                            correlation_id,
                        ))
                    }
                    Err(e) => {
                        error!("AppendEntries request failed: {}", e);
                        None
                    }
                }
            }
            RaftMessage::InstallSnapshot(snapshot_data) => {
                let snapshot_request: openraft::raft::InstallSnapshotRequest<TypeConfig> =
                    match ciborium::de::from_reader(snapshot_data.as_slice()) {
                        Ok(req) => req,
                        Err(e) => {
                            error!("Failed to deserialize InstallSnapshotRequest: {}", e);
                            return None;
                        }
                    };

                match raft_instance.install_snapshot(snapshot_request).await {
                    Ok(snapshot_response) => {
                        let mut response_payload = Vec::new();
                        if let Err(e) =
                            ciborium::ser::into_writer(&snapshot_response, &mut response_payload)
                        {
                            error!("Failed to serialize install snapshot response: {}", e);
                            return None;
                        }
                        Some((
                            RaftMessage::InstallSnapshotResponse(response_payload),
                            correlation_id,
                        ))
                    }
                    Err(e) => {
                        error!("InstallSnapshot request failed: {}", e);
                        None
                    }
                }
            }
            _ => {
                warn!("Received unexpected raft message type from {}", sender_id);
                None
            }
        }
    }

    /// Static helper: Sign message with optional correlation ID
    async fn sign_message_static(
        message: &Message,
        correlation_id: Option<Uuid>,
        cose_handler: &CoseHandler,
    ) -> Result<Bytes, ConsensusError> {
        // Serialize the message using CBOR
        let mut message_data = Vec::new();
        ciborium::ser::into_writer(message, &mut message_data).map_err(|e| {
            ConsensusError::InvalidMessage(format!("Failed to serialize message: {e}"))
        })?;

        // Determine message type
        let message_type = match message {
            Message::Application(app_msg) => match app_msg.as_ref() {
                ApplicationMessage::ClusterDiscovery(_) => "cluster_discovery_request",
                ApplicationMessage::ClusterDiscoveryResponse(_) => "cluster_discovery_response",
                ApplicationMessage::ClusterJoinRequest(_) => "cluster_join_request",
                ApplicationMessage::ClusterJoinResponse(_) => "cluster_join_response",
                ApplicationMessage::ConsensusRequest(_) => "consensus_request",
                ApplicationMessage::ConsensusResponse(_) => "consensus_response",
            },
            Message::Raft(RaftMessage::Vote(_)) => "raft_vote_request",
            Message::Raft(RaftMessage::VoteResponse(_)) => "raft_vote_response",
            Message::Raft(RaftMessage::AppendEntries(_)) => "raft_append_entries_request",
            Message::Raft(RaftMessage::AppendEntriesResponse(_)) => "raft_append_entries_response",
            Message::Raft(RaftMessage::InstallSnapshot(_)) => "raft_install_snapshot_request",
            Message::Raft(RaftMessage::InstallSnapshotResponse(_)) => {
                "raft_install_snapshot_response"
            }
        };

        // Sign with COSE
        let cose_message =
            cose_handler.create_signed_message(&message_data, message_type, correlation_id)?;
        let cose_data = cose_handler.serialize_cose_message(&cose_message)?;

        Ok(Bytes::from(cose_data))
    }

    /// Static helper: Complete pending request with type checking
    fn complete_pending_request_static(
        pending_request: Box<dyn std::any::Any + Send + Sync>,
        response: Box<dyn std::any::Any + Send + Sync>,
    ) -> Result<(), ConsensusError> {
        // Try discovery response first - use Err variant to recover the Box
        let response = match response.downcast::<ClusterDiscoveryResponse>() {
            Ok(discovery_response) => {
                if let Ok(discovery_request) =
                    pending_request.downcast::<PendingRequest<ClusterDiscoveryResponse>>()
                {
                    if discovery_request
                        .response_tx
                        .send(*discovery_response)
                        .is_err()
                    {
                        warn!("Discovery response receiver was dropped");
                    }
                    return Ok(());
                } else {
                    return Err(ConsensusError::InvalidMessage(
                        "Type mismatch: discovery response with non-discovery request".to_string(),
                    ));
                }
            }
            Err(response) => response, // Recover the original Box
        };

        // Try join response - use Err variant to recover the Box
        let response = match response.downcast::<ClusterJoinResponse>() {
            Ok(join_response) => {
                if let Ok(join_request) =
                    pending_request.downcast::<PendingRequest<ClusterJoinResponse>>()
                {
                    if join_request.response_tx.send(*join_response).is_err() {
                        warn!("Join response receiver was dropped");
                    }
                    return Ok(());
                } else {
                    return Err(ConsensusError::InvalidMessage(
                        "Type mismatch: join response with non-join request".to_string(),
                    ));
                }
            }
            Err(response) => response, // Recover the original Box
        };

        // Try raft response
        if let Ok(raft_response) = response.downcast::<Vec<u8>>() {
            if let Ok(raft_request) = pending_request.downcast::<PendingRequest<Vec<u8>>>() {
                if raft_request.response_tx.send(*raft_response).is_err() {
                    warn!("Raft response receiver was dropped");
                }
                return Ok(());
            } else {
                return Err(ConsensusError::InvalidMessage(
                    "Type mismatch: raft response with non-raft request".to_string(),
                ));
            }
        }

        Err(ConsensusError::InvalidMessage(
            "Unknown response type in pending request completion".to_string(),
        ))
    }

    /// Static helper: Add node to cluster with retry logic
    async fn add_node_to_cluster_with_retry_static(
        raft: &Arc<openraft::Raft<crate::types::TypeConfig>>,
        requester_id: NodeId,
        requester_node: proven_governance::GovernanceNode,
    ) -> Result<(), String> {
        const MAX_RETRIES: usize = 5;
        const RETRY_DELAY: tokio::time::Duration = tokio::time::Duration::from_millis(500);

        // First add as learner
        for attempt in 1..=MAX_RETRIES {
            info!("Attempt {} to add {} as learner", attempt, requester_id);

            match raft
                .add_learner(
                    requester_id.clone(),
                    crate::types::Node::from(requester_node.clone()),
                    true,
                )
                .await
            {
                Ok(_) => {
                    info!(
                        "Successfully added {} as learner on attempt {}",
                        requester_id, attempt
                    );
                    break;
                }
                Err(e) => {
                    let error_msg = format!("{}", e);
                    if error_msg.contains("undergoing a configuration change")
                        && attempt < MAX_RETRIES
                    {
                        info!(
                            "Cluster is undergoing configuration change, retrying add learner in {:?} (attempt {}/{})",
                            RETRY_DELAY, attempt, MAX_RETRIES
                        );
                        tokio::time::sleep(RETRY_DELAY).await;
                        continue;
                    } else {
                        return Err(format!("Failed to add {} as learner: {}", requester_id, e));
                    }
                }
            }
        }

        // Wait a bit for the learner to catch up
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        // Then promote to voter
        Self::promote_learner_to_voter_static(raft, requester_id.clone()).await
    }

    /// Static helper: Promote learner to voter with retry logic
    async fn promote_learner_to_voter_static(
        raft: &Arc<openraft::Raft<crate::types::TypeConfig>>,
        requester_id: NodeId,
    ) -> Result<(), String> {
        const MAX_RETRIES: usize = 5;
        const RETRY_DELAY: tokio::time::Duration = tokio::time::Duration::from_millis(500);

        for attempt in 1..=MAX_RETRIES {
            info!("Attempt {} to promote {} to voter", attempt, requester_id);

            // Get current membership
            let metrics = raft.metrics().borrow().clone();
            let current_members = metrics.membership_config.membership().voter_ids();
            let mut new_members: std::collections::BTreeSet<NodeId> = current_members.collect();

            // Add the new node
            new_members.insert(requester_id.clone());

            // Try to promote learner to voter
            match raft.change_membership(new_members, false).await {
                Ok(_) => {
                    info!(
                        "Successfully promoted {} to voter on attempt {}",
                        requester_id, attempt
                    );
                    return Ok(());
                }
                Err(e) => {
                    let error_msg = format!("{}", e);
                    if error_msg.contains("undergoing a configuration change") {
                        info!(
                            "Cluster is undergoing configuration change, retrying promotion in {:?} (attempt {}/{})",
                            RETRY_DELAY, attempt, MAX_RETRIES
                        );
                        tokio::time::sleep(RETRY_DELAY).await;
                        continue;
                    } else {
                        return Err(format!(
                            "Failed to promote {} to voter: {}",
                            requester_id, e
                        ));
                    }
                }
            }
        }

        Err(format!(
            "Failed to promote {} to voter after {} attempts",
            requester_id, MAX_RETRIES
        ))
    }
}
