//! Cluster management functionality
//!
//! This module handles cluster discovery, joining, and initialization operations.

use crate::{
    ConsensusGroupId,
    core::{
        engine::{global_layer::GlobalConsensusLayer, groups_layer::GroupsConsensusLayer},
        global::GlobalRequest,
    },
    error::{ConsensusResult, Error},
    network::{
        ClusterState, InitiatorReason,
        messages::{
            ClusterDiscoveryRequest, ClusterDiscoveryResponse, ClusterJoinRequest,
            ClusterJoinResponse, GlobalMessage, Message,
        },
    },
    operations::{GlobalOperation, handlers::GlobalOperationResponse},
};

use proven_governance::Governance;
use proven_network::NetworkManager;
use proven_topology::{NodeId, TopologyManager};
use proven_transport::Transport;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::RwLock;
use tracing::{debug, error, info};

/// Cluster manager for handling cluster operations
pub struct ClusterManager<T, G>
where
    T: Transport,
    G: Governance,
{
    /// Node ID
    node_id: NodeId,
    /// Global consensus layer
    global_layer: Arc<GlobalConsensusLayer>,
    /// Groups consensus layer
    groups_layer: Arc<RwLock<GroupsConsensusLayer<T, G>>>,
    /// Network manager
    network_manager: Arc<NetworkManager<T, G>>,
    /// Topology manager
    topology: Arc<TopologyManager<G>>,
    /// Current cluster state
    cluster_state: Arc<RwLock<ClusterState>>,
    /// Cluster join retry configuration
    cluster_join_retry_config: crate::config::ClusterJoinRetryConfig,
}

impl<T, G> ClusterManager<T, G>
where
    T: Transport,
    G: Governance,
{
    /// Create a new cluster manager
    pub fn new(
        node_id: NodeId,
        global_layer: Arc<GlobalConsensusLayer>,
        groups_layer: Arc<RwLock<GroupsConsensusLayer<T, G>>>,
        network_manager: Arc<NetworkManager<T, G>>,
        topology: Arc<TopologyManager<G>>,
        cluster_join_retry_config: crate::config::ClusterJoinRetryConfig,
    ) -> Self {
        Self {
            node_id,
            global_layer,
            groups_layer,
            network_manager,
            topology,
            cluster_state: Arc::new(RwLock::new(ClusterState::Discovering)),
            cluster_join_retry_config,
        }
    }

    /// Get current cluster state
    pub fn cluster_state(&self) -> Arc<RwLock<ClusterState>> {
        self.cluster_state.clone()
    }

    /// Check if we have an active cluster
    pub async fn has_active_cluster(&self) -> bool {
        let state = self.cluster_state.read().await;
        matches!(
            *state,
            ClusterState::Initiator { .. } | ClusterState::Joined { .. }
        )
    }

    /// Handle cluster discovery request
    pub async fn handle_discovery_request(
        &self,
        _sender_id: NodeId,
        _req: ClusterDiscoveryRequest,
    ) -> ConsensusResult<ClusterDiscoveryResponse> {
        info!("Handling cluster discovery request from {}", _sender_id);

        // Check if we're part of an active cluster
        let has_active_cluster = self.has_active_cluster().await;

        // Build discovery response based on our current state
        let response = {
            let metrics = self.global_layer.global_raft().metrics();
            let borrowed_metrics = metrics.borrow();

            ClusterDiscoveryResponse {
                responder_id: self.node_id.clone(),
                has_active_cluster,
                current_term: Some(borrowed_metrics.current_term),
                current_leader: borrowed_metrics.current_leader.clone(),
                cluster_size: Some(borrowed_metrics.membership_config.voter_ids().count()),
            }
        };

        Ok(response)
    }

    /// Handle cluster join request
    pub async fn handle_join_request(
        &self,
        sender_id: NodeId,
        req: ClusterJoinRequest,
    ) -> ConsensusResult<ClusterJoinResponse> {
        info!("Handling cluster join request from {}", sender_id);

        // Only process join requests if we're the leader
        if !self.global_layer.is_leader().await {
            return Ok(ClusterJoinResponse {
                error_message: Some("Not the cluster leader".to_string()),
                cluster_size: None,
                current_term: None,
                responder_id: self.node_id.clone(),
                success: false,
                current_leader: self
                    .global_layer
                    .global_raft()
                    .metrics()
                    .borrow()
                    .current_leader
                    .clone(),
            });
        }

        // Convert GovernanceNode to our Node type
        let node = req.requester_node;

        match self
            .global_layer
            .global_raft()
            .add_learner(sender_id.clone(), node, true)
            .await
        {
            Ok(_) => {
                info!("Successfully added learner {} to cluster", sender_id);

                // Get current metrics
                let metrics = self.global_layer.global_raft().metrics();
                let borrowed_metrics = metrics.borrow();
                let cluster_size = borrowed_metrics.membership_config.nodes().count();

                Ok(ClusterJoinResponse {
                    error_message: None,
                    cluster_size: Some(cluster_size),
                    current_term: Some(borrowed_metrics.current_term),
                    responder_id: self.node_id.clone(),
                    success: true,
                    current_leader: Some(self.node_id.clone()),
                })
            }
            Err(e) => {
                error!("Failed to add learner {} to cluster: {:?}", sender_id, e);

                Ok(ClusterJoinResponse {
                    error_message: Some(format!("Failed to add learner: {e:?}")),
                    cluster_size: None,
                    current_term: None,
                    responder_id: self.node_id.clone(),
                    success: false,
                    current_leader: None,
                })
            }
        }
    }

    /// Initialize as a single-node cluster
    pub async fn initialize_single_node(
        &self,
        reason: InitiatorReason,
        submit_global_request: impl Fn(
            GlobalRequest,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = ConsensusResult<GlobalOperationResponse>> + Send>,
        >,
    ) -> ConsensusResult<()> {
        info!("Initializing single-node cluster (reason: {:?})", reason);

        // Update cluster state
        {
            let mut state = self.cluster_state.write().await;
            *state = ClusterState::Initiator {
                initiated_at: Instant::now(),
                reason: reason.clone(),
            };
        }

        // Initialize global consensus
        // Get our node info from topology
        let my_node = self
            .topology
            .get_node(&self.node_id)
            .await
            .ok_or_else(|| Error::InvalidState("Node not found in topology".to_string()))?;

        let mut nodes = std::collections::BTreeMap::new();
        nodes.insert(self.node_id.clone(), my_node);

        self.global_layer
            .global_raft()
            .initialize(nodes)
            .await
            .map_err(|e| Error::Raft(format!("Failed to initialize global consensus: {e:?}")))?;

        info!("Global consensus initialized");

        // Wait for this node to become leader
        let start = Instant::now();
        let timeout = Duration::from_secs(10);
        loop {
            if self.global_layer.is_leader().await {
                break;
            }
            if start.elapsed() > timeout {
                return Err(Error::Timeout {
                    seconds: timeout.as_secs(),
                });
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        info!("Became global leader");

        // Initialize the initial consensus group
        let initial_group_id = ConsensusGroupId::initial();
        let initial_members = vec![self.node_id.clone()];

        // Submit global operation to create the initial group
        let operation = GlobalOperation::Group(crate::operations::GroupOperation::Create {
            group_id: initial_group_id,
            initial_members: initial_members.clone(),
        });

        let request = GlobalRequest { operation };
        let response = submit_global_request(request).await?;

        if !response.is_success() {
            return Err(Error::ConsensusFailed(
                response
                    .error()
                    .unwrap_or("Failed to create initial group")
                    .to_string(),
            ));
        }

        info!("Created initial consensus group {:?}", initial_group_id);

        // Initialize the consensus group
        self.groups_layer
            .read()
            .await
            .groups_manager()
            .write()
            .await
            .create_group(initial_group_id, initial_members)
            .await?;

        info!(
            "Single-node cluster initialization complete (group: {:?})",
            initial_group_id
        );

        Ok(())
    }

    /// Try to join an existing cluster
    pub async fn try_join_cluster(
        &self,
        target_node: NodeId,
        timeout: Duration,
    ) -> ConsensusResult<()> {
        info!("Attempting to join cluster via node {}", target_node);

        // Send join request
        let my_node = self
            .topology
            .get_node(&self.node_id)
            .await
            .ok_or_else(|| Error::InvalidState("Node not found in topology".to_string()))?;

        let join_request = ClusterJoinRequest {
            requester_id: self.node_id.clone(),
            requester_node: my_node,
        };

        // Send the request and wait for response
        let request_msg =
            Message::Global(Box::new(GlobalMessage::ClusterJoinRequest(join_request)));

        let target_node_clone = target_node.clone();
        match self
            .network_manager
            .request(target_node_clone, request_msg, timeout)
            .await
        {
            Ok(response_msg) => {
                if let Message::Global(msg) = response_msg {
                    if let GlobalMessage::ClusterJoinResponse(resp) = msg.as_ref() {
                        if resp.success {
                            // Update state to joined
                            let mut state = self.cluster_state.write().await;
                            *state = ClusterState::Joined {
                                joined_at: Instant::now(),
                                cluster_size: resp.cluster_size.unwrap_or(0),
                            };
                            drop(state);

                            info!(
                                "Successfully joined cluster (size: {})",
                                resp.cluster_size.unwrap_or(0)
                            );
                            Ok(())
                        } else {
                            Err(Error::InvalidOperation(format!(
                                "Join request rejected: {}",
                                resp.error_message
                                    .clone()
                                    .unwrap_or_else(|| "Unknown reason".to_string())
                            )))
                        }
                    } else {
                        Err(Error::InvalidOperation(
                            "Unexpected response type".to_string(),
                        ))
                    }
                } else {
                    Err(Error::InvalidOperation(
                        "Unexpected message type in response".to_string(),
                    ))
                }
            }
            Err(e) => Err(Error::Network(crate::error::NetworkError::SendFailed {
                peer: target_node.clone(),
                reason: e.to_string(),
            })),
        }
    }

    /// Discover active clusters in the network
    pub async fn discover_clusters(&self) -> ConsensusResult<Vec<ClusterDiscoveryResponse>> {
        info!("Starting cluster discovery");

        // Get all known peers from topology
        let peers = self.topology.get_all_peers().await;

        if peers.is_empty() {
            info!("No peers found in topology for discovery");
            return Ok(Vec::new());
        }

        info!("Sending discovery requests to {} peers", peers.len());

        // Create discovery request
        let request = ClusterDiscoveryRequest::new(self.node_id.clone());
        let mut responses = Vec::new();

        // Send discovery request to each peer
        for peer in peers {
            // Send discovery request with timeout
            let discovery_msg = Message::from(request.clone());

            match self
                .network_manager
                .request(
                    peer.node_id().clone(),
                    discovery_msg,
                    Duration::from_secs(5),
                )
                .await
            {
                Ok(response_msg) => match response_msg {
                    Message::Global(msg) => {
                        if let GlobalMessage::ClusterDiscoveryResponse(response) = msg.as_ref() {
                            info!(
                                "Received discovery response from {}: has_cluster={}, leader={:?}",
                                response.responder_id,
                                response.has_active_cluster,
                                response.current_leader
                            );
                            responses.push(response.clone());
                        } else {
                            debug!("Unexpected response type from {}", peer.node_id());
                        }
                    }
                    _ => {
                        debug!("Unexpected message type from {}", peer.node_id());
                    }
                },
                Err(e) => {
                    debug!("Discovery request to {} failed: {}", peer.node_id(), e);
                }
            }
        }

        info!(
            "Cluster discovery complete. Received {} responses",
            responses.len()
        );
        Ok(responses)
    }
}
