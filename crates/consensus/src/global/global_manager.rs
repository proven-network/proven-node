//! Global consensus manager
//!
//! This module manages the global Raft consensus operations using the new
//! categorized operations structure.

use super::{
    GlobalResponse, GlobalTypeConfig,
    global_state::{ConsensusGroupInfo, GlobalState},
};
use crate::{
    NodeId,
    allocation::ConsensusGroupId,
    error::{ConsensusResult, Error},
    global::StreamConfig,
    network::adaptor::{RaftAdapterRequest, RaftAdapterResponse},
    operations::{
        GlobalOperation, OperationContext, OperationValidator,
        group_ops::GroupOperation,
        node_ops::NodeOperation,
        routing_ops::RoutingOperation,
        stream_management_ops::StreamManagementOperation,
        validators::{
            GroupOperationValidator, NodeOperationValidator, RoutingOperationValidator,
            StreamManagementOperationValidator,
        },
    },
};
use openraft::{Config, Raft, RaftMetrics, RaftNetworkFactory, error::ClientWriteError};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{RwLock, oneshot};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// Request tracking for async consensus operations
pub struct PendingRequest {
    /// Response sender
    pub sender: oneshot::Sender<GlobalResponse>,
    /// The operation being requested
    pub operation: GlobalOperation,
}

/// Global consensus manager with operations support
pub struct GlobalManager<G, A>
where
    G: proven_governance::Governance + Send + Sync + 'static,
    A: proven_attestation::Attestor + Send + Sync + 'static,
{
    /// The Raft instance
    raft: Raft<GlobalTypeConfig>,
    /// Global state machine
    state: Arc<GlobalState>,
    /// Pending client requests
    pending_requests: Arc<RwLock<HashMap<u64, PendingRequest>>>,
    /// Request ID counter
    #[allow(dead_code)]
    request_counter: Arc<RwLock<u64>>,
    /// Node ID
    node_id: NodeId,
    /// Operation validators
    stream_validator: StreamManagementOperationValidator,
    group_validator: GroupOperationValidator,
    node_validator: NodeOperationValidator,
    routing_validator: RoutingOperationValidator,
    /// Topology manager reference
    topology: Option<Arc<crate::topology::TopologyManager<G>>>,
    /// Current cluster state
    cluster_state: Arc<RwLock<crate::network::ClusterState>>,
    /// Network manager for sending messages
    network_manager: Arc<crate::network::network_manager::NetworkManager<G, A>>,
    /// Pending discovery responses by correlation ID
    pending_discovery_responses: Arc<
        RwLock<HashMap<Uuid, oneshot::Sender<crate::network::messages::ClusterDiscoveryResponse>>>,
    >,
    /// Pending join responses by correlation ID
    pending_join_responses:
        Arc<RwLock<HashMap<Uuid, oneshot::Sender<crate::network::messages::ClusterJoinResponse>>>>,
    /// Cluster join retry configuration
    cluster_join_retry_config: crate::config::ClusterJoinRetryConfig,
    /// Raft adapter response channel for sending responses back to RaftNetwork
    raft_adapter_response_tx: tokio::sync::mpsc::UnboundedSender<(
        NodeId,
        Uuid,
        Box<RaftAdapterResponse<GlobalTypeConfig>>,
    )>,
}

impl<G, A> GlobalManager<G, A>
where
    G: proven_governance::Governance + Send + Sync + 'static,
    A: proven_attestation::Attestor + Send + Sync + 'static,
{
    /// Create a new GlobalManager
    #[allow(clippy::too_many_arguments)]
    pub async fn new<N, S>(
        node_id: NodeId,
        config: Config,
        network: N,
        storage: S,
        global_state: Arc<GlobalState>,
        topology: Option<Arc<crate::topology::TopologyManager<G>>>,
        network_manager: Arc<crate::network::network_manager::NetworkManager<G, A>>,
        cluster_join_retry_config: crate::config::ClusterJoinRetryConfig,
        raft_adapter_request_rx: tokio::sync::mpsc::UnboundedReceiver<(
            NodeId,
            Uuid,
            Box<RaftAdapterRequest<GlobalTypeConfig>>,
        )>,
        raft_adapter_response_tx: tokio::sync::mpsc::UnboundedSender<(
            NodeId,
            Uuid,
            Box<RaftAdapterResponse<GlobalTypeConfig>>,
        )>,
    ) -> ConsensusResult<Self>
    where
        N: RaftNetworkFactory<GlobalTypeConfig>,
        S: openraft::storage::RaftLogStorage<GlobalTypeConfig>
            + openraft::storage::RaftStateMachine<GlobalTypeConfig>
            + Clone
            + Send
            + Sync
            + 'static,
    {
        let config = Arc::new(config);
        let raft = Raft::new(node_id.clone(), config, network, storage.clone(), storage)
            .await
            .map_err(|e| Error::Raft(format!("Failed to create Raft instance: {}", e)))?;

        // Start the RaftAdapter processor task
        let network_manager_clone = network_manager.clone();
        tokio::spawn(Self::raft_adapter_processor_loop(
            raft_adapter_request_rx,
            network_manager_clone,
        ));

        // The Raft response processing is handled in handle_raft_message

        Ok(Self {
            raft: raft.clone(),
            state: global_state,
            pending_requests: Arc::new(RwLock::new(HashMap::new())),
            request_counter: Arc::new(RwLock::new(0)),
            node_id,
            stream_validator: StreamManagementOperationValidator,
            group_validator: GroupOperationValidator::default(),
            node_validator: NodeOperationValidator::default(),
            routing_validator: RoutingOperationValidator,
            topology,
            cluster_state: Arc::new(RwLock::new(crate::network::ClusterState::Discovering)),
            network_manager,
            pending_discovery_responses: Arc::new(RwLock::new(HashMap::new())),
            pending_join_responses: Arc::new(RwLock::new(HashMap::new())),
            cluster_join_retry_config,
            raft_adapter_response_tx,
        })
    }

    /// Get reference to the state
    pub fn state(&self) -> Arc<GlobalState> {
        self.state.clone()
    }

    /// Get the Raft metrics
    pub async fn metrics(&self) -> ConsensusResult<RaftMetrics<GlobalTypeConfig>> {
        let metrics = self.raft.metrics();
        let current_metrics = metrics.borrow().clone();
        Ok(current_metrics)
    }

    /// Initialize a single-node cluster
    pub async fn initialize_cluster(&self) -> ConsensusResult<()> {
        // For now, create a placeholder node
        // In production, this would come from governance
        let specializations = std::collections::HashSet::new();

        let governance_node = proven_governance::GovernanceNode {
            public_key: *self.node_id.verifying_key(),
            origin: "http://localhost:8080".to_string(),
            availability_zone: "us-east-1a".to_string(),
            region: "us-east-1".to_string(),
            specializations,
        };
        let node = crate::node::Node::from(governance_node);
        let mut nodes = std::collections::BTreeMap::new();
        nodes.insert(self.node_id.clone(), node);

        self.raft
            .initialize(nodes)
            .await
            .map_err(|e| Error::Raft(format!("Failed to initialize cluster: {}", e)))?;

        info!(
            "Initialized global consensus cluster with node {}",
            self.node_id
        );
        Ok(())
    }

    /// Join an existing cluster
    pub async fn join_cluster(
        &self,
        leader_id: NodeId,
        leader_addr: String,
    ) -> ConsensusResult<()> {
        // Implementation would depend on network factory
        info!(
            "Joining cluster via leader {} at {}",
            leader_id, leader_addr
        );
        Ok(())
    }

    /// Submit an operation through consensus
    pub async fn submit_operation(
        &self,
        operation: GlobalOperation,
    ) -> ConsensusResult<GlobalResponse> {
        // Validate the operation first
        let is_leader = self.is_leader().await;
        let context = OperationContext {
            global_state: &self.state,
            node_id: self.node_id.clone(),
            is_leader,
        };

        match &operation {
            GlobalOperation::StreamManagement(op) => {
                self.stream_validator.validate(op, &context).await?;
            }
            GlobalOperation::Group(op) => {
                self.group_validator.validate(op, &context).await?;
            }
            GlobalOperation::Node(op) => {
                self.node_validator.validate(op, &context).await?;
            }
            GlobalOperation::Routing(op) => {
                self.routing_validator.validate(op, &context).await?;
            }
        }

        // Create request with operation
        let request = super::GlobalRequest { operation };

        // Submit through Raft
        let response = self.raft.client_write(request).await.map_err(|e| {
            use openraft::error::RaftError;
            match e {
                RaftError::APIError(ClientWriteError::ForwardToLeader { .. }) => {
                    Error::NotLeader { leader: None }
                }
                _ => Error::Raft(format!("Write failed: {}", e)),
            }
        })?;

        Ok(response.data)
    }

    /// Process a response from the state machine
    pub async fn handle_state_machine_response(&self, request_id: u64, response: GlobalResponse) {
        let mut pending = self.pending_requests.write().await;
        if let Some(pending_request) = pending.remove(&request_id) {
            let _ = pending_request.sender.send(response);
        }
    }

    /// Get the next request ID
    #[allow(dead_code)]
    async fn next_request_id(&self) -> u64 {
        let mut counter = self.request_counter.write().await;
        let id = *counter;
        *counter += 1;
        id
    }

    // High-level operations using the new structure

    /// Create a stream
    pub async fn create_stream(
        &self,
        name: String,
        config: StreamConfig,
        group_id: ConsensusGroupId,
    ) -> ConsensusResult<GlobalResponse> {
        let operation = GlobalOperation::StreamManagement(StreamManagementOperation::Create {
            name,
            config,
            group_id,
        });
        self.submit_operation(operation).await
    }

    /// Delete a stream
    pub async fn delete_stream(&self, name: String) -> ConsensusResult<GlobalResponse> {
        let operation =
            GlobalOperation::StreamManagement(StreamManagementOperation::Delete { name });
        self.submit_operation(operation).await
    }

    /// Create a consensus group
    pub async fn create_group(
        &self,
        group_id: ConsensusGroupId,
        members: Vec<NodeId>,
    ) -> ConsensusResult<GlobalResponse> {
        let operation = GlobalOperation::Group(GroupOperation::Create {
            group_id,
            initial_members: members,
        });
        self.submit_operation(operation).await
    }

    /// Delete a consensus group
    pub async fn delete_group(
        &self,
        group_id: ConsensusGroupId,
    ) -> ConsensusResult<GlobalResponse> {
        let operation = GlobalOperation::Group(GroupOperation::Delete { group_id });
        self.submit_operation(operation).await
    }

    /// Assign a node to a group
    pub async fn assign_node_to_group(
        &self,
        node_id: NodeId,
        group_id: ConsensusGroupId,
    ) -> ConsensusResult<GlobalResponse> {
        let operation = GlobalOperation::Node(NodeOperation::AssignToGroup { node_id, group_id });
        self.submit_operation(operation).await
    }

    /// Subscribe a stream to a subject
    pub async fn subscribe_to_subject(
        &self,
        stream_name: String,
        subject_pattern: String,
    ) -> ConsensusResult<GlobalResponse> {
        let operation = GlobalOperation::Routing(RoutingOperation::Subscribe {
            stream_name,
            subject_pattern,
        });
        self.submit_operation(operation).await
    }

    /// Get stream configuration
    pub async fn get_stream_config(&self, stream_name: &str) -> Option<StreamConfig> {
        self.state.get_stream_config(stream_name).await
    }

    /// Get all consensus groups
    pub async fn get_all_groups(&self) -> Vec<ConsensusGroupInfo> {
        self.state.get_all_groups().await
    }

    /// Get a specific consensus group
    pub async fn get_group(&self, group_id: ConsensusGroupId) -> Option<ConsensusGroupInfo> {
        self.state.get_group(group_id).await
    }

    /// Check if this node is the leader
    pub async fn is_leader(&self) -> bool {
        let metrics = self.raft.metrics();
        let current_metrics = metrics.borrow();
        current_metrics.state.is_leader()
    }

    /// Get the current term
    pub fn current_term(&self) -> Option<u64> {
        let metrics = self.raft.metrics();
        let current_metrics = metrics.borrow();
        Some(current_metrics.current_term)
    }

    /// Get the current leader
    pub async fn current_leader(&self) -> Option<String> {
        let metrics = self.raft.metrics();
        let current_metrics = metrics.borrow();
        current_metrics
            .current_leader
            .as_ref()
            .map(|id| id.to_string())
    }

    /// Get the cluster size  
    pub fn cluster_size(&self) -> Option<usize> {
        let metrics = self.raft.metrics();
        let current_metrics = metrics.borrow();
        Some(current_metrics.membership_config.nodes().count())
    }

    /// Check if this node has an active cluster
    pub async fn has_active_cluster(&self) -> bool {
        self.is_leader().await || self.current_leader().await.is_some()
    }

    /// Propose a request through Raft consensus
    pub async fn propose_request(&self, request: &super::GlobalRequest) -> ConsensusResult<u64> {
        // Submit the operation directly
        let response = self.submit_operation(request.operation.clone()).await?;
        Ok(response.sequence)
    }

    /// Handle network messages (Raft and Application)
    pub async fn handle_network_message(
        &self,
        node_id: NodeId,
        message: crate::network::messages::Message,
        correlation_id: Option<uuid::Uuid>,
    ) -> ConsensusResult<()> {
        use crate::network::messages::Message;

        info!(
            "GlobalManager handling network message from {}: message_type={:?}, correlation_id={:?}",
            node_id,
            match &message {
                Message::Raft(_) => "Raft",
                Message::Application(app) => match app.as_ref() {
                    crate::network::messages::ApplicationMessage::ClusterDiscovery(_) =>
                        "ClusterDiscovery",
                    crate::network::messages::ApplicationMessage::ClusterDiscoveryResponse(_) =>
                        "ClusterDiscoveryResponse",
                    crate::network::messages::ApplicationMessage::ClusterJoinRequest(_) =>
                        "ClusterJoinRequest",
                    crate::network::messages::ApplicationMessage::ClusterJoinResponse(_) =>
                        "ClusterJoinResponse",
                    crate::network::messages::ApplicationMessage::ConsensusRequest(_) =>
                        "ConsensusRequest",
                    crate::network::messages::ApplicationMessage::ConsensusResponse(_) =>
                        "ConsensusResponse",
                    crate::network::messages::ApplicationMessage::PubSub(_) => "PubSub",
                },
            },
            correlation_id
        );

        match message {
            Message::Raft(raft_msg) => {
                self.handle_raft_message(node_id, raft_msg, correlation_id)
                    .await
            }
            Message::Application(app_msg) => {
                self.handle_application_message(node_id, *app_msg, correlation_id)
                    .await
            }
        }
    }

    /// Handle Raft protocol messages
    async fn handle_raft_message(
        &self,
        sender_id: NodeId,
        raft_message: crate::network::messages::RaftMessage,
        correlation_id: Option<Uuid>,
    ) -> ConsensusResult<()> {
        use crate::network::messages::RaftMessage;

        match raft_message {
            RaftMessage::Vote(vote_data) => {
                let vote_request: openraft::raft::VoteRequest<GlobalTypeConfig> =
                    match ciborium::de::from_reader(vote_data.as_slice()) {
                        Ok(req) => req,
                        Err(e) => {
                            error!("Failed to deserialize VoteRequest: {}", e);
                            return Ok(());
                        }
                    };

                match self.raft.vote(vote_request).await {
                    Ok(vote_response) => {
                        let mut response_payload = Vec::new();
                        if let Err(e) =
                            ciborium::ser::into_writer(&vote_response, &mut response_payload)
                        {
                            error!("Failed to serialize vote response: {}", e);
                            return Ok(());
                        }

                        // Send response back through network
                        let response_msg = crate::network::messages::Message::Raft(
                            RaftMessage::VoteResponse(response_payload),
                        );

                        if let Some(corr_id) = correlation_id {
                            self.network_manager()
                                .send_message_with_correlation(sender_id, response_msg, corr_id)
                                .await?;
                        } else {
                            self.network_manager()
                                .send_message(sender_id, response_msg)
                                .await?;
                        }
                    }
                    Err(e) => {
                        error!("Vote request failed: {}", e);
                    }
                }
            }
            RaftMessage::AppendEntries(append_data) => {
                let append_request: openraft::raft::AppendEntriesRequest<GlobalTypeConfig> =
                    match ciborium::de::from_reader(append_data.as_slice()) {
                        Ok(req) => req,
                        Err(e) => {
                            error!("Failed to deserialize AppendEntriesRequest: {}", e);
                            return Ok(());
                        }
                    };

                match self.raft.append_entries(append_request).await {
                    Ok(append_response) => {
                        let mut response_payload = Vec::new();
                        if let Err(e) =
                            ciborium::ser::into_writer(&append_response, &mut response_payload)
                        {
                            error!("Failed to serialize append entries response: {}", e);
                            return Ok(());
                        }

                        // Send response back through network
                        let response_msg = crate::network::messages::Message::Raft(
                            RaftMessage::AppendEntriesResponse(response_payload),
                        );

                        if let Some(corr_id) = correlation_id {
                            self.network_manager()
                                .send_message_with_correlation(sender_id, response_msg, corr_id)
                                .await?;
                        } else {
                            self.network_manager()
                                .send_message(sender_id, response_msg)
                                .await?;
                        }
                    }
                    Err(e) => {
                        error!("Append entries request failed: {}", e);
                    }
                }
            }
            RaftMessage::InstallSnapshot(snapshot_data) => {
                let snapshot_request: openraft::raft::InstallSnapshotRequest<GlobalTypeConfig> =
                    match ciborium::de::from_reader(snapshot_data.as_slice()) {
                        Ok(req) => req,
                        Err(e) => {
                            error!("Failed to deserialize InstallSnapshotRequest: {}", e);
                            return Ok(());
                        }
                    };

                match self.raft.install_snapshot(snapshot_request).await {
                    Ok(snapshot_response) => {
                        let mut response_payload = Vec::new();
                        if let Err(e) =
                            ciborium::ser::into_writer(&snapshot_response, &mut response_payload)
                        {
                            error!("Failed to serialize install snapshot response: {}", e);
                            return Ok(());
                        }

                        // Send response back through network
                        let response_msg = crate::network::messages::Message::Raft(
                            RaftMessage::InstallSnapshotResponse(response_payload),
                        );

                        if let Some(corr_id) = correlation_id {
                            self.network_manager()
                                .send_message_with_correlation(sender_id, response_msg, corr_id)
                                .await?;
                        } else {
                            self.network_manager()
                                .send_message(sender_id, response_msg)
                                .await?;
                        }
                    }
                    Err(e) => {
                        error!("Install snapshot request failed: {}", e);
                    }
                }
            }
            // Handle Raft response messages by forwarding to RaftAdapter
            RaftMessage::VoteResponse(payload) => {
                self.process_raft_response::<openraft::raft::VoteResponse<GlobalTypeConfig>>(
                    &payload,
                    correlation_id,
                    &sender_id,
                    "VoteResponse",
                    |resp| RaftAdapterResponse::Vote(Box::new(resp)),
                );
            }
            RaftMessage::AppendEntriesResponse(payload) => {
                self.process_raft_response::<openraft::raft::AppendEntriesResponse<GlobalTypeConfig>>(
                    &payload,
                    correlation_id,
                    &sender_id,
                    "AppendEntriesResponse",
                    RaftAdapterResponse::AppendEntries,
                );
            }
            RaftMessage::InstallSnapshotResponse(payload) => {
                self.process_raft_response::<openraft::raft::InstallSnapshotResponse<GlobalTypeConfig>>(
                    &payload,
                    correlation_id,
                    &sender_id,
                    "InstallSnapshotResponse",
                    RaftAdapterResponse::InstallSnapshot,
                );
            }
        }

        Ok(())
    }

    /// Helper to process Raft responses
    fn process_raft_response<T>(
        &self,
        payload: &[u8],
        correlation_id: Option<Uuid>,
        sender_id: &NodeId,
        response_type: &str,
        wrapper: impl FnOnce(T) -> RaftAdapterResponse<GlobalTypeConfig>,
    ) where
        T: for<'de> serde::Deserialize<'de>,
    {
        if let Some(corr_id) = correlation_id {
            match ciborium::de::from_reader::<T, _>(payload) {
                Ok(response) => {
                    let wrapped_response = Box::new(wrapper(response));
                    if let Err(e) = self.raft_adapter_response_tx.send((
                        sender_id.clone(),
                        corr_id,
                        wrapped_response,
                    )) {
                        error!("Failed to send {} to RaftAdapter: {}", response_type, e);
                    }
                }
                Err(e) => {
                    error!("Failed to deserialize {}: {}", response_type, e);
                }
            }
        } else {
            warn!(
                "Received {} without correlation ID from {}",
                response_type, sender_id
            );
        }
    }

    /// Handle application-level messages
    async fn handle_application_message(
        &self,
        node_id: NodeId,
        message: crate::network::messages::ApplicationMessage,
        correlation_id: Option<uuid::Uuid>,
    ) -> ConsensusResult<()> {
        use crate::network::messages::ApplicationMessage;

        match message {
            ApplicationMessage::ClusterDiscovery(req) => {
                self.handle_cluster_discovery_request(node_id, req, correlation_id)
                    .await
            }
            ApplicationMessage::ClusterDiscoveryResponse(resp) => {
                self.handle_cluster_discovery_response(node_id, resp, correlation_id)
                    .await
            }
            ApplicationMessage::ClusterJoinRequest(req) => {
                self.handle_cluster_join_request(node_id, req, correlation_id)
                    .await
            }
            ApplicationMessage::ClusterJoinResponse(resp) => {
                self.handle_cluster_join_response(node_id, resp, correlation_id)
                    .await
            }
            ApplicationMessage::ConsensusRequest(req) => {
                self.handle_consensus_request(node_id, req, correlation_id)
                    .await
            }
            ApplicationMessage::ConsensusResponse(resp) => {
                self.handle_consensus_response(node_id, resp, correlation_id)
                    .await
            }
            ApplicationMessage::PubSub(_msg) => {
                // PubSub messages are handled by the PubSub manager
                Ok(())
            }
        }
    }

    /// Handle cluster discovery request
    async fn handle_cluster_discovery_request(
        &self,
        node_id: NodeId,
        _request: crate::network::messages::ClusterDiscoveryRequest,
        correlation_id: Option<uuid::Uuid>,
    ) -> ConsensusResult<()> {
        use crate::network::messages::{ApplicationMessage, ClusterDiscoveryResponse, Message};

        info!("Handling cluster discovery request from {}", node_id);

        // Create response with our cluster state
        let response = ClusterDiscoveryResponse {
            responder_id: self.node_id.clone(),
            has_active_cluster: self.has_active_cluster().await,
            current_term: self.current_term(),
            current_leader: self
                .current_leader()
                .await
                .and_then(|s| NodeId::from_hex(&s).ok()),
            cluster_size: self.cluster_size(),
        };

        // Send response back through network manager
        let message = Message::Application(Box::new(ApplicationMessage::ClusterDiscoveryResponse(
            response.clone(),
        )));

        info!(
            "Sending discovery response back to {}: has_active_cluster={}, current_term={:?}, cluster_size={:?}",
            node_id, response.has_active_cluster, response.current_term, response.cluster_size
        );

        // Send response with correlation ID if provided
        if let Some(corr_id) = correlation_id {
            self.network_manager()
                .send_message_with_correlation(node_id, message, corr_id)
                .await
                .map_err(|e| {
                    Error::InvalidMessage(format!("Failed to send discovery response: {}", e))
                })?;
        } else {
            self.network_manager()
                .send_message(node_id, message)
                .await
                .map_err(|e| {
                    Error::InvalidMessage(format!("Failed to send discovery response: {}", e))
                })?;
        }

        Ok(())
    }

    /// Handle cluster discovery response
    async fn handle_cluster_discovery_response(
        &self,
        _node_id: NodeId,
        response: crate::network::messages::ClusterDiscoveryResponse,
        correlation_id: Option<uuid::Uuid>,
    ) -> ConsensusResult<()> {
        if let Some(corr_id) = correlation_id {
            let mut pending = self.pending_discovery_responses.write().await;
            if let Some(tx) = pending.remove(&corr_id) {
                let _ = tx.send(response);
            } else {
                debug!(
                    "No pending discovery request for correlation ID {}",
                    corr_id
                );
            }
        } else {
            debug!(
                "Received discovery response without correlation ID from {}",
                response.responder_id
            );
        }
        Ok(())
    }

    /// Handle cluster join request
    async fn handle_cluster_join_request(
        &self,
        node_id: NodeId,
        request: crate::network::messages::ClusterJoinRequest,
        correlation_id: Option<uuid::Uuid>,
    ) -> ConsensusResult<()> {
        use crate::network::messages::{ApplicationMessage, ClusterJoinResponse, Message};

        let response = if !self.is_leader().await {
            // Not the leader - return error with current leader info
            ClusterJoinResponse {
                error_message: Some("Not the leader".to_string()),
                cluster_size: None,
                current_term: None,
                responder_id: self.node_id.clone(),
                success: false,
                current_leader: self
                    .current_leader()
                    .await
                    .and_then(|s| NodeId::from_hex(&s).ok()),
            }
        } else {
            // We are the leader - try to add the node
            let node = crate::node::Node::from(request.requester_node);
            match self
                .raft
                .add_learner(request.requester_id.clone(), node, true)
                .await
            {
                Ok(_) => {
                    info!("Added node {:?} as learner", request.requester_id);

                    // Wait a bit for the learner to sync
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

                    // Get current membership and add the new node
                    let metrics = self.raft.metrics();
                    let current_membership = metrics.borrow().membership_config.clone();
                    let mut new_voters: Vec<NodeId> =
                        current_membership.membership().voter_ids().collect();

                    // Add the new node to voters
                    if !new_voters.contains(&request.requester_id) {
                        new_voters.push(request.requester_id.clone());
                    }

                    // Promote learner to voter
                    match self.raft.change_membership(new_voters, false).await {
                        Ok(_) => {
                            info!("Promoted node {:?} to voter", request.requester_id);
                        }
                        Err(e) => {
                            warn!("Failed to promote learner to voter: {}", e);
                        }
                    }

                    ClusterJoinResponse {
                        error_message: None,
                        cluster_size: self.cluster_size(),
                        current_term: self.current_term(),
                        responder_id: self.node_id.clone(),
                        success: true,
                        current_leader: Some(self.node_id.clone()),
                    }
                }
                Err(e) => ClusterJoinResponse {
                    error_message: Some(format!("Failed to add node: {}", e)),
                    cluster_size: None,
                    current_term: None,
                    responder_id: self.node_id.clone(),
                    success: false,
                    current_leader: self
                        .current_leader()
                        .await
                        .and_then(|s| NodeId::from_hex(&s).ok()),
                },
            }
        };

        // Send response back through network manager
        let message =
            Message::Application(Box::new(ApplicationMessage::ClusterJoinResponse(response)));

        if let Some(corr_id) = correlation_id {
            self.network_manager()
                .send_message_with_correlation(node_id, message, corr_id)
                .await
                .map_err(|e| {
                    Error::InvalidMessage(format!("Failed to send join response: {}", e))
                })?;
        } else {
            self.network_manager()
                .send_message(node_id, message)
                .await
                .map_err(|e| {
                    Error::InvalidMessage(format!("Failed to send join response: {}", e))
                })?;
        }

        Ok(())
    }

    /// Handle cluster join response
    async fn handle_cluster_join_response(
        &self,
        _node_id: NodeId,
        response: crate::network::messages::ClusterJoinResponse,
        correlation_id: Option<uuid::Uuid>,
    ) -> ConsensusResult<()> {
        if let Some(corr_id) = correlation_id {
            let mut pending = self.pending_join_responses.write().await;
            if let Some(tx) = pending.remove(&corr_id) {
                let _ = tx.send(response);
            } else {
                debug!("No pending join request for correlation ID {}", corr_id);
            }
        } else {
            debug!(
                "Received join response without correlation ID from {}",
                response.responder_id
            );
        }
        Ok(())
    }

    /// Handle consensus request
    async fn handle_consensus_request(
        &self,
        node_id: NodeId,
        request: super::GlobalRequest,
        correlation_id: Option<uuid::Uuid>,
    ) -> ConsensusResult<()> {
        use crate::network::messages::{ApplicationMessage, Message};

        // Submit through consensus
        let response = self.submit_operation(request.operation).await?;

        // Send response back through network manager
        let message =
            Message::Application(Box::new(ApplicationMessage::ConsensusResponse(response)));

        if let Some(corr_id) = correlation_id {
            self.network_manager()
                .send_message_with_correlation(node_id, message, corr_id)
                .await
                .map_err(|e| {
                    Error::InvalidMessage(format!("Failed to send consensus response: {}", e))
                })?;
        } else {
            self.network_manager()
                .send_message(node_id, message)
                .await
                .map_err(|e| {
                    Error::InvalidMessage(format!("Failed to send consensus response: {}", e))
                })?;
        }

        Ok(())
    }

    /// Handle consensus response
    async fn handle_consensus_response(
        &self,
        _node_id: NodeId,
        response: super::GlobalResponse,
        correlation_id: Option<uuid::Uuid>,
    ) -> ConsensusResult<()> {
        // If we have a correlation ID, this is a response to a request we made
        if let Some(request_id) = correlation_id {
            // Extract the numeric ID from the UUID (temporary solution)
            let id = request_id.as_u128() as u64;
            self.handle_state_machine_response(id, response).await;
        }

        Ok(())
    }

    /// Start topology management
    pub async fn start_topology(&self) -> ConsensusResult<()> {
        // Refresh topology from governance to get all peers
        if let Some(topology) = &self.topology {
            topology.refresh_topology().await?;
        }
        Ok(())
    }

    /// Discover existing clusters
    pub async fn discover_existing_clusters(
        &self,
    ) -> ConsensusResult<Vec<crate::network::messages::ClusterDiscoveryResponse>> {
        use crate::network::messages::{ApplicationMessage, ClusterDiscoveryRequest, Message};
        use tokio::time::{Duration, timeout};

        // Get all peers from topology
        let peers = self.topology().get_all_peers().await;
        info!(
            "Discovering existing clusters, found {} peers in topology",
            peers.len()
        );
        for peer in &peers {
            info!("  - Peer: {} at {}", peer.node_id(), peer.origin());
        }

        if peers.is_empty() {
            return Ok(Vec::new());
        }

        // Create channels for collecting responses
        let mut response_receivers = Vec::new();
        let mut pending_map = self.pending_discovery_responses.write().await;

        // Send discovery request to each peer
        for peer in &peers {
            let correlation_id = Uuid::new_v4();
            let (tx, rx) = oneshot::channel();

            // Track pending response
            pending_map.insert(correlation_id, tx);
            response_receivers.push(rx);

            // Create and send discovery request
            let discovery_request = ClusterDiscoveryRequest {
                requester_id: self.node_id.clone(),
            };

            let message = Message::Application(Box::new(ApplicationMessage::ClusterDiscovery(
                discovery_request,
            )));

            // Send with correlation ID for response tracking
            info!(
                "Sending discovery request to {} with correlation_id={}",
                peer.node_id(),
                correlation_id
            );
            if let Err(e) = self
                .network_manager()
                .send_message_with_correlation(peer.node_id().clone(), message, correlation_id)
                .await
            {
                warn!(
                    "Failed to send discovery request to {}: {}",
                    peer.node_id(),
                    e
                );
            } else {
                info!("Successfully sent discovery request to {}", peer.node_id());
            }
        }
        drop(pending_map);

        // Collect responses with timeout
        let mut responses = Vec::new();
        let discovery_timeout = Duration::from_secs(5);

        for rx in response_receivers.into_iter() {
            match timeout(discovery_timeout, rx).await {
                Ok(Ok(response)) => {
                    responses.push(response);
                }
                Ok(Err(_)) => {
                    debug!("Discovery response channel closed");
                }
                Err(_) => {
                    debug!("Discovery response timed out");
                }
            }
        }

        Ok(responses)
    }

    /// Get cluster state
    pub async fn cluster_state(&self) -> crate::network::ClusterState {
        self.cluster_state.read().await.clone()
    }

    /// Wait for leader election
    pub async fn wait_for_leader(
        &self,
        timeout: Option<std::time::Duration>,
    ) -> ConsensusResult<()> {
        use tokio::time::sleep;

        let timeout_duration = timeout.unwrap_or(std::time::Duration::from_secs(30));
        let start = std::time::Instant::now();

        while start.elapsed() < timeout_duration {
            if self.is_leader().await || self.current_leader().await.is_some() {
                return Ok(());
            }
            sleep(std::time::Duration::from_millis(100)).await;
        }

        Err(Error::Timeout {
            seconds: timeout_duration.as_secs(),
        })
    }

    /// Get topology manager
    pub fn topology(&self) -> Arc<crate::topology::TopologyManager<G>> {
        self.topology
            .as_ref()
            .expect("Topology manager not set. Call set_topology() first")
            .clone()
    }

    /// Set topology manager
    pub fn set_topology(&mut self, topology: Arc<crate::topology::TopologyManager<G>>) {
        self.topology = Some(topology);
    }

    /// Get network manager
    fn network_manager(&self) -> Arc<crate::network::network_manager::NetworkManager<G, A>> {
        self.network_manager.clone()
    }

    /// Set cluster state
    pub async fn set_cluster_state(&self, state: crate::network::ClusterState) {
        *self.cluster_state.write().await = state;
    }

    /// Initialize single node cluster
    pub async fn initialize_single_node_cluster(
        &self,
        node_id: NodeId,
        reason: crate::network::InitiatorReason,
    ) -> ConsensusResult<()> {
        if node_id != self.node_id {
            return Err(Error::InvalidOperation(
                "Cannot initialize cluster for different node".to_string(),
            ));
        }

        // Initialize the Raft cluster
        self.initialize_cluster().await?;

        // Set cluster state
        self.set_cluster_state(crate::network::ClusterState::Initiator {
            initiated_at: std::time::Instant::now(),
            reason,
        })
        .await;

        // Wait a bit for leader election
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Create a default consensus group after becoming leader
        if self.is_leader().await {
            info!("Creating default consensus group 1");

            // Submit through Raft to ensure proper consensus
            match self
                .create_group(
                    crate::allocation::ConsensusGroupId::new(1),
                    vec![self.node_id.clone()],
                )
                .await
            {
                Ok(_) => {
                    info!("Successfully created default consensus group 1");

                    // Wait for the group to be applied to state machine
                    let start = std::time::Instant::now();
                    while start.elapsed() < std::time::Duration::from_secs(1) {
                        if self
                            .get_group(crate::allocation::ConsensusGroupId::new(1))
                            .await
                            .is_some()
                        {
                            info!("Default consensus group 1 is ready");
                            break;
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    }
                }
                Err(e) => warn!("Failed to create default consensus group: {}", e),
            }
        }

        Ok(())
    }

    /// Initialize multi-node cluster
    pub async fn initialize_multi_node_cluster(
        &self,
        node_id: NodeId,
        reason: crate::network::InitiatorReason,
        peers: Vec<crate::Node>,
    ) -> ConsensusResult<()> {
        info!(
            "Initializing multi-node cluster as leader for node {} with {} peers",
            node_id,
            peers.len()
        );

        // Set cluster state to initiator
        self.set_cluster_state(crate::network::ClusterState::Initiator {
            initiated_at: std::time::Instant::now(),
            reason,
        })
        .await;

        // For now, start as single node - peers will join later via Raft
        // Get our own GovernanceNode from topology
        let own_node = self
            .topology()
            .get_own_node()
            .await
            .map_err(|e| Error::InvalidMessage(format!("Failed to get own node: {e}")))?;

        let membership = std::collections::BTreeMap::from([(node_id.clone(), own_node)]);

        self.raft
            .initialize(membership)
            .await
            .map_err(|e| Error::Raft(format!("Failed to initialize leader Raft: {e}")))?;

        info!("Successfully initialized multi-node cluster as leader");

        // Wait a bit for leader election
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Create a default consensus group after becoming leader
        if self.is_leader().await {
            info!("Creating default consensus group 1 for multi-node cluster");

            // Submit through Raft to ensure proper consensus
            match self
                .create_group(
                    crate::allocation::ConsensusGroupId::new(1),
                    vec![self.node_id.clone()],
                )
                .await
            {
                Ok(_) => {
                    info!("Successfully created default consensus group 1");

                    // Wait for the group to be applied to state machine
                    let start = std::time::Instant::now();
                    while start.elapsed() < std::time::Duration::from_secs(1) {
                        if self
                            .get_group(crate::allocation::ConsensusGroupId::new(1))
                            .await
                            .is_some()
                        {
                            info!("Default consensus group 1 is ready");
                            break;
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    }
                }
                Err(e) => warn!("Failed to create default consensus group: {}", e),
            }
        }

        Ok(())
    }

    /// Join existing cluster via Raft with retry logic
    pub async fn join_existing_cluster_via_raft(
        &self,
        node_id: NodeId,
        existing_cluster: &crate::network::messages::ClusterDiscoveryResponse,
    ) -> ConsensusResult<()> {
        use crate::network::messages::{ApplicationMessage, ClusterJoinRequest, Message};
        use tokio::time::{sleep, timeout};

        if node_id != self.node_id {
            return Err(Error::InvalidOperation(
                "Cannot join cluster for different node".to_string(),
            ));
        }

        // Ensure we have a leader to send the join request to
        let mut current_leader = existing_cluster
            .current_leader
            .as_ref()
            .ok_or_else(|| {
                Error::InvalidMessage(
                    "Cannot join cluster: no leader reported in discovery response".to_string(),
                )
            })?
            .clone();

        info!(
            "Joining existing cluster via Raft. Initial leader: {}, Term: {:?}",
            current_leader, existing_cluster.current_term
        );

        // Set cluster state to waiting to join
        self.set_cluster_state(crate::network::ClusterState::WaitingToJoin {
            requested_at: std::time::Instant::now(),
            leader_id: current_leader.clone(),
        })
        .await;

        // Get our own node information for the join request
        let requester_node = self
            .topology()
            .get_own_node()
            .await
            .map_err(|e| Error::InvalidMessage(format!("Failed to get own node: {e}")))?;

        // Create join request
        let join_request = ClusterJoinRequest {
            requester_id: self.node_id.clone(),
            requester_node: requester_node.into(),
        };

        let retry_config = &self.cluster_join_retry_config;
        let mut last_error = None;
        let mut retry_delay = retry_config.initial_delay;

        for attempt in 1..=retry_config.max_attempts {
            info!(
                "Cluster join attempt {} of {}, targeting leader: {}",
                attempt, retry_config.max_attempts, current_leader
            );

            // Generate correlation ID and create response channel
            let correlation_id = Uuid::new_v4();
            let (tx, rx) = oneshot::channel();

            // Add pending request
            {
                let mut pending = self.pending_join_responses.write().await;
                pending.insert(correlation_id, tx);
            }

            // Send join request with correlation ID
            let message = Message::Application(Box::new(ApplicationMessage::ClusterJoinRequest(
                join_request.clone(),
            )));

            match self
                .network_manager()
                .send_message_with_correlation(current_leader.clone(), message, correlation_id)
                .await
            {
                Ok(()) => {
                    info!(
                        "Sent join request to {} with correlation ID {} (attempt {})",
                        current_leader, correlation_id, attempt
                    );
                }
                Err(e) => {
                    warn!("Failed to send join request on attempt {}: {}", attempt, e);
                    last_error = Some(Error::InvalidMessage(format!(
                        "Failed to send join request: {}",
                        e
                    )));

                    // Clean up pending request
                    self.pending_join_responses
                        .write()
                        .await
                        .remove(&correlation_id);

                    // Wait before retrying, unless this is the last attempt
                    if attempt < retry_config.max_attempts {
                        sleep(retry_delay).await;
                        retry_delay = std::cmp::min(retry_delay * 2, retry_config.max_delay);
                    }
                    continue;
                }
            }

            // Wait for join response with timeout
            let join_response = match timeout(retry_config.request_timeout, rx).await {
                Ok(Ok(response)) => response,
                Ok(Err(_)) => {
                    warn!("Join request channel was cancelled on attempt {}", attempt);
                    last_error = Some(Error::InvalidMessage("Join request cancelled".to_string()));

                    // Wait before retrying, unless this is the last attempt
                    if attempt < retry_config.max_attempts {
                        sleep(retry_delay).await;
                        retry_delay = std::cmp::min(retry_delay * 2, retry_config.max_delay);
                    }
                    continue;
                }
                Err(_) => {
                    warn!("Join request timeout on attempt {}", attempt);
                    last_error = Some(Error::InvalidMessage("Join request timeout".to_string()));

                    // Clean up pending request
                    self.pending_join_responses
                        .write()
                        .await
                        .remove(&correlation_id);

                    // Wait before retrying, unless this is the last attempt
                    if attempt < retry_config.max_attempts {
                        sleep(retry_delay).await;
                        retry_delay = std::cmp::min(retry_delay * 2, retry_config.max_delay);
                    }
                    continue;
                }
            };

            if join_response.success {
                info!(
                    "Successfully joined cluster! Leader: {}, Size: {:?}, Term: {:?}",
                    join_response.responder_id,
                    join_response.cluster_size,
                    join_response.current_term
                );

                self.set_cluster_state(crate::network::ClusterState::Joined {
                    joined_at: std::time::Instant::now(),
                    cluster_size: join_response.cluster_size.unwrap_or(1),
                })
                .await;

                return Ok(());
            } else {
                let error_msg = join_response
                    .error_message
                    .unwrap_or_else(|| "Unknown join error".to_string());

                warn!("Join request failed on attempt {}: {}", attempt, error_msg);

                // Check if we have a new leader to try
                if let Some(new_leader) = join_response.current_leader {
                    if new_leader != current_leader {
                        info!(
                            "Received new leader {} from failed join response, will retry with new leader",
                            new_leader
                        );
                        current_leader = new_leader;

                        // Update cluster state with new leader
                        self.set_cluster_state(crate::network::ClusterState::WaitingToJoin {
                            requested_at: std::time::Instant::now(),
                            leader_id: current_leader.clone(),
                        })
                        .await;
                    }
                }

                last_error = Some(Error::InvalidMessage(format!(
                    "Join request failed: {}",
                    error_msg
                )));

                // Wait before retrying, unless this is the last attempt
                if attempt < retry_config.max_attempts {
                    sleep(retry_delay).await;
                    retry_delay = std::cmp::min(retry_delay * 2, retry_config.max_delay);
                }
            }
        }

        // All attempts failed
        let final_error = last_error.unwrap_or_else(|| {
            Error::InvalidMessage("Unknown error after all retry attempts".to_string())
        });

        warn!(
            "Failed to join cluster after {} attempts. Final error: {}",
            retry_config.max_attempts, final_error
        );

        self.set_cluster_state(crate::network::ClusterState::Failed {
            failed_at: std::time::Instant::now(),
            error: format!(
                "Failed after {} attempts: {}",
                retry_config.max_attempts, final_error
            ),
        })
        .await;

        Err(final_error)
    }

    /// Shutdown all components
    pub async fn shutdown_all(&self) -> ConsensusResult<()> {
        // TODO: Implement comprehensive shutdown
        Ok(())
    }

    /// Get current state
    pub async fn get_current_state(&self) -> ConsensusResult<Arc<GlobalState>> {
        Ok(self.state.clone())
    }

    /// Process RaftAdapter requests and send them through the network
    async fn raft_adapter_processor_loop(
        mut request_rx: tokio::sync::mpsc::UnboundedReceiver<(
            NodeId,
            Uuid,
            Box<RaftAdapterRequest<GlobalTypeConfig>>,
        )>,
        network_manager: Arc<crate::network::network_manager::NetworkManager<G, A>>,
    ) {
        debug!("RaftAdapter processor task started");

        while let Some((target_node_id, correlation_id, request)) = request_rx.recv().await {
            // Convert RaftAdapter request to network message
            let message = match *request {
                RaftAdapterRequest::Vote(vote_req) => {
                    let mut vote_data = Vec::new();
                    if let Err(e) = ciborium::ser::into_writer(&vote_req, &mut vote_data) {
                        error!("Failed to serialize Vote request: {}", e);
                        continue;
                    }
                    crate::network::messages::Message::Raft(
                        crate::network::messages::RaftMessage::Vote(vote_data),
                    )
                }
                RaftAdapterRequest::AppendEntries(append_req) => {
                    let mut append_data = Vec::new();
                    if let Err(e) = ciborium::ser::into_writer(&append_req, &mut append_data) {
                        error!("Failed to serialize AppendEntries request: {}", e);
                        continue;
                    }
                    crate::network::messages::Message::Raft(
                        crate::network::messages::RaftMessage::AppendEntries(append_data),
                    )
                }
                RaftAdapterRequest::InstallSnapshot(snapshot_req) => {
                    let mut snapshot_data = Vec::new();
                    if let Err(e) = ciborium::ser::into_writer(&snapshot_req, &mut snapshot_data) {
                        error!("Failed to serialize InstallSnapshot request: {}", e);
                        continue;
                    }
                    crate::network::messages::Message::Raft(
                        crate::network::messages::RaftMessage::InstallSnapshot(snapshot_data),
                    )
                }
            };

            // Send the message with correlation ID
            if let Err(e) = network_manager
                .send_message_with_correlation(target_node_id.clone(), message, correlation_id)
                .await
            {
                error!("Failed to send Raft message to {}: {}", target_node_id, e);
            }
        }

        debug!("RaftAdapter processor task exited");
    }
}
