//! Global consensus manager
//!
//! This module handles all global consensus concerns including Raft instance management,
//! cluster discovery, topology management, and cluster state coordination.

use super::{GlobalOperation, GlobalRequest, GlobalResponse, GlobalTypeConfig};
use crate::NodeId;
use crate::error::Error;
use crate::group_allocator::{
    GroupAllocator, GroupAllocatorConfig, MigrationConfig, MigrationCoordinator, MigrationEvent,
};
use crate::network::adaptor::RaftAdapterResponse;
use crate::network::messages::{
    ApplicationMessage, ClusterDiscoveryRequest, ClusterDiscoveryResponse, ClusterJoinRequest,
    ClusterJoinResponse, Message,
};
use crate::network::{ClusterState, InitiatorReason};

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::future;
use openraft::storage::{RaftLogStorage, RaftStateMachine};
use proven_governance::Governance;
use tokio::sync::{RwLock, oneshot};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

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

/// Global consensus manager that handles Raft instance, cluster discovery, and topology
pub struct GlobalManager<G, A>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug + Clone,
    A: proven_attestation::Attestor + Send + Sync + 'static + std::fmt::Debug + Clone,
{
    /// Our local node ID
    local_node_id: NodeId,

    /// Raft instance - owned and managed by GlobalManager
    raft_instance: Arc<openraft::Raft<GlobalTypeConfig>>,

    /// Topology manager for node discovery and management
    topology: Arc<crate::topology::TopologyManager<G>>,

    /// Generic pending requests by correlation ID - can handle any response type
    pending_requests: Arc<RwLock<HashMap<Uuid, Box<dyn std::any::Any + Send + Sync>>>>,

    /// Current cluster state
    cluster_state: Arc<RwLock<ClusterState>>,

    /// Cluster join retry configuration
    cluster_join_retry_config: crate::config::ClusterJoinRetryConfig,

    /// Network manager for sending messages
    network_manager: Arc<crate::network::network_manager::NetworkManager<G, A>>,

    /// Response channel for correlation
    response_tx: tokio::sync::mpsc::UnboundedSender<(Uuid, Box<dyn std::any::Any + Send + Sync>)>,

    /// RaftAdapter response channel
    raft_adapter_response_tx: tokio::sync::mpsc::UnboundedSender<(
        NodeId,
        Uuid,
        Box<crate::network::adaptor::RaftAdapterResponse<GlobalTypeConfig>>,
    )>,

    /// Global state reference for querying state
    global_state: Arc<super::GlobalState>,

    /// Group allocator for managing consensus group assignments
    group_allocator: Arc<GroupAllocator<G>>,

    /// Migration coordinator for managing group transitions
    migration_coordinator: Arc<RwLock<MigrationCoordinator>>,
}

impl<G, A> GlobalManager<G, A>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug + Clone,
    A: proven_attestation::Attestor + Send + Sync + 'static + std::fmt::Debug + Clone,
{
    /// Create a new GlobalManager
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        local_node_id: NodeId,
        storage: impl RaftLogStorage<GlobalTypeConfig>
        + RaftStateMachine<GlobalTypeConfig>
        + Clone
        + Send
        + Sync
        + 'static,
        raft_config: Arc<openraft::Config>,
        cluster_join_retry_config: crate::config::ClusterJoinRetryConfig,
        network_factory: crate::network::adaptor::NetworkFactory<GlobalTypeConfig>,
        network_manager: Arc<crate::network::network_manager::NetworkManager<G, A>>,
        topology: Arc<crate::topology::TopologyManager<G>>,
        global_state: Arc<super::GlobalState>,
        raft_adapter_request_rx: tokio::sync::mpsc::UnboundedReceiver<(
            NodeId,
            Uuid,
            Box<crate::network::adaptor::RaftAdapterRequest<GlobalTypeConfig>>,
        )>,
        raft_adapter_response_tx: tokio::sync::mpsc::UnboundedSender<(
            NodeId,
            Uuid,
            Box<crate::network::adaptor::RaftAdapterResponse<GlobalTypeConfig>>,
        )>,
    ) -> Result<Self, openraft::error::RaftError<GlobalTypeConfig>> {
        // Use the shared topology manager

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

        // Create shared state
        let pending_requests = Arc::new(RwLock::new(HashMap::new()));
        let cluster_state = Arc::new(RwLock::new(ClusterState::TransportReady));

        // Get response channel from network manager
        let response_tx = network_manager.get_response_sender();

        // Start the RaftAdapter processor task
        let network_manager_clone = network_manager.clone();
        tokio::spawn(Self::raft_adapter_processor_loop(
            raft_adapter_request_rx,
            network_manager_clone,
        ));

        // Start the Raft response processor task
        let raft_instance_clone = raft_instance.clone();
        let network_manager_clone2 = network_manager.clone();
        tokio::spawn(Self::raft_response_processor_loop(
            raft_adapter_response_tx.clone(),
            raft_instance_clone,
            network_manager_clone2,
        ));

        // Start the response correlation task
        if let Some(response_rx) = network_manager.take_response_receiver().await {
            let pending_requests_clone = pending_requests.clone();
            tokio::spawn(Self::response_correlation_loop(
                response_rx,
                pending_requests_clone,
            ));
        } else {
            warn!("Failed to get response receiver from NetworkManager");
        }

        // Create the group allocator
        let group_allocator_config = GroupAllocatorConfig::default();
        let group_allocator = Arc::new(GroupAllocator::new(
            group_allocator_config,
            topology.clone(),
        ));

        // Create the migration coordinator
        let migration_config = MigrationConfig::default();
        let migration_coordinator =
            Arc::new(RwLock::new(MigrationCoordinator::new(migration_config)));

        Ok(Self {
            local_node_id,
            raft_instance,
            topology,
            pending_requests,
            cluster_state,
            cluster_join_retry_config,
            network_manager,
            response_tx,
            raft_adapter_response_tx,
            global_state,
            group_allocator,
            migration_coordinator,
        })
    }

    // Network callback is no longer needed - we use network_manager directly

    /// Get the Raft instance
    pub fn raft_instance(&self) -> Arc<openraft::Raft<GlobalTypeConfig>> {
        self.raft_instance.clone()
    }

    /// Get the topology manager
    pub fn topology(&self) -> Arc<crate::topology::TopologyManager<G>> {
        self.topology.clone()
    }

    /// Query consensus groups that a node belongs to
    pub async fn query_node_groups(
        &self,
        node_id: &NodeId,
    ) -> Vec<super::state_machine::ConsensusGroupInfo> {
        self.global_state.get_node_groups(node_id).await
    }

    /// Get all consensus groups
    pub async fn get_all_groups(&self) -> Vec<super::state_machine::ConsensusGroupInfo> {
        self.global_state.get_all_groups().await
    }

    /// Check if this node is the current leader
    pub fn is_leader(&self) -> bool {
        let metrics_ref = self.raft_instance.metrics();
        let metrics = metrics_ref.borrow();
        metrics.current_leader.as_ref() == Some(&self.local_node_id)
    }

    /// Submit a request to the Raft consensus engine
    pub async fn submit_request(&self, request: GlobalRequest) -> Result<GlobalResponse, Error> {
        match self.raft_instance.client_write(request.clone()).await {
            Ok(response) => Ok(response.data),
            Err(e) => {
                error!("Failed to submit request to Raft: {}", e);
                Err(Error::Raft(format!("Raft client write failed: {}", e)))
            }
        }
    }

    /// Propose a request and get the sequence number
    pub async fn propose_request(&self, request: &GlobalRequest) -> Result<u64, Error> {
        let response = self.raft_instance.client_write(request.clone()).await;
        match response {
            Ok(client_write_response) => {
                let log_id = client_write_response.log_id;
                Ok(log_id.index)
            }
            Err(e) => {
                error!("Failed to propose request: {}", e);
                Err(Error::Raft(format!("Raft proposal failed: {}", e)))
            }
        }
    }

    /// Get Raft metrics
    pub fn metrics(&self) -> Option<openraft::RaftMetrics<GlobalTypeConfig>> {
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
    pub async fn shutdown_raft(&self) -> Result<(), Error> {
        if let Err(e) = self.raft_instance.shutdown().await {
            error!("Error shutting down Raft: {}", e);
            Err(Error::Raft(format!("Raft shutdown failed: {}", e)))
        } else {
            Ok(())
        }
    }

    /// Get the group allocator reference
    pub fn group_allocator(&self) -> Arc<GroupAllocator<G>> {
        self.group_allocator.clone()
    }

    /// Refresh the GroupAllocator's topology (no-op since TopologyManager handles this)
    pub async fn refresh_group_allocator_topology(&self) -> Result<(), Error> {
        // TopologyManager handles topology updates automatically
        Ok(())
    }

    /// Start migration coordination
    pub async fn start_migration_coordination(&self) -> Result<(), Error> {
        // Generate rebalancing plan
        let plan = self
            .group_allocator
            .generate_rebalancing_plan()
            .await
            .map_err(|e| {
                Error::InvalidOperation(format!("Failed to generate rebalancing plan: {:?}", e))
            })?;

        if let Some(plan) = plan {
            let mut coordinator = self.migration_coordinator.write().await;

            // Start coordination for each migration
            for migration in plan.migrations {
                coordinator
                    .start_coordination(migration.clone())
                    .await
                    .map_err(|e| {
                        Error::InvalidOperation(format!("Failed to start coordination: {:?}", e))
                    })?;

                // Submit operation to add node to target group
                let operation = GlobalOperation::AssignNodeToGroup {
                    node_id: migration.node_id.clone(),
                    group_id: migration.to_group,
                };

                let request = GlobalRequest { operation };
                self.raft_instance
                    .client_write(request)
                    .await
                    .map_err(|e| {
                        Error::Raft(format!("Failed to submit assign operation: {}", e))
                    })?;
            }
        }

        Ok(())
    }

    /// Handle migration event
    pub async fn handle_migration_event(&self, event: MigrationEvent) -> Result<(), Error> {
        match event {
            MigrationEvent::NodeSyncedWithTarget { node_id, group_id } => {
                info!("Node {:?} synced with target group {:?}", node_id, group_id);

                // Update coordinator
                let mut coordinator = self.migration_coordinator.write().await;
                coordinator
                    .mark_ready_to_leave(&node_id)
                    .await
                    .map_err(|e| {
                        Error::InvalidOperation(format!("Failed to mark ready to leave: {:?}", e))
                    })?;

                // Generate operation to remove from source group
                let operations = coordinator.generate_migration_operations(&node_id);
                for op in operations {
                    let request = GlobalRequest { operation: op };
                    self.raft_instance
                        .client_write(request)
                        .await
                        .map_err(|e| {
                            Error::Raft(format!("Failed to submit remove operation: {}", e))
                        })?;
                }
            }
            MigrationEvent::NodeRemovedFromSource { node_id, .. } => {
                // Complete the migration
                let mut coordinator = self.migration_coordinator.write().await;
                coordinator
                    .complete_migration(&node_id)
                    .await
                    .map_err(|e| {
                        Error::InvalidOperation(format!("Failed to complete migration: {:?}", e))
                    })?;

                // Also complete in allocator
                self.group_allocator
                    .complete_node_migration(&node_id)
                    .await
                    .map_err(|e| {
                        Error::InvalidOperation(format!(
                            "Failed to complete allocator migration: {:?}",
                            e
                        ))
                    })?;
            }
            MigrationEvent::MigrationFailed { node_id, reason } => {
                warn!("Migration failed for node {:?}: {}", node_id, reason);
                // TODO: Handle rollback
            }
            _ => {}
        }

        Ok(())
    }

    /// Check migration timeouts periodically
    pub async fn check_migration_timeouts(&self) -> Result<(), Error> {
        let mut coordinator = self.migration_coordinator.write().await;
        let timed_out = coordinator.check_timeouts().await;

        for node_id in timed_out {
            warn!("Migration timed out for node {:?}", node_id);
            // TODO: Handle timeout recovery
        }

        Ok(())
    }

    /// Initialize a single-node cluster
    pub async fn initialize_single_node_cluster(
        &self,
        node_id: NodeId,
        reason: InitiatorReason,
    ) -> Result<(), Error> {
        info!("Initializing single-node cluster for node {}", node_id);

        // Set cluster state to initiator
        self.set_cluster_state(ClusterState::Initiator {
            initiated_at: Instant::now(),
            reason,
        })
        .await;

        // Initialize Raft with just this node
        // Get our own GovernanceNode from topology
        let own_node = self
            .topology
            .get_own_node()
            .await
            .map_err(|e| Error::InvalidMessage(format!("Failed to get own node: {e}")))?;

        let membership = std::collections::BTreeMap::from([(node_id.clone(), own_node)]);

        self.raft_instance
            .initialize(membership)
            .await
            .map_err(|e| Error::Raft(format!("Failed to initialize Raft: {e}")))?;

        info!("Successfully initialized single-node cluster");

        // Wait a bit for Raft to stabilize before creating the group
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        // Create default consensus group
        info!("Creating default consensus group");
        let default_group_id = crate::allocation::ConsensusGroupId::new(1);
        let add_group_op = GlobalOperation::AddConsensusGroup {
            group_id: default_group_id,
            members: vec![node_id.clone()],
        };

        let request = GlobalRequest {
            operation: add_group_op,
        };

        info!(
            "Submitting AddConsensusGroup request for group {:?}",
            default_group_id
        );
        match self.submit_request(request).await {
            Ok(response) => {
                if response.success {
                    info!(
                        "Successfully submitted default consensus group {:?} creation (sequence: {})",
                        default_group_id, response.sequence
                    );
                } else {
                    warn!(
                        "Failed to create default consensus group: {:?}",
                        response.error
                    );
                }
            }
            Err(e) => {
                warn!("Error creating default consensus group: {}", e);
                // Not a critical error - cluster can still function
            }
        }

        Ok(())
    }

    /// Initialize a multi-node cluster (as leader)
    pub async fn initialize_multi_node_cluster(
        &self,
        node_id: NodeId,
        _peers: Vec<crate::Node>,
    ) -> Result<(), Error> {
        info!(
            "Initializing multi-node cluster as leader for node {}",
            node_id
        );

        // Set cluster state to initiator
        self.set_cluster_state(ClusterState::Initiator {
            initiated_at: Instant::now(),
            reason: InitiatorReason::DiscoveryTimeout,
        })
        .await;

        // For now, start as single node - peers will join later
        // Get our own GovernanceNode from topology
        let own_node = self
            .topology
            .get_own_node()
            .await
            .map_err(|e| Error::InvalidMessage(format!("Failed to get own node: {e}")))?;

        let membership = std::collections::BTreeMap::from([(node_id.clone(), own_node)]);

        self.raft_instance
            .initialize(membership)
            .await
            .map_err(|e| Error::Raft(format!("Failed to initialize leader Raft: {e}")))?;

        info!("Successfully initialized multi-node cluster as leader");

        // Wait a bit for Raft to stabilize before creating the group
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        // Create default consensus group
        info!("Creating default consensus group");
        let default_group_id = crate::allocation::ConsensusGroupId::new(1);
        let add_group_op = GlobalOperation::AddConsensusGroup {
            group_id: default_group_id,
            members: vec![node_id.clone()],
        };

        let request = GlobalRequest {
            operation: add_group_op,
        };

        info!(
            "Submitting AddConsensusGroup request for group {:?}",
            default_group_id
        );
        match self.submit_request(request).await {
            Ok(response) => {
                if response.success {
                    info!(
                        "Successfully submitted default consensus group {:?} creation (sequence: {})",
                        default_group_id, response.sequence
                    );
                } else {
                    warn!(
                        "Failed to create default consensus group: {:?}",
                        response.error
                    );
                }
            }
            Err(e) => {
                warn!("Error creating default consensus group: {}", e);
                // Not a critical error - cluster can still function
            }
        }

        Ok(())
    }

    /// Join an existing cluster via Raft with retry logic
    pub async fn join_existing_cluster_via_raft(
        &self,
        node_id: NodeId,
        existing_cluster: &ClusterDiscoveryResponse,
    ) -> Result<(), Error> {
        // Get retry configuration
        let retry_config = &self.cluster_join_retry_config;

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
        self.set_cluster_state(ClusterState::WaitingToJoin {
            requested_at: Instant::now(),
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
            requester_id: node_id.clone(),
            requester_node: requester_node.into(),
        };

        let mut last_error = None;
        let mut retry_delay = retry_config.initial_delay;

        for attempt in 1..=retry_config.max_attempts {
            info!(
                "Cluster join attempt {} of {}, targeting leader: {}",
                attempt, retry_config.max_attempts, current_leader
            );

            // Generate correlation ID and add pending request
            let correlation_id = Uuid::new_v4();
            let response_rx = self
                .add_pending_request::<ClusterJoinResponse>(
                    correlation_id,
                    current_leader.clone(),
                    "cluster_join_request".to_string(),
                )
                .await;

            // Send join request with correlation ID using network callback
            let message = Message::Application(Box::new(ApplicationMessage::ClusterJoinRequest(
                join_request.clone(),
            )));

            // Send using network callback
            match self
                .network_manager
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

                    // Wait before retrying, unless this is the last attempt
                    if attempt < retry_config.max_attempts {
                        tokio::time::sleep(retry_delay).await;
                        retry_delay = std::cmp::min(retry_delay * 2, retry_config.max_delay);
                    }
                    continue;
                }
            }

            // Wait for join response with timeout
            let join_response =
                match tokio::time::timeout(retry_config.request_timeout, response_rx).await {
                    Ok(Ok(response)) => response,
                    Ok(Err(_)) => {
                        warn!("Join request channel was cancelled on attempt {}", attempt);
                        last_error =
                            Some(Error::InvalidMessage("Join request cancelled".to_string()));

                        // Wait before retrying, unless this is the last attempt
                        if attempt < retry_config.max_attempts {
                            tokio::time::sleep(retry_delay).await;
                            retry_delay = std::cmp::min(retry_delay * 2, retry_config.max_delay);
                        }
                        continue;
                    }
                    Err(_) => {
                        warn!("Join request timeout on attempt {}", attempt);
                        last_error =
                            Some(Error::InvalidMessage("Join request timeout".to_string()));

                        // Wait before retrying, unless this is the last attempt
                        if attempt < retry_config.max_attempts {
                            tokio::time::sleep(retry_delay).await;
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

                self.set_cluster_state(ClusterState::Joined {
                    joined_at: Instant::now(),
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
                        self.set_cluster_state(ClusterState::WaitingToJoin {
                            requested_at: Instant::now(),
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
                    tokio::time::sleep(retry_delay).await;
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

        self.set_cluster_state(ClusterState::Failed {
            failed_at: Instant::now(),
            error: format!(
                "Failed after {} attempts: {}",
                retry_config.max_attempts, final_error
            ),
        })
        .await;

        Err(final_error)
    }

    /// Wait for the cluster to be ready (either Initiator or Joined state)
    pub async fn wait_for_leader(&self, timeout: Option<Duration>) -> Result<(), Error> {
        let timeout = timeout.unwrap_or(Duration::from_secs(30));
        let start_time = Instant::now();

        loop {
            let state = self.cluster_state().await;

            match state {
                ClusterState::Initiator { .. } | ClusterState::Joined { .. } => {
                    debug!("Cluster is ready with state: {:?}", state);
                    return Ok(());
                }
                ClusterState::Failed { error, .. } => {
                    return Err(Error::InvalidMessage(format!(
                        "Cluster failed to initialize: {}",
                        error
                    )));
                }
                _ => {
                    // Continue waiting
                    if start_time.elapsed() > timeout {
                        return Err(Error::InvalidMessage(format!(
                            "Timeout waiting for cluster to be ready. Current state: {:?}",
                            state
                        )));
                    }

                    tokio::time::sleep(Duration::from_millis(100)).await;
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
    pub async fn start_topology(&self) -> Result<(), Error> {
        self.topology.start().await
    }

    /// Discover existing clusters using the network callback
    pub async fn discover_existing_clusters(&self) -> Result<Vec<ClusterDiscoveryResponse>, Error> {
        // Get all peers from topology
        let all_peers = self.topology.get_all_peers().await;
        debug!("Discovering clusters among {} peers", all_peers.len());

        if all_peers.is_empty() {
            return Ok(Vec::new());
        }

        // Prepare all discovery requests
        let mut discovery_futures = Vec::new();
        let mut response_receivers = Vec::new();
        let mut correlation_ids = Vec::new();

        for peer in &all_peers {
            let peer_node_id = NodeId::new(peer.public_key());
            let discovery_request = ClusterDiscoveryRequest::new(self.local_node_id.clone());
            let correlation_id = Uuid::new_v4();

            // Add pending request before sending
            let response_rx = self
                .add_pending_request::<ClusterDiscoveryResponse>(
                    correlation_id,
                    peer_node_id.clone(),
                    "cluster_discovery_request".to_string(),
                )
                .await;

            correlation_ids.push(correlation_id);
            response_receivers.push(response_rx);

            let message = Message::Application(Box::new(ApplicationMessage::ClusterDiscovery(
                discovery_request,
            )));

            // Create future for sending this request
            let network_manager = self.network_manager.clone();
            let send_future = async move {
                if let Err(e) = network_manager
                    .send_message_with_correlation(peer_node_id.clone(), message, correlation_id)
                    .await
                {
                    warn!(
                        "Failed to send discovery request to {}: {}",
                        peer_node_id, e
                    );
                }
            };

            discovery_futures.push(send_future);
        }

        // Send all discovery requests in parallel
        futures::future::join_all(discovery_futures).await;

        // Wait for responses with timeout - process all in parallel
        let discovery_timeout = Duration::from_secs(5);

        // Create futures for all response receivers with timeout
        let response_futures: Vec<_> = response_receivers
            .into_iter()
            .map(|rx| tokio::time::timeout(discovery_timeout, rx))
            .collect();

        // Wait for all responses in parallel
        let results = future::join_all(response_futures).await;

        // Process results
        let mut responses = Vec::new();
        let mut successful_requests = 0;
        let mut failed_requests = 0;

        for result in results {
            match result {
                Ok(Ok(response)) => {
                    responses.push(response);
                    successful_requests += 1;
                }
                Ok(Err(_)) | Err(_) => {
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
            "Discovery complete: {} successful, {} failed out of {} requests. Responses: {:?}",
            successful_requests,
            failed_requests,
            all_peers.len(),
            responses
                .iter()
                .map(|r| {
                    let leader_str = r.current_leader.as_ref().map(|l| l.to_string());
                    format!(
                        "from {} - has_cluster: {}, leader: {:?}, size: {:?}",
                        r.responder_id,
                        r.has_active_cluster,
                        leader_str.as_ref().map(|s| &s[..8.min(s.len())]),
                        r.cluster_size
                    )
                })
                .collect::<Vec<_>>()
        );
        Ok(responses)
    }

    /// Comprehensive shutdown of all GlobalManager components
    pub async fn shutdown_all(&self) -> Result<(), Error> {
        info!(
            "Shutting down GlobalManager for node {}",
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

        // 3. Set cluster state to indicate shutdown
        self.set_cluster_state(ClusterState::Failed {
            failed_at: Instant::now(),
            error: "System shutdown".to_string(),
        })
        .await;

        info!(
            "GlobalManager shutdown complete for node {}",
            self.local_node_id
        );
        Ok(())
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
    pub async fn complete_pending_request<T: Send + Sync + 'static>(
        &self,
        correlation_id: Uuid,
        response: T,
    ) -> Result<(), Error> {
        let mut pending = self.pending_requests.write().await;

        if let Some(boxed_request) = pending.remove(&correlation_id) {
            // Downcast the boxed request to the expected type
            if let Ok(pending_request) = boxed_request.downcast::<PendingRequest<T>>() {
                // Send the response through the channel
                let _ = pending_request.response_tx.send(response);
                Ok(())
            } else {
                Err(Error::InvalidMessage("Response type mismatch".to_string()))
            }
        } else {
            Err(Error::InvalidMessage(
                "No pending request found".to_string(),
            ))
        }
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

    /// Handle incoming cluster discovery response with correlation ID
    pub async fn handle_cluster_discovery_response(
        &self,
        response: ClusterDiscoveryResponse,
        correlation_id: Option<Uuid>,
    ) {
        if let Some(corr_id) = correlation_id {
            // Send through response channel for correlation
            if let Err(e) = self.response_tx.send((corr_id, Box::new(response))) {
                warn!("Failed to send discovery response through channel: {}", e);
            }
        } else {
            warn!("Received discovery response without correlation ID - ignoring");
        }
    }

    /// Handle incoming network message
    pub async fn handle_network_message(
        &self,
        node_id: NodeId,
        message: Message,
        correlation_id: Option<Uuid>,
    ) -> Result<(), Error> {
        match message {
            Message::Application(app_msg) => {
                match *app_msg {
                    ApplicationMessage::ClusterDiscovery(req) => {
                        let (response_msg, response_correlation_id) = self
                            .handle_cluster_discovery_request(req, node_id.clone(), correlation_id)
                            .await;

                        // Send response back if we have one
                        if let Some(msg) = response_msg {
                            let response = Message::Application(Box::new(msg));
                            // Use the response correlation ID if provided, otherwise generate a new one
                            let corr_id = response_correlation_id
                                .or(correlation_id)
                                .unwrap_or_else(Uuid::new_v4);
                            if let Err(e) = self
                                .network_manager
                                .send_message_with_correlation(node_id, response, corr_id)
                                .await
                            {
                                error!("Failed to send cluster discovery response: {}", e);
                            }
                        }
                        Ok(())
                    }
                    ApplicationMessage::ClusterDiscoveryResponse(resp) => {
                        self.handle_cluster_discovery_response(resp, correlation_id)
                            .await;
                        Ok(())
                    }
                    ApplicationMessage::ClusterJoinRequest(req) => {
                        // Handle cluster join request
                        let response = self
                            .handle_cluster_join_request(req, node_id.clone(), correlation_id)
                            .await;

                        // Send response back
                        if let Some(resp) = response {
                            let response_msg = Message::Application(Box::new(
                                ApplicationMessage::ClusterJoinResponse(resp),
                            ));
                            let corr_id = correlation_id.unwrap_or_else(Uuid::new_v4);
                            if let Err(e) = self
                                .network_manager
                                .send_message_with_correlation(node_id, response_msg, corr_id)
                                .await
                            {
                                error!("Failed to send cluster join response: {}", e);
                            }
                        }
                        Ok(())
                    }
                    ApplicationMessage::ClusterJoinResponse(resp) => {
                        // Handle cluster join response
                        debug!("Received cluster join response: {:?}", resp);
                        // Send through response channel for correlation
                        if let Some(corr_id) = correlation_id {
                            if let Err(e) = self.response_tx.send((corr_id, Box::new(resp))) {
                                debug!(
                                    "Failed to send join response through correlation channel: {}",
                                    e
                                );
                            }
                        } else {
                            warn!("Received join response without correlation ID");
                        }
                        Ok(())
                    }
                    _ => {
                        // Other application messages can be handled here
                        debug!("Unhandled application message type");
                        Ok(())
                    }
                }
            }
            Message::Raft(raft_msg) => {
                // Process Raft messages and send responses
                match self
                    .process_raft_message(node_id.clone(), raft_msg, correlation_id)
                    .await
                {
                    Some((response_msg, response_corr_id)) => {
                        // Send response back
                        if let Err(e) = self
                            .network_manager
                            .send_message_with_correlation(
                                node_id,
                                crate::network::messages::Message::Raft(response_msg),
                                response_corr_id.unwrap_or_else(Uuid::new_v4),
                            )
                            .await
                        {
                            error!("Failed to send Raft response: {}", e);
                        }
                    }
                    None => {
                        // No response needed (e.g., for response messages)
                    }
                }
                Ok(())
            }
        }
    }

    /// Handle cluster join request
    async fn handle_cluster_join_request(
        &self,
        request: ClusterJoinRequest,
        sender_node_id: NodeId,
        correlation_id: Option<Uuid>,
    ) -> Option<ClusterJoinResponse> {
        // Only the leader can handle join requests
        let metrics = self.raft_instance.metrics().borrow().clone();
        let is_leader = metrics.current_leader.as_ref() == Some(&self.local_node_id);

        if !is_leader {
            warn!(
                "Received join request but not leader. Current leader: {:?}",
                metrics.current_leader
            );
            return Some(ClusterJoinResponse {
                responder_id: self.local_node_id.clone(),
                success: false,
                error_message: Some(format!(
                    "Not the leader. Current leader: {:?}",
                    metrics.current_leader
                )),
                cluster_size: None,
                current_term: None,
                current_leader: metrics.current_leader.clone(),
            });
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
            return Some(ClusterJoinResponse {
                responder_id: self.local_node_id.clone(),
                success: true,
                error_message: None,
                cluster_size: Some(
                    metrics.membership_config.membership().voter_ids().count()
                        + metrics.membership_config.membership().learner_ids().count(),
                ),
                current_term: Some(metrics.current_term),
                current_leader: metrics.current_leader.clone(),
            });
        }

        // Clone values for the async task
        let raft_clone = self.raft_instance.clone();
        let requester_id = request.requester_id.clone();
        let requester_node = crate::Node::from(request.requester_node.clone());
        let requester_governance_node = request.requester_node.clone();
        let local_node_id_clone = self.local_node_id.clone();
        let network_manager = self.network_manager.clone();
        let group_allocator = self.group_allocator.clone();
        let corr_id = correlation_id.unwrap_or_else(Uuid::new_v4);

        // Spawn a background task for async membership change handling
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

                    // Allocate groups for the new node
                    match group_allocator
                        .allocate_groups_for_node(&requester_id, &requester_governance_node)
                        .await
                    {
                        Ok(assigned_groups) => {
                            info!(
                                "Allocated {} groups for node {}: {:?}",
                                assigned_groups.len(),
                                requester_id,
                                assigned_groups
                            );

                            // Submit group assignments to global consensus
                            for group_id in &assigned_groups {
                                let operation = GlobalOperation::AssignNodeToGroup {
                                    node_id: requester_id.clone(),
                                    group_id: *group_id,
                                };
                                let request = GlobalRequest { operation };

                                // Submit to global consensus (fire and forget for now)
                                if let Err(e) = raft_clone.client_write(request).await {
                                    error!("Failed to submit group assignment to consensus: {}", e);
                                }
                            }
                        }
                        Err(e) => {
                            error!("Failed to allocate groups for node {}: {}", requester_id, e);
                            // Continue anyway - node is still in the cluster
                        }
                    }

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
                        current_leader: metrics.current_leader.clone(),
                    };

                    // Send response back through network manager with correlation ID
                    let response_msg = crate::network::messages::Message::Application(Box::new(
                        ApplicationMessage::ClusterJoinResponse(response),
                    ));
                    if let Err(e) = network_manager
                        .send_message_with_correlation(
                            sender_node_id.clone(),
                            response_msg,
                            corr_id,
                        )
                        .await
                    {
                        error!("Failed to send join success response: {}", e);
                    }
                }
                Err(e) => {
                    error!("Failed to add {} to cluster: {}", requester_id, e);
                    let response = ClusterJoinResponse {
                        responder_id: local_node_id_clone,
                        success: false,
                        error_message: Some(format!("Failed to add to cluster: {}", e)),
                        cluster_size: None,
                        current_term: None,
                        current_leader: None,
                    };

                    // Send error response back with correlation ID
                    let response_msg = crate::network::messages::Message::Application(Box::new(
                        ApplicationMessage::ClusterJoinResponse(response),
                    ));
                    if let Err(e) = network_manager
                        .send_message_with_correlation(sender_node_id, response_msg, corr_id)
                        .await
                    {
                        error!("Failed to send join error response: {}", e);
                    }
                }
            }
        });

        // Return None since we're handling the response asynchronously
        None
    }

    /// Static helper to add a node to the cluster with retry
    async fn add_node_to_cluster_with_retry_static(
        raft: &openraft::Raft<GlobalTypeConfig>,
        node_id: NodeId,
        node_info: crate::Node,
    ) -> Result<(), Error> {
        // First add as learner
        raft.add_learner(node_id.clone(), node_info, true)
            .await
            .map_err(|e| Error::Raft(format!("Failed to add learner: {}", e)))?;

        // Then promote to voter
        let membership = raft.metrics().borrow().membership_config.clone();
        let mut voter_ids: std::collections::BTreeSet<_> = membership.voter_ids().collect();
        voter_ids.insert(node_id);

        raft.change_membership(voter_ids, false)
            .await
            .map_err(|e| Error::Raft(format!("Failed to change membership: {}", e)))?;

        Ok(())
    }

    /// Process incoming Raft messages
    async fn process_raft_message(
        &self,
        sender_id: NodeId,
        raft_message: crate::network::messages::RaftMessage,
        correlation_id: Option<Uuid>,
    ) -> Option<(crate::network::messages::RaftMessage, Option<Uuid>)> {
        use crate::network::adaptor::RaftAdapterResponse;
        use crate::network::messages::RaftMessage;

        match raft_message {
            RaftMessage::Vote(vote_data) => {
                let vote_request: openraft::raft::VoteRequest<GlobalTypeConfig> =
                    match ciborium::de::from_reader(vote_data.as_slice()) {
                        Ok(req) => req,
                        Err(e) => {
                            error!("Failed to deserialize VoteRequest: {}", e);
                            return None;
                        }
                    };

                match self.raft_instance.vote(vote_request).await {
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
                let append_request: openraft::raft::AppendEntriesRequest<GlobalTypeConfig> =
                    match ciborium::de::from_reader(append_data.as_slice()) {
                        Ok(req) => req,
                        Err(e) => {
                            error!("Failed to deserialize AppendEntriesRequest: {}", e);
                            return None;
                        }
                    };

                match self.raft_instance.append_entries(append_request).await {
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
                        error!("Append entries request failed: {}", e);
                        None
                    }
                }
            }
            RaftMessage::InstallSnapshot(snapshot_data) => {
                let snapshot_request: openraft::raft::InstallSnapshotRequest<GlobalTypeConfig> =
                    match ciborium::de::from_reader(snapshot_data.as_slice()) {
                        Ok(req) => req,
                        Err(e) => {
                            error!("Failed to deserialize InstallSnapshotRequest: {}", e);
                            return None;
                        }
                    };

                match self.raft_instance.install_snapshot(snapshot_request).await {
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
                        error!("Install snapshot request failed: {}", e);
                        None
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
                None
            }
            RaftMessage::AppendEntriesResponse(payload) => {
                self.process_raft_response::<openraft::raft::AppendEntriesResponse<GlobalTypeConfig>>(
                    &payload,
                    correlation_id,
                    &sender_id,
                    "AppendEntriesResponse",
                    RaftAdapterResponse::AppendEntries,
                );
                None
            }
            RaftMessage::InstallSnapshotResponse(payload) => {
                self.process_raft_response::<openraft::raft::InstallSnapshotResponse<GlobalTypeConfig>>(
                    &payload,
                    correlation_id,
                    &sender_id,
                    "InstallSnapshotResponse",
                    RaftAdapterResponse::InstallSnapshot,
                );
                None
            }
        }
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

    /// Process RaftAdapter requests and send them through the network
    async fn raft_adapter_processor_loop(
        mut request_rx: tokio::sync::mpsc::UnboundedReceiver<(
            NodeId,
            Uuid,
            Box<crate::network::adaptor::RaftAdapterRequest<GlobalTypeConfig>>,
        )>,
        network_manager: Arc<crate::network::network_manager::NetworkManager<G, A>>,
    ) {
        debug!("RaftAdapter processor task started");

        while let Some((target_node_id, correlation_id, request)) = request_rx.recv().await {
            // Convert RaftAdapter request to network message
            let message = match *request {
                crate::network::adaptor::RaftAdapterRequest::Vote(vote_req) => {
                    let mut vote_data = Vec::new();
                    if let Err(e) = ciborium::ser::into_writer(&vote_req, &mut vote_data) {
                        error!("Failed to serialize Vote request: {}", e);
                        continue;
                    }
                    crate::network::messages::Message::Raft(
                        crate::network::messages::RaftMessage::Vote(vote_data),
                    )
                }
                crate::network::adaptor::RaftAdapterRequest::AppendEntries(append_req) => {
                    let mut append_data = Vec::new();
                    if let Err(e) = ciborium::ser::into_writer(&append_req, &mut append_data) {
                        error!("Failed to serialize AppendEntries request: {}", e);
                        continue;
                    }
                    crate::network::messages::Message::Raft(
                        crate::network::messages::RaftMessage::AppendEntries(append_data),
                    )
                }
                crate::network::adaptor::RaftAdapterRequest::InstallSnapshot(snapshot_req) => {
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

    /// Process Raft responses received from the network
    async fn raft_response_processor_loop(
        _raft_adapter_response_tx: tokio::sync::mpsc::UnboundedSender<(
            NodeId,
            Uuid,
            Box<crate::network::adaptor::RaftAdapterResponse<GlobalTypeConfig>>,
        )>,
        _raft_instance: Arc<openraft::Raft<GlobalTypeConfig>>,
        _network_manager: Arc<crate::network::network_manager::NetworkManager<G, A>>,
    ) {
        // This task would handle incoming Raft responses and forward them to the RaftAdapter
        // For now, this is handled inline when Raft messages are received
        debug!("Raft response processor task started");
        // The actual processing happens in handle_network_message when Raft messages are received
    }

    /// Process correlated responses and complete pending requests
    async fn response_correlation_loop(
        mut response_rx: tokio::sync::mpsc::UnboundedReceiver<(
            Uuid,
            Box<dyn std::any::Any + Send + Sync>,
        )>,
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

    /// Get the current global state
    pub async fn get_current_state(&self) -> Result<super::GlobalState, Error> {
        // Clone the current state
        Ok((*self.global_state).clone())
    }

    /// Complete a pending request by sending the response through its channel
    fn complete_pending_request_static(
        pending_request: Box<dyn std::any::Any + Send + Sync>,
        response: Box<dyn std::any::Any + Send + Sync>,
    ) -> Result<(), ()> {
        // Try ClusterDiscoveryResponse first
        if pending_request.is::<PendingRequest<ClusterDiscoveryResponse>>()
            && response.is::<ClusterDiscoveryResponse>()
        {
            let pending = pending_request.downcast::<PendingRequest<ClusterDiscoveryResponse>>();
            let resp = response.downcast::<ClusterDiscoveryResponse>();
            if let (Ok(pending), Ok(resp)) = (pending, resp) {
                let _ = pending.response_tx.send(*resp);
                return Ok(());
            }
            return Err(());
        }

        // Try ClusterJoinResponse
        if pending_request.is::<PendingRequest<ClusterJoinResponse>>()
            && response.is::<ClusterJoinResponse>()
        {
            let pending = pending_request.downcast::<PendingRequest<ClusterJoinResponse>>();
            let resp = response.downcast::<ClusterJoinResponse>();
            if let (Ok(pending), Ok(resp)) = (pending, resp) {
                let _ = pending.response_tx.send(*resp);
                return Ok(());
            }
        }

        Err(())
    }
}
