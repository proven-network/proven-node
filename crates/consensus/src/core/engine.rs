//! Proven consensus engine - the core implementation
//!
//! This module provides the main consensus engine that handles:
//! - Global and group consensus operations
//! - Lifecycle management
//! - Network and transport coordination
//! - PubSub messaging
//! - Stream management

pub mod cluster;
pub mod coordinator;
pub mod events;
pub mod global_layer;
pub mod groups_layer;
pub mod lifecycle;
pub mod routing;

// Re-export main types
pub use cluster::ClusterManager;
pub use coordinator::CrossLayerCoordinator;
pub use events::EventBus;
pub use global_layer::GlobalConsensusLayer;
pub use groups_layer::GroupsConsensusLayer;
pub use lifecycle::LifecycleManager;
pub use routing::{ConsensusRouter, GlobalOperationHandler, StreamRouter};

use crate::pubsub::{PubSubManager, StreamBridge};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use axum::Router;
use bytes::Bytes;
use openraft::{Config, Raft, RaftMetrics, RaftNetworkFactory};
use tokio::sync::RwLock;
use tracing::{error, info, warn};
use uuid::Uuid;

use proven_bootable::Bootable;
use proven_governance::Governance;

use crate::core::group::GroupsManager;

use crate::network::messages::GroupRaftMessage;
use crate::operations::handlers::{GlobalOperationResponse, GroupStreamOperationResponse};
use crate::{
    ConsensusGroupId,
    core::stream::{StreamStorageBackend, UnifiedStreamManager},
    core::{
        global::{GlobalConsensusTypeConfig, GlobalState, StreamConfig},
        group::{GroupConsensusTypeConfig, GroupStreamOperation},
        monitoring::MonitoringCoordinator,
    },
    error::{ConsensusResult, Error},
    network::{
        ClusterState, InitiatorReason,
        messages::{
            ClusterDiscoveryRequest, ClusterDiscoveryResponse, ClusterJoinRequest,
            ClusterJoinResponse, GlobalMessage, GroupMessage, Message,
        },
    },
    operations::{
        GlobalOperation, OperationContext, OperationValidator,
        validators::{
            GroupOperationValidator, NodeOperationValidator, RoutingOperationValidator,
            StreamManagementOperationValidator,
        },
    },
};
use proven_network::NetworkManager;
use proven_topology::{NodeId, TopologyManager};
use proven_transport::Transport;

/// Proven consensus engine - the core implementation
#[derive(Clone)]
pub struct Engine<T, G>
where
    T: Transport,
    G: Governance,
{
    /// Node identifier
    node_id: NodeId,

    /// Event bus for cross-layer communication
    event_bus: Arc<EventBus>,

    /// Global consensus layer
    global_layer: Arc<GlobalConsensusLayer>,

    /// Groups consensus layer
    groups_layer: Arc<RwLock<GroupsConsensusLayer<T, G>>>,

    /// Cross-layer coordinator
    coordinator: Arc<CrossLayerCoordinator<T, G>>,

    /// Cluster manager
    cluster_manager: Arc<ClusterManager<T, G>>,

    /// Stream router
    stream_router: Arc<StreamRouter<T, G>>,

    /// Lifecycle manager
    lifecycle_manager: Arc<LifecycleManager<T, G>>,

    /// Network manager for all communication
    network_manager: Arc<NetworkManager<T, G>>,

    /// Topology manager
    topology_manager: Arc<TopologyManager<G>>,

    /// Monitoring coordinator
    monitoring_coordinator: Option<Arc<MonitoringCoordinator<T, G>>>,

    /// Operation validators
    stream_validator: StreamManagementOperationValidator,
    group_validator: GroupOperationValidator,
    node_validator: NodeOperationValidator,
    routing_validator: RoutingOperationValidator,

    /// PubSub manager for messaging
    pubsub_manager: Arc<PubSubManager<T, G>>,

    /// Stream bridge for PubSub->Stream integration (temporary optional)
    stream_bridge: Option<Arc<StreamBridge<T, G>>>,

    /// Background task handles
    task_handles: Arc<std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>>,
}

impl<T, G> Engine<T, G>
where
    T: Transport,
    G: Governance,
{
    /// Create a new proven engine
    #[allow(clippy::too_many_arguments)]
    pub async fn new<GN, GL, GM>(
        node_id: NodeId,
        global_config: Config,
        global_network: GN,
        global_log_storage: GL,
        global_state_machine: GM,
        global_state: Arc<GlobalState>,
        topology: Arc<TopologyManager<G>>,
        network_manager: Arc<NetworkManager<T, G>>,
        local_base_config: Config,
        local_storage_config: &crate::config::StorageConfig,
        stream_storage_backend: StreamStorageBackend,
        cluster_join_retry_config: crate::config::ClusterJoinRetryConfig,
        monitoring_config: crate::config::monitoring::MonitoringConfig,
    ) -> ConsensusResult<Self>
    where
        GN: RaftNetworkFactory<GlobalConsensusTypeConfig>,
        GL: openraft::storage::RaftLogStorage<GlobalConsensusTypeConfig>
            + Clone
            + Send
            + Sync
            + 'static,
        GM: openraft::storage::RaftStateMachine<GlobalConsensusTypeConfig> + Send + Sync + 'static,
    {
        // Create global Raft instance
        let global_config_arc = Arc::new(global_config);
        let global_raft = Raft::new(
            node_id.clone(),
            global_config_arc.clone(),
            global_network,
            global_log_storage,
            global_state_machine,
        )
        .await
        .map_err(|e| Error::Raft(format!("Failed to create global Raft instance: {e}")))?;

        // Create event bus for cross-layer communication
        let event_bus = Arc::new(EventBus::new(100));

        // Create local storage factory
        let local_storage_factory =
            crate::core::group::storage::create_local_storage_factory(local_storage_config)?;

        // Create groups manager with allocator and stream migration enabled
        let groups_manager = Arc::new(RwLock::new(
            GroupsManager::new(
                node_id.clone(),
                local_base_config.clone(),
                local_storage_factory,
                stream_storage_backend.clone(),
                network_manager.clone(),
                topology.clone(),
            )
            .with_default_allocator()
            .with_default_stream_migration(),
        ));

        // Create layers
        let global_layer = Arc::new(GlobalConsensusLayer::new(
            node_id.clone(),
            Arc::new(global_raft),
            global_state.clone(),
        ));

        let mut groups_layer =
            GroupsConsensusLayer::new(node_id.clone(), groups_manager.clone(), event_bus.clone());

        // Start the groups layer event processor
        groups_layer.start().await?;

        let groups_layer_arc = Arc::new(RwLock::new(groups_layer));

        // Create coordinator
        let coordinator = Arc::new(CrossLayerCoordinator::new(event_bus.clone()));

        // Create cluster manager
        let cluster_manager = Arc::new(ClusterManager::new(
            node_id.clone(),
            global_layer.clone(),
            groups_layer_arc.clone(),
            network_manager.clone(),
            topology.clone(),
            cluster_join_retry_config,
        ));

        // Create stream router
        let stream_router = Arc::new(StreamRouter::new(
            global_state.clone(),
            groups_layer_arc.clone(),
        ));

        // Create lifecycle manager
        let lifecycle_manager = Arc::new(LifecycleManager::new(
            node_id.clone(),
            global_layer.clone(),
            groups_layer_arc.clone(),
            cluster_manager.clone(),
        ));

        // Create monitoring coordinator if monitoring is enabled
        let monitoring_coordinator = if monitoring_config.enabled {
            // Create system view first
            let system_view = Arc::new(crate::core::monitoring::SystemView::new(
                global_state.clone(),
                groups_manager.clone(),
                topology.clone(),
            ));

            // Create metrics registry if prometheus is enabled
            let metrics_registry = if monitoring_config.prometheus.enabled {
                match crate::core::monitoring::MetricsRegistry::new() {
                    Ok(registry) => Some(Arc::new(registry)),
                    Err(e) => {
                        warn!("Failed to create metrics registry: {}", e);
                        None
                    }
                }
            } else {
                None
            };

            let mut coordinator = MonitoringCoordinator::new(
                monitoring_config.clone(),
                system_view.clone(),
                node_id.clone(),
            );

            // Add metrics collector if registry was created
            if let Some(registry) = metrics_registry {
                let collector = Arc::new(crate::core::monitoring::MetricsCollector::new(
                    registry.clone(),
                    system_view.clone(),
                    monitoring_config.update_interval,
                ));
                coordinator = coordinator.with_metrics_collector(collector);
            }

            // Add groups layer reference for rebalancing
            coordinator = coordinator.with_groups_layer(groups_layer_arc.clone());

            Some(Arc::new(coordinator))
        } else {
            None
        };

        // Create PubSub manager
        let pubsub_manager = Arc::new(crate::pubsub::PubSubManager::new(
            node_id.clone(),
            network_manager.clone(),
            topology.clone(),
        ));

        // TODO: StreamBridge would be created here if needed
        let stream_bridge = None;

        let manager = Self {
            node_id: node_id.clone(),
            event_bus,
            global_layer,
            groups_layer: groups_layer_arc,
            coordinator,
            cluster_manager,
            stream_router,
            lifecycle_manager,
            network_manager: network_manager.clone(),
            topology_manager: topology.clone(),
            monitoring_coordinator,
            stream_validator: StreamManagementOperationValidator,
            group_validator: GroupOperationValidator {
                min_nodes: 1,
                max_nodes: 10,
            },
            node_validator: NodeOperationValidator {
                max_groups_per_node: 5,
            },
            routing_validator: RoutingOperationValidator,
            pubsub_manager,
            stream_bridge,
            task_handles: Arc::new(std::sync::Mutex::new(Vec::new())),
        };

        Ok(manager)
    }

    // Global consensus methods

    /// Submit a global request to the consensus system
    pub async fn submit_global_request(
        &self,
        request: GlobalOperation,
    ) -> ConsensusResult<GlobalOperationResponse> {
        let start = std::time::Instant::now();
        let op_type = match &request {
            GlobalOperation::StreamManagement(_) => "stream_management",
            GlobalOperation::Group(_) => "group",
            GlobalOperation::Node(_) => "node",
            GlobalOperation::Routing(_) => "routing",
        };

        // Validate the operation
        let context = OperationContext {
            node_id: self.node_id.clone(),
            global_state: &self.global_layer.global_state(),
            is_leader: self.is_global_leader().await,
        };

        match &request {
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

        // Submit through global layer
        let result = self.global_layer.submit_request(request).await;

        // Record metrics if available
        if let Some(metrics) = self.metrics_registry() {
            let duration = start.elapsed();
            let status = if result.is_ok() { "success" } else { "failure" };

            metrics
                .consensus
                .operations_total
                .with_label_values(&[op_type, status])
                .inc();

            metrics
                .consensus
                .operation_duration
                .with_label_values(&[op_type])
                .observe(duration.as_secs_f64());
        }

        result
    }

    /// Check if this node is the global leader
    pub async fn is_global_leader(&self) -> bool {
        self.global_layer.is_leader().await
    }

    /// Get global metrics
    pub async fn global_metrics(&self) -> RaftMetrics<GlobalConsensusTypeConfig> {
        self.global_layer.metrics().await
    }

    // Groups consensus methods

    /// Initialize a consensus group
    pub async fn initialize_group(
        &self,
        group_id: ConsensusGroupId,
        members: Vec<NodeId>,
    ) -> ConsensusResult<()> {
        self.lifecycle_manager
            .initialize_group(group_id, members)
            .await
    }

    /// Get the stream manager for a specific group
    pub async fn get_stream_manager(
        &self,
        group_id: ConsensusGroupId,
    ) -> ConsensusResult<Arc<UnifiedStreamManager>> {
        self.groups_layer
            .read()
            .await
            .groups_manager()
            .read()
            .await
            .get_stream_manager(group_id)
            .await
    }

    /// Process a local stream operation
    pub async fn process_local_operation(
        &self,
        group_id: ConsensusGroupId,
        operation: GroupStreamOperation,
    ) -> ConsensusResult<GroupStreamOperationResponse> {
        let start = std::time::Instant::now();
        let op_type = match &operation {
            GroupStreamOperation::Stream(_) => "stream",
            GroupStreamOperation::Migration(_) => "migration",
            GroupStreamOperation::PubSub(_) => "pubsub",
            GroupStreamOperation::Maintenance(_) => "maintenance",
        };

        let result = self
            .groups_layer
            .read()
            .await
            .process_operation(group_id, operation)
            .await;

        // Record metrics if available
        if let Some(metrics) = self.metrics_registry() {
            let duration = start.elapsed();
            let status = if result.is_ok() { "success" } else { "failure" };

            metrics
                .consensus
                .operations_total
                .with_label_values(&[op_type, status])
                .inc();

            metrics
                .consensus
                .operation_duration
                .with_label_values(&[op_type])
                .observe(duration.as_secs_f64());
        }

        result
    }

    // Stream creation with immediate initialization

    /// Create a stream (global operation with immediate local initialization)
    pub async fn create_stream(
        &self,
        name: String,
        config: StreamConfig,
        group_id: ConsensusGroupId,
    ) -> ConsensusResult<GlobalOperationResponse> {
        // Use the coordinator for cross-layer operation
        self.coordinator
            .create_stream(
                &self.global_layer,
                &*self.groups_layer.read().await,
                name,
                config,
                group_id,
            )
            .await
    }

    /// Initialize a stream in a consensus group
    /// Note: This is now handled by the coordinator - keeping for backward compatibility
    async fn initialize_stream_in_group(
        &self,
        stream_name: &str,
        config: &StreamConfig,
        group_id: ConsensusGroupId,
    ) -> ConsensusResult<()> {
        warn!(
            "Direct stream initialization is deprecated. Use create_stream() instead for proper coordination."
        );

        let stream_manager = self.get_stream_manager(group_id).await?;

        // Convert global StreamConfig to local StreamConfig
        let local_config = crate::config::stream::StreamConfig {
            max_messages: config.max_messages,
            max_bytes: config.max_bytes,
            max_age_secs: config.max_age_secs,
            storage_type: crate::config::stream::StorageType::Memory, // Default
            retention_policy: crate::config::stream::RetentionPolicy::Limits,
            pubsub_bridge_enabled: false,
            consensus_group: Some(group_id),
            compact_on_deletion: false,
            compression: crate::config::stream::CompressionType::None,
        };

        stream_manager
            .create_stream(stream_name.to_string(), local_config)
            .await
            .map_err(|e| Error::storage(format!("Failed to create stream: {e}")))?;

        info!(
            "Initialized stream '{}' in consensus group {:?}",
            stream_name, group_id
        );

        Ok(())
    }

    // Message handling

    /// Helper method to send a message
    async fn send(&self, target: NodeId, message: Message) -> ConsensusResult<()> {
        // Send the message directly
        let target_clone = target.clone();
        self.network_manager
            .send(target_clone, message)
            .await
            .map_err(|e| {
                Error::Network(crate::error::NetworkError::SendFailed {
                    peer: target,
                    reason: e.to_string(),
                })
            })
    }

    /// Handle network messages
    pub async fn handle_network_message(
        &self,
        sender_id: NodeId,
        message: Message,
        correlation_id: Option<Uuid>,
    ) -> ConsensusResult<()> {
        match message {
            Message::Global(global_msg) => {
                self.handle_global_message(sender_id, *global_msg, correlation_id)
                    .await
            }
            Message::Local(local_msg) => {
                self.handle_local_message(sender_id, *local_msg, correlation_id)
                    .await
            }
            Message::PubSub(pubsub_msg) => {
                self.handle_pubsub_message(sender_id, *pubsub_msg, correlation_id)
                    .await
            }
        }
    }

    /// Handle global messages
    async fn handle_global_message(
        &self,
        sender_id: NodeId,
        message: GlobalMessage,
        correlation_id: Option<Uuid>,
    ) -> ConsensusResult<()> {
        match message {
            GlobalMessage::Raft(raft_msg) => {
                self.handle_global_raft_message(sender_id, raft_msg, correlation_id)
                    .await
            }
            GlobalMessage::ClusterDiscovery(req) => {
                match self.handle_cluster_discovery(sender_id.clone(), req).await {
                    Ok(_response) => {
                        // Send response back if we have a correlation ID
                        // Responses are handled by the handler return value in the new design
                        // The handler should have returned the response
                        Ok(())
                    }
                    Err(e) => Err(e),
                }
            }
            GlobalMessage::ClusterJoinRequest(req) => {
                match self.handle_cluster_join(sender_id.clone(), req).await {
                    Ok(_response) => {
                        // Send response back if we have a correlation ID
                        // Responses are handled by the handler return value in the new design
                        // The handler should have returned the response
                        Ok(())
                    }
                    Err(e) => Err(e),
                }
            }
            GlobalMessage::ClusterDiscoveryResponse(_) | GlobalMessage::ClusterJoinResponse(_) => {
                // These responses are now handled internally by NetworkManager via correlation IDs
                Ok(())
            }
            GlobalMessage::ConsensusRequest(_) | GlobalMessage::ConsensusResponse(_) => {
                // These are handled by the GlobalManager directly
                Err(Error::InvalidMessage(
                    "Consensus requests should be handled by GlobalManager".to_string(),
                ))
            }
        }
    }

    /// Handle local messages
    async fn handle_local_message(
        &self,
        sender_id: NodeId,
        message: GroupMessage,
        correlation_id: Option<Uuid>,
    ) -> ConsensusResult<()> {
        match message {
            GroupMessage::Raft { group_id, message } => {
                self.handle_group_raft_message(sender_id, group_id, message, correlation_id)
                    .await
            }
            GroupMessage::GroupJoinRequest(req) => {
                self.handle_local_group_join_request(sender_id, req, correlation_id)
                    .await
            }
            GroupMessage::GroupJoinResponse(resp) => {
                self.handle_local_group_join_response(resp, correlation_id)
                    .await
            }
            _ => {
                // Other message types are handled elsewhere
                Err(Error::InvalidMessage(
                    "This local message type is not handled by Engine".to_string(),
                ))
            }
        }
    }

    /// Handle PubSub messages
    pub async fn handle_pubsub_message(
        &self,
        sender_id: NodeId,
        pubsub_msg: crate::pubsub::PubSubMessage,
        correlation_id: Option<Uuid>,
    ) -> ConsensusResult<()> {
        // Forward to PubSub manager
        self.pubsub_manager
            .handle_message(sender_id, pubsub_msg, correlation_id)
            .await
            .map_err(|e| Error::InvalidMessage(format!("PubSub error: {e}")))
    }

    // Message handler implementations

    /// Handle global Raft protocol messages
    async fn handle_global_raft_message(
        &self,
        sender_id: NodeId,
        raft_message: crate::network::messages::GlobalRaftMessage,
        _correlation_id: Option<Uuid>,
    ) -> ConsensusResult<()> {
        use crate::network::messages::{GlobalMessage, GlobalRaftMessage};

        match raft_message {
            GlobalRaftMessage::VoteRequest(vote_request) => {
                // Handle vote request through Raft
                match self.global_layer.global_raft().vote(vote_request).await {
                    Ok(vote_response) => {
                        // Serialize and send response
                        let mut response_bytes = Vec::new();
                        ciborium::into_writer(&vote_response, &mut response_bytes).map_err(
                            |e| {
                                Error::InvalidMessage(format!(
                                    "Failed to serialize VoteResponse: {e}"
                                ))
                            },
                        )?;

                        let response_msg = Message::Global(Box::new(GlobalMessage::Raft(
                            GlobalRaftMessage::VoteResponse(vote_response),
                        )));

                        self.send(sender_id, response_msg).await?
                    }
                    Err(e) => {
                        error!("Failed to handle vote request: {:?}", e);
                        return Err(Error::Raft(format!("Failed to handle vote request: {e:?}")));
                    }
                }
            }

            GlobalRaftMessage::AppendEntriesRequest(append_request) => {
                // Handle append entries through Raft
                match self
                    .global_layer
                    .global_raft()
                    .append_entries(append_request)
                    .await
                {
                    Ok(append_response) => {
                        let response_msg = Message::Global(Box::new(GlobalMessage::Raft(
                            GlobalRaftMessage::AppendEntriesResponse(append_response),
                        )));

                        self.send(sender_id, response_msg).await?
                    }
                    Err(e) => {
                        error!("Failed to handle append entries request: {:?}", e);
                        return Err(Error::Raft(format!(
                            "Failed to handle append entries request: {e:?}"
                        )));
                    }
                }
            }

            GlobalRaftMessage::InstallSnapshotRequest(install_snapshot_request) => {
                // Handle install snapshot through Raft
                match self
                    .global_layer
                    .global_raft()
                    .install_snapshot(install_snapshot_request)
                    .await
                {
                    Ok(snapshot_response) => {
                        let response_msg = Message::Global(Box::new(GlobalMessage::Raft(
                            GlobalRaftMessage::InstallSnapshotResponse(snapshot_response),
                        )));

                        self.send(sender_id, response_msg).await?
                    }
                    Err(e) => {
                        error!("Failed to handle install snapshot request: {:?}", e);
                        return Err(Error::Raft(format!(
                            "Failed to handle install snapshot request: {e:?}"
                        )));
                    }
                }
            }

            // Response messages should not be received here as they are handled by the RaftAdapter
            GlobalRaftMessage::VoteResponse(_) => {
                warn!("Received unexpected vote response");
            }

            GlobalRaftMessage::AppendEntriesResponse(_) => {
                warn!("Received unexpected append entries response");
            }

            GlobalRaftMessage::InstallSnapshotResponse(_) => {
                warn!("Received unexpected install snapshot response");
            }
        }

        Ok(())
    }

    pub async fn handle_cluster_discovery(
        &self,
        sender_id: NodeId,
        req: ClusterDiscoveryRequest,
    ) -> ConsensusResult<ClusterDiscoveryResponse> {
        self.cluster_manager
            .handle_discovery_request(sender_id, req)
            .await
    }

    pub async fn handle_cluster_join(
        &self,
        sender_id: NodeId,
        req: ClusterJoinRequest,
    ) -> ConsensusResult<ClusterJoinResponse> {
        info!("Handling cluster join request from {}", sender_id);

        // Only process join requests if we're the leader
        if !self.is_global_leader().await {
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
                error!("Failed to add learner {}: {:?}", sender_id, e);

                Ok(ClusterJoinResponse {
                    error_message: Some(format!("Failed to add node to cluster: {e:?}")),
                    cluster_size: None,
                    current_term: None,
                    responder_id: self.node_id.clone(),
                    success: false,
                    current_leader: Some(self.node_id.clone()),
                })
            }
        }
    }

    async fn handle_group_raft_message(
        &self,
        sender_id: NodeId,
        group_id: ConsensusGroupId,
        raft_message: crate::network::messages::GroupRaftMessage,
        correlation_id: Option<Uuid>,
    ) -> ConsensusResult<()> {
        use crate::network::messages::{GroupMessage, Message};

        // Get the Raft instance for this group
        let raft = self
            .groups_layer
            .read()
            .await
            .groups_manager()
            .read()
            .await
            .get_raft(group_id)
            .await?;

        match raft_message {
            GroupRaftMessage::VoteRequest(vote_request) => {
                // Handle vote request through local Raft
                match raft.vote(vote_request).await {
                    Ok(vote_response) => {
                        let response_msg = Message::Local(Box::new(GroupMessage::Raft {
                            group_id,
                            message: GroupRaftMessage::VoteResponse(vote_response),
                        }));

                        self.send(sender_id, response_msg).await?
                    }
                    Err(e) => {
                        error!("Failed to handle local vote request: {:?}", e);
                    }
                }
            }

            GroupRaftMessage::AppendEntriesRequest(append_request) => {
                // Handle append entries through local Raft
                match raft.append_entries(append_request).await {
                    Ok(append_response) => {
                        let response_msg = Message::Local(Box::new(GroupMessage::Raft {
                            group_id,
                            message: GroupRaftMessage::AppendEntriesResponse(append_response),
                        }));

                        self.send(sender_id, response_msg).await?
                    }
                    Err(e) => {
                        error!("Failed to handle local append entries request: {:?}", e);
                    }
                }
            }

            GroupRaftMessage::InstallSnapshotRequest(snapshot_request) => {
                // Handle install snapshot through local Raft
                match raft.install_snapshot(snapshot_request).await {
                    Ok(snapshot_response) => {
                        let response_msg = Message::Local(Box::new(GroupMessage::Raft {
                            group_id,
                            message: GroupRaftMessage::InstallSnapshotResponse(snapshot_response),
                        }));

                        self.send(sender_id, response_msg).await?
                    }
                    Err(e) => {
                        error!("Failed to handle local install snapshot request: {:?}", e);
                    }
                }
            }

            // Response messages - these need to be forwarded to the local RaftAdapter
            GroupRaftMessage::VoteResponse(_)
            | GroupRaftMessage::AppendEntriesResponse(_)
            | GroupRaftMessage::InstallSnapshotResponse(_) => {
                // TODO: Forward to local RaftAdapter
                // For now, just log
                info!("Received local Raft response for group {:?}", group_id);
            }
        }

        Ok(())
    }

    async fn handle_local_group_join_request(
        &self,
        sender_id: NodeId,
        req: crate::network::messages::LocalGroupJoinRequest,
        correlation_id: Option<Uuid>,
    ) -> ConsensusResult<()> {
        use crate::network::messages::{GroupMessage, LocalGroupJoinResponse, Message};

        info!(
            "Handling group join request from {} for group {:?}",
            sender_id, req.group_id
        );

        // Check if we're managing this group
        let is_managing = self
            .groups_layer
            .read()
            .await
            .groups_manager()
            .read()
            .await
            .get_managed_groups()
            .await
            .contains(&req.group_id);

        if !is_managing {
            let response = LocalGroupJoinResponse {
                group_id: req.group_id,
                responder_id: self.node_id.clone(),
                success: false,
                error_message: Some("Not managing this group".to_string()),
                members: None,
            };

            let response_msg = Message::Local(Box::new(GroupMessage::GroupJoinResponse(response)));

            // Use the Engine's send helper method
            self.send(sender_id, response_msg).await?;
            return Ok(());
        }

        // Check if we're the leader of this group
        let is_leader = self
            .groups_layer
            .read()
            .await
            .groups_manager()
            .read()
            .await
            .is_leader(req.group_id)
            .await?;

        if !is_leader {
            let response = LocalGroupJoinResponse {
                group_id: req.group_id,
                responder_id: self.node_id.clone(),
                success: false,
                error_message: Some("Not the group leader".to_string()),
                members: None,
            };

            let response_msg = Message::Local(Box::new(GroupMessage::GroupJoinResponse(response)));

            // Use the Engine's send helper method
            self.send(sender_id, response_msg).await?;
            return Ok(());
        }

        // Get the Raft instance for this group
        let raft = self
            .groups_layer
            .read()
            .await
            .groups_manager()
            .read()
            .await
            .get_raft(req.group_id)
            .await?;

        // Add the node as a learner
        match raft
            .add_learner(req.requester_id.clone(), req.requester_node, true)
            .await
        {
            Ok(_) => {
                info!(
                    "Successfully added learner {} to consensus group {:?}",
                    req.requester_id, req.group_id
                );

                // Get current members
                let members = {
                    let metrics = raft.metrics();
                    let borrowed_metrics = metrics.borrow();
                    borrowed_metrics
                        .membership_config
                        .nodes()
                        .map(|(id, _)| id.clone())
                        .collect::<Vec<NodeId>>()
                };

                let response = LocalGroupJoinResponse {
                    group_id: req.group_id,
                    responder_id: self.node_id.clone(),
                    success: true,
                    error_message: None,
                    members: Some(members),
                };

                let response_msg =
                    Message::Local(Box::new(GroupMessage::GroupJoinResponse(response)));

                // Use the Engine's send helper method
                self.send(sender_id, response_msg).await?
            }
            Err(e) => {
                error!(
                    "Failed to add learner {} to group {:?}: {:?}",
                    req.requester_id, req.group_id, e
                );

                let response = LocalGroupJoinResponse {
                    group_id: req.group_id,
                    responder_id: self.node_id.clone(),
                    success: false,
                    error_message: Some(format!("Failed to add node to group: {e:?}")),
                    members: None,
                };

                let response_msg =
                    Message::Local(Box::new(GroupMessage::GroupJoinResponse(response)));

                // Use the Engine's send helper method
                self.send(sender_id, response_msg).await?
            }
        }

        Ok(())
    }

    async fn handle_local_group_join_response(
        &self,
        resp: crate::network::messages::LocalGroupJoinResponse,
        _correlation_id: Option<Uuid>,
    ) -> ConsensusResult<()> {
        if resp.success {
            info!(
                "Successfully joined consensus group {:?} (members: {:?})",
                resp.group_id, resp.members
            );
        } else {
            error!(
                "Failed to join consensus group {:?}: {:?}",
                resp.group_id, resp.error_message
            );
        }

        // TODO: Handle pending join operations
        // For now, just acknowledge the response

        Ok(())
    }

    // Getters for components that need access

    /// Get reference to global state
    pub fn global_state(&self) -> Arc<GlobalState> {
        self.global_layer.global_state()
    }

    /// Get current state (alias for global_state for compatibility)
    pub async fn get_current_state(&self) -> ConsensusResult<Arc<GlobalState>> {
        Ok(self.global_layer.global_state())
    }

    /// Get reference to topology
    pub fn topology(&self) -> Option<Arc<TopologyManager<G>>> {
        Some(self.topology_manager.clone())
    }

    /// Get current cluster state
    pub async fn cluster_state(&self) -> ClusterState {
        self.cluster_manager.cluster_state().read().await.clone()
    }

    /// Get metrics for all consensus groups
    pub async fn get_all_group_metrics(
        &self,
    ) -> HashMap<ConsensusGroupId, openraft::RaftMetrics<GroupConsensusTypeConfig>> {
        self.groups_layer
            .read()
            .await
            .groups_manager()
            .read()
            .await
            .get_all_metrics()
            .await
    }

    /// Get list of managed consensus groups
    pub async fn get_managed_groups(&self) -> Vec<ConsensusGroupId> {
        self.groups_layer
            .read()
            .await
            .groups_manager()
            .read()
            .await
            .get_managed_groups()
            .await
    }

    /// Check if this node is the leader of a consensus group
    pub async fn is_group_leader(&self, group_id: ConsensusGroupId) -> ConsensusResult<bool> {
        self.groups_layer
            .read()
            .await
            .groups_manager()
            .read()
            .await
            .is_leader(group_id)
            .await
    }

    /// Get my consensus groups and migration states
    pub async fn get_my_groups(
        &self,
    ) -> HashMap<ConsensusGroupId, crate::core::group::NodeMigrationState> {
        // For now, return all groups as Active since we don't have migration state tracking yet
        let groups = self
            .groups_layer
            .read()
            .await
            .groups_manager()
            .read()
            .await
            .get_managed_groups()
            .await;
        groups
            .into_iter()
            .map(|id| (id, crate::core::group::NodeMigrationState::Active))
            .collect()
    }

    /// Check if any consensus groups are in migration
    pub async fn is_in_migration(&self) -> bool {
        // For now, return false since we don't have migration state tracking yet
        false
    }

    // Methods for compatibility with GlobalManager interface

    /// Get current term from global consensus
    pub async fn current_term(&self) -> Option<u64> {
        let metrics = self.global_metrics().await;
        Some(metrics.current_term)
    }

    /// Get current leader from global consensus
    pub async fn current_leader(&self) -> Option<String> {
        let metrics = self.global_metrics().await;
        metrics.current_leader.map(|node_id| node_id.to_string())
    }

    /// Get cluster size
    pub async fn cluster_size(&self) -> Option<usize> {
        let metrics = self.global_metrics().await;
        Some(metrics.membership_config.voter_ids().count())
    }

    /// Check if this node is the leader (global consensus)
    pub async fn is_leader(&self) -> bool {
        self.is_global_leader().await
    }

    /// Check if we have an active cluster
    pub async fn has_active_cluster(&self) -> bool {
        self.cluster_manager.has_active_cluster().await
    }

    /// Discover existing clusters
    pub async fn discover_existing_clusters(
        &self,
    ) -> ConsensusResult<Vec<ClusterDiscoveryResponse>> {
        self.cluster_manager.discover_clusters().await
    }

    /// Get global metrics
    pub async fn metrics(
        &self,
    ) -> ConsensusResult<openraft::RaftMetrics<GlobalConsensusTypeConfig>> {
        Ok(self.global_metrics().await)
    }

    /// Wait for global leader election
    pub async fn wait_for_global_leader(
        &self,
        timeout: Option<std::time::Duration>,
    ) -> ConsensusResult<()> {
        let start = std::time::Instant::now();
        let timeout = timeout.unwrap_or(std::time::Duration::from_secs(30));

        loop {
            if self.has_active_cluster().await {
                return Ok(());
            }

            if start.elapsed() > timeout {
                return Err(Error::Timeout {
                    seconds: timeout.as_secs(),
                });
            }

            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }

    /// Start topology discovery
    pub async fn start_topology(&self) -> ConsensusResult<()> {
        // Refresh topology from governance to ensure we have the latest peer information
        self.topology_manager.refresh_topology().await?;
        Ok(())
    }

    // Stream routing methods

    /// Route a stream operation to the appropriate consensus group
    pub async fn route_stream_operation(
        &self,
        stream_name: &str,
        operation: GroupStreamOperation,
    ) -> ConsensusResult<GroupStreamOperationResponse> {
        let start = std::time::Instant::now();

        let result = self
            .stream_router
            .route_stream_operation(stream_name, operation)
            .await;

        // Record stream-specific metrics if available
        if let Some(metrics) = self.metrics_registry()
            && let Ok(group_id) = self.get_stream_group(stream_name).await
        {
            let duration = start.elapsed();

            // Record message metric for message operations
            match &result {
                Ok(_) => {
                    metrics
                        .streams
                        .record_message(stream_name, group_id, duration);
                }
                Err(_) => {
                    metrics.streams.record_error(stream_name, "routing_error");
                }
            }
        }

        result
    }

    /// Get the consensus group ID for a stream
    pub async fn get_stream_group(&self, stream_name: &str) -> ConsensusResult<ConsensusGroupId> {
        self.stream_router.get_stream_group(stream_name).await
    }

    /// Get routing information for a stream (for debugging)
    pub async fn get_stream_routing_info(
        &self,
        stream_name: &str,
    ) -> ConsensusResult<crate::core::engine::routing::StreamRoutingInfo> {
        self.stream_router.get_routing_info(stream_name).await
    }

    /// Get all streams assigned to a specific group
    pub async fn get_streams_in_group(
        &self,
        group_id: ConsensusGroupId,
    ) -> ConsensusResult<Vec<String>> {
        self.stream_router.get_streams_in_group(group_id).await
    }

    /// Get stream assignment summary
    pub async fn get_stream_assignments(
        &self,
    ) -> ConsensusResult<std::collections::HashMap<ConsensusGroupId, Vec<String>>> {
        self.stream_router.get_stream_assignments().await
    }

    /// Check if a stream exists
    pub async fn stream_exists(&self, stream_name: &str) -> bool {
        self.global_layer
            .global_state()
            .get_stream(stream_name)
            .await
            .is_some()
    }

    /// Publish to a stream (routes to appropriate group)
    pub async fn publish_to_stream(
        &self,
        stream_name: &str,
        data: bytes::Bytes,
        metadata: Option<std::collections::HashMap<String, String>>,
    ) -> ConsensusResult<u64> {
        let operation =
            GroupStreamOperation::publish_to_stream(stream_name.to_string(), data, metadata);

        let response = self.route_stream_operation(stream_name, operation).await?;

        if response.is_success() {
            Ok(response.sequence().unwrap_or(0))
        } else {
            Err(Error::ConsensusFailed(
                response
                    .error()
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| "Unknown error".to_string()),
            ))
        }
    }

    /// Delete a stream (global operation)
    pub async fn delete_stream(
        &self,
        stream_name: &str,
    ) -> ConsensusResult<GlobalOperationResponse> {
        let response = self
            .coordinator
            .delete_stream(
                self.global_layer.as_ref(),
                &*self.groups_layer.read().await,
                stream_name.to_string(),
            )
            .await?;

        // If successful, clean up in consensus group
        if response.is_success()
            && let Some(stream_info) = self
                .global_layer
                .global_state()
                .get_stream(stream_name)
                .await
        {
            // Check if this node is part of the target group
            let group_info = self
                .global_layer
                .global_state()
                .get_group(stream_info.group_id)
                .await;
            if let Some(info) = group_info
                && info.members.contains(&self.node_id)
            {
                // Delete from consensus group using migration operation
                let operation = GroupStreamOperation::Migration(
                    crate::operations::local_stream_ops::MigrationOperation::RemoveStream {
                        stream_name: stream_name.to_string(),
                    },
                );
                if let Err(e) = self
                    .process_local_operation(stream_info.group_id, operation)
                    .await
                {
                    error!(
                        "Failed to delete stream {} from consensus group {:?}: {}",
                        stream_name, stream_info.group_id, e
                    );
                }
            }
        }

        Ok(response)
    }

    /// Direct read from a stream (bypasses consensus)
    pub async fn direct_read_from_stream(
        &self,
        stream_name: &str,
        sequence: u64,
    ) -> ConsensusResult<Option<bytes::Bytes>> {
        // Get the stream's group
        let stream_info = self
            .global_state()
            .get_stream(stream_name)
            .await
            .ok_or_else(|| Error::not_found(format!("Stream '{stream_name}' not found")))?;

        // Get the stream manager for this group
        let stream_manager = self.get_stream_manager(stream_info.group_id).await?;

        // Direct read using batch read with single sequence
        let messages = stream_manager
            .read_messages_batch(stream_name, &[sequence])
            .await
            .map_err(|e| Error::storage(format!("Failed to read message: {e}")))?;

        // Get the first (and only) message from the batch result
        match messages.into_iter().next() {
            Some(Some(msg)) => Ok(Some(msg.data)),
            Some(None) => Ok(None),
            None => Ok(None),
        }
    }

    // Cluster initialization methods

    /// Initialize as a single-node cluster
    pub async fn initialize_single_node_cluster(
        &self,
        reason: crate::network::InitiatorReason,
    ) -> ConsensusResult<()> {
        let global_layer = self.global_layer.clone();

        // Initialize the cluster
        self.lifecycle_manager
            .initialize_single_node_cluster(reason, |request| {
                let global_layer = global_layer.clone();
                Box::pin(async move { global_layer.submit_request(request.operation).await })
            })
            .await?;

        // Start monitoring if enabled
        // Note: Since MonitoringCoordinator requires &mut self for start(),
        // we should start it during initialization rather than here.
        // The monitoring tasks will be started when the coordinator is created.

        Ok(())
    }

    /// Initialize a multi-node cluster by starting as single-node and expecting others to join
    pub async fn initialize_multi_node_cluster(
        &self,
        reason: crate::network::InitiatorReason,
        peers: Vec<proven_topology::Node>,
    ) -> ConsensusResult<()> {
        let expected_peers: Vec<NodeId> = peers.iter().map(|n| n.node_id().clone()).collect();
        let global_layer = self.global_layer.clone();

        // Initialize the cluster
        self.lifecycle_manager
            .initialize_multi_node_cluster(reason, expected_peers, |request| {
                let global_layer = global_layer.clone();
                Box::pin(async move { global_layer.submit_request(request.operation).await })
            })
            .await?;

        // Start monitoring if enabled
        // Note: Since MonitoringCoordinator requires &mut self for start(),
        // we should start it during initialization rather than here.
        // The monitoring tasks will be started when the coordinator is created.

        Ok(())
    }

    /// Join an existing cluster via Raft membership change (learner -> voter)
    pub async fn join_existing_cluster_via_raft(
        &self,
        existing_cluster: &ClusterDiscoveryResponse,
    ) -> ConsensusResult<()> {
        use crate::network::{
            ClusterState,
            messages::{ClusterJoinRequest, GlobalMessage, Message},
        };

        info!(
            "Attempting to join existing cluster led by {:?}",
            existing_cluster.current_leader
        );

        let leader_id = existing_cluster
            .current_leader
            .as_ref()
            .ok_or_else(|| Error::InvalidState("No leader in discovered cluster".to_string()))?;

        // Update state to waiting
        {
            let cluster_state = self.cluster_manager.cluster_state();
            let mut state = cluster_state.write().await;
            *state = ClusterState::WaitingToJoin {
                requested_at: std::time::Instant::now(),
                leader_id: leader_id.clone(),
            };
        }

        // Get our node info from topology
        let my_node = self
            .topology_manager
            .get_node(&self.node_id)
            .await
            .ok_or_else(|| Error::InvalidState("Node not found in topology".to_string()))?;

        // Send join request to the leader
        let join_request = ClusterJoinRequest {
            requester_id: self.node_id.clone(),
            requester_node: my_node,
        };

        let message = Message::Global(Box::new(GlobalMessage::ClusterJoinRequest(join_request)));

        // Send the join request
        self.send(leader_id.clone(), message).await?;

        // Wait for join response with timeout
        let timeout = std::time::Duration::from_secs(30);
        let start = std::time::Instant::now();

        // TODO: We need to implement a proper response tracking mechanism
        // For now, we'll just wait and check our state periodically
        loop {
            // Check if we've been added to the cluster by monitoring metrics
            let metrics = self.global_metrics().await;
            if metrics
                .membership_config
                .nodes()
                .any(|(id, _)| id == &self.node_id)
            {
                // We've been added!
                let cluster_size = metrics.membership_config.nodes().count();

                // Update state to joined
                let cluster_state = self.cluster_manager.cluster_state();
                let mut state = cluster_state.write().await;
                *state = ClusterState::Joined {
                    joined_at: std::time::Instant::now(),
                    cluster_size,
                };

                info!("Successfully joined cluster (size: {})", cluster_size);

                // Now we need to discover and join any consensus groups
                // that we should be part of
                self.discover_and_join_groups().await?;

                return Ok(());
            }

            if start.elapsed() > timeout {
                return Err(Error::Timeout {
                    seconds: timeout.as_secs(),
                });
            }

            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
    }

    /// Discover and join consensus groups after joining global cluster
    async fn discover_and_join_groups(&self) -> ConsensusResult<()> {
        // Get all groups from global state
        let groups = self.global_layer.global_state().get_all_groups().await;

        for group_info in groups {
            if group_info.members.contains(&self.node_id) {
                info!(
                    "Should be part of consensus group {:?}, creating it",
                    group_info.id
                );

                // Create the consensus group
                self.groups_layer
                    .read()
                    .await
                    .groups_manager()
                    .write()
                    .await
                    .create_group(group_info.id, group_info.members.clone())
                    .await?;
            }
        }

        Ok(())
    }

    /// Get stream information
    pub async fn get_stream_info(
        &self,
        stream_name: &str,
    ) -> ConsensusResult<Option<crate::core::global::StreamInfo>> {
        Ok(self
            .global_layer
            .global_state()
            .get_stream(stream_name)
            .await)
    }

    /// Get consensus group information
    pub async fn get_group_info(
        &self,
        group_id: ConsensusGroupId,
    ) -> ConsensusResult<Option<crate::core::global::ConsensusGroupInfo>> {
        Ok(self.global_layer.global_state().get_group(group_id).await)
    }

    /// Get the number of streams in a group
    pub async fn get_group_stream_count(
        &self,
        group_id: ConsensusGroupId,
    ) -> ConsensusResult<usize> {
        let streams = self.get_streams_in_group(group_id).await?;
        Ok(streams.len())
    }

    // ============ Allocator Methods ============

    /// Allocate groups for a new node using the allocator
    pub async fn allocate_node_to_groups(
        &self,
        node_id: &NodeId,
    ) -> ConsensusResult<Vec<ConsensusGroupId>> {
        self.groups_layer
            .read()
            .await
            .groups_manager()
            .read()
            .await
            .allocate_node_to_groups(node_id)
            .await
    }

    /// Remove a node from all its allocated groups
    pub async fn deallocate_node_from_groups(
        &self,
        node_id: &NodeId,
    ) -> ConsensusResult<Vec<ConsensusGroupId>> {
        self.groups_layer
            .read()
            .await
            .groups_manager()
            .read()
            .await
            .deallocate_node_from_groups(node_id)
            .await
    }

    /// Get current group allocations
    pub async fn get_group_allocations(
        &self,
    ) -> ConsensusResult<crate::core::group::group_allocator::GroupAllocations> {
        self.groups_layer
            .read()
            .await
            .groups_manager()
            .read()
            .await
            .get_allocations()
            .await
    }

    /// Check if rebalancing is needed
    pub async fn needs_rebalancing(&self) -> ConsensusResult<bool> {
        self.groups_layer
            .read()
            .await
            .groups_manager()
            .read()
            .await
            .needs_rebalancing()
            .await
    }

    /// Generate a rebalancing plan
    pub async fn generate_rebalancing_plan(
        &self,
    ) -> ConsensusResult<Option<crate::core::group::group_allocator::RebalancingPlan>> {
        self.groups_layer
            .read()
            .await
            .groups_manager()
            .read()
            .await
            .generate_rebalancing_plan()
            .await
    }

    /// Start migrating a node between groups
    pub async fn start_node_migration(
        &self,
        node_id: &NodeId,
        from_group: ConsensusGroupId,
        to_group: ConsensusGroupId,
    ) -> ConsensusResult<()> {
        self.groups_layer
            .read()
            .await
            .groups_manager()
            .read()
            .await
            .start_node_migration(node_id, from_group, to_group)
            .await
    }

    /// Complete a node migration
    pub async fn complete_node_migration(&self, node_id: &NodeId) -> ConsensusResult<()> {
        self.groups_layer
            .read()
            .await
            .groups_manager()
            .read()
            .await
            .complete_node_migration(node_id)
            .await
    }

    /// Check if a node is currently migrating
    pub async fn is_node_migrating(&self, node_id: &NodeId) -> ConsensusResult<bool> {
        self.groups_layer
            .read()
            .await
            .groups_manager()
            .read()
            .await
            .is_node_migrating(node_id)
            .await
    }

    /// Get recommendations for group operations
    pub async fn get_group_recommendations(
        &self,
    ) -> ConsensusResult<Vec<crate::core::group::group_allocator::GroupRecommendation>> {
        self.groups_layer
            .read()
            .await
            .groups_manager()
            .read()
            .await
            .get_group_recommendations()
            .await
    }

    // ============ Stream Migration Methods ============

    /// Start a stream migration between groups
    pub async fn start_stream_migration(
        &self,
        stream_name: String,
        source_group: ConsensusGroupId,
        target_group: ConsensusGroupId,
    ) -> ConsensusResult<()> {
        self.groups_layer
            .read()
            .await
            .groups_manager()
            .read()
            .await
            .start_stream_migration(stream_name, source_group, target_group)
            .await
    }

    /// Execute the next step of a stream migration
    pub async fn execute_stream_migration_step(&self, stream_name: &str) -> ConsensusResult<()> {
        self.groups_layer
            .read()
            .await
            .groups_manager()
            .read()
            .await
            .execute_stream_migration_step(stream_name)
            .await
    }

    /// Get stream migration progress
    pub async fn get_stream_migration_progress(
        &self,
        stream_name: &str,
    ) -> ConsensusResult<Option<crate::core::group::migration::StreamMigrationProgress>> {
        self.groups_layer
            .read()
            .await
            .groups_manager()
            .read()
            .await
            .get_stream_migration_progress(stream_name)
            .await
    }

    /// Get all active stream migrations
    pub async fn get_active_stream_migrations(
        &self,
    ) -> ConsensusResult<Vec<crate::core::group::migration::ActiveStreamMigration>> {
        self.groups_layer
            .read()
            .await
            .groups_manager()
            .read()
            .await
            .get_active_stream_migrations()
            .await
    }

    /// Cancel a stream migration
    pub async fn cancel_stream_migration(&self, stream_name: &str) -> ConsensusResult<()> {
        self.groups_layer
            .read()
            .await
            .groups_manager()
            .read()
            .await
            .cancel_stream_migration(stream_name)
            .await
    }

    // ============ Monitoring Methods ============

    /// Get system-wide monitoring view
    pub async fn get_system_view(
        &self,
    ) -> ConsensusResult<Arc<crate::core::monitoring::SystemView<T, G>>> {
        let monitoring = self
            .monitoring_coordinator
            .as_ref()
            .ok_or_else(|| Error::InvalidOperation("Monitoring not enabled".to_string()))?;

        monitoring.get_system_view().await
    }

    /// Get monitoring view for a specific stream
    pub async fn get_stream_view(
        &self,
        stream_name: &str,
    ) -> ConsensusResult<Option<crate::core::monitoring::StreamHealth>> {
        let monitoring = self
            .monitoring_coordinator
            .as_ref()
            .ok_or_else(|| Error::InvalidOperation("Monitoring not enabled".to_string()))?;

        monitoring.get_stream_view(stream_name).await
    }

    /// Get monitoring view for a specific group
    pub async fn get_group_view(
        &self,
        group_id: ConsensusGroupId,
    ) -> ConsensusResult<Option<crate::core::monitoring::GroupHealth>> {
        let monitoring = self
            .monitoring_coordinator
            .as_ref()
            .ok_or_else(|| Error::InvalidOperation("Monitoring not enabled".to_string()))?;

        monitoring.get_group_view(group_id).await
    }

    /// Get monitoring view for a specific node
    pub async fn get_node_view(
        &self,
        node_id: &NodeId,
    ) -> ConsensusResult<Option<crate::core::monitoring::NodeHealth>> {
        let monitoring = self
            .monitoring_coordinator
            .as_ref()
            .ok_or_else(|| Error::InvalidOperation("Monitoring not enabled".to_string()))?;

        monitoring.get_node_view(node_id).await
    }

    /// Get monitoring views for all streams
    pub async fn get_all_stream_views(
        &self,
    ) -> ConsensusResult<Vec<crate::core::monitoring::StreamHealth>> {
        let monitoring = self
            .monitoring_coordinator
            .as_ref()
            .ok_or_else(|| Error::InvalidOperation("Monitoring not enabled".to_string()))?;

        monitoring.get_all_stream_views().await
    }

    /// Get monitoring views for all groups
    pub async fn get_all_group_views(
        &self,
    ) -> ConsensusResult<Vec<crate::core::monitoring::GroupHealth>> {
        let monitoring = self
            .monitoring_coordinator
            .as_ref()
            .ok_or_else(|| Error::InvalidOperation("Monitoring not enabled".to_string()))?;

        monitoring.get_all_group_views().await
    }

    /// Get monitoring views for all nodes
    pub async fn get_all_node_views(
        &self,
    ) -> ConsensusResult<Vec<crate::core::monitoring::NodeHealth>> {
        let monitoring = self
            .monitoring_coordinator
            .as_ref()
            .ok_or_else(|| Error::InvalidOperation("Monitoring not enabled".to_string()))?;

        monitoring.get_all_node_views().await
    }

    /// Get system health status
    pub async fn get_system_health(
        &self,
    ) -> ConsensusResult<crate::core::monitoring::HealthReport> {
        let monitoring = self
            .monitoring_coordinator
            .as_ref()
            .ok_or_else(|| Error::InvalidOperation("Monitoring not enabled".to_string()))?;

        monitoring.get_system_health().await
    }

    /// Trigger a manual health check
    pub async fn trigger_health_check(&self) -> ConsensusResult<()> {
        let monitoring = self
            .monitoring_coordinator
            .as_ref()
            .ok_or_else(|| Error::InvalidOperation("Monitoring not enabled".to_string()))?;

        monitoring.trigger_health_check().await
    }

    /// Get metrics registry if available
    pub fn metrics_registry(&self) -> Option<Arc<crate::core::monitoring::MetricsRegistry>> {
        self.monitoring_coordinator
            .as_ref()
            .and_then(|m| m.metrics_collector())
            .map(|c| c.registry().clone())
    }

    /// Get component health status
    pub async fn check_health(&self) -> crate::core::engine::lifecycle::ComponentHealth {
        self.lifecycle_manager.check_health().await
    }

    /// Perform graceful shutdown
    pub async fn graceful_shutdown(&self) -> ConsensusResult<()> {
        // Stop monitoring if enabled
        // Note: The monitoring coordinator will stop automatically when dropped
        // due to the shutdown signal in its destructor.

        // Shutdown other components
        self.lifecycle_manager.graceful_shutdown().await
    }
}

#[async_trait::async_trait]
impl<T, G> GlobalOperationHandler for Engine<T, G>
where
    T: Transport,
    G: Governance,
{
    async fn submit_global_request(
        &self,
        operation: GlobalOperation,
    ) -> ConsensusResult<GlobalOperationResponse> {
        self.submit_global_request(operation).await
    }

    async fn create_stream(
        &self,
        name: String,
        config: StreamConfig,
        group_id: ConsensusGroupId,
    ) -> ConsensusResult<GlobalOperationResponse> {
        self.create_stream(name, config, group_id).await
    }

    async fn delete_stream(&self, stream_name: &str) -> ConsensusResult<GlobalOperationResponse> {
        self.delete_stream(stream_name).await
    }
}

// Additional methods moved from Consensus

impl<T, G> Engine<T, G>
where
    T: Transport,
    G: Governance,
{
    // ==================== Lifecycle Management ====================

    /// Start the consensus system and handle cluster discovery
    pub async fn start(self: Arc<Self>) -> ConsensusResult<()> {
        info!("Starting proven engine for node {}", self.node_id);

        // Initialize transport and then complete startup with discovery
        self.clone().initialize_transport().await?;
        self.complete_startup().await
    }

    /// Initialize just the transport layer
    /// This is useful for tests that need to set up HTTP servers before discovery
    pub async fn initialize_transport(self: Arc<Self>) -> ConsensusResult<()> {
        // Start internal components first (network, handlers, etc.)
        self.clone().start_internal().await?;

        // Start topology manager to refresh from governance
        info!("Starting topology manager...");
        self.start_topology().await?;
        info!("Topology manager started");

        Ok(())
    }

    /// Complete startup after transport initialization
    /// Should be called after initialize_transport()
    pub async fn complete_startup(&self) -> ConsensusResult<()> {
        // Get all peers from topology
        let all_peers = self.topology_manager.get_all_peers().await;

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

        // Start monitoring if enabled
        if let Some(monitoring) = &self.monitoring_coordinator {
            let mut monitoring_mut = Arc::clone(monitoring);
            if let Some(coordinator) = Arc::get_mut(&mut monitoring_mut) {
                coordinator.start().await?;
            }
        }

        Ok(())
    }

    /// Discover existing clusters and join or become initiator
    async fn discover_and_join_cluster(
        &self,
        all_peers: Vec<proven_topology::Node>,
    ) -> ConsensusResult<()> {
        // Set state to discovering
        {
            let cluster_state = self.cluster_manager.cluster_state();
            let mut state = cluster_state.write().await;
            *state = ClusterState::Discovering;
        }

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

            // Perform discovery
            let responses = self.discover_existing_clusters().await?;

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
                let responses = self.discover_existing_clusters().await?;

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

    /// Register message handlers with NetworkManager
    async fn register_handlers(self: Arc<Self>) -> ConsensusResult<()> {
        use crate::network::messages::{ClusterDiscoveryRequest, ClusterJoinRequest};
        use proven_network::HandlerBuilderExt;

        // Register cluster discovery handler using the new ergonomic API
        self.network_manager
            .handle::<ClusterDiscoveryRequest>()
            .with_state(self.clone())
            .async_handler_with_error(|engine, sender_id, req| async move {
                engine.handle_cluster_discovery(sender_id, req).await
            })
            .map_err(|e| {
                Error::Network(crate::error::NetworkError::SendFailed {
                    peer: self.node_id.clone(),
                    reason: format!("Failed to register handler: {e}"),
                })
            })?;

        // Register cluster join handler
        self.network_manager
            .handle::<ClusterJoinRequest>()
            .with_state(self.clone())
            .async_handler_with_error(|engine, sender_id, req| async move {
                engine.handle_cluster_join(sender_id, req).await
            })
            .map_err(|e| {
                Error::Network(crate::error::NetworkError::SendFailed {
                    peer: self.node_id.clone(),
                    reason: format!("Failed to register handler: {e}"),
                })
            })?;

        // TODO: Register more handlers for other message types

        Ok(())
    }

    /// Internal startup logic
    pub async fn start_internal(self: Arc<Self>) -> ConsensusResult<()> {
        info!("Starting internal components for node {}", self.node_id);

        // Register handlers
        self.register_handlers().await?;

        // The new NetworkManager doesn't have a start_network method
        // Tasks are started automatically when NetworkManager is created

        // Components don't need explicit start calls as they're already initialized

        Ok(())
    }

    /// Shutdown the consensus system
    pub async fn shutdown(&self) -> ConsensusResult<()> {
        info!("Shutting down proven engine");

        // Comprehensive shutdown of NetworkManager (includes Raft, transport, topology)
        if let Err(e) = self.network_manager.shutdown().await {
            error!("Error during network manager shutdown: {}", e);
        }

        // Stop monitoring
        if let Some(monitoring) = &self.monitoring_coordinator {
            let mut monitoring_mut = Arc::clone(monitoring);
            if let Some(coordinator) = Arc::get_mut(&mut monitoring_mut) {
                coordinator.stop().await?;
            }
        }

        // Cleanup task handles
        let mut handles = self.task_handles.lock().unwrap();
        for handle in handles.drain(..) {
            handle.abort();
        }

        Ok(())
    }

    // ==================== Network Access ====================

    /// Get a reference to the network manager (for advanced use)
    pub fn network_manager(&self) -> Arc<NetworkManager<T, G>> {
        self.network_manager.clone()
    }

    /// Get reference to the PubSub manager
    pub fn pubsub_manager(&self) -> Arc<PubSubManager<T, G>>
    where
        T: Transport,
        G: Governance,
    {
        self.pubsub_manager.clone()
    }

    /// Check if this consensus instance supports HTTP integration
    pub fn supports_http_integration(&self) -> bool {
        // TODO: Check if transport supports HTTP integration
        false
    }

    /// Create HTTP router for WebSocket transport
    pub fn create_router(&self) -> ConsensusResult<Router> {
        // TODO: Implement router creation based on transport type
        Err(Error::InvalidOperation(
            "HTTP integration not supported".to_string(),
        ))
    }

    /// Get connected peers
    pub async fn get_connected_peers(&self) -> ConsensusResult<Vec<(NodeId, bool, SystemTime)>> {
        // TODO: Track connected peers in NetworkManager
        Ok(Vec::new())
    }

    // ==================== PubSub Operations ====================

    /// Publish a message to a PubSub subject
    pub async fn pubsub_publish(&self, subject: &str, payload: Bytes) -> ConsensusResult<()> {
        self.pubsub_manager
            .publish(subject, payload)
            .await
            .map_err(|e| Error::InvalidMessage(format!("Failed to publish: {e}")))
    }

    /// Subscribe to a PubSub subject
    pub async fn pubsub_subscribe(
        &self,
        subject: &str,
    ) -> ConsensusResult<crate::pubsub::Subscription<T, G>> {
        self.pubsub_manager
            .subscribe(subject)
            .await
            .map_err(|e| Error::InvalidMessage(format!("Failed to subscribe: {e}")))
    }

    /// Send a request and wait for response
    pub async fn pubsub_request(
        &self,
        subject: &str,
        payload: Bytes,
        timeout: Option<Duration>,
    ) -> ConsensusResult<Bytes> {
        self.pubsub_manager
            .request(subject, payload, timeout.unwrap_or(Duration::from_secs(30)))
            .await
            .map_err(|e| Error::InvalidMessage(format!("Request failed: {e}")))
    }

    // ==================== Cluster State ====================

    /// Wait for the cluster to be ready (either Initiator or Joined state)
    pub async fn wait_for_leader(&self, timeout: Option<Duration>) -> ConsensusResult<()> {
        // Use the global leader wait implementation
        self.wait_for_global_leader(timeout).await
    }

    /// Check if cluster discovery is in progress
    pub async fn is_cluster_discovering(&self) -> bool {
        matches!(self.cluster_state().await, ClusterState::Discovering)
    }

    // ==================== Helper Methods ====================

    /// Get cluster join retry config
    fn cluster_join_retry_config(&self) -> Option<crate::config::cluster::ClusterJoinRetryConfig> {
        // This would need to be stored or passed in
        None
    }

    /// Create message handler for network manager
    fn create_message_handler(
        self: Arc<Self>,
    ) -> Arc<dyn Fn(NodeId, Message, Option<Uuid>) + Send + Sync> {
        Arc::new(move |sender_id, message, correlation_id| {
            let engine = self.clone();
            tokio::spawn(async move {
                if let Err(e) = engine
                    .handle_network_message(sender_id, message, correlation_id)
                    .await
                {
                    error!("Error handling message: {}", e);
                }
            });
        })
    }
}

// ==================== Bootable Implementation ====================

#[async_trait]
impl<T, G> Bootable for Engine<T, G>
where
    T: Transport,
    G: Governance,
{
    fn bootable_name(&self) -> &'static str {
        "proven-engine"
    }

    async fn start(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // The Engine needs to be wrapped in Arc for its start method
        // This is a bit awkward, but necessary because Engine::start takes Arc<Self>
        // In practice, Engine should always be used through Arc anyway
        warn!(
            "Bootable::start called on Engine - this requires the Engine to already be in an Arc"
        );
        warn!("Consider using Engine::start directly with Arc<Engine> instead");

        // For now, return an error indicating the proper usage
        Err(Box::new(std::io::Error::other(
            "Engine must be wrapped in Arc to start. Use Arc<Engine>::start() directly.",
        )) as Box<dyn std::error::Error + Send + Sync>)
    }

    async fn shutdown(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Engine::shutdown(self).await.map_err(|e| {
            Box::new(std::io::Error::other(format!(
                "Failed to shutdown proven engine: {e}"
            ))) as Box<dyn std::error::Error + Send + Sync>
        })
    }

    async fn wait(&self) {
        // Wait for background tasks to complete
        // For now, just wait indefinitely
        // In a full implementation, this would wait for all background tasks
        tokio::time::sleep(tokio::time::Duration::from_secs(u64::MAX)).await;
    }
}

// ==================== Debug Implementation ====================

impl<T, G> std::fmt::Debug for Engine<T, G>
where
    T: Transport,
    G: Governance,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Engine")
            .field("node_id", &self.node_id)
            .field("cluster_state", &"<async>")
            .finish()
    }
}

#[cfg(test)]
mod tests;
