//! Membership service implementation

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use proven_attestation::Attestor;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use proven_network::NetworkManager;
use proven_storage::StorageAdaptor;
use proven_topology::{Node, NodeId, TopologyAdaptor, TopologyManager};
use proven_transport::Transport;

use crate::error::{ConsensusResult, Error, ErrorKind};
use crate::foundation::ServiceLifecycle;
use crate::foundation::events::EventBus;

use super::config::MembershipConfig;
use super::discovery::{DiscoveryManager, DiscoveryResult};
use super::events::MembershipEvent;
use super::handlers;
use super::health::HealthMonitor;
use super::messages::{
    self, AcceptProposalRequest, AcceptProposalResponse, GracefulShutdownRequest,
    MembershipMessage, MembershipResponse, ProposeClusterRequest, ProposeClusterResponse,
};
use super::types::{HealthInfo, MembershipView, NodeMembership};
use super::utils::now_timestamp;
use crate::foundation::{ClusterFormationState, NodeRole, NodeStatus};

/// Service state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ServiceState {
    NotStarted,
    Starting,
    Running,
    Stopping,
    Stopped,
}

/// Membership service for unified cluster membership management
pub struct MembershipService<T, G, A, S>
where
    T: Transport,
    G: TopologyAdaptor,
    A: Attestor,
    S: StorageAdaptor,
{
    config: MembershipConfig,
    node_id: NodeId,
    node_info: Node,
    network_manager: Arc<NetworkManager<T, G, A>>,
    topology_manager: Arc<TopologyManager<G>>,
    event_bus: Arc<EventBus>,

    /// Current membership view
    membership_view: Arc<RwLock<MembershipView>>,

    /// Discovery manager
    discovery_manager: Arc<DiscoveryManager<T, G, A>>,

    /// Health monitor
    health_monitor: Arc<HealthMonitor<T, G, A>>,

    /// Service state
    state: Arc<RwLock<ServiceState>>,

    /// Task tracker
    task_tracker: TaskTracker,

    /// Cancellation token
    cancellation_token: CancellationToken,

    /// Phantom data for storage
    _phantom: std::marker::PhantomData<S>,
}

impl<T, G, A, S> MembershipService<T, G, A, S>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    S: StorageAdaptor + 'static,
    A: Attestor + 'static,
{
    /// Create new membership service
    pub fn new(
        config: MembershipConfig,
        node_id: NodeId,
        node_info: Node,
        network_manager: Arc<NetworkManager<T, G, A>>,
        topology_manager: Arc<TopologyManager<G>>,
        event_bus: Arc<EventBus>,
    ) -> Self {
        let discovery_manager = Arc::new(DiscoveryManager::new(
            node_id,
            network_manager.clone(),
            topology_manager.clone(),
            config.discovery.clone(),
        ));

        let health_monitor = Arc::new(HealthMonitor::new(
            node_id,
            network_manager.clone(),
            config.health.clone(),
        ));

        Self {
            config,
            node_id,
            node_info,
            network_manager,
            topology_manager,
            event_bus,
            membership_view: Arc::new(RwLock::new(MembershipView::new())),
            discovery_manager,
            health_monitor,
            state: Arc::new(RwLock::new(ServiceState::NotStarted)),
            task_tracker: TaskTracker::new(),
            cancellation_token: CancellationToken::new(),
            _phantom: std::marker::PhantomData,
        }
    }

    /// Get the node ID
    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Get node information
    pub fn node_info(&self) -> &Node {
        &self.node_info
    }

    /// Get topology manager
    pub fn topology_manager(&self) -> &Arc<TopologyManager<G>> {
        &self.topology_manager
    }

    /// Get current membership view
    pub async fn get_membership_view(&self) -> MembershipView {
        self.membership_view.read().await.clone()
    }

    /// Get online members
    pub async fn get_online_members(&self) -> Vec<(NodeId, Node)> {
        let view = self.membership_view.read().await;
        view.online_nodes()
            .into_iter()
            .map(|m| (m.node_id, m.node_info.clone()))
            .collect()
    }

    /// Handle discovery and formation
    async fn handle_discovery_and_formation(&self) -> ConsensusResult<()> {
        info!("Starting discovery and formation process");

        // Update state
        {
            let mut view = self.membership_view.write().await;
            view.cluster_state = ClusterFormationState::Discovering {
                round: 0,
                started_at_ms: now_timestamp(),
            };
        }

        // Execute discovery
        let discovery_result = self.discovery_manager.discover_cluster().await?;

        match discovery_result {
            DiscoveryResult::SingleNode => {
                info!("Forming single-node cluster");
                self.form_single_node_cluster().await?;
            }
            DiscoveryResult::ShouldCoordinate { online_peers } => {
                info!(
                    "Coordinating cluster formation with {} peers",
                    online_peers.len()
                );
                self.coordinate_cluster_formation(online_peers).await?;
            }
            DiscoveryResult::WaitForCoordinator { coordinator, .. } => {
                info!("Waiting for coordinator {} to form cluster", coordinator);
                self.wait_for_formation(coordinator).await?;
            }
            DiscoveryResult::JoinExistingCluster { leader, members } => {
                info!("Joining existing cluster with leader {}", leader);
                self.join_existing_cluster(leader, members).await?;
            }
            DiscoveryResult::JoinFormingCluster {
                coordinator,
                formation_id,
            } => {
                info!("Joining forming cluster coordinated by {}", coordinator);
                self.join_forming_cluster(coordinator, formation_id).await?;
            }
        }

        Ok(())
    }

    /// Form a single-node cluster
    async fn form_single_node_cluster(&self) -> ConsensusResult<()> {
        let mut view = self.membership_view.write().await;

        // Add ourselves to membership as the leader
        let mut roles = HashSet::new();
        roles.insert(NodeRole::GlobalConsensusLeader);

        view.nodes.insert(
            self.node_id,
            NodeMembership {
                node_id: self.node_id,
                node_info: self.node_info.clone(),
                status: NodeStatus::Online,
                health: HealthInfo::new(),
                roles,
                last_seen_ms: now_timestamp(),
            },
        );

        view.cluster_state = ClusterFormationState::Active {
            members: vec![self.node_id],
            formed_at_ms: now_timestamp(),
        };

        drop(view);

        // Publish event
        self.publish_event(MembershipEvent::ClusterFormed {
            members: vec![self.node_id],
            coordinator: self.node_id,
        })
        .await;

        // Initialize global consensus
        use crate::services::global_consensus::commands::InitializeGlobalConsensus;
        use std::collections::BTreeMap;

        // Get node info for the member
        let mut members = BTreeMap::new();
        if let Some(node_info) = self.topology_manager.get_node(&self.node_id).await {
            members.insert(self.node_id, node_info);
        } else {
            // Use our own node info if not in topology yet
            members.insert(self.node_id, self.node_info.clone());
        }

        self.event_bus
            .request(InitializeGlobalConsensus { members })
            .await
            .map_err(|e| {
                Error::with_context(
                    ErrorKind::Service,
                    format!("Failed to initialize global consensus: {e}"),
                )
            })?;

        info!("Successfully initialized global consensus for single-node cluster");

        Ok(())
    }

    /// Coordinate cluster formation
    async fn coordinate_cluster_formation(
        &self,
        online_peers: Vec<(NodeId, Node)>,
    ) -> ConsensusResult<()> {
        let formation_id = Uuid::new_v4();
        let mut proposed_members = online_peers.clone();
        proposed_members.push((self.node_id, self.node_info.clone()));

        // Update state
        {
            let mut view = self.membership_view.write().await;
            view.cluster_state = ClusterFormationState::Forming {
                coordinator: self.node_id,
                formation_id,
                proposed_members: proposed_members.iter().map(|(id, _)| *id).collect(),
            };
        }

        // Send proposals to all members
        let request = MembershipMessage::ProposeCluster(ProposeClusterRequest {
            coordinator: self.node_id,
            formation_id,
            proposed_members: proposed_members.clone(),
            timeout_ms: self.config.formation.formation_timeout.as_millis() as u64,
        });

        let mut acceptances = 0;
        for (peer_id, _) in &online_peers {
            match self
                .network_manager
                .request_with_timeout(
                    *peer_id,
                    request.clone(),
                    self.config.discovery.node_request_timeout,
                )
                .await
            {
                Ok(MembershipResponse::ProposeCluster(ProposeClusterResponse {
                    accepted: true,
                    ..
                })) => {
                    acceptances += 1;
                }
                Ok(MembershipResponse::ProposeCluster(ProposeClusterResponse {
                    accepted: false,
                    rejection_reason,
                })) => {
                    warn!("Node {} rejected proposal: {:?}", peer_id, rejection_reason);
                }
                Err(e) => {
                    warn!("Failed to propose to {}: {}", peer_id, e);
                }
                _ => {}
            }
        }

        // Check if we have enough acceptances
        if acceptances >= self.config.formation.min_cluster_size.saturating_sub(1) {
            info!("Cluster formation accepted by {} nodes", acceptances);
            self.finalize_cluster_formation(proposed_members).await?;
        } else {
            warn!(
                "Insufficient acceptances: {} (needed {})",
                acceptances,
                self.config.formation.min_cluster_size.saturating_sub(1)
            );
            return Err(Error::with_context(
                ErrorKind::ClusterFormation,
                "Insufficient nodes accepted cluster proposal",
            ));
        }

        Ok(())
    }

    /// Finalize cluster formation
    async fn finalize_cluster_formation(
        &self,
        members: Vec<(NodeId, Node)>,
    ) -> ConsensusResult<()> {
        let mut view = self.membership_view.write().await;

        // Add all members
        for (node_id, node_info) in &members {
            let mut roles = HashSet::new();

            // The coordinator (us) becomes the initial leader
            if node_id == &self.node_id {
                roles.insert(NodeRole::GlobalConsensusLeader);
            }

            view.nodes.insert(
                *node_id,
                NodeMembership {
                    node_id: *node_id,
                    node_info: node_info.clone(),
                    status: if node_id == &self.node_id {
                        NodeStatus::Online
                    } else {
                        NodeStatus::Starting
                    },
                    health: HealthInfo::new(),
                    roles,
                    last_seen_ms: now_timestamp(),
                },
            );
        }

        let member_ids: Vec<NodeId> = members.iter().map(|(id, _)| *id).collect();
        view.cluster_state = ClusterFormationState::Active {
            members: member_ids.clone(),
            formed_at_ms: now_timestamp(),
        };

        drop(view);

        // Publish event
        self.publish_event(MembershipEvent::ClusterFormed {
            members: member_ids.clone(),
            coordinator: self.node_id,
        })
        .await;

        // Initialize global consensus
        use crate::services::global_consensus::commands::InitializeGlobalConsensus;
        use std::collections::BTreeMap;

        // Get node info for all members
        let mut members_map = BTreeMap::new();
        for node_id in member_ids {
            if let Some(node_info) = self.topology_manager.get_node(&node_id).await {
                members_map.insert(node_id, node_info);
            } else if node_id == self.node_id {
                // Use our own node info if not in topology yet
                members_map.insert(node_id, self.node_info.clone());
            } else {
                return Err(Error::with_context(
                    ErrorKind::NotFound,
                    format!("Could not find node info for member {node_id}"),
                ));
            }
        }

        self.event_bus
            .request(InitializeGlobalConsensus {
                members: members_map,
            })
            .await
            .map_err(|e| {
                Error::with_context(
                    ErrorKind::Service,
                    format!("Failed to initialize global consensus: {e}"),
                )
            })?;

        info!("Successfully initialized global consensus for multi-node cluster");

        Ok(())
    }

    /// Wait for cluster formation
    async fn wait_for_formation(&self, coordinator: NodeId) -> ConsensusResult<()> {
        let start = std::time::Instant::now();
        let timeout = self.config.formation.formation_timeout;

        while start.elapsed() < timeout {
            // Check if we received a proposal
            let state = self.membership_view.read().await.cluster_state.clone();
            if let ClusterFormationState::Active { .. } = state {
                return Ok(());
            }

            tokio::time::sleep(self.config.formation.formation_retry_delay).await;
        }

        Err(Error::with_context(
            ErrorKind::Timeout,
            format!("Timeout waiting for coordinator {coordinator} to form cluster"),
        ))
    }

    /// Join existing cluster
    async fn join_existing_cluster(
        &self,
        leader: NodeId,
        members: Vec<NodeId>,
    ) -> ConsensusResult<()> {
        info!(
            "Joining existing cluster with leader {} and {} members",
            leader,
            members.len()
        );

        // Update our membership view to reflect we're joining an existing cluster
        let mut view = self.membership_view.write().await;

        // Add ourselves if not already in the members list
        let mut all_members = members.clone();
        if !all_members.contains(&self.node_id) {
            all_members.push(self.node_id);
        }

        // For now, we just mark ourselves as part of the active cluster
        // The actual Raft join will happen when GlobalConsensusService initializes
        view.cluster_state = ClusterFormationState::Active {
            members: all_members.clone(),
            formed_at_ms: now_timestamp(),
        };

        // Add basic info for all members
        for member_id in &all_members {
            if member_id == &self.node_id {
                view.nodes.insert(
                    self.node_id,
                    NodeMembership {
                        node_id: self.node_id,
                        node_info: self.node_info.clone(),
                        status: NodeStatus::Online,
                        health: HealthInfo::new(),
                        roles: Default::default(),
                        last_seen_ms: now_timestamp(),
                    },
                );
            }
        }

        drop(view);

        // Send join request to the leader
        info!("Sending join request to leader {}", leader);
        let join_request = MembershipMessage::JoinCluster(messages::JoinClusterRequest {
            node_info: self.node_info.clone(),
            timestamp: now_timestamp(),
        });

        match self
            .network_manager
            .request_with_timeout(leader, join_request, Duration::from_secs(5))
            .await
        {
            Ok(MembershipResponse::JoinCluster(response)) => {
                if response.accepted {
                    info!("Join request accepted by leader");

                    // Publish event
                    self.publish_event(MembershipEvent::ClusterJoined {
                        members: all_members,
                        leader,
                    })
                    .await;
                } else {
                    error!(
                        "Join request rejected by leader: {:?}",
                        response.rejection_reason
                    );
                    return Err(Error::with_context(
                        ErrorKind::InvalidState,
                        format!(
                            "Join request rejected: {}",
                            response
                                .rejection_reason
                                .unwrap_or_else(|| "Unknown reason".to_string())
                        ),
                    ));
                }
            }
            Ok(_) => {
                error!("Unexpected response type from join request");
                return Err(Error::with_context(
                    ErrorKind::InvalidState,
                    "Unexpected response type from join request",
                ));
            }
            Err(e) => {
                error!("Failed to send join request to leader: {}", e);
                return Err(Error::with_context(
                    ErrorKind::Network,
                    format!("Failed to send join request: {e}"),
                ));
            }
        }

        Ok(())
    }

    /// Join forming cluster
    async fn join_forming_cluster(
        &self,
        coordinator: NodeId,
        formation_id: Uuid,
    ) -> ConsensusResult<()> {
        // Send acceptance
        let request = MembershipMessage::AcceptProposal(AcceptProposalRequest {
            formation_id,
            node_info: self.node_info.clone(),
        });

        match self
            .network_manager
            .request_with_timeout(
                coordinator,
                request,
                self.config.discovery.node_request_timeout,
            )
            .await
        {
            Ok(MembershipResponse::AcceptProposal(AcceptProposalResponse {
                success: true,
                cluster_state,
            })) => {
                let mut view = self.membership_view.write().await;
                view.cluster_state = cluster_state;
                Ok(())
            }
            _ => Err(Error::with_context(
                ErrorKind::ClusterFormation,
                "Failed to join forming cluster",
            )),
        }
    }

    /// Publish membership event
    async fn publish_event(&self, event: MembershipEvent) {
        self.event_bus.emit(event);
    }

    /// Register message handlers
    async fn register_handlers(&self) -> ConsensusResult<()> {
        use super::handler::MembershipHandler;

        info!("Registering membership service network handler");
        let handler = MembershipHandler::new(Arc::new(self.clone()));
        self.network_manager
            .register_service(handler)
            .await
            .map_err(|e| Error::with_context(ErrorKind::Network, e.to_string()))?;
        info!("Successfully registered membership service network handler");

        Ok(())
    }

    /// Register command handlers with the event bus
    pub fn register_command_handlers(self: Arc<Self>) {
        use crate::services::membership::command_handlers::commands::{
            GetMembershipHandler, GetNodeInfoHandler, GetOnlineMembersHandler, GetPeerInfoHandler,
            InitializeClusterHandler,
        };

        // Register InitializeCluster handler
        let init_handler = InitializeClusterHandler::new(self.clone());
        self.event_bus
            .handle_requests::<crate::services::membership::commands::InitializeCluster, _>(
                init_handler,
            )
            .expect("Failed to register InitializeCluster handler");

        // Register GetMembership handler
        let membership_handler = GetMembershipHandler::new(self.clone());
        self.event_bus
            .handle_requests::<crate::services::membership::commands::GetMembership, _>(
                membership_handler,
            )
            .expect("Failed to register GetMembership handler");

        // Register GetOnlineMembers handler
        let online_members_handler = GetOnlineMembersHandler::new(self.clone());
        self.event_bus
            .handle_requests::<crate::services::membership::commands::GetOnlineMembers, _>(
                online_members_handler,
            )
            .expect("Failed to register GetOnlineMembers handler");

        // Register GetNodeInfo handler
        let node_info_handler = GetNodeInfoHandler::new(self.clone());
        self.event_bus
            .handle_requests::<crate::services::membership::commands::GetNodeInfo, _>(
                node_info_handler,
            )
            .expect("Failed to register GetNodeInfo handler");

        // Register GetPeerInfo handler
        let peer_info_handler = GetPeerInfoHandler::new(self.clone());
        self.event_bus
            .handle_requests::<crate::services::membership::commands::GetPeerInfo, _>(
                peer_info_handler,
            )
            .expect("Failed to register GetPeerInfo handler");
    }

    /// Handle incoming messages
    pub async fn handle_message(
        &self,
        sender: NodeId,
        message: MembershipMessage,
    ) -> Result<MembershipResponse, proven_network::error::NetworkError> {
        match message {
            MembershipMessage::DiscoverCluster(req) => {
                let handler = handlers::DiscoverClusterHandler::new(
                    self.node_id,
                    self.membership_view.clone(),
                );
                match handler.handle(sender, req).await {
                    Ok(response) => Ok(MembershipResponse::DiscoverCluster(response)),
                    Err(e) => {
                        error!("Failed to handle discover request: {}", e);
                        Err(proven_network::error::NetworkError::Internal(e.to_string()))
                    }
                }
            }
            MembershipMessage::ProposeCluster(req) => {
                let handler = handlers::ProposeClusterHandler::new(
                    self.node_id,
                    self.membership_view.clone(),
                );
                match handler.handle(sender, req).await {
                    Ok(response) => Ok(MembershipResponse::ProposeCluster(response)),
                    Err(e) => {
                        error!("Failed to handle propose request: {}", e);
                        Err(proven_network::error::NetworkError::Internal(e.to_string()))
                    }
                }
            }
            MembershipMessage::AcceptProposal(req) => {
                let handler = handlers::AcceptProposalHandler::new(
                    self.node_id,
                    self.membership_view.clone(),
                );
                match handler.handle(sender, req).await {
                    Ok(response) => Ok(MembershipResponse::AcceptProposal(response)),
                    Err(e) => {
                        error!("Failed to handle accept proposal request: {}", e);
                        Err(proven_network::error::NetworkError::Internal(e.to_string()))
                    }
                }
            }
            MembershipMessage::HealthCheck(req) => {
                let handler =
                    handlers::HealthCheckHandler::new(self.node_id, self.membership_view.clone());
                match handler.handle(sender, req).await {
                    Ok(response) => Ok(MembershipResponse::HealthCheck(response)),
                    Err(e) => {
                        error!("Failed to handle health check request: {}", e);
                        Err(proven_network::error::NetworkError::Internal(e.to_string()))
                    }
                }
            }
            MembershipMessage::GracefulShutdown(req) => {
                let handler = handlers::GracefulShutdownHandler::new(
                    self.node_id,
                    self.membership_view.clone(),
                    self.event_bus.clone(),
                );
                match handler.handle(sender, req).await {
                    Ok(response) => Ok(MembershipResponse::GracefulShutdown(response)),
                    Err(e) => {
                        error!("Failed to handle graceful shutdown request: {}", e);
                        Err(proven_network::error::NetworkError::Internal(e.to_string()))
                    }
                }
            }
            MembershipMessage::JoinCluster(req) => {
                // Use the join handler
                let handler = handlers::JoinClusterHandler::new(
                    self.node_id,
                    self.membership_view.clone(),
                    self.event_bus.clone(),
                );

                match handler.handle(sender, req).await {
                    Ok(response) => Ok(MembershipResponse::JoinCluster(response)),
                    Err(e) => {
                        error!("Failed to handle join request: {}", e);
                        Ok(MembershipResponse::JoinCluster(
                            messages::JoinClusterResponse {
                                accepted: false,
                                rejection_reason: Some(format!("Internal error: {e}")),
                                cluster_state: None,
                            },
                        ))
                    }
                }
            }
        }
    }
}

#[async_trait]
impl<T, G, A, S> ServiceLifecycle for MembershipService<T, G, A, S>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    S: StorageAdaptor + 'static,
    A: Attestor + 'static,
{
    async fn initialize(&self) -> ConsensusResult<()> {
        Ok(())
    }

    async fn start(&self) -> ConsensusResult<()> {
        let mut state = self.state.write().await;
        match *state {
            ServiceState::NotStarted | ServiceState::Stopped => {
                *state = ServiceState::Starting;
            }
            _ => {
                return Err(Error::with_context(
                    ErrorKind::InvalidState,
                    "Service already started",
                ));
            }
        }
        drop(state);

        info!("Starting membership service");

        // Register handlers
        self.register_handlers().await?;

        // Start discovery and formation
        let service = self.clone();
        self.task_tracker.spawn(async move {
            if let Err(e) = service.handle_discovery_and_formation().await {
                error!("Discovery and formation failed: {}", e);
            }
        });

        // Start health monitoring
        let health_monitor = self.health_monitor.clone();
        let membership_view = self.membership_view.clone();
        let event_bus = self.event_bus.clone();
        let token = self.cancellation_token.clone();
        self.task_tracker.spawn(async move {
            health_monitor
                .start_monitoring(membership_view, event_bus, token)
                .await;
        });

        *self.state.write().await = ServiceState::Running;
        info!("Membership service started");

        Ok(())
    }

    async fn stop(&self) -> ConsensusResult<()> {
        let mut state = self.state.write().await;
        if *state != ServiceState::Running {
            return Ok(());
        }
        *state = ServiceState::Stopping;
        drop(state);

        info!("Stopping membership service");

        // Announce graceful shutdown to all cluster members
        let view = self.membership_view.read().await;
        let online_nodes: Vec<NodeId> = view
            .online_nodes()
            .iter()
            .filter(|n| n.node_id != self.node_id)
            .map(|n| n.node_id)
            .collect();
        drop(view);

        if !online_nodes.is_empty() {
            info!(
                "Announcing graceful shutdown to {} cluster members",
                online_nodes.len()
            );

            let shutdown_msg = MembershipMessage::GracefulShutdown(GracefulShutdownRequest {
                reason: Some("Service stopping".to_string()),
                timestamp: now_timestamp(),
            });

            // Best effort - don't wait for responses
            for node_id in online_nodes {
                let network = self.network_manager.clone();
                let msg = shutdown_msg.clone();
                tokio::spawn(async move {
                    if let Err(e) = network
                        .request_with_timeout(node_id, msg, Duration::from_millis(100))
                        .await
                    {
                        debug!("Failed to send shutdown announcement to {}: {}", node_id, e);
                    }
                });
            }

            // Give a brief moment for messages to be sent
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // Cancel all tasks
        self.cancellation_token.cancel();

        // Wait for tasks
        self.task_tracker.close();
        match tokio::time::timeout(std::time::Duration::from_secs(5), self.task_tracker.wait())
            .await
        {
            Ok(()) => debug!("All membership tasks completed"),
            Err(_) => warn!("Some membership tasks did not complete within timeout"),
        }

        // Unregister from network service
        let _ = self.network_manager.unregister_service("membership").await;

        // Unregister all event handlers to allow re-registration on restart
        use crate::services::membership::commands::{
            GetMembership, GetNodeInfo, GetOnlineMembers, GetPeerInfo, InitializeCluster,
        };
        let _ = self
            .event_bus
            .unregister_request_handler::<InitializeCluster>();
        let _ = self.event_bus.unregister_request_handler::<GetMembership>();
        let _ = self
            .event_bus
            .unregister_request_handler::<GetOnlineMembers>();
        let _ = self.event_bus.unregister_request_handler::<GetNodeInfo>();
        let _ = self.event_bus.unregister_request_handler::<GetPeerInfo>();

        *self.state.write().await = ServiceState::Stopped;
        info!("Membership service stopped");

        Ok(())
    }

    async fn is_healthy(&self) -> bool {
        let state = self.state.read().await;
        *state == ServiceState::Running
    }

    async fn status(&self) -> crate::foundation::traits::lifecycle::ServiceStatus {
        use crate::foundation::traits::lifecycle::ServiceStatus;
        let state = self.state.read().await;
        match *state {
            ServiceState::Running => ServiceStatus::Running,
            ServiceState::Stopped => ServiceStatus::Stopped,
            _ => ServiceStatus::Stopped,
        }
    }
}

// Clone implementation
impl<T, G, A, S> Clone for MembershipService<T, G, A, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
    A: Attestor,
{
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            node_id: self.node_id,
            node_info: self.node_info.clone(),
            network_manager: self.network_manager.clone(),
            topology_manager: self.topology_manager.clone(),
            event_bus: self.event_bus.clone(),
            membership_view: self.membership_view.clone(),
            discovery_manager: self.discovery_manager.clone(),
            health_monitor: self.health_monitor.clone(),
            state: self.state.clone(),
            task_tracker: self.task_tracker.clone(),
            cancellation_token: self.cancellation_token.clone(),
            _phantom: std::marker::PhantomData,
        }
    }
}
