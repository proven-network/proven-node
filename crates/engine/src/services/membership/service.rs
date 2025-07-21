//! Membership service implementation

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
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
use crate::foundation::{
    ServiceLifecycle,
    traits::{HealthStatus, ServiceHealth},
};
use crate::services::event::bus::EventBus;

use super::config::MembershipConfig;
use super::discovery::{DiscoveryManager, DiscoveryResult};
use super::events::MembershipEvent;
use super::health::HealthMonitor;
use super::messages::{
    AcceptProposalRequest, AcceptProposalResponse, ClusterState, DiscoverClusterResponse,
    GlobalConsensusInfo, GracefulShutdownRequest, GracefulShutdownResponse, HealthCheckResponse,
    MembershipMessage, MembershipResponse, ProposeClusterRequest, ProposeClusterResponse,
};
use super::types::{
    ClusterFormationState, HealthInfo, MembershipView, NodeMembership, NodeRole, NodeStatus,
};

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
pub struct MembershipService<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    config: MembershipConfig,
    node_id: NodeId,
    node_info: Node,
    network_manager: Arc<NetworkManager<T, G>>,
    topology_manager: Arc<TopologyManager<G>>,
    event_bus: Arc<EventBus>,

    /// Current membership view
    membership_view: Arc<RwLock<MembershipView>>,

    /// Discovery manager
    discovery_manager: Arc<DiscoveryManager<T, G>>,

    /// Health monitor
    health_monitor: Arc<HealthMonitor<T, G>>,

    /// Service state
    state: Arc<RwLock<ServiceState>>,

    /// Task tracker
    task_tracker: TaskTracker,

    /// Cancellation token
    cancellation_token: CancellationToken,

    /// Phantom data for storage
    _phantom: std::marker::PhantomData<S>,
}

impl<T, G, S> MembershipService<T, G, S>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    S: StorageAdaptor + 'static,
{
    /// Create new membership service
    pub fn new(
        config: MembershipConfig,
        node_id: NodeId,
        node_info: Node,
        network_manager: Arc<NetworkManager<T, G>>,
        topology_manager: Arc<TopologyManager<G>>,
        event_bus: Arc<EventBus>,
    ) -> Self {
        let discovery_manager = Arc::new(DiscoveryManager::new(
            node_id.clone(),
            network_manager.clone(),
            topology_manager.clone(),
            config.discovery.clone(),
        ));

        let health_monitor = Arc::new(HealthMonitor::new(
            node_id.clone(),
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

    /// Get current membership view
    pub async fn get_membership_view(&self) -> MembershipView {
        self.membership_view.read().await.clone()
    }

    /// Get online members
    pub async fn get_online_members(&self) -> Vec<(NodeId, Node)> {
        let view = self.membership_view.read().await;
        view.online_nodes()
            .into_iter()
            .map(|m| (m.node_id.clone(), m.node_info.clone()))
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
            self.node_id.clone(),
            NodeMembership {
                node_id: self.node_id.clone(),
                node_info: self.node_info.clone(),
                status: NodeStatus::Online,
                health: HealthInfo::new(),
                roles,
                last_seen_ms: now_timestamp(),
            },
        );

        view.cluster_state = ClusterFormationState::Active {
            members: vec![self.node_id.clone()],
            formed_at_ms: now_timestamp(),
        };

        drop(view);

        // Publish event
        self.publish_event(MembershipEvent::ClusterFormed {
            members: vec![self.node_id.clone()],
            coordinator: self.node_id.clone(),
        })
        .await;

        Ok(())
    }

    /// Coordinate cluster formation
    async fn coordinate_cluster_formation(
        &self,
        online_peers: Vec<(NodeId, Node)>,
    ) -> ConsensusResult<()> {
        let formation_id = Uuid::new_v4();
        let mut proposed_members = online_peers.clone();
        proposed_members.push((self.node_id.clone(), self.node_info.clone()));

        // Update state
        {
            let mut view = self.membership_view.write().await;
            view.cluster_state = ClusterFormationState::Forming {
                coordinator: self.node_id.clone(),
                formation_id,
                proposed_members: proposed_members.iter().map(|(id, _)| id.clone()).collect(),
            };
        }

        // Send proposals to all members
        let request = MembershipMessage::ProposeCluster(ProposeClusterRequest {
            coordinator: self.node_id.clone(),
            formation_id,
            proposed_members: proposed_members.clone(),
            timeout_ms: self.config.formation.formation_timeout.as_millis() as u64,
        });

        let mut acceptances = 0;
        for (peer_id, _) in &online_peers {
            match self
                .network_manager
                .service_request(
                    peer_id.clone(),
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
                node_id.clone(),
                NodeMembership {
                    node_id: node_id.clone(),
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

        let member_ids: Vec<NodeId> = members.iter().map(|(id, _)| id.clone()).collect();
        view.cluster_state = ClusterFormationState::Active {
            members: member_ids.clone(),
            formed_at_ms: now_timestamp(),
        };

        drop(view);

        // Publish event
        self.publish_event(MembershipEvent::ClusterFormed {
            members: member_ids,
            coordinator: self.node_id.clone(),
        })
        .await;

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
            all_members.push(self.node_id.clone());
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
                    self.node_id.clone(),
                    NodeMembership {
                        node_id: self.node_id.clone(),
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

        // Publish event
        self.publish_event(MembershipEvent::ClusterJoined {
            members: all_members,
            leader,
        })
        .await;

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
            .service_request(
                coordinator.clone(),
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
        self.event_bus.publish(event).await;
    }

    /// Register message handlers
    async fn register_handlers(&self) -> ConsensusResult<()> {
        let service = self.clone();
        self.network_manager
            .register_service::<MembershipMessage, _>(move |sender, message| {
                let service = service.clone();
                Box::pin(async move { service.handle_message(sender, message).await })
            })
            .await
            .map_err(|e| Error::with_context(ErrorKind::Network, e.to_string()))?;

        Ok(())
    }

    /// Handle incoming messages
    async fn handle_message(
        &self,
        sender: NodeId,
        message: MembershipMessage,
    ) -> Result<MembershipResponse, proven_network::error::NetworkError> {
        match message {
            MembershipMessage::DiscoverCluster(_req) => {
                let view = self.membership_view.read().await;

                // Convert cluster state and find the leader
                let mut cluster_state: ClusterState = view.cluster_state.clone().into();

                // If we have an active cluster, find who has the GlobalConsensusLeader role
                if let ClusterState::Active { ref mut leader, .. } = cluster_state {
                    *leader = view
                        .nodes_with_role(&NodeRole::GlobalConsensusLeader)
                        .first()
                        .map(|member| member.node_id.clone());
                }

                let response = DiscoverClusterResponse {
                    from_node: self.node_id.clone(),
                    cluster_state,
                    node_status: NodeStatus::Online, // TODO: Get from service state
                    timestamp: now_timestamp(),
                };
                Ok(MembershipResponse::DiscoverCluster(response))
            }
            MembershipMessage::ProposeCluster(req) => {
                // Accept if we're not already in a cluster
                let view = self.membership_view.read().await;
                let accepted = matches!(view.cluster_state, ClusterFormationState::NotFormed);
                drop(view);

                if accepted {
                    // Update our state
                    let mut view = self.membership_view.write().await;
                    view.cluster_state = ClusterFormationState::Forming {
                        coordinator: req.coordinator.clone(),
                        formation_id: req.formation_id,
                        proposed_members: req
                            .proposed_members
                            .iter()
                            .map(|(id, _)| id.clone())
                            .collect(),
                    };
                }

                Ok(MembershipResponse::ProposeCluster(ProposeClusterResponse {
                    accepted,
                    rejection_reason: if !accepted {
                        Some("Already in cluster".to_string())
                    } else {
                        None
                    },
                }))
            }
            MembershipMessage::AcceptProposal(_req) => {
                // TODO: Handle acceptance from a node
                let view = self.membership_view.read().await;
                Ok(MembershipResponse::AcceptProposal(AcceptProposalResponse {
                    success: true,
                    cluster_state: view.cluster_state.clone(),
                }))
            }
            MembershipMessage::HealthCheck(_req) => {
                // TODO: Get actual consensus info
                let response = HealthCheckResponse {
                    status: NodeStatus::Online,
                    load: None,
                    global_consensus_info: None,
                    timestamp: now_timestamp(),
                };
                Ok(MembershipResponse::HealthCheck(response))
            }
            MembershipMessage::GracefulShutdown(req) => {
                info!(
                    "Node {} announced graceful shutdown: {:?}",
                    sender, req.reason
                );

                // Immediately mark the node as offline
                let mut view = self.membership_view.write().await;
                if let Some(member) = view.nodes.get_mut(&sender) {
                    member.status = NodeStatus::Offline {
                        since_ms: now_timestamp(),
                    };
                    info!("Marked node {} as offline due to graceful shutdown", sender);
                }
                drop(view);

                // Publish event for immediate membership change
                self.publish_event(MembershipEvent::NodeGracefulShutdown {
                    node_id: sender.clone(),
                    reason: req.reason.clone(),
                })
                .await;

                // Also publish membership change required event
                self.publish_event(MembershipEvent::MembershipChangeRequired {
                    add_nodes: vec![],
                    remove_nodes: vec![sender],
                    reason: format!(
                        "Node gracefully shut down: {:?}",
                        req.reason
                            .unwrap_or_else(|| "No reason provided".to_string())
                    ),
                })
                .await;

                Ok(MembershipResponse::GracefulShutdown(
                    GracefulShutdownResponse { acknowledged: true },
                ))
            }
        }
    }
}

#[async_trait]
impl<T, G, S> ServiceLifecycle for MembershipService<T, G, S>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    S: StorageAdaptor + 'static,
{
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
            .map(|n| n.node_id.clone())
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
                        .service_request(node_id.clone(), msg, Duration::from_millis(100))
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

        // Unregister handlers
        self.network_manager
            .unregister_service::<MembershipMessage>()
            .await
            .map_err(|e| Error::with_context(ErrorKind::Network, e.to_string()))?;

        *self.state.write().await = ServiceState::Stopped;
        info!("Membership service stopped");

        Ok(())
    }

    async fn is_running(&self) -> bool {
        let state = self.state.read().await;
        *state == ServiceState::Running
    }

    async fn health_check(&self) -> ConsensusResult<ServiceHealth> {
        let state = self.state.read().await;
        if *state != ServiceState::Running {
            return Ok(ServiceHealth {
                name: "membership".to_string(),
                status: HealthStatus::Unhealthy,
                message: Some("Service not running".to_string()),
                subsystems: Vec::new(),
            });
        }

        Ok(ServiceHealth {
            name: "membership".to_string(),
            status: HealthStatus::Healthy,
            message: None,
            subsystems: Vec::new(),
        })
    }
}

// Clone implementation
impl<T, G, S> Clone for MembershipService<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            node_id: self.node_id.clone(),
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

/// Get current timestamp in milliseconds
fn now_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
