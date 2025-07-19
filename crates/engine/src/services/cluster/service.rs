//! Main cluster service implementation

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use proven_network::NetworkManager;
use proven_topology::TopologyAdaptor;
use proven_topology::{NodeId, TopologyManager};
use proven_transport::Transport;

use super::discovery::{DiscoveryConfig, DiscoveryManager, DiscoveryOutcome};
use super::formation::{FormationManager, FormationRequest, FormationResult};
use super::membership::{JoinRequest, LeaveRequest, MembershipManager};
use super::messages;
use super::state::{StateManager, StateTransition};
use super::types::*;
use crate::services::event::EventPublisher;

/// Boxed future type for callbacks
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Cluster formation callback type
pub type ClusterFormationCallback =
    Arc<dyn Fn(ClusterFormationEvent) -> BoxFuture<'static, ()> + Send + Sync>;

/// Cluster formation event
#[derive(Debug, Clone)]
pub enum ClusterFormationEvent {
    /// Cluster formation completed as leader
    FormedAsLeader {
        cluster_id: String,
        members: Vec<NodeId>,
    },
    /// Successfully joined cluster as follower
    JoinedAsFollower {
        cluster_id: String,
        leader: NodeId,
        members: Vec<NodeId>,
    },
    /// Cluster formation failed
    FormationFailed { error: String },
}

/// Cluster service for managing consensus clusters
pub struct ClusterService<T, G>
where
    T: Transport,
    G: TopologyAdaptor,
{
    /// Service configuration
    config: ClusterConfig,

    /// Node ID
    node_id: NodeId,

    /// Discovery manager
    discovery_manager: Option<Arc<DiscoveryManager<T, G>>>,

    /// Formation manager
    formation_manager: Arc<FormationManager>,

    /// Membership manager
    membership_manager: Arc<MembershipManager>,

    /// State manager
    state_manager: Arc<StateManager>,

    /// Background tasks
    background_tasks: Arc<RwLock<Vec<JoinHandle<()>>>>,

    /// Shutdown signal
    shutdown_signal: Arc<tokio::sync::Notify>,

    /// Service state
    service_state: Arc<RwLock<ServiceState>>,

    /// Formation callback
    formation_callback: Option<ClusterFormationCallback>,
}

/// Service state
#[derive(Debug, Clone, Copy, PartialEq)]
enum ServiceState {
    /// Not started
    NotStarted,
    /// Running
    Running,
    /// Stopping
    Stopping,
    /// Stopped
    Stopped,
}

impl<T, G> ClusterService<T, G>
where
    T: Transport,
    G: TopologyAdaptor,
{
    /// Create a new cluster service
    pub fn new(config: ClusterConfig, node_id: NodeId) -> Self {
        let formation_manager = Arc::new(FormationManager::new(
            node_id.clone(),
            config.formation.clone(),
        ));

        let membership_manager = Arc::new(MembershipManager::new(
            node_id.clone(),
            config.membership.clone(),
        ));

        let state_manager = Arc::new(StateManager::new());

        Self {
            config,
            node_id,
            discovery_manager: None,
            formation_manager,
            membership_manager,
            state_manager,
            background_tasks: Arc::new(RwLock::new(Vec::new())),
            shutdown_signal: Arc::new(tokio::sync::Notify::new()),
            service_state: Arc::new(RwLock::new(ServiceState::NotStarted)),
            formation_callback: None,
        }
    }

    /// Initialize discovery manager with required dependencies
    pub fn with_discovery(
        mut self,
        network: Arc<NetworkManager<T, G>>,
        topology: Arc<TopologyManager<G>>,
        event_publisher: EventPublisher,
    ) -> Self {
        // Convert from types::DiscoveryConfig to discovery::DiscoveryConfig
        let discovery_config = super::discovery::DiscoveryConfig {
            discovery_timeout: Duration::from_secs(30),
            discovery_interval: self.config.discovery.retry_delay,
            max_discovery_rounds: self.config.discovery.retry_attempts,
            node_request_timeout: Duration::from_secs(3),
            min_response_ratio: 0.5,
            min_responding_nodes: 1,
            enable_request_retry: true,
            max_retries_per_node: 2,
        };
        let discovery_manager = Arc::new(DiscoveryManager::new(
            self.node_id.clone(),
            network,
            topology,
            discovery_config,
            event_publisher,
        ));
        self.discovery_manager = Some(discovery_manager);
        self
    }

    /// Set the formation callback
    pub fn with_formation_callback(mut self, callback: ClusterFormationCallback) -> Self {
        self.formation_callback = Some(callback);
        self
    }

    /// Start the cluster service
    pub async fn start(&self) -> ClusterResult<()> {
        let mut state = self.service_state.write().await;
        match *state {
            ServiceState::NotStarted | ServiceState::Stopped => {
                *state = ServiceState::Running;
            }
            _ => {
                return Err(ClusterError::Internal(format!(
                    "Service cannot be started from {:?} state",
                    *state
                )));
            }
        }
        drop(state);

        info!("Starting cluster service for node {}", self.node_id);

        // Register service handlers for message forwarding
        self.register_service_handlers().await?;

        let mut tasks = self.background_tasks.write().await;

        // Start health check task
        tasks.push(self.spawn_health_check_task());

        // Start membership monitor
        tasks.push(self.spawn_membership_monitor());

        Ok(())
    }

    /// Stop the cluster service
    pub async fn stop(&self) -> ClusterResult<()> {
        let mut state = self.service_state.write().await;
        if *state != ServiceState::Running {
            return Ok(());
        }

        *state = ServiceState::Stopping;
        drop(state);

        info!("Stopping cluster service");

        // Leave cluster if active
        if self.state_manager.is_active() {
            let _ = self.leave_cluster("Service stopping".to_string()).await;
        }

        // Signal shutdown
        self.shutdown_signal.notify_waiters();

        // Wait for tasks
        let mut tasks = self.background_tasks.write().await;
        for task in tasks.drain(..) {
            if let Err(e) = task.await {
                warn!("Error stopping cluster task: {}", e);
            }
        }

        // Skip unregistering the service handler from NetworkManager
        // The handlers are mostly stateless and can safely remain registered
        // Unregistering was causing hangs, possibly due to Arc reference cycles
        tracing::debug!(
            "Skipping cluster service handler unregistration to avoid potential deadlock"
        );

        let mut state = self.service_state.write().await;
        *state = ServiceState::Stopped;

        Ok(())
    }

    /// Start discovery and handle the outcome
    pub async fn discover_and_join(&self) -> ClusterResult<()> {
        self.ensure_running().await?;

        let discovery_manager = self.discovery_manager.as_ref().ok_or_else(|| {
            ClusterError::Internal("Discovery manager not initialized".to_string())
        })?;

        // Start discovery
        let outcome = discovery_manager
            .start_discovery()
            .await
            .map_err(|e| ClusterError::Internal(e.to_string()))?;

        // Handle the discovery outcome
        match outcome {
            DiscoveryOutcome::SingleNode => {
                info!("No peers found, forming single-node cluster");
                self.form_cluster(FormationMode::SingleNode).await?;
            }
            DiscoveryOutcome::FoundCluster { leader_id } => {
                info!("Found existing cluster with leader {}, joining", leader_id);
                self.join_cluster(leader_id).await?;
            }
            DiscoveryOutcome::BecomeCoordinator { peers } => {
                info!("Elected as coordinator for {} peers", peers.len());
                self.form_cluster_with_peers(
                    FormationMode::MultiNode {
                        expected_size: peers.len() + 1,
                    },
                    peers,
                )
                .await?;
            }
            DiscoveryOutcome::ShouldJoinCoordinator { coordinator, .. } => {
                info!("Should join coordinator {}", coordinator);
                self.join_cluster(coordinator).await?;
            }
            DiscoveryOutcome::RetryDiscovery => {
                warn!("Discovery indicated retry needed");
                return Err(ClusterError::Internal("Discovery retry needed".to_string()));
            }
        }

        Ok(())
    }

    /// Form a new cluster
    pub async fn form_cluster(&self, mode: FormationMode) -> ClusterResult<()> {
        self.form_cluster_with_peers(mode, Vec::new()).await
    }

    /// Form a new cluster with known peers
    pub async fn form_cluster_with_peers(
        &self,
        mode: FormationMode,
        peers: Vec<NodeId>,
    ) -> ClusterResult<()> {
        self.ensure_running().await?;

        // Start forming
        self.state_manager.start_forming(mode)?;

        // Create formation request
        let request = FormationRequest {
            mode,
            strategy: self.config.formation.strategy,
            initial_peers: peers,
        };

        // Form cluster
        match self.formation_manager.form_cluster(request).await {
            Ok(result) => {
                if result.success {
                    // Mark as active
                    self.state_manager.mark_active(
                        NodeRole::Leader, // Assume leader for single-node
                        result.members.len(),
                    )?;

                    info!("Cluster formed successfully: {}", result.cluster_id);

                    // Update discovery manager's cluster state
                    if let Some(dm) = &self.discovery_manager {
                        dm.update_cluster_state(
                            true,                       // active
                            Some(1),                    // initial term
                            Some(self.node_id.clone()), // we are the leader
                            Some(result.members.len()), // cluster size
                        )
                        .await;
                    }

                    // Trigger formation callback
                    if let Some(callback) = &self.formation_callback {
                        let event = ClusterFormationEvent::FormedAsLeader {
                            cluster_id: result.cluster_id.clone(),
                            members: result.members.clone(),
                        };
                        tokio::spawn(callback(event));
                    }

                    Ok(())
                } else {
                    let error = result.error.unwrap_or_else(|| "Unknown error".to_string());
                    self.state_manager.mark_failed(error.clone())?;
                    Err(ClusterError::FormationFailed(error))
                }
            }
            Err(e) => {
                self.state_manager.mark_failed(e.to_string())?;

                // Trigger formation callback with failure
                if let Some(callback) = &self.formation_callback {
                    let event = ClusterFormationEvent::FormationFailed {
                        error: e.to_string(),
                    };
                    tokio::spawn(callback(event));
                }

                Err(e)
            }
        }
    }

    /// Handle join request
    async fn handle_join_request(
        node_id: NodeId,
        state_manager: Arc<StateManager>,
        membership_manager: Arc<MembershipManager>,
        sender: NodeId,
        request: messages::JoinRequest,
    ) -> messages::JoinResponse {
        debug!("Processing join request from {}", sender);

        let current_state = state_manager.get_state();
        match current_state {
            // Handle the case where we're still forming the cluster
            ClusterState::Forming { mode, .. } => {
                // If we're forming a multi-node cluster, tell the joiner to retry
                if let FormationMode::MultiNode { .. } = mode {
                    messages::JoinResponse {
                        error_message: Some(
                            "Cluster formation in progress, please retry".to_string(),
                        ),
                        cluster_size: None,
                        current_term: None,
                        responder_id: node_id,
                        success: false,
                        current_leader: None,
                    }
                } else {
                    messages::JoinResponse {
                        error_message: Some("Not accepting joins".to_string()),
                        cluster_size: None,
                        current_term: None,
                        responder_id: node_id,
                        success: false,
                        current_leader: None,
                    }
                }
            }
            ClusterState::Active {
                role, cluster_size, ..
            } => {
                if role != NodeRole::Leader {
                    return messages::JoinResponse {
                        error_message: Some("Not the cluster leader".to_string()),
                        cluster_size: Some(cluster_size),
                        current_term: None,
                        responder_id: node_id,
                        success: false,
                        current_leader: None, // TODO: Get actual leader
                    };
                }

                // Process the join
                info!("Processing join request from {}", request.requester_id);

                // Add to membership
                // TODO: Remove socket address from membership tracking - network layer handles routing by NodeId
                let join_req = JoinRequest {
                    node_id: request.requester_id.clone(),
                    address: "0.0.0.0:0".parse().unwrap(), // Placeholder - network layer handles routing
                    as_learner: false,
                };

                match membership_manager.process_join(join_req).await {
                    Ok(_) => {
                        let new_size = cluster_size + 1;
                        state_manager.update_cluster_size(new_size).ok();

                        messages::JoinResponse {
                            error_message: None,
                            cluster_size: Some(new_size),
                            current_term: None, // TODO: Get from consensus
                            responder_id: node_id.clone(),
                            success: true,
                            current_leader: Some(node_id),
                        }
                    }
                    Err(e) => messages::JoinResponse {
                        error_message: Some(e.to_string()),
                        cluster_size: Some(cluster_size),
                        current_term: None,
                        responder_id: node_id.clone(),
                        success: false,
                        current_leader: Some(node_id),
                    },
                }
            }
            _ => messages::JoinResponse {
                error_message: Some("Cluster not in active state".to_string()),
                cluster_size: None,
                current_term: None,
                responder_id: node_id,
                success: false,
                current_leader: None,
            },
        }
    }

    /// Join an existing cluster
    pub async fn join_cluster(&self, target_node: NodeId) -> ClusterResult<()> {
        self.ensure_running().await?;

        // Check if we're trying to join ourselves (this can happen if we're the coordinator)
        if target_node == self.node_id {
            info!("Cannot join cluster with ourselves as target, we must be the leader");
            return Ok(());
        }

        // Start joining
        self.state_manager.start_joining(target_node.clone())?;

        // Get our node info from topology (if we have discovery manager with topology)
        let our_node = if let Some(dm) = &self.discovery_manager {
            dm.topology.get_own_node().await.ok()
        } else {
            None
        };

        let our_node = our_node.ok_or_else(|| {
            ClusterError::Internal("Local node not found in topology".to_string())
        })?;

        // Create join request
        let join_request = messages::JoinRequest {
            requester_id: self.node_id.clone(),
            requester_node: our_node,
        };

        // Send join request to target node
        let network = self
            .discovery_manager
            .as_ref()
            .ok_or_else(|| ClusterError::Internal("No network access".to_string()))?
            .network
            .clone();

        // Try to join with retries for formation in progress
        let mut retry_count = 0;
        let max_retries = 10;
        let retry_delay = std::time::Duration::from_millis(500);

        let join_response = loop {
            let message = messages::ClusterServiceMessage::GlobalJoin {
                requester_id: join_request.requester_id.clone(),
                requester_node: join_request.requester_node.clone(),
            };

            let response = network
                .request_service(
                    target_node.clone(),
                    message,
                    std::time::Duration::from_secs(5),
                )
                .await
                .map_err(|e| ClusterError::Network(e.to_string()))?;

            match response {
                messages::ClusterServiceResponse::GlobalJoin {
                    success,
                    error_message,
                    cluster_size,
                    current_term,
                    responder_id,
                    current_leader,
                } => {
                    let join_resp = messages::JoinResponse {
                        success,
                        error_message: error_message.clone(),
                        cluster_size,
                        current_term,
                        responder_id,
                        current_leader,
                    };

                    if success {
                        break join_resp;
                    }

                    // Check if we should retry
                    if let Some(ref error_msg) = error_message
                        && error_msg.contains("formation in progress")
                        && retry_count < max_retries
                    {
                        retry_count += 1;
                        info!(
                            "Cluster formation in progress, retrying join ({}/{})",
                            retry_count, max_retries
                        );
                        tokio::time::sleep(retry_delay).await;
                        continue;
                    }

                    return Err(ClusterError::JoinRejected(
                        error_message.unwrap_or_else(|| "Unknown error".to_string()),
                    ));
                }
                _ => {
                    return Err(ClusterError::Network(
                        "Unexpected response type".to_string(),
                    ));
                }
            }
        };

        // Mark as active
        self.state_manager
            .mark_active(NodeRole::Follower, join_response.cluster_size.unwrap_or(2))?;

        // Update discovery manager's cluster state
        if let Some(dm) = &self.discovery_manager {
            dm.update_cluster_state(
                true,
                join_response.current_term,
                join_response.current_leader,
                join_response.cluster_size,
            )
            .await;
        }

        info!("Successfully joined cluster via node {}", target_node);

        // Trigger formation callback
        if let Some(callback) = &self.formation_callback {
            let event = ClusterFormationEvent::JoinedAsFollower {
                cluster_id: self.config.cluster_name.clone(),
                leader: target_node.clone(),
                members: vec![], // TODO: Get actual members from join response
            };
            tokio::spawn(callback(event));
        }

        Ok(())
    }

    /// Process a join request from another node
    pub async fn process_join_request(&self, requester_id: NodeId) -> ClusterResult<()> {
        self.ensure_running().await?;

        // Check if we're active and can accept new members
        if !self.state_manager.is_active() {
            return Err(ClusterError::NotMember);
        }

        let current_state = self.state_manager.get_state();
        match current_state {
            ClusterState::Active { role, .. } => {
                if role != NodeRole::Leader {
                    return Err(ClusterError::NotLeader);
                }
            }
            _ => {
                return Err(ClusterError::InvalidState(
                    "Not in active state".to_string(),
                ));
            }
        }

        info!("Processing join request from {}", requester_id);

        // Add the node to membership
        // TODO: Get actual address from topology or discovery info
        let join_request = JoinRequest {
            node_id: requester_id.clone(),
            address: "0.0.0.0:0".parse().unwrap(), // Placeholder - should come from discovery
            as_learner: false,                     // Join as full member
        };

        self.membership_manager.process_join(join_request).await?;

        // Update our cluster size
        let members = self.membership_manager.get_members().await;
        self.state_manager.update_cluster_size(members.len())?;

        info!("Node {} successfully added to cluster", requester_id);
        Ok(())
    }

    /// Leave the cluster
    pub async fn leave_cluster(&self, reason: String) -> ClusterResult<()> {
        self.ensure_running().await?;

        if !self.state_manager.is_active() {
            return Err(ClusterError::NotMember);
        }

        // Start leaving
        self.state_manager.start_leaving(reason.clone())?;

        // Create leave request
        let request = LeaveRequest {
            node_id: self.node_id.clone(),
            reason,
            graceful: true,
        };

        // Process leave
        self.membership_manager.process_leave(request).await?;

        // Reset state
        self.state_manager.reset()?;

        info!("Successfully left cluster");
        Ok(())
    }

    /// Get cluster information
    pub async fn get_cluster_info(&self) -> ClusterResult<ClusterInfo> {
        let state = self.state_manager.get_state();
        let members = self.membership_manager.get_members().await;

        Ok(ClusterInfo {
            cluster_id: self.config.cluster_name.clone(),
            state,
            members,
            leader: None,                 // Would be determined from consensus
            formed_at: SystemTime::now(), // Would be tracked
            last_updated: SystemTime::now(),
        })
    }

    /// Get cluster health
    pub async fn get_health(&self) -> ClusterResult<ClusterHealth> {
        let mut checks = Vec::new();

        // Check service state
        let service_check = HealthCheck {
            name: "service".to_string(),
            status: if self.is_running().await {
                HealthStatus::Healthy
            } else {
                HealthStatus::Unhealthy
            },
            message: None,
            duration: std::time::Duration::from_millis(0),
        };
        checks.push(service_check);

        // Check cluster state
        let state = self.state_manager.get_state();
        let state_check = HealthCheck {
            name: "state".to_string(),
            status: match state {
                ClusterState::Active { .. } => HealthStatus::Healthy,
                ClusterState::Failed { .. } => HealthStatus::Unhealthy,
                _ => HealthStatus::Degraded,
            },
            message: Some(format!("{state:?}")),
            duration: std::time::Duration::from_millis(0),
        };
        checks.push(state_check);

        // Overall status
        let status = if checks.iter().all(|c| c.status == HealthStatus::Healthy) {
            HealthStatus::Healthy
        } else if checks.iter().any(|c| c.status == HealthStatus::Unhealthy) {
            HealthStatus::Unhealthy
        } else {
            HealthStatus::Degraded
        };

        Ok(ClusterHealth {
            status,
            checks,
            last_check: SystemTime::now(),
        })
    }

    /// Get cluster metrics
    pub async fn get_metrics(&self) -> ClusterResult<ClusterMetrics> {
        let members = self.membership_manager.get_members().await;

        let active_nodes = members
            .values()
            .filter(|n| n.state == NodeState::Active)
            .count();

        let failed_nodes = members
            .values()
            .filter(|n| n.state == NodeState::Failed)
            .count();

        Ok(ClusterMetrics {
            total_nodes: members.len(),
            active_nodes,
            failed_nodes,
            leader_changes: 0,      // Would be tracked
            membership_changes: 0,  // Would be tracked
            avg_join_time_ms: 0.0,  // Would be calculated
            avg_leave_time_ms: 0.0, // Would be calculated
        })
    }

    // Private helper methods

    /// Ensure service is running
    async fn ensure_running(&self) -> ClusterResult<()> {
        let state = self.service_state.read().await;
        if *state != ServiceState::Running {
            return Err(ClusterError::NotStarted);
        }
        Ok(())
    }

    /// Check if service is running
    async fn is_running(&self) -> bool {
        *self.service_state.read().await == ServiceState::Running
    }

    /// Spawn health check task
    fn spawn_health_check_task(&self) -> JoinHandle<()> {
        let membership = self.membership_manager.clone();
        let interval = self.config.health.check_interval;
        let shutdown = self.shutdown_signal.clone();

        tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);
            interval_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    _ = interval_timer.tick() => {
                        // Check health of all members
                        let members = membership.get_members().await;
                        for (node_id, _) in members {
                            if let Err(e) = membership.check_node_health(&node_id).await {
                                warn!("Health check failed for node {}: {}", node_id, e);
                            }
                        }
                    }
                    _ = shutdown.notified() => {
                        debug!("Health check task shutting down");
                        break;
                    }
                }
            }
        })
    }

    /// Spawn membership monitor task
    fn spawn_membership_monitor(&self) -> JoinHandle<()> {
        let _membership = self.membership_manager.clone();
        let interval = self.config.membership.heartbeat_interval;
        let shutdown = self.shutdown_signal.clone();

        tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);
            interval_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    _ = interval_timer.tick() => {}
                    _ = shutdown.notified() => {
                        debug!("Membership monitor shutting down");
                        break;
                    }
                }
            }
        })
    }

    /// Register service handlers for message forwarding
    async fn register_service_handlers(&self) -> ClusterResult<()> {
        // Only register if we have a discovery manager and network access
        let discovery_manager = match &self.discovery_manager {
            Some(dm) => dm.clone(),
            None => {
                info!("No discovery manager configured, skipping service handler registration");
                return Ok(());
            }
        };

        // First, try to unregister any existing handler (in case of restart)
        use super::messages::ClusterServiceMessage;
        let _ = discovery_manager
            .network
            .unregister_service::<ClusterServiceMessage>()
            .await;

        let network = discovery_manager.network.clone();
        let node_id = self.node_id.clone();
        let state_manager = self.state_manager.clone();
        let membership_manager = self.membership_manager.clone();

        // Register handler for ClusterServiceMessage
        network
            .register_service::<messages::ClusterServiceMessage, _>(move |sender, message| {
                let discovery_manager = discovery_manager.clone();
                let node_id = node_id.clone();
                let state_manager = state_manager.clone();
                let membership_manager = membership_manager.clone();

                Box::pin(async move {
                    let response = match message {
                        messages::ClusterServiceMessage::Discovery { requester_id } => {
                            // Forward to discovery manager
                            discovery_manager
                                .handle_discovery_request(sender, requester_id)
                                .await
                        }
                        messages::ClusterServiceMessage::GlobalJoin {
                            requester_id,
                            requester_node,
                        } => {
                            // Handle join request
                            let request = messages::JoinRequest {
                                requester_id,
                                requester_node,
                            };
                            let response = Self::handle_join_request(
                                node_id,
                                state_manager,
                                membership_manager,
                                sender,
                                request,
                            )
                            .await;
                            messages::ClusterServiceResponse::GlobalJoin {
                                success: response.success,
                                error_message: response.error_message,
                                cluster_size: response.cluster_size,
                                current_term: response.current_term,
                                responder_id: response.responder_id,
                                current_leader: response.current_leader,
                            }
                        }
                        messages::ClusterServiceMessage::GroupJoin {
                            address: _,
                            capabilities: _,
                            node_id: _,
                        } => {
                            // TODO: Implement consensus group join request handling
                            messages::ClusterServiceResponse::GroupJoin {
                                accepted: false,
                                assigned_groups: vec![],
                                reason: Some("Not implemented yet".to_string()),
                            }
                        }
                        messages::ClusterServiceMessage::Heartbeat {
                            node_id: _,
                            state_hash: _,
                            timestamp: _,
                        } => {
                            // TODO: Implement heartbeat handling
                            messages::ClusterServiceResponse::HeartbeatAck
                        }
                    };
                    Ok(response)
                })
            })
            .await
            .map_err(|e| {
                ClusterError::Internal(format!("Failed to register service handler: {e}"))
            })?;

        info!("Successfully registered cluster service handlers");
        Ok(())
    }
}

impl<T, G> Drop for ClusterService<T, G>
where
    T: Transport,
    G: TopologyAdaptor,
{
    fn drop(&mut self) {
        // Ensure shutdown on drop
        self.shutdown_signal.notify_waiters();
    }
}

// Re-export config for convenience
pub use super::types::ClusterConfig;
