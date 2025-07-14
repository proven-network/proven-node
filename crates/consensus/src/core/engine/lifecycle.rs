//! Lifecycle management for the consensus system
//!
//! This module handles initialization, startup, shutdown, and health monitoring
//! of consensus components.

use crate::{
    ConsensusGroupId, NodeId,
    core::{
        engine::{
            cluster::ClusterManager, global_layer::GlobalConsensusLayer,
            groups_layer::GroupsConsensusLayer,
        },
        global::GlobalRequest,
    },
    error::{ConsensusResult, Error},
    network::{ClusterState, InitiatorReason},
};

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use proven_governance::Governance;
use proven_transport::Transport;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Lifecycle manager for consensus components
pub struct LifecycleManager<T, G>
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
    /// Cluster manager
    cluster_manager: Arc<ClusterManager<T, G>>,
    /// Component health status
    health_status: Arc<RwLock<ComponentHealth>>,
}

/// Health status of consensus components
#[derive(Debug, Clone, Default)]
pub struct ComponentHealth {
    /// Global consensus health
    pub global_consensus: HealthStatus,
    /// Groups consensus health
    pub groups_consensus: HealthStatus,
    /// Network health
    pub network: HealthStatus,
    /// Cluster health
    pub cluster: HealthStatus,
    /// Last health check time
    pub last_check: Option<Instant>,
}

/// Health status of a component
#[derive(Debug, Clone, PartialEq)]
pub enum HealthStatus {
    /// Component is healthy
    Healthy,
    /// Component is degraded but operational
    Degraded(String),
    /// Component is unhealthy
    Unhealthy(String),
    /// Component health is unknown
    Unknown,
}

impl Default for HealthStatus {
    fn default() -> Self {
        Self::Unknown
    }
}

impl<T, G> LifecycleManager<T, G>
where
    T: Transport,
    G: Governance,
{
    /// Create a new lifecycle manager
    pub fn new(
        node_id: NodeId,
        global_layer: Arc<GlobalConsensusLayer>,
        groups_layer: Arc<RwLock<GroupsConsensusLayer<T, G>>>,
        cluster_manager: Arc<ClusterManager<T, G>>,
    ) -> Self {
        Self {
            node_id,
            global_layer,
            groups_layer,
            cluster_manager,
            health_status: Arc::new(RwLock::new(ComponentHealth::default())),
        }
    }

    /// Start all consensus components
    pub async fn start_components(&self) -> ConsensusResult<()> {
        info!("Starting consensus components for node {}", self.node_id);

        // Start groups layer event processing
        self.groups_layer.write().await.start().await?;

        // Update health status
        self.update_health_status().await;

        info!("All consensus components started successfully");
        Ok(())
    }

    /// Stop all consensus components
    pub async fn stop_components(&self) -> ConsensusResult<()> {
        info!("Stopping consensus components for node {}", self.node_id);

        // Stop groups layer event processing
        self.groups_layer.write().await.stop().await?;

        // Update health status
        let mut health = self.health_status.write().await;
        health.global_consensus = HealthStatus::Unknown;
        health.groups_consensus = HealthStatus::Unknown;
        health.network = HealthStatus::Unknown;
        health.cluster = HealthStatus::Unknown;

        info!("All consensus components stopped");
        Ok(())
    }

    /// Initialize as a single-node cluster
    pub async fn initialize_single_node_cluster(
        &self,
        reason: InitiatorReason,
        submit_global_request: impl Fn(
            GlobalRequest,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = ConsensusResult<
                            crate::operations::handlers::GlobalOperationResponse,
                        >,
                    > + Send,
            >,
        >,
    ) -> ConsensusResult<()> {
        info!("Initializing single-node cluster (reason: {:?})", reason);

        self.cluster_manager
            .initialize_single_node(reason, submit_global_request)
            .await?;

        // Update health status
        self.update_health_status().await;

        Ok(())
    }

    /// Initialize a multi-node cluster
    pub async fn initialize_multi_node_cluster(
        &self,
        reason: InitiatorReason,
        expected_peers: Vec<NodeId>,
        submit_global_request: impl Fn(
            GlobalRequest,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = ConsensusResult<
                            crate::operations::handlers::GlobalOperationResponse,
                        >,
                    > + Send,
            >,
        >,
    ) -> ConsensusResult<()> {
        info!(
            "Initializing multi-node cluster with {} expected peers (reason: {:?})",
            expected_peers.len(),
            reason
        );

        // First initialize as single node
        self.initialize_single_node_cluster(reason, &submit_global_request)
            .await?;

        // Then wait for peers to join
        self.wait_for_nodes(expected_peers, Duration::from_secs(60))
            .await?;

        // Update health status
        self.update_health_status().await;

        Ok(())
    }

    /// Wait for specific nodes to join the cluster
    pub async fn wait_for_nodes(
        &self,
        expected_nodes: Vec<NodeId>,
        timeout: Duration,
    ) -> ConsensusResult<()> {
        let start = Instant::now();
        let mut remaining_nodes = expected_nodes;

        info!(
            "Waiting for {} nodes to join the cluster",
            remaining_nodes.len()
        );

        while !remaining_nodes.is_empty() && start.elapsed() < timeout {
            // Get current cluster members
            let metrics = self.global_layer.global_raft().metrics().borrow().clone();
            let current_members: Vec<NodeId> = metrics
                .membership_config
                .membership()
                .nodes()
                .map(|(id, _)| id.clone())
                .collect();

            // Remove nodes that have joined
            remaining_nodes.retain(|node| !current_members.contains(node));

            if remaining_nodes.is_empty() {
                info!("All expected nodes have joined the cluster");
                break;
            }

            debug!(
                "Still waiting for {} nodes to join: {:?}",
                remaining_nodes.len(),
                remaining_nodes
            );

            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        if !remaining_nodes.is_empty() {
            warn!(
                "Timeout waiting for nodes to join. Missing nodes: {:?}",
                remaining_nodes
            );
            return Err(Error::Timeout {
                seconds: timeout.as_secs(),
            });
        }

        Ok(())
    }

    /// Initialize a consensus group
    pub async fn initialize_group(
        &self,
        group_id: ConsensusGroupId,
        members: Vec<NodeId>,
    ) -> ConsensusResult<()> {
        info!(
            "Initializing consensus group {:?} with members: {:?}",
            group_id, members
        );

        // Check if we're a member
        if !members.contains(&self.node_id) {
            debug!(
                "Not a member of group {:?}, skipping initialization",
                group_id
            );
            return Ok(());
        }

        // Create the group in the groups layer
        self.groups_layer
            .write()
            .await
            .groups_manager()
            .write()
            .await
            .create_group(group_id, members)
            .await?;

        info!("Successfully initialized consensus group {:?}", group_id);
        Ok(())
    }

    /// Check health of all components
    pub async fn check_health(&self) -> ComponentHealth {
        self.update_health_status().await;
        self.health_status.read().await.clone()
    }

    /// Update health status of all components
    async fn update_health_status(&self) {
        let mut health = self.health_status.write().await;

        // Check global consensus health
        health.global_consensus = self.check_global_consensus_health().await;

        // Check groups consensus health
        health.groups_consensus = self.check_groups_consensus_health().await;

        // Check cluster health
        health.cluster = self.check_cluster_health().await;

        // Network health would need to be checked via NetworkManager
        // For now, assume healthy if we have an active cluster
        health.network = if matches!(health.cluster, HealthStatus::Healthy) {
            HealthStatus::Healthy
        } else {
            HealthStatus::Unknown
        };

        health.last_check = Some(Instant::now());
    }

    /// Check global consensus health
    async fn check_global_consensus_health(&self) -> HealthStatus {
        let metrics = self.global_layer.global_raft().metrics().borrow().clone();

        // Check if we have a leader
        if metrics.current_leader.is_none() {
            return HealthStatus::Unhealthy("No leader elected".to_string());
        }

        // Check if we're lagging
        if let Some(last_log_index) = metrics.last_log_index
            && let Some(committed) = metrics.last_applied
        {
            let last_index = last_log_index;
            let committed_index = committed.index;
            if last_index > committed_index + 100 {
                return HealthStatus::Degraded(format!(
                    "Lagging in applying logs: {} behind",
                    last_index - committed_index
                ));
            }
        }

        HealthStatus::Healthy
    }

    /// Check groups consensus health
    async fn check_groups_consensus_health(&self) -> HealthStatus {
        let groups = self.groups_layer.read().await.get_my_groups().await;

        if groups.is_empty() {
            return HealthStatus::Unknown;
        }

        let mut unhealthy_count = 0;
        let mut degraded_count = 0;

        // Check each group's health
        for group_id in groups.keys() {
            if let Ok(group) = self
                .groups_layer
                .read()
                .await
                .groups_manager()
                .read()
                .await
                .get_group(*group_id)
                .await
            {
                let metrics = group.raft.metrics().borrow().clone();

                if metrics.current_leader.is_none() {
                    unhealthy_count += 1;
                } else if let Some(last_log_index) = metrics.last_log_index
                    && let Some(committed) = metrics.last_applied
                    && last_log_index > committed.index + 50
                {
                    degraded_count += 1;
                }
            }
        }

        if unhealthy_count > 0 {
            HealthStatus::Unhealthy(format!("{unhealthy_count} groups without leader"))
        } else if degraded_count > 0 {
            HealthStatus::Degraded(format!("{degraded_count} groups lagging"))
        } else {
            HealthStatus::Healthy
        }
    }

    /// Check cluster health
    async fn check_cluster_health(&self) -> HealthStatus {
        let cluster_state = self.cluster_manager.cluster_state().read().await.clone();

        match cluster_state {
            ClusterState::Joined { cluster_size, .. } => {
                if cluster_size > 0 {
                    HealthStatus::Healthy
                } else {
                    HealthStatus::Degraded("Cluster size is 0".to_string())
                }
            }
            ClusterState::Initiator { .. } => HealthStatus::Healthy,
            ClusterState::Discovering => {
                HealthStatus::Degraded("Still discovering cluster".to_string())
            }
            ClusterState::TransportReady => {
                HealthStatus::Unhealthy("Transport ready but not joined to any cluster".to_string())
            }
            ClusterState::WaitingToJoin { .. } => {
                HealthStatus::Degraded("Waiting to join cluster".to_string())
            }
            ClusterState::Failed { error, .. } => {
                HealthStatus::Unhealthy(format!("Cluster initialization failed: {error}"))
            }
        }
    }

    /// Perform a graceful shutdown
    pub async fn graceful_shutdown(&self) -> ConsensusResult<()> {
        info!("Starting graceful shutdown for node {}", self.node_id);

        // Stop accepting new operations
        // This would need to be implemented in the components

        // Wait for pending operations to complete
        // TODO: Implement operation tracking

        // Leave consensus groups gracefully
        let groups = self.groups_layer.read().await.get_my_groups().await;
        for (group_id, _) in groups {
            info!("Leaving consensus group {:?}", group_id);
            // TODO: Implement leave group operation
        }

        // Stop components
        self.stop_components().await?;

        info!("Graceful shutdown complete");
        Ok(())
    }
}
