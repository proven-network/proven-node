//! Refactored orchestrator for hierarchical consensus system
//!
//! This module provides a cleaner orchestrator that:
//! - Uses dedicated views for read-only state access
//! - Leverages categorized operations for state changes
//! - Separates concerns between monitoring, allocation, and consensus management

use crate::{
    NodeId,
    allocation::ConsensusGroupId,
    config::{ConsensusConfig, HierarchicalConsensusConfig},
    error::{ConsensusResult, Error},
    global::{global_manager::GlobalManager, global_state::GlobalState},
    local::LocalConsensusManager,
    migration::MigrationCoordinator,
    operations::GlobalOperation,
    router::ConsensusRouter,
    views::{GroupView, NodeView, StreamManagementView},
};
use proven_attestation::Attestor;
use proven_governance::Governance;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

/// Configuration for view components
#[derive(Debug, Clone)]
pub struct ViewsConfig {
    /// Stream view configuration
    pub stream_view: StreamViewConfig,
    /// Group view configuration  
    pub group_view: GroupViewConfig,
    /// Node view configuration
    pub node_view: crate::views::node_view::NodeViewConfig,
}

/// Stream view configuration
#[derive(Debug, Clone)]
pub struct StreamViewConfig {
    /// Health check interval
    pub health_check_interval: std::time::Duration,
    /// Unhealthy threshold for message rate
    pub unhealthy_threshold: f64,
}

/// Group view configuration
#[derive(Debug, Clone)]
pub struct GroupViewConfig {
    /// Minimum nodes for healthy group
    pub min_nodes_for_health: usize,
    /// Maximum load imbalance ratio
    pub max_load_imbalance: f64,
}

impl Default for ViewsConfig {
    fn default() -> Self {
        Self {
            stream_view: StreamViewConfig {
                health_check_interval: std::time::Duration::from_secs(30),
                unhealthy_threshold: 0.1,
            },
            group_view: GroupViewConfig {
                min_nodes_for_health: 2,
                max_load_imbalance: 3.0,
            },
            node_view: Default::default(),
        }
    }
}

/// Refactored orchestrator with clear separation of concerns
pub struct Orchestrator<G, A>
where
    G: Governance + Send + Sync + 'static,
    A: Attestor + Send + Sync + 'static,
{
    /// Configuration
    _config: HierarchicalConsensusConfig,
    /// View configuration
    _views_config: ViewsConfig,

    // Core components
    /// Global consensus manager
    global_manager: Arc<GlobalManager<G, A>>,
    /// Global state reference
    global_state: Arc<GlobalState>,
    /// Local consensus manager
    _local_manager: Arc<LocalConsensusManager<G, A>>,
    /// Consensus router (for now, keep using existing router)
    router: Arc<ConsensusRouter<G, A>>,
    /// Migration coordinator
    _migration_coordinator: Arc<MigrationCoordinator<G, A>>,

    // View components
    /// Stream view for stream health and status
    stream_view: Arc<StreamManagementView>,
    /// Group view for group health and capacity
    group_view: Arc<GroupView>,
    /// Node view for node status and capacity
    node_view: Arc<NodeView>,

    /// Node ID
    node_id: NodeId,
    /// Background task handles
    background_tasks: Vec<JoinHandle<()>>,
    /// Shutdown signal
    shutdown: Arc<tokio::sync::Notify>,
}

impl<G, A> Orchestrator<G, A>
where
    G: Governance + Send + Sync + 'static,
    A: Attestor + Send + Sync + 'static,
{
    /// Create a new orchestrator with improved architecture
    pub async fn new(
        base_config: ConsensusConfig<G, A>,
        hierarchical_config: HierarchicalConsensusConfig,
        views_config: ViewsConfig,
        global_manager: Arc<GlobalManager<G, A>>,
        global_state: Arc<GlobalState>,
        node_id: NodeId,
    ) -> ConsensusResult<Self> {
        info!("Initializing orchestrator");

        // Create local storage factory
        let local_storage_factory = crate::local::group_storage::create_local_storage_factory(
            &hierarchical_config.local.storage_config,
        )?;

        // Create local consensus manager
        let local_manager = Arc::new(LocalConsensusManager::with_storage_factory_and_backend(
            node_id.clone(),
            hierarchical_config.local.base_raft_config.clone(),
            global_manager.clone(),
            local_storage_factory,
            base_config.stream_storage_backend.clone(),
        ));

        // Create allocation manager (temporary - will be replaced with GroupSelector)
        let allocation_manager = Arc::new(RwLock::new(crate::allocation::AllocationManager::new(
            hierarchical_config.allocation.strategy.clone(),
        )));

        // Create router
        let router = Arc::new(ConsensusRouter::new(
            global_manager.clone(),
            local_manager.clone(),
            allocation_manager,
        ));

        // Create migration coordinator
        let migration_coordinator = Arc::new(MigrationCoordinator::new(router.clone()));

        // Create view components
        let stream_view = Arc::new(StreamManagementView::new(global_state.clone()));
        let group_view = Arc::new(GroupView::new(
            global_state.clone(),
            views_config.group_view.min_nodes_for_health,
        ));
        let node_view = Arc::new(NodeView::new(
            global_state.clone(),
            views_config.node_view.clone(),
        ));

        let shutdown = Arc::new(tokio::sync::Notify::new());
        let mut background_tasks = Vec::new();

        // Start health monitoring task
        let health_task = Self::start_health_monitoring_task(
            stream_view.clone(),
            group_view.clone(),
            node_view.clone(),
            views_config.stream_view.health_check_interval,
            shutdown.clone(),
        );
        background_tasks.push(health_task);

        // Start group management task
        let group_task = Self::start_group_management_task(
            global_state.clone(),
            local_manager.clone(),
            node_view.clone(),
            router.clone(),
            node_id.clone(),
            shutdown.clone(),
        );
        background_tasks.push(group_task);

        // Start rebalancing task if enabled
        if hierarchical_config.allocation.rebalancing.enabled {
            let rebalancing_task = Self::start_rebalancing_task(
                stream_view.clone(),
                group_view.clone(),
                migration_coordinator.clone(),
                hierarchical_config.allocation.rebalancing.clone(),
                shutdown.clone(),
            );
            background_tasks.push(rebalancing_task);
        }

        Ok(Self {
            _config: hierarchical_config,
            _views_config: views_config,
            global_manager,
            global_state,
            _local_manager: local_manager,
            router,
            _migration_coordinator: migration_coordinator,
            stream_view,
            group_view,
            node_view,
            node_id,
            background_tasks,
            shutdown,
        })
    }

    /// Start health monitoring background task
    fn start_health_monitoring_task(
        stream_view: Arc<StreamManagementView>,
        group_view: Arc<GroupView>,
        node_view: Arc<NodeView>,
        interval: std::time::Duration,
        shutdown: Arc<tokio::sync::Notify>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(interval);

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        // Monitor stream health
                        stream_view.monitor_stream_health().await;

                        // Monitor group health
                        group_view.monitor_group_health().await;

                        // Monitor node health
                        node_view.monitor_health().await;

                        // Check for issues and log warnings
                        let unhealthy_streams = stream_view.get_unhealthy_streams().await;
                        if !unhealthy_streams.is_empty() {
                            warn!("Found {} unhealthy streams", unhealthy_streams.len());
                        }

                        let failed_groups = group_view.get_failed_groups().await;
                        if !failed_groups.is_empty() {
                            warn!("Found {} failed groups", failed_groups.len());
                        }

                        let failed_nodes = node_view.get_failed_nodes().await;
                        if !failed_nodes.is_empty() {
                            warn!("Found {} failed nodes", failed_nodes.len());
                        }
                    }
                    _ = shutdown.notified() => {
                        info!("Health monitoring task shutting down");
                        break;
                    }
                }
            }
        })
    }

    /// Start group management background task
    fn start_group_management_task(
        global_state: Arc<GlobalState>,
        local_manager: Arc<LocalConsensusManager<G, A>>,
        node_view: Arc<NodeView>,
        router: Arc<ConsensusRouter<G, A>>,
        node_id: NodeId,
        shutdown: Arc<tokio::sync::Notify>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));

            // Do initial sync immediately
            info!("Performing initial group sync for node {}", node_id);

            // Get initial groups this node belongs to
            let initial_groups = global_state.get_node_groups(&node_id).await;
            for group_info in &initial_groups {
                // Register group with allocation manager
                if let Err(e) = router
                    .register_consensus_group(group_info.id, group_info.members.clone())
                    .await
                {
                    error!(
                        "Failed to register group {:?} with allocation manager: {}",
                        group_info.id, e
                    );
                }

                // Update node view
                if let Err(e) = node_view.add_node_to_group(&node_id, group_info.id).await {
                    error!("Failed to update node view: {}", e);
                }

                // Create local consensus instance if needed
                if let Err(e) = local_manager
                    .create_group(group_info.id, group_info.members.clone())
                    .await
                {
                    error!(
                        "Failed to create local consensus group {:?}: {}",
                        group_info.id, e
                    );
                }
            }
            let mut known_groups: std::collections::HashSet<ConsensusGroupId> =
                initial_groups.into_iter().map(|g| g.id).collect();

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        // Get groups this node belongs to
                        let node_groups = global_state.get_node_groups(&node_id).await;
                        let current_groups: std::collections::HashSet<ConsensusGroupId> =
                            node_groups.into_iter().map(|g| g.id).collect();

                        // Find new groups
                        let new_groups: Vec<_> = current_groups.difference(&known_groups).cloned().collect();

                        // Find removed groups
                        let removed_groups: Vec<_> = known_groups.difference(&current_groups).cloned().collect();

                        // Handle new groups
                        for group_id in new_groups {
                            info!("Node assigned to new group {:?}", group_id);

                            if let Some(group_info) = global_state.get_group(group_id).await {
                                // Register group with allocation manager
                                if let Err(e) = router.register_consensus_group(
                                    group_id,
                                    group_info.members.clone(),
                                ).await {
                                    error!("Failed to register group {:?} with allocation manager: {}", group_id, e);
                                }

                                // Update node view
                                if let Err(e) = node_view.add_node_to_group(&node_id, group_id).await {
                                    error!("Failed to update node view: {}", e);
                                }

                                // Create local consensus instance
                                if let Err(e) = local_manager.create_group(
                                    group_id,
                                    group_info.members.clone(),
                                ).await {
                                    error!("Failed to create local consensus group {:?}: {}", group_id, e);
                                }
                            }
                        }

                        // Handle removed groups
                        for group_id in removed_groups {
                            info!("Node removed from group {:?}", group_id);

                            // Unregister group from allocation manager
                            if let Err(e) = router.unregister_consensus_group(group_id).await {
                                error!("Failed to unregister group {:?} from allocation manager: {}", group_id, e);
                            }

                            // Update node view
                            if let Err(e) = node_view.remove_node_from_group(&node_id, group_id).await {
                                error!("Failed to update node view: {}", e);
                            }

                            // Remove local consensus instance
                            if let Err(e) = local_manager.remove_group(group_id).await {
                                error!("Failed to remove local consensus group {:?}: {}", group_id, e);
                            }
                        }

                        known_groups = current_groups;
                    }
                    _ = shutdown.notified() => {
                        info!("Group management task shutting down");
                        break;
                    }
                }
            }
        })
    }

    /// Start rebalancing background task
    fn start_rebalancing_task(
        stream_view: Arc<StreamManagementView>,
        group_view: Arc<GroupView>,
        migration_coordinator: Arc<MigrationCoordinator<G, A>>,
        config: crate::config::RebalancingConfig,
        shutdown: Arc<tokio::sync::Notify>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(config.check_interval);
            let mut last_rebalance = std::time::Instant::now();

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        // Check cooldown
                        if last_rebalance.elapsed() < config.cooldown_period {
                            continue;
                        }

                        // Check if rebalancing is needed
                        let load_imbalance = group_view.calculate_load_imbalance().await;
                        if load_imbalance > config.max_imbalance_ratio {
                            info!("Load imbalance {} exceeds threshold, checking for migrations", load_imbalance);

                            // Get migration recommendations
                            let recommendations = group_view.get_rebalancing_recommendations(
                                1,  // min_streams_to_migrate
                                5,  // max_migrations_per_group
                            ).await;

                            // Get active migrations
                            let active_migrations = migration_coordinator.get_active_migrations().await;
                            let available_slots = config.max_concurrent_migrations.saturating_sub(active_migrations.len());

                            // Start migrations
                            for recommendation in recommendations.iter().take(available_slots) {
                                // Validate stream exists and is healthy
                                if let Some(health) = stream_view.get_stream_health(&recommendation.stream_name).await {
                                    if health.is_healthy {
                                        info!("Starting migration of '{}' from {:?} to {:?}",
                                            recommendation.stream_name,
                                            recommendation.from_group,
                                            recommendation.to_group
                                        );

                                        if let Err(e) = migration_coordinator.start_migration(
                                            recommendation.stream_name.clone(),
                                            recommendation.from_group,
                                            recommendation.to_group,
                                        ).await {
                                            error!("Failed to start migration: {}", e);
                                        }
                                    } else {
                                        warn!("Skipping migration of unhealthy stream '{}'", recommendation.stream_name);
                                    }
                                }
                            }

                            if !recommendations.is_empty() {
                                last_rebalance = std::time::Instant::now();
                            }
                        }
                    }
                    _ = shutdown.notified() => {
                        info!("Rebalancing task shutting down");
                        break;
                    }
                }
            }
        })
    }

    // Operation handling methods using new categorized operations

    /// Create a new stream using the new operation structure
    pub async fn create_stream(
        &self,
        name: String,
        config: crate::global::StreamConfig,
    ) -> ConsensusResult<ConsensusGroupId> {
        // Note: Stream creation validation is handled by the StreamManagementOperationValidator
        // in the global consensus layer when the operation is submitted

        // Select target group
        let target_group = self.select_group_for_stream(&name, &config).await?;

        // Use the new operations structure
        self.global_manager
            .create_stream(name.clone(), config, target_group)
            .await?;

        info!("Created stream '{}' in group {:?}", name, target_group);
        Ok(target_group)
    }

    /// Add a new consensus group
    pub async fn add_consensus_group(
        &self,
        members: Vec<NodeId>,
    ) -> ConsensusResult<ConsensusGroupId> {
        // Find next available group ID
        let existing_groups = self.global_state.get_all_groups().await;
        let next_id = existing_groups
            .iter()
            .map(|g| g.id.0)
            .max()
            .map(|max| max + 1)
            .unwrap_or(0);

        let group_id = ConsensusGroupId::new(next_id);

        // Validate group creation
        self.group_view
            .validate_group_creation(group_id, &members)
            .await?;

        // Use the new operations structure
        self.global_manager.create_group(group_id, members).await?;

        info!("Added consensus group {:?}", group_id);
        Ok(group_id)
    }

    /// Remove a node from the cluster
    pub async fn decommission_node(&self, node_id: NodeId) -> ConsensusResult<()> {
        // Use the new decommission operation
        let operation =
            GlobalOperation::Node(crate::operations::node_ops::NodeOperation::Decommission {
                node_id: node_id.clone(),
            });

        self.global_manager.submit_operation(operation).await?;

        info!("Decommissioned node {}", node_id);
        Ok(())
    }

    // Helper methods

    /// Select a consensus group for a new stream
    async fn select_group_for_stream(
        &self,
        _stream_name: &str,
        _config: &crate::global::StreamConfig,
    ) -> ConsensusResult<ConsensusGroupId> {
        // For now, use simple least-loaded strategy
        // This will be replaced with GroupSelector trait in Phase 3

        let groups = self.group_view.list_healthy_groups().await;
        if groups.is_empty() {
            return Err(Error::InvalidOperation(
                "No healthy groups available".to_string(),
            ));
        }

        // Find least loaded group
        let loads = self.group_view.get_group_loads().await;
        let least_loaded = loads
            .into_iter()
            .filter(|(id, _)| groups.contains(id))
            .min_by_key(|(_, load)| load.stream_count)
            .map(|(id, _)| id)
            .ok_or_else(|| Error::InvalidOperation("No suitable group found".to_string()))?;

        Ok(least_loaded)
    }

    /// Get system status using views
    pub async fn get_status(&self) -> SystemStatus {
        let stream_health = self.stream_view.get_all_stream_health().await;
        let group_health = self.group_view.get_all_group_health().await;
        let node_statuses = self.node_view.list_online_nodes().await;

        let unhealthy_streams = stream_health.values().filter(|h| !h.is_healthy).count();
        let unhealthy_groups = group_health.values().filter(|h| !h.is_healthy).count();

        SystemStatus {
            total_streams: stream_health.len(),
            unhealthy_streams,
            total_groups: group_health.len(),
            unhealthy_groups,
            online_nodes: node_statuses.len(),
            load_imbalance: self.group_view.calculate_load_imbalance().await,
        }
    }

    /// Shutdown the orchestrator
    pub async fn shutdown(mut self) -> ConsensusResult<()> {
        info!("Shutting down orchestrator");

        // Signal shutdown
        self.shutdown.notify_waiters();

        // Wait for tasks
        for task in self.background_tasks.drain(..) {
            if let Err(e) = task.await {
                error!("Background task failed during shutdown: {}", e);
            }
        }

        info!("Orchestrator shutdown complete");
        Ok(())
    }

    /// Get access to the consensus router
    pub fn router(&self) -> &Arc<ConsensusRouter<G, A>> {
        &self.router
    }

    /// Force sync of consensus groups with allocation manager
    /// This is useful for testing or when immediate sync is needed
    pub async fn sync_consensus_groups(&self) -> ConsensusResult<()> {
        let node_groups = self.global_state.get_node_groups(&self.node_id).await;

        for group_info in node_groups {
            // Register group with allocation manager
            self.router
                .register_consensus_group(group_info.id, group_info.members.clone())
                .await?;
        }

        Ok(())
    }
}

/// System status from the new orchestrator
#[derive(Debug, Clone)]
pub struct SystemStatus {
    /// Total number of streams
    pub total_streams: usize,
    /// Number of unhealthy streams
    pub unhealthy_streams: usize,
    /// Total number of groups
    pub total_groups: usize,
    /// Number of unhealthy groups
    pub unhealthy_groups: usize,
    /// Number of online nodes
    pub online_nodes: usize,
    /// Current load imbalance ratio
    pub load_imbalance: f64,
}
