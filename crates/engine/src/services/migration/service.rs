//! Main migration service implementation

use std::collections::HashMap;
use std::sync::Arc;

use proven_logger::{error, info, warn};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

use crate::foundation::ConsensusGroupId;
use proven_topology::NodeId;

use super::allocator::{GroupAllocation, GroupAllocator};
use super::node_migration::{NodeMigrationCoordinator, NodeMigrationProgress};
use super::rebalancer::{Rebalancer, RebalancingRecommendation};
use super::stream_migration::{StreamMigrationCoordinator, StreamMigrationProgress};
use super::types::*;

/// Migration service for managing data and node migrations
pub struct MigrationService {
    /// Service configuration
    config: MigrationConfig,

    /// Stream migration coordinator
    stream_coordinator: Arc<StreamMigrationCoordinator>,

    /// Node migration coordinator
    node_coordinator: Arc<NodeMigrationCoordinator>,

    /// Group allocator
    allocator: Arc<GroupAllocator>,

    /// Rebalancer
    rebalancer: Arc<Rebalancer>,

    /// Active migrations tracking
    active_migrations: Arc<RwLock<ActiveMigrations>>,

    /// Background tasks
    background_tasks: Arc<RwLock<Vec<JoinHandle<()>>>>,

    /// Shutdown signal
    shutdown: Arc<tokio::sync::Notify>,

    /// Service state
    state: Arc<RwLock<ServiceState>>,
}

/// Active migrations tracking
struct ActiveMigrations {
    /// Active stream migrations
    streams: HashMap<String, StreamMigrationProgress>,

    /// Active node migrations
    nodes: HashMap<NodeId, NodeMigrationProgress>,

    /// Active rebalancing plans
    plans: HashMap<String, RebalancingPlan>,
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

impl MigrationService {
    /// Create a new migration service
    pub fn new(config: MigrationConfig) -> Self {
        let stream_coordinator = Arc::new(StreamMigrationCoordinator::new(
            config.stream_migration.clone(),
        ));

        let node_coordinator =
            Arc::new(NodeMigrationCoordinator::new(config.node_migration.clone()));

        let allocator = Arc::new(GroupAllocator::new(config.allocation.clone()));

        let rebalancer = Arc::new(Rebalancer::new(
            config.rebalancing.clone(),
            allocator.clone(),
        ));

        Self {
            config,
            stream_coordinator,
            node_coordinator,
            allocator,
            rebalancer,
            active_migrations: Arc::new(RwLock::new(ActiveMigrations {
                streams: HashMap::new(),
                nodes: HashMap::new(),
                plans: HashMap::new(),
            })),
            background_tasks: Arc::new(RwLock::new(Vec::new())),
            shutdown: Arc::new(tokio::sync::Notify::new()),
            state: Arc::new(RwLock::new(ServiceState::NotStarted)),
        }
    }

    /// Start the migration service
    pub async fn start(&self) -> MigrationResult<()> {
        let mut state = self.state.write().await;
        match *state {
            ServiceState::NotStarted | ServiceState::Stopped => {
                *state = ServiceState::Running;
            }
            _ => {
                return Err(MigrationError::InvalidState(format!(
                    "Service cannot be started from {:?} state",
                    *state
                )));
            }
        }
        drop(state);

        info!("Starting migration service");

        let mut tasks = self.background_tasks.write().await;

        // Start rebalancing monitor if enabled
        if self.config.enable_auto_rebalancing {
            tasks.push(self.spawn_rebalancing_monitor());
        }

        // Start migration progress monitor
        tasks.push(self.spawn_progress_monitor());

        Ok(())
    }

    /// Stop the migration service
    pub async fn stop(&self) -> MigrationResult<()> {
        let mut state = self.state.write().await;
        if *state != ServiceState::Running {
            return Ok(());
        }

        *state = ServiceState::Stopping;
        drop(state);

        info!("Stopping migration service");

        // Signal shutdown
        self.shutdown.notify_waiters();

        // Wait for tasks to complete
        let mut tasks = self.background_tasks.write().await;
        for task in tasks.drain(..) {
            if let Err(e) = task.await {
                warn!("Error stopping migration task: {e}");
            }
        }

        let mut state = self.state.write().await;
        *state = ServiceState::Stopped;

        Ok(())
    }

    // Stream Migration Methods

    /// Start a stream migration
    pub async fn start_stream_migration(
        &self,
        stream_name: String,
        source_group: ConsensusGroupId,
        target_group: ConsensusGroupId,
    ) -> MigrationResult<()> {
        self.ensure_running().await?;

        // Check if migration already exists
        let active = self.active_migrations.read().await;
        if active.streams.contains_key(&stream_name) {
            return Err(MigrationError::AlreadyInProgress(stream_name));
        }
        drop(active);

        info!("Starting stream migration: {stream_name} from {source_group:?} to {target_group:?}");

        // Start migration
        let progress = self
            .stream_coordinator
            .start_migration(stream_name.clone(), source_group, target_group)
            .await?;

        // Track migration
        let mut active = self.active_migrations.write().await;
        active.streams.insert(stream_name, progress);

        Ok(())
    }

    /// Get stream migration progress
    pub async fn get_stream_migration_progress(
        &self,
        stream_name: &str,
    ) -> MigrationResult<Option<StreamMigrationProgress>> {
        let active = self.active_migrations.read().await;
        Ok(active.streams.get(stream_name).cloned())
    }

    /// Cancel a stream migration
    pub async fn cancel_stream_migration(&self, stream_name: &str) -> MigrationResult<()> {
        self.ensure_running().await?;

        self.stream_coordinator
            .cancel_migration(stream_name)
            .await?;

        let mut active = self.active_migrations.write().await;
        active.streams.remove(stream_name);

        Ok(())
    }

    /// Get all active stream migrations
    pub async fn get_active_stream_migrations(&self) -> MigrationResult<Vec<String>> {
        let active = self.active_migrations.read().await;
        Ok(active.streams.keys().cloned().collect())
    }

    // Node Migration Methods

    /// Start a node migration
    pub async fn start_node_migration(
        &self,
        node_id: NodeId,
        source_group: ConsensusGroupId,
        target_group: ConsensusGroupId,
    ) -> MigrationResult<()> {
        self.ensure_running().await?;

        // Check if migration already exists
        let active = self.active_migrations.read().await;
        if active.nodes.contains_key(&node_id) {
            return Err(MigrationError::AlreadyInProgress(node_id.to_string()));
        }
        drop(active);

        info!("Starting node migration: {node_id} from {source_group:?} to {target_group:?}");

        // Start migration
        let progress = self
            .node_coordinator
            .start_migration(node_id.clone(), source_group, target_group)
            .await?;

        // Track migration
        let mut active = self.active_migrations.write().await;
        active.nodes.insert(node_id, progress);

        Ok(())
    }

    /// Get node migration progress
    pub async fn get_node_migration_progress(
        &self,
        node_id: &NodeId,
    ) -> MigrationResult<Option<NodeMigrationProgress>> {
        let active = self.active_migrations.read().await;
        Ok(active.nodes.get(node_id).cloned())
    }

    /// Complete a node migration
    pub async fn complete_node_migration(&self, node_id: &NodeId) -> MigrationResult<()> {
        self.ensure_running().await?;

        self.node_coordinator.complete_migration(node_id).await?;

        let mut active = self.active_migrations.write().await;
        active.nodes.remove(node_id);

        Ok(())
    }

    // Allocation Methods

    /// Allocate groups for a new node
    pub async fn allocate_node_to_groups(
        &self,
        node_id: &NodeId,
    ) -> MigrationResult<Vec<ConsensusGroupId>> {
        self.ensure_running().await?;
        self.allocator.allocate_groups_for_node(node_id).await
    }

    /// Deallocate node from groups
    pub async fn deallocate_node_from_groups(
        &self,
        node_id: &NodeId,
    ) -> MigrationResult<Vec<ConsensusGroupId>> {
        self.ensure_running().await?;
        self.allocator.remove_node_from_groups(node_id).await
    }

    /// Get current allocations
    pub async fn get_allocations(&self) -> MigrationResult<GroupAllocation> {
        self.allocator.get_current_allocation().await
    }

    // Rebalancing Methods

    /// Check if rebalancing is needed
    pub async fn needs_rebalancing(&self) -> MigrationResult<bool> {
        self.rebalancer.needs_rebalancing().await
    }

    /// Generate a rebalancing plan
    pub async fn generate_rebalancing_plan(&self) -> MigrationResult<Option<RebalancingPlan>> {
        self.ensure_running().await?;
        self.rebalancer.generate_plan().await
    }

    /// Execute a rebalancing plan
    pub async fn execute_rebalancing_plan(&self, plan_id: &str) -> MigrationResult<()> {
        self.ensure_running().await?;

        let active = self.active_migrations.read().await;
        let plan = active
            .plans
            .get(plan_id)
            .ok_or_else(|| MigrationError::NotFound(plan_id.to_string()))?
            .clone();
        drop(active);

        // Execute stream migrations
        for migration in &plan.stream_migrations {
            self.start_stream_migration(
                migration.stream_name.clone(),
                migration.from_group,
                migration.to_group,
            )
            .await?;
        }

        // Execute node migrations
        for migration in &plan.node_migrations {
            self.start_node_migration(
                migration.node_id.clone(),
                migration.from_group,
                migration.to_group,
            )
            .await?;
        }

        Ok(())
    }

    /// Get recommendations for migrations
    pub async fn get_recommendations(&self) -> MigrationResult<Vec<RebalancingRecommendation>> {
        self.rebalancer.get_recommendations().await
    }

    // Private helper methods

    /// Ensure service is running
    async fn ensure_running(&self) -> MigrationResult<()> {
        let state = self.state.read().await;
        if *state != ServiceState::Running {
            return Err(MigrationError::NotStarted);
        }
        Ok(())
    }

    /// Spawn rebalancing monitor task
    fn spawn_rebalancing_monitor(&self) -> JoinHandle<()> {
        let rebalancer = self.rebalancer.clone();
        let active_migrations = self.active_migrations.clone();
        let interval = self.config.rebalancing.check_interval;
        let shutdown = self.shutdown.clone();

        tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);
            interval_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    _ = interval_timer.tick() => {
                        match rebalancer.check_and_suggest().await {
                            Ok(Some(plan)) => {
                                let mut active = active_migrations.write().await;
                                active.plans.insert(plan.id.clone(), plan);
                            }
                            Ok(None) => {},
                            Err(e) => {
                                error!("Rebalancing check failed: {e}");
                            }
                        }
                    }
                    _ = shutdown.notified() => {
                        info!("Rebalancing monitor shutting down");
                        break;
                    }
                }
            }
        })
    }

    /// Spawn progress monitor task
    fn spawn_progress_monitor(&self) -> JoinHandle<()> {
        let _stream_coordinator = self.stream_coordinator.clone();
        let _node_coordinator = self.node_coordinator.clone();
        let active_migrations = self.active_migrations.clone();
        let shutdown = self.shutdown.clone();

        tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(tokio::time::Duration::from_secs(5));
            interval_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    _ = interval_timer.tick() => {
                        // Update stream migration progress
                        let mut active = active_migrations.write().await;

                        // Remove completed stream migrations
                        let completed_streams: Vec<String> = active.streams.iter()
                            .filter_map(|(name, progress)| {
                                if matches!(progress.state, StreamMigrationState::Completed |
                                          StreamMigrationState::Failed { .. } |
                                          StreamMigrationState::Cancelled) {
                                    Some(name.clone())
                                } else {
                                    None
                                }
                            })
                            .collect();

                        for name in completed_streams {
                            active.streams.remove(&name);
                        }

                        // Remove completed node migrations
                        let completed_nodes: Vec<NodeId> = active.nodes.iter()
                            .filter_map(|(id, progress)| {
                                if matches!(progress.state, NodeMigrationState::Completed |
                                          NodeMigrationState::Failed { .. }) {
                                    Some(id.clone())
                                } else {
                                    None
                                }
                            })
                            .collect();

                        for id in completed_nodes {
                            active.nodes.remove(&id);
                        }
                    }
                    _ = shutdown.notified() => {
                        info!("Progress monitor shutting down");
                        break;
                    }
                }
            }
        })
    }
}

impl Drop for MigrationService {
    fn drop(&mut self) {
        // Ensure shutdown on drop
        self.shutdown.notify_waiters();
    }
}

// Re-export config for convenience
pub use super::types::MigrationConfig;
