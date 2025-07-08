//! Orchestrator for hierarchical consensus system
//!
//! This module provides the main orchestrator that manages the hierarchical
//! consensus system based on the provided configuration.

use crate::{
    NodeId,
    allocation::{AllocationManager, ConsensusGroupId},
    config::{ConsensusConfig, HierarchicalConsensusConfig},
    error::{ConsensusResult, Error},
    global::GlobalManager,
    local::LocalConsensusManager,
    migration::MigrationCoordinator,
    router::ConsensusRouter,
};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

/// Orchestrator for the hierarchical consensus system
pub struct HierarchicalOrchestrator<G, A>
where
    G: proven_governance::Governance + Send + Sync + 'static,
    A: proven_attestation::Attestor + Send + Sync + 'static,
{
    /// Configuration
    config: HierarchicalConsensusConfig,
    /// Consensus router
    router: Arc<ConsensusRouter<G, A>>,
    /// Migration coordinator
    migration_coordinator: Arc<MigrationCoordinator<G, A>>,
    /// Background task handles
    background_tasks: Vec<JoinHandle<()>>,
    /// Shutdown signal
    shutdown: Arc<tokio::sync::Notify>,
}

impl<G, A> HierarchicalOrchestrator<G, A>
where
    G: proven_governance::Governance + Send + Sync + 'static,
    A: proven_attestation::Attestor + Send + Sync + 'static,
{
    /// Create a new hierarchical orchestrator
    pub async fn new(
        _base_config: ConsensusConfig<G, A>,
        hierarchical_config: HierarchicalConsensusConfig,
        global_manager: Arc<GlobalManager<G, A>>,
        node_id: NodeId,
    ) -> ConsensusResult<Self> {
        info!("Initializing hierarchical consensus orchestrator");

        // Create storage factory based on configuration
        let storage_factory = crate::local::storage::factory::create_storage_factory(
            &hierarchical_config.local.storage_config,
        )?;

        // Create local consensus manager with storage factory
        let local_manager = Arc::new(LocalConsensusManager::with_storage_factory(
            node_id.clone(),
            hierarchical_config.local.base_raft_config.clone(),
            global_manager.clone(),
            storage_factory.into(),
        ));

        // Create allocation manager
        let allocation_manager = Arc::new(RwLock::new(AllocationManager::new(
            hierarchical_config.allocation.strategy.clone(),
        )));

        // Create router
        let router = Arc::new(ConsensusRouter::new(
            global_manager,
            local_manager.clone(),
            allocation_manager.clone(),
        ));

        // Create migration coordinator
        let migration_coordinator = Arc::new(MigrationCoordinator::new(router.clone()));

        let shutdown = Arc::new(tokio::sync::Notify::new());
        let mut background_tasks = Vec::new();

        // Start monitoring service if enabled
        if hierarchical_config.monitoring.enabled {
            let monitoring_task = Self::start_monitoring_task(
                router.clone(),
                migration_coordinator.clone(),
                hierarchical_config.monitoring.update_interval,
                shutdown.clone(),
            );
            background_tasks.push(monitoring_task);
        }

        // Start rebalancing service if enabled
        if hierarchical_config.allocation.rebalancing.enabled {
            let rebalancing_task = Self::start_rebalancing_task(
                router.clone(),
                migration_coordinator.clone(),
                hierarchical_config.allocation.rebalancing.clone(),
                shutdown.clone(),
            );
            background_tasks.push(rebalancing_task);
        }

        // Initialize local consensus groups
        Self::initialize_local_groups(&local_manager, &allocation_manager, &hierarchical_config)
            .await?;

        Ok(Self {
            config: hierarchical_config,
            router,
            migration_coordinator,
            background_tasks,
            shutdown,
        })
    }

    /// Create a test NodeId with a valid ed25519 key
    fn create_test_node_id(seed: u32) -> crate::NodeId {
        use ed25519_dalek::{SigningKey, VerifyingKey};
        let mut key_bytes = [0u8; 32];
        // Use the seed to create different keys
        key_bytes[..4].copy_from_slice(&seed.to_le_bytes());
        let signing_key = SigningKey::from_bytes(&key_bytes);
        let verifying_key: VerifyingKey = signing_key.verifying_key();
        crate::NodeId::new(verifying_key)
    }

    /// Initialize local consensus groups based on configuration
    async fn initialize_local_groups(
        _local_manager: &Arc<LocalConsensusManager<G, A>>,
        allocation_manager: &Arc<RwLock<AllocationManager>>,
        config: &HierarchicalConsensusConfig,
    ) -> ConsensusResult<()> {
        info!(
            "Initializing {} local consensus groups",
            config.local.initial_groups
        );

        let mut alloc_mgr = allocation_manager.write().await;

        // In a production system, this would:
        // 1. Submit AddConsensusGroup operations to global consensus
        // 2. Wait for global consensus to confirm group creation
        // 3. Query global consensus for group membership
        // 4. Join groups this node is assigned to

        // For now, create initial groups locally as a bootstrap mechanism
        // Real nodes would discover groups from global consensus
        for i in 0..config.local.initial_groups {
            let group_id = ConsensusGroupId::new(i);

            // Get group-specific configuration if available
            let _group_config = config.local.group_overrides.get(&group_id);

            // Register with allocation manager
            // TODO: Replace with actual node IDs from topology
            // For now, create placeholder NodeIds using valid ed25519 keys
            let members: Vec<crate::NodeId> = vec![
                Self::create_test_node_id(i * 3 + 1),
                Self::create_test_node_id(i * 3 + 2),
                Self::create_test_node_id(i * 3 + 3),
            ];
            alloc_mgr.add_group(group_id, members);

            info!(
                "Registered local consensus group {} with allocation manager",
                i
            );
        }

        Ok(())
    }

    /// Start monitoring background task
    fn start_monitoring_task(
        router: Arc<ConsensusRouter<G, A>>,
        migration_coordinator: Arc<MigrationCoordinator<G, A>>,
        update_interval: std::time::Duration,
        shutdown: Arc<tokio::sync::Notify>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(update_interval);

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        // Get active migrations
                        let active_migrations = migration_coordinator.get_active_migrations().await;

                        // Update metrics
                        router.update_monitoring_metrics(active_migrations).await;

                        // Check for alerts
                        let metrics = router.get_monitoring_metrics().await;
                        if metrics.distribution_stats.load_imbalance_ratio > 3.0 {
                            warn!("High load imbalance detected: {:.2}",
                                  metrics.distribution_stats.load_imbalance_ratio);
                        }
                    }
                    _ = shutdown.notified() => {
                        info!("Monitoring task shutting down");
                        break;
                    }
                }
            }
        })
    }

    /// Start rebalancing background task
    fn start_rebalancing_task(
        router: Arc<ConsensusRouter<G, A>>,
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
                        // Check if we're in cooldown
                        if last_rebalance.elapsed() < config.cooldown_period {
                            continue;
                        }

                        // Check if rebalancing is needed
                        if router.should_rebalance().await {
                            info!("Rebalancing needed, checking recommendations");

                            let recommendations = router.get_rebalancing_recommendations().await;
                            let active_migrations = migration_coordinator.get_active_migrations().await;

                            // Limit concurrent migrations
                            let available_slots = config.max_concurrent_migrations.saturating_sub(active_migrations.len());

                            for recommendation in recommendations.iter().take(available_slots) {
                                info!("Starting migration {} from group {:?} to {:?}",
                                      recommendation.stream_name,
                                      recommendation.from_group,
                                      recommendation.to_group);

                                if let Err(e) = migration_coordinator.start_migration(
                                    recommendation.stream_name.clone(),
                                    recommendation.from_group,
                                    recommendation.to_group,
                                ).await {
                                    error!("Failed to start migration: {}", e);
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

    /// Get the consensus router
    pub fn router(&self) -> &Arc<ConsensusRouter<G, A>> {
        &self.router
    }

    /// Get the migration coordinator
    pub fn migration_coordinator(&self) -> &Arc<MigrationCoordinator<G, A>> {
        &self.migration_coordinator
    }

    /// Update configuration dynamically
    pub async fn update_config(
        &mut self,
        new_config: HierarchicalConsensusConfig,
    ) -> ConsensusResult<()> {
        info!("Updating hierarchical consensus configuration");

        // Validate configuration changes
        if new_config.local.initial_groups != self.config.local.initial_groups {
            return Err(Error::InvalidOperation(
                "Cannot change initial group count after initialization".to_string(),
            ));
        }

        // Update monitoring interval if changed
        if new_config.monitoring.update_interval != self.config.monitoring.update_interval {
            // Would need to restart monitoring task with new interval
            warn!("Monitoring interval change requires restart to take effect");
        }

        self.config = new_config;
        Ok(())
    }

    /// Process pending migrations
    pub async fn process_migrations(&self) -> ConsensusResult<()> {
        let active_migrations = self.migration_coordinator.get_active_migrations().await;

        for migration in active_migrations {
            if let Err(e) = self
                .migration_coordinator
                .execute_migration(&migration.stream_name)
                .await
            {
                error!(
                    "Migration failed for stream {}: {}",
                    migration.stream_name, e
                );
                self.migration_coordinator
                    .fail_migration(&migration.stream_name, e.to_string())
                    .await?;
            }
        }

        Ok(())
    }

    /// Get current system status
    pub async fn get_status(&self) -> HierarchicalSystemStatus {
        let metrics = self.router.get_monitoring_metrics().await;
        let active_migrations = self.migration_coordinator.get_active_migrations().await;

        HierarchicalSystemStatus {
            total_streams: metrics.global_metrics.total_streams,
            total_groups: metrics.global_metrics.total_groups,
            active_migrations: active_migrations.len(),
            load_imbalance_ratio: metrics.distribution_stats.load_imbalance_ratio,
            unhealthy_groups: metrics
                .group_metrics
                .values()
                .filter(|g| !g.is_healthy)
                .count(),
            config_hash: self.calculate_config_hash(),
        }
    }

    /// Calculate a hash of the current configuration for change detection
    fn calculate_config_hash(&self) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        // Hash key configuration parameters
        self.config.local.initial_groups.hash(&mut hasher);
        self.config.global.max_streams.hash(&mut hasher);
        hasher.finish()
    }

    /// Shutdown the orchestrator
    pub async fn shutdown(mut self) -> ConsensusResult<()> {
        info!("Shutting down hierarchical orchestrator");

        // Signal shutdown to background tasks
        self.shutdown.notify_waiters();

        // Wait for tasks to complete
        for task in self.background_tasks.drain(..) {
            if let Err(e) = task.await {
                error!("Background task failed during shutdown: {}", e);
            }
        }

        info!("Hierarchical orchestrator shutdown complete");
        Ok(())
    }
}

/// Status of the hierarchical consensus system
#[derive(Debug, Clone)]
pub struct HierarchicalSystemStatus {
    /// Total number of streams
    pub total_streams: u64,
    /// Total number of consensus groups
    pub total_groups: u32,
    /// Number of active migrations
    pub active_migrations: usize,
    /// Current load imbalance ratio
    pub load_imbalance_ratio: f64,
    /// Number of unhealthy groups
    pub unhealthy_groups: usize,
    /// Configuration hash for change detection
    pub config_hash: u64,
}

#[cfg(test)]
mod tests {
    use crate::config::HierarchicalConfigBuilder;

    #[tokio::test]
    async fn test_orchestrator_initialization() {
        let config = HierarchicalConfigBuilder::new()
            .local(crate::config::LocalConsensusConfig {
                initial_groups: 2,
                ..Default::default()
            })
            .build();

        // Would need mock implementations to fully test
        assert_eq!(config.local.initial_groups, 2);
    }
}
