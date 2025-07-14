//! Monitoring coordinator that replaces the hierarchical orchestrator
//!
//! This coordinator manages all monitoring tasks including:
//! - Health monitoring
//! - Load balance monitoring
//! - Group membership monitoring

use super::{
    health::{HealthChecker, HealthStatus as ComponentHealthStatus},
    metrics::MetricsCollector,
    views::SystemView,
};
use crate::{
    ConsensusGroupId, NodeId, config::monitoring::MonitoringConfig, error::ConsensusResult,
};

use proven_governance::Governance;
use proven_transport::Transport;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

/// Health status summary
#[derive(Debug, Clone)]
pub struct HealthStatus {
    /// Overall health
    pub healthy: bool,
    /// Number of unhealthy streams
    pub unhealthy_streams: usize,
    /// Number of unhealthy groups
    pub unhealthy_groups: usize,
    /// Number of unhealthy nodes
    pub unhealthy_nodes: usize,
}

/// Load balance status
#[derive(Debug, Clone)]
pub struct LoadBalanceStatus {
    /// Whether load is balanced
    pub balanced: bool,
    /// Groups with too many streams
    pub overloaded_groups: Vec<ConsensusGroupId>,
    /// Groups with too few streams
    pub underloaded_groups: Vec<ConsensusGroupId>,
}

/// Monitoring coordinator that manages all monitoring tasks
pub struct MonitoringCoordinator<T, G>
where
    T: Transport,
    G: Governance,
{
    /// Monitoring configuration
    config: MonitoringConfig,
    /// System view for monitoring
    system_view: Arc<SystemView<T, G>>,
    /// Health checker
    health_checker: Arc<HealthChecker<T, G>>,
    /// Metrics collector
    metrics_collector: Option<Arc<MetricsCollector<T, G>>>,
    /// Node ID
    node_id: NodeId,
    /// Background monitoring tasks
    monitoring_tasks: Vec<JoinHandle<()>>,
    /// Shutdown signal
    shutdown: Arc<tokio::sync::Notify>,
    /// Reference to groups layer for rebalancing operations
    groups_layer:
        Option<Arc<RwLock<crate::core::engine::groups_layer::GroupsConsensusLayer<T, G>>>>,
}

impl<T, G> MonitoringCoordinator<T, G>
where
    T: Transport,
    G: Governance,
{
    /// Create a new monitoring coordinator
    pub fn new(
        config: MonitoringConfig,
        system_view: Arc<SystemView<T, G>>,
        node_id: NodeId,
    ) -> Self {
        let health_checker = Arc::new(HealthChecker::new(
            config.health_check.clone(),
            system_view.clone(),
        ));

        Self {
            config,
            system_view,
            health_checker,
            metrics_collector: None,
            node_id,
            monitoring_tasks: Vec::new(),
            shutdown: Arc::new(tokio::sync::Notify::new()),
            groups_layer: None,
        }
    }

    /// Create and start a monitoring coordinator
    pub async fn new_and_start(
        config: MonitoringConfig,
        system_view: Arc<SystemView<T, G>>,
        node_id: NodeId,
    ) -> ConsensusResult<Self> {
        let mut coordinator = Self::new(config, system_view, node_id);
        coordinator.start().await?;
        Ok(coordinator)
    }

    /// Set metrics collector
    pub fn with_metrics_collector(mut self, collector: Arc<MetricsCollector<T, G>>) -> Self {
        self.metrics_collector = Some(collector);
        self
    }

    /// Set groups layer reference for rebalancing
    pub fn with_groups_layer(
        mut self,
        groups_layer: Arc<RwLock<crate::core::engine::groups_layer::GroupsConsensusLayer<T, G>>>,
    ) -> Self {
        self.groups_layer = Some(groups_layer);
        self
    }

    /// Start all monitoring tasks
    pub async fn start(&mut self) -> ConsensusResult<()> {
        if !self.config.enabled {
            info!("Monitoring disabled by configuration");
            return Ok(());
        }

        info!("Starting monitoring coordinator");

        // Start health checker
        self.health_checker.start().await;

        // Start metrics collector if available
        if let Some(collector) = &self.metrics_collector {
            collector.start().await;
        }

        // Start load monitoring
        if self.config.stream_monitoring.enable_load_monitoring {
            let task = self.start_load_monitoring();
            self.monitoring_tasks.push(task);
        }

        // Start membership monitoring
        if self.config.group_monitoring.enable_membership_tracking {
            let task = self.start_membership_monitoring();
            self.monitoring_tasks.push(task);
        }

        // Start metrics update task if metrics collector is available
        if self.metrics_collector.is_some() {
            let task = self.start_metrics_update();
            self.monitoring_tasks.push(task);
        }

        // Start auto-rebalancing if enabled
        if self.config.stream_monitoring.enable_auto_rebalancing && self.groups_layer.is_some() {
            let task = self.start_auto_rebalancing();
            self.monitoring_tasks.push(task);
        }

        // Start migration policy monitoring
        if self.groups_layer.is_some() {
            let task = self.start_migration_policy_monitoring();
            self.monitoring_tasks.push(task);
        }

        Ok(())
    }

    /// Stop all monitoring tasks
    pub async fn stop(&mut self) -> ConsensusResult<()> {
        info!("Stopping monitoring coordinator");

        // Signal shutdown
        self.shutdown.notify_waiters();

        // Wait for all tasks to complete
        for task in self.monitoring_tasks.drain(..) {
            if let Err(e) = task.await {
                warn!("Error stopping monitoring task: {}", e);
            }
        }

        Ok(())
    }

    /// Get current health status
    pub async fn get_health_status(&self) -> ConsensusResult<HealthStatus> {
        let health_check = self.health_checker.check_now().await?;

        let unhealthy_streams = health_check
            .components
            .iter()
            .filter(|c| {
                c.component_type == super::health::ComponentType::Stream
                    && c.status != ComponentHealthStatus::Healthy
            })
            .count();

        let unhealthy_groups = health_check
            .components
            .iter()
            .filter(|c| {
                c.component_type == super::health::ComponentType::Group
                    && c.status != ComponentHealthStatus::Healthy
            })
            .count();

        let unhealthy_nodes = health_check
            .components
            .iter()
            .filter(|c| {
                c.component_type == super::health::ComponentType::Node
                    && c.status != ComponentHealthStatus::Healthy
            })
            .count();

        Ok(HealthStatus {
            healthy: health_check.status == ComponentHealthStatus::Healthy,
            unhealthy_streams,
            unhealthy_groups,
            unhealthy_nodes,
        })
    }

    /// Get load balance status
    pub async fn get_load_balance_status(&self) -> ConsensusResult<LoadBalanceStatus> {
        let stream_counts = self
            .system_view
            .stream_view
            .get_stream_count_by_group()
            .await?;

        if stream_counts.is_empty() {
            return Ok(LoadBalanceStatus {
                balanced: true,
                overloaded_groups: vec![],
                underloaded_groups: vec![],
            });
        }

        let total_streams: usize = stream_counts.values().sum();
        let avg_streams = total_streams as f64 / stream_counts.len() as f64;

        let mut overloaded = vec![];
        let mut underloaded = vec![];

        for (group_id, count) in stream_counts {
            let ratio = count as f64 / avg_streams;

            if ratio > self.config.stream_monitoring.load_balance_max_ratio {
                overloaded.push(group_id);
            } else if ratio < 1.0 / self.config.stream_monitoring.load_balance_max_ratio {
                underloaded.push(group_id);
            }
        }

        Ok(LoadBalanceStatus {
            balanced: overloaded.is_empty() && underloaded.is_empty(),
            overloaded_groups: overloaded,
            underloaded_groups: underloaded,
        })
    }

    /// Start load monitoring task
    fn start_load_monitoring(&self) -> JoinHandle<()> {
        let system_view = self.system_view.clone();
        let interval = self.config.update_interval;
        let max_ratio = self.config.stream_monitoring.load_balance_max_ratio;
        let shutdown = self.shutdown.clone();

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);

            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        // Check for load imbalances
                        if let Ok(stream_counts) = system_view.stream_view.get_stream_count_by_group().await
                            && !stream_counts.is_empty() {
                                let total_streams: usize = stream_counts.values().sum();

                                if total_streams == 0 {
                                    continue;
                                }

                                let avg_streams = total_streams as f64 / stream_counts.len() as f64;

                                for (group_id, count) in &stream_counts {
                                    let ratio = *count as f64 / avg_streams;
                                    if ratio > max_ratio {
                                        warn!(
                                            "Group {:?} is overloaded: {} streams ({}x average)",
                                            group_id, count, ratio
                                        );
                                    }
                                }
                            }
                    }
                    _ = shutdown.notified() => {
                        info!("Load monitoring task shutting down");
                        break;
                    }
                }
            }
        })
    }

    /// Start membership monitoring task
    fn start_membership_monitoring(&self) -> JoinHandle<()> {
        let system_view = self.system_view.clone();
        let node_id = self.node_id.clone();
        let interval = self.config.update_interval;
        let shutdown = self.shutdown.clone();

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);

            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        // Count groups this node belongs to
                        if let Ok(node_health) = system_view.node_view.get_node_health(&node_id).await
                            && let Some(health) = node_health {
                                info!(
                                    "Node {} is member of {} groups",
                                    node_id, health.group_count
                                );

                                if health.group_count == 0 {
                                    warn!("Node {} is not a member of any groups", node_id);
                                }
                            }
                    }
                    _ = shutdown.notified() => {
                        info!("Membership monitoring task shutting down");
                        break;
                    }
                }
            }
        })
    }

    /// Start metrics update task
    fn start_metrics_update(&self) -> JoinHandle<()> {
        let system_view = self.system_view.clone();
        let metrics_collector = self.metrics_collector.clone();
        let interval = self.config.update_interval;
        let shutdown = self.shutdown.clone();

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);

            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        if let Some(collector) = &metrics_collector {
                            let registry = collector.registry();

                            // Update stream count
                            if let Ok(streams) = system_view.stream_view.get_all_stream_health().await {
                                registry.consensus.active_streams.set(streams.len() as f64);
                            }

                            // Update group count
                            if let Ok(groups) = system_view.group_view.get_all_group_health().await {
                                registry.consensus.active_groups.set(groups.len() as f64);
                            }

                            // Update node count
                            if let Ok(nodes) = system_view.node_view.get_all_node_health().await {
                                registry.consensus.active_nodes.set(nodes.len() as f64);
                            }
                        }
                    }
                    _ = shutdown.notified() => {
                        info!("Metrics update task shutting down");
                        break;
                    }
                }
            }
        })
    }

    /// Start auto-rebalancing task
    fn start_auto_rebalancing(&self) -> JoinHandle<()> {
        let groups_layer = self.groups_layer.clone();
        let interval = self.config.stream_monitoring.rebalancing_check_interval;
        let cooldown = self.config.stream_monitoring.rebalancing_cooldown;
        let shutdown = self.shutdown.clone();
        let node_id = self.node_id.clone();

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            let mut last_rebalance = std::time::Instant::now();

            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        // Check if cooldown period has passed
                        if last_rebalance.elapsed() < cooldown {
                            continue;
                        }

                        if let Some(layer) = &groups_layer {
                            // Check if rebalancing is needed
                            let groups_manager = layer.read().await.groups_manager().clone();
                            match groups_manager.read().await.needs_rebalancing().await {
                                Ok(true) => {
                                    info!("Load imbalance detected, generating rebalancing plan");

                                    // Generate rebalancing plan
                                    match groups_manager.read().await.generate_rebalancing_plan().await {
                                        Ok(Some(plan)) => {
                                            info!(
                                                "Rebalancing plan generated: {} node migrations, {} groups to create, {} groups to remove",
                                                plan.migrations.len(),
                                                plan.groups_to_create.len(),
                                                plan.groups_to_remove.len()
                                            );

                                            // Create new groups first
                                            for group_creation in &plan.groups_to_create {
                                                info!(
                                                    "Creating new group in region {} with {} initial members",
                                                    group_creation.region,
                                                    group_creation.initial_members.len()
                                                );
                                                // Group creation would be handled through global consensus
                                            }

                                            // Execute node migrations
                                            for migration in &plan.migrations {
                                                if migration.node_id == node_id {
                                                    // Skip self-migration
                                                    continue;
                                                }

                                                match groups_manager.read().await.start_node_migration(
                                                    &migration.node_id,
                                                    migration.from_group,
                                                    migration.to_group,
                                                ).await {
                                                    Ok(_) => {
                                                        info!(
                                                            "Started node migration: {} from group {:?} to {:?}",
                                                            migration.node_id, migration.from_group, migration.to_group
                                                        );
                                                    }
                                                    Err(e) => {
                                                        error!(
                                                            "Failed to start node migration for {}: {}",
                                                            migration.node_id, e
                                                        );
                                                    }
                                                }
                                            }

                                            // Remove empty groups after migrations
                                            for group_id in &plan.groups_to_remove {
                                                info!("Group {:?} marked for removal after migrations complete", group_id);
                                                // Group removal would be handled through global consensus
                                            }

                                            // Update last rebalance time
                                            last_rebalance = std::time::Instant::now();
                                        }
                                        Ok(None) => {
                                            // No rebalancing plan generated
                                        }
                                        Err(e) => {
                                            error!("Failed to generate rebalancing plan: {}", e);
                                        }
                                    }
                                }
                                Ok(false) => {
                                    // No rebalancing needed
                                }
                                Err(e) => {
                                    error!("Failed to check if rebalancing is needed: {}", e);
                                }
                            }
                        }
                    }
                    _ = shutdown.notified() => {
                        info!("Auto-rebalancing task shutting down");
                        break;
                    }
                }
            }
        })
    }

    /// Start migration policy monitoring task
    fn start_migration_policy_monitoring(&self) -> JoinHandle<()> {
        let groups_layer = self.groups_layer.clone();
        let system_view = self.system_view.clone();
        let interval = self.config.update_interval;
        let shutdown = self.shutdown.clone();
        let load_threshold = self.config.stream_monitoring.load_balance_max_ratio;

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);

            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        if let Some(layer) = &groups_layer {
                            let groups_manager = layer.read().await.groups_manager().clone();

                            // Check for unhealthy streams that need migration
                            if let Ok(stream_healths) = system_view.stream_view.get_all_stream_health().await {
                                for stream in stream_healths {
                                    if stream.status == super::views::HealthStatus::Unhealthy {
                                        info!(
                                            "Stream {} is unhealthy, considering migration from group {:?}",
                                            stream.stream_name, stream.group_id
                                        );

                                        // Find a healthier group
                                        if let Ok(group_healths) = system_view.group_view.get_all_group_health().await
                                            && let Some(target_group) = group_healths
                                                .iter()
                                                .filter(|g| {
                                                    g.group_id != stream.group_id
                                                    && g.status == super::views::HealthStatus::Healthy
                                                    && g.has_quorum
                                                })
                                                .min_by_key(|g| g.stream_count)
                                            {
                                                match groups_manager.read().await.start_stream_migration(
                                                    stream.stream_name.clone(),
                                                    stream.group_id,
                                                    target_group.group_id,
                                                ).await {
                                                    Ok(_) => {
                                                        info!(
                                                            "Started health-based migration of stream {} from {:?} to {:?}",
                                                            stream.stream_name, stream.group_id, target_group.group_id
                                                        );
                                                    }
                                                    Err(e) => {
                                                        error!(
                                                            "Failed to migrate unhealthy stream {}: {}",
                                                            stream.stream_name, e
                                                        );
                                                    }
                                                }
                                            }
                                    }

                                    // Check for overloaded streams (high error rate or latency)
                                    if stream.error_count > 100 {
                                        warn!(
                                            "Stream {} has high error count: {}",
                                            stream.stream_name, stream.error_count
                                        );
                                        // Could trigger migration based on error threshold
                                    }
                                }
                            }

                            // Check for nodes with high resource usage
                            if let Ok(node_healths) = system_view.node_view.get_all_node_health().await {
                                for node in node_healths {
                                    // Migrate away from overloaded nodes
                                    if node.cpu_usage > 0.9 || node.memory_usage > 0.9 {
                                        warn!(
                                            "Node {} is overloaded: CPU {}%, Memory {}%",
                                            node.node_id,
                                            (node.cpu_usage * 100.0) as u32,
                                            (node.memory_usage * 100.0) as u32
                                        );
                                        // Could trigger node migration to reduce load
                                    }
                                }
                            }

                            // Check group load distribution for stream-level rebalancing
                            if let Ok(stream_counts) = system_view.stream_view.get_stream_count_by_group().await
                                && !stream_counts.is_empty() {
                                    let total_streams: usize = stream_counts.values().sum();
                                    let avg_streams = total_streams as f64 / stream_counts.len() as f64;

                                    for (group_id, count) in &stream_counts {
                                        let ratio = *count as f64 / avg_streams;

                                        // Trigger stream migration if group is significantly overloaded
                                        if ratio > load_threshold * 0.8 {
                                            info!(
                                                "Group {:?} is approaching overload with {} streams ({}x average)",
                                                group_id, count, ratio
                                            );
                                            // Stream migration would be triggered here based on policy
                                        }
                                    }
                                }
                        }
                    }
                    _ = shutdown.notified() => {
                        info!("Migration policy monitoring task shutting down");
                        break;
                    }
                }
            }
        })
    }

    // ============ View Access Methods ============

    /// Get system-wide monitoring view
    pub async fn get_system_view(&self) -> ConsensusResult<Arc<SystemView<T, G>>> {
        Ok(self.system_view.clone())
    }

    /// Get monitoring view for a specific stream
    pub async fn get_stream_view(
        &self,
        stream_name: &str,
    ) -> ConsensusResult<Option<crate::core::monitoring::StreamHealth>> {
        self.system_view
            .stream_view
            .get_stream_health(stream_name)
            .await
    }

    /// Get monitoring view for a specific group
    pub async fn get_group_view(
        &self,
        group_id: ConsensusGroupId,
    ) -> ConsensusResult<Option<crate::core::monitoring::GroupHealth>> {
        self.system_view.group_view.get_group_health(group_id).await
    }

    /// Get monitoring view for a specific node
    pub async fn get_node_view(
        &self,
        node_id: &NodeId,
    ) -> ConsensusResult<Option<crate::core::monitoring::NodeHealth>> {
        self.system_view.node_view.get_node_health(node_id).await
    }

    /// Get monitoring views for all streams
    pub async fn get_all_stream_views(
        &self,
    ) -> ConsensusResult<Vec<crate::core::monitoring::StreamHealth>> {
        self.system_view.stream_view.get_all_stream_health().await
    }

    /// Get monitoring views for all groups
    pub async fn get_all_group_views(
        &self,
    ) -> ConsensusResult<Vec<crate::core::monitoring::GroupHealth>> {
        self.system_view.group_view.get_all_group_health().await
    }

    /// Get monitoring views for all nodes
    pub async fn get_all_node_views(
        &self,
    ) -> ConsensusResult<Vec<crate::core::monitoring::NodeHealth>> {
        self.system_view.node_view.get_all_node_health().await
    }

    /// Get system health status
    pub async fn get_system_health(
        &self,
    ) -> ConsensusResult<crate::core::monitoring::HealthReport> {
        self.health_checker.generate_report().await
    }

    /// Trigger a manual health check
    pub async fn trigger_health_check(&self) -> ConsensusResult<()> {
        self.health_checker.check_now().await?;
        Ok(())
    }

    /// Get the metrics collector if available
    pub fn metrics_collector(&self) -> Option<&Arc<MetricsCollector<T, G>>> {
        self.metrics_collector.as_ref()
    }
}
