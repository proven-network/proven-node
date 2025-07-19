//! Main monitoring service implementation

use std::sync::Arc;
use std::time::SystemTime;

use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use crate::foundation::ConsensusGroupId;
use proven_topology::NodeId;

use super::health::HealthChecker;
use super::metrics::MetricsCollector;
use super::types::*;
use super::views::SystemView;

/// Monitoring service for consensus system observability
pub struct MonitoringService {
    /// Service configuration
    config: MonitoringConfig,

    /// Node ID
    node_id: NodeId,

    /// System view for monitoring
    system_view: Arc<SystemView>,

    /// Health checker
    health_checker: Arc<HealthChecker>,

    /// Metrics collector
    metrics_collector: Option<Arc<MetricsCollector>>,

    /// Background monitoring tasks
    monitoring_tasks: Arc<RwLock<Vec<JoinHandle<()>>>>,

    /// Shutdown signal
    shutdown: Arc<tokio::sync::Notify>,

    /// Service state
    state: Arc<RwLock<ServiceState>>,
}

/// Internal service state
#[derive(Debug, Clone, Copy, PartialEq)]
enum ServiceState {
    /// Service not started
    NotStarted,
    /// Service is running
    Running,
    /// Service is stopping
    Stopping,
    /// Service is stopped
    Stopped,
}

impl MonitoringService {
    /// Create a new monitoring service
    pub fn new(config: MonitoringConfig, node_id: NodeId, system_view: Arc<SystemView>) -> Self {
        let health_checker = Arc::new(HealthChecker::new(
            config.health_check_interval,
            system_view.clone(),
        ));

        Self {
            config,
            node_id,
            system_view,
            health_checker,
            metrics_collector: None,
            monitoring_tasks: Arc::new(RwLock::new(Vec::new())),
            shutdown: Arc::new(tokio::sync::Notify::new()),
            state: Arc::new(RwLock::new(ServiceState::NotStarted)),
        }
    }

    /// Set metrics collector for Prometheus support
    pub fn with_metrics_collector(mut self, collector: Arc<MetricsCollector>) -> Self {
        self.metrics_collector = Some(collector);
        self
    }

    /// Start the monitoring service
    pub async fn start(&self) -> MonitoringResult<()> {
        let mut state = self.state.write().await;
        match *state {
            ServiceState::NotStarted | ServiceState::Stopped => {
                *state = ServiceState::Running;
            }
            _ => {
                return Err(MonitoringError::Internal(format!(
                    "Service cannot be started from {:?} state",
                    *state
                )));
            }
        }
        drop(state);

        if !self.config.enabled {
            info!("Monitoring disabled by configuration");
            return Ok(());
        }

        info!("Starting monitoring service for node {}", self.node_id);

        let mut tasks = self.monitoring_tasks.write().await;

        // Start health monitoring task
        tasks.push(self.spawn_health_monitoring());

        // Start metrics update task if collector is available
        if self.metrics_collector.is_some() {
            tasks.push(self.spawn_metrics_update());
        }

        // Start system view update task
        tasks.push(self.spawn_view_update());

        Ok(())
    }

    /// Stop the monitoring service
    pub async fn stop(&self) -> MonitoringResult<()> {
        let mut state = self.state.write().await;
        if *state != ServiceState::Running {
            return Ok(());
        }

        *state = ServiceState::Stopping;
        drop(state);

        info!("Stopping monitoring service");

        // Signal shutdown
        self.shutdown.notify_waiters();

        // Wait for all tasks to complete
        let mut tasks = self.monitoring_tasks.write().await;
        for task in tasks.drain(..) {
            if let Err(e) = task.await {
                warn!("Error stopping monitoring task: {}", e);
            }
        }

        let mut state = self.state.write().await;
        *state = ServiceState::Stopped;

        Ok(())
    }

    /// Get current system health
    pub async fn get_health(&self) -> MonitoringResult<HealthReport> {
        self.ensure_running().await?;

        let system_health = self
            .health_checker
            .get_system_health()
            .await
            .map_err(|e| MonitoringError::Internal(e.to_string()))?;
        let node_views = self
            .system_view
            .get_all_node_health()
            .await
            .map_err(|e| MonitoringError::Internal(e.to_string()))?;
        let group_views = self
            .system_view
            .get_all_group_health()
            .await
            .map_err(|e| MonitoringError::Internal(e.to_string()))?;

        let mut issues = Vec::new();

        // Check for critical issues
        if system_health.status == HealthStatus::Unhealthy {
            issues.push("System is unhealthy".to_string());
        }

        let unhealthy_nodes = node_views
            .iter()
            .filter(|n| n.status != HealthStatus::Healthy)
            .count();
        if unhealthy_nodes > 0 {
            issues.push(format!("{unhealthy_nodes} nodes are unhealthy"));
        }

        let unhealthy_groups = group_views
            .iter()
            .filter(|g| g.status != HealthStatus::Healthy)
            .count();
        if unhealthy_groups > 0 {
            issues.push(format!("{unhealthy_groups} groups are unhealthy"));
        }

        Ok(HealthReport {
            system: system_health,
            nodes: node_views,
            groups: group_views,
            issues,
            generated_at: SystemTime::now(),
        })
    }

    /// Get monitoring view for a specific stream
    pub async fn get_stream_view(
        &self,
        stream_name: &str,
    ) -> MonitoringResult<Option<StreamHealth>> {
        self.ensure_running().await?;
        self.system_view
            .get_stream_health(stream_name)
            .await
            .map_err(|e| MonitoringError::ViewNotAvailable(e.to_string()))
    }

    /// Get monitoring view for a specific group
    pub async fn get_group_view(
        &self,
        group_id: ConsensusGroupId,
    ) -> MonitoringResult<Option<GroupHealth>> {
        self.ensure_running().await?;
        self.system_view
            .get_group_health(group_id)
            .await
            .map_err(|e| MonitoringError::ViewNotAvailable(e.to_string()))
    }

    /// Get monitoring view for a specific node
    pub async fn get_node_view(&self, node_id: &NodeId) -> MonitoringResult<Option<NodeHealth>> {
        self.ensure_running().await?;
        self.system_view
            .get_node_health(node_id)
            .await
            .map_err(|e| MonitoringError::ViewNotAvailable(e.to_string()))
    }

    /// Get all stream views
    pub async fn get_all_stream_views(&self) -> MonitoringResult<Vec<StreamHealth>> {
        self.ensure_running().await?;
        self.system_view
            .get_all_stream_health()
            .await
            .map_err(|e| MonitoringError::ViewNotAvailable(e.to_string()))
    }

    /// Get all group views
    pub async fn get_all_group_views(&self) -> MonitoringResult<Vec<GroupHealth>> {
        self.ensure_running().await?;
        self.system_view
            .get_all_group_health()
            .await
            .map_err(|e| MonitoringError::ViewNotAvailable(e.to_string()))
    }

    /// Get all node views
    pub async fn get_all_node_views(&self) -> MonitoringResult<Vec<NodeHealth>> {
        self.ensure_running().await?;
        self.system_view
            .get_all_node_health()
            .await
            .map_err(|e| MonitoringError::ViewNotAvailable(e.to_string()))
    }

    /// Get system view for advanced monitoring
    pub async fn get_system_view(&self) -> MonitoringResult<Arc<SystemView>> {
        self.ensure_running().await?;
        Ok(self.system_view.clone())
    }

    /// Trigger a manual health check
    pub async fn trigger_health_check(&self) -> MonitoringResult<()> {
        self.ensure_running().await?;
        self.health_checker
            .check_now()
            .await
            .map_err(|e| MonitoringError::Internal(e.to_string()))?;
        Ok(())
    }

    /// Get metrics registry if available
    pub fn metrics_registry(&self) -> Option<Arc<super::metrics::MetricsRegistry>> {
        self.metrics_collector.as_ref().map(|c| c.registry())
    }

    // Private helper methods

    /// Ensure service is running
    async fn ensure_running(&self) -> MonitoringResult<()> {
        let state = self.state.read().await;
        if *state != ServiceState::Running {
            return Err(MonitoringError::NotStarted);
        }
        Ok(())
    }

    /// Spawn health monitoring task
    fn spawn_health_monitoring(&self) -> JoinHandle<()> {
        let health_checker = self.health_checker.clone();
        let interval = self.config.health_check_interval;
        let shutdown = self.shutdown.clone();

        tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);
            interval_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    _ = interval_timer.tick() => {
                        if let Err(e) = health_checker.check_now().await {
                            error!("Health check failed: {}", e);
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

    /// Spawn metrics update task
    fn spawn_metrics_update(&self) -> JoinHandle<()> {
        let metrics_collector = self.metrics_collector.as_ref().unwrap().clone();
        let interval = self.config.update_interval;
        let shutdown = self.shutdown.clone();

        tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);
            interval_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    _ = interval_timer.tick() => {
                        if let Err(e) = metrics_collector.update_metrics().await {
                            error!("Metrics update failed: {}", e);
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

    /// Spawn view update task
    fn spawn_view_update(&self) -> JoinHandle<()> {
        let system_view = self.system_view.clone();
        let interval = self.config.update_interval;
        let shutdown = self.shutdown.clone();

        tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);
            interval_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    _ = interval_timer.tick() => {
                        if let Err(e) = system_view.refresh().await {
                            error!("System view refresh failed: {}", e);
                        }
                    }
                    _ = shutdown.notified() => {
                        info!("View update task shutting down");
                        break;
                    }
                }
            }
        })
    }
}

impl Drop for MonitoringService {
    fn drop(&mut self) {
        // Ensure shutdown on drop
        self.shutdown.notify_waiters();
    }
}

// Re-export the config type for convenience
pub use super::types::MonitoringConfig;
