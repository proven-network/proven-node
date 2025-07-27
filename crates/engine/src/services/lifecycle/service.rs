//! Main lifecycle service implementation

use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use super::health::{HealthChecker, HealthReport};
use super::shutdown::{ShutdownCoordinator, ShutdownOptions};
use super::startup::{StartupCoordinator, StartupOptions};
use super::types::*;

/// Lifecycle service for managing consensus components
pub struct LifecycleService {
    /// Service configuration
    config: LifecycleConfig,

    /// Component registry
    components: Arc<RwLock<ComponentRegistry>>,

    /// Health checker
    health_checker: Arc<HealthChecker>,

    /// Startup coordinator
    startup_coordinator: Arc<StartupCoordinator>,

    /// Shutdown coordinator
    shutdown_coordinator: Arc<ShutdownCoordinator>,

    /// Background tasks
    background_tasks: Arc<RwLock<Vec<JoinHandle<()>>>>,

    /// Shutdown signal
    shutdown_signal: Arc<tokio::sync::Notify>,

    /// Service state
    state: Arc<RwLock<ServiceState>>,
}

/// Component registry
pub(super) struct ComponentRegistry {
    /// Registered components
    pub(super) components: HashMap<String, ComponentInfo>,
    /// Component dependencies
    pub(super) dependencies: HashMap<String, Vec<String>>,
    /// Startup order
    pub(super) startup_order: Vec<String>,
    /// Shutdown order
    pub(super) shutdown_order: Vec<String>,
}

/// Component information
pub(super) struct ComponentInfo {
    /// Component name
    pub(super) name: String,
    /// Current state
    pub(super) state: ComponentState,
    /// Health status
    pub(super) health: ComponentHealth,
    /// Start time
    pub(super) start_time: Option<SystemTime>,
    /// Stop time
    pub(super) stop_time: Option<SystemTime>,
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

impl LifecycleService {
    /// Create a new lifecycle service
    pub fn new(config: LifecycleConfig) -> Self {
        let health_checker = Arc::new(HealthChecker::new(config.health_check_interval));
        let startup_coordinator = Arc::new(StartupCoordinator::new(config.retry_config.clone()));
        let shutdown_coordinator = Arc::new(ShutdownCoordinator::new(config.shutdown_timeout));

        let mut shutdown_order = config.shutdown_order.clone();
        if shutdown_order.is_empty() {
            // Use reverse of startup order
            shutdown_order = config.startup_order.clone();
            shutdown_order.reverse();
        }

        let components = Arc::new(RwLock::new(ComponentRegistry {
            components: HashMap::new(),
            dependencies: HashMap::new(),
            startup_order: config.startup_order.clone(),
            shutdown_order,
        }));

        Self {
            config,
            components,
            health_checker,
            startup_coordinator,
            shutdown_coordinator,
            background_tasks: Arc::new(RwLock::new(Vec::new())),
            shutdown_signal: Arc::new(tokio::sync::Notify::new()),
            state: Arc::new(RwLock::new(ServiceState::NotStarted)),
        }
    }

    /// Register a component
    pub async fn register_component(
        &self,
        name: String,
        dependencies: Vec<String>,
    ) -> LifecycleResult<()> {
        let mut registry = self.components.write().await;

        if registry.components.contains_key(&name) {
            return Err(LifecycleError::AlreadyRunning(name));
        }

        let component_info = ComponentInfo {
            name: name.clone(),
            state: ComponentState::NotInitialized,
            health: ComponentHealth {
                name: name.clone(),
                state: ComponentState::NotInitialized,
                status: HealthStatus::Unknown,
                last_check: SystemTime::now(),
                uptime: None,
                error_count: 0,
                metrics: HashMap::new(),
            },
            start_time: None,
            stop_time: None,
        };

        registry.components.insert(name.clone(), component_info);
        registry.dependencies.insert(name, dependencies);

        Ok(())
    }

    /// Start the lifecycle service
    pub async fn start(&self) -> LifecycleResult<()> {
        let mut state = self.state.write().await;
        match *state {
            ServiceState::NotStarted | ServiceState::Stopped => {
                *state = ServiceState::Running;
            }
            _ => {
                return Err(LifecycleError::AlreadyRunning(format!(
                    "Service cannot be started from {:?} state",
                    *state
                )));
            }
        }
        drop(state);

        info!("Starting lifecycle service");

        // Start health monitoring if enabled
        if self.config.enable_health_monitoring {
            let task = self.spawn_health_monitor();
            self.background_tasks.write().await.push(task);
        }

        Ok(())
    }

    /// Stop the lifecycle service
    pub async fn stop(&self) -> LifecycleResult<()> {
        let mut state = self.state.write().await;
        if *state != ServiceState::Running {
            return Ok(());
        }

        *state = ServiceState::Stopping;
        drop(state);

        info!("Stopping lifecycle service");

        // Signal shutdown
        self.shutdown_signal.notify_waiters();

        // Wait for background tasks
        let mut tasks = self.background_tasks.write().await;
        for task in tasks.drain(..) {
            if let Err(e) = task.await {
                warn!("Error stopping background task: {}", e);
            }
        }

        let mut state = self.state.write().await;
        *state = ServiceState::Stopped;

        Ok(())
    }

    /// Start all components
    pub async fn start_all(&self) -> LifecycleResult<()> {
        self.ensure_running().await?;

        let options = StartupOptions {
            mode: InitializationMode::Fresh,
            timeout: self.config.startup_timeout,
            parallel: false,
        };

        self.startup_coordinator
            .start_all(&self.components, options)
            .await
    }

    /// Stop all components (graceful shutdown)
    pub async fn shutdown(&self) -> LifecycleResult<()> {
        let options = ShutdownOptions {
            timeout: self.config.shutdown_timeout,
            force: false,
            save_state: true,
        };

        self.shutdown_coordinator
            .shutdown_all(&self.components, options)
            .await
    }

    /// Initialize cluster
    pub async fn initialize_cluster(
        &self,
        strategy: ClusterFormationStrategy,
    ) -> LifecycleResult<()> {
        self.ensure_running().await?;

        match strategy {
            ClusterFormationStrategy::SingleNode => {
                info!("Initializing single-node cluster");
                self.startup_coordinator.initialize_single_node().await
            }
            ClusterFormationStrategy::MultiNode { expected_peers } => {
                info!(
                    "Initializing multi-node cluster with {} peers",
                    expected_peers.len()
                );
                self.startup_coordinator
                    .initialize_multi_node(expected_peers)
                    .await
            }
            ClusterFormationStrategy::JoinExisting { target_node } => {
                info!("Joining existing cluster via node: {}", target_node);
                self.startup_coordinator.join_cluster(target_node).await
            }
            ClusterFormationStrategy::AutoDiscovery { timeout } => {
                info!("Auto-discovering cluster with timeout: {:?}", timeout);
                self.startup_coordinator
                    .auto_discover_cluster(timeout)
                    .await
            }
        }
    }

    /// Check component health
    pub async fn check_health(&self) -> LifecycleResult<HealthReport> {
        self.health_checker
            .check_all_components(&self.components)
            .await
    }

    /// Get component state
    pub async fn get_component_state(&self, name: &str) -> LifecycleResult<ComponentState> {
        let registry = self.components.read().await;
        registry
            .components
            .get(name)
            .map(|c| c.state)
            .ok_or_else(|| LifecycleError::ComponentNotFound(name.to_string()))
    }

    /// Get all component states
    pub async fn get_all_component_states(&self) -> HashMap<String, ComponentState> {
        let registry = self.components.read().await;
        registry
            .components
            .iter()
            .map(|(name, info)| (name.clone(), info.state))
            .collect()
    }

    /// Update component state
    pub async fn update_component_state(
        &self,
        name: &str,
        state: ComponentState,
    ) -> LifecycleResult<()> {
        let mut registry = self.components.write().await;
        let component = registry
            .components
            .get_mut(name)
            .ok_or_else(|| LifecycleError::ComponentNotFound(name.to_string()))?;

        component.state = state;

        match state {
            ComponentState::Running => {
                component.start_time = Some(SystemTime::now());
                component.stop_time = None;
            }
            ComponentState::Stopped | ComponentState::Failed => {
                component.stop_time = Some(SystemTime::now());
            }
            _ => {}
        }

        Ok(())
    }

    /// Get startup phase
    pub async fn get_startup_phase(&self) -> LifecycleResult<StartupPhase> {
        self.startup_coordinator.get_current_phase().await
    }

    /// Get shutdown phase
    pub async fn get_shutdown_phase(&self) -> LifecycleResult<ShutdownPhase> {
        self.shutdown_coordinator.get_current_phase().await
    }

    // Private helper methods

    /// Ensure service is running
    async fn ensure_running(&self) -> LifecycleResult<()> {
        let state = self.state.read().await;
        if *state != ServiceState::Running {
            return Err(LifecycleError::NotStarted);
        }
        Ok(())
    }

    /// Spawn health monitor task
    fn spawn_health_monitor(&self) -> JoinHandle<()> {
        let health_checker = self.health_checker.clone();
        let components = self.components.clone();
        let interval = self.config.health_check_interval;
        let shutdown = self.shutdown_signal.clone();

        tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);
            interval_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    _ = interval_timer.tick() => {
                        if let Err(e) = health_checker.check_all_components(&components).await {
                            error!("Health check failed: {}", e);
                        }
                    }
                    _ = shutdown.notified() => {
                        debug!("Health monitor shutting down");
                        break;
                    }
                }
            }
        })
    }
}

impl Drop for LifecycleService {
    fn drop(&mut self) {
        // Ensure shutdown on drop
        self.shutdown_signal.notify_waiters();
    }
}

// Re-export config for convenience
pub use super::types::LifecycleConfig;
