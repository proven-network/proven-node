//! Startup coordination for consensus components

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use proven_topology::NodeId;

use super::service::ComponentRegistry;
use super::types::*;

/// Startup options
#[derive(Debug, Clone)]
pub struct StartupOptions {
    /// Initialization mode
    pub mode: InitializationMode,
    /// Startup timeout
    pub timeout: Duration,
    /// Allow parallel startup
    pub parallel: bool,
}

/// Startup coordinator
pub struct StartupCoordinator {
    /// Retry configuration
    retry_config: RetryConfig,
    /// Current startup phase
    current_phase: Arc<RwLock<StartupPhase>>,
}

impl StartupCoordinator {
    /// Create a new startup coordinator
    pub fn new(retry_config: RetryConfig) -> Self {
        Self {
            retry_config,
            current_phase: Arc::new(RwLock::new(StartupPhase::PreInit)),
        }
    }

    /// Start all components
    pub(super) async fn start_all(
        &self,
        components: &Arc<RwLock<ComponentRegistry>>,
        options: StartupOptions,
    ) -> LifecycleResult<()> {
        info!("Starting all components with mode: {:?}", options.mode);

        let mut phase = self.current_phase.write().await;
        *phase = StartupPhase::PreInit;
        drop(phase);

        // Pre-initialization checks
        self.pre_init_checks(components).await?;

        // Start components in order
        let registry = components.read().await;
        let startup_order = registry.startup_order.clone();
        drop(registry);

        for component_name in &startup_order {
            self.start_component(components, component_name, &options)
                .await?;
        }

        let mut phase = self.current_phase.write().await;
        *phase = StartupPhase::Completed;

        info!("All components started successfully");
        Ok(())
    }

    /// Start a single component
    async fn start_component(
        &self,
        components: &Arc<RwLock<ComponentRegistry>>,
        name: &str,
        _options: &StartupOptions,
    ) -> LifecycleResult<()> {
        debug!("Starting component: {}", name);

        // Check dependencies first
        self.check_dependencies(components, name).await?;

        // Update state to starting
        self.update_component_state(components, name, ComponentState::Starting)
            .await?;

        // Simulate component startup with retry
        let mut attempts = 0;
        let mut delay = self.retry_config.initial_delay;

        loop {
            match self.perform_startup(name).await {
                Ok(()) => {
                    self.update_component_state(components, name, ComponentState::Running)
                        .await?;
                    info!("Component {} started successfully", name);
                    return Ok(());
                }
                Err(e) => {
                    attempts += 1;
                    if attempts >= self.retry_config.max_attempts {
                        self.update_component_state(components, name, ComponentState::Failed)
                            .await?;
                        return Err(LifecycleError::StartupFailed(format!(
                            "Component {name} failed to start after {attempts} attempts: {e}"
                        )));
                    }

                    warn!(
                        "Component {} startup attempt {} failed: {}",
                        name, attempts, e
                    );
                    tokio::time::sleep(delay).await;

                    // Exponential backoff
                    delay = std::cmp::min(
                        delay.mul_f32(self.retry_config.backoff_factor),
                        self.retry_config.max_delay,
                    );
                }
            }
        }
    }

    /// Perform actual component startup
    async fn perform_startup(&self, name: &str) -> LifecycleResult<()> {
        // In a real implementation, this would start the actual component
        // For now, simulate with a delay
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Update phase based on component
        let mut phase = self.current_phase.write().await;
        *phase = match name {
            "network" => StartupPhase::NetworkInit,
            "storage" => StartupPhase::StorageInit,
            "consensus" => StartupPhase::ConsensusInit,
            "services" => StartupPhase::ServiceRegistration,
            _ => *phase,
        };

        Ok(())
    }

    /// Pre-initialization checks
    async fn pre_init_checks(
        &self,
        components: &Arc<RwLock<ComponentRegistry>>,
    ) -> LifecycleResult<()> {
        let mut phase = self.current_phase.write().await;
        *phase = StartupPhase::PreInit;
        drop(phase);

        // Check that all required components are registered
        let registry = components.read().await;
        if registry.components.is_empty() {
            return Err(LifecycleError::InitializationFailed(
                "No components registered".to_string(),
            ));
        }

        Ok(())
    }

    /// Check component dependencies
    async fn check_dependencies(
        &self,
        components: &Arc<RwLock<ComponentRegistry>>,
        name: &str,
    ) -> LifecycleResult<()> {
        let registry = components.read().await;

        if let Some(deps) = registry.dependencies.get(name) {
            for dep in deps {
                if let Some(dep_info) = registry.components.get(dep) {
                    if dep_info.state != ComponentState::Running {
                        return Err(LifecycleError::StartupFailed(format!(
                            "Dependency {dep} is not running"
                        )));
                    }
                } else {
                    return Err(LifecycleError::ComponentNotFound(dep.clone()));
                }
            }
        }

        Ok(())
    }

    /// Update component state
    async fn update_component_state(
        &self,
        components: &Arc<RwLock<ComponentRegistry>>,
        name: &str,
        state: ComponentState,
    ) -> LifecycleResult<()> {
        let mut registry = components.write().await;

        if let Some(component) = registry.components.get_mut(name) {
            component.state = state;
            Ok(())
        } else {
            Err(LifecycleError::ComponentNotFound(name.to_string()))
        }
    }

    /// Initialize single-node cluster
    pub async fn initialize_single_node(&self) -> LifecycleResult<()> {
        info!("Initializing single-node cluster");
        // In a real implementation, this would initialize consensus as single node
        Ok(())
    }

    /// Initialize multi-node cluster
    pub async fn initialize_multi_node(&self, expected_peers: Vec<NodeId>) -> LifecycleResult<()> {
        info!(
            "Initializing multi-node cluster with {} peers",
            expected_peers.len()
        );
        // In a real implementation, this would coordinate multi-node initialization
        Ok(())
    }

    /// Join existing cluster
    pub async fn join_cluster(&self, target_node: NodeId) -> LifecycleResult<()> {
        info!("Joining cluster via node: {}", target_node);
        // In a real implementation, this would join an existing cluster
        Ok(())
    }

    /// Auto-discover cluster
    pub async fn auto_discover_cluster(&self, timeout: Duration) -> LifecycleResult<()> {
        info!("Auto-discovering cluster with timeout: {:?}", timeout);
        // In a real implementation, this would perform discovery
        Ok(())
    }

    /// Get current startup phase
    pub async fn get_current_phase(&self) -> LifecycleResult<StartupPhase> {
        let phase = self.current_phase.read().await;
        Ok(*phase)
    }
}
