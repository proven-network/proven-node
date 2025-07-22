//! Shutdown coordination for consensus components

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use super::service::ComponentRegistry;
use super::types::*;

/// Shutdown options
#[derive(Debug, Clone)]
pub struct ShutdownOptions {
    /// Shutdown timeout
    pub timeout: Duration,
    /// Force shutdown if graceful fails
    pub force: bool,
    /// Save state before shutdown
    pub save_state: bool,
}

/// Shutdown coordinator
pub struct ShutdownCoordinator {
    /// Shutdown timeout
    timeout: Duration,
    /// Current shutdown phase
    current_phase: Arc<RwLock<ShutdownPhase>>,
}

impl ShutdownCoordinator {
    /// Create a new shutdown coordinator
    pub fn new(timeout: Duration) -> Self {
        Self {
            timeout,
            current_phase: Arc::new(RwLock::new(ShutdownPhase::PreShutdown)),
        }
    }

    /// Shutdown all components
    pub(super) async fn shutdown_all(
        &self,
        components: &Arc<RwLock<ComponentRegistry>>,
        options: ShutdownOptions,
    ) -> LifecycleResult<()> {
        info!("Starting graceful shutdown of all components");

        let mut phase = self.current_phase.write().await;
        *phase = ShutdownPhase::PreShutdown;
        drop(phase);

        // Pre-shutdown preparation
        self.pre_shutdown(components, &options).await?;

        // Stop accepting new requests
        self.stop_accepting(components).await?;

        // Drain in-flight requests
        self.drain_requests(components, &options).await?;

        // Stop components in reverse order
        let registry = components.read().await;
        let shutdown_order = registry.shutdown_order.clone();
        drop(registry);

        for component_name in &shutdown_order {
            self.stop_component(components, component_name, &options)
                .await?;
        }

        // Cleanup
        self.cleanup(components, &options).await?;

        let mut phase = self.current_phase.write().await;
        *phase = ShutdownPhase::Completed;

        info!("All components shut down successfully");
        Ok(())
    }

    /// Pre-shutdown preparation
    async fn pre_shutdown(
        &self,
        components: &Arc<RwLock<ComponentRegistry>>,
        options: &ShutdownOptions,
    ) -> LifecycleResult<()> {
        let mut phase = self.current_phase.write().await;
        *phase = ShutdownPhase::PreShutdown;
        drop(phase);

        info!("Performing pre-shutdown preparation");

        if options.save_state {
            self.save_state(components).await?;
        }

        Ok(())
    }

    /// Stop accepting new requests
    async fn stop_accepting(
        &self,
        _components: &Arc<RwLock<ComponentRegistry>>,
    ) -> LifecycleResult<()> {
        let mut phase = self.current_phase.write().await;
        *phase = ShutdownPhase::StopAccepting;
        drop(phase);

        info!("Stopping acceptance of new requests");

        // In a real implementation, this would signal components to stop accepting work
        tokio::time::sleep(Duration::from_millis(100)).await;

        Ok(())
    }

    /// Drain in-flight requests
    async fn drain_requests(
        &self,
        _components: &Arc<RwLock<ComponentRegistry>>,
        options: &ShutdownOptions,
    ) -> LifecycleResult<()> {
        let mut phase = self.current_phase.write().await;
        *phase = ShutdownPhase::DrainRequests;
        drop(phase);

        info!("Draining in-flight requests");

        // Wait for requests to complete or timeout
        let drain_timeout = options.timeout / 3; // Use 1/3 of total timeout for draining

        tokio::select! {
            _ = self.wait_for_requests_to_complete() => {
                info!("All requests completed");
            }
            _ = tokio::time::sleep(drain_timeout) => {
                if options.force {
                    warn!("Request drain timeout, forcing shutdown");
                } else {
                    return Err(LifecycleError::ShutdownFailed(
                        "Timeout waiting for requests to complete".to_string()
                    ));
                }
            }
        }

        Ok(())
    }

    /// Wait for requests to complete
    async fn wait_for_requests_to_complete(&self) {
        // In a real implementation, this would monitor request completion
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    /// Stop a single component
    async fn stop_component(
        &self,
        components: &Arc<RwLock<ComponentRegistry>>,
        name: &str,
        options: &ShutdownOptions,
    ) -> LifecycleResult<()> {
        debug!("Stopping component: {}", name);

        // Update state to shutting down
        self.update_component_state(components, name, ComponentState::ShuttingDown)
            .await?;

        // Update phase based on component
        if name == "consensus" {
            let mut phase = self.current_phase.write().await;
            *phase = ShutdownPhase::StopConsensus;
        }

        // Perform shutdown with timeout
        tokio::select! {
            result = self.perform_shutdown(name) => {
                match result {
                    Ok(()) => {
                        self.update_component_state(components, name, ComponentState::Stopped).await?;
                        info!("Component {} stopped successfully", name);
                        Ok(())
                    }
                    Err(e) => {
                        if options.force {
                            warn!("Component {} shutdown failed: {}, forcing", name, e);
                            self.update_component_state(components, name, ComponentState::Stopped).await?;
                            Ok(())
                        } else {
                            error!("Component {} shutdown failed: {}", name, e);
                            self.update_component_state(components, name, ComponentState::Failed).await?;
                            Err(e)
                        }
                    }
                }
            }
            _ = tokio::time::sleep(options.timeout / 4) => {
                if options.force {
                    warn!("Component {} shutdown timeout, forcing", name);
                    self.update_component_state(components, name, ComponentState::Stopped).await?;
                    Ok(())
                } else {
                    Err(LifecycleError::Timeout(options.timeout.as_secs() / 4))
                }
            }
        }
    }

    /// Perform actual component shutdown
    async fn perform_shutdown(&self, _name: &str) -> LifecycleResult<()> {
        // In a real implementation, this would stop the actual component
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(())
    }

    /// Cleanup after shutdown
    async fn cleanup(
        &self,
        _components: &Arc<RwLock<ComponentRegistry>>,
        _options: &ShutdownOptions,
    ) -> LifecycleResult<()> {
        let mut phase = self.current_phase.write().await;
        *phase = ShutdownPhase::Cleanup;
        drop(phase);

        info!("Performing cleanup");

        // Clean up resources, close connections, etc.
        tokio::time::sleep(Duration::from_millis(50)).await;

        Ok(())
    }

    /// Save component state
    async fn save_state(&self, components: &Arc<RwLock<ComponentRegistry>>) -> LifecycleResult<()> {
        info!("Saving component state");

        let registry = components.read().await;
        for (name, component) in &registry.components {
            if component.state == ComponentState::Running {
                debug!("Saving state for component: {}", name);
                // In a real implementation, this would save component state
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
            if state == ComponentState::Stopped {
                component.stop_time = Some(std::time::SystemTime::now());
            }
            Ok(())
        } else {
            Err(LifecycleError::ComponentNotFound(name.to_string()))
        }
    }

    /// Get current shutdown phase
    pub async fn get_current_phase(&self) -> LifecycleResult<ShutdownPhase> {
        let phase = self.current_phase.read().await;
        Ok(*phase)
    }
}
