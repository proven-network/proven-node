//! Bootable trait implementation for NetworkManager

use async_trait::async_trait;
use proven_bootable::Bootable;
use proven_governance::Governance;
use proven_transport::Transport;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info};

use crate::NetworkManager;

/// Wrapper to make Arc<NetworkManager> bootable
pub struct BootableNetworkManager<T, G>
where
    T: Transport,
    G: Governance,
{
    inner: Arc<NetworkManager<T, G>>,
}

impl<T, G> BootableNetworkManager<T, G>
where
    T: Transport,
    G: Governance,
{
    /// Create a new bootable network manager wrapper
    pub fn new(network_manager: Arc<NetworkManager<T, G>>) -> Self {
        Self {
            inner: network_manager,
        }
    }

    /// Get the inner network manager
    pub fn inner(&self) -> &Arc<NetworkManager<T, G>> {
        &self.inner
    }
}

#[async_trait]
impl<T, G> Bootable for BootableNetworkManager<T, G>
where
    T: Transport,
    G: Governance,
{
    fn bootable_name(&self) -> &str {
        "NetworkManager"
    }

    async fn start(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Starting NetworkManager as bootable service");
        self.inner.clone().start_tasks().await?;
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Shutting down NetworkManager");

        let mut state = self.inner.bootable_state.write().await;

        // Send shutdown signal
        if let Some(shutdown_tx) = state.shutdown_tx.take() {
            let _ = shutdown_tx.send(()).await;
        }

        // Shutdown transport
        self.inner.transport.shutdown().await?;

        // Wait for all tasks to complete
        let tasks = vec![state.router_task.take(), state.cleanup_task.take()];

        for (i, task) in tasks.into_iter().enumerate() {
            if let Some(handle) = task {
                let task_name = match i {
                    0 => "router",
                    1 => "cleanup",
                    _ => "unknown",
                };

                match tokio::time::timeout(Duration::from_secs(5), handle).await {
                    Ok(Ok(())) => {
                        debug!("NetworkManager {} task shut down cleanly", task_name);
                    }
                    Ok(Err(e)) => {
                        error!("NetworkManager {} task panicked: {}", task_name, e);
                    }
                    Err(_) => {
                        error!(
                            "NetworkManager {} task did not shut down within timeout",
                            task_name
                        );
                    }
                }
            }
        }

        info!("NetworkManager shutdown complete");
        Ok(())
    }

    async fn wait(&self) {
        // Wait for any of the tasks to complete (which should only happen on error)
        let (router_handle, cleanup_handle) = {
            let state = self.inner.bootable_state.read().await;
            (
                state.router_task.as_ref().map(|t| t.abort_handle()),
                state.cleanup_task.as_ref().map(|t| t.abort_handle()),
            )
        };

        // Monitor all tasks
        loop {
            let mut task_failed = false;

            if let Some(handle) = &router_handle
                && handle.is_finished()
            {
                error!("NetworkManager router task failed unexpectedly");
                task_failed = true;
            }

            if let Some(handle) = &cleanup_handle
                && handle.is_finished()
            {
                error!("NetworkManager cleanup task failed unexpectedly");
                task_failed = true;
            }

            if task_failed {
                break;
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}
