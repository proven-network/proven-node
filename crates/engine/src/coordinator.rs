//! Service coordinator
//!
//! Manages service lifecycle and dependencies

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::RwLock;
use tracing::{error, info, warn};

use crate::error::{ConsensusResult, Error, ErrorKind};
use crate::foundation::traits::{
    ServiceCoordinator as ServiceCoordinatorTrait, ServiceLifecycle, lifecycle::ServiceStatus,
};

/// Service coordinator implementation
#[derive(Default)]
pub struct ServiceCoordinator {
    /// Registered services
    services: Arc<RwLock<HashMap<String, Arc<dyn ServiceLifecycle>>>>,
    /// Service start order
    start_order: Arc<RwLock<Vec<String>>>,
}

impl ServiceCoordinator {
    /// Create a new service coordinator
    pub fn new() -> Self {
        Self {
            services: Arc::new(RwLock::new(HashMap::new())),
            start_order: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Set service start order
    pub async fn set_start_order(&self, order: Vec<String>) {
        *self.start_order.write().await = order;
    }

    /// Register a service
    pub async fn register(&self, name: String, service: Arc<dyn ServiceLifecycle>) {
        let mut services = self.services.write().await;
        services.insert(name.clone(), service);

        let mut order = self.start_order.write().await;
        if !order.contains(&name) {
            order.push(name);
        }
    }
}

#[async_trait]
impl ServiceCoordinatorTrait for ServiceCoordinator {
    async fn register_service(
        &self,
        name: String,
        service: Arc<dyn ServiceLifecycle>,
    ) -> ConsensusResult<()> {
        let mut services = self.services.write().await;

        if services.contains_key(&name) {
            return Err(Error::with_context(
                ErrorKind::Service,
                format!("Service {name} already registered"),
            ));
        }

        services.insert(name.clone(), service);

        // Add to start order if not already present
        let mut order = self.start_order.write().await;
        if !order.contains(&name) {
            order.push(name);
        }

        Ok(())
    }

    async fn start_all(&self) -> ConsensusResult<()> {
        info!("Starting all services");

        let order = self.start_order.read().await.clone();
        let services = self.services.read().await;

        for name in &order {
            if let Some(service) = services.get(name) {
                info!("Starting service: {}", name);

                match service.start().await {
                    Ok(()) => info!("Service {} started successfully", name),
                    Err(e) => {
                        error!("Failed to start service {}: {}", name, e);

                        // Stop already started services
                        self.stop_started_services(&order, name).await;

                        return Err(Error::with_context(
                            ErrorKind::Service,
                            format!("Failed to start service {name}: {e}"),
                        ));
                    }
                }
            }
        }

        info!("All services started successfully");
        Ok(())
    }

    async fn stop_all(&self) -> ConsensusResult<()> {
        info!("Stopping all services");

        let mut order = self.start_order.read().await.clone();
        order.reverse(); // Stop in reverse order

        let services = self.services.read().await;
        let mut errors = Vec::new();

        for name in &order {
            if let Some(service) = services.get(name)
                && service.is_healthy().await
            {
                info!("Stopping service: {}", name);

                if let Err(e) = service.stop().await {
                    error!("Failed to stop service {}: {}", name, e);
                    errors.push(format!("{name}: {e}"));
                } else {
                    info!("Service {} stopped successfully", name);
                }
            }
        }

        if errors.is_empty() {
            info!("All services stopped successfully");
            Ok(())
        } else {
            Err(Error::with_context(
                ErrorKind::Service,
                format!("Failed to stop services: {}", errors.join(", ")),
            ))
        }
    }

    async fn get_status(&self) -> Vec<(String, ServiceStatus)> {
        let services = self.services.read().await;
        let mut status_list = Vec::new();

        for (name, service) in services.iter() {
            let status = service.status().await;
            status_list.push((name.clone(), status));
        }

        status_list
    }
}

impl ServiceCoordinator {
    /// Stop services that were started before a failure
    async fn stop_started_services(&self, order: &[String], failed_service: &str) {
        let services = self.services.read().await;

        for name in order {
            if name == failed_service {
                break; // Don't stop the failed service or ones after it
            }

            if let Some(service) = services.get(name)
                && service.is_healthy().await
            {
                warn!("Stopping service {} due to startup failure", name);
                if let Err(e) = service.stop().await {
                    error!("Failed to stop service {}: {}", name, e);
                }
            }
        }
    }

    /// Check if all services are healthy
    pub async fn all_healthy(&self) -> bool {
        let services = self.services.read().await;
        for service in services.values() {
            if !service.is_healthy().await {
                return false;
            }
        }
        true
    }
}
