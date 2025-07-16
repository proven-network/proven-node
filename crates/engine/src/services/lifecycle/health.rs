//! Health checking for lifecycle management

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use tokio::sync::RwLock;

use super::service::ComponentRegistry;
use super::types::*;

/// Health report for all components
#[derive(Debug, Clone)]
pub struct HealthReport {
    /// Overall system health
    pub overall: HealthStatus,
    /// Individual component health
    pub components: HashMap<String, ComponentHealth>,
    /// Critical issues
    pub issues: Vec<String>,
    /// Report timestamp
    pub timestamp: SystemTime,
}

/// Health checker for components
pub struct HealthChecker {
    /// Check interval
    check_interval: Duration,
    /// Last check results
    last_results: Arc<RwLock<Option<HealthReport>>>,
}

impl HealthChecker {
    /// Create a new health checker
    pub fn new(check_interval: Duration) -> Self {
        Self {
            check_interval,
            last_results: Arc::new(RwLock::new(None)),
        }
    }

    /// Check all components
    pub async fn check_all_components(
        &self,
        components: &Arc<RwLock<ComponentRegistry>>,
    ) -> LifecycleResult<HealthReport> {
        let registry = components.read().await;
        let mut component_health = HashMap::new();
        let mut issues = Vec::new();
        let mut unhealthy_count = 0;
        let mut degraded_count = 0;

        // Check each component
        for (name, info) in &registry.components {
            let health = self.check_component(info).await?;

            match &health.status {
                HealthStatus::Unhealthy { reason } => {
                    unhealthy_count += 1;
                    issues.push(format!("{name}: {reason}"));
                }
                HealthStatus::Degraded { reason } => {
                    degraded_count += 1;
                    issues.push(format!("{name} (degraded): {reason}"));
                }
                _ => {}
            }

            component_health.insert(name.clone(), health);
        }

        // Determine overall health
        let overall = if unhealthy_count > 0 {
            HealthStatus::Unhealthy {
                reason: format!("{unhealthy_count} components unhealthy"),
            }
        } else if degraded_count > 0 {
            HealthStatus::Degraded {
                reason: format!("{degraded_count} components degraded"),
            }
        } else if component_health.is_empty() {
            HealthStatus::Unknown
        } else {
            HealthStatus::Healthy
        };

        let report = HealthReport {
            overall,
            components: component_health,
            issues,
            timestamp: SystemTime::now(),
        };

        // Cache results
        let mut last_results = self.last_results.write().await;
        *last_results = Some(report.clone());

        Ok(report)
    }

    /// Check a single component
    async fn check_component(
        &self,
        info: &super::service::ComponentInfo,
    ) -> LifecycleResult<ComponentHealth> {
        let status = match info.state {
            ComponentState::Running => {
                // Component is running, check actual health
                // In a real implementation, this would perform actual health checks
                HealthStatus::Healthy
            }
            ComponentState::Failed => HealthStatus::Unhealthy {
                reason: "Component in failed state".to_string(),
            },
            ComponentState::NotInitialized | ComponentState::Stopped => HealthStatus::Unknown,
            _ => HealthStatus::Degraded {
                reason: format!("Component in transitional state: {:?}", info.state),
            },
        };

        let uptime = info
            .start_time
            .and_then(|start| SystemTime::now().duration_since(start).ok());

        Ok(ComponentHealth {
            name: info.name.clone(),
            state: info.state,
            status,
            last_check: SystemTime::now(),
            uptime,
            error_count: info.health.error_count,
            metrics: info.health.metrics.clone(),
        })
    }

    /// Get last health report
    pub async fn get_last_report(&self) -> Option<HealthReport> {
        let last_results = self.last_results.read().await;
        last_results.clone()
    }
}
