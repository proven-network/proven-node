//! Health checking and monitoring
//!
//! This module provides health checking functionality for the consensus system,
//! including component health checks and aggregate health reporting.

use crate::config::monitoring::HealthCheckConfig;
use crate::error::ConsensusResult;
use proven_governance::Governance;
use proven_network::Transport;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;
use tracing::{debug, warn};

/// Health status levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum HealthStatus {
    /// Component is healthy and functioning normally
    Healthy = 0,
    /// Component is degraded but still operational
    Degraded = 1,
    /// Component is unhealthy and may not be functioning properly
    Unhealthy = 2,
    /// Component status cannot be determined
    Unknown = 3,
}

impl HealthStatus {
    /// Check if the status indicates the component is operational
    pub fn is_operational(&self) -> bool {
        matches!(self, HealthStatus::Healthy | HealthStatus::Degraded)
    }

    /// Combine two health statuses, returning the worse one
    pub fn combine(self, other: HealthStatus) -> HealthStatus {
        std::cmp::max(self, other)
    }
}

/// Health of a specific component
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentHealth {
    /// Component name
    pub name: String,
    /// Component type
    pub component_type: ComponentType,
    /// Health status
    pub status: HealthStatus,
    /// Last check timestamp
    pub last_check: SystemTime,
    /// Additional details
    pub details: HashMap<String, String>,
    /// Error message if unhealthy
    pub error: Option<String>,
}

/// Type of component being monitored
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ComponentType {
    /// Global consensus layer
    Global,
    /// Consensus group
    Group,
    /// Individual node
    Node,
    /// Stream
    Stream,
    /// Network connectivity
    Network,
    /// Storage backend
    Storage,
    /// System resource
    System,
}

/// Result of a health check
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheckResult {
    /// Overall health status
    pub status: HealthStatus,
    /// Timestamp of the check
    pub timestamp: SystemTime,
    /// Individual component health
    pub components: Vec<ComponentHealth>,
    /// Summary message
    pub summary: String,
}

impl HealthCheckResult {
    /// Create a new health check result
    pub fn new(components: Vec<ComponentHealth>) -> Self {
        let status = components
            .iter()
            .map(|c| c.status)
            .fold(HealthStatus::Healthy, |acc, s| acc.combine(s));

        let unhealthy_count = components
            .iter()
            .filter(|c| c.status == HealthStatus::Unhealthy)
            .count();

        let degraded_count = components
            .iter()
            .filter(|c| c.status == HealthStatus::Degraded)
            .count();

        let summary = match status {
            HealthStatus::Healthy => "All components healthy".to_string(),
            HealthStatus::Degraded => format!("{degraded_count} components degraded"),
            HealthStatus::Unhealthy => format!("{unhealthy_count} components unhealthy"),
            HealthStatus::Unknown => "Unable to determine health status".to_string(),
        };

        Self {
            status,
            timestamp: SystemTime::now(),
            components,
            summary,
        }
    }

    /// Get components by type
    pub fn components_by_type(&self, component_type: ComponentType) -> Vec<&ComponentHealth> {
        self.components
            .iter()
            .filter(|c| c.component_type == component_type)
            .collect()
    }

    /// Get unhealthy components
    pub fn unhealthy_components(&self) -> Vec<&ComponentHealth> {
        self.components
            .iter()
            .filter(|c| c.status == HealthStatus::Unhealthy)
            .collect()
    }
}

/// Health report for external consumption
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthReport {
    /// Report ID
    pub id: String,
    /// Report timestamp
    pub timestamp: SystemTime,
    /// Overall system health
    pub system_health: HealthStatus,
    /// Component summary
    pub component_summary: ComponentSummary,
    /// Critical issues
    pub critical_issues: Vec<HealthIssue>,
    /// Warnings
    pub warnings: Vec<HealthIssue>,
    /// Recommendations
    pub recommendations: Vec<String>,
}

/// Summary of component health
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentSummary {
    pub total_components: usize,
    pub healthy_count: usize,
    pub degraded_count: usize,
    pub unhealthy_count: usize,
    pub unknown_count: usize,
}

impl From<&[ComponentHealth]> for ComponentSummary {
    fn from(components: &[ComponentHealth]) -> Self {
        Self {
            total_components: components.len(),
            healthy_count: components
                .iter()
                .filter(|c| c.status == HealthStatus::Healthy)
                .count(),
            degraded_count: components
                .iter()
                .filter(|c| c.status == HealthStatus::Degraded)
                .count(),
            unhealthy_count: components
                .iter()
                .filter(|c| c.status == HealthStatus::Unhealthy)
                .count(),
            unknown_count: components
                .iter()
                .filter(|c| c.status == HealthStatus::Unknown)
                .count(),
        }
    }
}

/// Health issue description
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthIssue {
    /// Issue severity
    pub severity: IssueSeverity,
    /// Component affected
    pub component: String,
    /// Issue description
    pub description: String,
    /// Potential impact
    pub impact: String,
    /// Suggested action
    pub action: Option<String>,
}

/// Issue severity levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum IssueSeverity {
    Low,
    Medium,
    High,
    Critical,
}

/// Health checker for the consensus system
pub struct HealthChecker<T, G>
where
    T: Transport,
    G: Governance,
{
    config: HealthCheckConfig,
    system_view: Arc<super::views::SystemView<T, G>>,
    last_check: Arc<RwLock<Option<HealthCheckResult>>>,
    check_interval: Duration,
}

impl<T, G> HealthChecker<T, G>
where
    T: Transport,
    G: Governance,
{
    /// Create a new health checker
    pub fn new(
        config: HealthCheckConfig,
        system_view: Arc<super::views::SystemView<T, G>>,
    ) -> Self {
        Self {
            check_interval: config.interval,
            config,
            system_view,
            last_check: Arc::new(RwLock::new(None)),
        }
    }

    /// Start periodic health checks
    pub async fn start(&self) {
        if !self.config.enabled {
            debug!("Health checks disabled");
            return;
        }

        let system_view = self.system_view.clone();
        let last_check = self.last_check.clone();
        let interval = self.check_interval;
        let timeout = self.config.timeout;

        tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);

            loop {
                interval_timer.tick().await;

                // Run health check with timeout
                let check_future = Self::run_health_check(system_view.clone());
                let result = match tokio::time::timeout(timeout, check_future).await {
                    Ok(Ok(result)) => result,
                    Ok(Err(e)) => {
                        warn!("Health check failed: {}", e);
                        HealthCheckResult::new(vec![ComponentHealth {
                            name: "system".to_string(),
                            component_type: ComponentType::System,
                            status: HealthStatus::Unknown,
                            last_check: SystemTime::now(),
                            details: HashMap::new(),
                            error: Some(e.to_string()),
                        }])
                    }
                    Err(_) => {
                        warn!("Health check timed out");
                        HealthCheckResult::new(vec![ComponentHealth {
                            name: "system".to_string(),
                            component_type: ComponentType::System,
                            status: HealthStatus::Unknown,
                            last_check: SystemTime::now(),
                            details: HashMap::new(),
                            error: Some("Health check timed out".to_string()),
                        }])
                    }
                };

                // Store result
                let mut last = last_check.write().await;
                *last = Some(result);
            }
        });
    }

    /// Get the last health check result
    pub async fn get_last_check(&self) -> Option<HealthCheckResult> {
        let last = self.last_check.read().await;
        last.clone()
    }

    /// Run a health check now
    pub async fn check_now(&self) -> ConsensusResult<HealthCheckResult> {
        Self::run_health_check(self.system_view.clone()).await
    }

    /// Generate a health report
    pub async fn generate_report(&self) -> ConsensusResult<HealthReport> {
        let result = self.check_now().await?;

        let mut critical_issues = Vec::new();
        let mut warnings = Vec::new();
        let mut recommendations = Vec::new();

        // Analyze components
        for component in &result.components {
            match component.status {
                HealthStatus::Unhealthy => {
                    critical_issues.push(HealthIssue {
                        severity: IssueSeverity::Critical,
                        component: component.name.clone(),
                        description: component
                            .error
                            .clone()
                            .unwrap_or_else(|| "Component unhealthy".to_string()),
                        impact: "Service degradation or outage".to_string(),
                        action: Some("Investigate and resolve immediately".to_string()),
                    });
                }
                HealthStatus::Degraded => {
                    warnings.push(HealthIssue {
                        severity: IssueSeverity::Medium,
                        component: component.name.clone(),
                        description: "Component performance degraded".to_string(),
                        impact: "Reduced performance or reliability".to_string(),
                        action: Some("Monitor and plan maintenance".to_string()),
                    });
                }
                _ => {}
            }
        }

        // Add recommendations
        if critical_issues.is_empty() && warnings.is_empty() {
            recommendations.push("System is healthy. Continue normal monitoring.".to_string());
        } else {
            if !critical_issues.is_empty() {
                recommendations.push("Address critical issues immediately".to_string());
            }
            if !warnings.is_empty() {
                recommendations.push("Schedule maintenance for degraded components".to_string());
            }
        }

        Ok(HealthReport {
            id: uuid::Uuid::new_v4().to_string(),
            timestamp: result.timestamp,
            system_health: result.status,
            component_summary: ComponentSummary::from(result.components.as_slice()),
            critical_issues,
            warnings,
            recommendations,
        })
    }

    /// Run a complete health check
    async fn run_health_check(
        system_view: Arc<super::views::SystemView<T, G>>,
    ) -> ConsensusResult<HealthCheckResult> {
        let mut components = Vec::new();

        // Check streams
        let stream_health = system_view.stream_view.get_all_stream_health().await?;
        for health in stream_health {
            components.push(ComponentHealth {
                name: health.stream_name.clone(),
                component_type: ComponentType::Stream,
                status: match health.status {
                    super::views::HealthStatus::Healthy => HealthStatus::Healthy,
                    super::views::HealthStatus::Degraded => HealthStatus::Degraded,
                    super::views::HealthStatus::Unhealthy => HealthStatus::Unhealthy,
                    super::views::HealthStatus::Unknown => HealthStatus::Unknown,
                },
                last_check: SystemTime::now(),
                details: {
                    let mut details = HashMap::new();
                    details.insert("group_id".to_string(), health.group_id.to_string());
                    details.insert("is_paused".to_string(), health.is_paused.to_string());
                    details
                },
                error: if health.status == super::views::HealthStatus::Unhealthy {
                    Some("Stream unhealthy".to_string())
                } else {
                    None
                },
            });
        }

        // Check groups
        let group_health = system_view.group_view.get_all_group_health().await?;
        for health in group_health {
            components.push(ComponentHealth {
                name: format!("group-{}", health.group_id),
                component_type: ComponentType::Group,
                status: match health.status {
                    super::views::HealthStatus::Healthy => HealthStatus::Healthy,
                    super::views::HealthStatus::Degraded => HealthStatus::Degraded,
                    super::views::HealthStatus::Unhealthy => HealthStatus::Unhealthy,
                    super::views::HealthStatus::Unknown => HealthStatus::Unknown,
                },
                last_check: SystemTime::now(),
                details: {
                    let mut details = HashMap::new();
                    details.insert("member_count".to_string(), health.member_count.to_string());
                    details.insert("has_quorum".to_string(), health.has_quorum.to_string());
                    details.insert("stream_count".to_string(), health.stream_count.to_string());
                    details
                },
                error: if !health.has_quorum {
                    Some("Group lacks quorum".to_string())
                } else {
                    None
                },
            });
        }

        // Check nodes
        let node_health = system_view.node_view.get_all_node_health().await?;
        for health in node_health {
            components.push(ComponentHealth {
                name: format!("node-{}", health.node_id),
                component_type: ComponentType::Node,
                status: match health.status {
                    super::views::HealthStatus::Healthy => HealthStatus::Healthy,
                    super::views::HealthStatus::Degraded => HealthStatus::Degraded,
                    super::views::HealthStatus::Unhealthy => HealthStatus::Unhealthy,
                    super::views::HealthStatus::Unknown => HealthStatus::Unknown,
                },
                last_check: SystemTime::now(),
                details: {
                    let mut details = HashMap::new();
                    details.insert("group_count".to_string(), health.group_count.to_string());
                    details.insert("is_reachable".to_string(), health.is_reachable.to_string());
                    details
                },
                error: if !health.is_reachable {
                    Some("Node unreachable".to_string())
                } else {
                    None
                },
            });
        }

        Ok(HealthCheckResult::new(components))
    }
}
