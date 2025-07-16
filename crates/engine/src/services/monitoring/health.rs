//! Health checking functionality for the monitoring service

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use tokio::sync::RwLock;

use super::types::*;
use super::views::SystemView;
use crate::error::ConsensusResult;

/// Health checker for monitoring system components
pub struct HealthChecker {
    /// Check interval
    check_interval: Duration,

    /// System view for checking health
    system_view: Arc<SystemView>,

    /// Last health check result
    last_check: Arc<RwLock<Option<SystemHealth>>>,
}

impl HealthChecker {
    /// Create a new health checker
    pub fn new(check_interval: Duration, system_view: Arc<SystemView>) -> Self {
        Self {
            check_interval,
            system_view,
            last_check: Arc::new(RwLock::new(None)),
        }
    }

    /// Perform a health check now
    pub async fn check_now(&self) -> ConsensusResult<()> {
        let health = self.calculate_system_health().await?;

        let mut last_check = self.last_check.write().await;
        *last_check = Some(health);

        Ok(())
    }

    /// Get the last system health check result
    pub async fn get_system_health(&self) -> ConsensusResult<SystemHealth> {
        let last_check = self.last_check.read().await;

        if let Some(health) = last_check.as_ref() {
            Ok(health.clone())
        } else {
            // Perform check if no previous result
            drop(last_check);
            self.check_now().await?;

            let last_check = self.last_check.read().await;
            Ok(last_check.as_ref().unwrap().clone())
        }
    }

    /// Calculate current system health
    async fn calculate_system_health(&self) -> ConsensusResult<SystemHealth> {
        let mut components = HashMap::new();

        // Check global consensus health
        let global_health = self.check_global_consensus().await?;
        components.insert("global_consensus".to_string(), global_health);

        // Check group consensus health
        let groups_health = self.check_groups_consensus().await?;
        components.insert("groups_consensus".to_string(), groups_health);

        // Check network health
        let network_health = self.check_network().await?;
        components.insert("network".to_string(), network_health);

        // Check storage health
        let storage_health = self.check_storage().await?;
        components.insert("storage".to_string(), storage_health);

        // Determine overall status
        let overall_status = if components
            .values()
            .all(|c| c.status == HealthStatus::Healthy)
        {
            HealthStatus::Healthy
        } else if components
            .values()
            .any(|c| c.status == HealthStatus::Unhealthy)
        {
            HealthStatus::Unhealthy
        } else {
            HealthStatus::Degraded
        };

        // Get cluster metrics
        let cluster_info = self.system_view.get_cluster_info().await?;
        let group_count = self.system_view.get_group_count().await?;
        let stream_count = self.system_view.get_stream_count().await?;

        Ok(SystemHealth {
            status: overall_status,
            components,
            cluster_size: cluster_info.node_count,
            active_groups: group_count,
            total_streams: stream_count,
            last_update: SystemTime::now(),
        })
    }

    /// Check global consensus health
    async fn check_global_consensus(&self) -> ConsensusResult<ComponentHealth> {
        let has_leader = self.system_view.has_global_leader().await?;
        let is_stable = self.system_view.is_global_consensus_stable().await?;

        let status = if has_leader && is_stable {
            HealthStatus::Healthy
        } else if has_leader {
            HealthStatus::Degraded
        } else {
            HealthStatus::Unhealthy
        };

        let details = if !has_leader {
            Some("No global leader elected".to_string())
        } else if !is_stable {
            Some("Global consensus is not stable".to_string())
        } else {
            None
        };

        Ok(ComponentHealth {
            name: "global_consensus".to_string(),
            status,
            last_check: SystemTime::now(),
            details,
        })
    }

    /// Check groups consensus health
    async fn check_groups_consensus(&self) -> ConsensusResult<ComponentHealth> {
        let groups = self.system_view.get_all_group_health().await?;

        let unhealthy_count = groups
            .iter()
            .filter(|g| g.status != HealthStatus::Healthy)
            .count();

        let status = if unhealthy_count == 0 {
            HealthStatus::Healthy
        } else if unhealthy_count < groups.len() / 2 {
            HealthStatus::Degraded
        } else {
            HealthStatus::Unhealthy
        };

        let details = if unhealthy_count > 0 {
            Some(format!(
                "{} out of {} groups are unhealthy",
                unhealthy_count,
                groups.len()
            ))
        } else {
            None
        };

        Ok(ComponentHealth {
            name: "groups_consensus".to_string(),
            status,
            last_check: SystemTime::now(),
            details,
        })
    }

    /// Check network health
    async fn check_network(&self) -> ConsensusResult<ComponentHealth> {
        let connected_peers = self.system_view.get_connected_peer_count().await?;
        let expected_peers = self.system_view.get_expected_peer_count().await?;

        let status = if connected_peers >= expected_peers {
            HealthStatus::Healthy
        } else if connected_peers > expected_peers / 2 {
            HealthStatus::Degraded
        } else {
            HealthStatus::Unhealthy
        };

        let details = Some(format!(
            "Connected to {connected_peers} out of {expected_peers} expected peers"
        ));

        Ok(ComponentHealth {
            name: "network".to_string(),
            status,
            last_check: SystemTime::now(),
            details,
        })
    }

    /// Check storage health
    async fn check_storage(&self) -> ConsensusResult<ComponentHealth> {
        // For now, assume storage is healthy if we can access it
        // In a real implementation, we'd check disk space, I/O performance, etc.
        Ok(ComponentHealth {
            name: "storage".to_string(),
            status: HealthStatus::Healthy,
            last_check: SystemTime::now(),
            details: None,
        })
    }
}
