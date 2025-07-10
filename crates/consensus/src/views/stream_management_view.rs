//! Stream lifecycle and allocation management
//!
//! The StreamManager is responsible for:
//! - Stream creation, update, and deletion
//! - Stream allocation to consensus groups
//! - Stream health monitoring
//! - Stream metadata management

use crate::{
    allocation::ConsensusGroupId,
    error::{ConsensusResult, Error, StreamError},
    global::{StreamConfig, global_state::GlobalState},
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Stream health status
#[derive(Debug, Clone, PartialEq)]
pub enum StreamHealth {
    /// Stream is healthy and operating normally
    Healthy,
    /// Stream is experiencing issues but still operational
    Degraded {
        /// The reason the stream is degraded
        reason: String,
    },
    /// Stream is not operational
    Unhealthy {
        /// The reason the stream is unhealthy
        reason: String,
    },
    /// Stream health is unknown (e.g., newly created)
    Unknown,
}

/// Stream statistics
#[derive(Debug, Clone, Default)]
pub struct StreamStats {
    /// Total messages in the stream
    pub message_count: u64,
    /// Last sequence number
    pub last_sequence: u64,
    /// Number of active subscriptions
    pub subscription_count: usize,
    /// Last update timestamp
    pub last_updated: Option<u64>,
    /// Stream size in bytes
    pub size_bytes: u64,
}

/// Stream health information
#[derive(Debug, Clone)]
pub struct StreamHealthInfo {
    /// Whether the stream is healthy
    pub is_healthy: bool,
    /// Health status
    pub health: StreamHealth,
    /// Stream statistics
    pub stats: Option<StreamStats>,
}

/// Read-only view of stream state for orchestrator decision-making
pub struct StreamManagementView {
    /// Reference to global state
    global_state: Arc<GlobalState>,
    /// Stream health tracking
    stream_health: Arc<RwLock<HashMap<String, StreamHealth>>>,
    /// Stream statistics cache
    stream_stats: Arc<RwLock<HashMap<String, StreamStats>>>,
    /// Stream to group allocation tracking
    stream_allocations: Arc<RwLock<HashMap<String, ConsensusGroupId>>>,
}

impl StreamManagementView {
    /// Create a new StreamView
    pub fn new(global_state: Arc<GlobalState>) -> Self {
        Self {
            global_state,
            stream_health: Arc::new(RwLock::new(HashMap::new())),
            stream_stats: Arc::new(RwLock::new(HashMap::new())),
            stream_allocations: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    // Note: Stream creation validation should be handled by StreamManagementOperationValidator
    // Views should remain read-only and not perform validations

    /// Record stream creation (called after consensus)
    pub async fn record_stream_created(&self, name: &str, group_id: ConsensusGroupId) {
        // Track allocation
        self.stream_allocations
            .write()
            .await
            .insert(name.to_string(), group_id);

        // Initialize health as unknown
        self.stream_health
            .write()
            .await
            .insert(name.to_string(), StreamHealth::Unknown);

        // Initialize empty stats
        self.stream_stats
            .write()
            .await
            .insert(name.to_string(), StreamStats::default());

        info!(
            "Recorded stream '{}' allocated to group {:?}",
            name, group_id
        );
    }

    /// Validate stream configuration update
    pub async fn validate_stream_update(
        &self,
        name: &str,
        _config: &StreamConfig,
    ) -> ConsensusResult<()> {
        // Check if stream exists
        self.global_state
            .get_stream_config(name)
            .await
            .ok_or_else(|| {
                Error::Stream(StreamError::NotFound {
                    name: name.to_string(),
                })
            })?;

        Ok(())
    }

    /// Validate stream deletion
    pub async fn validate_stream_deletion(&self, name: &str) -> ConsensusResult<()> {
        // Check if stream exists
        if self.global_state.get_stream_config(name).await.is_none() {
            return Err(Error::Stream(StreamError::NotFound {
                name: name.to_string(),
            }));
        }

        Ok(())
    }

    /// Record stream deletion (called after consensus)
    pub async fn record_stream_deleted(&self, name: &str) {
        // Remove from tracking
        self.stream_allocations.write().await.remove(name);
        self.stream_health.write().await.remove(name);
        self.stream_stats.write().await.remove(name);

        info!("Recorded deletion of stream '{}'", name);
    }

    /// Get stream allocation
    pub async fn get_stream_allocation(&self, name: &str) -> Option<ConsensusGroupId> {
        self.stream_allocations.read().await.get(name).copied()
    }

    /// Validate stream reallocation
    pub async fn validate_stream_reallocation(
        &self,
        name: &str,
        new_group_id: ConsensusGroupId,
    ) -> ConsensusResult<()> {
        // Check if stream exists
        self.global_state
            .get_stream_config(name)
            .await
            .ok_or_else(|| {
                Error::Stream(StreamError::NotFound {
                    name: name.to_string(),
                })
            })?;

        // Get current allocation
        let current_group = self.get_stream_allocation(name).await;
        if let Some(current) = current_group {
            if current == new_group_id {
                return Ok(()); // No change needed
            }
        }

        Ok(())
    }

    /// Record stream reallocation (called after consensus)
    pub async fn record_stream_reallocated(
        &self,
        name: &str,
        old_group_id: ConsensusGroupId,
        new_group_id: ConsensusGroupId,
    ) {
        // Update allocation tracking
        self.stream_allocations
            .write()
            .await
            .insert(name.to_string(), new_group_id);

        // Mark as degraded during migration
        self.update_health(
            name,
            StreamHealth::Degraded {
                reason: format!(
                    "Migrating from group {:?} to {:?}",
                    old_group_id, new_group_id
                ),
            },
        )
        .await;

        info!(
            "Recorded reallocation of stream '{}' from group {:?} to {:?}",
            name, old_group_id, new_group_id
        );
    }

    /// Update stream health status
    pub async fn update_health(&self, name: &str, health: StreamHealth) {
        self.stream_health
            .write()
            .await
            .insert(name.to_string(), health.clone());

        match &health {
            StreamHealth::Unhealthy { reason } => {
                warn!("Stream '{}' is unhealthy: {}", name, reason);
            }
            StreamHealth::Degraded { reason } => {
                debug!("Stream '{}' is degraded: {}", name, reason);
            }
            _ => {}
        }
    }

    /// Get stream health
    pub async fn get_health(&self, name: &str) -> StreamHealth {
        self.stream_health
            .read()
            .await
            .get(name)
            .cloned()
            .unwrap_or(StreamHealth::Unknown)
    }

    /// Update stream statistics
    pub async fn update_stats(&self, name: &str, stats: StreamStats) {
        self.stream_stats
            .write()
            .await
            .insert(name.to_string(), stats);
    }

    /// Get stream statistics
    pub async fn get_stats(&self, name: &str) -> Option<StreamStats> {
        self.stream_stats.read().await.get(name).cloned()
    }

    /// List all streams
    pub async fn list_streams(&self) -> Vec<String> {
        // Get from tracking data
        self.stream_allocations
            .read()
            .await
            .keys()
            .cloned()
            .collect()
    }

    /// List streams by group
    pub async fn list_streams_by_group(&self, group_id: ConsensusGroupId) -> Vec<String> {
        let allocations = self.stream_allocations.read().await;
        allocations
            .iter()
            .filter(|(_, gid)| **gid == group_id)
            .map(|(name, _)| name.clone())
            .collect()
    }

    /// Get streams with health status
    pub async fn list_streams_with_health(&self) -> HashMap<String, StreamHealth> {
        self.stream_health.read().await.clone()
    }

    /// Get unhealthy streams
    pub async fn get_unhealthy_streams(&self) -> Vec<(String, String)> {
        let health_map = self.stream_health.read().await;
        health_map
            .iter()
            .filter_map(|(name, health)| match health {
                StreamHealth::Unhealthy { reason } => Some((name.clone(), reason.clone())),
                _ => None,
            })
            .collect()
    }

    // Note: Stream name validation should be handled by StreamManagementOperationValidator
    // Views should remain read-only and not perform validations

    /// Check if any streams need rebalancing
    pub async fn get_rebalance_candidates(&self, _threshold: f64) -> Vec<String> {
        // This would analyze stream distribution and identify candidates
        // For now, return empty vec
        Vec::new()
    }

    /// Get stream health status
    pub async fn get_stream_health(&self, name: &str) -> Option<StreamHealthInfo> {
        let health = self.stream_health.read().await;
        let stats = self.stream_stats.read().await;

        health.get(name).map(|h| StreamHealthInfo {
            is_healthy: matches!(h, StreamHealth::Healthy),
            health: h.clone(),
            stats: stats.get(name).cloned(),
        })
    }

    /// Get all stream health information
    pub async fn get_all_stream_health(&self) -> HashMap<String, StreamHealthInfo> {
        let health = self.stream_health.read().await;
        let stats = self.stream_stats.read().await;

        health
            .iter()
            .map(|(name, h)| {
                (
                    name.clone(),
                    StreamHealthInfo {
                        is_healthy: matches!(h, StreamHealth::Healthy),
                        health: h.clone(),
                        stats: stats.get(name).cloned(),
                    },
                )
            })
            .collect()
    }

    /// Monitor stream health across all streams
    pub async fn monitor_stream_health(&self) {
        let streams = self.list_streams().await;

        for stream in streams {
            // Check various health metrics
            if let Some(stats) = self.get_stats(&stream).await {
                // Example health checks
                let health = if stats.last_updated.is_none() {
                    StreamHealth::Unknown
                } else if let Some(last_updated) = stats.last_updated {
                    let now = chrono::Utc::now().timestamp() as u64;
                    if now - last_updated > 3600 {
                        // No updates in last hour
                        StreamHealth::Degraded {
                            reason: "No recent updates".to_string(),
                        }
                    } else {
                        StreamHealth::Healthy
                    }
                } else {
                    StreamHealth::Healthy
                };

                self.update_health(&stream, health).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_stream_creation() {
        let global_state = Arc::new(GlobalState::new());
        let view = StreamManagementView::new(global_state);

        // Record the creation
        view.record_stream_created("test-stream", ConsensusGroupId::new(1))
            .await;

        // Views are read-only and don't perform validation
        // Validation should be done by StreamManagementOperationValidator
    }

    // Note: Stream name validation tests have been removed
    // as validation should be handled by StreamManagementOperationValidator, not views

    #[tokio::test]
    async fn test_stream_health_tracking() {
        let global_state = Arc::new(GlobalState::new());
        let view = StreamManagementView::new(global_state);

        // Record stream creation
        view.record_stream_created("test-stream", ConsensusGroupId::new(1))
            .await;

        // Initially unknown
        assert_eq!(view.get_health("test-stream").await, StreamHealth::Unknown);

        // Update health
        view.update_health("test-stream", StreamHealth::Healthy)
            .await;
        assert_eq!(view.get_health("test-stream").await, StreamHealth::Healthy);

        // Mark as unhealthy
        view.update_health(
            "test-stream",
            StreamHealth::Unhealthy {
                reason: "Test failure".to_string(),
            },
        )
        .await;

        let unhealthy = view.get_unhealthy_streams().await;
        assert_eq!(unhealthy.len(), 1);
        assert_eq!(unhealthy[0].0, "test-stream");
        assert_eq!(unhealthy[0].1, "Test failure");
    }
}
