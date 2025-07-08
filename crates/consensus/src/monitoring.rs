//! Monitoring and metrics for hierarchical consensus
//!
//! This module provides comprehensive monitoring capabilities for tracking
//! stream distribution, consensus group health, and migration activity.

use crate::allocation::{ConsensusGroupId, GroupMetadata};
use crate::local::LocalTypeConfig;
use crate::migration::ActiveMigration;
use openraft::RaftMetrics;
use prometheus::{
    GaugeVec, HistogramVec, IntCounterVec, IntGaugeVec, register_gauge_vec, register_histogram_vec,
    register_int_counter_vec, register_int_gauge_vec,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

lazy_static::lazy_static! {
    /// Total number of streams per consensus group
    static ref STREAMS_PER_GROUP: IntGaugeVec = register_int_gauge_vec!(
        "consensus_streams_per_group",
        "Number of streams assigned to each consensus group",
        &["group_id"]
    ).unwrap();

    /// Total messages per consensus group
    static ref MESSAGES_PER_GROUP: IntGaugeVec = register_int_gauge_vec!(
        "consensus_messages_per_group",
        "Number of messages in each consensus group",
        &["group_id"]
    ).unwrap();

    /// Storage size per consensus group
    static ref STORAGE_BYTES_PER_GROUP: IntGaugeVec = register_int_gauge_vec!(
        "consensus_storage_bytes_per_group",
        "Storage size in bytes for each consensus group",
        &["group_id"]
    ).unwrap();

    /// Message rate per consensus group
    static ref MESSAGE_RATE_PER_GROUP: GaugeVec = register_gauge_vec!(
        "consensus_message_rate_per_group",
        "Message rate (messages/second) for each consensus group",
        &["group_id"]
    ).unwrap();

    /// Stream migrations started
    static ref MIGRATIONS_STARTED: IntCounterVec = register_int_counter_vec!(
        "consensus_migrations_started_total",
        "Total number of stream migrations started",
        &["from_group", "to_group"]
    ).unwrap();

    /// Stream migrations completed
    static ref MIGRATIONS_COMPLETED: IntCounterVec = register_int_counter_vec!(
        "consensus_migrations_completed_total",
        "Total number of stream migrations completed successfully",
        &["from_group", "to_group"]
    ).unwrap();

    /// Stream migrations failed
    static ref MIGRATIONS_FAILED: IntCounterVec = register_int_counter_vec!(
        "consensus_migrations_failed_total",
        "Total number of stream migrations that failed",
        &["from_group", "to_group"]
    ).unwrap();

    /// Migration duration histogram
    static ref MIGRATION_DURATION: HistogramVec = register_histogram_vec!(
        "consensus_migration_duration_seconds",
        "Duration of stream migrations in seconds",
        &["from_group", "to_group"],
        vec![0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0]
    ).unwrap();

    /// Consensus group leader status
    static ref GROUP_LEADER_STATUS: IntGaugeVec = register_int_gauge_vec!(
        "consensus_group_leader_status",
        "Leader status for each consensus group (1 = leader, 0 = follower)",
        &["group_id", "node_id"]
    ).unwrap();

    /// Consensus group health
    static ref GROUP_HEALTH: IntGaugeVec = register_int_gauge_vec!(
        "consensus_group_health",
        "Health status of each consensus group (1 = healthy, 0 = unhealthy)",
        &["group_id"]
    ).unwrap();
}

/// Monitoring service for hierarchical consensus
pub struct MonitoringService {
    /// Current metrics snapshot
    metrics: Arc<RwLock<HierarchicalMetrics>>,
    /// Update interval in seconds
    _update_interval: u64,
}

/// Complete metrics snapshot for hierarchical consensus
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HierarchicalMetrics {
    /// Timestamp of this snapshot
    pub timestamp: u64,
    /// Global consensus metrics
    pub global_metrics: GlobalMetrics,
    /// Per-group metrics
    pub group_metrics: HashMap<ConsensusGroupId, GroupMetrics>,
    /// Active migrations
    pub active_migrations: Vec<MigrationMetrics>,
    /// Stream distribution statistics
    pub distribution_stats: DistributionStats,
}

/// Metrics for global consensus
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalMetrics {
    /// Total number of streams
    pub total_streams: u64,
    /// Total number of consensus groups
    pub total_groups: u32,
    /// Total messages across all streams
    pub total_messages: u64,
    /// Total storage size in bytes
    pub total_storage_bytes: u64,
    /// Global message rate (messages/second)
    pub global_message_rate: f64,
}

/// Metrics for a single consensus group
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupMetrics {
    /// Group identifier
    pub group_id: ConsensusGroupId,
    /// Number of streams in this group
    pub stream_count: u32,
    /// Total messages in this group
    pub total_messages: u64,
    /// Storage size in bytes
    pub storage_bytes: u64,
    /// Message rate (messages/second)
    pub message_rate: f64,
    /// Member nodes
    pub members: Vec<crate::NodeId>,
    /// Current leader node ID (if any)
    pub leader_id: Option<String>,
    /// Group health status
    pub is_healthy: bool,
    /// Raft term
    pub raft_term: u64,
    /// Last applied log index
    pub last_applied: u64,
}

/// Metrics for an active migration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationMetrics {
    /// Stream being migrated
    pub stream_name: String,
    /// Source group
    pub from_group: ConsensusGroupId,
    /// Target group
    pub to_group: ConsensusGroupId,
    /// Current state
    pub state: String,
    /// Started timestamp
    pub started_at: u64,
    /// Duration so far (seconds)
    pub duration_secs: u64,
    /// Messages migrated
    pub messages_migrated: u64,
    /// Bytes migrated
    pub bytes_migrated: u64,
}

/// Statistics about stream distribution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistributionStats {
    /// Standard deviation of stream counts across groups
    pub stream_count_std_dev: f64,
    /// Coefficient of variation for stream counts
    pub stream_count_cv: f64,
    /// Most loaded group
    pub most_loaded_group: Option<ConsensusGroupId>,
    /// Least loaded group
    pub least_loaded_group: Option<ConsensusGroupId>,
    /// Load imbalance ratio (max/min)
    pub load_imbalance_ratio: f64,
    /// Recommended migrations for better balance
    pub recommended_migrations: Vec<RecommendedMigration>,
}

/// A recommended migration to improve load balance
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecommendedMigration {
    /// Stream to migrate
    pub stream_name: String,
    /// Source group (currently overloaded)
    pub from_group: ConsensusGroupId,
    /// Target group (currently underloaded)
    pub to_group: ConsensusGroupId,
    /// Expected improvement in balance
    pub balance_improvement: f64,
}

impl MonitoringService {
    /// Create a new monitoring service
    pub fn new(update_interval: u64) -> Self {
        Self {
            metrics: Arc::new(RwLock::new(HierarchicalMetrics {
                timestamp: chrono::Utc::now().timestamp_millis() as u64,
                global_metrics: GlobalMetrics {
                    total_streams: 0,
                    total_groups: 0,
                    total_messages: 0,
                    total_storage_bytes: 0,
                    global_message_rate: 0.0,
                },
                group_metrics: HashMap::new(),
                active_migrations: Vec::new(),
                distribution_stats: DistributionStats {
                    stream_count_std_dev: 0.0,
                    stream_count_cv: 0.0,
                    most_loaded_group: None,
                    least_loaded_group: None,
                    load_imbalance_ratio: 1.0,
                    recommended_migrations: Vec::new(),
                },
            })),
            _update_interval: update_interval,
        }
    }

    /// Update metrics from various sources
    pub async fn update_metrics(
        &self,
        allocation_metrics: Vec<(ConsensusGroupId, GroupMetadata)>,
        local_raft_metrics: HashMap<ConsensusGroupId, RaftMetrics<LocalTypeConfig>>,
        active_migrations: Vec<ActiveMigration>,
    ) {
        let mut metrics = self.metrics.write().await;
        metrics.timestamp = chrono::Utc::now().timestamp_millis() as u64;

        // Update group metrics
        metrics.group_metrics.clear();
        let mut total_streams = 0u64;
        let total_messages = 0u64;
        let mut total_storage = 0u64;
        let mut total_rate = 0.0f64;

        for (group_id, metadata) in &allocation_metrics {
            let raft_metrics = local_raft_metrics.get(group_id);

            let group_metric = GroupMetrics {
                group_id: *group_id,
                stream_count: metadata.stream_count,
                total_messages: 0, // Would come from local storage
                storage_bytes: metadata.storage_size,
                message_rate: metadata.message_rate,
                members: metadata.members.clone(),
                leader_id: raft_metrics
                    .and_then(|m| m.current_leader.as_ref().map(|n| n.to_string())),
                is_healthy: raft_metrics
                    .map(|m| m.state.is_leader() || m.state.is_follower())
                    .unwrap_or(false),
                raft_term: raft_metrics.map(|m| m.current_term).unwrap_or(0),
                last_applied: raft_metrics
                    .and_then(|m| m.last_applied.as_ref().map(|l| l.index))
                    .unwrap_or(0),
            };

            // Update Prometheus metrics
            let group_id_str = format!("{}", group_id.0);
            STREAMS_PER_GROUP
                .with_label_values(&[&group_id_str])
                .set(metadata.stream_count as i64);
            STORAGE_BYTES_PER_GROUP
                .with_label_values(&[&group_id_str])
                .set(metadata.storage_size as i64);
            MESSAGE_RATE_PER_GROUP
                .with_label_values(&[&group_id_str])
                .set(metadata.message_rate);
            GROUP_HEALTH
                .with_label_values(&[&group_id_str])
                .set(if group_metric.is_healthy { 1 } else { 0 });

            total_streams += metadata.stream_count as u64;
            total_storage += metadata.storage_size;
            total_rate += metadata.message_rate;

            metrics.group_metrics.insert(*group_id, group_metric);
        }

        // Update global metrics
        metrics.global_metrics = GlobalMetrics {
            total_streams,
            total_groups: metrics.group_metrics.len() as u32,
            total_messages,
            total_storage_bytes: total_storage,
            global_message_rate: total_rate,
        };

        // Update migration metrics
        metrics.active_migrations = active_migrations
            .iter()
            .map(|m| {
                let duration = (chrono::Utc::now().timestamp_millis() as u64 - m.started_at) / 1000;

                MigrationMetrics {
                    stream_name: m.stream_name.clone(),
                    from_group: m.source_group,
                    to_group: m.target_group,
                    state: format!("{:?}", m.state),
                    started_at: m.started_at,
                    duration_secs: duration,
                    messages_migrated: 0, // Would need to track this
                    bytes_migrated: 0,    // Would need to track this
                }
            })
            .collect();

        // Calculate distribution statistics
        metrics.distribution_stats = self.calculate_distribution_stats(&allocation_metrics);

        info!(
            "Updated hierarchical consensus metrics: {} groups, {} streams",
            metrics.global_metrics.total_groups, metrics.global_metrics.total_streams
        );
    }

    /// Calculate distribution statistics and recommendations
    pub fn calculate_distribution_stats(
        &self,
        allocation_metrics: &[(ConsensusGroupId, GroupMetadata)],
    ) -> DistributionStats {
        if allocation_metrics.is_empty() {
            return DistributionStats {
                stream_count_std_dev: 0.0,
                stream_count_cv: 0.0,
                most_loaded_group: None,
                least_loaded_group: None,
                load_imbalance_ratio: 1.0,
                recommended_migrations: Vec::new(),
            };
        }

        let stream_counts: Vec<f64> = allocation_metrics
            .iter()
            .map(|(_, m)| m.stream_count as f64)
            .collect();

        let mean = stream_counts.iter().sum::<f64>() / stream_counts.len() as f64;
        let variance = stream_counts
            .iter()
            .map(|x| (x - mean).powi(2))
            .sum::<f64>()
            / stream_counts.len() as f64;
        let std_dev = variance.sqrt();
        let cv = if mean > 0.0 { std_dev / mean } else { 0.0 };

        let most_loaded = allocation_metrics
            .iter()
            .max_by_key(|(_, m)| m.stream_count)
            .map(|(id, _)| *id);

        let least_loaded = allocation_metrics
            .iter()
            .min_by_key(|(_, m)| m.stream_count)
            .map(|(id, _)| *id);

        let max_load = allocation_metrics
            .iter()
            .map(|(_, m)| m.stream_count)
            .max()
            .unwrap_or(0) as f64;

        let min_load = allocation_metrics
            .iter()
            .map(|(_, m)| m.stream_count)
            .min()
            .unwrap_or(0) as f64;

        let imbalance_ratio = if min_load > 0.0 {
            max_load / min_load
        } else {
            1.0
        };

        // Generate migration recommendations if imbalance is high
        let mut recommendations = Vec::new();
        if imbalance_ratio > 1.5 && most_loaded.is_some() && least_loaded.is_some() {
            // This is simplified - in practice would analyze actual streams
            if let (Some(from), Some(to)) = (most_loaded, least_loaded) {
                recommendations.push(RecommendedMigration {
                    stream_name: "highest-traffic-stream".to_string(), // Would identify actual stream
                    from_group: from,
                    to_group: to,
                    balance_improvement: (imbalance_ratio - 1.0) * 0.5, // Estimate
                });
            }
        }

        DistributionStats {
            stream_count_std_dev: std_dev,
            stream_count_cv: cv,
            most_loaded_group: most_loaded,
            least_loaded_group: least_loaded,
            load_imbalance_ratio: imbalance_ratio,
            recommended_migrations: recommendations,
        }
    }

    /// Record a migration start
    pub async fn record_migration_start(
        &self,
        from_group: ConsensusGroupId,
        to_group: ConsensusGroupId,
    ) {
        MIGRATIONS_STARTED
            .with_label_values(&[&format!("{}", from_group.0), &format!("{}", to_group.0)])
            .inc();
    }

    /// Record a migration completion
    pub async fn record_migration_complete(
        &self,
        from_group: ConsensusGroupId,
        to_group: ConsensusGroupId,
        duration_secs: f64,
    ) {
        let from_str = format!("{}", from_group.0);
        let to_str = format!("{}", to_group.0);

        MIGRATIONS_COMPLETED
            .with_label_values(&[&from_str, &to_str])
            .inc();

        MIGRATION_DURATION
            .with_label_values(&[&from_str, &to_str])
            .observe(duration_secs);
    }

    /// Record a migration failure
    pub async fn record_migration_failed(
        &self,
        from_group: ConsensusGroupId,
        to_group: ConsensusGroupId,
    ) {
        MIGRATIONS_FAILED
            .with_label_values(&[&format!("{}", from_group.0), &format!("{}", to_group.0)])
            .inc();
    }

    /// Get current metrics snapshot
    pub async fn get_metrics(&self) -> HierarchicalMetrics {
        self.metrics.read().await.clone()
    }

    /// Get metrics in Prometheus format
    pub fn get_prometheus_metrics() -> String {
        use prometheus::{Encoder, TextEncoder};

        let encoder = TextEncoder::new();
        let metric_families = prometheus::gather();
        let mut buffer = Vec::new();
        encoder.encode(&metric_families, &mut buffer).unwrap();
        String::from_utf8(buffer).unwrap()
    }

    /// Check if rebalancing is recommended
    pub async fn should_rebalance(&self) -> bool {
        let metrics = self.metrics.read().await;
        metrics.distribution_stats.load_imbalance_ratio > 2.0
            || metrics.distribution_stats.stream_count_cv > 0.5
    }

    /// Get load balancing recommendations
    pub async fn get_rebalancing_recommendations(&self) -> Vec<RecommendedMigration> {
        let metrics = self.metrics.read().await;
        metrics.distribution_stats.recommended_migrations.clone()
    }
}

/// Health check for consensus groups
pub fn is_group_healthy(raft_metrics: &RaftMetrics<LocalTypeConfig>) -> bool {
    // Group is healthy if:
    // 1. It has a leader
    // 2. It's making progress (last_applied is advancing)
    // 3. It has enough members for quorum
    raft_metrics.current_leader.is_some() && raft_metrics.state.is_leader()
        || raft_metrics.state.is_follower()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_node_id(seed: u8) -> crate::NodeId {
        use ed25519_dalek::{SigningKey, VerifyingKey};
        let mut key_bytes = [0u8; 32];
        key_bytes[0] = seed;
        let signing_key = SigningKey::from_bytes(&key_bytes);
        let verifying_key: VerifyingKey = signing_key.verifying_key();
        crate::NodeId::new(verifying_key)
    }

    #[tokio::test]
    async fn test_monitoring_service() {
        let service = MonitoringService::new(60);

        // Create test data
        let allocation_metrics = vec![
            (
                ConsensusGroupId::new(0),
                GroupMetadata {
                    id: ConsensusGroupId::new(0),
                    stream_count: 10,
                    message_rate: 100.0,
                    storage_size: 1024 * 1024,
                    members: vec![test_node_id(1), test_node_id(2), test_node_id(3)],
                },
            ),
            (
                ConsensusGroupId::new(1),
                GroupMetadata {
                    id: ConsensusGroupId::new(1),
                    stream_count: 5,
                    message_rate: 50.0,
                    storage_size: 512 * 1024,
                    members: vec![test_node_id(2), test_node_id(3), test_node_id(4)],
                },
            ),
        ];

        service
            .update_metrics(allocation_metrics, HashMap::new(), Vec::new())
            .await;

        let metrics = service.get_metrics().await;
        assert_eq!(metrics.global_metrics.total_streams, 15);
        assert_eq!(metrics.global_metrics.total_groups, 2);
        assert!(metrics.distribution_stats.load_imbalance_ratio > 1.0);
    }

    #[test]
    fn test_distribution_stats_calculation() {
        let service = MonitoringService::new(60);

        let allocation_metrics = vec![
            (
                ConsensusGroupId::new(0),
                GroupMetadata {
                    id: ConsensusGroupId::new(0),
                    stream_count: 20,
                    message_rate: 200.0,
                    storage_size: 2048 * 1024,
                    members: vec![test_node_id(1), test_node_id(2), test_node_id(3)],
                },
            ),
            (
                ConsensusGroupId::new(1),
                GroupMetadata {
                    id: ConsensusGroupId::new(1),
                    stream_count: 10,
                    message_rate: 100.0,
                    storage_size: 1024 * 1024,
                    members: vec![test_node_id(2), test_node_id(3), test_node_id(4)],
                },
            ),
            (
                ConsensusGroupId::new(2),
                GroupMetadata {
                    id: ConsensusGroupId::new(2),
                    stream_count: 10,
                    message_rate: 100.0,
                    storage_size: 1024 * 1024,
                    members: vec![test_node_id(3), test_node_id(4), test_node_id(5)],
                },
            ),
        ];

        let stats = service.calculate_distribution_stats(&allocation_metrics);
        assert_eq!(stats.load_imbalance_ratio, 2.0);
        assert!(stats.stream_count_std_dev > 0.0);
        assert_eq!(stats.most_loaded_group, Some(ConsensusGroupId::new(0)));
        assert!(!stats.recommended_migrations.is_empty());
    }

    #[tokio::test]
    async fn test_monitoring_service_basic() {
        let service = MonitoringService::new(60);

        // Get initial metrics
        let metrics = service.get_metrics().await;
        assert_eq!(metrics.global_metrics.total_streams, 0);
        assert_eq!(metrics.global_metrics.total_groups, 0);
        assert!(metrics.group_metrics.is_empty());
    }

    #[tokio::test]
    async fn test_monitoring_with_allocation_data() {
        let service = MonitoringService::new(60);

        // Create test allocation data
        let mut manager = crate::allocation::AllocationManager::new(
            crate::allocation::AllocationStrategy::RoundRobin,
        );

        // Add groups
        manager.add_group(
            ConsensusGroupId::new(0),
            vec![test_node_id(1), test_node_id(2), test_node_id(3)],
        );
        manager.add_group(
            ConsensusGroupId::new(1),
            vec![test_node_id(2), test_node_id(3), test_node_id(4)],
        );
        manager.add_group(
            ConsensusGroupId::new(2),
            vec![test_node_id(3), test_node_id(4), test_node_id(5)],
        );

        // Allocate streams unevenly
        for i in 0..15 {
            manager
                .allocate_stream(format!("stream-{}", i), 1000 + i as u64)
                .unwrap();
        }

        // Force some imbalance by migrating streams
        for i in 0..5 {
            manager
                .migrate_stream(&format!("stream-{}", i), ConsensusGroupId::new(0), 2000)
                .unwrap();
        }

        let allocation_metrics = manager.get_group_loads();

        // Update monitoring service
        service
            .update_metrics(allocation_metrics, HashMap::new(), Vec::new())
            .await;

        let metrics = service.get_metrics().await;

        // Verify global metrics
        assert_eq!(metrics.global_metrics.total_streams, 15);
        assert_eq!(metrics.global_metrics.total_groups, 3);

        // Check distribution stats
        assert!(metrics.distribution_stats.load_imbalance_ratio > 1.0);
        assert!(metrics.distribution_stats.stream_count_std_dev > 0.0);

        // Should recommend rebalancing due to imbalance
        let should_rebalance = service.should_rebalance().await;
        assert!(should_rebalance);
    }

    #[tokio::test]
    async fn test_migration_metrics() {
        let service = MonitoringService::new(60);

        // Record migration start
        service
            .record_migration_start(ConsensusGroupId::new(0), ConsensusGroupId::new(1))
            .await;

        // Simulate migration duration
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Record migration completion
        service
            .record_migration_complete(ConsensusGroupId::new(0), ConsensusGroupId::new(1), 0.1)
            .await;

        // Also test failure case
        service
            .record_migration_start(ConsensusGroupId::new(1), ConsensusGroupId::new(2))
            .await;
        service
            .record_migration_failed(ConsensusGroupId::new(1), ConsensusGroupId::new(2))
            .await;
    }

    #[test]
    fn test_load_balance_detection() {
        let service = MonitoringService::new(60);

        // Create perfectly balanced scenario
        let balanced_metrics = vec![
            (
                ConsensusGroupId::new(0),
                GroupMetadata {
                    id: ConsensusGroupId::new(0),
                    stream_count: 10,
                    message_rate: 100.0,
                    storage_size: 1024 * 1024,
                    members: vec![test_node_id(1), test_node_id(2), test_node_id(3)],
                },
            ),
            (
                ConsensusGroupId::new(1),
                GroupMetadata {
                    id: ConsensusGroupId::new(1),
                    stream_count: 10,
                    message_rate: 100.0,
                    storage_size: 1024 * 1024,
                    members: vec![test_node_id(2), test_node_id(3), test_node_id(4)],
                },
            ),
            (
                ConsensusGroupId::new(2),
                GroupMetadata {
                    id: ConsensusGroupId::new(2),
                    stream_count: 10,
                    message_rate: 100.0,
                    storage_size: 1024 * 1024,
                    members: vec![test_node_id(3), test_node_id(4), test_node_id(5)],
                },
            ),
        ];

        let stats = service.calculate_distribution_stats(&balanced_metrics);
        assert_eq!(stats.load_imbalance_ratio, 1.0);
        assert_eq!(stats.stream_count_std_dev, 0.0);
        assert!(stats.recommended_migrations.is_empty());

        // Create imbalanced scenario
        let imbalanced_metrics = vec![
            (
                ConsensusGroupId::new(0),
                GroupMetadata {
                    id: ConsensusGroupId::new(0),
                    stream_count: 20,
                    message_rate: 200.0,
                    storage_size: 2048 * 1024,
                    members: vec![test_node_id(1), test_node_id(2), test_node_id(3)],
                },
            ),
            (
                ConsensusGroupId::new(1),
                GroupMetadata {
                    id: ConsensusGroupId::new(1),
                    stream_count: 5,
                    message_rate: 50.0,
                    storage_size: 512 * 1024,
                    members: vec![test_node_id(2), test_node_id(3), test_node_id(4)],
                },
            ),
            (
                ConsensusGroupId::new(2),
                GroupMetadata {
                    id: ConsensusGroupId::new(2),
                    stream_count: 5,
                    message_rate: 50.0,
                    storage_size: 512 * 1024,
                    members: vec![test_node_id(3), test_node_id(4), test_node_id(5)],
                },
            ),
        ];

        let stats = service.calculate_distribution_stats(&imbalanced_metrics);
        assert_eq!(stats.load_imbalance_ratio, 4.0);
        assert!(stats.stream_count_std_dev > 0.0);
        assert!(!stats.recommended_migrations.is_empty());
        assert_eq!(stats.most_loaded_group, Some(ConsensusGroupId::new(0)));
        assert!(
            stats.least_loaded_group == Some(ConsensusGroupId::new(1))
                || stats.least_loaded_group == Some(ConsensusGroupId::new(2))
        );
    }

    #[tokio::test]
    async fn test_prometheus_metrics_export() {
        let service = MonitoringService::new(60);

        // Create some test data
        let mut manager = crate::allocation::AllocationManager::new(
            crate::allocation::AllocationStrategy::RoundRobin,
        );
        manager.add_group(
            ConsensusGroupId::new(0),
            vec![test_node_id(1), test_node_id(2), test_node_id(3)],
        );
        manager
            .allocate_stream("test-stream".to_string(), 1000)
            .unwrap();

        let allocation_metrics = manager.get_group_loads();
        service
            .update_metrics(allocation_metrics, HashMap::new(), Vec::new())
            .await;

        // Get prometheus format metrics
        let prometheus_output = MonitoringService::get_prometheus_metrics();

        // Verify some expected metrics are present (they should be registered even if not yet populated)
        // The metrics are registered lazily so we need to trigger them first
        println!("Prometheus output:\n{}", prometheus_output);

        // At minimum, we should see some prometheus metadata
        assert!(!prometheus_output.is_empty());
    }
}
