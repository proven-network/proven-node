//! Metrics collection and reporting
//!
//! This module provides metrics collection, aggregation, and export
//! functionality for the consensus system.

use crate::ConsensusGroupId;
use prometheus::{CounterVec, Gauge, GaugeVec, HistogramOpts, HistogramVec, Opts, Registry};
use proven_governance::Governance;
use proven_network::Transport;
use proven_topology::NodeId;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// Metrics registry for the consensus system
pub struct MetricsRegistry {
    /// Prometheus registry
    registry: Registry,
    /// Consensus metrics
    pub consensus: ConsensusMetrics,
    /// Stream metrics
    pub streams: StreamMetrics,
    /// Group metrics
    pub groups: GroupMetrics,
    /// Node metrics
    pub nodes: NodeMetrics,
}

impl MetricsRegistry {
    /// Create a new metrics registry
    pub fn new() -> prometheus::Result<Self> {
        let registry = Registry::new();

        let consensus = ConsensusMetrics::new(&registry)?;
        let streams = StreamMetrics::new(&registry)?;
        let groups = GroupMetrics::new(&registry)?;
        let nodes = NodeMetrics::new(&registry)?;

        Ok(Self {
            registry,
            consensus,
            streams,
            groups,
            nodes,
        })
    }

    /// Get the Prometheus registry
    pub fn registry(&self) -> &Registry {
        &self.registry
    }
}

/// Consensus-wide metrics
pub struct ConsensusMetrics {
    /// Total number of operations processed
    pub operations_total: CounterVec,
    /// Operation processing duration
    pub operation_duration: HistogramVec,
    /// Active streams gauge
    pub active_streams: Gauge,
    /// Active groups gauge
    pub active_groups: Gauge,
    /// Active nodes gauge
    pub active_nodes: Gauge,
}

impl ConsensusMetrics {
    fn new(registry: &Registry) -> prometheus::Result<Self> {
        let operations_total = CounterVec::new(
            Opts::new(
                "consensus_operations_total",
                "Total number of consensus operations",
            ),
            &["operation_type", "status"],
        )?;
        registry.register(Box::new(operations_total.clone()))?;

        let operation_duration = HistogramVec::new(
            HistogramOpts::new(
                "consensus_operation_duration_seconds",
                "Operation processing duration",
            ),
            &["operation_type"],
        )?;
        registry.register(Box::new(operation_duration.clone()))?;

        let active_streams = Gauge::new("consensus_active_streams", "Number of active streams")?;
        registry.register(Box::new(active_streams.clone()))?;

        let active_groups = Gauge::new(
            "consensus_active_groups",
            "Number of active consensus groups",
        )?;
        registry.register(Box::new(active_groups.clone()))?;

        let active_nodes = Gauge::new("consensus_active_nodes", "Number of active nodes")?;
        registry.register(Box::new(active_nodes.clone()))?;

        Ok(Self {
            operations_total,
            operation_duration,
            active_streams,
            active_groups,
            active_nodes,
        })
    }
}

/// Stream-specific metrics
pub struct StreamMetrics {
    /// Messages processed per stream
    pub messages_total: CounterVec,
    /// Message processing latency
    pub message_latency: HistogramVec,
    /// Stream storage size
    pub storage_bytes: GaugeVec,
    /// Stream error count
    pub errors_total: CounterVec,
    /// Stream checkpoint operations
    pub checkpoints_total: CounterVec,
}

impl StreamMetrics {
    fn new(registry: &Registry) -> prometheus::Result<Self> {
        let messages_total = CounterVec::new(
            Opts::new(
                "stream_messages_total",
                "Total messages processed per stream",
            ),
            &["stream_name", "group_id"],
        )?;
        registry.register(Box::new(messages_total.clone()))?;

        let message_latency = HistogramVec::new(
            HistogramOpts::new(
                "stream_message_latency_seconds",
                "Message processing latency",
            ),
            &["stream_name"],
        )?;
        registry.register(Box::new(message_latency.clone()))?;

        let storage_bytes = GaugeVec::new(
            Opts::new("stream_storage_bytes", "Stream storage size in bytes"),
            &["stream_name"],
        )?;
        registry.register(Box::new(storage_bytes.clone()))?;

        let errors_total = CounterVec::new(
            Opts::new("stream_errors_total", "Total stream errors"),
            &["stream_name", "error_type"],
        )?;
        registry.register(Box::new(errors_total.clone()))?;

        let checkpoints_total = CounterVec::new(
            Opts::new("stream_checkpoints_total", "Total checkpoint operations"),
            &["stream_name", "operation"],
        )?;
        registry.register(Box::new(checkpoints_total.clone()))?;

        Ok(Self {
            messages_total,
            message_latency,
            storage_bytes,
            errors_total,
            checkpoints_total,
        })
    }

    /// Record message processed
    pub fn record_message(&self, stream_name: &str, group_id: ConsensusGroupId, latency: Duration) {
        self.messages_total
            .with_label_values(&[stream_name, &group_id.to_string()])
            .inc();

        self.message_latency
            .with_label_values(&[stream_name])
            .observe(latency.as_secs_f64());
    }

    /// Update storage size
    pub fn update_storage_size(&self, stream_name: &str, bytes: u64) {
        self.storage_bytes
            .with_label_values(&[stream_name])
            .set(bytes as f64);
    }

    /// Record error
    pub fn record_error(&self, stream_name: &str, error_type: &str) {
        self.errors_total
            .with_label_values(&[stream_name, error_type])
            .inc();
    }

    /// Record checkpoint operation
    pub fn record_checkpoint(&self, stream_name: &str, operation: &str) {
        self.checkpoints_total
            .with_label_values(&[stream_name, operation])
            .inc();
    }
}

/// Group-specific metrics
pub struct GroupMetrics {
    /// Raft state transitions
    pub state_transitions: CounterVec,
    /// Elections triggered
    pub elections_total: CounterVec,
    /// Current term gauge
    pub current_term: GaugeVec,
    /// Committed index
    pub committed_index: GaugeVec,
    /// Group member count
    pub member_count: GaugeVec,
}

impl GroupMetrics {
    fn new(registry: &Registry) -> prometheus::Result<Self> {
        let state_transitions = CounterVec::new(
            Opts::new("group_state_transitions_total", "Raft state transitions"),
            &["group_id", "from_state", "to_state"],
        )?;
        registry.register(Box::new(state_transitions.clone()))?;

        let elections_total = CounterVec::new(
            Opts::new("group_elections_total", "Total elections triggered"),
            &["group_id", "reason"],
        )?;
        registry.register(Box::new(elections_total.clone()))?;

        let current_term = GaugeVec::new(
            Opts::new("group_current_term", "Current Raft term"),
            &["group_id"],
        )?;
        registry.register(Box::new(current_term.clone()))?;

        let committed_index = GaugeVec::new(
            Opts::new("group_committed_index", "Raft committed log index"),
            &["group_id"],
        )?;
        registry.register(Box::new(committed_index.clone()))?;

        let member_count = GaugeVec::new(
            Opts::new("group_member_count", "Number of members in group"),
            &["group_id"],
        )?;
        registry.register(Box::new(member_count.clone()))?;

        Ok(Self {
            state_transitions,
            elections_total,
            current_term,
            committed_index,
            member_count,
        })
    }

    /// Record state transition
    pub fn record_state_transition(&self, group_id: ConsensusGroupId, from: &str, to: &str) {
        self.state_transitions
            .with_label_values(&[&group_id.to_string(), from, to])
            .inc();
    }

    /// Record election
    pub fn record_election(&self, group_id: ConsensusGroupId, reason: &str) {
        self.elections_total
            .with_label_values(&[&group_id.to_string(), reason])
            .inc();
    }

    /// Update term
    pub fn update_term(&self, group_id: ConsensusGroupId, term: u64) {
        self.current_term
            .with_label_values(&[&group_id.to_string()])
            .set(term as f64);
    }

    /// Update committed index
    pub fn update_committed_index(&self, group_id: ConsensusGroupId, index: u64) {
        self.committed_index
            .with_label_values(&[&group_id.to_string()])
            .set(index as f64);
    }

    /// Update member count
    pub fn update_member_count(&self, group_id: ConsensusGroupId, count: usize) {
        self.member_count
            .with_label_values(&[&group_id.to_string()])
            .set(count as f64);
    }
}

/// Node-specific metrics
pub struct NodeMetrics {
    /// Network message count
    pub network_messages: CounterVec,
    /// Network latency
    pub network_latency: HistogramVec,
    /// Resource usage gauges
    pub cpu_usage: GaugeVec,
    pub memory_usage: GaugeVec,
    pub storage_usage: GaugeVec,
    /// Connection count
    pub connections: GaugeVec,
}

impl NodeMetrics {
    fn new(registry: &Registry) -> prometheus::Result<Self> {
        let network_messages = CounterVec::new(
            Opts::new("node_network_messages_total", "Total network messages"),
            &["node_id", "message_type", "direction"],
        )?;
        registry.register(Box::new(network_messages.clone()))?;

        let network_latency = HistogramVec::new(
            HistogramOpts::new("node_network_latency_seconds", "Network latency to peers"),
            &["from_node", "to_node"],
        )?;
        registry.register(Box::new(network_latency.clone()))?;

        let cpu_usage = GaugeVec::new(
            Opts::new("node_cpu_usage_percent", "CPU usage percentage"),
            &["node_id"],
        )?;
        registry.register(Box::new(cpu_usage.clone()))?;

        let memory_usage = GaugeVec::new(
            Opts::new("node_memory_usage_percent", "Memory usage percentage"),
            &["node_id"],
        )?;
        registry.register(Box::new(memory_usage.clone()))?;

        let storage_usage = GaugeVec::new(
            Opts::new("node_storage_usage_percent", "Storage usage percentage"),
            &["node_id"],
        )?;
        registry.register(Box::new(storage_usage.clone()))?;

        let connections = GaugeVec::new(
            Opts::new("node_connections", "Active network connections"),
            &["node_id"],
        )?;
        registry.register(Box::new(connections.clone()))?;

        Ok(Self {
            network_messages,
            network_latency,
            cpu_usage,
            memory_usage,
            storage_usage,
            connections,
        })
    }

    /// Record network message
    pub fn record_network_message(&self, node_id: &NodeId, message_type: &str, direction: &str) {
        self.network_messages
            .with_label_values(&[&node_id.to_string(), message_type, direction])
            .inc();
    }

    /// Record network latency
    pub fn record_network_latency(&self, from: &NodeId, to: &NodeId, latency: Duration) {
        self.network_latency
            .with_label_values(&[&from.to_string(), &to.to_string()])
            .observe(latency.as_secs_f64());
    }

    /// Update resource usage
    pub fn update_resource_usage(&self, node_id: &NodeId, cpu: f32, memory: f32, storage: f32) {
        self.cpu_usage
            .with_label_values(&[&node_id.to_string()])
            .set(cpu as f64);

        self.memory_usage
            .with_label_values(&[&node_id.to_string()])
            .set(memory as f64);

        self.storage_usage
            .with_label_values(&[&node_id.to_string()])
            .set(storage as f64);
    }

    /// Update connection count
    pub fn update_connections(&self, node_id: &NodeId, count: usize) {
        self.connections
            .with_label_values(&[&node_id.to_string()])
            .set(count as f64);
    }
}

/// Metrics collector that periodically collects and updates metrics
pub struct MetricsCollector<T, G>
where
    T: Transport,
    G: Governance,
{
    registry: Arc<MetricsRegistry>,
    system_view: Arc<super::views::SystemView<T, G>>,
    interval: Duration,
}

impl<T, G> MetricsCollector<T, G>
where
    T: Transport,
    G: Governance,
{
    /// Create a new metrics collector
    pub fn new(
        registry: Arc<MetricsRegistry>,
        system_view: Arc<super::views::SystemView<T, G>>,
        interval: Duration,
    ) -> Self {
        Self {
            registry,
            system_view,
            interval,
        }
    }

    /// Get the metrics registry
    pub fn registry(&self) -> &Arc<MetricsRegistry> {
        &self.registry
    }

    /// Start collecting metrics
    pub async fn start(&self) {
        let registry = self.registry.clone();
        let system_view = self.system_view.clone();
        let interval = self.interval;

        tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);

            loop {
                interval_timer.tick().await;

                // Update system-wide metrics
                if let Ok(summary) = system_view.get_system_summary().await {
                    registry
                        .consensus
                        .active_streams
                        .set(summary.stream_count as f64);
                    registry
                        .consensus
                        .active_groups
                        .set(summary.group_count as f64);
                    registry
                        .consensus
                        .active_nodes
                        .set(summary.node_count as f64);
                }

                // Update group metrics
                if let Ok(groups) = system_view.group_view.get_all_group_health().await {
                    for group in groups {
                        registry
                            .groups
                            .update_member_count(group.group_id, group.member_count);
                    }
                }
            }
        });
    }
}

/// Serializable metrics snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    /// Timestamp of the snapshot
    pub timestamp: std::time::SystemTime,
    /// Stream metrics
    pub streams: HashMap<String, StreamMetricsSnapshot>,
    /// Group metrics
    pub groups: HashMap<ConsensusGroupId, GroupMetricsSnapshot>,
    /// Node metrics
    pub nodes: HashMap<NodeId, NodeMetricsSnapshot>,
    /// System totals
    pub totals: SystemMetricsTotals,
}

/// Stream metrics snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamMetricsSnapshot {
    pub messages_total: u64,
    pub error_count: u64,
    pub storage_bytes: u64,
    pub avg_latency_ms: f64,
}

/// Group metrics snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupMetricsSnapshot {
    pub current_term: u64,
    pub committed_index: u64,
    pub member_count: usize,
    pub elections_count: u64,
}

/// Node metrics snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeMetricsSnapshot {
    pub cpu_usage: f32,
    pub memory_usage: f32,
    pub storage_usage: f32,
    pub connection_count: usize,
    pub messages_sent: u64,
    pub messages_received: u64,
}

/// System metrics totals
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemMetricsTotals {
    pub total_operations: u64,
    pub successful_operations: u64,
    pub failed_operations: u64,
    pub active_streams: usize,
    pub active_groups: usize,
    pub active_nodes: usize,
}
