//! Metrics collection for Prometheus integration

use std::sync::Arc;
use std::time::Duration;

use prometheus::{Counter, Gauge, HistogramOpts, Registry};
use prometheus::{register_counter_with_registry, register_gauge_with_registry};

use super::types::MonitoringError;
use super::views::SystemView;
use crate::error::ConsensusResult;

/// Metrics registry for Prometheus
pub struct MetricsRegistry {
    /// Prometheus registry
    registry: Registry,

    /// Consensus metrics
    pub consensus: ConsensusMetrics,

    /// Stream metrics
    pub streams: StreamMetrics,

    /// Network metrics
    pub network: NetworkMetrics,
}

/// Consensus-specific metrics
pub struct ConsensusMetrics {
    /// Total operations
    pub operations_total: prometheus::CounterVec,

    /// Operation duration histogram
    pub operation_duration: prometheus::HistogramVec,

    /// Current term gauge
    pub current_term: Gauge,

    /// Number of groups
    pub group_count: Gauge,

    /// Number of nodes
    pub node_count: Gauge,
}

/// Stream-specific metrics
pub struct StreamMetrics {
    /// Messages published
    messages_published: prometheus::CounterVec,

    /// Message latency
    message_latency: prometheus::HistogramVec,

    /// Active streams
    active_streams: Gauge,

    /// Total storage bytes
    storage_bytes: Gauge,
}

/// Network-specific metrics
pub struct NetworkMetrics {
    /// Messages sent
    pub messages_sent: Counter,

    /// Messages received
    pub messages_received: Counter,

    /// Connected peers
    pub connected_peers: Gauge,

    /// Network latency
    pub peer_latency: prometheus::HistogramVec,
}

impl MetricsRegistry {
    /// Create a new metrics registry
    pub fn new() -> Result<Self, MonitoringError> {
        let registry = Registry::new();

        // Create consensus metrics
        let operations_total = prometheus::CounterVec::new(
            prometheus::Opts::new("consensus_operations_total", "Total consensus operations"),
            &["operation_type", "status"],
        )
        .map_err(|e| MonitoringError::MetricsError(e.to_string()))?;
        registry
            .register(Box::new(operations_total.clone()))
            .map_err(|e| MonitoringError::MetricsError(e.to_string()))?;

        let operation_duration = prometheus::HistogramVec::new(
            HistogramOpts::new(
                "consensus_operation_duration_seconds",
                "Operation duration in seconds",
            ),
            &["operation_type"],
        )
        .map_err(|e| MonitoringError::MetricsError(e.to_string()))?;
        registry
            .register(Box::new(operation_duration.clone()))
            .map_err(|e| MonitoringError::MetricsError(e.to_string()))?;

        let current_term = register_gauge_with_registry!(
            "consensus_current_term",
            "Current consensus term",
            registry
        )
        .map_err(|e| MonitoringError::MetricsError(e.to_string()))?;

        let group_count = register_gauge_with_registry!(
            "consensus_group_count",
            "Number of consensus groups",
            registry
        )
        .map_err(|e| MonitoringError::MetricsError(e.to_string()))?;

        let node_count = register_gauge_with_registry!(
            "consensus_node_count",
            "Number of nodes in cluster",
            registry
        )
        .map_err(|e| MonitoringError::MetricsError(e.to_string()))?;

        let consensus = ConsensusMetrics {
            operations_total,
            operation_duration,
            current_term,
            group_count,
            node_count,
        };

        // Create stream metrics
        let messages_published = prometheus::CounterVec::new(
            prometheus::Opts::new(
                "stream_messages_published_total",
                "Total messages published",
            ),
            &["stream", "group"],
        )
        .map_err(|e| MonitoringError::MetricsError(e.to_string()))?;
        registry
            .register(Box::new(messages_published.clone()))
            .map_err(|e| MonitoringError::MetricsError(e.to_string()))?;

        let message_latency = prometheus::HistogramVec::new(
            HistogramOpts::new("stream_message_latency_seconds", "Message publish latency"),
            &["stream"],
        )
        .map_err(|e| MonitoringError::MetricsError(e.to_string()))?;
        registry
            .register(Box::new(message_latency.clone()))
            .map_err(|e| MonitoringError::MetricsError(e.to_string()))?;

        let active_streams = register_gauge_with_registry!(
            "stream_active_count",
            "Number of active streams",
            registry
        )
        .map_err(|e| MonitoringError::MetricsError(e.to_string()))?;

        let storage_bytes = register_gauge_with_registry!(
            "stream_storage_bytes_total",
            "Total stream storage in bytes",
            registry
        )
        .map_err(|e| MonitoringError::MetricsError(e.to_string()))?;

        let streams = StreamMetrics {
            messages_published,
            message_latency,
            active_streams,
            storage_bytes,
        };

        // Create network metrics
        let messages_sent = register_counter_with_registry!(
            "network_messages_sent_total",
            "Total network messages sent",
            registry
        )
        .map_err(|e| MonitoringError::MetricsError(e.to_string()))?;

        let messages_received = register_counter_with_registry!(
            "network_messages_received_total",
            "Total network messages received",
            registry
        )
        .map_err(|e| MonitoringError::MetricsError(e.to_string()))?;

        let connected_peers = register_gauge_with_registry!(
            "network_connected_peers",
            "Number of connected peers",
            registry
        )
        .map_err(|e| MonitoringError::MetricsError(e.to_string()))?;

        let peer_latency = prometheus::HistogramVec::new(
            HistogramOpts::new("network_peer_latency_seconds", "Peer network latency"),
            &["peer"],
        )
        .map_err(|e| MonitoringError::MetricsError(e.to_string()))?;
        registry
            .register(Box::new(peer_latency.clone()))
            .map_err(|e| MonitoringError::MetricsError(e.to_string()))?;

        let network = NetworkMetrics {
            messages_sent,
            messages_received,
            connected_peers,
            peer_latency,
        };

        Ok(Self {
            registry,
            consensus,
            streams,
            network,
        })
    }

    /// Get the Prometheus registry
    pub fn registry(&self) -> &Registry {
        &self.registry
    }
}

impl StreamMetrics {
    /// Record a message being published
    pub fn record_message(
        &self,
        stream: &str,
        group: crate::foundation::ConsensusGroupId,
        latency: Duration,
    ) {
        self.messages_published
            .with_label_values(&[stream, &format!("{group:?}")])
            .inc();

        self.message_latency
            .with_label_values(&[stream])
            .observe(latency.as_secs_f64());
    }

    /// Record an error
    pub fn record_error(&self, stream: &str, error_type: &str) {
        // Could add error-specific metrics here
    }
}

/// Metrics collector that updates Prometheus metrics
pub struct MetricsCollector {
    /// Metrics registry
    registry: Arc<MetricsRegistry>,

    /// System view for collecting metrics
    system_view: Arc<SystemView>,
}

impl MetricsCollector {
    /// Create a new metrics collector
    pub fn new(registry: Arc<MetricsRegistry>, system_view: Arc<SystemView>) -> Self {
        Self {
            registry,
            system_view,
        }
    }

    /// Get the registry
    pub fn registry(&self) -> Arc<MetricsRegistry> {
        self.registry.clone()
    }

    /// Update all metrics
    pub async fn update_metrics(&self) -> ConsensusResult<()> {
        // Update cluster metrics
        let cluster_info = self.system_view.get_cluster_info().await?;
        self.registry
            .consensus
            .node_count
            .set(cluster_info.node_count as f64);

        // Update group metrics
        let group_count = self.system_view.get_group_count().await?;
        self.registry.consensus.group_count.set(group_count as f64);

        // Update stream metrics
        let stream_count = self.system_view.get_stream_count().await?;
        self.registry
            .streams
            .active_streams
            .set(stream_count as f64);

        // Update network metrics
        let connected_peers = self.system_view.get_connected_peer_count().await?;
        self.registry
            .network
            .connected_peers
            .set(connected_peers as f64);

        // Calculate and update storage metrics
        let streams = self.system_view.get_all_stream_health().await?;
        let total_storage: u64 = streams.iter().map(|s| s.storage_bytes).sum();
        self.registry
            .streams
            .storage_bytes
            .set(total_storage as f64);

        Ok(())
    }

    /// Start the metrics collector
    pub async fn start(&self) {
        // Initial metrics update
        if let Err(e) = self.update_metrics().await {
            tracing::warn!("Failed to update initial metrics: {}", e);
        }
    }
}
