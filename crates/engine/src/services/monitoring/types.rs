//! Types for the monitoring service

use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::foundation::ConsensusGroupId;
use proven_topology::NodeId;

/// Result type for monitoring operations
pub type MonitoringResult<T> = Result<T, MonitoringError>;

/// Errors that can occur in the monitoring service
#[derive(Debug, Error)]
pub enum MonitoringError {
    /// Service not started
    #[error("Monitoring service not started")]
    NotStarted,

    /// Metrics registry error
    #[error("Metrics registry error: {0}")]
    MetricsError(String),

    /// View not available
    #[error("View not available: {0}")]
    ViewNotAvailable(String),

    /// Internal error
    #[error("Internal error: {0}")]
    Internal(String),
}

/// Health status levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HealthStatus {
    /// System is healthy
    Healthy,
    /// System is degraded but operational
    Degraded,
    /// System is unhealthy
    Unhealthy,
    /// Health status unknown
    Unknown,
}

/// Health information for a component
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentHealth {
    /// Component name
    pub name: String,
    /// Current status
    pub status: HealthStatus,
    /// Last check time
    pub last_check: SystemTime,
    /// Optional details
    pub details: Option<String>,
}

/// Overall system health
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemHealth {
    /// Overall status
    pub status: HealthStatus,
    /// Individual component health
    pub components: HashMap<String, ComponentHealth>,
    /// Cluster size
    pub cluster_size: usize,
    /// Number of active groups
    pub active_groups: usize,
    /// Number of streams
    pub total_streams: usize,
    /// Last update time
    pub last_update: SystemTime,
}

/// Health report for the entire system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthReport {
    /// System-wide health
    pub system: SystemHealth,
    /// Per-node health
    pub nodes: Vec<NodeHealth>,
    /// Per-group health
    pub groups: Vec<GroupHealth>,
    /// Critical issues if any
    pub issues: Vec<String>,
    /// Report generation time
    pub generated_at: SystemTime,
}

/// Stream health information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamHealth {
    /// Stream name
    pub name: String,
    /// Assigned consensus group
    pub group_id: ConsensusGroupId,
    /// Current status
    pub status: HealthStatus,
    /// Message count
    pub message_count: u64,
    /// Last message timestamp
    pub last_message_at: Option<SystemTime>,
    /// Write rate (messages per second)
    pub write_rate: f64,
    /// Read rate (messages per second)
    pub read_rate: f64,
    /// Storage size in bytes
    pub storage_bytes: u64,
    /// Whether stream is paused
    pub is_paused: bool,
}

/// Group health information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupHealth {
    /// Group ID
    pub id: ConsensusGroupId,
    /// Current leader
    pub leader: Option<NodeId>,
    /// Group members
    pub members: Vec<NodeId>,
    /// Raft term
    pub term: u64,
    /// Last log index
    pub last_log_index: u64,
    /// Commit index
    pub commit_index: u64,
    /// Number of streams
    pub stream_count: usize,
    /// Total storage size
    pub total_storage_bytes: u64,
    /// Health status
    pub status: HealthStatus,
    /// Active migrations
    pub active_migrations: usize,
}

/// Node health information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeHealth {
    /// Node ID
    pub id: NodeId,
    /// Node status
    pub status: HealthStatus,
    /// Is this node the global leader
    pub is_global_leader: bool,
    /// Groups this node participates in
    pub groups: Vec<ConsensusGroupId>,
    /// Groups where this node is leader
    pub leader_of_groups: Vec<ConsensusGroupId>,
    /// CPU usage percentage
    pub cpu_usage: Option<f32>,
    /// Memory usage percentage
    pub memory_usage: Option<f32>,
    /// Disk usage percentage
    pub disk_usage: Option<f32>,
    /// Network latency to other nodes
    pub peer_latencies: HashMap<NodeId, Duration>,
    /// Last seen timestamp
    pub last_seen: SystemTime,
}

/// Configuration for monitoring service
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonitoringConfig {
    /// Enable monitoring
    pub enabled: bool,

    /// Update interval for metrics
    pub update_interval: Duration,

    /// Health check interval
    pub health_check_interval: Duration,

    /// Prometheus configuration
    pub prometheus: PrometheusConfig,

    /// Retain metrics for this duration
    pub metrics_retention: Duration,

    /// Enable detailed logging
    pub detailed_logging: bool,
}

/// Prometheus configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrometheusConfig {
    /// Enable Prometheus metrics
    pub enabled: bool,

    /// Metrics endpoint path
    pub endpoint_path: String,

    /// Include detailed histograms
    pub detailed_histograms: bool,
}

impl Default for MonitoringConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            update_interval: Duration::from_secs(10),
            health_check_interval: Duration::from_secs(30),
            prometheus: PrometheusConfig::default(),
            metrics_retention: Duration::from_secs(3600), // 1 hour
            detailed_logging: false,
        }
    }
}

impl Default for PrometheusConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            endpoint_path: "/metrics".to_string(),
            detailed_histograms: false,
        }
    }
}
