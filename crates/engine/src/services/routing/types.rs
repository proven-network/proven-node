//! Types for the routing service

use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::foundation::ConsensusGroupId;
use proven_topology::NodeId;

/// Result type for routing operations
pub type RoutingResult<T> = Result<T, RoutingError>;

/// Errors that can occur during routing
#[derive(Debug, Error)]
pub enum RoutingError {
    /// Service not started
    #[error("Routing service not started")]
    NotStarted,

    /// Stream not found
    #[error("Stream not found: {0}")]
    StreamNotFound(String),

    /// Group not found
    #[error("Group not found: {0:?}")]
    GroupNotFound(ConsensusGroupId),

    /// No available groups
    #[error("No available groups for routing")]
    NoAvailableGroups,

    /// Routing failed
    #[error("Routing failed: {0}")]
    RoutingFailed(String),

    /// Operation not supported
    #[error("Operation not supported: {0}")]
    OperationNotSupported(String),

    /// Load balancing error
    #[error("Load balancing error: {0}")]
    LoadBalancingError(String),

    /// Internal error
    #[error("Internal error: {0}")]
    Internal(String),
}

/// Routing decision
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum RoutingDecision {
    /// Route to global consensus
    Global,
    /// Route to specific group
    Group(ConsensusGroupId),
    /// Route to local processing
    Local,
    /// Reject the operation
    Reject { reason: String },
}

/// Routing strategy
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum RoutingStrategy {
    /// Route to least loaded group
    LeastLoaded,
    /// Route based on hash
    HashBased,
    /// Route to geographically closest
    GeoProximity,
    /// Round-robin routing
    RoundRobin,
    /// Sticky routing (same stream always goes to same group)
    Sticky,
}

/// Stream routing information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamRoute {
    /// Stream name
    pub stream_name: String,
    /// Assigned group
    pub group_id: ConsensusGroupId,
    /// Assignment timestamp
    pub assigned_at: SystemTime,
    /// Routing strategy used
    pub strategy: RoutingStrategy,
    /// Is route active
    pub is_active: bool,
    /// Stream configuration
    pub config: Option<crate::services::stream::StreamConfig>,
}

/// Group routing information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupRoute {
    /// Group ID
    pub group_id: ConsensusGroupId,
    /// Group members
    pub members: Vec<NodeId>,
    /// Group leader
    pub leader: Option<NodeId>,
    /// Number of streams (used for load balancing new stream creation)
    pub stream_count: usize,
    /// Group health
    pub health: GroupHealth,
    /// Last updated
    pub last_updated: SystemTime,
    /// Location of the group relative to this node
    pub location: GroupLocation,
}

/// Group location relative to the current node
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum GroupLocation {
    /// Group exists on this node
    Local,
    /// Group exists on remote nodes only
    Remote,
    /// Group exists on both local and remote nodes
    Distributed,
}

/// Detailed group location information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupLocationInfo {
    /// Group ID
    pub group_id: ConsensusGroupId,
    /// Location type
    pub location: GroupLocation,
    /// Whether the group is local to the current node
    pub is_local: bool,
    /// Nodes that have this group
    pub nodes: Vec<NodeId>,
    /// Current leader of the group
    pub leader: Option<NodeId>,
}

/// Group health status
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum GroupHealth {
    /// Group is healthy
    Healthy,
    /// Group is degraded
    Degraded,
    /// Group is unhealthy
    Unhealthy,
    /// Group health unknown
    Unknown,
}

/// Complete routing information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingInfo {
    /// Stream routes
    pub stream_routes: HashMap<String, StreamRoute>,
    /// Group routes
    pub group_routes: HashMap<ConsensusGroupId, GroupRoute>,
    /// Active routing strategy
    pub default_strategy: RoutingStrategy,
    /// Routing metrics
    pub metrics: RoutingMetrics,
    /// Last refresh time
    pub last_refresh: SystemTime,
}

/// Routing metrics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RoutingMetrics {
    /// Total operations routed
    pub total_operations: u64,
    /// Successful routings
    pub successful_routes: u64,
    /// Failed routings
    pub failed_routes: u64,
    /// Average routing latency
    pub avg_latency_ms: f64,
    /// Routes by strategy
    pub routes_by_strategy: HashMap<String, u64>,
    /// Routes by group
    pub routes_by_group: HashMap<ConsensusGroupId, u64>,
}

/// Load information for a group
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadInfo {
    /// Group ID
    pub group_id: ConsensusGroupId,
    /// Number of streams
    pub stream_count: usize,
    /// Messages per second
    pub messages_per_sec: f64,
    /// Storage usage bytes
    pub storage_bytes: u64,
    /// CPU usage percentage
    pub cpu_usage: f32,
    /// Memory usage percentage
    pub memory_usage: f32,
    /// Load score (0.0 = no load, 1.0 = full load)
    pub load_score: f32,
}

/// Routing health status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingHealth {
    /// Overall health
    pub status: HealthStatus,
    /// Healthy groups
    pub healthy_groups: usize,
    /// Total groups
    pub total_groups: usize,
    /// Routing success rate
    pub success_rate: f32,
    /// Average routing latency
    pub avg_latency_ms: f64,
    /// Issues if any
    pub issues: Vec<String>,
}

/// Health status
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum HealthStatus {
    /// Healthy
    Healthy,
    /// Degraded
    Degraded,
    /// Unhealthy
    Unhealthy,
}

/// Routing configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingConfig {
    /// Default routing strategy
    pub default_strategy: RoutingStrategy,

    /// Enable load-aware routing
    pub enable_load_balancing: bool,

    /// Load check interval
    pub load_check_interval: Duration,

    /// Routing cache TTL
    pub cache_ttl: Duration,

    /// Max retries for failed routes
    pub max_retries: u32,

    /// Retry delay
    pub retry_delay: Duration,

    /// Enable sticky routing
    pub enable_sticky_routing: bool,

    /// Load thresholds
    pub load_thresholds: LoadThresholds,
}

/// Load thresholds for routing decisions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadThresholds {
    /// High load threshold (stop routing new streams)
    pub high_load: f32,
    /// Medium load threshold (prefer other groups)
    pub medium_load: f32,
    /// Rebalance threshold (trigger rebalancing)
    pub rebalance_threshold: f32,
}

impl Default for RoutingConfig {
    fn default() -> Self {
        Self {
            default_strategy: RoutingStrategy::LeastLoaded,
            enable_load_balancing: true,
            load_check_interval: Duration::from_secs(30),
            cache_ttl: Duration::from_secs(60),
            max_retries: 3,
            retry_delay: Duration::from_millis(100),
            enable_sticky_routing: true,
            load_thresholds: LoadThresholds::default(),
        }
    }
}

impl Default for LoadThresholds {
    fn default() -> Self {
        Self {
            high_load: 0.9,
            medium_load: 0.7,
            rebalance_threshold: 0.3, // 30% imbalance triggers rebalancing
        }
    }
}
