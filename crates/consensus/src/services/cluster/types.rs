//! Types for the cluster service

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use proven_topology::NodeId;

/// Result type for cluster operations
pub type ClusterResult<T> = Result<T, ClusterError>;

/// Errors that can occur during cluster operations
#[derive(Debug, Error)]
pub enum ClusterError {
    /// Service not started
    #[error("Cluster service not started")]
    NotStarted,

    /// Discovery failed
    #[error("Discovery failed: {0}")]
    DiscoveryFailed(String),

    /// Formation failed
    #[error("Formation failed: {0}")]
    FormationFailed(String),

    /// Join failed
    #[error("Join failed: {0}")]
    JoinFailed(String),

    /// Leave failed
    #[error("Leave failed: {0}")]
    LeaveFailed(String),

    /// Invalid state
    #[error("Invalid state: {0}")]
    InvalidState(String),

    /// Not leader
    #[error("Not cluster leader")]
    NotLeader,

    /// Already member
    #[error("Already a cluster member")]
    AlreadyMember,

    /// Not member
    #[error("Not a cluster member")]
    NotMember,

    /// Timeout
    #[error("Operation timed out")]
    Timeout,

    /// Network error
    #[error("Network error: {0}")]
    Network(String),

    /// Join rejected
    #[error("Join rejected: {0}")]
    JoinRejected(String),

    /// Internal error
    #[error("Internal error: {0}")]
    Internal(String),
}

/// Cluster state
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ClusterState {
    /// Not initialized
    Uninitialized,
    /// Discovering peers
    Discovering {
        /// Discovery start time
        started_at: SystemTime,
        /// Discovered peers
        discovered_peers: Vec<NodeId>,
    },
    /// Forming cluster
    Forming {
        /// Formation start time
        started_at: SystemTime,
        /// Formation mode
        mode: FormationMode,
    },
    /// Joining cluster
    Joining {
        /// Join start time
        started_at: SystemTime,
        /// Target node
        target_node: NodeId,
    },
    /// Active member
    Active {
        /// Join time
        joined_at: SystemTime,
        /// Current role
        role: NodeRole,
        /// Cluster size
        cluster_size: usize,
    },
    /// Leaving cluster
    Leaving {
        /// Leave start time
        started_at: SystemTime,
        /// Reason
        reason: String,
    },
    /// Failed state
    Failed {
        /// Failure time
        failed_at: SystemTime,
        /// Error message
        error: String,
    },
}

/// Cluster state transition
#[derive(Debug, Clone)]
pub struct ClusterTransition {
    /// From state
    pub from: ClusterState,
    /// To state
    pub to: ClusterState,
    /// Transition time
    pub timestamp: SystemTime,
    /// Transition reason
    pub reason: String,
}

/// Cluster information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterInfo {
    /// Cluster ID
    pub cluster_id: String,
    /// Current state
    pub state: ClusterState,
    /// Cluster members
    pub members: HashMap<NodeId, NodeInfo>,
    /// Current leader
    pub leader: Option<NodeId>,
    /// Formation time
    pub formed_at: SystemTime,
    /// Last updated
    pub last_updated: SystemTime,
}

/// Cluster metrics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ClusterMetrics {
    /// Total nodes
    pub total_nodes: usize,
    /// Active nodes
    pub active_nodes: usize,
    /// Failed nodes
    pub failed_nodes: usize,
    /// Leader changes
    pub leader_changes: u64,
    /// Membership changes
    pub membership_changes: u64,
    /// Average join time
    pub avg_join_time_ms: f64,
    /// Average leave time
    pub avg_leave_time_ms: f64,
}

/// Node information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    /// Node ID
    pub node_id: NodeId,
    /// Node address
    pub address: SocketAddr,
    /// Node state
    pub state: NodeState,
    /// Node role
    pub role: NodeRole,
    /// Join time
    pub joined_at: SystemTime,
    /// Last seen
    pub last_seen: SystemTime,
    /// Health status
    pub health: NodeHealth,
}

/// Node state
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum NodeState {
    /// Node is joining
    Joining,
    /// Node is active
    Active,
    /// Node is leaving
    Leaving,
    /// Node is unreachable
    Unreachable,
    /// Node has failed
    Failed,
}

/// Node role in cluster
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum NodeRole {
    /// Leader node
    Leader,
    /// Follower node
    Follower,
    /// Learner node (non-voting)
    Learner,
    /// Candidate node (during election)
    Candidate,
}

/// Node health status
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum NodeHealth {
    /// Node is healthy
    Healthy,
    /// Node is degraded
    Degraded,
    /// Node is unhealthy
    Unhealthy,
}

/// Membership change
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MembershipChange {
    /// Change ID
    pub id: String,
    /// Change type
    pub change_type: MembershipChangeType,
    /// Affected node
    pub node_id: NodeId,
    /// Change time
    pub timestamp: SystemTime,
    /// Change status
    pub status: MembershipStatus,
}

/// Membership change type
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum MembershipChangeType {
    /// Node joining
    Join,
    /// Node leaving
    Leave,
    /// Node promoted (learner to voter)
    Promote,
    /// Node demoted (voter to learner)
    Demote,
}

/// Membership status
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum MembershipStatus {
    /// Change pending
    Pending,
    /// Change in progress
    InProgress,
    /// Change completed
    Completed,
    /// Change failed
    Failed,
}

/// Cluster formation mode
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum FormationMode {
    /// Single node cluster
    SingleNode,
    /// Multi-node cluster
    MultiNode { expected_size: usize },
    /// Bootstrap from existing cluster
    Bootstrap,
}

/// Formation strategy
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum FormationStrategy {
    /// Immediate formation
    Immediate,
    /// Wait for quorum
    WaitForQuorum,
    /// Coordinated formation
    Coordinated,
}

/// Discovery mode
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum DiscoveryMode {
    /// Multicast discovery
    Multicast,
    /// Unicast with seed nodes
    Unicast,
    /// DNS-based discovery
    Dns,
    /// Cloud provider discovery
    CloudProvider,
}

/// Discovery strategy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryStrategy {
    /// Discovery mode
    pub mode: DiscoveryMode,
    /// Seed nodes for unicast
    pub seed_nodes: Vec<SocketAddr>,
    /// Discovery timeout
    pub timeout: Duration,
    /// Max peers to discover
    pub max_peers: usize,
}

/// Cluster health
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterHealth {
    /// Overall status
    pub status: HealthStatus,
    /// Health checks
    pub checks: Vec<HealthCheck>,
    /// Last check time
    pub last_check: SystemTime,
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

/// Health check result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheck {
    /// Check name
    pub name: String,
    /// Check status
    pub status: HealthStatus,
    /// Check message
    pub message: Option<String>,
    /// Check duration
    pub duration: Duration,
}

/// Cluster configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// Cluster name
    pub cluster_name: String,

    /// Discovery configuration
    pub discovery: DiscoveryConfig,

    /// Formation configuration
    pub formation: FormationConfig,

    /// Membership configuration
    pub membership: MembershipConfig,

    /// Health check configuration
    pub health: HealthConfig,
}

/// Discovery configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryConfig {
    /// Discovery strategy
    pub strategy: DiscoveryStrategy,
    /// Retry attempts
    pub retry_attempts: u32,
    /// Retry delay
    pub retry_delay: Duration,
}

/// Formation configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FormationConfig {
    /// Formation mode
    pub mode: FormationMode,
    /// Formation strategy
    pub strategy: FormationStrategy,
    /// Formation timeout
    pub timeout: Duration,
}

/// Membership configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MembershipConfig {
    /// Join timeout
    pub join_timeout: Duration,
    /// Leave timeout
    pub leave_timeout: Duration,
    /// Heartbeat interval
    pub heartbeat_interval: Duration,
    /// Failure detection threshold
    pub failure_threshold: u32,
}

/// Health configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthConfig {
    /// Health check interval
    pub check_interval: Duration,
    /// Health check timeout
    pub check_timeout: Duration,
    /// Unhealthy threshold
    pub unhealthy_threshold: u32,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            cluster_name: "consensus-cluster".to_string(),
            discovery: DiscoveryConfig {
                strategy: DiscoveryStrategy {
                    mode: DiscoveryMode::Multicast,
                    seed_nodes: Vec::new(),
                    timeout: Duration::from_secs(30),
                    max_peers: 100,
                },
                retry_attempts: 3,
                retry_delay: Duration::from_secs(5),
            },
            formation: FormationConfig {
                mode: FormationMode::SingleNode,
                strategy: FormationStrategy::Immediate,
                timeout: Duration::from_secs(60),
            },
            membership: MembershipConfig {
                join_timeout: Duration::from_secs(30),
                leave_timeout: Duration::from_secs(30),
                heartbeat_interval: Duration::from_secs(5),
                failure_threshold: 3,
            },
            health: HealthConfig {
                check_interval: Duration::from_secs(10),
                check_timeout: Duration::from_secs(5),
                unhealthy_threshold: 3,
            },
        }
    }
}
