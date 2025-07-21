//! Messages for membership service communication

use serde::{Deserialize, Serialize};

use proven_network::message::{HandledMessage, ServiceMessage};
use proven_topology::{Node, NodeId};

use super::types::{ClusterFormationState, HealthInfo, LoadInfo, NodeStatus};

/// Messages for membership service
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MembershipMessage {
    /// Discover what clusters exist and who's online
    DiscoverCluster(DiscoverClusterRequest),
    /// Propose to form a new cluster (coordinator only)
    ProposeCluster(ProposeClusterRequest),
    /// Accept/reject cluster proposal
    AcceptProposal(AcceptProposalRequest),
    /// Health check to verify node is still alive
    HealthCheck(HealthCheckRequest),
    /// Announce graceful shutdown
    GracefulShutdown(GracefulShutdownRequest),
}

impl ServiceMessage for MembershipMessage {
    type Response = MembershipResponse;

    fn service_id() -> &'static str {
        "membership"
    }
}

impl HandledMessage for MembershipMessage {
    type Response = MembershipResponse;
}

/// Responses for membership messages
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MembershipResponse {
    DiscoverCluster(DiscoverClusterResponse),
    ProposeCluster(ProposeClusterResponse),
    AcceptProposal(AcceptProposalResponse),
    HealthCheck(HealthCheckResponse),
    GracefulShutdown(GracefulShutdownResponse),
}

impl ServiceMessage for MembershipResponse {
    type Response = MembershipResponse;

    fn service_id() -> &'static str {
        "membership"
    }
}

/// Request to discover cluster state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoverClusterRequest {
    /// Unique ID for this discovery round
    pub round_id: uuid::Uuid,
    /// Timestamp of the request
    pub timestamp: u64,
}

/// Response to cluster discovery
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoverClusterResponse {
    /// Responding node
    pub from_node: NodeId,
    /// Current cluster state
    pub cluster_state: ClusterState,
    /// Node's current status
    pub node_status: NodeStatus,
    /// Response timestamp
    pub timestamp: u64,
}

/// Cluster state for discovery
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClusterState {
    /// No cluster exists yet
    NoCluster,
    /// Currently forming a cluster
    Forming {
        coordinator: NodeId,
        proposed_members: Vec<NodeId>,
        formation_id: uuid::Uuid,
    },
    /// Active cluster exists
    Active {
        leader: Option<NodeId>,
        members: Vec<NodeId>,
        term: u64,
        committed_index: u64,
    },
}

/// Request to form a new cluster
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProposeClusterRequest {
    /// Coordinator proposing the cluster
    pub coordinator: NodeId,
    /// Unique formation ID
    pub formation_id: uuid::Uuid,
    /// Proposed initial members (must have responded to discovery)
    pub proposed_members: Vec<(NodeId, Node)>,
    /// Formation timeout in milliseconds
    pub timeout_ms: u64,
}

/// Response to cluster proposal
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProposeClusterResponse {
    /// Whether this node accepts the proposal
    pub accepted: bool,
    /// Reason if rejected
    pub rejection_reason: Option<String>,
}

/// Request to accept a cluster proposal
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AcceptProposalRequest {
    /// Formation ID being accepted
    pub formation_id: uuid::Uuid,
    /// Node's topology info
    pub node_info: Node,
}

/// Response to accepting proposal
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AcceptProposalResponse {
    /// Whether the acceptance was processed
    pub success: bool,
    /// Current cluster state
    pub cluster_state: ClusterFormationState,
}

/// Health check request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheckRequest {
    /// Sequence number for this check
    pub sequence: u64,
    /// Timestamp
    pub timestamp: u64,
}

/// Health check response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheckResponse {
    /// Node's current status
    pub status: NodeStatus,
    /// Current load/capacity info
    pub load: Option<LoadInfo>,
    /// Global consensus state if known
    pub global_consensus_info: Option<GlobalConsensusInfo>,
    /// Timestamp
    pub timestamp: u64,
}

/// Information about global consensus state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalConsensusInfo {
    pub is_member: bool,
    pub current_leader: Option<NodeId>,
    pub current_term: u64,
}

impl From<ClusterFormationState> for ClusterState {
    fn from(state: ClusterFormationState) -> Self {
        match state {
            ClusterFormationState::NotFormed | ClusterFormationState::Discovering { .. } => {
                ClusterState::NoCluster
            }
            ClusterFormationState::Forming {
                coordinator,
                formation_id,
                proposed_members,
            } => ClusterState::Forming {
                coordinator,
                proposed_members,
                formation_id,
            },
            ClusterFormationState::Active { members, .. } => ClusterState::Active {
                leader: None, // Will be filled from consensus info
                members,
                term: 0,
                committed_index: 0,
            },
        }
    }
}

/// Request to announce graceful shutdown
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GracefulShutdownRequest {
    /// Reason for shutdown (optional)
    pub reason: Option<String>,
    /// Timestamp
    pub timestamp: u64,
}

/// Response to graceful shutdown announcement
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GracefulShutdownResponse {
    /// Acknowledgment received
    pub acknowledged: bool,
}
