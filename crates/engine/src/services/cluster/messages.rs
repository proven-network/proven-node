//! Cluster service network messages

use proven_network::ServiceMessage;
use proven_topology::{Node, NodeId};
use serde::{Deserialize, Serialize};

/// Cluster service message
#[allow(clippy::large_enum_variant)] // TODO: Box the large enum variants
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClusterServiceMessage {
    /// Request to discover existing clusters
    Discovery {
        /// ID of the requesting node
        requester_id: NodeId,
    },
    /// Request to join an existing cluster
    GlobalJoin {
        /// ID of the requesting node
        requester_id: NodeId,
        /// Governance node information for the requester
        requester_node: Node,
    },
    /// Consensus group join request (for group assignment after cluster formation)
    GroupJoin {
        /// Node address
        address: String,
        /// Node capabilities
        capabilities: Vec<String>,
        /// Node requesting to join
        node_id: NodeId,
    },
    /// Cluster heartbeat message
    Heartbeat {
        /// Node sending the heartbeat
        node_id: NodeId,
        /// Current cluster state hash
        state_hash: u64,
        /// Timestamp
        timestamp: std::time::SystemTime,
    },
}

/// Cluster service response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClusterServiceResponse {
    /// Response to discovery request
    Discovery {
        /// ID of the responding node
        responder_id: NodeId,
        /// Whether this node has an active cluster
        has_active_cluster: bool,
        /// Current raft term if in cluster
        current_term: Option<u64>,
        /// Current leader if known
        current_leader: Option<NodeId>,
        /// Size of the cluster if known
        cluster_size: Option<usize>,
    },
    /// Response to join request
    GlobalJoin {
        /// Error message if join failed
        error_message: Option<String>,
        /// Current cluster size after join (if successful)
        cluster_size: Option<usize>,
        /// Current term after join (if successful)
        current_term: Option<u64>,
        /// ID of the responding node (leader)
        responder_id: NodeId,
        /// Whether the join request was successful
        success: bool,
        /// ID of the current leader (if known)
        current_leader: Option<NodeId>,
    },
    /// Consensus group join response
    GroupJoin {
        /// Whether join was accepted
        accepted: bool,
        /// Assigned groups if accepted
        assigned_groups: Vec<crate::foundation::types::ConsensusGroupId>,
        /// Reason if rejected
        reason: Option<String>,
    },
    /// Heartbeat acknowledgement (empty response)
    HeartbeatAck,
}

impl ServiceMessage for ClusterServiceMessage {
    type Response = ClusterServiceResponse;

    fn service_id() -> &'static str {
        "cluster"
    }
}

/// Discovery round information
#[derive(Debug, Clone)]
pub struct DiscoveryRound {
    /// Nodes that responded to discovery
    pub responding_nodes: Vec<NodeId>,
    /// Nodes with active clusters  
    pub nodes_with_clusters: Vec<(NodeId, ClusterServiceResponse)>,
    /// Timestamp when discovery started
    pub started_at: std::time::Instant,
}

/// Join request structure (for internal use)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinRequest {
    /// ID of the requesting node
    pub requester_id: NodeId,
    /// Governance node information for the requester
    pub requester_node: Node,
}

/// Join response structure (for internal use)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinResponse {
    /// Error message if join failed
    pub error_message: Option<String>,
    /// Current cluster size after join (if successful)
    pub cluster_size: Option<usize>,
    /// Current raft term after join (if successful)
    pub current_term: Option<u64>,
    /// ID of the responding node (leader)
    pub responder_id: NodeId,
    /// Whether the join request was successful
    pub success: bool,
    /// ID of the current leader (if known)
    pub current_leader: Option<NodeId>,
}
