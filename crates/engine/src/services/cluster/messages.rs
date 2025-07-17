//! Cluster service network messages

use proven_network::message::HandledMessage;
use proven_network::namespace::MessageType;
use proven_topology::{Node, NodeId};
use serde::{Deserialize, Serialize};

/// Cluster service namespace
pub const CLUSTER_NAMESPACE: &str = "cluster";

/// Request to discover existing clusters
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryRequest {
    /// ID of the requesting node
    pub requester_id: NodeId,
}

/// Response to discovery request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryResponse {
    /// ID of the responding node
    pub responder_id: NodeId,
    /// Whether this node has an active cluster
    pub has_active_cluster: bool,
    /// Current raft term if in cluster
    pub current_term: Option<u64>,
    /// Current leader if known
    pub current_leader: Option<NodeId>,
    /// Size of the cluster if known
    pub cluster_size: Option<usize>,
}

/// Request to join an existing cluster
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinRequest {
    /// ID of the requesting node
    pub requester_id: NodeId,
    /// Governance node information for the requester
    pub requester_node: Node,
}

/// Response to join request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinResponse {
    /// Error message if join failed
    pub error_message: Option<String>,
    /// Current cluster size after join (if successful)
    pub cluster_size: Option<usize>,
    /// Current term after join (if successful)
    pub current_term: Option<u64>,
    /// ID of the responding node (leader)
    pub responder_id: NodeId,
    /// Whether the join request was successful
    pub success: bool,
    /// ID of the current leader (if known)
    pub current_leader: Option<NodeId>,
}

/// Discovery round information
#[derive(Debug, Clone)]
pub struct DiscoveryRound {
    /// Nodes that responded to discovery
    pub responding_nodes: Vec<NodeId>,
    /// Nodes with active clusters
    pub nodes_with_clusters: Vec<(NodeId, DiscoveryResponse)>,
    /// Timestamp when discovery started
    pub started_at: std::time::Instant,
}

// Implement MessageType for namespace-based architecture
impl MessageType for DiscoveryRequest {
    fn message_type(&self) -> &'static str {
        "discovery_request"
    }
}

impl MessageType for DiscoveryResponse {
    fn message_type(&self) -> &'static str {
        "discovery_response"
    }
}

impl MessageType for JoinRequest {
    fn message_type(&self) -> &'static str {
        "join_request"
    }
}

impl MessageType for JoinResponse {
    fn message_type(&self) -> &'static str {
        "join_response"
    }
}

// NetworkMessage trait is automatically implemented by the network crate for types implementing MessageType + Serialize + DeserializeOwned

// Implement HandledMessage for request types
impl HandledMessage for DiscoveryRequest {
    type Response = DiscoveryResponse;
}

impl HandledMessage for JoinRequest {
    type Response = JoinResponse;
}

/// Cluster heartbeat message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterHeartbeat {
    /// Node sending the heartbeat
    pub node_id: NodeId,
    /// Current cluster state hash
    pub state_hash: u64,
    /// Timestamp
    pub timestamp: std::time::SystemTime,
}

impl MessageType for ClusterHeartbeat {
    fn message_type(&self) -> &'static str {
        "cluster.heartbeat"
    }
}

/// Consensus group join request (for group assignment after cluster formation)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsensusGroupJoinRequest {
    /// Node address
    pub address: String,
    /// Node capabilities
    pub capabilities: Vec<String>,
    /// Node requesting to join
    pub node_id: NodeId,
}

/// Consensus group join response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsensusGroupJoinResponse {
    /// Whether join was accepted
    pub accepted: bool,
    /// Assigned groups if accepted
    pub assigned_groups: Vec<crate::foundation::types::ConsensusGroupId>,
    /// Reason if rejected
    pub reason: Option<String>,
}

impl MessageType for ConsensusGroupJoinRequest {
    fn message_type(&self) -> &'static str {
        "cluster.join_request"
    }
}

impl MessageType for ConsensusGroupJoinResponse {
    fn message_type(&self) -> &'static str {
        "cluster.join_response"
    }
}

impl HandledMessage for ConsensusGroupJoinRequest {
    type Response = ConsensusGroupJoinResponse;
}
