//! Commands for the membership service (request-response patterns)

use crate::foundation::events::Request;
use proven_topology::{Node, NodeId};
use std::time::Duration;

/// Initialize a new cluster with this node as coordinator
#[derive(Debug, Clone)]
pub struct InitializeCluster {
    /// Strategy for cluster formation
    pub strategy: ClusterFormationStrategy,
}

#[derive(Debug, Clone)]
pub enum ClusterFormationStrategy {
    /// Single node cluster (for testing/development)
    SingleNode,
    /// Bootstrap with initial set of nodes
    Bootstrap {
        /// Expected nodes to join before cluster is formed
        expected_nodes: Vec<NodeId>,
        /// Timeout for waiting for nodes
        timeout: Duration,
    },
    /// Join an existing cluster
    JoinExisting {
        /// Known nodes in the existing cluster
        contact_nodes: Vec<NodeId>,
    },
}

impl Request for InitializeCluster {
    type Response = ClusterFormationResult;

    fn request_type() -> &'static str {
        "InitializeCluster"
    }

    fn default_timeout() -> Duration {
        Duration::from_secs(60) // Cluster formation can take time
    }
}

#[derive(Debug, Clone)]
pub struct ClusterFormationResult {
    pub cluster_id: String,
    pub members: Vec<NodeId>,
    pub coordinator: NodeId,
}

/// Add a new member to the cluster
#[derive(Debug, Clone)]
pub struct AddMember {
    pub node_id: NodeId,
    pub node_info: Node,
}

impl Request for AddMember {
    type Response = ();

    fn request_type() -> &'static str {
        "AddMember"
    }

    fn default_timeout() -> Duration {
        Duration::from_secs(30)
    }
}

/// Remove a member from the cluster
#[derive(Debug, Clone)]
pub struct RemoveMember {
    pub node_id: NodeId,
    pub reason: String,
}

impl Request for RemoveMember {
    type Response = ();

    fn request_type() -> &'static str {
        "RemoveMember"
    }

    fn default_timeout() -> Duration {
        Duration::from_secs(30)
    }
}

/// Get current cluster membership information
#[derive(Debug, Clone)]
pub struct GetMembership;

impl Request for GetMembership {
    type Response = MembershipInfo;

    fn request_type() -> &'static str {
        "GetMembership"
    }

    fn default_timeout() -> Duration {
        Duration::from_secs(5)
    }
}

#[derive(Debug, Clone)]
pub struct MembershipInfo {
    pub cluster_id: String,
    pub members: Vec<(NodeId, Node)>,
    pub coordinator: Option<NodeId>,
    pub this_node: NodeId,
}

/// Get online members from the membership service
#[derive(Debug, Clone)]
pub struct GetOnlineMembers;

impl Request for GetOnlineMembers {
    type Response = Vec<(NodeId, Node)>;

    fn request_type() -> &'static str {
        "GetOnlineMembers"
    }

    fn default_timeout() -> Duration {
        Duration::from_secs(5)
    }
}

/// Get information about the current node
#[derive(Debug, Clone)]
pub struct GetNodeInfo;

impl Request for GetNodeInfo {
    type Response = Node;

    fn request_type() -> &'static str {
        "GetNodeInfo"
    }

    fn default_timeout() -> Duration {
        Duration::from_secs(5)
    }
}

/// Get information about a specific peer
#[derive(Debug, Clone)]
pub struct GetPeerInfo {
    pub node_id: NodeId,
}

impl Request for GetPeerInfo {
    type Response = Option<Node>;

    fn request_type() -> &'static str {
        "GetPeerInfo"
    }

    fn default_timeout() -> Duration {
        Duration::from_secs(5)
    }
}
