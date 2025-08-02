//! Commands for the global consensus service (request-response patterns)

use std::collections::BTreeMap;
use std::time::Duration;

use proven_topology::{Node, NodeId};

use crate::foundation::events::Request;
use crate::foundation::types::{ConsensusGroupId, StreamName};
use crate::foundation::{GlobalStateReader, StreamConfig, StreamInfo};

/// Initialize the global consensus cluster
#[derive(Debug, Clone)]
pub struct InitializeGlobalConsensus {
    pub members: BTreeMap<NodeId, Node>,
}

impl Request for InitializeGlobalConsensus {
    type Response = ();

    fn request_type() -> &'static str {
        "InitializeGlobalConsensus"
    }

    fn default_timeout() -> Duration {
        Duration::from_secs(60)
    }
}

/// Create a new consensus group
#[derive(Debug, Clone)]
pub struct CreateGroup {
    pub group_id: ConsensusGroupId,
    pub members: Vec<NodeId>,
}

impl Request for CreateGroup {
    type Response = ();

    fn request_type() -> &'static str {
        "CreateGroup"
    }

    fn default_timeout() -> Duration {
        Duration::from_secs(30)
    }
}

/// Dissolve a consensus group
#[derive(Debug, Clone)]
pub struct DissolveGroup {
    pub group_id: ConsensusGroupId,
}

impl Request for DissolveGroup {
    type Response = ();

    fn request_type() -> &'static str {
        "DissolveGroup"
    }

    fn default_timeout() -> Duration {
        Duration::from_secs(30)
    }
}

/// Create a new stream in a specific consensus group
#[derive(Debug, Clone)]
pub struct CreateGroupStream {
    pub stream_name: StreamName,
    pub config: StreamConfig,
    /// Target group ID. If None, the least loaded group will be selected
    pub target_group: Option<ConsensusGroupId>,
}

impl Request for CreateGroupStream {
    type Response = ConsensusGroupId; // Returns the group ID where stream was created

    fn request_type() -> &'static str {
        "CreateGroupStream"
    }

    fn default_timeout() -> Duration {
        Duration::from_secs(30)
    }
}

/// Create a new global stream (remains in global consensus)
#[derive(Debug, Clone)]
pub struct CreateGlobalStream {
    pub stream_name: StreamName,
    pub config: StreamConfig,
}

impl Request for CreateGlobalStream {
    type Response = StreamInfo; // Returns full stream info

    fn request_type() -> &'static str {
        "CreateGlobalStream"
    }

    fn default_timeout() -> Duration {
        Duration::from_secs(30)
    }
}

/// Delete a stream
#[derive(Debug, Clone)]
pub struct DeleteStream {
    pub stream_name: StreamName,
}

impl Request for DeleteStream {
    type Response = ();

    fn request_type() -> &'static str {
        "DeleteStream"
    }

    fn default_timeout() -> Duration {
        Duration::from_secs(30)
    }
}

/// Add a node to global consensus
#[derive(Debug, Clone)]
pub struct AddNodeToConsensus {
    pub node_id: NodeId,
}

impl Request for AddNodeToConsensus {
    type Response = Vec<NodeId>; // Returns current members after addition

    fn request_type() -> &'static str {
        "AddNodeToConsensus"
    }

    fn default_timeout() -> Duration {
        Duration::from_secs(30)
    }
}

/// Remove a node from global consensus
#[derive(Debug, Clone)]
pub struct RemoveNodeFromConsensus {
    pub node_id: NodeId,
}

impl Request for RemoveNodeFromConsensus {
    type Response = Vec<NodeId>; // Returns current members after removal

    fn request_type() -> &'static str {
        "RemoveNodeFromConsensus"
    }

    fn default_timeout() -> Duration {
        Duration::from_secs(30)
    }
}

/// Add a node to a specific consensus group
#[derive(Debug, Clone)]
pub struct AddNodeToGroup {
    pub node_id: NodeId,
    pub group_id: ConsensusGroupId,
}

impl Request for AddNodeToGroup {
    type Response = (); // No response needed

    fn request_type() -> &'static str {
        "AddNodeToGroup"
    }

    fn default_timeout() -> Duration {
        Duration::from_secs(10)
    }
}

/// Get current global consensus members
#[derive(Debug, Clone)]
pub struct GetGlobalConsensusMembers;

impl Request for GetGlobalConsensusMembers {
    type Response = Vec<NodeId>;

    fn request_type() -> &'static str {
        "GetGlobalConsensusMembers"
    }

    fn default_timeout() -> Duration {
        Duration::from_secs(10)
    }
}

/// Get current global consensus leader
#[derive(Debug, Clone)]
pub struct GetGlobalLeader;

impl Request for GetGlobalLeader {
    type Response = Option<NodeId>;

    fn request_type() -> &'static str {
        "GetGlobalLeader"
    }

    fn default_timeout() -> Duration {
        Duration::from_secs(5)
    }
}

/// Update global consensus membership (legacy - prefer AddNodeToConsensus/RemoveNodeFromConsensus)
#[derive(Debug, Clone)]
pub struct UpdateGlobalMembership {
    pub add_members: Vec<NodeId>,
    pub remove_members: Vec<NodeId>,
}

impl Request for UpdateGlobalMembership {
    type Response = ();

    fn request_type() -> &'static str {
        "UpdateGlobalMembership"
    }

    fn default_timeout() -> Duration {
        Duration::from_secs(60)
    }
}

/// Get global state snapshot
#[derive(Debug, Clone)]
pub struct GetGlobalState;

impl Request for GetGlobalState {
    type Response = GlobalStateReader;

    fn request_type() -> &'static str {
        "GetGlobalState"
    }

    fn default_timeout() -> Duration {
        Duration::from_secs(10)
    }
}

/// Submit a request to global consensus
#[derive(Debug, Clone)]
pub struct SubmitGlobalRequest {
    pub request: crate::consensus::global::GlobalRequest,
}

impl Request for SubmitGlobalRequest {
    type Response = crate::consensus::global::GlobalResponse;

    fn request_type() -> &'static str {
        "SubmitGlobalRequest"
    }

    fn default_timeout() -> Duration {
        Duration::from_secs(30)
    }
}
