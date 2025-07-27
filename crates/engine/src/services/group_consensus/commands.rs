//! Commands for the group consensus service (request-response patterns)

use std::time::Duration;

use proven_topology::NodeId;

use crate::consensus::group::types::{GroupRequest, GroupResponse};
use crate::foundation::events::Request;
use crate::foundation::types::{ConsensusGroupId, StreamName};

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
        Duration::from_secs(10)
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
        Duration::from_secs(10)
    }
}

/// Initialize a stream in a consensus group
#[derive(Debug, Clone)]
pub struct InitializeStreamInGroup {
    pub group_id: ConsensusGroupId,
    pub stream_name: StreamName,
}

impl Request for InitializeStreamInGroup {
    type Response = ();

    fn request_type() -> &'static str {
        "InitializeStreamInGroup"
    }

    fn default_timeout() -> Duration {
        Duration::from_secs(10)
    }
}

/// Submit a request to a consensus group
#[derive(Debug, Clone)]
pub struct SubmitToGroup {
    pub group_id: ConsensusGroupId,
    pub request: GroupRequest,
}

impl Request for SubmitToGroup {
    type Response = GroupResponse;

    fn request_type() -> &'static str {
        "SubmitToGroup"
    }

    fn default_timeout() -> Duration {
        Duration::from_secs(30)
    }
}

/// Get the list of groups this node is a member of
#[derive(Debug, Clone)]
pub struct GetNodeGroups;

impl Request for GetNodeGroups {
    type Response = Vec<ConsensusGroupId>;

    fn request_type() -> &'static str {
        "GetNodeGroups"
    }

    fn default_timeout() -> Duration {
        Duration::from_secs(5)
    }
}

/// Get information about a specific group
#[derive(Debug, Clone)]
pub struct GetGroupInfo {
    pub group_id: ConsensusGroupId,
}

impl Request for GetGroupInfo {
    type Response = Option<GroupInfo>;

    fn request_type() -> &'static str {
        "GetGroupInfo"
    }

    fn default_timeout() -> Duration {
        Duration::from_secs(5)
    }
}

#[derive(Debug, Clone)]
pub struct GroupInfo {
    pub group_id: ConsensusGroupId,
    pub members: Vec<NodeId>,
    pub leader: Option<NodeId>,
    pub streams: Vec<StreamName>,
}

/// Get the state of a stream in a group
#[derive(Debug, Clone)]
pub struct GetStreamState {
    pub group_id: ConsensusGroupId,
    pub stream_name: StreamName,
}

impl Request for GetStreamState {
    type Response = Option<crate::foundation::models::stream::StreamState>;

    fn request_type() -> &'static str {
        "GetStreamState"
    }

    fn default_timeout() -> Duration {
        Duration::from_secs(5)
    }
}

/// Get the state information for a specific group
#[derive(Debug, Clone)]
pub struct GetGroupState {
    pub group_id: ConsensusGroupId,
}

impl Request for GetGroupState {
    type Response = Option<crate::foundation::GroupStateInfo>;

    fn request_type() -> &'static str {
        "GetGroupState"
    }

    fn default_timeout() -> Duration {
        Duration::from_secs(5)
    }
}
