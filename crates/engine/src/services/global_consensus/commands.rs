//! Commands for the global consensus service (request-response patterns)

use crate::consensus::global::{GlobalRequest, GlobalResponse};
use crate::foundation::events::Request;
use crate::foundation::types::ConsensusGroupId;
use crate::services::stream::{StreamConfig, StreamName};
use proven_topology::NodeId;
use std::time::Duration;

/// Initialize the global consensus cluster
#[derive(Debug, Clone)]
pub struct InitializeGlobalConsensus {
    pub members: Vec<NodeId>,
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

/// Create a new stream
#[derive(Debug, Clone)]
pub struct CreateStream {
    pub stream_name: StreamName,
    pub config: StreamConfig,
    pub target_group: Option<ConsensusGroupId>,
}

impl Request for CreateStream {
    type Response = ConsensusGroupId; // Returns the group ID where stream was created

    fn request_type() -> &'static str {
        "CreateStream"
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

/// Update global consensus membership
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
    type Response = GlobalStateSnapshot;

    fn request_type() -> &'static str {
        "GetGlobalState"
    }

    fn default_timeout() -> Duration {
        Duration::from_secs(10)
    }
}

/// Snapshot of global consensus state
#[derive(Debug, Clone)]
pub struct GlobalStateSnapshot {
    /// All consensus groups
    pub groups: Vec<GroupSnapshot>,
    /// All streams
    pub streams: Vec<StreamSnapshot>,
}

/// Snapshot of a consensus group
#[derive(Debug, Clone)]
pub struct GroupSnapshot {
    pub group_id: ConsensusGroupId,
    pub members: Vec<NodeId>,
}

/// Snapshot of a stream
#[derive(Debug, Clone)]
pub struct StreamSnapshot {
    pub stream_name: StreamName,
    pub config: StreamConfig,
    pub group_id: ConsensusGroupId,
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
