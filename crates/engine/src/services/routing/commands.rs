//! Commands for the routing service (request-response patterns)

use crate::foundation::events::{Error, Request};
use crate::foundation::types::ConsensusGroupId;
use crate::services::stream::StreamName;
use proven_topology::NodeId;
use std::time::Duration;

/// Update a stream route in the routing table
#[derive(Debug, Clone)]
pub struct UpdateStreamRoute {
    pub stream_name: StreamName,
    pub group_id: ConsensusGroupId,
}

impl Request for UpdateStreamRoute {
    type Response = Result<(), Error>;

    fn request_type() -> &'static str {
        "UpdateStreamRoute"
    }

    fn default_timeout() -> Duration {
        Duration::from_secs(5)
    }
}

/// Update global leader in the routing table
#[derive(Debug, Clone)]
pub struct UpdateGlobalLeader {
    pub leader: NodeId,
}

impl Request for UpdateGlobalLeader {
    type Response = Result<(), Error>;

    fn request_type() -> &'static str {
        "UpdateGlobalLeader"
    }

    fn default_timeout() -> Duration {
        Duration::from_secs(5)
    }
}

/// Bulk update routing information from global consensus sync
#[derive(Debug, Clone)]
pub struct BulkUpdateRoutes {
    pub groups: Vec<(ConsensusGroupId, Vec<NodeId>, Option<NodeId>)>,
    pub streams: Vec<(StreamName, ConsensusGroupId)>,
}

impl Request for BulkUpdateRoutes {
    type Response = Result<(), Error>;

    fn request_type() -> &'static str {
        "BulkUpdateRoutes"
    }

    fn default_timeout() -> Duration {
        Duration::from_secs(10)
    }
}

/// Update group membership in routing table
#[derive(Debug, Clone)]
pub struct UpdateGroupMembership {
    pub group_id: ConsensusGroupId,
    pub members: Vec<NodeId>,
    pub leader: Option<NodeId>,
}

impl Request for UpdateGroupMembership {
    type Response = Result<(), Error>;

    fn request_type() -> &'static str {
        "UpdateGroupMembership"
    }

    fn default_timeout() -> Duration {
        Duration::from_secs(5)
    }
}
