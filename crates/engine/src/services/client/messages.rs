//! Network messages for client service

use serde::{Deserialize, Serialize};

use proven_network::ServiceMessage;
use proven_topology::NodeId;

use crate::{
    consensus::{
        global::{GlobalRequest, GlobalResponse},
        group::{GroupRequest, GroupResponse},
    },
    foundation::types::ConsensusGroupId,
};

/// Client service message
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum ClientServiceMessage {
    /// Forward a global consensus request to another node
    ForwardGlobalRequest {
        /// Original requester node
        requester_id: NodeId,
        /// The global request to forward
        request: GlobalRequest,
    },
    /// Forward a group consensus request to another node
    ForwardGroupRequest {
        /// Original requester node
        requester_id: NodeId,
        /// Target group ID
        group_id: ConsensusGroupId,
        /// The group request to forward
        request: GroupRequest,
    },
}

/// Client service response
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum ClientServiceResponse {
    /// Response to a forwarded global request
    ForwardGlobalResponse {
        /// The global response
        response: GlobalResponse,
    },
    /// Response to a forwarded group request
    ForwardGroupResponse {
        /// The group response
        response: GroupResponse,
    },
}

impl ServiceMessage for ClientServiceMessage {
    type Response = ClientServiceResponse;

    fn service_id() -> &'static str {
        "client"
    }
}
