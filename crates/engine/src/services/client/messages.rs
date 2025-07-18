//! Network messages for client service

use serde::{Deserialize, Serialize};

use proven_network::{HandledMessage, namespace::MessageType};
use proven_topology::NodeId;

use crate::{
    consensus::{
        global::{GlobalRequest, GlobalResponse},
        group::{GroupRequest, GroupResponse},
    },
    foundation::types::ConsensusGroupId,
};

/// Namespace for client messages
pub const CLIENT_NAMESPACE: &str = "client";

/// Forward a global consensus request to another node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForwardGlobalRequest {
    /// Original requester node
    pub requester_id: NodeId,
    /// The global request to forward
    pub request: GlobalRequest,
}

/// Response to a forwarded global request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForwardGlobalResponse {
    /// The global response
    pub response: GlobalResponse,
}

/// Forward a group consensus request to another node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForwardGroupRequest {
    /// Original requester node
    pub requester_id: NodeId,
    /// Target group ID
    pub group_id: ConsensusGroupId,
    /// The group request to forward
    pub request: GroupRequest,
}

/// Response to a forwarded group request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForwardGroupResponse {
    /// The group response
    pub response: GroupResponse,
}

// Implement MessageType trait for all messages

impl MessageType for ForwardGlobalRequest {
    fn message_type(&self) -> &'static str {
        "forward_global_request"
    }
}

impl MessageType for ForwardGlobalResponse {
    fn message_type(&self) -> &'static str {
        "forward_global_response"
    }
}

impl MessageType for ForwardGroupRequest {
    fn message_type(&self) -> &'static str {
        "forward_group_request"
    }
}

impl MessageType for ForwardGroupResponse {
    fn message_type(&self) -> &'static str {
        "forward_group_response"
    }
}

// Implement HandledMessage for request types

impl HandledMessage for ForwardGlobalRequest {
    type Response = ForwardGlobalResponse;
}

impl HandledMessage for ForwardGroupRequest {
    type Response = ForwardGroupResponse;
}
