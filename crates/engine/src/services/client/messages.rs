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
    GlobalRequest {
        /// Original requester node
        requester_id: NodeId,
        /// The global request to forward
        request: GlobalRequest,
    },
    /// Forward a group consensus request to another node
    GroupRequest {
        /// Original requester node
        requester_id: NodeId,
        /// Target group ID
        group_id: ConsensusGroupId,
        /// The group request to forward
        request: GroupRequest,
    },
    /// Forward a stream read request to another node
    ReadRequest {
        /// Original requester node
        requester_id: NodeId,
        /// Stream name
        stream_name: String,
        /// Start sequence
        start_sequence: u64,
        /// Number of messages to read
        count: u64,
    },
}

/// Client service response
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum ClientServiceResponse {
    /// Response to a forwarded global request
    GlobalResponse {
        /// The global response
        response: GlobalResponse,
    },
    /// Response to a forwarded group request
    GroupResponse {
        /// The group response
        response: GroupResponse,
    },

    /// Response to a forwarded read request
    ReadResponse {
        /// The messages read
        messages: Vec<crate::services::stream::StoredMessage>,
    },
}

impl ServiceMessage for ClientServiceMessage {
    type Response = ClientServiceResponse;

    fn service_id() -> &'static str {
        "client"
    }
}
