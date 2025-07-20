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

/// Reason for stream ending
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum StreamEndReason {
    /// Reached the end of the stream
    EndOfStream,
    /// Client cancelled the stream
    Cancelled,
    /// Server timeout
    Timeout,
    /// Stream was deleted
    StreamDeleted,
}

/// Client service message
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum ClientServiceMessage {
    /// Forward a global consensus request to another node
    Global {
        /// Original requester node
        requester_id: NodeId,
        /// The global request to forward
        request: GlobalRequest,
    },
    /// Forward a group consensus request to another node
    Group {
        /// Original requester node
        requester_id: NodeId,
        /// Target group ID
        group_id: ConsensusGroupId,
        /// The group request to forward
        request: GroupRequest,
    },
    /// Forward a stream read request to another node
    StreamRead {
        /// Original requester node
        requester_id: NodeId,
        /// Stream name
        stream_name: String,
        /// Start sequence
        start_sequence: u64,
        /// Number of messages to read
        count: u64,
    },
    /// Start a streaming session
    StreamStart {
        /// Original requester node
        requester_id: NodeId,
        /// Stream name
        stream_name: String,
        /// Start sequence
        start_sequence: u64,
        /// Optional end sequence (None means stream to end)
        end_sequence: Option<u64>,
        /// Batch size (messages per response)
        batch_size: u32,
    },
    /// Continue a streaming session
    StreamContinue {
        /// Original requester node
        requester_id: NodeId,
        /// Stream session ID
        session_id: uuid::Uuid,
        /// Maximum messages to return
        max_messages: u32,
    },
    /// Cancel a streaming session
    StreamCancel {
        /// Original requester node
        requester_id: NodeId,
        /// Stream session ID
        session_id: uuid::Uuid,
    },
}

/// Client service response
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum ClientServiceResponse {
    /// Response to a forwarded global request
    Global {
        /// The global response
        response: GlobalResponse,
    },
    /// Response to a forwarded group request
    Group {
        /// The group response
        response: GroupResponse,
    },
    /// Response to a forwarded read request
    StreamRead {
        /// The messages read
        messages: Vec<crate::services::stream::StoredMessage>,
    },
    /// Response to stream start/continue
    StreamBatch {
        /// Stream session ID
        session_id: uuid::Uuid,
        /// Messages in this batch
        messages: Vec<crate::services::stream::StoredMessage>,
        /// Whether more messages are available
        has_more: bool,
        /// Next sequence number (if has_more is true)
        next_sequence: Option<u64>,
    },
    /// Stream ended
    StreamEnd {
        /// Stream session ID
        session_id: uuid::Uuid,
        /// Reason for ending
        reason: StreamEndReason,
    },
    /// Stream error
    StreamError {
        /// Stream session ID
        session_id: uuid::Uuid,
        /// Error message
        error: String,
    },
}

impl ServiceMessage for ClientServiceMessage {
    type Response = ClientServiceResponse;

    fn service_id() -> &'static str {
        "client"
    }
}
