//! Types for the client service

use std::sync::Arc;

use proven_storage::LogIndex;
use proven_topology::NodeId;
use tokio::sync::oneshot;

use crate::{
    consensus::{
        global::{GlobalOperation, GlobalRequest, GlobalResponse},
        group::{GroupOperation, GroupRequest, GroupResponse},
    },
    error::ConsensusResult,
    foundation::types::ConsensusGroupId,
    services::stream::{MessageData, StreamConfig},
};

/// Client request types that can be submitted through the ClientService
#[derive(Debug)]
pub enum ClientRequest {
    /// Submit a global consensus operation
    Global {
        request: GlobalRequest,
        response_tx: oneshot::Sender<ConsensusResult<GlobalResponse>>,
    },

    /// Submit a group consensus operation
    Group {
        group_id: ConsensusGroupId,
        request: GroupRequest,
        response_tx: oneshot::Sender<ConsensusResult<GroupResponse>>,
    },

    /// Submit a stream operation (routing will determine target group)
    Stream {
        stream_name: String,
        request: GroupRequest,
        response_tx: oneshot::Sender<ConsensusResult<GroupResponse>>,
    },

    /// Query stream information
    GetStreamInfo {
        stream_name: String,
        response_tx: oneshot::Sender<ConsensusResult<Option<StreamInfo>>>,
    },

    /// Query group information
    GetGroupInfo {
        group_id: ConsensusGroupId,
        response_tx: oneshot::Sender<ConsensusResult<Option<GroupInfo>>>,
    },
}

/// Stream information
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StreamInfo {
    /// Stream name
    pub name: String,
    /// Stream configuration
    pub config: StreamConfig,
    /// Group ID that owns this stream
    pub group_id: ConsensusGroupId,
    /// Last sequence number
    pub last_sequence: LogIndex,
    /// Total message count
    pub message_count: u64,
}

/// Group information
#[derive(Debug, Clone)]
pub struct GroupInfo {
    pub id: ConsensusGroupId,
    pub members: Vec<NodeId>,
    pub leader: Option<NodeId>,
    pub streams: Vec<String>,
}
