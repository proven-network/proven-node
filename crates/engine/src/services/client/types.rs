//! Types for the client service

use std::sync::Arc;

use proven_topology::NodeId;
use tokio::sync::oneshot;

use crate::{
    consensus::{
        global::{GlobalOperation, GlobalRequest, GlobalResponse},
        group::{GroupOperation, GroupRequest, GroupResponse},
    },
    error::ConsensusResult,
    foundation::types::ConsensusGroupId,
    stream::{MessageData, StreamConfig},
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
#[derive(Debug, Clone)]
pub struct StreamInfo {
    pub name: String,
    pub config: StreamConfig,
    pub group_id: ConsensusGroupId,
    pub last_sequence: u64,
}

/// Group information
#[derive(Debug, Clone)]
pub struct GroupInfo {
    pub id: ConsensusGroupId,
    pub members: Vec<NodeId>,
    pub leader: Option<NodeId>,
    pub streams: Vec<String>,
}
