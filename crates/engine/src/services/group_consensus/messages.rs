//! Messages for group consensus service

use proven_network::{HandledMessage, namespace::MessageType};
use serde::{Deserialize, Serialize};

use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};

use crate::consensus::group::{GroupRequest, GroupResponse, GroupTypeConfig};
use crate::foundation::types::ConsensusGroupId;

/// Group consensus namespace
pub const GROUP_CONSENSUS_NAMESPACE: &str = "group_consensus";

// Group consensus message types

/// Group vote request
#[derive(Debug, Serialize, Deserialize)]
pub struct GroupVoteRequest {
    pub group_id: ConsensusGroupId,
    pub request: VoteRequest<GroupTypeConfig>,
}

/// Group vote response
#[derive(Debug, Serialize, Deserialize)]
pub struct GroupVoteResponse {
    pub group_id: ConsensusGroupId,
    pub response: VoteResponse<GroupTypeConfig>,
}

/// Group append entries request
#[derive(Debug, Serialize, Deserialize)]
pub struct GroupAppendEntriesRequest {
    pub group_id: ConsensusGroupId,
    pub request: AppendEntriesRequest<GroupTypeConfig>,
}

/// Group append entries response
#[derive(Debug, Serialize, Deserialize)]
pub struct GroupAppendEntriesResponse {
    pub group_id: ConsensusGroupId,
    pub response: AppendEntriesResponse<GroupTypeConfig>,
}

/// Group install snapshot request
#[derive(Debug, Serialize, Deserialize)]
pub struct GroupInstallSnapshotRequest {
    pub group_id: ConsensusGroupId,
    pub request: InstallSnapshotRequest<GroupTypeConfig>,
}

/// Group install snapshot response
#[derive(Debug, Serialize, Deserialize)]
pub struct GroupInstallSnapshotResponse {
    pub group_id: ConsensusGroupId,
    pub response: InstallSnapshotResponse<GroupTypeConfig>,
}

/// Group consensus application request
#[derive(Debug, Serialize, Deserialize)]
pub struct GroupConsensusRequest {
    pub group_id: ConsensusGroupId,
    pub request: GroupRequest,
}

/// Group consensus application response
#[derive(Debug, Serialize, Deserialize)]
pub struct GroupConsensusResponse {
    pub group_id: ConsensusGroupId,
    pub response: GroupResponse,
}

// Implement MessageType for all message types

impl MessageType for GroupVoteRequest {
    fn message_type(&self) -> &'static str {
        "vote_request"
    }
}

impl MessageType for GroupVoteResponse {
    fn message_type(&self) -> &'static str {
        "vote_response"
    }
}

impl MessageType for GroupAppendEntriesRequest {
    fn message_type(&self) -> &'static str {
        "append_entries_request"
    }
}

impl MessageType for GroupAppendEntriesResponse {
    fn message_type(&self) -> &'static str {
        "append_entries_response"
    }
}

impl MessageType for GroupInstallSnapshotRequest {
    fn message_type(&self) -> &'static str {
        "install_snapshot_request"
    }
}

impl MessageType for GroupInstallSnapshotResponse {
    fn message_type(&self) -> &'static str {
        "install_snapshot_response"
    }
}

impl MessageType for GroupConsensusRequest {
    fn message_type(&self) -> &'static str {
        "consensus_request"
    }
}

impl MessageType for GroupConsensusResponse {
    fn message_type(&self) -> &'static str {
        "consensus_response"
    }
}

// Implement HandledMessage for request types

impl HandledMessage for GroupVoteRequest {
    type Response = GroupVoteResponse;
}

impl HandledMessage for GroupAppendEntriesRequest {
    type Response = GroupAppendEntriesResponse;
}

impl HandledMessage for GroupInstallSnapshotRequest {
    type Response = GroupInstallSnapshotResponse;
}

impl HandledMessage for GroupConsensusRequest {
    type Response = GroupConsensusResponse;
}
