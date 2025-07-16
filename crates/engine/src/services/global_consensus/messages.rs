//! Messages for global consensus service

use proven_network::{HandledMessage, namespace::MessageType};
use serde::{Deserialize, Serialize};

use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};

use crate::consensus::global::{GlobalRequest, GlobalResponse, GlobalTypeConfig};

/// Global consensus namespace
pub const GLOBAL_CONSENSUS_NAMESPACE: &str = "global_consensus";

// Global consensus message types

/// Global vote request
#[derive(Debug, Serialize, Deserialize)]
pub struct GlobalVoteRequest(pub VoteRequest<GlobalTypeConfig>);

/// Global vote response
#[derive(Debug, Serialize, Deserialize)]
pub struct GlobalVoteResponse(pub VoteResponse<GlobalTypeConfig>);

/// Global append entries request
#[derive(Debug, Serialize, Deserialize)]
pub struct GlobalAppendEntriesRequest(pub AppendEntriesRequest<GlobalTypeConfig>);

/// Global append entries response
#[derive(Debug, Serialize, Deserialize)]
pub struct GlobalAppendEntriesResponse(pub AppendEntriesResponse<GlobalTypeConfig>);

/// Global install snapshot request
#[derive(Debug, Serialize, Deserialize)]
pub struct GlobalInstallSnapshotRequest(pub InstallSnapshotRequest<GlobalTypeConfig>);

/// Global install snapshot response
#[derive(Debug, Serialize, Deserialize)]
pub struct GlobalInstallSnapshotResponse(pub InstallSnapshotResponse<GlobalTypeConfig>);

/// Global consensus application request
#[derive(Debug, Serialize, Deserialize)]
pub struct GlobalConsensusRequest(pub GlobalRequest);

/// Global consensus application response
#[derive(Debug, Serialize, Deserialize)]
pub struct GlobalConsensusResponse(pub GlobalResponse);

// Implement MessageType for all message types

impl MessageType for GlobalVoteRequest {
    fn message_type(&self) -> &'static str {
        "vote_request"
    }
}

impl MessageType for GlobalVoteResponse {
    fn message_type(&self) -> &'static str {
        "vote_response"
    }
}

impl MessageType for GlobalAppendEntriesRequest {
    fn message_type(&self) -> &'static str {
        "append_entries_request"
    }
}

impl MessageType for GlobalAppendEntriesResponse {
    fn message_type(&self) -> &'static str {
        "append_entries_response"
    }
}

impl MessageType for GlobalInstallSnapshotRequest {
    fn message_type(&self) -> &'static str {
        "install_snapshot_request"
    }
}

impl MessageType for GlobalInstallSnapshotResponse {
    fn message_type(&self) -> &'static str {
        "install_snapshot_response"
    }
}

impl MessageType for GlobalConsensusRequest {
    fn message_type(&self) -> &'static str {
        "consensus_request"
    }
}

impl MessageType for GlobalConsensusResponse {
    fn message_type(&self) -> &'static str {
        "consensus_response"
    }
}

// Implement HandledMessage for request types

impl HandledMessage for GlobalVoteRequest {
    type Response = GlobalVoteResponse;
}

impl HandledMessage for GlobalAppendEntriesRequest {
    type Response = GlobalAppendEntriesResponse;
}

impl HandledMessage for GlobalInstallSnapshotRequest {
    type Response = GlobalInstallSnapshotResponse;
}

impl HandledMessage for GlobalConsensusRequest {
    type Response = GlobalConsensusResponse;
}
