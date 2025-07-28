//! Messages for group consensus service

use proven_network::{NetworkMessage, ServiceMessage};
use serde::{Deserialize, Serialize};

use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};

use crate::consensus::group::{GroupRequest, GroupResponse, GroupTypeConfig};
use crate::foundation::models::stream::StreamState;
use crate::foundation::{StreamName, types::ConsensusGroupId};

/// Group consensus service message
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum GroupConsensusMessage {
    /// Vote request from Raft
    Vote {
        group_id: ConsensusGroupId,
        request: VoteRequest<GroupTypeConfig>,
    },
    /// Append entries request from Raft
    AppendEntries {
        group_id: ConsensusGroupId,
        request: AppendEntriesRequest<GroupTypeConfig>,
    },
    /// Install snapshot request from Raft
    InstallSnapshot {
        group_id: ConsensusGroupId,
        request: InstallSnapshotRequest<GroupTypeConfig>,
    },
    /// Group-level consensus request
    Consensus {
        group_id: ConsensusGroupId,
        request: GroupRequest,
    },
    /// Get stream state request
    GetStreamState {
        group_id: ConsensusGroupId,
        stream_name: StreamName,
    },
}

/// Group consensus service response
#[allow(clippy::large_enum_variant)] // TODO: Box the large enum variants
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum GroupConsensusServiceResponse {
    /// Vote response
    Vote {
        group_id: ConsensusGroupId,
        response: VoteResponse<GroupTypeConfig>,
    },
    /// Append entries response
    AppendEntries {
        group_id: ConsensusGroupId,
        response: AppendEntriesResponse<GroupTypeConfig>,
    },
    /// Install snapshot response  
    InstallSnapshot {
        group_id: ConsensusGroupId,
        response: InstallSnapshotResponse<GroupTypeConfig>,
    },
    /// Application-level consensus response
    Consensus {
        group_id: ConsensusGroupId,
        response: GroupResponse,
    },
    /// Stream state response
    StreamState(Option<StreamState>),
    /// Error response
    Error(String),
}

impl NetworkMessage for GroupConsensusMessage {
    fn message_type() -> &'static str {
        "group_consensus_message"
    }
}

impl NetworkMessage for GroupConsensusServiceResponse {
    fn message_type() -> &'static str {
        "group_consensus_response"
    }
}

impl ServiceMessage for GroupConsensusMessage {
    type Response = GroupConsensusServiceResponse;

    fn service_id() -> &'static str {
        "group_consensus"
    }
}
