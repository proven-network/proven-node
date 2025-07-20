//! Messages for global consensus service

use proven_network::ServiceMessage;
use serde::{Deserialize, Serialize};

use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};

use crate::consensus::global::{GlobalRequest, GlobalResponse, GlobalTypeConfig};
use proven_topology::NodeId;

/// Global consensus service message
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum GlobalConsensusMessage {
    /// Vote request from Raft
    Vote(VoteRequest<GlobalTypeConfig>),
    /// Append entries request from Raft
    AppendEntries(AppendEntriesRequest<GlobalTypeConfig>),
    /// Install snapshot request from Raft
    InstallSnapshot(InstallSnapshotRequest<GlobalTypeConfig>),
    /// Global-level consensus request
    Consensus(GlobalRequest),
    /// Check if a cluster already exists
    CheckClusterExists(CheckClusterExistsRequest),
}

/// Request to check if a cluster already exists
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckClusterExistsRequest {
    /// The node ID making the request
    pub node_id: NodeId,
}

/// Global consensus service response
#[allow(clippy::large_enum_variant)] // TODO: Box the large enum variants
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum GlobalConsensusResponse {
    /// Vote response
    Vote(VoteResponse<GlobalTypeConfig>),
    /// Append entries response
    AppendEntries(AppendEntriesResponse<GlobalTypeConfig>),
    /// Install snapshot response  
    InstallSnapshot(InstallSnapshotResponse<GlobalTypeConfig>),
    /// Application-level consensus response
    Consensus(GlobalResponse),
    /// Response to cluster exists check
    CheckClusterExists(CheckClusterExistsResponse),
}

/// Response to check if a cluster already exists
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckClusterExistsResponse {
    /// Whether this node has an initialized cluster
    pub cluster_exists: bool,
    /// Current cluster leader if known
    pub current_leader: Option<NodeId>,
    /// Current term
    pub current_term: u64,
    /// Members of the cluster if known
    pub members: Vec<NodeId>,
}

impl ServiceMessage for GlobalConsensusMessage {
    type Response = GlobalConsensusResponse;

    fn service_id() -> &'static str {
        "global_consensus"
    }
}
