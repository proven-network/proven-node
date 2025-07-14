//! Network message types for consensus communication
//!
//! This module contains all message types used for network communication
//! between consensus nodes, including Raft protocol messages and
//! application-level cluster management messages.

use crate::ConsensusGroupId;
use crate::core::global::{GlobalConsensusTypeConfig, GlobalRequest, GlobalResponse};
use crate::core::group::GroupConsensusTypeConfig;

use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use proven_network::SerializableMessage;
use proven_topology::{Node, NodeId};
use serde::{Deserialize, Serialize};

/// Message types for consensus operations
/// Top-level message enum that maps to our main managers
#[derive(Debug, Serialize, Deserialize)]
pub enum Message {
    /// Messages for GlobalManager (global consensus, discovery, etc)
    Global(Box<GlobalMessage>),
    /// Messages for LocalManager (local consensus groups)
    Local(Box<GroupMessage>),
    /// Messages for PubSubManager
    PubSub(Box<crate::pubsub::PubSubMessage>),
}

type GlobalVoteRequest = VoteRequest<GlobalConsensusTypeConfig>;
type GlobalVoteResponse = VoteResponse<GlobalConsensusTypeConfig>;
type GlobalAppendEntriesRequest = AppendEntriesRequest<GlobalConsensusTypeConfig>;
type GlobalAppendEntriesResponse = AppendEntriesResponse<GlobalConsensusTypeConfig>;
type GlobalInstallSnapshotRequest = InstallSnapshotRequest<GlobalConsensusTypeConfig>;
type GlobalInstallSnapshotResponse = InstallSnapshotResponse<GlobalConsensusTypeConfig>;

/// Raft protocol messages - these are opaque to the application layer
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum GlobalRaftMessage {
    /// Raft vote request
    VoteRequest(GlobalVoteRequest),
    /// Raft vote response
    VoteResponse(GlobalVoteResponse),
    /// Raft append entries request
    AppendEntriesRequest(GlobalAppendEntriesRequest),
    /// Raft append entries response
    AppendEntriesResponse(GlobalAppendEntriesResponse),
    /// Raft install snapshot request
    InstallSnapshotRequest(GlobalInstallSnapshotRequest),
    /// Raft install snapshot response
    InstallSnapshotResponse(GlobalInstallSnapshotResponse),
}

/// Messages handled by GlobalManager
#[derive(Debug, Serialize, Deserialize)]
pub enum GlobalMessage {
    /// Raft protocol message for global consensus
    Raft(GlobalRaftMessage),
    /// Cluster discovery request
    ClusterDiscovery(ClusterDiscoveryRequest),
    /// Cluster discovery response
    ClusterDiscoveryResponse(ClusterDiscoveryResponse),
    /// Cluster join request
    ClusterJoinRequest(ClusterJoinRequest),
    /// Cluster join response
    ClusterJoinResponse(ClusterJoinResponse),
    /// Consensus messaging request
    ConsensusRequest(GlobalRequest),
    /// Consensus messaging response
    ConsensusResponse(GlobalResponse),
}

type GroupVoteRequest = VoteRequest<GroupConsensusTypeConfig>;
type GroupVoteResponse = VoteResponse<GroupConsensusTypeConfig>;
type GroupAppendEntriesRequest = AppendEntriesRequest<GroupConsensusTypeConfig>;
type GroupAppendEntriesResponse = AppendEntriesResponse<GroupConsensusTypeConfig>;
type GroupInstallSnapshotRequest = InstallSnapshotRequest<GroupConsensusTypeConfig>;
type GroupInstallSnapshotResponse = InstallSnapshotResponse<GroupConsensusTypeConfig>;

/// Raft protocol messages - these are opaque to the application layer
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum GroupRaftMessage {
    /// Raft vote request
    VoteRequest(GroupVoteRequest),
    /// Raft vote response
    VoteResponse(GroupVoteResponse),
    /// Raft append entries request
    AppendEntriesRequest(GroupAppendEntriesRequest),
    /// Raft append entries response
    AppendEntriesResponse(GroupAppendEntriesResponse),
    /// Raft install snapshot request
    InstallSnapshotRequest(GroupInstallSnapshotRequest),
    /// Raft install snapshot response
    InstallSnapshotResponse(GroupInstallSnapshotResponse),
}

/// Messages handled by LocalManager
#[derive(Debug, Serialize, Deserialize)]
pub enum GroupMessage {
    /// Raft protocol message for a specific local consensus group
    Raft {
        /// The consensus group this message is for
        group_id: ConsensusGroupId,
        /// The actual Raft message
        message: GroupRaftMessage,
    },
    /// Local group discovery request
    GroupDiscovery(LocalGroupDiscoveryRequest),
    /// Local group discovery response
    GroupDiscoveryResponse(LocalGroupDiscoveryResponse),
    /// Local group join request
    GroupJoinRequest(LocalGroupJoinRequest),
    /// Local group join response
    GroupJoinResponse(LocalGroupJoinResponse),
}

// SerializableMessage implementations for individual message types
impl SerializableMessage for ClusterDiscoveryRequest {
    fn message_type(&self) -> &'static str {
        "cluster_discovery_request"
    }
}

impl SerializableMessage for ClusterDiscoveryResponse {
    fn message_type(&self) -> &'static str {
        "cluster_discovery_response"
    }
}

impl SerializableMessage for ClusterJoinRequest {
    fn message_type(&self) -> &'static str {
        "cluster_join_request"
    }
}

impl SerializableMessage for ClusterJoinResponse {
    fn message_type(&self) -> &'static str {
        "cluster_join_response"
    }
}

impl SerializableMessage for GlobalRequest {
    fn message_type(&self) -> &'static str {
        "global_request"
    }
}

impl SerializableMessage for GlobalResponse {
    fn message_type(&self) -> &'static str {
        "global_response"
    }
}

// Implement HandledMessage for request/response pairs
impl proven_network::message::HandledMessage for ClusterDiscoveryRequest {
    type Response = ClusterDiscoveryResponse;
}

impl proven_network::message::HandledMessage for ClusterJoinRequest {
    type Response = ClusterJoinResponse;
}

impl proven_network::message::HandledMessage for GlobalRequest {
    type Response = GlobalResponse;
}

/// Request to discover existing clusters
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterDiscoveryRequest {
    /// ID of the requesting node
    pub requester_id: NodeId,
}

/// Response to cluster discovery request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterDiscoveryResponse {
    /// ID of the responding node
    pub responder_id: NodeId,
    /// Whether this node has an active cluster
    pub has_active_cluster: bool,
    /// Current raft term if in cluster
    pub current_term: Option<u64>,
    /// Current leader if known
    pub current_leader: Option<NodeId>,
    /// Size of the cluster if known
    pub cluster_size: Option<usize>,
}

/// Request to join an existing cluster
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterJoinRequest {
    /// ID of the requesting node
    pub requester_id: NodeId,
    /// Governance node information for the requester
    pub requester_node: Node,
}

/// Response to cluster join request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterJoinResponse {
    /// Error message if join failed
    pub error_message: Option<String>,
    /// Current cluster size after join (if successful)
    pub cluster_size: Option<usize>,
    /// Current term after join (if successful)
    pub current_term: Option<u64>,
    /// ID of the responding node (leader)
    pub responder_id: NodeId,
    /// Whether the join request was successful
    pub success: bool,
    /// ID of the current leader (if known)
    pub current_leader: Option<NodeId>,
}

/// Request to discover local consensus group leader
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalGroupDiscoveryRequest {
    /// ID of the consensus group
    pub group_id: ConsensusGroupId,
    /// ID of the requesting node
    pub requester_id: NodeId,
}

/// Response to local group discovery request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalGroupDiscoveryResponse {
    /// ID of the consensus group
    pub group_id: ConsensusGroupId,
    /// ID of the responding node
    pub responder_id: NodeId,
    /// Whether this node is part of the group
    pub is_member: bool,
    /// Current leader of the group if known
    pub leader_id: Option<NodeId>,
    /// Current term of the group if member
    pub current_term: Option<u64>,
    /// Members of the group if known
    pub members: Option<Vec<NodeId>>,
}

/// Request to join a local consensus group
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalGroupJoinRequest {
    /// ID of the consensus group
    pub group_id: ConsensusGroupId,
    /// ID of the requesting node
    pub requester_id: NodeId,
    /// Node information for the requester
    pub requester_node: Node,
}

/// Response to local group join request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalGroupJoinResponse {
    /// ID of the consensus group
    pub group_id: ConsensusGroupId,
    /// ID of the responding node (leader)
    pub responder_id: NodeId,
    /// Whether the join was successful
    pub success: bool,
    /// Error message if join failed
    pub error_message: Option<String>,
    /// Current members after join (if successful)
    pub members: Option<Vec<NodeId>>,
}

impl ClusterDiscoveryRequest {
    /// Create a new discovery request
    pub fn new(requester_id: NodeId) -> Self {
        Self { requester_id }
    }
}

impl ClusterDiscoveryResponse {
    /// Create a new discovery response
    pub fn new(responder_id: NodeId) -> Self {
        Self {
            responder_id,
            has_active_cluster: false,
            current_term: None,
            current_leader: None,
            cluster_size: None,
        }
    }
}

/// Conversion implementations for request/response types
impl From<ClusterDiscoveryRequest> for Message {
    fn from(request: ClusterDiscoveryRequest) -> Self {
        Message::Global(Box::new(GlobalMessage::ClusterDiscovery(request)))
    }
}

impl TryFrom<Message> for ClusterDiscoveryResponse {
    type Error = &'static str;

    fn try_from(message: Message) -> Result<Self, Self::Error> {
        match message {
            Message::Global(global_msg) => match *global_msg {
                GlobalMessage::ClusterDiscoveryResponse(response) => Ok(response),
                _ => Err("Message is not a ClusterDiscoveryResponse"),
            },
            _ => Err("Message is not a ClusterDiscoveryResponse"),
        }
    }
}

impl From<ClusterJoinRequest> for Message {
    fn from(request: ClusterJoinRequest) -> Self {
        Message::Global(Box::new(GlobalMessage::ClusterJoinRequest(request)))
    }
}

impl From<ClusterJoinResponse> for Message {
    fn from(response: ClusterJoinResponse) -> Self {
        Message::Global(Box::new(GlobalMessage::ClusterJoinResponse(response)))
    }
}

impl TryFrom<Message> for ClusterJoinResponse {
    type Error = &'static str;

    fn try_from(message: Message) -> Result<Self, Self::Error> {
        match message {
            Message::Global(global_msg) => match *global_msg {
                GlobalMessage::ClusterJoinResponse(response) => Ok(response),
                _ => Err("Message is not a ClusterJoinResponse"),
            },
            _ => Err("Message is not a ClusterJoinResponse"),
        }
    }
}

impl From<GlobalRequest> for Message {
    fn from(request: GlobalRequest) -> Self {
        Message::Global(Box::new(GlobalMessage::ConsensusRequest(request)))
    }
}

impl TryFrom<Message> for GlobalResponse {
    type Error = &'static str;

    fn try_from(message: Message) -> Result<Self, Self::Error> {
        match message {
            Message::Global(global_msg) => match *global_msg {
                GlobalMessage::ConsensusResponse(response) => Ok(response),
                _ => Err("Message is not a GlobalResponse"),
            },
            _ => Err("Message is not a GlobalResponse"),
        }
    }
}

impl From<LocalGroupDiscoveryRequest> for Message {
    fn from(request: LocalGroupDiscoveryRequest) -> Self {
        Message::Local(Box::new(GroupMessage::GroupDiscovery(request)))
    }
}

impl TryFrom<Message> for LocalGroupDiscoveryResponse {
    type Error = &'static str;

    fn try_from(message: Message) -> Result<Self, Self::Error> {
        match message {
            Message::Local(local_msg) => match *local_msg {
                GroupMessage::GroupDiscoveryResponse(response) => Ok(response),
                _ => Err("Message is not a LocalGroupDiscoveryResponse"),
            },
            _ => Err("Message is not a LocalGroupDiscoveryResponse"),
        }
    }
}

impl From<LocalGroupJoinRequest> for Message {
    fn from(request: LocalGroupJoinRequest) -> Self {
        Message::Local(Box::new(GroupMessage::GroupJoinRequest(request)))
    }
}

impl TryFrom<Message> for LocalGroupJoinResponse {
    type Error = &'static str;

    fn try_from(message: Message) -> Result<Self, Self::Error> {
        match message {
            Message::Local(local_msg) => match *local_msg {
                GroupMessage::GroupJoinResponse(response) => Ok(response),
                _ => Err("Message is not a LocalGroupJoinResponse"),
            },
            _ => Err("Message is not a LocalGroupJoinResponse"),
        }
    }
}

// Implement SerializableMessage trait for Message
impl SerializableMessage for Message {
    fn message_type(&self) -> &'static str {
        match self {
            Message::Global(global_msg) => match global_msg.as_ref() {
                GlobalMessage::Raft(_) => "global_raft",
                GlobalMessage::ClusterDiscovery(_) => "cluster_discovery",
                GlobalMessage::ClusterDiscoveryResponse(_) => "cluster_discovery_response",
                GlobalMessage::ClusterJoinRequest(_) => "cluster_join_request",
                GlobalMessage::ClusterJoinResponse(_) => "cluster_join_response",
                GlobalMessage::ConsensusRequest(_) => "consensus_request",
                GlobalMessage::ConsensusResponse(_) => "consensus_response",
            },
            Message::Local(group_msg) => match group_msg.as_ref() {
                GroupMessage::Raft { .. } => "group_raft",
                GroupMessage::GroupDiscovery(_) => "group_discovery",
                GroupMessage::GroupDiscoveryResponse(_) => "group_discovery_response",
                GroupMessage::GroupJoinRequest(_) => "group_join_request",
                GroupMessage::GroupJoinResponse(_) => "group_join_response",
            },
            Message::PubSub(_) => "pubsub",
        }
    }
}

// Implement HandledMessage for Message
impl proven_network::message::HandledMessage for Message {
    type Response = Message;
}

impl Message {
    /// Deserialize a Message from bytes
    pub fn from_bytes(bytes: &[u8]) -> proven_network::NetworkResult<Self> {
        ciborium::de::from_reader(bytes)
            .map_err(|e| proven_network::NetworkError::Serialization(e.to_string()))
    }
}
