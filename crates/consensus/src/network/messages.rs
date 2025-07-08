//! Network message types for consensus communication
//!
//! This module contains all message types used for network communication
//! between consensus nodes, including Raft protocol messages and
//! application-level cluster management messages.

use proven_governance::GovernanceNode;
use serde::{Deserialize, Serialize};

use crate::NodeId;
use crate::global::{GlobalRequest, GlobalResponse};

/// Message types for consensus operations
/// Top-level message enum with two main categories
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Message {
    /// Raft protocol messages (handled by Raft engine)
    Raft(RaftMessage),
    /// Application-level messages (handled by consensus layer)
    Application(Box<ApplicationMessage>),
}

/// Raft protocol messages - these are opaque to the application layer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftMessage {
    /// Raft vote request
    Vote(Vec<u8>),
    /// Raft vote response
    VoteResponse(Vec<u8>),
    /// Raft append entries request
    AppendEntries(Vec<u8>),
    /// Raft append entries response
    AppendEntriesResponse(Vec<u8>),
    /// Raft install snapshot request
    InstallSnapshot(Vec<u8>),
    /// Raft install snapshot response
    InstallSnapshotResponse(Vec<u8>),
}

/// Application-level messages - these are handled by the consensus layer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ApplicationMessage {
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
    /// PubSub message
    PubSub(Box<crate::pubsub::PubSubMessage>),
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
    pub requester_node: GovernanceNode,
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
        Message::Application(Box::new(ApplicationMessage::ClusterDiscovery(request)))
    }
}

impl TryFrom<Message> for ClusterDiscoveryResponse {
    type Error = &'static str;

    fn try_from(message: Message) -> Result<Self, Self::Error> {
        match message {
            Message::Application(app_msg) => match *app_msg {
                ApplicationMessage::ClusterDiscoveryResponse(response) => Ok(response),
                _ => Err("Message is not a ClusterDiscoveryResponse"),
            },
            _ => Err("Message is not a ClusterDiscoveryResponse"),
        }
    }
}

impl From<ClusterJoinRequest> for Message {
    fn from(request: ClusterJoinRequest) -> Self {
        Message::Application(Box::new(ApplicationMessage::ClusterJoinRequest(request)))
    }
}

impl From<ClusterJoinResponse> for Message {
    fn from(response: ClusterJoinResponse) -> Self {
        Message::Application(Box::new(ApplicationMessage::ClusterJoinResponse(response)))
    }
}

impl TryFrom<Message> for ClusterJoinResponse {
    type Error = &'static str;

    fn try_from(message: Message) -> Result<Self, Self::Error> {
        match message {
            Message::Application(app_msg) => match *app_msg {
                ApplicationMessage::ClusterJoinResponse(response) => Ok(response),
                _ => Err("Message is not a ClusterJoinResponse"),
            },
            _ => Err("Message is not a ClusterJoinResponse"),
        }
    }
}

impl From<GlobalRequest> for Message {
    fn from(request: GlobalRequest) -> Self {
        Message::Application(Box::new(ApplicationMessage::ConsensusRequest(request)))
    }
}

impl TryFrom<Message> for GlobalResponse {
    type Error = &'static str;

    fn try_from(message: Message) -> Result<Self, Self::Error> {
        match message {
            Message::Application(app_msg) => match *app_msg {
                ApplicationMessage::ConsensusResponse(response) => Ok(response),
                _ => Err("Message is not a MessagingResponse"),
            },
            _ => Err("Message is not a MessagingResponse"),
        }
    }
}
