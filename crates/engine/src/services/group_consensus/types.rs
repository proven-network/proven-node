//! Types for group consensus service

use serde::{Deserialize, Serialize};
use std::{num::NonZero, time::SystemTime};

use proven_topology::NodeId;

use crate::foundation::types::ConsensusGroupId;

/// Group state information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupStateInfo {
    /// Group ID
    pub group_id: ConsensusGroupId,
    /// Current members of the group
    pub members: Vec<NodeId>,
    /// Current leader of the group
    pub leader: Option<NodeId>,
    /// Current term
    pub term: u64,
    /// Whether this node is a member of the group
    pub is_member: bool,
    /// Streams managed by this group
    pub streams: Vec<StreamInfo>,
    /// Total messages across all streams
    pub total_messages: u64,
    /// Total bytes across all streams
    pub total_bytes: u64,
    /// When the group was created
    pub created_at: SystemTime,
    /// Last update time
    pub last_updated: SystemTime,
}

/// Stream information within a group
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamInfo {
    /// Stream name
    pub name: String,
    /// Number of messages in the stream
    pub message_count: u64,
    /// Next sequence number
    pub next_sequence: NonZero<u64>,
    /// First sequence number
    pub first_sequence: NonZero<u64>,
    /// Total bytes in the stream
    pub total_bytes: u64,
    /// Last update timestamp
    pub last_update: u64,
}
