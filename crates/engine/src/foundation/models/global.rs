//! Global consensus data models

use std::collections::HashMap;
use std::time::SystemTime;

use proven_topology::NodeId;
use serde::{Deserialize, Serialize};

use crate::foundation::types::{ConsensusGroupId, StreamName};

/// Consensus group information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupInfo {
    /// Group ID
    pub id: ConsensusGroupId,
    /// Member nodes
    pub members: Vec<NodeId>,
    /// Creation timestamp
    pub created_at: u64,
    /// Group metadata
    pub metadata: HashMap<String, String>,
}

/// Node information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    /// Node ID
    pub node_id: NodeId,
    /// When the node joined
    pub joined_at: u64,
    /// Node metadata
    pub metadata: HashMap<String, String>,
}

/// Detailed group state information (used by group consensus service)
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
    /// Stream names managed by this group
    pub stream_names: Vec<StreamName>,
    /// Total messages across all streams
    pub total_messages: u64,
    /// Total bytes across all streams
    pub total_bytes: u64,
    /// When the group was created
    pub created_at: SystemTime,
    /// Last update time
    pub last_updated: SystemTime,
}
