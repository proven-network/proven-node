//! Global consensus data models

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::foundation::types::ConsensusGroupId;
use proven_topology::NodeId;

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
