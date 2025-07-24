//! Enumeration types

use serde::{Deserialize, Serialize};

/// Node state in the consensus system
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeState {
    /// Node is initializing
    Initializing,
    /// Node is discovering peers
    Discovering,
    /// Node is joining cluster
    Joining,
    /// Node is active and participating
    Active,
    /// Node is leaving cluster
    Leaving,
    /// Node has failed
    Failed,
}

/// Group lifecycle status (renamed from GroupState2)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GroupStatus {
    /// Group is forming
    Forming,
    /// Group is active
    Active,
    /// Group is rebalancing
    Rebalancing,
    /// Group is dissolving
    Dissolving,
}

/// Consensus role
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConsensusRole {
    /// Leader of the consensus group
    Leader,
    /// Follower in the consensus group
    Follower,
    /// Candidate during election
    Candidate,
    /// Non-voting learner
    Learner,
}

/// Operation priority
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum OperationPriority {
    /// Low priority
    Low,
    /// Normal priority
    Normal,
    /// High priority
    High,
    /// Critical priority
    Critical,
}

impl Default for OperationPriority {
    fn default() -> Self {
        Self::Normal
    }
}
