//! Core types used throughout the consensus system

use serde::{Deserialize, Serialize};
use std::fmt;
use std::time::{Duration, SystemTime};

/// Consensus group identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ConsensusGroupId(u32);

impl ConsensusGroupId {
    /// Create a new consensus group ID
    pub fn new(id: u32) -> Self {
        Self(id)
    }

    /// Get the inner value
    pub fn value(&self) -> u32 {
        self.0
    }
}

impl fmt::Display for ConsensusGroupId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "group-{}", self.0)
    }
}

/// Operation identifier
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OperationId(uuid::Uuid);

impl OperationId {
    /// Create a new operation ID
    pub fn new() -> Self {
        Self(uuid::Uuid::new_v4())
    }

    /// Create from UUID
    pub fn from_uuid(id: uuid::Uuid) -> Self {
        Self(id)
    }

    /// Get the inner UUID
    pub fn as_uuid(&self) -> &uuid::Uuid {
        &self.0
    }
}

impl Default for OperationId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for OperationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

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

/// Group state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GroupState2 {
    /// Group is forming
    Forming,
    /// Group is active
    Active,
    /// Group is rebalancing
    Rebalancing,
    /// Group is dissolving
    Dissolving,
}

/// Timestamp type for consensus operations
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ConsensusTimestamp {
    /// Seconds since epoch
    secs: u64,
    /// Nanoseconds
    nanos: u32,
}

impl ConsensusTimestamp {
    /// Create a new timestamp from current time
    pub fn now() -> Self {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("System time before epoch");
        Self {
            secs: now.as_secs(),
            nanos: now.subsec_nanos(),
        }
    }

    /// Create from duration since epoch
    pub fn from_duration(duration: Duration) -> Self {
        Self {
            secs: duration.as_secs(),
            nanos: duration.subsec_nanos(),
        }
    }

    /// Get duration since epoch
    pub fn duration_since_epoch(&self) -> Duration {
        Duration::new(self.secs, self.nanos)
    }
}

/// Term number in consensus
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Term(u64);

impl Term {
    /// Create a new term
    pub fn new(term: u64) -> Self {
        Self(term)
    }

    /// Get the inner value
    pub fn value(&self) -> u64 {
        self.0
    }

    /// Increment the term
    pub fn increment(&mut self) {
        self.0 += 1;
    }
}

/// Log index in consensus
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct LogIndex(u64);

impl LogIndex {
    /// Create a new log index
    pub fn new(index: u64) -> Self {
        Self(index)
    }

    /// Get the inner value
    pub fn value(&self) -> u64 {
        self.0
    }

    /// Increment the index
    pub fn increment(&mut self) {
        self.0 += 1;
    }
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
