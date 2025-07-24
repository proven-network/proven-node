//! Identifier types

use serde::{Deserialize, Serialize};
use std::fmt;

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
