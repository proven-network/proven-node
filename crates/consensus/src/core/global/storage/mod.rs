//! Storage implementations for global consensus
//!
//! This module contains storage components specific to global consensus:
//! - Log storage for Raft log entries
//! - Factories for creating storage instances
//! - Snapshot functionality
//! - Type definitions for global consensus storage

use std::fmt::Debug;

use crate::core::global::GlobalConsensusTypeConfig;
use openraft::storage::{RaftLogStorage, RaftStateMachine};

// Storage implementation modules
pub mod factory;
pub mod log_storage;
pub mod log_store;
pub mod log_types;
pub mod snapshot;

// Re-export main types
pub use factory::{GlobalLogStorageType, GlobalStorageType, create_global_storage_factory};

// Re-export log types

/// Trait alias for log storage requirements
pub trait ConsensusLogStorage:
    RaftLogStorage<GlobalConsensusTypeConfig> + Debug + Send + Sync + Clone + 'static
{
}

/// Trait alias for state machine requirements
pub trait ConsensusStateMachine:
    RaftStateMachine<GlobalConsensusTypeConfig> + Debug + Send + Sync + 'static
{
}

// Implement the trait alias for the log storage type
impl ConsensusLogStorage for GlobalLogStorageType {}

#[cfg(test)]
mod tests;
