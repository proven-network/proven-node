//! Storage implementations for group consensus
//!
//! This module contains storage components specific to consensus groups:
//! - Log storage for group Raft log entries
//! - Factories for creating storage instances
//! - Type definitions for group consensus storage

use std::fmt::Debug;

use crate::core::group::GroupConsensusTypeConfig;
use openraft::storage::{RaftLogStorage, RaftStateMachine};

// Storage implementation modules
pub mod factory;
pub mod group_store;
pub mod log_storage;
pub mod log_types;

// Re-export main types
pub use factory::{GroupStorageFactory, create_local_storage_factory};

// Re-export log types

/// Trait alias for group log storage requirements
pub trait GroupLogStorage:
    RaftLogStorage<GroupConsensusTypeConfig> + Debug + Send + Sync + Clone + 'static
{
}

/// Trait alias for group state machine requirements
pub trait GroupStateMachine:
    RaftStateMachine<GroupConsensusTypeConfig> + Debug + Send + Sync + 'static
{
}
