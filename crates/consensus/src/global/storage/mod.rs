//! Storage implementations for consensus
//!
//! This module provides different storage backends for the consensus system.
//! All storage implementations must implement both RaftLogStorage and RaftStateMachine
//! traits from OpenRaft.

use std::fmt::Debug;

use crate::global::GlobalTypeConfig;
use openraft::storage::{RaftLogStorage, RaftStateMachine};

// Unified storage implementation
pub mod factory;
pub mod log_types;
pub mod unified;

// Re-export new unified storage
pub use factory::{
    GlobalStorageFactory, GlobalStorageType, UnifiedGlobalStorageFactory,
    create_global_storage_factory,
};
pub use unified::GlobalStorage;

/// Trait alias for consensus storage requirements
pub trait ConsensusStorage:
    RaftLogStorage<GlobalTypeConfig>
    + RaftStateMachine<GlobalTypeConfig>
    + Debug
    + Send
    + Sync
    + Clone
    + 'static
{
}

// Implement the trait alias for the unified storage type
impl ConsensusStorage for GlobalStorageType {}
