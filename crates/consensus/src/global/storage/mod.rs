//! Storage implementations for consensus
//!
//! This module provides different storage backends for the consensus system.
//! All storage implementations must implement both RaftLogStorage and RaftStateMachine
//! traits from OpenRaft.

use std::fmt::Debug;
use std::sync::Arc;

use crate::error::ConsensusResult;
use crate::global::GlobalState;
use crate::global::{GlobalRequest, GlobalResponse, GlobalTypeConfig};
use openraft::storage::{RaftLogStorage, RaftStateMachine};

pub mod memory;
pub mod rocksdb;

// Re-export for convenience
pub use memory::GlobalConsensusMemoryStorage;
pub use rocksdb::GlobalConsensusRocksStorage;

// Compatibility aliases - to be removed
pub use create_memory_storage as create_memory_storage_with_global_state;
pub use create_rocks_storage as create_rocks_storage_with_global_state;

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

// Implement the trait alias for our storage types
impl ConsensusStorage for GlobalConsensusMemoryStorage {}
impl ConsensusStorage for GlobalConsensusRocksStorage {}

/// Apply a messaging request to the state machine data
pub fn apply_request_to_state_machine(
    data: &mut std::collections::BTreeMap<String, String>,
    request: &GlobalRequest,
    sequence: u64,
) -> GlobalResponse {
    // For now, we'll store a simple representation in the BTreeMap
    // In a real implementation, this would integrate with StreamStore

    // Store the request as a JSON string for persistence
    if let Ok(json) = serde_json::to_string(&request) {
        data.insert(format!("request_{}", sequence), json);
    }

    // Return a response indicating the operation was stored
    // The actual processing would happen when applied to StreamStore
    GlobalResponse {
        sequence,
        success: true,
        error: None,
    }
}

/// Apply a messaging request to GlobalState
pub async fn apply_request_to_global_state(
    global_state: &Arc<GlobalState>,
    request: &GlobalRequest,
    sequence: u64,
) -> GlobalResponse {
    // Apply the operation to the global state
    global_state
        .apply_operation(&request.operation, sequence)
        .await
}

/// Create a new memory storage instance with GlobalState
pub fn create_memory_storage(
    global_state: Arc<GlobalState>,
) -> ConsensusResult<GlobalConsensusMemoryStorage> {
    Ok(GlobalConsensusMemoryStorage::new(global_state))
}

/// Create a new RocksDB storage instance with GlobalState
pub fn create_rocks_storage(
    db_path: &str,
    global_state: Arc<GlobalState>,
) -> ConsensusResult<GlobalConsensusRocksStorage> {
    GlobalConsensusRocksStorage::new_with_path(db_path, global_state)
}
