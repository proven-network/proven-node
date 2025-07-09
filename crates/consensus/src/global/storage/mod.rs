//! Storage implementations for consensus
//!
//! This module provides different storage backends for the consensus system.
//! All storage implementations must implement both RaftLogStorage and RaftStateMachine
//! traits from OpenRaft.

use std::fmt::Debug;
use std::sync::Arc;

use crate::global::GlobalState;
use crate::global::{GlobalRequest, GlobalResponse, GlobalTypeConfig};
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
