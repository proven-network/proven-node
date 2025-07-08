//! Storage implementations for consensus
//!
//! This module provides different storage backends for the consensus system.
//! All storage implementations must implement both RaftLogStorage and RaftStateMachine
//! traits from OpenRaft.

use std::fmt::Debug;
use std::sync::Arc;

use crate::error::ConsensusResult;
use crate::global::StreamStore;
use crate::global::{GlobalRequest, GlobalResponse, GlobalTypeConfig};
use openraft::storage::{RaftLogStorage, RaftStateMachine};

pub mod memory;
pub mod rocksdb;

// Re-export for convenience
pub use memory::MemoryConsensusStorage;
pub use rocksdb::RocksConsensusStorage;

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
impl ConsensusStorage for MemoryConsensusStorage {}
impl ConsensusStorage for RocksConsensusStorage {}

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

/// Apply a messaging request to a StreamStore
pub async fn apply_request_to_stream_store(
    stream_store: &Arc<StreamStore>,
    request: &GlobalRequest,
    sequence: u64,
) -> GlobalResponse {
    // Apply the operation to the stream store
    stream_store
        .apply_operation(&request.operation, sequence)
        .await
}

/// Create a new memory storage instance
pub fn create_memory_storage() -> ConsensusResult<MemoryConsensusStorage> {
    Ok(MemoryConsensusStorage::new())
}

/// Create a new memory storage instance with StreamStore
pub fn create_memory_storage_with_stream_store(
    stream_store: Arc<StreamStore>,
) -> ConsensusResult<MemoryConsensusStorage> {
    Ok(MemoryConsensusStorage::new_with_stream_store(stream_store))
}

/// Create a new RocksDB storage instance
pub fn create_rocks_storage(db_path: &str) -> ConsensusResult<RocksConsensusStorage> {
    RocksConsensusStorage::new_with_path(db_path)
}

/// Create a new RocksDB storage instance with StreamStore
pub fn create_rocks_storage_with_stream_store(
    db_path: &str,
    stream_store: Arc<StreamStore>,
) -> ConsensusResult<RocksConsensusStorage> {
    let storage = RocksConsensusStorage::new_with_path(db_path)?;
    Ok(RocksConsensusStorage::new_with_stream_store(
        storage.db.clone(),
        stream_store,
    ))
}
