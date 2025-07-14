//! Raft state machine implementations
//!
//! This module consolidates the state machines for both global and local
//! consensus layers, providing a unified interface for state management.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

pub mod global;
pub mod group;
pub mod local;

// Re-export state machines
pub use group::GroupStateMachine;
pub use local::{LocalStreamMetadata, StorageBackedLocalState};

// Alias for consistency
pub type LocalStateMachine = StorageBackedLocalState;
// New alias for group state machine (will replace LocalStateMachine)
pub type GroupStateType = GroupStateMachine;

/// Base trait for consensus state machines
#[async_trait]
pub trait ConsensusStateMachine: Send + Sync {
    /// The request type this state machine processes
    type Request: Send + Sync + Debug + Serialize + for<'de> Deserialize<'de>;

    /// The response type this state machine returns
    type Response: Send + Sync + Debug + Serialize + for<'de> Deserialize<'de>;

    /// The state type this state machine manages
    type State: Send + Sync;

    /// The snapshot data type
    type SnapshotData: Send + Sync;

    /// Apply a request to the state machine
    async fn apply(&mut self, request: Self::Request) -> Self::Response;

    /// Get a reference to the current state
    fn state(&self) -> &Self::State;

    /// Create a snapshot of the current state
    async fn snapshot(
        &self,
    ) -> Result<Self::SnapshotData, Box<dyn std::error::Error + Send + Sync>>;

    /// Restore state from a snapshot
    async fn restore(
        &mut self,
        snapshot: Self::SnapshotData,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}
